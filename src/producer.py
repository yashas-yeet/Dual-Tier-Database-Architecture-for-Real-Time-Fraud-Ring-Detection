# =============================================================================
# StreamGraph — Phase 2: Data Generator & Kafka Ingestion (producer.py)
# =============================================================================
# IEEE Research Project: Dual-Tier Database Architecture for Real-Time
# Financial Fraud Ring Detection
#
# PURPOSE:
# This script is the entry point of the StreamGraph pipeline. It reads a
# synthetic financial transaction dataset (PaySim format), serializes each
# row as a JSON message, and publishes it to the Kafka topic `raw-transactions`
# at a controllable rate.
#
# CONTROLLABLE RATE (for benchmarking):
# The `--tps` flag lets us set a target throughput in transactions per second.
# This is critical for the IEEE paper because we need to:
#   1. Measure maximum sustainable throughput (set --tps 0 for no throttling).
#   2. Measure latency at various load levels (e.g., 500, 1000, 5000 TPS).
#   3. Identify the saturation point where Tier 1 (Flink) starts backpressuring.
#
# PAYSIM DATASET:
# PaySim is a synthetic dataset that simulates mobile money transactions.
# It was generated using a multi-agent simulation calibrated on real
# financial logs from a mobile money service in an African country.
# Reference: Lopez-Rojas, E.A., Elmir, A., & Axelsson, S. (2016).
# "PaySim: A financial mobile money simulator for fraud detection."
#
# Schema (11 columns):
#   step        - Integer. Maps to 1 hour of real-world time (730 steps = 30 days).
#   type        - String. Transaction type: CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER.
#   amount      - Float. Transaction amount in local currency.
#   nameOrig    - String. Customer initiating the transaction (e.g., "C1234567890").
#   oldbalanceOrg  - Float. Sender's balance before the transaction.
#   newbalanceOrig - Float. Sender's balance after the transaction.
#   nameDest    - String. Recipient of the transaction (e.g., "M9876543210").
#   oldbalanceDest - Float. Recipient's balance before.
#   newbalanceDest - Float. Recipient's balance after.
#   isFraud     - Integer (0/1). Ground truth label: 1 = fraudulent.
#   isFlaggedFraud - Integer (0/1). Naive rule-based flag (not used by us).
#
# KEY DESIGN DECISIONS:
# 1. We use confluent-kafka (librdkafka wrapper) rather than kafka-python
#    because librdkafka's C implementation achieves 10-100x higher throughput
#    and supports idempotent production natively.
# 2. We partition by `nameOrig` (hash-based) to ensure all transactions from
#    the same account land on the same Kafka partition, which in turn ensures
#    they're processed by the same Flink subtask — a prerequisite for correct
#    per-account sliding window aggregation in Tier 1.
# 3. We inject a `producer_timestamp_ms` field into each message to enable
#    end-to-end latency measurement (producer → Flink → Neo4j).
# =============================================================================

import os
import sys
from pathlib import Path

# Auto-switch to project virtual environment if running with system Python
_project_root = Path(__file__).resolve().parent.parent
_venv_win = _project_root / ".venv" / "Scripts" / "python.exe"
_venv_nix = _project_root / ".venv" / "bin" / "python"
_venv_py = _venv_win if _venv_win.exists() else (_venv_nix if _venv_nix.exists() else None)

if _venv_py and Path(sys.executable).resolve() != _venv_py.resolve():
    import subprocess
    sys.exit(subprocess.call([str(_venv_py), __file__] + sys.argv[1:]))

import csv
import json
import time
import uuid
import signal
import logging
import argparse
import statistics
from pathlib import Path
from typing import Optional, Dict, Any, List

# ---------------------------------------------------------------------------
# confluent-kafka: High-performance Kafka producer based on librdkafka.
# The SerializingProducer wraps the raw Producer with pluggable serializers,
# but for JSON we handle serialization manually for full control over the
# schema and error handling.
# ---------------------------------------------------------------------------
from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# ---------------------------------------------------------------------------
# python-dotenv: Loads environment variables from the .env file into
# os.environ, enabling externalized configuration without modifying code.
# ---------------------------------------------------------------------------
from dotenv import load_dotenv

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
# We configure structured logging with timestamps, log level, and module name.
# For the IEEE paper, log output serves as an audit trail showing:
# - Exact throughput achieved at each reporting interval.
# - Any delivery failures or retries (indicating Kafka backpressure).
# - Total ingestion time and message count for reproducibility.
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # Also log to a file for post-hoc analysis.
        logging.FileHandler("producer.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("streamgraph.producer")


# =============================================================================
# PERFORMANCE METRICS COLLECTOR
# =============================================================================
# This class accumulates fine-grained performance metrics during the ingestion
# run. After completion, it produces a summary suitable for inclusion in the
# IEEE paper's evaluation section.
#
# Metrics tracked:
# - Total messages sent and confirmed.
# - Delivery errors (if any).
# - Throughput (messages/sec) over sliding intervals.
# - Delivery latency percentiles (p50, p95, p99).
# - Fraud vs. non-fraud message distribution.
# =============================================================================
class ProducerMetrics:
    """
    Thread-safe (single-threaded producer) metrics collector for Kafka
    message production. Collects delivery latencies, throughput measurements,
    and error counts for academic benchmarking.
    """

    def __init__(self):
        # Counters
        self.total_sent: int = 0            # Messages handed to librdkafka
        self.total_delivered: int = 0       # Confirmed by broker (acked)
        self.total_errors: int = 0          # Delivery failures
        self.total_fraud: int = 0           # Messages with isFraud=1
        self.total_legitimate: int = 0      # Messages with isFraud=0

        # Latency tracking (delivery callback latency in ms)
        self.delivery_latencies_ms: List[float] = []

        # Throughput tracking
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

        # Interval tracking (for periodic reporting)
        self.interval_start: float = time.monotonic()
        self.interval_count: int = 0
        self.interval_latencies: List[float] = []

        # Transaction type distribution
        self.type_counts: Dict[str, int] = {}

    def record_send(self, is_fraud: bool, txn_type: str) -> None:
        """Record that a message was handed to the producer's internal buffer."""
        self.total_sent += 1
        self.interval_count += 1
        if is_fraud:
            self.total_fraud += 1
        else:
            self.total_legitimate += 1
        self.type_counts[txn_type] = self.type_counts.get(txn_type, 0) + 1

    def record_delivery(self, latency_ms: float) -> None:
        """Record a successful delivery acknowledgment from the broker."""
        self.total_delivered += 1
        self.delivery_latencies_ms.append(latency_ms)
        self.interval_latencies.append(latency_ms)

    def record_error(self) -> None:
        """Record a delivery failure."""
        self.total_errors += 1

    def get_interval_stats(self, reset: bool = True) -> Dict[str, Any]:
        """
        Compute throughput and latency stats for the current reporting interval.
        Optionally resets the interval counters.

        Returns:
            Dictionary with interval TPS, latency percentiles, and counts.
        """
        now = time.monotonic()
        elapsed = now - self.interval_start
        tps = self.interval_count / elapsed if elapsed > 0 else 0.0

        lat_stats = {}
        if self.interval_latencies:
            sorted_lats = sorted(self.interval_latencies)
            lat_stats = {
                "p50_ms": round(sorted_lats[len(sorted_lats) // 2], 2),
                "p95_ms": round(sorted_lats[int(len(sorted_lats) * 0.95)], 2),
                "p99_ms": round(sorted_lats[int(len(sorted_lats) * 0.99)], 2),
                "max_ms": round(sorted_lats[-1], 2),
                "mean_ms": round(statistics.mean(sorted_lats), 2),
            }

        stats = {
            "interval_tps": round(tps, 1),
            "interval_count": self.interval_count,
            "interval_seconds": round(elapsed, 2),
            "total_sent": self.total_sent,
            "total_delivered": self.total_delivered,
            "total_errors": self.total_errors,
            **lat_stats,
        }

        if reset:
            self.interval_start = now
            self.interval_count = 0
            self.interval_latencies = []

        return stats

    def get_final_summary(self) -> Dict[str, Any]:
        """
        Compute the final summary statistics for the entire ingestion run.
        This is the data that goes into the IEEE paper's Table 1.
        """
        total_elapsed = (self.end_time or time.monotonic()) - (self.start_time or 0)
        overall_tps = self.total_sent / total_elapsed if total_elapsed > 0 else 0

        summary = {
            "total_messages_sent": self.total_sent,
            "total_messages_delivered": self.total_delivered,
            "total_delivery_errors": self.total_errors,
            "delivery_success_rate": f"{(self.total_delivered / max(self.total_sent, 1)) * 100:.2f}%",
            "total_elapsed_seconds": round(total_elapsed, 2),
            "overall_throughput_tps": round(overall_tps, 1),
            "fraud_messages": self.total_fraud,
            "legitimate_messages": self.total_legitimate,
            "fraud_ratio": f"{(self.total_fraud / max(self.total_sent, 1)) * 100:.4f}%",
            "transaction_type_distribution": self.type_counts,
        }

        # Global latency percentiles
        if self.delivery_latencies_ms:
            sorted_all = sorted(self.delivery_latencies_ms)
            summary["latency_p50_ms"] = round(sorted_all[len(sorted_all) // 2], 2)
            summary["latency_p95_ms"] = round(sorted_all[int(len(sorted_all) * 0.95)], 2)
            summary["latency_p99_ms"] = round(sorted_all[int(len(sorted_all) * 0.99)], 2)
            summary["latency_max_ms"] = round(sorted_all[-1], 2)
            summary["latency_mean_ms"] = round(statistics.mean(sorted_all), 2)
            summary["latency_stddev_ms"] = round(
                statistics.stdev(sorted_all) if len(sorted_all) > 1 else 0.0, 2
            )

        return summary


# =============================================================================
# KAFKA PRODUCER CONFIGURATION
# =============================================================================
# This function builds the librdkafka configuration dictionary. Each parameter
# is explained in terms of its impact on correctness and performance.
#
# FOR THE IEEE PAPER: The producer configuration directly affects the
# "ingestion throughput" metric. Key trade-offs:
# - linger.ms vs. latency: Higher linger → larger batches → higher throughput
#   but increased per-message latency.
# - batch.size vs. memory: Larger batches use more memory but reduce the
#   number of network round-trips.
# - acks vs. durability: acks=all ensures every message is replicated before
#   acknowledgment, at the cost of ~1ms additional latency per message.
# =============================================================================
def build_producer_config(bootstrap_servers: str) -> Dict[str, Any]:
    """
    Constructs the librdkafka configuration dictionary for the Kafka producer.

    Args:
        bootstrap_servers: Comma-separated list of Kafka broker addresses.

    Returns:
        Configuration dictionary suitable for confluent_kafka.Producer().
    """
    config = {
        # -------------------------------------------------------------------
        # CONNECTION
        # -------------------------------------------------------------------
        # Bootstrap servers: Initial contact points for the producer to
        # discover the full Kafka cluster topology. In our single-broker
        # setup, this is just localhost:9093 (the external listener).
        "bootstrap.servers": bootstrap_servers,

        # Client ID: Identifies this producer in broker logs and metrics.
        # Useful for debugging when multiple producers exist.
        "client.id": "streamgraph-producer",

        # -------------------------------------------------------------------
        # DURABILITY / RELIABILITY
        # -------------------------------------------------------------------
        # acks=all (-1): The broker waits for ALL in-sync replicas (ISRs)
        # to acknowledge the write before responding to the producer.
        # In our single-broker setup (replication factor = 1), this is
        # equivalent to acks=1, but we set it to 'all' for correctness
        # in case the cluster is later scaled to 3 brokers.
        "acks": "all",

        # Enable idempotent producer. This ensures exactly-once delivery
        # semantics by assigning a producer ID (PID) and sequence number
        # to each message. The broker deduplicates any retried messages.
        # Without this, network retries could cause duplicate messages.
        #
        # TRADE-OFF: Idempotent production adds ~0.5ms overhead per batch
        # but eliminates the possibility of duplicate records in the
        # commit log — essential for accurate fraud detection.
        "enable.idempotence": True,

        # Maximum number of unacknowledged requests per connection.
        # With idempotent production, this MUST be <= 5 (librdkafka
        # enforces this). Setting to 5 allows pipelining for throughput
        # while maintaining ordering guarantees.
        "max.in.flight.requests.per.connection": 5,

        # Number of retries for transient broker errors (e.g., NOT_LEADER).
        # With idempotent production, retries are safe (no duplicates).
        "retries": 2147483647,  # Effectively infinite (INT_MAX)

        # Retry backoff: Wait 100ms between retries to avoid overwhelming
        # a recovering broker.
        "retry.backoff.ms": 100,

        # -------------------------------------------------------------------
        # BATCHING & THROUGHPUT
        # -------------------------------------------------------------------
        # Linger time: The producer waits up to this many milliseconds for
        # additional messages to fill the batch before sending. Higher values
        # increase throughput (larger batches = fewer network round-trips)
        # at the cost of added latency.
        #
        # For benchmarking, we parameterize this via the .env file so we
        # can sweep over different values and measure the effect on TPS.
        "linger.ms": int(os.getenv("PRODUCER_LINGER_MS", "20")),

        # Batch size: Maximum size (in bytes) of a single message batch.
        # librdkafka accumulates messages in memory until either:
        #   (a) The batch reaches this size, OR
        #   (b) linger.ms expires.
        # Then the batch is compressed (if configured) and sent.
        # 64 KB is a good default for small JSON messages (~500 bytes each).
        "batch.size": int(os.getenv("PRODUCER_BATCH_SIZE", "65536")),

        # Compression: lz4 provides excellent compression ratio with minimal
        # CPU overhead. This reduces network bandwidth and broker disk I/O.
        # PaySim JSON messages compress well (~60% reduction) due to
        # repeated field names.
        "compression.type": "lz4",

        # -------------------------------------------------------------------
        # BUFFER MANAGEMENT
        # -------------------------------------------------------------------
        # Total memory available to the producer for buffering unsent
        # messages. If this buffer fills up (i.e., broker can't keep up),
        # produce() calls will block or raise BufferError.
        # 128 MB is generous for our research prototype.
        "queue.buffering.max.messages": 500000,
        "queue.buffering.max.kbytes": 131072,  # 128 MB

        # -------------------------------------------------------------------
        # MONITORING
        # -------------------------------------------------------------------
        # Statistics interval: librdkafka emits internal metrics at this
        # interval (in ms). We can capture these for deep performance analysis.
        "statistics.interval.ms": 5000,
    }

    return config


# =============================================================================
# PAYSIM TRANSACTION PARSER
# =============================================================================
# This class handles reading and parsing the PaySim CSV file. It supports:
# 1. Chunked reading for memory efficiency with large datasets.
# 2. Schema validation to catch malformed rows early.
# 3. JSON serialization with injected metadata (UUID, timestamp).
# =============================================================================
class PaySimParser:
    """
    Reads and parses the PaySim synthetic dataset CSV file.

    The parser validates each row against the expected schema and transforms
    it into a JSON-serializable dictionary with additional metadata fields
    required by the StreamGraph pipeline.
    """

    # Expected CSV column names (PaySim v1 schema).
    EXPECTED_COLUMNS = [
        "step", "type", "amount", "nameOrig", "oldbalanceOrg",
        "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest",
        "isFraud", "isFlaggedFraud",
    ]

    # Valid transaction types in PaySim.
    VALID_TYPES = {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}

    def __init__(self, csv_path: str, inject_rings: bool = True, ring_interval: int = 350):
        """
        Initialize the parser with the path to the PaySim CSV file.

        Args:
            csv_path: Absolute or relative path to the CSV file.
            inject_rings: If True, interleaves circular fraud ring transactions into the stream.
            ring_interval: Number of normal transactions between fraud ring injections.

        Raises:
            FileNotFoundError: If the CSV file does not exist.
        """
        self.csv_path = Path(csv_path)
        self.inject_rings = inject_rings
        self.ring_interval = ring_interval
        self._ring_counter = 0

        # Coordinated circular fraud rings (3-hop, 4-hop, and 5-hop cycles)
        self._fraud_rings = [
            ["C1084920412", "C7749102834", "C3382910482"],  # 3-hop
            ["C9912048123", "C5581920391", "C2281904812", "C8839102941"],  # 4-hop
            ["C4491820391", "C6629103948", "C1192840192", "C7738192048", "C9948192038"],  # 5-hop
            ["C8192019284", "C3301928401", "C5592810492"],  # 3-hop
            ["C6619284019", "C2291048192", "C8849102938", "C4401928491"],  # 4-hop
            ["C1102938491", "C5582910482", "C9928104918"],  # 3-hop
            ["C7728193049", "C3391028491", "C6601928374", "C2281930491"],  # 4-hop
        ]

        if not self.csv_path.exists():
            raise FileNotFoundError(
                f"PaySim dataset not found at: {self.csv_path.absolute()}\n"
                f"Download from: https://www.kaggle.com/datasets/ealaxi/paysim1\n"
                f"Place the CSV in: {self.csv_path.parent.absolute()}"
            )

        # Count total rows for progress reporting (subtract 1 for header).
        # We use a fast line-count method that doesn't parse CSV structure.
        self.total_rows = self._count_rows()
        logger.info(
            f"PaySim dataset loaded: {self.csv_path.name} "
            f"({self.total_rows:,} transactions)"
            f"{' [Circular Fraud Ring Injection ACTIVE]' if inject_rings else ''}"
        )

    def _count_rows(self) -> int:
        """
        Count the number of data rows in the CSV file efficiently.
        Uses a buffered binary read for speed on large files.
        """
        count = 0
        with open(self.csv_path, "rb") as f:
            # Skip header line
            f.readline()
            # Count remaining lines
            buf_size = 1024 * 1024  # 1 MB buffer
            while True:
                buf = f.read(buf_size)
                if not buf:
                    break
                count += buf.count(b"\n")
        return count

    def _validate_headers(self, headers: List[str]) -> None:
        """
        Validate that the CSV headers match the expected PaySim schema.

        Args:
            headers: List of column names from the CSV header row.

        Raises:
            ValueError: If headers don't match the expected schema.
        """
        # Normalize headers: strip whitespace and convert to lowercase for
        # robust matching (some CSV exports add trailing spaces).
        normalized = [h.strip().lower() for h in headers]
        expected_lower = [c.lower() for c in self.EXPECTED_COLUMNS]

        # Check that all expected columns are present (order doesn't matter).
        missing = set(expected_lower) - set(normalized)
        if missing:
            raise ValueError(
                f"CSV is missing expected columns: {missing}\n"
                f"Expected: {self.EXPECTED_COLUMNS}\n"
                f"Found: {headers}"
            )

    def iterate_transactions(self):
        """
        Generator that yields parsed transaction dictionaries from the CSV.

        Each yielded dictionary contains:
        - All original PaySim fields (with correct types).
        - `transaction_id`: A UUID4 string for unique identification.
        - `producer_timestamp_ms`: Epoch milliseconds when the message was
          created. This is injected at production time (not read time) for
          accurate end-to-end latency measurement.

        Yields:
            Dict[str, Any]: A single parsed transaction.

        Raises:
            ValueError: If a row fails schema validation.
        """
        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Validate headers on first access.
            if reader.fieldnames:
                self._validate_headers(reader.fieldnames)

            for row_num, row in enumerate(reader, start=1):
                try:
                    # ---------------------------------------------------------
                    # TYPE CASTING
                    # ---------------------------------------------------------
                    # CSV readers return all values as strings. We must cast
                    # to the correct types for downstream processing (Flink
                    # expects typed fields for windowed aggregation).
                    # ---------------------------------------------------------
                    transaction = {
                        # Unique transaction ID (UUID4).
                        # UUID4 uses random bytes, providing a collision-resistant
                        # identifier without coordination — essential in a
                        # distributed system where multiple producers might
                        # run concurrently.
                        "transaction_id": str(uuid.uuid4()),

                        # Simulation step (integer). Each step represents 1 hour
                        # in the PaySim simulation. We'll use this to derive
                        # synthetic timestamps for the sliding window.
                        "step": int(row["step"]),

                        # Transaction type: one of CASH_IN, CASH_OUT, DEBIT,
                        # PAYMENT, or TRANSFER.
                        "type": row["type"].strip(),

                        # Transaction amount (float, 2 decimal places).
                        "amount": round(float(row["amount"]), 2),

                        # Sender account identifier. This is our partition key
                        # for Kafka and the keying field for Flink's
                        # KeyedProcessFunction.
                        "nameOrig": row["nameOrig"].strip(),

                        # Sender's balance before and after the transaction.
                        # These are used for balance-based anomaly detection
                        # (not implemented in Tier 1, but available for
                        # future extension).
                        "oldbalanceOrg": round(float(row["oldbalanceOrg"]), 2),
                        "newbalanceOrig": round(float(row["newbalanceOrig"]), 2),

                        # Recipient account identifier.
                        "nameDest": row["nameDest"].strip(),

                        # Recipient's balance before and after.
                        "oldbalanceDest": round(float(row["oldbalanceDest"]), 2),
                        "newbalanceDest": round(float(row["newbalanceDest"]), 2),

                        # Ground truth label: 1 = fraudulent, 0 = legitimate.
                        # This is used ONLY for evaluation (computing precision,
                        # recall, F1 in the paper). It is NOT used by the
                        # detection logic — that would be data leakage.
                        "isFraud": int(row["isFraud"]),

                        # =====================================================
                        # INJECTED METADATA (not in original PaySim dataset)
                        # =====================================================

                        # Producer timestamp: Epoch milliseconds at the moment
                        # this message is created. Used to measure end-to-end
                        # latency: (Flink processing time) - (producer timestamp).
                        #
                        # NOTE: We inject this at iteration time, not at
                        # produce() time, because the TPS throttling loop may
                        # add variable delay. For precise latency measurement,
                        # we override this in the produce() callback.
                        "producer_timestamp_ms": int(time.time() * 1000),
                    }

                    # Validate transaction type.
                    if transaction["type"] not in self.VALID_TYPES:
                        logger.warning(
                            f"Row {row_num}: Unknown transaction type "
                            f"'{transaction['type']}'. Skipping."
                        )
                        continue

                    # Validate amount is non-negative.
                    if transaction["amount"] < 0:
                        logger.warning(
                            f"Row {row_num}: Negative amount "
                            f"{transaction['amount']}. Skipping."
                        )
                        continue

                    yield transaction

                    # Interleave coordinated circular fraud ring transactions into the live stream
                    if self.inject_rings and row_num % self.ring_interval == 0:
                        ring = self._fraud_rings[self._ring_counter % len(self._fraud_rings)]
                        self._ring_counter += 1
                        ring_amount = round(random.uniform(15000.0, 48000.0), 2)
                        step = transaction["step"]
                        now_ms = int(time.time() * 1000)

                        for i in range(len(ring)):
                            src = ring[i]
                            dst = ring[(i + 1) % len(ring)]
                            yield {
                                "transaction_id": str(uuid.uuid4()),
                                "step": step,
                                "type": "TRANSFER",
                                "amount": ring_amount,
                                "nameOrig": src,
                                "oldbalanceOrg": 60000.0,
                                "newbalanceOrig": 60000.0 - ring_amount,
                                "nameDest": dst,
                                "oldbalanceDest": 10000.0,
                                "newbalanceDest": 10000.0 + ring_amount,
                                "isFraud": 1,
                                "is_fraud_ring": True,
                                "ring_hop_index": i + 1,
                                "ring_total_hops": len(ring),
                                "producer_timestamp_ms": now_ms,
                            }

                except (ValueError, KeyError) as e:
                    logger.error(
                        f"Row {row_num}: Failed to parse — {type(e).__name__}: {e}"
                    )
                    continue


# =============================================================================
# RATE LIMITER (Token Bucket Algorithm)
# =============================================================================
# Implements a token bucket rate limiter to control the ingestion rate.
#
# FOR THE IEEE PAPER: The token bucket algorithm allows short bursts above
# the target rate while maintaining the average rate over time. This is
# more realistic than a strict per-message sleep because real-world
# transaction arrivals are bursty (Poisson-like), not uniform.
#
# How it works:
# - The bucket holds up to `burst_size` tokens.
# - Tokens are added at a rate of `target_tps` per second.
# - Each message consumes one token.
# - If no tokens are available, the producer sleeps until a token is generated.
# - This naturally smooths throughput to the target TPS.
# =============================================================================
class TokenBucketRateLimiter:
    """
    Token bucket rate limiter for controlling Kafka producer throughput.

    The token bucket is a well-studied algorithm in network traffic shaping
    (RFC 2697, RFC 2698). We apply it here to control the rate at which
    transactions are injected into the Kafka pipeline.
    """

    def __init__(self, target_tps: float, burst_size: int = 100):
        """
        Initialize the rate limiter.

        Args:
            target_tps: Target transactions per second. Set to 0 for
                        unlimited throughput (no throttling).
            burst_size: Maximum burst size (tokens the bucket can hold).
                        Allows short-term throughput spikes.
        """
        self.target_tps = target_tps
        self.burst_size = burst_size
        self.tokens = burst_size  # Start with a full bucket
        self.last_refill_time = time.monotonic()
        self.unlimited = target_tps <= 0

        if self.unlimited:
            logger.info("Rate limiter: UNLIMITED (no throttling)")
        else:
            logger.info(
                f"Rate limiter: {target_tps:.0f} TPS target, "
                f"burst size: {burst_size}"
            )

    def acquire(self) -> None:
        """
        Acquire a token, blocking if necessary until one is available.

        This method is called before each produce() call. If tokens are
        available, it returns immediately. Otherwise, it sleeps for the
        minimum time needed to generate a new token.
        """
        if self.unlimited:
            return

        now = time.monotonic()
        elapsed = now - self.last_refill_time

        # Refill tokens based on elapsed time.
        # tokens_to_add = elapsed * target_tps
        # Cap at burst_size to prevent unbounded accumulation.
        self.tokens += elapsed * self.target_tps
        self.tokens = min(self.tokens, self.burst_size)
        self.last_refill_time = now

        if self.tokens >= 1.0:
            # A token is available; consume it immediately.
            self.tokens -= 1.0
        else:
            # No tokens available; sleep until one is generated.
            # sleep_time = (1.0 - self.tokens) / self.target_tps
            sleep_time = (1.0 - self.tokens) / self.target_tps
            time.sleep(sleep_time)
            self.tokens = 0.0
            self.last_refill_time = time.monotonic()


# =============================================================================
# KAFKA PRODUCER WRAPPER
# =============================================================================
# Wraps the confluent-kafka Producer with:
# 1. Delivery callback handling for metrics collection.
# 2. Partitioning by nameOrig for account-level ordering.
# 3. Periodic flush and metrics reporting.
# 4. Graceful shutdown on SIGINT/SIGTERM.
# =============================================================================
class StreamGraphProducer:
    """
    High-level Kafka producer for the StreamGraph pipeline.

    Responsibilities:
    - Serialize PaySim transactions to JSON and publish to Kafka.
    - Partition messages by nameOrig for per-account ordering.
    - Collect delivery latency metrics for benchmarking.
    - Enforce TPS rate limits via a token bucket.
    - Handle graceful shutdown and final flush.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        target_tps: float = 0,
        report_interval: int = 10,
    ):
        """
        Initialize the StreamGraph Kafka producer.

        Args:
            bootstrap_servers: Kafka bootstrap server addresses.
            topic: Target Kafka topic name.
            target_tps: Target throughput (0 = unlimited).
            report_interval: Seconds between periodic metric reports.
        """
        self.topic = topic
        self.report_interval = report_interval
        self.metrics = ProducerMetrics()
        self.rate_limiter = TokenBucketRateLimiter(target_tps)
        self._shutdown_requested = False

        # Build producer configuration.
        config = build_producer_config(bootstrap_servers)
        logger.info(f"Kafka producer configuration: { {k: v for k, v in config.items() if 'password' not in k.lower()} }")

        # Instantiate the confluent-kafka Producer.
        # The Producer object maintains an internal message queue and a
        # background thread (the "librdkafka delivery thread") that handles
        # network I/O with the Kafka broker. produce() is non-blocking;
        # it enqueues the message and returns immediately. The delivery
        # callback fires asynchronously when the broker acknowledges (or
        # rejects) the message.
        self.producer = Producer(config)

        # Register signal handlers for graceful shutdown.
        # SIGINT (Ctrl+C) and SIGTERM (Docker stop) trigger a clean flush.
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info(
            f"Producer initialized | topic={topic} | "
            f"target_tps={target_tps} | "
            f"bootstrap_servers={bootstrap_servers}"
        )

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signal_name = signal.Signals(signum).name
        logger.info(f"Received {signal_name} — initiating graceful shutdown...")
        self._shutdown_requested = True

    def _delivery_callback(self, err, msg):
        """
        Callback invoked by librdkafka when a message is delivered (or fails).

        This callback runs on the librdkafka background thread. We use it to:
        1. Track delivery success/failure counts.
        2. Measure delivery latency (time between produce() call and broker ack).

        Args:
            err: KafkaError object if delivery failed, None on success.
            msg: The Message object that was delivered/failed.
        """
        if err is not None:
            # Delivery failed. Common causes:
            # - MSG_SIZE_TOO_LARGE: Message exceeds max.message.bytes.
            # - UNKNOWN_TOPIC_OR_PART: Topic doesn't exist (auto-create disabled).
            # - QUEUE_FULL: Producer's internal buffer is full (backpressure).
            self.metrics.record_error()
            logger.error(
                f"Delivery FAILED | topic={msg.topic()} | "
                f"partition={msg.partition()} | "
                f"error={err.str()}"
            )
        else:
            # Delivery succeeded. Compute latency from the timestamp we
            # embedded in the message headers.
            latency_ms = 0.0
            headers = msg.headers()
            if headers:
                for key, value in headers:
                    if key == "produce_time_ns":
                        produce_time_ns = int(value.decode("utf-8"))
                        latency_ms = (time.monotonic_ns() - produce_time_ns) / 1e6
                        break
            self.metrics.record_delivery(latency_ms)

    def produce_transaction(self, transaction: Dict[str, Any]) -> bool:
        """
        Serialize and publish a single transaction to Kafka.

        Args:
            transaction: Parsed PaySim transaction dictionary.

        Returns:
            True if the message was enqueued successfully, False otherwise.
        """
        try:
            # Apply rate limiting (blocks if necessary).
            self.rate_limiter.acquire()

            # Refresh the producer timestamp to the actual produce() time
            # for accurate latency measurement.
            transaction["producer_timestamp_ms"] = int(time.time() * 1000)

            # Serialize the transaction to JSON bytes.
            # We use json.dumps() with ensure_ascii=False for efficiency
            # (avoids escaping non-ASCII characters that aren't present
            # in PaySim data anyway).
            value_bytes = json.dumps(transaction, ensure_ascii=False).encode("utf-8")

            # Partition key: nameOrig (sender account ID).
            # confluent-kafka hashes this key using murmur2 (by default)
            # to determine the target partition. This ensures all transactions
            # from the same account go to the same partition, maintaining
            # per-account ordering — critical for the sliding window in Tier 1.
            key_bytes = transaction["nameOrig"].encode("utf-8")

            # Produce the message with a delivery callback.
            # Headers include a high-resolution timestamp for latency measurement.
            self.producer.produce(
                topic=self.topic,
                key=key_bytes,
                value=value_bytes,
                headers={
                    "produce_time_ns": str(time.monotonic_ns()).encode("utf-8"),
                    "transaction_id": transaction["transaction_id"].encode("utf-8"),
                },
                callback=self._delivery_callback,
            )

            # Record the send in metrics.
            self.metrics.record_send(
                is_fraud=bool(transaction["isFraud"]),
                txn_type=transaction["type"],
            )

            # Poll the producer to trigger delivery callbacks.
            # poll(0) is non-blocking; it processes any pending callbacks
            # without waiting. This is essential to prevent the callback
            # queue from growing unboundedly.
            self.producer.poll(0)

            return True

        except BufferError:
            # The producer's internal queue is full. This indicates that
            # the broker can't keep up with our production rate (backpressure).
            # We flush (blocking) to drain the queue, then retry.
            logger.warning(
                "Producer buffer full (backpressure detected). "
                "Flushing pending messages..."
            )
            self.producer.flush(timeout=10.0)
            return False

        except KafkaException as e:
            logger.error(f"Kafka error during produce: {e}")
            self.metrics.record_error()
            return False

    def run(self, csv_path: str, max_messages: int = 0, inject_rings: bool = True) -> Dict[str, Any]:
        """
        Main ingestion loop. Reads the PaySim CSV and publishes each
        transaction to Kafka.

        Args:
            csv_path: Path to the PaySim CSV file.
            max_messages: Maximum number of messages to send (0 = all).
            inject_rings: Whether to interleave circular fraud ring transactions into the stream.

        Returns:
            Final performance metrics dictionary.
        """
        # Initialize the CSV parser.
        parser = PaySimParser(csv_path, inject_rings=inject_rings)

        logger.info("=" * 70)
        logger.info("StreamGraph Producer — Starting Ingestion Run")
        logger.info(f"  Dataset: {csv_path}")
        logger.info(f"  Total rows: {parser.total_rows:,}")
        logger.info(f"  Max messages: {'all' if max_messages == 0 else f'{max_messages:,}'}")
        logger.info(f"  Target TPS: {'unlimited' if self.rate_limiter.unlimited else self.rate_limiter.target_tps:.0f}")
        logger.info(f"  Topic: {self.topic}")
        logger.info("=" * 70)

        self.metrics.start_time = time.monotonic()
        last_report_time = time.monotonic()
        message_count = 0

        try:
            for transaction in parser.iterate_transactions():
                # Check for shutdown signal.
                if self._shutdown_requested:
                    logger.info("Shutdown requested — stopping ingestion.")
                    break

                # Check message limit.
                if 0 < max_messages <= message_count:
                    logger.info(f"Reached message limit: {max_messages:,}")
                    break

                # Produce the transaction.
                success = self.produce_transaction(transaction)
                if success:
                    message_count += 1

                # Periodic metrics reporting.
                now = time.monotonic()
                if now - last_report_time >= self.report_interval:
                    stats = self.metrics.get_interval_stats()
                    logger.info(
                        f"[METRICS] "
                        f"TPS={stats['interval_tps']:>8.1f} | "
                        f"Sent={stats['total_sent']:>10,} | "
                        f"Delivered={stats['total_delivered']:>10,} | "
                        f"Errors={stats['total_errors']:>5} | "
                        f"p50={stats.get('p50_ms', 'N/A')}ms | "
                        f"p99={stats.get('p99_ms', 'N/A')}ms"
                    )
                    last_report_time = now

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — stopping ingestion.")

        finally:
            # Flush all remaining messages in the producer's buffer.
            # This blocks until all messages are delivered or the timeout
            # expires. Essential for ensuring no data loss at shutdown.
            logger.info("Flushing remaining messages to Kafka...")
            remaining = self.producer.flush(timeout=30.0)
            if remaining > 0:
                logger.warning(
                    f"{remaining} messages were NOT delivered "
                    f"(timeout during flush)."
                )
            else:
                logger.info("All messages delivered successfully.")

            self.metrics.end_time = time.monotonic()

        # Print final summary.
        summary = self.metrics.get_final_summary()
        logger.info("=" * 70)
        logger.info("StreamGraph Producer — Ingestion Complete")
        logger.info("=" * 70)
        for key, value in summary.items():
            if isinstance(value, dict):
                logger.info(f"  {key}:")
                for k, v in value.items():
                    logger.info(f"    {k}: {v}")
            else:
                logger.info(f"  {key}: {value}")
        logger.info("=" * 70)

        # Export summary to JSON file for post-hoc analysis.
        self._export_metrics(summary)

        return summary

    def _export_metrics(self, summary: Dict[str, Any]) -> None:
        """
        Export the final metrics summary to a JSON file in the benchmarks
        directory. This file can be consumed by analysis scripts for
        generating the IEEE paper's performance charts.
        """
        benchmarks_dir = Path(os.getenv("BENCHMARK_OUTPUT_DIR", "./benchmarks"))
        benchmarks_dir.mkdir(parents=True, exist_ok=True)

        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_file = benchmarks_dir / f"producer_metrics_{timestamp}.json"

        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            logger.info(f"Metrics exported to: {output_file.absolute()}")
        except IOError as e:
            logger.error(f"Failed to export metrics: {e}")


# =============================================================================
# SYNTHETIC DATA GENERATOR (Fallback)
# =============================================================================
# If the user doesn't have the PaySim CSV, this generator creates synthetic
# transactions with similar statistical properties. This ensures the project
# can be tested and benchmarked without requiring a Kaggle download.
#
# The synthetic generator creates:
# - Realistic account IDs (C-prefixed for customers, M-prefixed for merchants).
# - Transaction amounts following a log-normal distribution (empirically
#   observed in financial transaction data).
# - A configurable fraud rate (default 0.13%, matching PaySim's actual rate).
# - Circular transaction patterns (A→B→C→A) to test Tier 2 cycle detection.
# =============================================================================
import random

class SyntheticPaySimGenerator:
    """
    Generates synthetic financial transactions mimicking the PaySim dataset.

    This is a fallback for when the actual PaySim CSV is unavailable.
    The generated data preserves the key statistical properties needed for
    testing the StreamGraph pipeline:
    1. Log-normal amount distribution.
    2. Power-law account activity (few accounts generate most transactions).
    3. Injected fraud rings (circular transaction paths for Tier 2 testing).
    """

    TRANSACTION_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]

    def __init__(
        self,
        num_transactions: int = 100000,
        num_accounts: int = 5000,
        fraud_rate: float = 0.0013,
        fraud_ring_size: int = 50,
        seed: int = 42,
    ):
        """
        Initialize the synthetic generator.

        Args:
            num_transactions: Total number of transactions to generate.
            num_accounts: Number of unique customer accounts.
            fraud_rate: Fraction of transactions that are fraudulent.
            fraud_ring_size: Number of fraud ring patterns to inject.
            seed: Random seed for reproducibility.
        """
        self.num_transactions = num_transactions
        self.num_accounts = num_accounts
        self.fraud_rate = fraud_rate
        self.fraud_ring_size = fraud_ring_size

        # Set random seeds for reproducibility — critical for academic research.
        random.seed(seed)

        # Pre-generate account IDs.
        # Customer accounts: C-prefixed, 10-digit numeric.
        # Merchant accounts: M-prefixed, 10-digit numeric.
        self.customer_accounts = [
            f"C{random.randint(1000000000, 9999999999)}"
            for _ in range(num_accounts)
        ]
        self.merchant_accounts = [
            f"M{random.randint(1000000000, 9999999999)}"
            for _ in range(num_accounts // 10)  # 10% as many merchants
        ]

        # Pre-generate fraud rings.
        # Each ring is a list of 3-5 accounts that will form circular
        # transaction paths (A→B→C→A).
        self.fraud_rings = self._generate_fraud_rings()

        # Account balances (for realistic balance tracking).
        self.balances: Dict[str, float] = {}

        logger.info(
            f"Synthetic generator initialized: "
            f"{num_transactions:,} transactions, "
            f"{num_accounts:,} accounts, "
            f"fraud rate: {fraud_rate * 100:.2f}%, "
            f"{len(self.fraud_rings)} fraud rings"
        )

    def _generate_fraud_rings(self) -> List[List[str]]:
        """
        Generate fraud ring structures for Tier 2 testing.

        Each ring consists of 3-5 accounts that will participate in
        circular transaction paths. The ring structure is:
        ring[0] → ring[1] → ring[2] → ... → ring[0]
        """
        rings = []
        for _ in range(self.fraud_ring_size):
            ring_size = random.randint(3, 5)
            ring = random.sample(self.customer_accounts, ring_size)
            rings.append(ring)
        return rings

    def _get_balance(self, account_id: str) -> float:
        """Get or initialize an account's balance."""
        if account_id not in self.balances:
            # Initialize with a log-normal balance (mean ~$5000).
            self.balances[account_id] = round(
                random.lognormvariate(mu=8.5, sigma=1.0), 2
            )
        return self.balances[account_id]

    def generate(self):
        """
        Generator that yields synthetic PaySim-like transactions.

        Yields:
            Dict[str, Any]: A single synthetic transaction.
        """
        fraud_ring_transactions = self._schedule_fraud_ring_transactions()

        for i in range(self.num_transactions):
            step = i // 100  # ~100 transactions per simulation step

            # Check if this transaction should be a fraud ring member.
            if i in fraud_ring_transactions:
                yield fraud_ring_transactions[i]
                continue

            # Determine if this is a fraudulent transaction.
            is_fraud = random.random() < self.fraud_rate

            # Select transaction type.
            # Fraud is concentrated in TRANSFER and CASH_OUT (as in real PaySim).
            if is_fraud:
                txn_type = random.choice(["TRANSFER", "CASH_OUT"])
            else:
                txn_type = random.choices(
                    self.TRANSACTION_TYPES,
                    weights=[0.15, 0.25, 0.05, 0.35, 0.20],  # Empirical PaySim distribution
                    k=1
                )[0]

            # Select sender and recipient.
            # Power-law selection: a few accounts generate most transactions.
            sender_idx = int(random.paretovariate(1.5)) % len(self.customer_accounts)
            sender = self.customer_accounts[sender_idx]

            if txn_type in ("PAYMENT", "DEBIT"):
                recipient = random.choice(self.merchant_accounts)
            else:
                recipient = random.choice(self.customer_accounts)

            # Generate amount (log-normal distribution).
            if is_fraud:
                # Fraudulent transactions tend to be larger.
                amount = round(random.lognormvariate(mu=9.0, sigma=1.5), 2)
            else:
                amount = round(random.lognormvariate(mu=7.0, sigma=1.2), 2)

            # Compute balances.
            old_balance_org = self._get_balance(sender)
            new_balance_org = round(max(0, old_balance_org - amount), 2)
            old_balance_dest = self._get_balance(recipient)
            new_balance_dest = round(old_balance_dest + amount, 2)

            # Update stored balances.
            self.balances[sender] = new_balance_org
            self.balances[recipient] = new_balance_dest

            yield {
                "transaction_id": str(uuid.uuid4()),
                "step": step,
                "type": txn_type,
                "amount": amount,
                "nameOrig": sender,
                "oldbalanceOrg": old_balance_org,
                "newbalanceOrig": new_balance_org,
                "nameDest": recipient,
                "oldbalanceDest": old_balance_dest,
                "newbalanceDest": new_balance_dest,
                "isFraud": 1 if is_fraud else 0,
                "producer_timestamp_ms": int(time.time() * 1000),
            }

    def _schedule_fraud_ring_transactions(self) -> Dict[int, Dict]:
        """
        Pre-schedule fraud ring transactions at specific indices in the
        transaction stream. This ensures that circular transaction paths
        appear in the data, allowing us to test Tier 2 cycle detection.

        Returns:
            Dictionary mapping transaction index → transaction dictionary.
        """
        scheduled = {}
        transactions_per_ring = 2  # Each ring member sends at least 2 transactions

        for ring in self.fraud_rings:
            # Schedule ring transactions at random positions in the stream.
            base_idx = random.randint(0, self.num_transactions - len(ring) * transactions_per_ring - 1)

            for hop in range(len(ring)):
                sender = ring[hop]
                recipient = ring[(hop + 1) % len(ring)]  # Circular: last → first

                for rep in range(transactions_per_ring):
                    idx = base_idx + hop * transactions_per_ring + rep
                    if idx < self.num_transactions:
                        amount = round(random.lognormvariate(mu=9.5, sigma=0.5), 2)
                        old_bal = self._get_balance(sender)

                        scheduled[idx] = {
                            "transaction_id": str(uuid.uuid4()),
                            "step": idx // 100,
                            "type": "TRANSFER",
                            "amount": amount,
                            "nameOrig": sender,
                            "oldbalanceOrg": old_bal,
                            "newbalanceOrig": round(max(0, old_bal - amount), 2),
                            "nameDest": recipient,
                            "oldbalanceDest": self._get_balance(recipient),
                            "newbalanceDest": round(self._get_balance(recipient) + amount, 2),
                            "isFraud": 1,
                            "producer_timestamp_ms": int(time.time() * 1000),
                        }

        logger.info(
            f"Scheduled {len(scheduled)} fraud ring transactions "
            f"across {len(self.fraud_rings)} rings"
        )
        return scheduled


# =============================================================================
# SYNTHETIC DATA → CSV EXPORT
# =============================================================================
# Exports synthetic data to a CSV file matching the PaySim format.
# This allows the same producer.py code to work with both real and
# synthetic data without any code changes.
# =============================================================================
def generate_synthetic_csv(output_path: str, num_transactions: int = 100000) -> str:
    """
    Generate a synthetic PaySim-format CSV file.

    Args:
        output_path: Path to write the CSV file.
        num_transactions: Number of transactions to generate.

    Returns:
        Absolute path to the generated CSV file.
    """
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    generator = SyntheticPaySimGenerator(num_transactions=num_transactions)

    logger.info(f"Generating synthetic dataset: {num_transactions:,} transactions...")

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Write header matching PaySim format.
        writer.writerow([
            "step", "type", "amount", "nameOrig", "oldbalanceOrg",
            "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest",
            "isFraud", "isFlaggedFraud",
        ])

        for txn in generator.generate():
            writer.writerow([
                txn["step"],
                txn["type"],
                txn["amount"],
                txn["nameOrig"],
                txn["oldbalanceOrg"],
                txn["newbalanceOrig"],
                txn["nameDest"],
                txn["oldbalanceDest"],
                txn["newbalanceDest"],
                txn["isFraud"],
                0,  # isFlaggedFraud (not used by StreamGraph)
            ])

    logger.info(f"Synthetic dataset saved to: {output.absolute()}")
    return str(output.absolute())


# =============================================================================
# COMMAND-LINE INTERFACE
# =============================================================================
# The CLI provides full control over the ingestion parameters for benchmarking:
#
#   python producer.py --csv data/paysim_dataset.csv --tps 1000
#   python producer.py --synthetic --num-transactions 500000 --tps 0
#   python producer.py --synthetic --tps 5000 --max-messages 100000
#
# FOR THE IEEE PAPER: Run with different --tps values to generate the
# "Throughput vs. Latency" chart. Typical sweep: 100, 500, 1000, 2000,
# 5000, 10000, 0 (unlimited).
# =============================================================================
def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments for the StreamGraph producer."""
    parser = argparse.ArgumentParser(
        description="StreamGraph Producer — Ingest PaySim transactions into Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest real PaySim CSV at 1000 TPS:
  python producer.py --csv data/paysim_dataset.csv --tps 1000

  # Generate synthetic data and ingest at max speed:
  python producer.py --synthetic --num-transactions 500000 --tps 0

  # Ingest first 10,000 messages from synthetic data at 2000 TPS:
  python producer.py --synthetic --tps 2000 --max-messages 10000

  # Use custom Kafka bootstrap servers:
  python producer.py --synthetic --bootstrap-servers kafka1:9092,kafka2:9092
        """,
    )

    # Data source (mutually exclusive).
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--csv",
        type=str,
        help="Path to the PaySim CSV dataset file.",
    )
    source_group.add_argument(
        "--synthetic",
        action="store_true",
        help="Use the built-in synthetic data generator instead of a CSV.",
    )

    # Synthetic data parameters.
    parser.add_argument(
        "--num-transactions",
        type=int,
        default=100000,
        help="Number of synthetic transactions to generate (default: 100,000).",
    )
    parser.add_argument(
        "--fraud-rate",
        type=float,
        default=0.0013,
        help="Fraud rate for synthetic data (default: 0.13%%).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for synthetic data reproducibility (default: 42).",
    )

    # Ingestion control.
    parser.add_argument(
        "--tps",
        type=float,
        default=float(os.getenv("PRODUCER_TARGET_TPS", "1000")),
        help="Target transactions per second (0 = unlimited, default: 1000).",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Maximum number of messages to send (0 = all, default: all).",
    )

    # Kafka configuration.
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
        help="Kafka bootstrap server addresses (default: localhost:9093).",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=os.getenv("KAFKA_TOPIC_RAW", "raw-transactions"),
        help="Kafka topic to publish to (default: raw-transactions).",
    )

    # Reporting.
    parser.add_argument(
        "--report-interval",
        type=int,
        default=int(os.getenv("BENCHMARK_EXPORT_INTERVAL", "10")),
        help="Seconds between periodic metric reports (default: 10).",
    )

    # Fraud ring injection into CSV stream
    parser.add_argument(
        "--inject-rings",
        dest="inject_rings",
        action="store_true",
        default=True,
        help="Interleave coordinated circular fraud rings into CSV stream (default: True).",
    )
    parser.add_argument(
        "--no-inject-rings",
        dest="inject_rings",
        action="store_false",
        help="Disable fraud ring injection into CSV stream.",
    )

    return parser.parse_args()


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================
def main():
    """
    Main entry point for the StreamGraph Kafka producer.

    Workflow:
    1. Load environment configuration from .env.
    2. Parse CLI arguments.
    3. Prepare data source (CSV or synthetic).
    4. Initialize the Kafka producer.
    5. Run the ingestion loop with rate limiting.
    6. Report final metrics.
    """
    # Load .env configuration.
    # python-dotenv searches for .env in the current directory and parent
    # directories, making it work regardless of where the script is invoked.
    load_dotenv()

    # Parse arguments.
    args = parse_arguments()

    logger.info("=" * 70)
    logger.info("StreamGraph Producer — Initialization")
    logger.info("=" * 70)

    # Prepare data source.
    if args.synthetic:
        # Generate synthetic data to a temporary CSV.
        csv_path = os.getenv("PAYSIM_CSV_PATH", "./data/paysim_synthetic.csv")
        logger.info("Mode: SYNTHETIC DATA GENERATION")
        generate_synthetic_csv(
            output_path=csv_path,
            num_transactions=args.num_transactions,
        )
    else:
        csv_path = args.csv
        logger.info(f"Mode: CSV INGESTION from {csv_path}")

    # Initialize the Kafka producer.
    producer = StreamGraphProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        target_tps=args.tps,
        report_interval=args.report_interval,
    )

    # Run the ingestion loop.
    summary = producer.run(
        csv_path=csv_path,
        max_messages=args.max_messages,
        inject_rings=args.inject_rings,
    )

    # Exit with appropriate code.
    if summary.get("total_delivery_errors", 0) > 0:
        logger.warning("Completed with delivery errors.")
        sys.exit(1)
    else:
        logger.info("Completed successfully.")
        sys.exit(0)


if __name__ == "__main__":
    main()
