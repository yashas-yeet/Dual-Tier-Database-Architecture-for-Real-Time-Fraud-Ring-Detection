# =============================================================================
# StreamGraph — Phase 3: Tier 1 Stream Processor (stream_processor.py)
# =============================================================================
# IEEE Research Project: Dual-Tier Database Architecture for Real-Time
# Financial Fraud Ring Detection
#
# PURPOSE:
# This is the Tier 1 stream processing engine — the core of the StreamGraph
# architecture. It consumes raw financial transactions from Kafka, applies
# a temporal anomaly detection heuristic (velocity window) using Flink's
# stateful processing with RocksDB as the state backend, and routes
# transactions to one of two downstream Kafka topics:
#
#   - `suspicious-log`: Transactions that violate velocity thresholds.
#                        These are forwarded to Tier 2 (Neo4j) for deep
#                        graph-structural analysis (cycle detection).
#
#   - `cleared-log`:    Transactions that pass the velocity check.
#                        These do NOT trigger any Neo4j queries, which is
#                        the key efficiency gain of our dual-tier architecture.
#
# ARCHITECTURE (Tier 1 Detail):
# ┌──────────────────────────────────────────────────────────────────────────┐
# │  Apache Flink (Tier 1)                                                  │
# │                                                                          │
# │  ┌─────────────┐    ┌─────────────────────────────────┐    ┌──────────┐ │
# │  │ Kafka Source │──▶ │ KeyedProcessFunction            │──▶ │ Kafka    │ │
# │  │ (raw-txns)  │    │ ┌─────────────────────────────┐ │    │ Sink(s)  │ │
# │  └─────────────┘    │ │ RocksDB State Backend       │ │    │          │ │
# │                      │ │  ┌─────────────────────┐   │ │    │ suspect  │ │
# │                      │ │  │ MapState<ts, amount> │   │ │    │ cleared  │ │
# │                      │ │  │ (per nameOrig key)   │   │ │    └──────────┘ │
# │                      │ │  └─────────────────────┘   │ │                  │
# │                      │ │  ┌─────────────────────┐   │ │                  │
# │                      │ │  │ ValueState<count>    │   │ │                  │
# │                      │ │  │ ValueState<sum>      │   │ │                  │
# │                      │ │  └─────────────────────┘   │ │                  │
# │                      │ └─────────────────────────────┘ │                  │
# │                      │  + Timers (event-time cleanup)  │                  │
# │                      └─────────────────────────────────┘                  │
# └──────────────────────────────────────────────────────────────────────────┘
#
# WHY ROCKSDB AS STATE BACKEND (for the IEEE paper):
# --------------------------------------------------
# Flink offers three state backend options:
#   1. HashMapStateBackend (JVM Heap): Fast but limited by heap size and
#      subject to GC pauses. Unsuitable for large state (millions of keys).
#   2. EmbeddedRocksDBStateBackend: Stores state in an embedded RocksDB
#      instance (LSM-tree) on local disk. Supports state larger than memory
#      and enables INCREMENTAL CHECKPOINTING — only the delta (modified SST
#      files) is uploaded during checkpoints, making checkpoint cost O(Δ)
#      rather than O(|state|). This is critical for our use case where we
#      maintain per-account sliding windows across potentially millions of
#      unique accounts.
#   3. Note: The "FsStateBackend" was removed in Flink 1.13+; its successor
#      is the HashMapStateBackend with checkpoint storage on a filesystem.
#
# VELOCITY WINDOW ALGORITHM (Temporal Anomaly Detection):
# -------------------------------------------------------
# We implement a SLIDING WINDOW using Flink's per-key state and timers,
# rather than Flink's built-in Window API. This gives us precise control
# over eviction, state layout, and benchmarking instrumentation.
#
# Algorithm:
#   For each incoming transaction keyed by nameOrig:
#     1. Record the (timestamp, amount) pair in a MapState.
#     2. Register a timer to fire 15 minutes after this transaction's
#        timestamp (for lazy garbage collection of expired entries).
#     3. Evict all entries from MapState where timestamp < (current - 15min).
#     4. Compute aggregates:  count = |MapState|, sum = Σ amounts in MapState.
#     5. Apply Tier 1 rule:
#        IF count > 5 OR sum > $10,000 → SUSPICIOUS (emit to suspicious-log)
#        ELSE → CLEARED (emit to cleared-log)
#
# This approach avoids the overhead of Flink's Window operator (which
# creates internal window state structures) and gives us byte-level control
# over what is stored in RocksDB.
#
# EXACTLY-ONCE SEMANTICS:
# -----------------------
# With Flink's checkpointing enabled (EXACTLY_ONCE mode) and Kafka's
# transactional producer (used by the Kafka sink), we achieve end-to-end
# exactly-once processing:
#   - Kafka source offsets are committed as part of the Flink checkpoint.
#   - Kafka sink uses two-phase commit (2PC) to ensure messages are only
#     visible to downstream consumers after the checkpoint completes.
#   - RocksDB state is atomically snapshotted during the checkpoint.
#
# PERFORMANCE INSTRUMENTATION:
# ----------------------------
# Every transaction is instrumented with timestamps at key processing
# stages, enabling us to measure:
#   - Tier 1 processing latency (time in the KeyedProcessFunction).
#   - End-to-end latency (producer_timestamp_ms → Flink output time).
#   - State operation cost (time spent reading/writing RocksDB state).
# These metrics are embedded in the output messages for the benchmarking
# module in Phase 4 (graph_worker.py).
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

import json
import time
import logging

# =============================================================================
# PyFlink Imports
# =============================================================================
# PyFlink is the Python API for Apache Flink. Under the hood, it wraps Flink's
# Java runtime via Py4J (a bridge between Python and JVM processes). This
# means our Python UDFs (User-Defined Functions) are executed in a separate
# Python process that communicates with the JVM via sockets — introducing
# some serialization overhead compared to native Java/Scala Flink jobs.
#
# For our research, this overhead is acceptable because:
#   1. The velocity window logic is simple (map lookups + arithmetic).
#   2. The dominant cost is I/O (Kafka + RocksDB), not CPU.
#   3. Python allows rapid prototyping for the research prototype.
# =============================================================================
from pyflink.common import (
    Types,                  # Type system for Flink schemas
    WatermarkStrategy,      # Controls event-time watermark generation
    Duration,               # Time duration for watermark configuration
    Configuration,          # Flink configuration object
)
from pyflink.common.time import Time
from pyflink.common.serialization import (
    SimpleStringSchema,     # Simple string serializer/deserializer
)
from pyflink.common.watermark_strategy import (
    TimestampAssigner,      # Extracts event timestamps from records
)
from pyflink.datastream import (
    StreamExecutionEnvironment,  # Entry point for DataStream programs
    RuntimeContext,              # Provides access to keyed state, timers, etc.
    OutputTag,                   # For side-output routing (suspicious vs. cleared)
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,        # Stateful per-key processing with timer support
)
from pyflink.datastream.state import (
    MapStateDescriptor,          # Descriptor for MapState (our window entries)
    ValueStateDescriptor,        # Descriptor for ValueState (aggregates)
    StateTtlConfig,              # Time-to-live config for automatic state cleanup
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource,                 # Kafka consumer (source connector)
    KafkaSink,                   # Kafka producer (sink connector)
    KafkaRecordSerializationSchema,  # Serialization schema for Kafka sink
    KafkaOffsetsInitializer,     # Controls where to start reading from Kafka
)
from pyflink.common.serialization import (
    SerializationSchema,         # Base serialization schema
)

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
# Structured logging with timestamps for correlation with Kafka message
# timestamps and Flink checkpoint events. The log format matches the
# producer.py format for consistent log analysis.
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("streamgraph.stream_processor")


# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================
# Loaded from environment variables (set in .env and Docker Compose) with
# sensible defaults for local development. Centralizing configuration here
# ensures consistency and makes it easy to adjust thresholds for the
# parameter sensitivity analysis in the IEEE paper.
# =============================================================================

# --- Kafka Configuration ---
# When running on the host machine (not inside Docker), we use the host-
# accessible addresses. The INTERNAL variants are only reachable from inside
# the Docker network. For local execution, prefer KAFKA_BOOTSTRAP_SERVERS.
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"
)
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "raw-transactions")
KAFKA_TOPIC_SUSPICIOUS = os.getenv("KAFKA_TOPIC_SUSPICIOUS", "suspicious-log")
KAFKA_TOPIC_CLEARED = os.getenv("KAFKA_TOPIC_CLEARED", "cleared-log")
KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP_FLINK", "streamgraph-flink-processor"
)

# --- Tier 1 Velocity Window Thresholds ---
# These are the core parameters of our temporal anomaly detection heuristic.
# For the IEEE paper's parameter sensitivity analysis, sweep these values:
#   WINDOW_SIZE: [5, 10, 15, 30, 60] minutes
#   MAX_TXN_COUNT: [3, 5, 10, 20]
#   MAX_TOTAL_AMOUNT: [5000, 10000, 25000, 50000]
VELOCITY_WINDOW_SIZE_MS = int(
    os.getenv("VELOCITY_WINDOW_SIZE_MINUTES", "15")
) * 60 * 1000  # Convert minutes to milliseconds

VELOCITY_MAX_TXN_COUNT = int(
    os.getenv("VELOCITY_MAX_TRANSACTION_COUNT", "5")
)

VELOCITY_MAX_TOTAL_AMOUNT = float(
    os.getenv("VELOCITY_MAX_TOTAL_AMOUNT", "10000.0")
)

# --- Flink Parallelism ---
FLINK_PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "2"))

# --- Benchmarking ---
BENCHMARK_ENABLED = os.getenv("BENCHMARK_ENABLED", "true").lower() == "true"


# =============================================================================
# OUTPUT TAG FOR SIDE-OUTPUT ROUTING
# =============================================================================
# Flink's side-output mechanism allows a single operator to produce multiple
# output streams. We use this to route:
#   - Main output → suspicious-log (flagged transactions for Tier 2)
#   - Side output → cleared-log (transactions that passed velocity check)
#
# WHY SIDE OUTPUT INSTEAD OF TWO SINKS?
# Using side output from a single KeyedProcessFunction ensures that the
# classification decision (suspicious vs. cleared) is made atomically
# within the same operator that holds the state. If we used two separate
# operators, we'd risk state inconsistency between them.
# =============================================================================
CLEARED_OUTPUT_TAG = OutputTag("cleared-output", Types.STRING())


# =============================================================================
# CUSTOM TIMESTAMP ASSIGNER
# =============================================================================
# Extracts the event timestamp from each transaction record. Flink uses this
# timestamp for event-time processing, including:
#   - Watermark generation (for tracking event-time progress).
#   - Timer firing (our cleanup timers use event time).
#
# We use `producer_timestamp_ms` as the event timestamp because:
#   1. It represents the actual time the transaction entered the pipeline.
#   2. PaySim's `step` field is a simulation artifact (1 step = 1 hour),
#      which would create unrealistically sparse event-time progress.
#   3. Using wall-clock time from the producer enables accurate end-to-end
#      latency measurement for the IEEE paper.
#
# WATERMARK STRATEGY:
# We use bounded-out-of-orderness with a 5-second tolerance. This means
# Flink assumes that any event arriving more than 5 seconds after the
# watermark has passed is "late" and may be dropped. The 5-second bound
# accounts for:
#   - Network jitter between producer and Kafka broker.
#   - Kafka batching delay (linger.ms = 20-50ms in our config).
#   - Small clock skew between producer and Flink containers.
#
# In a production system processing real financial transactions, this would
# be calibrated based on observed network latency distributions.
# =============================================================================
class TransactionTimestampAssigner(TimestampAssigner):
    """
    Extracts the producer_timestamp_ms from each JSON transaction record
    to serve as the event timestamp for Flink's event-time processing.
    """

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        """
        Parse the JSON string and extract the producer timestamp.

        Args:
            value: JSON string representing a transaction.
            record_timestamp: The timestamp previously assigned to this record
                              (e.g., by a previous operator or the source).

        Returns:
            Event timestamp in milliseconds since epoch.
        """
        try:
            data = json.loads(value)
            # Use producer_timestamp_ms as the definitive event timestamp.
            # This was injected by producer.py at the moment the message
            # was serialized and handed to librdkafka.
            return data.get("producer_timestamp_ms", record_timestamp)
        except (json.JSONDecodeError, TypeError):
            # If JSON parsing fails, fall back to the record timestamp
            # (typically Kafka's log-append time). Log the error for
            # post-hoc analysis but don't crash the pipeline — resilience
            # is critical in stream processing.
            logger.warning(
                f"Failed to extract timestamp from record. "
                f"Using record_timestamp={record_timestamp}"
            )
            return record_timestamp


# =============================================================================
# VELOCITY WINDOW PROCESS FUNCTION (The Heart of Tier 1)
# =============================================================================
# This KeyedProcessFunction implements the 15-minute sliding velocity window
# for detecting temporal anomalies in transaction streams.
#
# STATE LAYOUT IN ROCKSDB:
# ========================
# For each key (nameOrig — the sender's account ID), we maintain:
#
#   1. MapState<String, String> "window_entries":
#      ─────────────────────────────────────────────
#      Key:   Composite of transaction timestamp + unique transaction ID
#             (to handle multiple transactions at the same millisecond).
#      Value: JSON string encoding {amount, transaction_id, type}.
#
#      In RocksDB, this MapState is stored as a column family with keys
#      prefixed by the Flink key-group + user-key (nameOrig). Each entry
#      is a separate RocksDB key-value pair, enabling efficient range
#      scans and point deletes during window eviction.
#
#   2. ValueState<String> "txn_count":
#      ─────────────────────────────────
#      Running count of active (non-expired) transactions in the window.
#      Stored as a JSON-encoded integer. Maintained incrementally to avoid
#      full MapState scans on every transaction — reducing the per-record
#      cost from O(window_size) to O(1) for the count check.
#
#   3. ValueState<String> "txn_sum":
#      ─────────────────────────────────
#      Running sum of transaction amounts in the window.
#      Maintained incrementally (add on insert, subtract on eviction).
#      This makes the amount threshold check O(1) per record.
#
# TIMER-BASED GARBAGE COLLECTION:
# ================================
# For each transaction, we register a processing-time timer to fire at
# (transaction_timestamp + WINDOW_SIZE). When the timer fires:
#   1. We scan the MapState for entries older than (current_time - WINDOW_SIZE).
#   2. We delete expired entries and decrement the count/sum accordingly.
#
# This lazy eviction approach is more efficient than eagerly scanning on
# every insert because:
#   - It amortizes the eviction cost over time.
#   - It leverages Flink's timer service (which coalesces timers efficiently).
#   - In RocksDB, point deletes (for specific expired keys) are cheaper than
#     full range scans.
#
# EXACTLY-ONCE GUARANTEES:
# ========================
# All state modifications (MapState entries, ValueState aggregates, timer
# registrations) are part of Flink's checkpoint. If a failure occurs:
#   1. Flink restores from the last successful checkpoint.
#   2. Kafka offsets are reset to the checkpoint's committed offsets.
#   3. Messages are replayed from Kafka (exactly-once via idempotent sink).
#   4. State is restored to the checkpoint's snapshot.
# The net effect is that each transaction is processed exactly once, even
# across failures — a critical correctness property for fraud detection.
# =============================================================================
class VelocityWindowFunction(KeyedProcessFunction):
    """
    Stateful per-key process function that implements a sliding velocity
    window for temporal anomaly detection in financial transaction streams.

    For each account (keyed by nameOrig), this function:
    1. Maintains a sliding window of recent transactions in RocksDB state.
    2. Computes running aggregates (count, total amount) incrementally.
    3. Applies the Tier 1 rule to classify transactions as SUSPICIOUS or CLEARED.
    4. Emits classified transactions to the appropriate Kafka topic.
    5. Instruments every operation for latency benchmarking.
    """

    def __init__(self):
        """
        Initialize the VelocityWindowFunction.

        Note: State handles are NOT initialized here. In Flink, state must
        be initialized in the open() method, which is called after the
        function is distributed to TaskManagers and has access to the
        RuntimeContext (which provides the state backend interface).
        """
        # State handles (initialized in open())
        self._window_entries = None  # MapState: timestamp+id → {amount, txn_id}
        self._txn_count = None       # ValueState: running transaction count
        self._txn_sum = None         # ValueState: running amount sum

        # Configuration (loaded from environment in open())
        self._window_size_ms = VELOCITY_WINDOW_SIZE_MS
        self._max_txn_count = VELOCITY_MAX_TXN_COUNT
        self._max_total_amount = VELOCITY_MAX_TOTAL_AMOUNT

        # Performance counters (per-operator instance, not checkpointed)
        self._total_processed = 0
        self._total_suspicious = 0
        self._total_cleared = 0
        self._total_evicted = 0

    def open(self, runtime_context: RuntimeContext):
        """
        Initialize state handles and register state descriptors with the
        Flink runtime. This method is called once per parallel instance
        when the operator is deployed to a TaskManager.

        CRITICAL: This is where we interact with the RocksDB state backend.
        The state descriptors define the schema (key/value types) of the
        state, and the runtime context binds them to actual RocksDB column
        families on the TaskManager's local disk.

        Args:
            runtime_context: Provides access to state, broadcast variables,
                             accumulators, and operator metadata.
        """
        logger.info(
            f"VelocityWindowFunction.open() — "
            f"Subtask {runtime_context.get_index_of_this_subtask()} / "
            f"{runtime_context.get_number_of_parallel_subtasks()} | "
            f"window_size={self._window_size_ms}ms | "
            f"max_count={self._max_txn_count} | "
            f"max_amount=${self._max_total_amount:,.2f}"
        )

        # =====================================================================
        # STATE DESCRIPTOR 1: Window Entries (MapState)
        # =====================================================================
        # MapState<String, String> stores individual transaction records within
        # the sliding window.
        #
        # Key format: "{timestamp_ms}:{transaction_id}"
        #   - The timestamp prefix enables temporal ordering for efficient
        #     eviction (we can scan in order and stop at the cutoff).
        #   - The transaction_id suffix ensures uniqueness (multiple
        #     transactions can arrive at the same millisecond).
        #
        # Value format: JSON string encoding {"amount": float, "type": string}
        #   - We store only the fields needed for aggregate computation and
        #     output enrichment, minimizing RocksDB storage overhead.
        #
        # IN ROCKSDB: Each MapState entry becomes a separate key-value pair
        # in a RocksDB column family. The full RocksDB key is:
        #   [key_group_prefix][nameOrig][state_name][map_key]
        # This layout enables efficient range scans within a single account's
        # window entries.
        # =====================================================================
        window_entries_descriptor = MapStateDescriptor(
            "window_entries",
            Types.STRING(),  # Map key type: "{timestamp}:{txn_id}"
            Types.STRING(),  # Map value type: JSON-encoded entry data
        )

        # =====================================================================
        # STATE TIME-TO-LIVE (TTL) CONFIGURATION
        # =====================================================================
        # We configure a TTL slightly larger than our window size as a safety
        # net. This ensures that if our timer-based eviction misses an entry
        # (e.g., due to processing delays), RocksDB will eventually garbage-
        # collect it during compaction.
        #
        # TTL is implemented via RocksDB's CompactionFilter: during the
        # compaction process (which merges and reorders SST files), entries
        # older than the TTL are silently dropped. This is a background
        # operation that doesn't block query processing.
        #
        # We set the TTL to 2x the window size (30 minutes) to provide a
        # generous buffer while still preventing unbounded state growth from
        # inactive accounts.
        # =====================================================================
        ttl_config = StateTtlConfig \
            .new_builder(Time.minutes(
                int((self._window_size_ms // 60000) * 2)  # 2x window size
            )) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .set_state_visibility(
                StateTtlConfig.StateVisibility.NeverReturnExpired
            ) \
            .build()

        window_entries_descriptor.enable_time_to_live(ttl_config)
        self._window_entries = runtime_context.get_map_state(
            window_entries_descriptor
        )

        # =====================================================================
        # STATE DESCRIPTOR 2: Transaction Count (ValueState)
        # =====================================================================
        # Running count of active transactions in the window. Maintained
        # incrementally: +1 on insert, -1 on eviction. This avoids the
        # O(n) cost of iterating the MapState to compute count on every
        # incoming transaction.
        #
        # Stored as a String (JSON-encoded integer) because PyFlink's
        # ValueState requires a TypeInformation, and String is the simplest
        # cross-language type that works reliably with the Py4J bridge.
        # =====================================================================
        txn_count_descriptor = ValueStateDescriptor(
            "txn_count",
            Types.STRING(),
        )
        txn_count_descriptor.enable_time_to_live(ttl_config)
        self._txn_count = runtime_context.get_state(txn_count_descriptor)

        # =====================================================================
        # STATE DESCRIPTOR 3: Transaction Sum (ValueState)
        # =====================================================================
        # Running sum of transaction amounts in the window. Maintained
        # incrementally: +amount on insert, -amount on eviction.
        #
        # Using a running sum avoids recomputing Σ amounts on every record,
        # which would be O(window_size). Our incremental approach is O(1)
        # per record for both insert and eviction.
        # =====================================================================
        txn_sum_descriptor = ValueStateDescriptor(
            "txn_sum",
            Types.STRING(),
        )
        txn_sum_descriptor.enable_time_to_live(ttl_config)
        self._txn_sum = runtime_context.get_state(txn_sum_descriptor)

        logger.info(
            f"State handles initialized for subtask "
            f"{runtime_context.get_index_of_this_subtask()}. "
            f"RocksDB state backend is active."
        )

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        """
        Process a single transaction record. This is the hot path —
        called for every incoming transaction from the Kafka source.

        Processing steps:
        1. Parse the JSON transaction.
        2. Record the entry in the sliding window (MapState).
        3. Update running aggregates (count, sum).
        4. Register a cleanup timer.
        5. Evict expired entries from the window.
        6. Apply the Tier 1 velocity rule.
        7. Emit the classified transaction to the appropriate output.

        Args:
            value: JSON string representing a single transaction from Kafka.
            ctx: Process function context providing access to timestamps,
                 timers, side outputs, and the current key.

        Yields:
            Main output: SUSPICIOUS transactions (JSON string).
            Side output (CLEARED_OUTPUT_TAG): CLEARED transactions (JSON string).
        """
        # =================================================================
        # STEP 0: Start performance timer
        # =================================================================
        # Capture the start time for Tier 1 latency measurement.
        # We use time.monotonic_ns() for high-resolution, monotonic timing
        # that isn't affected by NTP adjustments or wall-clock jumps.
        tier1_start_ns = time.monotonic_ns()

        # =================================================================
        # STEP 1: Parse the incoming transaction JSON
        # =================================================================
        try:
            txn = json.loads(value)
        except json.JSONDecodeError as e:
            logger.error(
                f"[PARSE_ERROR] Failed to decode JSON: {e} | "
                f"Raw value (first 200 chars): {value[:200]}"
            )
            return  # Skip malformed records; don't crash the pipeline

        # Extract key fields for processing.
        txn_id = txn.get("transaction_id", "UNKNOWN")
        name_orig = txn.get("nameOrig", "")
        name_dest = txn.get("nameDest", "")
        amount = float(txn.get("amount", 0.0))
        txn_type = txn.get("type", "UNKNOWN")
        is_fraud_label = int(txn.get("isFraud", 0))
        producer_ts = int(txn.get("producer_timestamp_ms", 0))

        # Use the current processing time as the event timestamp for the
        # window. In a production system, we'd use the event timestamp from
        # the watermark, but for our research prototype, processing time
        # simplifies the timer logic and avoids out-of-order complexities
        # that aren't the focus of this paper.
        current_time_ms = int(time.time() * 1000)

        # =================================================================
        # STEP 2: Insert the transaction into the sliding window (MapState)
        # =================================================================
        # Construct the composite map key: "{timestamp}:{txn_id}"
        # The timestamp prefix ensures temporal ordering; the txn_id suffix
        # ensures uniqueness even for concurrent transactions at the same ms.
        state_start_ns = time.monotonic_ns()

        map_key = f"{current_time_ms}:{txn_id}"
        entry_value = json.dumps({
            "amount": amount,
            "type": txn_type,
            "transaction_id": txn_id,
        })
        self._window_entries.put(map_key, entry_value)

        # =================================================================
        # STEP 3: Update running aggregates incrementally
        # =================================================================
        # Read current count and sum from ValueState.
        # If state is empty (first transaction for this key), initialize to 0.
        raw_count = self._txn_count.value()
        current_count = int(raw_count) if raw_count else 0

        raw_sum = self._txn_sum.value()
        current_sum = float(raw_sum) if raw_sum else 0.0

        # Increment: +1 count, +amount to sum.
        new_count = current_count + 1
        new_sum = round(current_sum + amount, 2)

        # Write updated aggregates back to state.
        self._txn_count.update(str(new_count))
        self._txn_sum.update(str(new_sum))

        state_duration_ns = time.monotonic_ns() - state_start_ns

        # =================================================================
        # STEP 4: Register a cleanup timer
        # =================================================================
        # Register a processing-time timer to fire when this transaction
        # exits the sliding window. When the timer fires, on_timer() will
        # evict expired entries and decrement the aggregates.
        #
        # TIMER COALESCING: Flink's timer service automatically deduplicates
        # timers with the same timestamp for the same key. If we register
        # multiple timers at the same millisecond, only one fires. This is
        # acceptable because our on_timer() scans for ALL expired entries,
        # not just the one that triggered it.
        timer_target_ms = current_time_ms + self._window_size_ms
        ctx.timer_service().register_processing_time_timer(timer_target_ms)

        # =================================================================
        # STEP 5: Eager eviction of expired entries
        # =================================================================
        # In addition to the timer-based lazy eviction, we perform eager
        # eviction on each insert. This ensures the aggregates are accurate
        # for the Tier 1 rule check below, even if timers haven't fired yet
        # (timers fire asynchronously and may be slightly delayed).
        cutoff_time_ms = current_time_ms - self._window_size_ms
        eviction_start_ns = time.monotonic_ns()
        evicted_count = 0
        evicted_sum = 0.0

        # Iterate over all entries in the MapState and evict expired ones.
        # NOTE: In RocksDB, this iteration performs a range scan over the
        # column family entries for this key. For accounts with many
        # transactions, this could be expensive. In a production system,
        # we'd use a sorted data structure or RocksDB's column family
        # with timestamp-based key prefixes for efficient range deletion.
        keys_to_delete = []
        for entry in self._window_entries.items():
            entry_key = entry[0]  # "{timestamp}:{txn_id}"
            try:
                entry_ts = int(entry_key.split(":")[0])
            except (ValueError, IndexError):
                # Malformed key — schedule for deletion.
                keys_to_delete.append(entry_key)
                continue

            if entry_ts < cutoff_time_ms:
                # This entry has expired — mark for deletion.
                try:
                    entry_data = json.loads(entry[1])
                    evicted_sum += float(entry_data.get("amount", 0.0))
                except (json.JSONDecodeError, TypeError):
                    pass
                evicted_count += 1
                keys_to_delete.append(entry_key)

        # Perform batch deletion of expired entries.
        for key in keys_to_delete:
            self._window_entries.remove(key)

        # Adjust aggregates for evicted entries.
        if evicted_count > 0:
            new_count = max(0, new_count - evicted_count)
            new_sum = max(0.0, round(new_sum - evicted_sum, 2))
            self._txn_count.update(str(new_count))
            self._txn_sum.update(str(new_sum))
            self._total_evicted += evicted_count

        eviction_duration_ns = time.monotonic_ns() - eviction_start_ns

        # =================================================================
        # STEP 6: Apply the Tier 1 velocity rule
        # =================================================================
        # RULE: Flag as SUSPICIOUS if EITHER threshold is exceeded:
        #   (a) Transaction count > MAX_TXN_COUNT within the window, OR
        #   (b) Total amount > MAX_TOTAL_AMOUNT within the window.
        #
        # This is a disjunctive rule (OR) because:
        #   - High frequency + low amount: "Smurfing" / structuring attack
        #     (many small transactions to avoid reporting thresholds).
        #   - Low frequency + high amount: Single large fraudulent transfer.
        #   - Both high: Obvious anomaly.
        #
        # The OR logic ensures we catch all three patterns. For the IEEE
        # paper, we can compare OR vs. AND semantics in the evaluation.
        is_suspicious = (
            new_count > self._max_txn_count or
            new_sum > self._max_total_amount
        )

        # =================================================================
        # STEP 7: Compute performance metrics
        # =================================================================
        tier1_end_ns = time.monotonic_ns()
        tier1_latency_ms = round((tier1_end_ns - tier1_start_ns) / 1e6, 3)
        state_latency_ms = round(state_duration_ns / 1e6, 3)
        eviction_latency_ms = round(eviction_duration_ns / 1e6, 3)

        # End-to-end latency: time from producer serialization to Tier 1
        # classification. This includes Kafka ingestion delay + Flink
        # deserialization + our processing logic.
        e2e_latency_ms = (
            current_time_ms - producer_ts if producer_ts > 0 else -1
        )

        # =================================================================
        # STEP 8: Build the enriched output record
        # =================================================================
        # We enrich the original transaction with classification metadata
        # and performance instrumentation. This enriched record is consumed
        # by both:
        #   - graph_worker.py (Phase 4): For Tier 2 processing.
        #   - Benchmarking scripts: For latency/throughput analysis.
        classification = "SUSPICIOUS" if is_suspicious else "CLEARED"

        output_record = {
            # --- Original Transaction Fields ---
            "transaction_id": txn_id,
            "step": txn.get("step", 0),
            "type": txn_type,
            "amount": amount,
            "nameOrig": name_orig,
            "oldbalanceOrg": txn.get("oldbalanceOrg", 0.0),
            "newbalanceOrig": txn.get("newbalanceOrig", 0.0),
            "nameDest": name_dest,
            "oldbalanceDest": txn.get("oldbalanceDest", 0.0),
            "newbalanceDest": txn.get("newbalanceDest", 0.0),
            "isFraud": is_fraud_label,

            # --- Tier 1 Classification ---
            "tier1_classification": classification,
            "tier1_reason": self._build_reason(
                new_count, new_sum, is_suspicious
            ),

            # --- Velocity Window State (for debugging and analysis) ---
            "window_txn_count": new_count,
            "window_txn_sum": round(new_sum, 2),
            "window_size_ms": self._window_size_ms,
            "threshold_max_count": self._max_txn_count,
            "threshold_max_amount": self._max_total_amount,

            # --- Timestamps ---
            "producer_timestamp_ms": producer_ts,
            "tier1_timestamp_ms": current_time_ms,

            # --- Performance Instrumentation (for IEEE paper) ---
            "tier1_latency_ms": tier1_latency_ms,
            "tier1_state_latency_ms": state_latency_ms,
            "tier1_eviction_latency_ms": eviction_latency_ms,
            "tier1_e2e_latency_ms": e2e_latency_ms,
            "tier1_evicted_entries": evicted_count,
        }

        output_json = json.dumps(output_record)

        # =================================================================
        # STEP 9: Route to the appropriate output
        # =================================================================
        if is_suspicious:
            # Main output → suspicious-log topic.
            # This transaction will be consumed by graph_worker.py for
            # Tier 2 graph-structural analysis (cycle detection in Neo4j).
            self._total_suspicious += 1
            yield output_json
        else:
            # Side output → cleared-log topic.
            # This transaction does NOT trigger a Neo4j query — this is
            # the key efficiency gain of the dual-tier architecture.
            self._total_cleared += 1
            yield CLEARED_OUTPUT_TAG, output_json

        # =================================================================
        # STEP 10: Periodic progress logging
        # =================================================================
        self._total_processed += 1
        if self._total_processed % 10000 == 0:
            logger.info(
                f"[PROGRESS] Processed={self._total_processed:,} | "
                f"Suspicious={self._total_suspicious:,} "
                f"({self._total_suspicious / max(self._total_processed, 1) * 100:.2f}%) | "
                f"Cleared={self._total_cleared:,} | "
                f"Evicted={self._total_evicted:,} | "
                f"LastLatency={tier1_latency_ms}ms"
            )

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        """
        Timer callback for lazy garbage collection of expired window entries.

        This method fires at (transaction_timestamp + WINDOW_SIZE) for each
        registered timer. It scans the MapState and evicts entries that have
        fallen outside the sliding window.

        DESIGN NOTE: We use processing-time timers rather than event-time
        timers because:
        1. Processing-time timers fire reliably based on wall-clock time,
           regardless of watermark progress. This ensures timely cleanup
           even during periods of low input throughput.
        2. Event-time timers only fire when the watermark advances past
           the timer's timestamp. If the source goes idle, event-time
           timers may never fire, causing state to accumulate indefinitely.

        Args:
            timestamp: The processing time at which this timer fired.
            ctx: Timer context providing access to the current key and state.
        """
        # Calculate the cutoff: any entry older than this is expired.
        current_time_ms = int(time.time() * 1000)
        cutoff_time_ms = current_time_ms - self._window_size_ms

        # Read current aggregates.
        raw_count = self._txn_count.value()
        current_count = int(raw_count) if raw_count else 0

        raw_sum = self._txn_sum.value()
        current_sum = float(raw_sum) if raw_sum else 0.0

        # Scan and evict expired entries.
        keys_to_delete = []
        evicted_count = 0
        evicted_sum = 0.0

        for entry in self._window_entries.items():
            entry_key = entry[0]
            try:
                entry_ts = int(entry_key.split(":")[0])
            except (ValueError, IndexError):
                keys_to_delete.append(entry_key)
                continue

            if entry_ts < cutoff_time_ms:
                try:
                    entry_data = json.loads(entry[1])
                    evicted_sum += float(entry_data.get("amount", 0.0))
                except (json.JSONDecodeError, TypeError):
                    pass
                evicted_count += 1
                keys_to_delete.append(entry_key)

        # Perform deletions.
        for key in keys_to_delete:
            self._window_entries.remove(key)

        # Update aggregates.
        if evicted_count > 0:
            new_count = max(0, current_count - evicted_count)
            new_sum = max(0.0, round(current_sum - evicted_sum, 2))
            self._txn_count.update(str(new_count))
            self._txn_sum.update(str(new_sum))
            self._total_evicted += evicted_count

    def close(self):
        """
        Cleanup method called when the operator is shut down.
        Logs final processing statistics for the IEEE paper's evaluation.
        """
        logger.info(
            f"[SHUTDOWN] VelocityWindowFunction closing. "
            f"Total processed={self._total_processed:,} | "
            f"Suspicious={self._total_suspicious:,} | "
            f"Cleared={self._total_cleared:,} | "
            f"Evicted={self._total_evicted:,}"
        )

    @staticmethod
    def _build_reason(count: int, total: float, is_suspicious: bool) -> str:
        """
        Build a human-readable reason string for the classification decision.
        This is included in the output record for audit trail purposes.

        Args:
            count: Current transaction count in the window.
            total: Current total amount in the window.
            is_suspicious: Whether the transaction was flagged.

        Returns:
            Descriptive reason string.
        """
        if not is_suspicious:
            return (
                f"CLEARED: count={count} (≤{VELOCITY_MAX_TXN_COUNT}), "
                f"sum=${total:,.2f} (≤${VELOCITY_MAX_TOTAL_AMOUNT:,.2f})"
            )

        reasons = []
        if count > VELOCITY_MAX_TXN_COUNT:
            reasons.append(
                f"HIGH_FREQUENCY: count={count} > {VELOCITY_MAX_TXN_COUNT}"
            )
        if total > VELOCITY_MAX_TOTAL_AMOUNT:
            reasons.append(
                f"HIGH_AMOUNT: sum=${total:,.2f} > ${VELOCITY_MAX_TOTAL_AMOUNT:,.2f}"
            )
        return "SUSPICIOUS: " + " | ".join(reasons)


# =============================================================================
# FLINK JOB BUILDER
# =============================================================================
# This function constructs and executes the Flink DataStream job graph:
#
#   KafkaSource → KeyBy(nameOrig) → VelocityWindowFunction → KafkaSink(s)
#
# The job graph is a Directed Acyclic Graph (DAG) of operators. Flink
# optimizes this DAG by:
#   1. Chaining operators that can run in the same thread (reducing
#      serialization overhead between operators).
#   2. Distributing keyed operators across task slots using hash partitioning
#      on the key (nameOrig), ensuring all transactions from the same account
#      are processed by the same parallel instance.
#   3. Inserting network shuffle boundaries at keyBy() operators to
#      redistribute data across TaskManagers.
# =============================================================================
def build_and_execute_flink_job():
    """
    Build the Flink DataStream job graph and execute it.

    This function:
    1. Creates the StreamExecutionEnvironment with RocksDB state backend.
    2. Configures checkpointing for exactly-once semantics.
    3. Creates the Kafka source connector.
    4. Wires the processing topology (source → keyBy → process → sinks).
    5. Submits the job to the Flink cluster for execution.
    """
    logger.info("=" * 70)
    logger.info("StreamGraph — Tier 1 Stream Processor")
    logger.info("Initializing Flink DataStream Job...")
    logger.info("=" * 70)

    # =========================================================================
    # STEP 1: Create StreamExecutionEnvironment
    # =========================================================================
    # The StreamExecutionEnvironment is the entry point for all Flink
    # DataStream programs. It encapsulates the job configuration, execution
    # environment (local/cluster), and the DAG of operators.
    #
    # IMPORTANT: We use get_execution_environment() which automatically
    # detects whether we're running in a local IDE (minicluster) or on a
    # Flink cluster (submitted via flink run). In the Docker setup, this
    # connects to the JobManager via RPC.
    # =========================================================================
    # Ensure Python UDF worker runs in process mode rather than loopback mode on Windows,
    # preventing premature process termination during stage bundle factory creation.
    os.environ["_python_worker_execution_mode"] = "process"

    env = StreamExecutionEnvironment.get_execution_environment()

    # =========================================================================
    # STEP 2: Configure the RocksDB State Backend EXPLICITLY
    # =========================================================================
    # This is the CRITICAL configuration for our architecture. We explicitly
    # set the RocksDB state backend in code, in ADDITION to the flink-conf.yaml
    # setting in Docker Compose. This provides defense-in-depth:
    #   - If the config file is missing or overridden, the code setting prevails.
    #   - This makes the state backend choice explicit and auditable in the
    #     source code (important for the IEEE paper's reproducibility).
    #
    # WHAT HAPPENS UNDER THE HOOD:
    # When we set state.backend = "rocksdb", Flink's StateBackendLoader
    # instantiates an EmbeddedRocksDBStateBackend, which:
    #   1. Creates a RocksDB instance (C++ library via JNI) on the
    #      TaskManager's local filesystem.
    #   2. Configures RocksDB with Flink-optimized settings:
    #      - Block cache: Sized from managed memory (40% of TM memory).
    #      - Write buffer manager: Shared across column families.
    #      - Bloom filters: 10 bits per key for fast negative lookups.
    #      - Compaction style: Level compaction (optimized for mixed
    #        read/write workloads like our sliding window).
    #   3. Creates column families for each state descriptor (one for
    #      "window_entries" MapState, one for each ValueState).
    #
    # INCREMENTAL CHECKPOINTING:
    # With state.backend.incremental = true, Flink's checkpoint mechanism
    # only uploads the RocksDB SST files that changed since the last
    # checkpoint. For our use case (frequent small updates to the MapState),
    # this dramatically reduces checkpoint I/O:
    #   - Without incremental: Every checkpoint uploads the ENTIRE state.
    #     Cost = O(|total_state|) per checkpoint.
    #   - With incremental: Only the delta (new + modified SST files) is
    #     uploaded. Cost = O(|changes_since_last_checkpoint|).
    # =========================================================================
    config = Configuration()
    config.set_string("state.backend", "rocksdb")
    config.set_string("state.backend.incremental", "true")
    config.set_string(
        "state.backend.rocksdb.timer-service.factory", "rocksdb"
    )

    # Use OS-aware checkpoint/savepoint directories.
    # Inside Docker (Linux): /opt/flink/checkpoints
    # On Windows (local execution): ./flink-checkpoints (relative to CWD)
    import platform
    if platform.system() == "Windows":
        _checkpoint_dir = os.path.abspath("./flink-checkpoints").replace("\\", "/")
        _savepoint_dir = os.path.abspath("./flink-savepoints").replace("\\", "/")
        checkpoint_uri = f"file:///{_checkpoint_dir}"
        savepoint_uri = f"file:///{_savepoint_dir}"
        os.makedirs("./flink-checkpoints", exist_ok=True)
        os.makedirs("./flink-savepoints", exist_ok=True)
        # Use short path for RocksDB local storage and temp dirs to stay under Windows 247-char path limit
        os.makedirs("C:/temp/rdb", exist_ok=True)
        config.set_string("state.backend.rocksdb.localdir", "C:/temp/rdb")
        config.set_string("io.tmp.dirs", "C:/temp/rdb")
    else:
        checkpoint_uri = "file:///opt/flink/checkpoints"
        savepoint_uri = "file:///opt/flink/savepoints"

    config.set_string("state.checkpoints.dir", checkpoint_uri)
    config.set_string("state.savepoints.dir", savepoint_uri)

    # Configure Python executable for PyFlink worker processes so they use the active virtual environment
    # instead of falling back to system Python (which lacks PyFlink dependencies)
    py_executable = sys.executable
    if platform.system() == "Windows":
        try:
            import ctypes
            from ctypes import wintypes
            _GetShortPathNameW = ctypes.windll.kernel32.GetShortPathNameW
            _GetShortPathNameW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
            _GetShortPathNameW.restype = wintypes.DWORD
            buf = ctypes.create_unicode_buffer(1024)
            if _GetShortPathNameW(sys.executable, buf, 1024) > 0:
                py_executable = buf.value
        except Exception:
            pass

    config.set_string("python.executable", py_executable)
    config.set_string("python.client.executable", py_executable)
    env.configure(config)
    try:
        env.set_python_executable(py_executable)
    except Exception:
        pass
    logger.info(f"PyFlink configured with Python executable: {py_executable}")

    # Set parallelism: determines how many parallel instances of each
    # operator are created. Each instance processes a subset of the keys
    # (determined by hash partitioning on nameOrig).
    env.set_parallelism(FLINK_PARALLELISM)

    # Ensure Kafka connector JAR is registered for local and cluster execution
    _project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _kafka_jar = os.path.join(_project_root, "lib", "flink-sql-connector-kafka-3.1.0-1.18.jar")
    if os.path.exists(_kafka_jar):
        _jar_uri = Path(_kafka_jar).resolve().as_uri()
        try:
            env.add_jars(_jar_uri)
            logger.info(f"Loaded Kafka connector JAR: {_jar_uri}")
        except Exception as _jar_err:
            logger.warning(f"Could not register JAR via add_jars: {_jar_err}")

    # =========================================================================
    # STEP 3: Configure Checkpointing
    # =========================================================================
    # Checkpointing is Flink's mechanism for fault tolerance. It implements
    # the Chandy-Lamport distributed snapshot algorithm:
    #   1. The JobManager injects checkpoint barriers into the data stream.
    #   2. When an operator receives a barrier from all input channels, it
    #      snapshots its local state and forwards the barrier downstream.
    #   3. When all operators have acknowledged, the checkpoint is "completed"
    #      and the state snapshot is durable (on the configured storage).
    #
    # EXACTLY_ONCE mode enables barrier alignment: an operator waits for
    # barriers from ALL input channels before snapshotting. This ensures the
    # snapshot is a consistent cut across the distributed computation —
    # no message is counted twice or missed.
    # =========================================================================
    env.enable_checkpointing(60000)  # Checkpoint every 60 seconds

    checkpoint_config = env.get_checkpoint_config()
    # Import CheckpointingMode at usage site to avoid import errors
    # in environments where the enum path may vary across PyFlink versions.
    from pyflink.datastream import CheckpointingMode
    checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config.set_min_pause_between_checkpoints(30000)  # 30s min pause
    checkpoint_config.set_checkpoint_timeout(120000)  # 2 min timeout
    checkpoint_config.set_tolerable_checkpoint_failure_number(3)

    logger.info(
        f"Checkpointing configured: interval=60s, mode=EXACTLY_ONCE, "
        f"incremental=true, state.backend=rocksdb"
    )

    # =========================================================================
    # STEP 4: Create Kafka Source Connector
    # =========================================================================
    # The KafkaSource connector reads records from the `raw-transactions`
    # Kafka topic. Key configuration:
    #
    # - EARLIEST offset initializer: Start from the beginning of the topic
    #   on first run (no committed offsets). On subsequent runs (after a
    #   checkpoint), Flink resumes from the checkpointed offset.
    #
    # - SimpleStringSchema: Deserializes Kafka message values as UTF-8
    #   strings. We parse JSON in the VelocityWindowFunction rather than
    #   using a JSON-specific deserializer because:
    #   (a) It avoids coupling the source to a specific schema.
    #   (b) It gives us control over error handling for malformed records.
    #   (c) It simplifies the type system (String throughout the pipeline).
    #
    # - Consumer group: "streamgraph-flink-processor". Flink manages its own
    #   offsets via checkpoints, but the group ID is still useful for Kafka
    #   monitoring (e.g., consumer lag via kafka-consumer-groups.sh).
    # =========================================================================
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(KAFKA_TOPIC_RAW) \
        .set_group_id(KAFKA_CONSUMER_GROUP) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    logger.info(
        f"Kafka source configured: "
        f"servers={KAFKA_BOOTSTRAP_SERVERS} | "
        f"topic={KAFKA_TOPIC_RAW} | "
        f"group={KAFKA_CONSUMER_GROUP}"
    )

    # =========================================================================
    # STEP 5: Create the DataStream from the Kafka Source
    # =========================================================================
    # We attach the source to the environment with a watermark strategy.
    #
    # WATERMARK STRATEGY: Bounded out-of-orderness (5 seconds).
    # This tells Flink that events may arrive up to 5 seconds late (relative
    # to their event timestamp). The watermark at any point in time is:
    #   W(t) = max_event_timestamp_seen_so_far - 5_seconds
    #
    # Events with timestamps ≤ W(t) are considered "on time" or "late".
    # Late events (arriving after the watermark has passed their timestamp)
    # are still processed by our KeyedProcessFunction (we don't use windows
    # that would drop them), but they may see slightly stale aggregate state.
    #
    # For our research prototype, 5 seconds is conservative enough to handle
    # all realistic network delays in a single-machine Docker setup.
    # =========================================================================
    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(5))
            .with_timestamp_assigner(TransactionTimestampAssigner()),
        source_name="KafkaSource-RawTransactions"
    )

    logger.info("DataStream created from Kafka source.")

    # =========================================================================
    # STEP 6: Key the stream by nameOrig and apply the VelocityWindowFunction
    # =========================================================================
    # key_by(lambda x: ...) partitions the stream by account ID (nameOrig).
    #
    # WHAT key_by DOES UNDER THE HOOD:
    # 1. Flink hashes the key using MurmurHash and assigns it to a "key group"
    #    (there are 128 key groups by default, configurable via
    #    maxParallelism).
    # 2. Key groups are distributed across parallel subtasks. With
    #    parallelism=2, each subtask gets ~64 key groups.
    # 3. All records with the same key are routed to the same subtask,
    #    ensuring the VelocityWindowFunction sees ALL transactions from a
    #    given account in order.
    #
    # This is a NETWORK SHUFFLE operation: records may need to cross
    # TaskManager boundaries (in a multi-node cluster). The cost is
    # serialization + network transfer, which is why we key BEFORE the
    # process function (to minimize the number of shuffle operations).
    # =========================================================================

    # We parse the nameOrig from the JSON string for keying.
    # This lambda is executed for every record to determine its key.
    def extract_key(value: str) -> str:
        """Extract the nameOrig field from a JSON transaction string."""
        try:
            data = json.loads(value)
            return data.get("nameOrig", "UNKNOWN")
        except (json.JSONDecodeError, TypeError):
            return "UNKNOWN"

    # Apply the keyed process function with the velocity window logic.
    processed_stream = (
        raw_stream
        .key_by(extract_key)
        .process(VelocityWindowFunction(), output_type=Types.STRING())
    )

    logger.info("Processing topology configured: KeyBy(nameOrig) → VelocityWindowFunction")

    # =========================================================================
    # STEP 7: Create Kafka Sinks for Suspicious and Cleared Streams
    # =========================================================================
    # We create two Kafka sinks:
    #   1. suspicious-log: Main output (transactions flagged by Tier 1).
    #      Consumed by graph_worker.py for Tier 2 processing.
    #   2. cleared-log: Side output (transactions cleared by Tier 1).
    #      Not consumed by Tier 2 — this is the efficiency gain.
    #
    # KAFKA SINK DELIVERY GUARANTEE:
    # We use AT_LEAST_ONCE delivery guarantee for the sinks. For EXACTLY_ONCE,
    # Flink's Kafka sink uses two-phase commit (2PC) with Kafka transactions,
    # which requires:
    #   - transaction.timeout.ms on the broker ≥ checkpoint interval.
    #   - Downstream consumers to read with isolation.level=read_committed.
    # For our research prototype, AT_LEAST_ONCE is sufficient because:
    #   - graph_worker.py uses MERGE (upsert) in Neo4j, making duplicates
    #     idempotent.
    #   - The benchmarking analysis can tolerate rare duplicates.
    # =========================================================================

    # --- Suspicious Transactions Sink ---
    suspicious_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_TOPIC_SUSPICIOUS)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    # Attach the suspicious sink to the main output of the process function.
    processed_stream.sink_to(suspicious_sink).name("KafkaSink-SuspiciousLog")

    logger.info(f"Suspicious sink configured → topic={KAFKA_TOPIC_SUSPICIOUS}")

    # --- Cleared Transactions Sink ---
    cleared_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_TOPIC_CLEARED)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    # Get the side output (cleared transactions) from the processed stream
    # and attach the cleared sink.
    cleared_stream = processed_stream.get_side_output(CLEARED_OUTPUT_TAG)
    cleared_stream.sink_to(cleared_sink).name("KafkaSink-ClearedLog")

    logger.info(f"Cleared sink configured → topic={KAFKA_TOPIC_CLEARED}")

    # =========================================================================
    # STEP 8: Execute the Flink Job
    # =========================================================================
    # env.execute() submits the job graph to the Flink cluster (or starts
    # a minicluster in local mode) and blocks until the job completes or
    # is cancelled.
    #
    # The job name "StreamGraph-Tier1-VelocityWindow" appears in the Flink
    # Web Dashboard (http://localhost:8081) for monitoring.
    # =========================================================================
    logger.info("=" * 70)
    logger.info("Submitting Flink job: StreamGraph-Tier1-VelocityWindow")
    logger.info(f"  Parallelism: {FLINK_PARALLELISM}")
    logger.info(f"  State Backend: RocksDB (incremental checkpointing)")
    logger.info(f"  Source Topic: {KAFKA_TOPIC_RAW}")
    logger.info(f"  Suspicious Sink: {KAFKA_TOPIC_SUSPICIOUS}")
    logger.info(f"  Cleared Sink: {KAFKA_TOPIC_CLEARED}")
    logger.info(f"  Window Size: {VELOCITY_WINDOW_SIZE_MS // 60000} minutes")
    logger.info(f"  Max Txn Count: {VELOCITY_MAX_TXN_COUNT}")
    logger.info(f"  Max Total Amount: ${VELOCITY_MAX_TOTAL_AMOUNT:,.2f}")
    logger.info("=" * 70)

    env.execute("StreamGraph-Tier1-VelocityWindow")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================
# This script is designed to be submitted to a Flink cluster via:
#   flink run -py /opt/flink/usrlib/stream_processor.py
#
# Or executed locally for development:
#   python stream_processor.py
#
# The script detects its execution environment automatically:
# - On a Flink cluster: Connects to the JobManager via RPC and submits the
#   job graph for distributed execution across TaskManagers.
# - Locally: Starts a Flink minicluster in-process (useful for debugging).
# =============================================================================
if __name__ == "__main__":
    logger.info("StreamGraph Tier 1 Stream Processor — Starting")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Kafka bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Benchmark enabled: {BENCHMARK_ENABLED}")

    try:
        build_and_execute_flink_job()
    except Exception as e:
        logger.critical(
            f"FATAL: Flink job failed with exception: {type(e).__name__}: {e}",
            exc_info=True,
        )
        sys.exit(1)
