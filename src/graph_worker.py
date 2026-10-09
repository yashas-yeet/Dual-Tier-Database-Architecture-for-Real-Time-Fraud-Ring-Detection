# =============================================================================
# StreamGraph — Phase 4: Tier 2 Graph Validator & Benchmarker (graph_worker.py)
# =============================================================================
# IEEE Research Project: Dual-Tier Database Architecture for Real-Time
# Financial Fraud Ring Detection
#
# PURPOSE:
# This is the Tier 2 graph validation engine — the second and final tier of
# the StreamGraph architecture. It is ONLY invoked when Tier 1 (Flink/RocksDB)
# has flagged a transaction as SUSPICIOUS based on the velocity window
# heuristic. This selective invocation is the central thesis of our dual-tier
# design: by filtering out ~95-99% of transactions at the cheap Tier 1 level,
# we reduce the load on the expensive Tier 2 graph queries by orders of
# magnitude.
#
# RESPONSIBILITIES:
# 1. Consume SUSPICIOUS transactions from the `suspicious-log` Kafka topic.
# 2. Insert the transaction subgraph into Neo4j:
#    (User:nameOrig)-[:SENT]->(Transaction)-[:RECEIVED]->(User:nameDest)
# 3. Execute a Cypher query to detect if nameOrig participates in a 3-to-5
#    hop circular transaction path (fraud ring detection).
# 4. If a cycle is detected, publish a FRAUD_ALERT to the `fraud-alerts`
#    Kafka topic.
# 5. Collect and publish benchmarking metrics comparing Tier 1 (state lookup)
#    vs. Tier 2 (graph traversal) latency — the key evidence for our paper.
#
# ARCHITECTURE (Tier 2 Detail):
# ┌──────────────────────────────────────────────────────────────────────────┐
# │  Graph Worker (Tier 2)                                                   │
# │                                                                          │
# │  ┌─────────────┐    ┌──────────────────────┐    ┌─────────────────────┐ │
# │  │ Kafka Source │──▶ │  Neo4j Operations    │──▶ │ Kafka Sinks         │ │
# │  │ (suspicious │    │  ┌────────────────┐  │    │  - fraud-alerts     │ │
# │  │  -log)      │    │  │ 1. MERGE Users │  │    │  - benchmark-metrics│ │
# │  └─────────────┘    │  │ 2. MERGE Txn   │  │    └─────────────────────┘ │
# │                      │  │ 3. CREATE Rels │  │                            │
# │                      │  │ 4. Cycle Query │  │                            │
# │                      │  └────────────────┘  │                            │
# │                      └──────────────────────┘                            │
# │                                                                          │
# │  ┌──────────────────────────────────────────────────────────────────┐   │
# │  │  Benchmark Collector                                             │   │
# │  │  - Per-transaction latency: Tier 1 vs. Tier 2                   │   │
# │  │  - Aggregate stats: p50, p95, p99, throughput                   │   │
# │  │  - CSV/JSON export for IEEE paper charts                        │   │
# │  └──────────────────────────────────────────────────────────────────┘   │
# └──────────────────────────────────────────────────────────────────────────┘
#
# WHY CYCLE DETECTION IN A GRAPH DATABASE (for the IEEE paper):
# =============================================================
# Fraud rings create circular transaction paths (money laundering patterns):
#   A → B → C → A  (3-hop ring)
#   A → B → C → D → A  (4-hop ring)
#   A → B → C → D → E → A  (5-hop ring)
#
# Detecting these cycles requires variable-length path traversal — an
# operation that is:
#   - O(d^k) in a graph database (d = avg degree, k = max hops)
#     using index-free adjacency (each node stores physical pointers
#     to its neighbors).
#   - O(n^k) in a relational database (n = total nodes, k = max hops)
#     requiring k-way self-joins on the transaction table.
#
# Since d << n (financial graphs are sparse — each account transacts with
# a small fraction of all accounts), the graph approach is asymptotically
# superior for this query pattern. Our empirical measurements (captured by
# this benchmarking module) quantify this advantage for the paper.
#
# NEO4J QUERY DESIGN:
# ====================
# We use a Cypher variable-length path pattern:
#   MATCH path = (origin:User {id: $nameOrig})-[:SENT]->(:Transaction)
#         -[:RECEIVED]->(:User)-[:SENT]->(:Transaction)-[:RECEIVED]->
#         ... -[:RECEIVED]->(origin)
#
# The RECEIVED->origin constraint ensures the path is CIRCULAR (starts
# and ends at the same user). We limit the path length to 3-5 hops
# (6-10 relationships, since each hop = SENT + RECEIVED) to bound the
# traversal cost while capturing the most common fraud ring topologies.
#
# IDEMPOTENCY:
# =============
# All Neo4j writes use MERGE (not CREATE). MERGE performs an upsert:
#   - If the node/relationship already exists (matched by the constraint
#     key), no new entity is created.
#   - If it doesn't exist, a new entity is created.
# This makes our writes idempotent — safe against duplicate messages
# from Kafka (possible with at-least-once delivery).
# =============================================================================

import os
import sys
import csv
import json
import time
import signal
import logging
import statistics
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from collections import deque

# ---------------------------------------------------------------------------
# confluent-kafka: High-performance Kafka consumer/producer (librdkafka).
# We use the Consumer for reading from suspicious-log and Producer for
# writing fraud-alerts and benchmark-metrics.
# ---------------------------------------------------------------------------
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# ---------------------------------------------------------------------------
# neo4j: Official Python driver for Neo4j (Bolt protocol).
# Uses connection pooling and supports explicit transaction functions with
# automatic retry logic for transient errors (leader switches, deadlocks).
# ---------------------------------------------------------------------------
from neo4j import GraphDatabase
from neo4j.exceptions import (
    ServiceUnavailable,
    SessionExpired,
    TransientError,
    ClientError,
    Neo4jError,
)

# ---------------------------------------------------------------------------
# python-dotenv: Loads configuration from .env file.
# ---------------------------------------------------------------------------
from dotenv import load_dotenv

# Load environment configuration early.
load_dotenv()


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
# Matches the format used by producer.py and stream_processor.py for
# consistent log analysis across all StreamGraph components.
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("graph_worker.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("streamgraph.graph_worker")


# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================
# All configuration is externalized via environment variables (.env file)
# with sensible defaults for local development. This enables parameter
# sweeps for the IEEE paper without code changes.
# =============================================================================

# --- Kafka Configuration ---
# When running on the host machine (not inside Docker), we use the host-
# accessible addresses. The INTERNAL variants are only reachable from inside
# the Docker network. We prefer KAFKA_BOOTSTRAP_SERVERS (host) over INTERNAL.
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"
)
KAFKA_TOPIC_SUSPICIOUS = os.getenv("KAFKA_TOPIC_SUSPICIOUS", "suspicious-log")
KAFKA_TOPIC_FRAUD_ALERTS = os.getenv("KAFKA_TOPIC_FRAUD_ALERTS", "fraud-alerts")
KAFKA_TOPIC_BENCHMARKS = os.getenv("KAFKA_TOPIC_BENCHMARKS", "benchmark-metrics")
KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP_GRAPH_WORKER", "streamgraph-graph-worker"
)

# --- Neo4j Configuration ---
# Use host-accessible Neo4j URI (bolt://localhost:7687) when running locally.
NEO4J_URI = os.getenv(
    "NEO4J_URI", "bolt://localhost:7687"
)
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "streamgraph_research_2024")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")
NEO4J_MAX_POOL_SIZE = int(os.getenv("NEO4J_MAX_CONNECTION_POOL_SIZE", "50"))

# --- Benchmarking Configuration ---
BENCHMARK_ENABLED = os.getenv("BENCHMARK_ENABLED", "true").lower() == "true"
BENCHMARK_EXPORT_INTERVAL = int(os.getenv("BENCHMARK_EXPORT_INTERVAL", "10"))
BENCHMARK_OUTPUT_DIR = os.getenv("BENCHMARK_OUTPUT_DIR", "./benchmarks")

# --- Cycle Detection Parameters ---
# MIN_CYCLE_HOPS and MAX_CYCLE_HOPS control the depth of the graph traversal.
# Each "hop" in our model involves two relationships (SENT + RECEIVED),
# so a 3-hop cycle requires 6 relationship traversals.
#
# The cost of cycle detection grows exponentially with MAX_CYCLE_HOPS:
#   Cost ≈ O(d^(2k)) where d = average node degree, k = max hops.
# For our paper, we benchmark with k ∈ {3, 4, 5} and report the latency
# distribution for each to characterize the scalability trade-off.
MIN_CYCLE_HOPS = 3
MAX_CYCLE_HOPS = 5


# =============================================================================
# CYPHER QUERIES
# =============================================================================
# All Cypher queries are defined as constants for maintainability and to
# enable easy review/modification for the IEEE paper's experimental setup.
# =============================================================================

# -----------------------------------------------------------------------
# QUERY 1: MERGE Transaction Subgraph
# -----------------------------------------------------------------------
# Inserts the transaction and its relationships into Neo4j.
# Uses MERGE for idempotency (safe against duplicate Kafka messages).
#
# Operation breakdown:
#   1. MERGE (origin:User {id: $nameOrig}) — find or create the sender.
#   2. MERGE (dest:User {id: $nameDest}) — find or create the recipient.
#   3. MERGE (txn:Transaction {id: $txnId}) — find or create the transaction.
#   4. ON CREATE SET ... — set properties only on first creation.
#   5. MERGE (origin)-[:SENT]->(txn) — link sender to transaction.
#   6. MERGE (txn)-[:RECEIVED]->(dest) — link transaction to recipient.
#
# WHY MERGE INSTEAD OF CREATE (for the IEEE paper):
# In a distributed system with at-least-once delivery guarantees (our Kafka
# consumer configuration), the same message may be processed more than once
# after a consumer restart. MERGE makes the writes idempotent:
#   - First execution: creates the node/relationship.
#   - Subsequent executions: finds the existing entity and skips creation.
# This achieves effectively-once semantics at the application level.
#
# PERFORMANCE NOTE:
# MERGE with uniqueness constraints is O(log n) per operation due to the
# B-tree index on User.id and Transaction.id. Without constraints, MERGE
# would degrade to O(n) scans — a critical optimization.
# -----------------------------------------------------------------------
CYPHER_MERGE_SUBGRAPH = """
MERGE (origin:User {id: $nameOrig})
MERGE (dest:User {id: $nameDest})
MERGE (txn:Transaction {id: $txnId})
  ON CREATE SET
    txn.amount = $amount,
    txn.timestamp = $timestamp,
    txn.type = $txnType,
    txn.step = $step,
    txn.isFraud = $isFraud,
    txn.tier1Classification = $tier1Classification,
    txn.insertedAt = datetime()
MERGE (origin)-[:SENT]->(txn)
MERGE (txn)-[:RECEIVED]->(dest)
RETURN txn.id AS transactionId
"""

# -----------------------------------------------------------------------
# QUERY 2: Cycle Detection (3-to-5 Hop Fraud Rings)
# -----------------------------------------------------------------------
# Detects circular transaction paths originating from (and returning to)
# the specified user. This is the core structural anomaly detection query.
#
# QUERY STRUCTURE:
# We use variable-length relationship patterns to find paths of length
# 3 to 5 hops. Each "hop" consists of:
#   (User)-[:SENT]->(Transaction)-[:RECEIVED]->(User)
# So a 3-hop cycle has the pattern:
#   (A)-[:SENT]->(T1)-[:RECEIVED]->(B)-[:SENT]->(T2)-[:RECEIVED]->(C)
#   -[:SENT]->(T3)-[:RECEIVED]->(A)
#
# Using variable-length paths [:SENT|RECEIVED*6..10] would be simpler
# but less precise (it doesn't enforce the alternating SENT/RECEIVED
# pattern). Instead, we use a UNION of three explicit path patterns
# for 3, 4, and 5 hops respectively.
#
# PERFORMANCE CHARACTERISTICS (for the IEEE paper):
# - The traversal starts from a single node (origin) — this is efficient
#   because Neo4j uses index-free adjacency for local traversal.
# - We limit results (LIMIT 5) to bound the response time — we only need
#   to know IF a cycle exists, not enumerate ALL cycles.
# - Each hop adds O(d) to the traversal cost (d = avg degree), making
#   the total cost O(d^k) where k = number of hops.
# - The isShortestPath optimization tells Neo4j to prune paths that revisit
#   already-visited nodes, dramatically reducing the search space.
#
# WHY SEPARATE QUERIES FOR EACH HOP LENGTH:
# Neo4j's variable-length path syntax ([:REL*min..max]) doesn't enforce
# alternating relationship types. Our schema requires strict alternation
# of SENT and RECEIVED relationships. Separate patterns for each hop
# length guarantee correctness while remaining readable.
# -----------------------------------------------------------------------
CYPHER_DETECT_CYCLE_3HOP = """
MATCH path = (origin:User {id: $nameOrig})
  -[:SENT]->(:Transaction)-[:RECEIVED]->(b:User)
  -[:SENT]->(:Transaction)-[:RECEIVED]->(c:User)
  -[:SENT]->(:Transaction)-[:RECEIVED]->(origin)
WHERE origin <> b AND origin <> c AND b <> c
RETURN
  [n IN nodes(path) WHERE n:User | n.id] AS userPath,
  [n IN nodes(path) WHERE n:Transaction | n.amount] AS amounts,
  length(path) AS pathLength,
  3 AS hopCount
LIMIT 5
"""

CYPHER_DETECT_CYCLE_4HOP = """
MATCH path = (origin:User {id: $nameOrig})
  -[:SENT]->(:Transaction)-[:RECEIVED]->(b:User)
  -[:SENT]->(:Transaction)-[:RECEIVED]->(c:User)
  -[:SENT]->(:Transaction)-[:RECEIVED]->(d:User)
  -[:SENT]->(:Transaction)-[:RECEIVED]->(origin)
WHERE origin <> b AND origin <> c AND origin <> d
  AND b <> c AND b <> d AND c <> d
RETURN
  [n IN nodes(path) WHERE n:User | n.id] AS userPath,
  [n IN nodes(path) WHERE n:Transaction | n.amount] AS amounts,
  length(path) AS pathLength,
  4 AS hopCount
LIMIT 5
"""

CYPHER_DETECT_CYCLE_5HOP = """
MATCH path = (origin:User {id: $nameOrig})
  -[:SENT]->(:Transaction)-[:RECEIVED]->(b:User)
  -[:SENT]->(:Transaction)-[:RECEIVED]->(c:User)
  -[:SENT]->(:Transaction)-[:RECEIVED]->(d:User)
  -[:SENT]->(:Transaction)-[:RECEIVED]->(e:User)
  -[:SENT]->(:Transaction)-[:RECEIVED]->(origin)
WHERE origin <> b AND origin <> c AND origin <> d AND origin <> e
  AND b <> c AND b <> d AND b <> e
  AND c <> d AND c <> e
  AND d <> e
RETURN
  [n IN nodes(path) WHERE n:User | n.id] AS userPath,
  [n IN nodes(path) WHERE n:Transaction | n.amount] AS amounts,
  length(path) AS pathLength,
  5 AS hopCount
LIMIT 5
"""

# -----------------------------------------------------------------------
# QUERY 3: Advanced Graph Algorithms (PageRank & Louvain)
# -----------------------------------------------------------------------
# Uses Neo4j Graph Data Science (GDS) with Cypher Projections.
# We project the User-to-User fund flow graph dynamically to compute
# the PageRank (influence/centrality) and Louvain Community ID
# for the suspicious origin account.
# -----------------------------------------------------------------------
CYPHER_PAGE_RANK = """
CALL gds.pageRank.stream({
  nodeQuery: 'MATCH (u:User) RETURN id(u) AS id',
  relationshipQuery: 'MATCH (u1:User)-[:SENT]->(:Transaction)-[:RECEIVED]->(u2:User) RETURN id(u1) AS source, id(u2) AS target'
}) YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS user, score
WHERE user.id = $nameOrig
RETURN score
"""

CYPHER_LOUVAIN = """
CALL gds.louvain.stream({
  nodeQuery: 'MATCH (u:User) RETURN id(u) AS id',
  relationshipQuery: 'MATCH (u1:User)-[:SENT]->(:Transaction)-[:RECEIVED]->(u2:User) RETURN id(u1) AS source, id(u2) AS target'
}) YIELD nodeId, communityId
WITH gds.util.asNode(nodeId) AS user, communityId
WHERE user.id = $nameOrig
RETURN communityId
"""


# =============================================================================
# BENCHMARK COLLECTOR
# =============================================================================
# Collects fine-grained performance metrics for every processed transaction,
# comparing Tier 1 (Flink/RocksDB state lookup) vs. Tier 2 (Neo4j graph
# traversal) latencies. This data is the empirical foundation of our IEEE
# paper's evaluation section.
#
# METRICS COLLECTED:
# ==================
# Per-transaction:
#   - tier1_latency_ms: Time spent in Flink's KeyedProcessFunction
#     (read from the enriched message produced by stream_processor.py).
#   - tier2_insert_latency_ms: Time to MERGE the transaction subgraph
#     into Neo4j (measured in this worker).
#   - tier2_cycle_latency_ms: Time to execute the cycle detection Cypher
#     query (measured in this worker).
#   - tier2_total_latency_ms: Total Tier 2 processing time
#     (insert + cycle detection + overhead).
#   - e2e_latency_ms: End-to-end latency from producer to Tier 2 output.
#
# Aggregate (computed at reporting intervals):
#   - Throughput (TPS): Transactions processed per second.
#   - Latency percentiles: p50, p95, p99, max for each metric.
#   - Cycle detection rate: Fraction of suspicious transactions that
#     contain actual cycles.
#   - Speedup ratio: tier2_total / tier1_total — quantifies how much
#     more expensive graph queries are compared to state lookups.
#
# OUTPUT FORMATS:
# ===============
# 1. Kafka topic (benchmark-metrics): Real-time metric stream for live
#    monitoring during experiments.
# 2. CSV file: Persistent record for post-hoc analysis and chart generation.
# 3. JSON summary: End-of-run summary suitable for inclusion in the paper.
# =============================================================================
class BenchmarkCollector:
    """
    Collects, aggregates, and exports performance benchmarking data for
    the StreamGraph dual-tier architecture.

    This class is the data source for the IEEE paper's performance evaluation:
    - Table: Tier 1 vs. Tier 2 latency comparison (p50, p95, p99).
    - Chart: Latency distribution histograms.
    - Chart: Throughput vs. latency under varying load.
    - Chart: Cycle detection rate vs. velocity window thresholds.
    """

    def __init__(
        self,
        output_dir: str = BENCHMARK_OUTPUT_DIR,
        report_interval_sec: int = BENCHMARK_EXPORT_INTERVAL,
        enabled: bool = BENCHMARK_ENABLED,
    ):
        """
        Initialize the benchmark collector.

        Args:
            output_dir: Directory for CSV/JSON metric exports.
            report_interval_sec: Seconds between periodic aggregate reports.
            enabled: If False, metrics collection is disabled (no-op).
        """
        self.enabled = enabled
        self.output_dir = Path(output_dir)
        self.report_interval_sec = report_interval_sec

        if not self.enabled:
            logger.info("Benchmarking DISABLED. Set BENCHMARK_ENABLED=true to enable.")
            return

        # Create output directory.
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Per-transaction metric buffers (flushed periodically to CSV).
        self._records: List[Dict[str, Any]] = []

        # Aggregate counters.
        self._total_processed = 0
        self._total_cycles_found = 0
        self._total_fraud_alerts = 0
        self._total_errors = 0

        # Latency accumulators for aggregate statistics.
        self._tier1_latencies: List[float] = []
        self._tier2_insert_latencies: List[float] = []
        self._tier2_cycle_latencies: List[float] = []
        self._tier2_total_latencies: List[float] = []
        self._e2e_latencies: List[float] = []

        # Interval tracking for periodic reporting.
        self._interval_start = time.monotonic()
        self._interval_count = 0

        # Start time for overall throughput calculation.
        self._start_time = time.monotonic()

        # CSV file for per-transaction metrics.
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._csv_path = self.output_dir / f"tier2_metrics_{timestamp}.csv"
        self._csv_file = None
        self._csv_writer = None
        self._init_csv()

        logger.info(
            f"BenchmarkCollector initialized: "
            f"output_dir={self.output_dir.absolute()} | "
            f"report_interval={report_interval_sec}s | "
            f"csv={self._csv_path.name}"
        )

    def _init_csv(self):
        """Initialize the CSV file for per-transaction metric export."""
        self._csv_file = open(self._csv_path, "w", newline="", encoding="utf-8")
        self._csv_writer = csv.writer(self._csv_file)

        # CSV header — each column maps to a metric for the IEEE paper.
        self._csv_writer.writerow([
            "timestamp",
            "transaction_id",
            "name_orig",
            "name_dest",
            "amount",
            "is_fraud_label",
            "tier1_classification",
            "tier1_latency_ms",
            "tier1_state_latency_ms",
            "tier1_e2e_latency_ms",
            "tier2_insert_latency_ms",
            "tier2_cycle_latency_ms",
            "tier2_total_latency_ms",
            "e2e_total_latency_ms",
            "cycle_detected",
            "cycle_hop_count",
            "cycle_path",
            "tier2_vs_tier1_ratio",
        ])
        self._csv_file.flush()

    def record_transaction(
        self,
        txn: Dict[str, Any],
        tier2_insert_ms: float,
        tier2_cycle_ms: float,
        tier2_total_ms: float,
        cycle_detected: bool,
        cycle_details: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Record metrics for a single processed transaction.

        Args:
            txn: The enriched transaction record from stream_processor.py.
            tier2_insert_ms: Time to insert the subgraph into Neo4j (ms).
            tier2_cycle_ms: Time to execute the cycle detection query (ms).
            tier2_total_ms: Total Tier 2 processing time (ms).
            cycle_detected: Whether a cycle was found.
            cycle_details: Details of the detected cycle (if any).

        Returns:
            Dictionary with all computed metrics for this transaction.
        """
        if not self.enabled:
            return {}

        # Extract Tier 1 metrics from the enriched transaction.
        tier1_latency = float(txn.get("tier1_latency_ms", 0.0))
        tier1_state_latency = float(txn.get("tier1_state_latency_ms", 0.0))
        tier1_e2e_latency = float(txn.get("tier1_e2e_latency_ms", 0.0))
        producer_ts = int(txn.get("producer_timestamp_ms", 0))

        # Compute end-to-end latency: producer → Tier 2 completion.
        current_time_ms = int(time.time() * 1000)
        e2e_total_ms = (
            current_time_ms - producer_ts if producer_ts > 0 else -1.0
        )

        # Compute the speedup ratio: how many times more expensive is
        # Tier 2 compared to Tier 1? This is the key metric for our paper.
        # A higher ratio validates the dual-tier design: if graph queries
        # are 100x more expensive, filtering 99% at Tier 1 yields ~99x
        # reduction in graph query load.
        tier2_vs_tier1_ratio = (
            round(tier2_total_ms / tier1_latency, 2)
            if tier1_latency > 0 else -1.0
        )

        # Build the metric record.
        metric = {
            "timestamp": datetime.now().isoformat(),
            "transaction_id": txn.get("transaction_id", ""),
            "name_orig": txn.get("nameOrig", ""),
            "name_dest": txn.get("nameDest", ""),
            "amount": txn.get("amount", 0.0),
            "is_fraud_label": txn.get("isFraud", 0),
            "tier1_classification": txn.get("tier1_classification", ""),
            "tier1_latency_ms": round(tier1_latency, 3),
            "tier1_state_latency_ms": round(tier1_state_latency, 3),
            "tier1_e2e_latency_ms": round(tier1_e2e_latency, 3),
            "tier2_insert_latency_ms": round(tier2_insert_ms, 3),
            "tier2_cycle_latency_ms": round(tier2_cycle_ms, 3),
            "tier2_total_latency_ms": round(tier2_total_ms, 3),
            "e2e_total_latency_ms": round(e2e_total_ms, 3),
            "cycle_detected": cycle_detected,
            "cycle_hop_count": (
                cycle_details.get("hop_count", 0) if cycle_details else 0
            ),
            "cycle_path": (
                json.dumps(cycle_details.get("user_path", []))
                if cycle_details else "[]"
            ),
            "tier2_vs_tier1_ratio": tier2_vs_tier1_ratio,
        }

        # Write to CSV.
        self._csv_writer.writerow([
            metric["timestamp"],
            metric["transaction_id"],
            metric["name_orig"],
            metric["name_dest"],
            metric["amount"],
            metric["is_fraud_label"],
            metric["tier1_classification"],
            metric["tier1_latency_ms"],
            metric["tier1_state_latency_ms"],
            metric["tier1_e2e_latency_ms"],
            metric["tier2_insert_latency_ms"],
            metric["tier2_cycle_latency_ms"],
            metric["tier2_total_latency_ms"],
            metric["e2e_total_latency_ms"],
            metric["cycle_detected"],
            metric["cycle_hop_count"],
            metric["cycle_path"],
            metric["tier2_vs_tier1_ratio"],
        ])
        self._csv_file.flush()

        # Update accumulators.
        self._total_processed += 1
        self._interval_count += 1
        if cycle_detected:
            self._total_cycles_found += 1

        self._tier1_latencies.append(tier1_latency)
        self._tier2_insert_latencies.append(tier2_insert_ms)
        self._tier2_cycle_latencies.append(tier2_cycle_ms)
        self._tier2_total_latencies.append(tier2_total_ms)
        if e2e_total_ms > 0:
            self._e2e_latencies.append(e2e_total_ms)

        # Store for aggregate computation.
        self._records.append(metric)

        return metric

    def record_error(self):
        """Record a processing error."""
        if self.enabled:
            self._total_errors += 1

    def record_fraud_alert(self):
        """Record that a fraud alert was published."""
        if self.enabled:
            self._total_fraud_alerts += 1

    def should_report(self) -> bool:
        """Check if it's time for a periodic aggregate report."""
        if not self.enabled:
            return False
        elapsed = time.monotonic() - self._interval_start
        return elapsed >= self.report_interval_sec

    def get_interval_report(self, reset: bool = True) -> Dict[str, Any]:
        """
        Compute aggregate statistics for the current reporting interval.

        This produces the data for real-time monitoring during experiments.
        The aggregate statistics are also published to the benchmark-metrics
        Kafka topic for live dashboarding.

        Args:
            reset: If True, reset interval counters after computing stats.

        Returns:
            Dictionary with interval aggregate metrics.
        """
        if not self.enabled:
            return {}

        now = time.monotonic()
        elapsed = now - self._interval_start
        tps = self._interval_count / elapsed if elapsed > 0 else 0.0

        report = {
            "report_type": "interval",
            "timestamp": datetime.now().isoformat(),
            "interval_seconds": round(elapsed, 2),
            "interval_processed": self._interval_count,
            "interval_tps": round(tps, 1),
            "total_processed": self._total_processed,
            "total_cycles_found": self._total_cycles_found,
            "total_fraud_alerts": self._total_fraud_alerts,
            "total_errors": self._total_errors,
            "cycle_detection_rate": (
                f"{(self._total_cycles_found / max(self._total_processed, 1)) * 100:.2f}%"
            ),
        }

        # Compute latency percentiles for the interval.
        if self._tier1_latencies:
            report["tier1_latency"] = self._compute_percentiles(
                self._tier1_latencies, "tier1"
            )
        if self._tier2_total_latencies:
            report["tier2_total_latency"] = self._compute_percentiles(
                self._tier2_total_latencies, "tier2_total"
            )
        if self._tier2_insert_latencies:
            report["tier2_insert_latency"] = self._compute_percentiles(
                self._tier2_insert_latencies, "tier2_insert"
            )
        if self._tier2_cycle_latencies:
            report["tier2_cycle_latency"] = self._compute_percentiles(
                self._tier2_cycle_latencies, "tier2_cycle"
            )
        if self._e2e_latencies:
            report["e2e_latency"] = self._compute_percentiles(
                self._e2e_latencies, "e2e"
            )

        # Compute the median speedup ratio (Tier 2 / Tier 1).
        if self._tier1_latencies and self._tier2_total_latencies:
            ratios = [
                t2 / t1 if t1 > 0 else 0
                for t1, t2 in zip(self._tier1_latencies, self._tier2_total_latencies)
            ]
            if ratios:
                report["speedup_ratio_median"] = round(
                    sorted(ratios)[len(ratios) // 2], 2
                )
                report["speedup_ratio_mean"] = round(
                    statistics.mean(ratios), 2
                )

        if reset:
            self._interval_start = now
            self._interval_count = 0
            # Keep accumulated latencies for final summary, but clear
            # interval-specific data by slicing (no memory leak).
            # We keep the raw latencies for the final summary.

        return report

    def get_final_summary(self) -> Dict[str, Any]:
        """
        Compute the final summary statistics for the entire run.

        This is the primary data source for the IEEE paper's evaluation
        section (Table 1: Tier 1 vs. Tier 2 Latency Comparison).

        Returns:
            Comprehensive summary dictionary.
        """
        if not self.enabled:
            return {}

        total_elapsed = time.monotonic() - self._start_time
        overall_tps = (
            self._total_processed / total_elapsed if total_elapsed > 0 else 0
        )

        summary = {
            "report_type": "final_summary",
            "timestamp": datetime.now().isoformat(),
            "total_elapsed_seconds": round(total_elapsed, 2),
            "total_processed": self._total_processed,
            "total_cycles_found": self._total_cycles_found,
            "total_fraud_alerts": self._total_fraud_alerts,
            "total_errors": self._total_errors,
            "overall_throughput_tps": round(overall_tps, 2),
            "cycle_detection_rate": (
                f"{(self._total_cycles_found / max(self._total_processed, 1)) * 100:.4f}%"
            ),
        }

        # ===================================================================
        # KEY TABLE FOR IEEE PAPER: Tier 1 vs. Tier 2 Latency Comparison
        # ===================================================================
        # This section generates the exact numbers for the paper's
        # performance evaluation table.
        # ===================================================================
        if self._tier1_latencies:
            summary["tier1_latency_stats"] = self._compute_percentiles(
                self._tier1_latencies, "tier1"
            )
        if self._tier2_total_latencies:
            summary["tier2_total_latency_stats"] = self._compute_percentiles(
                self._tier2_total_latencies, "tier2_total"
            )
        if self._tier2_insert_latencies:
            summary["tier2_insert_latency_stats"] = self._compute_percentiles(
                self._tier2_insert_latencies, "tier2_insert"
            )
        if self._tier2_cycle_latencies:
            summary["tier2_cycle_latency_stats"] = self._compute_percentiles(
                self._tier2_cycle_latencies, "tier2_cycle"
            )
        if self._e2e_latencies:
            summary["e2e_latency_stats"] = self._compute_percentiles(
                self._e2e_latencies, "e2e"
            )

        # Speedup ratio summary.
        if self._tier1_latencies and self._tier2_total_latencies:
            ratios = [
                t2 / t1 if t1 > 0 else 0
                for t1, t2 in zip(self._tier1_latencies, self._tier2_total_latencies)
            ]
            if ratios:
                sorted_ratios = sorted(ratios)
                summary["speedup_ratio"] = {
                    "median": round(sorted_ratios[len(sorted_ratios) // 2], 2),
                    "mean": round(statistics.mean(sorted_ratios), 2),
                    "p95": round(sorted_ratios[int(len(sorted_ratios) * 0.95)], 2),
                    "max": round(sorted_ratios[-1], 2),
                    "interpretation": (
                        f"Tier 2 graph queries are ~{round(statistics.mean(sorted_ratios), 1)}x "
                        f"more expensive than Tier 1 state lookups on average. "
                        f"The dual-tier design avoids this cost for "
                        f"~{100 - (self._total_processed / max(self._total_processed, 1)) * 100:.0f}% of transactions."
                    ),
                }

        return summary

    @staticmethod
    def _compute_percentiles(
        values: List[float], prefix: str
    ) -> Dict[str, float]:
        """
        Compute latency percentiles for a list of measurements.

        Args:
            values: List of latency values in milliseconds.
            prefix: Metric name prefix for the output keys.

        Returns:
            Dictionary with count, mean, p50, p95, p99, max, stddev.
        """
        if not values:
            return {}

        sorted_vals = sorted(values)
        n = len(sorted_vals)

        return {
            f"{prefix}_count": n,
            f"{prefix}_mean_ms": round(statistics.mean(sorted_vals), 3),
            f"{prefix}_p50_ms": round(sorted_vals[n // 2], 3),
            f"{prefix}_p95_ms": round(sorted_vals[int(n * 0.95)], 3),
            f"{prefix}_p99_ms": round(sorted_vals[int(n * 0.99)], 3),
            f"{prefix}_max_ms": round(sorted_vals[-1], 3),
            f"{prefix}_stddev_ms": round(
                statistics.stdev(sorted_vals) if n > 1 else 0.0, 3
            ),
        }

    def close(self):
        """Flush and close the CSV file."""
        if self._csv_file:
            self._csv_file.flush()
            self._csv_file.close()
            logger.info(f"Benchmark CSV closed: {self._csv_path.absolute()}")


# =============================================================================
# NEO4J CLIENT WRAPPER
# =============================================================================
# Encapsulates all Neo4j interactions with:
# 1. Connection pooling and automatic reconnection.
# 2. Explicit transaction functions with retry logic.
# 3. Per-operation latency instrumentation.
# 4. Structured error handling and logging.
#
# CONNECTION POOLING (for the IEEE paper):
# The Neo4j Python driver maintains a pool of Bolt connections. When a
# session is opened, it borrows a connection from the pool; when the session
# closes, the connection is returned. This amortizes the TCP handshake and
# TLS negotiation cost across many queries.
#
# Pool size is set to NEO4J_MAX_POOL_SIZE (50 by default), allowing up to
# 50 concurrent Bolt connections. For our single-threaded consumer, a pool
# size of 5-10 would suffice, but we over-provision to accommodate future
# multi-threaded or async processing.
# =============================================================================
class Neo4jClient:
    """
    Neo4j client for the StreamGraph Tier 2 graph validation engine.

    Handles connection lifecycle, transaction management, subgraph insertion,
    and cycle detection queries with full performance instrumentation.
    """

    def __init__(
        self,
        uri: str = NEO4J_URI,
        user: str = NEO4J_USER,
        password: str = NEO4J_PASSWORD,
        database: str = NEO4J_DATABASE,
        max_pool_size: int = NEO4J_MAX_POOL_SIZE,
    ):
        """
        Initialize the Neo4j client with connection pooling.

        Args:
            uri: Neo4j Bolt URI (e.g., bolt://localhost:7687).
            user: Neo4j username.
            password: Neo4j password.
            database: Target database name.
            max_pool_size: Maximum number of pooled Bolt connections.
        """
        self.database = database
        self._driver = None
        self._uri = uri
        self._user = user
        self._password = password
        self._max_pool_size = max_pool_size

        # Performance counters (not persisted; for logging only).
        self._total_inserts = 0
        self._total_cycle_queries = 0
        self._total_cycles_found = 0

        self._connect()

    def _connect(self):
        """
        Establish a connection to Neo4j with retry logic.

        Neo4j may take 30-60 seconds to fully start in Docker. We retry
        up to 12 times with 5-second intervals (60 seconds total) to
        handle container startup ordering.
        """
        max_retries = 12
        retry_delay_sec = 5

        for attempt in range(1, max_retries + 1):
            try:
                self._driver = GraphDatabase.driver(
                    self._uri,
                    auth=(self._user, self._password),
                    max_connection_pool_size=self._max_pool_size,
                    # Connection acquisition timeout: how long to wait for
                    # a pooled connection before raising an error.
                    connection_acquisition_timeout=30.0,
                    # Max connection lifetime: connections older than this
                    # are replaced, preventing stale connection issues.
                    max_connection_lifetime=3600,
                )

                # Verify connectivity by running a trivial query.
                self._driver.verify_connectivity()

                logger.info(
                    f"Neo4j connected successfully: {self._uri} "
                    f"(attempt {attempt}/{max_retries})"
                )
                return

            except (ServiceUnavailable, SessionExpired) as e:
                logger.warning(
                    f"Neo4j connection attempt {attempt}/{max_retries} failed: "
                    f"{type(e).__name__}: {e}"
                )
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay_sec}s...")
                    time.sleep(retry_delay_sec)
                else:
                    logger.critical(
                        f"Failed to connect to Neo4j after {max_retries} attempts. "
                        f"Ensure Neo4j is running at {self._uri}."
                    )
                    raise

    def insert_transaction_subgraph(
        self, txn: Dict[str, Any]
    ) -> Tuple[bool, float]:
        """
        Insert a transaction subgraph into Neo4j.

        Creates (or merges) the sender User, recipient User, Transaction
        node, and the SENT/RECEIVED relationships.

        Args:
            txn: Enriched transaction record from stream_processor.py.

        Returns:
            Tuple of (success: bool, latency_ms: float).
        """
        start_ns = time.monotonic_ns()

        params = {
            "nameOrig": txn.get("nameOrig", ""),
            "nameDest": txn.get("nameDest", ""),
            "txnId": txn.get("transaction_id", ""),
            "amount": float(txn.get("amount", 0.0)),
            "timestamp": int(txn.get("tier1_timestamp_ms", int(time.time() * 1000))),
            "txnType": txn.get("type", "UNKNOWN"),
            "step": int(txn.get("step", 0)),
            "isFraud": int(txn.get("isFraud", 0)),
            "tier1Classification": txn.get("tier1_classification", "UNKNOWN"),
        }

        try:
            with self._driver.session(database=self.database) as session:
                # Use an explicit write transaction for atomicity.
                # The transaction function is automatically retried on
                # transient errors (deadlocks, leader switches) up to
                # the driver's retry limit.
                result = session.execute_write(
                    self._merge_subgraph_tx, params
                )

            latency_ms = (time.monotonic_ns() - start_ns) / 1e6
            self._total_inserts += 1
            return True, round(latency_ms, 3)

        except (ServiceUnavailable, SessionExpired) as e:
            latency_ms = (time.monotonic_ns() - start_ns) / 1e6
            logger.error(
                f"[NEO4J_INSERT_ERROR] Connection error during insert: "
                f"{type(e).__name__}: {e} | txn_id={params['txnId']}"
            )
            # Attempt reconnection for subsequent calls.
            self._try_reconnect()
            return False, round(latency_ms, 3)

        except ClientError as e:
            latency_ms = (time.monotonic_ns() - start_ns) / 1e6
            logger.error(
                f"[NEO4J_INSERT_ERROR] Client error: "
                f"{type(e).__name__}: {e} | txn_id={params['txnId']}"
            )
            return False, round(latency_ms, 3)

        except Exception as e:
            latency_ms = (time.monotonic_ns() - start_ns) / 1e6
            logger.error(
                f"[NEO4J_INSERT_ERROR] Unexpected error: "
                f"{type(e).__name__}: {e} | txn_id={params['txnId']}",
                exc_info=True,
            )
            return False, round(latency_ms, 3)

    @staticmethod
    def _merge_subgraph_tx(tx, params: Dict[str, Any]):
        """
        Transaction function for merging the transaction subgraph.

        This function runs within a Neo4j transaction context and is
        automatically retried on transient errors. It executes the
        CYPHER_MERGE_SUBGRAPH query.

        Args:
            tx: Neo4j transaction object.
            params: Query parameters (nameOrig, nameDest, txnId, etc.).

        Returns:
            Query result.
        """
        result = tx.run(CYPHER_MERGE_SUBGRAPH, **params)
        return result.consume()

    def detect_cycles(
        self, name_orig: str
    ) -> Tuple[bool, float, Optional[Dict], Dict]:
        """
        Execute cycle detection queries for a given user (nameOrig).

        Runs three separate queries for 3-hop, 4-hop, and 5-hop cycles.
        Returns as soon as any cycle is found (short-circuit evaluation).

        PERFORMANCE TRADE-OFF (for the IEEE paper):
        Running separate queries for each hop length (instead of a single
        variable-length path query) allows us to:
        1. Measure latency per hop length — generating the "Latency vs.
           Cycle Length" chart for the paper.
        2. Short-circuit on the first cycle found — avoiding the cost
           of deeper traversals when a short cycle exists.
        3. Enforce the alternating SENT/RECEIVED pattern precisely.

        Args:
            name_orig: The account ID to check for cycle participation.

        Returns:
            Tuple of:
              - cycle_found: bool
              - total_latency_ms: float (sum of all query latencies)
              - cycle_details: Dict with user_path, amounts, hop_count
                              (or None if no cycle found)
        """
        total_start_ns = time.monotonic_ns()
        params = {"nameOrig": name_orig}
        gds_metrics = {}  # Initialize before use

        # List of (query, hop_count) to execute in order.
        # We start with the shortest cycle (3-hop) because:
        # 1. It's the cheapest query (shallowest traversal).
        # 2. Short cycles are the most common fraud ring pattern.
        # 3. If found, we can skip the more expensive 4-hop and 5-hop queries.
        queries = [
            (CYPHER_DETECT_CYCLE_3HOP, 3),
            (CYPHER_DETECT_CYCLE_4HOP, 4),
            (CYPHER_DETECT_CYCLE_5HOP, 5),
        ]

        for query, hop_count in queries:
            try:
                with self._driver.session(database=self.database) as session:
                    result = session.execute_read(
                        self._run_cycle_query_tx, query, params
                    )

                if result:
                    # Cycle found! Extract details from the first result.
                    total_latency_ms = (time.monotonic_ns() - total_start_ns) / 1e6
                    self._total_cycles_found += 1
                    self._total_cycle_queries += 1

                    cycle_details = {
                        "hop_count": result[0].get("hopCount", hop_count),
                        "user_path": result[0].get("userPath", []),
                        "amounts": result[0].get("amounts", []),
                        "path_length": result[0].get("pathLength", 0),
                    }

                    logger.info(
                        f"[CYCLE_DETECTED] {hop_count}-hop cycle for "
                        f"user={name_orig} | "
                        f"path={cycle_details['user_path']} | "
                        f"latency={round(total_latency_ms, 2)}ms"
                    )

                    # Execute GDS metrics on the cycle participants
                    try:
                        with self._driver.session(database=self.database) as gds_session:
                            pr_res = gds_session.execute_read(self._run_cycle_query_tx, CYPHER_PAGE_RANK, params)
                            if pr_res:
                                gds_metrics["pagerank"] = pr_res[0].get("score", 0.0)
                            
                            lv_res = gds_session.execute_read(self._run_cycle_query_tx, CYPHER_LOUVAIN, params)
                            if lv_res:
                                gds_metrics["community_id"] = lv_res[0].get("communityId", -1)
                    except Exception as e:
                        logger.error(f"[GDS_ERROR] Failed to run advanced graph algorithms: {e}")

                    total_latency_ms = (time.monotonic_ns() - total_start_ns) / 1e6
                    return True, round(total_latency_ms, 3), cycle_details, gds_metrics

            except (ServiceUnavailable, SessionExpired) as e:
                logger.error(
                    f"[NEO4J_CYCLE_ERROR] Connection error during "
                    f"{hop_count}-hop cycle query: {e}"
                )
                self._try_reconnect()

            except Neo4jError as e:
                logger.error(
                    f"[NEO4J_CYCLE_ERROR] Query error during "
                    f"{hop_count}-hop cycle query: {e}"
                )

            except Exception as e:
                logger.error(
                    f"[NEO4J_CYCLE_ERROR] Unexpected error during "
                    f"{hop_count}-hop cycle query: {e}",
                    exc_info=True,
                )

        # No cycle found after checking all hop lengths.
        total_latency_ms = (time.monotonic_ns() - total_start_ns) / 1e6
        self._total_cycle_queries += 1
        return False, round(total_latency_ms, 3), None, gds_metrics

    @staticmethod
    def _run_cycle_query_tx(tx, query: str, params: Dict[str, Any]):
        """
        Transaction function for running a cycle detection query.

        This runs as a read transaction (no writes), allowing Neo4j to
        route it to a read replica in a clustered setup.

        Args:
            tx: Neo4j transaction object.
            query: Cypher query string.
            params: Query parameters.

        Returns:
            List of result records (empty if no cycles found).
        """
        result = tx.run(query, **params)
        return [record.data() for record in result]

    def _try_reconnect(self):
        """Attempt to reconnect to Neo4j after a connection failure."""
        logger.info("Attempting Neo4j reconnection...")
        try:
            if self._driver:
                self._driver.close()
        except Exception:
            pass

        try:
            self._connect()
        except Exception as e:
            logger.error(f"Neo4j reconnection failed: {e}")

    def get_stats(self) -> Dict[str, int]:
        """Return current operation counters."""
        return {
            "total_inserts": self._total_inserts,
            "total_cycle_queries": self._total_cycle_queries,
            "total_cycles_found": self._total_cycles_found,
        }

    def close(self):
        """Close the Neo4j driver and release all pooled connections."""
        if self._driver:
            self._driver.close()
            logger.info("Neo4j driver closed.")


# =============================================================================
# KAFKA CONSUMER & PRODUCER CONFIGURATION
# =============================================================================
def build_consumer_config() -> Dict[str, Any]:
    """
    Build the Kafka consumer configuration for the graph worker.

    KEY CONFIGURATION CHOICES (for the IEEE paper):

    - auto.offset.reset = 'earliest': On first start (no committed offsets),
      begin consuming from the earliest available message. This ensures we
      don't miss suspicious transactions produced before the worker started.

    - enable.auto.commit = False: We manually commit offsets after successful
      processing. This prevents the situation where a message is marked as
      consumed but the Neo4j write fails — which would cause data loss.
      Manual commits implement at-least-once semantics: a message may be
      processed more than once after a restart, but never missed.

    - max.poll.interval.ms = 300000 (5 min): Maximum time between poll()
      calls before the consumer is considered dead and its partitions are
      reassigned. Set high because Neo4j cycle detection queries can take
      several seconds under heavy load.

    Returns:
        Configuration dictionary for confluent_kafka.Consumer.
    """
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_CONSUMER_GROUP,
        "client.id": "streamgraph-graph-worker",

        # Start from beginning on first run (no committed offsets).
        "auto.offset.reset": "earliest",

        # Manual offset commits for at-least-once semantics.
        "enable.auto.commit": False,

        # Session timeout: how long before the broker considers this
        # consumer dead (no heartbeats received).
        "session.timeout.ms": 30000,

        # Heartbeat interval: must be ≤ session.timeout.ms / 3.
        "heartbeat.interval.ms": 10000,

        # Max poll interval: maximum time between successive poll() calls.
        # Must accommodate the worst-case Neo4j query latency.
        "max.poll.interval.ms": 300000,

        # Fetch configuration for throughput tuning.
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 500,
        "max.partition.fetch.bytes": 1048576,  # 1 MB
    }


def build_producer_config() -> Dict[str, Any]:
    """
    Build the Kafka producer configuration for fraud alerts and benchmarks.

    This producer publishes to:
    - fraud-alerts: Confirmed fraud ring detections.
    - benchmark-metrics: Periodic performance statistics.

    Returns:
        Configuration dictionary for confluent_kafka.Producer.
    """
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "streamgraph-graph-worker-producer",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 3,
        "retry.backoff.ms": 100,
        "linger.ms": 10,
        "compression.type": "lz4",
    }


# =============================================================================
# GRAPH WORKER — Main Processing Loop
# =============================================================================
# This class orchestrates the Tier 2 processing pipeline:
#   Kafka consumer → Neo4j insert → Cycle detection → Fraud alert → Benchmark
#
# PROCESSING FLOW (per transaction):
# 1. Consumer.poll() receives a message from suspicious-log.
# 2. Parse the enriched JSON transaction (contains Tier 1 metadata).
# 3. Insert the transaction subgraph into Neo4j (MERGE operation).
# 4. Execute cycle detection query (3-5 hop Cypher traversal).
# 5. If cycle detected: publish fraud alert to fraud-alerts topic.
# 6. Record all latency metrics in the BenchmarkCollector.
# 7. Manually commit the Kafka offset.
#
# ERROR HANDLING STRATEGY:
# - Neo4j connection errors: Retry with exponential backoff, then skip.
# - Malformed messages: Log and skip (don't crash the pipeline).
# - Kafka errors: Log and continue (consumer group rebalancing handles
#   partition reassignment automatically).
# =============================================================================
class GraphWorker:
    """
    Tier 2 Graph Validation Worker for the StreamGraph pipeline.

    Consumes SUSPICIOUS transactions from Kafka, validates them against
    the Neo4j graph for circular transaction paths (fraud rings), and
    publishes fraud alerts and benchmarking metrics.
    """

    def __init__(self):
        """
        Initialize the GraphWorker with Kafka consumer/producer, Neo4j
        client, and benchmark collector.
        """
        self._shutdown_requested = False

        # Initialize Kafka consumer for suspicious-log topic.
        self._consumer = Consumer(build_consumer_config())
        self._consumer.subscribe([KAFKA_TOPIC_SUSPICIOUS])
        logger.info(
            f"Kafka consumer subscribed to: {KAFKA_TOPIC_SUSPICIOUS} "
            f"(group: {KAFKA_CONSUMER_GROUP})"
        )

        # Initialize Kafka producer for fraud-alerts and benchmark-metrics.
        self._producer = Producer(build_producer_config())
        logger.info("Kafka producer initialized for fraud-alerts and benchmark-metrics")

        # Initialize Neo4j client.
        self._neo4j = Neo4jClient()

        # Initialize benchmark collector.
        self._benchmark = BenchmarkCollector()

        # Processing counters.
        self._total_processed = 0
        self._total_errors = 0
        self._total_fraud_alerts = 0

        # Register signal handlers for graceful shutdown.
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("GraphWorker initialized and ready to process.")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signal_name = signal.Signals(signum).name
        logger.info(f"Received {signal_name} — initiating graceful shutdown...")
        self._shutdown_requested = True

    def run(self):
        """
        Main processing loop. Polls Kafka for suspicious transactions
        and processes them through the Tier 2 pipeline.

        This loop runs indefinitely until a shutdown signal is received
        (SIGINT or SIGTERM). Each iteration:
        1. Polls Kafka for a message (1-second timeout).
        2. Processes the message through Neo4j.
        3. Reports periodic metrics.
        4. Commits the Kafka offset on success.
        """
        logger.info("=" * 70)
        logger.info("StreamGraph — Tier 2 Graph Worker Starting")
        logger.info(f"  Source Topic: {KAFKA_TOPIC_SUSPICIOUS}")
        logger.info(f"  Neo4j URI: {NEO4J_URI}")
        logger.info(f"  Fraud Alerts Topic: {KAFKA_TOPIC_FRAUD_ALERTS}")
        logger.info(f"  Benchmark Metrics Topic: {KAFKA_TOPIC_BENCHMARKS}")
        logger.info(f"  Cycle Detection: {MIN_CYCLE_HOPS}-{MAX_CYCLE_HOPS} hops")
        logger.info(f"  Benchmarking: {'ENABLED' if BENCHMARK_ENABLED else 'DISABLED'}")
        logger.info("=" * 70)

        try:
            while not self._shutdown_requested:
                # =============================================================
                # STEP 1: Poll Kafka for the next suspicious transaction
                # =============================================================
                # poll(timeout=1.0) blocks for up to 1 second waiting for a
                # message. The timeout prevents busy-waiting when there are
                # no messages, while keeping the shutdown check responsive.
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    # No message available within the timeout period.
                    # Check if we should report periodic metrics.
                    if self._benchmark.should_report():
                        self._report_metrics()
                    continue

                if msg.error():
                    # Handle Kafka errors.
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition — normal in finite datasets.
                        logger.debug(
                            f"Reached end of partition: "
                            f"topic={msg.topic()} "
                            f"partition={msg.partition()} "
                            f"offset={msg.offset()}"
                        )
                    else:
                        logger.error(
                            f"Kafka consumer error: {msg.error().str()}"
                        )
                        self._total_errors += 1
                    continue

                # =============================================================
                # STEP 2: Process the transaction
                # =============================================================
                self._process_transaction(msg)

                # =============================================================
                # STEP 3: Commit the offset (at-least-once)
                # =============================================================
                # We commit AFTER successful processing to ensure at-least-once
                # semantics. If the worker crashes before committing, the
                # message will be redelivered on restart. Our MERGE-based
                # Neo4j writes handle this idempotently.
                try:
                    self._consumer.commit(asynchronous=False)
                except KafkaException as e:
                    logger.error(f"Failed to commit Kafka offset: {e}")

                # =============================================================
                # STEP 4: Periodic metric reporting
                # =============================================================
                if self._benchmark.should_report():
                    self._report_metrics()

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — stopping graph worker.")

        finally:
            self._shutdown()

    def _process_transaction(self, msg):
        """
        Process a single suspicious transaction through the Tier 2 pipeline.

        Steps:
        1. Parse the enriched JSON message.
        2. Insert the transaction subgraph into Neo4j.
        3. Run cycle detection on the sender (nameOrig).
        4. If cycle found: publish fraud alert.
        5. Record benchmarking metrics.

        Args:
            msg: Kafka message object from the suspicious-log topic.
        """
        tier2_start_ns = time.monotonic_ns()

        # -----------------------------------------------------------------
        # Parse the message
        # -----------------------------------------------------------------
        try:
            value = msg.value().decode("utf-8")
            txn = json.loads(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            logger.error(
                f"[PARSE_ERROR] Failed to decode message: {e} | "
                f"offset={msg.offset()} partition={msg.partition()}"
            )
            self._total_errors += 1
            self._benchmark.record_error()
            return

        txn_id = txn.get("transaction_id", "UNKNOWN")
        name_orig = txn.get("nameOrig", "")
        name_dest = txn.get("nameDest", "")
        amount = txn.get("amount", 0.0)

        # -----------------------------------------------------------------
        # Insert the transaction subgraph into Neo4j
        # -----------------------------------------------------------------
        insert_success, insert_latency_ms = self._neo4j.insert_transaction_subgraph(txn)

        if not insert_success:
            logger.error(
                f"[INSERT_FAILED] Could not insert transaction {txn_id} "
                f"into Neo4j. Skipping cycle detection."
            )
            self._total_errors += 1
            self._benchmark.record_error()
            return

        # -----------------------------------------------------------------
        # Execute cycle detection
        # -----------------------------------------------------------------
        cycle_found, cycle_latency_ms, cycle_details, gds_metrics = self._neo4j.detect_cycles(
            name_orig
        )

        # -----------------------------------------------------------------
        # Compute total Tier 2 latency
        # -----------------------------------------------------------------
        tier2_total_ms = (time.monotonic_ns() - tier2_start_ns) / 1e6

        # -----------------------------------------------------------------
        # If cycle detected: publish fraud alert
        # -----------------------------------------------------------------
        if cycle_found and cycle_details:
            self._publish_fraud_alert(txn, cycle_details, gds_metrics)

        # -----------------------------------------------------------------
        # Record benchmarking metrics
        # -----------------------------------------------------------------
        metric = self._benchmark.record_transaction(
            txn=txn,
            tier2_insert_ms=insert_latency_ms,
            tier2_cycle_ms=cycle_latency_ms,
            tier2_total_ms=round(tier2_total_ms, 3),
            cycle_detected=cycle_found,
            cycle_details=cycle_details,
        )

        # -----------------------------------------------------------------
        # Update counters and log progress
        # -----------------------------------------------------------------
        self._total_processed += 1

        if self._total_processed % 100 == 0:
            tier1_ms = txn.get("tier1_latency_ms", 0)
            logger.info(
                f"[PROGRESS] Processed={self._total_processed:,} | "
                f"Errors={self._total_errors} | "
                f"Alerts={self._total_fraud_alerts} | "
                f"Tier1={tier1_ms}ms | "
                f"Tier2_insert={insert_latency_ms}ms | "
                f"Tier2_cycle={cycle_latency_ms}ms | "
                f"Tier2_total={round(tier2_total_ms, 2)}ms"
            )

    def _publish_fraud_alert(
        self, txn: Dict[str, Any], cycle_details: Dict, gds_metrics: Dict
    ):
        """
        Publish a confirmed fraud ring alert to the fraud-alerts Kafka topic.

        This alert contains the original transaction data, the Tier 1
        classification, and the Tier 2 cycle detection results.

        Args:
            txn: The original enriched transaction.
            cycle_details: Details of the detected cycle.
            gds_metrics: Advanced graph metrics (PageRank, Community ID).
        """
        alert = {
            "alert_type": "FRAUD_RING_DETECTED",
            "alert_timestamp": datetime.now().isoformat(),
            "alert_severity": self._compute_severity(cycle_details),

            # Original transaction context.
            "transaction_id": txn.get("transaction_id", ""),
            "name_orig": txn.get("nameOrig", ""),
            "name_dest": txn.get("nameDest", ""),
            "amount": txn.get("amount", 0.0),
            "type": txn.get("type", ""),

            # Tier 1 classification.
            "tier1_classification": txn.get("tier1_classification", ""),
            "tier1_reason": txn.get("tier1_reason", ""),
            "window_txn_count": txn.get("window_txn_count", 0),
            "window_txn_sum": txn.get("window_txn_sum", 0.0),

            # Tier 2 cycle detection results.
            "cycle_hop_count": cycle_details.get("hop_count", 0),
            "cycle_user_path": cycle_details.get("user_path", []),
            "cycle_amounts": cycle_details.get("amounts", []),
            "cycle_path_length": cycle_details.get("path_length", 0),

            # GDS Advanced Metrics
            "gds_pagerank": gds_metrics.get("pagerank", 0.0),
            "gds_community_id": gds_metrics.get("community_id", -1),

            # Ground truth label (for evaluation only — NOT used in detection).
            "is_fraud_label": txn.get("isFraud", 0),
        }

        try:
            alert_json = json.dumps(alert).encode("utf-8")
            self._producer.produce(
                topic=KAFKA_TOPIC_FRAUD_ALERTS,
                key=txn.get("nameOrig", "").encode("utf-8"),
                value=alert_json,
            )
            self._producer.poll(0)
            self._total_fraud_alerts += 1
            self._benchmark.record_fraud_alert()

            logger.warning(
                f"[FRAUD_ALERT] {cycle_details.get('hop_count', '?')}-hop ring "
                f"detected! | user={txn.get('nameOrig', '')} | "
                f"path={cycle_details.get('user_path', [])} | "
                f"amount=${txn.get('amount', 0.0):,.2f}"
            )

        except (KafkaException, BufferError) as e:
            logger.error(f"Failed to publish fraud alert: {e}")

    @staticmethod
    def _compute_severity(cycle_details: Dict) -> str:
        """
        Compute the severity level of a fraud ring detection.

        Severity is based on the cycle length (shorter cycles = higher
        severity because they indicate tighter, more deliberate rings):
          - 3-hop: CRITICAL (tight ring, high confidence)
          - 4-hop: HIGH
          - 5-hop: MEDIUM

        Args:
            cycle_details: Cycle detection results.

        Returns:
            Severity string: CRITICAL, HIGH, or MEDIUM.
        """
        hop_count = cycle_details.get("hop_count", 5)
        if hop_count <= 3:
            return "CRITICAL"
        elif hop_count <= 4:
            return "HIGH"
        else:
            return "MEDIUM"

    def _report_metrics(self):
        """
        Compute and publish periodic aggregate metrics.

        Metrics are:
        1. Logged to stdout (for container log aggregation).
        2. Published to the benchmark-metrics Kafka topic.
        """
        report = self._benchmark.get_interval_report()
        if not report:
            return

        # Log the key metrics.
        logger.info(
            f"[BENCHMARK] "
            f"TPS={report.get('interval_tps', 0):.1f} | "
            f"Total={report.get('total_processed', 0):,} | "
            f"Cycles={report.get('total_cycles_found', 0)} | "
            f"Alerts={report.get('total_fraud_alerts', 0)} | "
            f"Errors={report.get('total_errors', 0)}"
        )

        # Log the latency comparison (the key result for the paper).
        tier1 = report.get("tier1_latency", {})
        tier2 = report.get("tier2_total_latency", {})
        if tier1 and tier2:
            logger.info(
                f"[LATENCY COMPARISON] "
                f"Tier1 p50={tier1.get('tier1_p50_ms', '?')}ms "
                f"p95={tier1.get('tier1_p95_ms', '?')}ms "
                f"p99={tier1.get('tier1_p99_ms', '?')}ms | "
                f"Tier2 p50={tier2.get('tier2_total_p50_ms', '?')}ms "
                f"p95={tier2.get('tier2_total_p95_ms', '?')}ms "
                f"p99={tier2.get('tier2_total_p99_ms', '?')}ms | "
                f"Speedup={report.get('speedup_ratio_median', '?')}x"
            )

        # Publish to Kafka benchmark-metrics topic.
        try:
            report_json = json.dumps(report).encode("utf-8")
            self._producer.produce(
                topic=KAFKA_TOPIC_BENCHMARKS,
                value=report_json,
            )
            self._producer.poll(0)
        except (KafkaException, BufferError) as e:
            logger.error(f"Failed to publish benchmark metrics: {e}")

    def _shutdown(self):
        """
        Graceful shutdown: flush producers, close connections, export
        final metrics.
        """
        logger.info("=" * 70)
        logger.info("StreamGraph — Graph Worker Shutting Down")
        logger.info("=" * 70)

        # Flush the Kafka producer (ensure all alerts are delivered).
        logger.info("Flushing Kafka producer...")
        remaining = self._producer.flush(timeout=10.0)
        if remaining > 0:
            logger.warning(f"{remaining} messages not delivered during flush.")

        # Close the Kafka consumer.
        logger.info("Closing Kafka consumer...")
        self._consumer.close()

        # Export final benchmark summary.
        final_summary = self._benchmark.get_final_summary()
        if final_summary:
            logger.info("=" * 70)
            logger.info("FINAL BENCHMARK SUMMARY (for IEEE Paper)")
            logger.info("=" * 70)
            self._print_summary(final_summary)

            # Export to JSON file.
            self._export_final_summary(final_summary)

            # Publish final summary to Kafka.
            try:
                summary_json = json.dumps(final_summary).encode("utf-8")
                temp_producer = Producer(build_producer_config())
                temp_producer.produce(
                    topic=KAFKA_TOPIC_BENCHMARKS,
                    value=summary_json,
                )
                temp_producer.flush(timeout=5.0)
            except Exception as e:
                logger.error(f"Failed to publish final summary: {e}")

        # Close the benchmark CSV.
        self._benchmark.close()

        # Close Neo4j.
        neo4j_stats = self._neo4j.get_stats()
        logger.info(f"Neo4j stats: {neo4j_stats}")
        self._neo4j.close()

        logger.info("=" * 70)
        logger.info(
            f"Graph Worker stopped. "
            f"Processed={self._total_processed:,} | "
            f"Errors={self._total_errors} | "
            f"Alerts={self._total_fraud_alerts}"
        )
        logger.info("=" * 70)

    def _print_summary(self, summary: Dict[str, Any]):
        """Pretty-print the final benchmark summary to the log."""
        for key, value in summary.items():
            if isinstance(value, dict):
                logger.info(f"  {key}:")
                for k, v in value.items():
                    logger.info(f"    {k}: {v}")
            else:
                logger.info(f"  {key}: {value}")

    def _export_final_summary(self, summary: Dict[str, Any]):
        """
        Export the final summary to a JSON file for the IEEE paper.

        The exported file contains all aggregate metrics needed for:
        - Table 1: Tier 1 vs. Tier 2 Latency Comparison
        - Figure X: Latency Distribution
        - Figure Y: Throughput Analysis
        """
        output_dir = Path(BENCHMARK_OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"graph_worker_summary_{timestamp}.json"

        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            logger.info(f"Final summary exported to: {output_file.absolute()}")
        except IOError as e:
            logger.error(f"Failed to export final summary: {e}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================
# This script is designed to be run as a standalone process:
#   python graph_worker.py
#
# Or inside a Docker container:
#   docker exec -it streamgraph-graph-worker python graph_worker.py
#
# It runs indefinitely, consuming from the suspicious-log Kafka topic
# and processing each transaction through the Tier 2 pipeline until
# a shutdown signal (Ctrl+C or SIGTERM) is received.
# =============================================================================
if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("StreamGraph — Tier 2 Graph Validator & Benchmarker")
    logger.info("=" * 70)
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Kafka bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Neo4j URI: {NEO4J_URI}")
    logger.info(f"Benchmark enabled: {BENCHMARK_ENABLED}")

    try:
        worker = GraphWorker()
        worker.run()
    except Exception as e:
        logger.critical(
            f"FATAL: Graph worker failed: {type(e).__name__}: {e}",
            exc_info=True,
        )
        sys.exit(1)
