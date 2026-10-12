# =============================================================================
# StreamGraph — Source Package
# =============================================================================
# IEEE Research Project: Dual-Tier Database Architecture for Real-Time
# Financial Fraud Ring Detection
#
# Components:
#   producer.py          — Phase 2: PaySim CSV → Kafka ingestion
#   stream_processor.py  — Phase 3: PyFlink + RocksDB (Tier 1)
#   graph_worker.py      — Phase 4: Neo4j graph validation (Tier 2)
#   dashboard.py         — Phase 5: Real-time monitoring dashboard
#   benchmark_analysis.py — Benchmark analysis & chart generation
# =============================================================================

__version__ = "1.0.0"
__project__ = "StreamGraph"
__authors__ = "Yashas Vishwakarma"
