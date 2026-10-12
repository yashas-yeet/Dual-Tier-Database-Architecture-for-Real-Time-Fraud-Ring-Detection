# =============================================================================
# StreamGraph — Tests for Tier 2 Graph Worker & Cycle Detection
# =============================================================================

import os
import shutil
import unittest
from pathlib import Path

from src.graph_worker import (
    GraphWorker,
    BenchmarkCollector,
    CYPHER_DETECT_CYCLE_3HOP,
    CYPHER_DETECT_CYCLE_4HOP,
    CYPHER_DETECT_CYCLE_5HOP,
    CYPHER_PAGE_RANK,
    CYPHER_LOUVAIN,
)


class TestGraphWorkerLogic(unittest.TestCase):
    """Test GraphWorker helper methods and Cypher query properties."""

    def test_compute_severity(self):
        # 3-hop is CRITICAL
        self.assertEqual(
            GraphWorker._compute_severity({"hop_count": 3}),
            "CRITICAL",
        )
        # <= 3 is CRITICAL
        self.assertEqual(
            GraphWorker._compute_severity({"hop_count": 2}),
            "CRITICAL",
        )
        # 4-hop is HIGH
        self.assertEqual(
            GraphWorker._compute_severity({"hop_count": 4}),
            "HIGH",
        )
        # >= 5-hop is MEDIUM
        self.assertEqual(
            GraphWorker._compute_severity({"hop_count": 5}),
            "MEDIUM",
        )
        self.assertEqual(
            GraphWorker._compute_severity({"hop_count": 6}),
            "MEDIUM",
        )

    def test_cypher_queries_structure(self):
        """Verify Cypher queries include required graph patterns and return fields."""
        for query, expected_hops in [
            (CYPHER_DETECT_CYCLE_3HOP, 3),
            (CYPHER_DETECT_CYCLE_4HOP, 4),
            (CYPHER_DETECT_CYCLE_5HOP, 5),
        ]:
            self.assertIn("$nameOrig", query)
            self.assertIn(":SENT", query)
            self.assertIn(":RECEIVED", query)
            self.assertIn("userPath", query)
            self.assertIn("hopCount", query)
            self.assertIn(f"{expected_hops} AS hopCount", query)

    def test_gds_algorithm_queries(self):
        """Verify GDS PageRank and Louvain queries."""
        self.assertIn("gds.pageRank.stream", CYPHER_PAGE_RANK)
        self.assertIn("gds.louvain.stream", CYPHER_LOUVAIN)
        self.assertIn("$nameOrig", CYPHER_PAGE_RANK)
        self.assertIn("$nameOrig", CYPHER_LOUVAIN)


class TestBenchmarkCollector(unittest.TestCase):
    """Test BenchmarkCollector per-transaction logging and summaries."""

    def setUp(self):
        self.test_dir = Path("./benchmarks/test_tmp")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.collector = BenchmarkCollector(
            output_dir=str(self.test_dir),
            report_interval_sec=5,
            enabled=True,
        )

    def tearDown(self):
        self.collector.close()
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_record_transaction_and_export(self):
        sample_txn = {
            "transaction_id": "TX_TEST_001",
            "nameOrig": "C10001",
            "nameDest": "M20001",
            "amount": 2500.0,
            "isFraud": 0,
            "tier1_classification": "SUSPICIOUS",
            "tier1_latency_ms": 1.25,
            "tier1_state_latency_ms": 0.85,
            "tier1_e2e_latency_ms": 2.50,
        }

        entry = self.collector.record_transaction(
            txn=sample_txn,
            tier2_insert_ms=8.5,
            tier2_cycle_ms=15.0,
            tier2_total_ms=23.5,
            cycle_detected=True,
            cycle_details={"hop_count": 3, "user_path": ["C10001", "C10002", "C10001"]},
        )

        self.assertEqual(entry["transaction_id"], "TX_TEST_001")
        self.assertEqual(entry["cycle_detected"], True)
        self.assertEqual(entry["cycle_hop_count"], 3)
        self.assertGreater(entry["tier2_vs_tier1_ratio"], 1.0)

        # Check summary
        summary = self.collector.get_final_summary()
        self.assertIsNotNone(summary)
        self.assertEqual(summary["total_processed"], 1)
        self.assertEqual(summary["total_cycles_found"], 1)


if __name__ == "__main__":
    unittest.main()
