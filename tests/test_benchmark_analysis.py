# =============================================================================
# StreamGraph — Tests for Benchmark Analysis & IEEE Visualization
# =============================================================================

import os
import shutil
import unittest
from pathlib import Path

from src.benchmark_analysis import (
    StatsCalculator,
    BenchmarkDataLoader,
    SummaryTableGenerator,
    generate_sample_benchmark_data,
)


class TestStatsCalculator(unittest.TestCase):
    """Test percentile and statistical summary calculations."""

    def test_compute_percentiles_empty(self):
        res = StatsCalculator.compute_percentiles([])
        self.assertEqual(res, {})

    def test_compute_percentiles_values(self):
        vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        stats = StatsCalculator.compute_percentiles(vals)
        self.assertEqual(stats["count"], 10)
        self.assertEqual(stats["min"], 1.0)
        self.assertEqual(stats["max"], 10.0)
        self.assertEqual(stats["mean"], 5.5)
        self.assertEqual(stats["median"], 6.0)
        self.assertIn("p95", stats)
        self.assertIn("p99", stats)
        self.assertIn("stddev", stats)

    def test_compute_throughput(self):
        records = [
            {"timestamp": "2026-01-01T12:00:00.123"},
            {"timestamp": "2026-01-01T12:00:00.456"},
            {"timestamp": "2026-01-01T12:00:01.100"},
            {"timestamp": "2026-01-01T12:00:01.200"},
            {"timestamp": "2026-01-01T12:00:01.300"},
        ]
        tps_points = StatsCalculator.compute_throughput(records)
        self.assertEqual(len(tps_points), 2)
        # Second 0 has 2 transactions, Second 1 has 3 transactions
        self.assertEqual(tps_points[0][1], 2)
        self.assertEqual(tps_points[1][1], 3)


class TestSummaryTableGenerator(unittest.TestCase):
    """Test generation of the summary table for the IEEE paper."""

    def setUp(self):
        self.test_dir = Path("./benchmarks/test_table_tmp")
        self.test_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_summary_table_generation(self):
        sample_records = [
            {
                "tier1_latency_ms": 1.2,
                "tier2_insert_latency_ms": 8.5,
                "tier2_cycle_latency_ms": 15.0,
                "tier2_total_latency_ms": 23.5,
                "e2e_total_latency_ms": 27.0,
                "tier2_vs_tier1_ratio": 19.5,
            },
            {
                "tier1_latency_ms": 1.4,
                "tier2_insert_latency_ms": 9.0,
                "tier2_cycle_latency_ms": 16.0,
                "tier2_total_latency_ms": 25.0,
                "e2e_total_latency_ms": 28.5,
                "tier2_vs_tier1_ratio": 17.8,
            },
        ]

        table_gen = SummaryTableGenerator()
        csv_path = table_gen.generate(sample_records, output_dir=str(self.test_dir))
        self.assertTrue(Path(csv_path).exists())

        # Verify summary_report.json also created
        json_path = self.test_dir / "summary_report.json"
        self.assertTrue(json_path.exists())


class TestSampleDataGenerator(unittest.TestCase):
    """Test synthetic sample benchmark generator."""

    def setUp(self):
        self.test_dir = Path("./benchmarks/test_sample_tmp")
        self.test_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_generate_sample_benchmark_data(self):
        csv_file = generate_sample_benchmark_data(str(self.test_dir), num_records=50)
        self.assertTrue(Path(csv_file).exists())

        loader = BenchmarkDataLoader(str(self.test_dir))
        records = loader.load_tier2_metrics(csv_file)
        self.assertEqual(len(records), 50)
        self.assertIn("tier1_latency_ms", records[0])
        self.assertIn("tier2_total_latency_ms", records[0])
        self.assertIn("cycle_detected", records[0])


if __name__ == "__main__":
    unittest.main()
