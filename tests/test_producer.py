# =============================================================================
# StreamGraph — Tests for Producer (PaySim Parser, Synthetic Gen, Rate Limiter)
# =============================================================================

import os
import unittest
from pathlib import Path

from src.producer import (
    ProducerMetrics,
    TokenBucketRateLimiter,
    PaySimParser,
    SyntheticPaySimGenerator,
    build_producer_config,
)


class TestProducerMetrics(unittest.TestCase):
    """Test ProducerMetrics counters, latency recording, and summaries."""

    def setUp(self):
        self.metrics = ProducerMetrics()

    def test_record_send(self):
        self.metrics.record_send(is_fraud=False, txn_type="PAYMENT")
        self.metrics.record_send(is_fraud=True, txn_type="TRANSFER")
        self.assertEqual(self.metrics.total_sent, 2)
        self.assertEqual(self.metrics.total_legitimate, 1)
        self.assertEqual(self.metrics.total_fraud, 1)
        self.assertEqual(self.metrics.type_counts["PAYMENT"], 1)
        self.assertEqual(self.metrics.type_counts["TRANSFER"], 1)

    def test_record_delivery(self):
        self.metrics.record_delivery(5.2)
        self.metrics.record_delivery(10.8)
        self.assertEqual(self.metrics.total_delivered, 2)
        self.assertEqual(len(self.metrics.delivery_latencies_ms), 2)

    def test_record_error(self):
        self.metrics.record_error()
        self.assertEqual(self.metrics.total_errors, 1)

    def test_final_summary_calculation(self):
        for i in range(10):
            self.metrics.record_send(is_fraud=(i % 5 == 0), txn_type="CASH_OUT")
            self.metrics.record_delivery(latency_ms=float(i + 1))
        summary = self.metrics.get_final_summary()
        self.assertEqual(summary["total_messages_sent"], 10)
        self.assertEqual(summary["total_messages_delivered"], 10)
        self.assertEqual(summary["total_delivery_errors"], 0)
        self.assertEqual(summary["fraud_messages"], 2)
        self.assertEqual(summary["legitimate_messages"], 8)
        self.assertIn("latency_p50_ms", summary)
        self.assertIn("overall_throughput_tps", summary)


class TestTokenBucketRateLimiter(unittest.TestCase):
    """Test the TokenBucketRateLimiter algorithm."""

    def test_unlimited_rate_limiter(self):
        limiter = TokenBucketRateLimiter(target_tps=0)
        self.assertTrue(limiter.unlimited)
        # Calling acquire should return immediately
        limiter.acquire()

    def test_limited_rate_limiter_tokens(self):
        limiter = TokenBucketRateLimiter(target_tps=1000, burst_size=50)
        self.assertFalse(limiter.unlimited)
        self.assertEqual(limiter.burst_size, 50)
        self.assertEqual(limiter.tokens, 50)
        limiter.acquire()
        self.assertLessEqual(limiter.tokens, 50)


class TestPaySimParser(unittest.TestCase):
    """Test PaySimParser with sample CSV dataset."""

    def setUp(self):
        self.sample_csv = Path("data/paysim_sample.csv")

    def test_parser_sample_file(self):
        if not self.sample_csv.exists():
            self.skipTest("Sample CSV not found")
        parser = PaySimParser(str(self.sample_csv))
        self.assertGreater(parser.total_rows, 0)

        transactions = []
        for txn in parser.iterate_transactions():
            transactions.append(txn)
            if len(transactions) >= 5:
                break

        self.assertEqual(len(transactions), 5)
        for txn in transactions:
            self.assertIn("transaction_id", txn)
            self.assertIn("step", txn)
            self.assertIn("type", txn)
            self.assertIn("amount", txn)
            self.assertIn("nameOrig", txn)
            self.assertIn("nameDest", txn)
            self.assertIn("isFraud", txn)
            self.assertIsInstance(txn["amount"], float)
            self.assertIsInstance(txn["step"], int)

    def test_parser_looping(self):
        if not self.sample_csv.exists():
            self.skipTest("Sample CSV not found")
        parser = PaySimParser(str(self.sample_csv))
        looped_count = 0
        for txn in parser.iterate_transactions(loop=True):
            looped_count += 1
            if looped_count >= 15:
                break
        self.assertEqual(looped_count, 15)


class TestSyntheticPaySimGenerator(unittest.TestCase):
    """Test SyntheticPaySimGenerator properties."""

    def test_generator_deterministic(self):
        gen = SyntheticPaySimGenerator(
            num_transactions=100,
            num_accounts=50,
            fraud_rate=0.05,
            seed=42,
        )
        txns = list(gen.generate())
        self.assertEqual(len(txns), 100)

        # Check fields
        for txn in txns:
            self.assertIn("transaction_id", txn)
            self.assertIn("amount", txn)
            self.assertIn("nameOrig", txn)
            self.assertIn("nameDest", txn)
            self.assertIn("isFraud", txn)
            self.assertGreater(txn["amount"], 0)

        # Verify fraud rings were generated
        self.assertGreater(len(gen.fraud_rings), 0)
        for ring in gen.fraud_rings:
            self.assertGreaterEqual(len(ring), 3)


class TestProducerConfig(unittest.TestCase):
    """Test Kafka producer configuration builder."""

    def test_build_config(self):
        cfg = build_producer_config("localhost:9092")
        self.assertEqual(cfg["bootstrap.servers"], "localhost:9092")
        self.assertEqual(cfg["acks"], "all")
        self.assertTrue(cfg["enable.idempotence"])
        self.assertEqual(cfg["compression.type"], "lz4")


if __name__ == "__main__":
    unittest.main()
