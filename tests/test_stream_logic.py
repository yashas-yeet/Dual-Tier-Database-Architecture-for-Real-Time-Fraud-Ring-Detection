# =============================================================================
# StreamGraph — Tests for Stream Processing & Tier 1 Velocity Window Rules
# =============================================================================

import json
import unittest
from datetime import datetime, timedelta

from src.stream_processor import (
    TransactionTimestampAssigner,
    VELOCITY_WINDOW_SIZE_MS,
    VELOCITY_MAX_TXN_COUNT,
    VELOCITY_MAX_TOTAL_AMOUNT,
)



class VelocityWindowRuleSimulator:
    """
    In-memory simulation of the KeyedProcessFunction velocity window
    logic to test the correctness of Tier 1 rule evaluation.
    """

    def __init__(
        self,
        window_ms: int = VELOCITY_WINDOW_SIZE_MS,
        max_count: int = VELOCITY_MAX_TXN_COUNT,
        max_amount: float = VELOCITY_MAX_TOTAL_AMOUNT,
    ):
        self.window_ms = window_ms
        self.max_count = max_count
        self.max_amount = max_amount
        # Per-account window entries: list of (timestamp_ms, amount, txn_id)
        self.windows = {}

    def process_transaction(self, txn: dict) -> dict:
        orig = txn["nameOrig"]
        ts = txn.get("producer_timestamp_ms", int(datetime.now().timestamp() * 1000))
        amount = float(txn["amount"])
        txn_id = txn["transaction_id"]

        if orig not in self.windows:
            self.windows[orig] = []

        # Evict expired entries older than (ts - window_ms)
        cutoff = ts - self.window_ms
        self.windows[orig] = [
            entry for entry in self.windows[orig] if entry[0] >= cutoff
        ]

        # Add current transaction
        self.windows[orig].append((ts, amount, txn_id))

        # Calculate metrics
        count = len(self.windows[orig])
        total_amount = sum(e[1] for e in self.windows[orig])

        # Rule evaluation
        is_suspicious = (count > self.max_count) or (total_amount > self.max_amount)
        classification = "SUSPICIOUS" if is_suspicious else "CLEARED"

        return {
            "transaction_id": txn_id,
            "nameOrig": orig,
            "tier1_classification": classification,
            "window_count": count,
            "window_sum": total_amount,
        }


class TestVelocityWindowRules(unittest.TestCase):
    """Test Tier 1 sliding window temporal anomaly detection rules."""

    def setUp(self):
        self.simulator = VelocityWindowRuleSimulator(
            window_ms=15 * 60 * 1000,
            max_count=5,
            max_amount=10000.0,
        )
        self.base_time = int(datetime(2026, 1, 1, 12, 0, 0).timestamp() * 1000)

    def test_normal_transactions_cleared(self):
        """Under threshold transactions should be CLEARED."""
        account = "C_NORMAL_01"
        for i in range(3):
            res = self.simulator.process_transaction({
                "transaction_id": f"TX_{i}",
                "nameOrig": account,
                "amount": 500.0,
                "producer_timestamp_ms": self.base_time + (i * 60000),
            })
            self.assertEqual(res["tier1_classification"], "CLEARED")
            self.assertEqual(res["window_count"], i + 1)
            self.assertEqual(res["window_sum"], (i + 1) * 500.0)

    def test_velocity_count_threshold_exceeded(self):
        """Exceeding 5 transactions within 15 minutes triggers SUSPICIOUS."""
        account = "C_BURST_01"
        for i in range(5):
            res = self.simulator.process_transaction({
                "transaction_id": f"TX_{i}",
                "nameOrig": account,
                "amount": 100.0,
                "producer_timestamp_ms": self.base_time + (i * 10000),
            })
            self.assertEqual(res["tier1_classification"], "CLEARED")

        # 6th transaction triggers threshold
        res = self.simulator.process_transaction({
            "transaction_id": "TX_6",
            "nameOrig": account,
            "amount": 100.0,
            "producer_timestamp_ms": self.base_time + (6 * 10000),
        })
        self.assertEqual(res["tier1_classification"], "SUSPICIOUS")
        self.assertEqual(res["window_count"], 6)

    def test_amount_threshold_exceeded(self):
        """Exceeding $10,000 within 15 minutes triggers SUSPICIOUS."""
        account = "C_WHALE_01"
        res = self.simulator.process_transaction({
            "transaction_id": "TX_LARGE_1",
            "nameOrig": account,
            "amount": 12000.0,
            "producer_timestamp_ms": self.base_time,
        })
        self.assertEqual(res["tier1_classification"], "SUSPICIOUS")
        self.assertEqual(res["window_sum"], 12000.0)

    def test_window_eviction_restores_cleared(self):
        """Old transactions outside the 15-minute window are evicted."""
        account = "C_TIME_01"
        # 5 transactions in first window
        for i in range(5):
            self.simulator.process_transaction({
                "transaction_id": f"TX_OLD_{i}",
                "nameOrig": account,
                "amount": 100.0,
                "producer_timestamp_ms": self.base_time + (i * 10000),
            })

        # Advance time by 20 minutes (past 15-minute window)
        new_time = self.base_time + (20 * 60 * 1000)
        res = self.simulator.process_transaction({
            "transaction_id": "TX_NEW_1",
            "nameOrig": account,
            "amount": 100.0,
            "producer_timestamp_ms": new_time,
        })
        # The 5 old transactions should have expired, count is now 1
        self.assertEqual(res["window_count"], 1)
        self.assertEqual(res["tier1_classification"], "CLEARED")


class TestTimestampAssigner(unittest.TestCase):
    """Test TransactionTimestampAssigner."""

    def setUp(self):
        self.assigner = TransactionTimestampAssigner()

    def test_extract_valid_timestamp(self):
        payload = json.dumps({"producer_timestamp_ms": 1700000000000, "amount": 100})
        ts = self.assigner.extract_timestamp(payload, record_timestamp=0)
        self.assertEqual(ts, 1700000000000)

    def test_fallback_record_timestamp(self):
        payload = "INVALID_JSON"
        ts = self.assigner.extract_timestamp(payload, record_timestamp=123456)
        self.assertEqual(ts, 123456)


if __name__ == "__main__":
    unittest.main()
