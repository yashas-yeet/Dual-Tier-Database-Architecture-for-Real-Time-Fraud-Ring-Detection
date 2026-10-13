import unittest
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.web_ui import StreamState, app


class TestWebUIGraphFiltering(unittest.TestCase):
    def setUp(self):
        self.state = StreamState()
        self.app = app.test_client()

    def test_normal_transactions_not_added_to_graph(self):
        """Verify that normal non-fraud transactions do NOT populate graph nodes/links."""
        normal_txn = {
            "transaction_id": "TXN_NORMAL_1",
            "nameOrig": "C10001",
            "nameDest": "C10002",
            "amount": 250.0,
            "type": "PAYMENT",
            "is_fraud_ring": False,
        }
        self.state.record_raw_transaction(normal_txn)

        self.assertEqual(len(self.state.nodes), 0, "Normal transaction should not add nodes to graph")
        self.assertEqual(len(self.state.links), 0, "Normal transaction should not add links to graph")

    def test_fraud_ring_transaction_added_to_graph(self):
        """Verify that transactions marked as is_fraud_ring ARE added to graph."""
        ring_txn = {
            "transaction_id": "TXN_RING_1",
            "nameOrig": "C9991",
            "nameDest": "C9992",
            "amount": 35000.0,
            "type": "TRANSFER",
            "is_fraud_ring": True,
        }
        self.state.record_raw_transaction(ring_txn)

        self.assertEqual(len(self.state.nodes), 2)
        self.assertTrue(self.state.nodes["C9991"]["is_ring"])
        self.assertTrue(self.state.nodes["C9992"]["is_ring"])
        self.assertEqual(len(self.state.links), 1)
        self.assertTrue(self.state.links[0]["is_ring"])

    def test_fraud_alert_creates_complete_cycle(self):
        """Verify that incoming fraud alert creates all nodes and cyclic edges."""
        alert = {
            "alert_id": "RING-3H-TEST",
            "cycle_nodes": ["C101", "C102", "C103"],
            "cycle_hop_count": 3,
            "amount": 42000.0,
            "alert_severity": "CRITICAL",
        }
        self.state.record_fraud_alert(alert)

        self.assertEqual(len(self.state.nodes), 3)
        for nid in ["C101", "C102", "C103"]:
            self.assertIn(nid, self.state.nodes)
            self.assertTrue(self.state.nodes[nid]["is_ring"])
            self.assertEqual(self.state.nodes[nid]["ring_id"], "RING-3H-TEST")

        self.assertEqual(len(self.state.links), 3)
        targets = [l["target"] for l in self.state.links]
        self.assertIn("C102", targets)
        self.assertIn("C103", targets)
        self.assertIn("C101", targets)

    def test_subscriber_queue_overflow_keeps_subscriber_alive(self):
        """Verify that subscriber queues are not dropped when full; ring buffer retains connection."""
        q = self.state.subscribe()
        self.assertIn(q, self.state.subscribers)

        # Broadcast more than queue capacity
        for i in range(2100):
            self.state.broadcast("test_event", {"idx": i})

        # Subscriber must still be registered and alive
        self.assertIn(q, self.state.subscribers)
        self.assertEqual(q.qsize(), 2000)

        # Unsubscribe works as expected
        self.state.unsubscribe(q)
        self.assertNotIn(q, self.state.subscribers)


if __name__ == "__main__":
    unittest.main()
