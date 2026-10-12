# =============================================================================
# StreamGraph — Phase 5: Real-Time Monitoring Dashboard
# =============================================================================
# IEEE Research Project: Dual-Tier Database Architecture for Real-Time
# Financial Fraud Ring Detection
#
# PURPOSE:
# Provides a live terminal-based dashboard for monitoring the StreamGraph
# pipeline during execution. Consumes metrics from the benchmark-metrics
# Kafka topic and displays real-time statistics including:
#   - Transaction throughput (TPS)
#   - Tier 1 vs. Tier 2 latency comparison
#   - Fraud ring detection alerts
#   - System health indicators
#
# USAGE:
#   python src/dashboard.py
#   python src/dashboard.py --refresh 5
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
import signal
import logging
import argparse
from datetime import datetime
from typing import Dict, Any, Optional, List
from collections import deque

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
KAFKA_TOPIC_BENCHMARKS = os.getenv("KAFKA_TOPIC_BENCHMARKS", "benchmark-metrics")
KAFKA_TOPIC_FRAUD_ALERTS = os.getenv("KAFKA_TOPIC_FRAUD_ALERTS", "fraud-alerts")

# ANSI color codes for terminal output.
class Colors:
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    DIM = "\033[2m"
    RESET = "\033[0m"


# =============================================================================
# DASHBOARD
# =============================================================================
class StreamGraphDashboard:
    """
    Real-time terminal dashboard for monitoring the StreamGraph pipeline.

    Displays:
    - Current throughput (TPS) for Tier 2 processing.
    - Latency comparison: Tier 1 (Flink/RocksDB) vs. Tier 2 (Neo4j).
    - Fraud ring detection count and recent alerts.
    - System uptime and total transactions processed.
    """

    def __init__(self, refresh_interval: int = 3):
        self.refresh_interval = refresh_interval
        self._shutdown = False
        self._last_report: Optional[Dict] = None
        self._fraud_alerts: deque = deque(maxlen=10)  # Keep last 10 alerts
        self._start_time = time.monotonic()

        # Kafka consumers.
        self._metrics_consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "streamgraph-dashboard-metrics",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        self._metrics_consumer.subscribe([KAFKA_TOPIC_BENCHMARKS])

        self._alerts_consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "streamgraph-dashboard-alerts",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        self._alerts_consumer.subscribe([KAFKA_TOPIC_FRAUD_ALERTS])

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        self._shutdown = True

    def run(self):
        """Main dashboard loop."""
        print(f"\n{Colors.BOLD}{Colors.CYAN}StreamGraph Dashboard{Colors.RESET}")
        print(f"{Colors.DIM}Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...{Colors.RESET}")
        print(f"{Colors.DIM}Press Ctrl+C to exit.{Colors.RESET}\n")

        while not self._shutdown:
            # Poll for new metrics.
            self._poll_metrics()
            self._poll_alerts()

            # Render the dashboard.
            self._render()

            # Wait for next refresh.
            time.sleep(self.refresh_interval)

        self._cleanup()

    def _poll_metrics(self):
        """Poll the benchmark-metrics topic for the latest report."""
        while True:
            msg = self._metrics_consumer.poll(timeout=0.1)
            if msg is None:
                break
            if msg.error():
                continue
            try:
                report = json.loads(msg.value().decode("utf-8"))
                self._last_report = report
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

    def _poll_alerts(self):
        """Poll the fraud-alerts topic for new alerts."""
        while True:
            msg = self._alerts_consumer.poll(timeout=0.1)
            if msg is None:
                break
            if msg.error():
                continue
            try:
                alert = json.loads(msg.value().decode("utf-8"))
                self._fraud_alerts.append(alert)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

    def _render(self):
        """Render the dashboard to the terminal."""
        # Clear screen.
        os.system("cls" if os.name == "nt" else "clear")

        uptime = time.monotonic() - self._start_time
        uptime_str = f"{int(uptime // 3600):02d}:{int((uptime % 3600) // 60):02d}:{int(uptime % 60):02d}"

        print(f"{Colors.BOLD}{Colors.CYAN}")
        print("╔══════════════════════════════════════════════════════════════════════╗")
        print("║             StreamGraph — Real-Time Fraud Detection Monitor         ║")
        print("║     Dual-Tier Database Architecture (Flink/RocksDB + Neo4j)         ║")
        print("╚══════════════════════════════════════════════════════════════════════╝")
        print(f"{Colors.RESET}")

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"  {Colors.DIM}Time: {now} | Uptime: {uptime_str} | Refresh: {self.refresh_interval}s{Colors.RESET}")
        print()

        if self._last_report is None:
            print(f"  {Colors.YELLOW}⏳ Waiting for metrics data...{Colors.RESET}")
            print(f"  {Colors.DIM}Ensure the pipeline is running (run_all.bat){Colors.RESET}")
            return

        report = self._last_report

        # ---- Throughput Section ----
        tps = report.get("interval_tps", 0)
        total = report.get("total_processed", 0)
        print(f"  {Colors.BOLD}── THROUGHPUT ──{Colors.RESET}")
        tps_bar = self._bar(tps, 100, 40)
        print(f"  TPS:       {Colors.GREEN}{tps:>8.1f}{Colors.RESET}  {tps_bar}")
        print(f"  Total:     {total:>8,} transactions processed")
        print()

        # ---- Latency Section ----
        print(f"  {Colors.BOLD}── LATENCY COMPARISON (ms) ──{Colors.RESET}")
        print(f"  {'':>22} {'p50':>8} {'p95':>8} {'p99':>8} {'mean':>8}")
        print(f"  {'─' * 55}")

        tier1 = report.get("tier1_latency", {})
        tier2 = report.get("tier2_total_latency", {})
        tier2_insert = report.get("tier2_insert_latency", {})
        tier2_cycle = report.get("tier2_cycle_latency", {})

        if tier1:
            self._print_latency_row("Tier 1 (Flink)", tier1, "tier1", Colors.BLUE)
        if tier2_insert:
            self._print_latency_row("Tier 2: Insert", tier2_insert, "tier2_insert", Colors.YELLOW)
        if tier2_cycle:
            self._print_latency_row("Tier 2: Cycle", tier2_cycle, "tier2_cycle", Colors.RED)
        if tier2:
            self._print_latency_row("Tier 2: Total", tier2, "tier2_total", Colors.RED)

        speedup = report.get("speedup_ratio_median", 0)
        if speedup > 0:
            print(f"\n  {Colors.BOLD}Speedup Ratio:{Colors.RESET} "
                  f"Tier 2 is {Colors.YELLOW}{speedup:.1f}x{Colors.RESET} more expensive than Tier 1")
        print()

        # ---- Detection Section ----
        cycles = report.get("total_cycles_found", 0)
        alerts = report.get("total_fraud_alerts", 0)
        errors = report.get("total_errors", 0)
        cycle_rate = report.get("cycle_detection_rate", "0%")

        print(f"  {Colors.BOLD}── FRAUD DETECTION ──{Colors.RESET}")
        print(f"  Cycles Found:   {Colors.RED if cycles > 0 else Colors.GREEN}{cycles:>6}{Colors.RESET}")
        print(f"  Fraud Alerts:   {Colors.RED if alerts > 0 else Colors.GREEN}{alerts:>6}{Colors.RESET}")
        print(f"  Detection Rate: {cycle_rate:>6}")
        print(f"  Errors:         {Colors.RED if errors > 0 else Colors.GREEN}{errors:>6}{Colors.RESET}")
        print()

        # ---- Recent Alerts ----
        if self._fraud_alerts:
            print(f"  {Colors.BOLD}{Colors.RED}── RECENT FRAUD ALERTS ──{Colors.RESET}")
            for alert in list(self._fraud_alerts)[-5:]:
                severity = alert.get("alert_severity", "?")
                hop_count = alert.get("cycle_hop_count", "?")
                user = alert.get("name_orig", "?")
                amount = alert.get("amount", 0)
                severity_color = {
                    "CRITICAL": Colors.RED,
                    "HIGH": Colors.YELLOW,
                    "MEDIUM": Colors.CYAN,
                }.get(severity, Colors.RESET)
                print(
                    f"  {severity_color}[{severity}]{Colors.RESET} "
                    f"{hop_count}-hop ring | "
                    f"User: {user} | "
                    f"Amount: ${amount:,.2f}"
                )
            print()

        print(f"  {Colors.DIM}Press Ctrl+C to exit | "
              f"Flink: http://localhost:8081 | "
              f"Neo4j: http://localhost:7474{Colors.RESET}")

    def _print_latency_row(self, label: str, stats: Dict, prefix: str, color: str):
        """Print a single row in the latency table."""
        p50 = stats.get(f"{prefix}_p50_ms", 0)
        p95 = stats.get(f"{prefix}_p95_ms", 0)
        p99 = stats.get(f"{prefix}_p99_ms", 0)
        mean = stats.get(f"{prefix}_mean_ms", 0)
        print(f"  {color}{label:<22}{Colors.RESET} {p50:>7.2f}  {p95:>7.2f}  {p99:>7.2f}  {mean:>7.2f}")

    @staticmethod
    def _bar(value: float, max_val: float, width: int = 40) -> str:
        """Create an ASCII progress bar."""
        filled = int(min(value / max_val, 1.0) * width)
        bar = "█" * filled + "░" * (width - filled)
        return f"[{bar}]"

    def _cleanup(self):
        """Clean up resources."""
        print(f"\n{Colors.YELLOW}Shutting down dashboard...{Colors.RESET}")
        self._metrics_consumer.close()
        self._alerts_consumer.close()
        print(f"{Colors.GREEN}Dashboard stopped.{Colors.RESET}")


# =============================================================================
# CLI
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="StreamGraph — Real-Time Monitoring Dashboard"
    )
    parser.add_argument(
        "--refresh", "-r",
        type=int,
        default=3,
        help="Dashboard refresh interval in seconds (default: 3).",
    )
    args = parser.parse_args()

    dashboard = StreamGraphDashboard(refresh_interval=args.refresh)
    dashboard.run()


if __name__ == "__main__":
    main()
