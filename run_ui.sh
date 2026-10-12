#!/usr/bin/env bash
# =============================================================================
# StreamGraph — 1-Click Visual Dashboard Launcher (Linux/macOS)
# =============================================================================

set -e
cd "$(dirname "$0")"

echo "====================================================================="
echo "  StreamGraph - Real-Time Fraud Ring Detection Visual Dashboard"
echo "  IEEE Research Project: Dual-Tier Architecture (Flink x Neo4j)"
echo "====================================================================="
echo ""

if ! command -v python3 &> /dev/null; then
    echo "ERROR: python3 is not installed or not in PATH."
    exit 1
fi

if [ ! -f ".venv/bin/python" ]; then
    echo "[SETUP] Creating Python virtual environment..."
    python3 -m venv .venv
    echo "[SETUP] Installing dependencies..."
    ./.venv/bin/pip install -r requirements.txt
fi

echo "[START] Launching StreamGraph Visual Dashboard on http://localhost:5000..."

# Try to open default browser in background
if command -v xdg-open &> /dev/null; then
    (sleep 2 && xdg-open http://localhost:5000) &
elif command -v open &> /dev/null; then
    (sleep 2 && open http://localhost:5000) &
fi

./.venv/bin/python src/web_ui.py --port 5000
