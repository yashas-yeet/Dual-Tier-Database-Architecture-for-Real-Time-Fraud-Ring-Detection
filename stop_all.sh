#!/usr/bin/env bash
# =============================================================================
# StreamGraph — Stop All Services for Linux / macOS
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==========================================="
echo " StreamGraph - Stopping All Services"
echo "==========================================="
echo ""

echo "[1/2] Stopping Python processes..."
pkill -f "src/graph_worker.py" 2>/dev/null || true
pkill -f "src/stream_processor.py" 2>/dev/null || true
pkill -f "src/producer.py" 2>/dev/null || true

echo "[2/2] Stopping Docker infrastructure..."
docker compose down

echo ""
echo "==========================================="
echo " All services stopped."
echo "==========================================="
