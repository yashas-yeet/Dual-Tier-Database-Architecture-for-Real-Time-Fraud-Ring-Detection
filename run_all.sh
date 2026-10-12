#!/usr/bin/env bash
# =============================================================================
# StreamGraph — Pipeline Runner for Linux / macOS
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==========================================="
echo " StreamGraph - Full Pipeline Runner"
echo "==========================================="
echo ""

# 1. Pre-flight checks
if ! command -v python3 &> /dev/null; then
    echo "ERROR: python3 is not installed or not on PATH."
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed or not on PATH."
    exit 1
fi

echo "   Python: OK"
echo "   Docker: OK"
echo ""

if [ ! -f ".env" ] && [ -f ".env.example" ]; then
    echo "[SETUP] Creating .env from .env.example..."
    cp .env.example .env
fi

# 2. Virtual Environment
if [ ! -d ".venv" ]; then
    echo "[SETUP] Creating Python virtual environment..."
    python3 -m venv .venv
fi

echo "[SETUP] Installing dependencies..."
./.venv/bin/pip install -q -r requirements.txt

# 3. Flink Kafka connector JAR
mkdir -p lib
if [ ! -f "lib/flink-sql-connector-kafka-3.1.0-1.18.jar" ]; then
    echo "[SETUP] Downloading Flink Kafka connector JAR..."
    curl -sS -L -o lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
fi

# 4. Start Docker Infrastructure
echo "[1/4] Starting Docker infrastructure (Kafka, Flink, Neo4j)..."
docker compose up -d

echo ""
echo "Waiting for Kafka and Neo4j to become ready..."
ATTEMPTS=0
until docker exec streamgraph-kafka kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null || [ $ATTEMPTS -ge 12 ]; do
    ATTEMPTS=$((ATTEMPTS+1))
    echo "   Waiting for Kafka... ($ATTEMPTS/12)"
    sleep 10
done

ATTEMPTS=0
until docker exec streamgraph-neo4j wget --no-verbose --tries=1 --spider http://localhost:7474 &> /dev/null || [ $ATTEMPTS -ge 12 ]; do
    ATTEMPTS=$((ATTEMPTS+1))
    echo "   Waiting for Neo4j... ($ATTEMPTS/12)"
    sleep 10
done

echo "   Infrastructure is READY."
echo ""

# 5. Start Workers
echo "[2/4] Starting Tier 2: Graph Worker..."
./.venv/bin/python src/graph_worker.py > graph_worker.log 2>&1 &
GRAPH_PID=$!
echo "   Graph Worker started (PID: $GRAPH_PID, logging to graph_worker.log)"
sleep 5

echo "[3/4] Starting Tier 1: Stream Processor..."
./.venv/bin/python src/stream_processor.py > stream_processor.log 2>&1 &
FLINK_PID=$!
echo "   Stream Processor started (PID: $FLINK_PID, logging to stream_processor.log)"
sleep 15

echo "[4/4] Starting Data Producer..."
if [ -f "data/paysim_dataset.csv" ]; then
    ./.venv/bin/python src/producer.py --csv data/paysim_dataset.csv --tps 100 > producer.log 2>&1 &
elif [ -f "data/paysim_sample.csv" ]; then
    ./.venv/bin/python src/producer.py --csv data/paysim_sample.csv --tps 100 > producer.log 2>&1 &
else
    ./.venv/bin/python src/producer.py --synthetic --num-transactions 10000 --tps 100 > producer.log 2>&1 &
fi
PRODUCER_PID=$!
echo "   Producer started (PID: $PRODUCER_PID, logging to producer.log)"

echo ""
echo "==========================================="
echo " All components started successfully!"
echo "==========================================="
echo " Monitor at:"
echo "   - Visual Web UI:   http://localhost:5000 (or ./run_ui.sh)"
echo "   - Flink Dashboard: http://localhost:8081"
echo "   - Neo4j Browser:   http://localhost:7474"
echo ""
echo " PIDs: Graph Worker=$GRAPH_PID, Stream Processor=$FLINK_PID, Producer=$PRODUCER_PID"
echo " Run ./stop_all.sh to terminate services."
