# StreamGraph — Dual-Tier Database Architecture for Real-Time Fraud Ring Detection

> IEEE Research Project: A streaming pipeline that combines Apache Flink (Tier 1: temporal anomaly detection with RocksDB state backend) and Neo4j (Tier 2: graph-structural cycle detection) to detect fraud rings in real-time financial transaction streams.

## Architecture

```
┌────────────┐    ┌─────────────────────────────────────────┐    ┌───────────┐
│  Producer   │    │  Tier 1: Apache Flink + RocksDB          │    │  Tier 2   │
│  (PaySim)   │───▶│  Velocity Window → SUSPICIOUS / CLEARED  │───▶│  Neo4j    │
│             │    │                                           │    │  (Graph)  │
└────────────┘    └─────────────────────────────────────────┘    └───────────┘
     │                                                                │
     │              Kafka: raw-transactions                           │
     └────────────────────────────────────────────────────────────────┘
                    Kafka: suspicious-log → fraud-alerts
```

**Key Insight**: Tier 1 filters ~95-99% of transactions cheaply (O(1) state lookup per record). Only the suspicious ~1-5% are forwarded to Tier 2 for expensive O(d^k) graph cycle detection in Neo4j. This reduces graph query load by orders of magnitude.

## Project Structure

```
streamgraph/
├── docker-compose.yml          # Infrastructure: Kafka, ZooKeeper, Flink, Neo4j
├── requirements.txt            # Python dependencies (pinned versions)
├── .env                        # Environment configuration
├── .env.example                # Environment template (version-controlled)
├── run_all.bat                 # 1-click start (Windows)
├── run_all.sh                  # 1-click start (Linux/macOS)
├── stop_all.bat                # 1-click stop (Windows)
├── stop_all.sh                 # 1-click stop (Linux/macOS)
├── LICENSE                     # MIT License
├── src/
│   ├── __init__.py             # Package init with project metadata
│   ├── producer.py             # Phase 2: PaySim CSV → Kafka ingestion
│   ├── stream_processor.py     # Phase 3: PyFlink + RocksDB (Tier 1)
│   ├── graph_worker.py         # Phase 4: Neo4j graph validation (Tier 2)
│   ├── dashboard.py            # Phase 5: Real-time terminal monitoring
│   └── benchmark_analysis.py   # Phase 5: Chart generation for IEEE paper
├── tests/
│   ├── __init__.py             # Test package initialization
│   ├── test_producer.py        # Tests for PaySim parser, rate limiter, synthetic gen
│   ├── test_stream_logic.py    # Tests for Tier 1 sliding velocity window rules
│   ├── test_graph_worker.py    # Tests for Tier 2 graph queries & severity scoring
│   ├── test_benchmark_analysis.py # Tests for statistical metrics & chart exports
│   └── run_tests.py            # Automated test suite runner (24 unit/integration tests)
├── data/
│   ├── paysim_dataset.csv      # PaySim synthetic financial dataset
│   └── paysim_sample.csv       # PaySim sample dataset for testing
├── lib/
│   └── flink-sql-connector-kafka-3.1.0-1.18.jar  # Flink Kafka connector
└── benchmarks/                 # Performance metrics & charts (generated)
    └── charts/                 # Publication-ready PNG charts
```

## Prerequisites

- **Docker Desktop** with Docker Compose v2+ (for infrastructure services)
- **Python 3.8 – 3.10** (required by Apache PyFlink 1.18.1; Python 3.10 recommended)
- **Java 8 or 11/17 (JDK)** with `JAVA_HOME` set (required by PyFlink local runtime)
- **~4 GB RAM** available for Docker containers

## Quick Start (1-Click)

The simplest way to run the entire pipeline:

**Windows:**
```batch
run_all.bat
```

**Linux / macOS:**
```bash
chmod +x run_all.sh stop_all.sh
./run_all.sh
```

This script will:
1. Check prerequisites (Python, Docker)
2. Create a virtual environment and install dependencies (first run only)
3. Start Docker infrastructure (Kafka, ZooKeeper, Flink, Neo4j)
4. Wait for services to become healthy
5. Launch all 3 Python components: Graph Worker (Tier 2), Stream Processor (Tier 1), Producer (Ingestion)

To stop everything:
- **Windows:** `stop_all.bat`
- **Linux / macOS:** `./stop_all.sh`

## Manual Setup (Step-by-Step)

### Phase 1: Start Infrastructure

```bash
docker compose up -d
```

Wait ~60-90 seconds for all services to be healthy. Verify:

| Service | URL / Address | Purpose |
|---------|---------------|---------|
| **Kafka** | `localhost:9093` (host) | Message broker |
| **Flink Dashboard** | http://localhost:8081 | Job monitoring |
| **Neo4j Browser** | http://localhost:7474 | Graph visualization |
| **Neo4j Bolt** | `bolt://localhost:7687` | Driver connections |

**Neo4j Credentials**: `neo4j` / `streamgraph_research_2024`

### Phase 2: Set Up Python Environment

```bash
# Create virtual environment
python -m venv .venv

# Activate it
.\.venv\Scripts\activate      # Windows (PowerShell)
# or
.venv\Scripts\activate.bat    # Windows (cmd)

# Install dependencies
pip install -r requirements.txt
```

### Phase 3: Start the Pipeline (in 3 separate terminals)

**Terminal 1 — Graph Worker (start first)**
```bash
python src/graph_worker.py
```
Connects to Neo4j and Kafka. Consumes suspicious transactions from `suspicious-log` topic.

**Terminal 2 — Stream Processor**
```bash
python src/stream_processor.py
```
Starts a PyFlink minicluster with RocksDB state backend. Consumes from `raw-transactions`, applies velocity window, outputs to `suspicious-log` and `cleared-log`.

**Terminal 3 — Producer**
```bash
# Using the PaySim CSV dataset (recommended)
python src/producer.py --csv data/paysim_dataset.csv --tps 100

# Or generate synthetic data
python src/producer.py --synthetic --num-transactions 100000 --tps 500

# Max throughput (no throttling)
python src/producer.py --csv data/paysim_dataset.csv --tps 0
```

### Phase 4: Monitor & Benchmark

- **Flink Dashboard** (http://localhost:8081): Monitor stream processing job, task slots, checkpoints.
- **Neo4j Browser** (http://localhost:7474): Query the transaction graph interactively.
- **Real-time Dashboard**: `python src/dashboard.py` — live terminal dashboard with TPS, latency, and fraud alerts.
- **Benchmark output**: Check `benchmarks/` directory for JSON/CSV metrics files.

**Real-Time Dashboard:**
```bash
# Launch the live terminal dashboard (updates every 3 seconds)
python src/dashboard.py

# Custom refresh interval
python src/dashboard.py --refresh 5
```

**Benchmark Analysis (IEEE Paper Charts):**
```bash
# Generate all charts and summary tables after a pipeline run
python src/benchmark_analysis.py

# Offline evaluation / Demo (auto-generates synthetic benchmark data and charts)
python src/benchmark_analysis.py --demo

# Generate from a specific metrics file
python src/benchmark_analysis.py --input benchmarks/tier2_metrics_20240101_120000.csv

# Summary table only (no charts)
python src/benchmark_analysis.py --no-charts
```

Generated charts (saved to `benchmarks/charts/`):
| Chart | Description |
|-------|-------------|
| `tier1_vs_tier2_latency.png` | Box plot: Tier 1 vs. Tier 2 latency comparison |
| `latency_distribution.png` | Histograms of latency distributions |
| `throughput_timeline.png` | TPS over time during benchmark run |
| `e2e_latency_cdf.png` | CDF of end-to-end latency |
| `speedup_ratio_distribution.png` | Distribution of Tier2/Tier1 cost ratios |
| `latency_over_time.png` | Scatter plot of per-transaction latency |
| `summary_table.csv` | Formatted table for the paper |

Example Neo4j queries:
```cypher
-- Count nodes and relationships
MATCH (n) RETURN labels(n) AS type, count(n) AS count;

-- Find fraud rings (3-hop cycles)
MATCH path = (a:User)-[:SENT]->(:Transaction)-[:RECEIVED]->(b:User)
  -[:SENT]->(:Transaction)-[:RECEIVED]->(c:User)
  -[:SENT]->(:Transaction)-[:RECEIVED]->(a)
WHERE a <> b AND b <> c AND a <> c
RETURN path LIMIT 5;
```

## Configuration

All parameters are configurable via the `.env` file:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9093` | Kafka broker address |
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt endpoint |
| `NEO4J_PASSWORD` | `streamgraph_research_2024` | Neo4j password |
| `VELOCITY_WINDOW_SIZE_MINUTES` | `15` | Sliding window size |
| `VELOCITY_MAX_TRANSACTION_COUNT` | `5` | Max transactions before flagging |
| `VELOCITY_MAX_TOTAL_AMOUNT` | `10000.0` | Max amount ($) before flagging |
| `PRODUCER_TARGET_TPS` | `1000` | Default ingestion rate |
| `FLINK_PARALLELISM` | `2` | PyFlink operator parallelism |
| `BENCHMARK_ENABLED` | `true` | Enable/disable metrics collection |

## Troubleshooting

### Docker Issues
```bash
# Check service health
docker compose ps

# View logs for a specific service
docker compose logs kafka
docker compose logs neo4j
docker compose logs flink-jobmanager

# Full restart (clears volumes)
docker compose down -v
docker compose up -d
```

### Kafka Connection Refused
- Ensure Docker is running and Kafka container is healthy
- Wait at least 60 seconds after `docker compose up -d`
- Check: `docker exec streamgraph-kafka kafka-topics --bootstrap-server localhost:9092 --list`

### Neo4j Connection Error
- Wait for the Neo4j container to fully start (~40 seconds)
- Check: open http://localhost:7474 in a browser
- The graph_worker.py retries 12 times with 5-second intervals automatically

### PyFlink RocksDB Errors
- Ensure you're using Python 3.10 (PyFlink 1.18.1 has best support for 3.10)
- On Windows, the minicluster creates `./flink-checkpoints/` locally
- If issues persist, try: `pip install apache-flink==1.18.1 --force-reinstall`

### Insufficient Memory
- Docker containers need ~4 GB total RAM
- Reduce allocation in `docker-compose.yml` if needed:
  - Kafka: `KAFKA_HEAP_OPTS: "-Xmx512m"`
  - Neo4j: `NEO4J_server_memory_heap_max__size: "512m"`
  - Flink: `jobmanager.memory.process.size: 1024m`

## Running Without Docker (Alternatives)

If you prefer not to use Docker:
1. **PyFlink (Tier 1)**: Already runs natively on your machine via Python (`python src/stream_processor.py`) using its built-in minicluster and local RocksDB storage.
2. **Neo4j (Tier 2)**:
   - **Neo4j Desktop**: Install the official Windows native [Neo4j Desktop application](https://neo4j.com/download/), create a local database, and point `NEO4J_URI=bolt://localhost:7687` in [.env](file:///c:/Users/Yashas%20Vishwakarma/Desktop/project/.env).
   - **Neo4j AuraDB (Cloud)**: Create a free cloud database on [Neo4j AuraDB](https://neo4j.com/cloud/platform/aura-graph-database/) (no installation required), and set `NEO4J_URI=neo4j+s://<instance-id>.databases.neo4j.io` with your credentials in [.env](file:///c:/Users/Yashas%20Vishwakarma/Desktop/project/.env).
3. **Kafka (Message Broker)**:
   - **Redpanda**: Download [Redpanda](https://redpanda.com/) (a single lightweight C++ binary replacement for Kafka that runs without ZooKeeper or JVM).
   - **Confluent Cloud (Free Tier)**: Provision a free managed Kafka cluster on Confluent Cloud and update `KAFKA_BOOTSTRAP_SERVERS` in [.env](file:///c:/Users/Yashas%20Vishwakarma/Desktop/project/.env).

## Running Automated Tests

The repository contains an automated test suite (24 unit and integration tests) covering all pipeline layers:

```bash
# Run the complete test suite
python tests/run_tests.py

# Or run tests using standard unittest discovery
python -m unittest discover tests
```

### Test Coverage
- **`tests/test_producer.py`**: Validates PaySim CSV streaming parser, token-bucket rate limiter (`TokenBucketRateLimiter`), `ProducerMetrics` tracking, and synthetic transaction generator (`SyntheticPaySimGenerator`).
- **`tests/test_stream_logic.py`**: Validates Tier 1 temporal anomaly detection rules (transaction count threshold, amount sum threshold, sliding window state eviction).
- **`tests/test_graph_worker.py`**: Validates Cypher cycle detection query structures (3-hop, 4-hop, 5-hop), severity scoring (`CRITICAL`, `HIGH`, `MEDIUM`), and per-transaction metrics collection.
- **`tests/test_benchmark_analysis.py`**: Validates latency percentiles computation (p50, p95, p99), throughput aggregation, IEEE summary table generator, and synthetic benchmark generator.

