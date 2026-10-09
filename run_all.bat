@echo off
REM =============================================================================
REM StreamGraph — 1-Click Runner for Windows
REM =============================================================================
REM Starts the entire StreamGraph pipeline:
REM   Phase 1: Docker infrastructure (Kafka, Flink, Neo4j)
REM   Phase 2: Data Producer (Kafka ingestion)
REM   Phase 3: Stream Processor (PyFlink/RocksDB Tier 1)
REM   Phase 4: Graph Worker (Neo4j Tier 2)
REM =============================================================================

cd /d "%~dp0"

echo ===========================================
echo  StreamGraph - Full Pipeline Runner
echo ===========================================
echo.

REM -------------------------------------------------------------------------
REM PRE-FLIGHT CHECKS
REM -------------------------------------------------------------------------
echo [PRE-FLIGHT] Checking prerequisites...

where python >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Python is not installed or not on PATH.
    echo Install Python 3.10+ from https://www.python.org/downloads/
    pause
    exit /b 1
)

where docker >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Docker is not installed or not on PATH.
    echo Install Docker Desktop from https://www.docker.com/products/docker-desktop
    pause
    exit /b 1
)

echo    Python:  OK
echo    Docker:  OK
echo.

if not exist ".env" (
    if exist ".env.example" (
        echo [SETUP] Creating .env from .env.example...
        copy /y ".env.example" ".env" >nul
    )
)

REM -------------------------------------------------------------------------
REM VIRTUAL ENVIRONMENT SETUP
REM -------------------------------------------------------------------------
if not exist ".venv\Scripts\python.exe" (
    echo [SETUP] Creating Python virtual environment...
    python -m venv .venv
    if %ERRORLEVEL% neq 0 (
        echo ERROR: Failed to create virtual environment.
        pause
        exit /b 1
    )
    echo    Virtual environment created.
) else (
    echo [SETUP] Virtual environment already exists.
)

echo [SETUP] Installing dependencies (this may take a few minutes on first run)...
.\.venv\Scripts\pip.exe install -q -r requirements.txt
if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to install dependencies.
    pause
    exit /b 1
)
echo    Dependencies installed.

REM Ensure Flink Kafka connector JAR is present
if not exist "lib\flink-sql-connector-kafka-3.1.0-1.18.jar" (
    echo [SETUP] Downloading Flink Kafka connector JAR...
    powershell -Command "New-Item -ItemType Directory -Force -Path 'lib' | Out-Null; Invoke-WebRequest -Uri 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar' -OutFile 'lib\flink-sql-connector-kafka-3.1.0-1.18.jar'"
)
if exist ".venv\Lib\site-packages\pyflink\lib" (
    if not exist ".venv\Lib\site-packages\pyflink\lib\flink-sql-connector-kafka-3.1.0-1.18.jar" (
        copy /y "lib\flink-sql-connector-kafka-3.1.0-1.18.jar" ".venv\Lib\site-packages\pyflink\lib\" >nul
    )
)
echo    Flink Kafka connector: OK
echo.

REM -------------------------------------------------------------------------
REM PHASE 1: Start Docker Infrastructure
REM -------------------------------------------------------------------------
echo [1/4] Starting Docker infrastructure (Kafka, Flink, Neo4j)...
docker compose up -d
if %ERRORLEVEL% neq 0 (
    echo ERROR: Docker Compose failed. Is Docker Desktop running?
    pause
    exit /b 1
)

echo.
echo Waiting for services to become healthy...
echo    (This takes ~60-90 seconds on first start)
echo.

REM Wait for Kafka to be ready (poll every 10 seconds, max 12 attempts = 2 min)
set /a ATTEMPTS=0
:wait_kafka
set /a ATTEMPTS+=1
if %ATTEMPTS% gtr 12 (
    echo WARNING: Kafka health check timed out. Proceeding anyway...
    goto kafka_ready
)
docker exec streamgraph-kafka kafka-topics --bootstrap-server localhost:9092 --list >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo    Waiting for Kafka... (attempt %ATTEMPTS%/12)
    timeout /t 10 /nobreak >nul
    goto wait_kafka
)
:kafka_ready
echo    Kafka:  READY
echo.

REM Wait for Neo4j to be ready (poll every 10 seconds, max 12 attempts = 2 min)
set /a ATTEMPTS=0
:wait_neo4j
set /a ATTEMPTS+=1
if %ATTEMPTS% gtr 12 (
    echo WARNING: Neo4j health check timed out. Proceeding anyway...
    goto neo4j_ready
)
docker exec streamgraph-neo4j wget --no-verbose --tries=1 --spider http://localhost:7474 >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo    Waiting for Neo4j... (attempt %ATTEMPTS%/12)
    timeout /t 10 /nobreak >nul
    goto wait_neo4j
)
:neo4j_ready
echo    Neo4j:  READY
echo.

REM -------------------------------------------------------------------------
REM PHASE 4: Start Graph Worker FIRST (it needs to be ready before Tier 1
REM produces suspicious transactions)
REM -------------------------------------------------------------------------
echo [2/4] Starting Tier 2: Graph Worker (Neo4j cycle detection)...
start "StreamGraph - Graph Worker" /D "%~dp0" cmd /k ".\.venv\Scripts\python.exe src\graph_worker.py"

echo    Waiting 5 seconds for Graph Worker to connect to Neo4j...
timeout /t 5 /nobreak >nul
echo.

REM -------------------------------------------------------------------------
REM PHASE 3: Start Stream Processor (PyFlink Tier 1)
REM -------------------------------------------------------------------------
echo [3/4] Starting Tier 1: Stream Processor (PyFlink velocity window)...
start "StreamGraph - Stream Processor" /D "%~dp0" cmd /k ".\.venv\Scripts\python.exe src\stream_processor.py"

echo    Waiting 20 seconds for PyFlink minicluster to start...
timeout /t 20 /nobreak >nul
echo.

REM -------------------------------------------------------------------------
REM PHASE 2: Start Producer (Data Ingestion)
REM -------------------------------------------------------------------------
echo [4/4] Starting Data Producer (Ingestion at 100 TPS)...
if exist "data\paysim_dataset.csv" (
    echo    Using data\paysim_dataset.csv
    start "StreamGraph - Producer" /D "%~dp0" cmd /k ".\.venv\Scripts\python.exe src\producer.py --csv data\paysim_dataset.csv --tps 100"
) else if exist "data\paysim_sample.csv" (
    echo    Using data\paysim_sample.csv
    start "StreamGraph - Producer" /D "%~dp0" cmd /k ".\.venv\Scripts\python.exe src\producer.py --csv data\paysim_sample.csv --tps 100"
) else (
    echo    Using built-in synthetic generator
    start "StreamGraph - Producer" /D "%~dp0" cmd /k ".\.venv\Scripts\python.exe src\producer.py --synthetic --num-transactions 10000 --tps 100"
)

echo.
echo ===========================================
echo  All components started successfully!
echo ===========================================
echo.
echo  Running in separate windows:
echo    - Graph Worker     (Tier 2 / Neo4j)
echo    - Stream Processor (Tier 1 / PyFlink)
echo    - Producer         (Data Ingestion)
echo.
echo  Monitor at:
echo    - Flink Dashboard: http://localhost:8081
echo    - Neo4j Browser:   http://localhost:7474
echo.
echo  To stop everything:
echo    1. Close the 3 Python windows (or Ctrl+C in each)
echo    2. Run: stop_all.bat
echo.
pause
