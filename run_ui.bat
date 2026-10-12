@echo off
REM =============================================================================
REM StreamGraph — 1-Click Visual Dashboard Launcher (Windows)
REM =============================================================================
REM Starts the interactive web monitoring UI for the StreamGraph pipeline:
REM   - Real-time streaming metrics & throughput
REM   - Force-directed interactive fraud ring graph visualizer
REM   - Dual-tier latency analytics (Tier 1 Flink vs Tier 2 Neo4j)
REM   - Live fraud alerts forensic log
REM   - IEEE paper evaluation charts & benchmark report
REM   - Presentation demo controls (inject rings, bursts, speed slider)
REM =============================================================================

cd /d "%~dp0"

echo =====================================================================
echo   StreamGraph - Real-Time Fraud Ring Detection Visual Dashboard
echo   IEEE Research Project: Dual-Tier Architecture (Flink x Neo4j)
echo =====================================================================
echo.

where python >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Python is not installed or not on PATH.
    echo Please install Python 3.10+ from https://www.python.org/downloads/
    pause
    exit /b 1
)

REM Check virtual environment
if not exist ".venv\Scripts\python.exe" (
    echo [SETUP] Creating Python virtual environment...
    python -m venv .venv
    echo [SETUP] Installing dependencies...
    .\.venv\Scripts\pip install -r requirements.txt
)

echo [START] Launching StreamGraph Visual Dashboard...
echo [INFO]  Web UI starting at: http://localhost:5000
echo.

REM Automatically open browser after 1.5 seconds
start "" cmd /c "timeout /t 2 >nul & start http://localhost:5000"

REM Run Web UI server
.\.venv\Scripts\python src\web_ui.py --port 5000

pause
