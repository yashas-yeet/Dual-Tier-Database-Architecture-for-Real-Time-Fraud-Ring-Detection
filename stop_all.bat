@echo off
REM =============================================================================
REM StreamGraph — Stop All Services
REM =============================================================================

cd /d "%~dp0"

echo ===========================================
echo  StreamGraph - Stopping All Services
echo ===========================================
echo.

echo [1/2] Stopping Docker infrastructure...
docker compose down
echo.

echo [2/2] Note: Close the Python windows (Graph Worker, Stream Processor,
echo        Producer) manually if they are still running, or they will
echo        exit on their own when Kafka becomes unavailable.
echo.

echo ===========================================
echo  All services stopped.
echo ===========================================
pause
