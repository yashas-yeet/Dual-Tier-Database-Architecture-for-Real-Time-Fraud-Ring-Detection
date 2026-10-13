@echo off
REM =============================================================================
REM StreamGraph — Stop All Services
REM =============================================================================

cd /d "%~dp0"

echo ===========================================
echo  StreamGraph - Stopping All Services
echo ===========================================
echo.

echo [1/3] Terminating StreamGraph Python components...
taskkill /F /FI "WINDOWTITLE eq StreamGraph*" >nul 2>&1
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'src[\\/](producer|stream_processor|graph_worker|web_ui)\.py' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1
echo    Python components stopped.
echo.

echo [2/3] Stopping Docker infrastructure...
docker compose down
echo.

echo [3/3] Cleanup complete.
echo ===========================================
echo  All services stopped.
echo ===========================================
pause
