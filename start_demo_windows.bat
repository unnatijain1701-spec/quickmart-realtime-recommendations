@echo off
REM Starts the whole demo on a Windows laptop that runs Kafka directly (no WSL). Kafka must be in C:\kafka.
cd /d "%~dp0"
if not exist "venv\Scripts\python.exe" (
  echo The venv is missing. Finish the Python setup steps first.
  pause
  exit /b 1
)
if not exist ".env" (
  echo The .env file is missing. Copy it into this folder.
  pause
  exit /b 1
)
if not exist "C:\kafka\bin\windows\kafka-server-start.bat" (
  echo Kafka is not in C:\kafka. Finish the Kafka setup steps first.
  pause
  exit /b 1
)
set "KAFKA_HEAP_OPTS=-Xmx512m -Xms512m"
echo Starting Kafka...
start "Kafka (keep open)" cmd /k C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\kraft\server.properties
echo Waiting 40 seconds for Kafka to be ready...
timeout /t 40 /nobreak >nul
venv\Scripts\python.exe m2_create_topics.py
start "A consumer" cmd /k venv\Scripts\python.exe m2_store_consumer.py
timeout /t 3 /nobreak >nul
start "B processor" cmd /k venv\Scripts\python.exe m3_processor.py --trend-window 90 --trend-slide 2 --stale-refresh 300
timeout /t 20 /nobreak >nul
start "C producer" cmd /k venv\Scripts\python.exe m2_producer.py --rate 20
start "D shop" cmd /k venv\Scripts\python.exe m4_app.py
start "E dashboard" cmd /k venv\Scripts\python.exe m5_dashboard.py
echo.
echo Everything is starting. Wait about 6 minutes before presenting, then open in Chrome:
echo   Dashboard:    http://127.0.0.1:8050
echo   Shop (live):  http://127.0.0.1:8000/?group=treatment
echo   Shop (stale): http://127.0.0.1:8000/?group=control
echo.
echo To stop: close the black windows.
pause
