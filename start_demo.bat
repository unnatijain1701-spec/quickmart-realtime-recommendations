@echo off
REM Starts the whole demo on THIS laptop. Double-click it, or run it from the qc folder.
cd /d "%~dp0"
if not exist "venv\Scripts\python.exe" (
  echo The venv is missing. Do steps 1 to 6 of SETUP_NEW_LAPTOP.md first.
  pause
  exit /b 1
)
if not exist ".env" (
  echo The .env file is missing. Copy it from your own laptop into this folder.
  pause
  exit /b 1
)
echo Starting Kafka inside Ubuntu...
start "Kafka (keep open)" cmd /k wsl -d Ubuntu -e bash -c "~/start_kafka.sh"
echo Waiting 45 seconds for Kafka to be ready...
timeout /t 45 /nobreak >nul
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
echo   Dashboard:   http://127.0.0.1:8050
echo   Shop (live):  http://127.0.0.1:8000/?group=treatment
echo   Shop (stale): http://127.0.0.1:8000/?group=control
echo.
echo To stop: close the black windows, then run:  wsl --shutdown
pause
