@echo off
REM Start DM Tools server via WSL on Windows startup
REM This script is intended for Windows Task Scheduler (trigger: At startup)
wsl -d Ubuntu -- bash -c "cd /home/joepvanschagen/projects/dm-tools && source venv/bin/activate && uvicorn backend.main:app --host 0.0.0.0 --port 8003 --reload"
