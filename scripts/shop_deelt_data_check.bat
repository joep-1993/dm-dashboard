@echo off
REM Shop deelt data check – runs daily at 07:00 via Task Scheduler
cd /d C:\Users\l.davidowski\dm-dashboard
venv\Scripts\python.exe scripts\shop_deelt_data_check.py >> logs\shop_deelt_data_check.log 2>&1
