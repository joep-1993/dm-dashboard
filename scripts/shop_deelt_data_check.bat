@echo off
REM Shop deelt data check – runs daily at 07:00 via Task Scheduler
cd /d C:\Users\l.davidowski\dm-dashboard

REM Kill zombie processes from a previous run (if any)
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*shop_deelt_data_check.py*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Output ('Killed zombie PID ' + $_.ProcessId) }" >> logs\shop_deelt_data_check.log 2>&1

venv\Scripts\python.exe scripts\shop_deelt_data_check.py >> logs\shop_deelt_data_check.log 2>&1
