@echo off
REM DMA OOS Cycle – runs every 6h via Task Scheduler
REM Calls the local dashboard API to scan, exclude, and re-enable OOS products.

cd /d C:\Users\l.davidowski\dm-dashboard
venv\Scripts\python.exe scripts\dma_oos_cycle.py >> logs\dma_oos_cycle.log 2>&1
