#!/bin/bash
# Start DM Tools server (Docker-free)
cd "$(dirname "$0")"
source venv/bin/activate
exec uvicorn backend.main:app --host 0.0.0.0 --port 8003 --reload
