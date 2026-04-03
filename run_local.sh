#!/bin/bash
# DM Tools - Run locally without Docker
# First time: ./run_local.sh setup
# After that: ./run_local.sh

set -e

if [ "$1" = "setup" ]; then
    echo "=== DM Tools Local Setup ==="

    # Check Python version
    if ! command -v python3 &> /dev/null; then
        echo "Error: python3 is required. Install Python 3.11+ first."
        exit 1
    fi

    PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    echo "Found Python $PYTHON_VERSION"

    # Create virtual environment
    if [ ! -d "venv" ]; then
        echo "Creating virtual environment..."
        python3 -m venv venv
    fi

    # Activate venv
    source venv/bin/activate

    # Install dependencies
    echo "Installing dependencies..."
    pip install --upgrade pip
    pip install -r requirements.txt

    # Create .env if it doesn't exist
    if [ ! -f ".env" ]; then
        cp .env.example .env
        echo ""
        echo "Created .env from .env.example."
        echo ">>> Please edit .env with your actual credentials before starting. <<<"
        echo ""
    fi

    # Initialize database tables
    echo "Initializing database..."
    python -m backend.database

    echo ""
    echo "=== Setup complete! ==="
    echo "Run ./run_local.sh to start the server."
    exit 0
fi

# Activate venv
if [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "No venv found. Run './run_local.sh setup' first."
    exit 1
fi

# Symlink thema_ads_optimized if not present (Docker mounts this as a volume)
if [ ! -e "thema_ads_optimized" ] && [ -d "../theme_ads/thema_ads_optimized" ]; then
    ln -s ../theme_ads/thema_ads_optimized thema_ads_optimized
fi

echo "Starting DM Tools on http://localhost:8003 ..."
uvicorn backend.main:app --host 0.0.0.0 --port 8003 --reload
