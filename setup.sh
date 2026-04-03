#!/bin/bash
# DM Dashboard - Setup Script
# Run this once to set up the project without Docker.

set -e

echo "=== DM Dashboard Setup ==="

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
echo ""
echo "To start the server:"
echo "  source venv/bin/activate"
echo "  uvicorn backend.main:app --host 0.0.0.0 --port 8003 --reload"
echo ""
echo "Then open: http://localhost:8003"
