#!/bin/bash
set -e

echo "🚀 Starting Thema Ads Processing System..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  Warning: .env file not found. Creating from example..."
    cp .env.example .env
    echo "📝 Please edit .env with your API keys before continuing."
    exit 1
fi

# Build and start Docker containers
echo "🐳 Building Docker containers..."
docker-compose build

echo "🐳 Starting services..."
docker-compose up -d

# Wait for database to be ready
echo "⏳ Waiting for database..."
sleep 5

# Initialize database
echo "📊 Initializing database..."
docker-compose exec -T app python backend/database.py

echo ""
echo "✅ System is ready!"
echo ""
echo "📱 Access the application:"
echo "   Main App:        http://localhost:8001/static/index.html"
echo "   Thema Ads:       http://localhost:8001/static/thema-ads.html"
echo "   API Docs:        http://localhost:8001/docs"
echo ""
echo "📝 View logs:"
echo "   docker-compose logs -f app"
echo ""
echo "🛑 Stop system:"
echo "   docker-compose down"
