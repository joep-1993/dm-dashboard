#!/bin/bash

echo "🚀 Setting up your FastAPI project..."
echo ""

# Check for .env file
if [ ! -f .env ]; then
    echo "📝 Creating .env from template..."
    cp .env.example .env
    echo "⚠️  Please edit .env and add your OPENAI_API_KEY"
    echo ""
fi

# Build Docker images
echo "🐳 Building Docker containers..."
docker-compose build

echo ""
echo "✅ Setup complete!"
echo ""
echo "To start your application:"
echo "  docker-compose up"
echo ""
echo "Then open http://localhost:8001 in your browser"
