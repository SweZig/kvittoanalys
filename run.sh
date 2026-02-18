#!/usr/bin/env bash
# DocVision — Quick start script

set -e

echo "🔍 DocVision — Document & Image Analysis"
echo "=========================================="

# Check for .env
if [ ! -f .env ]; then
    echo "📋 Creating .env from template..."
    cp .env.example .env
    echo "⚠️  Edit .env and add your ANTHROPIC_API_KEY before running!"
    exit 1
fi

# Check for virtual environment
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

echo "🔄 Activating virtual environment..."
source venv/bin/activate

echo "📥 Installing dependencies..."
pip install -r requirements.txt --quiet

echo ""
echo "🚀 Starting DocVision API..."
echo "   → API docs: http://localhost:8000/docs"
echo "   → Health:   http://localhost:8000/health"
echo ""

uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
