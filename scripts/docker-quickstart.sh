#!/bin/bash
# DriftDB Docker Quick Start Script

set -e

echo "🚀 Starting DriftDB with Docker..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if docker-compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose is not installed. Please install docker-compose first."
    exit 1
fi

# Build the image
echo "📦 Building DriftDB Docker image..."
docker-compose build

# Start the services
echo "🎯 Starting DriftDB server..."
docker-compose up -d

# Wait for server to be ready
echo "⏳ Waiting for DriftDB to be ready..."
sleep 5

# Check if server is running
if docker-compose ps | grep -q "driftdb-server.*Up"; then
    echo "✅ DriftDB is running!"
    echo ""
    echo "📊 Connection details:"
    echo "  PostgreSQL: postgresql://driftdb:driftdb@localhost:5433/driftdb"
    echo "  PgAdmin: http://localhost:8080 (admin@driftdb.local / driftdb)"
    echo ""
    echo "🔧 Useful commands:"
    echo "  View logs:    docker-compose logs -f driftdb"
    echo "  Stop server:  docker-compose down"
    echo "  Clean data:   docker-compose down -v"
    echo ""
    echo "📚 Try connecting with:"
    echo "  psql -h localhost -p 5433 -d driftdb -U driftdb"
else
    echo "❌ Failed to start DriftDB. Check logs with: docker-compose logs"
    exit 1
fi