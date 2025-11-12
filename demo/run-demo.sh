#!/bin/bash

# DriftDB Interactive Demo Launcher
# Automatically detects available tools and starts the demo

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          DriftDB Interactive Demo Launcher                     ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Change to demo directory
cd "$(dirname "$0")"

# Default port
PORT=${1:-8080}

# Check for available web servers
if command -v node &> /dev/null; then
    echo "✓ Node.js detected - using Node HTTP server"
    echo ""
    node server.js "$PORT"
elif command -v python3 &> /dev/null; then
    echo "✓ Python 3 detected - using Python HTTP server"
    echo ""
    echo "  🚀 Server running at: http://localhost:$PORT"
    echo ""
    echo "  Press Ctrl+C to stop the server"
    echo ""
    python3 -m http.server "$PORT"
elif command -v python &> /dev/null; then
    echo "✓ Python 2 detected - using Python HTTP server"
    echo ""
    echo "  🚀 Server running at: http://localhost:$PORT"
    echo ""
    echo "  Press Ctrl+C to stop the server"
    echo ""
    python -m SimpleHTTPServer "$PORT"
else
    echo "⚠ No web server found (node, python, or python3)"
    echo ""
    echo "You can still open the demo directly in your browser:"
    echo ""
    echo "  Option 1: Double-click index.html"
    echo "  Option 2: Run: open index.html (macOS)"
    echo "  Option 3: Run: xdg-open index.html (Linux)"
    echo ""
    echo "Or install Node.js or Python to run a local server."
    echo ""
    exit 1
fi
