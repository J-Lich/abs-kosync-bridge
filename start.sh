#!/bin/bash

# Graceful shutdown handler
cleanup() {
    echo "🛑 Shutting down services..."
    kill $DAEMON_PID $WEB_PID 2>/dev/null
    wait
    exit 0
}

# Set up signal handlers for graceful shutdown
trap cleanup SIGTERM SIGINT

echo "🚀 Starting ABS-KoSync Enhanced..."
echo ""

# Start the main sync daemon in the background
echo "  📡 Starting sync daemon..."
python /app/src/main.py &
DAEMON_PID=$!

# Wait a moment for daemon to initialize
sleep 3

# Start the web server in the background
echo "  🌐 Starting web interface..."
python /app/web_server.py &
WEB_PID=$!

echo ""
echo "✅ All services started successfully!"
echo "   • Sync Daemon PID: $DAEMON_PID"
echo "   • Web Server PID: $WEB_PID"
echo "   • Web UI available at: http://localhost:5757"
echo ""
echo "Press Ctrl+C to stop..."

# Wait for either process to exit
wait
