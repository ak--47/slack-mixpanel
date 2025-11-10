#!/bin/bash
# Run Slack-Mixpanel pipeline for last 7 days
# Usage: ./scripts/run-all-7days.sh

set -e

PORT=${PORT:-8080}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/logs/all-7days-$(date +%Y%m%d-%H%M%S).log"

echo "Starting server on port $PORT..."
mkdir -p "$SCRIPT_DIR/logs"

# Start server in background
npm start > "$LOG_FILE" 2>&1 &
SERVER_PID=$!

# Cleanup function
cleanup() {
    echo "Shutting down server (PID: $SERVER_PID)..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

# Wait for server to be ready
echo "Waiting for server to start..."
for i in {1..30}; do
    if curl -s http://localhost:$PORT/health > /dev/null 2>&1; then
        echo "Server is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Server failed to start within 30 seconds"
        exit 1
    fi
    sleep 1
done

# Make the request
echo "Running pipeline: POST /all?days=7"
echo "Output logging to: $LOG_FILE"
echo ""

curl -X POST "http://localhost:$PORT/all?days=7" \
    -H "Content-Type: application/json" \
    -w "\n\nHTTP Status: %{http_code}\nTotal Time: %{time_total}s\n" \
    2>&1 | tee -a "$LOG_FILE"

echo ""
echo "Pipeline complete. Full logs: $LOG_FILE"
