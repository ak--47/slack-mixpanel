#!/bin/bash
# Run Slack-Mixpanel pipeline for full backfill (13 months)
# Usage: ./scripts/run-all-backfill.sh

set -e

PORT=${PORT:-8080}
LOG_FILE="logs/all-backfill-$(date +%Y%m%d-%H%M%S).log"

echo "Starting server on port $PORT..."
mkdir -p logs

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
echo "Running pipeline: POST /mixpanel-all?backfill=true"
echo "⚠️  WARNING: This will process 13 months of historical data and may take a while"
echo "Output logging to: $LOG_FILE"
echo ""

curl -X POST "http://localhost:$PORT/mixpanel-all?backfill=true" \
    -H "Content-Type: application/json" \
    -w "\n\nHTTP Status: %{http_code}\nTotal Time: %{time_total}s\n" \
    2>&1 | tee -a "$LOG_FILE"

echo ""
echo "Pipeline complete. Full logs: $LOG_FILE"
