#!/bin/bash
# Run Slack-Mixpanel pipeline for custom date range
# Usage: ./scripts/run-all-daterange.sh 2024-01-01 2024-01-31

set -e

if [ $# -ne 2 ]; then
    echo "Usage: $0 START_DATE END_DATE"
    echo "Example: $0 2024-01-01 2024-01-31"
    exit 1
fi

START_DATE=$1
END_DATE=$2
PORT=${PORT:-8080}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/logs/all-daterange-${START_DATE}_${END_DATE}-$(date +%Y%m%d-%H%M%S).log"

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
echo "Running pipeline: POST /all?start_date=$START_DATE&end_date=$END_DATE"
echo "Output logging to: $LOG_FILE"
echo ""

curl -X POST "http://localhost:$PORT/all?start_date=$START_DATE&end_date=$END_DATE" \
    -H "Content-Type: application/json" \
    -w "\n\nHTTP Status: %{http_code}\nTotal Time: %{time_total}s\n" \
    2>&1 | tee -a "$LOG_FILE"

echo ""
echo "Pipeline complete. Full logs: $LOG_FILE"
