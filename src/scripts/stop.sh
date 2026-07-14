#!/usr/bin/env bash

set -euo pipefail

APP_NAME="meet-dev"
PID_FILE=".${APP_NAME}.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "⚠️  No PID file found. Is $APP_NAME running?"
    exit 1
fi

PID=$(cat "$PID_FILE")

if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "⚠️  Process $PID not running. Cleaning up..."
    rm -f "$PID_FILE"
    exit 0
fi

echo "🛑 Stopping $APP_NAME (PID $PID)..."

# Send SIGTERM (graceful shutdown)
kill "$PID"

# Wait for graceful shutdown
TIMEOUT=10
while ps -p "$PID" > /dev/null 2>&1; do
    if [ "$TIMEOUT" -le 0 ]; then
        echo "⚠️  Force killing process..."
        kill -9 "$PID"
        break
    fi
    sleep 1
    TIMEOUT=$((TIMEOUT - 1))
done

rm -f "$PID_FILE"

echo "✅ Stopped successfully."