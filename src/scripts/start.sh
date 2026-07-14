#!/usr/bin/env bash

set -euo pipefail

APP_NAME="meet-dev"
PID_FILE=".${APP_NAME}.pid"
LOG_FILE="${APP_NAME}.log"

# Prevent duplicate runs
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "⚠️  $APP_NAME is already running (PID $OLD_PID)"
        echo "👉 Stop it first using ./stop.sh"
        exit 1
    else
        echo "🧹 Removing stale PID file"
        rm -f "$PID_FILE"
    fi
fi

echo "🚀 Starting $APP_NAME..."
echo "📄 Logs: tail -f $LOG_FILE"

# Start Honcho using uv
uv run honcho start > "$LOG_FILE" 2>&1 &

PID=$!
echo $PID > "$PID_FILE"

sleep 1

if ps -p "$PID" > /dev/null 2>&1; then
    echo "✅ Started successfully (PID $PID)"
else
    echo "❌ Failed to start. Check logs:"
    echo "👉 tail -n 50 $LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
fi