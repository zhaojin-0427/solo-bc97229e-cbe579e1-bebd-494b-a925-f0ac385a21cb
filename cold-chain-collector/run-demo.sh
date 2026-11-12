#!/usr/bin/env bash
# Local demo without Docker: starts the gateway simulator and the collector
# web app against ./_demo_data. Requires Python 3.11+ with Flask installed
# (pip install -r requirements.txt).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
DEMO="$ROOT/_demo_data"
export WATCH_DIR="$DEMO/gateway"
export DB_PATH="$DEMO/db/collector.db"
export POLL_INTERVAL="${POLL_INTERVAL:-1}"
export HTTP_PORT="${HTTP_PORT:-8000}"
mkdir -p "$WATCH_DIR" "$(dirname "$DB_PATH")"

cleanup() {
    echo
    echo "[demo] shutting down..."
    [ -n "${WEB_PID:-}" ] && kill "$WEB_PID" 2>/dev/null || true
    [ -n "${GW_PID:-}" ] && kill "$GW_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "[demo] starting gateway simulator -> $WATCH_DIR"
python3 "$ROOT/gateway/simulator.py" \
    --watch-dir "$WATCH_DIR" \
    --interval 1 \
    --rotate-every 20 \
    --half-line-every 7 \
    --bad-every 11 &
GW_PID=$!

echo "[demo] starting collector web on http://127.0.0.1:$HTTP_PORT"
python3 -m app.wsgi &
WEB_PID=$!

echo "[demo] open http://127.0.0.1:$HTTP_PORT  (Ctrl-C to stop)"
wait
