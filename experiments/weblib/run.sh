#!/usr/bin/env bash
# Builds a forwarder of the experiment, starts it, runs the browser driver against it and
# stops it again:   ./run.sh rtc-forwarder
set -euo pipefail
cd "$(dirname "$0")"
BIN=${1:-rtc-forwarder}
export HTTP_PORT=${HTTP_PORT:-39400} UDP_PORT=${UDP_PORT:-39401}
cargo build -q -p "$BIN"
[ -d driver/node_modules ] || (cd driver && npm install --no-audit --no-fund >/dev/null)
"target/debug/$BIN" > "target/$BIN.log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT
sleep 1
(cd driver && node run.js "http://127.0.0.1:$HTTP_PORT")
