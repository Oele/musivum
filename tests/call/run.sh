#!/usr/bin/env bash
# Runs the scripted call (run.js) against one server and writes what the clients observed to
# out/<target>.json.
#
#   ./run.sh reference     LiveKit itself (livekit-server, which has to be the pinned
#                          version), started here with reference-livekit.yaml
#   ./run.sh musivum       musivum (target/release/musivum-server, built here), started
#                          with musivum.yaml, which has the same ports
#
# Everything listens on the loopback interface, on ports 39500 to 39511.
set -euo pipefail
cd "$(dirname "$0")"
TARGET=${1:-reference}
KEY=calltestkey
SECRET=calltestsecret-0123456789abcdef0123456789abcdef
[ -d node_modules ] || npm install --no-audit --no-fund >/dev/null
mkdir -p out

case "$TARGET" in
  reference)
    WANT=$(sed -n 's/^tag = "v\(.*\)"$/\1/p' ../../upstream.toml | head -n 1)
    HAVE=$(livekit-server --version | awk '{print $NF}')
    [ "$WANT" = "$HAVE" ] || { echo "livekit-server is $HAVE, the pin is $WANT" >&2; exit 1; }
    livekit-server --config reference-livekit.yaml > out/reference-server.log 2>&1 &
    URL=ws://127.0.0.1:39500
    ;;
  musivum)
    (cd ../.. && nice -n 10 cargo build --release -p musivum-server)
    ../../target/release/musivum-server --config musivum.yaml > out/musivum-server.log 2>&1 &
    URL=ws://127.0.0.1:39500
    ;;
  *)
    echo "unknown target: $TARGET" >&2; exit 2 ;;
esac
PID=$!
PIDS="$PID"
trap 'kill $PIDS 2>/dev/null || true' EXIT
for _ in $(seq 50); do
  (exec 3<>/dev/tcp/127.0.0.1/39500) 2>/dev/null && break
  sleep 0.1
done
# The server's health check on /: 200 and "OK" once its node is up.
echo "health check: $(curl -s -w ' (%{http_code})' http://127.0.0.1:39500/ || true)"
# With TAP=1 the clients go through sigtap.js, which writes the signalling they exchange
# with the server to out/<target>-signalling.jsonl.
if [ "${TAP:-}" = 1 ]; then
  node sigtap.js 39511 127.0.0.1:39500 "out/$TARGET-signalling.jsonl" &
  PIDS="$PIDS $!"
  sleep 0.5
  URL=ws://127.0.0.1:39511
fi
node run.js "$TARGET" "$URL" "$KEY" "$SECRET"
