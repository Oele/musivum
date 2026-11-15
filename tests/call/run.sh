#!/usr/bin/env bash
# Runs the scripted call (run.js) against one server and writes what the clients observed to
# out/<target>.json.
#
#   ./run.sh reference     LiveKit itself (livekit-server, which has to be the pinned
#                          version: $LIVEKIT_SERVER, else the one on the PATH, else
#                          ~/.cache/musivum/gobin/livekit-server, LiveKit's release binary
#                          for linux/amd64 checked against the release's checksums.txt),
#                          started here with reference-livekit.yaml
#   ./run.sh musivum       musivum (target/release/musivum-server, built here), started
#                          with musivum.yaml, which has the same ports
#
# Everything listens on the loopback interface, on ports 39500 to 39511.
#
# With TURN=1 the server has its embedded TURN server as well (turn-livekit.yaml,
# turn-musivum.yaml: UDP 39540, TLS 39541, relays from 39545 to 39559, with a self-signed
# certificate made here under target/call-turn) and the clients may only use it (RELAY=1 for
# run.js: iceTransportPolicy 'relay'); the output is out/<target>-turn.json. TURN_ONLY=tls (or
# udp) leaves the clients only the TURN server's URL of that transport.
set -euo pipefail
cd "$(dirname "$0")"
TARGET=${1:-reference}
KEY=calltestkey
SECRET=calltestsecret-0123456789abcdef0123456789abcdef
[ -d node_modules ] || npm install --no-audit --no-fund >/dev/null
mkdir -p out
# With NETEM=<scenario ...|all> the call goes through the impairment relay of ../netem instead,
# scenario by scenario (netem.js), on ports of its own (39520 to 39539): the server's
# signalling on $PORT, its media on 127.0.0.1:39522, advertised as 127.0.0.2:39522, which the
# clients take to be the relay's.
PORT=39500
if [ -n "${NETEM:-}" ]; then PORT=39521; export PAGE_PORT=39538; fi

CONF_REFERENCE=reference-livekit.yaml
CONF_MUSIVUM=musivum.yaml
NAME=$TARGET${BROWSER:+-$BROWSER}
if [ "${TURN:-}" = 1 ]; then
  CONF_REFERENCE=turn-livekit.yaml
  CONF_MUSIVUM=turn-musivum.yaml
  NAME=$TARGET-turn
  export RELAY=1 TURN_TLS_PORT=39541 TURN_DOMAIN=turn.musivum.test
  CERTS=../../target/call-turn
  if [ ! -f "$CERTS/cert.pem" ]; then
    mkdir -p "$CERTS"
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 30 \
      -subj "/CN=$TURN_DOMAIN" -addext "subjectAltName=DNS:$TURN_DOMAIN" \
      -keyout "$CERTS/key.pem" -out "$CERTS/cert.pem" 2>/dev/null
  fi
fi

case "$TARGET" in
  reference)
    WANT=$(sed -n 's/^tag = "v\(.*\)"$/\1/p' ../../upstream.toml | head -n 1)
    LIVEKIT_SERVER=${LIVEKIT_SERVER:-$(command -v livekit-server || echo "$HOME/.cache/musivum/gobin/livekit-server")}
    HAVE=$("$LIVEKIT_SERVER" --version | awk '{print $NF}')
    [ "$WANT" = "$HAVE" ] || { echo "livekit-server is $HAVE, the pin is $WANT" >&2; exit 1; }
    "$LIVEKIT_SERVER" --config "${NETEM:+../netem/}$CONF_REFERENCE" > out/reference-server.log 2>&1 &
    URL=ws://127.0.0.1:$PORT
    ;;
  musivum)
    (cd ../.. && nice -n 10 cargo build --release -p musivum-server)
    ../../target/release/musivum-server --config "${NETEM:+../netem/}$CONF_MUSIVUM" > out/musivum-server.log 2>&1 &
    URL=ws://127.0.0.1:$PORT
    ;;
  *)
    echo "unknown target: $TARGET" >&2; exit 2 ;;
esac
PID=$!
PIDS="$PID"
trap 'kill $PIDS 2>/dev/null || true' EXIT
for _ in $(seq 50); do
  (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && break
  sleep 0.1
done
# The server's health check on /: 200 and "OK" once its node is up.
echo "health check: $(curl -s -w ' (%{http_code})' http://127.0.0.1:$PORT/ || true)"
# With TAP=1 the clients go through sigtap.js, which writes the signalling they exchange
# with the server to out/<target>-signalling.jsonl.
if [ "${TAP:-}" = 1 ]; then
  node sigtap.js 39511 127.0.0.1:$PORT "out/$TARGET-signalling.jsonl" &
  PIDS="$PIDS $!"
  sleep 0.5
  URL=ws://127.0.0.1:39511
fi
if [ -n "${NETEM:-}" ]; then
  node netem.js "$TARGET" "$URL" "$KEY" "$SECRET" $NETEM
  exit
fi
node run.js "$NAME" "$URL" "$KEY" "$SECRET"
