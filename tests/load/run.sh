#!/usr/bin/env bash
# Load test: LiveKit's own load generator (`lk perf load-test`, livekit-cli, Apache-2.0)
# against one server, scenario by scenario (load.py has them), one at a time. Each run writes
# results/<target>-<scenario>.json and prints a one-line summary; `python3 -I compare.py`
# puts both targets side by side.
#
#   ./run.sh reference [a b c d]   LiveKit itself (livekit-server, the pinned version)
#   ./run.sh musivum [a b c d]     target/release/musivum-server, built here
#
# Without scenarios it runs a, b and c; d (100 subscribers) is asked for by name.
#
# lk is release v2.19.4 of livekit-cli, LiveKit's binary for linux/amd64 from its GitHub
# release, checked against the release's checksums.txt and put in ~/.cache/musivum/gobin (or
# $LK). A `go install` of the module does not work: its test media are Git LFS objects and
# its PortAudio sources a git submodule, neither of which is in the module.
#
# Everything is on 127.0.0.1, ports 39560 to 39562. load.py has the safety rules (temperature,
# load, memory) and stops a run that breaks them.
set -euo pipefail
cd "$(dirname "$0")"
TARGET=${1:?usage: run.sh reference|musivum [scenario ...]}
shift
SCENARIOS=${*:-a b c}
LK=${LK:-$HOME/.cache/musivum/gobin/lk}
# lk must never find the live server: no LIVEKIT_* from the environment, and every call to it
# names its URL and key (load.py).
unset LIVEKIT_URL LIVEKIT_API_KEY LIVEKIT_API_SECRET

case "$TARGET" in
  reference)
    WANT=$(sed -n 's/^tag = "v\(.*\)"$/\1/p' ../../upstream.toml | head -n 1)
    HAVE=$(livekit-server --version | awk '{print $NF}')
    [ "$WANT" = "$HAVE" ] || { echo "livekit-server is $HAVE, the pin is $WANT" >&2; exit 1; }
    SERVER=$(command -v livekit-server)
    CONFIG=reference-livekit.yaml
    ;;
  musivum)
    (cd ../.. && nice -n 10 cargo build --release -p musivum-server)
    SERVER=$(cd ../.. && pwd)/target/release/musivum-server
    CONFIG=musivum.yaml
    ;;
  *)
    echo "unknown target: $TARGET" >&2; exit 2 ;;
esac

ulimit -d 8388608
for s in $SCENARIOS; do
  timeout 600 python3 -I load.py "$TARGET" "$s" "$SERVER" "$CONFIG" "$LK"
done
