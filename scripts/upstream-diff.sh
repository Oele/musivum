#!/usr/bin/env bash
# What changed upstream in the files musivum is ported from, between the pinned version of
# LiveKit (upstream.toml) and a newer one; docs/PORTING.md ("Following upstream") is the
# procedure. The work is done by upstream_diff.py, with Python's standard library only.
#
#   scripts/upstream-diff.sh <new-tag>                   the report, in target/upstream/<new-tag>/
#   scripts/upstream-diff.sh --from <tag> <new-tag>      the same from another tag than the pin
#   scripts/upstream-diff.sh --bump-unchanged <new-tag>  moves the headers whose origins did not
#                                                        change to <new-tag>
#
# It fetches (read only) LiveKit's tags into target/upstream/livekit.git and the Go modules
# that moved into target/upstream/gomod. Needs git, Python 3.11 or later, and go.
set -euo pipefail
exec python3 -I "$(dirname "$0")/upstream_diff.py" "$@"
