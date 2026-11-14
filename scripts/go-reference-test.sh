#!/usr/bin/env bash
# Runs LiveKit's own Go tests at the pinned version, as the reference for the port:
#   scripts/go-reference-test.sh ./pkg/sfu/...
#   scripts/go-reference-test.sh -run TestRTPMunger -v ./pkg/sfu
# The Go modules LiveKit depends on are kept under reference/gomod, apart from the user's
# own module cache, so that what is read as a reference is what the pin names.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -d reference/livekit ] || scripts/fetch-reference.sh
export GOMODCACHE="$PWD/reference/gomod"
export GOCACHE="${XDG_CACHE_HOME:-$HOME/.cache}/musivum/gocache"
export GOFLAGS=-modcacherw
cd reference/livekit
exec go test "$@"
