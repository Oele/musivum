#!/usr/bin/env bash
# Fetches the pinned version of LiveKit (upstream.toml) into reference/livekit, which is what
# the port is made from and checked against. The directory is not part of this repository.
set -euo pipefail
cd "$(dirname "$0")/.."
value() { sed -n "s/^$1 = \"\(.*\)\"$/\1/p" upstream.toml | head -n 1; }
REPO=$(value repository); TAG=$(value tag); COMMIT=$(value commit)
DIR=reference/livekit
if [ -d "$DIR/.git" ] && [ "$(git -C "$DIR" rev-parse HEAD)" = "$COMMIT" ]; then
  echo "reference/livekit is at $TAG ($COMMIT)"
else
  rm -rf "$DIR"
  git clone --quiet --depth 1 --branch "$TAG" "$REPO" "$DIR"
  GOT=$(git -C "$DIR" rev-parse HEAD)
  [ "$GOT" = "$COMMIT" ] || { echo "the tag $TAG is at $GOT, upstream.toml says $COMMIT" >&2; exit 1; }
  echo "fetched $REPO at $TAG ($COMMIT) into $DIR"
fi
# The Go modules that version of LiveKit is built from (livekit/protocol,
# livekit/mediatransportutil, pion and the rest), at the versions its go.mod names, into
# reference/gomod. Parts of them are ported too; docs/port-map.md says which. Only the ones
# whose licence has been checked are to be opened: see CLAUDE.md.
if command -v go >/dev/null; then
  (cd "$DIR" && GOMODCACHE="$PWD/../gomod" GOFLAGS=-modcacherw go mod download)
  echo "LiveKit's Go modules are in reference/gomod"
else
  echo "go is not installed: reference/gomod was not fetched" >&2
fi
