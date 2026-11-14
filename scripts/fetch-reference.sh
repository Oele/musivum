#!/usr/bin/env bash
# Fetches the pinned version of LiveKit (upstream.toml) into reference/livekit, which is what
# the port is made from and checked against. The directory is not part of this repository.
set -euo pipefail
cd "$(dirname "$0")/.."
value() { sed -n "s/^$1 = \"\(.*\)\"$/\1/p" upstream.toml | head -n 1; }
REPO=$(value repository); TAG=$(value tag); COMMIT=$(value commit)
DIR=reference/livekit
if [ -d "$DIR/.git" ] && [ "$(git -C "$DIR" rev-parse HEAD)" = "$COMMIT" ]; then
  echo "reference/livekit is at $TAG ($COMMIT)"; exit 0
fi
rm -rf "$DIR"
git clone --quiet --depth 1 --branch "$TAG" "$REPO" "$DIR"
GOT=$(git -C "$DIR" rev-parse HEAD)
[ "$GOT" = "$COMMIT" ] || { echo "the tag $TAG is at $GOT, upstream.toml says $COMMIT" >&2; exit 1; }
echo "fetched $REPO at $TAG ($COMMIT) into $DIR"
