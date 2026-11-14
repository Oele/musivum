#!/usr/bin/env bash
# The runner cargo uses for this repository (.cargo/config.toml): runs the given program with
# a cap on its memory, and a test binary also with a cap on its running time.
#
#   MUSIVUM_RUN_MEMORY_KB   address space the program may have, in kB (default 8 GB;
#                           "unlimited" for none)
#   MUSIVUM_TEST_TIMEOUT    seconds a test binary may run (default 1800; 0 for no limit)
#
# Why: a mistake in a loop (or a mutation made on purpose to see a test fail) can allocate
# without end, and the machines this is developed on do other work too.
set -euo pipefail
MEMORY_KB=${MUSIVUM_RUN_MEMORY_KB:-8388608}
if [ "$MEMORY_KB" != unlimited ]; then
  ulimit -v "$MEMORY_KB"
fi
TIMEOUT=${MUSIVUM_TEST_TIMEOUT:-1800}
case "$1" in
  */deps/*)
    if [ "$TIMEOUT" != 0 ] && command -v timeout >/dev/null; then
      exec timeout --kill-after=10 "$TIMEOUT" "$@"
    fi
    ;;
esac
exec "$@"
