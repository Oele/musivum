#!/usr/bin/env bash
# What has to pass before a commit (CLAUDE.md), and what CI runs: formatting, clippy with
# warnings as errors, and all tests of the workspace.
#
# `cargo test --workspace` builds tests/embed, which builds aws-lc-sys: that needs cmake and a
# C compiler. One test waits a minute; the whole run takes a few minutes after the build.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
