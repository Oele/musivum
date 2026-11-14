# Working on musivum

musivum is a port to Rust of the media-forwarding part of LiveKit's server. Read
`docs/PORTING.md` first: it has the scope, the order of work and how each step is checked.

## Rules of origin

What goes into this repository has to be free to be here under the Apache License 2.0.

- **May be read and ported from:** LiveKit's own repositories under Apache-2.0
  (`livekit/livekit`, `livekit/protocol`, `livekit/mediatransportutil`, `livekit/psrpc`; check
  the licence of any other one before its first use), pion (MIT), str0m (MIT), webrtc-rs
  (MIT or Apache-2.0), and the specifications (IETF RFCs, W3C WebRTC).
- **Must not be opened from a session that writes this code:** anything under the AGPL or
  GPL. That includes Element Call, `element-hq/lk-jwt-service`, FluffyChat and Famedly's
  Dart SDK, and Synapse. What such software does may be described from the outside (what a
  client sends, what a server answers); how it does it may not be looked up in its source.
- Anything else: check its licence first, and write down in `docs/PORTING.md` that it was
  used.

## How a ported file looks

- It starts with a header that names its origin and what was changed:

  ```rust
  // Ported from LiveKit, pkg/sfu/rtpmunger.go at v1.13.11.
  // Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
  // Changes: translated from Go to Rust; <what else differs from the original, if anything>.
  ```

- It stays close to the original: the same names (in Rust's spelling), the same order of
  functions, the same comments where they still apply. The next version of LiveKit has to be
  followable as a diff against the file it was ported from.
- Its tests are ported with it, in the same step, and pass before the step counts as done.
  A behaviour of the original that looks wrong is ported as it is and noted, not fixed on the
  way.
- A file that is written new for musivum has no such header.

## Other rules

- `reference/livekit` is the pinned upstream source (`scripts/fetch-reference.sh`). It is not
  committed. The pin is `upstream.toml`.
- This repository may become public. No host names, addresses, account names or log extracts
  of real servers in code, comments, commit messages or documents.
- Before a commit: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace`.
- Do not publish to crates.io, make the repository public, or post in LiveKit's or anybody
  else's issue tracker without the owner saying so.
