# musivum

A media server for voice and video calls that speaks [LiveKit](https://livekit.io)'s
protocol, written in Rust: a port of the part of LiveKit's server that receives each
participant's audio and video and forwards it to the others (its selective forwarding unit),
with what that needs around it to run on its own.

*Opus musivum* is the Latin name for mosaic work.

## Why

Matrix calls (MatrixRTC, as Element Call and Element X make them) send their media through a
LiveKit server, and today that means one implementation, in Go, run as a separate service
next to the homeserver. musivum is meant to be

- a second implementation that existing clients can use unchanged, because it speaks the same
  protocol;
- a library, so that a homeserver written in Rust can have calls built in instead of a second
  service to install, configure and proxy
  ([asaroton](https://github.com/Oele/asaroton) is the one it is written for);
- a standalone server as well, for everybody else.

## Where it stands

musivum is young: the first commit is from November 2026. It is a port of LiveKit's server
at v1.13.11 (`upstream.toml`), file by file, with LiveKit's own tests ported alongside; about
1,260 tests pass. It carries real calls:

- A scripted call of two participants with LiveKit's JavaScript SDK in headless Chrome
  (joining, simulcast video and audio, layer switching, data messages, active speakers,
  mute, connection quality, leaving) gives the same 20 observations against musivum as
  against LiveKit, and the same through musivum's TURN server alone.
- Behind a homeserver: Element Call in Chrome with two and three participants and for 30
  minutes, over UDP, over ICE over TCP only and through TURN only, and Element X on Android,
  all as with LiveKit on the same configuration.
- Compared with LiveKit on a degraded network (loss, delay, bandwidth caps, a blackout; three
  runs of seven scenarios each) it behaves the same within the runs' spread, with one
  difference in how fast the frame rate climbs back after a cap is lifted. Under load (up to
  100 subscribers of one track, 10 calls of 4) every subscriber gets all its media; musivum
  uses about half of LiveKit's memory, and from 13% less CPU (50 subscribers in a room) to
  8% more (calls of four).

Also in Firefox and in WebKit's engine (Playwright's builds) the scripted call behaves as
against LiveKit. Not tested yet: Safari itself, LiveKit's native SDKs (but Go's, in the load
generator), more than one machine. It runs in one homeserver's production since November 2026,
where the first real networks with address translation found two bugs in the WebRTC library
it stands on (fixed in patched copies). Expect rough
edges; `docs/port-status.md` has the details, file by file and measurement by measurement.

It was written by one person working with an AI coding assistant, Claude, which wrote most
of the code; the commit history says so commit by commit. What keeps it honest is LiveKit:
its tests, ported with each file and passing here, and the same calls and measurements run
against LiveKit itself, with the results kept in the repository.

## What it does, and what it leaves out

- **Signalling:** `/rtc` and `/rtc/v1` over a WebSocket, in protobuf or its JSON form, with
  `/rtc/validate`; LiveKit's access tokens.
- **Media:** ICE over UDP and TCP, IPv4 and IPv6, DTLS, SRTP and data channels on its own
  connection layer over [`rtc`](https://github.com/webrtc-rs/rtc) of webrtc-rs; simulcast and
  SVC layer selection, dynacast, RED, FlexFEC, retransmission, bandwidth estimation and the
  stream allocator, active speakers, connection quality, data tracks: LiveKit's code, ported.
- **TURN:** LiveKit's embedded TURN server, over UDP and TLS.
- **Around it:** the room API over Twirp, webhooks, Prometheus metrics, and LiveKit's own
  configuration file, read as LiveKit reads it.
- **Left out:** running as a cluster (Redis), agents, egress, ingress, SIP, WHIP, and the
  analytics LiveKit sends to its cloud.

## Running it

```sh
cargo build --release -p musivum-server
target/release/musivum-server --config livekit.yaml
```

`musivum-server` takes `livekit-server`'s command line, environment variables and
configuration file. Options it reads but cannot apply, and sections for what it leaves out,
are logged once at start; `docs/port-status.md` lists them. `musivum-server generate-keys`
and `create-join-token` work as LiveKit's do.

## As a library

The crate `musivum` is the server without an HTTP server of its own: its endpoints are
functions over the `http` crate's types, with a small async trait for the WebSocket.
`musivum-server` builds the binary from it with axum, and offers `musivum_server::embed` for
a program that wants musivum inside its own process:

```rust
let lk = Embedded::start(Config::new(&yaml, true)?, EmbedOptions::default()).await?;
let app = axum::Router::new().nest("/livekit", lk.router());   // /rtc, /rtc/v1, the room API
lk.create_room(CreateRoomRequest { name, empty_timeout: 300, ..Default::default() }).await?;
let mut events = lk.events();                                   // the webhook events, in process
// ...
lk.shutdown(std::time::Duration::from_secs(5)).await;
```

It starts everything `livekit-server` starts (media, TURN, the room manager, webhooks) on the
caller's runtime, without an HTTP listener of its own; the module's documentation says what
it runs on threads of its own. Calls, events and the advertised TURN port are as the
embedding program needs them.

## Working on it

- `scripts/check.sh` runs what has to pass: `cargo fmt --all --check`,
  `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace`. CI
  runs it on every push and pull request (`.github/workflows/check.yml`), and the browser
  call against musivum by hand and weekly (`call.yml`).
- `scripts/upstream-diff.sh <tag>` reports what a new release of LiveKit changed in the files
  musivum is ported from; `upstream.yml` runs it weekly and opens an issue per release.
  `docs/PORTING.md` ("Following upstream") is the procedure.
- `tests/call` runs the scripted browser call against `musivum` or `reference` (LiveKit at
  the pinned version); `tests/netem` and `tests/load` are the degraded network and the load
  test. They need Node, Chrome, and for the reference `livekit-server` and LiveKit's `lk`.
- `tests/embed` builds musivum as a program that uses rustls with aws-lc-rs would, so a
  workspace build also builds aws-lc-sys, which needs cmake and a C compiler.
- `docs/PORTING.md` is the design: what is taken from LiveKit, what it stands on, how it is
  checked. `docs/porting-guide.md` is how a Go file becomes a Rust file here,
  `docs/port-map.md` the survey of LiveKit's source, `docs/port-status.md` what is done,
  `docs/webrtc-library.md` the connection layer.
- What may go into the repository is in `CLAUDE.md`, the instructions the AI assistant works
  by and contributors too: code may be ported from LiveKit's Apache-2.0 repositories, pion,
  webrtc-rs and str0m, and specifications; never from anything under the GPL or AGPL, which
  includes Element Call and Synapse. Every other source is recorded in `docs/PORTING.md`.

## License

Apache License 2.0, see [LICENSE](LICENSE) and [NOTICE](NOTICE). musivum is a derived work of
LiveKit, © LiveKit, Inc., and is not affiliated with or endorsed by LiveKit, Inc. It contains
code ported from pion (MIT) and copies of two crates of webrtc-rs (MIT or Apache-2.0); NOTICE
names every source.
