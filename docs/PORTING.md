# The port

The plan. It was written when the project was set up, on 2026-11-14, and corrected the same
day from the survey of LiveKit's source: `docs/port-map.md` is the map, file by file, and
`docs/port-status.md` says what is done.

## What musivum is to be

A media server that LiveKit's client SDKs can hold a call through without knowing that it is
not LiveKit: join a room, publish audio, video and screen sharing with simulcast, receive the
others' tracks in a quality that fits the connection, data messages, and reconnecting. The
media stays end-to-end encrypted where the clients encrypt it; the server forwards it.

It is a library first, with a server binary around it.

## What is taken from LiveKit, and what is not

LiveKit's server at v1.13.11 is about 85,000 lines of Go that people wrote, and about 48,000
lines of tests (whole files, generated fakes not counted).

| Part of LiveKit's server | Size | Tests | In musivum |
|---|---|---|---|
| `pkg/sfu`: receiving, buffering and forwarding media; choosing layers; estimating bandwidth | 35k lines | 16.5k | **Ported**, with its tests. This is the reason for porting at all. It needs pion only for packet types and at four seams. |
| `pkg/rtc`: rooms, participants, tracks, subscriptions, the signalling messages | 26k | 7.7k | **Ported where it is logic** (who gets what, when); the parts that are wiring to the WebRTC library (`transport.go` above all) are written for the Rust one. |
| `pkg/service`: the WebSocket and HTTP endpoints, the room API, webhooks, tokens | ~12k | ~6k | **Written new and small**: one node, the signalling endpoint, and the handful of API calls a Matrix homeserver or token service makes. |
| `pkg/routing`, `pkg/agent`, `pkg/telemetry`, the rest | ~13k | ~8k | **Left out**: running as a cluster, agents, analytics. So are recording, ingest and telephony, which are separate programs upstream. |

LiveKit's protocol definitions (`livekit/protocol`, Protocol Buffers) are used as they are.
Two more repositories of LiveKit's have code that the above stands on and that is ported with
it: `livekit/mediatransportutil` (about 2,800 lines and 2,200 of tests) and parts of
`livekit/protocol` (mime types, tokens, SDP helpers).

## What it stands on

LiveKit is written against pion, the WebRTC library for Go (connection setup, DTLS, SRTP,
RTP and RTCP, data channels: far more code than LiveKit itself). That is not ported. musivum
stands on **`rtc`**, the protocol core of webrtc-rs (MIT or Apache-2.0): no I/O of its own,
pion's vocabulary (peer connection, transceivers, interceptors), and, what decided it, it
hands over RTP and RTCP as they are and leaves retransmission, feedback, bandwidth estimation
and pacing to LiveKit's ported code. str0m, the other candidate, does those itself.
`docs/webrtc-library.md` has the experiment that was run, what `rtc` lacks (one field in its
RTP packet, carried as a patch in `third_party/rtc-rtp`) and what was not tested.

## Sources used beyond the listed ones

`CLAUDE.md` lists what may be read and ported from. Used besides, each after its licence
was checked:

- **golang-jwt/jwt v5.3.1** (MIT), the JWT library LiveKit's `auth` package is built on:
  parsing and validating a token in `protocol/auth/jwt.rs` follow its `parser.go`,
  `validator.go`, `types.go`, `token.go` and `hmac.go`. Its notice is in `NOTICE`.

- **jxskiss/base62 v1.1.0** (MIT), which LiveKit's protocol uses for hashed ids and
  secrets: its encoder is ported in `protocol/utils/base62.rs`. Its notice is in `NOTICE`.
- **elliotchance/orderedmap v3.1.1** (MIT), which LiveKit's VP8 munger keeps its caches
  in, and **counterfeiter** (MIT), which generates the fakes LiveKit's tests use: both read
  to see what they do, nothing copied (`sfu/codecmunger/orderedmap.rs` and
  `crates/musivum-fakes` are written new).
- **d5/tengo v2.17.0** (MIT), the script language LiveKit's per-client rules are written
  in: read to see what its operators do on the types the rules use; nothing copied
  (`clientconfiguration/match.rs` gives the client's fields those comparisons).
- **golang.org/x/mod** `semver` and **google.golang.org/protobuf** (both BSD-3-Clause): read
  for how versions are compared and what a merge into nothing does; nothing copied
  (`clientconfiguration/semver.rs` is written new and held against the original by a test).
- **pion/ice** as LiveKit builds it (`livekit/ice` v4.4.0-warp.3, MIT): the rules by which
  a node's advertised addresses follow from its NAT 1:1 mappings (`gather.go`,
  `external_ip_mapper.go`) are followed in `mediatransportutil/rtcconfig/webrtc_config.rs`,
  reduced to what that package configures.
- **gopkg.in/yaml.v3 v3.0.1**: its decoder (`decode.go`, `resolve.go`, Apache-2.0, Canonical)
  is followed in `config/yaml.rs`, so that a LiveKit configuration file is read as LiveKit
  reads it (which scalars are bools and numbers, decoding onto defaults, strict mode). Its
  files that derive from libyaml (MIT) were not used; the YAML syntax is read by
  `saphyr-parser`. Its notice is in `NOTICE`.
- **gorilla/websocket v1.5.3** (BSD-2-Clause), the WebSocket library LiveKit's signalling
  endpoint uses: how it answers control frames, what it does at a close and the texts of
  its close errors are followed in `service/wsprotocol.rs` and `service/errors.rs`, so that
  a client sees the same. Its notice is in `NOTICE`.
- **ua-parser/uap-go** (Apache-2.0; its `uaparser/` MIT): read for the shapes of its results
  and `ToVersionString`. Its regular expressions (uap-core) are not used: `service/useragent.rs`
  is a small recogniser written new, held against the original's answers for 41 user agents.
- **mackerelio/go-osstat v0.2.8** (Apache-2.0, Hatena), which LiveKit's `hwstats` reads the
  system's load with: its Linux readers of `/proc/loadavg`, `/proc/stat` and `/proc/meminfo`
  are ported in `protocol/utils/hwstats/osstat.rs`. Its notice is in `NOTICE`.
- **frostbyte73/core v0.1.1** (MIT), small helpers LiveKit uses: its `Throttle` is ported in
  `go/core/throttle.rs`, its `Pool` (with the part of `Fuse` it uses) in
  `go/core/pool.rs`. Its notice is in `NOTICE`.
- **hashicorp/go-retryablehttp v0.7.8** (MPL-2.0), the HTTP client LiveKit sends webhooks
  with: its source was **not** opened, nor go-cleanhttp's. `protocol/webhook/retryablehttp.rs`
  is written new from its documentation (the README and the godoc of `NewClient`,
  `DefaultRetryPolicy`, `DefaultBackoff`, `ErrorHandler`) and from a Go test that called only
  its exported API at the pin and watched what it did against test servers.
- **livekit/psrpc v0.8.2** (Apache-2.0), LiveKit's RPC between nodes: read for what its
  local bus does for a single node; `routing/bus.rs` stands in its place, written new.
- **urfave/cli v3.11.0** (MIT), the command line library of LiveKit's `cmd/server`: its source
  was not read; what `crates/musivum-server` does with flags and the environment was
  observed by running LiveKit's `getConfig` at the pin.
- **uber-go/zap v1.28.0** (MIT): `zapcore/level.go` read for the level names the server's
  logger accepts (`musivum-server/src/logger.rs`, written new), with the protocol logger's
  `InitFromConfig` and `ResolveComponentLevel`.
- **gorilla/websocket v1.5.3**, further: the read limit (`ErrReadLimit`, close 1009) and
  `handleProtocolError` (close 1002) are followed in `musivum-server/src/service/axumws.rs`.
- **Howard Hinnant's `civil_from_days`** (public domain), for the date in the health check.
- **twitchtv/twirp v8.1.3** (Apache-2.0, Twitch), the RPC protocol of LiveKit's room API:
  its specification (`PROTOCOL.md`) and `errors.go`, `server_options.go`, `context.go`,
  `interceptors.go`, with the code protoc-gen-twirp generated in LiveKit's protocol
  (`livekit_room.twirp.go` and its shared helpers), are followed in `service/twirp.rs`. Its
  notice is in `NOTICE`.
- **google.golang.org/protobuf v1.36.12** (BSD-3-Clause): `encoding/protojson` read for how
  the room API's JSON is written and read; nothing copied (`service/protojson.rs`, new).
- **google.golang.org/grpc v1.84.0** (Apache-2.0): `status.FromError` read for how LiveKit's
  `xtwirp` maps an error.
- **Go's standard library** (BSD-3-Clause) was read for what the originals call
  (`math/rand/v2`, `sort.Search`). Nothing of it is in the repository: a draft that had
  translated its ChaCha8 generator was removed, and identifiers take their random numbers
  from the `rand` crate.

## Order of work

1. **Survey.** Read `pkg/sfu` and `pkg/rtc` and write `docs/port-map.md`: for every upstream
   file, whether it is ported, rewritten or left out, what it depends on, and what its tests
   need. Read which version of the protocol the pinned release speaks.
2. **`pkg/sfu`**, leaves first, each file with its tests, in the layers of the port map:
   what it stands on in `mediatransportutil` and `protocol`, then statistics and bandwidth
   estimation, the buffer, layer selection, and last the forwarder, the receiver and the
   down track. It needs no network and its tests say when it is right.
3. **The WebRTC library** (chosen: `rtc`) and the wiring to it: the peer connection
   (`pkg/rtc/transport.go`) and the logic of `pkg/rtc`.
4. **Signalling**: the WebSocket endpoint and the messages a call needs.
5. **A first call**: two browsers with LiveKit's JavaScript SDK, then Element Call.
6. **Rooms, the API and webhooks** that a homeserver needs; building it into asaroton behind
   a setting.
7. **Quality**: against LiveKit on a degraded network, see below.

## How it is checked

- **The ported tests.** A ported file is done when its ported tests pass.
- **Real clients against both servers.** The same scripted call (LiveKit's JavaScript SDK in
  a headless browser, LiveKit's Rust SDK, Element Call) against LiveKit and against musivum;
  what the clients observe has to match.
- **A degraded network.** `NETEM=all tests/call/run.sh <target>` runs the scripted call with
  all media through an impairment relay in user space (`tests/netem`, `netem-relay`): loss
  at random and in bursts, delay with jitter, and a bandwidth cap with a queue, per client
  and direction, from a fixed seed. The server advertises 127.0.0.2:39522, the clients point
  that candidate at the relay and keep their own candidates back, and each scenario ends by
  checking the browsers' packet counts against the relay's. Seven scenarios of 60 s: clean,
  2% loss, 5% loss in bursts, 150 ± 30 ms delay, the subscriber's downlink capped to
  500 kbit/s for 20 s, the publisher's uplink capped to 1000 kbit/s for 20 s, a 3 s
  blackout. Once a second Chrome's statistics give the layer and frame rate a subscriber
  receives, its freezes, losses, NACKs, PLIs, jitter buffer delay and round-trip time, and
  from those the time to get back to its layer and frame rate. LiveKit's results are in
  `tests/netem/baseline/`; `node tests/netem/compare.js baseline/reference musivum` marks
  where musivum freezes more, recovers later, or gets a lower layer or frame rate. Until
  nothing is marked, LiveKit stays the default wherever both can be used. A run on a busy
  machine is flagged and says little.

## Following upstream

`upstream.toml` pins the version that was ported. To move it: list what changed upstream
between the two tags in the files that were ported, apply those changes to the Rust files,
port the changed tests, move the pin. Ported files stay close to their originals for this.

## Open questions

- (Settled 2026-11-14: LiveKit's embedded TURN server is ported, from pion/turn, so that
  musivum stays a drop-in replacement; the owner's deployment uses it.)
- Proposing the padding size field and a sender's codecs to webrtc-rs, so that
  `third_party/rtc-rtp` and `third_party/rtc` can go.
- (Settled: the server reads LiveKit's own configuration file, `crate::config`. Sections for
  what musivum leaves out are accepted and ignored, with a log line each.)
