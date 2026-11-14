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
  are ported in `routing/hwstats/osstat.rs`. Its notice is in `NOTICE`.
- **frostbyte73/core v0.1.1** (MIT), small helpers LiveKit uses: its `Throttle` is ported in
  `routing/hwstats/cpu.rs`, its `Pool` (with the part of `Fuse` it uses) in
  `protocol/webhook/pool.rs`. Its notice is in `NOTICE`.
- **hashicorp/go-retryablehttp v0.7.8** (MPL-2.0), the HTTP client LiveKit sends webhooks
  with: its source was **not** opened, nor go-cleanhttp's. `protocol/webhook/retryablehttp.rs`
  is written new from its documentation (the README and the godoc of `NewClient`,
  `DefaultRetryPolicy`, `DefaultBackoff`, `ErrorHandler`) and from a Go test that called only
  its exported API at the pin and watched what it did against test servers.
- **livekit/psrpc v0.8.2** (Apache-2.0), LiveKit's RPC between nodes: read for what its
  local bus does for a single node; `routing/bus.rs` stands in its place, written new.
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
- **A degraded network.** Loss, delay and a bandwidth cap between client and server; freezes,
  the quality that is chosen and the time to recover are measured for both servers. Until
  musivum is as good here, LiveKit stays the default wherever both can be used.

## Following upstream

`upstream.toml` pins the version that was ported. To move it: list what changed upstream
between the two tags in the files that were ported, apply those changes to the Rust files,
port the changed tests, move the pin. Ported files stay close to their originals for this.

## Open questions

- Clients without UDP: LiveKit offers TCP and an embedded TURN server over TLS. ICE over
  TCP is in the connection layer now; a TURN server would have to be found or built.
- Proposing the padding size field and a sender's codecs to webrtc-rs, so that
  `third_party/rtc-rtp` and `third_party/rtc` can go.
- (Settled: the server reads LiveKit's own configuration file, `crate::config`. Sections for
  what musivum leaves out are accepted and ignored, with a log line each.)
