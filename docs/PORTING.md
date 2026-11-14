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

- Clients without UDP: LiveKit offers TCP and an embedded TURN server over TLS. `rtc` has
  ICE over TCP; a TURN server would have to be found or built.
- Proposing the padding size field to webrtc-rs, so that `third_party/rtc-rtp` can go.
- Whether the server binary should accept LiveKit's own configuration file, so that it can
  be swapped in for `livekit-server` without changes.
