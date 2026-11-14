# The port

This is the plan as it stood when the project was set up, on 2026-11-14, before LiveKit's
source had been read: the sizes below are measured from file sizes, nothing more. The first
piece of work is the survey that turns this page into a map.

## What musivum is to be

A media server that LiveKit's client SDKs can hold a call through without knowing that it is
not LiveKit: join a room, publish audio, video and screen sharing with simulcast, receive the
others' tracks in a quality that fits the connection, data messages, and reconnecting. The
media stays end-to-end encrypted where the clients encrypt it; the server forwards it.

It is a library first, with a server binary around it.

## What is taken from LiveKit, and what is not

LiveKit's server at v1.13.11 is about 85,000 lines of Go that people wrote, and about 48,000
lines of tests.

| Part of LiveKit's server | Size | Tests | In musivum |
|---|---|---|---|
| `pkg/sfu`: receiving, buffering and forwarding media; choosing layers; estimating bandwidth | ~32k lines | ~17k | **Ported**, with its tests. This is the reason for porting at all. |
| `pkg/rtc`: rooms, participants, tracks, subscriptions, the signalling messages | ~25k | ~10k | **Ported where it is logic** (who gets what, when); the parts that are wiring to the WebRTC library are written for the Rust one. |
| `pkg/service`: the WebSocket and HTTP endpoints, the room API, webhooks, tokens | ~12k | ~6k | **Written new and small**: one node, the signalling endpoint, and the handful of API calls a Matrix homeserver or token service makes. |
| `pkg/routing`, `pkg/agent`, `pkg/telemetry`, the rest | ~13k | ~8k | **Left out**: running as a cluster, agents, analytics. So are recording, ingest and telephony, which are separate programs upstream. |

LiveKit's protocol definitions (`livekit/protocol`, Protocol Buffers) are used as they are.

## What it stands on

LiveKit is written against pion, the WebRTC library for Go (connection setup, DTLS, SRTP,
RTP and RTCP, data channels: far more code than LiveKit itself). That is not ported. musivum
stands on a WebRTC library for Rust, and which one is the first decision to make:

- **str0m** (MIT): built for servers of this kind, without I/O of its own, one owner per
  connection. Its shape differs from pion's, so the ported logic meets it through an adapter.
- **webrtc-rs** (MIT or Apache-2.0): a port of pion to Rust with pion's interfaces, so the
  ported code would fit with fewer changes; to be checked how complete and how alive it is.

To be decided by a small experiment: the same slice of `pkg/sfu` wired to each, judged on how
much of the original structure survives and on what each library lacks (bandwidth
estimation, TCP and TURN candidates, simulcast and SVC, data channels).

## Order of work

1. **Survey.** Read `pkg/sfu` and `pkg/rtc` and write `docs/port-map.md`: for every upstream
   file, whether it is ported, rewritten or left out, what it depends on, and what its tests
   need. Read which version of the protocol the pinned release speaks.
2. **The parts that are pure logic**, leaves first, each with its tests: rewriting sequence
   numbers and timestamps, reading the codecs' headers, choosing layers, the stream
   allocator. These need no network and their tests say when they are right.
3. **The WebRTC library**, chosen by the experiment above, and the wiring to it.
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

- Which WebRTC library (above).
- Clients without UDP: LiveKit offers TCP and an embedded TURN server over TLS. What the
  chosen library offers decides how much of that has to be built.
- Whether the server binary should accept LiveKit's own configuration file, so that it can
  be swapped in for `livekit-server` without changes.
