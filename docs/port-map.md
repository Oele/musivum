# The port map

What every file of LiveKit's `pkg/sfu` and `pkg/rtc` at v1.13.11 becomes in musivum, what it
depends on and what its tests need. Written from the source on 2026-11-14; `docs/PORTING.md`
has the plan this page belongs to, `docs/port-status.md` has what is done.

Line counts are whole files (licence header, comments and blank lines included), as `wc -l`
gives them. "Tests" is the `_test.go` file next to a source file where there is one.

## What the survey found

1. **`pkg/sfu` does not need a WebRTC library to be ported.** It is about 35,100 lines with
   16,500 lines of tests (the generated fakes, 5,100 lines, not counted). Of pion it uses
   three kinds of things:
   - packets: `pion/rtp` (`Header`, `Packet`, header extensions by id) and `pion/rtcp` (the
     packet types and their marshalling). These are data and stay data in Rust.
   - small descriptions: `webrtc.RTPCodecParameters`, `RTPCodecCapability`, `RTPCodecType`,
     `PayloadType`, `SSRC`, `RTCPFeedback`, `RTPHeaderExtensionParameter`, `RTPParameters`,
     and a few SDP constants (the URIs of header extensions). Plain structs; musivum has
     its own copies, ported from pion.
   - four seams where pion calls in or is called: `webrtc.TrackLocal` (`Bind`, `Unbind`)
     with `TrackLocalWriter` and `TrackLocalContext` in `downtrack.go`; `webrtc.RTPReceiver`
     with `TrackRemote` in `receiver.go` and `track_remote.go`; `webrtc.RTPTransceiver` in
     `downtrack.go` (only to read the negotiated codecs and the mid); and the `interceptor`
     package for the two interceptors in `pkg/sfu/interceptor`. These become traits, and
     the WebRTC library that is chosen implements them.

   So step 2 of the plan is all of `pkg/sfu`, not only its leaves, and the choice of the
   WebRTC library can be made while that is going on.

2. **The WebRTC library attaches in `pkg/rtc`, in few files.** `transport.go` (3,533 lines)
   is LiveKit's wrapper around a pion `PeerConnection` and is where nearly all of it is:
   offer and answer, ICE candidates and restarts, data channels, the interceptor registry,
   the bandwidth estimator's wiring. `transportmanager.go`, `participant_sdp.go`,
   `mediaengine.go`, `config.go` and `types/ice.go` are the others. The rest of `pkg/rtc`
   (rooms, participants, subscriptions, permissions, dynacast, the supervisor, data tracks)
   is logic with pion's types passing through.

3. **LiveKit does its own media transport control and expects the library not to.** It
   answers NACKs itself from its own packet history (`sequencer.go`, `bucket`), generates
   NACKs and transport-wide feedback itself (`buffer`, `twcc.Responder`), estimates
   bandwidth itself (`bwe/sendsidebwe`, `bwe/remotebwe`), paces and probes itself
   (`pacer`, `ccutils/prober.go`), decodes FlexFEC itself, and writes and reads RTX
   itself. It registers hardly any of pion's interceptors. A Rust library that insists on
   doing these things (retransmission, feedback, estimation) inside would fight the ported
   code; one that hands over RTP and RTCP as they are fits. This is the main criterion for
   the experiment in step 3.

4. **The pinned release speaks protocol version 18** (`pkg/rtc/types/protocol_version.go`,
   `CurrentProtocol = 18`), with the protocol definitions of `livekit/protocol` at
   `v1.53.1-0.20261023175527-46332a036e25`. Version 16 added moving participants, 17 the
   packet trailer, 18 negotiated data channels. Clients name their version when they
   connect; the server answers each according to what its version supports, which is the
   list in that file.

5. **Code outside `livekit/livekit` that has to come along**, all Apache-2.0:
   `livekit/mediatransportutil` (about 2,800 lines with 2,200 of tests: sequence number
   unwrapping, the packet bucket, the NACK queue, the TWCC responder, RED, codec header
   parsing for VP8, H.264, H.265 and key frame detection, a one-way-delay estimator) and
   parts of `livekit/protocol` (the mime types, token verification and grants, SDP helpers,
   identifiers, the signalling message encoding, and the Protocol Buffers definitions).

6. **Upstream's own tests pass here.** `go test ./pkg/sfu/... ./pkg/rtc/...` at the pin, Go
   1.26.8, on this machine: see "The reference run" at the end.

## Corrections to the plan

- `pkg/sfu` without its leaves is not a meaningful unit: step 2 ports the whole of it, in
  the layers below. The sizes in `docs/PORTING.md` were close (35,100 lines and 16,500 of
  tests against the estimated 32,000 and 17,000; `pkg/rtc` 26,100 and 7,700 against 25,000
  and 10,000).
- There are three candidates for the WebRTC library, not two: webrtc-rs has become two
  things since the plan's description of it, `rtc` (0.21, a protocol core without I/O, new)
  and `webrtc` (0.21, the asynchronous interface on top of it; the older 0.17 line, which
  was the port of pion, is in maintenance).
- The packet types are a decision of their own, needed before the library: see "Packets".

## Where things go

One library crate, with modules that mirror upstream's paths, so that a file and its origin
are found from each other without a table:

| Upstream | In musivum |
|---|---|
| `livekit/livekit` `pkg/sfu/x/y.go` | `crates/musivum/src/sfu/x/y.rs` |
| `livekit/livekit` `pkg/rtc/x.go` | `crates/musivum/src/rtc/x.rs` |
| `livekit/livekit` `pkg/utils`, `pkg/config` | `crates/musivum/src/utils`, `src/config` |
| `livekit/mediatransportutil` `pkg/x/y.go` | `crates/musivum/src/mediatransportutil/x/y.rs` |
| `livekit/protocol` `x/y.go` | `crates/musivum/src/protocol/x/y.rs` |
| `livekit/protocol` `protobufs/*.proto` | `crates/musivum-protocol` (generated at build time) |
| pion's small types and constants | `crates/musivum/src/webrtc` |
| the server program | `crates/musivum-server` |

A Go `_test.go` file becomes a `#[cfg(test)] mod tests` in a file of the same name with
`_test` appended (`rtpmunger_test.rs`), kept next to its source, so that upstream's test
files can be followed as files too.

## How Go becomes Rust

Kept the same everywhere, so that a ported file reads like its original:

- A Go struct with its own `sync.Mutex` or `RWMutex` keeps a lock of its own: the fields the
  lock guards move into an inner struct behind `parking_lot::Mutex` or `RwLock`, and the
  methods take `&self`. A Go struct without a lock, used under its owner's lock, gets
  `&mut self` methods.
- `atomic.X` fields stay atomics. A pointer shared between owners is an `Arc`; a back
  reference is a `Weak` or a callback.
- An interface is a trait; a callback field (`onX func(...)`) is a boxed closure.
- A goroutine is a task on the runtime (tokio) or, where upstream means a dedicated worker,
  a thread. Timers are the runtime's.
- `logger.Logger` with its key and value pairs is `protocol::logger::Logger`, written to
  `tracing`; arguments are only formatted when the level is enabled.
- Counterfeiter fakes (`sfufakes`, `typesfakes`, `transportfakes`, `routingfakes`) are not
  ported as generated code: a test gets a hand-written fake of the trait it needs, in
  `testutils`.
- Prometheus counters (`pkg/telemetry/prometheus`) become calls into one small `metrics`
  module that counts and can be read; no Prometheus dependency in the library.

## Packets

`pion/rtp` and `pion/rtcp` are used by value throughout `pkg/sfu`. In Rust:

- RTP: `rtc-rtp` (webrtc-rs, MIT or Apache-2.0), which is the port of `pion/rtp` with the
  same shape (`Header`, `Packet`, extensions by id, marshal and unmarshal). To be confirmed
  when the first file that needs it is ported; if it does not hold up, the fallback is a
  port of `pion/rtp`'s header code into `src/webrtc` (about 600 lines, MIT).
- RTCP: `rtc-rtcp`, the port of `pion/rtcp`, on the same terms.

These two crates have small dependencies (`bytes`, `rtc-shared`) and do not commit musivum
to the rest of webrtc-rs.

## The layers of `pkg/sfu`, in the order they are ported

Each layer only needs the ones above it.

0. **No dependencies inside LiveKit.** `mediatransportutil` (`utils` wraparound, `helpers`,
   `bucket`, `nack`, `twcc`, `red`, `codec`, `latency`); `protocol` (`codecs/mime`,
   `utils/mono`, `utils/rtputil`, the logger); `sfu/utils`, `sfu/audio`,
   `sfu/packettrailer`, `sfu/rtpextension/*`, `sfu/ccutils`, `sfu/flexfec`,
   `sfu/datachannel/bitrate.go`.
1. `sfu/rtpstats`; `sfu/bwe` with `remotebwe` and `sendsidebwe`; the parts of `sfu/buffer`
   that stand alone (`videolayer`, `videolayerutils`, `fps`, `frameintegrity`,
   `dependencydescriptorparser`, `datastats`, `rtcpreader`).
2. `sfu/buffer` proper (`buffer_base`, `buffer`, `factory`); `sfu/streamtracker`;
   `sfu/codecmunger`; `sfu/videolayerselector` with `temporallayerselector`; `sfu/pacer`;
   `sfu/connectionquality`.
3. `pkg/sfu` itself: `rtpmunger`, `sequencer`, `rtxbudget`, `playoutdelay`, `forwardstats`,
   `streamtrackermanager`, `forwarder`, `receiver_base`, `receiver`, `redreceiver`,
   `redprimaryreceiver`, `downtrack`.
4. `sfu/streamallocator`.

## `pkg/sfu`

P = ported with its tests. S = ported, with a seam (trait) where pion was. N = written new
for the Rust library, or not needed. Sizes: source lines / test lines.

### `pkg/sfu` (the package itself)

| File | Size | | Depends on | Notes |
|---|---|---|---|---|
| `downtrack.go` | 2884 / (1484) | S | forwarder, sequencer, rtpstats, pacer, connectionquality, bwe, ccutils, rtpextension, packettrailer | The subscriber side of a track: receives `ExtPacket`s from a receiver, has the forwarder translate them, writes RTP, answers NACKs from the sequencer, sends padding and probes, handles RTCP from the subscriber. Seams: `TrackLocal` (`Bind`, `Unbind`), `TrackLocalWriter`, `TrackLocalContext`, `RTPTransceiver`, the RTCP reader. Its test is `downtrack_downstream_integration_test.go`, which runs a down track over pion's virtual network (`pkg/testutils/vnettest`) against both estimators: ported after step 3, onto whatever the chosen library offers for a simulated network. |
| `forwarder.go` | 2624 / 3131 | P | buffer, codecmunger, videolayerselector, rtpmunger, rtpstats | Which layer a subscriber gets and how packets are rewritten for it; the allocation arithmetic the stream allocator uses. The largest test file of the port. |
| `receiver_base.go` | 1245 / 289 (`receiver_test.go`) | P | buffer, streamtrackermanager, rtpstats, audio, bucket, sfu/utils (down track spreader) | The publisher side of a track, without the WebRTC part: buffers per layer, forwarding to down tracks, PLI throttling, bitrates per layer. |
| `receiver.go` | 370 | S | receiver_base, connectionquality | `WebRTCReceiver`: `receiver_base` plus pion's `RTPReceiver` and `TrackRemote`. |
| `track_remote.go` | 52 | S | | The `TrackRemote` interface and a version made from SDP. Becomes the trait. |
| `redreceiver.go` | 274 / 508 | P | buffer, bucket | Wraps a receiver of Opus to give RED to subscribers that want it. |
| `redprimaryreceiver.go` | 204 / (in `redreceiver_test.go`) | P | buffer, mtu/red | The other direction: RED from the publisher, plain Opus to subscribers. |
| `rtpmunger.go` | 353 / 587 | P | buffer (`ExtPacket`), sfu/utils (range map) | Rewrites sequence numbers and timestamps for one down track. |
| `sequencer.go` | 510 / 446 | P | abscapturetime, sfu/utils | Remembers what was sent under which sequence number, to answer NACKs. |
| `streamtrackermanager.go` | 658 | P | streamtracker, buffer | One stream tracker per layer; says which layers are live and at what bitrate. No test file of its own (covered through `receiver` and `forwarder`). |
| `forwardstats.go` | 367 / 410 | P | | Forwarding latency and jitter, sampled. Prometheus calls become `metrics`. |
| `playoutdelay.go` | 205 / 81 | P | rtpextension/playoutdelay, rtpstats | Chooses the playout delay to signal to a subscriber. |
| `rtxbudget.go` | 113 / 76 | P | | Limits how much retransmission a subscriber may ask for. |
| `interfaces.go` | 189 | P | | `TrackReceiver`, `TrackSender`, `REDTransformer` and the listeners, as traits. |
| `sfu.go`, `errors.go` | 58 | P | | Packet and header pools (kept as pools only if they still pay in Rust), errors. |
| `export_test.go` | 104 | P | | Test-only access to internals; in Rust the tests are in the module and need less of it. |

### `pkg/sfu/buffer`

| File | Size | | Depends on | Notes |
|---|---|---|---|---|
| `buffer_base.go` | 1643 / 781 (`buffer_test.go`) | P | bucket, nack, codec, rtpstats, audio, dependencydescriptor, abscapturetime | The receive side of one stream: unwraps sequence numbers, stores packets, makes `ExtPacket`s (key frame, layers, codec header), NACKs, audio level, sender reports. |
| `buffer.go` | 844 / 716 (`buffer_fec_test.go`) | P | buffer_base, flexfec, twcc | Adds what a live stream needs: pending packets before bind, RTX repair streams, FlexFEC recovery, TWCC, REMB. The FEC test generates FEC with pion's encoder (`interceptor/pkg/flexfec`): the test vectors are produced once from Go and checked in, or the encoder is ported for tests. |
| `factory.go` | 183 | S | buffer, rtcpreader | Buffers and RTCP readers per SSRC, handed to pion as its packet buffers (`packetio`). With a library that delivers packets by callback this is a plain map. |
| `rtcpreader.go` | 63 | P | | |
| `dependencydescriptorparser.go` | 389 / 290 | P | dependencydescriptor, mtu/utils, codec | Per stream state for reading the AV1 dependency descriptor. |
| `fps.go` | 745 / 439 | P | | Frame rate per temporal layer, for VP8/VP9 picture ids and for dependency descriptors. |
| `frameintegrity.go` | 238 / 147 | P | dependencydescriptor | |
| `videolayerutils.go` | 520 / 892 | P | mime | Rid, quality and spatial layer conversions for every publishing mode. |
| `videolayer.go`, `streamstats.go`, `datastats.go` | 185 / 66 | P | rtpstats | |

### The other packages of `pkg/sfu`

| Package | Size | | Depends on | Notes |
|---|---|---|---|---|
| `utils` | 568 / 471 | P | | `rangemap`, `downtrackspreader`, `debounce`, `helpers` (which also has two functions on pion's interceptor types: those go with the seam). `race.go` and `norace.go` are Go build tags and have no counterpart. |
| `audio` | 197 / 158 | P | | Audio level over a window, from the RTP header extension. |
| `packettrailer` | 48 / 182 | P | | Finds LiveKit's packet trailer (protocol 17) at the end of a payload. |
| `rtpextension/playoutdelay` | 72 / 57 | P | | |
| `rtpextension/abscapturetime` | 119 | P | mtu helpers | |
| `rtpextension/dependencydescriptor` | 1478 / 391 | P | | Reads and writes the AV1 dependency descriptor, bit by bit. Has a benchmark file (247 lines of the 391) that becomes a `criterion` bench or is left. |
| `flexfec` | 524 / 739 | P | rtp | FlexFEC-03 decoder. Tests encode with pion's encoder: see `buffer.go`. |
| `ccutils` | 1016 | P | | Trend detector, prober, probe regulator. No tests upstream; exercised through `bwe` and the stream allocator. |
| `bwe` | 200 | P | ccutils | The estimator interface and a null one. |
| `bwe/remotebwe` | 875 | P | bwe, ccutils | Estimation from REMB and NACKs (for subscribers without transport-wide feedback). No tests upstream. |
| `bwe/sendsidebwe` | 2292 / 107 | P | bwe, ccutils, rtcp | LiveKit's send-side estimator, from transport-wide feedback. One small test. |
| `rtpstats` | 4477 / 332 | P | mtu helpers, mtu/utils, mtu/latency, rtcp | Receiver and sender statistics, RTCP reports, drift. |
| `streamtracker` | 1076 / 320 | P | buffer | Whether a layer is live, by packets, by frames or from dependency descriptors. |
| `codecmunger` | 729 / 640 | P | buffer, mtu/codec | Rewrites VP8 picture ids and TL0PICIDX; temporal layers from dependency descriptors. |
| `videolayerselector` | 1672 / 1008 | P | buffer, dependencydescriptor | Per packet: forward or drop, for simulcast, VP9 SVC and dependency descriptors. |
| `videolayerselector/temporallayerselector` | 245 / 219 | P | buffer | |
| `connectionquality` | 1178 / 1009 | P | rtpstats, buffer | The quality score a client is shown. |
| `pacer` | 654 / 108 | S | bwe, ccutils, rtp | Sends packets out at a pace; calls `TrackLocalWriter`. |
| `streamallocator` | 1840 | S | sfu, buffer, bwe, pacer, ccutils | Divides a subscriber's estimated bandwidth over its video tracks. No unit tests upstream (covered by the integration test of `downtrack`); uses `cc.BandwidthEstimator` from pion's interceptors only for the optional pion estimator, which is not carried over. |
| `datachannel` | 327 / 290 | S | | `bitrate.go` is logic (P). `datachannel_writer.go` wraps pion's data channel with a write deadline and belongs to the seam. |
| `interceptor` | 385 | N | | `rtx.go` (learns which SSRC repairs which from the first packets) and `unhandlesimulcast.go` (feeds simulcast streams that arrive without rid) are written as pion interceptors. The behaviour is kept; the form depends on the library. |
| `testutils` | 218 | P | | Builders for test packets. |
| `sfufakes` | 5085 | N | | Generated. See "How Go becomes Rust". |

## `pkg/rtc`

L = logic, ported with its tests. W = wiring to the WebRTC library: the behaviour is kept,
the code is written against the Rust library. X = left out.

| File | Size | | Notes |
|---|---|---|---|
| `room.go` | 2429 / 923 | L | A room: joining and leaving, who is told what, active speakers, data messages, connection quality updates. The parts for agents, egress and moving between nodes are left out. |
| `participant.go` | 4763 / 1416 | L, W | A participant: state, permissions, publishing (pending tracks, `AddTrack`, media tracks), the subscriber side, migration, ICE and connection state. The logic is ported; what it does with transceivers and SDP goes through the transport. Migration between nodes is left out. |
| `participant_signal.go` | 459 | L | The messages a participant sends to its client. |
| `participant_sdp.go` | 419 | W | Reads the publisher's offer and adjusts the answer (codec preferences, simulcast, RED, DTX, stereo). The decisions are ported, the SDP handling is against the library's SDP type. |
| `participant_data_track.go`, `datatrack.go`, `datadowntrack.go`, `datatrack_stats.go`, `updatatrackmanager.go` | 815 / 71 | L | Data tracks (a newer kind of data channel traffic with handles). After the first call. |
| `participant_data_blob.go`, `participant_data_blob_handler.go` | 214 / 530 | L | Data blobs stored with a participant. After the first call. |
| `transport.go` | 3533 / 1616 (three test files) | W | `PCTransport`: the peer connection. Negotiation state machine, ICE, data channels, the estimator and pacer per connection, stream allocator hookup. The negotiation logic (who offers, glare, restarts, pending candidates) is kept in the same shape. Its tests connect two pion peer connections in one process; in Rust they connect the chosen library to itself. |
| `transportmanager.go` | 1134 | L, W | The publisher and subscriber transports of one participant (or the single one), fallback from UDP to TCP to TURN, data channel sending with back pressure. |
| `transport/` | 122 | L | The handler interface and negotiation states. |
| `mediatrack.go` | 726 / 197 | L | A published track: receivers per codec, dynacast, quality, RTCP to the publisher. |
| `mediatrackreceiver.go` | 1247 | L | The codec receivers of a track and its `TrackInfo`. |
| `mediatracksubscriptions.go` | 366 | L | The subscribers of a track: makes down tracks and adds them to subscribers' transports. |
| `subscribedtrack.go` | 506 | L | One subscription from the subscriber's side: settings, mute, layers. |
| `subscriptionmanager.go` | 1741 / 1077 | L | Desired against actual subscriptions, reconciled; permissions; retries. |
| `uptrackmanager.go` | 459 / 329 | L | Published tracks and the subscription permissions on them. |
| `roomtrackmanager.go` | 263 | L | The tracks of a room and who waits for them. |
| `wrappedreceiver.go` | 576 | L | A receiver as one subscriber sees it: codec choice, RED translation. |
| `mediaengine.go` | 268 / 241 | W | Which codecs and header extensions are offered; pion's `MediaEngine`. The tables are ported, the registration is against the library. |
| `config.go` | 233 | W | `WebRTCConfig`: the header extensions and feedback per direction; pion's `SettingEngine`. |
| `dynacast/` | 1253 / 570 | L | Tells publishers which layers anybody watches. |
| `supervisor/` | 437 / 254 | L | Watches that published tracks and subscriptions come about in time. |
| `signalling/` | 1076 / 157 | L | Builds and reads signal messages, with sequence numbers for resuming. The asynchronous signaller's message sink becomes the WebSocket of the one node. |
| `types/interfaces.go` | 1058 | L | The interfaces everything above meets through. |
| `types/protocol_version.go`, `types/trafficstats.go` | 270 | L | |
| `types/ice.go` | 414 | W | Records and filters ICE candidates for logging and the fallback decision; on pion's candidate type. |
| `clientinfo.go`, `utils.go`, `errors.go`, `user_packet_deduper.go`, `subscribepacer.go`, `signalanddatastats.go`, `medialossproxy.go` | 986 / 223 | L | Small things. |
| `migrationdatacache.go` | 69 / 57 | X | Only for moving a participant between nodes. |
| `egress.go` | 176 | X | Starting recordings. |
| `testutils.go` | 94 | N | Helpers on the generated fakes. |
| `types/typesfakes`, `transport/transportfakes` | 24162 | N | Generated. |

## The rest of LiveKit's server

| Package | Size | In musivum |
|---|---|---|
| `pkg/config` | 1220 / 392 | The structs that `pkg/sfu` and `pkg/rtc` read are ported with their defaults (`RTCConfig`, `AudioConfig`, `VideoConfig`, `RoomConfig`, `CongestionControlConfig`, the stream tracker settings, the limits). Loading from LiveKit's YAML file is an open question of the plan. |
| `pkg/utils` | 585 / 373 | Ported: `opsqueue`, `changenotifier`, `incrementaldispatcher`, `math`, `slice`. |
| `pkg/clientconfiguration` | 348 / 189 | Ported: per-client settings (which browsers get which codecs disabled and the like). The rules are written in a script language upstream (tengo); they become Rust conditions. |
| `pkg/service` | 12820 / 4793 | Written new and small. Read for behaviour: `rtcservice.go` (the WebSocket endpoint and its query parameters), `signal.go`, `wsprotocol.go`, `roommanager.go` (starting a session: 1,261 lines of which the single-node path is a fraction), `auth.go`, `roomservice.go` and `twirp.go` (the room API), `turn.go`. Left out: Redis stores, agents, SIP, ingress, egress, WHIP, WebTransport. |
| `pkg/routing` | 1592 / 50 | Left out, except the two interfaces a participant talks through (`MessageSink`, `MessageSource`) and the shape of a local node. |
| `pkg/telemetry` | 1786 / 950 | Left out as a service. The few calls that carry events webhooks need (participant joined, left, track published) go to a small trait. |
| `pkg/telemetry/prometheus` | 1432 / 127 | See "How Go becomes Rust". |
| `pkg/agent`, `pkg/metric` | 5300 | Left out. |
| `test/` | 5055 | LiveKit's integration tests with its Go client. `singlenode_test.go` and `scenarios.go` are the list of behaviours a call has to show; they are run against musivum from outside (see `docs/PORTING.md`, "How it is checked"), not ported. |

## From `livekit/mediatransportutil`

| Package | Size | | Notes |
|---|---|---|---|
| `helpers.go` | 119 / 48 | P | NTP time conversions, RTT from a receiver report. |
| `pkg/utils` (`wraparound.go`) | 244 / 679 | P | Unwraps 16 and 32 bit counters to 64 bit; used everywhere. |
| `pkg/bucket` | 383 / 633 | P | The ring of received packets that NACKs are answered from. |
| `pkg/nack` | 292 / 167 | P | Which packets to ask for again, and when. |
| `pkg/twcc` | 72 / 102 | P | Builds transport-wide feedback for a publisher (a thin layer over pion's recorder: that recorder comes from `pion/interceptor` and is ported with it). |
| `pkg/red` | 197 / 216 | P | RED (RFC 2198) headers. |
| `pkg/codec` | 1096 / 210 | P | VP8 payload descriptor, H.264 and H.265 NAL parsing, key frame detection for VP8, VP9, H.264, H.265, AV1, video size. |
| `pkg/latency` | 196 | P | One-way-delay estimator. |
| `pkg/interceptor` (`rttfromxr.go`) | 196 | W | RTT for a publisher from extended reports; a pion interceptor. |
| `pkg/rtcconfig` | 1164 / 119 | W | Port ranges, the node's addresses, STUN; produces pion's settings. The options are kept, the code is against the library. |
| `pkg/pacer`, `pkg/transport` | 955 / 126 | X | An older pacer that LiveKit's server does not use; pion's UDP mux setup. |

## From `livekit/protocol`

| Part | Size | | Notes |
|---|---|---|---|
| `protobufs/livekit_models.proto`, `livekit_rtc.proto`, `livekit_room.proto`, `livekit_webhook.proto`, `livekit_internal.proto`, `livekit_metrics.proto` and what they import | | as is | Compiled by `crates/musivum-protocol` at build time, from a copy of the `.proto` files at the pinned version. |
| `codecs/mime`, `codecs` | 660 | P | Mime types as an enum, and which codecs are what. |
| `utils/mono`, `utils/rtputil`, `utils/guid`, `utils/pointer` and single functions of `utils` | ~500 / ~200 | P | Function by function, as files need them. |
| `logger` | 1471 | N | Replaced by a small `Logger` over `tracing` with the same calls. |
| `auth` | 1257 / 1094 | P | Access tokens: verifying, the grants and what they allow. JWT with HS256. |
| `sdp` | 768 / 240 | W | Helpers on pion's SDP type (finding the simulcast rids, the BUNDLE mid, DTLS fingerprints). |
| `signalling` | 86 | P | |
| `datatrack` | 500 / 558 | P | The data track packet format. With data tracks. |
| `webhook` | 1431 / 984 | N | Sending webhooks is written new (a signed HTTP POST); the event shapes are the protocol's. |
| `observability`, `rpc`, `psrpc`, `redis`, `egress`, `ingress`, `sip`, `agent`, `connector` | | X | |

## What the tests need

- **Nothing but the code under test and packet builders**: most of `pkg/sfu`'s tests
  (`forwarder_test.go`, `rtpmunger_test.go`, `sequencer_test.go`, `codecmunger`,
  `videolayerselector`, `buffer` apart from FEC, `rtpstats`, `connectionquality`,
  `streamtracker`, `dependencydescriptor`, `utils`, `audio`, `red`) and all of
  `mediatransportutil`'s. `pkg/sfu/testutils` builds the packets.
- **Real time**: several tests sleep (stream trackers, forward stats, the pacer, the
  supervisor, the buffer's NACK timing). They are ported with the same waits first; a
  test clock is a later improvement and would be a change against upstream.
- **A VP8 packetiser** (`pion/rtp/codecs`) in `forwarder_test.go`, for one helper that
  builds a VP8 payload: the bytes are written out in the test.
- **A FlexFEC encoder** (`pion/interceptor/pkg/flexfec`) in `flexfec/decoder_test.go` and
  `buffer_fec_test.go`: test vectors generated from Go, or the encoder ported under
  `testutils`. Decided when `flexfec` is ported.
- **Fakes of interfaces** (`sfufakes`, `typesfakes`, `routingfakes`, `transportfakes`,
  `telemetryfakes`): `receiver_test.go`, `redreceiver_test.go`, and nearly all tests of
  `pkg/rtc` (`room_test.go`, `participant_internal_test.go`, `subscriptionmanager_test.go`,
  `uptrackmanager_test.go`, `dynacast`, `supervisor`, `signalling`). Hand-written fakes.
- **Two peer connections in one process**: `transport_test.go`, `transport_fec_test.go`,
  `transport_media_sections_test.go`. Ported onto the chosen library.
- **A simulated network** (`pion/transport/vnet`):
  `downtrack_downstream_integration_test.go`. See `downtrack.go` above.

## The reference run

`scripts/go-reference-test.sh ./pkg/sfu/... ./pkg/rtc/...` at the pin, Go 1.26.8, on
2026-11-14: every package that has tests passed. `pkg/sfu` took 68 s (the integration test
over the virtual network), `pkg/rtc` 24 s, `pkg/sfu/buffer` 9 s, `pkg/rtc/signalling` 5 s,
`pkg/sfu/datachannel` 3 s, the others under 2 s each. Without tests: `bwe`, `bwe/remotebwe`,
`ccutils`, `interceptor`, `abscapturetime`, `streamallocator`, `rtc/transport`, `rtc/types`.

The same script is how a single upstream test is run to see what the original does when a
ported test disagrees.
