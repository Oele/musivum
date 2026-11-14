# The port

What musivum takes from LiveKit and how, what it stands on, and how it is checked against
LiveKit. `docs/port-map.md` is the survey of LiveKit's source made before the port, file by
file; `docs/port-status.md` says what is done and how musivum compares with LiveKit.

## What musivum is

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
| `pkg/service`: the WebSocket and HTTP endpoints, the room API, webhooks, tokens, TURN | ~12k | ~6k | **Ported for one node**: the signalling endpoint, the room API over Twirp, the room manager, the embedded TURN server, authentication. The HTTP server itself is musivum's own (axum), and the library offers the endpoints over the `http` crate's types. |
| `pkg/routing`, `pkg/telemetry` | ~4k | ~2k | **Ported for one node**: the local router and node, the telemetry service with its webhooks and Prometheus metrics. |
| `pkg/agent`, the rest of `pkg/service` | ~9k | ~6k | **Left out**: running as a cluster, agents, analytics sent to LiveKit, egress, ingress, SIP, WHIP. Recording, ingest and telephony are separate programs upstream. |

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
`docs/webrtc-library.md` has the experiment that was run, the connection layer musivum
built on `rtc` (one UDP socket per address and port for all connections, ICE over TCP), what
`rtc` lacks and the two small patches musivum carries for it (`third_party/rtc-rtp`,
`third_party/rtc`).

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
- **prometheus/client_golang v1.24.1** and **prometheus/common v0.72.0** (Apache-2.0): read
  for how metrics are served and written (`promhttp`, `expfmt`); followed in
  `telemetry/prometheus/exposition.rs` and `musivum-server/src/service/promserver.rs`, nothing
  copied. **munnerz/goautoneg** (BSD-3-Clause): the order in which it parses an `Accept`
  header, followed in `exposition.rs`.
- **pion/turn v5.0.13** (MIT), the TURN server LiveKit embeds: its server side (`server.go`,
  `lt_cred.go`, the relay address generators, `internal/server`, `internal/allocation`,
  `internal/auth`, `internal/ipnet`) is ported in `webrtc/turn/`, with **pion/stun v3.1.7**'s
  list of known attributes and `ForEach`. Not ported: its client, RFC 6062 TCP relays (LiveKit
  refuses them), its virtual network. Pion's notice is in `NOTICE`.
- **pires/go-proxyproto v0.15.0** (Apache-2.0): read for how LiveKit's TURN listener treats
  the PROXY protocol; `service/proxyproto.rs` is written new from **HAProxy's PROXY protocol
  specification** (v1 and v2).
- **Go's standard library** (BSD-3-Clause) was read for what the originals call
  (`math/rand/v2`, `sort.Search`). Nothing of it is in the repository: a draft that had
  translated its ChaCha8 generator was removed, and identifiers take their random numbers
  from the `rand` crate.

## How it was built

In this order: a survey of LiveKit's source (`docs/port-map.md`); all of `pkg/sfu` with its
tests, leaves first, which needs no network; the choice of the WebRTC library and a
connection layer on it; `pkg/rtc` (rooms, participants, transports, subscriptions); the
signalling endpoint, the room manager and the server binary; then the first call through
musivum with LiveKit's JavaScript SDK in a browser, and the comparisons with LiveKit below.
The room API, webhooks, metrics, ICE over TCP and the embedded TURN server followed.

## What is not done

- Clients other than Chrome (and Element Call and Element X on Android, which use it):
  Safari and WebKit, which includes Element X on iOS, and LiveKit's native SDKs.
- More than one machine, and networks other than the loopback interface beyond what a
  homeserver's lab on one host covers: address translation and ICE restarts on an address
  change are untested.
- Latency and freezes under load, and runs of hours.
- What LiveKit does for a cluster, agents, egress, ingress, SIP and WHIP: left out on purpose.

## How it is checked

- **The ported tests.** A ported file is done when its ported tests pass.
- **Real clients against both servers.** The same scripted call (`tests/call`: two
  participants with LiveKit's JavaScript SDK in headless Chrome) against LiveKit and against
  musivum; what the clients observe has to match (20 observations, and a 21st for a variant
  through TURN only). Element Call and Element X were run behind a homeserver as well.
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
  from those the time to get back to its layer and frame rate. Three runs per scenario and
  server are in `tests/netem/runs/`; `node tests/netem/compare.js 'runs/*/reference'
  'runs/*/musivum'` gives medians and ranges, and marks a difference only where the two
  servers' ranges do not overlap. A run on a busy machine is flagged and says little.
- **Load.** `tests/load/run.sh <reference|musivum> [a b c d]` runs LiveKit's own load
  generator (`lk perf load-test` of livekit-cli v2.19.4, Apache-2.0, LiveKit's release
  binary) against one server on the loopback interface (ports 39560 to 39562), 60 s per
  scenario: one room with 2 publishers of simulcast video and audio and 10 (a) or 50 (b)
  subscribers; 10 calls of 4 (c); one publisher and 100 subscribers (d). Each second it
  samples the server's CPU, RSS and threads, the machine's load and the CPU temperature;
  `lk` reports per subscriber the tracks it got, their bitrate and the packets lost. A run
  starts below 75 °C and a load of 8, and is stopped above 90 °C for 30 s (the fan takes a
  few seconds to spin up) or 97 °C for 10 s, a load of 16 or an RSS of 6 GB. Three runs per
  scenario and server are in `tests/load/runs/`; `python3 -I tests/load/compare.py
  'runs/*/reference' 'runs/*/musivum'` compares them the same way.

## Following upstream

`upstream.toml` pins the version of LiveKit that was ported and the modules its `go.mod`
names; the header of every ported file names its origin at that version. Ported files stay
close to their originals so that each release can be followed as a diff. Per release:

1. **The weekly check.** `.github/workflows/upstream.yml` looks up LiveKit's latest release
   every Tuesday (and by hand). When it is newer than the pin, it runs the report below and
   opens an issue "LiveKit vX.Y.Z" in this repository with its summary; the whole report is
   the run's artifact.
2. **The report.** `scripts/upstream-diff.sh vX.Y.Z` fetches LiveKit at the pin and at the
   new tag, and the Go modules that moved, into `target/upstream/`, reads the header of every
   Rust file, and writes `target/upstream/vX.Y.Z/report.md`: per Rust file the origin files
   that changed and the size of their diff (the diff is in `diffs/<Rust file>.diff`); origin
   files deleted or renamed; new upstream files in the ported packages; the copied `.proto`
   files that changed; headers it could not read; places that name the old version outside
   a header; the Rust files whose origins did not change. `--from <tag>` compares from
   another tag than the pin. `python3 -I scripts/upstream_diff_test.py` holds its reading of
   headers against every header in the tree: a new form of header goes into both.
3. **Porting the changes.** Each Rust file whose origins changed takes their diff by the
   rules of the first port (`docs/porting-guide.md`): the same names, the upstream change
   translated where it falls, the changed tests ported with it and passing, a header that
   names the new version and says in "Changes" what still differs. One commit per ported
   file (with its test file); the files are independent, so agents can share them out by
   package. A new upstream file gets a decision: ported with its tests, or left out with a
   line that says so in the Rust file nearest to it. A deleted or renamed origin is followed.
4. **The modules.** When the protocol moves, the same files of its new version are copied
   over `crates/musivum-protocol/protobufs/` (its README). Files ported from pion's modules
   and the others the report lists are ported files like LiveKit's, step 3. musivum's
   WebRTC library, webrtc-rs's `rtc` and `rtc-rtp`, is not part of the pin: when it is
   moved, the new crate replaces the sources in `third_party/<crate>/` and its
   `MUSIVUM.patch` is applied again, or the directory goes when upstream has the change
   (`MUSIVUM.md` there).
5. **Moving the pin.** `scripts/upstream-diff.sh --bump-unchanged vX.Y.Z` writes the new
   version into each header whose origins did not change, and into no other; the places the
   report lists as naming the old version outside a header are looked at by hand.
   `target/upstream/vX.Y.Z/upstream.toml` becomes `upstream.toml`, and
   `scripts/fetch-reference.sh` fetches the new reference. Run against the new pin, the
   report lists no header that does not name it.
6. **Before it is committed.** `scripts/check.sh`; the browser call against both servers
   (`tests/call/run.sh musivum` and `reference`, with `livekit-server` at the new version);
   and for a release that touches `pkg/sfu`'s forwarding (`forwarder.go`, `downtrack.go`,
   `buffer/`, `sequencer.go`), bandwidth (`bwe/`, `streamallocator/`, `pacer/`) or layer
   selection (`videolayerselector/`), the degraded-network and load comparisons (`tests/netem`,
   `tests/load`, three runs per scenario and server, as above).

## Open questions

- Proposing the padding size field and a transceiver's codec handling to webrtc-rs, so that
  `third_party/rtc-rtp` and `third_party/rtc` can go.
- The remaining CPU difference under load (4 to 18% more than LiveKit): most of it is in the
  write of each packet through `rtc`'s pipeline (`docs/port-status.md`, "Under load").
