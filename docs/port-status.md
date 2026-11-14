# What is ported

One line per upstream file or package that has been dealt with. "Tests" is the number of
upstream test functions that were ported and pass, of the number upstream has; a difference
is explained in the notes. `docs/port-map.md` says what each file is and where it goes.

After the table: the options of LiveKit's configuration that are read but not applied, where
the port cannot be the original, and how musivum compares with LiveKit in real calls, on a
degraded network and under load.

## Done

Paths in musivum are under `crates/musivum/src/`. "mtu" is `livekit/mediatransportutil`,
"protocol" is `livekit/protocol`.

| Upstream | In musivum | Tests | Notes |
|---|---|---|---|
| mtu `pkg/utils/wraparound.go` | `mediatransportutil/utils/wraparound.rs` | 6 / 6 | |
| mtu `pkg/bucket/bucket.go`, `errors.go` | `mediatransportutil/bucket/` | 6 / 6 | `GetPacket` on a nil bucket is the caller's `None`. Also run against the Go original: 288,000 operations, identical. |
| mtu `pkg/nack/nack.go` | `mediatransportutil/nack/nack.rs` | 3 / 3 | Also run against the Go original, identical. |
| mtu `pkg/latency/owd_estimator.go` | `mediatransportutil/latency/owd_estimator.rs` | none upstream | Four tests added, their expected values printed by the Go original for the same series. |
| mtu `pkg/twcc/twcc.go` | `mediatransportutil/twcc/twcc.rs` | 1 / 1 | One of the six cases of upstream's test fails upstream too (see below); the port asserts what the original does. One test added that compares 27 marshalled feedback packets with pion's bytes. |
| pion/interceptor `pkg/twcc/twcc.go`, `arrival_time_map.go`, `internal/sequencenumber/unwrapper.go` | `webrtc/interceptor/twcc/`, `webrtc/interceptor/internal/sequencenumber/` | 15 / 15 | Only the recorder and what it stands on, not pion's interceptors around it. The assertion on allocations in one test is replaced by one on the buffer not moving. Two benchmarks left out. |
| mtu `pkg/codec/vp8.go`, `h26xhelper.go`, `helpers.go`, `keyframe.go` | `mediatransportutil/codec/` (same names) | 6 / 6 | Twelve tests added (`vp8.go` and `keyframe.go` have none upstream). `reference_test.rs` compares the port with what the Go original says about 326 payloads, pinned in `testdata/` with the Go test that printed it. Also run against the original with about 700,000 generated payloads: identical, the two panics below included. |
| pion/rtp `codecs/vp9_packet.go`, `common.go`, `error.go` | `webrtc/rtp/codecs/` (same names) | 5 / 7 | Only `VP9Packet` and what it stands on: the payloader and the other depacketizers are not used by LiveKit. The tests of allocations assert that buffers stay where they are instead. `Vp9Packet.payload` is a copy of the payload (`Vec<u8>`): a cost per VP9 packet, to be looked at with the buffer. |
| mtu `pkg/red/red.go` | `mediatransportutil/red/red.rs` | 4 / 4 | Also run against the original with 60 generated streams: identical. |
| mtu `pkg/rtcconfig/config.go`, `ip.go`, `webrtc_config.go`, `rtc_unix.go` | `mediatransportutil/rtcconfig/` (same names) | 3 / 3 | The `rtc:` section of LiveKit's configuration with its defaults, validation and error texts; 23 tests added, with values from a run of the original. What it produces is for musivum's connection layer: a `NodeConfig` and a `SettingEngineConfig` (data, since `rtc`'s setting engine cannot be copied and changed per connection as pion's is). Options the layer cannot honour yet are accepted and logged as not applied: see the table below. No test sends anything off the machine (STUN goes to a responder of the test's own). |
| mtu `helpers.go` | `mediatransportutil/helpers.rs` | 1 / 1 | Two tests added (the way back from NTP time, RTT from a receiver report). |
| pion/rtp `packet.go`, `error.go` | `webrtc/rtp/packet.rs`, `error.rs` | 43 / 43 | Four tests added. Benchmarks left out. Run against pion with 432,008 generated inputs: every output identical, error texts and the cases where pion panics included. |
| pion/rtp `abssendtimeextension.go`, `audiolevelextension.go`, `transportccextension.go`, `playoutdelayextension.go`, `abscapturetimeextension.go` | `webrtc/rtp/` (same names) | 20 / 20 | Benchmarks left out. |
| pion/rtp `header_extension.go` | `webrtc/rtp/header_extension.rs` | 0 / 13 | Only the one function `packet.go` calls; LiveKit does not use the types of this file. |
| protocol `codecs/mime/mimetype.go` | `protocol/codecs/mime/mimetype.rs` | none upstream | Five tests added. The conversion to `roomobs` (observability) is left out. |
| protocol `codecs/codecs.go` | `protocol/codecs/codecs.rs` | none upstream | Three tests added. |
| protocol `auth/accesstoken.go`, `grants.go`, `verifier.go`, `provider.go`, `interfaces.go` | `protocol/auth/` (same names, with `jwt.rs` and `json.rs`) | 13 / 13 | Seventeen tests added. `jwt.rs` is what the original takes from golang-jwt (MIT), ported in part; `json.rs` is new. Tokens signed by Go verify here and tokens signed here verify in Go (run both ways, every grant; for fixed claims and times the token is Go's byte for byte). More than 200 unusual tokens and 60 key files were put through Go, and the tests expect Go's outcome. Token times are compared with the system clock, as in Go. `authfakes` is not ported. |
| protocol `utils/`: `timed_version.go`, `bitmap.go`, `time_size_cache.go`, `timed_aggregator.go`, `rtpstats.go`, `protoproxy.go`, `dedupedslice.go`, `guid/id.go`, `id.go`, `connectionquality.go`, `error.go`, `data_channel_rpc.go`, `secret.go`, `pointer/to.go` | `protocol/utils/` (same names) | 11 / 12 | 29 tests added, with values from the originals. `guid.Hash` and its test are not ported (it reproduces Go's ChaCha8 sequence for a seed; only request ids of the service use it). Identifiers take their random numbers from the `rand` crate. `base62.rs` is jxskiss/base62's encoder (MIT). The database methods of `TimedVersion`, `LocalNodeID` and the psrpc conversion are left out. `ProtoProxy` is generic over a value with `Default` and `PartialEq`; its `update_fn` has to hold its owner weakly. |
| protocol `datatrack/packet.go`, `extension_e2ee.go`, `extension_participant_sid.go`, `extension_user_timestamp.go` | `protocol/datatrack/` (same names) | 5 / 5 | The fuzz test runs its property over the 15 seeds; the fuzzing itself is not carried over. Three tests added. A participant id that is not UTF-8 is read with replacement characters (a Go string holds any bytes). Also run against the original with 5,173 inputs (truncated at every length, flipped bits): all agreed. |
| protocol `sdp/sdp.go`, `signalling/utils.go` | `protocol/sdp/sdp.rs`, `protocol/signalling/utils.rs` | 4 / 4 | On the parsed description of `rtc`'s SDP crate. `reference_test.rs` in both compares with 253 and 62 lines printed by the originals for a browser's real offer and answer and for made-up ones. pion's `GetCodecMap` and `GetCodecForPayloadType` are ported (`webrtc/sdp/util.rs`, 15 of pion's 26 tests) because `rtc`'s own lookup knows no static payload types and drops an `a=fmtp` with a space in it. `rtc` refuses a media line without formats, which pion takes. |
| protocol `utils/mono/mono.go` | `protocol/utils/mono.rs` | 0 / 2 | The clock itself is `go::time`; upstream's two tests are about Go's `time.Time` and have nothing to test here. |
| protocol `livekit/types.go` | `protocol/livekit/types.rs` | 0 / 4 | Only the identifier types; upstream's tests are of the YAML conversions, which are not ported. |
| protocol `utils/rtputil/rtpconverter.go` | `protocol/utils/rtputil.rs` | 1 / 1 | |
| protocol `utils/parallel.go` | `protocol/utils/parallel.rs` | 2 / 2 | Workers are scoped threads started per call (upstream: goroutines). One benchmark left out. |
| `pkg/sfu/utils/rangemap.go` | `sfu/utils/rangemap.rs` | 1 / 1 | |
| `pkg/sfu/utils/downtrackspreader.go` | `sfu/utils/downtrackspreader.rs` | 4 / 4 | The assertion on allocations per call in `TestBroadcastRTP` is left out (counting them needs `unsafe`). Parallel broadcast uses threads per call: fine at the default threshold, which keeps it serial. |
| `pkg/sfu/utils/debounce.go` | `sfu/utils/debounce.rs` | none upstream | One test added. |
| `pkg/sfu/ccutils/trenddetector.go`, `prober.go`, `probe_regulator.go`, `probe_signal.go` | `sfu/ccutils/` (same names) | none upstream | Fourteen tests added, their expected values taken from the Go originals. Re-read line by line against the originals. |
| `pkg/sfu/bwe/bwe.go`, `null_bwe.go` | `sfu/bwe/bwe.rs`, `null_bwe.rs` | none upstream | Two tests added. `Bwe` and `BweListener` are traits; `NullBwe` does not implement `Bwe` (upstream's has no `Type()` either) and is held by the estimators for the calls they do not define. |
| `pkg/sfu/utils/helpers.go` | `sfu/utils/helpers.rs` | none upstream | Five tests added. pion's `interceptor.RTPHeaderExtension` is `webrtc::interceptor::RtpHeaderExtension`. |
| `pkg/sfu/bwe/remotebwe/` (four files) | `sfu/bwe/remotebwe/` (same names) | none upstream | 22 tests added, their expected values printed by the Go original for the same calls with the same sleeps. The config `Default`s are Go's zero values; the original's defaults are the `DEFAULT_...` constants. |
| `pkg/sfu/bwe/sendsidebwe/` (eight files) | `sfu/bwe/sendsidebwe/` (same names) | 1 / 1 | At the pin the congestion detector handles a feedback report in the call, under its lock, and calls the listener after unlocking: no worker. Also run against the original with a session of 13,822 packets and 429 feedback reports (delay rising, loss, recovery, a pause, probes, reports out of order and repeated, the wraps of the sequence number and of the reference time): 439 lines of state, identical; 46 of them pinned in `reference_test.rs`. Left out of the comparison, because the original reads the clock there: the detector's own estimate of the feedback interval, finalizing a probe by settle wait. |
| `pkg/sfu/rtpstats/` (six files) | `sfu/rtpstats/` (same names) | 4 / 4 | One lock per statistics object, the embedded bases as fields inside it; inherited methods are forwarded. Also run against the original with a script of 8,843 operations on receivers, senders and their lite forms (loss, reordering, wraps, restarts, pauses of hours, sender and receiver reports in and out of order): 3,620 lines, identical; 50 of them pinned. Left out of the comparison, because the original reads the clock there: durations, rates and the times of the last PLI, FIR and key frame. One benchmark left out. |
| `pkg/sfu/streamtracker/` (five files) | `sfu/streamtracker/` (same names) | 2 / 2 | Eleven tests added, with values from the original. A worker holds its tracker strongly, as a goroutine does: a tracker dropped without `stop()` keeps ticking. `testutils.WithTimeout` is `testutils::with_timeout` and `with_timeout_async`. |
| `pkg/sfu/videolayerselector/` (eleven files) and `temporallayerselector/` (four) | `sfu/videolayerselector/` (same names) | 20 / 20 | Two assertions on allocations left out. `reference_test.rs` runs 675 commands and expects what the original prints for them (simulcast, VP9, the VP8 temporal selector, the constructors from another selector). The selector's `recover()` around the descriptor writer is `catch_unwind`; a packet whose descriptor lacks a part is not selected where Go would dereference nil. The `...FromOther` constructors take the old selector and its `Base`. |
| `pkg/sfu/codecmunger/` (four files) | `sfu/codecmunger/` (same names, with `orderedmap.rs`) | 8 / 8 | `update_and_get` returns the header by value, as the original does at the pin. The ordered map is written new on the standard library. A hand-made VP8 descriptor with `m` set and header size 0 differs (Go computes -1 and marshals). Also run against the original: 24,000 steps, 25,818 lines with the munger's full state, identical. |
| `pkg/sfu/streamtrackermanager.go` | `sfu/streamtrackermanager.rs` | none upstream | Eight tests added, their expected events and states printed by the Go original for the same steps. The listener is held weakly. `Bitrates` and `ReceiverCodecState` of `receiver_base.go` are in `sfu/receiver_base.rs` ahead of the receiver. |
| `pkg/sfu/interfaces.go` | `sfu/interfaces.rs` | | `TrackReceiver`, `RedTransformer`, `TrackSender`, `DownTrackListener` and `DownTrackStreamAllocatorListener` as traits with fakes. |
| `pkg/sfu/pacer/` (six files) | `sfu/pacer/` (same names) | 2 / 2 | Ten tests added: what each of the three pacers stamps and tells the estimator, compared with a run of the original (header bytes, sizes, send times where LiveKit's rounding differs from pion's). A packet owns its parts: no pools and no scratch buffers, at the cost of two small allocations per send for the stamped extensions. `Pacer` has a fake. One assertion on allocations left out. |
| `pkg/sfu/redreceiver.go`, `redprimaryreceiver.go` | `sfu/redreceiver.rs`, `redprimaryreceiver.rs` | 4 / 4 | The methods that forward to the wrapped receiver are what Go's embedding was. Seven assertions that compare with `ReceiverBase` are left out until the receiver is ported, and `TestRedReceiverReusedReadBuffer` runs with a stand-in for the buffer: both to be redone then. The state `ForwardRTP` changes has a lock the original does not need. `close_track_senders` of `receiver_base.go` is in `redreceiver.rs` until the receiver is ported. |
| `pkg/sfu/receiver_base.go`, `receiver.go`, `track_remote.go` | `sfu/receiver_base.rs`, `receiver.rs`, `track_remote.rs` | 2 / 2 | Eight tests added that run a real buffer behind a receiver, most with values from the same steps against the original (packets at the down tracks in order, a down track added later, a read by sequence number, PLI throttling, a restart, the close path, nothing holding a closed receiver). One forwarding task per buffer, which holds its receiver weakly. A RED transformer is dropped on close (it holds the receiver's base). A restart waits for the forwarders with `block_in_place`: the receiver needs the multi-thread runtime. `sfu.go` is pools only and has no counterpart. With it, the assertions of `redreceiver_test.rs` that had waited for the receiver and the buffer are restored. One benchmark left out. |
| `pkg/sfu/interceptor/rtx.go`, `unhandlesimulcast.go`; mtu `pkg/interceptor/rttfromxr.go` | `sfu/interceptor/` (same names) | none upstream | pion's interceptors as objects the packet sink calls. `unhandlesimulcast` is ported as logic and called by nothing: stamping mid and rid onto packets cannot place a stream in `rtc`, which decides a packet's stream before anything sees it. |
| (new: ICE over TCP) | `webrtc/peerconnection/tcp.rs`, `node.rs` | | Passive ICE-TCP (RFC 6544, RFC 4571 framing) on a TCP listener of the node, with no change to `rtc`; pion's TCP mux's limits. Switched on by `rtc.tcp_port` (`WebRtcConfig::new_node`); `force_tcp` leaves UDP out. Two tests connect two connections over TCP only (media both ways, a data channel) and close streams that break the rules without disturbing a connection; 10 runs in a row. Not there: simultaneous-open, a second address family, TURN. |
| (new, in place of pion's buffer factory door) | `sfu/interceptor/sink.rs` | | The packet sink of a connection: RTP to the buffer of its SSRC, each RTCP packet to the reader of every SSRC it is addressed to, repair streams paired from the description or learned from the first packets (the simulcast case `rtc` does not pair), buffers closed with their stream. Eighteen tests with the three ported files, four of them on two connections in one process (media into a bound buffer, RTX repairing it, three simulcast layers each paired with its repair stream). |
| `pkg/sfu/streamallocator/streamallocator.go`, `track.go`, `streamstateupdate.go` | `sfu/streamallocator/` (same names) | none of its own | Its events are handled one at a time on an ops queue, as upstream. pion's own estimator (off by default upstream) is not carried over. Fifteen tests added; ten of them assert what the original gives for the same steps (optimal allocation, pause and resume, probing with padding and with media, an early warning, screen share priority, giving and taking bits when deficient). It brought the two down track tests that needed it, one of which waits 60 s on real timers, as upstream. |
| `pkg/sfu/downtrack.go` | `sfu/downtrack.rs` | 10 / 11 | Its tests are upstream's integration test over pion's virtual network; the eight that exercise the down track itself run on a bound context of the test's own with a recording writer and a fake receiver, their expected bytes and counts taken from a run of the original. The two that need the stream allocator came with it. Not ported: the one that tests pion's virtual network. Two tests added. Each lock guards what it guards upstream. The down track holds its receiver strongly and its listeners and transceiver weakly. `close` blocks the calling thread up to a second, as upstream. The SDES item with the transceiver's mid is not written (`rtc-rtcp` has no such item type). One assertion on allocations left out. `DownTrackStreamAllocatorListener` of `interfaces.go` came with it. |
| `pkg/sfu/forwarder.go` | `sfu/forwarder.rs` | 34 / 34 | Every subtest and table case. A selector is swapped by taking the old one out of its field (a null selector stands in until the new one is assigned). `GetTranslationParams` returns the parameters together with the error, as the original does. The 400 random pairs of one subtest are written out from a run of Go's generator. |
| `pkg/sfu/playoutdelay.go` | `sfu/playoutdelay.rs` | 1 / 1 | |
| `pkg/sfu/connectionquality/scorer.go`, `connectionstats.go` | `sfu/connectionquality/` (same names) | 3 / 3 | Every table case. The providers of statistics are traits held weakly. Also run against the original: 728 lines from the scorer over six kinds of track (score bit for bit, quality) and 18 from `get_stat`, identical; 58 pinned. `f64::ln` agreed with Go's `math.Log` on every input of the run, which nothing guarantees in general. |
| `pkg/sfu/rtpmunger.go` | `sfu/rtpmunger.rs` | 9 / 9 | `update_and_get_sn_ts` returns the parameters together with the error, as the original does. Also run against the original: 6,000 operations, 13,548 lines of results and state, identical. |
| `pkg/sfu/sequencer.go`, `rtxbudget.go` | `sfu/sequencer.rs`, `rtxbudget.rs` | 7 / 7 | Methods on a nil budget take `Option<&RtxBudget>`. Both carry `allow(dead_code)` until the down track uses them. |
| `pkg/sfu/forwardstats.go` | `sfu/forwardstats.rs` | 14 / 14 | Two tests added. One benchmark left out. |
| `pkg/sfu/errors.go` | `sfu/errors.rs` | none upstream | The package's errors; empty upstream at the pin, the four values of the munger are defined in `downtrack.go` there. |
| `pkg/telemetry/` (`telemetryservice.go`, `events.go`, `stats.go`, `statsworker.go`, `statsconn.go`; `analyticsservice.go` interface and null service) | `telemetry/` (same names) | 20 / 20 | The service behind the 37 calls `pkg/rtc` reports through: webhook events, statistics into the counters, the stats workers. The analytics half of each event is built and handed to the null analytics service (LiveKit's recorder is left out). A differential test sends the five webhooks of a room's life to an in-process server and compares body, content type, user agent and the signed token's claims with what the original sends for the same calls: equal. `RoomProjectReporter` is left out (observability). |
| protocol `webhook/`, `utils/timeoutqueue.go` | `protocol/webhook/` | 10 / 10 | The notifiers with their queues, the filter, the verifier. Sent with reqwest (rustls, no OpenSSL) through `retryablehttp.rs`, written new with go-retryablehttp's documented retries and backoff. The time-out queue is in `protocol/utils`, frostbyte73/core's pool in `go/core`. |
| `pkg/telemetry/prometheus` (two functions of `packets.go`) | `telemetry/prometheus/` | | Written new: counters, gauges and histograms without a Prometheus dependency, readable by tests and by a later endpoint. Its module comment says how a function is added. |
| `pkg/sfu/flexfec/decoder.go` | `sfu/flexfec/decoder.rs` | 19 / 19 | The lookup of media packets is handed to `decode_fec` with every packet instead of to the constructor (it reads the buffer's state under the buffer's lock). Three benchmarks left out. `reference_test.rs` compares decoder and encoder with 241 lines of the original's output, all three masks included. |
| pion/interceptor `pkg/flexfec/flexfec_encoder_03.go`, `flexfec_coverage.go`, `flexfec_encoder.go`, `util/` | `webrtc/interceptor/flexfec/` (tests only) | 11 / 11 | The FlexFEC-03 encoder, for the decoder's tests. pion's own decoder, its interceptor and their tests are not ported. |
| `pkg/sfu/testutils/rtptest/packet.go` | `sfu/testutils/rtptest.rs` | none upstream | |
| `pkg/sfu/audio/audiolevel.go` | `sfu/audio/audiolevel.rs` | 1 / 1 | |
| `pkg/sfu/packettrailer/packet_trailer.go` | `sfu/packettrailer/packet_trailer.rs` | 1 / 1 | |
| `pkg/sfu/rtpextension/dependencydescriptor/` (five files) | `sfu/rtpextension/dependencydescriptor/` (same names) | 7 / 8 | `TestDependencyDescriptorMarshalAllocs` counts allocations and is left out (see the guide); two benchmarks left out. Also compared with 143 lines of output of the original for the same inputs (`reference_test.rs`): identical except where Go tells a nil slice from an empty one, see below. |
| `pkg/sfu/rtpextension/playoutdelay/playoutdelay.go` | `sfu/rtpextension/playoutdelay/playoutdelay.rs` | 1 / 1 | |
| `pkg/sfu/rtpextension/abscapturetime/abscapturetime.go` | `sfu/rtpextension/abscapturetime/abscapturetime.rs` | none upstream | One test added. |
| `pkg/sfu/datachannel/bitrate.go` | `sfu/datachannel/bitrate.rs` | 6 / 6 | |
| `pkg/sfu/datachannel/datachannel_writer.go` | `sfu/datachannel/datachannel_writer.rs` | 3 / 3 | Where pion's write blocks until a deadline, the writer waits for the channel's buffered amount to fall to a threshold (64 KiB, a number of musivum's: pion waits on the association's pending queue, which the layer cannot see) and fails at the original's deadline. It waits only when asked to, as upstream blocks only with a slow threshold configured. Three tests added. Not run against a real channel pair. |
| `pkg/sfu/buffer/videolayerutils.go` | `sfu/buffer/videolayerutils.rs` | 7 / 7 | Qualities are taken and returned as the protocol's enum: a number that is no value of the enum comes back as the default where Go returns the number. Also compared with the original over 290 track infos in every rid space: identical. |
| `pkg/sfu/buffer/frameintegrity.go` | `sfu/buffer/frameintegrity.rs` | 3 / 3 | A frame entity is handed the packet history instead of pointing to it. Also compared with the original (100,000 queries): identical. |
| `pkg/sfu/buffer/datastats.go` | `sfu/buffer/datastats.rs` | 2 / 2 | |
| `pkg/sfu/buffer/rtcpreader.go` | `sfu/buffer/rtcpreader.rs` | none upstream | One test added. `Read` has no counterpart. `close` without a callback set does nothing (the original calls a nil function). |
| `pkg/sfu/buffer/fps.go` | `sfu/buffer/fps.rs` | 3 / 3 | The VP9 and dependency descriptor calculators are handles whose state is shared with their per-layer views. Also run against the original with 3,000 generated streams: identical. |
| `pkg/sfu/buffer/dependencydescriptorparser.go` | `sfu/buffer/dependencydescriptorparser.rs` | 7 / 7 | The assertion on allocations is left out. Decode targets are sorted with a stable sort (Go's is stable only up to 12 elements). Also run against the original with 680,650 packets: identical wherever the input bytes were. |
| `pkg/sfu/buffer/streamstats.go` | `sfu/buffer/streamstats.rs` | none upstream | Written, not declared in the module yet: it needs `rtpstats`. |
| `pkg/sfu/testutils/data.go` | `sfu/testutils/data.rs` | none upstream | Test helpers. The two test codecs are functions. |
| `pkg/sfu/buffer/buffer_base.go`, `buffer.go`, `factory.go` | `sfu/buffer/buffer_base.rs`, `buffer.rs`, `factory.rs` | 21 / 21 | One lock for the buffer and its base, as upstream. A read waits as a blocking call (`read_extended`, for a plain thread) or as a future (`read_extended_async`, for a task); both give `None` for a restart and `Eof` once closed. `Buffer.Read`, which only pion called, has no counterpart. The factory is the map it was; `packetio.BufferPacketType` is an enum. No pools: a read copies the packet once. Also run against the original with 28,445 operations on three kinds of stream (loss, RTX repair, reordering, padding, wraps, a publisher restart, a reader that falls behind): 12,958 lines, identical; 355 pinned. Left out of the comparison, because the original reads the clock there: rates and jitter, PLIs, the timing of NACKs (the NACK queue's content is compared instead), the audio level. One assertion on allocations and two benchmarks left out. The out-of-band sequence number paths are ported and reached by nothing, here as upstream. |
| `pkg/rtc/types/protocol_version.go`, `trafficstats.go` | `rtc/types/` (same names) | none upstream | Five tests added. `livekit.StreamType` is written out (its proto file is not compiled here). |
| `pkg/rtc/transport/negotiationstate.go` | `rtc/transport/negotiationstate.rs` | none upstream | |
| `pkg/rtc/clientinfo.go`, `errors.go`, `user_packet_deduper.go`, `subscribepacer.go`, `utils.go`, `participant_data_blob.go` | `rtc/` (same names) | 14 / 14 | Twenty tests added, with values from the original. `Recover` is `recover` and `recover_async` around `catch_unwind`. `IsEOF` is a guess until the transport's write errors exist. |
| `pkg/clientconfiguration/` (four files) | `clientconfiguration/` (same names, with `semver.rs`) | 4 / 4 | A rule is a closure written through `script_match!` with the upstream expression as its body, token for token; the client's fields carry the script language's comparisons. Two table cases that expect a script error are compile errors here. `NewScriptMatch` with a string at run time is left out (nothing feeds it). Also run against the original with 989 clients and 1,962 version pairs: all agreed; 93 lines pinned. |
| `pkg/config/config.go` | `config/` (`config.rs`, with `yaml.rs`, `fields.rs`, `logger.rs`, `webhook.rs`, `metric.rs`) | 11 / 16 | LiveKit's configuration file, read as LiveKit reads it: defaults first, the file over them, strict mode, yaml.v3's rules for scalars (a decoder of musivum's own on `saphyr-parser`, following yaml.v3's; serde cannot decode onto defaults or switch strictness at run time). Compared with the original value by value: 315 values of the defaults, 320 of the sample file with every option uncommented, 353 of a file of edge cases, and the texts of 31 errors with their line numbers: all agree. Twelve sections for what musivum leaves out (`redis`, `agents`, `sip`, `ingress`, `psrpc`, ...) are accepted and ignored (`prometheus` and `prometheus_port` are read since the metrics endpoint) with a log line each; strict mode does not look inside them. Not ported: the generated command line flags (two tests), `psrpc` (two tests), a test that reflects over struct tags. 21 tests added. |
| `pkg/service/interfaces.go`, `localstore.go`, `roomallocator.go`, `auth.go`, `utils.go`, `wsprotocol.go`, `errors.go` | `service/` (same names, with `useragent.rs`) | 5 / 7 | The foundation of the outer layer, for one node, over the `http` crate's types (no HTTP server in the library). Authentication of a request and validation of a connect request return the original's status codes and texts (compared with a run of it). Signal messages on a WebSocket through a small async trait a server implements for its socket, with gorilla's handling of control frames. The user agent is read by a small recogniser that gives uap-core's names for the browsers, systems and devices the client rules name (41 user agents, all equal to the original's answers). Not ported: the preset half of `TestCreateRoom` (named room configurations are left out), `TestRemoveDoubleSlashes` (Go's HTTP plumbing). Node selection, agents, SIP, ingress and egress are left out. |
| `pkg/service/roommanager.go`, `signal.go` (the server half) | `service/roommanager.rs`, `signal.rs` | 1 / 2 | The room manager of one node: sessions started through the local router create or find the room, make and join the participant, and run its signal loop; resumes; the room-side operations of the room API. Six tests added on a real room manager through the router (the join response for protocols 16 and 18 compared with the original's, a second participant announced, a mute, close and idle close, a resume, every room-side operation with the original's errors). Not ported: the egress test of `roommanager_test.go`. Left out: agents, egress, WHIP, the embedded TURN server's part of the ICE servers. |
| `pkg/service/roomservice.go`, `twirp.go`; the room and participant servers of protocol `rpc` | `service/roomservice.rs`, `twirp.rs`, `protojson.rs`; `routing/rpc.rs` | 4 / 5 | The room API over Twirp (`/twirp/livekit.RoomService/<Method>`, protobuf and JSON), framework-neutral like the rest of `service`. psrpc's room and participant servers are traits registered per topic on the local bus. 61 requests (every method in both encodings, missing token or grant, limits, not found, routing errors, malformed bodies) answered with the same status, content type and body as the original's Twirp server. Not ported: `TestAgentDispatchMetadataLimits` (agents). The room manager is the room's and each participant's server on the bus (registered per topic, killed at close and at `Stop`, as upstream); the binary serves the prefix. |
| `pkg/service/rtcservice.go` | `service/rtcservice.rs` | 1 / 1 | The signalling endpoint (`/rtc`, `/rtc/v1` and their `validate`), framework-neutral: a server authenticates, calls `v0`/`v1`, and runs the `RtcUpgrade` it gets on the upgraded socket. Gorilla's handshake checks are made in its order. Six tests added, their expected values from one run of the original: the `StartSession` of five requests (`join_request` gzip and plain, the query form, reconnects), a session over an in-memory socket, twelve `validate` cases, the refusals. |
| pion/turn's server side (`server.go`, `server_config.go`, `lt_cred.go`, the relay address generators, `internal/server`, `internal/allocation`, `internal/auth`, `internal/ipnet`, `internal/proto`) | `webrtc/turn/` | 49 / 61 | The TURN server, on tokio with rustls; message types of `rtc-stun`/`rtc-turn`. Not ported: the client, RFC 6062 TCP relays (Connect, ConnectionBind: LiveKit refuses TCP relays; a TCP Allocate gets LiveKit's 508, Connect and ConnectionBind get no answer where LiveKit's answers 447 and 400; browsers send neither) with their 12 tests, and the virtual network (`TestServerVNet`). Tests use ephemeral ports or 39545..=39559, never 3478. |
| `pkg/service/turn.go`, `turnrelay.go`, `turnquota.go`; `pkg/telemetry/statsconn.go`'s relay generator | `service/turn.rs`, `turnrelay.rs`, `turnquota.rs`, `proxyproto.rs` (new); `telemetry/statsconn.rs` | 35 / 35 | LiveKit's embedded TURN server: UDP and TLS listeners (certificate files, `external_tls`, the PROXY protocol), the relay policy (only the node's media ports; `allow_restricted_peer_cidrs`, `deny_peer_cidrs`), the per-user quota, credentials from the API keys handed out in the join response. Wired into `musivum-server` (made in `Start`, closed in `Stop`). A relay-only variant of the browser call (`RELAY=1 tests/call/run.sh`, Chrome with `iceTransportPolicy: 'relay'`, TURN on ports 39540..39559) gives 21 of 21 against musivum, over UDP and TLS and over TLS alone, as against LiveKit. |
| `cmd/server/main.go`, `commands.go`, `pkg/service/server.go` | `crates/musivum-server` (`main.rs`, `commands.rs`, `service/server.rs`, `service/utils.rs` for the middlewares) | | The server binary: LiveKit's flags and environment (compared with LiveKit's `getConfig` at the pin), dev mode, `generate-keys`, `ports`, `create-join-token`; binding, the health check, graceful stop, the request body limit, path normalisation, authentication. The WebSocket adapter for `SignalWebSocket` on axum (`axumws.rs`, new): no permessage-deflate, which tungstenite does not have, so the server declines it and runs uncompressed. Prometheus' endpoint is served on `prometheus.port` with basic auth (`promserver.rs`, `basic_auth.rs`): the text format only, no gzip, no Go runtime metrics. 23 tests and the endpoint's. `tests/call/run.sh musivum` starts it; the call fails at `/rtc` (404) until the signalling endpoint is wired in. |
| `pkg/utils/opsqueue.go`, `changenotifier.go`, `incrementaldispatcher.go`, `math.go`, `slice.go`, `iceconfigcache.go`, `logging.go`, `protocol.go` | `utils/` (same names) | 8 / 8 | Fourteen tests added. The ops queue's worker is a thread (an op may block, and `stop().wait()` has to work from a thread of the runtime): one thread per queue. `context.go` is not ported (only the service and the router use it). |
| (new, in place of `sfufakes`, `typesfakes`, `routingfakes`, `transportfakes`) | `crates/musivum-fakes`, `testutils/fakes.rs` | | A procedural macro that makes a trait's fake at compile time, with counterfeiter's calls. 18 tests of its own; the uses of fakes in `downtrack_downstream_integration_test.go` and `room_test.go` were written out with it line for line as a check. A trait of 150 methods adds about a third of a second to a check of the tests. |
| (new) | `webrtc/track.rs`, `webrtc/peerconnection/` | | Written new: the traits `pkg/sfu` meets the WebRTC library through, and musivum's peer connection on `rtc`. Twelve tests. See `docs/webrtc-library.md`. |
| `pkg/routing/interfaces.go` (the message sink and source, `ParticipantInit`), `messagechannel.go`, `errors.go` | `routing/` (same names, with `message.rs`) | 1 / 1 | Only what one node needs: no relay between nodes. A message is an enum of signal request and signal response. A reader awaits `recv()` or blocks on `blocking_recv()`. Four tests added. |
| `pkg/routing/interfaces.go` (the router), `localrouter.go`, `node.go`, `nodestats.go`, `roommanager.go`, `signal.go` (one node's path) | `routing/` (same names, with `bus.rs` new) | none upstream | The router of one node. psrpc's local bus, which LiveKit passes a session's start through even on one node, is `bus.rs`: a direct call where psrpc stood, in the original's order of steps (compared with a run of the original's local router over psrpc's local bus). `redisrouter.go` is left out. Two tests added. |
| `pkg/routing/selector/` | `routing/selector/` | 8 / 8 | The limits and availability of a node by its statistics, as the connect validation checks them. `regionaware.go` (choosing among nodes of several regions) is left out. |
| protocol `utils/hwstats`, with go-osstat's Linux readers | `protocol/utils/hwstats/` | 7 / 8 | The system's CPU and memory load for the node's statistics, read from `/proc` and cgroups; other systems report zero. Not ported: the process monitor (egress), `TestAggregateMemoryStats`. |
| (new: Prometheus' text format) | `telemetry/prometheus/exposition.rs` | | The collectors written as client_golang writes them (text format 0.0.4, the content type `expfmt.Negotiate` gives for an `Accept` header), with the node's constant labels: byte for byte equal to the Go client's output for every kind of collector and 22 float cases, and the content type for 17 `Accept` headers. |
| `pkg/telemetry/prometheus` (`GetNodeStats`, `RoomStarted`, `RoomEnded`, the join counters) | `telemetry/prometheus/` (`node.rs`, `rooms.rs`, `packets.rs`) | | The node's statistics read the same counters the telemetry service writes. Traffic control statistics are zero (they need netlink). |
| `pkg/rtc/signalling/` (ten files) | `rtc/signalling/` (same names) | 2 / 2 | The three interfaces are traits with default method bodies where Go embeds an "unimplemented" struct. Nine messages built by `signalling.rs` are compared byte for byte with the original's. Nothing of psrpc is ported. `ParticipantCloseReason` and `SignallingCloseReason` of `types/interfaces.go` are ported with it. |
| `pkg/rtc/types/interfaces.go` | `rtc/types/interfaces.rs` | none upstream | The seventeen interfaces as traits with fakes: 327 of 330 methods (left out: the three that return zap's deferred fields or the observability reporters). The rules by which a signature became Rust's are in `docs/porting-guide.md`. Seven tests added; the tables of close reasons are compared with a run of the original. |
| `pkg/rtc/testutils.go` | `rtc/testutils.rs` | none upstream | The mock participant and track that most tests of the package start from. One test added. |
| `pkg/rtc/types/ice.go` | `rtc/types/ice.rs` | none upstream | On the candidate type of `rtc`. Eight tests added, their expected values from a run of the original over 25 candidate lines. Where `rtc`'s candidate parser differs from pion's the test asserts the library's answer with pion's beside it: an IPv6 address with a zone and a host name ending in `.invalid` are parse errors, `tcptype` is only read directly behind the type. Nothing calls `set_selected_pair` yet: the layer has no event for the selected pair. |
| `pkg/rtc/config.go` | `rtc/config.rs` | none upstream | The header extensions, RTCP feedback, FlexFEC, RTX budget and buffer sizes of both directions are compared with the original for the default configuration and with each send-side estimator switch: identical. |
| `pkg/rtc/mediaengine.go` | `rtc/mediaengine.rs` | 4 / 4 | Registers into `rtc`'s media engine, which has no getters: the tests make an offer on a connection of the layer and read it, as upstream's do. A full offer compared with the original's: the same payload types, rtpmap, fmtp, feedback and extension ids; the `a=extmap` lines come in another order. |
| `pkg/rtc/transport/handler.go` | `rtc/transport/handler.rs` | none upstream | The trait has default bodies equal to `UnimplementedHandler`'s, and a fake. |
| `pkg/rtc/participant_data_blob_handler.go` | `rtc/participant_data_blob_handler.rs` | 4 / 4 | The original's methods of the participant are provided methods of a trait the participant will implement. |
| `pkg/rtc/uptrackmanager.go` | `rtc/uptrackmanager.rs` | 2 / 2 | |
| `pkg/rtc/wrappedreceiver.go`, `subscribedtrack.go`, `mediatracksubscriptions.go`, `mediatrackreceiver.go` | `rtc/` (same names) | none upstream | 25 tests added, most with values from the same steps run against the original (receiver chosen by negotiated codec, a dummy receiver upgraded, settings applied after the debounce, a subscriber added, refused twice, removed, a cached transceiver reused through `replace_track`, receivers ordered by priority, a codec regression). Upstream's two tests of `mediatrack.go` already pass against `MediaTrackReceiver`. |
| `pkg/rtc/mediatrack.go` | `rtc/mediatrack.rs` | 2 / 2 | Eight tests added that run `add_receiver` with real buffers from a buffer factory, their values from the same steps against the original (three simulcast layers, a backup codec, RED both ways, mute, dynacast, close). A second announcement of the same SSRC, which `rtc` makes after every renegotiation, is harmless here, where upstream would close the track: the one place this file leaves the original, tested. |
| `pkg/rtc/transport.go` | `rtc/transport_impl.rs` (`rtc::transport` is taken by the subpackage) | 24 / 25 | LiveKit's wrapper around one peer connection, on musivum's connection layer: the negotiation state machine, ICE candidates and their filtering, data channels with their writers, the estimator, pacer and stream allocator of a subscriber connection, the packet sink. All three test files on real connections over the loopback interface, ten runs in a row. Ignored, with the gap named: the test of skipping DTLS HelloVerify (`rtc` has no such setting). The two tests of configuring a send-only audio transceiver pass since a patch of `rtc` gives a sender pion's codecs (`third_party/rtc`). Paths for TURN are ported as logic and reach nothing; TCP candidates exist when the node has a TCP listener. Migration stops where the layer has no `SetMid`, so a participant moving between nodes reconnects fully. |
| `pkg/rtc/transportmanager.go` | `rtc/transportmanager.rs` | none upstream | A participant's one or two transports: routing of signalling, data messages, ICE configuration and the fallbacks from UDP to TCP to TURN. Five tests added on real transports over the loopback interface (two connections and one, a failed connection and the next attempt's ICE configuration); the 22 rows of the fallback decisions compared with the original's answers for the same inputs. The decisions are ported exactly; with no TURN in the connection layer, a fallback to TURN changes the preference and nothing else. |
| `pkg/rtc/room.go` | `rtc/room.rs` | 10 / 10 | Joining and leaving, updates to the others, subscriptions, active speakers, data messages and RPC between participants, connection quality, the room's timeouts. Agents, egress and the observability reporters are left out (a comment at the first place of each). The tests run against the fake telemetry service: no upstream test asserts on telemetry. 20 runs in a row. |
| `pkg/rtc/participant.go`, `participant_signal.go`, `participant_sdp.go`, `participant_data_track.go` | `rtc/` (same names) | 21 / 21 | The participant: publishing and subscribing, signal requests, the SDP munging, data tracks and data blobs, RPC between participants, the state machine and its close paths. Re-read against the original by a second agent (which found and fixed a close path that let go of the participant too early). Left out: the observability reporters, `pkg/metric`, the migration data cache; and the wait in `onMediaTrack` for a first packet to learn a payload type, which the connection layer makes unneeded (it announces a track at its first packet, with its codec). 10 runs in a row. |
| `pkg/rtc/subscriptionmanager.go` | `rtc/subscriptionmanager.rs` | 11 / 11 | The manager holds its participant weakly; the callbacks for a closed track hold it strongly until the track has closed, as upstream's closures do. The reconciler is a task; `close` waits for it as upstream does. The tests' three overridable timeouts are constants with the test values in a test build. `subscriptionTimeout` is the transport's ICE failure timeout, written out until `transport.go` is ported. |
| `pkg/rtc/roomtrackmanager.go`, `medialossproxy.go`, `signalanddatastats.go` | `rtc/` (same names) | none upstream | Twelve tests added. Of `signalanddatastats.go` the counting and the reports to telemetry are ported; what feeds the observability reporters is left out, so `BytesTrackStats::new` takes four arguments where upstream's takes seven. |
| `pkg/rtc/supervisor/` (two files) | `rtc/supervisor/` (same names) | 2 / 2 | One test added. |
| `pkg/rtc/datatrack.go`, `datadowntrack.go`, `datatrack_stats.go`, `updatatrackmanager.go` | `rtc/` (same names) | 1 / 1 | Six tests added, their expected bytes and counts from a run of the original. The byte statistics (`BytesTrackStats`) are wired in; a down track's statistics hold the subscriber weakly. |
| `pkg/rtc/dynacast/` (six files) | `rtc/dynacast/` (same names) | 3 / 3 | The base is generic over the state of its manager, which its one lock guards as upstream. The listener is held weakly (the media track is listener and owner). A manager dropped without `close()` notifies nobody afterwards. `RegressTo` with nothing to regress to does nothing where Go dereferences nil. |

The files of the row group from `rangemap.go` down were also run against the Go originals
with the same pseudo-random inputs (34,021 lines of output, identical).

## Options of the `rtc:` section that are accepted and not applied

`mediatransportutil/rtcconfig` reads every option LiveKit has. These need something the
connection layer (`webrtc/peerconnection`) does not have yet; each is logged when set:

| Option | What happens now | What it needs |
|---|---|---|
| `use_mdns: true` | `rtc` is left at query only | the node routing mDNS answers |
| `stun_servers` as ICE servers of a node that is not ICE lite | the list is computed, nothing gathers | server reflexive candidates in the layer |
| `batch_io`, the SCTP window settings, the 16 MiB socket buffers | kept as data | batched writes; settings `rtc` 0.21 does not have; buffer sizes on the node's socket |

IPv6, `interfaces`/`ips` (a socket per address), `udp_port` as a range, `port_range_start`/`_end`
(the first free ports of the range, as many as processors, given to connections in turn),
`use_stun_port_as_ice` and ICE over TCP (`tcp_port`, `force_tcp`) are applied
(`docs/webrtc-library.md`). LiveKit's embedded TURN server is ported (`turn:`).

## Where the port cannot be the original

- **Nil against empty in the dependency descriptor.** Go's writer takes a nil `FrameDiffs`
  or `DecodeTargetIndications` as "as the template has it" and an empty one as "none". A
  `Vec` has no nil. In the port `frame_diffs` is always taken as given (which is how the
  forwarding path behaves upstream, where the parser's storage is never nil) and an empty
  `decode_target_indications` is taken as nil. A descriptor made by hand with nil frame
  diffs is therefore written differently (`08005a10` where Go writes `08005a`); the cases
  are pinned in `reference_test.rs`.
- **The zero time in a protocol timestamp.** Go's `timestamppb.New(time.Time{})` gives year
  1; `go::time::Time` cannot tell its zero from the Unix epoch, and `timestamppb_new` gives
  the epoch for it.
- **Access tokens and key files, at the edges.** Each is asserted in a test marked "not as
  the original". Written JSON is the same but not byte for byte (Go escapes `<`, `>`, `&`;
  the order of map entries). Of duplicate claims the last wins (Go merges two objects). In
  a room configuration inside a token, an unknown enum value is refused (Go keeps or drops
  it) and of a duplicate field the last wins (Go refuses). A key file with a tab right
  after the colon is refused (Go reads it); three forms Go refuses are read. The first two
  could be had from `pbjson-build`'s options in `musivum-protocol`; not tried.
- **Unset times in `RTPStats`.** The last key frame, the last FIR and the last layer lock PLI
  of statistics that never had one are written as 1970-01-01 where the original writes
  0001-01-01 (the zero time, see above).
- **Float to integer out of range in `rtpstats`.** `fraction_lost` of an interval in which
  every expected packet was lost is 255 here and 0 in Go on amd64 (`uint8(256.0)`).
- **Protocol messages in the log are not redacted.** Upstream logs a message through
  `logger.Proto`, which leaves out fields marked as sensitive (metadata, names, attributes,
  the TURN credential). The port logs a message with `Debug`, in full. The lines concerned
  are at debug level ("handling signal request", "sent signal response", a participant's
  init). To be settled for the whole crate before it serves real users.
- **Simulcast announced by SSRCs instead of rids.** A publisher whose description lists
  its layers as `a=ssrc-group:SIM` without rids gets one track in `rtc` (the last SSRC
  listed); its other layers reach the sink and wait in buffers nobody binds. pion places
  them with LiveKit's `unhandlesimulcast` interceptor. LiveKit's SDKs and the browsers
  tested announce rids. Pinned by a test of the sink.
- **A client that nominates another candidate pair without an ICE restart is not
  followed.** `rtc`'s ICE agent selects a pair only while it has none; pion's follows a
  later nomination. The server keeps sending to the old address until ICE times out.
- **The mid in a sender's SDES.** The down track's source description carries the CNAME
  only; the original adds an item with the transceiver's mid (type 15), which `rtc-rtcp`
  0.21 cannot write and reads as the end of the list.
- **Assertions on allocations per call** (`testing.AllocsPerRun`, in about ten upstream test
  files) are left out: counting allocations needs an allocator of the test binary's own,
  which needs `unsafe`, and the workspace forbids it.

## The first call

On 2026-11-14 the scripted call of `tests/call` (two participants with LiveKit's JavaScript
SDK in headless Chrome: join, publish simulcast video and audio, subscribe, layer switching
by element size, data of both kinds, active speakers, mute, connection quality, unpublish,
leave) went through musivum with all 20 observations as against LiveKit, in four runs in a
row. The call needed no fix to go through; comparing its signalling and logs with LiveKit's
found three differences, fixed: the connection layer announced a remote stream again after
every renegotiation (`on_track` is now once per stream, as pion's), the setting engine left
out the TCP network types (no TCP candidates were offered), and TCP candidates lacked pion's
priority offset. What still differed then: no IPv6 candidates (added since), and candidate lines
without pion's `ufrag` attribute.

## Behind asaroton

On 2026-11-14 asaroton's lab ran musivum in place of `livekit-server`, with the configuration
asaroton writes for LiveKit: a call of two Element Web peers in Element Call (90 s) worked as
with LiveKit (both over UDP, each receiving both 720p videos for the whole call). It found
one bug: no webhook ever left the server binary, because the senders ran on musivum's own
runtime, which had no network IO (fixed, with a test of the server made as `main` makes it;
after the fix asaroton received the same 14 webhooks as from LiveKit, and its delayed leave
events went out 2 ms after a participant closed). Also as with LiveKit: a three-way browser
call (the tiles drop to 360p with three and return to 720p when one leaves), and Element X
26.10.1 on Android in a call with Element Web (video both ways, the hang-up seen at once).
Then, each against LiveKit on the same configuration: a relay-only call (Chrome kept off
direct UDP, ICE over TCP off, TURN over TLS on a name of the lab's own CA): both peers on
relay over UDP and TLS, two 720p videos each for the whole call, as with LiveKit; the relay
policy refused the host's other local addresses and took only the one in
`allow_restricted_peer_cidrs`. A call over ICE over TCP alone (no UDP, the TURN name not
resolvable): both peers on host candidates over TCP, video both ways. A call of 30 minutes
with two browsers: every video at 18.8 to 20.1 fps in every 15 s interval (the fake camera
sends 20), no stall; musivum's memory 41 MB at the start and 47 MB at the end, 62 s of CPU
in the 30 minutes; no warnings or panics. Not tested there: Safari and iOS, Element X
through TURN, more than three participants, federation, more than one machine.

## On a degraded network

The seven scenarios of `tests/netem`, three runs of each against each server (2026-11-14,
musivum after the fan-out change; the two taken in turn, each run started below a load
average of 2, none flagged busy; `tests/netem/runs/`,
`node tests/netem/compare.js 'runs/*/reference' 'runs/*/musivum'`). The relay's loss has the
same seed in every run, so the spread is that of timing. Medians [ranges], LiveKit against
musivum: a clean network, no freeze in either. 2% random loss: 1 [0–1] freeze in each, 720p
throughout. 5% loss in bursts: 5 [4–5] freezes against 4 [3–4] (1.7 against 1.3 s), 720p
throughout. 150 ms delay with jitter: 2 [2–3] freezes in each, both down to 360p (musivum once
to 180p); back at 720p within the run in none of LiveKit's runs, in two of musivum's (22 and
23 s). Subscriber's downlink capped: both down to 180p in all six runs, 2 [1–4] against
2 [1–2] freezes (3.2 [3.0–7.3] against 2.5 [2.5–2.6] s). Publisher's uplink capped: 360p
during, recovered in 18 [18–21] against 20 [17–21] s. 3 s blackout: one freeze of 3.2 s and
11 s to recover in both (LiveKit once not at all).

The differences the single runs suggested were noise: musivum's freezes under 2% loss did not
repeat, and the other two were LiveKit's baseline being the odd one out (its three new runs
also go down to 180p under the capped downlink and also receive no frame in the blackout).
What holds up (ranges apart in all three runs, which happens by chance one time in ten for a
metric): at the end of the capped-downlink run the subscriber has 720p at 5 fps from musivum
and at 10 fps from LiveKit. Both climb the same way (360p at 10 fps, 720p at 5, 720p at 10);
musivum reaches 720p sooner, LiveKit takes the next step within the run (once only after 24 s
at 720p/5). musivum may step up the frame rate more slowly; a longer run would tell. Smaller
and consistent: under the 150 ms delay musivum keeps 11 fps where LiveKit drops to 7–10, but
conceals more audio (434 against 327 ms) and has a 9 ms higher round trip; under loss its
subscriber counts about 10% fewer lost packets.

## Under load

`tests/load` (LiveKit's `lk load-test`), three runs of each scenario against each server
(2026-11-14, the two taken in turn, each run started below a load average of 2;
`tests/load/runs/`, `python3 -I tests/load/compare.py 'runs/*/reference' 'runs/*/musivum'`).
In all 24 runs every subscriber gets all its media, with no loss and no join errors, up to
100 subscribers of one track and 10 calls of 4. musivum's memory is lower in every run (idle
13 MB against 50; at the peak 49 to 63% of LiveKit's). Its CPU is higher in every run, by 4
to 18%; the ranges are within 4% of each median, so the difference is real. Steady CPU in
cores, median [range]:

| Scenario | LiveKit | musivum at first (one run) | musivum now |
|---|---|---|---|
| a: one room, 2 publishers, 10 subscribers | 0.049 [0.048–0.050] | 0.054 | 0.051 [0.051–0.053] |
| b: the same with 50 subscribers | 0.202 [0.201–0.206] | 0.838 | 0.236 [0.235–0.237] |
| c: 10 calls of 4 | 0.286 [0.281–0.286] | 0.340 | 0.309 [0.307–0.314] |
| d: one publisher, 100 subscribers | 0.190 [0.189–0.194] | 0.467 | 0.225 [0.223–0.225] |

The load generator itself uses more CPU against musivum (b: 0.31 against 0.24 cores, d: 0.28
against 0.25) while receiving the same media packets, so musivum sends it something more
(RTCP, padding or probes); not looked into yet.

What closed the gap, found with `perf` on both servers: the ported fan-out of a packet to
its down tracks started a thread of the operating system per worker for every packet, then
ran on rayon's pool, whose idle threads spin between packets. It now runs as Go runs it: the
caller is the first worker and starts more only while work is left, on a small pool of its
own whose threads sleep as soon as the queue is empty (`go/sync.rs`, with Go's
`sync.WaitGroup`). What is left is in the write of each packet: `rtc`'s sans-I/O pipeline
passes it through every handler with queues and copies (19 to 25% of musivum's CPU in b),
where pion writes straight through; closing that means changing the connection layer or
`rtc`. Not measured yet: latency and freezes under load, runs of minutes, joins and leaves
during a run. Each ops queue is a thread of its own (four per participant): they cost
threads, not CPU.
