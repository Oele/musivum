# What is ported

One line per upstream file or package that has been dealt with. "Tests" is the number of
upstream test functions that were ported and pass, of the number upstream has; a difference
is explained in the notes. `docs/port-map.md` says what each file is and where it goes.

Differences from upstream's behaviour that were found on the way are listed at the end: what
looked wrong upstream and was ported as it is, and what could not be ported as it is.

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
| `pkg/config/config.go` | `config/` (`config.rs`, with `yaml.rs`, `fields.rs`, `logger.rs`, `webhook.rs`, `metric.rs`) | 11 / 16 | LiveKit's configuration file, read as LiveKit reads it: defaults first, the file over them, strict mode, yaml.v3's rules for scalars (a decoder of musivum's own on `saphyr-parser`, following yaml.v3's; serde cannot decode onto defaults or switch strictness at run time). Compared with the original value by value: 315 values of the defaults, 320 of the sample file with every option uncommented, 353 of a file of edge cases, and the texts of 31 errors with their line numbers: all agree. Fourteen sections for what musivum leaves out (`redis`, `agents`, `sip`, `ingress`, `psrpc`, ...) are accepted and ignored with a log line each; strict mode does not look inside them. Not ported: the generated command line flags (two tests), `psrpc` (two tests), a test that reflects over struct tags. 21 tests added. |
| `pkg/service/interfaces.go`, `localstore.go`, `roomallocator.go`, `auth.go`, `utils.go`, `wsprotocol.go`, `errors.go` | `service/` (same names, with `useragent.rs`) | 5 / 7 | The foundation of the outer layer, for one node, over the `http` crate's types (no HTTP server in the library). Authentication of a request and validation of a connect request return the original's status codes and texts (compared with a run of it). Signal messages on a WebSocket through a small async trait a server implements for its socket, with gorilla's handling of control frames. The user agent is read by a small recogniser that gives uap-core's names for the browsers, systems and devices the client rules name (41 user agents, all equal to the original's answers). Not ported: the preset half of `TestCreateRoom` (named room configurations are left out), `TestRemoveDoubleSlashes` (Go's HTTP plumbing). Node selection, agents, SIP, ingress and egress are left out. |
| `pkg/utils/opsqueue.go`, `changenotifier.go`, `incrementaldispatcher.go`, `math.go`, `slice.go`, `iceconfigcache.go`, `logging.go`, `protocol.go` | `utils/` (same names) | 8 / 8 | Fourteen tests added. The ops queue's worker is a thread (an op may block, and `stop().wait()` has to work from a thread of the runtime): one thread per queue. `context.go` is not ported (only the service and the router use it). |
| (new, in place of `sfufakes`, `typesfakes`, `routingfakes`, `transportfakes`) | `crates/musivum-fakes`, `testutils/fakes.rs` | | A procedural macro that makes a trait's fake at compile time, with counterfeiter's calls. 18 tests of its own; the uses of fakes in `downtrack_downstream_integration_test.go` and `room_test.go` were written out with it line for line as a check. A trait of 150 methods adds about a third of a second to a check of the tests. |
| (new) | `webrtc/track.rs`, `webrtc/peerconnection/` | | Written new: the traits `pkg/sfu` meets the WebRTC library through, and musivum's peer connection on `rtc`. Twelve tests. See `docs/webrtc-library.md`. |
| `pkg/routing/interfaces.go` (the message sink and source, `ParticipantInit`), `messagechannel.go`, `errors.go` | `routing/` (same names, with `message.rs`) | 1 / 1 | Only what one node needs: no relay between nodes. A message is an enum of signal request and signal response. A reader awaits `recv()` or blocks on `blocking_recv()`. Four tests added. |
| `pkg/routing/interfaces.go` (the router), `localrouter.go`, `node.go`, `nodestats.go`, `roommanager.go`, `signal.go` (one node's path) | `routing/` (same names, with `bus.rs` new) | none upstream | The router of one node. psrpc's local bus, which LiveKit passes a session's start through even on one node, is `bus.rs`: a direct call where psrpc stood, in the original's order of steps (compared with a run of the original's local router over psrpc's local bus). `redisrouter.go` is left out. Two tests added. |
| `pkg/routing/selector/` | `routing/selector/` | 8 / 8 | The limits and availability of a node by its statistics, as the connect validation checks them. `regionaware.go` (choosing among nodes of several regions) is left out. |
| protocol `utils/hwstats`, with go-osstat's Linux readers | `protocol/utils/hwstats/` | 7 / 8 | The system's CPU and memory load for the node's statistics, read from `/proc` and cgroups; other systems report zero. Not ported: the process monitor (egress), `TestAggregateMemoryStats`. |
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
| `udp_port` as a range, `port_range_start`/`_end` | the first port is used | several sockets per node, or a decision that one port stands for the range |
| `use_stun_port_as_ice` | nothing is bound on 3478 | a second UDP listener |
| `interfaces`, `ips` | they choose which addresses are advertised; the socket listens on all | a socket per address |
| `use_mdns: true` | `rtc` is left at query only | the node routing mDNS answers |
| `stun_servers` as ICE servers of a node that is not ICE lite | the list is computed, nothing gathers | server reflexive candidates in the layer |
| IPv6 addresses | not advertised | a second (IPv6) listener |
| `batch_io`, the SCTP window settings, the 16 MiB socket buffers | kept as data | batched writes; settings `rtc` 0.21 does not have; buffer sizes on the node's socket |

Clients that cannot use UDP are therefore not served yet (an open question of the plan).

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

## Noted on the way

What looked wrong or fragile upstream and was ported as it is:

- `audio/audiolevel.go`: the duration of a gap is computed in 32 bit, `(ts - highest) * 1e3 /
  ClockRate`, and wraps for a gap of more than about 89 s at 48 kHz. A clock rate of 0
  divides by zero.
- `utils/rangemap.go`: `GetValue` and `DecValue` compare keys plainly in some places and
  with wrap-around in others, so behaviour after the key type wraps is odd.
- `datachannel/bitrate.go`: a window shorter than 1 ms divides by zero.
- mtu `bucket_test.go`: `TestQueueEdgesExtended` asserts `require.Error(t, err,
  ErrPacketSizeInvalid)`, which passes on any error. `TestGrowingFullBucket` does not notice
  if growing stops moving the wrapped slots (`TestQueueGrow` does).
- mtu `nack.go`, `Pairs()`: a sequence number that is in the queue twice is counted each
  time but adds no bit. A sequence number 1 to 17 below the first entry of the queue, met
  before any pair is open, is counted but lands in no pair (read, not run).
- mtu `bucket.go`: `addPacket` refuses packets longer than the slot size minus the size
  header although the slot has room (read, not run); `wrap()` reduces by repeated
  subtraction, so a very large jump in a 64 bit sequence number loops for a long time.
- pion/rtp `packet.go` (the first four confirmed by running pion):
  `MarshalPacketTo` with the padding bit set and a padding size of 0 writes a zero byte 255
  bytes past the payload, or panics if the buffer is shorter (`Packet.MarshalTo` refuses the
  same packet). A padding size without the padding bit grows the packet without marking it.
  `SetExtension` on a header without extensions checks neither id nor size. A reused
  `Header` keeps the previous extension profile when the next packet has no extension. A
  one-byte extension with an empty payload marshals as `id<<4 | 0xFF`. More than 15 CSRCs
  spill into the version bits.
- pion/rtp tests: `TestClonePacket` compares a byte with an untyped integer and cannot
  fail; `TestAbsCaptureTimeExtension_Roundtrip` computes its second value from the first
  input; `TestBasic` builds an expected packet for excessive padding and never compares it.
- `ccutils/prober.go`: a worker only ends when it next wakes and finds no cluster, so a
  cluster added right after the queue emptied gets a second worker and both drive it (about
  twice the probes, measured in Go and in the port). `OnProbeClusterSwitch` is called with
  the prober's lock held. Cluster sizes use `math.Round(x + 0.5)` as a ceiling, which adds
  one when `x` is whole.
- `ccutils/trenddetector.go`: a lowest value of 0 means "not set"; a flat series collapses
  to one sample and is inconclusive for lack of samples, not by its trend.
- protocol `codecs.go`, `areFmtpLinesEqual`: not symmetric (`"a=1"` against `"b="` is equal,
  the other way round is not), and parameters without `=` are ignored. protocol
  `mimetype.go`: `IsMimeTypeStringAudio` and `...Video` compare the prefix with case while
  everything else folds case.
- mtu `twcc_test.go`, `TestBuildFeedbackOnPush`: the case "should not build when mSSRC is
  invalid" expects no feedback and gets one, at the versions LiveKit is built with (run:
  "expected: 0, actual: 1"). Nothing in the responder or in pion's recorder looks at the
  media SSRC.
- pion/interceptor `twcc_test.go`: two tests have inputs that depend on Go's order of
  evaluation inside a slice literal; the port uses the values Go actually produces.
- mtu `owd_estimator.go`: after a perfectly steady phase the estimator does not follow a
  lasting rise of 20 to 100 ms even after 600 reports (seen in the port; follows from the
  same lines upstream, not run there).
- `rtc-rtcp` against pion/rtcp, for transport-wide feedback: the bytes agree. Differences
  that code reading subscribers' feedback has to know: a delta that does not fit its type
  fails the whole marshal (pion skips it); when unmarshalling a truncated packet, a large
  delta with one byte left is read as a small one (pion: "packet too short"; read, not
  tested).
- `dependencydescriptor/dependencydescriptorwriter.go`, `findBestTemplate`: the search for a
  cheaper template runs over templates of other layers and can pick one (a frame of
  temporal layer 1 is written with a template of layer 2 and reads back as layer 2), and
  for a frame of the structure's last layer only that layer's first template is tried. The
  writer writes the extension's structure, not the descriptor's attached one, when both are
  set. Marshalling without a structure or without frame dependencies panics (a nil
  dereference upstream; the selector there wraps the call in `recover()`).
- `dependencydescriptor/dependencydescriptorreader.go`: a reused descriptor keeps its
  attached structure and active decode targets from the packet before.
- `buffer/videolayerutils.go`, `LayerPresenceFromTrackInfo`: indexes an array with a
  layer's quality after checking only that it is not above `HIGH`. A negative quality in a
  `TrackInfo` panics, in Go (run at the pin) and in the port. To be checked with
  `pkg/rtc` whether a client's layers reach a `TrackInfo` unvalidated.
- `buffer/frameintegrity.go`, `PacketsConsecutive`: with a history of 64 packets or fewer
  it can shift by a negative count, which panics in Go (run) and in the port. LiveKit uses
  1024. `buffer/rtcpreader.go`: `Write` reports 0 bytes written on success.
  `buffer/datastats.go`: a duration of zero gives an infinite or undefined bitrate.
- `bwe/remotebwe`: `ProbeClusterDone` with the invalid cluster on an estimator that is not
  probing starts a hangover (a new controller holds the invalid cluster, and only the ids
  are compared). `ProbeClusterFinalize` after a congesting probe sets the state back to
  none without telling the listener. While congested, the committed capacity follows the
  estimate up as well as down. The NACK window's maximum only ends in `Add`.
- `rtpstats`: the lite receiver and sender hand `ToProto` the start and the highest
  extended sequence number where it takes packets expected and packets seen, so `Packets`
  is the highest sequence number and the loss percentage is lost over the start. The lite
  sender decrements `packetsLost` on every duplicate or late packet without a check. A
  reception report's `LastSequenceNumber` is the extended highest plus one. In the
  receiver, a packet from before the start that has a payload and a newer time stamp is
  taken for a rollover of the sequence number: 65,536 ahead, about 63,000 packets counted
  lost (seen in the run). `Test_RTPStatsReceiver_Update` starts at a random sequence number
  and fails when that is within 13 of 65535, upstream and in the port.
- mtu `rtcconfig`: `use_external_ip` with `port_range_start` equal to `port_range_end`
  panics (`rand.Intn(0)`), here too. A YAML mapping or sequence for `udp_port` or `node_ip`
  is silently ignored. `GetLocalIPAddresses` does not look at whether an interface is up.
  `port_range_start` alone suppresses the default ports in `Validate`, and then neither a
  range nor a port is used.
- protocol `webhook`: any answer that is not a 5xx (a 401, a 404) counts as sent.
  `DefaultNotifier.QueueNotify` returns at the first notifier's error, so later URLs miss
  that event; its `SetKeys`, `SetFilter` and `RegisterProcessedHook` never reach the extra
  notifier. A `Retry-After` is not capped. `errNoKey` says "no singing key". Read without a
  lock upstream (a data race) and under it here: `IsAllowed`, `EventKey`.
- `pkg/routing`: `LocalRouter.Stop()` does not stop the statistics worker. A node without
  statistics counts as available and then panics when sorted by them;
  `NewLocalNodeFromNodeProto` gives such a node. `hwstats` panics on an empty file, and
  go-osstat on a first line of `/proc/stat` with more than ten values. Which cgroup version
  is probed first is random upstream (a range over a map) and fixed here.
- `rtc/participant.go`: in `PerformRpc`, `responseTimeout - lastRTT` can wrap (uint32).
  `Close` clears the pending RPC acknowledgements before it resolves the pending responses,
  so their timers still fire; a response resolves its acknowledgement and never deletes it.
  `setTrackMuted`, `UpdateAudioTrack` and `UpdateVideoTrack` change pending tracks under the
  read lock (a data race; the write lock here).
- `rtc/transportmanager.go`: `ProcessPendingPublisherDataChannels` shares one `ordered`
  variable across its loop, so a reliable channel listed after a lossy one is created
  unordered. Once an ICE preference is set, `configureICE` removes the media loss callback
  and never restores it. `SubscriberClose` and `SetMigrateInfo` with a single connection
  dereference a nil subscriber.
- `rtc/room.go`: `RemoveParticipant` decrements `NumParticipants`, which is never
  incremented (recounted by `updateProto`). `Join` holds the room's write lock while it calls
  out to the participant, so a callback that calls back into the room deadlocks.
  `BroadcastDataMessageForRoom` checks `source` for nil and then dereferences it.
  `connectionQualityWorker` sends one more round after the room has closed.
- `rtc/transport.go`: `CreateDataChannelIfEmpty` logs the empty returned label instead of
  the channel's; `createAndSendOffer` warns about "offer before answer" with ids read
  without a lock.
- `rtc/mediatrack.go`: `NotifySubscriberNodeMediaLoss` on a video track dereferences the
  loss proxy, which only audio tracks have. `SetMuted(false)` forces a dynacast update
  before unmuting, and the listener reads the mute state on another thread, so the notice
  after unmuting can be lost.
- `rtc/mediatrackreceiver.go`, `UpdateAudioTrack`: `DedupeSlice` zeroes the tail of the
  caller's slice, and zero is `TF_STEREO`, so a feature named twice turns stereo on (run in
  the original). `subscribedtrack.go`: the loop over the RTCP feedback writes to copies of
  the codecs and changes nothing.
- `rtc/transport/handler.go`: `UnimplementedHandler.OnFailed` takes one parameter where the
  interface's takes two, so `UnimplementedHandler` alone does not implement `Handler` in Go.
  `participant_data_blob_handler.go`: the text for a data blob over the limit is "async
  attribute definition exceeds limit".
- `rtc/uptrackmanager.go`: `maybeRevokeSubscriptions` calls into the tracks with the write
  lock held. `roomtrackmanager.go`: `GetTrackInfo` gives nothing while the earliest track
  under an id is closing, even if a later one under that id is open. `medialossproxy.go`
  has no timer: a collected loss is passed on only by the next report after the interval.
- `rtc/datatrack.go`: `Close` neither closes nor removes the down tracks, so a closed track
  refuses new subscribers and keeps forwarding to the ones it has. `updatatrackmanager.go`:
  a second track under the same handle replaces the first without closing it (the refusal
  is in the participant). `supervisor`: after `Stop` it checks once more at its next tick.
- `pkg/service`: `ValidateConnectRequest` does not check the sizes of metadata and
  attributes (the room manager does). `GetClientIP` returns the whole `X-Forwarded-For`
  list, not its first address. The device model is put together as "Generic Smartphone
  Smartphone" and "Samsung SM-S918B SM-S918B". The client configuration rule
  `c.browser == "safari"` never matches iOS, whose browser is named "Mobile Safari".
  `UnlockRoom` without a lock panics. `CreateRoom` ignores `isExplicit`.
- `pkg/config`: `config-sample.yaml` documents `debug_handler_port:`, which is not a key
  and fails strict loading upstream too. A secret that YAML reads as a number is silently
  dropped on the `LIVEKIT_KEYS` path and kept as text in the file's `keys:`. A file of only
  comments gives "could not parse config: EOF".
- `rtc/signalling`: `signallerAsync.WriteMessage` warns "unknown message type" for a
  message that is no signal response and then writes it to the sink anyway; the handler
  drops the errors of `HandleOffer` and `UpdateMetadata` without logging them, and logs the
  others.
- protocol `sdp`: `SDPFragment.Unmarshal` never recognises a plain `a=end-of-candidates`;
  `PatchICECredentialAndCandidatesIntoSDP` writes the candidates into every media section;
  `ExtractStreamID` returns the second token of `a=msid`, which is the track id.
- `bwe/sendsidebwe`: `ProbeClusterIsGoalReached` is true only when the probe signal is
  not "not congesting", which reads inverted, and its validity check uses a result that is
  only set when the cluster is done, so the goal cannot be reached while the probe runs. A
  repeated feedback report counts its packets as acknowledged again, and a packet reported
  lost in two overlapping reports is counted lost twice. A receive time of 0 means "not
  received", which is also the relative receive time of the first packet ever received.
  The YAML key `estimaton_window_duration` is spelled so upstream. `math.Exp` and
  `math.Log10` are Rust's `exp` and `log10`, which may differ in the last bit (no
  difference showed in the session).
- `streamtracker`: the tracker from dependency descriptors keeps layer 0 active on a mask
  without active decode targets, and reports nothing when paused (it reports "stopped" for
  all layers when unpaused). `StreamTracker.Stop()` leaves the status as it was. The
  upstream timing tests fail if the test stalls for about 500 ms; so do the ported ones.
- `flexfec` (pion's encoder): more than 110 media packets keep the previous batch, and a
  reused encoder then protects the old packets under the new base sequence number; 110
  packets are allowed where the masks hold 109. `sfu/utils/helpers.go`,
  `FindRTXPayloadType`: compares the whole fmtp line with `apt=N`, so `apt=96;rtx-time=200`
  is not found. Two upstream tests cannot fail on what they are named for
  (`TestDecoderResetsOnBigSequenceGap`, RED's "history window edge").
- protocol `datatrack`: the size of the extensions is held and written in 16 bits, so from
  65,531 bytes of extensions on a packet that was read marshals to bytes that do not read
  back, or read back as another packet (run in both, pinned). `Header.Unmarshal` into a
  header that was used before appends to its extensions, and marshalling that runs out of
  the buffer (read, not run; the server reads into a fresh packet).
- protocol `auth`: `GetCanPublishSource` compares the stored strings exactly while
  `GetCanPublishSources` upper-cases them; `checkOutputForCredentials` has no case for
  image outputs; a token without `iss` is verified with the secret of the key `""`;
  `TestVerifier`'s "incorrect key" case uses an expired token and cannot notice a missing
  signature check (a test was added).
- protocol `utils`: `rtpstats.go` compares the time of the last FIR with the last PLI;
  a `ProtoProxy` with refresh interval 0 never updates, against its comment;
  `TimedAggregator`'s `Get...AndRestartAt` restarts also when the read failed, so time goes
  backwards; `NewBitmap` allocates more than asked; two assertions of
  `timed_aggregator_test.go` have their arguments the wrong way round and cannot fail.
  `pkg/utils/opsqueue.go`: `Stop()` on a queue that was never started never closes its
  channel.
- `buffer/fps.go`: three comparisons ignore wrap-around of the frame number or timestamp, so
  a layer is not measured across a wrap until the window resets. `videolayerselector`:
  `DDForwarder.Forward` marshals without the `recover()` the selector has.
- `receiver_base.go`: in `SetCodecWithState` the reason "codec suspended" is overwritten by
  the fall-through, so buffers are always cleared with "codec invalid". A detected restart
  waits for the forwarders of all layers while only the detecting buffer wakes its reader,
  so an idle simulcast layer holds the restart until its next packet. On a WebRTC receiver,
  the RED receivers and `ReceiverRestart` hand out the embedded base, so calls through a
  RED receiver skip the outer type's overrides. `ErrDownTrackAlreadyExist` is never
  returned.
- `interceptor/unhandlesimulcast.go`: the value of the repaired-rid extension is assigned
  to `rid`, and `rsid` stays empty. mtu `rttfromxr.go`: LiveKit's RTCP reader reads
  nothing, so upstream the interceptor's read side never runs and only its once-a-second
  receiver reference time report is live.
- `buffer/buffer.go`: `Write` always reports 0 bytes written; `Close` calls `onClose` on
  every call; the FEC decoder is fed media packets under their padding-adjusted sequence
  numbers while the FEC masks are in the publisher's numbering (it affects only the
  decoder's tracking of the newest number and of gaps). `buffer_base.go`: a packet handed
  to an unbound buffer, a restart before the bind, and a bind to a codec that is neither
  audio nor video each dereference nil upstream and panic with a message here.
  `TestCodecChange` registers a callback for a stream restart that nothing in the test
  causes.
- `forwardstats.go`, the sample buffer: a producer reserves a slot, stores the value and
  then publishes the slot's number, without a lock. A producer that is held up between
  reserving and publishing for longer than the other producers need to go once round its
  shard publishes an old number over a newer one; the reader then takes the slot for "not
  published yet" and waits at it until it is lapped again, and what was waiting behind it
  is neither read nor counted as dropped. Seen once as a failure of
  `TestForwardSampleBufferConcurrent` (ported as it is) at a load average of 44; the cause
  is from reading the code, which is the original's line for line.
- `streamallocator.go`: `Stop()` on an allocator that was never started blocks forever.
  Under an early warning, a subscriber that raises its maximum layer is dropped to the
  lowest available layer until the channel clears. A change from congested back to none
  while tracks are deficient changes nothing: more comes only from probing. The first REMB
  counts one sent packet per down track that has sent nothing.
- `downtrack.go`: in `Bind`, for encrypted tracks, the check `!IsOpus(u) || !IsRED(u)` is
  true for every codec, so the early break for "RED and Opus" never happens. In
  `handleRTCP` the jitter in milliseconds is computed in 32 bits (it wraps) and divided by
  the clock rate of the first upstream codec, not the negotiated one; a clock rate of 0
  panics. `doBind` calls `onBinding` with the bind lock held, and `SetReceiver` calls into
  both receivers with it held.
- `forwarder.go`: three log lines log `"targetLayer"` with the current layer;
  `GetNextHigherTransition` takes the write lock though it only reads; a state with more
  than three sender reports panics in `SeedState`. `TestForwarderGetTranslationParamsMuted`
  would pass without the mute check, since a video forwarder without a target drops anyway.
- `connectionquality`: the discounted jitter is written into the remote view as a side
  effect of `toAggregateDeltaInfo`, which is not called on the early return for a receiver
  report from before the start, so the analytics then carry the undiscounted jitter.
  `updateScoreFromReceiverReport` uses the wall clock also when a time is passed in.
- `videolayerselector`, `TestSelectorDecisionCacheJumpIsBounded`: asserts a wall-clock bound
  of one second; the port allows ten in a build without optimisation (it went over the
  second on a loaded machine).
- `redreceiver.go`: a repeated packet goes into the history again, so the next RED packet
  carries it twice. `redprimaryreceiver.go`, `ReadRTP`: ignores the error of the unmarshal,
  and the packet written back keeps the RED payload type. Upstream's tests never reach the
  branch that drops on a sequence number or timestamp inversion (their packets have no
  extended numbers).
- `pacer`: the estimator is told of a packet, and gives out a transport-wide sequence
  number for it, before the extension is stamped, so a stamp that fails leaves a number
  that is never sent. The size it is told is the caller's header size, not the header as
  sent. The leaky bucket's worker looks at its stop signal only inside its send loop: with
  a bitrate of 0 and bytes owed it never ends and never sends again.
- `streamtrackermanager.go`: `UpdateTrackInfo` while unmuted resets every tracker, so each
  active layer is reported stopped until its next packet. `RemoveTracker` leaves the layer
  available and tells the listener nothing; on a layer of a dependency descriptor stream it
  stops the shared tracker for all layers. `RemoveTracker` and `SetMaxExpectedSpatialLayer`
  do not check their layer and panic on one out of range (`AddTracker` and `GetTracker`
  check). `DistanceToDesired` can be negative.
- `codecmunger/vp8.go`: a late packet gets the cached picture id offset but the current
  TL0PICIDX and KEYIDX offsets; the wrap handler is moved before the out-of-order check, so
  a late packet that then misses the cache still moves it; on a jump of N picture ids, N
  cache entries are inserted under the forwarder's lock before the cache is trimmed to 50.
- `rtpmunger.go`: `updateSnOffset` sets the offset to 0 when the range map lookup fails,
  where the sequencer's keeps the old one. `sequencer.go`: a packet with source and target
  sequence number 0 in the sequencer's first millisecond reads as an invalid slot.
  `rtpmunger_test.go`: `require.Error(t, err, errOutOfOrderSequenceNumberCacheMiss)` does
  not compare the error.
- `clientconfiguration/match.go`: a version that is not valid semver (any with four parts,
  such as `9.1.2.3`) is compared as a string, byte by byte, so Safari `9.1.2.3` counts as
  later than `18.3` and `100.0.0.0` as earlier. `==` on versions is semver equality
  (`18.3.0` equals `18.3`). A matching rule that is not merged returns its configuration
  alone and drops what merged rules before it gave.
- `pkg/rtc`: `CanHandleReconnectResponse` panics on a `ClientInfo` without a message.
  `UserPacketDeduper` takes a nonce of 16 zero bytes for a duplicate at first sight.
  `RTPStatsDiffToTrafficStats` clamps lost packets at zero while padding and out-of-order
  counts wrap. `TrafficLoadToTrafficRate` divides by an elapsed time of zero.
- mtu `codec/vp8.go`: `MarshalTo` trusts `HeaderSize` and panics on a buffer that is at
  least that large but smaller than what the flags write; `Unmarshal` does not reset a
  reused struct. `codec/h26xhelper.go`: `parseH264SPS` ignores read errors (a truncated SPS
  still gives a size) and reads the eight scaling list flags but never the lists (a wrong
  size for an SPS that has them); `ExtractH264VideoSize` and `IsH264KeyFrame` disagree on
  where a unit starts in an MTAP. `codec/keyframe.go`: `IsH265KeyFrame` reads the FU header
  with the mask of the NAL header, so the start of a fragmented SPS or PPS is not
  recognised; `IsAV1KeyFrame` never reports a key frame when W is 0.
- pion/rtp `vp9_packet.go`: `Unmarshal` appends to `PDiff`, `PGTID` and `PGU` without
  clearing them, so a reused packet accumulates (LiveKit resets the packet first).
- protocol `utils/rtputil`: the factor 3 in the reduction loop of `NewRTPConverter` does
  nothing (10^9 has no factor 3). protocol `utils/parallel.go`: a step of 0 never ends.

## In progress, not on `main`

Stopped on 2026-11-14 when the account's usage limit was reached; each is a branch with its
work in progress committed (not reviewed, not merged):

| What | Branch | State when stopped |
|---|---|---|

Next after these, in this order: `sfu/rtpstats` and `sfu/bwe/sendsidebwe` (what they
waited for, the utilities, is merged); `sfu/pacer` (the seam traits are merged); then the
buffer proper, `connectionquality`, `playoutdelay`; then the forwarder, the receivers, the
down track, the stream allocator; then `pkg/rtc`.
