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
| mtu `helpers.go` | `mediatransportutil/helpers.rs` | 1 / 1 | Two tests added (the way back from NTP time, RTT from a receiver report). |
| pion/rtp `packet.go`, `error.go` | `webrtc/rtp/packet.rs`, `error.rs` | 43 / 43 | Four tests added. Benchmarks left out. Run against pion with 432,008 generated inputs: every output identical, error texts and the cases where pion panics included. |
| pion/rtp `abssendtimeextension.go`, `audiolevelextension.go`, `transportccextension.go`, `playoutdelayextension.go`, `abscapturetimeextension.go` | `webrtc/rtp/` (same names) | 20 / 20 | Benchmarks left out. |
| pion/rtp `header_extension.go` | `webrtc/rtp/header_extension.rs` | 0 / 13 | Only the one function `packet.go` calls; LiveKit does not use the types of this file. |
| protocol `codecs/mime/mimetype.go` | `protocol/codecs/mime/mimetype.rs` | none upstream | Five tests added. The conversion to `roomobs` (observability) is left out. |
| protocol `codecs/codecs.go` | `protocol/codecs/codecs.rs` | none upstream | Three tests added. |
| protocol `auth/accesstoken.go`, `grants.go`, `verifier.go`, `provider.go`, `interfaces.go` | `protocol/auth/` (same names, with `jwt.rs` and `json.rs`) | 13 / 13 | Seventeen tests added. `jwt.rs` is what the original takes from golang-jwt (MIT), ported in part; `json.rs` is new. Tokens signed by Go verify here and tokens signed here verify in Go (run both ways, every grant; for fixed claims and times the token is Go's byte for byte). More than 200 unusual tokens and 60 key files were put through Go, and the tests expect Go's outcome. Token times are compared with the system clock, as in Go. `authfakes` is not ported. |
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
| `pkg/sfu/streamtracker/` (five files) | `sfu/streamtracker/` (same names) | 2 / 2 | Eleven tests added, with values from the original. A worker holds its tracker strongly, as a goroutine does: a tracker dropped without `stop()` keeps ticking. `testutils.WithTimeout` is `testutils::with_timeout` and `with_timeout_async`. |
| `pkg/sfu/flexfec/decoder.go` | `sfu/flexfec/decoder.rs` | 19 / 19 | The lookup of media packets is handed to `decode_fec` with every packet instead of to the constructor (it reads the buffer's state under the buffer's lock). Three benchmarks left out. `reference_test.rs` compares decoder and encoder with 241 lines of the original's output, all three masks included. |
| pion/interceptor `pkg/flexfec/flexfec_encoder_03.go`, `flexfec_coverage.go`, `flexfec_encoder.go`, `util/` | `webrtc/interceptor/flexfec/` (tests only) | 11 / 11 | The FlexFEC-03 encoder, for the decoder's tests. pion's own decoder, its interceptor and their tests are not ported. |
| `pkg/sfu/testutils/rtptest/packet.go` | `sfu/testutils/rtptest.rs` | none upstream | |
| `pkg/sfu/audio/audiolevel.go` | `sfu/audio/audiolevel.rs` | 1 / 1 | |
| `pkg/sfu/packettrailer/packet_trailer.go` | `sfu/packettrailer/packet_trailer.rs` | 1 / 1 | |
| `pkg/sfu/rtpextension/dependencydescriptor/` (five files) | `sfu/rtpextension/dependencydescriptor/` (same names) | 7 / 8 | `TestDependencyDescriptorMarshalAllocs` counts allocations and is left out (see the guide); two benchmarks left out. Also compared with 143 lines of output of the original for the same inputs (`reference_test.rs`): identical except where Go tells a nil slice from an empty one, see below. |
| `pkg/sfu/rtpextension/playoutdelay/playoutdelay.go` | `sfu/rtpextension/playoutdelay/playoutdelay.rs` | 1 / 1 | |
| `pkg/sfu/rtpextension/abscapturetime/abscapturetime.go` | `sfu/rtpextension/abscapturetime/abscapturetime.rs` | none upstream | One test added. |
| `pkg/sfu/datachannel/bitrate.go` | `sfu/datachannel/bitrate.rs` | 6 / 6 | |
| `pkg/sfu/buffer/videolayerutils.go` | `sfu/buffer/videolayerutils.rs` | 7 / 7 | Qualities are taken and returned as the protocol's enum: a number that is no value of the enum comes back as the default where Go returns the number. Also compared with the original over 290 track infos in every rid space: identical. |
| `pkg/sfu/buffer/frameintegrity.go` | `sfu/buffer/frameintegrity.rs` | 3 / 3 | A frame entity is handed the packet history instead of pointing to it. Also compared with the original (100,000 queries): identical. |
| `pkg/sfu/buffer/datastats.go` | `sfu/buffer/datastats.rs` | 2 / 2 | |
| `pkg/sfu/buffer/rtcpreader.go` | `sfu/buffer/rtcpreader.rs` | none upstream | One test added. `Read` has no counterpart. `close` without a callback set does nothing (the original calls a nil function). |
| `pkg/sfu/buffer/streamstats.go` | `sfu/buffer/streamstats.rs` | none upstream | Written, not declared in the module yet: it needs `rtpstats`. |
| `pkg/sfu/testutils/data.go` | `sfu/testutils/data.rs` | none upstream | Test helpers. The two test codecs are functions. |
| `pkg/sfu/buffer/buffer_base.go` | `sfu/buffer/buffer_base.rs` | | Only the type `ExtPacket` so far, so that what takes packets from the buffer can be ported before it. |
| `pkg/rtc/types/protocol_version.go`, `trafficstats.go` | `rtc/types/` (same names) | none upstream | Five tests added. `livekit.StreamType` is written out (its proto file is not compiled here). |
| `pkg/rtc/transport/negotiationstate.go` | `rtc/transport/negotiationstate.rs` | none upstream | |
| `pkg/rtc/clientinfo.go`, `errors.go`, `user_packet_deduper.go`, `subscribepacer.go`, `utils.go`, `participant_data_blob.go` | `rtc/` (same names) | 14 / 14 | Twenty tests added, with values from the original. `Recover` is `recover` and `recover_async` around `catch_unwind`. `IsEOF` is a guess until the transport's write errors exist. |

The files of the row group from `rangemap.go` down were also run against the Go originals
with the same pseudo-random inputs (34,021 lines of output, identical).

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
- protocol `auth`: `GetCanPublishSource` compares the stored strings exactly while
  `GetCanPublishSources` upper-cases them; `checkOutputForCredentials` has no case for
  image outputs; a token without `iss` is verified with the secret of the key `""`;
  `TestVerifier`'s "incorrect key" case uses an expired token and cannot notice a missing
  signature check (a test was added).
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
