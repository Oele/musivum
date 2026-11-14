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
| mtu `helpers.go` | `mediatransportutil/helpers.rs` | 1 / 1 | Two tests added (the way back from NTP time, RTT from a receiver report). |
| pion/rtp `packet.go`, `error.go` | `webrtc/rtp/packet.rs`, `error.rs` | 43 / 43 | Four tests added. Benchmarks left out. Run against pion with 432,008 generated inputs: every output identical, error texts and the cases where pion panics included. |
| pion/rtp `abssendtimeextension.go`, `audiolevelextension.go`, `transportccextension.go`, `playoutdelayextension.go`, `abscapturetimeextension.go` | `webrtc/rtp/` (same names) | 20 / 20 | Benchmarks left out. |
| pion/rtp `header_extension.go` | `webrtc/rtp/header_extension.rs` | 0 / 13 | Only the one function `packet.go` calls; LiveKit does not use the types of this file. |
| protocol `codecs/mime/mimetype.go` | `protocol/codecs/mime/mimetype.rs` | none upstream | Five tests added. The conversion to `roomobs` (observability) is left out. |
| protocol `codecs/codecs.go` | `protocol/codecs/codecs.rs` | none upstream | Three tests added. |
| protocol `utils/mono/mono.go` | `protocol/utils/mono.rs` | 0 / 2 | The clock itself is `go::time`; upstream's two tests are about Go's `time.Time` and have nothing to test here. |
| protocol `livekit/types.go` | `protocol/livekit/types.rs` | 0 / 4 | Only the identifier types; upstream's tests are of the YAML conversions, which are not ported. |
| protocol `utils/rtputil/rtpconverter.go` | `protocol/utils/rtputil.rs` | 1 / 1 | |
| protocol `utils/parallel.go` | `protocol/utils/parallel.rs` | 2 / 2 | Workers are scoped threads started per call (upstream: goroutines). One benchmark left out. |
| `pkg/sfu/utils/rangemap.go` | `sfu/utils/rangemap.rs` | 1 / 1 | |
| `pkg/sfu/utils/downtrackspreader.go` | `sfu/utils/downtrackspreader.rs` | 4 / 4 | The assertion on allocations per call in `TestBroadcastRTP` is left out (counting them needs `unsafe`). Parallel broadcast uses threads per call: fine at the default threshold, which keeps it serial. |
| `pkg/sfu/utils/debounce.go` | `sfu/utils/debounce.rs` | none upstream | One test added. |
| `pkg/sfu/ccutils/trenddetector.go`, `prober.go`, `probe_regulator.go`, `probe_signal.go` | `sfu/ccutils/` (same names) | none upstream | Fourteen tests added, their expected values taken from the Go originals. Re-read line by line against the originals. |
| `pkg/sfu/bwe/bwe.go`, `null_bwe.go` | `sfu/bwe/bwe.rs`, `null_bwe.rs` | none upstream | Two tests added. `Bwe` and `BweListener` are traits; `NullBwe` does not implement `Bwe` (upstream's has no `Type()` either) and is held by the estimators for the calls they do not define. |
| `pkg/sfu/audio/audiolevel.go` | `sfu/audio/audiolevel.rs` | 1 / 1 | |
| `pkg/sfu/packettrailer/packet_trailer.go` | `sfu/packettrailer/packet_trailer.rs` | 1 / 1 | |
| `pkg/sfu/rtpextension/playoutdelay/playoutdelay.go` | `sfu/rtpextension/playoutdelay/playoutdelay.rs` | 1 / 1 | |
| `pkg/sfu/rtpextension/abscapturetime/abscapturetime.go` | `sfu/rtpextension/abscapturetime/abscapturetime.rs` | none upstream | One test added. |
| `pkg/sfu/datachannel/bitrate.go` | `sfu/datachannel/bitrate.rs` | 6 / 6 | |

The files of the row group from `rangemap.go` down were also run against the Go originals
with the same pseudo-random inputs (34,021 lines of output, identical).

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
- protocol `utils/rtputil`: the factor 3 in the reduction loop of `NewRTPConverter` does
  nothing (10^9 has no factor 3). protocol `utils/parallel.go`: a step of 0 never ends.
