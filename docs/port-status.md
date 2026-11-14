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
| mtu `helpers.go` | `mediatransportutil/helpers.rs` | 1 / 1 | Two tests added (the way back from NTP time, RTT from a receiver report). |
| protocol `utils/mono/mono.go` | `protocol/utils/mono.rs` | 0 / 2 | The clock itself is `go::time`; upstream's two tests are about Go's `time.Time` and have nothing to test here. |
| protocol `livekit/types.go` | `protocol/livekit/types.rs` | 0 / 4 | Only the identifier types; upstream's tests are of the YAML conversions, which are not ported. |
| protocol `utils/rtputil/rtpconverter.go` | `protocol/utils/rtputil.rs` | 1 / 1 | |
| protocol `utils/parallel.go` | `protocol/utils/parallel.rs` | 2 / 2 | Workers are scoped threads started per call (upstream: goroutines). One benchmark left out. |
| `pkg/sfu/utils/rangemap.go` | `sfu/utils/rangemap.rs` | 1 / 1 | |
| `pkg/sfu/utils/downtrackspreader.go` | `sfu/utils/downtrackspreader.rs` | 4 / 4 | The assertion on allocations per call in `TestBroadcastRTP` is left out (counting them needs `unsafe`). Parallel broadcast uses threads per call: fine at the default threshold, which keeps it serial. |
| `pkg/sfu/utils/debounce.go` | `sfu/utils/debounce.rs` | none upstream | One test added. |
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
- protocol `utils/rtputil`: the factor 3 in the reduction loop of `NewRTPConverter` does
  nothing (10^9 has no factor 3). protocol `utils/parallel.go`: a step of 0 never ends.
