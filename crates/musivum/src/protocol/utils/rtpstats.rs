// Ported from LiveKit's protocol, utils/rtpstats.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the sums wrap as Go's do. A time is kept here as what
// a timestamp of the protocol holds, seconds and nanoseconds since 1970 (`GoTime`, at the end
// of this file with `timestamppb.New` and `AsTime`), and not as a `crate::go::time::Time`:
// what the original gives depends on the difference between Go's zero time (the year 1, which
// it writes for a time that is not set) and 1970-01-01 (which it reads from a timestamp that
// is missing), and a `Time` has one zero for both.

use std::collections::HashMap;

use pbjson_types::Timestamp;

use crate::go::time::{self, Duration};
use crate::protocol::livekit;

pub fn aggregate_rtp_stats(
    stats_list: &[livekit::RtpStats],
    gap_histogram_size: usize,
) -> Option<livekit::RtpStats> {
    if stats_list.is_empty() {
        return None;
    }

    let mut start_time = GoTime::ZERO;
    let mut end_time = GoTime::ZERO;

    let mut packets = 0u32;
    let mut bytes = 0u64;
    let mut header_bytes = 0u64;
    let mut packets_lost = 0u32;
    let mut packets_duplicate = 0u32;
    let mut bytes_duplicate = 0u64;
    let mut header_bytes_duplicate = 0u64;
    let mut packets_padding = 0u32;
    let mut bytes_padding = 0u64;
    let mut header_bytes_padding = 0u64;
    let mut fec_packets = 0u32;
    let mut fec_bytes = 0u64;
    let mut fec_packets_discarded = 0u32;
    let mut fec_packets_recovered = 0u32;
    let mut packets_out_of_order = 0u32;
    let mut frames = 0u32;
    let mut key_frames = 0u32;
    let mut last_key_frame = GoTime::ZERO;
    let mut jitter = 0.0;
    let mut max_jitter = 0f64;
    let mut gap_histogram: HashMap<i32, u32> = HashMap::with_capacity(gap_histogram_size);
    let mut nacks = 0u32;
    let mut nack_acks = 0u32;
    let mut nack_misses = 0u32;
    let mut nack_repeated = 0u32;
    let mut plis = 0u32;
    let mut last_pli = GoTime::ZERO;
    let mut layer_lock_plis = 0u32;
    let mut last_layer_lock_pli = GoTime::ZERO;
    let mut firs = 0u32;
    let mut last_fir = GoTime::ZERO;
    let mut rtt = 0u32;
    let mut max_rtt = 0u32;

    for stats in stats_list {
        if start_time.is_zero() || start_time.after(as_time(&stats.start_time)) {
            start_time = as_time(&stats.start_time);
        }

        if end_time.is_zero() || end_time.before(as_time(&stats.end_time)) {
            end_time = as_time(&stats.end_time);
        }

        packets = packets.wrapping_add(stats.packets);
        bytes = bytes.wrapping_add(stats.bytes);
        header_bytes = header_bytes.wrapping_add(stats.header_bytes);

        packets_lost = packets_lost.wrapping_add(stats.packets_lost);

        packets_duplicate = packets_duplicate.wrapping_add(stats.packets_duplicate);
        bytes_duplicate = bytes_duplicate.wrapping_add(stats.bytes_duplicate);
        header_bytes_duplicate = header_bytes_duplicate.wrapping_add(stats.header_bytes_duplicate);

        packets_padding = packets_padding.wrapping_add(stats.packets_padding);
        bytes_padding = bytes_padding.wrapping_add(stats.bytes_padding);
        header_bytes_padding = header_bytes_padding.wrapping_add(stats.header_bytes_padding);

        fec_packets = fec_packets.wrapping_add(stats.fec_packets);
        fec_bytes = fec_bytes.wrapping_add(stats.fec_bytes);
        fec_packets_discarded = fec_packets_discarded.wrapping_add(stats.fec_packets_discarded);
        fec_packets_recovered = fec_packets_recovered.wrapping_add(stats.fec_packets_recovered);

        packets_out_of_order = packets_out_of_order.wrapping_add(stats.packets_out_of_order);

        frames = frames.wrapping_add(stats.frames);

        key_frames = key_frames.wrapping_add(stats.key_frames);
        if last_key_frame.is_zero() || last_key_frame.before(as_time(&stats.last_key_frame)) {
            last_key_frame = as_time(&stats.last_key_frame);
        }

        jitter += stats.jitter_current;
        if stats.jitter_max > max_jitter {
            max_jitter = stats.jitter_max;
        }

        for (burst, count) in &stats.gap_histogram {
            let total = gap_histogram.entry(*burst).or_default();
            *total = total.wrapping_add(*count);
        }

        nacks = nacks.wrapping_add(stats.nacks);
        nack_acks = nack_acks.wrapping_add(stats.nack_acks);
        nack_misses = nack_misses.wrapping_add(stats.nack_misses);
        nack_repeated = nack_repeated.wrapping_add(stats.nack_repeated);

        plis = plis.wrapping_add(stats.plis);
        if last_pli.is_zero() || last_pli.before(as_time(&stats.last_pli)) {
            last_pli = as_time(&stats.last_pli);
        }

        layer_lock_plis = layer_lock_plis.wrapping_add(stats.layer_lock_plis);
        if last_layer_lock_pli.is_zero()
            || last_layer_lock_pli.before(as_time(&stats.last_layer_lock_pli))
        {
            last_layer_lock_pli = as_time(&stats.last_layer_lock_pli);
        }

        firs = firs.wrapping_add(stats.firs);
        // As upstream: the time of the last FIR is compared with that of the last PLI.
        if last_fir.is_zero() || last_pli.before(as_time(&stats.last_fir)) {
            last_fir = as_time(&stats.last_fir);
        }

        rtt = rtt.wrapping_add(stats.rtt_current);
        if stats.rtt_max > max_rtt {
            max_rtt = stats.rtt_max;
        }
    }

    if end_time.is_zero() {
        end_time = GoTime::now();
    }
    let elapsed = end_time.sub(start_time).seconds();

    let packet_lost_rate = packets_lost as f64 / elapsed;
    let packet_lost_percentage =
        packets_lost as f32 / (packets as f32 + packets_lost as f32) * 100.0;

    let packet_rate = packets as f64 / elapsed;
    let packet_duplicate_rate = packets_duplicate as f64 / elapsed;
    let packet_padding_rate = packets_padding as f64 / elapsed;

    let bitrate = bytes as f64 * 8.0 / elapsed;
    let bitrate_duplicate = bytes_duplicate as f64 * 8.0 / elapsed;
    let bitrate_padding = bytes_padding as f64 * 8.0 / elapsed;

    let frame_rate = frames as f64 / elapsed;

    Some(livekit::RtpStats {
        start_time: timestamp_new(start_time),
        end_time: timestamp_new(end_time),
        duration: elapsed,
        packets,
        packet_rate,
        bytes,
        header_bytes,
        bitrate,
        packets_lost,
        packet_loss_rate: packet_lost_rate,
        packet_loss_percentage: packet_lost_percentage,
        packets_duplicate,
        packet_duplicate_rate,
        bytes_duplicate,
        header_bytes_duplicate,
        bitrate_duplicate,
        packets_padding,
        packet_padding_rate,
        bytes_padding,
        header_bytes_padding,
        bitrate_padding,
        fec_packets,
        fec_bytes,
        fec_packets_discarded,
        fec_packets_recovered,
        packets_out_of_order,
        frames,
        frame_rate,
        key_frames,
        last_key_frame: timestamp_new(last_key_frame),
        jitter_current: jitter / stats_list.len() as f64,
        jitter_max: max_jitter,
        gap_histogram,
        nacks,
        nack_acks,
        nack_misses,
        nack_repeated,
        plis,
        last_pli: timestamp_new(last_pli),
        layer_lock_plis,
        last_layer_lock_pli: timestamp_new(last_layer_lock_pli),
        firs,
        last_fir: timestamp_new(last_fir),
        rtt_current: rtt / stats_list.len() as u32,
        rtt_max: max_rtt,
        // no aggregation for drift calculations
        ..Default::default()
    })
}

// The seconds of Go's zero time, the first of January of the year 1.
const GO_ZERO_TIME_SECONDS: i64 = -62_135_596_800;

/// A `time.Time` of the original, as far as this file needs one: the seconds since 1970 and
/// the nanoseconds within the second. Written new, see the header.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct GoTime {
    seconds: i64,
    nanos: i32,
}

impl GoTime {
    // time.Time{}
    const ZERO: GoTime = GoTime {
        seconds: GO_ZERO_TIME_SECONDS,
        nanos: 0,
    };

    // time.Unix
    fn unix(mut sec: i64, mut nsec: i64) -> GoTime {
        if !(0..1_000_000_000).contains(&nsec) {
            let n = nsec / 1_000_000_000;
            sec = sec.wrapping_add(n);
            nsec -= n * 1_000_000_000;
            if nsec < 0 {
                nsec += 1_000_000_000;
                sec = sec.wrapping_sub(1);
            }
        }
        GoTime {
            seconds: sec,
            nanos: nsec as i32,
        }
    }

    // time.Now
    fn now() -> GoTime {
        GoTime::unix(0, time::now().unix_nano())
    }

    fn is_zero(self) -> bool {
        self == GoTime::ZERO
    }

    fn before(self, u: GoTime) -> bool {
        self < u
    }

    fn after(self, u: GoTime) -> bool {
        self > u
    }

    // t.Sub(u), which gives the longest or the shortest duration when the difference is more
    // than a duration holds.
    fn sub(self, u: GoTime) -> Duration {
        let d = (self.seconds as i128 - u.seconds as i128) * 1_000_000_000
            + (self.nanos as i128 - u.nanos as i128);
        Duration(d.clamp(i64::MIN as i128, i64::MAX as i128) as i64)
    }
}

// timestamppb.New
fn timestamp_new(t: GoTime) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: t.seconds,
        nanos: t.nanos,
    })
}

// (*timestamppb.Timestamp).AsTime: a timestamp that is missing reads as 1970-01-01
fn as_time(ts: &Option<Timestamp>) -> GoTime {
    let Some(ts) = ts else {
        return GoTime::unix(0, 0);
    };
    GoTime::unix(ts.seconds, ts.nanos as i64)
}

#[cfg(test)]
#[path = "rtpstats_test.rs"]
mod rtpstats_test;
