// Ported from LiveKit, pkg/telemetry/prometheus/packets.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the functions that ported code calls are here,
// more are added as they are needed. The collectors are musivum's own (see the module's
// description) and are statics, so there is no `initPacketStats` to call first; the constant
// labels `node_id` and `node_type` are left to whoever serves the metrics.

use super::{Counter, Gauge, Histogram, MetricFamily, MetricVec};
use crate::go::atomic;

/// `type Direction string` of the original, with its two values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Direction {
    Incoming,
    Outgoing,
}

impl Direction {
    /// `string(direction)`
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Incoming => "incoming",
            Direction::Outgoing => "outgoing",
        }
    }
}

// read by the node's statistics upstream (`node.go`)
pub(super) static FORWARD_LATENCY: atomic::Uint32 = atomic::Uint32::new(0);

pub static PROM_KEY_FRAME_TOTAL: MetricVec<Counter, 1> =
    MetricVec::new(["direction"], Counter::new);
pub static PROM_FORWARD_LATENCY: Gauge = Gauge::new();
pub static PROM_FORWARD_LATENCY_HIST: Histogram<10> = Histogram::new(
    // 50us, 100us, 250us, 500us, 1ms, 2ms, 3ms, 5ms, 10ms, 20ms
    [
        50.0 * 1000.0,
        100.0 * 1000.0,
        250.0 * 1000.0,
        500.0 * 1000.0,
        1.0 * 1000.0 * 1000.0,
        2.0 * 1000.0 * 1000.0,
        3.0 * 1000.0 * 1000.0,
        5.0 * 1000.0 * 1000.0,
        10.0 * 1000.0 * 1000.0,
        20.0 * 1000.0 * 1000.0,
    ],
);
pub static PROM_RTX_CAPPED_TOTAL: MetricVec<Counter, 1> = MetricVec::new(["kind"], Counter::new);
// promFECLabels
pub static PROM_FEC_PACKET_TOTAL: MetricVec<Counter, 2> =
    MetricVec::new(["direction", "type"], Counter::new);
pub static PROM_FEC_PACKET_BYTES: MetricVec<Counter, 2> =
    MetricVec::new(["direction", "type"], Counter::new);

// The collectors of this file under the names upstream registers them with.
pub(super) fn collect(families: &mut Vec<MetricFamily>) {
    families.push(MetricFamily::of_vec(
        "livekit_keyframe_total",
        "Key frames received from publishers (incoming) and forwarded to subscribers (outgoing, once per subscriber).",
        &PROM_KEY_FRAME_TOTAL,
    ));
    families.push(MetricFamily::of(
        "livekit_forward_latency",
        "",
        &PROM_FORWARD_LATENCY,
    ));
    families.push(MetricFamily::of(
        "livekit_forward_latency_ns",
        "",
        &PROM_FORWARD_LATENCY_HIST,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_rtx_capped_total",
        "NACKed packets not retransmitted because the down track was over its retransmission budget.",
        &PROM_RTX_CAPPED_TOTAL,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_fec_total",
        "",
        &PROM_FEC_PACKET_TOTAL,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_fec_bytes",
        "",
        &PROM_FEC_PACKET_BYTES,
    ));
}

pub fn record_fec_upstream(
    received: usize,
    recovered: usize,
    discarded: usize,
    bytes_received: u64,
) {
    if received > 0 {
        PROM_FEC_PACKET_TOTAL
            .with_label_values([Direction::Incoming.as_str(), "received"])
            .add(received as u64);
    }
    if recovered > 0 {
        PROM_FEC_PACKET_TOTAL
            .with_label_values([Direction::Incoming.as_str(), "recovered"])
            .add(recovered as u64);
    }
    if discarded > 0 {
        PROM_FEC_PACKET_TOTAL
            .with_label_values([Direction::Incoming.as_str(), "discarded"])
            .add(discarded as u64);
    }
    if bytes_received > 0 {
        PROM_FEC_PACKET_BYTES
            .with_label_values([Direction::Incoming.as_str(), "received"])
            .add(bytes_received);
    }
}

pub fn record_rtx_capped(kind: &str, packets: u32) {
    PROM_RTX_CAPPED_TOTAL
        .with_label_values([kind])
        .add(packets as u64);
}

pub fn record_forward_latency_sample(forward_latency: i64) {
    PROM_FORWARD_LATENCY_HIST.observe(forward_latency as f64);
}

pub fn record_forward_latency(p90: u32) {
    FORWARD_LATENCY.store(p90);
    PROM_FORWARD_LATENCY.set(p90 as f64);
}

pub fn increment_key_frames(direction: Direction) {
    PROM_KEY_FRAME_TOTAL
        .with_label_values([direction.as_str()])
        .inc();
}

// The original has tests of its own (`packets_test.go`) for functions that are not here yet.
// These are new. The statics are shared with every test of the crate that reports to them
// while this one runs, so it looks at what only it changes.
#[cfg(test)]
mod tests {
    use super::super::HistogramSnapshot;
    use super::*;

    #[test]
    fn fec_from_publishers_counts_by_type_and_skips_zeros() {
        let count = |t: &str| {
            PROM_FEC_PACKET_TOTAL
                .with_label_values([Direction::Incoming.as_str(), t])
                .get()
        };
        let bytes = || {
            PROM_FEC_PACKET_BYTES
                .with_label_values([Direction::Incoming.as_str(), "received"])
                .get()
        };
        let before = (
            count("received"),
            count("recovered"),
            count("discarded"),
            bytes(),
        );
        record_fec_upstream(5, 2, 1, 1200);
        record_fec_upstream(3, 0, 0, 0);
        let after = (
            count("received"),
            count("recovered"),
            count("discarded"),
            bytes(),
        );
        assert!(after.0 >= before.0 + 8);
        assert!(after.1 >= before.1 + 2);
        assert!(after.2 > before.2);
        assert!(after.3 >= before.3 + 1200);
    }

    #[test]
    fn capped_retransmissions_count_under_their_kind() {
        let count = || {
            PROM_RTX_CAPPED_TOTAL
                .with_label_values(["a kind of this test"])
                .get()
        };
        let before = count();
        record_rtx_capped("a kind of this test", 3);
        record_rtx_capped("a kind of this test", 4);
        assert_eq!(before + 7, count());
    }

    #[test]
    fn a_forward_latency_sample_counts_in_its_bucket() {
        let before = PROM_FORWARD_LATENCY_HIST.snapshot();
        // 15 ms: above 10 ms, not above 20 ms
        record_forward_latency_sample(15_000_000);
        // 1 s: above the last bound
        record_forward_latency_sample(1_000_000_000);
        let after = PROM_FORWARD_LATENCY_HIST.snapshot();

        let in_bucket =
            |snapshot: &HistogramSnapshot| snapshot.le(20_000_000.0) - snapshot.le(10_000_000.0);
        let above = |snapshot: &HistogramSnapshot| snapshot.count - snapshot.le(20_000_000.0);
        assert!(in_bucket(&after) > in_bucket(&before));
        assert!(above(&after) > above(&before));
        assert!(after.sum >= before.sum + 1_015_000_000.0);
    }

    #[test]
    fn the_forward_latency_is_what_was_recorded_last() {
        // Other tests record their own (a few milliseconds at most), so try until this one
        // was the last to.
        let p90 = u32::MAX - 7;
        for _ in 0..1000 {
            record_forward_latency(p90);
            if FORWARD_LATENCY.load() == p90 && PROM_FORWARD_LATENCY.get() == p90 as f64 {
                return;
            }
            std::thread::yield_now();
        }
        panic!("the recorded forward latency was never read back");
    }

    #[test]
    fn a_key_frame_counts_under_its_direction() {
        let count = |direction: Direction| {
            PROM_KEY_FRAME_TOTAL
                .with_label_values([direction.as_str()])
                .get()
        };
        let (incoming, outgoing) = (count(Direction::Incoming), count(Direction::Outgoing));
        increment_key_frames(Direction::Outgoing);
        increment_key_frames(Direction::Outgoing);
        increment_key_frames(Direction::Incoming);
        assert!(count(Direction::Outgoing) >= outgoing + 2);
        assert!(count(Direction::Incoming) > incoming);
    }
}
