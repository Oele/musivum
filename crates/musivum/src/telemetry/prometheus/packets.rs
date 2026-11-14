// Ported from LiveKit, pkg/telemetry/prometheus/packets.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the functions that ported code calls are here,
// more are added as they are needed. The collectors are musivum's own (see the module's
// description) and are statics, so there is no `initPacketStats` to call first; the constant
// labels `node_id` and `node_type` are left to whoever serves the metrics.

use super::{Gauge, Histogram, MetricFamily};
use crate::go::atomic;

// read by the node's statistics upstream (`node.go`)
pub(super) static FORWARD_LATENCY: atomic::Uint32 = atomic::Uint32::new(0);

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

// The collectors of this file under the names upstream registers them with.
pub(super) fn collect(families: &mut Vec<MetricFamily>) {
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
}

pub fn record_forward_latency_sample(forward_latency: i64) {
    PROM_FORWARD_LATENCY_HIST.observe(forward_latency as f64);
}

pub fn record_forward_latency(p90: u32) {
    FORWARD_LATENCY.store(p90);
    PROM_FORWARD_LATENCY.set(p90 as f64);
}

// The original has tests of its own (`packets_test.go`) for functions that are not here yet.
// These are new. The statics are shared with every test of the crate that reports to them
// while this one runs, so it looks at what only it changes.
#[cfg(test)]
mod tests {
    use super::super::HistogramSnapshot;
    use super::*;

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
}
