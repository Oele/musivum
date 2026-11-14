// Ported from LiveKit, pkg/sfu/datachannel/bitrate_test.go at v1.13.11.
// Copyright LiveKit, Inc. (the original file has no notice of its own). Licensed under the
// Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are loops over the modes.

use super::*;
use crate::testutils::assert_in_delta;

const BUSY_MODES: [BitrateMode; 2] = [BitrateMode::BusyOnly, BitrateMode::ExcludeIdleDrain];

// 1 KB written every second, each fully drained before the next write: never
// backlogged, so there is no estimate.
#[test]
fn test_bitrate_calculator_sparse_fast_ack() {
    for mode in BUSY_MODES {
        // require.NotNil(t, c) of the original has nothing to check here.
        let c = BitrateCalculator::new(BITRATE_DURATION, BITRATE_WINDOW, mode);

        let t0 = time::now();
        for i in 0..5 {
            let ts = t0.add(i * time::SECOND);
            c.add_bytes(1000, 1000, ts);
            let (_, ok) = c.bitrate(ts);
            assert!(!ok, "{mode}");
        }
    }
}

// Standing backlog of 1000 bytes; every 100ms write 800 and 800 drain, sampled
// buffer 1000+800=1800. 800 B / 100ms = 64000 bps.
#[test]
fn test_bitrate_calculator_sustained_backlog() {
    for mode in BUSY_MODES {
        let c = BitrateCalculator::new(BITRATE_DURATION, BITRATE_WINDOW, mode);

        let t0 = time::now();
        c.add_bytes(800, 1800, t0);
        for i in 1..=30 {
            c.add_bytes(800, 1800, t0.add(i * 100 * time::MILLISECOND));
        }

        let (bitrate, ok) = c.bitrate(t0.add(3 * time::SECOND));
        assert!(ok, "{mode}");
        assert_in_delta(64000.0, bitrate as f64, 2000.0);
    }
}

// Buffer only climbs and nothing drains (stalled connection): the estimate
// converges to ~0.
#[test]
fn test_bitrate_calculator_stalled_connection() {
    for mode in BUSY_MODES {
        let c = BitrateCalculator::new(BITRATE_DURATION, BITRATE_WINDOW, mode);

        let t0 = time::now();
        let mut buffered = 0;
        for i in 0..20 {
            buffered += 1000; // wrote 1000, nothing drained
            c.add_bytes(1000, buffered, t0.add(i * 50 * time::MILLISECOND));
        }

        let (bitrate, ok) = c.bitrate(t0.add(time::SECOND));
        assert!(ok, "{mode}");
        assert!(bitrate < 1000, "{mode}: {bitrate}");
    }
}

// A single write that drains to empty is not backlogged at the next sample, so
// no estimate is produced.
#[test]
fn test_bitrate_calculator_flush_not_counted() {
    for mode in BUSY_MODES {
        let c = BitrateCalculator::new(BITRATE_DURATION, BITRATE_WINDOW, mode);

        let t0 = time::now();
        c.add_bytes(100, 100, t0);
        let ts = t0.add(100 * time::MILLISECOND);
        c.add_bytes(0, 0, ts);
        let (_, ok) = c.bitrate(ts);
        assert!(!ok, "{mode}");
    }
}

// An idle drain followed by a backlog: BusyOnly counts the idle-drained bytes
// over busy time only and reports a higher rate than ExcludeIdleDrain.
#[test]
fn test_bitrate_calculator_mixed_idle_then_backlog() {
    let off = BitrateCalculator::new(BITRATE_DURATION, BITRATE_WINDOW, BitrateMode::BusyOnly);
    let on = BitrateCalculator::new(
        BITRATE_DURATION,
        BITRATE_WINDOW,
        BitrateMode::ExcludeIdleDrain,
    );

    let t0 = time::now();
    let feed = |written: i64, buffered: i64, ts: Time| {
        off.add_bytes(written, buffered, ts);
        on.add_bytes(written, buffered, ts);
    };

    // 5000 drains during an idle gap (not backlogged), then a sustained backlog
    // of 800 every 100ms over a standing 5000.
    feed(5000, 5000, t0);
    feed(5000, 5000, t0.add(100 * time::MILLISECOND));
    for i in 1..=5 {
        feed(800, 5800, t0.add((100 + i * 100) * time::MILLISECOND));
    }
    let query_ts = t0.add(700 * time::MILLISECOND);

    let (r_off, ok_off) = off.bitrate(query_ts);
    let (r_on, ok_on) = on.bitrate(query_ts);
    assert!(ok_off);
    assert!(ok_on);
    assert!(r_off > r_on, "{r_off} > {r_on}");
}

// No observable buffer: throughput over elapsed time. 1000 B every 100ms =
// 80000 bps.
#[test]
fn test_bitrate_calculator_wall_clock() {
    let c = BitrateCalculator::new(BITRATE_DURATION, BITRATE_WINDOW, BitrateMode::WallClock);

    let t0 = time::now();
    for i in 0..20 {
        c.add_bytes(1000, 0, t0.add(i * 100 * time::MILLISECOND));
    }

    let (bitrate, ok) = c.bitrate(t0.add(2 * time::SECOND));
    assert!(ok);
    assert_in_delta(80000.0, bitrate as f64, 8000.0);
}
