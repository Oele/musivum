// Ported from LiveKit's protocol, utils/rtpstats_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The original has one test; the others here are new and
// say so.

use super::*;
use crate::go::time::{MILLISECOND, SECOND, Time};

// timestamppb.New of a time of the port.
fn timestamp(t: Time) -> Option<Timestamp> {
    timestamp_new(go_time(t))
}

fn go_time(t: Time) -> GoTime {
    GoTime::unix(0, t.unix_nano())
}

// timestamppb.New(time.Time{}) and timestamppb.New(time.Unix(0, 0)): the two ways upstream
// writes a time that is not set.
const YEAR_1: Option<Timestamp> = Some(Timestamp {
    seconds: GO_ZERO_TIME_SECONDS,
    nanos: 0,
});
const YEAR_1970: Option<Timestamp> = Some(Timestamp {
    seconds: 0,
    nanos: 0,
});

#[test]
fn test_aggregate_rtp_stats_fec() {
    let start = time::now();
    let end = start.add(SECOND);
    let stats = aggregate_rtp_stats(
        &[
            livekit::RtpStats {
                start_time: timestamp(start),
                end_time: timestamp(end),
                fec_packets: 3,
                fec_bytes: 900,
                fec_packets_discarded: 1,
                fec_packets_recovered: 2,
                ..Default::default()
            },
            livekit::RtpStats {
                start_time: timestamp(start),
                end_time: timestamp(end),
                fec_packets: 4,
                fec_bytes: 1200,
                fec_packets_discarded: 2,
                fec_packets_recovered: 3,
                ..Default::default()
            },
        ],
        8,
    )
    .unwrap();

    assert_eq!(7, stats.fec_packets);
    assert_eq!(2100, stats.fec_bytes);
    assert_eq!(3, stats.fec_packets_discarded);
    assert_eq!(5, stats.fec_packets_recovered);
}

// New for musivum: the sums, the rates over the whole span and the latest of the times. The
// values are the ones the original gives for these stats (run at the pin).
#[test]
fn stats_are_added_up_over_the_whole_span() {
    assert_eq!(None, aggregate_rtp_stats(&[], 8));

    let start = Time::unix(1_700_000_000, 250_000_000);
    let stats = aggregate_rtp_stats(
        &[
            livekit::RtpStats {
                start_time: timestamp(start.add(SECOND)),
                end_time: timestamp(start.add(3 * SECOND)),
                packets: 100,
                bytes: 10_000,
                packets_lost: 10,
                frames: 30,
                jitter_current: 4.0,
                jitter_max: 9.0,
                gap_histogram: HashMap::from([(1, 2), (3, 1)]),
                plis: 1,
                last_pli: timestamp(start.add(2 * SECOND)),
                firs: 1,
                last_fir: timestamp(start.add(SECOND)),
                rtt_current: 30,
                rtt_max: 80,
                ..Default::default()
            },
            livekit::RtpStats {
                start_time: timestamp(start),
                end_time: timestamp(start.add(4 * SECOND)),
                packets: 300,
                bytes: 30_000,
                packets_lost: 30,
                frames: 90,
                jitter_current: 2.0,
                jitter_max: 5.0,
                gap_histogram: HashMap::from([(1, 5)]),
                plis: 2,
                last_pli: timestamp(start.add(SECOND)),
                last_key_frame: timestamp(start.add(500 * MILLISECOND)),
                rtt_current: 51,
                rtt_max: 60,
                ..Default::default()
            },
        ],
        8,
    )
    .unwrap();

    assert_eq!(timestamp(start), stats.start_time);
    assert_eq!(timestamp(start.add(4 * SECOND)), stats.end_time);
    assert_eq!(4.0, stats.duration);
    assert_eq!(400, stats.packets);
    assert_eq!(100.0, stats.packet_rate);
    assert_eq!(40_000, stats.bytes);
    assert_eq!(80_000.0, stats.bitrate);
    assert_eq!(40, stats.packets_lost);
    assert_eq!(10.0, stats.packet_loss_rate);
    assert_eq!(40.0 / 440.0 * 100.0, stats.packet_loss_percentage);
    assert_eq!(120, stats.frames);
    assert_eq!(30.0, stats.frame_rate);
    assert_eq!(3.0, stats.jitter_current);
    assert_eq!(9.0, stats.jitter_max);
    assert_eq!(HashMap::from([(1, 7), (3, 1)]), stats.gap_histogram);
    assert_eq!(3, stats.plis);
    assert_eq!(timestamp(start.add(2 * SECOND)), stats.last_pli);
    assert_eq!(
        timestamp(start.add(500 * MILLISECOND)),
        stats.last_key_frame
    );
    assert_eq!(timestamp(start.add(SECOND)), stats.last_fir);
    assert_eq!(40, stats.rtt_current);
    assert_eq!(80, stats.rtt_max);
    // a time that no stats had a timestamp for is 1970-01-01, which a missing timestamp reads
    // as
    assert_eq!(YEAR_1970, stats.last_layer_lock_pli);
    assert_eq!(None, stats.packet_drift);
}

// New for musivum: a time that is not set comes in three forms, a timestamp that is missing,
// 1970-01-01 (upstream writes `time.Unix(0, 0)` for the last PLI) and the year 1 (a
// `time.Time{}`, for the last key frame and the last FIR). The values are the ones the
// original gives for these stats (run at the pin).
#[test]
fn times_that_are_not_set_come_out_as_the_original_gives_them() {
    let start = Time::unix(1_700_000_000, 250_000_000);
    let one = livekit::RtpStats {
        start_time: timestamp(start),
        end_time: timestamp(start.add(2 * SECOND)),
        packets: 10,
        last_key_frame: YEAR_1,
        last_pli: YEAR_1970,
        last_fir: YEAR_1,
        ..Default::default()
    };
    let stats = aggregate_rtp_stats(&[one.clone(), one], 8).unwrap();
    assert_eq!(2.0, stats.duration);
    assert_eq!(10.0, stats.packet_rate);
    assert_eq!(YEAR_1, stats.last_key_frame);
    assert_eq!(YEAR_1970, stats.last_pli);
    assert_eq!(YEAR_1970, stats.last_layer_lock_pli);
    assert_eq!(YEAR_1, stats.last_fir);

    // no end time at all: 1970 is taken for it, and the time that passed is negative
    let stats = aggregate_rtp_stats(
        &[livekit::RtpStats {
            start_time: timestamp(start),
            packets: 10,
            bytes: 1000,
            packets_lost: 10,
            ..Default::default()
        }],
        8,
    )
    .unwrap();
    assert_eq!(timestamp(start), stats.start_time);
    assert_eq!(YEAR_1970, stats.end_time);
    assert_eq!(-1.70000000025e+09, stats.duration);
    assert_eq!(-5.882352940311419e-09, stats.packet_rate);
    assert_eq!(-4.705882352249135e-06, stats.bitrate);
    assert_eq!(-5.882352940311419e-09, stats.packet_loss_rate);
    assert_eq!(50.0, stats.packet_loss_percentage);

    // an end time of the year 1: the time now is taken for it
    let stats = aggregate_rtp_stats(
        &[livekit::RtpStats {
            start_time: timestamp(time::now().add(-10 * SECOND)),
            end_time: YEAR_1,
            packets: 100,
            ..Default::default()
        }],
        8,
    )
    .unwrap();
    assert!((10.0..11.0).contains(&stats.duration), "{}", stats.duration);
    assert_eq!(100.0 / stats.duration, stats.packet_rate);

    // neither a start nor an end: no time passed
    let stats = aggregate_rtp_stats(
        &[livekit::RtpStats {
            packets: 10,
            bytes: 1000,
            ..Default::default()
        }],
        8,
    )
    .unwrap();
    assert_eq!(YEAR_1970, stats.start_time);
    assert_eq!(YEAR_1970, stats.end_time);
    assert_eq!(0.0, stats.duration);
    assert_eq!(f64::INFINITY, stats.packet_rate);
    assert_eq!(f64::INFINITY, stats.bitrate);
    assert!(stats.packet_loss_rate.is_nan());
    assert_eq!(0.0, stats.packet_loss_percentage);

    // a start that is missing is 1970 and so the earliest, whichever stats have it
    let with_start = livekit::RtpStats {
        start_time: timestamp(start),
        end_time: timestamp(start.add(SECOND)),
        packets: 10,
        ..Default::default()
    };
    let without_start = livekit::RtpStats {
        start_time: None,
        ..with_start.clone()
    };
    for stats_list in [
        [without_start.clone(), with_start.clone()],
        [with_start.clone(), without_start],
    ] {
        let stats = aggregate_rtp_stats(&stats_list, 8).unwrap();
        assert_eq!(YEAR_1970, stats.start_time);
        assert_eq!(1.70000000125e+09, stats.duration);
        assert_eq!(1.1764705873702422e-08, stats.packet_rate);
    }

    // a start of the year 1 gives way to a later one when it comes first, and is taken as
    // the earliest when it comes second; from the year 1 it is the longest time there is
    let year_1_start = livekit::RtpStats {
        start_time: YEAR_1,
        ..with_start.clone()
    };
    let stats = aggregate_rtp_stats(&[year_1_start.clone(), with_start.clone()], 8).unwrap();
    assert_eq!(timestamp(start), stats.start_time);
    assert_eq!(1.0, stats.duration);
    assert_eq!(20.0, stats.packet_rate);
    let stats = aggregate_rtp_stats(&[with_start, year_1_start], 8).unwrap();
    assert_eq!(YEAR_1, stats.start_time);
    assert_eq!(9.223372036854776e+09, stats.duration);
    assert_eq!(2.168404344971009e-09, stats.packet_rate);

    // nanoseconds that are out of range in a timestamp
    let stats = aggregate_rtp_stats(
        &[livekit::RtpStats {
            start_time: Some(Timestamp {
                seconds: 100,
                nanos: -1,
            }),
            end_time: Some(Timestamp {
                seconds: 100,
                nanos: 1_500_000_000,
            }),
            packets: 3,
            ..Default::default()
        }],
        8,
    )
    .unwrap();
    assert_eq!(
        Some(Timestamp {
            seconds: 99,
            nanos: 999_999_999
        }),
        stats.start_time
    );
    assert_eq!(
        Some(Timestamp {
            seconds: 101,
            nanos: 500_000_000
        }),
        stats.end_time
    );
    assert_eq!(1.500000001, stats.duration);
    assert_eq!(1.9999999986666666, stats.packet_rate);
}

// New for musivum: the sums wrap as the original's do (values run at the pin).
#[test]
fn the_sums_wrap() {
    let start = Time::unix(1_700_000_000, 250_000_000);
    let stats = aggregate_rtp_stats(
        &[
            livekit::RtpStats {
                start_time: timestamp(start),
                end_time: timestamp(start.add(SECOND)),
                packets: 0xffff_ffff,
                rtt_current: 0xffff_ffff,
                ..Default::default()
            },
            livekit::RtpStats {
                start_time: timestamp(start),
                end_time: timestamp(start.add(SECOND)),
                packets: 3,
                rtt_current: 5,
                ..Default::default()
            },
        ],
        8,
    )
    .unwrap();
    assert_eq!(2, stats.packets);
    assert_eq!(2.0, stats.packet_rate);
    assert_eq!(2, stats.rtt_current);
}

// New for musivum: holds what upstream does with the time of the last FIR, which it compares
// with the time of the last PLI instead of with itself (see the loop in `aggregate_rtp_stats`).
#[test]
fn the_last_fir_is_compared_with_the_last_pli_as_upstream() {
    let start = Time::unix(1_700_000_000, 0);
    let stats = aggregate_rtp_stats(
        &[
            livekit::RtpStats {
                start_time: timestamp(start),
                end_time: timestamp(start.add(20 * SECOND)),
                last_pli: timestamp(start.add(10 * SECOND)),
                last_fir: timestamp(start.add(SECOND)),
                ..Default::default()
            },
            livekit::RtpStats {
                start_time: timestamp(start),
                end_time: timestamp(start.add(20 * SECOND)),
                // later than the FIR above, and still not taken: it is before the last PLI
                last_fir: timestamp(start.add(5 * SECOND)),
                ..Default::default()
            },
        ],
        8,
    )
    .unwrap();
    assert_eq!(timestamp(start.add(SECOND)), stats.last_fir);
}
