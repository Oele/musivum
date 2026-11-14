// Ported from LiveKit, pkg/rtc/types/trafficstats.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the pointers that the original never checks for nil
// (the stats of a `TrafficTypeStats`, the entries of the lists) are values; the four named
// results of `TrafficLoadToTrafficRate` are a tuple in the same order; `Timestamp.AsTime()`
// of a timestamp that may be absent is the function `as_time`; `StreamType` is defined here,
// see there. The zero `Time` of `crate::go::time` is the Unix epoch, where Go's is the year 1:
// a time that is exactly the Unix epoch, which is what an absent timestamp of the protocol
// becomes, counts as "not set" in `aggregate_traffic_stats` here and as a time upstream.
// The tests at the end are new: the original has none.

use musivum_protocol::Timestamp;

use crate::go::time::{self, Time};
use crate::protocol::livekit;

/// `livekit.StreamType` upstream, an enumeration of `livekit_analytics.proto`. That file is
/// not among the definitions `musivum-protocol` compiles, so the type is written out here,
/// with the names and numbers the generated one would have.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i32)]
pub enum StreamType {
    #[default]
    Upstream = 0,
    Downstream = 1,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrafficStats {
    pub start_time: Time,
    pub end_time: Time,
    pub packets: u32,
    pub packets_lost: u32,
    pub packets_padding: u32,
    pub packets_out_of_order: u32,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrafficTypeStats {
    pub track_type: livekit::TrackType,
    pub stream_type: StreamType,
    pub traffic_stats: TrafficStats,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrafficLoad {
    pub traffic_type_stats: Vec<TrafficTypeStats>,
}

// `Timestamp.AsTime()` of an optional timestamp of the protocol: an absent one is the Unix
// epoch, as a nil one is upstream.
fn as_time(ts: &Option<Timestamp>) -> Time {
    match ts {
        Some(ts) => time::timestamppb_as_time(ts),
        None => Time::unix(0, 0),
    }
}

pub fn rtp_stats_diff_to_traffic_stats(
    before: Option<&livekit::RtpStats>,
    after: Option<&livekit::RtpStats>,
) -> Option<TrafficStats> {
    let after = after?;

    let mut start_time = &after.start_time;
    if let Some(before) = before {
        start_time = &before.end_time;
    }

    let get_after = || TrafficStats {
        start_time: as_time(start_time),
        end_time: as_time(&after.end_time),
        packets: after.packets,
        packets_lost: after.packets_lost,
        packets_padding: after.packets_padding,
        packets_out_of_order: after.packets_out_of_order,
        bytes: after
            .bytes
            .wrapping_add(after.bytes_duplicate)
            .wrapping_add(after.bytes_padding),
    };

    let Some(before) = before else {
        return Some(get_after());
    };

    if after.packets.wrapping_sub(before.packets) > (1 << 31) {
        // after packets < before packets, probably got reset, just return after
        return Some(get_after());
    }
    if (after
        .bytes
        .wrapping_add(after.bytes_duplicate)
        .wrapping_add(after.bytes_padding))
    .wrapping_sub(
        before
            .bytes
            .wrapping_add(before.bytes_duplicate)
            .wrapping_add(before.bytes_padding),
    ) > (1 << 63)
    {
        // after bytes < before bytes, probably got reset, just return after
        return Some(get_after());
    }

    let mut packets_lost = 0u32;
    if after.packets_lost >= before.packets_lost {
        packets_lost = after.packets_lost - before.packets_lost;
    }
    Some(TrafficStats {
        start_time: as_time(start_time),
        end_time: as_time(&after.end_time),
        packets: after.packets.wrapping_sub(before.packets),
        packets_lost,
        packets_padding: after.packets_padding.wrapping_sub(before.packets_padding),
        packets_out_of_order: after
            .packets_out_of_order
            .wrapping_sub(before.packets_out_of_order),
        bytes: (after
            .bytes
            .wrapping_add(after.bytes_duplicate)
            .wrapping_add(after.bytes_padding))
        .wrapping_sub(
            before
                .bytes
                .wrapping_add(before.bytes_duplicate)
                .wrapping_add(before.bytes_padding),
        ),
    })
}

pub fn aggregate_traffic_stats(stats_list: &[TrafficStats]) -> Option<TrafficStats> {
    if stats_list.is_empty() {
        return None;
    }

    let mut start_time = Time::default();
    let mut end_time = Time::default();

    let mut packets = 0u32;
    let mut packets_lost = 0u32;
    let mut packets_padding = 0u32;
    let mut packets_out_of_order = 0u32;
    let mut bytes = 0u64;

    for stats in stats_list {
        if start_time.is_zero() || start_time.after(stats.start_time) {
            start_time = stats.start_time;
        }

        if end_time.is_zero() || end_time.before(stats.end_time) {
            end_time = stats.end_time;
        }

        packets = packets.wrapping_add(stats.packets);
        packets_lost = packets_lost.wrapping_add(stats.packets_lost);
        packets_padding = packets_padding.wrapping_add(stats.packets_padding);
        packets_out_of_order = packets_out_of_order.wrapping_add(stats.packets_out_of_order);
        bytes = bytes.wrapping_add(stats.bytes);
    }

    if end_time.is_zero() {
        end_time = time::now();
    }
    Some(TrafficStats {
        start_time,
        end_time,
        packets,
        packets_lost,
        packets_padding,
        packets_out_of_order,
        bytes,
    })
}

/// Returns `(packet_rate_in, byte_rate_in, packet_rate_out, byte_rate_out)`.
pub fn traffic_load_to_traffic_rate(traffic_load: Option<&TrafficLoad>) -> (f64, f64, f64, f64) {
    let mut packet_rate_in = 0.0;
    let mut byte_rate_in = 0.0;
    let mut packet_rate_out = 0.0;
    let mut byte_rate_out = 0.0;
    let Some(traffic_load) = traffic_load else {
        return (packet_rate_in, byte_rate_in, packet_rate_out, byte_rate_out);
    };

    for traffic_type_stat in &traffic_load.traffic_type_stats {
        let elapsed = traffic_type_stat
            .traffic_stats
            .end_time
            .sub(traffic_type_stat.traffic_stats.start_time)
            .seconds();
        let packet_rate = traffic_type_stat.traffic_stats.packets as f64 / elapsed;
        let byte_rate = traffic_type_stat.traffic_stats.bytes as f64 / elapsed;
        match traffic_type_stat.stream_type {
            StreamType::Upstream => {
                packet_rate_in += packet_rate;
                byte_rate_in += byte_rate;
            }
            StreamType::Downstream => {
                packet_rate_out += packet_rate;
                byte_rate_out += byte_rate;
            }
        }
    }
    (packet_rate_in, byte_rate_in, packet_rate_out, byte_rate_out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no tests. The expected values of these tests are
    // what the original gives for the same inputs (printed by a Go test at the pin).

    fn rtp_stats(end_sec: i64, packets: u32, bytes: u64) -> livekit::RtpStats {
        let mut stats = livekit::RtpStats {
            packets,
            bytes,
            ..Default::default()
        };
        stats.end_time.get_or_insert_default().seconds = end_sec;
        stats
    }

    fn traffic_stats(
        start_time: Time,
        end_time: Time,
        counts: (u32, u32, u32, u32),
        bytes: u64,
    ) -> TrafficStats {
        TrafficStats {
            start_time,
            end_time,
            packets: counts.0,
            packets_lost: counts.1,
            packets_padding: counts.2,
            packets_out_of_order: counts.3,
            bytes,
        }
    }

    #[test]
    fn the_difference_of_two_rtp_stats() {
        let mut before = livekit::RtpStats {
            packets: 1000,
            packets_lost: 10,
            packets_padding: 20,
            packets_out_of_order: 3,
            bytes: 100000,
            bytes_duplicate: 500,
            bytes_padding: 700,
            ..Default::default()
        };
        let start = before.start_time.get_or_insert_default();
        (start.seconds, start.nanos) = (100, 5);
        let end = before.end_time.get_or_insert_default();
        (end.seconds, end.nanos) = (110, 7);
        let mut after = livekit::RtpStats {
            packets: 1800,
            packets_lost: 25,
            packets_padding: 50,
            packets_out_of_order: 7,
            bytes: 190000,
            bytes_duplicate: 900,
            bytes_padding: 1500,
            ..Default::default()
        };
        after.start_time = before.start_time;
        let end = after.end_time.get_or_insert_default();
        (end.seconds, end.nanos) = (120, 9);

        assert_eq!(None, rtp_stats_diff_to_traffic_stats(None, None));
        assert_eq!(None, rtp_stats_diff_to_traffic_stats(Some(&before), None));
        assert_eq!(
            Some(traffic_stats(
                Time::unix(100, 5),
                Time::unix(120, 9),
                (1800, 25, 50, 7),
                192400
            )),
            rtp_stats_diff_to_traffic_stats(None, Some(&after))
        );
        assert_eq!(
            Some(traffic_stats(
                Time::unix(110, 7),
                Time::unix(120, 9),
                (800, 15, 30, 4),
                91200
            )),
            rtp_stats_diff_to_traffic_stats(Some(&before), Some(&after))
        );
        // fewer packets after than before: a reset, the later one as it is, but starting at
        // the end of the earlier one
        assert_eq!(
            Some(traffic_stats(
                Time::unix(120, 9),
                Time::unix(110, 7),
                (1000, 10, 20, 3),
                101200
            )),
            rtp_stats_diff_to_traffic_stats(Some(&after), Some(&before))
        );

        // fewer bytes after than before: a reset too
        let b = livekit::RtpStats {
            bytes_duplicate: 1,
            bytes_padding: 2,
            ..rtp_stats(2, 10, 5000)
        };
        let a = livekit::RtpStats {
            bytes_duplicate: 1,
            bytes_padding: 2,
            ..rtp_stats(3, 20, 100)
        };
        assert_eq!(
            Some(traffic_stats(
                Time::unix(2, 0),
                Time::unix(3, 0),
                (20, 0, 0, 0),
                103
            )),
            rtp_stats_diff_to_traffic_stats(Some(&b), Some(&a))
        );

        // fewer lost after than before is none lost; the other counts wrap
        let b = livekit::RtpStats {
            packets_lost: 5,
            packets_padding: 4,
            packets_out_of_order: 3,
            ..rtp_stats(2, 10, 100)
        };
        let a = livekit::RtpStats {
            packets_lost: 2,
            packets_padding: 1,
            packets_out_of_order: 1,
            ..rtp_stats(3, 20, 200)
        };
        assert_eq!(
            Some(traffic_stats(
                Time::unix(2, 0),
                Time::unix(3, 0),
                (10, 0, 4294967293, 4294967294),
                100
            )),
            rtp_stats_diff_to_traffic_stats(Some(&b), Some(&a))
        );

        // fewer packets after than before, but more bytes: a reset, the later one as it is
        let b = livekit::RtpStats {
            packets_lost: 1,
            packets_padding: 2,
            packets_out_of_order: 3,
            ..rtp_stats(2, 10, 100)
        };
        let a = livekit::RtpStats {
            packets_lost: 4,
            packets_padding: 5,
            packets_out_of_order: 6,
            ..rtp_stats(3, 5, 200)
        };
        assert_eq!(
            Some(traffic_stats(
                Time::unix(2, 0),
                Time::unix(3, 0),
                (5, 4, 5, 6),
                200
            )),
            rtp_stats_diff_to_traffic_stats(Some(&b), Some(&a))
        );

        // where a reset begins: at more than half the range of the counter. After one packet
        // and one byte: the packets and bytes after, and what the difference is.
        let b = rtp_stats(2, 1, 1);
        for (packets, bytes, expected_packets, expected_bytes) in [
            ((1u32 << 31) + 1, 1u64, 1u32 << 31, 0u64),
            ((1 << 31) + 2, 1, (1 << 31) + 2, 1),
            (2, (1 << 63) + 1, 1, 1 << 63),
            (2, (1 << 63) + 2, 2, (1 << 63) + 2),
        ] {
            assert_eq!(
                Some(traffic_stats(
                    Time::unix(2, 0),
                    Time::unix(3, 0),
                    (expected_packets, 0, 0, 0),
                    expected_bytes
                )),
                rtp_stats_diff_to_traffic_stats(Some(&b), Some(&rtp_stats(3, packets, bytes))),
                "packets {packets}, bytes {bytes}"
            );
        }

        // without timestamps the times are the Unix epoch
        let a = livekit::RtpStats {
            packets: 1,
            ..Default::default()
        };
        let expected = Some(traffic_stats(
            Time::unix(0, 0),
            Time::unix(0, 0),
            (1, 0, 0, 0),
            0,
        ));
        assert_eq!(expected, rtp_stats_diff_to_traffic_stats(None, Some(&a)));
        assert_eq!(
            expected,
            rtp_stats_diff_to_traffic_stats(Some(&livekit::RtpStats::default()), Some(&a))
        );

        // the sum of the three byte counts wraps
        let a = livekit::RtpStats {
            bytes_duplicate: 3,
            bytes_padding: 4,
            ..rtp_stats(3, 1, u64::MAX)
        };
        assert_eq!(
            Some(traffic_stats(
                Time::unix(0, 0),
                Time::unix(3, 0),
                (1, 0, 0, 0),
                6
            )),
            rtp_stats_diff_to_traffic_stats(None, Some(&a))
        );
    }

    #[test]
    fn the_aggregate_of_traffic_stats() {
        assert_eq!(None, aggregate_traffic_stats(&[]));

        let s1 = traffic_stats(Time::unix(100, 0), Time::unix(110, 0), (1, 2, 3, 4), 5);
        let s2 = traffic_stats(Time::unix(90, 0), Time::unix(105, 0), (10, 20, 30, 40), 50);
        let s3 = traffic_stats(
            Time::unix(95, 0),
            Time::unix(120, 0),
            (100, 200, 300, 400),
            500,
        );
        assert_eq!(Some(s1), aggregate_traffic_stats(&[s1]));
        let all = Some(traffic_stats(
            Time::unix(90, 0),
            Time::unix(120, 0),
            (111, 222, 333, 444),
            555,
        ));
        assert_eq!(all, aggregate_traffic_stats(&[s1, s2, s3]));
        assert_eq!(all, aggregate_traffic_stats(&[s3, s2, s1]));

        // without times: no start, and the end is now
        let zero = TrafficStats {
            packets: 7,
            ..Default::default()
        };
        let before = time::now();
        let agg = aggregate_traffic_stats(&[zero]).unwrap();
        assert!(agg.start_time.is_zero());
        assert!(!agg.end_time.before(before) && !agg.end_time.after(time::now()));
        assert_eq!(7, agg.packets);

        // an entry without times before one with times takes those
        assert_eq!(
            Some(traffic_stats(
                Time::unix(100, 0),
                Time::unix(110, 0),
                (8, 2, 3, 4),
                5
            )),
            aggregate_traffic_stats(&[zero, s1])
        );
        // and after one with times, its zero time is the earliest start
        let agg = aggregate_traffic_stats(&[s1, zero]).unwrap();
        assert!(agg.start_time.is_zero());
        assert_eq!(Time::unix(110, 0), agg.end_time);
        assert_eq!(8, agg.packets);

        // the sums wrap
        let wrap = traffic_stats(
            Time::unix(1, 0),
            Time::unix(2, 0),
            (u32::MAX, 0, 0, 0),
            u64::MAX,
        );
        assert_eq!(
            Some(traffic_stats(
                Time::unix(1, 0),
                Time::unix(110, 0),
                (0, 2, 3, 4),
                4
            )),
            aggregate_traffic_stats(&[wrap, s1])
        );
    }

    #[test]
    fn the_rates_of_a_traffic_load() {
        assert_eq!(
            (0.0, 0.0, 0.0, 0.0),
            traffic_load_to_traffic_rate(None),
            "no load"
        );
        assert_eq!(
            (0.0, 0.0, 0.0, 0.0),
            traffic_load_to_traffic_rate(Some(&TrafficLoad::default())),
            "empty load"
        );

        let entry = |track_type, stream_type, end_time: Time, packets, bytes| TrafficTypeStats {
            track_type,
            stream_type,
            traffic_stats: TrafficStats {
                start_time: Time::unix(100, 0),
                end_time,
                packets,
                bytes,
                ..Default::default()
            },
        };
        let load = TrafficLoad {
            traffic_type_stats: vec![
                entry(
                    livekit::TrackType::Audio,
                    StreamType::Upstream,
                    Time::unix(110, 0),
                    500,
                    40000,
                ),
                entry(
                    livekit::TrackType::Video,
                    StreamType::Upstream,
                    Time::unix(104, 0),
                    1000,
                    1000000,
                ),
                entry(
                    livekit::TrackType::Audio,
                    StreamType::Downstream,
                    Time::unix(100, 500000000),
                    30,
                    3000,
                ),
                entry(
                    livekit::TrackType::Video,
                    StreamType::Downstream,
                    Time::unix(108, 0),
                    4000,
                    2000000,
                ),
            ],
        };
        assert_eq!(
            (300.0, 254000.0, 560.0, 256000.0),
            traffic_load_to_traffic_rate(Some(&load))
        );

        // no time between start and end: the division by zero of the original
        let load = TrafficLoad {
            traffic_type_stats: vec![entry(
                livekit::TrackType::Audio,
                StreamType::Upstream,
                Time::unix(100, 0),
                500,
                0,
            )],
        };
        let (packet_rate_in, byte_rate_in, packet_rate_out, byte_rate_out) =
            traffic_load_to_traffic_rate(Some(&load));
        assert_eq!(f64::INFINITY, packet_rate_in);
        assert!(byte_rate_in.is_nan());
        assert_eq!((0.0, 0.0), (packet_rate_out, byte_rate_out));
    }

    #[test]
    fn stream_types_have_the_numbers_of_the_protocol() {
        assert_eq!(0, StreamType::Upstream as i32);
        assert_eq!(1, StreamType::Downstream as i32);
    }
}
