// Ported from LiveKit, pkg/telemetry/statsworker.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only what the interface of the service and `pkg/rtc`
// name is here: `ReferenceGuard`, and `CondenseStat` with `CondensedStat` and `isValid`. Not
// ported: `ReferenceCount`, `StatsWorker` and `coalesce`, which are of the implementation of
// the service. A guard is shared by the participant that makes it and the service
// (`Arc<ReferenceGuard>`), so its two flags, plain `bool`s that `ReferenceCount` sets
// upstream, are atomics, visible in this module for what takes its place.
// `MarshalLogObject` is `Debug`, with the keys of the original. `condense_stat` returns its
// two results as a tuple. In it a stream without a start time starts at the Unix epoch, as
// upstream, but `go::time::Time` cannot tell the epoch from "no time yet": a start time of a
// later stream replaces it, where upstream keeps the epoch. The tests at the end are new: the
// original has none of these.

use std::fmt;

use crate::go::atomic;
use crate::go::time::{Time, timestamppb_as_time};
use crate::protocol::livekit;

#[derive(Default)]
pub struct ReferenceGuard {
    pub(super) activated: atomic::Bool,
    pub(super) released: atomic::Bool,
}

impl fmt::Debug for ReferenceGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReferenceGuard")
            .field("self", &std::ptr::from_ref(self).addr())
            .field("activated", &self.activated.load())
            .field("released", &self.released.load())
            .finish()
    }
}

// ----------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CondensedStat {
    pub start_time: Time,
    pub end_time: Time,
    pub bytes: u64,
    pub packets: u32,
    pub packets_lost: u32,
    pub frames: u32,
}

pub fn condense_stat(stat: &livekit::AnalyticsStat) -> (CondensedStat, bool) {
    let mut ps = CondensedStat::default();
    let ok = is_valid(stat);
    if !ok {
        return (ps, ok);
    }

    for stream in &stat.streams {
        let start_time = stream
            .start_time
            .as_ref()
            .map(timestamppb_as_time)
            .unwrap_or(Time::ZERO);
        let end_time = stream
            .end_time
            .as_ref()
            .map(timestamppb_as_time)
            .unwrap_or(Time::ZERO);
        if ps.start_time.is_zero() || start_time.before(ps.start_time) {
            ps.start_time = start_time;
        }
        if end_time.after(ps.end_time) {
            ps.end_time = end_time;
        }

        ps.bytes = ps.bytes.wrapping_add(stream.primary_bytes);
        ps.packets = ps.packets.wrapping_add(stream.primary_packets);
        ps.packets_lost = ps.packets_lost.wrapping_add(stream.packets_lost);
        if stream.frames > ps.frames {
            ps.frames = stream.frames;
        }
    }

    (ps, ok)
}

fn is_valid(stat: &livekit::AnalyticsStat) -> bool {
    for analytics_stream in &stat.streams {
        if (analytics_stream.primary_packets as i32) < 0
            || (analytics_stream.primary_bytes as i64) < 0
            || (analytics_stream.retransmit_packets as i32) < 0
            || (analytics_stream.retransmit_bytes as i64) < 0
            || (analytics_stream.padding_packets as i32) < 0
            || (analytics_stream.padding_bytes as i64) < 0
            || (analytics_stream.packets_lost as i32) < 0
            || (analytics_stream.packets_out_of_order as i32) < 0
            || (analytics_stream.frames as i32) < 0
            || (analytics_stream.nacks as i32) < 0
            || (analytics_stream.plis as i32) < 0
            || (analytics_stream.firs as i32) < 0
        {
            return false;
        }

        for video_layer in &analytics_stream.video_layers {
            if (video_layer.packets as i32) < 0
                || (video_layer.bytes as i64) < 0
                || (video_layer.frames as i32) < 0
            {
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::go::time::{SECOND, timestamppb_new};

    // New for musivum.
    #[test]
    fn a_guard_starts_unused_and_is_logged_with_the_keys_of_the_original() {
        let guard = ReferenceGuard::default();
        assert!(!guard.activated.load());
        assert!(!guard.released.load());

        guard.activated.store(true);
        let address = std::ptr::from_ref(&guard).addr();
        assert_eq!(
            format!("ReferenceGuard {{ self: {address}, activated: true, released: false }}"),
            format!("{guard:?}")
        );
    }

    // New for musivum.
    #[test]
    fn condense_stat_sums_the_streams_of_a_valid_stat() {
        let t0 = Time::unix(1_700_000_000, 0);
        let stream =
            |start: Time, end: Time, bytes, packets, lost, frames| livekit::AnalyticsStream {
                start_time: Some(timestamppb_new(start)),
                end_time: Some(timestamppb_new(end)),
                primary_bytes: bytes,
                primary_packets: packets,
                packets_lost: lost,
                frames,
                // not counted
                retransmit_bytes: 1000,
                padding_packets: 7,
                ..Default::default()
            };
        let mut stat = livekit::AnalyticsStat {
            streams: vec![
                stream(t0 + SECOND, t0 + SECOND * 3, 500, 5, 1, 30),
                stream(t0, t0 + SECOND * 2, 700, 7, 2, 20),
            ],
            ..Default::default()
        };

        let (cs, ok) = condense_stat(&stat);
        assert!(ok);
        assert_eq!(
            CondensedStat {
                start_time: t0,
                end_time: t0 + SECOND * 3,
                bytes: 1200,
                packets: 12,
                packets_lost: 3,
                frames: 30,
            },
            cs
        );

        // a count that would be negative as a signed number makes the stat invalid
        stat.streams[1].nacks = 1 << 31;
        assert_eq!((CondensedStat::default(), false), condense_stat(&stat));
        stat.streams[1].nacks = 0;
        stat.streams[1].video_layers = vec![livekit::AnalyticsVideoLayer {
            bytes: 1 << 63,
            ..Default::default()
        }];
        assert_eq!((CondensedStat::default(), false), condense_stat(&stat));

        // no streams: valid and empty
        assert_eq!(
            (CondensedStat::default(), true),
            condense_stat(&livekit::AnalyticsStat::default())
        );
    }
}
