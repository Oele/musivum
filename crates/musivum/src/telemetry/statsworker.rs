// Ported from LiveKit, pkg/telemetry/statsworker.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. A guard is shared by the participant that makes it and
// the service (`Arc<ReferenceGuard>`), so its two flags, plain `bool`s that `ReferenceCount`
// sets under the worker's lock upstream, are atomics; the functions that take a guard take an
// `Option<&ReferenceGuard>`, `None` for nil. `MarshalLogObject` is `Debug`, with the keys of
// the original; a nil worker is logged as `None`. The `context.Context` of the worker is
// dropped. The worker is an `Arc`; its `next`, the link of the service's list of workers that
// the service changes, is under a lock of its own. The worker's `t` is a `Weak` of the
// service, which holds the worker (nil in the tests: a `Weak` that leads nowhere, which
// `Flush` passes over). `isConnected` is with what the worker's lock guards, where it is read
// and written. A stat is kept by value. `SetConnected` of a nil worker, which does nothing,
// is the caller's `if let`. `condense_stat` returns its two results as a tuple.
// `go::time::Time` cannot tell the Unix epoch from Go's zero time, so in `coalesce` and
// `condense_stat` a stream without a start time, which starts at the epoch upstream, is
// replaced by the start time of a later stream where upstream keeps the epoch; and the
// coalesced stream of stats without streams starts and ends at the epoch, where upstream it
// is the year 1. The tests at the end are new; `statsworker_test.rs` has the original's.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use super::TelemetryService;
use crate::go::atomic;
use crate::go::time::{self, Duration, Time, timestamppb_as_time, timestamppb_new};
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::utils;
use crate::warnw;

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
pub struct ReferenceCount {
    count: i64,
}

impl ReferenceCount {
    pub fn activate(&mut self, guard: Option<&ReferenceGuard>) {
        if let Some(guard) = guard
            && !guard.activated.load()
        {
            guard.activated.store(true);
            self.count += 1;
        }
    }

    pub fn release(&mut self, guard: Option<&ReferenceGuard>) -> bool {
        let Some(guard) = guard else {
            return false;
        };
        if !guard.activated.load() || guard.released.load() {
            return false;
        }
        guard.released.store(true);
        self.count -= 1;
        self.count == 0
    }
}

// ----------------------------------------

// StatsWorker handles participant stats
pub struct StatsWorker {
    pub(super) next: Mutex<Option<Arc<StatsWorker>>>,

    t: Weak<dyn TelemetryService>,
    pub(super) room_id: livekit::RoomId,
    room_name: livekit::RoomName,
    pub(super) participant_id: livekit::ParticipantId,
    participant_identity: livekit::ParticipantIdentity,

    lock: RwLock<StatsWorkerInner>,
}

struct StatsWorkerInner {
    is_connected: bool,
    outgoing_per_track: HashMap<livekit::TrackId, Vec<livekit::AnalyticsStat>>,
    incoming_per_track: HashMap<livekit::TrackId, Vec<livekit::AnalyticsStat>>,
    ref_count: ReferenceCount,
    closed_at: Time,
}

pub(super) fn new_stats_worker(
    t: Weak<dyn TelemetryService>,
    room_id: livekit::RoomId,
    room_name: livekit::RoomName,
    participant_id: livekit::ParticipantId,
    identity: livekit::ParticipantIdentity,
    guard: Option<&ReferenceGuard>,
) -> Arc<StatsWorker> {
    let mut inner = StatsWorkerInner {
        is_connected: false,
        outgoing_per_track: HashMap::new(),
        incoming_per_track: HashMap::new(),
        ref_count: ReferenceCount::default(),
        closed_at: Time::ZERO,
    };
    inner.ref_count.activate(guard);
    Arc::new(StatsWorker {
        next: Mutex::new(None),
        t,
        room_id,
        room_name,
        participant_id,
        participant_identity: identity,
        lock: RwLock::new(inner),
    })
}

impl StatsWorker {
    pub fn on_track_stat(
        &self,
        track_id: livekit::TrackId,
        direction: livekit::StreamType,
        stat: livekit::AnalyticsStat,
    ) {
        let mut s = self.lock.write();
        if direction == livekit::StreamType::Downstream {
            s.outgoing_per_track.entry(track_id).or_default().push(stat);
        } else {
            s.incoming_per_track.entry(track_id).or_default().push(stat);
        }
        drop(s);
    }

    pub fn participant_id(&self) -> livekit::ParticipantId {
        self.participant_id.clone()
    }

    pub fn set_connected(&self) {
        // a nil worker: see the header

        let mut s = self.lock.write();
        s.is_connected = true;
        drop(s);
    }

    pub fn is_connected(&self) -> bool {
        let s = self.lock.read();

        s.is_connected
    }

    pub fn flush(&self, now: Time, close_wait: Duration) -> bool {
        let ts = timestamppb_new(now);

        let mut s = self.lock.write();
        let stats = Vec::with_capacity(s.incoming_per_track.len() + s.outgoing_per_track.len());

        let incoming_per_track = std::mem::take(&mut s.incoming_per_track);

        let outgoing_per_track = std::mem::take(&mut s.outgoing_per_track);

        let closed = !s.closed_at.is_zero() && now.sub(s.closed_at) > close_wait;
        drop(s);

        let stats = self.collect_stats(
            &ts,
            livekit::StreamType::Upstream,
            incoming_per_track,
            stats,
        );
        let stats = self.collect_stats(
            &ts,
            livekit::StreamType::Downstream,
            outgoing_per_track,
            stats,
        );
        if !stats.is_empty()
            && let Some(t) = self.t.upgrade()
        {
            t.send_stats(&stats);
        }

        closed
    }

    pub fn close(&self, guard: Option<&ReferenceGuard>) -> bool {
        let mut s = self.lock.write();

        if !s.ref_count.release(guard) {
            return false;
        }

        let ok = s.closed_at.is_zero();
        if ok {
            s.closed_at = time::now();
        }
        ok
    }

    pub fn closed(&self, guard: Option<&ReferenceGuard>) -> bool {
        let mut s = self.lock.write();
        if s.closed_at.is_zero() {
            s.ref_count.activate(guard);
            return false;
        }
        true
    }

    fn collect_stats(
        &self,
        ts: &musivum_protocol::Timestamp,
        stream_type: livekit::StreamType,
        per_track: HashMap<livekit::TrackId, Vec<livekit::AnalyticsStat>>,
        mut stats: Vec<livekit::AnalyticsStat>,
    ) -> Vec<livekit::AnalyticsStat> {
        for (track_id, analytics_stats) in per_track {
            let Some(mut coalesced) = coalesce(&analytics_stats) else {
                continue;
            };

            coalesced.time_stamp = Some(*ts);
            coalesced.track_id = track_id.to_string();
            coalesced.set_kind(stream_type);
            coalesced.room_id = self.room_id.to_string();
            coalesced.participant_id = self.participant_id.to_string();
            coalesced.room_name = self.room_name.to_string();
            stats.push(coalesced);
        }
        stats
    }
}

impl fmt::Debug for StatsWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.lock.read();

        f.debug_struct("StatsWorker")
            .field("room", &self.room_name)
            .field("roomID", &self.room_id)
            .field("participant", &self.participant_identity)
            .field("participantID", &self.participant_id)
            .field("isConnected", &s.is_connected)
            .field("closedAt", &s.closed_at)
            .field("refCount", &s.ref_count)
            .finish()
    }
}

// -------------------------------------------------------------------------

// create a single stream and single video layer post aggregation
// (the two branches that set the minimum score are the original's)
#[allow(clippy::if_same_then_else)]
fn coalesce(stats: &[livekit::AnalyticsStat]) -> Option<livekit::AnalyticsStat> {
    if stats.is_empty() {
        return None;
    }

    // find aggregates across streams
    let mut start_time = Time::ZERO;
    let mut end_time = Time::ZERO;
    let mut score_sum = 0.0f32; // used for average
    let mut min_score = 0.0f32; // min score in batched stats
    let mut scores: Vec<f32> = Vec::new(); // used for median
    let mut max_rtt = 0u32;
    let mut max_jitter = 0u32;
    let mut coalesced_video_layers: HashMap<i32, livekit::AnalyticsVideoLayer> = HashMap::new();
    let mut coalesced_stream = livekit::AnalyticsStream::default();
    for stat in stats {
        if !is_valid(stat) {
            warnw!(logger::get_logger(), "telemetry skipping invalid stat", nil, "stat" => stat);
            continue;
        }

        // only consider non-zero scores
        if stat.score > 0.0 {
            if min_score == 0.0 {
                min_score = stat.score;
            } else if stat.score < min_score {
                min_score = stat.score;
            }
            score_sum += stat.score;
            scores.push(stat.score);
        }

        for analytics_stream in &stat.streams {
            let start = analytics_stream
                .start_time
                .as_ref()
                .map(timestamppb_as_time)
                .unwrap_or(Time::ZERO);
            if start_time.is_zero() || start_time.after(start) {
                start_time = start;
            }

            let end = analytics_stream
                .end_time
                .as_ref()
                .map(timestamppb_as_time)
                .unwrap_or(Time::ZERO);
            if end_time.is_zero() || end_time.before(end) {
                end_time = end;
            }

            if analytics_stream.rtt > max_rtt {
                max_rtt = analytics_stream.rtt;
            }

            if analytics_stream.jitter > max_jitter {
                max_jitter = analytics_stream.jitter;
            }

            let cs = &mut coalesced_stream;
            cs.primary_packets = cs
                .primary_packets
                .wrapping_add(analytics_stream.primary_packets);
            cs.primary_bytes = cs
                .primary_bytes
                .wrapping_add(analytics_stream.primary_bytes);
            cs.retransmit_packets = cs
                .retransmit_packets
                .wrapping_add(analytics_stream.retransmit_packets);
            cs.retransmit_bytes = cs
                .retransmit_bytes
                .wrapping_add(analytics_stream.retransmit_bytes);
            cs.padding_packets = cs
                .padding_packets
                .wrapping_add(analytics_stream.padding_packets);
            cs.padding_bytes = cs
                .padding_bytes
                .wrapping_add(analytics_stream.padding_bytes);
            cs.packets_lost = cs.packets_lost.wrapping_add(analytics_stream.packets_lost);
            cs.packets_out_of_order = cs
                .packets_out_of_order
                .wrapping_add(analytics_stream.packets_out_of_order);
            cs.frames = cs.frames.wrapping_add(analytics_stream.frames);
            cs.nacks = cs.nacks.wrapping_add(analytics_stream.nacks);
            cs.plis = cs.plis.wrapping_add(analytics_stream.plis);
            cs.firs = cs.firs.wrapping_add(analytics_stream.firs);

            for video_layer in &analytics_stream.video_layers {
                match coalesced_video_layers.get_mut(&video_layer.layer) {
                    None => {
                        coalesced_video_layers.insert(video_layer.layer, *video_layer);
                    }
                    Some(coalesced_video_layer) => {
                        coalesced_video_layer.packets = coalesced_video_layer
                            .packets
                            .wrapping_add(video_layer.packets);
                        coalesced_video_layer.bytes =
                            coalesced_video_layer.bytes.wrapping_add(video_layer.bytes);
                        coalesced_video_layer.frames = coalesced_video_layer
                            .frames
                            .wrapping_add(video_layer.frames);
                    }
                }
            }
        }
    }
    coalesced_stream.start_time = Some(timestamppb_new(start_time));
    coalesced_stream.end_time = Some(timestamppb_new(end_time));
    coalesced_stream.rtt = max_rtt;
    coalesced_stream.jitter = max_jitter;

    // whittle it down to one video layer, just the max available layer
    let mut max_video_layer = -1i32;
    for coalesced_video_layer in coalesced_video_layers.values() {
        if max_video_layer == -1 || max_video_layer < coalesced_video_layer.layer {
            max_video_layer = coalesced_video_layer.layer;
            coalesced_stream.video_layers = vec![*coalesced_video_layer];
        }
    }

    let mut stat = livekit::AnalyticsStat {
        min_score,
        median_score: utils::median(&scores),
        streams: vec![coalesced_stream],
        mime: stats[stats.len() - 1].mime.clone(), // use the latest Mime
        ..Default::default()
    };
    let num_scores = scores.len();
    if num_scores > 0 {
        stat.score = score_sum / num_scores as f32;
    }
    Some(stat)
}

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
#[path = "statsworker_test.rs"]
mod statsworker_test;

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
