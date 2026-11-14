// Ported from LiveKit, pkg/sfu/streamallocator/track.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - A track is shared (the allocator's map, its shadow, and the sorted lists of a handler
//   all hold it), so `NewTrack` is `Track::new` and gives an `Arc`, and its methods take
//   `&self`. The fields that change (priority, max layer, the two NACK counters, dirty,
//   stream state) are behind a lock of the track's own, which is held for the read or the
//   write only. Upstream they are plain fields: some are written under the allocator's
//   `videoTracksMu`, some by the events worker without it, and the sorters read them
//   without it.
// - The three sorters (`sort.Interface` upstream: `Len`, `Swap`, `Less`) are lists with a
//   `sort` of their own, which is `sort.Sort` of the original. `less` is the original's
//   `Less`, on the values it compares instead of on two indices: `sort` reads those values
//   once per track before it sorts, where `Less` reads them at every comparison. They can
//   change while a list is sorted (a priority or a max layer by a call from another thread,
//   a distance with the bitrates the publisher measures), and Rust's sort may panic on an
//   order that changes under it, where Go's gives some order.
// The test at the end is new: the original has none.

use std::cmp::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::{DownTrack, VideoAllocation, VideoTransition};

use super::StreamState;
use super::streamallocator::{C_PRIORITY_DEFAULT_SCREENSHARE, C_PRIORITY_DEFAULT_VIDEO};

pub struct Track {
    down_track: Arc<DownTrack>,
    source: livekit::TrackSource,
    is_multi_layered: bool,
    publisher_id: livekit::ParticipantId,
    #[allow(dead_code)] // not read upstream either
    logger: Logger,

    lock: Mutex<TrackInner>,
}

// The fields of the original that change after `NewTrack`: see the head of this file.
struct TrackInner {
    priority: u8,

    max_layer: buffer::VideoLayer,

    total_packets: u32,
    total_repeated_nacks: u32,

    is_dirty: bool,

    stream_state: StreamState,
}

impl Track {
    pub fn new(
        down_track: Arc<DownTrack>,
        source: livekit::TrackSource,
        is_multi_layered: bool,
        publisher_id: livekit::ParticipantId,
        logger: Logger,
    ) -> Arc<Track> {
        let t = Track {
            down_track,
            source,
            is_multi_layered,
            publisher_id,
            logger,
            lock: Mutex::new(TrackInner {
                priority: 0,
                max_layer: buffer::VideoLayer::default(),
                total_packets: 0,
                total_repeated_nacks: 0,
                is_dirty: false,
                stream_state: StreamState::Inactive,
            }),
        };
        t.set_priority(0);
        t.set_max_layer(t.down_track.max_layer());

        Arc::new(t)
    }

    pub fn set_dirty(&self, is_dirty: bool) -> bool {
        let mut t = self.lock.lock();
        if t.is_dirty == is_dirty {
            return false;
        }

        t.is_dirty = is_dirty;
        true
    }

    pub fn set_stream_state(&self, stream_state: StreamState) -> bool {
        let mut t = self.lock.lock();
        if t.stream_state == stream_state {
            return false;
        }

        t.stream_state = stream_state;
        true
    }

    pub fn is_subscribe_mutable(&self) -> bool {
        self.lock.lock().stream_state != StreamState::Paused
    }

    pub fn set_priority(&self, mut priority: u8) -> bool {
        if priority == 0 {
            priority = match self.source {
                livekit::TrackSource::ScreenShare => C_PRIORITY_DEFAULT_SCREENSHARE,
                _ => C_PRIORITY_DEFAULT_VIDEO,
            };
        }

        let mut t = self.lock.lock();
        if t.priority == priority {
            return false;
        }

        t.priority = priority;
        true
    }

    pub fn priority(&self) -> u8 {
        self.lock.lock().priority
    }

    pub fn down_track(&self) -> &Arc<DownTrack> {
        &self.down_track
    }

    pub fn is_managed(&self) -> bool {
        self.source != livekit::TrackSource::ScreenShare || self.is_multi_layered
    }

    pub fn id(&self) -> livekit::TrackId {
        livekit::TrackId::from(self.down_track.id())
    }

    pub fn publisher_id(&self) -> livekit::ParticipantId {
        self.publisher_id.clone()
    }

    pub fn set_max_layer(&self, layer: buffer::VideoLayer) -> bool {
        let mut t = self.lock.lock();
        if t.max_layer == layer {
            return false;
        }

        t.max_layer = layer;
        true
    }

    pub fn write_padding_rtp(&self, bytes_to_send: i64) -> i64 {
        self.down_track
            .write_padding_rtp(bytes_to_send, false, false)
    }

    pub fn write_probe_packets(&self, bytes_to_send: i64) -> i64 {
        self.down_track.write_probe_packets(bytes_to_send, false)
    }

    pub fn allocate_optimal(&self, allow_overshoot: bool, hold: bool) -> VideoAllocation {
        self.down_track.allocate_optimal(allow_overshoot, hold)
    }

    pub fn provisional_allocate_prepare(&self) {
        self.down_track.provisional_allocate_prepare();
    }

    pub fn provisional_allocate_reset(&self) {
        self.down_track.provisional_allocate_reset();
    }

    pub fn provisional_allocate(
        &self,
        available_channel_capacity: i64,
        layer: buffer::VideoLayer,
        allow_pause: bool,
        allow_overshoot: bool,
    ) -> (bool, i64) {
        self.down_track.provisional_allocate(
            available_channel_capacity,
            layer,
            allow_pause,
            allow_overshoot,
        )
    }

    pub fn provisional_allocate_get_cooperative_transition(
        &self,
        allow_overshoot: bool,
    ) -> VideoTransition {
        self.down_track
            .provisional_allocate_get_cooperative_transition(allow_overshoot)
    }

    pub fn provisional_allocate_get_best_weighted_transition(&self) -> VideoTransition {
        self.down_track
            .provisional_allocate_get_best_weighted_transition()
    }

    pub fn provisional_allocate_commit(&self) -> VideoAllocation {
        self.down_track.provisional_allocate_commit()
    }

    pub fn allocate_next_higher(
        &self,
        available_channel_capacity: i64,
        allow_overshoot: bool,
    ) -> (VideoAllocation, bool) {
        self.down_track
            .allocate_next_higher(available_channel_capacity, allow_overshoot)
    }

    pub fn get_next_higher_transition(&self, allow_overshoot: bool) -> (VideoTransition, bool) {
        self.down_track.get_next_higher_transition(allow_overshoot)
    }

    pub fn pause(&self) -> VideoAllocation {
        self.down_track.pause()
    }

    pub fn is_deficient(&self) -> bool {
        self.down_track.is_deficient()
    }

    pub fn bandwidth_requested(&self) -> i64 {
        self.down_track.bandwidth_requested()
    }

    pub fn distance_to_desired(&self) -> f64 {
        self.down_track.distance_to_desired()
    }

    pub fn get_nack_delta(&self) -> (u32, u32) {
        let (total_packets, total_repeated_nacks) = self.down_track.get_nack_stats();

        let mut t = self.lock.lock();
        let packet_delta = total_packets.wrapping_sub(t.total_packets);
        t.total_packets = total_packets;

        let nack_delta = total_repeated_nacks.wrapping_sub(t.total_repeated_nacks);
        t.total_repeated_nacks = total_repeated_nacks;

        (packet_delta, nack_delta)
    }

    // What `TrackSorter` compares of a track.
    fn priority_and_max_layer(&self) -> (u8, buffer::VideoLayer) {
        let t = self.lock.lock();
        (t.priority, t.max_layer)
    }
}

// ------------------------------------------------

// Go's `sort.Sort` of a list with the `Less` given: see the head of this file. `key` is
// read once per track, and `less` says of two keys whether the first comes earlier.
fn sort<K>(tracks: &mut Vec<Arc<Track>>, key: impl Fn(&Track) -> K, less: impl Fn(&K, &K) -> bool) {
    let mut keyed: Vec<(K, Arc<Track>)> = tracks.drain(..).map(|t| (key(&t), t)).collect();
    keyed.sort_by(|(i, _), (j, _)| {
        if less(i, j) {
            Ordering::Less
        } else if less(j, i) {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    });
    tracks.extend(keyed.into_iter().map(|(_, t)| t));
}

// ------------------------------------------------

#[derive(Default)]
pub struct TrackSorter(pub Vec<Arc<Track>>);

impl TrackSorter {
    pub fn sort(&mut self) {
        sort(&mut self.0, Track::priority_and_max_layer, Self::less);
    }

    fn less(i: &(u8, buffer::VideoLayer), j: &(u8, buffer::VideoLayer)) -> bool {
        //
        // TrackSorter is used to allocate layer-by-layer.
        // So, higher priority track should come earlier so that it gets an earlier shot at each layer
        //
        let (i_priority, i_max_layer) = *i;
        let (j_priority, j_max_layer) = *j;
        if i_priority != j_priority {
            return i_priority > j_priority;
        }

        if i_max_layer.spatial != j_max_layer.spatial {
            return i_max_layer.spatial > j_max_layer.spatial;
        }

        i_max_layer.temporal > j_max_layer.temporal
    }
}

// ------------------------------------------------

#[derive(Default)]
pub struct MaxDistanceSorter(pub Vec<Arc<Track>>);

impl MaxDistanceSorter {
    pub fn sort(&mut self) {
        sort(
            &mut self.0,
            |t| (t.priority(), t.distance_to_desired()),
            Self::less,
        );
    }

    fn less(i: &(u8, f64), j: &(u8, f64)) -> bool {
        //
        // MaxDistanceSorter is used to find a deficient track to use for probing during recovery from congestion.
        // So, higher priority track should come earlier so that they have a chance to recover sooner.
        //
        let (i_priority, i_distance_to_desired) = *i;
        let (j_priority, j_distance_to_desired) = *j;
        if i_priority != j_priority {
            return i_priority > j_priority;
        }

        i_distance_to_desired > j_distance_to_desired
    }
}

// ------------------------------------------------

#[derive(Default)]
pub struct MinDistanceSorter(pub Vec<Arc<Track>>);

impl MinDistanceSorter {
    pub fn sort(&mut self) {
        sort(
            &mut self.0,
            |t| (t.priority(), t.distance_to_desired()),
            Self::less,
        );
    }

    fn less(i: &(u8, f64), j: &(u8, f64)) -> bool {
        //
        // MinDistanceSorter is used to find excess bandwidth in cooperative allocation.
        // So, lower priority track should come earlier so that they contribute bandwidth to higher priority tracks.
        //
        let (i_priority, i_distance_to_desired) = *i;
        let (j_priority, j_distance_to_desired) = *j;
        if i_priority != j_priority {
            return i_priority < j_priority;
        }

        i_distance_to_desired < j_distance_to_desired
    }
}

// ------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn layer(spatial: i32, temporal: i32) -> buffer::VideoLayer {
        buffer::VideoLayer { spatial, temporal }
    }

    // New for musivum: the original has no tests. The three orders, as the comments of the
    // original's `Less` functions give them.
    #[test]
    fn the_sorters_order_as_the_original() {
        // higher priority first, then the higher max layer
        assert!(TrackSorter::less(&(2, layer(0, 0)), &(1, layer(2, 3))));
        assert!(!TrackSorter::less(&(1, layer(2, 3)), &(2, layer(0, 0))));
        assert!(TrackSorter::less(&(1, layer(2, 0)), &(1, layer(1, 3))));
        assert!(TrackSorter::less(&(1, layer(1, 3)), &(1, layer(1, 2))));
        assert!(!TrackSorter::less(&(1, layer(1, 2)), &(1, layer(1, 2))));

        // higher priority first, then the one farther from its desired layer
        assert!(MaxDistanceSorter::less(&(2, 0.5), &(1, 4.0)));
        assert!(!MaxDistanceSorter::less(&(1, 4.0), &(2, 0.5)));
        assert!(MaxDistanceSorter::less(&(1, 4.0), &(1, 0.5)));
        assert!(!MaxDistanceSorter::less(&(1, 0.5), &(1, 0.5)));

        // lower priority first, then the one closer to its desired layer
        assert!(MinDistanceSorter::less(&(1, 4.0), &(2, 0.5)));
        assert!(!MinDistanceSorter::less(&(2, 0.5), &(1, 4.0)));
        assert!(MinDistanceSorter::less(&(1, 0.5), &(1, 4.0)));
        assert!(!MinDistanceSorter::less(&(1, 0.5), &(1, 0.5)));
    }
}
