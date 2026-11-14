// Ported from LiveKit, pkg/sfu/streamtrackermanager.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `StreamTrackerManagerListener` is a trait, and the
// manager holds its listener as a `Weak`: the listener is the receiver that owns the manager,
// and a listener that is gone is no listener; `on_bitrate_report` is given the layers as a
// slice; `StreamTrackerType`, a string in the original, is an enum, with `Unknown` for every
// text that is neither of the two (the empty one of a configuration that names no type
// included), for which no tracker is made, as in the original; the YAML tags of the
// configuration are doc comments on its fields, as nothing reads a configuration file yet,
// and the three default configurations are built on first use; the manager is made as an
// `Arc` and keeps a `Weak` of itself, which the callbacks it gives to its trackers capture
// (the manager holds the trackers: an `Arc` there would be a cycle); `new` takes the
// configuration by reference and copies the half that is for the track's source; a track
// info that may be nil is an `Option`; everything `lock` guards is in an inner struct, and
// the functions the original calls with the lock held take it; the bitrate reporter is a
// task whose ticker is a tokio interval and which holds the manager as the goroutine does (a
// manager of a video track lives until `close`), `closed` is a `CancellationToken`, and
// `go listener.OnMaxPublishedLayerChanged(layer)` is a task too. The tests are new: the
// original has none.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Weak};

use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

use crate::go::time::{self, Duration};
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::streamtracker;
use crate::{debugw, errorw};

use super::Bitrates;

// ---------------------------------------------------

pub trait StreamTrackerManagerListener: Send + Sync {
    fn on_available_layers_changed(&self);
    fn on_bitrate_availability_changed(&self);
    fn on_max_published_layer_changed(&self, max_published_layer: i32);
    fn on_max_temporal_layer_seen_changed(&self, max_temporal_layer_seen: i32);
    fn on_max_available_layer_changed(&self, max_available_layer: i32);
    fn on_bitrate_report(&self, available_layers: &[i32], bitrates: Bitrates);
}

// ---------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum StreamTrackerType {
    /// Any other text in YAML, and the empty one of a configuration that names no type.
    #[default]
    Unknown,
    /// `packet` in YAML.
    Packet,
    /// `frame` in YAML.
    Frame,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamTrackerPacketConfig {
    /// `samples_required` in YAML.
    pub samples_required: u32, // number of samples needed per cycle
    /// `cycles_required` in YAML.
    pub cycles_required: u32, // number of cycles needed to be active
    /// `cycle_duration` in YAML.
    pub cycle_duration: Duration,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct StreamTrackerFrameConfig {
    /// `min_fps` in YAML.
    pub min_fps: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StreamTrackerConfig {
    /// `stream_tracker_type` in YAML.
    pub stream_tracker_type: StreamTrackerType,
    /// `bitrate_report_interval` in YAML.
    pub bitrate_report_interval: HashMap<i32, Duration>,
    /// `packet_tracker` in YAML.
    pub packet_tracker: HashMap<i32, streamtracker::StreamTrackerPacketConfig>,
    /// `frame_tracker` in YAML.
    pub frame_tracker: HashMap<i32, streamtracker::StreamTrackerFrameConfig>,
}

pub static DEFAULT_STREAM_TRACKER_CONFIG_VIDEO: LazyLock<StreamTrackerConfig> =
    LazyLock::new(|| StreamTrackerConfig {
        stream_tracker_type: StreamTrackerType::Packet,
        bitrate_report_interval: HashMap::from([
            (0, time::SECOND),
            (1, time::SECOND),
            (2, time::SECOND),
        ]),
        packet_tracker: streamtracker::DEFAULT_STREAM_TRACKER_PACKET_CONFIG_VIDEO.clone(),
        frame_tracker: streamtracker::DEFAULT_STREAM_TRACKER_FRAME_CONFIG_VIDEO.clone(),
    });

pub static DEFAULT_STREAM_TRACKER_CONFIG_SCREENSHARE: LazyLock<StreamTrackerConfig> =
    LazyLock::new(|| StreamTrackerConfig {
        stream_tracker_type: StreamTrackerType::Packet,
        bitrate_report_interval: HashMap::from([
            (0, 4 * time::SECOND),
            (1, 4 * time::SECOND),
            (2, 4 * time::SECOND),
        ]),
        packet_tracker: streamtracker::DEFAULT_STREAM_TRACKER_PACKET_CONFIG_SCREENSHARE.clone(),
        frame_tracker: streamtracker::DEFAULT_STREAM_TRACKER_FRAME_CONFIG_SCREENSHARE.clone(),
    });

// ---------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StreamTrackerManagerConfig {
    /// `video` in YAML.
    pub video: StreamTrackerConfig,
    /// `screenshare` in YAML.
    pub screenshare: StreamTrackerConfig,
}

pub static DEFAULT_STREAM_TRACKER_MANAGER_CONFIG: LazyLock<StreamTrackerManagerConfig> =
    LazyLock::new(|| StreamTrackerManagerConfig {
        video: DEFAULT_STREAM_TRACKER_CONFIG_VIDEO.clone(),
        screenshare: DEFAULT_STREAM_TRACKER_CONFIG_SCREENSHARE.clone(),
    });

// ---------------------------------------------------

// `len(s.trackers)` of the original
const NUM_SPATIAL_LAYERS: usize = (buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize;

pub struct StreamTrackerManager {
    logger: Logger,
    mime_type: mime::MimeType,
    video_layer_mode: livekit::video_layer::Mode,
    clock_rate: u32,

    tracker_config: StreamTrackerConfig,

    // what `lock sync.RWMutex` guards upstream
    inner: RwLock<StreamTrackerManagerInner>,

    closed: CancellationToken,

    // the manager itself, for the callbacks of its trackers
    this: Weak<StreamTrackerManager>,
}

struct StreamTrackerManagerInner {
    track_info: Option<livekit::TrackInfo>,
    max_published_layer: i32,
    max_temporal_layer_seen: i32,

    dd_tracker: Option<Arc<streamtracker::StreamTrackerDependencyDescriptor>>,
    trackers: [Option<Arc<dyn streamtracker::StreamTrackerWorker>>; NUM_SPATIAL_LAYERS],

    available_layers: Vec<i32>,
    max_expected_layer: i32,

    listener: Option<Weak<dyn StreamTrackerManagerListener>>,
}

impl StreamTrackerManager {
    pub fn new(
        logger: Logger,
        track_info: &livekit::TrackInfo,
        mime_type: mime::MimeType,
        clock_rate: u32,
        config: &StreamTrackerManagerConfig,
    ) -> Arc<StreamTrackerManager> {
        let tracker_config = match track_info.source() {
            livekit::TrackSource::ScreenShare => config.screenshare.clone(),
            livekit::TrackSource::Camera => config.video.clone(),
            _ => config.video.clone(),
        };

        let s = Arc::new_cyclic(|this| StreamTrackerManager {
            logger,
            mime_type,
            video_layer_mode: buffer::get_video_layer_mode_for_mime_type(
                mime_type,
                Some(track_info),
            ),
            clock_rate,
            tracker_config,
            inner: RwLock::new(StreamTrackerManagerInner {
                track_info: Some(track_info.clone()),
                max_published_layer: buffer::INVALID_LAYER_SPATIAL,
                max_temporal_layer_seen: buffer::INVALID_LAYER_TEMPORAL,
                dd_tracker: None,
                trackers: Default::default(),
                available_layers: Vec::new(),
                max_expected_layer: buffer::INVALID_LAYER_SPATIAL,
                listener: None,
            }),
            closed: CancellationToken::new(),
            this: this.clone(),
        });

        s.max_expected_layer_from_track_info(true);

        if track_info.r#type() == livekit::TrackType::Video {
            time::spawn(s.clone().bitrate_reporter());
        }
        s
    }

    pub fn close(&self) {
        self.closed.cancel();
    }

    pub fn set_listener(&self, listener: Option<Weak<dyn StreamTrackerManagerListener>>) {
        let mut inner = self.inner.write();
        inner.listener = listener;
    }

    fn get_listener(&self) -> Option<Arc<dyn StreamTrackerManagerListener>> {
        let inner = self.inner.read();

        inner.listener.as_ref().and_then(Weak::upgrade)
    }

    fn create_stream_tracker_packet(
        &self,
        layer: i32,
    ) -> Option<Box<dyn streamtracker::StreamTrackerImpl>> {
        let packet_tracker_config = self.tracker_config.packet_tracker.get(&layer)?;

        let params = streamtracker::StreamTrackerPacketParams {
            config: *packet_tracker_config,
            logger: self.logger.with_values(&[("layer", &layer)]),
        };
        Some(Box::new(streamtracker::StreamTrackerPacket::new(params)))
    }

    fn create_stream_tracker_frame(
        &self,
        layer: i32,
    ) -> Option<Box<dyn streamtracker::StreamTrackerImpl>> {
        let frame_tracker_config = self.tracker_config.frame_tracker.get(&layer)?;

        let params = streamtracker::StreamTrackerFrameParams {
            config: *frame_tracker_config,
            clock_rate: self.clock_rate,
            logger: self.logger.with_values(&[("layer", &layer)]),
        };
        Some(Box::new(streamtracker::StreamTrackerFrame::new(params)))
    }

    pub fn add_dependency_descriptor_trackers(&self) {
        let Some(&bitrate_interval) = self.tracker_config.bitrate_report_interval.get(&0) else {
            return;
        };
        let mut add_all_trackers = false;
        {
            let mut inner = self.inner.write();
            if inner.dd_tracker.is_none() {
                inner.dd_tracker = Some(streamtracker::StreamTrackerDependencyDescriptor::new(
                    streamtracker::StreamTrackerParams {
                        bitrate_report_interval: bitrate_interval,
                        logger: self.logger.with_values(&[("layer", &0)]),
                        ..Default::default()
                    },
                ));
                add_all_trackers = true;
            }
        }
        if add_all_trackers {
            for i in 0..=buffer::DEFAULT_MAX_LAYER_SPATIAL {
                self.add_tracker(i);
            }
        }
    }

    pub fn add_tracker(&self, layer: i32) -> Option<Arc<dyn streamtracker::StreamTrackerWorker>> {
        if layer < 0 || layer as usize >= NUM_SPATIAL_LAYERS {
            return None;
        }

        let mut tracker: Option<Arc<dyn streamtracker::StreamTrackerWorker>>;
        {
            let inner = self.inner.write();
            tracker = inner.trackers[layer as usize].clone();
            if tracker.is_some() {
                return tracker;
            }

            if let Some(dd_tracker) = &inner.dd_tracker {
                tracker = Some(dd_tracker.layered_tracker(layer));
            }
        }

        let bitrate_interval = *self.tracker_config.bitrate_report_interval.get(&layer)?;

        let tracker = match tracker {
            Some(tracker) => tracker,
            None => {
                let tracker_impl = match self.tracker_config.stream_tracker_type {
                    StreamTrackerType::Packet => self.create_stream_tracker_packet(layer),
                    StreamTrackerType::Frame => self.create_stream_tracker_frame(layer),
                    StreamTrackerType::Unknown => None,
                };
                let tracker_impl = tracker_impl?;

                streamtracker::StreamTracker::new(streamtracker::StreamTrackerParams {
                    stream_tracker_impl: Some(tracker_impl),
                    bitrate_report_interval: bitrate_interval,
                    logger: self.logger.with_values(&[("layer", &layer)]),
                })
            }
        };

        debugw!(self.logger, "stream tracker add track", "layer" => layer);
        tracker.on_status_changed(Box::new({
            let this = self.this.clone();
            move |status| {
                let Some(s) = this.upgrade() else {
                    return;
                };
                debugw!(
                    s.logger,
                    "stream tracker status changed",
                    "layer" => layer,
                    "status" => status,
                );
                if status == streamtracker::StreamStatus::Stopped {
                    s.remove_available_layer(layer);
                } else {
                    s.add_available_layer(layer);
                }
            }
        }));
        tracker.on_bitrate_available(Box::new({
            let this = self.this.clone();
            move || {
                let Some(s) = this.upgrade() else {
                    return;
                };
                if let Some(listener) = s.get_listener() {
                    listener.on_bitrate_availability_changed();
                }
            }
        }));

        let paused;
        let mut notify = false;
        {
            let mut inner = self.inner.write();
            paused = inner.track_info.as_ref().is_some_and(|ti| ti.muted);
            inner.trackers[layer as usize] = Some(tracker.clone());

            if layer > inner.max_published_layer {
                inner.max_published_layer = layer;
                notify = true;
            }
        }

        if notify && let Some(listener) = self.get_listener() {
            time::spawn(async move {
                listener.on_max_published_layer_changed(layer);
            });
        }

        tracker.set_paused(paused);
        tracker.start();
        Some(tracker)
    }

    pub fn remove_tracker(&self, layer: i32) {
        let tracker = {
            let mut inner = self.inner.write();
            inner.trackers[layer as usize].take()
        };

        if let Some(tracker) = tracker {
            tracker.stop();
        }
    }

    pub fn remove_all_trackers(&self) {
        let trackers;
        let dd_tracker;
        {
            let mut inner = self.inner.write();
            trackers = std::mem::take(&mut inner.trackers);
            inner.available_layers = Vec::new();

            inner.max_expected_layer = buffer::INVALID_LAYER_SPATIAL;
            self.max_expected_layer_from_track_info_locked(&mut inner, true);

            dd_tracker = inner.dd_tracker.take();
        }

        for tracker in trackers.into_iter().flatten() {
            tracker.stop();
        }
        if let Some(dd_tracker) = dd_tracker {
            dd_tracker.stop();
        }
    }

    pub fn get_tracker(&self, layer: i32) -> Option<Arc<dyn streamtracker::StreamTrackerWorker>> {
        let inner = self.inner.read();

        if layer < 0 || layer as usize >= inner.trackers.len() {
            errorw!(self.logger, "unexpected layer", nil, "layer" => layer);
            return None;
        }
        inner.trackers[layer as usize].clone()
    }

    fn set_paused(&self, paused: bool) {
        let trackers = {
            let inner = self.inner.write();
            inner.trackers.clone()
        };

        for tracker in trackers.into_iter().flatten() {
            tracker.set_paused(paused);
        }
    }

    pub fn update_track_info(&self, ti: Option<&livekit::TrackInfo>) {
        let paused;
        {
            let mut inner = self.inner.write();
            inner.track_info = ti.cloned();
            self.max_expected_layer_from_track_info_locked(&mut inner, false);

            paused = inner.track_info.as_ref().is_some_and(|ti| ti.muted);
        }

        self.set_paused(paused);
    }

    pub fn set_max_expected_spatial_layer(&self, layer: i32) -> i32 {
        let prev;
        let mut trackers_to_reset: Vec<Arc<dyn streamtracker::StreamTrackerWorker>> = Vec::new();
        {
            let mut inner = self.inner.write();
            prev = inner.max_expected_layer;
            if layer <= inner.max_expected_layer {
                // some higher layer(s) expected to stop, nothing else to do
                inner.max_expected_layer = layer;
                return prev;
            }

            //
            // Some higher layer is expected to start.
            // If the layer was not detected as stopped (i.e. it is still in available layers),
            // resetting tracker will declare layer available afresh. That's fine as it will be
            // a no-op in available layers handling.
            //
            for l in inner.max_expected_layer + 1..=layer {
                if let Some(tracker) = &inner.trackers[l as usize] {
                    trackers_to_reset.push(tracker.clone());
                }
            }
            inner.max_expected_layer = layer;
        }

        for tracker in trackers_to_reset {
            tracker.reset();
        }

        prev
    }

    pub fn distance_to_desired(&self) -> f64 {
        let inner = self.inner.read();

        if inner.track_info.as_ref().is_some_and(|ti| ti.muted)
            || inner.max_expected_layer < 0
            || inner.max_temporal_layer_seen < 0
        {
            return 0.0;
        }

        let (al, brs) = self.get_layered_bitrate_locked(&inner);

        let mut max_layer = buffer::INVALID_LAYER;
        'done: for s in (0..brs.len() as i32).rev() {
            for t in (0..brs[0].len() as i32).rev() {
                if brs[s as usize][t as usize] != 0 {
                    max_layer = buffer::VideoLayer {
                        spatial: s,
                        temporal: t,
                    };
                    break 'done;
                }
            }
        }

        // before bit rate measurement is available, stream tracker could declare layer seen, account for that
        for layer in al {
            if layer > max_layer.spatial {
                max_layer.spatial = layer;
                max_layer.temporal = inner.max_temporal_layer_seen; // till bit rate measurement is available, assume max seen as temporal
            }
        }

        let mut adjusted_max_layers = max_layer;
        if !max_layer.is_valid() {
            adjusted_max_layers = buffer::VideoLayer {
                spatial: 0,
                temporal: 0,
            };
        }

        let mut distance = (inner
            .max_expected_layer
            .wrapping_sub(adjusted_max_layers.spatial))
        .wrapping_mul(inner.max_temporal_layer_seen.wrapping_add(1))
        .wrapping_add(
            inner
                .max_temporal_layer_seen
                .wrapping_sub(adjusted_max_layers.temporal),
        );
        if !max_layer.is_valid() {
            distance = distance.wrapping_add(1);
        }

        distance as f64 / inner.max_temporal_layer_seen.wrapping_add(1) as f64
    }

    pub fn get_max_published_layer(&self) -> i32 {
        let inner = self.inner.read();

        inner.max_published_layer
    }

    pub fn get_layered_bitrate(&self) -> (Vec<i32>, Bitrates) {
        let inner = self.inner.read();

        self.get_layered_bitrate_locked(&inner)
    }

    #[allow(clippy::needless_range_loop, clippy::manual_memcpy)] // the loops as the original has them
    fn get_layered_bitrate_locked(
        &self,
        inner: &StreamTrackerManagerInner,
    ) -> (Vec<i32>, Bitrates) {
        let mut br = Bitrates::default();

        for (i, tracker) in inner.trackers.iter().enumerate() {
            if let Some(tracker) = tracker {
                let mut tls = vec![0i64; (buffer::DEFAULT_MAX_LAYER_TEMPORAL + 1) as usize];
                if inner.available_layers.contains(&(i as i32)) {
                    tls = tracker.bitrate_temporal_cumulative();
                }

                for j in 0..br[i].len() {
                    br[i][j] = tls[j];
                }
            }
        }

        // accumulate bitrates for SVC streams without dependency descriptor
        if self.video_layer_mode == livekit::video_layer::Mode::MultipleSpatialLayersPerStream
            && inner.dd_tracker.is_none()
        {
            for i in (1..br.len()).rev() {
                for j in (0..br[i].len()).rev() {
                    if br[i][j] != 0 {
                        for k in (0..i).rev() {
                            br[i][j] = br[i][j].wrapping_add(br[k][j]);
                        }
                    }
                }
            }
        }

        let available_layers = inner.available_layers.clone();

        (available_layers, br)
    }

    fn add_available_layer(&self, layer: i32) {
        let is_max_layer_change;
        {
            let mut inner = self.inner.write();
            if inner.available_layers.contains(&layer) {
                return;
            }

            inner.available_layers.push(layer);
            inner.available_layers.sort();

            // check if new layer is the max layer
            is_max_layer_change = inner.available_layers[inner.available_layers.len() - 1] == layer;

            debugw!(
                self.logger,
                "available layers changed - layer seen",
                "added" => layer,
                "availableLayers" => inner.available_layers,
            );
        }

        if let Some(listener) = self.get_listener() {
            listener.on_available_layers_changed();

            if is_max_layer_change {
                listener.on_max_available_layer_changed(layer);
            }
        }
    }

    fn remove_available_layer(&self, layer: i32) {
        let mut prev_max_layer = buffer::INVALID_LAYER_SPATIAL;
        let mut cur_max_layer = buffer::INVALID_LAYER_SPATIAL;
        {
            let mut inner = self.inner.write();
            if !inner.available_layers.is_empty() {
                prev_max_layer = inner.available_layers[inner.available_layers.len() - 1];
            }

            let mut new_layers: Vec<i32> =
                Vec::with_capacity((buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize);
            for &l in &inner.available_layers {
                if l != layer {
                    new_layers.push(l);
                }
            }
            new_layers.sort();
            inner.available_layers = new_layers;

            debugw!(
                self.logger,
                "available layers changed - layer gone",
                "removed" => layer,
                "availableLayers" => inner.available_layers,
            );

            if !inner.available_layers.is_empty() {
                cur_max_layer = inner.available_layers[inner.available_layers.len() - 1];
            }
        }

        // need to immediately switch off unavailable layers
        if let Some(listener) = self.get_listener() {
            listener.on_available_layers_changed();

            // if maxLayer was removed, send the new maxLayer
            if cur_max_layer != prev_max_layer {
                listener.on_max_available_layer_changed(cur_max_layer);
            }
        }
    }

    fn max_expected_layer_from_track_info(&self, force: bool) {
        let mut inner = self.inner.write();

        self.max_expected_layer_from_track_info_locked(&mut inner, force);
    }

    fn max_expected_layer_from_track_info_locked(
        &self,
        inner: &mut StreamTrackerManagerInner,
        force: bool,
    ) {
        let mut max_expected_layer = buffer::INVALID_LAYER_SPATIAL;
        if let Some(track_info) = &inner.track_info {
            for layer in buffer::get_video_layers_for_mime_type(self.mime_type, Some(track_info)) {
                if layer.spatial_layer > max_expected_layer {
                    max_expected_layer = layer.spatial_layer;
                }
            }
        }

        // when max expected is higher than current max, trackers are reset
        // which allows a layer start to be detected on initial packets from that higher layer,
        // so update max only on track info max being lower than current max
        if force || max_expected_layer < inner.max_expected_layer {
            inner.max_expected_layer = max_expected_layer;
        }
    }

    pub fn get_max_temporal_layer_seen(&self) -> i32 {
        let inner = self.inner.read();

        inner.max_temporal_layer_seen
    }

    fn update_max_temporal_layer_seen(&self, brs: Bitrates) {
        let mut max_temporal_layer_seen = buffer::INVALID_LAYER_TEMPORAL;
        'done: for t in (0..brs[0].len() as i32).rev() {
            for s in (0..brs.len() as i32).rev() {
                if brs[s as usize][t as usize] != 0 {
                    max_temporal_layer_seen = t;
                    break 'done;
                }
            }
        }

        {
            let mut inner = self.inner.write();
            if max_temporal_layer_seen <= inner.max_temporal_layer_seen {
                return;
            }

            inner.max_temporal_layer_seen = max_temporal_layer_seen;
        }

        if let Some(listener) = self.get_listener() {
            listener.on_max_temporal_layer_seen_changed(max_temporal_layer_seen);
        }
    }

    async fn bitrate_reporter(self: Arc<Self>) {
        // Go's `time.NewTicker(time.Second)`: the first tick after a second, and ticks that
        // were missed are dropped, not made up for
        let period = time::SECOND.to_std();
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.closed.cancelled() => {
                    return;
                }

                _ = ticker.tick() => {
                    let (al, brs) = self.get_layered_bitrate();
                    self.update_max_temporal_layer_seen(brs);

                    if let Some(listener) = self.get_listener() {
                        listener.on_bitrate_report(&al, brs);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "streamtrackermanager_test.rs"]
mod streamtrackermanager_test;
