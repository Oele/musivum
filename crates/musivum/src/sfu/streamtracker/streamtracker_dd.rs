// Ported from LiveKit, pkg/sfu/streamtracker/streamtracker_dd.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the tracker is made as an `Arc`, and `set_paused`,
// `observe` and `layered_tracker` take it as that `Arc`: the first two to hand it to the
// worker they start, a task whose ticker is a tokio interval; everything `lock` guards is in
// an inner struct; `bitrate_temporal_cumulative` returns a copy where the original returns a
// slice of the tracker's own array, which its caller then reads without the lock; a
// descriptor that is absent in what `observe` is given is treated as no descriptor at all,
// and absent frame dependencies as no decode target indications, where the original would
// dereference a nil pointer (its parser never leaves either nil); the layered wrapper holds
// the tracker where the original embeds it, has the four methods of its own as upstream and
// passes the rest of `StreamTrackerWorker` on to the tracker.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::go::atomic;
use crate::go::time::{self, Time};
use crate::sfu::buffer;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::{debugw, errorw, warnw};

use super::streamtracker::{NUM_TEMPORAL_LAYERS, OnBitrateAvailable, OnStatusChanged, new_ticker};
use super::{StreamStatus, StreamTrackerParams, StreamTrackerWorker};

const NUM_SPATIAL_LAYERS: usize = (buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize;

pub struct StreamTrackerDependencyDescriptor {
    generation: atomic::Uint32,
    params: StreamTrackerParams,

    // what `lock sync.RWMutex` guards upstream
    inner: RwLock<StreamTrackerDependencyDescriptorInner>,
}

struct StreamTrackerDependencyDescriptorInner {
    paused: bool,
    max_spatial_layer: i32,
    max_temporal_layer: i32,

    on_status_changed: [Option<OnStatusChanged>; NUM_SPATIAL_LAYERS],
    on_bitrate_available: [Option<OnBitrateAvailable>; NUM_SPATIAL_LAYERS],

    last_bitrate_report: Time,
    bytes_for_bitrate: [[i64; NUM_TEMPORAL_LAYERS]; NUM_SPATIAL_LAYERS],
    bitrate: [[i64; NUM_TEMPORAL_LAYERS]; NUM_SPATIAL_LAYERS],

    is_stopped: bool,
}

impl StreamTrackerDependencyDescriptor {
    pub fn new(params: StreamTrackerParams) -> Arc<StreamTrackerDependencyDescriptor> {
        Arc::new(StreamTrackerDependencyDescriptor {
            generation: atomic::Uint32::new(0),
            params,
            inner: RwLock::new(StreamTrackerDependencyDescriptorInner {
                paused: false,
                max_spatial_layer: buffer::INVALID_LAYER_SPATIAL,
                max_temporal_layer: buffer::INVALID_LAYER_TEMPORAL,
                on_status_changed: Default::default(),
                on_bitrate_available: Default::default(),
                last_bitrate_report: Time::ZERO,
                bytes_for_bitrate: [[0; NUM_TEMPORAL_LAYERS]; NUM_SPATIAL_LAYERS],
                bitrate: [[0; NUM_TEMPORAL_LAYERS]; NUM_SPATIAL_LAYERS],
                is_stopped: false,
            }),
        })
    }

    pub fn start(&self) {}

    pub fn stop(&self) {
        let mut inner = self.inner.write();

        if inner.is_stopped {
            return;
        }
        inner.is_stopped = true;

        // bump generation to trigger exit of worker
        self.generation.inc();
    }

    pub fn on_status_changed(&self, layer: i32, f: impl Fn(StreamStatus) + Send + Sync + 'static) {
        let mut inner = self.inner.write();
        inner.on_status_changed[layer as usize] = Some(Arc::new(f));
    }

    pub fn on_bitrate_available(&self, layer: i32, f: impl Fn() + Send + Sync + 'static) {
        let mut inner = self.inner.write();
        inner.on_bitrate_available[layer as usize] = Some(Arc::new(f));
    }

    pub fn status(&self, layer: i32) -> StreamStatus {
        let inner = self.inner.read();

        if layer > inner.max_spatial_layer {
            return StreamStatus::Stopped;
        }

        StreamStatus::Active
    }

    pub fn bitrate_temporal_cumulative(&self, layer: i32) -> Vec<i64> {
        let inner = self.inner.read();

        if layer > inner.max_spatial_layer {
            let brs = vec![0i64; inner.bitrate[0].len()];
            return brs;
        }

        inner.bitrate[layer as usize].to_vec()
    }

    pub fn reset(&self) {}

    fn reset_locked(&self, inner: &mut StreamTrackerDependencyDescriptorInner) {
        // bump generation to trigger exit of current worker
        self.generation.inc();

        for bytes_for_bitrate in inner.bytes_for_bitrate.iter_mut() {
            bytes_for_bitrate.fill(0);
        }
        for bitrate in inner.bitrate.iter_mut() {
            bitrate.fill(0);
        }

        inner.max_spatial_layer = buffer::INVALID_LAYER_SPATIAL;
        inner.max_temporal_layer = buffer::INVALID_LAYER_TEMPORAL;
    }

    pub fn set_paused(self: &Arc<Self>, paused: bool) {
        let mut notify_fns: Vec<Option<OnStatusChanged>> = Vec::new();
        let mut notify_status = StreamStatus::default();
        {
            let mut inner = self.inner.write();
            if inner.paused == paused {
                return;
            }
            inner.paused = paused;

            if !paused {
                self.reset_locked(&mut inner);

                notify_status = StreamStatus::Stopped;
                notify_fns.extend(inner.on_status_changed.iter().cloned());
            } else {
                inner.last_bitrate_report = time::now();
                time::spawn(self.clone().worker(self.generation.inc()));
            }
        }

        for f in notify_fns.into_iter().flatten() {
            f(notify_status);
        }
    }

    pub fn observe(
        self: &Arc<Self>,
        _temporal_layer: i32,
        pkt_size: usize,
        payload_size: usize,
        _has_marker: bool,
        _ts: u32,
        dd_val: Option<&buffer::ExtDependencyDescriptor>,
    ) {
        let mut notify_fns: Vec<Option<OnStatusChanged>> = Vec::new();
        let mut notify_status = StreamStatus::default();
        {
            let mut inner = self.inner.write();

            if inner.is_stopped || inner.paused || payload_size == 0 {
                return;
            }
            let Some(dd_val) = dd_val else {
                return;
            };
            let Some(descriptor) = &dd_val.descriptor else {
                return;
            };

            if let Some(mask) = descriptor.active_decode_targets_bitmask
                && dd_val.active_decode_targets_updated
            {
                let mut max_spatial: i32 = 0;
                let mut max_temporal: i32 = 0;
                for dt in &dd_val.decode_targets {
                    if mask & shl_1(dt.target) != dd::DecodeTargetIndication::NotPresent as u32 {
                        if max_spatial < dt.layer.spatial {
                            max_spatial = dt.layer.spatial;
                        }
                        if max_temporal < dt.layer.temporal {
                            max_temporal = dt.layer.temporal;
                        }
                    }
                }
                if max_spatial > buffer::DEFAULT_MAX_LAYER_SPATIAL {
                    max_spatial = buffer::DEFAULT_MAX_LAYER_SPATIAL;
                    warnw!(
                        self.params.logger,
                        "max spatial layer exceeded",
                        nil,
                        "maxSpatial" => max_spatial,
                    );
                }
                if max_temporal > buffer::DEFAULT_MAX_LAYER_TEMPORAL {
                    max_temporal = buffer::DEFAULT_MAX_LAYER_TEMPORAL;
                    warnw!(
                        self.params.logger,
                        "max temporal layer exceeded",
                        nil,
                        "maxTemporal" => max_temporal,
                    );
                }

                debugw!(
                    self.params.logger,
                    "max layer changed",
                    "maxSpatial" => max_spatial,
                    "maxTemporal" => max_temporal,
                );
                let old_max_spatial = inner.max_spatial_layer;
                inner.max_spatial_layer = max_spatial;
                inner.max_temporal_layer = max_temporal;
                if old_max_spatial == -1 {
                    inner.last_bitrate_report = time::now();
                    time::spawn(self.clone().worker(self.generation.inc()));
                }

                if old_max_spatial > inner.max_spatial_layer {
                    notify_status = StreamStatus::Stopped;
                    for i in inner.max_spatial_layer + 1..=old_max_spatial {
                        notify_fns.push(inner.on_status_changed[i as usize].clone());
                    }
                } else if old_max_spatial < inner.max_spatial_layer {
                    notify_status = StreamStatus::Active;
                    for i in old_max_spatial + 1..=inner.max_spatial_layer {
                        notify_fns.push(inner.on_status_changed[i as usize].clone());
                    }
                }
            }

            let dtis: &[dd::DecodeTargetIndication] = match &descriptor.frame_dependencies {
                Some(frame_dependencies) => &frame_dependencies.decode_target_indications,
                None => &[],
            };

            for dt in &dd_val.decode_targets {
                if dtis.len() as i64 <= dt.target {
                    errorw!(
                        self.params.logger,
                        "len(dtis) less than target",
                        nil,
                        "target" => dt.target,
                        "dtis" => dtis,
                    );
                    continue;
                }
                // we are not dropping discardable frames now, so only ignore not present frames
                if dtis[dt.target as usize] == dd::DecodeTargetIndication::NotPresent {
                    continue;
                }

                if (dt.layer.spatial as i64) < inner.bytes_for_bitrate.len() as i64
                    && (dt.layer.temporal as i64)
                        < inner.bytes_for_bitrate[dt.layer.spatial as usize].len() as i64
                {
                    let bytes_for_bitrate = &mut inner.bytes_for_bitrate[dt.layer.spatial as usize]
                        [dt.layer.temporal as usize];
                    *bytes_for_bitrate = bytes_for_bitrate.wrapping_add(pkt_size as i64);
                }
            }
        }

        for f in notify_fns.into_iter().flatten() {
            f(notify_status);
        }
    }

    async fn worker(self: Arc<Self>, generation: u32) {
        let mut ticker_bitrate = new_ticker(self.params.bitrate_report_interval);

        loop {
            ticker_bitrate.tick().await;
            if generation != self.generation.load() {
                return;
            }
            self.bitrate_report();
        }
    }

    fn bitrate_report(&self) {
        // run this even if paused to drain out bitrate if there are no packets coming in
        let mut available_changed_fns: Vec<OnBitrateAvailable> = Vec::new();
        {
            let mut guard = self.inner.write();
            let inner = &mut *guard;
            let now = time::now();
            let diff = now.sub(inner.last_bitrate_report);
            inner.last_bitrate_report = now;

            for spatial in 0..inner.bytes_for_bitrate.len() {
                let bytes_for_bitrate = &mut inner.bytes_for_bitrate[spatial];
                let mut bitrate_availability_changed = false;
                let bitrates = &mut inner.bitrate[spatial];
                for i in 0..bytes_for_bitrate.len() {
                    let bitrate =
                        (bytes_for_bitrate[i].wrapping_mul(8) as f64 / diff.seconds()) as i64;
                    if (bitrates[i] == 0 && bitrate > 0) || (bitrates[i] > 0 && bitrate == 0) {
                        bitrate_availability_changed = true;
                    }
                    bitrates[i] = bitrate;
                    bytes_for_bitrate[i] = 0;
                }

                if bitrate_availability_changed
                    && let Some(on_bitrate_available) = &inner.on_bitrate_available[spatial]
                {
                    available_changed_fns.push(on_bitrate_available.clone());
                }
            }
        }

        for f in available_changed_fns {
            f();
        }
    }

    pub fn layered_tracker(
        self: &Arc<Self>,
        layer: i32,
    ) -> Arc<StreamTrackerDependencyDescriptorLayered> {
        Arc::new(StreamTrackerDependencyDescriptorLayered {
            stream_tracker_dependency_descriptor: self.clone(),
            layer,
        })
    }
}

// Go's `1 << count` on a `uint32`: zero for a count of 32 or more; a negative count is a panic,
// there and here.
fn shl_1(count: i64) -> u32 {
    assert!(count >= 0, "negative shift amount");
    u32::try_from(count)
        .ok()
        .and_then(|count| 1u32.checked_shl(count))
        .unwrap_or(0)
}

// ----------------------------
// Layered wrapper for StreamTrackerWorker
pub struct StreamTrackerDependencyDescriptorLayered {
    stream_tracker_dependency_descriptor: Arc<StreamTrackerDependencyDescriptor>,
    layer: i32,
}

impl StreamTrackerDependencyDescriptorLayered {
    pub fn on_status_changed(&self, f: impl Fn(StreamStatus) + Send + Sync + 'static) {
        self.stream_tracker_dependency_descriptor
            .on_status_changed(self.layer, f);
    }

    pub fn on_bitrate_available(&self, f: impl Fn() + Send + Sync + 'static) {
        self.stream_tracker_dependency_descriptor
            .on_bitrate_available(self.layer, f);
    }

    pub fn status(&self) -> StreamStatus {
        self.stream_tracker_dependency_descriptor.status(self.layer)
    }

    pub fn bitrate_temporal_cumulative(&self) -> Vec<i64> {
        self.stream_tracker_dependency_descriptor
            .bitrate_temporal_cumulative(self.layer)
    }
}

// The four methods above, and the tracker's own for the rest: what the original gets by
// embedding the tracker.
impl StreamTrackerWorker for StreamTrackerDependencyDescriptorLayered {
    fn start(&self) {
        self.stream_tracker_dependency_descriptor.start();
    }

    fn stop(&self) {
        self.stream_tracker_dependency_descriptor.stop();
    }

    fn reset(&self) {
        self.stream_tracker_dependency_descriptor.reset();
    }

    fn on_status_changed(&self, f: Box<dyn Fn(StreamStatus) + Send + Sync>) {
        StreamTrackerDependencyDescriptorLayered::on_status_changed(self, f);
    }

    fn on_bitrate_available(&self, f: Box<dyn Fn() + Send + Sync>) {
        StreamTrackerDependencyDescriptorLayered::on_bitrate_available(self, f);
    }

    fn status(&self) -> StreamStatus {
        StreamTrackerDependencyDescriptorLayered::status(self)
    }

    fn bitrate_temporal_cumulative(&self) -> Vec<i64> {
        StreamTrackerDependencyDescriptorLayered::bitrate_temporal_cumulative(self)
    }

    fn set_paused(&self, paused: bool) {
        self.stream_tracker_dependency_descriptor.set_paused(paused);
    }

    fn observe(
        &self,
        temporal_layer: i32,
        pkt_size: usize,
        payload_size: usize,
        has_marker: bool,
        ts: u32,
        dd: Option<&buffer::ExtDependencyDescriptor>,
    ) {
        self.stream_tracker_dependency_descriptor.observe(
            temporal_layer,
            pkt_size,
            payload_size,
            has_marker,
            ts,
            dd,
        );
    }
}

#[cfg(test)]
#[path = "streamtracker_dd_test.rs"]
mod streamtracker_dd_test;

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;

    use super::streamtracker_dd_test::create_descriptor_dependency_for_targets;
    use super::*;
    use crate::protocol::logger;
    use crate::testutils;

    use StreamStatus::{Active, Stopped};

    // A bitrate over a time that is measured as it passes comes out a little under what it
    // is by the numbers (11999 for 12000 in the original, over a second): no bitrate where
    // none is expected, else more than half of the expected one and not more than it.
    #[track_caller]
    fn assert_bitrates(actual: &[i64], expected: [i64; NUM_TEMPORAL_LAYERS], step: &str) {
        assert_eq!(actual.len(), expected.len(), "{step}");
        for (actual, expected) in actual.iter().zip(expected) {
            assert!(
                (expected == 0 && *actual == 0) || (expected / 2 < *actual && *actual <= expected),
                "{step}: got {actual} where {expected} is expected",
            );
        }
    }

    // The descriptor of the original's test for these layers, all of its decode targets
    // active and required, after `change`.
    fn descriptor(
        max_spatial: i64,
        max_temporal: i64,
        change: impl FnOnce(&mut buffer::ExtDependencyDescriptor),
    ) -> buffer::ExtDependencyDescriptor {
        let mut ext_dd = create_descriptor_dependency_for_targets(max_spatial, max_temporal);
        change(&mut ext_dd);
        ext_dd
    }

    fn dtis(ext_dd: &mut buffer::ExtDependencyDescriptor) -> &mut Vec<dd::DecodeTargetIndication> {
        let descriptor = ext_dd.descriptor.as_mut().unwrap();
        let frame_dependencies = descriptor.frame_dependencies.as_mut().unwrap();
        &mut Arc::make_mut(frame_dependencies).decode_target_indications
    }

    // New for musivum: the original's test is about the statuses of the layers only. What is
    // expected here is what the original does at the same steps (taken from running it), but
    // for the last step, which the original does not survive. The worker's ticker is an hour
    // away: the test makes the bitrate reports.
    #[tokio::test(flavor = "multi_thread")]
    async fn layers_and_bitrates_from_the_descriptors() {
        let dd_tracker = StreamTrackerDependencyDescriptor::new(StreamTrackerParams {
            bitrate_report_interval: time::HOUR,
            logger: logger::get_logger(),
            ..Default::default()
        });
        // what the callbacks of the layers were called with, in order
        let events: Arc<Mutex<Vec<String>>> = Arc::default();
        let mut layered_trackers = Vec::new();
        for layer in 0..=buffer::DEFAULT_MAX_LAYER_SPATIAL {
            let layered_tracker = dd_tracker.layered_tracker(layer);
            layered_tracker.on_status_changed({
                let events = events.clone();
                move |status| events.lock().push(format!("{layer}:{status}"))
            });
            layered_tracker.on_bitrate_available({
                let events = events.clone();
                move || events.lock().push(format!("{layer}:bitrate"))
            });
            layered_tracker.start();
            layered_trackers.push(layered_tracker);
        }
        let observe =
            |pkt_size: usize, payload_size: usize, ext_dd: buffer::ExtDependencyDescriptor| {
                dd_tracker.observe(0, pkt_size, payload_size, false, 0, Some(&ext_dd));
            };
        // a report over the last second
        let report = || {
            dd_tracker.inner.write().last_bitrate_report = time::now().add(-time::SECOND);
            dd_tracker.bitrate_report();
        };
        // the calls of the callbacks since the last check, the statuses of the three layers,
        // the highest spatial and temporal layer, and the generation
        let check = |step: &str,
                     expected_events: &[&str],
                     expected_statuses: [StreamStatus; 3],
                     expected_max: (i32, i32),
                     expected_generation: u32| {
            assert_eq!(
                std::mem::take(&mut *events.lock()),
                expected_events,
                "{step}"
            );
            let statuses: Vec<StreamStatus> = layered_trackers
                .iter()
                .map(|layered_tracker| layered_tracker.status())
                .collect();
            assert_eq!(statuses, expected_statuses, "{step}");
            let inner = dd_tracker.inner.read();
            assert_eq!(
                (inner.max_spatial_layer, inner.max_temporal_layer),
                expected_max,
                "{step}"
            );
            assert_eq!(dd_tracker.generation.load(), expected_generation, "{step}");
        };
        let bytes = || dd_tracker.inner.read().bytes_for_bitrate;
        let check_bitrates = |step: &str, expected: [[i64; NUM_TEMPORAL_LAYERS]; 3]| {
            for (layered_tracker, expected) in layered_trackers.iter().zip(expected) {
                assert_bitrates(
                    &layered_tracker.bitrate_temporal_cumulative(),
                    expected,
                    step,
                );
            }
        };
        const NO_BYTES: [[i64; NUM_TEMPORAL_LAYERS]; 3] = [[0; NUM_TEMPORAL_LAYERS]; 3];

        check("new", &[], [Stopped, Stopped, Stopped], (-1, -1), 0);
        assert_eq!(bytes(), NO_BYTES);
        check_bitrates("new", NO_BYTES);

        observe(1000, 0, descriptor(1, 1, |_| {}));
        check("no payload", &[], [Stopped, Stopped, Stopped], (-1, -1), 0);
        assert_eq!(bytes(), NO_BYTES);

        // the first descriptor with active decode targets starts the worker
        observe(1000, 500, descriptor(1, 1, |_| {}));
        check(
            "two by two",
            &["0:active", "1:active"],
            [Active, Active, Stopped],
            (1, 1),
            1,
        );
        assert_eq!(
            bytes(),
            [[1000, 1000, 0, 0], [1000, 1000, 0, 0], [0, 0, 0, 0]]
        );

        // a frame that is not in decode target 3 (spatial layer 1, temporal layer 1)
        observe(
            500,
            500,
            descriptor(1, 1, |ext_dd| {
                dtis(ext_dd)[3] = dd::DecodeTargetIndication::NotPresent;
            }),
        );
        check(
            "not present in target 3",
            &[],
            [Active, Active, Stopped],
            (1, 1),
            1,
        );
        assert_eq!(
            bytes(),
            [[1500, 1500, 0, 0], [1500, 1000, 0, 0], [0, 0, 0, 0]]
        );
        check_bitrates("before the first report", NO_BYTES);

        report();
        check(
            "report",
            &["0:bitrate", "1:bitrate"],
            [Active, Active, Stopped],
            (1, 1),
            1,
        );
        let two_by_two = [[12000, 12000, 0, 0], [12000, 8000, 0, 0], [0, 0, 0, 0]];
        check_bitrates("report", two_by_two);
        assert_eq!(bytes(), NO_BYTES);

        // the active decode targets are not said to have changed: the layers stay as they
        // are, the bytes are counted
        observe(
            100,
            500,
            descriptor(2, 2, |ext_dd| {
                ext_dd.active_decode_targets_updated = false;
            }),
        );
        check(
            "three by three, not updated",
            &[],
            [Active, Active, Stopped],
            (1, 1),
            1,
        );
        assert_eq!(bytes(), [[100, 100, 100, 0]; 3]);

        // the same without a mask
        observe(
            100,
            500,
            descriptor(2, 2, |ext_dd| {
                ext_dd
                    .descriptor
                    .as_mut()
                    .unwrap()
                    .active_decode_targets_bitmask = None;
            }),
        );
        check(
            "three by three, no mask",
            &[],
            [Active, Active, Stopped],
            (1, 1),
            1,
        );
        assert_eq!(bytes(), [[200, 200, 200, 0]; 3]);

        // only the decode targets of spatial layer 0 are active: layer 1 has stopped, and
        // has no bitrates to show
        let with_mask = |mask: u32| {
            descriptor(2, 2, |ext_dd| {
                ext_dd
                    .descriptor
                    .as_mut()
                    .unwrap()
                    .active_decode_targets_bitmask = Some(mask);
            })
        };
        observe(100, 500, with_mask(0b000000111));
        check(
            "three by three, mask 0b111",
            &["1:stopped"],
            [Active, Stopped, Stopped],
            (0, 2),
            1,
        );
        check_bitrates(
            "three by three, mask 0b111",
            [[12000, 12000, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]],
        );
        assert_eq!(bytes(), [[300, 300, 300, 0]; 3]);

        // no decode target is active: layer 0 stays
        observe(100, 500, with_mask(0));
        check(
            "three by three, mask 0",
            &[],
            [Active, Stopped, Stopped],
            (0, 0),
            1,
        );
        assert_eq!(bytes(), [[400, 400, 400, 0]; 3]);

        // fewer indications than decode targets: the first four are counted
        observe(
            100,
            500,
            descriptor(2, 2, |ext_dd| {
                dtis(ext_dd).truncate(4);
            }),
        );
        check(
            "three by three, 4 indications",
            &["1:active", "2:active"],
            [Active, Active, Active],
            (2, 2),
            1,
        );
        check_bitrates("three by three, 4 indications", two_by_two);
        assert_eq!(
            bytes(),
            [[500, 500, 500, 0], [500, 400, 400, 0], [400, 400, 400, 0]]
        );

        // more layers than there can be
        observe(100, 500, descriptor(3, 4, |_| {}));
        check("four by five", &[], [Active, Active, Active], (2, 3), 1);
        assert_eq!(
            bytes(),
            [
                [600, 600, 600, 100],
                [600, 500, 500, 100],
                [500, 500, 500, 100]
            ]
        );

        report();
        check(
            "report",
            &["0:bitrate", "1:bitrate", "2:bitrate"],
            [Active, Active, Active],
            (2, 3),
            1,
        );
        check_bitrates(
            "report",
            [
                [4800, 4800, 4800, 800],
                [4800, 4000, 4000, 800],
                [4000, 4000, 4000, 800],
            ],
        );
        assert_eq!(bytes(), NO_BYTES);

        report();
        check(
            "report, nothing new",
            &["0:bitrate", "1:bitrate", "2:bitrate"],
            [Active, Active, Active],
            (2, 3),
            1,
        );
        check_bitrates("report, nothing new", NO_BYTES);

        // a pause says nothing, leaves the layers and starts a worker of its own
        dd_tracker.set_paused(true);
        check("paused", &[], [Active, Active, Active], (2, 3), 2);
        observe(1000, 500, descriptor(1, 1, |_| {}));
        check("paused, packet", &[], [Active, Active, Active], (2, 3), 2);
        assert_eq!(bytes(), NO_BYTES);
        dd_tracker.set_paused(true);
        check("paused again", &[], [Active, Active, Active], (2, 3), 2);

        // it is the end of the pause that stops every layer
        dd_tracker.set_paused(false);
        check(
            "resumed",
            &["0:stopped", "1:stopped", "2:stopped"],
            [Stopped, Stopped, Stopped],
            (-1, -1),
            3,
        );
        observe(1000, 500, descriptor(0, 0, |_| {}));
        check(
            "one by one",
            &["0:active"],
            [Active, Stopped, Stopped],
            (0, 0),
            4,
        );
        assert_eq!(bytes(), [[1000, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]);

        dd_tracker.stop();
        observe(1000, 500, descriptor(1, 1, |_| {}));
        check(
            "stopped, packet",
            &[],
            [Active, Stopped, Stopped],
            (0, 0),
            5,
        );
        assert_eq!(bytes(), [[1000, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]);
        dd_tracker.stop();
        check("stopped again", &[], [Active, Stopped, Stopped], (0, 0), 5);
    }

    // New for musivum, with the worker at work. Every check is made when what is checked
    // cannot change any more, or waits for what is bound to come.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_worker_reports_the_bitrate_until_the_generation_changes() {
        let dd_tracker = StreamTrackerDependencyDescriptor::new(StreamTrackerParams {
            bitrate_report_interval: 50 * time::MILLISECOND,
            logger: logger::get_logger(),
            ..Default::default()
        });
        let events: Arc<Mutex<Vec<String>>> = Arc::default();
        let layered_tracker = dd_tracker.layered_tracker(0);
        layered_tracker.on_status_changed({
            let events = events.clone();
            move |status| events.lock().push(format!("0:{status}"))
        });
        layered_tracker.on_bitrate_available({
            let events = events.clone();
            move || events.lock().push("0:bitrate".to_string())
        });
        let last_report = || dd_tracker.inner.read().last_bitrate_report;

        // the bitrate comes and, a report later, goes
        dd_tracker.observe(0, 1000, 500, false, 0, Some(&descriptor(0, 0, |_| {})));
        testutils::with_timeout_async(
            || {
                let events = events.lock();
                if events.len() == 3 {
                    return String::new();
                }
                format!("events {events:?}")
            },
            None,
        )
        .await;
        let all = ["0:active", "0:bitrate", "0:bitrate"];
        assert_eq!(*events.lock(), all);
        assert_eq!(dd_tracker.generation.load(), 1);

        // the worker that a pause starts goes on reporting
        dd_tracker.set_paused(true);
        dd_tracker.observe(0, 1000, 500, false, 0, Some(&descriptor(0, 0, |_| {})));
        let last_report_when_paused = last_report();
        testutils::with_timeout_async(
            || {
                if last_report().after(last_report_when_paused) {
                    return String::new();
                }
                "no bitrate report while paused".to_string()
            },
            None,
        )
        .await;
        assert_eq!(*events.lock(), all);
        assert_eq!(dd_tracker.generation.load(), 2);

        // and ends at the tick after the stop
        dd_tracker.stop();
        time::sleep(150 * time::MILLISECOND).await;
        let last_report_when_ended = last_report();
        time::sleep(150 * time::MILLISECOND).await;
        assert_eq!(last_report(), last_report_when_ended);
        assert_eq!(*events.lock(), all);
        assert_eq!(dd_tracker.generation.load(), 3);
    }

    // New for musivum, and for what is different here: the original dereferences the
    // descriptor and its frame dependencies without looking, and its parser sets both.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_descriptor_or_frame_dependencies_that_are_absent() {
        let dd_tracker = StreamTrackerDependencyDescriptor::new(StreamTrackerParams {
            bitrate_report_interval: time::HOUR,
            logger: logger::get_logger(),
            ..Default::default()
        });
        const NO_BYTES: [[i64; NUM_TEMPORAL_LAYERS]; 3] = [[0; NUM_TEMPORAL_LAYERS]; 3];

        // no descriptor: as if nothing had come
        let ext_dd = descriptor(1, 1, |ext_dd| ext_dd.descriptor = None);
        dd_tracker.observe(0, 1000, 500, false, 0, Some(&ext_dd));
        assert_eq!(dd_tracker.status(0), Stopped);
        assert_eq!(dd_tracker.inner.read().bytes_for_bitrate, NO_BYTES);
        assert_eq!(dd_tracker.generation.load(), 0);

        // no frame dependencies: the layers of the mask, and no bytes
        let ext_dd = descriptor(1, 1, |ext_dd| {
            ext_dd.descriptor.as_mut().unwrap().frame_dependencies = None;
        });
        dd_tracker.observe(0, 1000, 500, false, 0, Some(&ext_dd));
        assert_eq!(dd_tracker.status(1), Active);
        assert_eq!(dd_tracker.status(2), Stopped);
        assert_eq!(dd_tracker.inner.read().bytes_for_bitrate, NO_BYTES);
        assert_eq!(dd_tracker.generation.load(), 1);

        dd_tracker.stop();
    }
}
