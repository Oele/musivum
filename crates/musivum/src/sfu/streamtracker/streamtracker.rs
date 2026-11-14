// Ported from LiveKit, pkg/sfu/streamtracker/streamtracker.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `StreamStatus` is an enum and its `String()` is
// `Display`, without the text for a value that is neither of the two; the tracker is made as
// an `Arc` and keeps a `Weak` of itself, which is how `observe` hands the tracker to the
// worker it starts; the worker goroutine is a task and its two tickers are tokio intervals
// (`new_ticker`, at the end, is new); everything `lock` guards is in an inner struct, and
// that includes `params.StreamTrackerImpl`, which has `&mut self` methods and is called with
// the lock held: the worker, which asks it for the check interval without the lock in the
// original, takes the lock for that; a tracker without an implementation panics on its first
// use, as the original does on the nil interface; the two callbacks, fields that the
// original writes without the lock, have a lock of their own; `bitrate_temporal_cumulative`
// returns a `Vec`. The tests at the end are new: the original tests this file only through
// `streamtracker_packet_test.go`.

use std::fmt;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::warnw;

use super::{StreamStatusChange, StreamTrackerImpl, StreamTrackerWorker};

// ------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum StreamStatus {
    #[default]
    Stopped,
    Active,
}

impl fmt::Display for StreamStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            StreamStatus::Stopped => "stopped",
            StreamStatus::Active => "active",
        })
    }
}

// ------------------------------------------------------------

#[derive(Default)]
pub struct StreamTrackerParams {
    pub stream_tracker_impl: Option<Box<dyn StreamTrackerImpl>>,
    pub bitrate_report_interval: Duration,

    pub logger: Logger,
}

pub(super) type OnStatusChanged = Arc<dyn Fn(StreamStatus) + Send + Sync>;
pub(super) type OnBitrateAvailable = Arc<dyn Fn() + Send + Sync>;

pub(super) const NUM_TEMPORAL_LAYERS: usize = (buffer::DEFAULT_MAX_LAYER_TEMPORAL + 1) as usize;

pub struct StreamTracker {
    // without its `stream_tracker_impl`, which is in `inner`
    params: StreamTrackerParams,

    on_status_changed: Mutex<Option<OnStatusChanged>>,
    on_bitrate_available: Mutex<Option<OnBitrateAvailable>>,

    generation: atomic::Uint32,

    // what `lock sync.RWMutex` guards upstream
    pub(super) inner: RwLock<StreamTrackerInner>,

    // the tracker itself, for the worker that `observe` starts
    this: Weak<StreamTracker>,
}

pub(super) struct StreamTrackerInner {
    // `params.StreamTrackerImpl` of the original
    stream_tracker_impl: Option<Box<dyn StreamTrackerImpl>>,

    paused: bool,

    status: StreamStatus,
    last_notified_status: StreamStatus,

    last_bitrate_report: Time,
    bytes_for_bitrate: [i64; NUM_TEMPORAL_LAYERS],
    bitrate: [i64; NUM_TEMPORAL_LAYERS],

    is_stopped: bool,
}

impl StreamTrackerInner {
    fn stream_tracker_impl(&mut self) -> &mut dyn StreamTrackerImpl {
        self.stream_tracker_impl
            .as_deref_mut()
            .expect("the stream tracker has a StreamTrackerImpl")
    }

    pub(super) fn set_status_locked(&mut self, status: StreamStatus) {
        self.status = status;
    }
}

impl StreamTracker {
    pub fn new(mut params: StreamTrackerParams) -> Arc<StreamTracker> {
        let stream_tracker_impl = params.stream_tracker_impl.take();
        Arc::new_cyclic(|this| StreamTracker {
            params,
            on_status_changed: Mutex::new(None),
            on_bitrate_available: Mutex::new(None),
            generation: atomic::Uint32::new(0),
            inner: RwLock::new(StreamTrackerInner {
                stream_tracker_impl,
                paused: false,
                status: StreamStatus::Stopped,
                last_notified_status: StreamStatus::Stopped,
                last_bitrate_report: Time::ZERO,
                bytes_for_bitrate: [0; NUM_TEMPORAL_LAYERS],
                bitrate: [0; NUM_TEMPORAL_LAYERS],
                is_stopped: false,
            }),
            this: this.clone(),
        })
    }

    pub fn on_status_changed(&self, f: impl Fn(StreamStatus) + Send + Sync + 'static) {
        *self.on_status_changed.lock() = Some(Arc::new(f));
    }

    pub fn on_bitrate_available(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.on_bitrate_available.lock() = Some(Arc::new(f));
    }

    pub fn status(&self) -> StreamStatus {
        let inner = self.inner.read();

        inner.status
    }

    fn maybe_notify_status(&self) {
        let mut status = StreamStatus::default();
        let mut notify = false;
        {
            let mut inner = self.inner.write();
            if inner.status != inner.last_notified_status {
                notify = true;
                status = inner.status;
                inner.last_notified_status = inner.status;
            }
        }

        if notify {
            let on_status_changed = self.on_status_changed.lock().clone();
            if let Some(on_status_changed) = on_status_changed {
                on_status_changed(status);
            }
        }
    }

    pub fn start(&self) {
        let mut inner = self.inner.write();

        inner.stream_tracker_impl().start();
    }

    pub fn stop(&self) {
        let mut inner = self.inner.write();

        if inner.is_stopped {
            return;
        }
        inner.is_stopped = true;

        // bump generation to trigger exit of worker
        self.generation.inc();

        inner.stream_tracker_impl().stop();
    }

    pub fn reset(&self) {
        {
            let mut inner = self.inner.write();
            if inner.is_stopped {
                return;
            }

            self.reset_locked(&mut inner);
        }

        self.maybe_notify_status();
    }

    fn reset_locked(&self, inner: &mut StreamTrackerInner) {
        // bump generation to trigger exit of current worker
        self.generation.inc();

        inner.set_status_locked(StreamStatus::Stopped);

        inner.bytes_for_bitrate.fill(0);
        inner.bitrate.fill(0);

        inner.stream_tracker_impl().reset();
    }

    pub fn set_paused(&self, paused: bool) {
        {
            let mut inner = self.inner.write();
            inner.paused = paused;
            if !paused {
                self.reset_locked(&mut inner);
            } else {
                // bump generation to trigger exit of current worker
                self.generation.inc();

                inner.set_status_locked(StreamStatus::Stopped);
            }
        }

        self.maybe_notify_status();
    }

    pub fn observe(
        &self,
        temporal_layer: i32,
        pkt_size: usize,
        payload_size: usize,
        has_marker: bool,
        ts: u32,
        _dd: Option<&buffer::ExtDependencyDescriptor>,
    ) {
        let status_change;
        {
            let mut inner = self.inner.write();

            if inner.is_stopped || inner.paused || payload_size == 0 {
                return;
            }

            status_change = inner.stream_tracker_impl().observe(has_marker, ts);
            if status_change == StreamStatusChange::Active {
                inner.set_status_locked(StreamStatus::Active);
                inner.last_bitrate_report = time::now();

                let generation = self.generation.load();
                if let Some(s) = self.this.upgrade() {
                    time::spawn(s.worker(generation));
                }
            }

            if temporal_layer >= 0 && (temporal_layer as usize) < inner.bytes_for_bitrate.len() {
                let bytes_for_bitrate = &mut inner.bytes_for_bitrate[temporal_layer as usize];
                *bytes_for_bitrate = bytes_for_bitrate.wrapping_add(pkt_size as i64);
            } else if temporal_layer as i64 >= inner.bytes_for_bitrate.len() as i64 {
                warnw!(
                    self.params.logger,
                    "invalid temporal layer",
                    nil,
                    "temporalLayer" => temporal_layer,
                    "pktSize" => pkt_size,
                    "payloadSize" => payload_size,
                    "hasMarker" => has_marker,
                    "ts" => ts,
                );
            }
        }

        if status_change != StreamStatusChange::None {
            self.maybe_notify_status();
        }
    }

    // BitrateTemporalCumulative returns the current stream bitrate temporal layer accumulated with lower temporal layers.
    #[allow(clippy::needless_range_loop)] // the loops as the original has them
    pub fn bitrate_temporal_cumulative(&self) -> Vec<i64> {
        let inner = self.inner.read();

        // copy and process
        let mut brs = inner.bitrate.to_vec();

        for i in (1..brs.len()).rev() {
            if brs[i] != 0 {
                for j in (0..i).rev() {
                    brs[i] = brs[i].wrapping_add(brs[j]);
                }
            }
        }

        // clear higher layers
        for i in 0..brs.len() {
            if brs[i] == 0 {
                for j in i + 1..brs.len() {
                    brs[j] = 0;
                }
            }
        }

        brs
    }

    async fn worker(self: Arc<Self>, generation: u32) {
        let check_interval = self
            .inner
            .read()
            .stream_tracker_impl
            .as_deref()
            .expect("the stream tracker has a StreamTrackerImpl")
            .get_check_interval();
        let mut ticker = new_ticker(check_interval);

        let mut ticker_bitrate = new_ticker(self.params.bitrate_report_interval);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if generation != self.generation.load() {
                        return;
                    }
                    self.update_status();
                }

                _ = ticker_bitrate.tick() => {
                    if generation != self.generation.load() {
                        return;
                    }
                    self.bitrate_report();
                }
            }
        }
    }

    pub(super) fn update_status(&self) {
        {
            let mut inner = self.inner.write();
            match inner.stream_tracker_impl().check_status() {
                StreamStatusChange::Stopped => inner.set_status_locked(StreamStatus::Stopped),
                StreamStatusChange::Active => inner.set_status_locked(StreamStatus::Active),
                StreamStatusChange::None => {}
            }
        }

        self.maybe_notify_status();
    }

    fn bitrate_report(&self) {
        // run this even if paused to drain out bitrate if there are no packets coming in
        let mut bitrate_availability_changed = false;
        {
            let mut inner = self.inner.write();
            let now = time::now();
            let diff = now.sub(inner.last_bitrate_report);
            inner.last_bitrate_report = now;

            for i in 0..inner.bytes_for_bitrate.len() {
                let bitrate =
                    (inner.bytes_for_bitrate[i].wrapping_mul(8) as f64 / diff.seconds()) as i64;
                if (inner.bitrate[i] == 0 && bitrate > 0) || (inner.bitrate[i] > 0 && bitrate == 0)
                {
                    bitrate_availability_changed = true;
                }
                inner.bitrate[i] = bitrate;
                inner.bytes_for_bitrate[i] = 0;
            }
        }

        if bitrate_availability_changed {
            let on_bitrate_available = self.on_bitrate_available.lock().clone();
            if let Some(on_bitrate_available) = on_bitrate_available {
                on_bitrate_available();
            }
        }
    }
}

// The original's `*StreamTracker` is a `StreamTrackerWorker` by the methods above.
impl StreamTrackerWorker for StreamTracker {
    fn start(&self) {
        StreamTracker::start(self);
    }

    fn stop(&self) {
        StreamTracker::stop(self);
    }

    fn reset(&self) {
        StreamTracker::reset(self);
    }

    fn on_status_changed(&self, f: Box<dyn Fn(StreamStatus) + Send + Sync>) {
        StreamTracker::on_status_changed(self, f);
    }

    fn on_bitrate_available(&self, f: Box<dyn Fn() + Send + Sync>) {
        StreamTracker::on_bitrate_available(self, f);
    }

    fn status(&self) -> StreamStatus {
        StreamTracker::status(self)
    }

    fn bitrate_temporal_cumulative(&self) -> Vec<i64> {
        StreamTracker::bitrate_temporal_cumulative(self)
    }

    fn set_paused(&self, paused: bool) {
        StreamTracker::set_paused(self, paused);
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
        StreamTracker::observe(
            self,
            temporal_layer,
            pkt_size,
            payload_size,
            has_marker,
            ts,
            dd,
        );
    }
}

// Go's `time.NewTicker(d)` for the workers of this package: the first tick comes after `d`,
// and ticks that a slow worker missed are dropped, not made up for. Like the original it
// panics for an interval that is not positive; here that ends the worker, not the process.
pub(super) fn new_ticker(d: Duration) -> tokio::time::Interval {
    let period = d.to_std();
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker
}

#[cfg(test)]
mod tests {
    use super::super::{StreamTrackerPacket, StreamTrackerPacketConfig, StreamTrackerPacketParams};
    use super::*;
    use crate::protocol::logger;
    use crate::testutils;

    use StreamStatus::{Active, Stopped};

    // A tracker by packets that counts how often it is asked for its status.
    struct CountingStreamTrackerPacket {
        stp: StreamTrackerPacket,
        checks: Arc<atomic::Uint32>,
    }

    impl StreamTrackerImpl for CountingStreamTrackerPacket {
        fn start(&mut self) {
            self.stp.start();
        }

        fn stop(&mut self) {
            self.stp.stop();
        }

        fn reset(&mut self) {
            self.stp.reset();
        }

        fn get_check_interval(&self) -> Duration {
            self.stp.get_check_interval()
        }

        fn observe(&mut self, has_marker: bool, ts: u32) -> StreamStatusChange {
            self.stp.observe(has_marker, ts)
        }

        fn check_status(&mut self) -> StreamStatusChange {
            self.checks.inc();
            self.stp.check_status()
        }
    }

    // A tracker by packets that is active after one cycle with a packet, and the number of
    // times its status was checked.
    fn new_tracker(
        cycle_duration: Duration,
        bitrate_report_interval: Duration,
    ) -> (Arc<StreamTracker>, Arc<atomic::Uint32>) {
        let stp = StreamTrackerPacket::new(StreamTrackerPacketParams {
            config: StreamTrackerPacketConfig {
                samples_required: 1,
                cycles_required: 1,
                cycle_duration,
            },
            logger: logger::get_logger(),
        });
        let checks = Arc::new(atomic::Uint32::new(0));

        let tracker = StreamTracker::new(StreamTrackerParams {
            stream_tracker_impl: Some(Box::new(CountingStreamTrackerPacket {
                stp,
                checks: checks.clone(),
            })),
            bitrate_report_interval,
            logger: logger::get_logger(),
        });
        (tracker, checks)
    }

    // A bitrate over a time that is measured as it passes comes out a little under what it
    // is by the numbers (15999 for 16000 in the original, over a second): no bitrate where
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

    // New for musivum: the original tests this file only through the packet tracker's test,
    // which leaves out the bitrates and the worker. What is expected in these tests is what
    // the original does at the same steps (taken from running it).
    #[test]
    fn bitrates_add_up_over_the_temporal_layers_below_a_gap() {
        let (tracker, _) = new_tracker(time::SECOND, time::SECOND);
        for (bitrate, expected) in [
            ([100, 200, 300, 400], [100, 300, 600, 1000]),
            ([100, 0, 300, 0], [100, 0, 0, 0]),
            ([100, 0, 0, 400], [100, 0, 0, 0]),
            ([0, 200, 300, 400], [0, 0, 0, 0]),
            ([100, 200, 0, 400], [100, 300, 0, 0]),
            ([100, 200, 300, 0], [100, 300, 600, 0]),
            ([0, 0, 0, 0], [0, 0, 0, 0]),
        ] {
            tracker.inner.write().bitrate = bitrate;
            assert_eq!(
                tracker.bitrate_temporal_cumulative(),
                expected,
                "{bitrate:?}"
            );
        }
    }

    // New for musivum. The worker's tickers are an hour away: the test makes the reports.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_bitrate_report_counts_the_bytes_of_each_temporal_layer() {
        let (tracker, _) = new_tracker(time::HOUR, time::HOUR);
        let available = Arc::new(atomic::Uint32::new(0));
        tracker.on_bitrate_available({
            let available = available.clone();
            move || {
                available.inc();
            }
        });
        tracker.start();
        // a report over the last second
        let report = || {
            tracker.inner.write().last_bitrate_report = time::now().add(-time::SECOND);
            tracker.bitrate_report();
        };

        tracker.observe(0, 1000, 500, false, 0, None);
        tracker.observe(0, 1000, 500, false, 0, None);
        tracker.observe(2, 500, 100, false, 0, None);
        // not counted: no such layers, and no payload
        tracker.observe(4, 700, 100, false, 0, None);
        tracker.observe(-1, 700, 100, false, 0, None);
        tracker.observe(1, 700, 0, false, 0, None);
        assert_eq!(tracker.inner.read().bytes_for_bitrate, [2000, 0, 500, 0]);
        report();
        let bitrate = tracker.inner.read().bitrate;
        assert_bitrates(&bitrate, [16000, 0, 4000, 0], "layers 0 and 2");
        assert_eq!(tracker.inner.read().bytes_for_bitrate, [0, 0, 0, 0]);
        assert_bitrates(
            &tracker.bitrate_temporal_cumulative(),
            [16000, 0, 0, 0],
            "layers 0 and 2, cumulative",
        );
        assert_eq!(available.load(), 1);

        tracker.observe(0, 1000, 500, false, 0, None);
        tracker.observe(1, 2000, 500, false, 0, None);
        tracker.observe(2, 500, 100, false, 0, None);
        tracker.observe(3, 250, 100, false, 0, None);
        report();
        let bitrate = tracker.inner.read().bitrate;
        assert_bitrates(&bitrate, [8000, 16000, 4000, 2000], "all four layers");
        assert_bitrates(
            &tracker.bitrate_temporal_cumulative(),
            [8000, 24000, 28000, 30000],
            "all four layers, cumulative",
        );
        assert_eq!(available.load(), 2);

        tracker.observe(0, 1000, 500, false, 0, None);
        tracker.observe(1, 2000, 500, false, 0, None);
        report();
        let bitrate = tracker.inner.read().bitrate;
        assert_bitrates(&bitrate, [8000, 16000, 0, 0], "layers 0 and 1");
        assert_bitrates(
            &tracker.bitrate_temporal_cumulative(),
            [8000, 24000, 0, 0],
            "layers 0 and 1, cumulative",
        );
        assert_eq!(available.load(), 3);

        report();
        assert_eq!(tracker.inner.read().bitrate, [0, 0, 0, 0]);
        assert_eq!(tracker.bitrate_temporal_cumulative(), [0, 0, 0, 0]);
        assert_eq!(available.load(), 4);

        // nothing changed: nothing said
        report();
        assert_eq!(tracker.inner.read().bitrate, [0, 0, 0, 0]);
        assert_eq!(available.load(), 4);

        // a reset forgets the bytes
        tracker.observe(3, 250, 100, false, 0, None);
        assert_eq!(tracker.inner.read().bytes_for_bitrate, [0, 0, 0, 250]);
        tracker.reset();
        assert_eq!(tracker.inner.read().bytes_for_bitrate, [0, 0, 0, 0]);
        assert_eq!(tracker.inner.read().bitrate, [0, 0, 0, 0]);
        assert_eq!(tracker.status(), Stopped);

        tracker.stop();
    }

    // New for musivum. Every check is made when what is checked cannot change any more, so
    // that the test does not depend on how fast it runs.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_worker_checks_the_status_and_reports_the_bitrate() {
        let (tracker, _) = new_tracker(50 * time::MILLISECOND, 50 * time::MILLISECOND);
        // the statuses and the number of calls of the two callbacks
        let seen: Arc<Mutex<(Vec<StreamStatus>, u32)>> = Arc::default();
        tracker.on_status_changed({
            let seen = seen.clone();
            move |status| {
                seen.lock().0.push(status);
            }
        });
        tracker.on_bitrate_available({
            let seen = seen.clone();
            move || {
                seen.lock().1 += 1;
            }
        });
        tracker.start();
        // the callbacks so far, the status, the bitrates and the generation
        let snapshot = || {
            let (statuses, available) = seen.lock().clone();
            (
                statuses,
                available,
                tracker.status(),
                tracker.bitrate_temporal_cumulative(),
                tracker.generation.load(),
            )
        };
        let wait = |n: usize, a: u32| {
            let seen = &seen;
            testutils::with_timeout_async(
                move || {
                    let seen = seen.lock();
                    if seen.0.len() == n && seen.1 == a {
                        return String::new();
                    }
                    format!("statuses {:?} available {}", seen.0, seen.1)
                },
                None,
            )
        };

        // ignored: no payload
        tracker.observe(0, 100, 0, false, 0, None);
        assert_eq!(snapshot(), (vec![], 0, Stopped, vec![0, 0, 0, 0], 0));

        // a packet makes it active and starts the worker, which reports the bitrate and
        // then, with no more packets, sees it stop
        tracker.observe(1, 100, 50, false, 0, None);
        wait(2, 2).await;
        assert_eq!(
            snapshot(),
            (vec![Active, Stopped], 2, Stopped, vec![0, 0, 0, 0], 0)
        );

        // the same worker sees it start again
        tracker.observe(1, 100, 50, false, 0, None);
        wait(4, 4).await;
        assert_eq!(
            snapshot(),
            (
                vec![Active, Stopped, Active, Stopped],
                4,
                Stopped,
                vec![0, 0, 0, 0],
                0
            )
        );

        // paused: the worker ends, packets are ignored
        tracker.set_paused(true);
        tracker.observe(1, 100, 50, false, 0, None);
        assert_eq!(tracker.inner.read().bytes_for_bitrate, [0, 0, 0, 0]);
        time::sleep(150 * time::MILLISECOND).await;
        assert_eq!(
            snapshot(),
            (
                vec![Active, Stopped, Active, Stopped],
                4,
                Stopped,
                vec![0, 0, 0, 0],
                1
            )
        );

        // resumed: the first packet starts a new worker
        tracker.set_paused(false);
        assert_eq!(
            snapshot(),
            (
                vec![Active, Stopped, Active, Stopped],
                4,
                Stopped,
                vec![0, 0, 0, 0],
                2
            )
        );
        tracker.observe(1, 100, 50, false, 0, None);
        wait(6, 6).await;
        let all = vec![Active, Stopped, Active, Stopped, Active, Stopped];
        assert_eq!(snapshot(), (all.clone(), 6, Stopped, vec![0, 0, 0, 0], 2));

        // stopped: the worker ends, packets are ignored
        tracker.stop();
        tracker.observe(1, 100, 50, false, 0, None);
        assert_eq!(tracker.inner.read().bytes_for_bitrate, [0, 0, 0, 0]);
        time::sleep(150 * time::MILLISECOND).await;
        assert_eq!(snapshot(), (all.clone(), 6, Stopped, vec![0, 0, 0, 0], 3));

        // a reset after the stop does nothing; a pause still counts as one
        tracker.reset();
        assert_eq!(tracker.generation.load(), 3);
        tracker.set_paused(true);
        assert_eq!(snapshot(), (all, 6, Stopped, vec![0, 0, 0, 0], 4));
    }

    // New for musivum. Each of the two tickers ends the worker when the generation has
    // changed: one tracker whose bitrate ticker is an hour away, one whose status ticker is.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_worker_ends_at_the_tick_after_a_change_of_generation() {
        let (tracker, checks) = new_tracker(50 * time::MILLISECOND, time::HOUR);
        tracker.start();
        tracker.observe(0, 100, 50, false, 0, None);
        testutils::with_timeout_async(
            || {
                if checks.load() >= 2 {
                    return String::new();
                }
                format!("the worker checked the status {} times", checks.load())
            },
            None,
        )
        .await;
        tracker.set_paused(true);
        time::sleep(150 * time::MILLISECOND).await;
        let checks_when_ended = checks.load();
        time::sleep(150 * time::MILLISECOND).await;
        assert_eq!(checks.load(), checks_when_ended);

        let (tracker, _) = new_tracker(time::HOUR, 50 * time::MILLISECOND);
        let available = Arc::new(atomic::Uint32::new(0));
        tracker.on_bitrate_available({
            let available = available.clone();
            move || {
                available.inc();
            }
        });
        tracker.start();
        tracker.observe(0, 100, 50, false, 0, None);
        // the bitrate comes and, a report later, goes
        testutils::with_timeout_async(
            || {
                if available.load() == 2 {
                    return String::new();
                }
                format!("the bitrate changed {} times", available.load())
            },
            None,
        )
        .await;
        tracker.stop();
        time::sleep(150 * time::MILLISECOND).await;
        let last_report_when_ended = tracker.inner.read().last_bitrate_report;
        time::sleep(150 * time::MILLISECOND).await;
        assert_eq!(
            tracker.inner.read().last_bitrate_report,
            last_report_when_ended
        );
    }
}
