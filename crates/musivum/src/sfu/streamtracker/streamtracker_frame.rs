// Ported from LiveKit, pkg/sfu/streamtracker/streamtracker_frame.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NewStreamTrackerFrame` is `StreamTrackerFrame::new`
// and gives the type itself (boxed by whoever wants a `dyn StreamTrackerImpl`); the methods
// that are not the trait's (`reset_fps_calculator` and the three `update_...`) stand
// together after the trait's; the two maps of defaults are built on first use; the YAML tag
// of `StreamTrackerFrameConfig` is not carried over (the field has the name of the key).
// The tests at the end are new: the original has none.

use std::collections::HashMap;
use std::sync::LazyLock;

use crate::debugw;
use crate::go::time::{self, Duration, Time};
use crate::protocol::logger::Logger;

use super::{StreamStatusChange, StreamTrackerImpl};

const CHECK_INTERVAL: Duration = Duration::from_millis(500);
const STATUS_CHECK_TOLERANCE: f64 = 0.98;
const FRAME_RATE_RESOLUTION: f64 = 0.01; // 1 frame every 100 seconds
const FRAME_RATE_INCREASE_FACTOR: f64 = 0.6; // slow increase
const FRAME_RATE_DECREASE_FACTOR: f64 = 0.9; // fast decrease

// -------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct StreamTrackerFrameConfig {
    pub min_fps: f64,
}

pub static DEFAULT_STREAM_TRACKER_FRAME_CONFIG_VIDEO: LazyLock<
    HashMap<i32, StreamTrackerFrameConfig>,
> = LazyLock::new(|| {
    HashMap::from([
        (0, StreamTrackerFrameConfig { min_fps: 5.0 }),
        (1, StreamTrackerFrameConfig { min_fps: 5.0 }),
        (2, StreamTrackerFrameConfig { min_fps: 5.0 }),
    ])
});

pub static DEFAULT_STREAM_TRACKER_FRAME_CONFIG_SCREENSHARE: LazyLock<
    HashMap<i32, StreamTrackerFrameConfig>,
> = LazyLock::new(|| {
    HashMap::from([
        (0, StreamTrackerFrameConfig { min_fps: 0.5 }),
        (1, StreamTrackerFrameConfig { min_fps: 0.5 }),
        (2, StreamTrackerFrameConfig { min_fps: 0.5 }),
    ])
});

// -------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct StreamTrackerFrameParams {
    pub config: StreamTrackerFrameConfig,
    pub clock_rate: u32,
    pub logger: Logger,
}

pub struct StreamTrackerFrame {
    params: StreamTrackerFrameParams,

    initialized: bool,

    ts_initialized: bool,
    oldest_ts: u32,
    newest_ts: u32,
    num_frames: i64,

    estimated_frame_rate: f64,
    eval_interval: Duration,
    last_status_check_at: Time,
}

impl StreamTrackerFrame {
    pub fn new(params: StreamTrackerFrameParams) -> Self {
        let mut s = StreamTrackerFrame {
            params,
            initialized: false,
            ts_initialized: false,
            oldest_ts: 0,
            newest_ts: 0,
            num_frames: 0,
            estimated_frame_rate: 0.0,
            eval_interval: Duration::ZERO,
            last_status_check_at: Time::ZERO,
        };
        s.reset();
        s
    }
}

impl StreamTrackerImpl for StreamTrackerFrame {
    fn start(&mut self) {}

    fn stop(&mut self) {}

    fn reset(&mut self) {
        self.initialized = false;

        self.reset_fps_calculator();

        self.last_status_check_at = Time::ZERO;
    }

    fn get_check_interval(&self) -> Duration {
        CHECK_INTERVAL
    }

    fn observe(&mut self, has_marker: bool, ts: u32) -> StreamStatusChange {
        if has_marker {
            if !self.ts_initialized {
                self.ts_initialized = true;
                self.oldest_ts = ts;
                self.newest_ts = ts;
                self.num_frames = 1;
            } else {
                let mut diff = ts.wrapping_sub(self.oldest_ts);
                if diff > (1 << 31) {
                    self.oldest_ts = ts;
                }
                diff = ts.wrapping_sub(self.newest_ts);
                if diff < (1 << 31) {
                    self.newest_ts = ts;
                }
                self.num_frames = self.num_frames.wrapping_add(1);
            }
        }

        // When starting up, check for first packet and declare active.
        // Happens under following conditions
        //   1. Start up
        //   2. Unmute (stream restarting)
        //   3. Layer starting after dynacast pause
        if !self.initialized {
            self.initialized = true;
            self.last_status_check_at = time::now();
            return StreamStatusChange::Active;
        }

        StreamStatusChange::None
    }

    fn check_status(&mut self) -> StreamStatusChange {
        if !self.initialized {
            // should not be getting called when not initialized, but be safe
            return StreamStatusChange::None;
        }

        if !self.update_status_check_time() {
            return StreamStatusChange::None;
        }

        if self.update_estimated_frame_rate() == 0.0 {
            // when stream is stopped, reset FPS calculator to ensure re-start is not done until at least two frames are available,
            // i. e. enough frames available to be able to calculate FPS
            self.reset_fps_calculator();
            return StreamStatusChange::Stopped;
        }

        StreamStatusChange::Active
    }
}

impl StreamTrackerFrame {
    fn reset_fps_calculator(&mut self) {
        self.ts_initialized = false;
        self.oldest_ts = 0;
        self.newest_ts = 0;
        self.num_frames = 0;

        self.estimated_frame_rate = 0.0;
        self.update_eval_interval();
    }

    fn update_status_check_time(&mut self) -> bool {
        // check only at intervals based on estimated frame rate
        if self.last_status_check_at.is_zero() {
            self.last_status_check_at = time::now();
        }
        if time::since(self.last_status_check_at)
            < Duration((STATUS_CHECK_TOLERANCE * self.eval_interval.0 as f64) as i64)
        {
            return false;
        }
        self.last_status_check_at = time::now();
        true
    }

    fn update_estimated_frame_rate(&mut self) -> f64 {
        let diff = self.newest_ts.wrapping_sub(self.oldest_ts);
        if diff == 0 || self.num_frames < 2 {
            return 0.0;
        }

        let frame_rate = round_frame_rate(
            self.params.clock_rate as f64 / diff as f64 * (self.num_frames - 1) as f64,
        );

        // reset for next evaluation interval
        self.oldest_ts = self.newest_ts;
        self.num_frames = 1;

        let mut factor = 1.0;
        if self.estimated_frame_rate < frame_rate {
            // slow increase, prevents shortening eval interval too quickly on frame rate going up
            factor = FRAME_RATE_INCREASE_FACTOR;
        } else if self.estimated_frame_rate > frame_rate {
            // fast decrease, prevents declaring stream stop too quickly on frame rate going down
            factor = FRAME_RATE_DECREASE_FACTOR;
        }

        let estimated_frame_rate =
            round_frame_rate(frame_rate * factor + self.estimated_frame_rate * (1.0 - factor));
        if self.estimated_frame_rate != estimated_frame_rate {
            self.estimated_frame_rate = estimated_frame_rate;
            self.update_eval_interval();
            debugw!(
                self.params.logger,
                "updating estimated frame rate",
                "estimatedFPS" => estimated_frame_rate,
                "evalInterval" => self.eval_interval,
            );
        }

        frame_rate
    }

    fn update_eval_interval(&mut self) {
        // STREAM-TRACKER-FRAME-TODO: This will run into challenges for frame rate falling steeply, How to address that?
        // Maybe, look at some referential rules (between layers) for possibilities to solve it. Currently, this is addressed
        // by setting a source aware min FPS to ensure evaluation window is long enough to avoid declaring stop too quickly.
        self.eval_interval = CHECK_INTERVAL;
        if self.estimated_frame_rate > 0.0 {
            let estimated_frame_rate_interval =
                Duration((time::SECOND.0 as f64 / self.estimated_frame_rate) as i64);
            if estimated_frame_rate_interval > self.eval_interval {
                self.eval_interval = estimated_frame_rate_interval;
            }
        }
        if self.params.config.min_fps > 0.0 {
            let min_fps_interval =
                Duration((time::SECOND.0 as f64 / self.params.config.min_fps) as i64);
            if min_fps_interval > self.eval_interval {
                self.eval_interval = min_fps_interval;
            }
        }
    }
}

// ------------------------------------------------------------------------------

fn round_frame_rate(frame_rate: f64) -> f64 {
    (frame_rate / FRAME_RATE_RESOLUTION).round() * FRAME_RATE_RESOLUTION
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::logger;

    const NONE: StreamStatusChange = StreamStatusChange::None;
    const STOPPED: StreamStatusChange = StreamStatusChange::Stopped;
    const ACTIVE: StreamStatusChange = StreamStatusChange::Active;

    fn new_tracker(min_fps: f64, clock_rate: u32) -> StreamTrackerFrame {
        StreamTrackerFrame::new(StreamTrackerFrameParams {
            config: StreamTrackerFrameConfig { min_fps },
            clock_rate,
            logger: logger::get_logger(),
        })
    }

    // The change that a step returned and what the tracker holds after it: the estimated
    // frame rate, the evaluation interval in nanoseconds, whether it has a timestamp, the
    // oldest and the newest timestamp, the number of frames, whether it has seen a packet.
    type Seen = (StreamStatusChange, f64, i64, bool, u32, u32, i64, bool);

    #[track_caller]
    fn expect(change: StreamStatusChange, s: &StreamTrackerFrame, expected: Seen, step: &str) {
        let seen: Seen = (
            change,
            s.estimated_frame_rate,
            s.eval_interval.nanoseconds(),
            s.ts_initialized,
            s.oldest_ts,
            s.newest_ts,
            s.num_frames,
            s.initialized,
        );
        assert_eq!(seen, expected, "{step}");
    }

    // The last check was long ago: the next one is due.
    fn past(s: &mut StreamTrackerFrame) {
        s.last_status_check_at = time::now().add(-time::HOUR);
    }

    // The last check is yet to come: the next one is not due.
    fn future(s: &mut StreamTrackerFrame) {
        s.last_status_check_at = time::now().add(time::HOUR);
    }

    // New for musivum: the original has no tests of the frame tracker. What is expected in
    // these tests is what the original does at the same steps (taken from running it).
    #[test]
    fn a_camera_at_changing_frame_rates() {
        const MS_500: i64 = 500_000_000;
        let mut s = new_tracker(5.0, 90000);
        assert_eq!(s.get_check_interval(), 500 * time::MILLISECOND);
        expect(
            s.check_status(),
            &s,
            (NONE, 0.0, MS_500, false, 0, 0, 0, false),
            "new, check",
        );

        // the first packet makes it active, whether it ends a frame or not
        expect(
            s.observe(false, 1000),
            &s,
            (ACTIVE, 0.0, MS_500, false, 0, 0, 0, true),
            "packet without marker",
        );
        expect(
            s.observe(false, 1000),
            &s,
            (NONE, 0.0, MS_500, false, 0, 0, 0, true),
            "packet without marker again",
        );
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (STOPPED, 0.0, MS_500, false, 0, 0, 0, true),
            "check, no frame",
        );

        // one frame is not enough for a frame rate
        expect(
            s.observe(true, 3000),
            &s,
            (NONE, 0.0, MS_500, true, 3000, 3000, 1, true),
            "one frame",
        );
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (STOPPED, 0.0, MS_500, false, 0, 0, 0, true),
            "check, one frame",
        );

        // 30 fps: the estimate goes up slowly
        let mut ts: u32 = 6000;
        for _ in 0..16 {
            s.observe(true, ts);
            ts += 3000;
        }
        expect(
            NONE,
            &s,
            (NONE, 0.0, MS_500, true, 6000, 51000, 16, true),
            "16 frames at 30 fps",
        );
        future(&mut s);
        expect(
            s.check_status(),
            &s,
            (NONE, 0.0, MS_500, true, 6000, 51000, 16, true),
            "check, not due",
        );
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 18.0, MS_500, true, 51000, 51000, 1, true),
            "check",
        );
        for _ in 0..15 {
            s.observe(true, ts);
            ts += 3000;
        }
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 25.2, MS_500, true, 96000, 96000, 1, true),
            "15 more at 30 fps, check",
        );

        // and down fast
        ts -= 3000;
        for _ in 0..8 {
            ts += 6000;
            s.observe(true, ts);
        }
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 16.02, MS_500, true, 144000, 144000, 1, true),
            "8 at 15 fps, check",
        );
        for _ in 0..2 {
            ts += 45000;
            s.observe(true, ts);
        }
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 3.4, MS_500, true, 234000, 234000, 1, true),
            "2 at 2 fps, check",
        );

        // below 2 fps the evaluation interval is that of the estimated frame rate
        ts += 90000;
        s.observe(true, ts);
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 1.24, 806_451_612, true, 324000, 324000, 1, true),
            "1 at 1 fps, check",
        );
        ts += 90000;
        s.observe(true, ts);
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 1.02, 980_392_156, true, 414000, 414000, 1, true),
            "1 more at 1 fps, check",
        );
        ts += 88235;
        s.observe(true, ts);
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 1.02, 980_392_156, true, 502235, 502235, 1, true),
            "1 at 1.02 fps, check",
        );

        // no frame in an interval: stopped, and it takes two frames to start again
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (STOPPED, 0.0, MS_500, false, 0, 0, 0, true),
            "no more frames, check",
        );
        ts += 3000;
        expect(
            s.observe(true, ts),
            &s,
            (NONE, 0.0, MS_500, true, 505235, 505235, 1, true),
            "one frame after the stop",
        );
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (STOPPED, 0.0, MS_500, false, 0, 0, 0, true),
            "check, one frame",
        );
        ts += 3000;
        s.observe(true, ts);
        ts += 3000;
        s.observe(true, ts);
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 18.0, MS_500, true, 511235, 511235, 1, true),
            "two frames, check",
        );

        // after a reset the first packet makes it active again
        s.reset();
        expect(
            NONE,
            &s,
            (NONE, 0.0, MS_500, false, 0, 0, 0, false),
            "reset",
        );
        assert!(s.last_status_check_at.is_zero());
        expect(
            s.check_status(),
            &s,
            (NONE, 0.0, MS_500, false, 0, 0, 0, false),
            "check after reset",
        );
        expect(
            s.observe(true, ts),
            &s,
            (ACTIVE, 0.0, MS_500, true, 511235, 511235, 1, true),
            "frame after reset",
        );
        assert!(!s.last_status_check_at.is_zero());
    }

    // New for musivum.
    #[test]
    fn a_screen_share_with_timestamps_that_wrap() {
        const S_2: i64 = 2_000_000_000;
        let mut s = new_tracker(0.5, 90000);
        expect(NONE, &s, (NONE, 0.0, S_2, false, 0, 0, 0, false), "new");
        expect(
            s.observe(true, 0xFFFF_0000),
            &s,
            (ACTIVE, 0.0, S_2, true, 4294901760, 4294901760, 1, true),
            "first frame",
        );
        expect(
            s.observe(true, 0x0000_1000),
            &s,
            (NONE, 0.0, S_2, true, 4294901760, 4096, 2, true),
            "frame after the wrap",
        );
        expect(
            s.observe(true, 0xFFFE_0000),
            &s,
            (NONE, 0.0, S_2, true, 4294836224, 4096, 3, true),
            "older frame",
        );
        expect(
            s.observe(true, 0xFFFF_8000),
            &s,
            (NONE, 0.0, S_2, true, 4294836224, 4096, 4, true),
            "frame in between",
        );
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 1.2, S_2, true, 4096, 4096, 1, true),
            "check",
        );

        // half the range ahead is neither older nor newer, but it is counted
        expect(
            s.observe(true, 0x8000_1000),
            &s,
            (NONE, 1.2, S_2, true, 4096, 4096, 2, true),
            "half the range ahead",
        );
        expect(
            s.observe(true, 0x0000_1000 + 300_000),
            &s,
            (NONE, 1.2, S_2, true, 4096, 304096, 3, true),
            "frame",
        );
        expect(
            s.observe(true, 0x0000_1000 + 600_000),
            &s,
            (NONE, 1.2, S_2, true, 4096, 604096, 4, true),
            "frame",
        );
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 0.53, S_2, true, 604096, 604096, 1, true),
            "check, three frames at 0.15 fps",
        );
        expect(
            s.observe(true, 0x0000_1000 + 600_000 + 900_000),
            &s,
            (NONE, 0.53, S_2, true, 604096, 1504096, 2, true),
            "frame",
        );
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 0.14, 7_142_857_142, true, 1504096, 1504096, 1, true),
            "check, 0.1 fps",
        );

        // a frame rate that rounds to nothing is a stop
        expect(
            s.observe(true, 0x0000_1000 + 600_000 + 900_000 + 90_000_000),
            &s,
            (NONE, 0.14, 7_142_857_142, true, 1504096, 91504096, 2, true),
            "frame",
        );
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (STOPPED, 0.0, S_2, false, 0, 0, 0, true),
            "check, 0.001 fps",
        );
    }

    // New for musivum.
    #[test]
    fn no_minimum_frame_rate_and_the_rounding() {
        const MS_500: i64 = 500_000_000;
        const FPS: f64 = 29.990000000000002;
        let mut s = StreamTrackerFrame::new(StreamTrackerFrameParams {
            clock_rate: 48000,
            logger: logger::get_logger(),
            ..Default::default()
        });
        expect(NONE, &s, (NONE, 0.0, MS_500, false, 0, 0, 0, false), "new");
        expect(
            s.observe(true, 0),
            &s,
            (ACTIVE, 0.0, MS_500, true, 0, 0, 1, true),
            "first frame",
        );
        expect(
            s.observe(true, 960),
            &s,
            (NONE, 0.0, MS_500, true, 0, 960, 2, true),
            "frame",
        );
        expect(
            s.observe(true, 1920),
            &s,
            (NONE, 0.0, MS_500, true, 0, 1920, 3, true),
            "frame",
        );
        expect(
            s.observe(true, 2881),
            &s,
            (NONE, 0.0, MS_500, true, 0, 2881, 4, true),
            "frame",
        );
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, FPS, MS_500, true, 2881, 2881, 1, true),
            "check",
        );
        expect(
            s.observe(true, 2881 + 70000),
            &s,
            (NONE, FPS, MS_500, true, 2881, 72881, 2, true),
            "frame",
        );
        past(&mut s);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 3.62, MS_500, true, 72881, 72881, 1, true),
            "check",
        );

        assert_eq!(round_frame_rate(29.994), 29.990000000000002);
        assert_eq!(round_frame_rate(29.995), 30.0);
        assert_eq!(round_frame_rate(0.005), 0.01);
        assert_eq!(round_frame_rate(0.0049), 0.0);
        assert_eq!(round_frame_rate(14.985), 14.99);
    }

    // New for musivum.
    #[test]
    fn a_check_is_due_when_nearly_all_of_the_evaluation_interval_has_passed() {
        const S_2: i64 = 2_000_000_000;
        let mut s = new_tracker(0.5, 90000);
        expect(
            s.observe(true, 1000),
            &s,
            (ACTIVE, 0.0, S_2, true, 1000, 1000, 1, true),
            "first frame",
        );
        expect(
            s.observe(true, 4000),
            &s,
            (NONE, 0.0, S_2, true, 1000, 4000, 2, true),
            "second frame",
        );
        s.last_status_check_at = time::now().add(-time::SECOND);
        expect(
            s.check_status(),
            &s,
            (NONE, 0.0, S_2, true, 1000, 4000, 2, true),
            "check after 1 s of 2 s",
        );
        s.last_status_check_at = time::now().add(-1970 * time::MILLISECOND);
        expect(
            s.check_status(),
            &s,
            (ACTIVE, 18.0, S_2, true, 4000, 4000, 1, true),
            "check after 1.97 s of 2 s",
        );

        // without a time of the last check, the interval starts at the check
        s.observe(true, 7000);
        s.last_status_check_at = Time::ZERO;
        expect(
            s.check_status(),
            &s,
            (NONE, 18.0, S_2, true, 4000, 7000, 2, true),
            "check without a last check",
        );
        assert!(!s.last_status_check_at.is_zero());
    }
}
