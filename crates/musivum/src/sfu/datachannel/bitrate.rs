// Ported from LiveKit, pkg/sfu/datachannel/bitrate.go at v1.13.11.
// Copyright LiveKit, Inc. (the original file has no notice of its own). Licensed under the
// Apache License, Version 2.0.
// Changes: translated from Go to Rust; `BitrateMode` is an enum, so there is no "unknown"
// mode to name; the queue of windows is the standard library's `VecDeque` where the original
// has `github.com/gammazero/deque`; what does not change after construction (the two
// durations and the mode) is outside the lock; the unexported `bitrate` is `bitrate_inner`,
// since the exported `Bitrate` has that name here.

use std::collections::VecDeque;
use std::fmt;

use parking_lot::Mutex;

use crate::go::time::{self, Duration, Time};
use crate::protocol::utils::mono;

pub const BITRATE_DURATION: Duration = Duration::from_secs(2);
pub const BITRATE_WINDOW: Duration = Duration::from_millis(100);

// BitrateMode selects how the sliding-window rate is computed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitrateMode {
    // BitrateModeBusyOnly divides drained bytes by backlogged ("busy") time
    // only; idle gaps are excluded from the denominator. All drained bytes count.
    BusyOnly,

    // BitrateModeExcludeIdleDrain is like BitrateModeBusyOnly but also excludes
    // bytes drained across a non-backlogged interval from the numerator.
    ExcludeIdleDrain,

    // BitrateModeWallClock divides all bytes by elapsed wall-clock time. For
    // consumers with no send buffer, e.g. a receiver measuring its incoming rate.
    WallClock,
}

impl fmt::Display for BitrateMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BitrateMode::BusyOnly => write!(f, "busyOnly"),
            BitrateMode::ExcludeIdleDrain => write!(f, "excludeIdleDrain"),
            BitrateMode::WallClock => write!(f, "wallClock"),
        }
    }
}

// BitrateCalculator calculates bitrate over sliding window
pub struct BitrateCalculator {
    window_duration: Duration,
    duration: Duration,
    mode: BitrateMode,

    // what `lock sync.Mutex` guards upstream
    inner: Mutex<BitrateCalculatorInner>,
}

struct BitrateCalculatorInner {
    windows: VecDeque<BitrateWindow>,
    active: BitrateWindow,

    bytes: i64,
    busy: Duration,
    last_buffered_amount: i64,
    last: Time,
    start: Time,
}

impl BitrateCalculator {
    // NewBitrateCalculator creates a calculator. See BitrateMode for how mode
    // governs the numerator and denominator.
    pub fn new(duration: Duration, window: Duration, mode: BitrateMode) -> Self {
        let mut window_cnt = (duration + (window - time::NANOSECOND)) / window;
        if window_cnt == 0 {
            window_cnt = 1;
        }
        let now = mono::now();
        Self {
            duration,
            window_duration: window,
            mode,
            inner: Mutex::new(BitrateCalculatorInner {
                // SetBaseCap(windowCnt + 1)
                windows: VecDeque::with_capacity(usize::try_from(window_cnt + 1).unwrap_or(0)),
                active: BitrateWindow {
                    start: now,
                    ..Default::default()
                },
                bytes: 0,
                busy: Duration::ZERO,
                last_buffered_amount: 0,
                last: Time::ZERO,
                start: now,
            }),
        }
    }

    pub fn add_bytes(&self, mut bytes: i64, buffered_amout: i64, ts: Time) {
        let mut c = self.inner.lock();

        // bufferedAmout is sampled right after the write, so it includes the bytes
        // just enqueued; subtracting them gives the buffer occupancy before the
        // write. Between writes the buffer only drains, so a non-zero pre-write
        // level means it stayed backlogged for the whole interval.
        let backlogged = buffered_amout.wrapping_sub(bytes) > 0;
        let mut busy = Duration::ZERO;
        if !c.last.is_zero() && backlogged {
            busy = ts.sub(c.last);
            if busy < Duration::ZERO {
                busy = Duration::ZERO;
            }
        }
        c.last = ts;

        // bytes that actually drained since the previous sample
        bytes = bytes.wrapping_sub(buffered_amout.wrapping_sub(c.last_buffered_amount));
        if bytes < 0 {
            // it is possible that internal buffering (non-data like DCEP packet from webrtc) caused bytes to be negative
            bytes = 0;
        }
        c.last_buffered_amount = buffered_amout;

        if self.mode == BitrateMode::ExcludeIdleDrain && !backlogged {
            // drain across a non-backlogged interval finished at an unknown point,
            // so it is not a reliable rate sample
            bytes = 0;
        }

        if ts.sub(c.active.start) >= self.window_duration {
            let active = c.active;
            c.windows.push_back(active);
            c.active.start = ts;
            c.active.bytes = 0;
            c.active.busy = Duration::ZERO;

            while let Some(w) = c.windows.front().copied() {
                // pop expired windows
                if ts.sub(w.start) > (self.duration + self.window_duration) {
                    c.bytes = c.bytes.wrapping_sub(w.bytes);
                    c.busy -= w.busy;
                    c.windows.pop_front();
                } else {
                    c.start = w.start;
                    break;
                }
            }
            if c.windows.is_empty() {
                c.start = ts;
                c.bytes = 0;
                c.busy = Duration::ZERO;
            }
        }
        c.bytes = c.bytes.wrapping_add(bytes);
        c.active.bytes = c.active.bytes.wrapping_add(bytes);
        c.busy += busy;
        c.active.busy += busy;
    }

    pub fn bitrate(&self, ts: Time) -> (i64, bool) {
        self.bitrate_inner(ts, false)
    }

    pub fn force_bitrate(&self, ts: Time) -> (i64, bool) {
        self.bitrate_inner(ts, true)
    }

    fn bitrate_inner(&self, ts: Time, force: bool) -> (i64, bool) {
        let c = self.inner.lock();
        // busy modes divide by backlogged time; wall-clock mode by elapsed time
        let mut denom = c.busy;
        if self.mode == BitrateMode::WallClock {
            denom = ts.sub(c.start);
        }
        if denom < self.window_duration {
            if force {
                denom = self.window_duration;
            } else {
                return (0, false);
            }
        }

        (
            c.bytes.wrapping_mul(8).wrapping_mul(1000) / denom.milliseconds(),
            true,
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct BitrateWindow {
    start: Time,
    bytes: i64,
    busy: Duration,
}

#[cfg(test)]
#[path = "bitrate_test.rs"]
mod bitrate_test;
