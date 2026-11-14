// Ported from LiveKit's protocol, utils/timed_version.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; a generator is handed out as an `Arc` of the trait, and
// the struct behind it, `timedVersionGenerator` upstream, is `DefaultTimedVersionGenerator`
// (Rust has no names that differ only in their first letter); `TimedVersionFromProto` takes an
// `Option`, for the nil the original accepts. Left out: what stores a version in a database
// (`Value`, `Scan`, `GormDataType`, `MarshalBinary`, `UnmarshalBinary`), and `New()` of the
// generator, which is not part of the interface.

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::go::time::{self, Time};
use crate::protocol::livekit;

const TICK_BITS: u64 = 13;
const TICK_MASK: u64 = (1 << TICK_BITS) - 1;

// time.Date(2000, 0, 0, 0, 0, 0, 0, time.UTC).UnixMicro(): month 0 and day 0 make this the
// 30th of November 1999.
const EPOCH: i64 = 943_920_000_000_000;

pub trait TimedVersionGenerator: Send + Sync {
    fn next(&self) -> TimedVersion;
}

fn timed_version_components(v: TimedVersion) -> (i64, i32) {
    (
        ((v.0 >> TICK_BITS) as i64).wrapping_add(EPOCH),
        (v.0 & TICK_MASK) as i32,
    )
}

fn timed_version_from_components(mut ts: i64, ticks: i32) -> TimedVersion {
    if ts < EPOCH {
        ts = EPOCH;
    }
    TimedVersion((((ts - EPOCH) as u64) << TICK_BITS) | ticks as u64)
}

#[derive(Default)]
struct DefaultTimedVersionGenerator {
    mu: Mutex<DefaultTimedVersionGeneratorInner>,
}

#[derive(Default)]
struct DefaultTimedVersionGeneratorInner {
    ts: i64,
    ticks: u64,
}

pub fn new_default_timed_version_generator() -> Arc<dyn TimedVersionGenerator> {
    Arc::new(DefaultTimedVersionGenerator::default())
}

impl TimedVersionGenerator for DefaultTimedVersionGenerator {
    fn next(&self) -> TimedVersion {
        let mut ts = time::now().unix_micro();

        let mut g = self.mu.lock();

        loop {
            if ts < g.ts {
                ts = g.ts;
            }
            if g.ts == ts {
                // if incrementing the ticks would overflow the version sleep for a
                // microsecond then try again.
                if g.ticks == TICK_MASK {
                    std::thread::sleep(time::MICROSECOND.to_std());
                    ts = time::now().unix_micro();
                    continue;
                }
                g.ticks += 1;
            } else {
                g.ts = ts;
                g.ticks = 0;
            }
            return timed_version_from_components(g.ts, g.ticks as i32);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TimedVersion(pub u64);

pub fn timed_version_from_proto(proto: Option<&livekit::TimedVersion>) -> TimedVersion {
    timed_version_from_components(
        proto.map_or(0, |p| p.unix_micro),
        proto.map_or(0, |p| p.ticks),
    )
}

pub fn timed_version_from_time(t: Time) -> TimedVersion {
    timed_version_from_components(t.unix_micro(), 0)
}

impl TimedVersion {
    pub fn update(&mut self, other: TimedVersion) -> bool {
        self.upgrade(other)
    }

    pub fn upgrade(&mut self, other: TimedVersion) -> bool {
        if *self < other {
            *self = other;
            return true;
        }
        false
    }

    pub fn downgrade(&mut self, other: TimedVersion) -> bool {
        if *self > other {
            *self = other;
            return true;
        }
        false
    }

    pub fn store(&mut self, other: TimedVersion) {
        *self = other;
    }

    pub fn load(self) -> TimedVersion {
        self
    }

    pub fn next(self) -> TimedVersion {
        TimedVersion(self.0.wrapping_add(1))
    }

    pub fn prev(self) -> TimedVersion {
        if self.0 == 0 {
            return self;
        }
        TimedVersion(self.0 - 1)
    }

    pub fn after(self, other: TimedVersion) -> bool {
        self > other
    }

    pub fn compare(self, other: TimedVersion) -> i64 {
        match self.cmp(&other) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn to_proto(self) -> livekit::TimedVersion {
        let (ts, ticks) = timed_version_components(self);
        livekit::TimedVersion {
            unix_micro: ts,
            ticks,
        }
    }

    pub fn time(self) -> Time {
        let (ts, _) = timed_version_components(self);
        Time::from_unix_nano(ts.wrapping_mul(1000))
    }
}

impl fmt::Display for TimedVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (ts, ticks) = timed_version_components(*self);
        write!(f, "{ts}.{ticks}")
    }
}

impl fmt::Debug for TimedVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
#[path = "timed_version_test.rs"]
mod timed_version_test;
