// Ported from LiveKit, pkg/sfu/buffer/datastats.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; what `lock` guards, which is all but the params, is in
// an inner struct; the number of bytes of an update is unsigned.

use parking_lot::RwLock;

use crate::go::time::{self, Duration, Time};
use crate::protocol::livekit;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DataStatsParam {
    pub window_duration: Duration,
}

pub struct DataStats {
    params: DataStatsParam,

    // what `lock sync.RWMutex` guards upstream
    inner: RwLock<DataStatsInner>,
}

struct DataStatsInner {
    total_bytes: i64,
    start_time: Time,
    end_time: Time,
    window_start: i64,
    window_bytes: i64,
}

impl DataStats {
    pub fn new(params: DataStatsParam) -> Self {
        Self {
            params,
            inner: RwLock::new(DataStatsInner {
                total_bytes: 0,
                start_time: time::now(),
                end_time: Time::ZERO,
                window_start: time::now().unix_nano(),
                window_bytes: 0,
            }),
        }
    }

    pub fn update(&self, bytes: usize, time: i64) {
        let mut s = self.inner.write();
        s.total_bytes = s.total_bytes.wrapping_add(bytes as i64);

        if self.params.window_duration > Duration::ZERO
            && time.wrapping_sub(s.window_start) > self.params.window_duration.nanoseconds()
        {
            s.window_bytes = 0;
            s.window_start = time;
        }
        s.window_bytes = s.window_bytes.wrapping_add(bytes as i64);
    }

    pub fn to_proto_active(&self) -> livekit::RtpStats {
        if self.params.window_duration == Duration::ZERO {
            return livekit::RtpStats::default();
        }
        let s = self.inner.read();
        let now = time::now().unix_nano();
        let duration = now.wrapping_sub(s.window_start);
        if duration > self.params.window_duration.nanoseconds() {
            return livekit::RtpStats::default();
        }
        let seconds = duration as f64 / 1e9;

        livekit::RtpStats {
            start_time: Some(time::timestamppb_new(Time::unix(
                s.window_start / 1_000_000_000,
                s.window_start % 1_000_000_000,
            ))),
            end_time: Some(time::timestamppb_new(Time::unix(0, now))),
            duration: seconds,
            bytes: s.window_bytes as u64,
            bitrate: s.window_bytes as f64 * 8.0 / seconds,
            ..Default::default()
        }
    }

    pub fn stop(&self) {
        let mut s = self.inner.write();
        s.end_time = time::now();
    }

    pub fn to_proto_aggregate_only(&self) -> livekit::RtpStats {
        let s = self.inner.read();

        let mut end = s.end_time;
        if end.is_zero() {
            end = time::now();
        }
        livekit::RtpStats {
            start_time: Some(time::timestamppb_new(s.start_time)),
            end_time: Some(time::timestamppb_new(end)),
            duration: end.sub(s.start_time).seconds(),
            bytes: s.total_bytes as u64,
            bitrate: s.total_bytes as f64 * 8.0 / end.sub(s.start_time).seconds(),
            ..Default::default()
        }
    }
}

#[cfg(test)]
#[path = "datastats_test.rs"]
mod datastats_test;
