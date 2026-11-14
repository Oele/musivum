// Ported from LiveKit's protocol, utils/rtputil/rtpconverter.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the Go package `rtputil` has this one file, so the
// module is the file.

use crate::go::time::{self, Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RtpConverter {
    ts: u64,
    rtp: u64,
}

impl RtpConverter {
    pub fn new(mut clock_rate: i64) -> Self {
        let mut ts = time::SECOND.nanoseconds();
        for i in [10i64, 3, 2] {
            while ts % i == 0 && clock_rate % i == 0 {
                ts /= i;
                clock_rate /= i;
            }
        }

        Self {
            ts: ts as u64,
            rtp: clock_rate as u64,
        }
    }

    pub fn to_duration(&self, rtp_ticks: u32) -> Duration {
        Duration(((rtp_ticks as u64).wrapping_mul(self.ts) / self.rtp) as i64)
    }

    pub fn to_duration_ext(&self, rtp_ext_ticks: u64) -> Duration {
        Duration((rtp_ext_ticks.wrapping_mul(self.ts) / self.rtp) as i64)
    }

    pub fn to_rtp(&self, duration: Duration) -> u32 {
        (duration.nanoseconds().wrapping_mul(self.rtp as i64) / (self.ts as i64)) as u32
    }

    pub fn to_rtp_ext(&self, duration: Duration) -> u64 {
        (duration.nanoseconds().wrapping_mul(self.rtp as i64) / (self.ts as i64)) as u64
    }
}

#[cfg(test)]
#[path = "rtputil_test.rs"]
mod rtputil_test;
