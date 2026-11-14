// Ported from LiveKit's protocol, utils/mono/mono.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the monotonic clock anchored to the wall clock at start
// is `crate::go::time`, which every time value of musivum comes from, so only the entry points
// remain here. `FromTime`, `Unix` and `Parse` have no counterpart: there are no times with a
// wall-clock part to convert from.

use crate::go::time::{self, Time};

/// Now creates a monotonic time without reading the system wall clock
pub fn now() -> Time {
    time::now()
}

/// UnixNano returns the number of nanoseconds elapsed, based on the application
/// start time.
pub fn unix_nano() -> i64 {
    time::now().unix_nano()
}

/// UnixMicro returns the number of microseconds elapsed, based on the
/// application start time.
pub fn unix_micro() -> i64 {
    unix_nano() / 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    // Upstream's test (mono_test.go, TestMonoZero and TestMono) checks that times made by
    // this package compare with each other across a jump of the wall clock; here that is a
    // property of `go::time` and is tested there. What is left to check is the units.
    #[test]
    fn test_mono() {
        let a = unix_nano();
        let b = now().unix_nano();
        let c = unix_micro();
        assert!(b >= a);
        assert!(c >= a / 1000);
        assert!(
            c - a / 1000 < 1_000_000,
            "micros and nanos are of the same clock"
        );
    }
}
