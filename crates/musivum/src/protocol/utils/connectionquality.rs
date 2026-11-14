// Ported from LiveKit's protocol, utils/connectionquality.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

use crate::protocol::livekit;

pub fn is_connection_quality_lower(
    prev: livekit::ConnectionQuality,
    curr: livekit::ConnectionQuality,
) -> bool {
    if prev != livekit::ConnectionQuality::Lost && curr == livekit::ConnectionQuality::Lost {
        return true;
    }

    if prev == livekit::ConnectionQuality::Lost && curr != livekit::ConnectionQuality::Lost {
        return false;
    }

    // WARNING NOTE: comparing protobuf enums directly
    (curr as i32) < (prev as i32)
}

pub fn is_connection_quality_higher(
    prev: livekit::ConnectionQuality,
    curr: livekit::ConnectionQuality,
) -> bool {
    if is_connection_quality_lower(prev, curr) {
        return false;
    }

    curr != prev
}

#[cfg(test)]
mod tests {
    use super::*;
    use livekit::ConnectionQuality::{Excellent, Good, Lost, Poor};

    // New for musivum: upstream has no test for this file.
    #[test]
    fn lost_is_the_lowest_although_its_number_is_the_highest() {
        let ordered = [Lost, Poor, Good, Excellent];
        for (i, prev) in ordered.into_iter().enumerate() {
            for (j, curr) in ordered.into_iter().enumerate() {
                assert_eq!(
                    j < i,
                    is_connection_quality_lower(prev, curr),
                    "lower: {prev:?} to {curr:?}"
                );
                assert_eq!(
                    j > i,
                    is_connection_quality_higher(prev, curr),
                    "higher: {prev:?} to {curr:?}"
                );
            }
        }
    }
}
