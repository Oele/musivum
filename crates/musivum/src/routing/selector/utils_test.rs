// Ported from LiveKit, pkg/routing/selector/utils_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

use crate::go::time;
use crate::protocol::livekit;
use crate::routing::selector;

#[test]
fn test_is_available() {
    // still available
    {
        let n = livekit::Node {
            stats: Some(livekit::NodeStats {
                updated_at: time::now().unix_secs() - 3,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(selector::is_available(&n), "still available");
    }

    // expired
    {
        let n = livekit::Node {
            stats: Some(livekit::NodeStats {
                updated_at: time::now().unix_secs() - 20,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(!selector::is_available(&n), "expired");
    }
}
