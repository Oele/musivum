// Ported from LiveKit, pkg/sfu/videolayerselector/simulcast_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

use super::*;
use crate::protocol::logger;
use crate::webrtc::rtp;

fn key_frame_on_layer(spatial: i32, temporal: i32) -> buffer::ExtPacket {
    buffer::ExtPacket {
        video_layer: buffer::VideoLayer { spatial, temporal },
        packet: rtp::Packet::default(),
        is_key_frame: true,
        ..Default::default()
    }
}

// On initial acquisition the selector must latch directly onto the target layer and ignore
// lower-layer key frames that arrive first, so a subscriber requesting the top layer does not
// briefly decode a lower layer (a visible quality ramp).
#[test]
fn test_simulcast_select_acquires_target_layer_directly() {
    let mut s = Simulcast::new(logger::get_logger());
    s.set_enable_start_at_desired_quality(true);
    s.set_max(buffer::VideoLayer {
        spatial: 2,
        temporal: 2,
    });
    s.set_max_seen(buffer::VideoLayer {
        spatial: 2,
        temporal: 2,
    });
    s.set_target(buffer::VideoLayer {
        spatial: 2,
        temporal: 2,
    });
    s.set_request_spatial(2);
    s.set_current(buffer::INVALID_LAYER);

    // lower-layer key frames arriving first must NOT be latched
    assert!(!s.select(&key_frame_on_layer(0, 2), 0).is_selected);
    assert!(!s.get_current().is_valid());
    assert!(!s.select(&key_frame_on_layer(1, 2), 1).is_selected);
    assert!(!s.get_current().is_valid());

    // the target layer key frame latches directly
    assert!(s.select(&key_frame_on_layer(2, 2), 2).is_selected);
    assert_eq!(2i32, s.get_current().spatial);
}

// Acquisition follows whatever target the allocator set: when the target is lowered (e.g. the
// acquisition grace expired and the allocator fell back to the highest layer seen), the selector
// latches that lower layer directly. This is the fallback path that prevents a stall when the
// originally requested layer never shows up.
#[test]
fn test_simulcast_select_acquires_lowered_target() {
    let mut s = Simulcast::new(logger::get_logger());
    s.set_enable_start_at_desired_quality(true);
    s.set_max(buffer::VideoLayer {
        spatial: 2,
        temporal: 2,
    });
    s.set_max_seen(buffer::VideoLayer {
        spatial: 1,
        temporal: 2,
    });
    // allocator dropped the target to the highest layer actually seen
    s.set_target(buffer::VideoLayer {
        spatial: 1,
        temporal: 2,
    });
    s.set_request_spatial(1);
    s.set_current(buffer::INVALID_LAYER);

    assert!(!s.select(&key_frame_on_layer(0, 2), 0).is_selected);
    assert!(!s.get_current().is_valid());

    assert!(s.select(&key_frame_on_layer(1, 2), 1).is_selected);
    assert_eq!(1i32, s.get_current().spatial);
}
