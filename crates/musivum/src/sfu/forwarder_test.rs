// Ported from LiveKit, pkg/sfu/forwarder_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - What the original's tests reach of the forwarder (`f.vls`, `f.lastAllocation`,
//   `f.started`, ...) is behind its lock here: `f.lock.write().vls` and so on, and the
//   functions the original calls with the lock held are given the inner struct.
// - `GetTranslationParams` gives the parameters and a `Result`: `require.NoError` is an
//   equality with `Ok(())`. A test packet is a value, so `require.NotNil` on it has no
//   counterpart.
// - `require.IsType` on the codec munger is `as_any().is::<..>()`.
// - `dropSelector` embeds the selector it wraps and so has all its methods. Here it gives
//   out the wrapped selector's `Base`, which the trait's methods use, and passes on the
//   methods that a selector has of its own.
// - The slices of available layers are compared as vectors: `[]int32{}` and nil are the
//   same.
// - `TestForwarderSwitchBackAfterReferenceTimestampWrap`, "random start and duration": the
//   400 starts and durations of Go's `rand.New(rand.NewSource(1))` are written out, taken
//   from a run of a Go program that draws them as the test does.
// - The subtests are blocks or loops that name themselves in their assertions.

use super::*;
use crate::mediatransportutil;
use crate::mediatransportutil::codec;
use crate::protocol::logger;
use crate::sfu::pacer;
use crate::sfu::testutils;
use crate::testutils::assert_in_delta;
use crate::webrtc::rtp::codecs;

fn disable(f: &Forwarder) {
    let mut f = f.lock.write();
    f.vls.set_current(buffer::INVALID_LAYER);
    f.vls.set_target(buffer::INVALID_LAYER);
}

fn new_forwarder(codec: &webrtc::RtpCodecCapability, kind: webrtc::RtpCodecType) -> Forwarder {
    let f = Forwarder::new(
        kind,
        logger::get_logger(),
        true, // skipReferenceTS
        true, // disableOpportunisticAllocation
        true, // enableStartAtDesiredQuality
        None,
    );
    f.determine_codec(codec, &[], livekit::video_layer::Mode::Unused);
    f
}

// `[]webrtc.RTPHeaderExtensionParameter{{URI: dd.ExtensionURI}}`
fn dd_extensions() -> Vec<webrtc::RtpHeaderExtensionParameter> {
    vec![webrtc::RtpHeaderExtensionParameter {
        uri: dd::EXTENSION_URI.to_string(),
        ..Default::default()
    }]
}

#[test]
fn test_forwarder_mute() {
    let f = new_forwarder(&testutils::test_opus_codec(), webrtc::RtpCodecType::Audio);
    assert!(!f.is_muted());
    let muted = f.mute(false, true);
    assert!(!muted); // no change in mute state
    assert!(!f.is_muted());

    let muted = f.mute(true, false);
    assert!(!muted);
    assert!(!f.is_muted());

    let muted = f.mute(true, true);
    assert!(muted);
    assert!(f.is_muted());

    let muted = f.mute(false, true);
    assert!(muted);
    assert!(!f.is_muted());
}

#[test]
fn test_forwarder_layers_audio() {
    let f = new_forwarder(&testutils::test_opus_codec(), webrtc::RtpCodecType::Audio);

    assert_eq!(buffer::INVALID_LAYER, f.max_layer());

    assert_eq!(buffer::INVALID_LAYER, f.current_layer());
    assert_eq!(buffer::INVALID_LAYER, f.target_layer());

    let (changed, max_layer) = f.set_max_spatial_layer(1);
    assert!(!changed);
    assert_eq!(buffer::INVALID_LAYER, max_layer);

    let (changed, max_layer) = f.set_max_temporal_layer(1);
    assert!(!changed);
    assert_eq!(buffer::INVALID_LAYER, max_layer);

    assert_eq!(buffer::INVALID_LAYER, f.max_layer());
}

#[test]
fn test_forwarder_layers_video() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);

    let max_layer = f.max_layer();
    let expected_layers = buffer::VideoLayer {
        spatial: buffer::INVALID_LAYER_SPATIAL,
        temporal: buffer::DEFAULT_MAX_LAYER_TEMPORAL,
    };
    assert_eq!(expected_layers, max_layer);

    assert_eq!(buffer::INVALID_LAYER, f.current_layer());
    assert_eq!(buffer::INVALID_LAYER, f.target_layer());

    let expected_layers = buffer::VideoLayer {
        spatial: buffer::DEFAULT_MAX_LAYER_SPATIAL,
        temporal: buffer::DEFAULT_MAX_LAYER_TEMPORAL,
    };
    let (changed, max_layer) = f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    assert!(changed);
    assert_eq!(expected_layers, max_layer);

    let (changed, max_layer) = f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL - 1);
    assert!(changed);
    let expected_layers = buffer::VideoLayer {
        spatial: buffer::DEFAULT_MAX_LAYER_SPATIAL - 1,
        temporal: buffer::DEFAULT_MAX_LAYER_TEMPORAL,
    };
    assert_eq!(expected_layers, max_layer);
    assert_eq!(expected_layers, f.max_layer());

    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 1,
    });
    let (changed, max_layer) = f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL - 1);
    assert!(!changed);
    assert_eq!(expected_layers, max_layer);
    assert_eq!(expected_layers, f.max_layer());

    let (changed, max_layer) = f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);
    assert!(!changed);
    assert_eq!(expected_layers, max_layer);

    let (changed, max_layer) = f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL - 1);
    assert!(changed);
    let expected_layers = buffer::VideoLayer {
        spatial: buffer::DEFAULT_MAX_LAYER_SPATIAL - 1,
        temporal: buffer::DEFAULT_MAX_LAYER_TEMPORAL - 1,
    };
    assert_eq!(expected_layers, max_layer);
    assert_eq!(expected_layers, f.max_layer());
}

#[test]
fn test_forwarder_allocate_optimal() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);

    let empty_bitrates = Bitrates::default();
    let bitrates: Bitrates = [[2, 3, 0, 0], [4, 0, 0, 5], [0, 7, 0, 0]];

    // invalid max layers
    f.lock.write().vls.set_max(buffer::INVALID_LAYER);
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::FeedDry,
        bandwidth_requested: 0,
        bandwidth_delta: 0,
        bitrates,
        target_layer: buffer::INVALID_LAYER,
        request_layer_spatial: buffer::INVALID_LAYER_SPATIAL,
        max_layer: buffer::INVALID_LAYER,
        distance_to_desired: 0.0,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[], bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);

    f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);

    // should still have target at buffer.InvalidLayer until max publisher layer is available
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::FeedDry,
        bandwidth_requested: 0,
        bandwidth_delta: 0,
        bitrates,
        target_layer: buffer::INVALID_LAYER,
        request_layer_spatial: buffer::INVALID_LAYER_SPATIAL,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 0.0,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[], bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);

    f.set_max_published_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);

    // muted should not consume any bandwidth
    f.mute(true, true);
    disable(&f);
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::Muted,
        bandwidth_requested: 0,
        bandwidth_delta: 0,
        bitrates,
        target_layer: buffer::INVALID_LAYER,
        request_layer_spatial: buffer::INVALID_LAYER_SPATIAL,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 0.0,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[], bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);

    f.mute(false, true);

    // pub muted should not consume any bandwidth
    f.pub_mute(true);
    disable(&f);
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::PubMuted,
        bandwidth_requested: 0,
        bandwidth_delta: 0,
        bitrates,
        target_layer: buffer::INVALID_LAYER,
        request_layer_spatial: buffer::INVALID_LAYER_SPATIAL,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 0.0,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[], bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);

    f.pub_mute(false);

    // when max layers changes, target is opportunistic, but requested spatial layer should be at max
    f.set_max_temporal_layer_seen(buffer::DEFAULT_MAX_LAYER_TEMPORAL);
    f.lock.write().vls.set_max(buffer::VideoLayer {
        spatial: 1,
        temporal: 3,
    });
    let max = f.lock.read().vls.get_max();
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::None,
        bandwidth_requested: bitrates[2][1],
        bandwidth_delta: bitrates[2][1],
        bandwidth_needed: bitrates[1][3],
        bitrates,
        target_layer: buffer::DEFAULT_MAX_LAYER,
        request_layer_spatial: max.spatial,
        max_layer: max,
        distance_to_desired: -1.0,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[], bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(buffer::DEFAULT_MAX_LAYER, f.target_layer());

    // reset max layers for rest of the tests below
    f.lock.write().vls.set_max(buffer::DEFAULT_MAX_LAYER);

    // when feed is dry and current is not valid, should set up for opportunistic forwarding
    // NOTE: feed is dry due to availableLayers = nil, some valid bitrates may be passed in here for testing purposes only
    disable(&f);
    let expected_target_layer = buffer::DEFAULT_MAX_LAYER;
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::None,
        bandwidth_requested: bitrates[2][1],
        bandwidth_delta: 0,
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: -0.5,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[], bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());

    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    }); // set to valid to trigger paths in tests below
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 3,
    }); // set to valid to trigger paths in tests below

    // when feed is dry and current is valid, should stay at current
    let expected_target_layer = buffer::VideoLayer {
        spatial: 0,
        temporal: 3,
    };
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::FeedDry,
        bandwidth_requested: 0,
        bandwidth_delta: 0 - bitrates[2][1],
        bitrates: empty_bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: -0.75,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[], empty_bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());

    f.lock.write().vls.set_current(buffer::INVALID_LAYER);

    // opportunistic target if feed is not dry and current is not valid, i. e. not forwarding
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::None,
        bandwidth_requested: bitrates[2][1],
        bandwidth_delta: bitrates[2][1],
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: buffer::DEFAULT_MAX_LAYER,
        request_layer_spatial: 1,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: -0.5,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[0, 1], bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(buffer::DEFAULT_MAX_LAYER, f.target_layer());

    // when holding in above scenario, should choose the lowest available layer
    let expected_target_layer = buffer::VideoLayer {
        spatial: 1,
        temporal: 0,
    };
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::None,
        bandwidth_requested: bitrates[1][0],
        bandwidth_delta: bitrates[1][0] - bitrates[2][1],
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: 1,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 1.25,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[1, 2], bitrates, true, true);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());

    // opportunistic target if feed is dry and current is not valid, i. e. not forwarding
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::None,
        bandwidth_requested: bitrates[2][1],
        bandwidth_delta: bitrates[2][1] - bitrates[1][0],
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: buffer::DEFAULT_MAX_LAYER,
        request_layer_spatial: 2,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: -0.5,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[], bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(buffer::DEFAULT_MAX_LAYER, f.target_layer());

    // when holding in above scenario, should choose layer 0
    let expected_target_layer = buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    };
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::None,
        bandwidth_requested: bitrates[0][0],
        bandwidth_delta: bitrates[0][0] - bitrates[2][1],
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: 0,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 2.25,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[], bitrates, true, true);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());

    // if feed is not dry and current is not locked, should be opportunistic (with and without overshoot)
    f.lock.write().vls.set_target(buffer::INVALID_LAYER);
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::FeedDry,
        bandwidth_requested: 0,
        bandwidth_delta: 0 - bitrates[0][0],
        bandwidth_needed: 0,
        bitrates: empty_bitrates,
        target_layer: buffer::DEFAULT_MAX_LAYER,
        request_layer_spatial: 1,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: -1.0,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[0, 1], empty_bitrates, false, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(buffer::DEFAULT_MAX_LAYER, f.target_layer());

    f.lock.write().vls.set_target(buffer::INVALID_LAYER);
    let expected_target_layer = buffer::VideoLayer {
        spatial: 2,
        temporal: buffer::DEFAULT_MAX_LAYER_TEMPORAL,
    };
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::None,
        bandwidth_requested: bitrates[2][1],
        bandwidth_delta: bitrates[2][1],
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: 1,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: -0.5,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[0, 1], bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());

    // switches request layer to highest available if feed is not dry and current is valid and current is not available
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 1,
    });
    let expected_target_layer = buffer::VideoLayer {
        spatial: 1,
        temporal: buffer::DEFAULT_MAX_LAYER_TEMPORAL,
    };
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::None,
        bandwidth_requested: bitrates[1][3],
        bandwidth_delta: bitrates[1][3] - bitrates[2][1],
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: 1,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 0.5,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[1], bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());

    // when holding in above scenario, should switch to lowest available layer
    let expected_target_layer = buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    };
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::None,
        bandwidth_requested: bitrates[0][0],
        bandwidth_delta: bitrates[0][0] - bitrates[1][3],
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: 0,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 2.25,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[0, 1], bitrates, true, true);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());

    // stays the same if feed is not dry and current is valid, available and locked
    f.lock.write().vls.set_max(buffer::VideoLayer {
        spatial: 0,
        temporal: 1,
    });
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 1,
    });
    f.lock.write().vls.set_request_spatial(0);
    let expected_target_layer = buffer::VideoLayer {
        spatial: 0,
        temporal: 1,
    };
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::FeedDry,
        bandwidth_requested: 0,
        bandwidth_delta: 0 - bitrates[0][0],
        bitrates: empty_bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: 0,
        max_layer: f.lock.read().vls.get_max(),
        distance_to_desired: 0.0,
        ..Default::default()
    };
    let result = f.allocate_optimal(&[0], empty_bitrates, true, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());
}

#[test]
fn test_forwarder_provisional_allocate() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);
    f.set_max_published_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer_seen(buffer::DEFAULT_MAX_LAYER_TEMPORAL);

    // Reset to invalid layers for testing allocation from scratch
    disable(&f);

    let bitrates: Bitrates = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]];

    f.provisional_allocate_prepare(&[], bitrates);

    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
        true,
        false,
    );
    assert!(is_candidate);
    assert_eq!(bitrates[0][0], used_bitrate);

    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 2,
            temporal: 3,
        },
        true,
        false,
    );
    assert!(is_candidate);
    assert_eq!(bitrates[2][3] - bitrates[0][0], used_bitrate);

    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 0,
            temporal: 3,
        },
        true,
        false,
    );
    assert!(is_candidate);
    assert_eq!(bitrates[0][3] - bitrates[2][3], used_bitrate);

    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 1,
            temporal: 2,
        },
        true,
        false,
    );
    assert!(is_candidate);
    assert_eq!(bitrates[1][2] - bitrates[0][3], used_bitrate);

    // available not enough to reach (2, 2), allocating at (2, 2) should not succeed
    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][2] - bitrates[1][2] - 1,
        buffer::VideoLayer {
            spatial: 2,
            temporal: 2,
        },
        true,
        false,
    );
    assert!(!is_candidate);
    assert_eq!(0i64, used_bitrate);

    // committing should set target to (1, 2)
    let expected_target_layer = buffer::VideoLayer {
        spatial: 1,
        temporal: 2,
    };
    let expected_result = VideoAllocation {
        is_deficient: true,
        bandwidth_requested: bitrates[1][2],
        bandwidth_delta: bitrates[1][2],
        bandwidth_needed: bitrates[2][3],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 1.25,
        ..Default::default()
    };
    let result = f.provisional_allocate_commit();
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());

    // when nothing fits and pausing disallowed, should allocate (0, 0)
    f.lock.write().vls.set_target(buffer::INVALID_LAYER);
    f.provisional_allocate_prepare(&[], bitrates);
    let (is_candidate, used_bitrate) = f.provisional_allocate(
        0,
        buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
        false,
        false,
    );
    assert!(is_candidate);
    assert_eq!(1i64, used_bitrate);

    // committing should set target to (0, 0)
    let expected_target_layer = buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    };
    let expected_result = VideoAllocation {
        is_deficient: true,
        bandwidth_requested: bitrates[0][0],
        bandwidth_delta: bitrates[0][0] - bitrates[1][2],
        bandwidth_needed: bitrates[2][3],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 2.75,
        ..Default::default()
    };
    let result = f.provisional_allocate_commit();
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());

    //
    // Test allowOvershoot.
    // Max spatial set to 0 and layer 0 bit rates are not available.
    //
    f.set_max_spatial_layer(0);
    let bitrates: Bitrates = [[0, 0, 0, 0], [5, 6, 7, 8], [9, 10, 11, 12]];

    f.provisional_allocate_prepare(&[], bitrates);

    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
        false,
        true,
    );
    assert!(!is_candidate);
    assert_eq!(0i64, used_bitrate);

    // overshoot should succeed
    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 2,
            temporal: 3,
        },
        false,
        true,
    );
    assert!(is_candidate);
    assert_eq!(bitrates[2][3], used_bitrate);

    // overshoot should succeed - this should win as this is lesser overshoot
    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 1,
            temporal: 3,
        },
        false,
        true,
    );
    assert!(is_candidate);
    assert_eq!(bitrates[1][3] - bitrates[2][3], used_bitrate);

    // committing should set target to (1, 3)
    let expected_target_layer = buffer::VideoLayer {
        spatial: 1,
        temporal: 3,
    };
    let expected_max_layer = buffer::VideoLayer {
        spatial: 0,
        temporal: 3,
    };
    let expected_result = VideoAllocation {
        bandwidth_requested: bitrates[1][3],
        bandwidth_delta: bitrates[1][3] - 1, // 1 is the last allocation bandwidth requested
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: expected_max_layer,
        distance_to_desired: -1.75,
        ..Default::default()
    };
    let result = f.provisional_allocate_commit();
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());

    //
    // Even if overshoot is allowed, but if higher layers do not have bit rates, should continue with current layer.
    //
    let bitrates: Bitrates = [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]];

    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 2,
    });
    f.provisional_allocate_prepare(&[], bitrates);

    // all the provisional allocations should not succeed because the feed is dry
    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
        false,
        true,
    );
    assert!(!is_candidate);
    assert_eq!(0i64, used_bitrate);

    // overshoot should not succeed
    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 2,
            temporal: 3,
        },
        false,
        true,
    );
    assert!(!is_candidate);
    assert_eq!(0i64, used_bitrate);

    // overshoot should not succeed
    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 1,
            temporal: 3,
        },
        false,
        true,
    );
    assert!(!is_candidate);
    assert_eq!(0i64, used_bitrate);

    // committing should set target to (0, 2), i. e. leave it at current for opportunistic forwarding
    let expected_target_layer = buffer::VideoLayer {
        spatial: 0,
        temporal: 2,
    };
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::FeedDry,
        bandwidth_requested: bitrates[0][2],
        bandwidth_delta: bitrates[0][2] - 8, // 8 is the last allocation bandwidth requested
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: expected_max_layer,
        distance_to_desired: 1.0,
        ..Default::default()
    };
    let result = f.provisional_allocate_commit();
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());

    //
    // Same case as above, but current is above max, so target should go to invalid
    //
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 1,
        temporal: 2,
    });
    f.provisional_allocate_prepare(&[], bitrates);

    // all the provisional allocations below should not succeed because the feed is dry
    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
        false,
        true,
    );
    assert!(!is_candidate);
    assert_eq!(0i64, used_bitrate);

    // overshoot should not succeed
    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 2,
            temporal: 3,
        },
        false,
        true,
    );
    assert!(!is_candidate);
    assert_eq!(0i64, used_bitrate);

    // overshoot should not succeed
    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 1,
            temporal: 3,
        },
        false,
        true,
    );
    assert!(!is_candidate);
    assert_eq!(0i64, used_bitrate);

    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::FeedDry,
        bandwidth_requested: 0,
        bandwidth_delta: 0,
        bitrates,
        target_layer: buffer::INVALID_LAYER,
        request_layer_spatial: buffer::INVALID_LAYER_SPATIAL,
        max_layer: expected_max_layer,
        distance_to_desired: 1.0,
        ..Default::default()
    };
    let result = f.provisional_allocate_commit();
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(buffer::INVALID_LAYER, f.target_layer());
    assert_eq!(buffer::INVALID_LAYER, f.current_layer());
}

#[test]
fn test_forwarder_provisional_allocate_mute() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);

    // Reset to invalid layers for testing muted state
    disable(&f);

    let bitrates: Bitrates = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]];

    f.mute(true, true);
    f.provisional_allocate_prepare(&[], bitrates);

    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
        true,
        false,
    );
    assert!(!is_candidate);
    assert_eq!(0i64, used_bitrate);

    let (is_candidate, used_bitrate) = f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 1,
            temporal: 2,
        },
        true,
        true,
    );
    assert!(!is_candidate);
    assert_eq!(0i64, used_bitrate);

    // committing should set target to buffer.InvalidLayer as track is muted
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::Muted,
        bandwidth_requested: 0,
        bandwidth_delta: 0,
        bitrates,
        target_layer: buffer::INVALID_LAYER,
        request_layer_spatial: buffer::INVALID_LAYER_SPATIAL,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 0.0,
        ..Default::default()
    };
    let result = f.provisional_allocate_commit();
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(buffer::INVALID_LAYER, f.target_layer());
}

#[test]
fn test_forwarder_provisional_allocate_get_cooperative_transition() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);
    f.set_max_published_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer_seen(buffer::DEFAULT_MAX_LAYER_TEMPORAL);

    // Reset to invalid layers for testing cooperative transition from scratch
    disable(&f);

    let available_layers: Vec<i32> = vec![0, 1, 2];
    let bitrates: Bitrates = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 0, 0]];

    f.provisional_allocate_prepare(&available_layers, bitrates);

    // from scratch (buffer.InvalidLayer) should give back layer (0, 0)
    let expected_transition = VideoTransition {
        from: buffer::INVALID_LAYER,
        to: buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
        bandwidth_delta: 1,
    };
    let (transition, al, brs) = f.provisional_allocate_get_cooperative_transition(false);
    assert_eq!(expected_transition, transition);
    assert_eq!(available_layers, al);
    assert_eq!(bitrates, brs);

    // committing should set target to (0, 0)
    let expected_layers = buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    };
    let expected_result = VideoAllocation {
        is_deficient: true,
        bandwidth_requested: 1,
        bandwidth_delta: 1,
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_layers,
        request_layer_spatial: expected_layers.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 2.25,
        ..Default::default()
    };
    let result = f.provisional_allocate_commit();
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_layers, f.target_layer());

    // a higher target that is already streaming, just maintain it
    let target_layer = buffer::VideoLayer {
        spatial: 2,
        temporal: 1,
    };
    f.lock.write().vls.set_target(target_layer);
    f.lock.write().last_allocation.bandwidth_requested = 10;
    let expected_transition = VideoTransition {
        from: target_layer,
        to: target_layer,
        bandwidth_delta: 0,
    };
    let (transition, al, brs) = f.provisional_allocate_get_cooperative_transition(false);
    assert_eq!(expected_transition, transition);
    assert_eq!(available_layers, al);
    assert_eq!(bitrates, brs);

    // committing should set target to (2, 1)
    let expected_layers = buffer::VideoLayer {
        spatial: 2,
        temporal: 1,
    };
    let expected_result = VideoAllocation {
        bandwidth_requested: 10,
        bandwidth_delta: 0,
        bitrates,
        bandwidth_needed: bitrates[2][1],
        target_layer: expected_layers,
        request_layer_spatial: expected_layers.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 0.0,
        ..Default::default()
    };
    let result = f.provisional_allocate_commit();
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_layers, f.target_layer());

    // from a target that has become unavailable, should switch to lower available layer
    let target_layer = buffer::VideoLayer {
        spatial: 2,
        temporal: 2,
    };
    f.lock.write().vls.set_target(target_layer);
    let expected_transition = VideoTransition {
        from: target_layer,
        to: buffer::VideoLayer {
            spatial: 2,
            temporal: 1,
        },
        bandwidth_delta: 0,
    };
    let (transition, al, brs) = f.provisional_allocate_get_cooperative_transition(false);
    assert_eq!(expected_transition, transition);
    assert_eq!(available_layers, al);
    assert_eq!(bitrates, brs);

    f.provisional_allocate_commit();

    // mute
    f.mute(true, true);
    f.provisional_allocate_prepare(&available_layers, bitrates);

    // mute should send target to buffer.InvalidLayer
    let expected_transition = VideoTransition {
        from: buffer::VideoLayer {
            spatial: 2,
            temporal: 1,
        },
        to: buffer::INVALID_LAYER,
        bandwidth_delta: -10,
    };
    let (transition, al, brs) = f.provisional_allocate_get_cooperative_transition(false);
    assert_eq!(expected_transition, transition);
    assert_eq!(available_layers, al);
    assert_eq!(bitrates, brs);

    f.provisional_allocate_commit();

    //
    // Test allowOvershoot
    //
    f.mute(false, true);
    f.set_max_spatial_layer(0);

    let available_layers: Vec<i32> = vec![1, 2];
    let bitrates: Bitrates = [[0, 0, 0, 0], [5, 6, 7, 8], [9, 10, 0, 0]];

    f.lock.write().vls.set_target(buffer::INVALID_LAYER);
    f.provisional_allocate_prepare(&available_layers, bitrates);

    // from scratch (buffer.InvalidLayer) should go to a layer past maximum as overshoot is allowed
    let expected_transition = VideoTransition {
        from: buffer::INVALID_LAYER,
        to: buffer::VideoLayer {
            spatial: 1,
            temporal: 0,
        },
        bandwidth_delta: 5,
    };
    let (transition, al, brs) = f.provisional_allocate_get_cooperative_transition(true);
    assert_eq!(expected_transition, transition);
    assert_eq!(available_layers, al);
    assert_eq!(bitrates, brs);

    // committing should set target to (1, 0)
    let expected_layers = buffer::VideoLayer {
        spatial: 1,
        temporal: 0,
    };
    let expected_max_layer = buffer::VideoLayer {
        spatial: 0,
        temporal: buffer::DEFAULT_MAX_LAYER_TEMPORAL,
    };
    let expected_result = VideoAllocation {
        bandwidth_requested: 5,
        bandwidth_delta: 5,
        bitrates,
        target_layer: expected_layers,
        request_layer_spatial: expected_layers.spatial,
        max_layer: expected_max_layer,
        distance_to_desired: -1.0,
        ..Default::default()
    };
    let result = f.provisional_allocate_commit();
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_layers, f.target_layer());

    //
    // Test continuing at current layers when feed is dry
    //
    let bitrates: Bitrates = [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]];

    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 2,
    });
    f.lock.write().vls.set_target(buffer::INVALID_LAYER);
    f.provisional_allocate_prepare(&[], bitrates);

    // from scratch (buffer.InvalidLayer) should go to current layer
    // NOTE: targetLayer is set to buffer.InvalidLayer for testing, but in practice current layers valid and target layers invalid should not happen
    let expected_transition = VideoTransition {
        from: buffer::INVALID_LAYER,
        to: buffer::VideoLayer {
            spatial: 0,
            temporal: 2,
        },
        bandwidth_delta: -5, // 5 was the bandwidth needed for the last allocation
    };
    let (transition, al, brs) = f.provisional_allocate_get_cooperative_transition(true);
    assert_eq!(expected_transition, transition);
    assert_eq!(Vec::<i32>::new(), al);
    assert_eq!(bitrates, brs);

    // committing should set target to (0, 2)
    let expected_layers = buffer::VideoLayer {
        spatial: 0,
        temporal: 2,
    };
    let expected_result = VideoAllocation {
        bandwidth_requested: 0,
        bandwidth_delta: -5,
        bitrates,
        target_layer: expected_layers,
        request_layer_spatial: expected_layers.spatial,
        max_layer: expected_max_layer,
        distance_to_desired: -0.5,
        ..Default::default()
    };
    let result = f.provisional_allocate_commit();
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_layers, f.target_layer());

    // committing should set target to current layers to enable opportunistic forwarding
    let expected_result = VideoAllocation {
        bandwidth_requested: 0,
        bandwidth_delta: 0,
        bitrates,
        target_layer: expected_layers,
        request_layer_spatial: expected_layers.spatial,
        max_layer: expected_max_layer,
        distance_to_desired: -0.5,
        ..Default::default()
    };
    let result = f.provisional_allocate_commit();
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_layers, f.target_layer());
}

#[test]
fn test_forwarder_provisional_allocate_get_best_weighted_transition() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);

    let available_layers: Vec<i32> = vec![0, 1, 2];
    let bitrates: Bitrates = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]];

    f.provisional_allocate_prepare(&available_layers, bitrates);

    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 2,
        temporal: 2,
    });
    f.lock.write().last_allocation.bandwidth_requested = bitrates[2][2];
    let expected_transition = VideoTransition {
        from: f.target_layer(),
        to: buffer::VideoLayer {
            spatial: 2,
            temporal: 0,
        },
        bandwidth_delta: -2,
    };
    let (transition, al, brs) = f.provisional_allocate_get_best_weighted_transition();
    assert_eq!(expected_transition, transition);
    assert_eq!(available_layers, al);
    assert_eq!(bitrates, brs);
}

#[test]
fn test_forwarder_allocate_next_higher() {
    let f = new_forwarder(&testutils::test_opus_codec(), webrtc::RtpCodecType::Audio);
    f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);
    f.set_max_published_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);

    let empty_bitrates = Bitrates::default();
    let bitrates: Bitrates = [[2, 3, 0, 0], [4, 0, 0, 5], [0, 7, 0, 0]];

    let (result, boosted) = f.allocate_next_higher(100_000_000, &[], bitrates, false);
    assert_eq!(VIDEO_ALLOCATION_DEFAULT, result); // no layer for audio
    assert!(!boosted);

    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);
    f.set_max_published_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer_seen(buffer::DEFAULT_MAX_LAYER_TEMPORAL);

    // when not in deficient state, does not boost
    let (result, boosted) = f.allocate_next_higher(100_000_000, &[], bitrates, false);
    assert_eq!(VIDEO_ALLOCATION_DEFAULT, result);
    assert!(!boosted);

    // if layers have not caught up, should not allocate next layer even if deficient
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    let (result, boosted) = f.allocate_next_higher(100_000_000, &[], bitrates, false);
    assert_eq!(VIDEO_ALLOCATION_DEFAULT, result);
    assert!(!boosted);

    f.lock.write().last_allocation.is_deficient = true;
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });

    // move from (0, 0) -> (0, 1), i.e. a higher temporal layer is available in the same spatial layer
    let expected_target_layer = buffer::VideoLayer {
        spatial: 0,
        temporal: 1,
    };
    let expected_result = VideoAllocation {
        is_deficient: true,
        bandwidth_requested: 3,
        bandwidth_delta: 1,
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 2.0,
        ..Default::default()
    };
    let (result, boosted) = f.allocate_next_higher(100_000_000, &[], bitrates, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());
    assert!(boosted);

    // empty bitrates cannot increase layer, i. e. last allocation is left unchanged
    let (result, boosted) = f.allocate_next_higher(100_000_000, &[], empty_bitrates, false);
    assert_eq!(expected_result, result);
    assert!(!boosted);

    // move from (0, 1) -> (1, 0), i.e. a higher spatial layer is available
    let current_spatial = f.lock.read().vls.get_current().spatial;
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: current_spatial,
        temporal: 1,
    });
    let expected_target_layer = buffer::VideoLayer {
        spatial: 1,
        temporal: 0,
    };
    let expected_result = VideoAllocation {
        is_deficient: true,
        bandwidth_requested: 4,
        bandwidth_delta: 1,
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 1.25,
        ..Default::default()
    };
    let (result, boosted) = f.allocate_next_higher(100_000_000, &[], bitrates, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());
    assert!(boosted);

    // next higher, move from (1, 0) -> (1, 3), still deficient though
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 1,
        temporal: 0,
    });
    let expected_target_layer = buffer::VideoLayer {
        spatial: 1,
        temporal: 3,
    };
    let expected_result = VideoAllocation {
        is_deficient: true,
        bandwidth_requested: 5,
        bandwidth_delta: 1,
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 0.5,
        ..Default::default()
    };
    let (result, boosted) = f.allocate_next_higher(100_000_000, &[], bitrates, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());
    assert!(boosted);

    // next higher, move from (1, 3) -> (2, 1), optimal allocation
    let current_spatial = f.lock.read().vls.get_current().spatial;
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: current_spatial,
        temporal: 3,
    });
    let expected_target_layer = buffer::VideoLayer {
        spatial: 2,
        temporal: 1,
    };
    let expected_result = VideoAllocation {
        bandwidth_requested: 7,
        bandwidth_delta: 2,
        bitrates,
        bandwidth_needed: bitrates[2][1],
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 0.0,
        ..Default::default()
    };
    let (result, boosted) = f.allocate_next_higher(100_000_000, &[], bitrates, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());
    assert!(boosted);

    // ask again, should return not boosted as there is no room to go higher
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 2,
        temporal: 1,
    });
    let (result, boosted) = f.allocate_next_higher(100_000_000, &[], bitrates, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());
    assert!(!boosted);

    // turn off everything, allocating next layer should result in streaming lowest layers
    disable(&f);
    f.lock.write().last_allocation.is_deficient = true;
    f.lock.write().last_allocation.bandwidth_requested = 0;

    let expected_target_layer = buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    };
    let expected_result = VideoAllocation {
        is_deficient: true,
        bandwidth_requested: 2,
        bandwidth_delta: 2,
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 2.25,
        ..Default::default()
    };
    let (result, boosted) = f.allocate_next_higher(100_000_000, &[], bitrates, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());
    assert!(boosted);

    // no new available capacity cannot bump up layer
    let expected_result = VideoAllocation {
        is_deficient: true,
        bandwidth_requested: 2,
        bandwidth_delta: 2,
        bandwidth_needed: bitrates[2][1],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 2.25,
        ..Default::default()
    };
    let (result, boosted) = f.allocate_next_higher(0, &[], bitrates, false);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());
    assert!(!boosted);

    // test allowOvershoot
    f.set_max_spatial_layer(0);

    let bitrates: Bitrates = [[0, 0, 0, 0], [5, 6, 7, 8], [9, 10, 11, 12]];

    let target = f.lock.read().vls.get_target();
    f.lock.write().vls.set_current(target);

    let expected_target_layer = buffer::VideoLayer {
        spatial: 1,
        temporal: 0,
    };
    let expected_max_layer = buffer::VideoLayer {
        spatial: 0,
        temporal: buffer::DEFAULT_MAX_LAYER_TEMPORAL,
    };
    let expected_result = VideoAllocation {
        bandwidth_requested: bitrates[1][0],
        bandwidth_delta: bitrates[1][0],
        bitrates,
        target_layer: expected_target_layer,
        request_layer_spatial: expected_target_layer.spatial,
        max_layer: expected_max_layer,
        distance_to_desired: -1.0,
        ..Default::default()
    };
    // overshoot should return (1, 0) even if there is not enough capacity
    let (result, boosted) = f.allocate_next_higher(bitrates[1][0] - 1, &[], bitrates, true);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(expected_target_layer, f.target_layer());
    assert!(boosted);
}

#[test]
fn test_forwarder_pause() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);
    f.set_max_published_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer_seen(buffer::DEFAULT_MAX_LAYER_TEMPORAL);

    let bitrates: Bitrates = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]];

    f.provisional_allocate_prepare(&[], bitrates);
    f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
        true,
        false,
    );
    // should have set target at (0, 0)
    f.provisional_allocate_commit();

    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::Bandwidth,
        is_deficient: true,
        bandwidth_requested: 0,
        bandwidth_delta: 0 - bitrates[0][0],
        bandwidth_needed: bitrates[2][3],
        bitrates,
        target_layer: buffer::INVALID_LAYER,
        request_layer_spatial: buffer::INVALID_LAYER_SPATIAL,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 3.75,
    };
    let result = f.pause(&[], bitrates);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(buffer::INVALID_LAYER, f.target_layer());
}

#[test]
fn test_forwarder_pause_mute() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);
    f.set_max_published_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);

    let bitrates: Bitrates = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]];

    f.provisional_allocate_prepare(&[], bitrates);
    f.provisional_allocate(
        bitrates[2][3],
        buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
        true,
        true,
    );
    // should have set target at (0, 0)
    f.provisional_allocate_commit();

    f.mute(true, true);
    let expected_result = VideoAllocation {
        pause_reason: VideoPauseReason::Muted,
        bandwidth_requested: 0,
        bandwidth_delta: 0 - bitrates[0][0],
        bitrates,
        target_layer: buffer::INVALID_LAYER,
        request_layer_spatial: buffer::INVALID_LAYER_SPATIAL,
        max_layer: buffer::DEFAULT_MAX_LAYER,
        distance_to_desired: 0.0,
        ..Default::default()
    };
    let result = f.pause(&[], bitrates);
    assert_eq!(expected_result, result);
    assert_eq!(expected_result, f.lock.read().last_allocation);
    assert_eq!(buffer::INVALID_LAYER, f.target_layer());
}

#[test]
fn test_forwarder_get_translation_params_muted() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    f.mute(true, true);

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params);
    assert!(ext_pkt.is_ok());
    let ext_pkt = ext_pkt.unwrap();

    let expected_tp = TranslationParams {
        should_drop: true,
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);
}

#[test]
fn test_forwarder_get_translation_params_audio() {
    let f = new_forwarder(&testutils::test_opus_codec(), webrtc::RtpCodecType::Audio);

    let params = testutils::TestExtPacketParams {
        sequence_number: 23332,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        is_out_of_order: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    // should not start on an out-of-order packet
    let expected_tp = TranslationParams {
        should_drop: true,
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);
    assert!(!f.lock.read().started);
    assert_eq!(0, f.lock.read().last_ssrc);

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    // should lock onto the first in-order packet
    let expected_tp = TranslationParams {
        is_starting: true,
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Contiguous,
            ext_sequence_number: 23333,
            ext_timestamp: 0xabcdef,
        },
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);
    assert!(f.lock.read().started);
    assert_eq!(f.lock.read().last_ssrc, params.ssrc);

    // send a duplicate, should be dropped
    let expected_tp = TranslationParams {
        should_drop: true,
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // add a missing sequence number to the cache
    let err = f
        .lock
        .write()
        .rtp_munger
        .sn_range_map
        .exclude_range(23334, 23335);
    assert!(err.is_ok());

    let params = testutils::TestExtPacketParams {
        sequence_number: 23336,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let (_, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);

    // out-of-order packet should get offset from cache
    let params = testutils::TestExtPacketParams {
        sequence_number: 23335,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let expected_tp = TranslationParams {
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::OutOfOrder,
            ext_sequence_number: 23334,
            ext_timestamp: 0xabcdef,
        },
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // padding only packet in order should be dropped
    let params = testutils::TestExtPacketParams {
        sequence_number: 23337,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let expected_tp = TranslationParams {
        should_drop: true,
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // in order packet should be forwarded
    let params = testutils::TestExtPacketParams {
        sequence_number: 23338,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let expected_tp = TranslationParams {
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Contiguous,
            ext_sequence_number: 23336,
            ext_timestamp: 0xabcdef,
        },
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // padding only packet after a gap should not be dropped
    let params = testutils::TestExtPacketParams {
        sequence_number: 23340,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let expected_tp = TranslationParams {
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Gap,
            ext_sequence_number: 23338,
            ext_timestamp: 0xabcdef,
        },
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // out-of-order should be forwarded using cache
    let params = testutils::TestExtPacketParams {
        sequence_number: 23336,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let expected_tp = TranslationParams {
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::OutOfOrder,
            ext_sequence_number: 23335,
            ext_timestamp: 0xabcdef,
        },
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // switching source should lock onto the new source, but sequence number should be contiguous
    let params = testutils::TestExtPacketParams {
        sequence_number: 123,
        timestamp: 0xfedcba,
        ssrc: 0x87654321,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let expected_tp = TranslationParams {
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Contiguous,
            ext_sequence_number: 23339,
            ext_timestamp: 0xabcdf0,
        },
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);
    assert_eq!(f.lock.read().last_ssrc, params.ssrc);
}

#[test]
fn test_forwarder_get_translation_params_video() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);

    let params = testutils::TestExtPacketParams {
        sequence_number: 23332,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        marker: true,
        is_out_of_order: true,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: false,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    // should not start on an out-of-order packet
    let expected_tp = TranslationParams {
        should_drop: true,
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);
    assert!(!f.lock.read().started);
    assert_eq!(0, f.lock.read().last_ssrc);

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        marker: true,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: false,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    // no target layers, should drop
    let expected_tp = TranslationParams {
        should_drop: true,
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // although target layer matches, not a key frame, so should drop
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 1,
    });
    let expected_tp = TranslationParams {
        should_drop: true,
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // should lock onto packet (key frame)
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    let expected_vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let marshalled_vp8 = expected_vp8.marshal().unwrap();
    let expected_tp = TranslationParams {
        is_starting: true,
        is_switching: true,
        is_resuming: true,
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Contiguous,
            ext_sequence_number: 23333,
            ext_timestamp: 0xabcdef,
        },
        incoming_header_size: 6,
        codec_bytes: codec_header_of(&marshalled_vp8),
        codec_bytes_len: marshalled_vp8.len(),
        marker: true,
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);
    assert!(f.lock.read().started);
    assert_eq!(f.lock.read().last_ssrc, params.ssrc);

    // send a duplicate, should be dropped
    let expected_tp = TranslationParams {
        should_drop: true,
        marker: true,
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // out-of-order packet not in cache should be dropped
    let params = testutils::TestExtPacketParams {
        sequence_number: 23332,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    let expected_tp = TranslationParams {
        should_drop: true,
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // padding only packet in order should be dropped
    let params = testutils::TestExtPacketParams {
        sequence_number: 23334,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    let expected_tp = TranslationParams {
        should_drop: true,
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // in order packet should be forwarded
    let params = testutils::TestExtPacketParams {
        sequence_number: 23335,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    let expected_vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    let marshalled_vp8 = marshalled_vp8.unwrap();
    let expected_tp = TranslationParams {
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Contiguous,
            ext_sequence_number: 23334,
            ext_timestamp: 0xabcdef,
        },
        incoming_header_size: 6,
        codec_bytes: codec_header_of(&marshalled_vp8),
        codec_bytes_len: marshalled_vp8.len(),
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // temporal layer matching target, should be forwarded
    let params = testutils::TestExtPacketParams {
        sequence_number: 23336,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        s: true,
        i: true,
        m: true,
        picture_id: 13468,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 1,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    let expected_vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13468,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 1,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    let marshalled_vp8 = marshalled_vp8.unwrap();
    let expected_tp = TranslationParams {
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Contiguous,
            ext_sequence_number: 23335,
            ext_timestamp: 0xabcdef,
        },
        incoming_header_size: 6,
        codec_bytes: codec_header_of(&marshalled_vp8),
        codec_bytes_len: marshalled_vp8.len(),
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // temporal layer higher than target, should be dropped
    let params = testutils::TestExtPacketParams {
        sequence_number: 23337,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13468,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 2,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    let expected_tp = TranslationParams {
        should_drop: true,
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Contiguous,
            ext_sequence_number: 23336,
            ext_timestamp: 0xabcdef,
        },
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // RTP sequence number and VP8 picture id should be contiguous after dropping higher temporal layer picture
    let params = testutils::TestExtPacketParams {
        sequence_number: 23338,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13469,
        l: true,
        tl0picidx: 234,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: false,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    let expected_vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13469,
        l: true,
        tl0picidx: 234,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: false,
        ..Default::default()
    };
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    let marshalled_vp8 = marshalled_vp8.unwrap();
    let expected_tp = TranslationParams {
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Contiguous,
            ext_sequence_number: 23336,
            ext_timestamp: 0xabcdef,
        },
        incoming_header_size: 6,
        codec_bytes: codec_header_of(&marshalled_vp8),
        codec_bytes_len: marshalled_vp8.len(),
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // padding only packet after a gap should be forwarded
    let params = testutils::TestExtPacketParams {
        sequence_number: 23340,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let expected_tp = TranslationParams {
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Gap,
            ext_sequence_number: 23338,
            ext_timestamp: 0xabcdef,
        },
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // out-of-order should be forwarded using cache, even if it is padding only
    let params = testutils::TestExtPacketParams {
        sequence_number: 23339,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let expected_tp = TranslationParams {
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::OutOfOrder,
            ext_sequence_number: 23337,
            ext_timestamp: 0xabcdef,
        },
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 0);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);

    // switching SSRC (happens for new layer or new track source)
    // should lock onto the new source, but sequence number should be contiguous
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 1,
        temporal: 1,
    });

    let params = testutils::TestExtPacketParams {
        sequence_number: 123,
        timestamp: 0xfedcba,
        ssrc: 0x87654321,
        payload_size: 20,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: false,
        picture_id: 45,
        l: true,
        tl0picidx: 12,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 30,
        header_size: 5,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    let expected_vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13470,
        l: true,
        tl0picidx: 235,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 24,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    let marshalled_vp8 = marshalled_vp8.unwrap();
    let expected_tp = TranslationParams {
        is_switching: true,
        rtp: TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Contiguous,
            ext_sequence_number: 23339,
            ext_timestamp: 0xabcdf0,
        },
        incoming_header_size: 5,
        codec_bytes: codec_header_of(&marshalled_vp8),
        codec_bytes_len: marshalled_vp8.len(),
        ..Default::default()
    };
    let (actual_tp, err) = f.get_translation_params(&ext_pkt, 1);
    assert_eq!(Ok(()), err);
    assert_eq!(expected_tp, actual_tp);
    assert_eq!(f.lock.read().last_ssrc, params.ssrc);
}

#[test]
fn test_forwarder_vp8_temporal_up_switch_at_layer_sync() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 2,
    });

    // a frame without Y may reference a dropped frame of its layer
    struct Step {
        name: &'static str,
        tid: u8,
        y: bool,
        expected_this: i32,
        expected_current: i32,
    }
    #[rustfmt::skip]
    let steps = [
        Step { name: "TL2 without Y", tid: 2, y: false, expected_this: 0, expected_current: 0 },
        Step { name: "TL1 without Y", tid: 1, y: false, expected_this: 0, expected_current: 0 },
        Step { name: "TL0", tid: 0, y: false, expected_this: 0, expected_current: 0 },
        Step { name: "TL2 with Y before TL1 sync", tid: 2, y: true, expected_this: 0, expected_current: 0 },
        Step { name: "TL1 with Y", tid: 1, y: true, expected_this: 1, expected_current: 1 },
        Step { name: "TL2 without Y after TL1 switch", tid: 2, y: false, expected_this: 1, expected_current: 1 },
        Step { name: "TL2 with Y", tid: 2, y: true, expected_this: 2, expected_current: 2 },
    ];
    for (i, step) in steps.iter().enumerate() {
        let ext_pkt = testutils::get_test_ext_packet_vp8(
            &testutils::TestExtPacketParams {
                sequence_number: i as u16,
                payload_size: 20,
                ..Default::default()
            },
            &codec::Vp8 {
                s: true,
                i: true,
                picture_id: i as u16,
                t: true,
                tid: step.tid,
                y: step.y,
                ..Default::default()
            },
        );
        assert!(ext_pkt.is_ok());
        let ext_pkt = ext_pkt.unwrap();
        assert_eq!(
            step.expected_this,
            f.lock.write().vls.select_temporal(&ext_pkt),
            "{}",
            step.name
        );
        assert_eq!(
            step.expected_current,
            f.lock.read().vls.get_current().temporal,
            "{}",
            step.name
        );
    }

    // key frame refreshes all buffers, up-switch at it even without Y
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    let ext_pkt = testutils::get_test_ext_packet_vp8(
        &testutils::TestExtPacketParams {
            sequence_number: 10,
            payload_size: 20,
            ..Default::default()
        },
        &codec::Vp8 {
            s: true,
            i: true,
            picture_id: 10,
            t: true,
            tid: 0,
            is_key_frame: true,
            ..Default::default()
        },
    );
    assert!(ext_pkt.is_ok());
    let ext_pkt = ext_pkt.unwrap();
    assert_eq!(2i32, f.lock.write().vls.select_temporal(&ext_pkt));
    assert_eq!(2i32, f.lock.read().vls.get_current().temporal);
}

#[test]
fn test_forwarder_vp8_temporal_up_switch_with_dependency_descriptor() {
    let f = Forwarder::new(
        webrtc::RtpCodecType::Video,
        logger::get_logger(),
        true,
        true,
        true,
        None,
    );
    f.determine_codec(
        &testutils::test_vp8_codec(),
        &dd_extensions(),
        livekit::video_layer::Mode::Unused,
    );
    assert!(
        f.lock
            .read()
            .codec_munger
            .as_any()
            .is::<codecmunger::DdTemporal>()
    );
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 2,
    });

    // libwebrtc strips the VP8 payload descriptor when it sends the dependency
    // descriptor: no TID and no layer sync bit
    let packet = |frame_num: u64,
                  tid: i64,
                  dtis: &str,
                  frame_diffs: &[i64],
                  is_key_frame: bool|
     -> buffer::ExtPacket {
        let ext_pkt = testutils::get_test_ext_packet_vp8_dd(
            &testutils::TestExtPacketParams {
                sequence_number: frame_num as u16,
                payload_size: 20,
                marker: true,
                is_key_frame,
                ..Default::default()
            },
            &testutils::TestDdParams {
                first_packet_in_frame: true,
                ext_frame_num: frame_num,
                temporal_id: tid,
                dtis: dtis.to_string(),
                frame_diffs: frame_diffs.to_vec(),
                decode_targets: testutils::TEST_VP8_DECODE_TARGETS.to_vec(),
            },
        );
        assert!(ext_pkt.is_ok());
        ext_pkt.unwrap()
    };

    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    assert_eq!(
        0i32,
        f.lock
            .write()
            .vls
            .select_temporal(&packet(0, 0, "SSS", &[], true))
    );
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 2,
    });
    assert_eq!(
        0i32,
        f.lock
            .write()
            .vls
            .select_temporal(&packet(1, 2, "--D", &[1], false))
    );
    assert_eq!(0i32, f.lock.read().vls.get_current().temporal);
    assert_eq!(
        2i32,
        f.lock
            .write()
            .vls
            .select_temporal(&packet(2, 1, "-SS", &[2], false))
    );
    assert_eq!(2i32, f.lock.read().vls.get_current().temporal);

    // padding never carries the descriptor and leaves it on
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    let padding = testutils::get_test_ext_packet(&testutils::TestExtPacketParams {
        sequence_number: 3,
        padding_size: 20,
        ..Default::default()
    });
    assert!(padding.is_ok());
    let padding = padding.unwrap();
    let _ = f.get_translation_params_video(&mut f.lock.write(), &padding, 0);
    assert!(f.lock.read().is_dd_available);

    // a packet with a payload and without the descriptor means the buffer stopped
    // parsing it, the Y bit then raises the layer
    let ext_pkt = testutils::get_test_ext_packet_vp8(
        &testutils::TestExtPacketParams {
            sequence_number: 4,
            payload_size: 20,
            ..Default::default()
        },
        &codec::Vp8 {
            s: true,
            i: true,
            picture_id: 4,
            t: true,
            tid: 0,
            ..Default::default()
        },
    );
    assert!(ext_pkt.is_ok());
    let ext_pkt = ext_pkt.unwrap();
    let _ = f.get_translation_params_video(&mut f.lock.write(), &ext_pkt, 0);
    assert!(!f.lock.read().is_dd_available);
    assert!(f.lock.read().codec_munger.as_any().is::<codecmunger::Vp8>());

    let ext_pkt = testutils::get_test_ext_packet_vp8(
        &testutils::TestExtPacketParams {
            sequence_number: 5,
            payload_size: 20,
            ..Default::default()
        },
        &codec::Vp8 {
            s: true,
            i: true,
            picture_id: 5,
            t: true,
            tid: 1,
            y: true,
            ..Default::default()
        },
    );
    assert!(ext_pkt.is_ok());
    let ext_pkt = ext_pkt.unwrap();
    assert_eq!(1i32, f.lock.write().vls.select_temporal(&ext_pkt));
    assert_eq!(1i32, f.lock.read().vls.get_current().temporal);
}

#[test]
fn test_forwarder_vp8_dd_temporal_filter_after_gap() {
    let f = Forwarder::new(
        webrtc::RtpCodecType::Video,
        logger::get_logger(),
        true,
        true,
        true,
        None,
    );
    f.determine_codec(
        &testutils::test_vp8_codec(),
        &dd_extensions(),
        livekit::video_layer::Mode::Unused,
    );
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });

    // one packet per frame, the minimized VP8 payload descriptor has no picture ID
    let packet = |sn: u16, frame_num: u64, tid: i64, is_key_frame: bool| -> buffer::ExtPacket {
        let ext_pkt = testutils::get_test_ext_packet_vp8_dd(
            &testutils::TestExtPacketParams {
                sequence_number: sn,
                timestamp: (frame_num as u32).wrapping_mul(3000),
                ssrc: 0x12345678,
                payload_size: 20,
                marker: true,
                is_key_frame,
                ..Default::default()
            },
            &testutils::TestDdParams {
                first_packet_in_frame: true,
                ext_frame_num: frame_num,
                temporal_id: tid,
                dtis: "SSS".to_string(),
                decode_targets: testutils::TEST_VP8_DECODE_TARGETS.to_vec(),
                ..Default::default()
            },
        );
        assert!(ext_pkt.is_ok());
        ext_pkt.unwrap()
    };

    struct Step {
        name: &'static str,
        sn: u16,
        frame_num: u64,
        tid: i64,
        is_key_frame: bool,
        should_drop: bool,
    }
    #[rustfmt::skip]
    let steps = [
        Step { name: "key frame", sn: 100, frame_num: 0, tid: 0, is_key_frame: true, should_drop: false },
        Step { name: "TL2 after a gap", sn: 102, frame_num: 2, tid: 2, is_key_frame: false, should_drop: true },
        Step { name: "TL2", sn: 103, frame_num: 3, tid: 2, is_key_frame: false, should_drop: true },
        Step { name: "TL0", sn: 104, frame_num: 4, tid: 0, is_key_frame: false, should_drop: false },
        Step { name: "TL2 after TL0", sn: 105, frame_num: 5, tid: 2, is_key_frame: false, should_drop: true },
    ];
    for step in &steps {
        let (tp, err) = f.get_translation_params(
            &packet(step.sn, step.frame_num, step.tid, step.is_key_frame),
            0,
        );
        assert_eq!(Ok(()), err, "{}", step.name);
        assert_eq!(step.should_drop, tp.should_drop, "{}", step.name);
    }
}

#[test]
fn test_forwarder_vp8_dd_with_picture_id() {
    let f = Forwarder::new(
        webrtc::RtpCodecType::Video,
        logger::get_logger(),
        true,
        true,
        true,
        None,
    );
    f.determine_codec(
        &testutils::test_vp8_codec(),
        &dd_extensions(),
        livekit::video_layer::Mode::Unused,
    );
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 2,
    });
    assert!(
        f.lock
            .read()
            .codec_munger
            .as_any()
            .is::<codecmunger::DdTemporal>()
    );

    // the one byte descriptor keeps the dependency descriptor munger
    let ext_pkt = testutils::get_test_ext_packet_vp8_dd(
        &testutils::TestExtPacketParams {
            sequence_number: 1,
            timestamp: 3000,
            ssrc: 0x12345678,
            payload_size: 20,
            marker: true,
            is_key_frame: true,
            ..Default::default()
        },
        &testutils::TestDdParams {
            first_packet_in_frame: true,
            ext_frame_num: 1,
            dtis: "SSS".to_string(),
            decode_targets: testutils::TEST_VP8_DECODE_TARGETS.to_vec(),
            ..Default::default()
        },
    );
    assert!(ext_pkt.is_ok());
    let ext_pkt = ext_pkt.unwrap();
    let _ = f.get_translation_params(&ext_pkt, 0);
    assert!(
        f.lock
            .read()
            .codec_munger
            .as_any()
            .is::<codecmunger::DdTemporal>()
    );
    assert!(f.lock.read().started);

    // a picture ID next to the dependency descriptor needs the VP8 munger
    let ext_pkt = testutils::get_test_ext_packet_vp8_dd(
        &testutils::TestExtPacketParams {
            sequence_number: 2,
            timestamp: 6000,
            ssrc: 0x12345678,
            payload_size: 20,
            marker: true,
            ..Default::default()
        },
        &testutils::TestDdParams {
            first_packet_in_frame: true,
            ext_frame_num: 2,
            temporal_id: 1,
            dtis: "-SS".to_string(),
            frame_diffs: vec![1],
            decode_targets: testutils::TEST_VP8_DECODE_TARGETS.to_vec(),
        },
    );
    assert!(ext_pkt.is_ok());
    let mut ext_pkt = ext_pkt.unwrap();
    ext_pkt.vp8 = Some(codec::Vp8 {
        s: true,
        i: true,
        picture_id: 2,
        ..Default::default()
    });
    let _ = f.get_translation_params(&ext_pkt, 0);
    assert!(f.lock.read().codec_munger.as_any().is::<codecmunger::Vp8>());
    match f.lock.read().codec_munger.get_state() {
        Some(codecmunger::State::Vp8MungerState(state)) => {
            assert!(state.picture_id_used, "seeded from the packet");
        }
        state => panic!("not a *livekit.VP8MungerState: {state:?}"),
    }
    assert!(
        f.lock.read().is_dd_available,
        "temporal layers still switch with the dependency descriptor"
    );
}

// Go's `hex.DecodeString`.
#[track_caller]
fn hex_decode_string(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd length hex string");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("invalid byte in hex string"))
        .collect()
}

// testKeyFrameDescriptor is an L3T3 key frame descriptor with its dependency structure, the
// capture the dependency descriptor package tests with
fn test_key_frame_descriptor() -> dd::DependencyDescriptor {
    let raw = hex_decode_string(
        "c1017280081485214eafffaaaa863cf0430c10c302afc0aaa0063c00430010c002a000a80006000040001d954926e082b04a0941b820ac1282503157f974000ca864330e222222eca8655304224230eca877530077004200ef008601df010d",
    );
    let mut ext = dd::DependencyDescriptorExtension::default();
    let res = ext.unmarshal(&raw);
    assert!(res.is_ok());
    let key_frame_descriptor = ext.descriptor;
    assert!(key_frame_descriptor.attached_structure.is_some());
    key_frame_descriptor
}

// testDDPacket is a packet of frame frameNum on temporalID, with keyFrameDescriptor as its
// dependency descriptor, its structure attached only on a key frame
fn test_dd_packet(
    key_frame_descriptor: &dd::DependencyDescriptor,
    sn: u16,
    frame_num: u64,
    temporal_id: i64,
    is_key_frame: bool,
) -> buffer::ExtPacket {
    let ext_pkt = testutils::get_test_ext_packet_vp8_dd(
        &testutils::TestExtPacketParams {
            sequence_number: sn,
            timestamp: 0xabcdefu32.wrapping_add((frame_num as u32).wrapping_mul(3000)),
            ssrc: 0x12345678,
            payload_size: 20,
            marker: true,
            is_key_frame,
            ..Default::default()
        },
        &testutils::TestDdParams {
            first_packet_in_frame: true,
            ext_frame_num: frame_num,
            temporal_id,
            dtis: "SSS".to_string(),
            decode_targets: testutils::TEST_VP8_DECODE_TARGETS.to_vec(),
            ..Default::default()
        },
    );
    assert!(ext_pkt.is_ok());
    let mut ext_pkt = ext_pkt.unwrap();
    let mut descriptor = key_frame_descriptor.clone();
    descriptor.frame_number = frame_num as u16;
    if !is_key_frame {
        descriptor.attached_structure = None;
        descriptor.active_decode_targets_bitmask = None;
    }
    ext_pkt.dependency_descriptor.as_mut().unwrap().descriptor = Some(descriptor);
    ext_pkt
}

#[test]
fn test_forwarder_vp8_forwards_dependency_descriptor() {
    let key_frame_descriptor = test_key_frame_descriptor();
    let packet = |sn: u16, frame_num: u64, is_key_frame: bool| -> buffer::ExtPacket {
        test_dd_packet(&key_frame_descriptor, sn, frame_num, 0, is_key_frame)
    };

    let f = Forwarder::new(
        webrtc::RtpCodecType::Video,
        logger::get_logger(),
        true,
        true,
        true,
        None,
    );
    f.determine_codec(
        &testutils::test_vp8_codec(),
        &dd_extensions(),
        livekit::video_layer::Mode::Unused,
    );
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });

    // the key frame carries the structure, too big to hold inline
    let (tp, err) = f.get_translation_params(&packet(10, 100, true), 0);
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    assert!(!tp.dd_bytes_spill.is_empty());

    let (tp, err) = f.get_translation_params(&packet(11, 101, false), 0);
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    assert!(!tp.dd_inline().is_empty());

    // a restarted upstream receiver sets the codec again, the frame numbers continue on
    // the same downtrack
    f.determine_codec(
        &testutils::test_vp8_codec(),
        &dd_extensions(),
        livekit::video_layer::Mode::Unused,
    );
    let (tp, err) = f.get_translation_params(&packet(12, 50, true), 0);
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    let mut out = dd::DependencyDescriptorExtension {
        descriptor: dd::DependencyDescriptor::default(),
        structure: key_frame_descriptor.attached_structure.clone(),
    };
    let res = out.unmarshal(&tp.dd_bytes_spill);
    assert!(res.is_ok());
    assert_eq!(102u16, out.descriptor.frame_number);

    // the last packet of a TL2 frame that starts a down switch to TL0 is forwarded at TL2,
    // its descriptor names the TL2 decode target
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 2,
    });
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    let mut tl2 = packet(13, 51, false);
    tl2.video_layer.temporal = 2;
    let (tp, err) = f.get_translation_params(&tl2, 0);
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    assert_eq!(
        0i32,
        f.lock.read().vls.get_current().temporal,
        "the switch takes effect after this packet"
    );
    let mut out = dd::DependencyDescriptorExtension {
        descriptor: dd::DependencyDescriptor::default(),
        structure: key_frame_descriptor.attached_structure.clone(),
    };
    let res = out.unmarshal(tp.dd_inline());
    assert!(res.is_ok());
    assert_eq!(
        Some(1u32 | 1 << 1 | 1 << 2),
        out.descriptor.active_decode_targets_bitmask
    );

    // a migrated subscriber: the state is seeded before the codec is known, and the new
    // node's first key frame continues after the last number sent
    let state = f.get_state();
    assert_eq!(
        103u64,
        state
            .as_ref()
            .unwrap()
            .dependency_descriptor_forwarder
            .unwrap()
            .ext_last_frame_number
    );
    let f = Forwarder::new(
        webrtc::RtpCodecType::Video,
        logger::get_logger(),
        true,
        true,
        true,
        None,
    );
    f.seed_state(state.as_ref());

    // it migrates on before its codec is known, the next node still gets the number
    let state = f.get_state();
    assert_eq!(
        103u64,
        state
            .as_ref()
            .unwrap()
            .dependency_descriptor_forwarder
            .unwrap()
            .ext_last_frame_number
    );
    let f = Forwarder::new(
        webrtc::RtpCodecType::Video,
        logger::get_logger(),
        true,
        true,
        true,
        None,
    );
    f.seed_state(state.as_ref());
    f.determine_codec(
        &testutils::test_vp8_codec(),
        &dd_extensions(),
        livekit::video_layer::Mode::Unused,
    );
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    let (tp, err) = f.get_translation_params(&packet(20, 60, true), 0);
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    let mut out = dd::DependencyDescriptorExtension {
        descriptor: dd::DependencyDescriptor::default(),
        structure: key_frame_descriptor.attached_structure.clone(),
    };
    let res = out.unmarshal(&tp.dd_bytes_spill);
    assert!(res.is_ok());
    assert_eq!(104u16, out.descriptor.frame_number);

    // without the extension the forwarder writes no descriptor
    let f = Forwarder::new(
        webrtc::RtpCodecType::Video,
        logger::get_logger(),
        true,
        true,
        true,
        None,
    );
    f.determine_codec(
        &testutils::test_vp8_codec(),
        &[],
        livekit::video_layer::Mode::Unused,
    );
    assert!(f.lock.read().dd_forwarder.is_none());
}

#[test]
fn test_forwarder_pad_dropped_slot() {
    let f = Forwarder::new(
        webrtc::RtpCodecType::Video,
        logger::get_logger(),
        true,
        true,
        true,
        None,
    );
    f.determine_codec(
        &testutils::test_vp8_codec(),
        &dd_extensions(),
        livekit::video_layer::Mode::Unused,
    );
    f.lock.write().vls.set_current(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });

    let packet = |sn: u16, frame_num: u64, tid: i64, is_out_of_order: bool| -> buffer::ExtPacket {
        let ext_pkt = testutils::get_test_ext_packet_vp8_dd(
            &testutils::TestExtPacketParams {
                sequence_number: sn,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 20,
                marker: true,
                is_out_of_order,
                ..Default::default()
            },
            &testutils::TestDdParams {
                first_packet_in_frame: true,
                ext_frame_num: frame_num,
                temporal_id: tid,
                dtis: "SSS".to_string(),
                decode_targets: testutils::TEST_VP8_DECODE_TARGETS.to_vec(),
                ..Default::default()
            },
        );
        assert!(ext_pkt.is_ok());
        ext_pkt.unwrap()
    };

    let (tp, err) = f.get_translation_params(&packet(10, 0, 0, false), 0);
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);

    // a gap, the munger hands out a slot for sequence number 11
    let (tp, err) = f.get_translation_params(&packet(12, 2, 0, false), 0);
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    assert_eq!(12u64, tp.rtp.ext_sequence_number);

    // the late packet is on a filtered layer, its slot gets padding
    let (tp, err) = f.get_translation_params(&packet(11, 1, 2, true), 0);
    assert_eq!(Ok(()), err);
    assert!(tp.should_drop);
    assert!(tp.pad_dropped_slot);
    assert_eq!(11u64, tp.rtp.ext_sequence_number);
    assert_eq!(0xabcdefu64, tp.rtp.ext_timestamp);

    // an in-order drop rolls the slot back instead
    let (tp, err) = f.get_translation_params(&packet(13, 3, 2, false), 0);
    assert_eq!(Ok(()), err);
    assert!(tp.should_drop);
    assert!(!tp.pad_dropped_slot);
    let (tp, err) = f.get_translation_params(&packet(14, 4, 0, false), 0);
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    assert_eq!(13u64, tp.rtp.ext_sequence_number);
}

// dropSelector drops one packet as relevant, the result a decode target selector gives for a
// frame outside the target
struct DropSelector {
    video_layer_selector: Box<dyn videolayerselector::VideoLayerSelector>,
    drop_sn: u64,
}

impl videolayerselector::VideoLayerSelector for DropSelector {
    fn get_base(&self) -> &videolayerselector::Base {
        self.video_layer_selector.get_base()
    }

    fn get_base_mut(&mut self) -> &mut videolayerselector::Base {
        self.video_layer_selector.get_base_mut()
    }

    fn into_base(self: Box<Self>) -> videolayerselector::Base {
        self.video_layer_selector.into_base()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    // what a selector has of its own, passed on to the embedded one
    fn is_overshoot_okay(&self) -> bool {
        self.video_layer_selector.is_overshoot_okay()
    }

    fn check_sync(&self) -> (bool, i32) {
        self.video_layer_selector.check_sync()
    }

    fn rollback(&mut self) {
        self.video_layer_selector.rollback()
    }

    fn select(
        &mut self,
        ext_pkt: &buffer::ExtPacket,
        layer: i32,
    ) -> videolayerselector::VideoLayerSelectorResult {
        if ext_pkt.ext_sequence_number == self.drop_sn {
            return videolayerselector::VideoLayerSelectorResult {
                is_relevant: true,
                ..Default::default()
            };
        }
        self.video_layer_selector.select(ext_pkt, layer)
    }
}

#[test]
fn test_forwarder_pad_filtered_slot_after_gap() {
    struct TestCase {
        name: &'static str,
        drop_by_selector: bool,
    }
    for tc in [
        TestCase {
            name: "codec munger",
            drop_by_selector: false,
        },
        TestCase {
            name: "layer selector",
            drop_by_selector: true,
        },
    ] {
        let f = Forwarder::new(
            webrtc::RtpCodecType::Video,
            logger::get_logger(),
            true,
            true,
            true,
            None,
        );
        f.determine_codec(
            &testutils::test_vp8_codec(),
            &dd_extensions(),
            livekit::video_layer::Mode::Unused,
        );
        f.lock.write().vls.set_current(buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        });
        f.lock.write().vls.set_target(buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        });
        if tc.drop_by_selector {
            let mut inner = f.lock.write();
            let vls = f.take_vls(&mut inner);
            inner.vls = Box::new(DropSelector {
                video_layer_selector: vls,
                drop_sn: 12,
            });
        }

        let packet =
            |sn: u16, frame_num: u64, tid: i64, is_out_of_order: bool| -> buffer::ExtPacket {
                let ext_pkt = testutils::get_test_ext_packet_vp8_dd(
                    &testutils::TestExtPacketParams {
                        sequence_number: sn,
                        timestamp: 0xabcdefu32.wrapping_add((frame_num as u32).wrapping_mul(3000)),
                        ssrc: 0x12345678,
                        payload_size: 20,
                        marker: true,
                        is_out_of_order,
                        ..Default::default()
                    },
                    &testutils::TestDdParams {
                        first_packet_in_frame: true,
                        ext_frame_num: frame_num,
                        temporal_id: tid,
                        dtis: "SSS".to_string(),
                        decode_targets: testutils::TEST_VP8_DECODE_TARGETS.to_vec(),
                        ..Default::default()
                    },
                );
                assert!(ext_pkt.is_ok(), "{}", tc.name);
                ext_pkt.unwrap()
            };

        let (tp, err) = f.get_translation_params(&packet(10, 0, 0, false), 0);
        assert_eq!(Ok(()), err, "{}", tc.name);
        assert!(!tp.should_drop, "{}", tc.name);

        // 11 is lost, 12 is on a filtered layer: its slot gets padding and is not rolled back
        let (tp, err) = f.get_translation_params(&packet(12, 2, 2, false), 0);
        assert_eq!(Ok(()), err, "{}", tc.name);
        assert!(tp.should_drop, "{}", tc.name);
        assert!(tp.pad_dropped_slot, "{}", tc.name);
        assert_eq!(12u64, tp.rtp.ext_sequence_number, "{}", tc.name);

        // the retransmission of 11 still has its slot
        let (tp, err) = f.get_translation_params(&packet(11, 1, 0, true), 0);
        assert_eq!(Ok(()), err, "{}", tc.name);
        assert!(!tp.should_drop, "{}", tc.name);
        assert_eq!(11u64, tp.rtp.ext_sequence_number, "{}", tc.name);

        // the next packet follows the padded slot
        let (tp, err) = f.get_translation_params(&packet(13, 3, 0, false), 0);
        assert_eq!(Ok(()), err, "{}", tc.name);
        assert!(!tp.should_drop, "{}", tc.name);
        assert_eq!(13u64, tp.rtp.ext_sequence_number, "{}", tc.name);
    }
}

#[test]
fn test_forwarder_get_sn_ts_for_padding() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 1,
    });
    f.lock.write().vls.set_current(buffer::INVALID_LAYER);

    // send it through so that forwarder locks onto stream
    let _ = f.get_translation_params(&ext_pkt, 0);

    // pause stream and get padding, it should still work
    disable(&f);

    // should get back frame end needed as the last packet did not have RTP marker set
    let snts = f.get_sn_ts_for_padding(5, 0, false);
    assert!(snts.is_ok());
    let snts = snts.unwrap();

    let num_padding = 5usize;
    let clock_rate = 0u32;
    let frame_rate = 5u32;
    let mut snts_expected = vec![SnTs::default(); num_padding];
    for (i, snts_expected) in snts_expected.iter_mut().enumerate() {
        *snts_expected = SnTs {
            ext_sequence_number: 23333 + i as u64 + 1,
            ext_timestamp: 0xabcdef + (i as u64 * clock_rate as u64) / frame_rate as u64,
        };
    }
    assert_eq!(snts_expected, snts);

    // now that there is a marker, timestamp should jump on first padding when asked again
    let snts = f.get_sn_ts_for_padding(num_padding, 0, false);
    assert!(snts.is_ok());
    let snts = snts.unwrap();

    for (i, snts_expected) in snts_expected.iter_mut().enumerate() {
        *snts_expected = SnTs {
            ext_sequence_number: 23338 + i as u64 + 1,
            ext_timestamp: 0xabcdef + ((i + 1) as u64 * clock_rate as u64) / frame_rate as u64,
        };
    }
    assert_eq!(snts_expected, snts);
}

#[test]
#[allow(clippy::manual_div_ceil)] // the rounding as the original has it
fn test_forwarder_get_sn_ts_for_blank_frames() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 1,
    });
    f.lock.write().vls.set_current(buffer::INVALID_LAYER);

    // send it through so that forwarder locks onto stream
    let _ = f.get_translation_params(&ext_pkt, 0);

    // should get back frame end needed as the last packet did not have RTP marker set
    let num_blank_frames = 6usize;
    let res = f.get_sn_ts_for_blank_frames(30, num_blank_frames);
    assert!(res.is_ok());
    let (snts, frame_end_needed) = res.unwrap();
    assert!(frame_end_needed);

    // there should be one more than RTPBlankFramesMax as one would have been allocated to end previous frame
    let num_padding = num_blank_frames + 1;
    let clock_rate = testutils::test_vp8_codec().clock_rate;
    let frame_rate = 30u32;
    let mut snts_expected = vec![SnTs::default(); num_padding];
    for (i, snts_expected) in snts_expected.iter_mut().enumerate() {
        // first blank frame should have same timestamp as last frame as end frame is synthesized
        let mut ts = params.timestamp;
        if i != 0 {
            // +1 here due to expected time stamp bumpint by at least one so that time stamp is always moving ahead
            ts = params.timestamp + 1 + ((i as u32 * clock_rate) + frame_rate - 1) / frame_rate;
        }
        *snts_expected = SnTs {
            ext_sequence_number: params.sequence_number as u64 + i as u64 + 1,
            ext_timestamp: ts as u64,
        };
    }
    assert_eq!(snts_expected, snts);

    // now that there is a marker, timestamp should jump on first padding when asked again
    // also number of padding should be RTPBlankFramesMax
    let num_padding = num_blank_frames;
    snts_expected.truncate(num_padding);
    for (i, snts_expected) in snts_expected.iter_mut().enumerate() {
        *snts_expected = SnTs {
            ext_sequence_number: params.sequence_number as u64 + snts.len() as u64 + i as u64 + 1,
            // +1 here due to expected time stamp bumpint by at least one so that time stamp is always moving ahead
            ext_timestamp: snts[snts.len() - 1].ext_timestamp
                + 1
                + (((i + 1) as u64 * clock_rate as u64) + frame_rate as u64 - 1)
                    / frame_rate as u64,
        };
    }
    let res = f.get_sn_ts_for_blank_frames(30, num_blank_frames);
    assert!(res.is_ok());
    let (snts, frame_end_needed) = res.unwrap();
    assert!(!frame_end_needed);
    assert_eq!(snts_expected, snts);
}

#[test]
fn test_forwarder_get_padding_vp8() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 13,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    f.lock.write().vls.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 1,
    });
    f.lock.write().vls.set_current(buffer::INVALID_LAYER);

    // send it through so that forwarder locks onto stream
    let _ = f.get_translation_params(&ext_pkt, 0);

    // getting padding with frame end needed, should repeat the last picture id
    let expected_vp8 = codec::Vp8 {
        first_byte: 16,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let buf = f.get_padding(true);
    assert!(buf.is_ok());
    let buf = buf.unwrap();
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    assert_eq!(marshalled_vp8.unwrap(), buf);

    // getting padding with no frame end needed, should get next picture id
    let expected_vp8 = codec::Vp8 {
        first_byte: 16,
        i: true,
        m: true,
        picture_id: 13468,
        l: true,
        tl0picidx: 234,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 24,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let buf = f.get_padding(false);
    assert!(buf.is_ok());
    let buf = buf.unwrap();
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    assert_eq!(marshalled_vp8.unwrap(), buf);
}

#[test]
fn test_forwarder_initial_acquisition_grace() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);

    // subscriber requested the top spatial layer
    f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
    f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);
    f.set_max_temporal_layer_seen(buffer::DEFAULT_MAX_LAYER_TEMPORAL);

    let bitrates: Bitrates = [[2, 3, 0, 0], [4, 0, 0, 5], [0, 7, 0, 0]];

    // subscriber-first: only layer 1 has been seen so far and nothing is being forwarded yet
    // (current invalid). this arms the initial-acquisition grace.
    assert!(f.set_max_published_layer(1));
    assert!(f.within_acquire_grace_locked(&f.lock.read()));

    // during the grace, the target aims straight at the requested layer (2) and requests a key
    // frame for it, even though only layer 1 has been seen - so acquisition does not ramp up
    // gradually as higher layers are detected
    let alloc = f.allocate_optimal(&[0, 1], bitrates, false, false);
    assert_eq!(2i32, alloc.target_layer.spatial);
    assert_eq!(2i32, alloc.request_layer_spatial);

    // force the grace to expire while still not streaming: must signal that a re-allocation is
    // needed so the target can fall back
    f.lock.write().acquire_deadline = 1; // a deadline far in the past
    assert!(f.maybe_expire_acquire_grace());
    assert!(!f.maybe_expire_acquire_grace()); // only fires once

    // after the grace, the target falls back to the highest layer actually seen (1) instead of
    // stalling while waiting for a requested layer that never showed up
    let alloc = f.allocate_optimal(&[0, 1], bitrates, false, false);
    assert_eq!(1i32, alloc.target_layer.spatial);
    assert_eq!(1i32, alloc.request_layer_spatial);
}

#[test]
fn test_get_ref_layer_rtp_timestamp_bounds() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    let inner = f.lock.read();

    let layer_count = inner.ref_infos.len() as i32;

    let res = f.get_ref_layer_rtp_timestamp(&inner, 1000, layer_count, 0);
    assert!(res.is_err());

    let res = f.get_ref_layer_rtp_timestamp(&inner, 1000, 0, layer_count);
    assert!(res.is_err());

    let res = f.get_ref_layer_rtp_timestamp(&inner, 1000, -1, 0);
    assert!(res.is_err());

    let res = f.get_ref_layer_rtp_timestamp(&inner, 1000, 0, -1);
    assert!(res.is_err());

    // same-layer translation does not need sender reports
    let res = f.get_ref_layer_rtp_timestamp(&inner, 1000, 0, 0);
    assert_eq!(Ok(1000u32), res);

    // last valid index must be accepted by the bounds check (may still error for missing SR)
    let res = f.get_ref_layer_rtp_timestamp(&inner, 1000, layer_count - 1, 0);
    assert!(res.is_err()); // unavailable sender report, not invalid layer
    assert!(res.unwrap_err().to_string().contains("unavailable"));
}

// TestForwarderIsEndOfLayerFrame checks the end-of-layer-frame detection used to
// locate packet trailers, which VP9 SVC can carry at the end of any spatial layer
// frame and not just at the end of a picture.
#[test]
fn test_forwarder_is_end_of_layer_frame() {
    let vp9_codec = webrtc::RtpCodecCapability {
        mime_type: mime::MimeType::Vp9.to_string(),
        clock_rate: 90000,
        ..Default::default()
    };

    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    assert!(!f.is_end_of_layer_frame(
        &f.lock.read(),
        &buffer::ExtPacket {
            vp9: Some(codecs::Vp9Packet {
                e: true,
                ..Default::default()
            }),
            ..Default::default()
        }
    ));

    let f = new_forwarder(&vp9_codec, webrtc::RtpCodecType::Video);
    assert!(f.is_end_of_layer_frame(
        &f.lock.read(),
        &buffer::ExtPacket {
            vp9: Some(codecs::Vp9Packet {
                e: true,
                ..Default::default()
            }),
            ..Default::default()
        }
    ));
    assert!(!f.is_end_of_layer_frame(
        &f.lock.read(),
        &buffer::ExtPacket {
            vp9: Some(codecs::Vp9Packet {
                e: false,
                ..Default::default()
            }),
            ..Default::default()
        }
    ));
    assert!(!f.is_end_of_layer_frame(&f.lock.read(), &buffer::ExtPacket::default()));

    assert!(f.is_end_of_layer_frame(
        &f.lock.read(),
        &buffer::ExtPacket {
            dependency_descriptor: Some(buffer::ExtDependencyDescriptor {
                descriptor: Some(dd::DependencyDescriptor {
                    last_packet_in_frame: true,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            vp9: Some(codecs::Vp9Packet {
                e: false,
                ..Default::default()
            }),
            ..Default::default()
        }
    ));
    assert!(!f.is_end_of_layer_frame(
        &f.lock.read(),
        &buffer::ExtPacket {
            dependency_descriptor: Some(buffer::ExtDependencyDescriptor {
                descriptor: Some(dd::DependencyDescriptor {
                    last_packet_in_frame: false,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            vp9: Some(codecs::Vp9Packet {
                e: true,
                ..Default::default()
            }),
            ..Default::default()
        }
    ));
    assert!(!f.is_end_of_layer_frame(
        &f.lock.read(),
        &buffer::ExtPacket {
            dependency_descriptor: Some(buffer::ExtDependencyDescriptor::default()),
            ..Default::default()
        }
    ));
}

fn codec_header_of(b: &[u8]) -> [u8; codecmunger::MAX_HEADER_SIZE] {
    let mut hdr = [0u8; codecmunger::MAX_HEADER_SIZE];
    hdr[..b.len()].copy_from_slice(b);
    hdr
}

// The write stream of the packet below, which is never sent: a `pacer::Packet` cannot be made
// without one.
struct NoWriter;

impl webrtc::TrackLocalWriter for NoWriter {
    fn write_rtp(&self, _header: &rtp::Header, _payload: &[u8]) -> Result<usize, webrtc::Error> {
        Ok(0)
    }
}

// WriteRTP holds the dependency descriptor in pacer scratch sized to pion's one
// byte extension profile payload cap, which has to be the inline size
// TranslationParams carries, or descriptors would take the heap copy fallback
#[test]
fn test_pacer_scratch_fits_inline_dependency_descriptor() {
    let mut p = pacer::Packet::new(Arc::new(NoWriter));

    assert_eq!(
        Some(dd::MAX_INLINE_EXTENSION_SIZE),
        p.hold_extension(&[0u8; dd::MAX_INLINE_EXTENSION_SIZE])
            .map(|held| held.len())
    );
    assert!(
        p.hold_extension(&[0u8; dd::MAX_INLINE_EXTENSION_SIZE + 1])
            .is_none()
    );
}

#[test]
fn test_forwarder_needs_periodic_sync_check() {
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    assert!(!f.needs_periodic_sync_check());

    // VP8 with the dependency descriptor uses it only for temporal layers
    f.determine_codec(
        &testutils::test_vp8_codec(),
        &dd_extensions(),
        livekit::video_layer::Mode::Unused,
    );
    assert!(!f.needs_periodic_sync_check());

    // a new max published layer before any layer is forwarded arms the acquisition grace
    assert!(f.set_max_published_layer(1));
    assert!(f.needs_periodic_sync_check());

    // spatial layers selected with the dependency descriptor
    let f = new_forwarder(&testutils::test_vp8_codec(), webrtc::RtpCodecType::Video);
    let av1 = webrtc::RtpCodecCapability {
        mime_type: mime::MimeType::Av1.to_string(),
        clock_rate: 90000,
        ..Default::default()
    };
    f.determine_codec(&av1, &dd_extensions(), livekit::video_layer::Mode::Unused);
    assert!(f.needs_periodic_sync_check());
}

// TestForwarderSimulcastTemporalLayerAfterSwitch checks that, after a spatial layer switch on a
// key frame (which is always on temporal layer 0), the forwarder reaches its target layer so that
// GetNextHigherTransition (and hence stream allocator probing) is not blocked forever.
//
// VP9, AV1 and H.264 simulcast without the dependency descriptor forward every temporal layer of
// the selected spatial layer, so the current temporal layer has to follow the target. VP8 steps
// up through its temporal layer selector.
#[test]
fn test_forwarder_simulcast_temporal_layer_after_switch() {
    let bitrates: Bitrates = [[100, 150, 200, 0], [300, 450, 600, 0], [900, 1350, 1800, 0]];

    struct TestCase {
        name: &'static str,
        previous_codec: Option<webrtc::RtpCodecCapability>,
        codec: webrtc::RtpCodecCapability,
        target: buffer::VideoLayer,
        expected_current: buffer::VideoLayer,
    }
    let codec_of = |mime_type: mime::MimeType| webrtc::RtpCodecCapability {
        mime_type: mime_type.to_string(),
        clock_rate: 90000,
        ..Default::default()
    };
    let test_cases = [
        TestCase {
            name: "VP9 simulcast, three temporal layers",
            previous_codec: None,
            codec: codec_of(mime::MimeType::Vp9),
            target: buffer::VideoLayer {
                spatial: 1,
                temporal: 2,
            },
            expected_current: buffer::VideoLayer {
                spatial: 1,
                temporal: 2,
            },
        },
        TestCase {
            name: "AV1 simulcast, three temporal layers",
            previous_codec: None,
            codec: codec_of(mime::MimeType::Av1),
            target: buffer::VideoLayer {
                spatial: 1,
                temporal: 2,
            },
            expected_current: buffer::VideoLayer {
                spatial: 1,
                temporal: 2,
            },
        },
        TestCase {
            name: "VP9 simulcast, one temporal layer",
            previous_codec: None,
            codec: codec_of(mime::MimeType::Vp9),
            target: buffer::VideoLayer {
                spatial: 1,
                temporal: 0,
            },
            expected_current: buffer::VideoLayer {
                spatial: 1,
                temporal: 0,
            },
        },
        TestCase {
            name: "H.264 simulcast",
            previous_codec: None,
            codec: codec_of(mime::MimeType::H264),
            target: buffer::VideoLayer {
                spatial: 1,
                temporal: 0,
            },
            expected_current: buffer::VideoLayer {
                spatial: 1,
                temporal: 0,
            },
        },
        TestCase {
            // the VP8 temporal layer selector moves to the target on a key frame
            name: "VP8 simulcast, three temporal layers",
            previous_codec: None,
            codec: testutils::test_vp8_codec(),
            target: buffer::VideoLayer {
                spatial: 1,
                temporal: 2,
            },
            expected_current: buffer::VideoLayer {
                spatial: 1,
                temporal: 2,
            },
        },
        TestCase {
            // the VP8 temporal layer selector must not be carried over on codec change
            name: "VP8 to VP9 simulcast, three temporal layers",
            previous_codec: Some(testutils::test_vp8_codec()),
            codec: codec_of(mime::MimeType::Vp9),
            target: buffer::VideoLayer {
                spatial: 1,
                temporal: 2,
            },
            expected_current: buffer::VideoLayer {
                spatial: 1,
                temporal: 2,
            },
        },
        TestCase {
            name: "VP8 to AV1 simulcast, three temporal layers",
            previous_codec: Some(testutils::test_vp8_codec()),
            codec: codec_of(mime::MimeType::Av1),
            target: buffer::VideoLayer {
                spatial: 1,
                temporal: 2,
            },
            expected_current: buffer::VideoLayer {
                spatial: 1,
                temporal: 2,
            },
        },
    ];

    for tc in &test_cases {
        let f = Forwarder::new(
            webrtc::RtpCodecType::Video,
            logger::get_logger(),
            true,  // skipReferenceTS
            true,  // disableOpportunisticAllocation
            false, // enableStartAtDesiredQuality
            None,
        );
        if let Some(previous_codec) = &tc.previous_codec {
            f.determine_codec(
                previous_codec,
                &[],
                livekit::video_layer::Mode::OneSpatialLayerPerStream,
            );
        }
        f.determine_codec(
            &tc.codec,
            &[],
            livekit::video_layer::Mode::OneSpatialLayerPerStream,
        );
        f.set_max_spatial_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
        f.set_max_temporal_layer(buffer::DEFAULT_MAX_LAYER_TEMPORAL);
        f.set_max_published_layer(buffer::DEFAULT_MAX_LAYER_SPATIAL);
        f.set_max_temporal_layer_seen(buffer::DEFAULT_MAX_LAYER_TEMPORAL);

        // down-switched for bandwidth: was on the top layer, target is now lower
        f.lock.write().last_allocation.is_deficient = true;
        f.lock.write().vls.set_current(buffer::VideoLayer {
            spatial: 2,
            temporal: tc.target.temporal,
        });
        f.lock.write().vls.set_target(tc.target);

        // target pending (spatial switch not done yet): no higher transition
        let (_, available) = f.get_next_higher_transition(bitrates, false);
        assert!(!available, "{}", tc.name);

        // key frame of the target spatial layer, temporal layer 0
        let params = testutils::TestExtPacketParams {
            sequence_number: 23333,
            timestamp: 0xabcdef,
            ssrc: 0x12345678,
            payload_size: 20,
            is_key_frame: true,
            video_layer: buffer::VideoLayer {
                spatial: tc.target.spatial,
                temporal: 0,
            },
            ..Default::default()
        };
        let ext_pkt = if tc.codec.mime_type == testutils::test_vp8_codec().mime_type {
            testutils::get_test_ext_packet_vp8(
                &params,
                &codec::Vp8 {
                    first_byte: 25,
                    i: true,
                    m: true,
                    picture_id: 13467,
                    l: true,
                    tl0picidx: 233,
                    t: true,
                    tid: 0,
                    y: true,
                    k: true,
                    keyidx: 23,
                    header_size: 6,
                    is_key_frame: true,
                    ..Default::default()
                },
            )
        } else {
            testutils::get_test_ext_packet(&params)
        };
        assert!(ext_pkt.is_ok(), "{}", tc.name);
        let ext_pkt = ext_pkt.unwrap();

        let (tp, err) = f.get_translation_params(&ext_pkt, tc.target.spatial);
        assert_eq!(Ok(()), err, "{}", tc.name);
        assert!(!tp.should_drop, "{}", tc.name);
        assert!(tp.is_switching, "{}", tc.name);

        assert_eq!(
            tc.expected_current,
            f.lock.read().vls.get_current(),
            "{}",
            tc.name
        );
        // target reached: the next higher transition (and hence probing) is available
        let (_, available) = f.get_next_higher_transition(bitrates, false);
        assert!(available, "{}", tc.name);
    }
}

// VP9 and AV1 simulcast with the dependency descriptor filter temporal layers by frame and
// forward the descriptor, as VP8 does. A publisher that stops sending the descriptor gets
// every temporal layer forwarded.
#[test]
fn test_forwarder_simulcast_dependency_descriptor() {
    for mime_type in [mime::MimeType::Vp9, mime::MimeType::Av1] {
        let key_frame_descriptor = test_key_frame_descriptor();
        let f = Forwarder::new(
            webrtc::RtpCodecType::Video,
            logger::get_logger(),
            true,
            true,
            true,
            None,
        );
        f.determine_codec(
            &webrtc::RtpCodecCapability {
                mime_type: mime_type.to_string(),
                clock_rate: 90000,
                ..Default::default()
            },
            &dd_extensions(),
            livekit::video_layer::Mode::OneSpatialLayerPerStream,
        );
        assert!(
            f.lock
                .read()
                .codec_munger
                .as_any()
                .is::<codecmunger::DdTemporal>(),
            "{mime_type}"
        );
        assert!(f.lock.read().dd_forwarder.is_some(), "{mime_type}");
        f.lock.write().vls.set_current(buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        });
        f.lock.write().vls.set_target(buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        });

        let (tp, err) =
            f.get_translation_params(&test_dd_packet(&key_frame_descriptor, 10, 100, 0, true), 0);
        assert_eq!(Ok(()), err, "{mime_type}");
        assert!(!tp.should_drop, "{mime_type}");
        assert!(!tp.dd_bytes_spill.is_empty(), "{mime_type}");

        // TL2 is above the target and is filtered, its slot is rolled back
        let (tp, err) =
            f.get_translation_params(&test_dd_packet(&key_frame_descriptor, 11, 101, 2, false), 0);
        assert_eq!(Ok(()), err, "{mime_type}");
        assert!(tp.should_drop, "{mime_type}");

        let (tp, err) =
            f.get_translation_params(&test_dd_packet(&key_frame_descriptor, 12, 102, 0, false), 0);
        assert_eq!(Ok(()), err, "{mime_type}");
        assert!(!tp.should_drop, "{mime_type}");
        assert_eq!(11u64, tp.rtp.ext_sequence_number, "{mime_type}");
        assert!(!tp.dd_inline().is_empty(), "{mime_type}");

        // the publisher stops sending the descriptor
        let no_dd = testutils::get_test_ext_packet(&testutils::TestExtPacketParams {
            sequence_number: 13,
            timestamp: 0xabcdef + 103 * 3000,
            ssrc: 0x12345678,
            payload_size: 20,
            marker: true,
            ..Default::default()
        });
        assert!(no_dd.is_ok(), "{mime_type}");
        let no_dd = no_dd.unwrap();
        let (tp, err) = f.get_translation_params(&no_dd, 0);
        assert_eq!(Ok(()), err, "{mime_type}");
        assert!(!tp.should_drop, "{mime_type}");
        assert!(f.lock.read().dd_forwarder.is_none(), "{mime_type}");
        assert!(
            f.lock
                .read()
                .codec_munger
                .as_any()
                .is::<codecmunger::Null>(),
            "{mime_type}"
        );
        assert!(tp.dd_inline().is_empty(), "{mime_type}");
    }
}

// ---------------------------------------------------------------------------
// Switching back to a layer after the reference layer's RTP timestamp wrapped,
// on a downtrack that started with dummy forwarding (dummyStartTSOffset != 0).

const WRAP_TEST_SSRC_LOW: u32 = 0x0a0a0a0a;
const WRAP_TEST_SSRC_HIGH: u32 = 0x0c0c0c0c;
const WRAP_TEST_FRAME: u32 = 3000; // 30 fps at 90 kHz
const WRAP_TEST_HIGH_TS: u32 = 0x10000000;

fn new_wrap_test_forwarder(skip_reference_ts: bool) -> Forwarder {
    let f = Forwarder::new(
        webrtc::RtpCodecType::Video,
        logger::get_logger(),
        skip_reference_ts,
        false, // disableOpportunisticAllocation
        false, // enableStartAtDesiredQuality
        Some(Arc::new(rtpstats::RtpStatsSender::new(
            rtpstats::RtpStatsParams::default(),
            1024,
        ))),
    );
    f.determine_codec(
        &webrtc::RtpCodecCapability {
            mime_type: webrtc::MIME_TYPE_H264.to_string(),
            clock_rate: 90000,
            ..Default::default()
        },
        &[],
        livekit::video_layer::Mode::OneSpatialLayerPerStream,
    );
    f.set_max_spatial_layer(2);
    f.set_max_temporal_layer(0);
    f.set_max_published_layer(2);
    f
}

fn wrap_test_bitrates(layer: i32) -> Bitrates {
    let mut brs = Bitrates::default();
    brs[layer as usize][0] = 1_000_000;
    brs
}

#[track_caller]
fn wrap_test_key_frame(
    ssrc: u32,
    sn: u16,
    ts: u32,
    ts_cycles: i64,
    layer: i32,
) -> buffer::ExtPacket {
    let ext_pkt = testutils::get_test_ext_packet(&testutils::TestExtPacketParams {
        sequence_number: sn,
        timestamp: ts,
        ts_cycles,
        ssrc,
        payload_size: 20,
        marker: true,
        is_key_frame: true,
        video_layer: buffer::VideoLayer {
            spatial: layer,
            temporal: 0,
        },
        ..Default::default()
    });
    assert!(ext_pkt.is_ok());
    ext_pkt.unwrap()
}

fn wrap_test_sender_report(at: Time, rtp_ts: u32) -> livekit::RtcpSenderReportState {
    livekit::RtcpSenderReportState {
        rtp_timestamp: rtp_ts,
        rtp_timestamp_ext: rtp_ts as u64,
        ntp_timestamp: mediatransportutil::to_ntp_time(at).0,
        at: at.unix_nano(),
        at_adjusted: at.unix_nano(),
        ..Default::default()
    }
}

// runToHighLayer starts the forwarder with dummy forwarding, locks it onto layer 0 (whose first
// timestamp is lowStart), switches it to layer 2 when layer 0 stops, forwards layer 2 for
// `elapsed`, and then has the allocator target layer 0 again. It returns layer 0's timestamp and
// timestamp cycles at that point, for the keyframe that switches back.
#[track_caller]
fn run_to_high_layer(f: &Forwarder, low_start: u32, elapsed: Duration) -> (u32, i64) {
    // dummy start, then lock onto layer 0: layer 0 becomes the reference layer
    f.maybe_start(&mut f.lock.write());
    f.allocate_optimal(&[0], wrap_test_bitrates(0), true, false);
    let (tp, err) = f.get_translation_params(
        &wrap_test_key_frame(WRAP_TEST_SSRC_LOW, 1000, low_start, 0, 0),
        0,
    );
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    assert_eq!(0i32, f.lock.read().reference_layer_spatial);
    assert_eq!(0i32, f.current_layer().spatial);

    let now = time::now();
    f.set_ref_sender_report(
        0,
        &wrap_test_sender_report(now, low_start.wrapping_add(WRAP_TEST_FRAME)),
    );
    f.set_ref_sender_report(
        2,
        &wrap_test_sender_report(now, WRAP_TEST_HIGH_TS + WRAP_TEST_FRAME),
    );

    // layer 0 stops, layer 2 is available: switch to layer 2
    let alloc = f.allocate_optimal(&[2], wrap_test_bitrates(2), true, false);
    assert_eq!(2i32, alloc.target_layer.spatial);
    let (tp, err) = f.get_translation_params(
        &wrap_test_key_frame(
            WRAP_TEST_SSRC_HIGH,
            5000,
            WRAP_TEST_HIGH_TS + 2 * WRAP_TEST_FRAME,
            0,
            2,
        ),
        2,
    );
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    assert_eq!(2i32, f.current_layer().spatial);

    // forward layer 2 for `elapsed`
    let ticks = (elapsed.seconds() * 90000.0) as u64;
    let high_ts = WRAP_TEST_HIGH_TS as u64 + ticks;
    let (tp, err) = f.get_translation_params(
        &wrap_test_key_frame(
            WRAP_TEST_SSRC_HIGH,
            5001,
            high_ts as u32,
            (high_ts >> 32) as i64,
            2,
        ),
        2,
    );
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);

    // layer 2 stops, layer 0 is back: the allocator targets layer 0
    let alloc = f.allocate_optimal(&[0], wrap_test_bitrates(0), true, false);
    assert_eq!(0i32, alloc.target_layer.spatial);

    let low_ts = low_start as u64 + ticks + WRAP_TEST_FRAME as u64;
    (low_ts as u32, (low_ts >> 32) as i64)
}

#[test]
fn test_forwarder_switch_back_after_reference_timestamp_wrap() {
    // maybeStart draws the dummy first timestamp in [2^31, 2^31 + 2^30), so a reference layer
    // starting above 2^31 + 2^30 always yields a negative dummyStartTSOffset
    const LOW_START: u32 = 0xf0000000;

    // after the wrap
    {
        let f = new_wrap_test_forwarder(false);
        let (low_ts, cycles) = run_to_high_layer(&f, LOW_START, time::HOUR);

        // preconditions: negative offset, layer 0 wrapped and its 32-bit timestamp is below |offset|
        let offset = f.lock.read().dummy_start_ts_offset as i64;
        assert!(offset < 0, "after the wrap");
        assert_eq!(1, cycles, "after the wrap");
        assert!((low_ts as i64) < -offset, "after the wrap");

        let (tp, err) = f.get_translation_params(
            &wrap_test_key_frame(WRAP_TEST_SSRC_LOW, 1001, low_ts, cycles, 0),
            0,
        );
        assert_eq!(Ok(()), err, "after the wrap");
        assert!(!tp.should_drop, "after the wrap: switch point rejected");
        assert_eq!(0i32, f.current_layer().spatial, "after the wrap");
    }

    // before the wrap
    {
        let f = new_wrap_test_forwarder(false);
        let (low_ts, cycles) = run_to_high_layer(&f, LOW_START, 30 * time::MINUTE);
        assert_eq!(0, cycles, "before the wrap");

        let (tp, err) = f.get_translation_params(
            &wrap_test_key_frame(WRAP_TEST_SSRC_LOW, 1001, low_ts, cycles, 0),
            0,
        );
        assert_eq!(Ok(()), err, "before the wrap");
        assert!(!tp.should_drop, "before the wrap: switch point rejected");
        assert_eq!(0i32, f.current_layer().spatial, "before the wrap");
    }

    // random start and duration
    {
        for (&start, &elapsed_seconds) in WRAP_TEST_RANDOM_STARTS
            .iter()
            .zip(WRAP_TEST_RANDOM_ELAPSED_SECONDS.iter())
        {
            let f = new_wrap_test_forwarder(false);
            let elapsed = elapsed_seconds * time::SECOND;
            let (low_ts, cycles) = run_to_high_layer(&f, start, elapsed);

            let (tp, err) = f.get_translation_params(
                &wrap_test_key_frame(WRAP_TEST_SSRC_LOW, 1001, low_ts, cycles, 0),
                0,
            );
            assert_eq!(Ok(()), err, "random start and duration");
            assert!(
                !tp.should_drop,
                "random start and duration: switch point rejected, start {start}, elapsed {elapsed}"
            );
        }
    }
}

// What the original draws for "random start and duration" from
// `rng := rand.New(rand.NewSource(1))`: `rng.Uint32()` for the start and
// `60+rng.Int63n(26*3600)` for the seconds, 400 times in turn. Go's generator is not ported,
// so its numbers are written out here, from one run of a Go program that draws them so.
const WRAP_TEST_RANDOM_STARTS: [u32; 400] = [
    2596996162, 2854263694, 1823804162, 281908850, 416480912, 2212821389, 920256325, 1366049456,
    1215622422, 2916646474, 872680990, 2451023057, 1258916094, 3232276575, 3716585580, 2249791083,
    680014774, 4188630858, 2554683474, 2972222970, 744172826, 2337130389, 1817424866, 1088948157,
    3387032319, 3781903914, 3841254378, 4195826001, 954725776, 1037299406, 4006544902, 3440505210,
    785656533, 3852551122, 4204469567, 390143126, 3981378005, 1494450894, 3053323154, 2789535992,
    3246237246, 561142274, 3849758482, 3097306067, 367307783, 2674597757, 1017145093, 804215881,
    2697661104, 1208304411, 1867934851, 2362863031, 3131807374, 2206820, 1717917180, 2594066196,
    127437182, 12210768, 2533318534, 3502138542, 1968995452, 112807962, 1072424133, 1062860987,
    2545299638, 2980012105, 2315889770, 3224502774, 3234803056, 1528008769, 995702448, 2140587224,
    108207233, 2531381080, 2457094102, 1768507280, 2111437688, 3423984611, 3363109602, 560123204,
    3177527534, 422555156, 428335644, 327234684, 685695486, 1385602332, 2451789070, 2938509808,
    2252709314, 3076778742, 55086860, 421039401, 3549593441, 1478821740, 929736495, 1726881131,
    724473658, 3555924096, 248791390, 1767552401, 3353313259, 229757662, 1077015772, 4182790799,
    92486895, 399303777, 1339538206, 2092676637, 2885484039, 3866652314, 1371517350, 1719843706,
    2770420834, 1458556941, 1015018773, 153565041, 2687947845, 311163323, 3778904840, 3639985909,
    1064484121, 322282363, 2702959033, 2714434079, 63683067, 295305609, 2788243018, 3585463438,
    2840662080, 1333631481, 4153638568, 1329500471, 1792400843, 1746921148, 4115336141, 3400206676,
    65203089, 3885984348, 184347049, 1494043213, 3607546704, 534137245, 3586072234, 32990530,
    1590286959, 2762540258, 3397857443, 1489769022, 3530862107, 2573519718, 1776316817, 3915351057,
    983193548, 1506524751, 1304820212, 2884184572, 4136652229, 3470080519, 4070599446, 4094215174,
    779926748, 3570153062, 3673529966, 1174651033, 3907394955, 2141610073, 4191438210, 193233579,
    4088405775, 2301204818, 3715898319, 2484857205, 2364876690, 2189855306, 2107944240, 1127388811,
    1595523019, 1785641113, 3872461160, 1176496955, 3674122956, 4248289986, 734207617, 2291156530,
    3494345769, 1104698047, 1439219346, 20958506, 986011055, 4079950097, 4134439462, 3060862612,
    1750337062, 1492365354, 2566527308, 3577088661, 4023128374, 3093721208, 1933396386, 2029337912,
    1556201844, 205592989, 3247897998, 949182841, 1678655519, 642894248, 3288177706, 1956116951,
    352056179, 1144101090, 425503163, 683774103, 2712171259, 4155403390, 1452476744, 3596888755,
    2157021947, 1988739519, 3195767560, 1066942813, 1819787089, 2821141789, 2734996066, 1661557221,
    542730790, 2196185047, 4271801818, 3123811290, 2207714417, 2343345469, 1259646708, 608935175,
    2711620020, 6062535, 3813037698, 2560567840, 3564128803, 283187946, 1441038932, 2658712220,
    1394335827, 361579919, 2953082892, 95052488, 336062872, 981722977, 1168423850, 2842538438,
    4160726228, 1565086353, 885516103, 3291797238, 1236561826, 1942201082, 1670984478, 32334443,
    19281403, 1021518970, 3406581728, 1890192433, 3367343165, 438893626, 703766644, 520539111,
    4287807926, 443621516, 1351154359, 615326666, 2954095254, 140943104, 1438034074, 1497139938,
    3928612693, 680560209, 1130212629, 811554447, 2754635528, 2698943460, 1447339906, 1839341789,
    1489491442, 3457418803, 1620463515, 1921152524, 424569443, 113922630, 3364988985, 4188173183,
    1901146802, 2630395824, 414433033, 1023024566, 1881229538, 882455388, 2019403180, 384647765,
    79044741, 4083776537, 2161503053, 2342417650, 868925525, 4085254502, 4143225426, 1166414187,
    1098957875, 812939683, 1531317596, 2643138802, 3360561437, 3282485470, 529019363, 2297984712,
    938750148, 2697121146, 1967666323, 3723462185, 1745428429, 1991310126, 2676325497, 1426746080,
    3366150781, 3369791599, 1329247589, 226616702, 1641077202, 1253242120, 1846652026, 1396213796,
    1773390187, 3319192106, 593455203, 1436304376, 2755547154, 2322901634, 2140316568, 297249814,
    1779625891, 4004234497, 3057713809, 646869505, 2630456228, 2620330906, 361514346, 3435748390,
    444926091, 2139910599, 4054816798, 3372043550, 2635350680, 2643487079, 310365102, 3910603557,
    3696282212, 3581129231, 2483091022, 338436629, 2364033673, 2055826721, 774767813, 2826952256,
    3315177225, 869555127, 3255387575, 2207463047, 53605651, 4196280555, 2090873003, 4088635616,
    2013369318, 1822302193, 2325902728, 3206146474, 3409342385, 2758704790, 3046460289, 2493103216,
    85362127, 3104514705, 3620236803, 4178134545, 751407560, 2882891342, 3267716628, 387975273,
    425937568, 807426474, 680625046, 419459990, 4063235271, 640054214, 3175232165, 3865061712,
    3469241979, 2344398283, 1537363204, 3433411840, 1921101007, 3328938601, 1732113758, 308813630,
];

const WRAP_TEST_RANDOM_ELAPSED_SECONDS: [i64; 400] = [
    20011, 38111, 46580, 57008, 5909, 42147, 63696, 89333, 72151, 2991, 67616, 68497, 80769, 45310,
    14231, 74903, 46919, 88999, 75767, 80640, 50776, 85531, 82767, 1522, 15057, 32181, 41572,
    50439, 36685, 11794, 51561, 2070, 74377, 9539, 84610, 76783, 56162, 60917, 80561, 61078, 57701,
    6281, 17540, 34515, 17313, 2277, 60678, 50374, 72040, 68531, 16083, 14865, 55225, 3943, 87039,
    62173, 58682, 33849, 63847, 67259, 30704, 77789, 55659, 64774, 68656, 53403, 17448, 41360,
    56268, 33395, 32362, 8407, 21189, 53398, 8535, 75709, 63564, 7834, 87760, 22649, 52496, 61561,
    48144, 44339, 33435, 24357, 92025, 92670, 69107, 21688, 29296, 4649, 77278, 52572, 25009,
    84110, 410, 55140, 83310, 11500, 13974, 55269, 23660, 51293, 39573, 2365, 61277, 10180, 52411,
    53050, 52271, 24478, 83102, 71427, 79169, 35446, 81236, 23688, 49975, 48414, 43018, 43700,
    61112, 42119, 80574, 74465, 25926, 54593, 8097, 40322, 46124, 51507, 26571, 1472, 15501, 68988,
    30316, 88927, 70217, 15497, 7704, 4649, 28069, 10259, 64489, 14485, 89953, 9084, 47921, 91591,
    29977, 77016, 7683, 35494, 35826, 27636, 18534, 11276, 33944, 24097, 14477, 32941, 72138,
    69084, 63903, 30306, 67760, 46474, 48438, 2320, 14343, 42209, 74772, 3765, 45024, 81849, 69999,
    43747, 16381, 54360, 5671, 55936, 75292, 89864, 88750, 33630, 79996, 42771, 23973, 35883,
    87785, 67542, 90247, 6275, 89165, 50287, 43977, 28509, 6868, 76344, 42898, 63207, 69280, 12575,
    92073, 28989, 5767, 18823, 32384, 2685, 17810, 44509, 43606, 68628, 17679, 33654, 50939, 71638,
    67475, 49803, 59080, 60090, 40292, 39519, 18834, 1488, 41643, 44004, 50380, 49990, 73315,
    83722, 70536, 45588, 67547, 66814, 68963, 20303, 45944, 73162, 72285, 14903, 68633, 70577,
    37078, 54782, 62332, 58559, 66735, 75730, 38235, 13032, 92008, 48985, 30554, 75210, 16955,
    24598, 23741, 91100, 24802, 41851, 34531, 51713, 64702, 72545, 3409, 90984, 89449, 41590, 7292,
    50517, 5332, 26422, 31013, 85340, 80539, 69330, 45869, 35491, 90711, 54558, 48315, 11391,
    66124, 30519, 84753, 46237, 75275, 14514, 81712, 41209, 6313, 58769, 10825, 9052, 64289, 89811,
    88909, 29421, 58958, 81535, 63229, 81508, 74268, 27404, 31792, 49629, 64686, 38867, 79854,
    81026, 55938, 21570, 8071, 32613, 58203, 37718, 89803, 1817, 68067, 56983, 27910, 25221, 33951,
    58477, 27318, 33379, 21060, 73827, 51405, 91356, 51914, 83801, 85373, 3648, 36792, 37850,
    78526, 12300, 52036, 65792, 8522, 74235, 29669, 65890, 41644, 842, 26012, 2683, 85279, 16730,
    7822, 84960, 38919, 48526, 43850, 86960, 66208, 9053, 20697, 43261, 21002, 79014, 83136, 13120,
    51467, 38252, 32087, 62663, 44076, 75993, 8159, 66570, 52300, 53384, 60803, 1039, 37808, 35365,
    51248, 41977, 17962, 52563, 59795, 10387, 54265, 46015, 73805, 83167, 12184, 2174, 57893,
    91113, 17698, 25407, 58934, 86132, 87717, 71130,
];

// Receiver restart on a downtrack that started with dummy forwarding: the new reference layer's
// timestamps must keep mapping onto the downtrack's existing timeline on every switch after
// the restart, not only on the first one.
#[test]
fn test_forwarder_restart_keeps_timestamp_continuity() {
    let f = new_wrap_test_forwarder(false);
    f.rtp_stats.as_ref().unwrap().set_clock_rate(90000);

    // dummy start, lock onto layer 0 with a reference timestamp that yields a negative offset
    f.maybe_start(&mut f.lock.write());
    f.allocate_optimal(&[0], wrap_test_bitrates(0), true, false);
    let (tp, err) = f.get_translation_params(
        &wrap_test_key_frame(WRAP_TEST_SSRC_LOW, 1000, 0xf0000000, 0, 0),
        0,
    );
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    assert!((f.lock.read().dummy_start_ts_offset as i64) < 0);

    // the downtrack has been sending: rtpStats is initialized on the first forwarded packet
    f.rtp_stats.as_ref().unwrap().update(
        mono::unix_nano(),
        tp.rtp.ext_sequence_number,
        tp.rtp.ext_timestamp,
        true,
        12,
        20,
        0,
        false,
    );

    // upstream receiver restart: new stream, new timestamp base for every layer
    f.restart();
    f.determine_codec(
        &webrtc::RtpCodecCapability {
            mime_type: webrtc::MIME_TYPE_H264.to_string(),
            clock_rate: 90000,
            ..Default::default()
        },
        &[],
        livekit::video_layer::Mode::OneSpatialLayerPerStream,
    );
    const NEW_LOW_START: u32 = 0x20000000;
    let now = time::now();
    f.set_ref_sender_report(0, &wrap_test_sender_report(now, NEW_LOW_START));
    f.set_ref_sender_report(2, &wrap_test_sender_report(now, WRAP_TEST_HIGH_TS));

    f.allocate_optimal(&[0], wrap_test_bitrates(0), true, false);
    let (tp, err) = f.get_translation_params(
        &wrap_test_key_frame(WRAP_TEST_SSRC_LOW, 1, NEW_LOW_START, 0, 0),
        0,
    );
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    let last_ts = tp.rtp.ext_timestamp;

    // one frame later, switch to layer 2: the timestamp must advance by about one frame
    f.allocate_optimal(&[2], wrap_test_bitrates(2), true, false);
    let (tp, err) = f.get_translation_params(
        &wrap_test_key_frame(
            WRAP_TEST_SSRC_HIGH,
            1,
            WRAP_TEST_HIGH_TS + WRAP_TEST_FRAME,
            0,
            2,
        ),
        2,
    );
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    let advance = tp.rtp.ext_timestamp.wrapping_sub(last_ts) as i64;
    assert!(advance > 0);
    assert!(
        advance < 90000,
        "timestamp jumped {} ticks ({:.1} s) across the switch after restart",
        advance,
        advance as f64 / 90000.0
    );
}

// Stream without a usable reference layer (incomplete sender reports, skipReferenceTS): refTS is
// the last outgoing timestamp, already on the downtrack's timeline. No dummy start offset may be
// computed or applied, otherwise the gap between the last packet and the restart is added again
// on every later switch.
#[test]
fn test_forwarder_skip_reference_ts_restart_no_offset() {
    let f = new_wrap_test_forwarder(true);
    f.rtp_stats.as_ref().unwrap().set_clock_rate(90000);

    f.maybe_start(&mut f.lock.write());
    f.allocate_optimal(&[0], wrap_test_bitrates(0), true, false);
    let (tp, err) = f.get_translation_params(
        &wrap_test_key_frame(WRAP_TEST_SSRC_LOW, 1000, 0xf0000000, 0, 0),
        0,
    );
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    assert_eq!(0, f.lock.read().dummy_start_ts_offset);
    let first_ts = tp.rtp.ext_timestamp;

    // the last packet went out 5 s ago
    const GAP: Duration = Duration::from_secs(5);
    f.rtp_stats.as_ref().unwrap().update(
        mono::unix_nano() - GAP.nanoseconds(),
        tp.rtp.ext_sequence_number,
        first_ts,
        true,
        12,
        20,
        0,
        false,
    );

    // upstream restart, new SSRC: the 5 s gap shows up once, at the restart
    f.restart();
    f.determine_codec(
        &webrtc::RtpCodecCapability {
            mime_type: webrtc::MIME_TYPE_H264.to_string(),
            clock_rate: 90000,
            ..Default::default()
        },
        &[],
        livekit::video_layer::Mode::OneSpatialLayerPerStreamIncompleteRtcpSr,
    );
    f.allocate_optimal(&[0], wrap_test_bitrates(0), true, false);
    let (tp, err) = f.get_translation_params(
        &wrap_test_key_frame(WRAP_TEST_SSRC_HIGH, 1, 0x20000000, 0, 0),
        0,
    );
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    assert_eq!(0, f.lock.read().dummy_start_ts_offset);
    assert_in_delta(
        GAP.seconds() * 90000.0,
        (tp.rtp.ext_timestamp.wrapping_sub(first_ts) as i64) as f64,
        90000.0,
    );
    let last_ts = tp.rtp.ext_timestamp;

    // one frame later, switch to another layer: the gap must not be added again
    f.allocate_optimal(&[2], wrap_test_bitrates(2), true, false);
    let (tp, err) = f.get_translation_params(
        &wrap_test_key_frame(
            WRAP_TEST_SSRC_LOW,
            1,
            WRAP_TEST_HIGH_TS + WRAP_TEST_FRAME,
            0,
            2,
        ),
        2,
    );
    assert_eq!(Ok(()), err);
    assert!(!tp.should_drop);
    let advance = tp.rtp.ext_timestamp.wrapping_sub(last_ts) as i64;
    assert!(advance >= 0);
    assert!(
        advance < 90000,
        "timestamp jumped {} ticks ({:.1} s) on the switch after restart",
        advance,
        advance as f64 / 90000.0
    );
}
