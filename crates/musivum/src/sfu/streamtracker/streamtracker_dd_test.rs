// Ported from LiveKit, pkg/sfu/streamtracker/streamtracker_dd_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the test runs on the runtime as the tracker starts a
// worker task; the statuses that the callbacks write are behind a lock.

use std::sync::Arc;

use parking_lot::Mutex;

use super::super::*;
use crate::go::time;
use crate::protocol::logger;
use crate::sfu::buffer;
use crate::sfu::rtpextension::dependencydescriptor as dd;

pub(super) fn create_descriptor_dependency_for_targets(
    max_spatial: i64,
    max_temporal: i64,
) -> buffer::ExtDependencyDescriptor {
    let mut targets: Vec<buffer::DependencyDescriptorDecodeTarget> = Vec::new();
    let mut mask: u32 = 0;
    for i in 0..=max_spatial {
        for j in 0..=max_temporal {
            targets.push(buffer::DependencyDescriptorDecodeTarget {
                target: targets.len() as i64,
                layer: buffer::VideoLayer {
                    spatial: i as i32,
                    temporal: j as i32,
                },
            });
            mask |= 1 << (targets.len() - 1) as u32;
        }
    }

    let mut dtis = vec![dd::DecodeTargetIndication::default(); targets.len()];
    for t in &targets {
        dtis[t.target as usize] = dd::DecodeTargetIndication::Required;
    }

    buffer::ExtDependencyDescriptor {
        descriptor: Some(dd::DependencyDescriptor {
            active_decode_targets_bitmask: Some(mask),
            frame_dependencies: Some(Arc::new(dd::FrameDependencyTemplate {
                decode_target_indications: dtis,
                ..Default::default()
            })),
            ..Default::default()
        }),
        decode_targets: targets,
        active_decode_targets_updated: true,
        ..Default::default()
    }
}

#[track_caller]
fn check_statues(statuses: &[StreamStatus], expected: StreamStatus, max_spatial: i64) {
    for i in 0..=max_spatial {
        assert_eq!(expected, statuses[i as usize]);
    }

    for i in max_spatial + 1..statuses.len() as i64 {
        assert_ne!(expected, statuses[i as usize]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stream_tracker_dd() {
    let dd_tracker = StreamTrackerDependencyDescriptor::new(StreamTrackerParams {
        bitrate_report_interval: time::SECOND,
        logger: logger::get_logger(),
        ..Default::default()
    });
    let mut layered_trackers: Vec<Arc<dyn StreamTrackerWorker>> =
        Vec::with_capacity((buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize);
    let statuses = Arc::new(Mutex::new(vec![
        StreamStatus::default();
        (buffer::DEFAULT_MAX_LAYER_SPATIAL + 1)
            as usize
    ]));
    for i in 0..=buffer::DEFAULT_MAX_LAYER_SPATIAL as i64 {
        let layered_track = dd_tracker.layered_tracker(i as i32);
        let layer = i as usize;
        layered_track.on_status_changed({
            let statuses = statuses.clone();
            move |status| {
                statuses.lock()[layer] = status;
            }
        });
        layered_track.start();
        layered_trackers.push(layered_track);
    }

    // no active layers
    dd_tracker.observe(0, 1000, 1000, false, 0, None);
    check_statues(
        &statuses.lock(),
        StreamStatus::Active,
        buffer::INVALID_LAYER_SPATIAL as i64,
    );

    // layer seen [0,1]
    dd_tracker.observe(
        0,
        1000,
        1000,
        false,
        0,
        Some(&create_descriptor_dependency_for_targets(1, 1)),
    );
    check_statues(&statuses.lock(), StreamStatus::Active, 1);

    // layer seen [0,1,2]
    dd_tracker.observe(
        0,
        1000,
        1000,
        false,
        0,
        Some(&create_descriptor_dependency_for_targets(2, 1)),
    );
    check_statues(&statuses.lock(), StreamStatus::Active, 2);

    // layer 2 gone, layer seen [0,1]
    dd_tracker.observe(
        0,
        1000,
        1000,
        false,
        0,
        Some(&create_descriptor_dependency_for_targets(1, 1)),
    );
    check_statues(&statuses.lock(), StreamStatus::Active, 1);

    // defer ddTracker.Stop()
    dd_tracker.stop();
}
