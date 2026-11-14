// Ported from LiveKit, pkg/sfu/streamtracker/streamtracker_packet_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks of the one test, which runs
// on the runtime as the trackers start worker tasks; `setStatusLocked`, which the original
// calls without the lock, is called with it.

use std::sync::Arc;

use parking_lot::RwLock;

use super::super::*;
use crate::go::atomic;
use crate::go::time::{self, Duration};
use crate::protocol::logger;
use crate::testutils;

fn new_stream_tracker_packet(
    samples_required: u32,
    cycles_required: u32,
    cycle_duration: Duration,
) -> Arc<StreamTracker> {
    let stp = StreamTrackerPacket::new(StreamTrackerPacketParams {
        config: StreamTrackerPacketConfig {
            samples_required,
            cycles_required,
            cycle_duration,
        },
        logger: logger::get_logger(),
    });

    StreamTracker::new(StreamTrackerParams {
        stream_tracker_impl: Some(Box::new(stp)),
        bitrate_report_interval: time::SECOND,
        logger: logger::get_logger(),
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stream_tracker() {
    {
        let name = "flips to active on first observe";
        let callback_called = Arc::new(atomic::Bool::new(false));
        let tracker = new_stream_tracker_packet(5, 60, 500 * time::MILLISECOND);
        tracker.start();
        tracker.on_status_changed({
            let callback_called = callback_called.clone();
            move |_status| {
                callback_called.store(true);
            }
        });
        assert_eq!(StreamStatus::Stopped, tracker.status(), "{name}");

        // observe first packet
        tracker.observe(0, 20, 10, false, 0, None);

        testutils::with_timeout_async(
            || {
                if callback_called.load() {
                    return String::new();
                }

                "first packet didn't activate stream".to_string()
            },
            None,
        )
        .await;

        assert_eq!(StreamStatus::Active, tracker.status(), "{name}");
        assert!(callback_called.load(), "{name}");

        tracker.stop();
    }

    {
        let name = "flips to inactive immediately";
        let tracker = new_stream_tracker_packet(5, 60, 500 * time::MILLISECOND);
        tracker.start();
        assert_eq!(StreamStatus::Stopped, tracker.status(), "{name}");

        let callback_status = Arc::new(RwLock::new(StreamStatus::Stopped));
        tracker.on_status_changed({
            let callback_status = callback_status.clone();
            move |status| {
                *callback_status.write() = status;
            }
        });

        tracker.observe(0, 20, 10, false, 0, None);
        testutils::with_timeout_async(
            || {
                if *callback_status.read() == StreamStatus::Active {
                    return String::new();
                }

                "first packet did not activate stream".to_string()
            },
            None,
        )
        .await;
        assert_eq!(StreamStatus::Active, tracker.status(), "{name}");

        // run a single iteration
        tracker.update_status();

        testutils::with_timeout_async(
            || {
                if *callback_status.read() == StreamStatus::Stopped {
                    return String::new();
                }

                "inactive cycle did not declare stream stopped".to_string()
            },
            None,
        )
        .await;
        assert_eq!(StreamStatus::Stopped, tracker.status(), "{name}");
        assert_eq!(StreamStatus::Stopped, *callback_status.read(), "{name}");

        tracker.stop();
    }

    {
        let name = "flips back to active after iterations";
        let tracker = new_stream_tracker_packet(1, 2, 500 * time::MILLISECOND);
        tracker.start();
        assert_eq!(StreamStatus::Stopped, tracker.status(), "{name}");

        tracker.observe(0, 20, 10, false, 0, None);
        testutils::with_timeout_async(
            || {
                if tracker.status() == StreamStatus::Active {
                    return String::new();
                }

                "first packet did not activate stream".to_string()
            },
            None,
        )
        .await;

        tracker
            .inner
            .write()
            .set_status_locked(StreamStatus::Stopped);

        tracker.observe(0, 20, 10, false, 0, None);
        tracker.update_status();
        assert_eq!(StreamStatus::Stopped, tracker.status(), "{name}");

        tracker.observe(0, 20, 10, false, 0, None);
        tracker.update_status();
        assert_eq!(StreamStatus::Active, tracker.status(), "{name}");

        tracker.stop();
    }

    {
        let name = "changes to inactive when paused";
        let tracker = new_stream_tracker_packet(5, 60, 500 * time::MILLISECOND);
        tracker.start();
        tracker.observe(0, 20, 10, false, 0, None);
        testutils::with_timeout_async(
            || {
                if tracker.status() == StreamStatus::Active {
                    return String::new();
                }

                "first packet did not activate stream".to_string()
            },
            None,
        )
        .await;

        tracker.set_paused(true);
        tracker.update_status();
        assert_eq!(StreamStatus::Stopped, tracker.status(), "{name}");

        tracker.stop();
    }

    {
        let name = "flips back to active on first observe after reset";
        let callback_called = Arc::new(atomic::Uint32::new(0));
        let tracker = new_stream_tracker_packet(5, 60, 500 * time::MILLISECOND);
        tracker.start();
        tracker.on_status_changed({
            let callback_called = callback_called.clone();
            move |_status| {
                callback_called.inc();
            }
        });
        assert_eq!(StreamStatus::Stopped, tracker.status(), "{name}");

        // observe first packet
        tracker.observe(0, 20, 10, false, 0, None);

        testutils::with_timeout_async(
            || {
                if callback_called.load() == 1 {
                    return String::new();
                }

                format!(
                    "expected onStatusChanged to be called once, actual: {}",
                    callback_called.load()
                )
            },
            None,
        )
        .await;

        assert_eq!(StreamStatus::Active, tracker.status(), "{name}");
        assert_eq!(1u32, callback_called.load(), "{name}");

        // observe a few more
        tracker.observe(0, 20, 10, false, 0, None);
        tracker.observe(0, 20, 10, false, 0, None);
        tracker.observe(0, 20, 10, false, 0, None);
        tracker.observe(0, 20, 10, false, 0, None);
        tracker.update_status();

        // should still be active
        assert_eq!(StreamStatus::Active, tracker.status(), "{name}");
        assert_eq!(1u32, callback_called.load(), "{name}");

        // Reset. The first packet after reset should flip state again
        tracker.reset();
        assert_eq!(StreamStatus::Stopped, tracker.status(), "{name}");
        assert_eq!(2u32, callback_called.load(), "{name}");

        // first packet after reset
        tracker.observe(0, 20, 10, false, 0, None);

        testutils::with_timeout_async(
            || {
                if callback_called.load() == 3 {
                    return String::new();
                }

                format!(
                    "expected onStatusChanged to be called thrice, actual {}",
                    callback_called.load()
                )
            },
            None,
        )
        .await;

        assert_eq!(StreamStatus::Active, tracker.status(), "{name}");
        assert_eq!(3u32, callback_called.load(), "{name}");

        tracker.stop();
    }
}
