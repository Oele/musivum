// Ported from LiveKit, pkg/rtc/supervisor/publication_monitor_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks, each under a comment with
// its name.

use std::sync::Arc;

use super::*;
use crate::protocol::logger;
use crate::rtc::types::{
    FakeLocalMediaTrack, LocalMediaTrack, OperationMonitorData, OperationMonitorEvent,
};

fn new_monitor_for_test() -> Arc<PublicationMonitor> {
    PublicationMonitor::new(PublicationMonitorParams {
        track_id: "TR_test".into(),
        is_peer_connection_connected: true,
        logger: logger::get_logger(),
    })
}

// addUnmuted adds a pending publish the way the participant does: add, then set mute
fn add_unmuted(m: &PublicationMonitor) {
    m.post_event(
        OperationMonitorEvent::AddPendingPublication,
        OperationMonitorData::None,
    );
    m.post_event(
        OperationMonitorEvent::SetPublicationMute,
        OperationMonitorData::Bool(false),
    );
}

// age moves the publish window start back past the publish wait and returns it
fn age(m: &PublicationMonitor) -> Time {
    let mut inner = m.inner.write();
    inner.unmuted_at = inner.unmuted_at.add(-2 * PUBLISH_WAIT_DURATION);
    inner.unmuted_at
}

fn get_unmuted_at(m: &PublicationMonitor) -> Time {
    let inner = m.inner.read();
    inner.unmuted_at
}

fn new_track() -> Arc<dyn LocalMediaTrack> {
    Arc::new(FakeLocalMediaTrack::default())
}

// Each subtest is one row of the re-arm case table: the event, and whether the
// publish timer restarts because a queued start reached the front.
#[test]
fn test_publication_monitor_rearm() {
    // first add: no re-arm
    {
        let m = new_monitor_for_test();
        m.post_event(
            OperationMonitorEvent::AddPendingPublication,
            OperationMonitorData::None,
        );
        assert!(get_unmuted_at(&m).is_zero(), "first add: no re-arm");
    }

    // successor queued before the track publishes: no re-arm
    {
        let name = "successor queued before the track publishes: no re-arm";
        let m = new_monitor_for_test();
        add_unmuted(&m);
        let aged = age(&m);

        m.post_event(
            OperationMonitorEvent::AddPendingPublication,
            OperationMonitorData::None,
        );
        assert_eq!(aged, get_unmuted_at(&m), "{name}");
        assert_eq!(Err(Error::PublishTimeout.into()), m.check(), "{name}");
    }

    // track publishes: no re-arm
    {
        let name = "track publishes: no re-arm";
        let m = new_monitor_for_test();
        add_unmuted(&m);
        add_unmuted(&m);
        let aged = age(&m);

        m.post_event(
            OperationMonitorEvent::SetPublishedTrack,
            OperationMonitorData::LocalMediaTrack(new_track()),
        );
        assert_eq!(aged, get_unmuted_at(&m), "{name}");
        assert_eq!(Ok(()), m.check(), "{name}"); // front is the live track's end
    }

    // successor queued while the track is live: no re-arm
    {
        let name = "successor queued while the track is live: no re-arm";
        let m = new_monitor_for_test();
        add_unmuted(&m);
        m.post_event(
            OperationMonitorEvent::SetPublishedTrack,
            OperationMonitorData::LocalMediaTrack(new_track()),
        );
        let aged = age(&m);

        m.post_event(
            OperationMonitorEvent::AddPendingPublication,
            OperationMonitorData::None,
        );
        assert_eq!(aged, get_unmuted_at(&m), "{name}");
        assert_eq!(Ok(()), m.check(), "{name}");
    }

    // track closes with a successor queued: re-arm
    {
        let name = "track closes with a successor queued: re-arm";
        let m = new_monitor_for_test();
        add_unmuted(&m);
        add_unmuted(&m);
        let track = new_track();
        m.post_event(
            OperationMonitorEvent::SetPublishedTrack,
            OperationMonitorData::LocalMediaTrack(track.clone()),
        );
        age(&m);

        m.post_event(
            OperationMonitorEvent::ClearPublishedTrack,
            OperationMonitorData::LocalMediaTrack(track),
        );
        assert!(
            time::since(get_unmuted_at(&m)) < PUBLISH_WAIT_DURATION,
            "{name}"
        );
        assert_eq!(Ok(()), m.check(), "{name}");

        // the successor still times out once its own window passes without media
        age(&m);
        assert_eq!(Err(Error::PublishTimeout.into()), m.check(), "{name}");
    }

    // track closes with a muted successor queued: clock cleared
    {
        let name = "track closes with a muted successor queued: clock cleared";
        let m = new_monitor_for_test();
        add_unmuted(&m);
        add_unmuted(&m);
        let track = new_track();
        m.post_event(
            OperationMonitorEvent::SetPublishedTrack,
            OperationMonitorData::LocalMediaTrack(track.clone()),
        );
        m.post_event(
            OperationMonitorEvent::SetPublicationMute,
            OperationMonitorData::Bool(true),
        );
        age(&m);

        m.post_event(
            OperationMonitorEvent::ClearPublishedTrack,
            OperationMonitorData::LocalMediaTrack(track),
        );
        assert!(get_unmuted_at(&m).is_zero(), "{name}");

        // the clock starts on unmute
        m.post_event(
            OperationMonitorEvent::SetPublicationMute,
            OperationMonitorData::Bool(false),
        );
        assert!(
            time::since(get_unmuted_at(&m)) < PUBLISH_WAIT_DURATION,
            "{name}"
        );
    }

    // track closes with nothing queued: no re-arm
    {
        let name = "track closes with nothing queued: no re-arm";
        let m = new_monitor_for_test();
        add_unmuted(&m);
        let track = new_track();
        m.post_event(
            OperationMonitorEvent::SetPublishedTrack,
            OperationMonitorData::LocalMediaTrack(track.clone()),
        );
        let aged = age(&m);

        m.post_event(
            OperationMonitorEvent::ClearPublishedTrack,
            OperationMonitorData::LocalMediaTrack(track),
        );
        assert_eq!(aged, get_unmuted_at(&m), "{name}");
        assert!(m.is_idle(), "{name}");
    }
}
