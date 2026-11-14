// Ported from LiveKit, pkg/rtc/supervisor/participant_supervisor_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks, each under a comment with
// its name. The test at the end is new: upstream's test goes around the timer.

use std::sync::Arc;

use parking_lot::Mutex;

use super::*;
use crate::go::time::MILLISECOND;
use crate::protocol::logger;
use crate::rtc::supervisor::PUBLISH_WAIT_DURATION;
use crate::rtc::types::FakeLocalMediaTrack;
use crate::testutils;

// newSupervisorForTest skips the ticker goroutine, tests call checkPublications directly
fn new_supervisor_for_test() -> ParticipantSupervisor {
    let s = ParticipantSupervisor {
        params: ParticipantSupervisorParams {
            logger: logger::get_logger(),
        },
        inner: RwLock::new(ParticipantSupervisorInner {
            is_publisher_connected: false,
            publications: HashMap::new(),
            on_publication_error: None,
        }),
        is_stopped: atomic::Bool::new(false),
    };
    s.set_publisher_peer_connection_connected(true);
    s
}

fn add_unmuted_publication(s: &ParticipantSupervisor, track_id: &livekit::TrackId) {
    s.add_publication(track_id);
    s.set_publication_mute(track_id, false);
}

// agePublication moves the publish window start back past the publish wait
fn age_publication(s: &ParticipantSupervisor, track_id: &livekit::TrackId) {
    let inner = s.inner.read();
    let pm = as_publication_monitor(&inner.publications[track_id].op_mon).unwrap();
    let mut pm_inner = pm.inner.write();
    pm_inner.unmuted_at = pm_inner.unmuted_at.add(-2 * PUBLISH_WAIT_DURATION);
}

#[test]
fn test_timed_out_publications() {
    let track_id = livekit::TrackId::from("TR_test");

    // abandoned publication is reported and can be removed
    {
        let name = "abandoned publication is reported and can be removed";
        let s = new_supervisor_for_test();
        add_unmuted_publication(&s, &track_id);

        s.check_publications();
        assert!(s.timed_out_publications().is_empty(), "{name}");

        age_publication(&s, &track_id);
        s.check_publications();
        assert_eq!(vec![track_id.clone()], s.timed_out_publications(), "{name}");

        s.remove_publication(&track_id);
        assert!(s.timed_out_publications().is_empty(), "{name}");
        assert!(s.inner.read().publications.is_empty(), "{name}");
    }

    // muted publication is not reported
    {
        let name = "muted publication is not reported";
        let s = new_supervisor_for_test();
        add_unmuted_publication(&s, &track_id);
        s.set_publication_mute(&track_id, true);

        age_publication(&s, &track_id);
        s.check_publications();
        assert!(s.timed_out_publications().is_empty(), "{name}");
    }

    // stale timeout is not reported once the track publishes
    {
        let name = "stale timeout is not reported once the track publishes";
        let s = new_supervisor_for_test();
        add_unmuted_publication(&s, &track_id);
        add_unmuted_publication(&s, &track_id);

        age_publication(&s, &track_id);
        s.check_publications();
        assert_eq!(vec![track_id.clone()], s.timed_out_publications(), "{name}");

        // media arrives late, before the next tick clears the cached error
        s.set_published_track(&track_id, Arc::new(FakeLocalMediaTrack::default()));
        assert!(s.timed_out_publications().is_empty(), "{name}");
    }

    // publication with a published track is not removed
    {
        let name = "publication with a published track is not removed";
        let s = new_supervisor_for_test();
        add_unmuted_publication(&s, &track_id);
        s.set_published_track(&track_id, Arc::new(FakeLocalMediaTrack::default()));

        s.remove_publication(&track_id);
        assert!(
            s.inner.read().publications.contains_key(&track_id),
            "{name}"
        );
    }

    // live publication with a queued successor is not reported
    {
        let name = "live publication with a queued successor is not reported";
        let s = new_supervisor_for_test();
        add_unmuted_publication(&s, &track_id);
        add_unmuted_publication(&s, &track_id); // queued successor shares the track id
        s.set_published_track(&track_id, Arc::new(FakeLocalMediaTrack::default()));

        age_publication(&s, &track_id);
        s.check_publications();
        assert!(s.timed_out_publications().is_empty(), "{name}");
    }
}

// New for musivum: the supervisor as `new` makes it, with its timer. A publication whose
// wait has passed is reported to the callback once, by the task; a stopped supervisor checks
// at most once more; and the task does not keep the supervisor alive.
#[tokio::test(flavor = "multi_thread")]
async fn the_timer_reports_a_timed_out_publication_and_stops() {
    let track_id = livekit::TrackId::from("TR_test");
    let s = ParticipantSupervisor::new(ParticipantSupervisorParams {
        logger: logger::get_logger(),
    });
    let errored = Arc::new(Mutex::new(Vec::<livekit::TrackId>::new()));
    {
        let errored = errored.clone();
        s.on_publication_error(move |track_id| errored.lock().push(track_id.clone()));
    }
    s.set_publisher_peer_connection_connected(true);
    add_unmuted_publication(&s, &track_id);
    age_publication(&s, &track_id);

    {
        let errored = errored.clone();
        testutils::eventually_async(
            move || !errored.lock().is_empty(),
            (3 * MONITOR_INTERVAL).to_std(),
            (10 * MILLISECOND).to_std(),
        )
        .await;
    }
    assert_eq!(vec![track_id.clone()], s.timed_out_publications());

    // Stopped, the loop ends at its next tick, at which the error that stays is not
    // reported again: a publication that times out after that tick is not found any more.
    s.stop();
    time::sleep(MONITOR_INTERVAL + 200 * MILLISECOND).await;
    assert_eq!(vec![track_id.clone()], *errored.lock());
    let other = livekit::TrackId::from("TR_other");
    add_unmuted_publication(&s, &other);
    age_publication(&s, &other);
    time::sleep(MONITOR_INTERVAL + 200 * MILLISECOND).await;
    assert_eq!(vec![track_id.clone()], *errored.lock());
    assert_eq!(vec![track_id], s.timed_out_publications());

    // the task of a supervisor that was not stopped holds it weakly
    let s = ParticipantSupervisor::new(ParticipantSupervisorParams {
        logger: logger::get_logger(),
    });
    let weak = Arc::downgrade(&s);
    drop(s);
    testutils::eventually_async(
        move || weak.upgrade().is_none(),
        MONITOR_INTERVAL.to_std(),
        (10 * MILLISECOND).to_std(),
    )
    .await;
}
