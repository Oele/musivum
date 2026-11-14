// Ported from LiveKit, pkg/telemetry/statsworker_test.go at v1.13.11.
// Licensed under the Apache License, Version 2.0 (the original file has no copyright line).
// Changes: translated from Go to Rust. The worker's nil service is a `Weak` that leads
// nowhere. Logging a nil worker is logging `None` (`Option<Arc<StatsWorker>>`, as
// `getOrCreateWorker` logs the worker it found); the zap encoder is `Debug`. The service of
// the second test is made by `new_inner`, which starts no queue and no ticker, as the
// original's `&telemetryService{workers: ...}` has none.

use std::sync::{Arc, Weak};

use super::*;
use crate::telemetry::{NullTelemetryService, TelemetryServiceImpl};

fn no_service() -> Weak<dyn TelemetryService> {
    Weak::<NullTelemetryService>::new()
}

#[test]
fn test_stats_worker() {
    // reference counted close works
    {
        let (g0, g1) = (ReferenceGuard::default(), ReferenceGuard::default());
        let w = new_stats_worker(
            no_service(),
            "".into(),
            "".into(),
            "".into(),
            "".into(),
            Some(&g0),
        );
        assert!(!w.closed(Some(&g1)));
        assert!(!w.close(Some(&g0)));
        assert!(!w.closed(Some(&g1)));
        assert!(w.close(Some(&g1)));
        assert!(w.closed(Some(&g1)));
    }

    // logging a nil worker does not panic
    {
        let w: Option<Arc<StatsWorker>> = None;
        assert_eq!("None", format!("{w:?}"));
    }
}

#[test]
fn test_get_or_create_worker_released_guard() {
    // ParticipantActive overtaken by the participant's close arrives with a guard that
    // ParticipantLeft already released. It must not replace the closed worker with one
    // nothing can release.
    let ts = TelemetryServiceImpl::new_inner(None, Arc::new(super::super::NullAnalyticService {}));
    let (room_id, p_id) = (
        livekit::RoomId::from("room"),
        livekit::ParticipantId::from("participant"),
    );

    let g = ReferenceGuard::default();
    let (w, found) = ts.get_or_create_worker(&room_id, &"".into(), &p_id, &"".into(), Some(&g));
    assert!(!found);
    let w = w.expect("a new worker");
    assert!(w.close(Some(&g)));

    // closed worker still in the map
    {
        let (late, found) =
            ts.get_or_create_worker(&room_id, &"".into(), &p_id, &"".into(), Some(&g));
        assert!(found);
        assert!(Arc::ptr_eq(&w, &late.unwrap()));
        assert!(Arc::ptr_eq(&w, &ts.worker_in_map(&room_id, &p_id).unwrap()));
    }

    // closed worker already reaped
    {
        ts.remove_from_map(&room_id, &p_id);

        let (late, found) =
            ts.get_or_create_worker(&room_id, &"".into(), &p_id, &"".into(), Some(&g));
        assert!(found);
        assert!(late.is_none());
        assert_eq!(0, ts.num_workers_in_map());
        // late.SetConnected() of a nil worker does nothing
        if let Some(late) = late {
            late.set_connected();
        }
    }
}
