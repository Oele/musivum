// New for musivum: the original has no tests of these stats. The interval of the reports is
// the original's, five seconds: the test of it runs on the runtime's paused clock.

use std::time::Duration as StdDuration;

use super::*;

use crate::rtc::types::FakeParticipantTelemetryListener;
use crate::telemetry::{FakeTelemetryService, stats_key_for_data};
use crate::testutils::eventually_async;

const WAIT: StdDuration = StdDuration::from_secs(5);
const TICK: StdDuration = StdDuration::from_millis(5);

// What a report holds: one stream with the bytes and the messages of the interval.
fn stat(bytes: u64, packets: u32) -> livekit::AnalyticsStat {
    livekit::AnalyticsStat {
        streams: vec![livekit::AnalyticsStream {
            primary_bytes: bytes,
            primary_packets: packets,
            ..Default::default()
        }],
        ..Default::default()
    }
}

// The same as the telemetry service is handed it for a signal connection: with the room.
fn stat_of_room(room: &livekit::Room, bytes: u64, packets: u32) -> livekit::AnalyticsStat {
    livekit::AnalyticsStat {
        room_id: room.sid.clone(),
        room_name: room.name.clone(),
        ..stat(bytes, packets)
    }
}

fn room(sid: &str, name: &str) -> livekit::Room {
    livekit::Room {
        sid: sid.to_string(),
        name: name.to_string(),
        ..Default::default()
    }
}

fn participant(sid: &str, identity: &str) -> livekit::ParticipantInfo {
    livekit::ParticipantInfo {
        sid: sid.to_string(),
        identity: identity.to_string(),
        kind: livekit::participant_info::Kind::Agent as i32,
        kind_details: vec![livekit::participant_info::KindDetail::Forwarded as i32],
        ..Default::default()
    }
}

fn totals(s: &TrafficTotals) -> (u64, u32, u64, u32) {
    (s.send_bytes, s.send_messages, s.recv_bytes, s.recv_messages)
}

#[test]
fn the_track_of_a_participants_bytes_is_named_after_its_type_and_the_participant() {
    assert_eq!("DT", BytesTrackType::Data.to_string());
    assert_eq!("SG", BytesTrackType::Signal.to_string());
    assert_eq!(
        "TR_DTPA_1",
        bytes_track_id_for_participant_id(BytesTrackType::Data, &"PA_1".into()).as_str()
    );
    assert_eq!(
        "TR_SGPA_1",
        bytes_track_id_for_participant_id(BytesTrackType::Signal, &"PA_1".into()).as_str()
    );
}

#[tokio::test(start_paused = true)]
async fn bytes_are_counted_in_each_direction_and_reported_at_the_interval_until_stopped() {
    let listener = Arc::new(FakeParticipantTelemetryListener::default());
    let l = listener.clone();
    let s = BytesTrackStats::new("NL", &"TR_data".into(), &"PA_1".into(), move || {
        let l: Arc<dyn ParticipantTelemetryListener> = l.clone();
        l
    });
    let key = |stream_type| stats_key_for_data("NL", stream_type, "PA_1".into(), "TR_data".into());

    s.add_bytes(100, true);
    s.add_bytes(20, true);
    s.add_bytes(7, false);
    let t = s.get_traffic_totals();
    assert_eq!((120, 2, 7, 1), totals(&t));
    assert!(time::since(t.at) >= Duration::ZERO && time::since(t.at) < time::MINUTE);

    // nothing is reported before the interval is over; then what was received and what was
    // sent, each under its key
    tokio::time::sleep(StdDuration::from_millis(4900)).await;
    assert_eq!(0, listener.on_track_stats_call_count());
    tokio::time::sleep(StdDuration::from_millis(200)).await;
    assert_eq!(2, listener.on_track_stats_call_count());
    assert_eq!(
        (key(livekit::StreamType::Upstream), stat(7, 1)),
        listener.on_track_stats_args_for_call(0)
    );
    assert_eq!(
        (key(livekit::StreamType::Downstream), stat(120, 2)),
        listener.on_track_stats_args_for_call(1)
    );

    // an interval in which nothing was counted has no report
    tokio::time::sleep(StdDuration::from_secs(5)).await;
    assert_eq!(2, listener.on_track_stats_call_count());

    // a report is of its interval and of the directions that had bytes; the totals are of
    // all the time
    s.add_bytes(30, true);
    tokio::time::sleep(StdDuration::from_secs(5)).await;
    assert_eq!(3, listener.on_track_stats_call_count());
    assert_eq!(
        (key(livekit::StreamType::Downstream), stat(30, 1)),
        listener.on_track_stats_args_for_call(2)
    );
    assert_eq!((150, 3, 7, 1), totals(&s.get_traffic_totals()));

    // `stop` ends the worker: it reports what was counted since the last report and lets
    // go of the stats
    s.add_bytes(9, false);
    assert_eq!(2, Arc::strong_count(&s));
    s.stop();
    tokio::time::sleep(StdDuration::from_millis(1)).await;
    assert_eq!(4, listener.on_track_stats_call_count());
    assert_eq!(
        (key(livekit::StreamType::Upstream), stat(9, 1)),
        listener.on_track_stats_args_for_call(3)
    );
    assert_eq!(1, Arc::strong_count(&s));

    // what is counted after that is in the totals and in no report
    s.add_bytes(1, true);
    tokio::time::sleep(StdDuration::from_secs(11)).await;
    assert_eq!(4, listener.on_track_stats_call_count());
    assert_eq!((151, 4, 16, 2), totals(&s.get_traffic_totals()));
}

#[tokio::test(start_paused = true)]
async fn stats_that_counted_nothing_report_nothing() {
    let listener = Arc::new(FakeParticipantTelemetryListener::default());
    let l = listener.clone();
    let s = BytesTrackStats::new("", &"TR_data".into(), &"PA_1".into(), move || {
        let l: Arc<dyn ParticipantTelemetryListener> = l.clone();
        l
    });

    tokio::time::sleep(StdDuration::from_secs(11)).await;
    s.stop();
    tokio::time::sleep(StdDuration::from_millis(1)).await;
    assert_eq!(1, Arc::strong_count(&s));
    assert_eq!(0, listener.on_track_stats_call_count());
    assert_eq!((0, 0, 0, 0), totals(&s.get_traffic_totals()));
}

#[tokio::test(flavor = "multi_thread")]
async fn signal_stats_report_to_the_telemetry_service_once_room_and_participant_are_known() {
    let telemetry = Arc::new(FakeTelemetryService::default());
    let s = BytesSignalStats::new(telemetry.clone());

    // counted before anything is known: kept for the first report
    s.add_bytes(10, false);

    // no room, a room without an ID and no participant resolve nothing
    s.resolve_room(None);
    s.resolve_room(Some(&room("", "a-room")));
    s.resolve_participant(None);
    assert!(s.resolved.read().ri.is_none());
    assert!(s.resolved.read().pi.is_none());

    // the room alone starts nothing
    let ri = livekit::Room {
        metadata: "not kept".to_string(),
        ..room("RM_1", "a-room")
    };
    s.resolve_room(Some(&ri));
    assert_eq!(0, telemetry.participant_joined_call_count());
    assert!(s.mu.lock().is_none());

    // with the participant the stats start: the participant joins, as far as telemetry is
    // concerned, with what the stats keep of the room and of the participant
    let pi = livekit::ParticipantInfo {
        name: "not kept".to_string(),
        ..participant("PA_1", "alice")
    };
    s.resolve_participant(Some(&pi));
    assert_eq!(1, telemetry.participant_joined_call_count());
    assert!(s.mu.lock().is_some());
    let (joined_room, joined_participant, client_info, client_meta, should_send_event, guard) =
        telemetry.participant_joined_args_for_call(0);
    assert_eq!(room("RM_1", "a-room"), joined_room);
    assert_eq!(participant("PA_1", "alice"), joined_participant);
    assert_eq!(None, client_info);
    assert_eq!(None, client_meta);
    assert!(!should_send_event);
    let guard = guard.expect("a guard is handed over");

    // what is resolved stays
    s.resolve_room(Some(&room("RM_2", "another-room")));
    s.resolve_participant(Some(&participant("PA_2", "bob")));
    assert_eq!(1, telemetry.participant_joined_call_count());

    s.add_bytes(100, true);
    s.add_bytes(5, true);
    assert_eq!((105, 2, 10, 1), totals(&s.get_traffic_totals()));
    assert_eq!(0, telemetry.track_stats_call_count());

    // `stop` ends the worker: a last report, with the room and under the key of the
    // participant's signal track, and the participant leaves
    s.stop();
    eventually_async(|| telemetry.participant_left_call_count() == 1, WAIT, TICK).await;
    assert_eq!(2, telemetry.track_stats_call_count());
    let key = |stream_type| stats_key_for_data("", stream_type, "PA_1".into(), "TR_SGPA_1".into());
    assert_eq!(
        (
            "RM_1".into(),
            "a-room".into(),
            key(livekit::StreamType::Upstream),
            stat_of_room(&joined_room, 10, 1)
        ),
        telemetry.track_stats_args_for_call(0)
    );
    assert_eq!(
        (
            "RM_1".into(),
            "a-room".into(),
            key(livekit::StreamType::Downstream),
            stat_of_room(&joined_room, 105, 2)
        ),
        telemetry.track_stats_args_for_call(1)
    );
    let (left_room, left_participant, should_send_event, left_guard) =
        telemetry.participant_left_args_for_call(0);
    assert_eq!(room("RM_1", "a-room"), left_room);
    assert_eq!(participant("PA_1", "alice"), left_participant);
    assert!(!should_send_event);
    assert!(Arc::ptr_eq(&guard, &left_guard.unwrap()));

    // the worker lets go of the stats
    eventually_async(|| Arc::strong_count(&s) == 1, WAIT, TICK).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn signal_stats_that_counted_nothing_report_nothing() {
    let telemetry = Arc::new(FakeTelemetryService::default());
    let s = BytesSignalStats::new(telemetry.clone());

    s.resolve_participant(Some(&participant("PA_1", "alice")));
    s.resolve_room(Some(&room("RM_1", "a-room")));
    assert_eq!(1, telemetry.participant_joined_call_count());

    s.stop();
    eventually_async(|| telemetry.participant_left_call_count() == 1, WAIT, TICK).await;
    assert_eq!(0, telemetry.track_stats_call_count());
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_waits_for_the_worker_and_the_stats_start_again_for_another_room() {
    let telemetry = Arc::new(FakeTelemetryService::default());
    let s = BytesSignalStats::new(telemetry.clone());

    // before the stats started, `reset` forgets what was resolved
    s.resolve_room(Some(&room("RM_1", "first")));
    s.reset();
    s.resolve_participant(Some(&participant("PA_1", "alice")));
    assert_eq!(0, telemetry.participant_joined_call_count());
    s.resolve_room(Some(&room("RM_1", "first")));
    assert_eq!(1, telemetry.participant_joined_call_count());
    let first_guard = telemetry.participant_joined_args_for_call(0).5.unwrap();

    // when `reset` returns the worker is done: it has reported and the participant has left
    s.add_bytes(40, true);
    s.reset();
    assert_eq!(1, telemetry.track_stats_call_count());
    assert_eq!(
        (
            "RM_1".into(),
            "first".into(),
            stats_key_for_data(
                "",
                livekit::StreamType::Downstream,
                "PA_1".into(),
                "TR_SGPA_1".into()
            ),
            stat_of_room(&room("RM_1", "first"), 40, 1)
        ),
        telemetry.track_stats_args_for_call(0)
    );
    assert_eq!(1, telemetry.participant_left_call_count());
    let (left_room, left_participant, _, left_guard) = telemetry.participant_left_args_for_call(0);
    assert_eq!(room("RM_1", "first"), left_room);
    assert_eq!(participant("PA_1", "alice"), left_participant);
    assert!(Arc::ptr_eq(&first_guard, &left_guard.unwrap()));
    assert!(s.mu.lock().is_none());
    assert!(s.resolved.read().ri.is_none());
    assert!(s.resolved.read().pi.is_none());

    // what is counted meanwhile is reported for the room that comes next, under the key of
    // the participant there and with a guard of its own
    s.add_bytes(2, false);
    s.resolve_room(Some(&room("RM_2", "second")));
    s.resolve_participant(Some(&participant("PA_2", "alice")));
    assert_eq!(2, telemetry.participant_joined_call_count());
    let second_guard = telemetry.participant_joined_args_for_call(1).5.unwrap();
    assert!(!Arc::ptr_eq(&first_guard, &second_guard));

    s.stop();
    eventually_async(|| telemetry.participant_left_call_count() == 2, WAIT, TICK).await;
    assert_eq!(2, telemetry.track_stats_call_count());
    assert_eq!(
        (
            "RM_2".into(),
            "second".into(),
            stats_key_for_data(
                "",
                livekit::StreamType::Upstream,
                "PA_2".into(),
                "TR_SGPA_2".into()
            ),
            stat_of_room(&room("RM_2", "second"), 2, 1)
        ),
        telemetry.track_stats_args_for_call(1)
    );
    let (left_room, left_participant, _, left_guard) = telemetry.participant_left_args_for_call(1);
    assert_eq!(room("RM_2", "second"), left_room);
    assert_eq!(participant("PA_2", "alice"), left_participant);
    assert!(Arc::ptr_eq(&second_guard, &left_guard.unwrap()));
    assert_eq!((40, 1, 2, 1), totals(&s.get_traffic_totals()));
}
