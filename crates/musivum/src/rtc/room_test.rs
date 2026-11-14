// Ported from LiveKit, pkg/rtc/room_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The two `init()` have no counterpart: the counters of
// `telemetry::prometheus` need no `Init`, the logger no configuration, and
// `roomUpdateInterval = defaultDelay` is the value `ROOM_UPDATE_INTERVAL` has in a test build
// (room.rs). `newRoomWithParticipants` gives the room `FakeTelemetryService` where upstream
// builds the real telemetry service around a webhook notifier with a fake key provider:
// that service is not ported, and no test here looks at what reaches telemetry or its
// webhooks, so nothing that is asserted is lost. `t.Cleanup(rm.Close)` is a guard that closes
// the room when it is dropped (`TestRoom`). `WebRTCConfig{}` is a configuration of the node
// with the receiver's sizes zero, as in the original, made once. A `t.Run` is a block that
// names its subtest in the messages of its assertions; a table is a loop.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Barrier, LazyLock};
use std::time::Duration as StdDuration;

use parking_lot::Mutex;

use super::*;
use crate::config;
use crate::go::atomic;
use crate::go::time;
use crate::protocol::livekit;
use crate::rtc::config::{ReceiverConfig, WebRtcConfig};
use crate::rtc::testutils::{new_mock_participant, new_mock_track};
use crate::rtc::types::{
    self, FakeLocalParticipant, FakeMediaTrack, LocalParticipant as _, Participant as _,
};
use crate::sfu;
use crate::sfu::audio;
use crate::telemetry::FakeTelemetryService;
use crate::testutils::{eventually_async, fakes, with_timeout_async};
use crate::webrtc::peerconnection::DtlsRole;

const NUM_PARTICIPANTS: usize = 3;
const DEFAULT_DELAY: StdDuration = StdDuration::from_millis(10);
const AUDIO_UPDATE_INTERVAL: u32 = 25;

fn ice_servers_for_room() -> Vec<livekit::IceServer> {
    vec![livekit::IceServer {
        urls: vec!["stun:stun.l.google.com:19302".to_string()],
        ..Default::default()
    }]
}

// `p == op` of two interfaces upstream: the same participant.
fn same<T: ?Sized, U: ?Sized>(a: &Arc<T>, b: &Arc<U>) -> bool {
    std::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b))
}

fn downcast(p: &Arc<dyn types::LocalParticipant>) -> Arc<FakeLocalParticipant> {
    fakes::downcast::<FakeLocalParticipant>(p.clone())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_joined_state() {
    {
        let name = "new room should return joinedAt 0";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 0,
            ..Default::default()
        });
        assert_eq!(0, rm.first_joined_at(), "{name}");
        assert_eq!(0, rm.last_left_at(), "{name}");
    }

    {
        let name = "should be current time when a participant joins";
        let s = time::now().unix_secs();
        let rm = new_room_with_participants(TestRoomOpts {
            num: 1,
            ..Default::default()
        });
        assert!(s <= rm.first_joined_at(), "{name}");
        assert_eq!(0, rm.last_left_at(), "{name}");
    }

    {
        let name = "should be set when a participant leaves";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 1,
            ..Default::default()
        });
        let p0 = rm.get_participants()[0].clone();
        let s = time::now().unix_secs();
        rm.remove_participant(
            &p0.identity(),
            &p0.id(),
            types::ParticipantCloseReason::ClientRequestLeave,
        );
        assert!(s <= rm.last_left_at(), "{name}");
    }

    {
        let name = "LastLeftAt should be set when there are still participants in the room";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 2,
            ..Default::default()
        });
        let p0 = rm.get_participants()[0].clone();
        rm.remove_participant(
            &p0.identity(),
            &p0.id(),
            types::ParticipantCloseReason::ClientRequestLeave,
        );
        assert!(rm.last_left_at() > 0, "{name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_room_join() {
    {
        let name = "joining returns existing participant data";
        let rm = new_room_with_participants(TestRoomOpts {
            num: NUM_PARTICIPANTS,
            ..Default::default()
        });
        let p_new = new_mock_participant(
            "new".into(),
            types::ProtocolVersion(types::CURRENT_PROTOCOL),
            false,
            false,
            rm.local_participant_listener(),
        );

        let _ = rm.join(p_new.clone(), None, None, &ice_servers_for_room());

        // expect new participant to get a JoinReply
        let res = p_new.send_join_response_args_for_call(0);
        assert_eq!(
            livekit::RoomId::from(&res.room.as_ref().unwrap().sid),
            rm.id(),
            "{name}"
        );
        assert_eq!(NUM_PARTICIPANTS, res.other_participants.len(), "{name}");
        assert_eq!(NUM_PARTICIPANTS + 1, rm.get_participants().len(), "{name}");
        assert!(!res.ice_servers.is_empty(), "{name}");
    }

    {
        let name = "subscribe to existing channels upon join";
        let num_existing = 3;
        let rm = new_room_with_participants(TestRoomOpts {
            num: num_existing,
            ..Default::default()
        });
        let lpl = rm.local_participant_listener();
        let p = new_mock_participant(
            "new".into(),
            types::ProtocolVersion(types::CURRENT_PROTOCOL),
            false,
            false,
            lpl.clone(),
        );

        let err = rm.join(
            p.clone(),
            None,
            Some(ParticipantOptions {
                auto_subscribe: true,
                ..Default::default()
            }),
            &ice_servers_for_room(),
        );
        assert_eq!(Ok(()), err, "{name}");

        p.state_returns(livekit::participant_info::State::Active);
        lpl.on_state_change(p.clone());

        // it should become a subscriber when connectivity changes
        let mut num_tracks = 0;
        for op in rm.get_participants() {
            if same(&p, &op) {
                continue;
            }

            num_tracks += op.get_published_tracks().len();
        }
        assert_eq!(num_tracks, p.subscribe_to_track_call_count(), "{name}");
    }

    {
        let name = "participant state change is broadcasted to others";
        let rm = new_room_with_participants(TestRoomOpts {
            num: NUM_PARTICIPANTS,
            ..Default::default()
        });
        let changed_participant: Arc<Mutex<Option<Arc<dyn types::Participant>>>> = Arc::default();
        {
            let changed_participant = changed_participant.clone();
            rm.on_participant_changed(move |participant| {
                *changed_participant.lock() = Some(participant);
            });
        }
        let participants = rm.get_participants();
        let p = downcast(&participants[0]);
        let disconnected_participant = downcast(&participants[1]);
        disconnected_participant.state_returns(livekit::participant_info::State::Disconnected);

        rm.remove_participant(
            &p.identity(),
            &p.id(),
            types::ParticipantCloseReason::ClientRequestLeave,
        );
        tokio::time::sleep(DEFAULT_DELAY).await;

        assert!(
            changed_participant
                .lock()
                .as_ref()
                .is_some_and(|changed| same(&p, changed)),
            "{name}"
        );

        let mut num_updates = 0;
        for op in &participants {
            if same(op, &p) || same(op, &disconnected_participant) {
                assert_eq!(0, p.send_participant_update_call_count(), "{name}");
                continue;
            }
            let fake_p = downcast(op);
            assert_eq!(1, fake_p.send_participant_update_call_count(), "{name}");
            num_updates += 1;
        }
        assert_eq!(NUM_PARTICIPANTS - 2, num_updates, "{name}");
    }

    {
        let name = "cannot exceed max participants";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 1,
            ..Default::default()
        });
        rm.lock.write().proto_room.max_participants = 1;
        let p = new_mock_participant(
            "second".into(),
            types::ProtocolVersion(0),
            false,
            false,
            rm.local_participant_listener(),
        );

        let err = rm.join(p, None, None, &ice_servers_for_room());
        assert_eq!(Err(Error::MaxParticipantsExceeded), err, "{name}");
    }
}

// various state changes to participant and that others are receiving update
#[tokio::test(flavor = "multi_thread")]
async fn test_participant_update() {
    type Action = fn(&dyn types::LocalParticipant);
    let tests: [(&str, bool, Action); 3] = [
        ("track mutes are sent to everyone", true, |p| {
            p.set_track_muted(
                &livekit::MuteTrackRequest {
                    muted: true,
                    ..Default::default()
                },
                false,
            );
        }),
        ("track metadata updates are sent to everyone", true, |p| {
            p.set_metadata("");
        }),
        (
            "track publishes are sent to existing participants",
            true,
            |p| {
                p.add_track(&livekit::AddTrackRequest {
                    r#type: livekit::TrackType::Video as i32,
                    ..Default::default()
                });
            },
        ),
    ];

    for (name, send_to_sender, action) in tests {
        let rm = new_room_with_participants(TestRoomOpts {
            num: 3,
            ..Default::default()
        });
        // remember how many times send has been called for each
        let mut call_counts: HashMap<livekit::ParticipantId, usize> = HashMap::new();
        for p in rm.get_participants() {
            let fp = downcast(&p);
            call_counts.insert(p.id(), fp.send_participant_update_call_count());
        }

        let sender = rm.get_participants()[0].clone();
        action(&*sender);

        // go through the other participants, make sure they've received update
        for p in rm.get_participants() {
            let mut expected = call_counts[&p.id()];
            if !same(&p, &sender) || send_to_sender {
                expected += 1;
            }
            let fp = downcast(&p);
            assert_eq!(expected, fp.send_participant_update_call_count(), "{name}");
        }
    }
}

type Validate = Box<dyn Fn(&str, &Room, &[ParticipantUpdate])>;

#[derive(Default)]
struct PushAndDequeueCase {
    name: &'static str,
    pi: livekit::ParticipantInfo,
    close_reason: types::ParticipantCloseReason,
    immediate: bool,
    existing: Option<ParticipantUpdate>,
    expected: Vec<ParticipantUpdate>,
    validate: Option<Validate>,
}

#[tokio::test(flavor = "multi_thread")]
async fn test_push_and_dequeue_updates() {
    let identity: &'static str = "test_user";
    let publisher1v1 = livekit::ParticipantInfo {
        identity: identity.to_string(),
        sid: "1".to_string(),
        is_publisher: true,
        version: 1,
        joined_at: 0,
        ..Default::default()
    };
    let publisher1v2 = livekit::ParticipantInfo {
        identity: identity.to_string(),
        sid: "1".to_string(),
        is_publisher: true,
        version: 2,
        joined_at: 1,
        ..Default::default()
    };
    let publisher2 = livekit::ParticipantInfo {
        identity: identity.to_string(),
        sid: "2".to_string(),
        is_publisher: true,
        version: 1,
        joined_at: 2,
        ..Default::default()
    };
    let subscriber1v1 = livekit::ParticipantInfo {
        identity: identity.to_string(),
        sid: "1".to_string(),
        version: 1,
        joined_at: 0,
        ..Default::default()
    };
    let subscriber1v2 = livekit::ParticipantInfo {
        identity: identity.to_string(),
        sid: "1".to_string(),
        version: 2,
        joined_at: 1,
        ..Default::default()
    };

    fn require_pi_equals(name: &str, a: &livekit::ParticipantInfo, b: &livekit::ParticipantInfo) {
        assert_eq!(a.sid, b.sid, "{name}");
        assert_eq!(a.identity, b.identity, "{name}");
        assert_eq!(a.version, b.version, "{name}");
    }
    let queued = move |rm: &Room| {
        rm.batched_updates
            .lock()
            .get(&livekit::ParticipantIdentity::from(identity))
            .cloned()
    };
    let update = |pi: &livekit::ParticipantInfo| ParticipantUpdate {
        participant_info: pi.clone(),
        ..Default::default()
    };

    let test_cases = vec![
        PushAndDequeueCase {
            name: "publisher updates are immediate",
            pi: publisher1v1.clone(),
            expected: vec![update(&publisher1v1)],
            ..Default::default()
        },
        PushAndDequeueCase {
            name: "subscriber updates are queued",
            pi: subscriber1v1.clone(),
            ..Default::default()
        },
        PushAndDequeueCase {
            name: "last version is enqueued",
            pi: subscriber1v2.clone(),
            // clone the existing value since it can be modified when setting to disconnected
            existing: Some(update(&subscriber1v1)),
            validate: Some({
                let subscriber1v2 = subscriber1v2.clone();
                Box::new(move |name, rm, _| {
                    let queued = queued(rm);
                    assert!(queued.is_some(), "{name}");
                    require_pi_equals(name, &subscriber1v2, &queued.unwrap().participant_info);
                })
            }),
            ..Default::default()
        },
        PushAndDequeueCase {
            name: "latest version when immediate",
            pi: subscriber1v2.clone(),
            existing: Some(update(&subscriber1v1)),
            immediate: true,
            expected: vec![update(&subscriber1v2)],
            validate: Some(Box::new(move |name, rm, _| {
                let queued = queued(rm);
                assert!(queued.is_none(), "{name}");
            })),
            ..Default::default()
        },
        PushAndDequeueCase {
            name: "out of order updates are rejected",
            pi: subscriber1v1.clone(),
            existing: Some(update(&subscriber1v2)),
            validate: Some({
                let subscriber1v2 = subscriber1v2.clone();
                Box::new(move |name, rm, _| {
                    let queued = queued(rm);
                    require_pi_equals(name, &subscriber1v2, &queued.unwrap().participant_info);
                })
            }),
            ..Default::default()
        },
        PushAndDequeueCase {
            name: "sid change is broadcasted immediately with synthsized disconnect",
            pi: publisher2.clone(),
            // just to test if update contain the close reason
            close_reason: types::ParticipantCloseReason::ServiceRequestRemoveParticipant,
            existing: Some(ParticipantUpdate {
                participant_info: subscriber1v2.clone(),
                close_reason: types::ParticipantCloseReason::Stale,
                ..Default::default()
            }),
            expected: vec![
                ParticipantUpdate {
                    participant_info: livekit::ParticipantInfo {
                        identity: identity.to_string(),
                        sid: "1".to_string(),
                        version: 2,
                        state: livekit::participant_info::State::Disconnected as i32,
                        ..Default::default()
                    },
                    is_synthesized_disconnect: true,
                    close_reason: types::ParticipantCloseReason::Stale,
                },
                ParticipantUpdate {
                    participant_info: publisher2.clone(),
                    close_reason: types::ParticipantCloseReason::ServiceRequestRemoveParticipant,
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        PushAndDequeueCase {
            name: "when switching to publisher, queue is cleared",
            pi: publisher1v2.clone(),
            existing: Some(update(&subscriber1v1)),
            expected: vec![update(&publisher1v2)],
            validate: Some(Box::new(|name, rm, _| {
                assert!(rm.batched_updates.lock().is_empty(), "{name}");
            })),
            ..Default::default()
        },
    ];

    for tc in test_cases {
        let name = tc.name;
        let rm = new_room_with_participants(TestRoomOpts {
            num: 1,
            ..Default::default()
        });
        if let Some(existing) = &tc.existing {
            rm.batched_updates.lock().insert(
                livekit::ParticipantIdentity::from(&existing.participant_info.identity),
                existing.clone(),
            );
        }
        let mut batched_updates = rm.batched_updates.lock();
        let updates = push_and_dequeue_updates(
            tc.pi.clone(),
            tc.close_reason,
            tc.immediate,
            rm.get_participant(&livekit::ParticipantIdentity::from(&tc.pi.identity))
                .as_deref()
                .map(|p| p as &dyn types::Participant),
            &mut batched_updates,
        );
        drop(batched_updates);
        assert_eq!(tc.expected.len(), updates.len(), "{name}");
        for (i, item) in tc.expected.iter().enumerate() {
            require_pi_equals(name, &item.participant_info, &updates[i].participant_info);
            assert_eq!(
                item.is_synthesized_disconnect, updates[i].is_synthesized_disconnect,
                "{name}"
            );
            assert_eq!(item.close_reason, updates[i].close_reason, "{name}");
        }

        if let Some(validate) = &tc.validate {
            validate(name, &rm, &updates);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_room_closure() {
    {
        let name = "room closes after participant leaves";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 1,
            ..Default::default()
        });
        let closed: Arc<Mutex<(bool, types::RoomCloseReason)>> = Arc::default();
        {
            let closed = closed.clone();
            rm.on_close(move |reason| {
                *closed.lock() = (true, reason);
            });
        }
        let p = rm.get_participants()[0].clone();
        // allows immediate close after
        rm.lock.write().proto_room.empty_timeout = 0;
        rm.remove_participant(
            &p.identity(),
            &p.id(),
            types::ParticipantCloseReason::ClientRequestLeave,
        );

        tokio::time::sleep(
            StdDuration::from_secs(rm.to_proto().departure_timeout as u64) + DEFAULT_DELAY,
        )
        .await;

        rm.close_if_empty();
        assert_eq!(0, rm.get_participants().len(), "{name}");
        let (is_closed, close_reason) = *closed.lock();
        assert!(is_closed, "{name}");
        assert_eq!(types::RoomCloseReason::IdleTimeout, close_reason, "{name}");

        assert_eq!(
            Err(Error::RoomClosed),
            rm.join(p, None, None, &ice_servers_for_room()),
            "{name}"
        );
    }

    // the idle check runs every second and can land while the last participant
    // is still closing, when the room is empty but its departure has not been
    // recorded yet
    let close_during_remove = |prior_departure: bool| -> bool {
        let rm = new_room_with_participants(TestRoomOpts {
            num: 1,
            ..Default::default()
        });
        let is_closed = Arc::new(atomic::Bool::new(false));
        {
            let is_closed = is_closed.clone();
            rm.on_close(move |_| {
                is_closed.store(true);
            });
        }

        {
            let mut r = rm.lock.write();
            r.proto_room.departure_timeout = 20;
            if prior_departure {
                // someone left earlier in the session, so a departure is recorded
                r.proto_room.creation_time = time::now().unix_secs();
                rm.left_at.store(time::now().add(-time::MINUTE).unix_secs());
            } else {
                // older than the empty timeout, as a room in a call for a while is
                r.proto_room.creation_time = time::now().add(-10 * time::MINUTE).unix_secs();
            }
        }

        let p = downcast(&rm.get_participants()[0]);
        // `closing` and `release`, channels that are closed upstream
        let closing = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        {
            let (closing, release) = (closing.clone(), release.clone());
            p.close_stub(move |_, _, _| {
                closing.wait();
                release.wait();
                Ok(())
            });
        }

        let remove = {
            let (rm, p) = (rm.0.clone(), p.clone());
            std::thread::spawn(move || {
                rm.remove_participant(
                    &p.identity(),
                    &p.id(),
                    types::ParticipantCloseReason::ClientRequestLeave,
                );
            })
        };

        closing.wait();
        assert!(rm.get_participants().is_empty());
        rm.close_if_empty();
        release.wait();
        remove.join().unwrap();

        is_closed.load()
    };

    {
        let name = "room does not close while the last participant is closing";
        assert!(!close_during_remove(false), "{name}");
    }

    {
        let name =
            "room does not close while the last participant is closing, after an earlier departure";
        assert!(!close_during_remove(true), "{name}");
    }

    {
        let name = "room does not close before empty timeout";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 0,
            ..Default::default()
        });
        let closed: Arc<Mutex<(bool, types::RoomCloseReason)>> = Arc::default();
        {
            let closed = closed.clone();
            rm.on_close(move |reason| {
                *closed.lock() = (true, reason);
            });
        }
        assert_ne!(0, rm.lock.read().proto_room.empty_timeout, "{name}");
        rm.close_if_empty();
        let (is_closed, close_reason) = *closed.lock();
        assert!(!is_closed, "{name}");
        assert_eq!(types::RoomCloseReason::Unknown, close_reason, "{name}");
    }

    {
        let name = "room closes after empty timeout";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 0,
            ..Default::default()
        });
        let closed: Arc<Mutex<(bool, types::RoomCloseReason)>> = Arc::default();
        {
            let closed = closed.clone();
            rm.on_close(move |reason| {
                *closed.lock() = (true, reason);
            });
        }
        rm.lock.write().proto_room.empty_timeout = 1;

        tokio::time::sleep(StdDuration::from_millis(1010)).await;
        rm.close_if_empty();
        let (is_closed, close_reason) = *closed.lock();
        assert!(is_closed, "{name}");
        assert_eq!(types::RoomCloseReason::IdleTimeout, close_reason, "{name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_track() {
    let name = "new track should be added to ready participants";
    let rm = new_room_with_participants(TestRoomOpts {
        num: 3,
        ..Default::default()
    });
    let lpl = rm.local_participant_listener();

    let participants = rm.get_participants();
    let p0 = downcast(&participants[0]);
    p0.state_returns(livekit::participant_info::State::Joined);
    let p1 = downcast(&participants[1]);
    p1.state_returns(livekit::participant_info::State::Active);

    let publisher = downcast(&participants[2]);

    // pub adds track
    let track = new_mock_track(livekit::TrackType::Video, "webcam");
    lpl.on_track_published(publisher, track);

    // only p1 should've been subscribed to
    assert_eq!(0, p0.subscribe_to_track_call_count(), "{name}");
    assert_eq!(1, p1.subscribe_to_track_call_count(), "{name}");
}

fn get_active_speaker_updates(p: &FakeLocalParticipant) -> Vec<Vec<livekit::SpeakerInfo>> {
    let mut updates = Vec::new();
    let num_calls = p.send_speaker_update_call_count();
    for i in 0..num_calls {
        let (infos, _) = p.send_speaker_update_args_for_call(i);
        updates.push(infos);
    }
    updates
}

#[tokio::test(flavor = "multi_thread")]
async fn test_active_speakers() {
    let audio_update_duration = StdDuration::from_millis(AUDIO_UPDATE_INTERVAL as u64 + 10);
    {
        let name = "participant should not be getting audio updates (protocol 2)";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 1,
            protocol: 2,
            ..Default::default()
        });
        let p = downcast(&rm.get_participants()[0]);
        assert!(rm.get_active_speakers().is_empty(), "{name}");

        tokio::time::sleep(audio_update_duration).await;

        let updates = get_active_speaker_updates(&p);
        assert!(updates.is_empty(), "{name}");
    }

    {
        let name = "speakers should be sorted by loudness";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 2,
            ..Default::default()
        });
        let participants = rm.get_participants();
        let p = downcast(&participants[0]);
        let p2 = downcast(&participants[1]);
        p.get_audio_level_returns((20.0, true));
        p2.get_audio_level_returns((10.0, true));

        let speakers = rm.get_active_speakers();
        assert_eq!(2, speakers.len(), "{name}");
        assert_eq!(p.id().as_str(), speakers[0].sid, "{name}");
        assert_eq!(p2.id().as_str(), speakers[1].sid, "{name}");
    }

    {
        let name = "participants are getting audio updates (protocol 3+)";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 2,
            protocol: 3,
            ..Default::default()
        });
        let participants = rm.get_participants();
        let p = downcast(&participants[0]);
        tokio::time::sleep(StdDuration::from_millis(1)).await; // let the first update cycle run
        p.get_audio_level_returns((30.0, true));

        let speakers = rm.get_active_speakers();
        assert!(!speakers.is_empty(), "{name}");
        assert_eq!(p.id().as_str(), speakers[0].sid, "{name}");

        with_timeout_async(
            || {
                for op in &participants {
                    let op = downcast(op);
                    let updates = get_active_speaker_updates(&op);
                    if updates.is_empty() {
                        return format!("{} did not get any audio updates", op.identity());
                    }
                }
                String::new()
            },
            None,
        )
        .await;

        // no longer speaking, send update with empty items
        p.get_audio_level_returns((127.0, false));

        with_timeout_async(
            || {
                let updates = get_active_speaker_updates(&p);
                let last_update = &updates[updates.len() - 1];
                if last_update.is_empty() {
                    return "did not get updates of speaker going quiet".to_string();
                }
                if last_update[0].active {
                    return "speaker should not have been active".to_string();
                }
                String::new()
            },
            None,
        )
        .await;
    }

    {
        // audio level is smoothed
        let rm = new_room_with_participants(TestRoomOpts {
            num: 2,
            protocol: 3,
            audio_smooth_intervals: 3,
            ..Default::default()
        });

        let participants = rm.get_participants();
        let p = downcast(&participants[0]);
        let op = downcast(&participants[1]);
        p.get_audio_level_returns((30.0, true));
        let converted_level = audio::convert_audio_level(30.0) as f32;

        with_timeout_async(
            || {
                let updates = get_active_speaker_updates(&op);
                if updates.is_empty() {
                    return "no speaker updates received".to_string();
                }
                let last_speakers = &updates[updates.len() - 1];
                if last_speakers.is_empty() {
                    return "no speakers in the update".to_string();
                }
                if last_speakers[0].level > converted_level {
                    return String::new();
                }
                "level mismatch".to_string()
            },
            None,
        )
        .await;

        with_timeout_async(
            || {
                let updates = get_active_speaker_updates(&op);
                if updates.is_empty() {
                    return "no updates received".to_string();
                }
                let last_speakers = &updates[updates.len() - 1];
                if last_speakers.is_empty() {
                    return "no speakers found".to_string();
                }
                if last_speakers[0].level > converted_level {
                    return String::new();
                }
                "did not match expected levels".to_string()
            },
            None,
        )
        .await;

        p.get_audio_level_returns((127.0, false));
        with_timeout_async(
            || {
                let updates = get_active_speaker_updates(&op);
                if updates.is_empty() {
                    return "no speaker updates received".to_string();
                }
                let last_speakers = &updates[updates.len() - 1];
                if last_speakers.len() == 1 && !last_speakers[0].active {
                    return String::new();
                }
                "speakers didn't go back to zero".to_string()
            },
            None,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_data_channel() {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Mode {
        CurApi,
        LegacySid,
        LegacyIdentity,
    }
    let modes = [Mode::CurApi, Mode::LegacySid, Mode::LegacyIdentity];
    let mode_name = |mode: Mode| match mode {
        Mode::CurApi => "cur",
        Mode::LegacySid => "legacy sid",
        Mode::LegacyIdentity => "legacy identity",
    };

    fn user(dp: &mut livekit::DataPacket) -> &mut livekit::UserPacket {
        match &mut dp.value {
            Some(livekit::data_packet::Value::User(u)) => u,
            _ => panic!("not a user packet"),
        }
    }
    let set_source =
        |mode: Mode, dp: &mut livekit::DataPacket, p: &dyn types::LocalParticipant| match mode {
            Mode::CurApi => dp.participant_identity = p.identity().to_string(),
            Mode::LegacySid => user(dp).participant_sid = p.id().to_string(),
            Mode::LegacyIdentity => user(dp).participant_identity = p.identity().to_string(),
        };
    let set_dest =
        |mode: Mode, dp: &mut livekit::DataPacket, p: &dyn types::LocalParticipant| match mode {
            Mode::CurApi => dp.destination_identities = vec![p.identity().to_string()],
            Mode::LegacySid => user(dp).destination_sids = vec![p.id().to_string()],
            Mode::LegacyIdentity => {
                user(dp).destination_identities = vec![p.identity().to_string()];
            }
        };

    for mode in modes {
        let name = format!("participants should receive data/{}", mode_name(mode));
        let rm = new_room_with_participants(TestRoomOpts {
            num: 3,
            ..Default::default()
        });

        let lpl = rm.local_participant_listener();

        let participants = rm.get_participants();
        let p = downcast(&participants[0]);

        let mut packet = livekit::DataPacket {
            kind: livekit::data_packet::Kind::Reliable as i32,
            value: Some(livekit::data_packet::Value::User(livekit::UserPacket {
                payload: b"message..".to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        };
        set_source(mode, &mut packet, &*p);

        let mut packet_exp = packet.clone();
        if mode != Mode::LegacySid {
            packet_exp.participant_identity = p.identity().to_string();
            user(&mut packet_exp).participant_identity = p.identity().to_string();
        }

        let encoded = packet_exp.encode_to_vec();
        lpl.on_data_message(p.clone(), packet.kind(), &packet);

        // ensure everyone has received the packet
        for op in &participants {
            let fp = downcast(op);
            if same(&fp, &p) {
                assert_eq!(0, fp.send_data_message_call_count(), "{name}");
                continue;
            }
            assert_eq!(1, fp.send_data_message_call_count(), "{name}");
            let (_, got, _, _) = fp.send_data_message_args_for_call(0);
            assert_eq!(encoded, got, "{name}");
        }
    }

    for mode in modes {
        let name = format!(
            "only one participant should receive the data/{}",
            mode_name(mode)
        );
        let rm = new_room_with_participants(TestRoomOpts {
            num: 4,
            ..Default::default()
        });

        let lpl = rm.local_participant_listener();

        let participants = rm.get_participants();
        let p = downcast(&participants[0]);
        let p1 = downcast(&participants[1]);

        let mut packet = livekit::DataPacket {
            kind: livekit::data_packet::Kind::Reliable as i32,
            value: Some(livekit::data_packet::Value::User(livekit::UserPacket {
                payload: b"message to p1..".to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        };
        set_source(mode, &mut packet, &*p);
        set_dest(mode, &mut packet, &*p1);

        let mut packet_exp = packet.clone();
        if mode != Mode::LegacySid {
            packet_exp.participant_identity = p.identity().to_string();
            user(&mut packet_exp).participant_identity = p.identity().to_string();
            packet_exp.destination_identities = vec![p1.identity().to_string()];
            user(&mut packet_exp).destination_identities = vec![p1.identity().to_string()];
        }

        let encoded = packet_exp.encode_to_vec();
        lpl.on_data_message(p.clone(), packet.kind(), &packet);

        // only p1 should receive the data
        for op in &participants {
            let fp = downcast(op);
            if !same(&fp, &p1) {
                assert_eq!(0, fp.send_data_message_call_count(), "{name}");
            }
        }
        assert_eq!(1, p1.send_data_message_call_count(), "{name}");
        let (_, got, _, _) = p1.send_data_message_args_for_call(0);
        assert_eq!(encoded, got, "{name}");
    }

    {
        let name = "publishing disallowed";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 2,
            ..Default::default()
        });

        let participants = rm.get_participants();
        let p = downcast(&participants[0]);
        p.can_publish_data_returns(false);

        let packet = livekit::DataPacket {
            kind: livekit::data_packet::Kind::Reliable as i32,
            value: Some(livekit::data_packet::Value::User(livekit::UserPacket {
                payload: Vec::new(),
                ..Default::default()
            })),
            ..Default::default()
        };
        if p.can_publish_data() {
            let lpl = rm.local_participant_listener();
            lpl.on_data_message(p.clone(), packet.kind(), &packet);
        }

        // no one should've been sent packet
        for op in &participants {
            let fp = downcast(op);
            assert_eq!(0, fp.send_data_message_call_count(), "{name}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_hidden_participants() {
    {
        let name = "other participants don't receive hidden updates";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 2,
            num_hidden: 1,
            ..Default::default()
        });

        let p_new = new_mock_participant(
            "new".into(),
            types::ProtocolVersion(types::CURRENT_PROTOCOL),
            false,
            false,
            rm.local_participant_listener(),
        );
        let _ = rm.join(p_new.clone(), None, None, &ice_servers_for_room());

        // expect new participant to get a JoinReply
        let res = p_new.send_join_response_args_for_call(0);
        assert_eq!(
            livekit::RoomId::from(&res.room.as_ref().unwrap().sid),
            rm.id(),
            "{name}"
        );
        assert_eq!(2, res.other_participants.len(), "{name}");
        assert_eq!(4, rm.get_participants().len(), "{name}");
        assert!(!res.ice_servers.is_empty(), "{name}");
        assert_eq!("testregion", res.server_info.unwrap().region, "{name}");
    }

    {
        // hidden participant subscribes to tracks
        let rm = new_room_with_participants(TestRoomOpts {
            num: 2,
            ..Default::default()
        });
        let lpl = rm.local_participant_listener();
        let hidden = new_mock_participant(
            "hidden".into(),
            types::ProtocolVersion(types::CURRENT_PROTOCOL),
            true,
            false,
            lpl.clone(),
        );

        let err = rm.join(
            hidden.clone(),
            None,
            Some(ParticipantOptions {
                auto_subscribe: true,
                ..Default::default()
            }),
            &ice_servers_for_room(),
        );
        assert_eq!(Ok(()), err, "hidden participant subscribes to tracks");

        hidden.state_returns(livekit::participant_info::State::Active);
        lpl.on_state_change(hidden.clone());

        eventually_async(
            || hidden.subscribe_to_track_call_count() == 2,
            StdDuration::from_secs(5),
            StdDuration::from_millis(10),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_room_update() {
    {
        let name = "updates are sent when participant joined";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 1,
            ..Default::default()
        });

        let p1 = downcast(&rm.get_participants()[0]);
        assert_eq!(0, p1.send_room_update_call_count(), "{name}");

        let p2 = new_mock_participant(
            "p2".into(),
            types::ProtocolVersion(types::CURRENT_PROTOCOL),
            false,
            false,
            rm.local_participant_listener(),
        );
        assert_eq!(
            Ok(()),
            rm.join(p2, None, None, &ice_servers_for_room()),
            "{name}"
        );

        // p1 should have received an update
        tokio::time::sleep(2 * DEFAULT_DELAY).await;
        assert!(1 <= p1.send_room_update_call_count(), "{name}");
        assert_eq!(
            2,
            p1.send_room_update_args_for_call(p1.send_room_update_call_count() - 1)
                .num_participants,
            "{name}"
        );
    }

    {
        let name = "participants should receive metadata update";
        let rm = new_room_with_participants(TestRoomOpts {
            num: 2,
            ..Default::default()
        });

        rm.set_metadata("test metadata...");

        // callbacks are updated from goroutine
        tokio::time::sleep(2 * DEFAULT_DELAY).await;

        for op in rm.get_participants() {
            let fp = downcast(&op);
            // room updates are now sent for both participant joining and room metadata
            assert!(fp.send_room_update_call_count() >= 1, "{name}");
        }
    }
}

#[derive(Default)]
struct TestRoomOpts {
    num: usize,
    num_hidden: usize,
    protocol: i32,
    audio_smooth_intervals: u32,
}

// The room of a test, closed when the test is done with it: `t.Cleanup(rm.Close)`.
struct TestRoom(Arc<Room>);

impl Deref for TestRoom {
    type Target = Arc<Room>;

    fn deref(&self) -> &Arc<Room> {
        &self.0
    }
}

impl Drop for TestRoom {
    fn drop(&mut self) {
        self.0.close(types::RoomCloseReason::Unknown);
    }
}

// `WebRTCConfig{}`: a configuration of the node with nothing set that the room reads.
fn web_rtc_config() -> WebRtcConfig {
    static CONFIG: LazyLock<WebRtcConfig> = LazyLock::new(|| {
        let mut conf = config::Config::new("", true).unwrap();
        conf.rtc.rtc_config.tcp_port = 0;
        let mut c = WebRtcConfig::new(&conf).unwrap();
        c.web_rtc_config.setting_engine = Default::default();
        c.web_rtc_config.ice_servers = Vec::new();
        c.web_rtc_config.nat_1to1_ips = Vec::new();
        c.web_rtc_config.use_mdns = false;
        c.buffer_factory = None;
        c.receiver = ReceiverConfig::default();
        c.answering_dtls_role = DtlsRole::Unspecified;
        c
    });
    CONFIG.clone()
}

fn new_room_with_participants(opts: TestRoomOpts) -> TestRoom {
    // Not here: the fake key provider and the webhook notifier, which upstream builds the
    // telemetry service from: see the header.
    let rm = TestRoom(Room::new(
        &livekit::Room {
            name: "room".to_string(),
            ..Default::default()
        },
        None,
        web_rtc_config(),
        config::RoomConfig {
            empty_timeout: 5 * 60,
            departure_timeout: 1,
            ..Default::default()
        },
        &sfu::AudioConfig {
            audio_level_config: audio::AudioLevelConfig {
                update_interval: AUDIO_UPDATE_INTERVAL,
                smooth_intervals: opts.audio_smooth_intervals,
                ..Default::default()
            },
            ..Default::default()
        },
        &livekit::ServerInfo {
            edition: livekit::server_info::Edition::Standard as i32,
            // `version.Version` of LiveKit: the version of this crate here
            version: env!("CARGO_PKG_VERSION").to_string(),
            protocol: types::CURRENT_PROTOCOL,
            node_id: "testnode".to_string(),
            region: "testregion".to_string(),
            ..Default::default()
        },
        Arc::new(FakeTelemetryService::default()),
    ));
    for i in 0..opts.num + opts.num_hidden {
        let identity = livekit::ParticipantIdentity::from(format!("p{i}"));
        let participant = new_mock_participant(
            identity,
            types::ProtocolVersion(opts.protocol),
            i >= opts.num,
            true,
            rm.local_participant_listener(),
        );
        let err = rm.join(
            participant.clone(),
            None,
            Some(ParticipantOptions {
                auto_subscribe: true,
                ..Default::default()
            }),
            &ice_servers_for_room(),
        );
        assert_eq!(Ok(()), err);
        participant.state_returns(livekit::participant_info::State::Active);
        participant.is_ready_returns(true);
        // each participant has a track
        participant.get_published_tracks_returns(vec![Arc::new(FakeMediaTrack::default())]);
    }
    rm
}
