//! Tests of `interfaces.rs`. All of them are new for musivum: the original has no tests of
//! `pkg/rtc/types/interfaces.go`. They check what the functions of the file give against the
//! original, and that the fakes of the traits can be used the way upstream's tests use
//! counterfeiter's.

use std::io::{self, Cursor, Read};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;

use super::*;
use crate::go::atomic;
use crate::protocol::auth::ClaimGrants;
use crate::routing::{FakeMessageSink, MessageSink};
use crate::sfu::pacer::{FakePacer, Pacer};
use crate::testutils::fakes::{self, NotRecorded};

/// What the functions of `interfaces.go` give at v1.13.11, printed by a Go test that loops
/// over every value and one past the last:
///
/// - `PCR`: a `ParticipantCloseReason`, its `String()`, and its `ToDisconnectReason()` as a
///   number and as text;
/// - `DR`: a `livekit.DisconnectReason`, its text, and `IsIntentionalDisconnect` of it;
/// - `SCR`: a `SignallingCloseReason` and its `String()`;
/// - `RCR`: a `RoomCloseReason`, its `String()`, its `ToProto()` as a number and as text,
///   and its `ToParticipantCloseReason()` as a number and as text;
/// - `MS`: a `MigrateState` and its `String()`; `OME`: an `OperationMonitorEvent` and its
///   `String()`; `KEY`: the two keys of `AddOnClose`.
///
/// The rows one past the last value (`PCR 30`, `SCR 11`, `RCR 6`, `MS 3`, `OME 5`) are
/// left out: an enum cannot hold such a number.
const GO_TABLE: &str = "\
PCR 0 NONE 0 UNKNOWN_REASON
PCR 1 CLIENT_REQUEST_LEAVE 1 CLIENT_INITIATED
PCR 2 ROOM_MANAGER_STOP 3 SERVER_SHUTDOWN
PCR 3 VERIFY_FAILED 7 JOIN_FAILURE
PCR 4 JOIN_FAILED 7 JOIN_FAILURE
PCR 5 JOIN_TIMEOUT 7 JOIN_FAILURE
PCR 6 MESSAGE_BUS_FAILED 7 JOIN_FAILURE
PCR 7 PEER_CONNECTION_DISCONNECTED 14 CONNECTION_TIMEOUT
PCR 8 DUPLICATE_IDENTITY 2 DUPLICATE_IDENTITY
PCR 9 MIGRATION_COMPLETE 8 MIGRATION
PCR 10 STALE 2 DUPLICATE_IDENTITY
PCR 11 SERVICE_REQUEST_REMOVE_PARTICIPANT 4 PARTICIPANT_REMOVED
PCR 12 SERVICE_REQUEST_DELETE_ROOM 5 ROOM_DELETED
PCR 13 SIMULATE_MIGRATION 8 MIGRATION
PCR 14 SIMULATE_NODE_FAILURE 3 SERVER_SHUTDOWN
PCR 15 SIMULATE_SERVER_LEAVE 3 SERVER_SHUTDOWN
PCR 16 SIMULATE_LEAVE_REQUEST 1 CLIENT_INITIATED
PCR 17 NEGOTIATE_FAILED 6 STATE_MISMATCH
PCR 18 MIGRATION_REQUESTED 8 MIGRATION
PCR 19 PUBLICATION_ERROR 6 STATE_MISMATCH
PCR 20 SUBSCRIPTION_ERROR 6 STATE_MISMATCH
PCR 21 DATA_CHANNEL_ERROR 6 STATE_MISMATCH
PCR 22 MIGRATE_CODEC_MISMATCH 6 STATE_MISMATCH
PCR 23 SIGNAL_SOURCE_CLOSE 9 SIGNAL_CLOSE
PCR 24 ROOM_CLOSED 10 ROOM_CLOSED
PCR 25 USER_UNAVAILABLE 11 USER_UNAVAILABLE
PCR 26 USER_REJECTED 12 USER_REJECTED
PCR 27 MOVE_FAILED 6 STATE_MISMATCH
PCR 28 AGENT_ERROR 16 AGENT_ERROR
PCR 29 SIP_TRUNK_FAILURE 13 SIP_TRUNK_FAILURE
DR 0 UNKNOWN_REASON false
DR 1 CLIENT_INITIATED true
DR 2 DUPLICATE_IDENTITY true
DR 3 SERVER_SHUTDOWN true
DR 4 PARTICIPANT_REMOVED true
DR 5 ROOM_DELETED true
DR 6 STATE_MISMATCH false
DR 7 JOIN_FAILURE false
DR 8 MIGRATION true
DR 9 SIGNAL_CLOSE false
DR 10 ROOM_CLOSED true
DR 11 USER_UNAVAILABLE false
DR 12 USER_REJECTED false
DR 13 SIP_TRUNK_FAILURE true
DR 14 CONNECTION_TIMEOUT false
DR 15 MEDIA_FAILURE false
DR 16 AGENT_ERROR false
SCR 0 UNKNOWN
SCR 1 MIGRATION
SCR 2 RESUME
SCR 3 TRANSPORT_FAILURE
SCR 4 FULL_RECONNECT_PUBLICATION_ERROR
SCR 5 FULL_RECONNECT_SUBSCRIPTION_ERROR
SCR 6 FULL_RECONNECT_DATA_CHANNEL_ERROR
SCR 7 FULL_RECONNECT_NEGOTIATE_FAILED
SCR 8 PARTICIPANT_CLOSE
SCR 9 DISCONNECT_ON_RESUME
SCR 10 DISCONNECT_ON_RESUME_NO_MESSAGES
RCR 0 UNKNOWN 0 ROOM_END_UNKNOWN 0 NONE
RCR 1 API_DELETE 1 ROOM_END_API_DELETE 12 SERVICE_REQUEST_DELETE_ROOM
RCR 2 IDLE_TIMEOUT 2 ROOM_END_IDLE_TIMEOUT 24 ROOM_CLOSED
RCR 3 SERVER_SHUTDOWN 3 ROOM_END_SERVER_SHUTDOWN 2 ROOM_MANAGER_STOP
RCR 4 SUPERSEDED 4 ROOM_END_SUPERSEDED 24 ROOM_CLOSED
RCR 5 OPEN_FAILED 5 ROOM_END_OPEN_FAILED 0 NONE
MS 0 MIGRATE_STATE_INIT
MS 1 MIGRATE_STATE_SYNC
MS 2 MIGRATE_STATE_COMPLETE
OME 0 PUBLISHER_PEER_CONNECTION_CONNECTED
OME 1 ADD_PENDING_PUBLICATION
OME 2 SET_PUBLICATION_MUTE
OME 3 SET_PUBLISHED_TRACK
OME 4 CLEAR_PUBLISHED_TRACK
KEY normal whip
";

// In the order of the original's `iota`.
const PARTICIPANT_CLOSE_REASONS: [ParticipantCloseReason; 30] = [
    ParticipantCloseReason::None,
    ParticipantCloseReason::ClientRequestLeave,
    ParticipantCloseReason::RoomManagerStop,
    ParticipantCloseReason::VerifyFailed,
    ParticipantCloseReason::JoinFailed,
    ParticipantCloseReason::JoinTimeout,
    ParticipantCloseReason::MessageBusFailed,
    ParticipantCloseReason::PeerConnectionDisconnected,
    ParticipantCloseReason::DuplicateIdentity,
    ParticipantCloseReason::MigrationComplete,
    ParticipantCloseReason::Stale,
    ParticipantCloseReason::ServiceRequestRemoveParticipant,
    ParticipantCloseReason::ServiceRequestDeleteRoom,
    ParticipantCloseReason::SimulateMigration,
    ParticipantCloseReason::SimulateNodeFailure,
    ParticipantCloseReason::SimulateServerLeave,
    ParticipantCloseReason::SimulateLeaveRequest,
    ParticipantCloseReason::NegotiateFailed,
    ParticipantCloseReason::MigrationRequested,
    ParticipantCloseReason::PublicationError,
    ParticipantCloseReason::SubscriptionError,
    ParticipantCloseReason::DataChannelError,
    ParticipantCloseReason::MigrateCodecMismatch,
    ParticipantCloseReason::SignalSourceClose,
    ParticipantCloseReason::RoomClosed,
    ParticipantCloseReason::UserUnavailable,
    ParticipantCloseReason::UserRejected,
    ParticipantCloseReason::MoveFailed,
    ParticipantCloseReason::AgentError,
    ParticipantCloseReason::SipTrunkFailure,
];

const SIGNALLING_CLOSE_REASONS: [SignallingCloseReason; 11] = [
    SignallingCloseReason::Unknown,
    SignallingCloseReason::Migration,
    SignallingCloseReason::Resume,
    SignallingCloseReason::TransportFailure,
    SignallingCloseReason::FullReconnectPublicationError,
    SignallingCloseReason::FullReconnectSubscriptionError,
    SignallingCloseReason::FullReconnectDataChannelError,
    SignallingCloseReason::FullReconnectNegotiateFailed,
    SignallingCloseReason::ParticipantClose,
    SignallingCloseReason::DisconnectOnResume,
    SignallingCloseReason::DisconnectOnResumeNoMessages,
];

const ROOM_CLOSE_REASONS: [RoomCloseReason; 6] = [
    RoomCloseReason::Unknown,
    RoomCloseReason::ApiDelete,
    RoomCloseReason::IdleTimeout,
    RoomCloseReason::ServerShutdown,
    RoomCloseReason::Superseded,
    RoomCloseReason::OpenFailed,
];

const MIGRATE_STATES: [MigrateState; 3] = [
    MigrateState::Init,
    MigrateState::Sync,
    MigrateState::Complete,
];

const OPERATION_MONITOR_EVENTS: [OperationMonitorEvent; 5] = [
    OperationMonitorEvent::PublisherPeerConnectionConnected,
    OperationMonitorEvent::AddPendingPublication,
    OperationMonitorEvent::SetPublicationMute,
    OperationMonitorEvent::SetPublishedTrack,
    OperationMonitorEvent::ClearPublishedTrack,
];

// New for musivum: the reasons, states and events have the numbers and the texts of the
// original, and its functions on them give what the original's give.
#[test]
fn the_functions_of_the_file_give_what_the_original_gives() {
    let mut rows = [0usize; 7];
    for line in GO_TABLE.lines() {
        let f: Vec<&str> = line.split(' ').collect();
        let number = |i: usize| f[i].parse::<usize>().unwrap();
        match f[0] {
            "PCR" => {
                let reason = PARTICIPANT_CLOSE_REASONS[number(1)];
                assert_eq!(number(1), reason as usize, "{line}");
                assert_eq!(f[2], reason.to_string(), "{line}");
                let disconnect_reason = reason.to_disconnect_reason();
                assert_eq!(number(3), disconnect_reason as usize, "{line}");
                assert_eq!(f[4], disconnect_reason.as_str_name(), "{line}");
                rows[0] += 1;
            }
            "DR" => {
                let reason = livekit::DisconnectReason::try_from(number(1) as i32).unwrap();
                assert_eq!(f[2], reason.as_str_name(), "{line}");
                assert_eq!(
                    f[3].parse::<bool>().unwrap(),
                    is_intentional_disconnect(reason),
                    "{line}"
                );
                rows[1] += 1;
            }
            "SCR" => {
                let reason = SIGNALLING_CLOSE_REASONS[number(1)];
                assert_eq!(number(1), reason as usize, "{line}");
                assert_eq!(f[2], reason.to_string(), "{line}");
                rows[2] += 1;
            }
            "RCR" => {
                let reason = ROOM_CLOSE_REASONS[number(1)];
                assert_eq!(number(1), reason as usize, "{line}");
                assert_eq!(f[2], reason.to_string(), "{line}");
                assert_eq!(number(3), reason.to_proto() as usize, "{line}");
                assert_eq!(f[4], reason.to_proto().as_str_name(), "{line}");
                let participant_close_reason = reason.to_participant_close_reason();
                assert_eq!(number(5), participant_close_reason as usize, "{line}");
                assert_eq!(f[6], participant_close_reason.to_string(), "{line}");
                rows[3] += 1;
            }
            "MS" => {
                let state = MIGRATE_STATES[number(1)];
                assert_eq!(number(1), state as usize, "{line}");
                assert_eq!(f[2], state.to_string(), "{line}");
                rows[4] += 1;
            }
            "OME" => {
                let event = OPERATION_MONITOR_EVENTS[number(1)];
                assert_eq!(number(1), event as usize, "{line}");
                assert_eq!(f[2], event.to_string(), "{line}");
                rows[5] += 1;
            }
            "KEY" => {
                assert_eq!(f[1], PARTICIPANT_CLOSE_KEY_NORMAL);
                assert_eq!(f[2], PARTICIPANT_CLOSE_KEY_WHIP);
                rows[6] += 1;
            }
            other => panic!("a row of an unknown kind: {other}"),
        }
    }
    // every value of every enumeration was looked at
    assert_eq!([30, 17, 11, 6, 3, 5, 1], rows);

    // the zero values are the original's
    assert_eq!(ParticipantCloseReason::None, Default::default());
    assert_eq!(SignallingCloseReason::Unknown, Default::default());
    assert_eq!(RoomCloseReason::Unknown, Default::default());
    assert_eq!(MigrateState::Init, Default::default());
    assert_eq!(
        OperationMonitorEvent::PublisherPeerConnectionConnected,
        Default::default()
    );
}

fn track_id(s: &str) -> livekit::TrackId {
    livekit::TrackId::from(s)
}

fn participant_id(s: &str) -> livekit::ParticipantId {
    livekit::ParticipantId::from(s)
}

// New for musivum: `FakeLocalParticipant` as upstream's tests use counterfeiter's. It is
// configured and called through `Arc<dyn LocalParticipant>`, has the methods of
// `Participant`, is handed on as an `Arc<dyn Participant>` and got back from it.
#[test]
fn a_fake_local_participant_is_a_participant_and_is_got_back_from_one() {
    let fake = Arc::new(FakeLocalParticipant::default());
    let local: Arc<dyn LocalParticipant> = fake.clone();

    // nothing was said: the zero values, as counterfeiter's fake gives Go's
    assert_eq!(participant_id(""), local.id());
    assert!(local.get_client_info().is_none());
    assert!(local.get_published_track(&track_id("TR_1")).is_none());
    assert_eq!(Ok(()), local.send_refresh_token("token"));
    assert!(matches!(local.get_answer(), Ok((sd, 0)) if sd.sdp.is_empty()));
    assert!(*local.claim_grants() == ClaimGrants::default());
    // a channel that is never closed
    assert!(!local.disconnected().is_cancelled());
    assert_eq!(MigrateState::Init, local.migrate_state());

    // a value, of the embedded interface, and two results
    fake.identity_returns("alice".into());
    fake.get_audio_level_returns((20.0, true));
    assert_eq!("alice", local.identity().as_str());
    assert_eq!((20.0, true), local.get_audio_level());

    // a message that is handed out is the object that is kept
    let client_info = Arc::new(livekit::ClientInfo {
        browser: "firefox".to_string(),
        ..Default::default()
    });
    fake.get_client_info_returns(Some(client_info.clone()));
    assert!(Arc::ptr_eq(&client_info, &local.get_client_info().unwrap()));

    // an error, and what was recorded of a call: an identifier, bytes, numbers
    fake.send_data_message_returns(Err(Error::DataChannelUnavailable));
    assert_eq!(
        Err(Error::DataChannelUnavailable),
        local.send_data_message(
            livekit::data_packet::Kind::Reliable,
            &[1, 2, 3],
            &participant_id("PA_2"),
            7
        )
    );
    assert_eq!(1, fake.send_data_message_call_count());
    let (kind, data, sender_id, seq) = fake.send_data_message_args_for_call(0);
    assert_eq!(livekit::data_packet::Kind::Reliable, kind);
    assert_eq!(vec![1, 2, 3], data);
    assert_eq!(participant_id("PA_2"), sender_id);
    assert_eq!(7, seq);
    // an error of another package that is passed on
    fake.handle_signal_message_returns(Err(Error::from(
        crate::rtc::signalling::Error::InvalidMessageType,
    )));
    let err = local
        .handle_signal_message(livekit::SignalResponse::default().into())
        .unwrap_err();
    assert_eq!("invalid message type", err.to_string());

    // a stub that is handed a callback, which may be absent
    let other: Arc<dyn LocalParticipant> = Arc::new(FakeLocalParticipant::default());
    let closed = Arc::new(atomic::Int32::new(0));
    fake.add_on_close_calls(move |_, callback| {
        if let Some(callback) = callback {
            callback(other.clone());
        }
    });
    let counter = closed.clone();
    local.add_on_close(
        PARTICIPANT_CLOSE_KEY_NORMAL,
        Some(Box::new(move |p| {
            assert!(p.id().is_empty());
            counter.inc();
        })),
    );
    local.add_on_close(PARTICIPANT_CLOSE_KEY_NORMAL, None);
    assert_eq!(1, closed.load());
    assert_eq!(
        ("normal".to_string(), NotRecorded),
        fake.add_on_close_args_for_call(1)
    );

    // objects that are handed in are recorded as they are; a sink may be absent
    let sink = Arc::new(FakeMessageSink::default());
    local.swap_response_sink(Some(sink.clone()), SignallingCloseReason::Resume);
    local.swap_response_sink(None, SignallingCloseReason::ParticipantClose);
    let (swapped, reason) = fake.swap_response_sink_args_for_call(0);
    let swapped: Arc<dyn MessageSink> = swapped.unwrap();
    assert!(Arc::ptr_eq(
        &sink,
        &fakes::downcast::<FakeMessageSink>(swapped)
    ));
    assert_eq!(SignallingCloseReason::Resume, reason);
    assert!(fake.swap_response_sink_args_for_call(1).0.is_none());

    // an object without a zero value has to be given
    let pacer = Arc::new(FakePacer::default());
    fake.get_pacer_returns(pacer.clone());
    local.get_pacer().set_bitrate(1_000);
    assert_eq!(1_000, pacer.set_bitrate_args_for_call(0));

    // the channels of an RPC
    fake.perform_rpc_calls(|req, result_ch, _| {
        result_ch.try_send(req.method.clone()).unwrap();
    });
    let (result_ch, mut results) = mpsc::channel(1);
    let (error_ch, mut errors) = mpsc::channel(1);
    let req = livekit::PerformRpcRequest {
        method: "greet".to_string(),
        ..Default::default()
    };
    local.perform_rpc(&req, result_ch, error_ch);
    assert_eq!("greet", results.try_recv().unwrap());
    let (recorded, _, error_ch) = fake.perform_rpc_args_for_call(0);
    assert_eq!(req, recorded);
    error_ch.try_send(Error::ParticipantSessionClosed).unwrap();
    assert_eq!(Error::ParticipantSessionClosed, errors.try_recv().unwrap());

    // handed on as a participant, as a room hands its participants on
    let participant: Arc<dyn Participant> = local.clone();
    assert_eq!("alice", participant.identity().as_str());
    assert_eq!(2, fake.identity_call_count());

    // Go's `p.(*typesfakes.FakeLocalParticipant)`
    let got = fakes::downcast::<FakeLocalParticipant>(participant.clone());
    assert!(Arc::ptr_eq(&fake, &got));
    got.is_ready_returns(true);
    assert!(local.is_ready());

    // Go's `p.(types.LocalParticipant)`
    let as_local = participant
        .clone()
        .as_local_participant()
        .expect("the fake of a local participant is one");
    assert!(as_local.is_ready());
    let not_local: Arc<dyn Participant> = Arc::new(FakeParticipant::default());
    assert!(not_local.clone().as_local_participant().is_none());
    assert!(!not_local.is_closed());
}

// New for musivum: the fakes of a published track. A subscriber is added with the three
// outcomes the subscription manager tells apart, and a local track is got from a track.
#[test]
fn a_fake_media_track_is_configured_and_a_local_one_is_got_from_a_track() {
    let fake = Arc::new(FakeLocalMediaTrack::default());
    let track: Arc<dyn MediaTrack> = fake.clone();
    let subscriber: Arc<dyn LocalParticipant> = Arc::new(FakeLocalParticipant::default());

    // nothing was said: no track and no error, as counterfeiter's fake answers
    assert!(matches!(track.add_subscriber(subscriber.clone()), Ok(None)));
    assert_eq!(livekit::TrackInfo::default(), track.to_proto());
    assert_eq!(0, track.get_num_subscribers());

    let subscribed = Arc::new(FakeSubscribedTrack::default());
    fake.add_subscriber_returns(Ok(Some(subscribed.clone())));
    let got = track.add_subscriber(subscriber.clone()).unwrap().unwrap();
    assert!(Arc::ptr_eq(
        &subscribed,
        &fakes::downcast::<FakeSubscribedTrack>(got)
    ));
    fake.add_subscriber_returns(Err(Error::TrackNotFound));
    assert_eq!(
        Some(Error::TrackNotFound),
        track.add_subscriber(subscriber.clone()).err()
    );
    // the participant that was handed in
    assert!(Arc::ptr_eq(
        &subscriber,
        &fake.add_subscriber_args_for_call(2)
    ));

    // a stub over a list of identifiers
    fake.revoke_disallowed_subscribers_calls(|allowed| {
        allowed
            .iter()
            .filter(|identity| identity.as_str() != "alice")
            .cloned()
            .collect()
    });
    assert_eq!(
        vec![livekit::ParticipantIdentity::from("bob")],
        track.revoke_disallowed_subscribers(&["alice".into(), "bob".into()])
    );

    // a callback that is called at once
    fake.add_on_close_calls(|f| {
        if let Some(f) = f {
            f(true);
        }
    });
    let resumed = Arc::new(atomic::Bool::new(false));
    let flag = resumed.clone();
    track.add_on_close(Some(Box::new(move |is_expected_to_resume| {
        flag.store(is_expected_to_resume);
    })));
    track.add_on_close(None);
    assert!(resumed.load());

    // Go's `track.(types.LocalMediaTrack)`, and a method of the local track
    fake.get_connection_score_and_quality_returns((4.5, livekit::ConnectionQuality::Excellent));
    let local = track
        .clone()
        .as_local_media_track()
        .expect("the fake of a local media track is one");
    assert_eq!(
        (4.5, livekit::ConnectionQuality::Excellent),
        local.get_connection_score_and_quality()
    );
    assert!(local.get_track_stats().is_none());
    let not_local: Arc<dyn MediaTrack> = Arc::new(FakeMediaTrack::default());
    assert!(not_local.clone().as_local_media_track().is_none());
    // Go's `track.(*typesfakes.FakeMediaTrack)`
    fakes::downcast::<FakeMediaTrack>(not_local.clone()).is_muted_returns(true);
    assert!(not_local.is_muted());

    // what is posted to an operation monitor
    let monitor = Arc::new(FakeOperationMonitor::default());
    let op_mon: Arc<dyn OperationMonitor> = monitor.clone();
    op_mon.post_event(
        OperationMonitorEvent::AddPendingPublication,
        OperationMonitorData::None,
    );
    op_mon.post_event(
        OperationMonitorEvent::SetPublicationMute,
        OperationMonitorData::Bool(true),
    );
    op_mon.post_event(
        OperationMonitorEvent::SetPublishedTrack,
        OperationMonitorData::LocalMediaTrack(local.clone()),
    );
    assert!(matches!(
        monitor.post_event_args_for_call(1),
        (
            OperationMonitorEvent::SetPublicationMute,
            OperationMonitorData::Bool(true)
        )
    ));
    let (event, data) = monitor.post_event_args_for_call(2);
    assert_eq!(OperationMonitorEvent::SetPublishedTrack, event);
    let OperationMonitorData::LocalMediaTrack(posted) = data else {
        panic!("a track was posted");
    };
    assert!(Arc::ptr_eq(
        &fake,
        &fakes::downcast::<FakeLocalMediaTrack>(posted)
    ));
    assert_eq!(Ok(()), op_mon.check());
    monitor.check_returns(Err(Error::InternalError));
    assert_eq!(Err(Error::InternalError), op_mon.check());
    // Go's `opMon.(*PublicationMonitor)`, with the fake where the monitor would be
    fakes::downcast::<FakeOperationMonitor>(op_mon.clone()).is_idle_returns(true);
    assert!(op_mon.is_idle());
}

// New for musivum: the fake of a subscription, with what may be absent of it.
#[test]
fn a_fake_subscribed_track_has_what_a_subscription_has() {
    let fake = Arc::new(FakeSubscribedTrack::default());
    let subscribed: Arc<dyn SubscribedTrack> = fake.clone();

    // nothing was said: what may be nil upstream is absent
    assert!(subscribed.down_track().is_none());
    assert!(subscribed.media_track().is_none());
    assert!(subscribed.rtp_sender().is_none());
    assert!(!subscribed.needs_negotiation());

    let media_track = Arc::new(FakeMediaTrack::default());
    media_track.name_returns("camera".to_string());
    fake.media_track_returns(Some(media_track.clone()));
    fake.publisher_id_returns(participant_id("PA_1"));
    let subscriber = Arc::new(FakeLocalParticipant::default());
    subscriber.get_country_returns("NL".to_string());
    fake.subscriber_returns(subscriber.clone());
    assert_eq!("camera", subscribed.media_track().unwrap().name());
    assert_eq!(participant_id("PA_1"), subscribed.publisher_id());
    assert_eq!("NL", subscribed.subscriber().get_country());

    // the callback of a bind is told how it went, with the error of the connection
    fake.add_on_bind_calls(|f| {
        f(Ok(()));
        f(Err(Error::from(crate::webrtc::Error::UnsupportedCodec)));
    });
    let outcomes = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen = outcomes.clone();
    subscribed.add_on_bind(Box::new(move |outcome| seen.lock().push(outcome)));
    assert_eq!(
        vec![
            Ok(()),
            Err(Error::Webrtc(crate::webrtc::Error::UnsupportedCodec))
        ],
        *outcomes.lock()
    );

    // a callback that is taken away again, and a message that is recorded
    subscribed.on_close(Some(Box::new(|_| {})));
    subscribed.on_close(None);
    assert_eq!(2, fake.on_close_call_count());
    let settings = livekit::UpdateTrackSettings {
        disabled: true,
        ..Default::default()
    };
    subscribed.update_subscriber_settings(&settings, true);
    assert_eq!(
        (settings, true),
        fake.update_subscriber_settings_args_for_call(0)
    );

    // Go's `st.(*typesfakes.FakeSubscribedTrack)`
    let got = fakes::downcast::<FakeSubscribedTrack>(subscribed.clone());
    got.is_bound_returns(true);
    assert!(subscribed.is_bound());
}

// New for musivum: the fakes of the listeners record who they were called for, and the null
// listeners can stand where a listener is asked for.
#[test]
fn the_listeners_are_faked_and_the_null_listeners_do_nothing() {
    let fake = Arc::new(FakeLocalParticipantListener::default());
    let local_listener: Arc<dyn LocalParticipantListener> = fake.clone();
    // what `GetParticipantListener` hands out
    let listener: Arc<dyn ParticipantListener> = local_listener.clone();

    let p = Arc::new(FakeLocalParticipant::default());
    let track = Arc::new(FakeMediaTrack::default());
    listener.on_track_published(p.clone(), track.clone());
    assert_eq!(1, fake.on_track_published_call_count());
    let (who, what) = fake.on_track_published_args_for_call(0);
    assert!(Arc::ptr_eq(
        &p,
        &fakes::downcast::<FakeLocalParticipant>(who)
    ));
    assert!(Arc::ptr_eq(
        &track,
        &fakes::downcast::<FakeMediaTrack>(what)
    ));

    // a result of the listener, which the participant hands on
    assert_eq!(
        Ok(()),
        local_listener.on_sync_state(p.clone(), &livekit::SyncState::default())
    );
    fake.on_sync_state_returns(Err(Error::RoomClosed));
    assert_eq!(
        Err(Error::RoomClosed),
        local_listener.on_sync_state(p.clone(), &livekit::SyncState::default())
    );

    // identifiers and messages are recorded by value
    let participant_tracks = vec![livekit::ParticipantTracks {
        participant_sid: "PA_2".to_string(),
        track_sids: vec!["TR_2".to_string()],
    }];
    local_listener.on_update_subscriptions(
        p.clone(),
        &[track_id("TR_1")],
        &participant_tracks,
        true,
    );
    let (_, track_ids, recorded, subscribe) = fake.on_update_subscriptions_args_for_call(0);
    assert_eq!(vec![track_id("TR_1")], track_ids);
    assert_eq!(participant_tracks, recorded);
    assert!(subscribe);

    // Go's `listener.(*typesfakes.FakeLocalParticipantListener)`
    let got = fakes::downcast::<FakeLocalParticipantListener>(local_listener.clone());
    assert!(Arc::ptr_eq(&fake, &got));

    let telemetry = Arc::new(FakeParticipantTelemetryListener::default());
    let telemetry_listener: Arc<dyn ParticipantTelemetryListener> = telemetry.clone();
    telemetry_listener.on_track_subscribe_failed(
        &participant_id("PA_1"),
        &track_id("TR_1"),
        &Error::NoTrackPermission,
        true,
    );
    assert_eq!(
        (
            participant_id("PA_1"),
            track_id("TR_1"),
            Error::NoTrackPermission,
            true
        ),
        telemetry.on_track_subscribe_failed_args_for_call(0)
    );
    // Go's `p.GetTelemetryListener().(*typesfakes.FakeParticipantTelemetryListener)`
    p.get_telemetry_listener_returns(telemetry_listener.clone());
    let got = fakes::downcast::<FakeParticipantTelemetryListener>(p.get_telemetry_listener());
    assert_eq!(1, got.on_track_subscribe_failed_call_count());

    // the null listeners, where a listener is asked for
    let null_listener: Arc<dyn LocalParticipantListener> =
        Arc::new(NullLocalParticipantListener::default());
    null_listener.on_state_change(p.clone());
    null_listener.on_track_published(p.clone(), track.clone());
    assert_eq!(
        Ok(()),
        null_listener.on_update_subscription_permission(
            p.clone(),
            &livekit::SubscriptionPermission::default()
        )
    );
    assert_eq!(
        Ok(()),
        null_listener.on_simulate_scenario(p.clone(), &livekit::SimulateScenario::default())
    );
    let null_listener: Arc<dyn ParticipantListener> = Arc::new(NullParticipantListener::default());
    null_listener.on_participant_update(p.clone());
    let null_telemetry: Arc<dyn ParticipantTelemetryListener> =
        Arc::new(NullParticipantTelemetryListener::default());
    null_telemetry.on_track_muted(&participant_id("PA_1"), &livekit::TrackInfo::default());
    p.get_telemetry_listener_returns(null_telemetry);
    p.get_telemetry_listener().on_track_stats(
        &telemetry::StatsKey::default(),
        &livekit::AnalyticsStat::default(),
    );
}

// New for musivum: the fake of a room resolves a track, with the change notifier of
// `pkg/utils` as the notifier of the result, and the helper of a participant hands out what
// the room has cached.
#[test]
fn a_fake_room_resolves_a_track_with_the_change_notifier_of_utils() {
    let fake = Arc::new(FakeRoom::default());
    let room: Arc<dyn Room> = fake.clone();
    let subscriber: Arc<dyn LocalParticipant> = Arc::new(FakeLocalParticipant::default());

    // nothing was said: the zero value of the result, with nothing in it
    let res = room.resolve_media_track_for_subscriber(subscriber.clone(), &track_id("TR_1"));
    assert!(res.track_changed_notifier.is_none());
    assert!(res.track.is_none());
    assert!(!res.has_permission);
    assert!(room.get_local_participants().is_empty());

    let notifier = crate::utils::ChangeNotifier::new();
    let track = Arc::new(FakeMediaTrack::default());
    fake.resolve_media_track_for_subscriber_returns(MediaResolverResult {
        track_changed_notifier: Some(notifier.clone()),
        track: Some(track.clone()),
        has_permission: true,
        publisher_id: participant_id("PA_1"),
        ..Default::default()
    });
    let res = room.resolve_media_track_for_subscriber(subscriber.clone(), &track_id("TR_1"));
    assert!(res.has_permission);
    assert_eq!(participant_id("PA_1"), res.publisher_id);
    assert!(res.track_removed_notifier.is_none());
    assert_eq!(
        track_id("TR_1"),
        fake.resolve_media_track_for_subscriber_args_for_call(1).1
    );

    // the notifier through the interface
    let changed = res.track_changed_notifier.unwrap();
    assert!(!changed.has_observers());
    changed.add_observer("PA_2", Box::new(|| {}));
    assert!(notifier.has_observers());
    changed.remove_observer("PA_2");
    assert!(!changed.has_observers());
    let fake_notifier = Arc::new(FakeChangeNotifier::default());
    let changed: Arc<dyn ChangeNotifier> = fake_notifier.clone();
    changed.notify_changed();
    assert_eq!(1, fake_notifier.notify_changed_call_count());

    // a stub with several arguments, and a list of participants
    fake.remove_participant_calls(|identity, p_id, reason| {
        assert_eq!("alice", identity.as_str());
        assert!(p_id.is_empty());
        assert_eq!(
            ParticipantCloseReason::ServiceRequestRemoveParticipant,
            reason
        );
    });
    room.remove_participant(
        &"alice".into(),
        &participant_id(""),
        ParticipantCloseReason::ServiceRequestRemoveParticipant,
    );
    assert_eq!(1, fake.remove_participant_call_count());
    fake.get_local_participants_returns(vec![subscriber.clone()]);
    assert!(Arc::ptr_eq(&subscriber, &room.get_local_participants()[0]));

    // the helper of a participant
    let fake_helper = Arc::new(FakeLocalParticipantHelper::default());
    let helper: Arc<dyn LocalParticipantHelper> = fake_helper.clone();
    assert!(
        helper
            .get_participant_info(&participant_id("PA_1"))
            .is_none()
    );
    assert_eq!(
        Ok(HashMap::new()),
        helper.get_subscriber_forwarder_state(subscriber.clone())
    );
    let cached = Arc::new(DataMessageCache {
        data: Bytes::from_static(b"hello"),
        sender_id: participant_id("PA_1"),
        seq: 3,
        dest_identities: Vec::new(),
    });
    fake_helper.get_cached_reliable_data_message_returns(vec![cached.clone()]);
    let seqs = HashMap::from([(participant_id("PA_1"), 2)]);
    let msgs = helper.get_cached_reliable_data_message(&seqs);
    assert!(Arc::ptr_eq(&cached, &msgs[0]));
    assert_eq!(
        seqs,
        fake_helper.get_cached_reliable_data_message_args_for_call(0)
    );
    // Go's `helper.(*typesfakes.FakeLocalParticipantHelper)`
    fakes::downcast::<FakeLocalParticipantHelper>(helper.clone())
        .should_regress_codec_returns(true);
    assert!(helper.should_regress_codec());
}

// New for musivum: the fakes of the data tracks, and of the connection of the service.
#[test]
fn the_fakes_of_data_tracks_and_of_the_websocket_are_configured() {
    let fake = Arc::new(FakeDataTrack::default());
    let data_track: Arc<dyn DataTrack> = fake.clone();
    let subscriber: Arc<dyn LocalParticipant> = Arc::new(FakeLocalParticipant::default());

    // nothing was said: no down track and no error
    assert!(matches!(
        data_track.add_subscriber(subscriber.clone()),
        Ok(None)
    ));
    let down_track = Arc::new(FakeDataDownTrack::default());
    fake.add_subscriber_returns(Ok(Some(down_track.clone())));
    let got = data_track
        .add_subscriber(subscriber.clone())
        .unwrap()
        .unwrap();
    // Go's `ddt.(*typesfakes.FakeDataDownTrack)`
    assert!(Arc::ptr_eq(
        &down_track,
        &fakes::downcast::<FakeDataDownTrack>(got.clone())
    ));
    assert!(got.publish_data_track().is_none());
    down_track.publish_data_track_returns(Some(data_track.clone()));
    down_track.handle_returns(4);
    assert_eq!(4, got.handle());
    assert!(got.publish_data_track().is_some());
    got.on_close(None);
    got.update_subscription_options(None);
    assert!(
        down_track
            .update_subscription_options_args_for_call(0)
            .is_none()
    );

    // a packet is recorded with its bytes and its arrival time
    let packet = datatrack::Packet {
        payload: Bytes::from_static(b"payload"),
        ..Default::default()
    };
    data_track.handle_packet(&[9, 8], &packet, 1_000);
    assert_eq!(
        (vec![9, 8], packet.clone(), 1_000),
        fake.handle_packet_args_for_call(0)
    );

    let fake_sender = Arc::new(FakeDataTrackSender::default());
    fake_sender.subscriber_id_returns(participant_id("PA_3"));
    let sender: Arc<dyn DataTrackSender> = fake_sender.clone();
    assert_eq!(Ok(()), data_track.add_data_down_track(sender.clone()));
    assert_eq!(
        participant_id("PA_3"),
        fake.add_data_down_track_args_for_call(0).subscriber_id()
    );
    sender.write_packet(&[1], &packet, 2_000);
    assert_eq!(1, fake_sender.write_packet_call_count());

    let fake_transport = Arc::new(FakeDataTrackTransport::default());
    let transport: Arc<dyn DataTrackTransport> = fake_transport.clone();
    assert_eq!(Ok(()), transport.send_data_track_message(&[1, 2]));
    fake_transport.send_data_track_message_returns(Err(Error::DataChannelBufferFull));
    assert_eq!(
        Err(Error::DataChannelBufferFull),
        transport.send_data_track_message(&[3])
    );
    assert_eq!(
        vec![3],
        fake_transport.send_data_track_message_args_for_call(1)
    );
    subscriber_gets_transport(&transport);

    // the WebSocket of the service: its errors are I/O errors
    let fake_conn = Arc::new(FakeWebsocketClient::default());
    let conn: Arc<dyn WebsocketClient> = fake_conn.clone();
    assert_eq!((0, Vec::new()), conn.read_message().unwrap());
    fake_conn.next_reader_returns(Ok((2, Box::new(Cursor::new(vec![7u8, 8])))));
    let (message_type, mut reader) = conn.next_reader().unwrap();
    let mut payload = Vec::new();
    reader.read_to_end(&mut payload).unwrap();
    assert_eq!((2, vec![7, 8]), (message_type, payload));
    fake_conn.write_message_returns_with(|| Err(io::Error::from(io::ErrorKind::BrokenPipe)));
    for _ in 0..2 {
        assert_eq!(
            io::ErrorKind::BrokenPipe,
            conn.write_message(1, b"text").unwrap_err().kind()
        );
    }
    assert_eq!(
        (1, b"text".to_vec()),
        fake_conn.write_message_args_for_call(1)
    );
    assert!(conn.close().is_ok());
}

// What `GetDataTrackTransport` hands out is given through a participant.
fn subscriber_gets_transport(transport: &Arc<dyn DataTrackTransport>) {
    let p = FakeLocalParticipant::default();
    p.get_data_track_transport_returns(transport.clone());
    assert!(
        p.get_data_track_transport()
            .send_data_track_message(&[])
            .is_err()
    );
    let pacer: Arc<dyn Pacer> = Arc::new(FakePacer::default());
    p.get_pacer_returns(pacer.clone());
    assert!(Arc::ptr_eq(&pacer, &p.get_pacer()));
}
