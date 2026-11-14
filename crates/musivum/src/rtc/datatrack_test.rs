// Ported from LiveKit, pkg/rtc/datatrack_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The subscriber of the tests is given a transport for
// data tracks: the fake has none to return by itself, where counterfeiter's returns nil.
// `GetReporterReturns` is left out with the reporter. The tests after the first are new:
// upstream's one test does not reach the down tracks. What they expect is the original's:
// it is what a Go test laid over `pkg/rtc` at v1.13.11 printed for the same calls. The
// counts of the byte statistics follow from the sizes of the packets.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use super::*;
use crate::go::time::{MILLISECOND, SECOND};
use crate::protocol::logger;
use crate::rtc::datatrack_stats::DataTrackStatsInner;
use crate::rtc::types::{
    FakeDataTrackTransport, FakeLocalParticipant, FakeParticipantTelemetryListener, Participant,
};
use crate::telemetry;
use crate::testutils::{eventually, fakes};

fn new_test_data_track_subscriber(
    id: &str,
    identity: &str,
    is_recorder: bool,
) -> Arc<FakeLocalParticipant> {
    let sub = Arc::new(FakeLocalParticipant::default());
    sub.id_returns(id.into());
    sub.identity_returns(identity.into());
    sub.is_recorder_returns(is_recorder);
    sub.get_logger_returns(logger::get_logger());
    // `sub.GetReporterReturns(roomobs.NewNoopParticipantSessionReporter())`: the reporters
    // are not ported
    sub.get_telemetry_listener_returns(Arc::new(FakeParticipantTelemetryListener::default()));
    sub.get_data_track_transport_returns(Arc::new(FakeDataTrackTransport::default()));
    sub
}

#[test]
fn test_data_track_revoke_disallowed_subscribers() {
    let dt = DataTrack::new(
        DataTrackParams {
            logger: logger::get_logger(),
            participant_id: Box::new(|| "pubID".into()),
            participant_identity: "pub".into(),
            bytes_track_stats: None,
        },
        &livekit::DataTrackInfo {
            pub_handle: 1,
            sid: "DTR_test".into(),
            name: "test".into(),
            ..Default::default()
        },
    );

    let allowed = new_test_data_track_subscriber("allowedID", "allowed", false);
    let disallowed = new_test_data_track_subscriber("disallowedID", "disallowed", false);
    let recorder = new_test_data_track_subscriber("recorderID", "recorder", true);

    for sub in [&allowed, &disallowed, &recorder] {
        let res = dt.add_subscriber(sub.clone());
        assert_eq!(None, res.err());
        assert!(dt.is_subscriber(&sub.id()));
    }

    let revoked = dt.revoke_disallowed_subscribers(&["allowed".into()]);
    assert_eq!(
        vec![livekit::ParticipantIdentity::from("disallowed")],
        revoked
    );

    // disallowed subscriber is removed, allowed and permission exempt (recorder) subscribers are kept
    assert!(dt.is_subscriber(&allowed.id()));
    assert!(!dt.is_subscriber(&disallowed.id()));
    assert!(dt.is_subscriber(&recorder.id()));

    dt.close();
}

// ------------------------------------------------
// New for musivum, from here on.

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

// What a publisher sends: a packet without extensions that starts a frame, and one with two
// extensions (E2EE and user timestamp) that ends it, both under the publisher's handle 7.
const IN_0: &str = "10000007006400050102030468656c6c6f";
const IN_1: &str =
    "0c00000700650005010203050006010d030102030405060708090a0b0c0208010203040506070800deadbeef0001";

struct Subscribed {
    dt: Arc<DataTrack>,
    bytes: Arc<BytesTrackStats>,
    sub1: Arc<FakeLocalParticipant>,
    tl1: Arc<FakeParticipantTelemetryListener>,
    tr1: Arc<FakeDataTrackTransport>,
    ddt1: Arc<dyn types::DataDownTrack>,
    tr2: Arc<FakeDataTrackTransport>,
    ddt2: Arc<dyn types::DataDownTrack>,
}

fn new_subscriber_with_transport(
    id: &str,
    identity: &str,
    handle: u16,
    tr: &Arc<FakeDataTrackTransport>,
) -> Arc<FakeLocalParticipant> {
    let sub = new_test_data_track_subscriber(id, identity, false);
    sub.get_next_subscribed_data_track_handle_returns(handle);
    sub.get_data_track_transport_returns(tr.clone());
    sub
}

// A data track under the handle 7 with byte statistics and two subscribers, whose down
// tracks have the handles 0x1234 and 0x00ab.
fn new_subscribed_data_track() -> Subscribed {
    let bytes = BytesTrackStats::new("", &"DTR_test".into(), &"pubID".into(), || {
        Arc::new(FakeParticipantTelemetryListener::default())
    });
    let dt = DataTrack::new(
        DataTrackParams {
            logger: logger::get_logger(),
            participant_id: Box::new(|| "pubID".into()),
            participant_identity: "pub".into(),
            bytes_track_stats: Some(bytes.clone()),
        },
        &livekit::DataTrackInfo {
            pub_handle: 7,
            sid: "DTR_test".into(),
            name: "test".into(),
            ..Default::default()
        },
    );

    let tr1 = Arc::new(FakeDataTrackTransport::default());
    let tr2 = Arc::new(FakeDataTrackTransport::default());
    let sub1 = new_subscriber_with_transport("sub1ID", "sub1", 0x1234, &tr1);
    let tl1 = Arc::new(FakeParticipantTelemetryListener::default());
    sub1.get_telemetry_listener_returns(tl1.clone());
    let sub2 = new_subscriber_with_transport("sub2ID", "sub2", 0x00ab, &tr2);
    let ddt1 = dt.add_subscriber(sub1.clone()).unwrap().unwrap();
    let ddt2 = dt.add_subscriber(sub2).unwrap().unwrap();
    Subscribed {
        dt,
        bytes,
        sub1,
        tl1,
        tr1,
        ddt1,
        tr2,
        ddt2,
    }
}

// As the participant does with what arrives: the packet is parsed, and goes to the track
// with the bytes it came in.
fn handle_raw(dt: &DataTrack, raw: &str, arrival_time: i64) {
    let raw = Bytes::from(unhex(raw));
    let mut packet = datatrack::Packet::default();
    packet.unmarshal(&raw).unwrap();
    dt.handle_packet(&raw, &packet, arrival_time);
}

fn stats_of(dt: &DataTrack) -> DataTrackStatsInner {
    dt.stats.inner.lock().clone()
}

#[test]
fn a_packet_arrives_at_each_subscribed_down_track_under_its_handle() {
    let s = new_subscribed_data_track();
    assert_eq!(4660, s.ddt1.handle());
    assert_eq!(171, s.ddt2.handle());

    // a subscriber subscribes once
    let res = s.dt.add_subscriber(s.sub1.clone());
    assert_eq!(Some(Error::AlreadySubscribed), res.err());
    assert_eq!("already subscribed", Error::AlreadySubscribed.to_string());

    // the handle is the down track's, the rest of the packet is as it came
    let expected = [
        (
            IN_0,
            "10001234006400050102030468656c6c6f",
            "100000ab006400050102030468656c6c6f",
        ),
        (
            IN_1,
            "0c00123400650005010203050006010d030102030405060708090a0b0c0208010203040506070800deadbeef0001",
            "0c0000ab00650005010203050006010d030102030405060708090a0b0c0208010203040506070800deadbeef0001",
        ),
    ];
    for (i, (raw, out1, out2)) in expected.into_iter().enumerate() {
        handle_raw(&s.dt, raw, 1000 + i as i64);
        assert_eq!(i + 1, s.tr1.send_data_track_message_call_count());
        assert_eq!(i + 1, s.tr2.send_data_track_message_call_count());
        assert_eq!(
            unhex(out1),
            s.tr1.send_data_track_message_args_for_call(i),
            "packet {i}"
        );
        assert_eq!(
            unhex(out2),
            s.tr2.send_data_track_message_args_for_call(i),
            "packet {i}"
        );
    }

    // the track counts what arrived, with the bytes as they came
    let stats = stats_of(&s.dt);
    assert_eq!(1000, stats.start_time);
    assert_eq!(0, stats.end_time);
    assert_eq!(101, stats.highest_sequence_number);
    assert_eq!(2, stats.num_packets);
    assert_eq!(0, stats.num_packets_lost);
    assert_eq!(0, stats.num_packets_out_of_order);
    assert_eq!(1, stats.num_frames);
    assert_eq!(63, stats.num_bytes);

    // the bytes are counted as they came, by the track's statistics,
    let totals = s.bytes.get_traffic_totals();
    assert_eq!(63, totals.recv_bytes);
    assert_eq!(2, totals.recv_messages);
    assert_eq!(0, totals.send_bytes);
    assert_eq!(0, totals.send_messages);
    // and as they went, by the statistics of each down track, which tell the subscriber's
    // telemetry when the down track is closed
    s.ddt1.close();
    eventually(
        || s.tl1.on_track_stats_call_count() == 1,
        (5 * SECOND).to_std(),
        (10 * MILLISECOND).to_std(),
    );
    let (key, stat) = s.tl1.on_track_stats_args_for_call(0);
    assert_eq!(
        telemetry::stats_key_for_data(
            "",
            livekit::StreamType::Downstream,
            "sub1ID".into(),
            "DTR_test".into()
        ),
        key
    );
    assert_eq!(1, stat.streams.len());
    assert_eq!(63, stat.streams[0].primary_bytes);
    assert_eq!(2, stat.streams[0].primary_packets);
    s.ddt2.close();
    s.dt.close();

    assert_eq!(7, s.dt.pub_handle());
    assert_eq!("DTR_test", s.dt.id().as_str());
    assert_eq!("test", s.dt.name());
    assert_eq!("pubID", s.dt.publisher_id().as_str());
    assert_eq!("pub", s.dt.publisher_identity().as_str());
    assert_eq!(7, s.dt.to_proto().pub_handle);
}

#[test]
fn a_closed_down_track_gets_nothing_more() {
    let s = new_subscribed_data_track();
    handle_raw(&s.dt, IN_0, 1000);
    handle_raw(&s.dt, IN_1, 1001);

    // removed as a subscriber, the down track is closed and says so once
    let closed = Arc::new(AtomicUsize::new(0));
    {
        let closed = closed.clone();
        s.ddt2.on_close(Some(Box::new(move || {
            closed.fetch_add(1, Ordering::SeqCst);
        })));
    }
    s.dt.remove_subscriber(&"sub2ID".into());
    assert_eq!(1, closed.load(Ordering::SeqCst));
    assert!(!s.dt.is_subscriber(&"sub2ID".into()));
    assert!(s.dt.is_subscriber(&"sub1ID".into()));
    s.ddt2.close();
    assert_eq!(1, closed.load(Ordering::SeqCst));
    // a callback set on a closed down track is called at once
    let late = Arc::new(AtomicUsize::new(0));
    {
        let late = late.clone();
        s.ddt2.on_close(Some(Box::new(move || {
            late.fetch_add(1, Ordering::SeqCst);
        })));
    }
    assert_eq!(1, late.load(Ordering::SeqCst));

    handle_raw(&s.dt, IN_0, 1002);
    assert_eq!(3, s.tr1.send_data_track_message_call_count());
    assert_eq!(2, s.tr2.send_data_track_message_call_count());

    // A down track that is closed by itself gets nothing more either, and stays a
    // subscription of the track.
    s.ddt1.close();
    handle_raw(&s.dt, IN_0, 1003);
    assert_eq!(3, s.tr1.send_data_track_message_call_count());
    assert!(s.dt.is_subscriber(&"sub1ID".into()));
    let res = s.dt.add_subscriber(s.sub1.clone());
    assert_eq!(Some(Error::AlreadySubscribed), res.err());

    let d1 = fakes::downcast::<DataDownTrack>(s.ddt1.clone());
    assert!(d1.subscriber_id().as_str().starts_with("sub1ID:"));
    assert_eq!("DTR_test", d1.id().as_str());
    assert_eq!("test", d1.name());
    let publish_data_track = d1.publish_data_track().unwrap();
    assert!(std::ptr::addr_eq(
        Arc::as_ptr(&publish_data_track),
        Arc::as_ptr(&s.dt)
    ));
    drop(publish_data_track);

    // a transport that fails is asked once per packet
    let tr3 = Arc::new(FakeDataTrackTransport::default());
    tr3.send_data_track_message_returns(Err(Error::TransportFailure));
    let sub3 = new_subscriber_with_transport("sub3ID", "sub3", 9, &tr3);
    assert_eq!(None, s.dt.add_subscriber(sub3).err());
    handle_raw(&s.dt, IN_0, 1004);
    assert_eq!(1, tr3.send_data_track_message_call_count());

    // a closed track takes no new subscriber, the ones it has still get packets
    s.dt.close();
    let tr4 = Arc::new(FakeDataTrackTransport::default());
    let sub4 = new_subscriber_with_transport("sub4ID", "sub4", 10, &tr4);
    let res = s.dt.add_subscriber(sub4);
    assert_eq!(Some(Error::ReceiverClosed), res.as_ref().err().cloned());
    assert!(res.is_err_and(|err| err.to_string() == "datatrack is closed"));
    assert!(!s.dt.is_subscriber(&"sub4ID".into()));
    handle_raw(&s.dt, IN_0, 1005);
    assert_eq!(2, tr3.send_data_track_message_call_count());
    assert_eq!(0, tr4.send_data_track_message_call_count());

    // the packets that came again were counted as out of order, the one after the close
    // by its bytes only
    let stats = stats_of(&s.dt);
    assert_eq!(1000, stats.start_time);
    assert_ne!(0, stats.end_time);
    assert_eq!(101, stats.highest_sequence_number);
    assert_eq!(5, stats.num_packets);
    assert_eq!(0, stats.num_packets_lost);
    assert_eq!(3, stats.num_packets_out_of_order);
    assert_eq!(1, stats.num_frames);
    assert_eq!(131, stats.num_bytes);
}

// What Go's garbage collector does by itself: a track that nobody holds any more goes, with
// subscribers and down tracks in it, and a down track that is still held outlives it.
#[test]
fn a_down_track_does_not_keep_its_data_track() {
    let Subscribed {
        dt,
        bytes,
        ddt1,
        ddt2,
        ..
    } = new_subscribed_data_track();
    let weak = Arc::downgrade(&dt);
    drop(dt);
    assert!(weak.upgrade().is_none());

    assert!(ddt1.publish_data_track().is_none());
    let d1 = fakes::downcast::<DataDownTrack>(ddt1.clone());
    assert_eq!("", d1.id().as_str());
    assert_eq!("", d1.name());
    let closed = Arc::new(AtomicUsize::new(0));
    {
        let closed = closed.clone();
        ddt1.on_close(Some(Box::new(move || {
            closed.fetch_add(1, Ordering::SeqCst);
        })));
    }
    ddt1.close();
    assert_eq!(1, closed.load(Ordering::SeqCst));
    ddt2.close();
    bytes.stop();

    // and no down track is made for a track that is gone
    let res = DataDownTrack::new(DataDownTrackParams {
        logger: logger::get_logger(),
        subscriber_id: "subID".into(),
        publish_data_track: weak,
        handle: 1,
        transport: Arc::new(FakeDataTrackTransport::default()),
        bytes_track_stats: None,
    });
    assert_eq!(Some(Error::ReceiverClosed), res.err());
}
