// Ported from LiveKit, pkg/rtc/participant_internal_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Every participant is made by
// `new_participant_for_test_with_opts`, as upstream, on a node of its own (on the loopback
// interface); a test that makes one runs on the runtime, as a participant's transports do. A
// plain pion peer connection of a test is a connection of musivum's layer, on a node of its own.
// What else differs:
// - A test that the original runs on a zero `ParticipantImpl{}` (`TestIsReady`,
//   `TestPublishCodecFallbackSkipsRTX`) runs on what Rust can make: `TestIsReady` on a test
//   participant whose state it sets, `TestPublishCodecFallbackSkipsRTX` on
//   `setup_enabled_codecs`, which gives the two lists it sets upstream.
// - `p.UpTrackManager.publishedTracks["cid"] = track` (a write into the manager's map
//   "without lock - for testing purpose only") is `add_published_track`, which is the same for
//   a fake track (the key is the track's ID there, which nothing here reads), and the length
//   of that map is the number of `get_published_tracks`.
// - The twirp code of `ErrMoveOldClientVersion` (`FailedPrecondition`): psrpc is not ported,
//   and the error has no code here; the assertion is a comment at its place.
// - `p.params.Sink = msg` (TestDisconnectTiming) changes nothing upstream, the participant
//   having handed its sink to the signaller when it was made: the parameters are not changed
//   after that here, and the line is a comment.
// - `p.params.LoggerResolver` and `p.params.ReporterResolver` (TestLeaveJoinSession) are not
//   there: a comment at their place.
// - pion's default codecs (`webrtc.NewPeerConnection(webrtc.Configuration{})`): the client's
//   media engine registers, in pion's order, those of pion's default codecs that the port's
//   `create_media_engine` registers (`pion_default_codecs`); `transceiver.Sender().GetParameters()`
//   is the receiver's parameters, which are the transceiver's codecs as pion's sender's are
//   (see the header of transport_impl.rs). A simulcast sender of pion (`sender.AddEncoding`)
//   is a sender of one stream whose offer is written as pion writes a simulcast one (its
//   SSRCs left out, its rids and `a=simulcast` added), as the connection layer's own test of
//   simulcast does it: the layer has no simulcast sender.
// - What a stub of a fake sees is recorded and asserted on the test's thread, where the
//   original calls `require` inside the stub (on the transport's thread).

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration as StdDuration;

use super::*;
use crate::protocol::codecs::{OPUS_CODEC_PARAMETERS, RED_CODEC_PARAMETERS};
use crate::protocol::logger;
use crate::protocol::sdp as lksdp;
use crate::routing::{self, FakeMessageSink};
use crate::rtc::types::{
    FakeLocalMediaTrack, FakeLocalParticipantHelper, FakeLocalParticipantListener, FakeMediaTrack,
    FakeParticipantTelemetryListener,
};
use crate::rtc::{DirectionConfig, create_media_engine, logger_with_participant};
use crate::testutils::{eventually_async, fakes, with_timeout_async};
use crate::webrtc::peerconnection::{
    Configuration, MediaEngine, NodeConfig, PeerConnection, RtpTransceiverDirection,
    RtpTransceiverInit, SdpType,
};
use crate::webrtc::{RtpCodecCapability, RtpCodecParameters, RtpCodecType, TrackLocalContext};

const FIVE_SECONDS: StdDuration = StdDuration::from_secs(5);
const TEN_MS: StdDuration = StdDuration::from_millis(10);

#[tokio::test(flavor = "multi_thread")]
async fn test_is_ready() {
    let tests = [
        (livekit::participant_info::State::Joining, false),
        (livekit::participant_info::State::Joined, true),
        (livekit::participant_info::State::Active, true),
        (livekit::participant_info::State::Disconnected, false),
    ];

    for (state, ready) in tests {
        // `p := &ParticipantImpl{}`: a test participant, whose state is set (see the header)
        let p = new_participant_for_test("test");
        p.state.store(state as i32);
        assert_eq!(ready, p.is_ready(), "{}", state.as_str_name());
    }
}

// Disabling a codec can leave the publish list with only RTX and FEC. The
// fallback must still pick a real codec, or the track registers as RTX and
// subscribers see a blank tile.
#[test]
fn test_publish_codec_fallback_skips_rtx() {
    let h264 = livekit::Codec {
        mime: mime::MimeType::H264.as_str().to_string(),
        ..Default::default()
    };
    let rtx = livekit::Codec {
        mime: mime::MimeType::Rtx.as_str().to_string(),
        ..Default::default()
    };

    // `newParticipant().setupEnabledCodecs(...)`: the lists it sets (see the header)
    let setup_enabled_codecs =
        |publish: &[livekit::Codec], disabled: Option<&livekit::DisabledCodecs>| {
            ParticipantImpl::setup_enabled_codecs(&logger::get_logger(), publish, &[], disabled).0
        };

    {
        // h264 enabled, rtx is last and not chosen
        let enabled_publish_codecs = setup_enabled_codecs(&[h264.clone(), rtx.clone()], None);

        assert_eq!(
            mime::MimeType::H264.as_str(),
            select_alternative_video_codec(&enabled_publish_codecs)
        );
    }

    {
        // h264 disabled leaves rtx only, fallback avoids rtx
        let disabled = livekit::DisabledCodecs {
            publish: vec![livekit::Codec {
                mime: mime::MimeType::H264.as_str().to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let enabled_publish_codecs = setup_enabled_codecs(&[h264, rtx], Some(&disabled));

        assert_eq!(1, enabled_publish_codecs.len());
        assert!(mime::is_mime_type_string_rtx(
            &enabled_publish_codecs[0].mime
        ));

        let alt = select_alternative_video_codec(&enabled_publish_codecs);
        assert!(
            !mime::is_mime_type_string_rtx(&alt),
            "fallback must not be rtx, got {alt:?}"
        );
        assert_eq!(mime::MimeType::Vp8.as_str(), alt);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_supports_moving() {
    {
        // current protocol version
        let p = new_participant_for_test_with_opts(
            "test",
            Some(ParticipantOpts {
                protocol_version: types::ProtocolVersion(types::CURRENT_PROTOCOL),
                ..Default::default()
            }),
        );
        assert_eq!(Ok(()), p.supports_moving());
    }

    // a move rejected because of what the participant is cannot be retried into working, but it is
    // still a caller error, so it has to surface as a 4xx rather than a 500
    {
        // client version that cannot be moved
        let p = new_participant_for_test_with_opts(
            "test",
            Some(ParticipantOpts {
                protocol_version: types::ProtocolVersion(6),
                ..Default::default()
            }),
        );

        let err = p.supports_moving();
        assert_eq!(Err(Error::MoveOldClientVersion), err);

        // `require.ErrorAs(t, err, &twirpErr)` and
        // `require.Equal(t, twirp.FailedPrecondition, twirpErr.Code())`: the error has no code
        // of psrpc here (see the header)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_track_publishing() {
    {
        // should send the correct events
        let p = new_participant_for_test("test");
        let track = Arc::new(FakeMediaTrack::default());
        track.id_returns("id".into());
        let published = Arc::new(AtomicBool::new(false));
        let updated = Arc::new(AtomicBool::new(false));
        let listener = fake_listener(&p);
        {
            let updated = updated.clone();
            listener.on_track_updated_calls(move |_p, _track| {
                updated.store(true, Ordering::SeqCst);
            });
        }
        {
            let published = published.clone();
            listener.on_track_published_calls(move |_p, _track| {
                published.store(true, Ordering::SeqCst);
            });
        }
        p.up_track_manager.add_published_track(track.clone());
        p.handle_track_published(track, false, false);
        assert!(
            published.load(Ordering::SeqCst),
            "should send the correct events"
        );
        assert!(
            !updated.load(Ordering::SeqCst),
            "should send the correct events"
        );
        assert_eq!(
            1,
            p.up_track_manager.get_published_tracks().len(),
            "should send the correct events"
        );
    }

    {
        // sends back trackPublished event
        let p = new_participant_for_test("test");
        let sink = fake_sink(&p);
        p.add_track(&livekit::AddTrackRequest {
            cid: "cid".to_string(),
            name: "webcam".to_string(),
            r#type: livekit::TrackType::Video as i32,
            width: 1024,
            height: 768,
            ..Default::default()
        });
        assert_eq!(1, sink.write_message_call_count());
        let res = signal_response(sink.write_message_args_for_call(0));
        let Some(livekit::signal_response::Message::TrackPublished(published)) = res.message else {
            panic!("not a TrackPublished: {res:?}");
        };
        assert_eq!("cid", published.cid);
        let track = published.track.unwrap();
        assert_eq!("webcam", track.name);
        assert_eq!(livekit::TrackType::Video as i32, track.r#type);
        assert_eq!(1024, track.width);
        assert_eq!(768, track.height);
    }

    {
        // should not allow adding of duplicate tracks
        let p = new_participant_for_test("test");
        let sink = fake_sink(&p);
        p.add_track(&livekit::AddTrackRequest {
            cid: "cid".to_string(),
            name: "webcam".to_string(),
            r#type: livekit::TrackType::Video as i32,
            ..Default::default()
        });
        p.add_track(&livekit::AddTrackRequest {
            cid: "cid".to_string(),
            name: "duplicate".to_string(),
            r#type: livekit::TrackType::Audio as i32,
            ..Default::default()
        });

        // error response on duplicate adds a message
        assert_eq!(
            2,
            sink.write_message_call_count(),
            "should not allow adding of duplicate tracks"
        );
    }

    {
        // should queue adding of duplicate tracks if already published by client id in signalling
        let name = "should queue adding of duplicate tracks if already published by client id in signalling";
        let p = new_participant_for_test("test");
        let sink = fake_sink(&p);

        let track = Arc::new(FakeLocalMediaTrack::default());
        track.has_signal_cid_calls(|s| s == "cid");
        track.to_proto_returns(livekit::TrackInfo::default());
        // directly add to publishedTracks without lock - for testing purpose only
        p.up_track_manager.add_published_track(track);

        p.add_track(&livekit::AddTrackRequest {
            cid: "cid".to_string(),
            name: "webcam".to_string(),
            r#type: livekit::TrackType::Video as i32,
            ..Default::default()
        });
        // `queued` `RequestResponse` should add a message
        assert_eq!(1, sink.write_message_call_count(), "{name}");
        assert_eq!(1, pending_track_infos(&p, "cid").len(), "{name}");

        // add again - it should be added to the queue
        p.add_track(&livekit::AddTrackRequest {
            cid: "cid".to_string(),
            name: "webcam".to_string(),
            r#type: livekit::TrackType::Video as i32,
            ..Default::default()
        });
        // `queued` `RequestResponse`s should have been sent for duplicate additions
        assert_eq!(2, sink.write_message_call_count(), "{name}");
        let track_infos = pending_track_infos(&p, "cid");
        assert_eq!(2, track_infos.len(), "{name}");

        // check SID is the same
        assert_eq!(track_infos[0].sid, track_infos[1].sid, "{name}");
    }

    {
        // should queue adding of duplicate tracks if already published by client id in sdp
        let name =
            "should queue adding of duplicate tracks if already published by client id in sdp";
        let p = new_participant_for_test("test");
        let sink = fake_sink(&p);

        let track = Arc::new(FakeLocalMediaTrack::default());
        track.to_proto_returns(livekit::TrackInfo::default());
        track.has_sdp_cid_calls(|s| s == "cid");
        // directly add to publishedTracks without lock - for testing purpose only
        p.up_track_manager.add_published_track(track);

        p.add_track(&livekit::AddTrackRequest {
            cid: "cid".to_string(),
            name: "webcam".to_string(),
            r#type: livekit::TrackType::Video as i32,
            ..Default::default()
        });
        // `queued` `RequestResponse` should add a message
        assert_eq!(1, sink.write_message_call_count(), "{name}");
        assert_eq!(1, pending_track_infos(&p, "cid").len(), "{name}");

        // add again - it should be added to the queue
        p.add_track(&livekit::AddTrackRequest {
            cid: "cid".to_string(),
            name: "webcam".to_string(),
            r#type: livekit::TrackType::Video as i32,
            ..Default::default()
        });
        // `queued` `RequestResponse`s should have been sent for duplicate additions
        assert_eq!(2, sink.write_message_call_count(), "{name}");
        let track_infos = pending_track_infos(&p, "cid");
        assert_eq!(2, track_infos.len(), "{name}");

        // check SID is the same
        assert_eq!(track_infos[0].sid, track_infos[1].sid, "{name}");
    }

    {
        // should release timed out pending tracks when at the pending track limit
        let name = "should release timed out pending tracks when at the pending track limit";
        let p = new_participant_for_test_with_opts(
            "test",
            Some(ParticipantOpts {
                protocol_version: types::ProtocolVersion(7),
                ..Default::default()
            }),
        );
        let sink = fake_sink(&p);
        let supervisor = p.supervisor.clone().unwrap();

        // one slot holds two queued entries that share a track id, both before any media
        let queued = p.add_pending_track(&add_track_request("cidq", "q0"));
        assert!(queued.is_some(), "{name}");
        let queued = queued.unwrap();
        p.add_pending_track(&add_track_request("cidq", "q1")); // queued, returns nil
        assert_eq!(2, pending_track_infos(&p, "cidq").len(), "{name}");

        let mut sids = Vec::new();
        for i in 0..C_MAX_PENDING_TRACKS - 1 {
            let ti =
                p.add_pending_track(&add_track_request(&format!("cid{i}"), &format!("track{i}")));
            assert!(ti.is_some(), "{name}");
            sids.push(livekit::TrackId::from(ti.unwrap().sid.as_str()));
        }
        assert_eq!(C_MAX_PENDING_TRACKS, num_pending_tracks(&p), "{name}");

        // nothing timed out, so a new publish is rejected
        assert!(
            p.add_pending_track(&add_track_request("fresh", "fresh"))
                .is_none(),
            "{name}"
        );

        // one lone track times out, a new publish takes its slot
        supervisor.set_publication_timed_out_for_test(&sids[0]);
        let num_messages = sink.write_message_call_count();
        assert!(
            p.add_pending_track(&add_track_request("fresh", "fresh"))
                .is_some(),
            "{name}"
        );
        assert!(!has_pending_track(&p, "cid0"), "{name}");
        assert!(has_pending_track(&p, "fresh"), "{name}");
        assert_eq!(C_MAX_PENDING_TRACKS, num_pending_tracks(&p), "{name}");
        assert!(supervisor.timed_out_publications().is_empty(), "{name}");

        assert_eq!(num_messages + 1, sink.write_message_call_count(), "{name}");
        let res = signal_response(sink.write_message_args_for_call(num_messages));
        let Some(livekit::signal_response::Message::TrackUnpublished(unpublished)) = res.message
        else {
            panic!("{name}: not a TrackUnpublished: {res:?}");
        };
        assert_eq!(sids[0].as_str(), unpublished.track_sid, "{name}");

        // the queued track id times out, both entries are released together
        supervisor.set_publication_timed_out_for_test(&livekit::TrackId::from(queued.sid.as_str()));
        assert!(
            p.add_pending_track(&add_track_request("fresh2", "fresh2"))
                .is_some(),
            "{name}"
        );
        assert!(!has_pending_track(&p, "cidq"), "{name}");
        assert!(has_pending_track(&p, "fresh2"), "{name}");
        assert_eq!(C_MAX_PENDING_TRACKS, num_pending_tracks(&p), "{name}");
    }

    {
        // should not match late media of a released pending track to another pending track
        let name =
            "should not match late media of a released pending track to another pending track";
        let p = new_participant_for_test_with_opts(
            "test",
            Some(ParticipantOpts {
                protocol_version: types::ProtocolVersion(7),
                ..Default::default()
            }),
        );

        let released = p.add_pending_track(&add_track_request("cidA", "a"));
        assert!(released.is_some(), "{name}");
        let released = released.unwrap();
        // SDP client id that an offer would set, as Firefox does
        p.pending_tracks_lock
            .write()
            .pending_tracks
            .get_mut("cidA")
            .unwrap()
            .track_infos[0]
            .codecs[0]
            .sdp_cid = "sdpA".to_string();

        {
            let mut pt = p.pending_tracks_lock.write();
            p.release_pending_track_locked(&mut pt, &livekit::TrackId::from(released.sid.as_str()));
        }

        let other = p.add_pending_track(&add_track_request("cidB", "b"));
        assert!(other.is_some(), "{name}");
        let other = other.unwrap();

        // media for the released track, by signal or SDP client id, does not fall back to the other track by type
        for cid in ["cidA", "sdpA"] {
            let (_, ti, _, _, _) = p.pending_tracks_lock.read().get_pending_track(
                cid,
                livekit::TrackType::Video,
                true,
            );
            assert!(ti.is_none(), "{name}: {cid}");
        }
        // an unknown client id still falls back by type, as before
        let (_, ti, _, _, _) = p.pending_tracks_lock.read().get_pending_track(
            "unknown",
            livekit::TrackType::Video,
            true,
        );
        assert!(ti.is_some(), "{name}");
        assert_eq!(other.sid, ti.unwrap().sid, "{name}");

        // reusing the released client id publishes on it again
        let reused = p.add_pending_track(&add_track_request("cidA", "a2"));
        assert!(reused.is_some(), "{name}");
        let (_, ti, _, _, _) =
            p.pending_tracks_lock
                .read()
                .get_pending_track("cidA", livekit::TrackType::Video, true);
        assert!(ti.is_some(), "{name}");
        assert_eq!(reused.unwrap().sid, ti.unwrap().sid, "{name}");
        // the old SDP id is cleared too, a reused transceiver keeps it
        let pt = p.pending_tracks_lock.write();
        assert!(!pt.is_released_track_cid_locked("sdpA"), "{name}");
    }

    {
        // should clear a released track when a new request reuses one of its codec client ids
        let name =
            "should clear a released track when a new request reuses one of its codec client ids";
        let p = new_participant_for_test("test");

        let mut pt = p.pending_tracks_lock.write();
        pt.add_released_track_cids_locked(vec![
            "cidA".to_string(),
            "codecA".to_string(),
            "sdpA".to_string(),
        ]);
        pt.add_released_track_cids_locked(vec!["cidB".to_string()]);
        pt.clear_released_track_cids_locked(&livekit::AddTrackRequest {
            cid: "cidC".to_string(),
            simulcast_codecs: vec![livekit::SimulcastCodec {
                codec: "vp8".to_string(),
                cid: "codecA".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        });
        for cid in ["cidA", "codecA", "sdpA"] {
            assert!(!pt.is_released_track_cid_locked(cid), "{name}: {cid}");
        }
        assert!(pt.is_released_track_cid_locked("cidB"), "{name}");
    }

    {
        // should bound released track client ids
        let name = "should bound released track client ids";
        let p = new_participant_for_test("test");

        let mut pt = p.pending_tracks_lock.write();
        for i in 0..=C_MAX_RELEASED_TRACKS {
            pt.add_released_track_cids_locked(vec![format!("cid{i}"), format!("sdp{i}")]);
        }
        assert_eq!(
            C_MAX_RELEASED_TRACKS,
            pt.released_track_cids.len(),
            "{name}"
        );
        // the oldest track ages out with all of its ids
        assert!(!pt.is_released_track_cid_locked("cid0"), "{name}");
        assert!(!pt.is_released_track_cid_locked("sdp0"), "{name}");
        assert!(
            pt.is_released_track_cid_locked(&format!("sdp{C_MAX_RELEASED_TRACKS}")),
            "{name}"
        );
    }

    {
        // should not allow adding disallowed sources
        let name = "should not allow adding disallowed sources";
        let p = new_participant_for_test("test");
        p.set_permission(Some(&livekit::ParticipantPermission {
            can_publish: true,
            can_publish_sources: vec![livekit::TrackSource::Camera as i32],
            ..Default::default()
        }));
        let sink = fake_sink(&p);
        p.add_track(&livekit::AddTrackRequest {
            cid: "cid".to_string(),
            name: "webcam".to_string(),
            source: livekit::TrackSource::Camera as i32,
            r#type: livekit::TrackType::Video as i32,
            ..Default::default()
        });
        assert_eq!(1, sink.write_message_call_count(), "{name}");

        p.add_track(&livekit::AddTrackRequest {
            cid: "cid2".to_string(),
            name: "rejected source".to_string(),
            r#type: livekit::TrackType::Audio as i32,
            source: livekit::TrackSource::Microphone as i32,
            ..Default::default()
        });
        // an error response for disallowed source should send a `RequestResponse`.
        assert_eq!(2, sink.write_message_call_count(), "{name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_permission_data_track_subscriptions() {
    let p = new_participant_for_test_with_opts(
        "sub",
        Some(ParticipantOpts {
            permissions: Some(livekit::ParticipantPermission {
                can_subscribe: true,
                ..Default::default()
            }),
            ..Default::default()
        }),
    );

    let pub_id = livekit::ParticipantId::from("PA_pub");
    let dt = DataTrack::new(
        DataTrackParams {
            logger: logger::get_logger(),
            participant_id: {
                let pub_id = pub_id.clone();
                Box::new(move || pub_id.clone())
            },
            participant_identity: "pub".into(),
            bytes_track_stats: None,
        },
        &livekit::DataTrackInfo {
            pub_handle: 1,
            sid: "DTR_test".to_string(),
            name: "test".to_string(),
            ..Default::default()
        },
    );

    let helper =
        fakes::downcast::<FakeLocalParticipantHelper>(p.params.participant_helper.clone().unwrap());
    helper.resolve_data_track_returns(types::DataResolverResult {
        data_track: Some(dt.clone()),
        has_permission: true,
        publisher_id: pub_id.clone(),
        publisher_identity: "pub".into(),
        ..Default::default()
    });

    p.subscription_manager.subscribe_to_data_track(&dt.id());
    // the data track adds the subscriber before the subscription manager records it, and a
    // revoke only removes recorded subscriptions, so wait for both
    // "data track subscription should be established"
    eventually_async(
        || {
            dt.is_subscriber(&p.id())
                && p.subscription_manager.get_subscribed_data_tracks().len() == 1
        },
        FIVE_SECONDS,
        TEN_MS,
    )
    .await;

    // revoking canSubscribe should drop data track subscriptions, like media track subscriptions
    assert!(p.set_permission(Some(&livekit::ParticipantPermission {
        can_subscribe: false,
        ..Default::default()
    })));
    assert!(!dt.is_subscriber(&p.id()));
    assert!(
        p.subscription_manager
            .get_subscribed_data_tracks()
            .is_empty()
    );

    // granting canSubscribe again should restore the subscription
    assert!(p.set_permission(Some(&livekit::ParticipantPermission {
        can_subscribe: true,
        ..Default::default()
    })));
    // "data track subscription should be restored"
    eventually_async(|| dt.is_subscriber(&p.id()), FIVE_SECONDS, TEN_MS).await;

    // the deferred closes
    dt.close();
    let _ = p.close(false, types::ParticipantCloseReason::None, false);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_out_of_order_updates() {
    let p = new_participant_for_test("test");
    p.update_state(livekit::participant_info::State::Joined);
    p.set_metadata("initial metadata");
    let sink = fakes::downcast::<FakeMessageSink>(p.get_response_sink().unwrap());
    let pi1 = p.to_proto();
    p.set_metadata("second update");
    let pi2 = p.to_proto();

    assert!(pi2.version > pi1.version);

    // send the second update first
    assert_eq!(Ok(()), p.send_participant_update(&[pi2]));
    assert_eq!(Ok(()), p.send_participant_update(&[pi1]));

    // only sent once, and it's the earlier message
    assert_eq!(1, sink.write_message_call_count());
    let sent = signal_response(sink.write_message_args_for_call(0));
    let Some(livekit::signal_response::Message::Update(update)) = sent.message else {
        panic!("not an update: {sent:?}");
    };
    assert_eq!("second update", update.participants[0].metadata);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recently_disconnected_keeps_update_order() {
    let p = new_participant_for_test("test");

    // fill the update cache (128 entries), the last update is a disconnect
    const CACHE_SIZE: usize = 128;
    for i in 0..CACHE_SIZE {
        let mut state = livekit::participant_info::State::Active;
        if i == CACHE_SIZE - 1 {
            state = livekit::participant_info::State::Disconnected;
        }
        assert_eq!(
            Ok(()),
            p.send_participant_update(&[livekit::ParticipantInfo {
                sid: format!("PA_{i}"),
                identity: format!("p{i}"),
                version: 1,
                state: state as i32,
                ..Default::default()
            }])
        );
    }

    let disconnected = p.get_recently_disconnected_participants();
    assert_eq!(1, disconnected.len());
    assert_eq!("PA_127", disconnected[0].sid);

    // an update for a new participant evicts the least recently updated entry,
    // not the disconnect that was just reported
    assert_eq!(
        Ok(()),
        p.send_participant_update(&[livekit::ParticipantInfo {
            sid: "PA_new".to_string(),
            identity: "new".to_string(),
            version: 1,
            state: livekit::participant_info::State::Active as i32,
            ..Default::default()
        }])
    );

    let disconnected = p.get_recently_disconnected_participants();
    assert_eq!(1, disconnected.len());
    assert_eq!("PA_127", disconnected[0].sid);
}

// after disconnection, things should continue to function and not panic
#[tokio::test(flavor = "multi_thread")]
async fn test_disconnect_timing() {
    {
        // Negotiate doesn't panic after channel closed
        let p = new_participant_for_test("test");
        let msg = routing::MessageChannel::new(
            livekit::ConnectionId::from("test"),
            routing::DEFAULT_MESSAGE_CHANNEL_SIZE,
        );
        // `p.params.Sink = msg`: no counterpart (see the header)
        let reader = {
            let read_chan = msg.read_chan();
            std::thread::spawn(move || {
                while let Some(msg) = read_chan.blocking_recv() {
                    println!("received message from chan {msg:?}");
                }
            })
        };
        let track = Arc::new(FakeMediaTrack::default());
        p.up_track_manager.add_published_track(track.clone());
        p.handle_track_published(track, false, false);

        // close channel and then try to Negotiate
        msg.close();
        let _ = reader.join();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_correct_joined_at() {
    let p = new_participant_for_test("test");
    let info = p.to_proto();
    assert_ne!(0, info.joined_at);
    assert!(time::now().unix_secs() - info.joined_at <= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mute_setting() {
    {
        // can set mute when track is pending
        let p = new_participant_for_test("test");
        let ti = livekit::TrackInfo {
            sid: "testTrack".to_string(),
            ..Default::default()
        };
        p.pending_tracks_lock.write().pending_tracks.insert(
            "cid".to_string(),
            PendingTrackInfo {
                track_infos: vec![ti.clone()],
                ..Default::default()
            },
        );

        p.set_track_muted(
            &livekit::MuteTrackRequest {
                sid: ti.sid.clone(),
                muted: true,
            },
            false,
        );
        assert!(
            pending_track_infos(&p, "cid")[0].muted,
            "can set mute when track is pending"
        );
    }

    {
        // can publish a muted track
        let p = new_participant_for_test("test");
        p.add_track(&livekit::AddTrackRequest {
            cid: "cid".to_string(),
            r#type: livekit::TrackType::Audio as i32,
            muted: true,
            ..Default::default()
        });

        let (_, ti, _, _, _) =
            p.pending_tracks_lock
                .read()
                .get_pending_track("cid", livekit::TrackType::Audio, false);
        assert!(ti.is_some(), "can publish a muted track");
        assert!(ti.unwrap().muted, "can publish a muted track");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscriber_as_primary() {
    {
        // protocol 4 uses subs as primary
        let p = new_participant_for_test_with_opts(
            "test",
            Some(ParticipantOpts {
                permissions: Some(livekit::ParticipantPermission {
                    can_subscribe: true,
                    can_publish: true,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );
        assert!(p.subscriber_as_primary(), "protocol 4 uses subs as primary");
    }

    {
        // protocol 2 uses pub as primary
        let p = new_participant_for_test_with_opts(
            "test",
            Some(ParticipantOpts {
                protocol_version: types::ProtocolVersion(2),
                permissions: Some(livekit::ParticipantPermission {
                    can_subscribe: true,
                    can_publish: true,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );
        assert!(!p.subscriber_as_primary(), "protocol 2 uses pub as primary");
    }

    {
        // publisher only uses pub as primary
        let p = new_participant_for_test_with_opts(
            "test",
            Some(ParticipantOpts {
                permissions: Some(livekit::ParticipantPermission {
                    can_subscribe: false,
                    can_publish: true,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );
        assert!(
            !p.subscriber_as_primary(),
            "publisher only uses pub as primary"
        );

        // ensure that it doesn't change after perms
        p.set_permission(Some(&livekit::ParticipantPermission {
            can_subscribe: true,
            can_publish: true,
            ..Default::default()
        }));
        assert!(
            !p.subscriber_as_primary(),
            "publisher only uses pub as primary"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_disable_codecs() {
    let participant = new_participant_for_test_with_opts(
        "123",
        Some(ParticipantOpts {
            publisher: false,
            client_conf: Some(livekit::ClientConfiguration {
                disabled_codecs: Some(livekit::DisabledCodecs {
                    codecs: vec![livekit::Codec {
                        mime: "video/h264".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
    );

    participant.set_migrate_state(types::MigrateState::Complete);

    let pc = new_pion_default_peer_connection();
    let transceiver = pc
        .add_transceiver_from_kind(
            RtpCodecType::Video,
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Sendrecv,
            }),
        )
        .unwrap();
    let sdp = pc.create_offer(None).unwrap();
    let _ = pc.set_local_description(sdp.clone());
    let codecs = transceiver.receiver().unwrap().get_parameters().codecs;
    let mut found264 = false;
    for c in &codecs {
        if mime::is_mime_type_string_h264(&c.mime_type) {
            found264 = true;
        }
    }
    assert!(found264);
    let offer_id = 42u32;

    // negotiated codec should not contain h264
    let sink = Arc::new(FakeMessageSink::default());
    participant.swap_response_sink(Some(sink.clone()), types::SignallingCloseReason::Unknown);
    let answer: Arc<Mutex<Option<SessionDescription>>> = Arc::new(Mutex::new(None));
    let answer_received = Arc::new(AtomicBool::new(false));
    let answer_id_received = Arc::new(AtomicU32::new(0));
    {
        let answer = answer.clone();
        let answer_received = answer_received.clone();
        let answer_id_received = answer_id_received.clone();
        sink.write_message_calls(move |msg| {
            if let Some(livekit::signal_response::Message::Answer(sd)) = response_message(&msg) {
                let (sd, answer_id, _) = protosignalling::from_proto_session_description(sd);
                *answer.lock() = Some(sd);
                answer_received.store(true, Ordering::SeqCst);
                answer_id_received.store(answer_id, Ordering::SeqCst);
            }
            Ok(())
        });
    }
    let _ = participant.handle_offer(&livekit::SessionDescription {
        r#type: "offer".to_string(),
        sdp: sdp.sdp.clone(),
        id: offer_id,
        ..Default::default()
    });

    with_timeout_async(
        || {
            if answer_received.load(Ordering::SeqCst)
                && answer_id_received.load(Ordering::SeqCst) == offer_id
            {
                String::new()
            } else {
                "answer not received OR answer id mismatch".to_string()
            }
        },
        None,
    )
    .await;
    let answer = answer.lock().clone().unwrap();
    pc.set_remote_description(answer.clone())
        .unwrap_or_else(|err| panic!("{err}: {}\n{}", answer.sdp, sdp.sdp));

    let codecs = transceiver.receiver().unwrap().get_parameters().codecs;
    let mut found264 = false;
    for c in &codecs {
        if mime::is_mime_type_string_h264(&c.mime_type) {
            found264 = true;
        }
    }
    assert!(!found264);

    let _ = pc.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_disable_publish_codec() {
    let participant = new_participant_for_test_with_opts(
        "123",
        Some(ParticipantOpts {
            publisher: true,
            client_conf: Some(livekit::ClientConfiguration {
                disabled_codecs: Some(livekit::DisabledCodecs {
                    publish: vec![livekit::Codec {
                        mime: "video/h264".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
    );

    for codec in &participant.enabled_publish_codecs {
        assert!(!mime::is_mime_type_string_h264(&codec.mime));
    }

    let sink = Arc::new(FakeMessageSink::default());
    participant.swap_response_sink(Some(sink.clone()), types::SignallingCloseReason::Unknown);
    // what the stub sees of a published track: the MIME type of its first codec
    let publish_received: Arc<Mutex<Option<Vec<String>>>> = Arc::new(Mutex::new(None));
    let record_published = |publish_received: &Arc<Mutex<Option<Vec<String>>>>| {
        let publish_received = publish_received.clone();
        move |msg: Message| {
            if let Some(livekit::signal_response::Message::TrackPublished(published)) =
                response_message(&msg)
            {
                let codecs = published
                    .track
                    .as_ref()
                    .map(|t| t.codecs.iter().map(|c| c.mime_type.clone()).collect())
                    .unwrap_or_default();
                *publish_received.lock() = Some(codecs);
            }
            Ok(())
        }
    };
    sink.write_message_calls(record_published(&publish_received));

    // simulcast codec response should pick an alternative
    participant.add_track(&livekit::AddTrackRequest {
        cid: "cid1".to_string(),
        r#type: livekit::TrackType::Video as i32,
        simulcast_codecs: vec![livekit::SimulcastCodec {
            codec: "h264".to_string(),
            cid: "cid1".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    });

    eventually_async(|| publish_received.lock().is_some(), FIVE_SECONDS, TEN_MS).await;
    let codecs = publish_received.lock().take().unwrap();
    assert!(!codecs.is_empty());
    assert!(mime::is_mime_type_string_vp8(&codecs[0]));

    // publishing a supported codec should not change
    sink.write_message_calls(record_published(&publish_received));
    participant.add_track(&livekit::AddTrackRequest {
        cid: "cid2".to_string(),
        r#type: livekit::TrackType::Video as i32,
        simulcast_codecs: vec![livekit::SimulcastCodec {
            codec: "vp8".to_string(),
            cid: "cid2".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    });
    eventually_async(|| publish_received.lock().is_some(), FIVE_SECONDS, TEN_MS).await;
    let codecs = publish_received.lock().take().unwrap();
    assert!(!codecs.is_empty());
    assert!(mime::is_mime_type_string_vp8(&codecs[0]));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_prefer_media_codec_for_publisher() {
    struct TestCase {
        name: &'static str,
        media_kind: &'static str,
        track_base_cid: &'static str,
        preferred_codec: &'static str,
        add_track: livekit::AddTrackRequest,
        mime_type_string_checker: fn(&str) -> bool,
        mime_type_codec_string_checker: fn(&str) -> bool,
        transceiver_mime_type: mime::MimeType,
    }
    let test_cases = [
        TestCase {
            name: "video",
            media_kind: "video",
            track_base_cid: "preferH264Video",
            preferred_codec: "h264",
            add_track: livekit::AddTrackRequest {
                r#type: livekit::TrackType::Video as i32,
                name: "video".to_string(),
                width: 1280,
                height: 720,
                source: livekit::TrackSource::Camera as i32,
                ..Default::default()
            },
            mime_type_string_checker: mime::is_mime_type_string_h264,
            mime_type_codec_string_checker: mime::is_mime_type_codec_string_h264,
            transceiver_mime_type: mime::MimeType::Vp8,
        },
        TestCase {
            name: "audio",
            media_kind: "audio",
            track_base_cid: "preferPCMAAudio",
            preferred_codec: "pcma",
            add_track: livekit::AddTrackRequest {
                r#type: livekit::TrackType::Audio as i32,
                name: "audio".to_string(),
                source: livekit::TrackSource::Microphone as i32,
                ..Default::default()
            },
            mime_type_string_checker: mime::is_mime_type_string_pcma,
            mime_type_codec_string_checker: mime::is_mime_type_codec_string_pcma,
            transceiver_mime_type: mime::MimeType::Opus,
        },
    ];
    for tc in test_cases {
        let participant = new_participant_for_test_with_opts(
            "123",
            Some(ParticipantOpts {
                publisher: true,
                ..Default::default()
            }),
        );
        participant.set_migrate_state(types::MigrateState::Complete);

        let pc = Arc::new(new_pion_default_peer_connection());

        for i in 0..2 {
            // publish preferred track without client using setCodecPreferences()
            let track_cid = format!("{}-{}", tc.track_base_cid, i);
            let mut req = tc.add_track.clone();
            req.simulcast_codecs = vec![livekit::SimulcastCodec {
                codec: tc.preferred_codec.to_string(),
                cid: track_cid.clone(),
                ..Default::default()
            }];
            participant.add_track(&req);

            let track = StaticTrack::new(
                RtpCodecCapability {
                    mime_type: tc.transceiver_mime_type.as_str().to_string(),
                    ..Default::default()
                },
                &track_cid,
                &track_cid,
                "",
            );
            let transceiver = pc
                .add_transceiver_from_track(
                    track,
                    Some(RtpTransceiverInit {
                        direction: RtpTransceiverDirection::Sendrecv,
                    }),
                )
                .unwrap();
            let mut codecs = transceiver.receiver().unwrap().get_parameters().codecs;

            if i > 0 {
                // the negotiated codecs order could be updated by first negotiation,
                // reorder to make tested preferred codec not preferred
                while (tc.mime_type_string_checker)(&codecs[0].mime_type) {
                    codecs.rotate_left(1);
                }
            }
            // preferred codec should not be preferred in `offer`
            assert!(
                !(tc.mime_type_string_checker)(&codecs[0].mime_type),
                "{}: codecs {codecs:?}",
                tc.name
            );

            let sdp = pc.create_offer(None).unwrap();
            pc.set_local_description(sdp.clone()).unwrap();
            let offer_id = 23u32;

            let sink = Arc::new(FakeMessageSink::default());
            participant
                .swap_response_sink(Some(sink.clone()), types::SignallingCloseReason::Unknown);
            let answer: Arc<Mutex<Option<SessionDescription>>> = Arc::new(Mutex::new(None));
            let answer_received = Arc::new(AtomicBool::new(false));
            let answer_id_received = Arc::new(AtomicU32::new(0));
            {
                let answer = answer.clone();
                let answer_received = answer_received.clone();
                let answer_id_received = answer_id_received.clone();
                let pc = pc.clone();
                sink.write_message_calls(move |msg| {
                    if let Some(livekit::signal_response::Message::Answer(sd)) =
                        response_message(&msg)
                    {
                        let (sd, answer_id, _) =
                            protosignalling::from_proto_session_description(sd);
                        let _ = pc.set_remote_description(sd.clone());
                        *answer.lock() = Some(sd);
                        answer_received.store(true, Ordering::SeqCst);
                        answer_id_received.store(answer_id, Ordering::SeqCst);
                    }
                    Ok(())
                });
            }
            let _ = participant.handle_offer(&livekit::SessionDescription {
                r#type: "offer".to_string(),
                sdp: sdp.sdp.clone(),
                id: offer_id,
                ..Default::default()
            });

            eventually_async(
                || {
                    answer_received.load(Ordering::SeqCst)
                        && answer_id_received.load(Ordering::SeqCst) == offer_id
                },
                FIVE_SECONDS,
                TEN_MS,
            )
            .await;

            let mut have_preferred = false;
            let answer = answer.lock().clone().unwrap();
            let parsed = answer.unmarshal().unwrap();
            let mut media_section_index = 0;
            for m in &parsed.media_descriptions {
                if m.media_name.media == tc.media_kind {
                    if media_section_index == i {
                        let codecs = lksdp::codecs_from_media_description(m).unwrap();
                        if (tc.mime_type_codec_string_checker)(&codecs[0].name) {
                            have_preferred = true;
                            break;
                        }
                    }
                    media_section_index += 1;
                }
            }

            assert!(
                have_preferred,
                "{}: {} should be preferred for {} section {}, answer sdp: \n{}",
                tc.name, tc.preferred_codec, tc.media_kind, i, answer.sdp
            );
        }
        let _ = pc.close();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_one_shot_simulcast_offer_applies_rids() {
    let participant = new_participant_for_test_with_opts(
        "whip",
        Some(ParticipantOpts {
            publisher: true,
            one_shot: true,
            ..Default::default()
        }),
    );
    participant.set_migrate_state(types::MigrateState::Complete);

    let pc = new_pion_default_peer_connection();

    let codec = RtpCodecCapability {
        mime_type: mime::MimeType::Vp8.as_str().to_string(),
        ..Default::default()
    };
    // `medium` (rid "h") with `low` (rid "q") added as an encoding of its sender: a sender of
    // one stream, whose offer is written as pion writes a simulcast one (see the header)
    let medium = StaticTrack::new(codec, "video", "stream", "h");
    pc.add_track(medium).unwrap();

    let offer = pc.create_offer(None).unwrap();
    pc.set_local_description(offer.clone()).unwrap();
    let offer = as_simulcast_offer(&offer, &["h", "q"]);
    assert!(offer.sdp.contains("a=simulcast:send h;q"), "{}", offer.sdp);

    assert_eq!(
        Ok(()),
        participant.handle_offer(&livekit::SessionDescription {
            r#type: "offer".to_string(),
            sdp: offer.sdp.clone(),
            ..Default::default()
        })
    );

    let pti = participant
        .pending_tracks_lock
        .read()
        .pending_tracks
        .get("video")
        .cloned();
    assert!(pti.is_some());
    let pti = pti.unwrap();

    let layers = &pti.track_infos[0].codecs[0].layers;
    assert_eq!(2, layers.len());
    assert_eq!(livekit::VideoQuality::Low as i32, layers[0].quality);
    assert_eq!("q", layers[0].rid);
    assert_eq!(0, layers[0].spatial_layer);
    assert_eq!(livekit::VideoQuality::Medium as i32, layers[1].quality);
    assert_eq!("h", layers[1].rid);
    assert_eq!(1, layers[1].spatial_layer);

    let _ = pc.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_one_shot_single_layer_offer_unchanged() {
    let participant = new_participant_for_test_with_opts(
        "whip",
        Some(ParticipantOpts {
            publisher: true,
            one_shot: true,
            ..Default::default()
        }),
    );
    participant.set_migrate_state(types::MigrateState::Complete);

    let pc = new_pion_default_peer_connection();

    let track = StaticTrack::new(
        RtpCodecCapability {
            mime_type: mime::MimeType::Vp8.as_str().to_string(),
            ..Default::default()
        },
        "video",
        "stream",
        "",
    );
    pc.add_transceiver_from_track(
        track,
        Some(RtpTransceiverInit {
            direction: RtpTransceiverDirection::Sendonly,
        }),
    )
    .unwrap();

    let offer = pc.create_offer(None).unwrap();
    pc.set_local_description(offer.clone()).unwrap();
    assert!(!offer.sdp.contains("a=simulcast"));
    assert!(!offer.sdp.contains("a=rid"));

    assert_eq!(
        Ok(()),
        participant.handle_offer(&livekit::SessionDescription {
            r#type: "offer".to_string(),
            sdp: offer.sdp.clone(),
            ..Default::default()
        })
    );

    let pti = participant
        .pending_tracks_lock
        .read()
        .pending_tracks
        .get("video")
        .cloned();
    assert!(pti.is_some());
    let pti = pti.unwrap();

    let layers = &pti.track_infos[0].codecs[0].layers;
    assert_eq!(1, layers.len());
    assert_eq!("", layers[0].rid);
    assert_eq!(0, layers[0].spatial_layer);

    let _ = pc.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_prefer_audio_codec_for_red() {
    let participant = new_participant_for_test_with_opts(
        "123",
        Some(ParticipantOpts {
            publisher: true,
            ..Default::default()
        }),
    );
    participant.set_migrate_state(types::MigrateState::Complete);

    let mut me = MediaEngine::default();
    let nack = webrtc::RtcpFeedback {
        r#type: webrtc::TYPE_RTCP_FB_NACK.to_string(),
        parameter: String::new(),
    };
    let mut opus_codec_parameters: RtpCodecParameters = OPUS_CODEC_PARAMETERS.clone();
    opus_codec_parameters.rtp_codec_capability.rtcp_feedback = vec![nack.clone()];
    webrtc::peerconnection::register_codec(&mut me, &opus_codec_parameters, RtpCodecType::Audio)
        .unwrap();
    let mut red_codec_parameters: RtpCodecParameters = RED_CODEC_PARAMETERS.clone();
    red_codec_parameters.rtp_codec_capability.rtcp_feedback = vec![nack];
    webrtc::peerconnection::register_codec(&mut me, &red_codec_parameters, RtpCodecType::Audio)
        .unwrap();

    let pc = Arc::new(new_peer_connection_with(me));

    for (idx, disable_red) in [false, true, false, true].into_iter().enumerate() {
        let name = format!("disableRed={disable_red}");
        let track_cid = format!("audiotrack{idx}");
        let mut req = livekit::AddTrackRequest {
            r#type: livekit::TrackType::Audio as i32,
            cid: track_cid.clone(),
            ..Default::default()
        };
        if idx < 2 {
            req.disable_red = disable_red;
        } else {
            let codec = if disable_red { "opus" } else { "red" };
            req.simulcast_codecs = vec![livekit::SimulcastCodec {
                codec: codec.to_string(),
                cid: track_cid.clone(),
                ..Default::default()
            }];
        }
        participant.add_track(&req);

        let track = StaticTrack::new(
            RtpCodecCapability {
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
            &track_cid,
            &track_cid,
            "",
        );

        let transceiver = pc
            .add_transceiver_from_track(
                track,
                Some(RtpTransceiverInit {
                    direction: RtpTransceiverDirection::Sendrecv,
                }),
            )
            .unwrap();
        // `transceiver.Sender().GetParameters().Codecs`: the receiver's (see the header)
        let mut codecs = transceiver.receiver().unwrap().get_parameters().codecs;
        for i in 0..codecs.len() {
            if codecs[i].mime_type == "audio/opus" {
                if i != 0 {
                    codecs.swap(0, i);
                }
                break;
            }
        }
        transceiver.set_codec_preferences(codecs.clone()).unwrap();
        let sdp = pc.create_offer(None).unwrap();
        let _ = pc.set_local_description(sdp.clone());
        // opus should be preferred
        assert_eq!(codecs[0].mime_type, "audio/opus", "{name}: {}", sdp.sdp);
        let offer_id = 0xffffffu32;

        let sink = Arc::new(FakeMessageSink::default());
        participant.swap_response_sink(Some(sink.clone()), types::SignallingCloseReason::Unknown);
        let answer: Arc<Mutex<Option<SessionDescription>>> = Arc::new(Mutex::new(None));
        let answer_received = Arc::new(AtomicBool::new(false));
        let answer_id_received = Arc::new(AtomicU32::new(0));
        {
            let answer = answer.clone();
            let answer_received = answer_received.clone();
            let answer_id_received = answer_id_received.clone();
            let pc = pc.clone();
            sink.write_message_calls(move |msg| {
                if let Some(livekit::signal_response::Message::Answer(sd)) = response_message(&msg)
                {
                    let (sd, answer_id, _) = protosignalling::from_proto_session_description(sd);
                    let _ = pc.set_remote_description(sd.clone());
                    *answer.lock() = Some(sd);
                    answer_received.store(true, Ordering::SeqCst);
                    answer_id_received.store(answer_id, Ordering::SeqCst);
                }
                Ok(())
            });
        }
        let _ = participant.handle_offer(&livekit::SessionDescription {
            r#type: "offer".to_string(),
            sdp: sdp.sdp.clone(),
            id: offer_id,
            ..Default::default()
        });
        eventually_async(
            || {
                answer_received.load(Ordering::SeqCst)
                    && answer_id_received.load(Ordering::SeqCst) == offer_id
            },
            FIVE_SECONDS,
            TEN_MS,
        )
        .await;

        let mut red_preferred = false;
        let answer = answer.lock().clone().unwrap();
        let parsed = answer.unmarshal().unwrap();
        let mut audio_section_index = 0;
        for m in &parsed.media_descriptions {
            if m.media_name.media == "audio" {
                if audio_section_index == idx {
                    let codecs = lksdp::codecs_from_media_description(m).unwrap();
                    // nack is always enabled. if red is preferred, server will not generate nack request
                    let mut nack_enabled = false;
                    for c in &codecs {
                        if c.name == "opus" {
                            for fb in &c.rtcp_feedback {
                                if fb.contains("nack") {
                                    nack_enabled = true;
                                    break;
                                }
                            }
                        }
                    }
                    assert!(nack_enabled, "{name}: nack should be enabled for opus");

                    if mime::is_mime_type_codec_string_red(&codecs[0].name) {
                        red_preferred = true;
                        break;
                    }
                }
                audio_section_index += 1;
            }
        }
        assert_eq!(
            !disable_red, red_preferred,
            "{name}: offer : \n{}\nanswer sdp: \n{}",
            sdp.sdp, answer.sdp
        );
    }
    let _ = pc.close();
}

#[derive(Default)]
struct ParticipantOpts {
    permissions: Option<livekit::ParticipantPermission>,
    protocol_version: types::ProtocolVersion,
    publisher: bool,
    client_conf: Option<livekit::ClientConfiguration>,
    client_info: Option<livekit::ClientInfo>,
    migration: bool,
    one_shot: bool,
}

fn new_participant_for_test_with_opts(
    identity: &str,
    opts: Option<ParticipantOpts>,
) -> Arc<ParticipantImpl> {
    let mut opts = opts.unwrap_or_default();
    if opts.protocol_version.0 == 0 {
        opts.protocol_version = types::ProtocolVersion(6);
    }
    let mut conf = config::Config::new("", true).unwrap();
    // disable mux, it doesn't play too well with unit test
    conf.rtc.rtc_config.tcp_port = 0;
    let mut rtc_conf = WebRtcConfig::new(&conf).unwrap();
    let ff = buffer::FactoryOfBufferFactory::new(500, 200);
    rtc_conf.set_buffer_factory(ff.create_buffer_factory());
    let mut grants = auth::ClaimGrants {
        video: Some(auth::VideoGrant::default()),
        ..Default::default()
    };
    if let Some(permissions) = &opts.permissions {
        let video = grants.video.as_mut().unwrap();
        video.set_can_publish(permissions.can_publish);
        video.set_can_publish_data(permissions.can_publish_data);
        video.set_can_subscribe(permissions.can_subscribe);
    }

    let mut enabled_codecs = Vec::with_capacity(conf.room.enabled_codecs.len());
    for c in &conf.room.enabled_codecs {
        enabled_codecs.push(livekit::Codec {
            mime: c.mime.clone(),
            fmtp_line: c.fmtp_line.clone(),
        });
    }
    let identity = livekit::ParticipantIdentity::from(identity);
    let sid = livekit::ParticipantId::from(guid::new(putils::PARTICIPANT_PREFIX).as_str());
    let p = ParticipantImpl::new(ParticipantParams {
        sid: sid.clone(),
        identity: identity.clone(),
        config: Arc::new(rtc_conf),
        sink: Some(Arc::new(FakeMessageSink::default())),
        protocol_version: opts.protocol_version,
        session_start_time: time::now(),
        pli_throttle_config: conf.rtc.pli_throttle,
        grants: Some(Arc::new(grants)),
        publish_enabled_codecs: enabled_codecs.clone(),
        subscribe_enabled_codecs: enabled_codecs,
        client_conf: opts.client_conf,
        client_info: ClientInfo {
            client_info: opts.client_info.map(Arc::new),
        },
        logger: logger_with_participant(&logger::get_logger(), &identity, &sid, false),
        // `Reporter: roomobs.NewNoopParticipantSessionReporter()`: not there (see the header
        // of participant.rs)
        telemetry_listener: Some(Arc::new(FakeParticipantTelemetryListener::default())),
        version_generator: putils::new_default_timed_version_generator(),
        participant_listener: Some(Arc::new(FakeLocalParticipantListener::default())),
        participant_helper: Some(Arc::new(FakeLocalParticipantHelper::default())),
        migration: opts.migration,
        use_one_shot_signalling_mode: opts.one_shot,
        node: new_node(),
        // Go's zero values for the rest
        name: Default::default(),
        audio_config: Default::default(),
        video_config: Default::default(),
        limit_config: Default::default(),
        trailer: Vec::new(),
        congestion_control_config: Default::default(),
        sim_tracks: HashMap::new(),
        token_expires_at: Time::ZERO,
        initial_version: 0,
        region: String::new(),
        reconnect: false,
        adaptive_stream: false,
        allow_tcp_fallback: false,
        tcp_fallback_rtt_threshold: 0,
        allow_udp_unstable_fallback: false,
        turns_enabled: false,
        disable_supervisor: false,
        reconnect_on_publication_error: false,
        reconnect_on_subscription_error: false,
        reconnect_on_data_channel_error: false,
        disable_dynacast: false,
        subscriber_allow_pause: false,
        subscription_limit_audio: 0,
        subscription_limit_video: 0,
        subscribe_pacer: None,
        playout_delay: None,
        sync_streams: false,
        forward_stats: None,
        disable_sender_report_pass_through: false,
        enable_metrics: false,
        data_channel_max_buffered_amount: 0,
        datachannel_slow_threshold: 0,
        datachannel_lossy_target_latency: Duration::ZERO,
        datachannel_data_track_target_latency: Duration::ZERO,
        fire_on_track_by_sdp: false,
        disable_codec_regression: false,
        last_pub_reliable_seq: 0,
        country: String::new(),
        prefer_video_size_from_media: false,
        use_single_peer_connection: false,
        enable_data_tracks: false,
        enable_rtp_stream_restart_detection: false,
        force_backup_codec_policy_simulcast: false,
        disable_transceiver_reuse_for_e2ee: false,
        enable_participant_data_blob: false,
        enable_start_at_desired_quality: false,
        migration_wait_duration: Duration::ZERO,
        exclude_ipv6_local_candidates: false,
        enable_warp: false,
        media_batch_io_enabled: false,
    })
    .unwrap();
    p.is_publisher.store(opts.publisher);
    p.update_state(livekit::participant_info::State::Active);

    p
}

fn new_participant_for_test(identity: &str) -> Arc<ParticipantImpl> {
    new_participant_for_test_with_opts(identity, None)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unsequenced_reliable_data_buffered_while_joining() {
    // unsequenced reliable data, i. e. room service SendData, arriving before the
    // reliable data channel is writable should be held and replayed, not dropped
    {
        // buffers and replays
        let p = new_participant_for_test("test");
        assert!(
            !p.reliable_data_info
                .joining_message_lock
                .lock()
                .can_write_reliable
        );

        let empty = livekit::ParticipantId::default();
        assert_eq!(
            Ok(()),
            p.send_data_message(livekit::data_packet::Kind::Reliable, b"one", &empty, 0)
        );
        assert_eq!(
            Ok(()),
            p.send_data_message(livekit::data_packet::Kind::Reliable, b"two", &empty, 0)
        );

        {
            let joining = p.reliable_data_info.joining_message_lock.lock();
            assert_eq!(
                vec![b"one".to_vec(), b"two".to_vec()],
                joining.unsequenced_messages
            );
            assert_eq!(6, joining.unsequenced_bytes);
        }

        p.replay_joining_reliable_messages();

        let joining = p.reliable_data_info.joining_message_lock.lock();
        assert!(joining.can_write_reliable, "buffers and replays");
        assert!(
            joining.unsequenced_messages.is_empty(),
            "buffers and replays"
        );
        assert_eq!(0, joining.unsequenced_bytes, "buffers and replays");
    }

    {
        // does not buffer once writable
        let p = new_participant_for_test("test");
        p.replay_joining_reliable_messages();

        // no data channel in test, so the write through fails rather than getting buffered
        assert!(
            p.send_data_message(
                livekit::data_packet::Kind::Reliable,
                b"one",
                &livekit::ParticipantId::default(),
                0
            )
            .is_err(),
            "does not buffer once writable"
        );
        assert!(
            p.reliable_data_info
                .joining_message_lock
                .lock()
                .unsequenced_messages
                .is_empty(),
            "does not buffer once writable"
        );
    }

    {
        // bounded buffer
        let p = new_participant_for_test("test");

        let data = vec![0u8; C_MAX_JOINING_UNSEQUENCED_RELIABLE_BYTES];
        let empty = livekit::ParticipantId::default();
        assert_eq!(
            Ok(()),
            p.send_data_message(livekit::data_packet::Kind::Reliable, &data, &empty, 0)
        );
        assert!(
            p.send_data_message(livekit::data_packet::Kind::Reliable, b"overflow", &empty, 0)
                .is_err(),
            "bounded buffer"
        );

        let joining = p.reliable_data_info.joining_message_lock.lock();
        assert_eq!(1, joining.unsequenced_messages.len(), "bounded buffer");
        assert_eq!(1, joining.unsequenced_dropped, "bounded buffer");
    }

    {
        // lossy is not buffered
        let p = new_participant_for_test("test");

        assert!(
            p.send_data_message(
                livekit::data_packet::Kind::Lossy,
                b"one",
                &livekit::ParticipantId::default(),
                0
            )
            .is_err(),
            "lossy is not buffered"
        );
        assert!(
            p.reliable_data_info
                .joining_message_lock
                .lock()
                .unsequenced_messages
                .is_empty(),
            "lossy is not buffered"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_migrating_in_participant_waits_for_reconnect_response() {
    // a migrating in participant resumes on a ReconnectResponse, the client takes it only
    // as the first message on the resumed signal connection, so everything else waits
    let new_migrating_participant = || {
        let p = new_participant_for_test_with_opts(
            "test",
            Some(ParticipantOpts {
                migration: true,
                protocol_version: types::ProtocolVersion(17),
                client_info: Some(livekit::ClientInfo {
                    sdk: livekit::client_info::Sdk::Js as i32,
                    version: "2.15.2".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );
        let sink = fake_sink(&p);
        (p, sink)
    };

    {
        // not ready until the response is sent
        let name = "not ready until the response is sent";
        let (p, sink) = new_migrating_participant();
        assert!(!p.is_ready(), "{name}");

        assert_eq!(
            Ok(()),
            p.handle_reconnect_and_send_response(
                livekit::ReconnectReason::RrUnknown,
                &livekit::ReconnectResponse {
                    last_message_seq: 21,
                    ..Default::default()
                },
                &[],
            )
        );
        assert!(p.is_ready(), "{name}");

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let res = signal_response(sink.write_message_args_for_call(0));
        let Some(livekit::signal_response::Message::Reconnect(reconnect)) = res.message else {
            panic!("{name}: not a ReconnectResponse: {res:?}");
        };
        assert_eq!(21, reconnect.last_message_seq, "{name}");
    }

    {
        // the response goes out first
        let name = "the response goes out first";
        let (p, sink) = new_migrating_participant();

        // a room update while waiting is dropped, it is re-sent after the migration
        assert_eq!(
            Ok(()),
            p.send_room_update(&livekit::Room {
                name: "test".to_string(),
                ..Default::default()
            })
        );
        // participant updates while waiting are queued
        assert_eq!(
            Ok(()),
            p.send_participant_update(&[livekit::ParticipantInfo {
                sid: "PA_other".to_string(),
                identity: "other".to_string(),
                version: 1,
                ..Default::default()
            }])
        );
        assert_eq!(0, sink.write_message_call_count(), "{name}");

        assert_eq!(
            Ok(()),
            p.handle_reconnect_and_send_response(
                livekit::ReconnectReason::RrUnknown,
                &livekit::ReconnectResponse::default(),
                &[],
            )
        );

        assert_eq!(2, sink.write_message_call_count(), "{name}");
        let first = signal_response(sink.write_message_args_for_call(0));
        assert!(
            matches!(
                first.message,
                Some(livekit::signal_response::Message::Reconnect(_))
            ),
            "{name}"
        );
        let second = signal_response(sink.write_message_args_for_call(1));
        let Some(livekit::signal_response::Message::Update(update)) = second.message else {
            panic!("{name}: not an update: {second:?}");
        };
        assert_eq!(1, update.participants.len(), "{name}");
        assert_eq!("PA_other", update.participants[0].sid, "{name}");
    }

    {
        // a client that cannot handle the response is ready right away
        let name = "a client that cannot handle the response is ready right away";
        let p = new_participant_for_test_with_opts(
            "test",
            Some(ParticipantOpts {
                migration: true,
                protocol_version: types::ProtocolVersion(17),
                client_info: Some(livekit::ClientInfo {
                    sdk: livekit::client_info::Sdk::Js as i32,
                    version: "1.6.2".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );
        let sink = fake_sink(&p);

        assert_eq!(
            Ok(()),
            p.handle_reconnect_and_send_response(
                livekit::ReconnectReason::RrUnknown,
                &livekit::ReconnectResponse::default(),
                &[],
            )
        );
        assert!(p.is_ready(), "{name}");
        assert_eq!(0, sink.write_message_call_count(), "{name}");

        assert_eq!(
            Ok(()),
            p.send_room_update(&livekit::Room {
                name: "test".to_string(),
                ..Default::default()
            })
        );
        assert_eq!(1, sink.write_message_call_count(), "{name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_resumed_participant_waits_for_reconnect_response() {
    // a resumed connection has to open with the ReconnectResponse too, and the
    // participant is ready throughout, so the signaller holds messages back
    let new_resumed_participant = |version: &str| {
        let p = new_participant_for_test_with_opts(
            "test",
            Some(ParticipantOpts {
                protocol_version: types::ProtocolVersion(17),
                client_info: Some(livekit::ClientInfo {
                    sdk: livekit::client_info::Sdk::Js as i32,
                    version: version.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );
        assert!(p.is_ready());

        let sink = Arc::new(FakeMessageSink::default());
        p.swap_response_sink(Some(sink.clone()), types::SignallingCloseReason::Resume);
        (p, sink)
    };

    {
        // the response goes out first
        let name = "the response goes out first";
        let (p, sink) = new_resumed_participant("2.15.2");

        // a room update in this window is dropped, the resume path re-sends room state
        assert_eq!(
            Ok(()),
            p.send_room_update(&livekit::Room {
                name: "test".to_string(),
                ..Default::default()
            })
        );
        // participant updates are queued
        assert_eq!(
            Ok(()),
            p.send_participant_update(&[livekit::ParticipantInfo {
                sid: "PA_other".to_string(),
                identity: "other".to_string(),
                version: 1,
                ..Default::default()
            }])
        );
        assert_eq!(0, sink.write_message_call_count(), "{name}");

        assert_eq!(
            Ok(()),
            p.handle_reconnect_and_send_response(
                livekit::ReconnectReason::RrSignalDisconnected,
                &livekit::ReconnectResponse {
                    last_message_seq: 7,
                    ..Default::default()
                },
                &[],
            )
        );

        assert_eq!(2, sink.write_message_call_count(), "{name}");
        let first = signal_response(sink.write_message_args_for_call(0));
        let Some(livekit::signal_response::Message::Reconnect(reconnect)) = first.message else {
            panic!("{name}: not a ReconnectResponse: {first:?}");
        };
        assert_eq!(7, reconnect.last_message_seq, "{name}");
        let second = signal_response(sink.write_message_args_for_call(1));
        let Some(livekit::signal_response::Message::Update(update)) = second.message else {
            panic!("{name}: not an update: {second:?}");
        };
        assert_eq!("PA_other", update.participants[0].sid, "{name}");
    }

    {
        // a connection that opens without a response still delivers the queue
        let name = "a connection that opens without a response still delivers the queue";
        let (p, sink) = new_resumed_participant("2.15.2");

        assert_eq!(
            Ok(()),
            p.send_participant_update(&[livekit::ParticipantInfo {
                sid: "PA_other".to_string(),
                identity: "other".to_string(),
                version: 1,
                ..Default::default()
            }])
        );
        assert_eq!(0, sink.write_message_call_count(), "{name}");

        // no ReconnectResponse written, this is what the handshake window does on expiry
        p.signaller.open_handshake();

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let res = signal_response(sink.write_message_args_for_call(0));
        let Some(livekit::signal_response::Message::Update(update)) = res.message else {
            panic!("{name}: not an update: {res:?}");
        };
        assert_eq!("PA_other", update.participants[0].sid, "{name}");
    }

    {
        // a client that cannot handle the response is not held back
        let name = "a client that cannot handle the response is not held back";
        let (p, sink) = new_resumed_participant("1.6.2");

        assert_eq!(
            Ok(()),
            p.handle_reconnect_and_send_response(
                livekit::ReconnectReason::RrSignalDisconnected,
                &livekit::ReconnectResponse::default(),
                &[],
            )
        );
        assert_eq!(0, sink.write_message_call_count(), "{name}");

        assert_eq!(
            Ok(()),
            p.send_room_update(&livekit::Room {
                name: "test".to_string(),
                ..Default::default()
            })
        );
        assert_eq!(1, sink.write_message_call_count(), "{name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leave_join_session() {
    let p = new_participant_for_test("leave-join-session");
    // `p.params.LoggerResolver = zaputil.NoOpDeferrer{}` and the reporter's resolver: not
    // there (see the header)
    let tl = fakes::downcast::<FakeParticipantTelemetryListener>(p.get_telemetry_listener());

    let track = Arc::new(FakeLocalMediaTrack::default());
    track.id_returns("TR_test".into());
    track.published_returns(true);
    p.up_track_manager.add_published_track(track);

    // leaving reports the track unpublished, then runs the leave under the left session's
    // guard, and the next session gets a fresh one
    let prev_guard = p.telemetry_guard().unwrap();
    let left = Arc::new(AtomicBool::new(false));
    {
        let left = left.clone();
        let p2 = p.clone();
        let prev_guard = prev_guard.clone();
        let tl = tl.clone();
        p.leave_session(Some(Box::new(move || {
            left.store(true, Ordering::SeqCst);
            assert!(Arc::ptr_eq(&prev_guard, &p2.telemetry_guard().unwrap()));
            assert_eq!(1, tl.on_track_unpublished_call_count());
        })));
    }
    assert!(left.load(Ordering::SeqCst));
    assert!(!Arc::ptr_eq(&prev_guard, &p.telemetry_guard().unwrap()));
    assert_eq!(1, tl.on_track_unpublished_call_count());
    let (_, _, _, was_published, should_send) = tl.on_track_unpublished_args_for_call(0);
    assert!(was_published);
    assert!(should_send);

    // joining runs the join first, then reports the track published again
    let joined = Arc::new(AtomicBool::new(false));
    {
        let joined = joined.clone();
        let tl = tl.clone();
        p.join_session(Some(Box::new(move || {
            joined.store(true, Ordering::SeqCst);
            assert_eq!(0, tl.on_track_published_call_count());
        })));
    }
    assert!(joined.load(Ordering::SeqCst));
    assert_eq!(1, tl.on_track_published_call_count());
    let (_, _, _, should_send) = tl.on_track_published_args_for_call(0);
    assert!(should_send);

    // nil callbacks are fine
    p.leave_session(None);
    p.join_session(None);
    assert_eq!(2, tl.on_track_unpublished_call_count());
    assert_eq!(2, tl.on_track_published_call_count());
}

// ---- helpers of this file, new

// A node of its own, as a pion connection has sockets of its own.
fn new_node() -> Node {
    Node::new(NodeConfig::default()).unwrap()
}

// `webrtc.NewAPI(webrtc.WithMediaEngine(me)).NewPeerConnection(webrtc.Configuration{})`, on a
// node of its own.
fn new_peer_connection_with(me: MediaEngine) -> PeerConnection {
    PeerConnection::new(
        &new_node(),
        Configuration {
            media_engine: me,
            ..Default::default()
        },
    )
    .unwrap()
}

// `webrtc.NewPeerConnection(webrtc.Configuration{})`: pion's default codecs (see the header).
fn new_pion_default_peer_connection() -> PeerConnection {
    let codecs: Vec<livekit::Codec> = pion_default_codecs()
        .into_iter()
        .map(|mime_type| livekit::Codec {
            mime: mime_type.as_str().to_string(),
            ..Default::default()
        })
        .collect();
    new_peer_connection_with(
        create_media_engine(&codecs, &DirectionConfig::default(), false).unwrap(),
    )
}

// The codecs of pion's `RegisterDefaultCodecs` that `create_media_engine` registers, in pion's
// order (G722 is not among them).
fn pion_default_codecs() -> Vec<mime::MimeType> {
    vec![
        mime::MimeType::Opus,
        mime::MimeType::Pcmu,
        mime::MimeType::Pcma,
        mime::MimeType::Vp8,
        mime::MimeType::Vp9,
        mime::MimeType::H264,
        mime::MimeType::Av1,
        mime::MimeType::H265,
        mime::MimeType::Rtx,
    ]
}

// pion's offer of a sender with these encodings: its SSRCs left out, the rids of the layers and
// `a=simulcast` added to its (one) media section, as pion writes them.
fn as_simulcast_offer(offer: &SessionDescription, rids: &[&str]) -> SessionDescription {
    let mut sdp = String::new();
    for line in offer.sdp.lines() {
        if !line.starts_with("a=ssrc") {
            sdp.push_str(line);
            sdp.push_str("\r\n");
        }
    }
    for rid in rids {
        sdp.push_str(&format!("a=rid:{rid} send\r\n"));
    }
    sdp.push_str(&format!("a=simulcast:send {}\r\n", rids.join(";")));
    SessionDescription {
        r#type: SdpType::Offer,
        sdp,
    }
}

// `webrtc.NewTrackLocalStaticRTP(capability, id, streamID, webrtc.WithRTPStreamID(rid))`: binds
// to the first codec it is offered whose MIME type is its own.
struct StaticTrack {
    capability: RtpCodecCapability,
    id: String,
    stream_id: String,
    rid: String,
}

impl StaticTrack {
    fn new(
        capability: RtpCodecCapability,
        id: &str,
        stream_id: &str,
        rid: &str,
    ) -> Arc<StaticTrack> {
        Arc::new(StaticTrack {
            capability,
            id: id.to_string(),
            stream_id: stream_id.to_string(),
            rid: rid.to_string(),
        })
    }
}

impl webrtc::TrackLocal for StaticTrack {
    fn bind(&self, t: Arc<dyn TrackLocalContext>) -> Result<RtpCodecParameters, webrtc::Error> {
        t.codec_parameters()
            .into_iter()
            .find(|c| {
                c.rtp_codec_capability
                    .mime_type
                    .eq_ignore_ascii_case(&self.capability.mime_type)
            })
            .ok_or(webrtc::Error::UnsupportedCodec)
    }

    fn unbind(&self, _t: Arc<dyn TrackLocalContext>) -> Result<(), webrtc::Error> {
        Ok(())
    }

    fn id(&self) -> String {
        self.id.clone()
    }

    fn rid(&self) -> String {
        self.rid.clone()
    }

    fn stream_id(&self) -> String {
        self.stream_id.clone()
    }

    fn kind(&self) -> RtpCodecType {
        if mime::is_mime_type_string_audio(&self.capability.mime_type) {
            RtpCodecType::Audio
        } else {
            RtpCodecType::Video
        }
    }
}

// `p.params.Sink.(*routingfakes.FakeMessageSink)`
fn fake_sink(p: &ParticipantImpl) -> Arc<FakeMessageSink> {
    fakes::downcast::<FakeMessageSink>(p.params.sink.clone().unwrap())
}

// `p.listener().(*typesfakes.FakeLocalParticipantListener)`
fn fake_listener(p: &ParticipantImpl) -> Arc<FakeLocalParticipantListener> {
    fakes::downcast::<FakeLocalParticipantListener>(p.listener())
}

// `msg.(*livekit.SignalResponse)`
fn signal_response(msg: Message) -> livekit::SignalResponse {
    match msg {
        Message::SignalResponse(res) => *res,
        msg => panic!("not a SignalResponse: {msg:?}"),
    }
}

// what a `*livekit.SignalResponse` carries, `None` for another message
fn response_message(msg: &Message) -> Option<&livekit::signal_response::Message> {
    match msg {
        Message::SignalResponse(res) => res.message.as_ref(),
        _ => None,
    }
}

// `&livekit.AddTrackRequest{Cid: cid, Name: name, Type: livekit.TrackType_VIDEO}`
fn add_track_request(cid: &str, name: &str) -> livekit::AddTrackRequest {
    livekit::AddTrackRequest {
        cid: cid.to_string(),
        name: name.to_string(),
        r#type: livekit::TrackType::Video as i32,
        ..Default::default()
    }
}

// `p.pendingTracks[cid].trackInfos`
fn pending_track_infos(p: &ParticipantImpl, cid: &str) -> Vec<livekit::TrackInfo> {
    p.pending_tracks_lock.read().pending_tracks[cid]
        .track_infos
        .clone()
}

// `p.pendingTracks[cid] != nil`
fn has_pending_track(p: &ParticipantImpl, cid: &str) -> bool {
    p.pending_tracks_lock
        .read()
        .pending_tracks
        .contains_key(cid)
}

// `len(p.pendingTracks)`
fn num_pending_tracks(p: &ParticipantImpl) -> usize {
    p.pending_tracks_lock.read().pending_tracks.len()
}
