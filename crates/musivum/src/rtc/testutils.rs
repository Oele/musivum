// Ported from LiveKit, pkg/rtc/testutils.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; compiled for tests only. The stubs that tell the
// listener hold the fake weakly: the fake keeps its stubs, and a stub that held the fake
// would keep it alive. `GetReporterReturns` is left out with the reporter (the trait has no
// `GetReporter` here).

use std::sync::Arc;

use crate::protocol::livekit;
use crate::protocol::logger;
use crate::protocol::utils;
use crate::protocol::utils::guid;
use crate::rtc::types;
use crate::sfu::connectionquality;

pub fn new_mock_participant(
    identity: livekit::ParticipantIdentity,
    protocol: types::ProtocolVersion,
    hidden: bool,
    publisher: bool,
    participant_listener: Arc<dyn types::LocalParticipantListener>,
) -> Arc<types::FakeLocalParticipant> {
    let p = Arc::new(types::FakeLocalParticipant::default());
    let sid = guid::new(utils::PARTICIPANT_PREFIX);
    p.id_returns(livekit::ParticipantId::from(&sid));
    p.identity_returns(identity.clone());
    p.state_returns(livekit::participant_info::State::Joined);
    p.protocol_version_returns(protocol);
    p.can_subscribe_returns(true);
    p.can_publish_source_returns(!hidden);
    p.can_publish_data_returns(!hidden);
    p.hidden_returns(hidden);
    p.to_proto_returns(livekit::ParticipantInfo {
        sid: sid.clone(),
        identity: identity.as_str().to_string(),
        state: livekit::participant_info::State::Joined as i32,
        is_publisher: publisher,
        ..Default::default()
    });
    p.to_proto_with_version_returns((
        livekit::ParticipantInfo {
            sid: sid.clone(),
            identity: identity.as_str().to_string(),
            state: livekit::participant_info::State::Joined as i32,
            is_publisher: publisher,
            ..Default::default()
        },
        utils::TimedVersion(0),
    ));
    p.get_connection_quality_returns(livekit::ConnectionQualityInfo {
        participant_sid: sid,
        quality: livekit::ConnectionQuality::Excellent as i32,
        score: connectionquality::MAX_MOS,
    });

    {
        let p_weak = Arc::downgrade(&p);
        let participant_listener = participant_listener.clone();
        p.set_metadata_calls(move |_m| {
            if let Some(p) = p_weak.upgrade() {
                participant_listener.on_participant_update(p);
            }
        });
    }
    let update_track = {
        let p_weak = Arc::downgrade(&p);
        move || {
            if let Some(p) = p_weak.upgrade() {
                participant_listener
                    .on_track_updated(p, new_mock_track(livekit::TrackType::Video, "testcam"));
            }
        }
    };
    let update_track = Arc::new(update_track);

    {
        let update_track = update_track.clone();
        p.set_track_muted_calls(move |_mute, _from_server| {
            update_track();
            None
        });
    }
    p.add_track_calls(move |_req| {
        update_track();
    });
    p.get_logger_returns(logger::get_logger());

    p
}

pub fn new_mock_track(kind: livekit::TrackType, name: &str) -> Arc<types::FakeMediaTrack> {
    let t = Arc::new(types::FakeMediaTrack::default());
    t.id_returns(livekit::TrackId::from(guid::new(utils::TRACK_PREFIX)));
    t.kind_returns(kind);
    t.name_returns(name.to_string());
    t.to_proto_returns(livekit::TrackInfo {
        r#type: kind as i32,
        name: name.to_string(),
        ..Default::default()
    });
    t
}

// New for musivum: the helpers above are used by most tests of the package, so they get one.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtc::types::{LocalParticipant, Participant};

    #[test]
    fn a_mock_participant_answers_and_tells_its_listener() {
        let listener = Arc::new(types::FakeLocalParticipantListener::default());
        let p = new_mock_participant(
            "alice".into(),
            types::ProtocolVersion(types::CURRENT_PROTOCOL),
            false,
            true,
            listener.clone(),
        );

        assert_eq!("alice", p.identity().as_str());
        assert!(p.id().as_str().starts_with(utils::PARTICIPANT_PREFIX));
        assert_eq!(p.id().as_str(), p.to_proto().sid);
        assert!(p.to_proto().is_publisher);
        assert!(p.can_publish_data() && !p.hidden());

        // a hidden one publishes nothing
        let hidden = new_mock_participant(
            "bob".into(),
            types::ProtocolVersion(types::CURRENT_PROTOCOL),
            true,
            false,
            listener.clone(),
        );
        assert!(hidden.hidden() && !hidden.can_publish_data());
        assert!(!hidden.can_publish_source(livekit::TrackSource::Camera));

        assert_eq!(0, listener.on_participant_update_call_count());
        p.set_metadata("meta");
        assert_eq!(1, listener.on_participant_update_call_count());

        assert_eq!(0, listener.on_track_updated_call_count());
        p.add_track(&livekit::AddTrackRequest::default());
        assert!(
            p.set_track_muted(&livekit::MuteTrackRequest::default(), false)
                .is_none()
        );
        assert_eq!(2, listener.on_track_updated_call_count());
        let (_, track) = listener.on_track_updated_args_for_call(0);
        assert_eq!("testcam", track.name());
        assert_eq!(livekit::TrackType::Video, track.kind());

        // The stubs do not keep the fake alive. (With a listener that records what it is
        // told, as the one above, the listener does: it holds the participants it was given.)
        let p = new_mock_participant(
            "carol".into(),
            types::ProtocolVersion(types::CURRENT_PROTOCOL),
            false,
            true,
            Arc::new(types::NullLocalParticipantListener::default()),
        );
        p.set_metadata("meta");
        p.add_track(&livekit::AddTrackRequest::default());
        let weak = Arc::downgrade(&p);
        drop(p);
        assert!(weak.upgrade().is_none());
    }
}
