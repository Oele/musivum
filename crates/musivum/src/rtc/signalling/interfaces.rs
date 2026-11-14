// Ported from LiveKit, pkg/rtc/signalling/interfaces.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. A message is a `routing::Message`; one that may be
// nil, which the builders of `ParticipantSignalling` return and `WriteMessage` takes, is an
// `Option`. The builders take what they build a message from by value: it goes into the
// message. `WriteMessage` returns the error of the sink it writes to, a `routing::Error`.
// New: every method has a default body, which is what the method of the same name does in
// the original's `signallingUnimplemented`, `signalhandlerUnimplemented` and
// `signallerUnimplemented`. Go embeds one of those in a struct to get the methods the struct
// does not define itself; here an implementation leaves those methods out.

use std::sync::Arc;

use super::errors::Error;
use crate::protocol::livekit;
use crate::routing::{self, Message, MessageSink};
use crate::rtc::types::SignallingCloseReason;

#[allow(unused_variables)]
pub trait ParticipantSignalHandler: Send + Sync {
    fn handle_message(&self, msg: Message) -> Result<(), Error> {
        Ok(())
    }
}

#[allow(unused_variables)]
pub trait ParticipantSignaller: Send + Sync {
    fn swap_response_sink(
        &self,
        sink: Option<Arc<dyn MessageSink>>,
        reason: SignallingCloseReason,
    ) {
    }
    fn get_response_sink(&self) -> Option<Arc<dyn MessageSink>> {
        None
    }
    fn close_signal_connection(&self, reason: SignallingCloseReason) {}

    // HandshakePending reports whether a resumed connection is still waiting for the
    // ReconnectResponse it has to open with
    fn handshake_pending(&self) -> bool {
        false
    }
    // OpenHandshake lets messages flow on a resumed connection that does not open with
    // a ReconnectResponse
    fn open_handshake(&self) {}

    fn write_message(&self, msg: Option<Message>) -> Result<(), routing::Error> {
        Ok(())
    }
}

#[allow(unused_variables)]
pub trait ParticipantSignalling: Send + Sync {
    fn signal_join_response(&self, join: livekit::JoinResponse) -> Option<Message> {
        None
    }
    fn signal_participant_update(
        &self,
        participants: Vec<livekit::ParticipantInfo>,
    ) -> Option<Message> {
        None
    }
    fn signal_speaker_update(&self, speakers: Vec<livekit::SpeakerInfo>) -> Option<Message> {
        None
    }
    fn signal_room_update(&self, room: livekit::Room) -> Option<Message> {
        None
    }
    fn signal_connection_quality_update(
        &self,
        connection_quality: livekit::ConnectionQualityUpdate,
    ) -> Option<Message> {
        None
    }
    fn signal_refresh_token(&self, token: String) -> Option<Message> {
        None
    }
    fn signal_request_response(
        &self,
        request_response: livekit::RequestResponse,
    ) -> Option<Message> {
        None
    }
    fn signal_room_moved_response(
        &self,
        room_moved: livekit::RoomMovedResponse,
    ) -> Option<Message> {
        None
    }
    fn signal_reconnect_response(&self, reconnect: livekit::ReconnectResponse) -> Option<Message> {
        None
    }
    fn signal_ice_candidate(&self, trickle: livekit::TrickleRequest) -> Option<Message> {
        None
    }
    fn signal_track_muted(&self, mute: livekit::MuteTrackRequest) -> Option<Message> {
        None
    }
    fn signal_track_published(
        &self,
        track_published: livekit::TrackPublishedResponse,
    ) -> Option<Message> {
        None
    }
    fn signal_track_unpublished(
        &self,
        track_unpublished: livekit::TrackUnpublishedResponse,
    ) -> Option<Message> {
        None
    }
    fn signal_track_subscribed(
        &self,
        track_subscribed: livekit::TrackSubscribed,
    ) -> Option<Message> {
        None
    }
    fn signal_leave_request(&self, leave: livekit::LeaveRequest) -> Option<Message> {
        None
    }
    fn signal_sdp_answer(&self, answer: livekit::SessionDescription) -> Option<Message> {
        None
    }
    fn signal_sdp_offer(&self, offer: livekit::SessionDescription) -> Option<Message> {
        None
    }
    fn signal_stream_state_update(
        &self,
        stream_state_update: livekit::StreamStateUpdate,
    ) -> Option<Message> {
        None
    }
    fn signal_subscribed_quality_update(
        &self,
        subscribed_quality_update: livekit::SubscribedQualityUpdate,
    ) -> Option<Message> {
        None
    }
    fn signal_subscription_response(
        &self,
        subscription_response: livekit::SubscriptionResponse,
    ) -> Option<Message> {
        None
    }
    fn signal_subscription_permission_update(
        &self,
        subscription_permission_update: livekit::SubscriptionPermissionUpdate,
    ) -> Option<Message> {
        None
    }
    fn signal_media_sections_requirement(
        &self,
        media_sections_requirement: livekit::MediaSectionsRequirement,
    ) -> Option<Message> {
        None
    }
    fn signal_subscribed_audio_codec_update(
        &self,
        subscribed_audio_codec_update: livekit::SubscribedAudioCodecUpdate,
    ) -> Option<Message> {
        None
    }
    fn signal_publish_data_track_response(
        &self,
        publish_data_track_response: livekit::PublishDataTrackResponse,
    ) -> Option<Message> {
        None
    }
    fn signal_unpublish_data_track_response(
        &self,
        unpublish_data_track_response: livekit::UnpublishDataTrackResponse,
    ) -> Option<Message> {
        None
    }
    fn signal_data_track_subscriber_handles(
        &self,
        data_track_subscriber_handles: livekit::DataTrackSubscriberHandles,
    ) -> Option<Message> {
        None
    }
    fn signal_store_data_blob_response(
        &self,
        store_data_blob_response: livekit::StoreDataBlobResponse,
    ) -> Option<Message> {
        None
    }
    fn signal_get_data_blob_response(
        &self,
        get_data_blob_response: livekit::GetDataBlobResponse,
    ) -> Option<Message> {
        None
    }
}
