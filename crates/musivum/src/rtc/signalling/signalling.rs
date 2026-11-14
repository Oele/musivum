// Ported from LiveKit, pkg/rtc/signalling/signalling.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. A message is a `routing::Message`, and nil, which the
// builders of the lists return for an empty list, is `None`. What a message is built from is
// taken by value and goes into it. `NewSignalling` returns the signalling in an `Arc`, not as
// the interface.

use std::sync::Arc;

use super::interfaces::ParticipantSignalling;
use crate::protocol::livekit::{self, signal_response};
use crate::protocol::logger::Logger;
use crate::routing::Message;

#[derive(Clone, Debug, Default)]
pub struct SignallingParams {
    pub logger: Logger,
}

pub struct Signalling {
    // Not read, as in the original.
    #[allow(dead_code)]
    params: SignallingParams,
}

impl Signalling {
    pub fn new(params: SignallingParams) -> Arc<Self> {
        Arc::new(Self { params })
    }
}

impl ParticipantSignalling for Signalling {
    fn signal_join_response(&self, join: livekit::JoinResponse) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::Join(join)),
        }))
    }

    fn signal_participant_update(
        &self,
        participants: Vec<livekit::ParticipantInfo>,
    ) -> Option<Message> {
        if participants.is_empty() {
            return None;
        }

        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::Update(
                livekit::ParticipantUpdate { participants },
            )),
        }))
    }

    fn signal_speaker_update(&self, speakers: Vec<livekit::SpeakerInfo>) -> Option<Message> {
        if speakers.is_empty() {
            return None;
        }

        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::SpeakersChanged(
                livekit::SpeakersChanged { speakers },
            )),
        }))
    }

    fn signal_room_update(&self, room: livekit::Room) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::RoomUpdate(livekit::RoomUpdate {
                room: Some(room),
            })),
        }))
    }

    fn signal_connection_quality_update(
        &self,
        connection_quality: livekit::ConnectionQualityUpdate,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::ConnectionQuality(
                connection_quality,
            )),
        }))
    }

    fn signal_refresh_token(&self, token: String) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::RefreshToken(token)),
        }))
    }

    fn signal_request_response(
        &self,
        request_response: livekit::RequestResponse,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::RequestResponse(request_response)),
        }))
    }

    fn signal_room_moved_response(
        &self,
        room_moved: livekit::RoomMovedResponse,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::RoomMoved(room_moved)),
        }))
    }

    fn signal_reconnect_response(&self, reconnect: livekit::ReconnectResponse) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::Reconnect(reconnect)),
        }))
    }

    fn signal_ice_candidate(&self, trickle: livekit::TrickleRequest) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::Trickle(trickle)),
        }))
    }

    fn signal_track_muted(&self, mute: livekit::MuteTrackRequest) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::Mute(mute)),
        }))
    }

    fn signal_track_published(
        &self,
        track_published: livekit::TrackPublishedResponse,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::TrackPublished(track_published)),
        }))
    }

    fn signal_track_unpublished(
        &self,
        track_unpublished: livekit::TrackUnpublishedResponse,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::TrackUnpublished(
                track_unpublished,
            )),
        }))
    }

    fn signal_track_subscribed(
        &self,
        track_subscribed: livekit::TrackSubscribed,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::TrackSubscribed(track_subscribed)),
        }))
    }

    fn signal_leave_request(&self, leave: livekit::LeaveRequest) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::Leave(leave)),
        }))
    }

    fn signal_sdp_answer(&self, answer: livekit::SessionDescription) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::Answer(answer)),
        }))
    }

    fn signal_sdp_offer(&self, offer: livekit::SessionDescription) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::Offer(offer)),
        }))
    }

    fn signal_stream_state_update(
        &self,
        stream_state_update: livekit::StreamStateUpdate,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::StreamStateUpdate(
                stream_state_update,
            )),
        }))
    }

    fn signal_subscribed_quality_update(
        &self,
        subscribed_quality_update: livekit::SubscribedQualityUpdate,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::SubscribedQualityUpdate(
                subscribed_quality_update,
            )),
        }))
    }

    fn signal_subscription_response(
        &self,
        subscription_response: livekit::SubscriptionResponse,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::SubscriptionResponse(
                subscription_response,
            )),
        }))
    }

    fn signal_subscription_permission_update(
        &self,
        subscription_permission_update: livekit::SubscriptionPermissionUpdate,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::SubscriptionPermissionUpdate(
                subscription_permission_update,
            )),
        }))
    }

    fn signal_media_sections_requirement(
        &self,
        media_sections_requirement: livekit::MediaSectionsRequirement,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::MediaSectionsRequirement(
                media_sections_requirement,
            )),
        }))
    }

    fn signal_subscribed_audio_codec_update(
        &self,
        subscribed_audio_codec_update: livekit::SubscribedAudioCodecUpdate,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::SubscribedAudioCodecUpdate(
                subscribed_audio_codec_update,
            )),
        }))
    }

    fn signal_publish_data_track_response(
        &self,
        publish_data_track_response: livekit::PublishDataTrackResponse,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::PublishDataTrackResponse(
                publish_data_track_response,
            )),
        }))
    }

    fn signal_unpublish_data_track_response(
        &self,
        unpublish_data_track_response: livekit::UnpublishDataTrackResponse,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::UnpublishDataTrackResponse(
                unpublish_data_track_response,
            )),
        }))
    }

    fn signal_data_track_subscriber_handles(
        &self,
        data_track_subscriber_handles: livekit::DataTrackSubscriberHandles,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::DataTrackSubscriberHandles(
                data_track_subscriber_handles,
            )),
        }))
    }

    fn signal_store_data_blob_response(
        &self,
        store_data_blob_response: livekit::StoreDataBlobResponse,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::StoreDataBlobResponse(
                store_data_blob_response,
            )),
        }))
    }

    fn signal_get_data_blob_response(
        &self,
        get_data_blob_response: livekit::GetDataBlobResponse,
    ) -> Option<Message> {
        Some(Message::from(livekit::SignalResponse {
            message: Some(signal_response::Message::GetDataBlobResponse(
                get_data_blob_response,
            )),
        }))
    }
}

#[cfg(test)]
#[path = "signalling_test.rs"]
mod signalling_test;
