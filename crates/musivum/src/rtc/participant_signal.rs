// Ported from LiveKit, pkg/rtc/participant_signal.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The methods are those of `ParticipantImpl`
// (participant.rs), in an `impl` block of their own; the traits it implements hand on to them.
// A message is built from a copy of what the method is given (the builders of
// `ParticipantSignalling` take it by value), and what `WriteMessage` returns, an error of the
// sink, is passed on as `Error::Routing`. The update cache is the participant's (`UpdateCache`
// in participant.rs) and is behind `updateLock`, as upstream uses it, `VerifySubscribeParticipantInfo`
// apart (see participant.rs). `sendICECandidate` returns the error of the connection layer where
// pion's `ToJSON` cannot fail. A participant update is logged by the `Debug` of the cache's
// entry, which writes what its `String()` writes upstream.

use std::collections::HashMap;
use std::sync::Arc;

use super::participant::{ParticipantImpl, ParticipantUpdateInfo};
use super::types;
use crate::go::time::{self, Duration};
use crate::protocol::livekit;
use crate::protocol::signalling as protosignalling;
use crate::routing::MessageSink;
use crate::rtc::Error;
use crate::webrtc;
use crate::webrtc::peerconnection::{IceCandidate, SessionDescription};
use crate::{debugw, errorw, warnw};

impl ParticipantImpl {
    pub fn swap_response_sink(
        &self,
        sink: Option<Arc<dyn MessageSink>>,
        reason: types::SignallingCloseReason,
    ) {
        self.signaller.swap_response_sink(sink, reason);
    }

    pub fn get_response_sink(&self) -> Option<Arc<dyn MessageSink>> {
        self.signaller.get_response_sink()
    }

    pub fn close_signal_connection(&self, reason: types::SignallingCloseReason) {
        self.signaller.close_signal_connection(reason);
    }

    pub fn send_join_response(&self, join_response: &livekit::JoinResponse) -> Result<(), Error> {
        // keep track of participant updates and versions
        {
            let mut updates = self.update_lock.lock();
            for op in &join_response.other_participants {
                updates.update_cache.add(
                    livekit::ParticipantId::from(op.sid.as_str()),
                    ParticipantUpdateInfo {
                        identity: livekit::ParticipantIdentity::from(op.identity.as_str()),
                        version: op.version,
                        state: op.state(),
                        updated_at: time::now(),
                    },
                );
            }
        }

        // send Join response
        self.signaller
            .write_message(self.signalling.signal_join_response(join_response.clone()))?;

        // update state after sending message, so that no participant updates could slip through before JoinResponse is sent
        let queued_updates = {
            let mut updates = self.update_lock.lock();
            if self.state() == livekit::participant_info::State::Joining {
                self.update_state(livekit::participant_info::State::Joined);
            }
            std::mem::take(&mut updates.queued_updates)
        };

        if !queued_updates.is_empty() {
            return self.send_participant_update(&queued_updates);
        }

        Ok(())
    }

    pub fn send_participant_update(
        &self,
        participants_to_update: &[livekit::ParticipantInfo],
    ) -> Result<(), Error> {
        let valid_updates = {
            let mut updates = self.update_lock.lock();
            if self.is_disconnected() {
                return Ok(());
            }

            // read under the lock the flush takes, so an update either queues before the flush
            // or goes out after the connection has opened
            if !self.is_ready() || self.signaller.handshake_pending() {
                // queue up updates
                updates
                    .queued_updates
                    .extend_from_slice(participants_to_update);
                return Ok(());
            }
            let mut valid_updates = Vec::with_capacity(participants_to_update.len());
            for pi in participants_to_update {
                let mut is_valid = true;
                let p_id = livekit::ParticipantId::from(pi.sid.as_str());
                if let Some(last_version) = updates.update_cache.get(&p_id) {
                    // this is a message delivered out of order, a more recent version of the message had already been
                    // sent.
                    if pi.version < last_version.version {
                        debugw!(
                            self.params.logger,
                            "skipping outdated participant update",
                            "otherParticipant" => pi.identity,
                            "otherPID" => pi.sid,
                            "version" => pi.version,
                            "lastVersion" => last_version,
                        );
                        is_valid = false;
                    }
                }
                if pi.permission.as_ref().is_some_and(|p| p.hidden) && pi.sid != self.id().as_str()
                {
                    debugw!(self.params.logger, "skipping hidden participant update", "otherParticipant" => pi.identity);
                    is_valid = false;
                }
                if is_valid {
                    updates.update_cache.add(
                        p_id,
                        ParticipantUpdateInfo {
                            identity: livekit::ParticipantIdentity::from(pi.identity.as_str()),
                            version: pi.version,
                            state: pi.state(),
                            updated_at: time::now(),
                        },
                    );
                    valid_updates.push(pi.clone());
                }
            }
            valid_updates
        };

        Ok(self
            .signaller
            .write_message(self.signalling.signal_participant_update(valid_updates))?)
    }

    // SendSpeakerUpdate notifies participant changes to speakers. only send members that have changed since last update
    pub fn send_speaker_update(
        &self,
        speakers: &[livekit::SpeakerInfo],
        force: bool,
    ) -> Result<(), Error> {
        if !self.is_ready() {
            return Ok(());
        }

        let mut scoped_speakers = Vec::new();
        if force {
            scoped_speakers = speakers.to_vec();
        } else {
            for s in speakers {
                let participant_id = livekit::ParticipantId::from(s.sid.as_str());
                if self.subscription_manager.is_subscribed_to(&participant_id)
                    || participant_id == self.id()
                {
                    scoped_speakers.push(s.clone());
                }
            }
        }

        Ok(self
            .signaller
            .write_message(self.signalling.signal_speaker_update(scoped_speakers))?)
    }

    pub fn send_room_update(&self, room: &livekit::Room) -> Result<(), Error> {
        Ok(self
            .signaller
            .write_message(self.signalling.signal_room_update(room.clone()))?)
    }

    pub fn send_connection_quality_update(
        &self,
        update: &livekit::ConnectionQualityUpdate,
    ) -> Result<(), Error> {
        Ok(self.signaller.write_message(
            self.signalling
                .signal_connection_quality_update(update.clone()),
        )?)
    }

    pub fn send_refresh_token(&self, token: &str) -> Result<(), Error> {
        Ok(self
            .signaller
            .write_message(self.signalling.signal_refresh_token(token.to_string()))?)
    }

    pub(super) fn send_request_response(
        &self,
        request_response: livekit::RequestResponse,
    ) -> Result<(), Error> {
        if !self.params.client_info.supports_request_response() {
            return Ok(());
        }

        if request_response.reason == livekit::request_response::Reason::Ok as i32
            && !self.protocol_version().supports_non_error_signal_response()
        {
            return Ok(());
        }

        Ok(self
            .signaller
            .write_message(self.signalling.signal_request_response(request_response))?)
    }

    pub fn send_room_moved_response(
        &self,
        room_moved_response: &livekit::RoomMovedResponse,
    ) -> Result<(), Error> {
        Ok(self.signaller.write_message(
            self.signalling
                .signal_room_moved_response(room_moved_response.clone()),
        )?)
    }

    pub fn handle_reconnect_and_send_response(
        &self,
        reconnect_reason: livekit::ReconnectReason,
        reconnect_response: &livekit::ReconnectResponse,
        recently_disconnected_participants: &[livekit::ParticipantInfo], // when migrating in, these are the participants disconnected received from migrate out node
    ) -> Result<(), Error> {
        self.transport_manager
            .handle_client_reconnect(reconnect_reason);

        if !self.params.client_info.can_handle_reconnect_response() {
            // no ReconnectResponse opens this connection, so nothing to hold back
            return self.reconnect_response_sent_and_flush();
        }

        // send reconnect response
        let result = self.signaller.write_message(
            self.signalling
                .signal_reconnect_response(reconnect_response.clone()),
        );

        // mark sent after sending the message, so that nothing could slip through before
        // ReconnectResponse is sent. Marked on a failed write too: the sink is gone in that
        // case, and leaving it unmarked would hold back every message after it.
        let flush_result = self.reconnect_response_sent_and_flush();
        result?;
        flush_result?;

        if self.params.protocol_version.supports_disconnected_update() {
            if !recently_disconnected_participants.is_empty() {
                self.signaller.write_message(
                    self.signalling
                        .signal_participant_update(recently_disconnected_participants.to_vec()),
                )?;
            }
            return self.send_disconnect_updates_for_reconnect();
        }

        Ok(())
    }

    // reconnectResponseSentAndFlush makes a migrating in participant ready and sends what was
    // queued up while the connection was waiting for its ReconnectResponse.
    fn reconnect_response_sent_and_flush(&self) -> Result<(), Error> {
        // a successful write opens the connection by itself, this covers the paths that do
        // not write one, i. e. a client that cannot handle it and a failed write
        self.signaller.open_handshake();

        let queued_updates = {
            let mut updates = self.update_lock.lock();
            self.reconnect_response_sent.store(true);
            std::mem::take(&mut updates.queued_updates)
        };

        if !queued_updates.is_empty() {
            return self.send_participant_update(&queued_updates);
        }

        Ok(())
    }

    // flushQueuedUpdates sends the updates queued while the connection was closed for the
    // handshake. Called when the connection opens, including when it opens without a
    // ReconnectResponse having been written.
    pub(super) fn flush_queued_updates(&self) {
        let queued_updates = std::mem::take(&mut self.update_lock.lock().queued_updates);

        if queued_updates.is_empty() {
            return;
        }

        if let Err(err) = self.send_participant_update(&queued_updates) {
            warnw!(
                self.params.logger,
                "could not send queued participant updates",
                err
            );
        }
    }

    fn send_disconnect_updates_for_reconnect(&self) -> Result<(), Error> {
        let recently_disconnected_participants = self.get_recently_disconnected_participants();
        if !recently_disconnected_participants.is_empty() {
            return Ok(self.signaller.write_message(
                self.signalling
                    .signal_participant_update(recently_disconnected_participants),
            )?);
        }
        Ok(())
    }

    pub fn get_recently_disconnected_participants(&self) -> Vec<livekit::ParticipantInfo> {
        // look back a little more than last signal receive time as WebSocket close
        // on client side might have flushed messages and client application may have
        // lost connectivity earlier.
        let last_signal_at = self
            .transport_manager
            .last_seen_signal_at()
            .add(Duration::from_secs(-3));
        let mut recently_disconnected_participants = Vec::new();
        let updates = self.update_lock.lock();
        let keys = updates.update_cache.keys();
        for key in keys.iter().rev() {
            // Peek, as Get would move each entry to the front and reverse the eviction order
            if let Some(info) = updates.update_cache.peek(key) {
                if info.updated_at.before(last_signal_at) {
                    continue;
                }

                if info.state == livekit::participant_info::State::Disconnected {
                    recently_disconnected_participants.push(livekit::ParticipantInfo {
                        sid: key.as_str().to_string(),
                        identity: info.identity.as_str().to_string(),
                        version: info.version,
                        state: livekit::participant_info::State::Disconnected as i32,
                        ..Default::default()
                    });
                }
            }
        }
        drop(updates);

        recently_disconnected_participants
    }

    pub(super) fn send_ice_candidate(
        &self,
        ic: Option<&IceCandidate>,
        target: livekit::SignalTarget,
    ) -> Result<(), Error> {
        let prev_ic = std::mem::replace(&mut *self.ic_queue[target as usize].lock(), ic.cloned());
        let Some(prev_ic) = prev_ic else {
            return Ok(());
        };

        let trickle = protosignalling::to_proto_trickle(
            &prev_ic.to_json().map_err(webrtc::Error::from)?,
            target,
            ic.is_none(),
        );
        debugw!(self.params.logger, "sending ICE candidate", "transport" => target, "trickle" => trickle);

        Ok(self
            .signaller
            .write_message(self.signalling.signal_ice_candidate(trickle))?)
    }

    pub(super) fn send_track_muted(&self, track_id: &livekit::TrackId, muted: bool) {
        let _ = self
            .signaller
            .write_message(
                self.signalling
                    .signal_track_muted(livekit::MuteTrackRequest {
                        sid: track_id.as_str().to_string(),
                        muted,
                    }),
            );
    }

    pub(super) fn send_track_published(
        &self,
        cid: &str,
        ti: &livekit::TrackInfo,
    ) -> Result<(), Error> {
        debugw!(self.pub_logger, "sending track published", "cid" => cid, "trackInfo" => ti);
        Ok(self
            .signaller
            .write_message(self.signalling.signal_track_published(
                livekit::TrackPublishedResponse {
                    cid: cid.to_string(),
                    track: Some(ti.clone()),
                },
            ))?)
    }

    pub(super) fn send_track_unpublished(&self, track_id: &livekit::TrackId) {
        let _ = self
            .signaller
            .write_message(self.signalling.signal_track_unpublished(
                livekit::TrackUnpublishedResponse {
                    track_sid: track_id.as_str().to_string(),
                },
            ));
    }

    pub(super) fn send_track_has_been_subscribed(&self, track_id: &livekit::TrackId) {
        if !self.params.client_info.supports_track_subscribed_event() {
            return;
        }
        let _ = self
            .signaller
            .write_message(
                self.signalling
                    .signal_track_subscribed(livekit::TrackSubscribed {
                        track_sid: track_id.as_str().to_string(),
                    }),
            );
        debugw!(self.params.logger, "track has been subscribed", "trackID" => track_id);
    }

    pub(super) fn send_leave_request(
        &self,
        reason: types::ParticipantCloseReason,
        is_expected_to_resume: bool,
        is_expected_to_reconnect: bool,
        send_only_if_supporting_leave_request_with_action: bool,
    ) -> Result<(), Error> {
        let mut leave: Option<livekit::LeaveRequest> = None;
        if self.protocol_version().supports_regions_in_leave_request() {
            let mut l = livekit::LeaveRequest {
                reason: reason.to_disconnect_reason() as i32,
                ..Default::default()
            };
            let action = if is_expected_to_resume {
                livekit::leave_request::Action::Resume
            } else if is_expected_to_reconnect {
                livekit::leave_request::Action::Reconnect
            } else {
                livekit::leave_request::Action::Disconnect
            };
            l.action = action as i32;
            if action != livekit::leave_request::Action::Disconnect {
                // sending region settings even for RESUME just in case client wants to a full reconnect despite server saying RESUME
                let address = self
                    .params
                    .client_info
                    .client_info
                    .as_ref()
                    .map(|c| c.address.clone())
                    .unwrap_or_default();
                l.regions = self.helper().get_region_settings(&address);
            }
            leave = Some(l);
        } else if !send_only_if_supporting_leave_request_with_action {
            leave = Some(livekit::LeaveRequest {
                can_reconnect: is_expected_to_reconnect,
                reason: reason.to_disconnect_reason() as i32,
                ..Default::default()
            });
        }
        if let Some(leave) = leave {
            return Ok(self
                .signaller
                .write_message(self.signalling.signal_leave_request(leave))?);
        }

        Ok(())
    }

    pub(super) fn send_sdp_answer(
        &self,
        answer: SessionDescription,
        answer_id: u32,
        mid_to_track_id: HashMap<String, String>,
    ) -> Result<(), Error> {
        // `ToProtoSessionDescription` gives nil for an empty description, which the builder
        // takes as an empty one
        let answer =
            protosignalling::to_proto_session_description(answer, answer_id, mid_to_track_id)
                .unwrap_or_default();
        Ok(self
            .signaller
            .write_message(self.signalling.signal_sdp_answer(answer))?)
    }

    pub(super) fn send_sdp_offer(
        &self,
        offer: SessionDescription,
        offer_id: u32,
        mid_to_track_id: HashMap<String, String>,
    ) -> Result<(), Error> {
        let offer = protosignalling::to_proto_session_description(offer, offer_id, mid_to_track_id)
            .unwrap_or_default();
        Ok(self
            .signaller
            .write_message(self.signalling.signal_sdp_offer(offer))?)
    }

    pub(super) fn send_stream_state_update(
        &self,
        stream_state_update: livekit::StreamStateUpdate,
    ) -> Result<(), Error> {
        Ok(self.signaller.write_message(
            self.signalling
                .signal_stream_state_update(stream_state_update),
        )?)
    }

    pub(super) fn send_subscribed_quality_update(
        &self,
        subscribed_quality_update: livekit::SubscribedQualityUpdate,
    ) -> Result<(), Error> {
        Ok(self.signaller.write_message(
            self.signalling
                .signal_subscribed_quality_update(subscribed_quality_update),
        )?)
    }

    pub(super) fn send_subscribed_audio_codec_update(
        &self,
        subscribed_audio_codec_update: livekit::SubscribedAudioCodecUpdate,
    ) -> Result<(), Error> {
        Ok(self.signaller.write_message(
            self.signalling
                .signal_subscribed_audio_codec_update(subscribed_audio_codec_update),
        )?)
    }

    pub(super) fn send_subscription_response(
        &self,
        track_id: &livekit::TrackId,
        sub_err: livekit::SubscriptionError,
    ) -> Result<(), Error> {
        Ok(self
            .signaller
            .write_message(self.signalling.signal_subscription_response(
                livekit::SubscriptionResponse {
                    track_sid: track_id.as_str().to_string(),
                    err: sub_err as i32,
                },
            ))?)
    }

    pub fn send_subscription_permission_update(
        &self,
        publisher_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        allowed: bool,
    ) -> Result<(), Error> {
        debugw!(self.sub_logger, "sending subscription permission update", "publisherID" => publisher_id, "trackID" => track_id, "allowed" => allowed);
        let result =
            self.signaller
                .write_message(self.signalling.signal_subscription_permission_update(
                    livekit::SubscriptionPermissionUpdate {
                        participant_sid: publisher_id.as_str().to_string(),
                        track_sid: track_id.as_str().to_string(),
                        allowed,
                    },
                ));
        if let Err(err) = &result {
            errorw!(
                self.sub_logger,
                "could not send subscription permission update",
                err
            );
        }
        Ok(result?)
    }

    pub(super) fn send_media_sections_requirement(
        &self,
        num_audios: u32,
        num_videos: u32,
    ) -> Result<(), Error> {
        debugw!(
            self.pub_logger,
            "sending media sections requirement",
            "numAudios" => num_audios,
            "numVideos" => num_videos,
        );
        let result =
            self.signaller
                .write_message(self.signalling.signal_media_sections_requirement(
                    livekit::MediaSectionsRequirement {
                        num_audios,
                        num_videos,
                    },
                ));
        if let Err(err) = &result {
            errorw!(
                self.sub_logger,
                "could not send media sections requirement",
                err
            );
        }
        Ok(result?)
    }

    pub(super) fn send_publish_data_track_response(
        &self,
        dti: &livekit::DataTrackInfo,
    ) -> Result<(), Error> {
        Ok(self
            .signaller
            .write_message(self.signalling.signal_publish_data_track_response(
                livekit::PublishDataTrackResponse {
                    info: Some(dti.clone()),
                },
            ))?)
    }

    pub(super) fn send_unpublish_data_track_response(
        &self,
        dti: &livekit::DataTrackInfo,
    ) -> Result<(), Error> {
        Ok(self
            .signaller
            .write_message(self.signalling.signal_unpublish_data_track_response(
                livekit::UnpublishDataTrackResponse {
                    info: Some(dti.clone()),
                },
            ))?)
    }

    pub fn send_data_track_subscriber_handles(
        &self,
        handles: &HashMap<u32, livekit::data_track_subscriber_handles::PublishedDataTrack>,
    ) -> Result<(), Error> {
        Ok(self
            .signaller
            .write_message(self.signalling.signal_data_track_subscriber_handles(
                livekit::DataTrackSubscriberHandles {
                    sub_handles: handles.clone(),
                },
            ))?)
    }

    pub(super) fn send_store_data_blob_response(
        &self,
        request_id: u32,
        key: Option<livekit::DataBlobKey>,
    ) -> Result<(), Error> {
        Ok(self.signaller.write_message(
            self.signalling
                .signal_store_data_blob_response(livekit::StoreDataBlobResponse {
                    request_id,
                    key,
                }),
        )?)
    }

    pub(super) fn send_get_data_blob_response(
        &self,
        request_id: u32,
        data_blob: Option<livekit::DataBlob>,
    ) -> Result<(), Error> {
        Ok(self
            .signaller
            .write_message(self.signalling.signal_get_data_blob_response(
                livekit::GetDataBlobResponse {
                    request_id,
                    blob: data_blob,
                },
            ))?)
    }
}
