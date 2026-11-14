// Ported from LiveKit, pkg/rtc/participant_data_track.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The methods are those of `ParticipantImpl`
// (participant.rs), in an `impl` block of their own. `NewBytesTrackStats` takes no kind, kind
// details or reporter here (signalanddatastats.rs). A request is logged by its `Debug`, where
// the original logs it through `logger.Proto`. A message of the data track that does not
// decode is taken from the slice of the data channel into `Bytes`, as the decoder takes it.

use std::sync::Arc;

use bytes::Bytes;

use super::participant::ParticipantImpl;
use super::types;
use super::{BytesTrackStats, DataTrack, DataTrackParams};
use crate::protocol::datatrack;
use crate::protocol::livekit;
use crate::protocol::utils::{self as putils, TimedVersion, guid};
use crate::rtc::Error;
use crate::{errorw, warnw};

impl ParticipantImpl {
    pub fn handle_publish_data_track_request(&self, req: &livekit::PublishDataTrackRequest) {
        if !self.can_publish_data() || !self.params.enable_data_tracks {
            warnw!(self.pub_logger, "no permission to publish data track", nil, "req" => req);
            let _ = self.send_request_response(livekit::RequestResponse {
                reason: livekit::request_response::Reason::NotAllowed as i32,
                message: "does not have permission to publish data".to_string(),
                request: Some(livekit::request_response::Request::PublishDataTrack(
                    req.clone(),
                )),
                ..Default::default()
            });
            return;
        }

        if req.pub_handle == 0 || req.pub_handle > 65535 {
            warnw!(self.pub_logger, "invalid data track handle", nil, "req" => req);
            let _ = self.send_request_response(livekit::RequestResponse {
                reason: livekit::request_response::Reason::InvalidHandle as i32,
                message: "handle should be > 0 AND < 65536".to_string(),
                request: Some(livekit::request_response::Request::PublishDataTrack(
                    req.clone(),
                )),
                ..Default::default()
            });
            return;
        }

        if req.name.is_empty() || req.name.len() > 256 {
            warnw!(self.pub_logger, "invalid data track name", nil, "req" => req);
            let _ = self.send_request_response(livekit::RequestResponse {
                reason: livekit::request_response::Reason::InvalidName as i32,
                message: "name should not be empty and should not exceed 256 characters"
                    .to_string(),
                request: Some(livekit::request_response::Request::PublishDataTrack(
                    req.clone(),
                )),
                ..Default::default()
            });
            return;
        }

        if !self
            .params
            .limit_config
            .check_data_track_frame_encoding(req.frame_encoding.as_ref())
            || !self
                .params
                .limit_config
                .check_data_track_schema_id(req.schema.as_ref())
        {
            warnw!(self.pub_logger, "invalid encoding identifier", nil, "req" => req);
            let _ = self.send_request_response(livekit::RequestResponse {
                reason: livekit::request_response::Reason::InvalidRequest as i32,
                message: "encoding identifier is empty or exceeds the maximum length".to_string(),
                request: Some(livekit::request_response::Request::PublishDataTrack(
                    req.clone(),
                )),
                ..Default::default()
            });
            return;
        }

        let published_data_tracks = self.up_data_track_manager.get_published_data_tracks();
        for dt in published_data_tracks {
            let mut message = "";
            let mut reason = livekit::request_response::Reason::Ok;
            if dt.pub_handle() == req.pub_handle as u16 {
                message = "a data track with same handle already exists";
                reason = livekit::request_response::Reason::DuplicateHandle;
            } else if dt.name() == req.name {
                message = "a data track with same name already exists";
                reason = livekit::request_response::Reason::DuplicateName;
            }
            if !message.is_empty() {
                warnw!(
                    self.pub_logger,
                    "cannot publish duplicate data track", nil,
                    "req" => req,
                    "existing" => dt.to_proto(),
                );
                let _ = self.send_request_response(livekit::RequestResponse {
                    reason: reason as i32,
                    message: message.to_string(),
                    request: Some(livekit::request_response::Request::PublishDataTrack(
                        req.clone(),
                    )),
                    ..Default::default()
                });
                return;
            }
        }

        let dti = livekit::DataTrackInfo {
            pub_handle: req.pub_handle,
            sid: guid::new(putils::DATA_TRACK_PREFIX),
            name: req.name.clone(),
            encryption: req.encryption,
            frame_encoding: req.frame_encoding.clone(),
            schema: req.schema.clone(),
        };
        let dt = DataTrack::new(
            DataTrackParams {
                logger: self.params.logger.with_values(&[("trackID", &dti.sid)]),
                participant_id: self.participant_id_func(),
                participant_identity: self.params.identity.clone(),
                bytes_track_stats: Some(BytesTrackStats::new(
                    &self.params.country,
                    &livekit::TrackId::from(dti.sid.as_str()),
                    &self.id(),
                    self.telemetry_listener_func(),
                )),
            },
            &dti,
        );

        self.up_data_track_manager.add_published_data_track(dt);

        let _ = self.send_publish_data_track_response(&dti);

        self.set_is_publisher(true);
        self.dirty.store(true);
    }

    pub fn handle_unpublish_data_track_request(&self, req: &livekit::UnpublishDataTrackRequest) {
        let Some(dt) = self
            .up_data_track_manager
            .get_published_data_track(req.pub_handle as u16)
        else {
            warnw!(self.pub_logger, "unpublish data track not found", nil, "req" => req);
            let _ = self.send_request_response(livekit::RequestResponse {
                reason: livekit::request_response::Reason::NotFound as i32,
                request: Some(livekit::request_response::Request::UnpublishDataTrack(*req)),
                ..Default::default()
            });
            return;
        };

        self.up_data_track_manager
            .remove_published_data_track(dt.clone());

        let _ = self.send_unpublish_data_track_response(&dt.to_proto());

        self.dirty.store(true);
    }

    pub fn handle_update_data_subscription(&self, req: &livekit::UpdateDataSubscription) {
        self.listener()
            .on_update_data_subscriptions(self.arc(), req);
    }

    pub(super) fn on_received_data_track_message(&self, data: &[u8], arrival_time: i64) {
        let mut packet = datatrack::Packet::default();
        if let Err(err) = packet.unmarshal(&Bytes::copy_from_slice(data)) {
            errorw!(
                self.params.logger,
                "could not unmarshal data track message",
                err
            );
            return;
        }

        self.up_data_track_manager
            .handle_received_data_track_message(data, &packet, arrival_time);

        self.listener()
            .on_data_track_message(self.arc(), data, &packet);
    }

    // wraps the promoted UpTrackManager.UpdateSubscriptionPermission to also revoke
    // data track subscriptions that are no longer permitted
    #[allow(clippy::type_complexity)]
    pub fn update_subscription_permission(
        &self,
        subscription_permission: Option<&livekit::SubscriptionPermission>,
        timed_version: TimedVersion,
        resolver_by_sid: Option<
            Box<
                dyn Fn(&livekit::ParticipantId) -> Option<Arc<dyn types::LocalParticipant>>
                    + Send
                    + Sync,
            >,
        >,
    ) -> Result<(), Error> {
        self.up_track_manager.update_subscription_permission(
            subscription_permission,
            timed_version,
            resolver_by_sid,
        )?;

        self.maybe_revoke_data_track_subscriptions();
        Ok(())
    }

    fn maybe_revoke_data_track_subscriptions(&self) {
        for dt in self.up_data_track_manager.get_published_data_tracks() {
            let Some(allowed) = self.up_track_manager.get_allowed_subscribers(&dt.id()) else {
                // no restrictions
                continue;
            };

            dt.revoke_disallowed_subscribers(&allowed);
        }
    }

    pub fn get_next_subscribed_data_track_handle(&self) -> u16 {
        let mut inner = self.lock.write();

        inner.next_subscribed_data_track_handle =
            inner.next_subscribed_data_track_handle.wrapping_add(1);
        if inner.next_subscribed_data_track_handle == 0 {
            inner.next_subscribed_data_track_handle =
                inner.next_subscribed_data_track_handle.wrapping_add(1);
        }

        inner.next_subscribed_data_track_handle
    }
}
