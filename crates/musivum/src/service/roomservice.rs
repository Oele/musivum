// Ported from LiveKit, pkg/service/roomservice.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for one node.
// - The `context.Context` of a call is the request's extensions (`http::Extensions`), where
//   the auth middleware puts the grants and the Twirp server its state (twirp.rs); a call made
//   in a test takes an `Extensions` with grants from `with_grants`.
// - `roomStore.(OSSServiceStore)`: the store is given twice, as the `ServiceStore` and, where
//   it is one, as the `OssServiceStore` (`None` where the original's assertion fails).
// - Egress is left out: there is no `rtc.EgressLauncher`, which is nil upstream when no egress
//   is connected, so a room with `egress` set gets `ErrEgressNotConnected`, as upstream then.
// - `RecordRequest` and `RecordResponse` (twirp.go) are not called: they only fill in what the
//   hooks of `TwirpTelemetry` keep, which the server does not install upstream either.
// - `rpc.UnimplementedRoomServer` and `rpc.UnimplementedParticipantServer` have no
//   counterpart: the room service implements neither server.
// - The errors are those of the package (errors.rs): a Twirp error the original makes with
//   `twirp.NewError` or `twirp.InvalidArgumentError` is `Error::Twirp`, an error of the clients
//   is `Error::Psrpc`. `twirpAuthError` of auth.go is here.

use std::sync::Arc;

use http::Extensions;

use super::{
    Error, OssServiceStore, RoomAllocator, ServiceStore, TwirpErrorCode, append_log_fields,
    ensure_admin_permission, ensure_create_permission, ensure_dest_room_permission,
    ensure_list_permission, get_grants, invalid_argument_error, new_error,
};
use crate::config::{ApiConfig, LimitConfig};
use crate::go::time::Duration;
use crate::protocol::livekit;
use crate::protocol::utils as putils;
use crate::routing::{
    MessageRouter, PsrpcError, TopicFormatter, TypedParticipantClient, TypedRoomClient,
    with_request_timeout,
};

pub struct RoomService {
    limit_conf: LimitConfig,
    api_conf: ApiConfig,
    router: Arc<dyn MessageRouter>,
    room_allocator: Arc<dyn RoomAllocator>,
    room_store: Arc<dyn ServiceStore>,
    // `roomStore.(OSSServiceStore)`
    oss_room_store: Option<Arc<dyn OssServiceStore>>,
    topic_formatter: Arc<dyn TopicFormatter>,
    room_client: Arc<dyn TypedRoomClient>,
    participant_client: Arc<dyn TypedParticipantClient>,
}

impl RoomService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        limit_conf: LimitConfig,
        api_conf: ApiConfig,
        router: Arc<dyn MessageRouter>,
        room_allocator: Arc<dyn RoomAllocator>,
        service_store: Arc<dyn ServiceStore>,
        oss_service_store: Option<Arc<dyn OssServiceStore>>,
        topic_formatter: Arc<dyn TopicFormatter>,
        room_client: Arc<dyn TypedRoomClient>,
        participant_client: Arc<dyn TypedParticipantClient>,
    ) -> Result<Arc<RoomService>, Error> {
        Ok(Arc::new(RoomService {
            limit_conf,
            api_conf,
            router,
            room_allocator,
            room_store: service_store,
            oss_room_store: oss_service_store,
            topic_formatter,
            room_client,
            participant_client,
        }))
    }

    pub fn create_room(
        &self,
        ctx: &Extensions,
        req: &livekit::CreateRoomRequest,
    ) -> Result<livekit::Room, Error> {
        append_log_fields(ctx, &[("room", &req.name), ("request", req)]);
        if let Err(err) = ensure_create_permission(get_grants(ctx)) {
            return Err(twirp_auth_error(&err));
        } else if req.egress.is_some() {
            // `s.egressLauncher == nil`: egress is left out
            return Err(Error::EgressNotConnected);
        }

        if !self.limit_conf.check_room_name_length(&req.name) {
            return Err(Error::RoomNameExceedsLimitsMaxLength(
                self.limit_conf.max_room_name_length,
            ));
        }

        if !self.limit_conf.check_metadata_size(&req.metadata) {
            return Err(Error::Twirp(invalid_argument_error(
                &Error::MetadataExceedsLimits.to_string(),
                &self.limit_conf.max_metadata_size.to_string(),
            )));
        }

        for ad in &req.agents {
            if !self.limit_conf.check_metadata_size(&ad.metadata) {
                return Err(Error::Twirp(invalid_argument_error(
                    &Error::MetadataExceedsLimits.to_string(),
                    &self.limit_conf.max_metadata_size.to_string(),
                )));
            }
            if !self.limit_conf.check_attributes_size(&ad.attributes) {
                return Err(Error::Twirp(invalid_argument_error(
                    &Error::AttributeExceedsLimits.to_string(),
                    &self.limit_conf.max_attributes_size.to_string(),
                )));
            }
        }

        self.room_allocator.select_room_node(
            &livekit::RoomName::from(req.name.as_str()),
            &livekit::NodeId::from(req.node_id.as_str()),
        )?;

        let room = self.router.create_room(req)?;
        Ok(room)
    }

    pub fn list_rooms(
        &self,
        ctx: &Extensions,
        req: &livekit::ListRoomsRequest,
    ) -> Result<livekit::ListRoomsResponse, Error> {
        append_log_fields(ctx, &[("room", &req.names)]);
        if let Err(err) = ensure_list_permission(get_grants(ctx)) {
            return Err(twirp_auth_error(&err));
        }

        let mut names = None;
        if !req.names.is_empty() {
            names = Some(
                req.names
                    .iter()
                    .map(|n| livekit::RoomName::from(n.as_str()))
                    .collect::<Vec<_>>(),
            );
        }
        // TODO: translate error codes to Twirp
        let rooms = self.room_store.list_rooms(names.as_deref())?;

        let res = livekit::ListRoomsResponse { rooms };
        Ok(res)
    }

    pub fn delete_room(
        &self,
        ctx: &Extensions,
        req: &livekit::DeleteRoomRequest,
    ) -> Result<livekit::DeleteRoomResponse, Error> {
        append_log_fields(ctx, &[("room", &req.room)]);
        if let Err(err) = ensure_create_permission(get_grants(ctx)) {
            return Err(twirp_auth_error(&err));
        }

        let room_name = livekit::RoomName::from(req.room.as_str());
        if !self.room_store.room_exists(&room_name)? {
            return Err(Error::RoomNotFound);
        }

        // ensure at least one node is available to handle the request
        self.router.create_room(&livekit::CreateRoomRequest {
            name: req.room.clone(),
            ..Default::default()
        })?;

        self.room_client
            .delete_room(&self.topic_formatter.room_topic(&room_name), req)?;

        if let Some(os) = &self.oss_room_store {
            os.delete_room(&room_name)?;
        }
        let res = livekit::DeleteRoomResponse {};
        Ok(res)
    }

    pub fn list_participants(
        &self,
        ctx: &Extensions,
        req: &livekit::ListParticipantsRequest,
    ) -> Result<livekit::ListParticipantsResponse, Error> {
        append_log_fields(ctx, &[("room", &req.room)]);
        let room_name = livekit::RoomName::from(req.room.as_str());
        if let Err(err) = ensure_admin_permission(get_grants(ctx), &room_name) {
            return Err(twirp_auth_error(&err));
        }

        let res = if self.api_conf.enable_psrpc_for_get_list_particpants {
            self.room_client
                .list_participants(&self.topic_formatter.room_topic(&room_name), req)?
        } else if let Some(store) = &self.oss_room_store {
            let participants = store.list_participants(&room_name)?;
            livekit::ListParticipantsResponse { participants }
        } else {
            return Err(PsrpcError::unimplemented().into());
        };

        Ok(res)
    }

    pub fn get_participant(
        &self,
        ctx: &Extensions,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::ParticipantInfo, Error> {
        append_log_fields(ctx, &[("room", &req.room), ("participant", &req.identity)]);
        let room_name = livekit::RoomName::from(req.room.as_str());
        if let Err(err) = ensure_admin_permission(get_grants(ctx), &room_name) {
            return Err(twirp_auth_error(&err));
        }

        let participant = if self.api_conf.enable_psrpc_for_get_list_particpants {
            self.room_client
                .get_participant(&self.topic_formatter.room_topic(&room_name), req)?
        } else if let Some(store) = &self.oss_room_store {
            store.load_participant(
                &room_name,
                &livekit::ParticipantIdentity::from(req.identity.as_str()),
            )?
        } else {
            return Err(PsrpcError::unimplemented().into());
        };

        Ok(participant)
    }

    pub fn remove_participant(
        &self,
        ctx: &Extensions,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::RemoveParticipantResponse, Error> {
        append_log_fields(ctx, &[("room", &req.room), ("participant", &req.identity)]);

        let room_name = livekit::RoomName::from(req.room.as_str());
        let identity = livekit::ParticipantIdentity::from(req.identity.as_str());
        if let Err(err) = ensure_admin_permission(get_grants(ctx), &room_name) {
            return Err(twirp_auth_error(&err));
        }

        if let Some(os) = &self.oss_room_store
            && !os.has_participant(&room_name, &identity)?
        {
            return Err(Error::ParticipantNotFound);
        }

        let res = self.participant_client.remove_participant(
            &self
                .topic_formatter
                .participant_topic(&room_name, &identity),
            req,
        )?;
        Ok(res)
    }

    pub fn mute_published_track(
        &self,
        ctx: &Extensions,
        req: &livekit::MuteRoomTrackRequest,
    ) -> Result<livekit::MuteRoomTrackResponse, Error> {
        append_log_fields(
            ctx,
            &[
                ("room", &req.room),
                ("participant", &req.identity),
                ("trackID", &req.track_sid),
                ("muted", &req.muted),
            ],
        );
        let room_name = livekit::RoomName::from(req.room.as_str());
        if let Err(err) = ensure_admin_permission(get_grants(ctx), &room_name) {
            return Err(twirp_auth_error(&err));
        }

        let res = self.participant_client.mute_published_track(
            &self.topic_formatter.participant_topic(
                &room_name,
                &livekit::ParticipantIdentity::from(req.identity.as_str()),
            ),
            req,
        )?;
        Ok(res)
    }

    pub fn update_participant(
        &self,
        ctx: &Extensions,
        req: &livekit::UpdateParticipantRequest,
    ) -> Result<livekit::ParticipantInfo, Error> {
        append_log_fields(ctx, &[("room", &req.room), ("participant", &req.identity)]);

        if !self.limit_conf.check_participant_name_length(&req.name) {
            return Err(Error::Twirp(invalid_argument_error(
                &Error::NameExceedsLimits.to_string(),
                &self.limit_conf.max_participant_name_length.to_string(),
            )));
        }

        if !self.limit_conf.check_metadata_size(&req.metadata) {
            return Err(Error::Twirp(invalid_argument_error(
                &Error::MetadataExceedsLimits.to_string(),
                &self.limit_conf.max_metadata_size.to_string(),
            )));
        }

        if !self.limit_conf.check_attributes_size(&req.attributes) {
            return Err(Error::Twirp(invalid_argument_error(
                &Error::AttributeExceedsLimits.to_string(),
                &self.limit_conf.max_attributes_size.to_string(),
            )));
        }

        let room_name = livekit::RoomName::from(req.room.as_str());
        let identity = livekit::ParticipantIdentity::from(req.identity.as_str());
        if let Err(err) = ensure_admin_permission(get_grants(ctx), &room_name) {
            return Err(twirp_auth_error(&err));
        }

        if let Some(os) = &self.oss_room_store
            && !os.has_participant(&room_name, &identity)?
        {
            return Err(Error::ParticipantNotFound);
        }

        let res = self.participant_client.update_participant(
            &self
                .topic_formatter
                .participant_topic(&room_name, &identity),
            req,
        )?;
        Ok(res)
    }

    pub fn update_subscriptions(
        &self,
        ctx: &Extensions,
        req: &livekit::UpdateSubscriptionsRequest,
    ) -> Result<livekit::UpdateSubscriptionsResponse, Error> {
        let mut track_sids = req.track_sids.clone();
        for pt in &req.participant_tracks {
            track_sids.extend(pt.track_sids.iter().cloned());
        }
        append_log_fields(
            ctx,
            &[
                ("room", &req.room),
                ("participant", &req.identity),
                ("trackID", &track_sids),
            ],
        );

        let room_name = livekit::RoomName::from(req.room.as_str());
        if let Err(err) = ensure_admin_permission(get_grants(ctx), &room_name) {
            return Err(twirp_auth_error(&err));
        }

        let res = self.participant_client.update_subscriptions(
            &self.topic_formatter.participant_topic(
                &room_name,
                &livekit::ParticipantIdentity::from(req.identity.as_str()),
            ),
            req,
        )?;
        Ok(res)
    }

    pub fn send_data(
        &self,
        ctx: &Extensions,
        req: &livekit::SendDataRequest,
    ) -> Result<livekit::SendDataResponse, Error> {
        let room_name = livekit::RoomName::from(req.room.as_str());
        append_log_fields(ctx, &[("room", &room_name), ("size", &req.data.len())]);
        if let Err(err) = ensure_admin_permission(get_grants(ctx), &room_name) {
            return Err(twirp_auth_error(&err));
        }

        // nonce is either absent or 128-bit UUID
        if !req.nonce.is_empty() && req.nonce.len() != 16 {
            return Err(Error::Twirp(new_error(
                TwirpErrorCode::InvalidArgument,
                &format!(
                    "nonce should be 16-bytes or not present, got: {} bytes",
                    req.nonce.len()
                ),
            )));
        }

        let res = self
            .room_client
            .send_data(&self.topic_formatter.room_topic(&room_name), req)?;
        Ok(res)
    }

    pub fn update_room_metadata(
        &self,
        ctx: &Extensions,
        req: &livekit::UpdateRoomMetadataRequest,
    ) -> Result<livekit::Room, Error> {
        append_log_fields(ctx, &[("room", &req.room), ("size", &req.metadata.len())]);
        if !self.limit_conf.check_metadata_size(&req.metadata) {
            return Err(Error::Twirp(invalid_argument_error(
                &Error::MetadataExceedsLimits.to_string(),
                &self.limit_conf.max_metadata_size.to_string(),
            )));
        }

        let room_name = livekit::RoomName::from(req.room.as_str());
        if let Err(err) = ensure_admin_permission(get_grants(ctx), &room_name) {
            return Err(twirp_auth_error(&err));
        }

        if !self.room_store.room_exists(&room_name)? {
            return Err(Error::RoomNotFound);
        }

        let room = self
            .room_client
            .update_room_metadata(&self.topic_formatter.room_topic(&room_name), req)?;

        Ok(room)
    }

    pub fn forward_participant(
        &self,
        ctx: &Extensions,
        req: &livekit::ForwardParticipantRequest,
    ) -> Result<livekit::ForwardParticipantResponse, Error> {
        let room_name = livekit::RoomName::from(req.room.as_str());
        append_log_fields(ctx, &[("room", &room_name), ("participant", &req.identity)]);
        if let Err(err) = ensure_dest_room_permission(
            get_grants(ctx),
            &room_name,
            &livekit::RoomName::from(req.destination_room.as_str()),
        ) {
            return Err(twirp_auth_error(&err));
        }

        if req.room == req.destination_room {
            return Err(Error::Twirp(invalid_argument_error(
                &Error::DestinationSameAsSourceRoom.to_string(),
                "",
            )));
        }

        let res = self.participant_client.forward_participant(
            &self.topic_formatter.participant_topic(
                &room_name,
                &livekit::ParticipantIdentity::from(req.identity.as_str()),
            ),
            req,
        )?;
        Ok(res)
    }

    pub fn move_participant(
        &self,
        ctx: &Extensions,
        req: &livekit::MoveParticipantRequest,
    ) -> Result<livekit::MoveParticipantResponse, Error> {
        let room_name = livekit::RoomName::from(req.room.as_str());
        append_log_fields(ctx, &[("room", &room_name), ("participant", &req.identity)]);
        if let Err(err) = ensure_dest_room_permission(
            get_grants(ctx),
            &room_name,
            &livekit::RoomName::from(req.destination_room.as_str()),
        ) {
            return Err(twirp_auth_error(&err));
        }

        if req.room == req.destination_room {
            return Err(Error::Twirp(invalid_argument_error(
                &Error::DestinationSameAsSourceRoom.to_string(),
                "",
            )));
        }

        let res = self.participant_client.move_participant(
            &self.topic_formatter.participant_topic(
                &room_name,
                &livekit::ParticipantIdentity::from(req.identity.as_str()),
            ),
            req,
        )?;
        Ok(res)
    }

    pub fn perform_rpc(
        &self,
        ctx: &Extensions,
        req: &livekit::PerformRpcRequest,
    ) -> Result<livekit::PerformRpcResponse, Error> {
        let room_name = livekit::RoomName::from(req.room.as_str());
        append_log_fields(
            ctx,
            &[
                ("room", &room_name),
                ("participant", &req.destination_identity),
            ],
        );

        if let Err(err) = ensure_admin_permission(get_grants(ctx), &room_name) {
            return Err(twirp_auth_error(&err));
        }
        if req.destination_identity.is_empty() {
            return Err(Error::DestinationIdentityRequired);
        }

        let res = self.participant_client.perform_rpc(
            &self.topic_formatter.participant_topic(
                &room_name,
                &livekit::ParticipantIdentity::from(req.destination_identity.as_str()),
            ),
            req,
            &[with_request_timeout(perform_rpc_request_timeout(req))],
        )?;
        Ok(res)
    }
}

// performRpcRequestTimeout covers the participant's response window plus the ack round trip,
// since the psrpc default request timeout is shorter than the default RPC response timeout
fn perform_rpc_request_timeout(req: &livekit::PerformRpcRequest) -> Duration {
    let mut response_timeout = Duration::from_millis(req.response_timeout_ms as i64);
    if response_timeout <= Duration(0) {
        response_timeout = putils::DATA_CHANNEL_RPC_DEFAULT_RESPONSE_TIMEOUT;
    }
    response_timeout + putils::DATA_CHANNEL_RPC_MAX_ROUND_TRIP_LATENCY
}

// auth.go's twirpAuthError
fn twirp_auth_error(err: &Error) -> Error {
    Error::Twirp(new_error(TwirpErrorCode::Unauthenticated, &err.to_string()))
}

#[cfg(test)]
#[path = "roomservice_test.rs"]
mod roomservice_test;
