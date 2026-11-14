// Ported from LiveKit's protocol, rpc/room.psrpc.go, rpc/participant.psrpc.go and the topics
// of rpc/typed_api.go at v1.53.1-0.20261023175527-46332a036e25; with what of psrpc v0.8.2's
// errors.go and request options the calls here use.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for one node and without psrpc's bus.
// - The two services of `rpc/room.proto` and `rpc/participant.proto`, which the room manager
//   serves for the room service (`pkg/service`), are what psrpc generates for them: the
//   traits a server implements (`RoomServerImpl`, `ParticipantServerImpl`, without the
//   `context.Context`) and the typed clients (`TypedRoomClient`, `TypedParticipantClient`,
//   without the context; only `PerformRpc` takes request options, as only that call is
//   given one). The generic `RoomClient[RoomTopicType]` is the typed one: topics are always
//   `RoomTopic` and `ParticipantTopic`.
// - A server is registered on the bus of the process under a topic (`bus.rs`), where the
//   original's `NewTypedRoomServer(svc, bus)` and `RegisterAllRoomTopics(topic)` subscribe
//   it on psrpc's bus. A client calls the server registered for the topic directly, on the
//   caller's thread. A topic with no server registered gives `ErrNoResponse` at once, where
//   psrpc gives it when no server has claimed the request within its selection timeout (a
//   second, tried three times by the client's retries). Request options are taken and not
//   read: there is no timeout on a direct call (the room manager's `PerformRpc` bounds its
//   wait itself, with a shorter timeout than the one psrpc is given).
// - The errors: what psrpc carries of a server's error to the client, its code and its text
//   (`PsrpcError`, psrpc's `NewErrorFromResponse`), with the codes of psrpc's errors.go. A
//   server that has an error of another kind turns it into one, as psrpc's server does
//   (`service::Error` has a `From` for this).
// - Not here: the `Register...Topic`, `Deregister...Topic`, `Shutdown` and `Kill` of the
//   generated servers (the bus has the registration), the `Close` of the clients does
//   nothing, `ClientParams` (a client is made from the bus alone), and the other services of
//   `rpc`.

use std::fmt;
use std::sync::Arc;

use http::StatusCode;

use super::Error;
use super::bus::MessageBus;
use crate::go::time::Duration;
use crate::protocol::livekit;

// ------------------------------------------------
// typed_api.go: topics

#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct ParticipantTopic(pub String);

#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct RoomTopic(pub String);

impl fmt::Debug for ParticipantTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Debug for RoomTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

pub fn format_participant_topic(
    room_name: &livekit::RoomName,
    identity: &livekit::ParticipantIdentity,
) -> ParticipantTopic {
    ParticipantTopic(format!("{room_name}_{identity}"))
}

pub fn format_room_topic(room_name: &livekit::RoomName) -> RoomTopic {
    RoomTopic(room_name.to_string())
}

struct TopicFormatterImpl;

pub fn new_topic_formatter() -> Arc<dyn TopicFormatter> {
    Arc::new(TopicFormatterImpl)
}

impl TopicFormatter for TopicFormatterImpl {
    fn participant_topic(
        &self,
        room_name: &livekit::RoomName,
        identity: &livekit::ParticipantIdentity,
    ) -> ParticipantTopic {
        format_participant_topic(room_name, identity)
    }

    fn room_topic(&self, room_name: &livekit::RoomName) -> RoomTopic {
        format_room_topic(room_name)
    }
}

pub trait TopicFormatter: Send + Sync {
    fn participant_topic(
        &self,
        room_name: &livekit::RoomName,
        identity: &livekit::ParticipantIdentity,
    ) -> ParticipantTopic;
    fn room_topic(&self, room_name: &livekit::RoomName) -> RoomTopic;
}

// ------------------------------------------------
// psrpc: errors and request options

/// psrpc's `ErrorCode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PsrpcErrorCode {
    Ok,

    // Request Canceled by client
    Canceled,
    // Could not unmarshal request
    MalformedRequest,
    // Could not unmarshal result
    MalformedResponse,
    // Request timed out
    DeadlineExceeded,
    // Service unavailable due to load and/or affinity constraints
    Unavailable,
    // Unknown (server returned non-psrpc error)
    Unknown,

    // Invalid argument in request
    InvalidArgument,
    // Entity not found
    NotFound,
    // Cannot produce and entity matching requested format
    NotAcceptable,
    // Duplicate creation attempted
    AlreadyExists,
    // Caller does not have required permissions
    PermissionDenied,
    // Some resource has been exhausted, e.g. memory or quota
    ResourceExhausted,
    // Inconsistent state to carry out request
    FailedPrecondition,
    // Request aborted
    Aborted,
    // Operation was out of range
    OutOfRange,
    // Operation is not implemented by the server
    Unimplemented,
    // Operation failed due to an internal error
    Internal,
    // Irrecoverable loss or corruption of data
    DataLoss,
    // Similar to PermissionDenied, used when the caller is unidentified
    Unauthenticated,
    // Cannot consume the entity in the given format
    UnprocessableEntity,
    // Upstream server error
    UpstreamServerError,
    // Upstread client error
    UpstreamClientError,
}

impl PsrpcErrorCode {
    /// The code's text: Go's `string(code)`.
    pub fn as_str(self) -> &'static str {
        match self {
            PsrpcErrorCode::Ok => "",
            PsrpcErrorCode::Canceled => "canceled",
            PsrpcErrorCode::MalformedRequest => "malformed_request",
            PsrpcErrorCode::MalformedResponse => "malformed_result",
            PsrpcErrorCode::DeadlineExceeded => "deadline_exceeded",
            PsrpcErrorCode::Unavailable => "unavailable",
            PsrpcErrorCode::Unknown => "unknown",
            PsrpcErrorCode::InvalidArgument => "invalid_argument",
            PsrpcErrorCode::NotFound => "not_found",
            PsrpcErrorCode::NotAcceptable => "not_acceptable",
            PsrpcErrorCode::AlreadyExists => "already_exists",
            PsrpcErrorCode::PermissionDenied => "permission_denied",
            PsrpcErrorCode::ResourceExhausted => "resource_exhausted",
            PsrpcErrorCode::FailedPrecondition => "failed_precondition",
            PsrpcErrorCode::Aborted => "aborted",
            PsrpcErrorCode::OutOfRange => "out_of_range",
            PsrpcErrorCode::Unimplemented => "unimplemented",
            PsrpcErrorCode::Internal => "internal",
            PsrpcErrorCode::DataLoss => "data_loss",
            PsrpcErrorCode::Unauthenticated => "unauthenticated",
            PsrpcErrorCode::UnprocessableEntity => "unprocessable_entity",
            PsrpcErrorCode::UpstreamServerError => "upstream_server_error",
            PsrpcErrorCode::UpstreamClientError => "upstream_client_error",
        }
    }

    /// psrpc's `ToHTTP`.
    pub fn to_http(self) -> StatusCode {
        match self {
            PsrpcErrorCode::Ok => StatusCode::OK,
            PsrpcErrorCode::Unknown
            | PsrpcErrorCode::MalformedResponse
            | PsrpcErrorCode::Internal
            | PsrpcErrorCode::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
            PsrpcErrorCode::InvalidArgument | PsrpcErrorCode::MalformedRequest => {
                StatusCode::BAD_REQUEST
            }
            PsrpcErrorCode::NotFound => StatusCode::NOT_FOUND,
            PsrpcErrorCode::NotAcceptable => StatusCode::NOT_ACCEPTABLE,
            PsrpcErrorCode::AlreadyExists | PsrpcErrorCode::Aborted => StatusCode::CONFLICT,
            PsrpcErrorCode::PermissionDenied => StatusCode::FORBIDDEN,
            PsrpcErrorCode::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            PsrpcErrorCode::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
            PsrpcErrorCode::OutOfRange => StatusCode::RANGE_NOT_SATISFIABLE,
            PsrpcErrorCode::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            PsrpcErrorCode::Canceled
            | PsrpcErrorCode::DeadlineExceeded
            | PsrpcErrorCode::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            PsrpcErrorCode::Unauthenticated => StatusCode::UNAUTHORIZED,
            PsrpcErrorCode::UnprocessableEntity => StatusCode::UNPROCESSABLE_ENTITY,
            PsrpcErrorCode::UpstreamServerError => StatusCode::BAD_GATEWAY,
            PsrpcErrorCode::UpstreamClientError => StatusCode::FAILED_DEPENDENCY,
        }
    }
}

impl fmt::Display for PsrpcErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// psrpc's `Error` as a call returns it: its code and its text, which is what psrpc carries of
/// a server's error to the client (`NewErrorFromResponse`). `PsrpcError::new` is
/// `psrpc.NewErrorf(code, msg)`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{msg}")]
pub struct PsrpcError {
    code: PsrpcErrorCode,
    msg: String,
}

impl PsrpcError {
    pub fn new(code: PsrpcErrorCode, msg: impl Into<String>) -> PsrpcError {
        PsrpcError {
            code,
            msg: msg.into(),
        }
    }

    pub fn code(&self) -> PsrpcErrorCode {
        self.code
    }

    /// psrpc's `ErrRequestTimedOut`.
    pub fn request_timed_out() -> PsrpcError {
        PsrpcError::new(PsrpcErrorCode::DeadlineExceeded, "request timed out")
    }

    /// psrpc's `ErrNoResponse`: no server has answered for the topic.
    pub fn no_response() -> PsrpcError {
        PsrpcError::new(PsrpcErrorCode::Unavailable, "no response from servers")
    }

    /// psrpc's `ErrUnimplemented`.
    pub fn unimplemented() -> PsrpcError {
        PsrpcError::new(PsrpcErrorCode::Unimplemented, "method is not implemented")
    }
}

/// psrpc's `RequestOption`, as far as the calls here are given one.
#[derive(Clone, Debug, PartialEq)]
pub enum RequestOption {
    /// `psrpc.WithRequestTimeout(timeout)`
    RequestTimeout(Duration),
}

/// psrpc's `RequestOpts`, what the options are applied to.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RequestOpts {
    pub timeout: Duration,
}

impl RequestOption {
    /// What Go does by calling the option with the options: `o(&ro)`.
    pub fn apply(&self, o: &mut RequestOpts) {
        match self {
            RequestOption::RequestTimeout(timeout) => o.timeout = *timeout,
        }
    }
}

pub fn with_request_timeout(timeout: Duration) -> RequestOption {
    RequestOption::RequestTimeout(timeout)
}

// ------------------------------------------------
// room.psrpc.go

#[cfg_attr(test, musivum_fakes::fake)]
pub trait RoomServerImpl: Send + Sync {
    fn delete_room(
        &self,
        req: &livekit::DeleteRoomRequest,
    ) -> Result<livekit::DeleteRoomResponse, PsrpcError>;

    fn send_data(
        &self,
        req: &livekit::SendDataRequest,
    ) -> Result<livekit::SendDataResponse, PsrpcError>;

    fn update_room_metadata(
        &self,
        req: &livekit::UpdateRoomMetadataRequest,
    ) -> Result<livekit::Room, PsrpcError>;

    fn list_participants(
        &self,
        req: &livekit::ListParticipantsRequest,
    ) -> Result<livekit::ListParticipantsResponse, PsrpcError>;

    fn get_participant(
        &self,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::ParticipantInfo, PsrpcError>;
}

/// `rpc.TypedRoomClient`, `RoomClient[RoomTopic]`.
#[cfg_attr(test, musivum_fakes::fake)]
pub trait TypedRoomClient: Send + Sync {
    fn delete_room(
        &self,
        room: &RoomTopic,
        req: &livekit::DeleteRoomRequest,
    ) -> Result<livekit::DeleteRoomResponse, PsrpcError>;

    fn send_data(
        &self,
        room: &RoomTopic,
        req: &livekit::SendDataRequest,
    ) -> Result<livekit::SendDataResponse, PsrpcError>;

    fn update_room_metadata(
        &self,
        room: &RoomTopic,
        req: &livekit::UpdateRoomMetadataRequest,
    ) -> Result<livekit::Room, PsrpcError>;

    fn list_participants(
        &self,
        room: &RoomTopic,
        req: &livekit::ListParticipantsRequest,
    ) -> Result<livekit::ListParticipantsResponse, PsrpcError>;

    fn get_participant(
        &self,
        room: &RoomTopic,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::ParticipantInfo, PsrpcError>;

    // Close immediately, without waiting for pending RPCs
    fn close(&self);
}

struct RoomClient {
    client: Arc<MessageBus>,
}

/// `rpc.NewTypedRoomClient`, given the bus where the original is given `rpc.ClientParams`.
pub fn new_typed_room_client(bus: Arc<MessageBus>) -> Result<Arc<dyn TypedRoomClient>, Error> {
    Ok(Arc::new(RoomClient { client: bus }))
}

impl TypedRoomClient for RoomClient {
    fn delete_room(
        &self,
        room: &RoomTopic,
        req: &livekit::DeleteRoomRequest,
    ) -> Result<livekit::DeleteRoomResponse, PsrpcError> {
        self.client.room_server(room)?.delete_room(req)
    }

    fn send_data(
        &self,
        room: &RoomTopic,
        req: &livekit::SendDataRequest,
    ) -> Result<livekit::SendDataResponse, PsrpcError> {
        self.client.room_server(room)?.send_data(req)
    }

    fn update_room_metadata(
        &self,
        room: &RoomTopic,
        req: &livekit::UpdateRoomMetadataRequest,
    ) -> Result<livekit::Room, PsrpcError> {
        self.client.room_server(room)?.update_room_metadata(req)
    }

    fn list_participants(
        &self,
        room: &RoomTopic,
        req: &livekit::ListParticipantsRequest,
    ) -> Result<livekit::ListParticipantsResponse, PsrpcError> {
        self.client.room_server(room)?.list_participants(req)
    }

    fn get_participant(
        &self,
        room: &RoomTopic,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::ParticipantInfo, PsrpcError> {
        self.client.room_server(room)?.get_participant(req)
    }

    fn close(&self) {}
}

// ------------------------------------------------
// participant.psrpc.go

#[cfg_attr(test, musivum_fakes::fake)]
pub trait ParticipantServerImpl: Send + Sync {
    fn remove_participant(
        &self,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::RemoveParticipantResponse, PsrpcError>;

    fn mute_published_track(
        &self,
        req: &livekit::MuteRoomTrackRequest,
    ) -> Result<livekit::MuteRoomTrackResponse, PsrpcError>;

    fn update_participant(
        &self,
        req: &livekit::UpdateParticipantRequest,
    ) -> Result<livekit::ParticipantInfo, PsrpcError>;

    fn update_subscriptions(
        &self,
        req: &livekit::UpdateSubscriptionsRequest,
    ) -> Result<livekit::UpdateSubscriptionsResponse, PsrpcError>;

    fn forward_participant(
        &self,
        req: &livekit::ForwardParticipantRequest,
    ) -> Result<livekit::ForwardParticipantResponse, PsrpcError>;

    fn move_participant(
        &self,
        req: &livekit::MoveParticipantRequest,
    ) -> Result<livekit::MoveParticipantResponse, PsrpcError>;

    fn perform_rpc(
        &self,
        req: &livekit::PerformRpcRequest,
    ) -> Result<livekit::PerformRpcResponse, PsrpcError>;
}

/// `rpc.TypedParticipantClient`, `ParticipantClient[ParticipantTopic]`.
#[cfg_attr(test, musivum_fakes::fake)]
pub trait TypedParticipantClient: Send + Sync {
    fn remove_participant(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::RemoveParticipantResponse, PsrpcError>;

    fn mute_published_track(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::MuteRoomTrackRequest,
    ) -> Result<livekit::MuteRoomTrackResponse, PsrpcError>;

    fn update_participant(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::UpdateParticipantRequest,
    ) -> Result<livekit::ParticipantInfo, PsrpcError>;

    fn update_subscriptions(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::UpdateSubscriptionsRequest,
    ) -> Result<livekit::UpdateSubscriptionsResponse, PsrpcError>;

    fn forward_participant(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::ForwardParticipantRequest,
    ) -> Result<livekit::ForwardParticipantResponse, PsrpcError>;

    fn move_participant(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::MoveParticipantRequest,
    ) -> Result<livekit::MoveParticipantResponse, PsrpcError>;

    fn perform_rpc(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::PerformRpcRequest,
        opts: &[RequestOption],
    ) -> Result<livekit::PerformRpcResponse, PsrpcError>;

    // Close immediately, without waiting for pending RPCs
    fn close(&self);
}

struct ParticipantClient {
    client: Arc<MessageBus>,
}

/// `rpc.NewTypedParticipantClient`, given the bus where the original is given
/// `rpc.ClientParams`.
pub fn new_typed_participant_client(
    bus: Arc<MessageBus>,
) -> Result<Arc<dyn TypedParticipantClient>, Error> {
    Ok(Arc::new(ParticipantClient { client: bus }))
}

impl TypedParticipantClient for ParticipantClient {
    fn remove_participant(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::RemoveParticipantResponse, PsrpcError> {
        self.client
            .participant_server(participant)?
            .remove_participant(req)
    }

    fn mute_published_track(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::MuteRoomTrackRequest,
    ) -> Result<livekit::MuteRoomTrackResponse, PsrpcError> {
        self.client
            .participant_server(participant)?
            .mute_published_track(req)
    }

    fn update_participant(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::UpdateParticipantRequest,
    ) -> Result<livekit::ParticipantInfo, PsrpcError> {
        self.client
            .participant_server(participant)?
            .update_participant(req)
    }

    fn update_subscriptions(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::UpdateSubscriptionsRequest,
    ) -> Result<livekit::UpdateSubscriptionsResponse, PsrpcError> {
        self.client
            .participant_server(participant)?
            .update_subscriptions(req)
    }

    fn forward_participant(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::ForwardParticipantRequest,
    ) -> Result<livekit::ForwardParticipantResponse, PsrpcError> {
        self.client
            .participant_server(participant)?
            .forward_participant(req)
    }

    fn move_participant(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::MoveParticipantRequest,
    ) -> Result<livekit::MoveParticipantResponse, PsrpcError> {
        self.client
            .participant_server(participant)?
            .move_participant(req)
    }

    // the options (the request's timeout) are not read: see the description at the top
    fn perform_rpc(
        &self,
        participant: &ParticipantTopic,
        req: &livekit::PerformRpcRequest,
        _opts: &[RequestOption],
    ) -> Result<livekit::PerformRpcResponse, PsrpcError> {
        self.client
            .participant_server(participant)?
            .perform_rpc(req)
    }

    fn close(&self) {}
}

#[cfg(test)]
#[path = "rpc_test.rs"]
mod rpc_test;
