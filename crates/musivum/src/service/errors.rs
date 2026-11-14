// Ported from LiveKit, pkg/service/errors.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are one enum, the one of the package, which
// also has the errors that auth.go, roomallocator.go, utils.go, wsprotocol.go and
// rtcservice.go define, and
// a variant for each kind of error from elsewhere that the ported files pass on. An error
// that the original wraps with a detail (`fmt.Errorf("%w: ...", ErrX, ...)`) is a variant of
// its own with the same text. Upstream these errors are psrpc errors with a code;
// `to_http` gives the status psrpc's `ToHttp` makes of that code. Not here: the errors of
// egress, ingress and SIP (`ErrEgressNotFound`, `ErrIngressNotConnected`, `ErrIngressNotFound`,
// `ErrIngressNonReusable`, `ErrSIPNotConnected`, `ErrSIPTrunkNotFound`,
// `ErrSIPDispatchRuleNotFound`, `ErrSIPParticipantNotFound`), which musivum leaves out;
// `ErrEgressNotConnected` is here, as the room service answers with it when there is no
// egress. For the room service (roomservice.rs) there are also an error made as a Twirp error,
// an error that came back through psrpc (`routing::PsrpcError`), `psrpc_error`, which finds the
// psrpc error in an error as Go's `errors.As` does, and the conversion of an error into what
// psrpc carries to a client (`From<Error> for routing::PsrpcError`).

use std::io;

use http::StatusCode;

use super::TwirpError;
use crate::protocol::utils as putils;
use crate::routing::{self, PsrpcError, PsrpcErrorCode};
use crate::rtc;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("identity cannot be empty")]
    IdentityEmpty,
    #[error("participant sid cannot be empty")]
    ParticipantSidEmpty,
    #[error("name length exceeds limits")]
    NameExceedsLimits,
    #[error("metadata size exceeds limits")]
    MetadataExceedsLimits,
    #[error("attribute size exceeds limits")]
    AttributeExceedsLimits,
    #[error("no room name")]
    NoRoomName,
    #[error("room name length exceeds limits")]
    RoomNameExceedsLimits,
    #[error("participant identity length exceeds limits")]
    ParticipantIdentityExceedsLimits,
    #[error("destination room cannot be the same as source room")]
    DestinationSameAsSourceRoom,
    #[error("operation cannot be completed")]
    OperationFailed,
    #[error("participant does not exist")]
    ParticipantNotFound,
    #[error("requested room does not exist")]
    RoomNotFound,
    #[error("could not lock room")]
    RoomLockFailed,
    #[error("could not unlock room, lock token does not match")]
    RoomUnlockFailed,
    #[error("remote unmute not enabled")]
    RemoteUnmuteNoteEnabled,
    #[error("track is not found")]
    TrackNotFound,
    #[error("api_key is required to use webhooks")]
    WebHookMissingApiKey,
    #[error("invalid message type")]
    InvalidMessageType,
    #[error("no connect request")]
    NoConnectRequest,
    #[error("no connect response")]
    NoConnectResponse,
    #[error("destination identity is required")]
    DestinationIdentityRequired,
    #[error("egress not connected (redis required)")]
    EgressNotConnected,

    // ErrRoomNameExceedsLimits and ErrParticipantIdentityExceedsLimits as ValidateConnectRequest
    // wraps them (utils.go)
    #[error("room name length exceeds limits: max length {0}")]
    RoomNameExceedsLimitsMaxLength(i64),
    #[error("participant identity length exceeds limits: max length {0}")]
    ParticipantIdentityExceedsLimitsMaxLength(i64),

    // roomallocator.go, an InvalidArgument of psrpc
    #[error("unknown room configuration in create room request")]
    UnknownRoomConfiguration,

    // auth.go
    #[error("permissions denied")]
    PermissionDenied,
    #[error("invalid authorization header. Must start with Bearer ")]
    MissingAuthorization,
    #[error("invalid authorization token")]
    InvalidAuthorizationToken,
    /// ErrInvalidAuthorizationToken with what the verifier said of the token.
    #[error("invalid authorization token: {0}")]
    InvalidAuthorizationTokenDetail(String),
    #[error("invalid API key")]
    InvalidApiKey,

    // utils.go. ErrGzipReadFailed is only ever returned with the error of the decompressor
    // after it. ErrRequestBodyTooLarge, of RequestBodyLimiter, is not here.
    #[error("cannot read decompressed data: {0}")]
    GzipReadFailed(String),
    #[error("decompressed data too large")]
    GzipTooLarge,

    // wsprotocol.go
    #[error("signal message exceeds size limit of {0} bytes")]
    SignalMessageExceedsSizeLimit(i64),

    /// gorilla/websocket's `*CloseError`: the client closed the connection with this code
    /// (1005 when its close frame had no status, 1006 when the connection ended without
    /// one), with gorilla's text.
    #[error("{}", close_error_text(*code, text))]
    WebSocketClose { code: u16, text: String },
    /// An error of the connection a WebSocket runs on, as the server's socket reports it.
    #[error("{message}")]
    Io {
        kind: io::ErrorKind,
        message: String,
    },
    /// A binary frame that is not a message of the protocol (`proto.Unmarshal`).
    #[error("{0}")]
    ProtoDecode(String),
    /// A text frame that is not a message in the protocol's JSON form (`protojson`).
    #[error("{0}")]
    ProtoJson(String),

    /// An error of `pkg/rtc` that the original returns from here (`rtc.ErrPermissionDenied`,
    /// `rtc.ErrLimitExceeded`).
    #[error(transparent)]
    Rtc(#[from] rtc::Error),

    // roommanager.go and signal.go, which make these where they return them (`errors.New`,
    // `errors.Wrap`)
    #[error("could not restart closed participant")]
    CouldNotRestartClosedParticipant,
    #[error("could not restart participant")]
    CouldNotRestartParticipant,
    #[error("not implemented")]
    NotImplemented,
    #[error("no API keys configured")]
    NoApiKeysConfigured,
    #[error("failed to read participant from session: {0}")]
    ReadParticipantFromSession(String),
    /// An error of `pkg/routing` that the room manager passes on (of the router).
    #[error(transparent)]
    Routing(#[from] crate::routing::Error),
    /// An error of the protocol's `auth` that the room manager passes on (`ToJWT`).
    #[error(transparent)]
    Auth(#[from] crate::protocol::auth::Error),

    // rtcservice.go: errJoinRequestTooLarge, and the errors it makes where it returns them
    #[error("join_request is required")]
    JoinRequestRequired,
    #[error("cannot decode attributes")]
    CannotDecodeAttributes,
    #[error("cannot base64 decode wrapped join request")]
    CannotBase64DecodeWrappedJoinRequest,
    #[error("cannot unmarshal wrapped join request")]
    CannotUnmarshalWrappedJoinRequest,
    #[error("join request too large")]
    JoinRequestTooLarge,
    #[error("cannot unmarshal join request")]
    CannotUnmarshalJoinRequest,
    #[error("cannot read decompressed join request")]
    CannotReadDecompressedJoinRequest,
    #[error("timed out while waiting for signal response")]
    SignalResponseTimedOut,
    #[error("connection closed by media")]
    ConnectionClosedByMedia,
    /// `fmt.Errorf("unexpected message type: %T", msg)`, with the Go type of the message.
    #[error("unexpected message type: {0}")]
    UnexpectedMessageType(String),
    /// gorilla/websocket's `HandshakeError`: why a request could not be upgraded.
    #[error("{0}")]
    Handshake(String),

    // roomservice.go
    /// An error made as a Twirp error (`twirp.NewError`, `twirp.InvalidArgumentError`,
    /// `twirpAuthError`), which the Twirp server answers with as it is.
    #[error("{0}")]
    Twirp(TwirpError),
    /// An error that came back through psrpc, from the server of a room or a participant
    /// (`routing::TypedRoomClient`, `TypedParticipantClient`), or `psrpc.ErrUnimplemented`.
    #[error(transparent)]
    Psrpc(#[from] PsrpcError),
}

impl Error {
    /// psrpc's `ToHttp` of the error's code, for the errors that are psrpc errors upstream
    /// (those of errors.go and roomallocator.go); `None` for the others, which the original
    /// does not take for psrpc errors.
    pub fn to_http(&self) -> Option<StatusCode> {
        Some(match self {
            // psrpc.InvalidArgument
            Error::IdentityEmpty
            | Error::ParticipantSidEmpty
            | Error::NameExceedsLimits
            | Error::MetadataExceedsLimits
            | Error::AttributeExceedsLimits
            | Error::NoRoomName
            | Error::RoomNameExceedsLimits
            | Error::ParticipantIdentityExceedsLimits
            | Error::DestinationSameAsSourceRoom
            | Error::WebHookMissingApiKey
            | Error::NoConnectRequest
            | Error::NoConnectResponse
            | Error::DestinationIdentityRequired
            | Error::UnknownRoomConfiguration => StatusCode::BAD_REQUEST,
            // psrpc.Internal
            Error::OperationFailed
            | Error::RoomLockFailed
            | Error::RoomUnlockFailed
            | Error::InvalidMessageType
            | Error::EgressNotConnected => StatusCode::INTERNAL_SERVER_ERROR,
            // psrpc.NotFound
            Error::ParticipantNotFound | Error::RoomNotFound | Error::TrackNotFound => {
                StatusCode::NOT_FOUND
            }
            // psrpc.FailedPrecondition
            Error::RemoteUnmuteNoteEnabled => StatusCode::PRECONDITION_FAILED,
            // psrpc.ErrRequestTimedOut, a psrpc.DeadlineExceeded
            Error::Routing(routing::Error::RequestTimedOut) => StatusCode::SERVICE_UNAVAILABLE,
            // an error of the room manager, passed back as psrpc passes it
            Error::Routing(routing::Error::Service(err)) => return err.to_http(),
            Error::Psrpc(err) => err.code().to_http(),
            _ => return None,
        })
    }

    /// The psrpc error in the error, as Go's `errors.As(err, &psrpcErr)` finds it: its code
    /// and its own text (for an error that wraps one, the text of the wrapped error). `None`
    /// for an error that is not a psrpc error upstream and wraps none.
    pub fn psrpc_error(&self) -> Option<PsrpcError> {
        let code = match self {
            // psrpc.InvalidArgument
            Error::IdentityEmpty
            | Error::ParticipantSidEmpty
            | Error::NameExceedsLimits
            | Error::MetadataExceedsLimits
            | Error::AttributeExceedsLimits
            | Error::NoRoomName
            | Error::RoomNameExceedsLimits
            | Error::ParticipantIdentityExceedsLimits
            | Error::DestinationSameAsSourceRoom
            | Error::WebHookMissingApiKey
            | Error::NoConnectRequest
            | Error::NoConnectResponse
            | Error::DestinationIdentityRequired
            | Error::UnknownRoomConfiguration => PsrpcErrorCode::InvalidArgument,
            // psrpc.Internal
            Error::OperationFailed
            | Error::RoomLockFailed
            | Error::RoomUnlockFailed
            | Error::InvalidMessageType
            | Error::EgressNotConnected => PsrpcErrorCode::Internal,
            // psrpc.NotFound
            Error::ParticipantNotFound | Error::RoomNotFound | Error::TrackNotFound => {
                PsrpcErrorCode::NotFound
            }
            // psrpc.FailedPrecondition
            Error::RemoteUnmuteNoteEnabled => PsrpcErrorCode::FailedPrecondition,

            // `fmt.Errorf("%w: max length %d", ...)` of a psrpc error
            Error::RoomNameExceedsLimitsMaxLength(_) => {
                return Error::RoomNameExceedsLimits.psrpc_error();
            }
            Error::ParticipantIdentityExceedsLimitsMaxLength(_) => {
                return Error::ParticipantIdentityExceedsLimits.psrpc_error();
            }

            Error::Psrpc(err) => return Some(err.clone()),
            Error::Routing(err) => match err {
                // an error of the room manager, passed back as psrpc passes it
                routing::Error::Service(err) => return err.psrpc_error(),
                // psrpc.ErrRequestTimedOut
                routing::Error::RequestTimedOut => PsrpcErrorCode::DeadlineExceeded,
                _ => return None,
            },
            Error::Rtc(err) => match err {
                // `PsrpcError()` of the RPC's error
                rtc::Error::DataChannelRpc(err) => match err.code {
                    putils::DATA_CHANNEL_RPC_APPLICATION_ERROR => PsrpcErrorCode::Internal,
                    putils::DATA_CHANNEL_RPC_CONNECTION_TIMEOUT => PsrpcErrorCode::Canceled,
                    putils::DATA_CHANNEL_RPC_RESPONSE_TIMEOUT => PsrpcErrorCode::Canceled,
                    putils::DATA_CHANNEL_RPC_RECIPIENT_DISCONNECTED => PsrpcErrorCode::Unavailable,
                    putils::DATA_CHANNEL_RPC_RESPONSE_PAYLOAD_TOO_LARGE => {
                        PsrpcErrorCode::MalformedResponse
                    }
                    putils::DATA_CHANNEL_RPC_SEND_FAILED => PsrpcErrorCode::Internal,
                    putils::DATA_CHANNEL_RPC_UNSUPPORTED_METHOD => PsrpcErrorCode::InvalidArgument,
                    putils::DATA_CHANNEL_RPC_RECIPIENT_NOT_FOUND => PsrpcErrorCode::NotFound,
                    putils::DATA_CHANNEL_RPC_REQUEST_PAYLOAD_TOO_LARGE => {
                        PsrpcErrorCode::MalformedRequest
                    }
                    putils::DATA_CHANNEL_RPC_UNSUPPORTED_SERVER => PsrpcErrorCode::Unimplemented,
                    putils::DATA_CHANNEL_RPC_UNSUPPORTED_VERSION => PsrpcErrorCode::Unimplemented,
                    _ => PsrpcErrorCode::Internal,
                },
                // participant.go's errors of `SupportsMoving`
                rtc::Error::MoveOldClientVersion | rtc::Error::CannotBeMoved { .. } => {
                    PsrpcErrorCode::FailedPrecondition
                }
                _ => return None,
            },
            _ => return None,
        };
        Some(PsrpcError::new(code, self.to_string()))
    }
}

/// What psrpc's server makes of a handler's error for the client: the psrpc error in it, with
/// its code and text, or else the error's text with the code `Unknown`. This is how the room
/// manager's errors go back through the bus (`routing::RoomServerImpl`,
/// `ParticipantServerImpl`).
impl From<Error> for PsrpcError {
    fn from(err: Error) -> PsrpcError {
        match err.psrpc_error() {
            Some(err) => err,
            None => PsrpcError::new(PsrpcErrorCode::Unknown, err.to_string()),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io {
            kind: err.kind(),
            message: err.to_string(),
        }
    }
}

// The codes of a close frame (RFC 6455, section 7.4.1), as gorilla/websocket names them.
pub const CLOSE_NORMAL_CLOSURE: u16 = 1000;
pub const CLOSE_GOING_AWAY: u16 = 1001;
pub const CLOSE_PROTOCOL_ERROR: u16 = 1002;
pub const CLOSE_UNSUPPORTED_DATA: u16 = 1003;
pub const CLOSE_NO_STATUS_RECEIVED: u16 = 1005;
pub const CLOSE_ABNORMAL_CLOSURE: u16 = 1006;
pub const CLOSE_INVALID_FRAME_PAYLOAD_DATA: u16 = 1007;
pub const CLOSE_POLICY_VIOLATION: u16 = 1008;
pub const CLOSE_MESSAGE_TOO_BIG: u16 = 1009;
pub const CLOSE_MANDATORY_EXTENSION: u16 = 1010;
pub const CLOSE_INTERNAL_SERVER_ERR: u16 = 1011;
pub const CLOSE_TLS_HANDSHAKE: u16 = 1015;

// What gorilla/websocket's CloseError says of itself.
fn close_error_text(code: u16, text: &str) -> String {
    let name = match code {
        CLOSE_NORMAL_CLOSURE => " (normal)",
        CLOSE_GOING_AWAY => " (going away)",
        CLOSE_PROTOCOL_ERROR => " (protocol error)",
        CLOSE_UNSUPPORTED_DATA => " (unsupported data)",
        CLOSE_NO_STATUS_RECEIVED => " (no status)",
        CLOSE_ABNORMAL_CLOSURE => " (abnormal closure)",
        CLOSE_INVALID_FRAME_PAYLOAD_DATA => " (invalid payload data)",
        CLOSE_POLICY_VIOLATION => " (policy violation)",
        CLOSE_MESSAGE_TOO_BIG => " (message too big)",
        CLOSE_MANDATORY_EXTENSION => " (mandatory extension missing)",
        CLOSE_INTERNAL_SERVER_ERR => " (internal server error)",
        CLOSE_TLS_HANDSHAKE => " (TLS handshake error)",
        _ => "",
    };
    let mut s = format!("websocket: close {code}{name}");
    if !text.is_empty() {
        s.push_str(": ");
        s.push_str(text);
    }
    s
}
