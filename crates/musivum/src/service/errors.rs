// Ported from LiveKit, pkg/service/errors.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are one enum, the one of the package, which
// also has the errors that auth.go, roomallocator.go, utils.go and wsprotocol.go define, and
// a variant for each kind of error from elsewhere that the ported files pass on. An error
// that the original wraps with a detail (`fmt.Errorf("%w: ...", ErrX, ...)`) is a variant of
// its own with the same text. Upstream these errors are psrpc errors with a code;
// `to_http` gives the status psrpc's `ToHttp` makes of that code. Not here: the errors of
// egress, ingress and SIP (`ErrEgressNotFound`, `ErrEgressNotConnected`,
// `ErrIngressNotConnected`, `ErrIngressNotFound`, `ErrIngressNonReusable`,
// `ErrSIPNotConnected`, `ErrSIPTrunkNotFound`, `ErrSIPDispatchRuleNotFound`,
// `ErrSIPParticipantNotFound`), which musivum leaves out.

use std::io;

use http::StatusCode;

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
    /// The error of `rtc::WebRtcConfig::new` (of `rtcconfig`, which is not `Clone`), as its
    /// text.
    #[error("{0}")]
    RtcConfig(String),
    /// An error of `pkg/routing` that the room manager passes on (of the router).
    #[error(transparent)]
    Routing(#[from] crate::routing::Error),
    /// An error of the protocol's `auth` that the room manager passes on (`ToJWT`).
    #[error(transparent)]
    Auth(#[from] crate::protocol::auth::Error),
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
            | Error::InvalidMessageType => StatusCode::INTERNAL_SERVER_ERROR,
            // psrpc.NotFound
            Error::ParticipantNotFound | Error::RoomNotFound | Error::TrackNotFound => {
                StatusCode::NOT_FOUND
            }
            // psrpc.FailedPrecondition
            Error::RemoteUnmuteNoteEnabled => StatusCode::PRECONDITION_FAILED,
            _ => return None,
        })
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
