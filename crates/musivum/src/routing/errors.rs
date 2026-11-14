// Ported from LiveKit, pkg/routing/errors.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are one enum, the one of the package, with
// the two of `signal.go` (`ErrSignalWriteFailed`, `ErrSignalMessageDropped`) at the end.
// Variants that are not in the original: `RequestTimedOut`, psrpc's `ErrRequestTimedOut`,
// which a request to a node that no server is registered for gets (see `bus.rs`); `Json`, the error
// of `encoding/json` that `ToStartSession` passes on, as its text; and `Service`, an error of
// the room manager (`pkg/service`) that the in-process calls in place of psrpc pass back,
// boxed because the service's errors hold this package's.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("could not find object")]
    NotFound,
    #[error("ip address is required and not set")]
    IpNotSet,
    #[error("handler not defined")]
    HandlerNotDefined,
    #[error("current node isn't the RTC node for the room")]
    IncorrectRtcNode,
    #[error("could not locate the node")]
    NodeNotFound,
    #[error("reached configured limit for node")]
    NodeLimitReached,
    #[error("invalid router message")]
    InvalidRouterMessage,
    #[error("channel closed")]
    ChannelClosed,
    #[error("channel is full")]
    ChannelFull,

    // errors when starting signal connection
    #[error("request channel closed")]
    RequestChannelClosed,
    #[error("could not migrate participant")]
    CouldNotMigrateParticipant,
    #[error("client info not set")]
    ClientInfoNotSet,

    // signal.go
    #[error("signal write failed")]
    SignalWriteFailed,
    #[error("signal message dropped")]
    SignalMessageDropped,

    #[error("request timed out")]
    RequestTimedOut,
    #[error("{0}")]
    Json(String),
    #[error(transparent)]
    Service(Box<crate::service::Error>),
}

impl From<crate::service::Error> for Error {
    fn from(err: crate::service::Error) -> Error {
        Error::Service(Box::new(err))
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Error {
        Error::Json(err.to_string())
    }
}
