// Ported from LiveKit, pkg/rtc/errors.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are one enum, the one of the package:
// errors that other files of `pkg/rtc` define are added to it when those files are ported.
// New: a variant for each other package whose errors the interfaces of `pkg/rtc/types` pass
// on, where the original returns that package's `error` as it is. The test at the end is
// new: the original has none.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("room has already closed")]
    RoomClosed,
    #[error("participant session is already closed")]
    ParticipantSessionClosed,
    #[error("no permissions to access the room")]
    PermissionDenied,
    #[error("room has exceeded its max participants")]
    MaxParticipantsExceeded,
    #[error("node has exceeded its configured limit")]
    LimitExceeded,
    #[error("a participant with the same identity is already in the room")]
    AlreadyJoined,
    #[error("data channel is not available")]
    DataChannelUnavailable,
    #[error("data channel buffer is full")]
    DataChannelBufferFull,
    #[error("transport failure")]
    TransportFailure,
    #[error("participant identity cannot be empty")]
    EmptyIdentity,
    #[error("participant ID cannot be empty")]
    EmptyParticipantId,
    #[error("VideoGrant is missing")]
    MissingGrants,
    #[error("internal error")]
    InternalError,

    // Track subscription related
    #[error("participant is not allowed to subscribe to this track")]
    NoTrackPermission,
    #[error("participant is not given permission to subscribe to tracks")]
    NoSubscribePermission,
    #[error("track cannot be found")]
    TrackNotFound,
    #[error("track not bound")]
    TrackNotBound,
    #[error("participant has exceeded its subscription limit")]
    SubscriptionLimitExceeded,

    #[error("participant is not given permission to subscribe to metrics")]
    NoSubscribeMetricsPermission,

    // errors of other packages that the methods of `types/interfaces.go` pass on: of the
    // sink a participant's messages are written to, of the signal handler and the limits on
    // a participant's metadata, of the down track a subscription makes, of the peer
    // connection, and what `PerformRpc` answers on its error channel
    #[error(transparent)]
    Routing(#[from] crate::routing::Error),
    #[error(transparent)]
    Signalling(#[from] crate::rtc::signalling::Error),
    #[error(transparent)]
    Sfu(#[from] crate::sfu::Error),
    #[error(transparent)]
    Webrtc(#[from] crate::webrtc::Error),
    #[error(transparent)]
    DataChannelRpc(#[from] crate::protocol::utils::DataChannelRpcError),
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no tests. The messages are the original's.
    #[test]
    fn errors_have_the_messages_of_the_original() {
        let table = [
            (Error::RoomClosed, "room has already closed"),
            (
                Error::ParticipantSessionClosed,
                "participant session is already closed",
            ),
            (Error::PermissionDenied, "no permissions to access the room"),
            (
                Error::MaxParticipantsExceeded,
                "room has exceeded its max participants",
            ),
            (
                Error::LimitExceeded,
                "node has exceeded its configured limit",
            ),
            (
                Error::AlreadyJoined,
                "a participant with the same identity is already in the room",
            ),
            (
                Error::DataChannelUnavailable,
                "data channel is not available",
            ),
            (Error::DataChannelBufferFull, "data channel buffer is full"),
            (Error::TransportFailure, "transport failure"),
            (Error::EmptyIdentity, "participant identity cannot be empty"),
            (Error::EmptyParticipantId, "participant ID cannot be empty"),
            (Error::MissingGrants, "VideoGrant is missing"),
            (Error::InternalError, "internal error"),
            (
                Error::NoTrackPermission,
                "participant is not allowed to subscribe to this track",
            ),
            (
                Error::NoSubscribePermission,
                "participant is not given permission to subscribe to tracks",
            ),
            (Error::TrackNotFound, "track cannot be found"),
            (Error::TrackNotBound, "track not bound"),
            (
                Error::SubscriptionLimitExceeded,
                "participant has exceeded its subscription limit",
            ),
            (
                Error::NoSubscribeMetricsPermission,
                "participant is not given permission to subscribe to metrics",
            ),
        ];
        assert_eq!(19, table.len());
        for (err, message) in table {
            assert_eq!(message, err.to_string());
        }
    }
}
