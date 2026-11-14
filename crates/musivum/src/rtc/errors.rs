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

    // uptrackmanager.go
    #[error("either participant identity or SID needed")]
    SubscriptionPermissionNeedsId,

    // mediatracksubscriptions.go (`errAlreadySubscribed`, which datatrack.go returns too)
    #[error("already subscribed")]
    AlreadySubscribed,
    // mediatracksubscriptions.go (`errNotFound`)
    #[error("not found")]
    NotFound,

    // datatrack.go (`errReceiverClosed`)
    #[error("datatrack is closed")]
    ReceiverClosed,

    // mediatrackreceiver.go (`ErrNotOpen`, `ErrNoReceiver`). subscriptionmanager.go defines no
    // errors of its own: it tells these two apart,
    #[error("track is not open")]
    NotOpen,
    #[error("cannot subscribe without a receiver in place")]
    NoReceiver,
    // and its WaitUntilSubscribed returns Go's context.DeadlineExceeded when its time is up
    #[error("context deadline exceeded")]
    DeadlineExceeded,

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
    // of the supervisor of a participant's publications, which `OperationMonitor::check`
    // returns
    #[error(transparent)]
    Supervisor(#[from] crate::rtc::supervisor::Error),
    // of package `transport` (transport/handler.go), which the methods of its `Handler`
    // return where nothing handles them
    #[error(transparent)]
    Transport(#[from] crate::rtc::transport::Error),

    // transport.go
    #[error("no ICE transport")]
    NoIceTransport,
    #[error("ICE restart without local SDP settled")]
    IceRestartWithoutLocalSdp,
    #[error("ICE restart on closed peer connection")]
    IceRestartOnClosedPeerConnection,
    #[error("no transceiver")]
    NoTransceiver,
    #[error("no sender")]
    NoSender,
    #[error("mid not found")]
    MidNotFound,
    #[error("not using synchronous local candidates mode")]
    NotSynchronousLocalCandidatesMode,
    #[error("no remote description")]
    NoRemoteDescription,
    #[error("no local description")]
    NoLocalDescription,
    #[error("invalid sdp fragment")]
    InvalidSdpFragment,
    #[error("could not get bundle mid")]
    NoBundleMid,
    #[error("media mid does not match bundle mid")]
    MidMismatch,
    #[error("ice credential mismatch")]
    IceCredentialMismatch,
    // the errors transport.go makes where it returns them (`errors.New`)
    #[error("data tracks not enabled")]
    DataTracksNotEnabled,
    #[error("unknown data channel label")]
    UnknownDataChannelLabel,
    #[error("cannot forward non user data packet")]
    CannotForwardNonUserDataPacket,
    // what `proto.Unmarshal` says of a data packet that does not decode, as text
    #[error("{0}")]
    ProtoUnmarshal(String),
    // `errors.Wrap(err, message)` of github.com/pkg/errors, with which transport.go says what
    // it was doing when the connection failed it; the cause is kept, as `errors.Is` finds it
    #[error("{message}: {cause}")]
    Wrap {
        message: &'static str,
        cause: Box<Error>,
    },
    // errors of other packages that transport.go passes on: of the SDP helpers of LiveKit's
    // protocol, and of the writer of a data channel
    #[error(transparent)]
    Sdp(#[from] crate::protocol::sdp::Error),
    #[error(transparent)]
    DataChannelWriter(#[from] crate::sfu::datachannel::Error),
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
