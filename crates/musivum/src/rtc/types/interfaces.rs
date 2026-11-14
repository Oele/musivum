// Ported from LiveKit, pkg/rtc/types/interfaces.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only `SubscribedCodecQuality` and the reasons for
// closing a participant and its signal connection are here so far; the interfaces follow when
// what they name of `pkg/sfu` is ported. `ParticipantCloseReason` and `SignallingCloseReason`
// are enums and their `String()` is `Display`, without the text for a number that is none of
// the reasons, which an enum cannot hold; `ToDisconnectReason` and `IsIntentionalDisconnect`
// are not here yet.

use std::fmt;

use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscribedCodecQuality {
    pub codec_mime: MimeType,
    pub quality: livekit::VideoQuality,
}

// ---------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ParticipantCloseReason {
    #[default]
    None,
    ClientRequestLeave,
    RoomManagerStop,
    VerifyFailed,
    JoinFailed,
    JoinTimeout,
    MessageBusFailed,
    PeerConnectionDisconnected,
    DuplicateIdentity,
    MigrationComplete,
    Stale,
    ServiceRequestRemoveParticipant,
    ServiceRequestDeleteRoom,
    SimulateMigration,
    SimulateNodeFailure,
    SimulateServerLeave,
    SimulateLeaveRequest,
    NegotiateFailed,
    MigrationRequested,
    PublicationError,
    SubscriptionError,
    DataChannelError,
    MigrateCodecMismatch,
    SignalSourceClose,
    RoomClosed,
    UserUnavailable,
    UserRejected,
    MoveFailed,
    AgentError,
    SipTrunkFailure,
}

impl fmt::Display for ParticipantCloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ParticipantCloseReason::None => "NONE",
            ParticipantCloseReason::ClientRequestLeave => "CLIENT_REQUEST_LEAVE",
            ParticipantCloseReason::RoomManagerStop => "ROOM_MANAGER_STOP",
            ParticipantCloseReason::VerifyFailed => "VERIFY_FAILED",
            ParticipantCloseReason::JoinFailed => "JOIN_FAILED",
            ParticipantCloseReason::JoinTimeout => "JOIN_TIMEOUT",
            ParticipantCloseReason::MessageBusFailed => "MESSAGE_BUS_FAILED",
            ParticipantCloseReason::PeerConnectionDisconnected => "PEER_CONNECTION_DISCONNECTED",
            ParticipantCloseReason::DuplicateIdentity => "DUPLICATE_IDENTITY",
            ParticipantCloseReason::MigrationComplete => "MIGRATION_COMPLETE",
            ParticipantCloseReason::Stale => "STALE",
            ParticipantCloseReason::ServiceRequestRemoveParticipant => {
                "SERVICE_REQUEST_REMOVE_PARTICIPANT"
            }
            ParticipantCloseReason::ServiceRequestDeleteRoom => "SERVICE_REQUEST_DELETE_ROOM",
            ParticipantCloseReason::SimulateMigration => "SIMULATE_MIGRATION",
            ParticipantCloseReason::SimulateNodeFailure => "SIMULATE_NODE_FAILURE",
            ParticipantCloseReason::SimulateServerLeave => "SIMULATE_SERVER_LEAVE",
            ParticipantCloseReason::SimulateLeaveRequest => "SIMULATE_LEAVE_REQUEST",
            ParticipantCloseReason::NegotiateFailed => "NEGOTIATE_FAILED",
            ParticipantCloseReason::MigrationRequested => "MIGRATION_REQUESTED",
            ParticipantCloseReason::PublicationError => "PUBLICATION_ERROR",
            ParticipantCloseReason::SubscriptionError => "SUBSCRIPTION_ERROR",
            ParticipantCloseReason::DataChannelError => "DATA_CHANNEL_ERROR",
            ParticipantCloseReason::MigrateCodecMismatch => "MIGRATE_CODEC_MISMATCH",
            ParticipantCloseReason::SignalSourceClose => "SIGNAL_SOURCE_CLOSE",
            ParticipantCloseReason::RoomClosed => "ROOM_CLOSED",
            ParticipantCloseReason::UserUnavailable => "USER_UNAVAILABLE",
            ParticipantCloseReason::UserRejected => "USER_REJECTED",
            ParticipantCloseReason::MoveFailed => "MOVE_FAILED",
            ParticipantCloseReason::AgentError => "AGENT_ERROR",
            ParticipantCloseReason::SipTrunkFailure => "SIP_TRUNK_FAILURE",
        })
    }
}

// ---------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SignallingCloseReason {
    #[default]
    Unknown,
    Migration,
    Resume,
    TransportFailure,
    FullReconnectPublicationError,
    FullReconnectSubscriptionError,
    FullReconnectDataChannelError,
    FullReconnectNegotiateFailed,
    ParticipantClose,
    DisconnectOnResume,
    DisconnectOnResumeNoMessages,
}

impl fmt::Display for SignallingCloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SignallingCloseReason::Unknown => "UNKNOWN",
            SignallingCloseReason::Migration => "MIGRATION",
            SignallingCloseReason::Resume => "RESUME",
            SignallingCloseReason::TransportFailure => "TRANSPORT_FAILURE",
            SignallingCloseReason::FullReconnectPublicationError => {
                "FULL_RECONNECT_PUBLICATION_ERROR"
            }
            SignallingCloseReason::FullReconnectSubscriptionError => {
                "FULL_RECONNECT_SUBSCRIPTION_ERROR"
            }
            SignallingCloseReason::FullReconnectDataChannelError => {
                "FULL_RECONNECT_DATA_CHANNEL_ERROR"
            }
            SignallingCloseReason::FullReconnectNegotiateFailed => {
                "FULL_RECONNECT_NEGOTIATE_FAILED"
            }
            SignallingCloseReason::ParticipantClose => "PARTICIPANT_CLOSE",
            SignallingCloseReason::DisconnectOnResume => "DISCONNECT_ON_RESUME",
            SignallingCloseReason::DisconnectOnResumeNoMessages => {
                "DISCONNECT_ON_RESUME_NO_MESSAGES"
            }
        })
    }
}
