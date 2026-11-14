// Ported from LiveKit, pkg/rtc/types/interfaces.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The seventeen interfaces are traits, each with a fake
// for tests (also `ChangeNotifier` and `OperationMonitor`, which upstream has no fake of).
// Everything of `pkg/rtc` is written against these signatures, so they follow rules, the
// same ones everywhere:
// - Names, order and comments are the original's; receivers are `&self`. An embedded
//   interface is a supertrait.
// - An object that several hold (whatever is behind an interface of this file or of another
//   package, a `*sfu.DownTrack`, pion's sender and transceiver) is an `Arc<dyn Trait>`
//   (`Arc<sfu::DownTrack>`), by value both ways. It is in an `Option` where an
//   implementation returns nil, or a caller upstream passes nil or checks for it
//   (`get_published_track`, `SubscribedTrack::media_track`, `swap_response_sink`). The two
//   `add_subscriber` return `Ok(None)` for the original's "no track and no error", which
//   the subscription manager asks for.
// - A message of the protocol is borrowed when taken (`&livekit::X`; `Option<&livekit::X>`
//   where a caller passes nil or the implementation checks for it): who keeps it copies
//   it. Returned, it is owned (`livekit::X`, `Option<..>` where nil is returned), as the
//   implementations build or copy it for the call. It is an `Arc<..>` where the original
//   hands out the object it keeps, which it replaces and never changes: `get_client_info`,
//   `claim_grants`, `subscription_permission`, `get_data_blob`, `get_all_data_blob`, and
//   the cached messages of `get_cached_reliable_data_message`.
// - An identifier (`livekit::TrackId`, ...) is taken by reference and returned by value. A
//   `string` is `&str` and `String`, a `[]T` is `&[T]` and `Vec<T>` (a `[]*livekit.X` is a
//   slice of messages), a `[]byte` is `&[u8]` and `Vec<u8>` (`Bytes` as the field of
//   `DataMessageCache`), a map is `&HashMap` and `HashMap`. Numbers, enumerations,
//   `MimeType`, `TimedVersion`, `Time`, `Duration`, and the structs the original passes by
//   value (`AddTrackParams`, `MoveToRoomParams`, `sfu::DownTrackState`) are passed by value.
//   An `int` is a `usize` where it counts, an `i32` where it is a layer or the type of a
//   WebSocket message.
// - `error` is `Result<_, Error>` with the `Error` of `pkg/rtc` (`rtc/errors.rs`), which has
//   a transparent variant for each package whose errors a method here passes on. Several
//   results are a tuple in the original's order, inside the `Result` when the last of them
//   is the error. An `error` that is handed in is `&Error` (`on_track_subscribe_failed`), or
//   a `Result<(), Error>` where nil says "no error" (the callback of `add_on_bind`).
// - A callback is a `Box<dyn Fn(..) + Send + Sync>`, in an `Option` where a caller passes
//   nil (upstream's tests too) or the implementation checks for it. Its arguments follow
//   the rules above.
// - A channel that is closed as a signal (`disconnected`) is a
//   `tokio_util::sync::CancellationToken`, cancelled where the original closes the channel.
//   The two channels of `perform_rpc` are the sending halves of tokio `mpsc` channels.
// - Where upstream's code or tests assert the type behind an interface (`x.(*MediaTrack)`,
//   `x.(*typesfakes.FakeLocalParticipant)`), the trait has `Any` as a supertrait: the
//   `Arc<dyn Trait>` converts to an `Arc<dyn Any + Send + Sync>`, which has `downcast`.
//   Those are `Participant`, `LocalParticipant`, `LocalParticipantHelper`,
//   `LocalParticipantListener`, `ParticipantTelemetryListener`, `MediaTrack`,
//   `LocalMediaTrack`, `DataDownTrack`, `SubscribedTrack` and `OperationMonitor`.
// What a test meets in the fakes that it does not meet in counterfeiter's:
// - A callback, the packets of `write_subscriber_rtcp` and the down track of
//   `handle_receiver_report` are not recorded (`NotRecorded` stands at their place); a stub
//   gets them.
// - Where a method returns an object that cannot be nil here, the fake has no zero value
//   to give and the test says what is returned before the code under test asks:
//   `get_participant_listener`, `get_telemetry_listener`, `get_buffer_factory`, `get_pacer`,
//   `get_data_track_transport`, `add_track_local`, `add_transceiver_from_track_local`,
//   `SubscribedTrack::subscriber`, `WebsocketClient::next_reader`.
// What is new, because Rust has no counterpart:
// - `AsLocalParticipant` and `AsLocalMediaTrack`, supertraits of `Participant` and
//   `MediaTrack` with one method each: Go's `p.(types.LocalParticipant)` and
//   `track.(types.LocalMediaTrack)`, from one interface to another. An implementation
//   answers `Some(self)` or `None`; the fakes of the two local traits answer `Some`, the
//   fakes of the other two `None`, as the assertions on counterfeiter's fakes do.
// - `impl ChangeNotifier for utils::ChangeNotifier`, at the end: upstream it is one by
//   having the methods.
// What differs otherwise:
// - Not here: `LocalParticipant.GetLoggerResolver` (the deferred fields of LiveKit's zap
//   logger, which the logger here does not have), `GetReporter` and `GetReporterResolver`
//   (`roomobs`, LiveKit's observability, which is not ported). A comment stands at the
//   place of each.
// - The enumerations are enums; their `String()` is `Display`, without the text for a
//   number that is none of the values, which an enum cannot hold.
// - `OperationMonitorData`, an `any` upstream, is an enum of the three things that are
//   posted: nothing, a bool, a local media track.
// - `proto.Message` is `routing::Message` and is taken by value, as the sink and the signal
//   handler take it. `webrtc.TrackLocal`, `*webrtc.RTPSender` and `*webrtc.RTPTransceiver`
//   are the traits of `crate::webrtc`, `webrtc.SessionDescription` is the one of
//   `webrtc::peerconnection`, `[]rtcp.Packet` is `&[Box<dyn rtcp::Packet>]`,
//   `map[string]any` is `sfu::DebugInfo`.
// - `WebsocketClient`, which is the service's and stands on a WebSocket library, returns
//   `std::io::Error`: the package's `Error` can be compared and cloned, an I/O error cannot.
//   `io.Reader` is a `Box<dyn std::io::Read + Send>`.
// - A field of a struct that is an interface is an `Option<Arc<dyn Trait>>`: upstream builds
//   these structs field by field, from a zero value that has nil there.
// - `NullLocalParticipantListener` embeds `NullParticipantListener` upstream and has its
//   methods written out here. The three null listeners implement their traits.
// - A parameter the original leaves unnamed has the name it has in the implementation.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{IceConnectionInfo, ProtocolVersion};
use crate::go::time::{Duration, Time};
use crate::protocol::auth;
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::datatrack;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils::TimedVersion;
use crate::routing::{Message, MessageSink};
use crate::rtc::Error;
use crate::sfu;
use crate::sfu::buffer;
use crate::sfu::pacer;
use crate::telemetry;
use crate::webrtc;
use crate::webrtc::peerconnection::SessionDescription;
use crate::webrtc::rtcp;
use crate::webrtc::rtcp::receiver_report::ReceiverReport;

#[cfg_attr(test, musivum_fakes::fake)]
pub trait WebsocketClient: Send + Sync {
    fn read_message(&self) -> io::Result<(i32, Vec<u8>)>;
    fn next_reader(&self) -> io::Result<(i32, Box<dyn io::Read + Send>)>;
    fn write_message(&self, message_type: i32, data: &[u8]) -> io::Result<()>;
    fn write_control(&self, message_type: i32, data: &[u8], deadline: Time) -> io::Result<()>;
    fn set_read_deadline(&self, deadline: Time) -> io::Result<()>;
    fn close(&self) -> io::Result<()>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AddSubscriberParams {
    pub all_tracks: bool,
    pub track_ids: Vec<livekit::TrackId>,
}

// ---------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum MigrateState {
    #[default]
    Init,
    Sync,
    Complete,
}

impl fmt::Display for MigrateState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            MigrateState::Init => "MIGRATE_STATE_INIT",
            MigrateState::Sync => "MIGRATE_STATE_SYNC",
            MigrateState::Complete => "MIGRATE_STATE_COMPLETE",
        })
    }
}

// ---------------------------------------------

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

impl ParticipantCloseReason {
    pub fn to_disconnect_reason(self) -> livekit::DisconnectReason {
        match self {
            ParticipantCloseReason::ClientRequestLeave
            | ParticipantCloseReason::SimulateLeaveRequest => {
                livekit::DisconnectReason::ClientInitiated
            }
            ParticipantCloseReason::RoomManagerStop => livekit::DisconnectReason::ServerShutdown,
            ParticipantCloseReason::VerifyFailed
            | ParticipantCloseReason::JoinFailed
            | ParticipantCloseReason::JoinTimeout
            | ParticipantCloseReason::MessageBusFailed => {
                // expected to be connected but is not
                livekit::DisconnectReason::JoinFailure
            }
            ParticipantCloseReason::PeerConnectionDisconnected => {
                livekit::DisconnectReason::ConnectionTimeout
            }
            ParticipantCloseReason::DuplicateIdentity | ParticipantCloseReason::Stale => {
                livekit::DisconnectReason::DuplicateIdentity
            }
            ParticipantCloseReason::MigrationRequested
            | ParticipantCloseReason::MigrationComplete
            | ParticipantCloseReason::SimulateMigration => livekit::DisconnectReason::Migration,
            ParticipantCloseReason::ServiceRequestRemoveParticipant => {
                livekit::DisconnectReason::ParticipantRemoved
            }
            ParticipantCloseReason::ServiceRequestDeleteRoom => {
                livekit::DisconnectReason::RoomDeleted
            }
            ParticipantCloseReason::SimulateNodeFailure
            | ParticipantCloseReason::SimulateServerLeave => {
                livekit::DisconnectReason::ServerShutdown
            }
            ParticipantCloseReason::NegotiateFailed
            | ParticipantCloseReason::PublicationError
            | ParticipantCloseReason::SubscriptionError
            | ParticipantCloseReason::DataChannelError
            | ParticipantCloseReason::MigrateCodecMismatch
            | ParticipantCloseReason::MoveFailed => livekit::DisconnectReason::StateMismatch,
            ParticipantCloseReason::SignalSourceClose => livekit::DisconnectReason::SignalClose,
            ParticipantCloseReason::RoomClosed => livekit::DisconnectReason::RoomClosed,
            ParticipantCloseReason::UserUnavailable => livekit::DisconnectReason::UserUnavailable,
            ParticipantCloseReason::UserRejected => livekit::DisconnectReason::UserRejected,
            ParticipantCloseReason::AgentError => livekit::DisconnectReason::AgentError,
            ParticipantCloseReason::SipTrunkFailure => livekit::DisconnectReason::SipTrunkFailure,
            // the other types will map to unknown reason
            _ => livekit::DisconnectReason::UnknownReason,
        }
    }
}

// ---------------------------------------------

// IsIntentionalDisconnect reports whether a disconnect reason represents an
// intentional/expected closure (client leaving, admin action, room teardown,
// migration, etc.) as opposed to a connection failure.
pub fn is_intentional_disconnect(reason: livekit::DisconnectReason) -> bool {
    matches!(
        reason,
        livekit::DisconnectReason::ClientInitiated
            | livekit::DisconnectReason::ServerShutdown
            | livekit::DisconnectReason::DuplicateIdentity
            | livekit::DisconnectReason::Migration
            | livekit::DisconnectReason::ParticipantRemoved
            | livekit::DisconnectReason::RoomDeleted
            | livekit::DisconnectReason::RoomClosed
            | livekit::DisconnectReason::SipTrunkFailure
    )
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

// ---------------------------------------------

pub const PARTICIPANT_CLOSE_KEY_NORMAL: &str = "normal";
pub const PARTICIPANT_CLOSE_KEY_WHIP: &str = "whip";

// ---------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum RoomCloseReason {
    #[default]
    Unknown,
    ApiDelete,
    IdleTimeout,
    ServerShutdown,
    Superseded,
    OpenFailed,
}

impl fmt::Display for RoomCloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RoomCloseReason::Unknown => "UNKNOWN",
            RoomCloseReason::ApiDelete => "API_DELETE",
            RoomCloseReason::IdleTimeout => "IDLE_TIMEOUT",
            RoomCloseReason::ServerShutdown => "SERVER_SHUTDOWN",
            RoomCloseReason::Superseded => "SUPERSEDED",
            RoomCloseReason::OpenFailed => "OPEN_FAILED",
        })
    }
}

impl RoomCloseReason {
    pub fn to_proto(self) -> livekit::RoomEndReason {
        match self {
            RoomCloseReason::ApiDelete => livekit::RoomEndReason::RoomEndApiDelete,
            RoomCloseReason::IdleTimeout => livekit::RoomEndReason::RoomEndIdleTimeout,
            RoomCloseReason::ServerShutdown => livekit::RoomEndReason::RoomEndServerShutdown,
            RoomCloseReason::Superseded => livekit::RoomEndReason::RoomEndSuperseded,
            RoomCloseReason::OpenFailed => livekit::RoomEndReason::RoomEndOpenFailed,
            _ => livekit::RoomEndReason::RoomEndUnknown,
        }
    }

    // ToParticipantCloseReason gives the reason participants are closed with when the
    // room closes for this reason, so the two can never disagree.
    pub fn to_participant_close_reason(self) -> ParticipantCloseReason {
        match self {
            RoomCloseReason::ApiDelete => ParticipantCloseReason::ServiceRequestDeleteRoom,
            RoomCloseReason::IdleTimeout | RoomCloseReason::Superseded => {
                ParticipantCloseReason::RoomClosed
            }
            RoomCloseReason::ServerShutdown => ParticipantCloseReason::RoomManagerStop,
            _ => ParticipantCloseReason::None,
        }
    }
}

// ---------------------------------------------

/// Go's `p.(types.LocalParticipant)` on a `types.Participant`: the participant as a local
/// one, if it is one. New, see the header.
pub trait AsLocalParticipant {
    fn as_local_participant(self: Arc<Self>) -> Option<Arc<dyn LocalParticipant>>;
}

#[cfg_attr(test, musivum_fakes::fake(extends()))]
pub trait Participant: AsLocalParticipant + Any + Send + Sync {
    fn id(&self) -> livekit::ParticipantId;
    fn identity(&self) -> livekit::ParticipantIdentity;
    fn state(&self) -> livekit::participant_info::State;
    fn connected_at(&self) -> Time;
    fn close_reason(&self) -> ParticipantCloseReason;
    fn kind(&self) -> livekit::participant_info::Kind;
    fn kind_details(&self) -> Vec<livekit::participant_info::KindDetail>;
    fn is_recorder(&self) -> bool;
    fn is_dependent(&self) -> bool;
    fn is_agent(&self) -> bool;

    fn get_logger(&self) -> Logger;

    fn can_skip_broadcast(&self) -> bool;
    fn version(&self) -> TimedVersion;
    fn to_proto(&self) -> livekit::ParticipantInfo;
    fn to_proto_with_version(&self) -> (livekit::ParticipantInfo, TimedVersion);

    fn is_publisher(&self) -> bool;
    fn get_published_track(&self, track_id: &livekit::TrackId) -> Option<Arc<dyn MediaTrack>>;
    fn get_published_tracks(&self) -> Vec<Arc<dyn MediaTrack>>;
    fn remove_published_track(&self, track: Arc<dyn MediaTrack>, is_expected_to_resume: bool);

    fn get_published_data_tracks(&self) -> Vec<Arc<dyn DataTrack>>;
    fn get_published_data_track(&self, handle: u16) -> Option<Arc<dyn DataTrack>>;
    fn remove_published_data_track(&self, track: Arc<dyn DataTrack>);

    fn get_audio_level(&self) -> (f64, bool);

    // HasPermission checks permission of the subscriber by identity. Returns true if subscriber is allowed to subscribe
    // to the track with trackID
    fn has_permission(
        &self,
        track_id: &livekit::TrackId,
        sub_identity: &livekit::ParticipantIdentity,
    ) -> bool;

    // permissions
    fn hidden(&self) -> bool;

    fn migrate_state(&self) -> MigrateState;

    fn close(
        &self,
        send_leave: bool,
        reason: ParticipantCloseReason,
        is_expected_to_resume: bool,
    ) -> Result<(), Error>;
    fn is_closed(&self) -> bool;
    fn is_disconnected(&self) -> bool;

    fn subscription_permission(
        &self,
    ) -> (Option<Arc<livekit::SubscriptionPermission>>, TimedVersion);

    // updates from remotes
    #[allow(clippy::type_complexity)]
    fn update_subscription_permission(
        &self,
        subscription_permission: Option<&livekit::SubscriptionPermission>,
        timed_version: TimedVersion,
        resolver_by_sid: Option<
            Box<dyn Fn(&livekit::ParticipantId) -> Option<Arc<dyn LocalParticipant>> + Send + Sync>,
        >,
    ) -> Result<(), Error>;

    fn debug_info(&self) -> sfu::DebugInfo;

    fn handle_received_data_track_message(
        &self,
        data: &[u8],
        packet: &datatrack::Packet,
        arrival_time: i64,
    );

    fn get_participant_listener(&self) -> Arc<dyn ParticipantListener>;

    fn add_data_blob(&self, data_blob: &livekit::DataBlob);
    fn get_data_blob(&self, key: Option<&livekit::DataBlobKey>) -> Option<Arc<livekit::DataBlob>>;
}

// The fake of a participant is not a local participant, the fake of a local one is.
#[cfg(test)]
impl AsLocalParticipant for FakeParticipant {
    fn as_local_participant(self: Arc<Self>) -> Option<Arc<dyn LocalParticipant>> {
        None
    }
}

#[cfg(test)]
impl AsLocalParticipant for FakeLocalParticipant {
    fn as_local_participant(self: Arc<Self>) -> Option<Arc<dyn LocalParticipant>> {
        Some(self)
    }
}

// -------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AddTrackParams {
    pub stereo: bool,
    pub red: bool,
}

#[derive(Clone, Default)]
pub struct MoveToRoomParams {
    pub room_name: livekit::RoomName,
    pub participant_id: livekit::ParticipantId,
    pub listener: Option<Arc<dyn LocalParticipantListener>>,
    pub telemetry_listener: Option<Arc<dyn ParticipantTelemetryListener>>,
    pub helper: Option<Arc<dyn LocalParticipantHelper>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataMessageCache {
    pub data: Bytes,
    pub sender_id: livekit::ParticipantId,
    pub seq: u32,
    pub dest_identities: Vec<livekit::ParticipantIdentity>,
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait LocalParticipantHelper: Any + Send + Sync {
    fn resolve_media_track(
        &self,
        lp: Arc<dyn LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> MediaResolverResult;
    fn resolve_data_track(
        &self,
        lp: Arc<dyn LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> DataResolverResult;
    fn get_participant_info(
        &self,
        p_id: &livekit::ParticipantId,
    ) -> Option<livekit::ParticipantInfo>;
    fn get_region_settings(&self, ip: &str) -> Option<livekit::RegionSettings>;
    fn get_subscriber_forwarder_state(
        &self,
        p: Arc<dyn LocalParticipant>,
    ) -> Result<HashMap<livekit::TrackId, livekit::RtpForwarderState>, Error>;
    fn should_regress_codec(&self) -> bool;
    fn get_cached_reliable_data_message(
        &self,
        seqs: &HashMap<livekit::ParticipantId, u32>,
    ) -> Vec<Arc<DataMessageCache>>;
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait LocalParticipant: Participant {
    fn telemetry_guard(&self) -> Option<Arc<telemetry::ReferenceGuard>>;
    fn get_telemetry_listener(&self) -> Arc<dyn ParticipantTelemetryListener>;

    // getters
    fn get_country(&self) -> String;
    fn get_trailer(&self) -> Vec<u8>;
    // Not here: GetLoggerResolver() logger.DeferredFieldResolver, the deferred fields of
    // LiveKit's zap logger, which the logger here does not have.
    // Not here: GetReporter() roomobs.ParticipantSessionReporter, of LiveKit's
    // observability, which is not ported.
    // Not here: GetReporterResolver() roomobs.ParticipantReporterResolver, the same.
    fn get_adaptive_stream(&self) -> bool;
    fn get_enable_start_at_desired_quality(&self) -> bool;
    fn protocol_version(&self) -> ProtocolVersion;
    fn supports_sync_stream_id(&self) -> bool;
    fn supports_transceiver_reuse(&self, mt: Arc<dyn MediaTrack>) -> bool;
    fn is_using_single_peer_connection(&self) -> bool;
    fn is_ready(&self) -> bool;
    fn active_at(&self) -> Time;
    fn disconnected(&self) -> CancellationToken;
    fn is_connection_canceled(&self, close_reason: ParticipantCloseReason) -> bool;
    fn is_idle(&self) -> bool;
    fn subscriber_as_primary(&self) -> bool;
    fn get_client_info(&self) -> Option<Arc<livekit::ClientInfo>>;
    fn get_client_configuration(&self) -> Option<livekit::ClientConfiguration>;
    fn get_buffer_factory(&self) -> Arc<buffer::Factory>;
    fn get_playout_delay_config(&self) -> Option<livekit::PlayoutDelay>;
    fn get_pending_track(&self, track_id: &livekit::TrackId) -> Option<livekit::TrackInfo>;
    fn get_ice_connection_info(&self) -> Vec<IceConnectionInfo>;
    fn has_ice_connected(&self) -> bool;
    fn has_connected(&self) -> bool;
    fn get_enabled_publish_codecs(&self) -> Vec<livekit::Codec>;
    fn get_publisher_ice_session_ufrag(&self) -> Result<String, Error>;
    fn supports_moving(&self) -> Result<(), Error>;
    fn get_last_reliable_sequence(&self, migrate_out: bool) -> u32;
    fn is_warp_enabled(&self) -> bool;

    fn swap_response_sink(&self, sink: Option<Arc<dyn MessageSink>>, reason: SignallingCloseReason);
    fn get_response_sink(&self) -> Option<Arc<dyn MessageSink>>;
    fn close_signal_connection(&self, reason: SignallingCloseReason);
    fn update_last_seen_signal(&self);
    fn set_signal_source_valid(&self, valid: bool);
    fn handle_signal_source_close(&self);

    // updates
    fn update_metadata(
        &self,
        update: &livekit::UpdateParticipantMetadata,
        from_admin: bool,
    ) -> Result<(), Error>;
    fn set_name(&self, name: &str);
    fn set_metadata(&self, metadata: &str);
    fn set_attributes(&self, attributes: &HashMap<String, String>);
    fn update_audio_track(&self, update: &livekit::UpdateLocalAudioTrack) -> Result<(), Error>;
    fn update_video_track(&self, update: &livekit::UpdateLocalVideoTrack) -> Result<(), Error>;

    // permissions
    fn claim_grants(&self) -> Arc<auth::ClaimGrants>;
    fn token_expires_at(&self) -> Time;
    fn set_permission(&self, permission: Option<&livekit::ParticipantPermission>) -> bool;
    fn can_publish(&self) -> bool;
    fn can_publish_source(&self, source: livekit::TrackSource) -> bool;
    fn can_subscribe(&self) -> bool;
    fn can_publish_data(&self) -> bool;

    // PeerConnection
    fn handle_ice_trickle(&self, trickle_request: &livekit::TrickleRequest);
    fn handle_offer(&self, sd: &livekit::SessionDescription) -> Result<(), Error>;
    fn get_answer(&self) -> Result<(SessionDescription, u32), Error>;
    fn handle_ice_trickle_sdp_fragment(&self, sdp_fragment: &str) -> Result<(), Error>;
    fn handle_ice_restart_sdp_fragment(&self, sdp_fragment: &str) -> Result<String, Error>;
    fn add_track(&self, req: &livekit::AddTrackRequest);
    fn set_track_muted(
        &self,
        mute: &livekit::MuteTrackRequest,
        from_admin: bool,
    ) -> Option<livekit::TrackInfo>;

    fn handle_answer(&self, sd: &livekit::SessionDescription);
    fn negotiate(&self, force: bool);
    fn ice_restart(&self, ice_config: Option<&livekit::IceConfig>);
    #[allow(clippy::type_complexity)]
    fn add_track_local(
        &self,
        track_local: Arc<dyn webrtc::TrackLocal>,
        params: AddTrackParams,
    ) -> Result<(Arc<dyn webrtc::RtpSender>, Arc<dyn webrtc::RtpTransceiver>), Error>;
    #[allow(clippy::type_complexity)]
    fn add_transceiver_from_track_local(
        &self,
        track_local: Arc<dyn webrtc::TrackLocal>,
        params: AddTrackParams,
    ) -> Result<(Arc<dyn webrtc::RtpSender>, Arc<dyn webrtc::RtpTransceiver>), Error>;
    fn remove_track_local(&self, sender: Arc<dyn webrtc::RtpSender>) -> Result<(), Error>;

    fn write_subscriber_rtcp(
        &self,
        #[cfg_attr(test, fake(skip))] pkts: &[Box<dyn rtcp::Packet>],
    ) -> Result<(), Error>;

    // subscriptions
    fn subscribe_to_track(&self, track_id: &livekit::TrackId, is_sync: bool);
    fn unsubscribe_from_track(&self, track_id: &livekit::TrackId);
    fn update_subscribed_track_settings(
        &self,
        track_id: &livekit::TrackId,
        settings: &livekit::UpdateTrackSettings,
    );
    fn get_subscribed_tracks(&self) -> Vec<Arc<dyn SubscribedTrack>>;
    fn get_subscribed_data_tracks(&self) -> Vec<Arc<dyn DataDownTrack>>;
    fn is_track_name_subscribed(
        &self,
        publisher_identity: &livekit::ParticipantIdentity,
        track_name: &str,
    ) -> bool;
    fn subscribe_to_data_track(&self, track_id: &livekit::TrackId);
    fn unsubscribe_from_data_track(&self, track_id: &livekit::TrackId);
    fn update_data_track_subscription_options(
        &self,
        track_id: &livekit::TrackId,
        subscription_options: Option<&livekit::DataTrackSubscriptionOptions>,
    );
    fn verify(&self) -> bool;
    fn verify_subscribe_participant_info(&self, p_id: &livekit::ParticipantId, version: u32);
    // WaitUntilSubscribed waits until all subscriptions have been settled, or if the timeout
    // has been reached. If the timeout expires, it will return an error.
    fn wait_until_subscribed(&self, timeout: Duration) -> Result<(), Error>;
    fn stop_and_get_subscribed_tracks_forwarder_state(
        &self,
    ) -> HashMap<livekit::TrackId, livekit::RtpForwarderState>;
    fn supports_codec_change(&self) -> bool;

    // returns list of participant identities that the current participant is subscribed to
    fn get_subscribed_participants(&self) -> Vec<livekit::ParticipantId>;
    fn is_subscribed_to(&self, sid: &livekit::ParticipantId) -> bool;

    fn get_connection_quality(&self) -> livekit::ConnectionQualityInfo;

    // server sent messages
    fn send_join_response(&self, join_response: &livekit::JoinResponse) -> Result<(), Error>;
    fn send_participant_update(
        &self,
        participants: &[livekit::ParticipantInfo],
    ) -> Result<(), Error>;
    fn send_speaker_update(
        &self,
        speakers: &[livekit::SpeakerInfo],
        force: bool,
    ) -> Result<(), Error>;
    fn send_data_message(
        &self,
        kind: livekit::data_packet::Kind,
        data: &[u8],
        sender_id: &livekit::ParticipantId,
        seq: u32,
    ) -> Result<(), Error>;
    fn send_data_message_unlabeled(
        &self,
        data: &[u8],
        use_raw: bool,
        sender: &livekit::ParticipantIdentity,
    ) -> Result<(), Error>;
    fn send_room_update(&self, room: &livekit::Room) -> Result<(), Error>;
    fn send_connection_quality_update(
        &self,
        update: &livekit::ConnectionQualityUpdate,
    ) -> Result<(), Error>;
    fn send_subscription_permission_update(
        &self,
        publisher_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        allowed: bool,
    ) -> Result<(), Error>;
    fn send_refresh_token(&self, token: &str) -> Result<(), Error>;
    fn handle_reconnect_and_send_response(
        &self,
        reconnect_reason: livekit::ReconnectReason,
        reconnect_response: &livekit::ReconnectResponse,
        recently_disconnected_participants: &[livekit::ParticipantInfo],
    ) -> Result<(), Error>;
    fn issue_full_reconnect(&self, reason: ParticipantCloseReason);
    fn send_room_moved_response(&self, moved: &livekit::RoomMovedResponse) -> Result<(), Error>;
    fn send_data_track_subscriber_handles(
        &self,
        handles: &HashMap<u32, livekit::data_track_subscriber_handles::PublishedDataTrack>,
    ) -> Result<(), Error>;
    fn get_recently_disconnected_participants(&self) -> Vec<livekit::ParticipantInfo>;

    #[allow(clippy::type_complexity)]
    fn add_on_close(
        &self,
        key: &str,
        callback: Option<Box<dyn Fn(Arc<dyn LocalParticipant>) + Send + Sync>>,
    );
    fn on_claims_changed(&self, callback: Box<dyn Fn(Arc<dyn LocalParticipant>) + Send + Sync>);

    fn handle_receiver_report(
        &self,
        #[cfg_attr(test, fake(skip))] dt: &sfu::DownTrack,
        report: &ReceiverReport,
    );

    // session migration
    fn maybe_start_migration(
        &self,
        force: bool,
        on_start: Option<Box<dyn Fn() + Send + Sync>>,
    ) -> bool;
    fn notify_migration(&self);
    fn set_migrate_state(&self, s: MigrateState);
    fn set_migrate_info(
        &self,
        previous_offer: Option<&SessionDescription>,
        previous_answer: Option<&SessionDescription>,
        media_tracks: &[livekit::TrackPublishedResponse],
        data_channels: &[livekit::DataChannelInfo],
        data_channel_receive_state: &[livekit::DataChannelReceiveState],
        data_tracks: &[livekit::PublishDataTrackResponse],
    );
    fn is_reconnect(&self) -> bool;
    fn is_migration(&self) -> bool;
    fn leave_session(&self, leave: Option<Box<dyn Fn() + Send + Sync>>);
    fn join_session(&self, join: Option<Box<dyn Fn() + Send + Sync>>);
    fn move_to_room(&self, params: MoveToRoomParams);

    fn update_media_rtt(&self, rtt: u32);
    fn update_signaling_rtt(&self, rtt: u32);

    fn cache_down_track(
        &self,
        track_id: &livekit::TrackId,
        rtp_transceiver: Arc<dyn webrtc::RtpTransceiver>,
        down_track_state: sfu::DownTrackState,
    );
    fn uncache_down_track(&self, rtp_transceiver: Arc<dyn webrtc::RtpTransceiver>);
    fn get_cached_down_track(
        &self,
        track_id: &livekit::TrackId,
    ) -> (Option<Arc<dyn webrtc::RtpTransceiver>>, sfu::DownTrackState);

    fn set_ice_config(&self, ice_config: Option<&livekit::IceConfig>);
    fn get_ice_config(&self) -> Option<livekit::IceConfig>;
    #[allow(clippy::type_complexity)]
    fn on_ice_config_changed(
        &self,
        callback: Box<dyn Fn(Arc<dyn LocalParticipant>, &livekit::IceConfig) + Send + Sync>,
    );

    fn update_subscribed_quality(
        &self,
        node_id: &livekit::NodeId,
        track_id: &livekit::TrackId,
        max_qualities: &[SubscribedCodecQuality],
    ) -> Result<(), Error>;
    fn update_subscribed_audio_codecs(
        &self,
        node_id: &livekit::NodeId,
        track_id: &livekit::TrackId,
        codecs: &[livekit::SubscribedAudioCodec],
    ) -> Result<(), Error>;
    fn update_media_loss(
        &self,
        node_id: &livekit::NodeId,
        track_id: &livekit::TrackId,
        fractional_loss: u32,
    ) -> Result<(), Error>;

    // down stream bandwidth management
    fn set_subscriber_allow_pause(&self, allow_pause: bool);
    fn set_subscriber_channel_capacity(&self, channel_capacity: i64);

    fn get_pacer(&self) -> Arc<dyn pacer::Pacer>;

    fn get_disable_sender_report_pass_through(&self) -> bool;

    fn handle_metrics(
        &self,
        sender_participant_id: &livekit::ParticipantId,
        batch: &livekit::MetricsBatch,
    ) -> Result<(), Error>;
    fn handle_update_subscriptions(
        &self,
        track_ids: &[livekit::TrackId],
        participant_tracks: &[livekit::ParticipantTracks],
        subscribe: bool,
    );
    fn handle_update_subscription_permission(
        &self,
        subscription_permission: &livekit::SubscriptionPermission,
    ) -> Result<(), Error>;
    fn handle_sync_state(&self, sync_state: &livekit::SyncState) -> Result<(), Error>;
    fn handle_simulate_scenario(
        &self,
        simulate_scenario: &livekit::SimulateScenario,
    ) -> Result<(), Error>;
    fn handle_leave_request(&self, reason: ParticipantCloseReason);

    fn handle_publish_data_track_request(&self, req: &livekit::PublishDataTrackRequest);
    fn handle_unpublish_data_track_request(&self, req: &livekit::UnpublishDataTrackRequest);
    fn handle_update_data_subscription(&self, req: &livekit::UpdateDataSubscription);
    fn handle_store_data_blob_request(&self, req: &livekit::StoreDataBlobRequest);
    fn handle_get_data_blob_request(&self, req: &livekit::GetDataBlobRequest);
    fn process_get_data_blob_request(
        &self,
        req: &livekit::GetDataBlobRequest,
        publisher: Option<Arc<dyn Participant>>,
    );

    fn handle_signal_message(&self, msg: Message) -> Result<(), Error>;

    fn perform_rpc(
        &self,
        req: &livekit::PerformRpcRequest,
        result_ch: mpsc::Sender<String>,
        error_ch: mpsc::Sender<Error>,
    );

    fn get_data_track_transport(&self) -> Arc<dyn DataTrackTransport>;

    fn clear_participant_listener(&self);

    fn get_next_subscribed_data_track_handle(&self) -> u16;

    fn get_all_data_blob(&self) -> Vec<Arc<livekit::DataBlob>>;
}

// ---------------------------------------------

#[cfg_attr(test, musivum_fakes::fake)]
pub trait ParticipantListener: Send + Sync {
    fn on_participant_update(&self, p: Arc<dyn Participant>);
    fn on_track_published(&self, p: Arc<dyn Participant>, track: Arc<dyn MediaTrack>);
    fn on_track_updated(&self, p: Arc<dyn Participant>, track: Arc<dyn MediaTrack>);
    fn on_track_unpublished(&self, p: Arc<dyn Participant>, track: Arc<dyn MediaTrack>);
    fn on_data_track_published(&self, p: Arc<dyn Participant>, track: Arc<dyn DataTrack>);
    fn on_data_track_unpublished(&self, p: Arc<dyn Participant>, track: Arc<dyn DataTrack>);
    fn on_data_track_message(
        &self,
        p: Arc<dyn Participant>,
        data: &[u8],
        packet: &datatrack::Packet,
    );
    fn on_metrics(&self, p: Arc<dyn Participant>, dp: &livekit::DataPacket);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NullParticipantListener {}

impl ParticipantListener for NullParticipantListener {
    fn on_participant_update(&self, _p: Arc<dyn Participant>) {}
    fn on_track_published(&self, _p: Arc<dyn Participant>, _track: Arc<dyn MediaTrack>) {}
    fn on_track_updated(&self, _p: Arc<dyn Participant>, _track: Arc<dyn MediaTrack>) {}
    fn on_track_unpublished(&self, _p: Arc<dyn Participant>, _track: Arc<dyn MediaTrack>) {}
    fn on_data_track_published(&self, _p: Arc<dyn Participant>, _track: Arc<dyn DataTrack>) {}
    fn on_data_track_unpublished(&self, _p: Arc<dyn Participant>, _track: Arc<dyn DataTrack>) {}
    fn on_data_track_message(
        &self,
        _p: Arc<dyn Participant>,
        _data: &[u8],
        _packet: &datatrack::Packet,
    ) {
    }
    fn on_metrics(&self, _p: Arc<dyn Participant>, _dp: &livekit::DataPacket) {}
}

// ---------------------------------------------

#[cfg_attr(test, musivum_fakes::fake)]
pub trait LocalParticipantListener: ParticipantListener + Any {
    fn on_state_change(&self, p: Arc<dyn LocalParticipant>);
    fn on_subscriber_ready(&self, p: Arc<dyn LocalParticipant>);
    fn on_migrate_state_change(&self, p: Arc<dyn LocalParticipant>, migrate_state: MigrateState);
    fn on_data_message(
        &self,
        p: Arc<dyn LocalParticipant>,
        kind: livekit::data_packet::Kind,
        dp: &livekit::DataPacket,
    );
    fn on_data_message_unlabeled(&self, p: Arc<dyn LocalParticipant>, data: &[u8]);
    fn on_subscribe_status_changed(
        &self,
        p: Arc<dyn LocalParticipant>,
        publisher_id: &livekit::ParticipantId,
        subscribed: bool,
    );
    fn on_update_subscriptions(
        &self,
        p: Arc<dyn LocalParticipant>,
        track_ids: &[livekit::TrackId],
        participant_tracks: &[livekit::ParticipantTracks],
        subscribe: bool,
    );
    fn on_update_subscription_permission(
        &self,
        p: Arc<dyn LocalParticipant>,
        subscription_permission: &livekit::SubscriptionPermission,
    ) -> Result<(), Error>;
    fn on_update_data_subscriptions(
        &self,
        p: Arc<dyn LocalParticipant>,
        req: &livekit::UpdateDataSubscription,
    );
    fn on_store_data_blob(&self, p: Arc<dyn LocalParticipant>, data_blob: &livekit::DataBlob);
    fn on_get_data_blob(&self, p: Arc<dyn LocalParticipant>, req: &livekit::GetDataBlobRequest);
    fn on_sync_state(
        &self,
        p: Arc<dyn LocalParticipant>,
        state: &livekit::SyncState,
    ) -> Result<(), Error>;
    fn on_simulate_scenario(
        &self,
        p: Arc<dyn LocalParticipant>,
        simulate_scenario: &livekit::SimulateScenario,
    ) -> Result<(), Error>;
    fn on_leave(&self, p: Arc<dyn LocalParticipant>, close_reason: ParticipantCloseReason);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NullLocalParticipantListener {
    pub null_participant_listener: NullParticipantListener,
}

// The methods of the embedded `NullParticipantListener`.
impl ParticipantListener for NullLocalParticipantListener {
    fn on_participant_update(&self, p: Arc<dyn Participant>) {
        self.null_participant_listener.on_participant_update(p);
    }
    fn on_track_published(&self, p: Arc<dyn Participant>, track: Arc<dyn MediaTrack>) {
        self.null_participant_listener.on_track_published(p, track);
    }
    fn on_track_updated(&self, p: Arc<dyn Participant>, track: Arc<dyn MediaTrack>) {
        self.null_participant_listener.on_track_updated(p, track);
    }
    fn on_track_unpublished(&self, p: Arc<dyn Participant>, track: Arc<dyn MediaTrack>) {
        self.null_participant_listener
            .on_track_unpublished(p, track);
    }
    fn on_data_track_published(&self, p: Arc<dyn Participant>, track: Arc<dyn DataTrack>) {
        self.null_participant_listener
            .on_data_track_published(p, track);
    }
    fn on_data_track_unpublished(&self, p: Arc<dyn Participant>, track: Arc<dyn DataTrack>) {
        self.null_participant_listener
            .on_data_track_unpublished(p, track);
    }
    fn on_data_track_message(
        &self,
        p: Arc<dyn Participant>,
        data: &[u8],
        packet: &datatrack::Packet,
    ) {
        self.null_participant_listener
            .on_data_track_message(p, data, packet);
    }
    fn on_metrics(&self, p: Arc<dyn Participant>, dp: &livekit::DataPacket) {
        self.null_participant_listener.on_metrics(p, dp);
    }
}

impl LocalParticipantListener for NullLocalParticipantListener {
    fn on_state_change(&self, _p: Arc<dyn LocalParticipant>) {}
    fn on_subscriber_ready(&self, _p: Arc<dyn LocalParticipant>) {}
    fn on_migrate_state_change(&self, _p: Arc<dyn LocalParticipant>, _migrate_state: MigrateState) {
    }
    fn on_data_message(
        &self,
        _p: Arc<dyn LocalParticipant>,
        _kind: livekit::data_packet::Kind,
        _dp: &livekit::DataPacket,
    ) {
    }
    fn on_data_message_unlabeled(&self, _p: Arc<dyn LocalParticipant>, _data: &[u8]) {}
    fn on_subscribe_status_changed(
        &self,
        _p: Arc<dyn LocalParticipant>,
        _publisher_id: &livekit::ParticipantId,
        _subscribed: bool,
    ) {
    }
    fn on_update_subscriptions(
        &self,
        _p: Arc<dyn LocalParticipant>,
        _track_ids: &[livekit::TrackId],
        _participant_tracks: &[livekit::ParticipantTracks],
        _subscribe: bool,
    ) {
    }
    fn on_update_subscription_permission(
        &self,
        _p: Arc<dyn LocalParticipant>,
        _subscription_permission: &livekit::SubscriptionPermission,
    ) -> Result<(), Error> {
        Ok(())
    }
    fn on_update_data_subscriptions(
        &self,
        _p: Arc<dyn LocalParticipant>,
        _req: &livekit::UpdateDataSubscription,
    ) {
    }
    fn on_store_data_blob(&self, _p: Arc<dyn LocalParticipant>, _data_blob: &livekit::DataBlob) {}
    fn on_get_data_blob(&self, _p: Arc<dyn LocalParticipant>, _req: &livekit::GetDataBlobRequest) {}
    fn on_sync_state(
        &self,
        _p: Arc<dyn LocalParticipant>,
        _state: &livekit::SyncState,
    ) -> Result<(), Error> {
        Ok(())
    }
    fn on_simulate_scenario(
        &self,
        _p: Arc<dyn LocalParticipant>,
        _simulate_scenario: &livekit::SimulateScenario,
    ) -> Result<(), Error> {
        Ok(())
    }
    fn on_leave(&self, _p: Arc<dyn LocalParticipant>, _close_reason: ParticipantCloseReason) {}
}

// ---------------------------------------------

#[cfg_attr(test, musivum_fakes::fake)]
pub trait ParticipantTelemetryListener: Any + Send + Sync {
    fn on_track_publish_requested(
        &self,
        p_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        ti: &livekit::TrackInfo,
        should_send_event: bool,
    );
    fn on_track_published(
        &self,
        p_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        ti: &livekit::TrackInfo,
        should_send_event: bool,
    );
    fn on_track_unpublished(
        &self,
        p_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        ti: &livekit::TrackInfo,
        was_published_locally: bool,
        should_send_event: bool,
    );
    fn on_track_subscribe_requested(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo);
    fn on_track_subscribed(
        &self,
        p_id: &livekit::ParticipantId,
        ti: &livekit::TrackInfo,
        publisher_info: &livekit::ParticipantInfo,
        should_send_event: bool,
    );
    fn on_track_unsubscribed(
        &self,
        p_id: &livekit::ParticipantId,
        ti: &livekit::TrackInfo,
        should_send_event: bool,
    );
    fn on_track_subscribe_failed(
        &self,
        p_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        err: &Error,
        is_user_error: bool,
    );
    fn on_track_subscribe_stream_started(
        &self,
        p_id: &livekit::ParticipantId,
        ti: &livekit::TrackInfo,
        elapsed: Duration,
    );
    fn on_track_muted(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo);
    fn on_track_unmuted(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo);
    fn on_track_published_update(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo);
    fn on_track_max_subscribed_video_quality(
        &self,
        p_id: &livekit::ParticipantId,
        ti: &livekit::TrackInfo,
        mime: MimeType,
        max_quality: livekit::VideoQuality,
    );
    fn on_track_publish_rtp_stats(
        &self,
        p_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        layer: i32,
        stats: &livekit::RtpStats,
    );
    fn on_track_subscribe_rtp_stats(
        &self,
        p_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        stats: &livekit::RtpStats,
    );

    fn on_track_stats(&self, key: &telemetry::StatsKey, stat: &livekit::AnalyticsStat);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NullParticipantTelemetryListener {}

impl ParticipantTelemetryListener for NullParticipantTelemetryListener {
    fn on_track_publish_requested(
        &self,
        _p_id: &livekit::ParticipantId,
        _identity: &livekit::ParticipantIdentity,
        _ti: &livekit::TrackInfo,
        _should_send_event: bool,
    ) {
    }
    fn on_track_published(
        &self,
        _p_id: &livekit::ParticipantId,
        _identity: &livekit::ParticipantIdentity,
        _ti: &livekit::TrackInfo,
        _should_send_event: bool,
    ) {
    }
    fn on_track_unpublished(
        &self,
        _p_id: &livekit::ParticipantId,
        _identity: &livekit::ParticipantIdentity,
        _ti: &livekit::TrackInfo,
        _was_published_locally: bool,
        _should_send_event: bool,
    ) {
    }
    fn on_track_subscribe_requested(
        &self,
        _p_id: &livekit::ParticipantId,
        _ti: &livekit::TrackInfo,
    ) {
    }
    fn on_track_subscribed(
        &self,
        _p_id: &livekit::ParticipantId,
        _ti: &livekit::TrackInfo,
        _publisher_info: &livekit::ParticipantInfo,
        _should_send_event: bool,
    ) {
    }
    fn on_track_unsubscribed(
        &self,
        _p_id: &livekit::ParticipantId,
        _ti: &livekit::TrackInfo,
        _should_send_event: bool,
    ) {
    }
    fn on_track_subscribe_failed(
        &self,
        _p_id: &livekit::ParticipantId,
        _track_id: &livekit::TrackId,
        _err: &Error,
        _is_user_error: bool,
    ) {
    }
    fn on_track_subscribe_stream_started(
        &self,
        _p_id: &livekit::ParticipantId,
        _ti: &livekit::TrackInfo,
        _elapsed: Duration,
    ) {
    }
    fn on_track_muted(&self, _p_id: &livekit::ParticipantId, _ti: &livekit::TrackInfo) {}
    fn on_track_unmuted(&self, _p_id: &livekit::ParticipantId, _ti: &livekit::TrackInfo) {}
    fn on_track_published_update(&self, _p_id: &livekit::ParticipantId, _ti: &livekit::TrackInfo) {}
    fn on_track_max_subscribed_video_quality(
        &self,
        _p_id: &livekit::ParticipantId,
        _ti: &livekit::TrackInfo,
        _mime: MimeType,
        _max_quality: livekit::VideoQuality,
    ) {
    }
    fn on_track_publish_rtp_stats(
        &self,
        _p_id: &livekit::ParticipantId,
        _track_id: &livekit::TrackId,
        _mime_type: MimeType,
        _layer: i32,
        _stats: &livekit::RtpStats,
    ) {
    }
    fn on_track_subscribe_rtp_stats(
        &self,
        _p_id: &livekit::ParticipantId,
        _track_id: &livekit::TrackId,
        _mime_type: MimeType,
        _stats: &livekit::RtpStats,
    ) {
    }

    fn on_track_stats(&self, _key: &telemetry::StatsKey, _stat: &livekit::AnalyticsStat) {}
}

// ---------------------------------------------

// Room is a container of participants, and can provide room-level actions
#[cfg_attr(test, musivum_fakes::fake)]
pub trait Room: Send + Sync {
    fn name(&self) -> livekit::RoomName;
    fn id(&self) -> livekit::RoomId;
    fn remove_participant(
        &self,
        identity: &livekit::ParticipantIdentity,
        p_id: &livekit::ParticipantId,
        reason: ParticipantCloseReason,
    );
    fn update_subscriptions(
        &self,
        participant: Arc<dyn LocalParticipant>,
        track_ids: &[livekit::TrackId],
        participant_tracks: &[livekit::ParticipantTracks],
        subscribe: bool,
    );
    fn resolve_media_track_for_subscriber(
        &self,
        sub: Arc<dyn LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> MediaResolverResult;
    fn resolve_data_track_for_subscriber(
        &self,
        sub: Arc<dyn LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> DataResolverResult;
    fn get_local_participants(&self) -> Vec<Arc<dyn LocalParticipant>>;
    fn is_data_message_user_packet_duplicate(&self, ip: &livekit::UserPacket) -> bool;
}

/// Go's `track.(types.LocalMediaTrack)` on a `types.MediaTrack`: the track as a local one,
/// if it is one. New, see the header.
pub trait AsLocalMediaTrack {
    fn as_local_media_track(self: Arc<Self>) -> Option<Arc<dyn LocalMediaTrack>>;
}

// MediaTrack represents a media track
#[cfg_attr(test, musivum_fakes::fake(extends()))]
pub trait MediaTrack: AsLocalMediaTrack + Any + Send + Sync {
    fn id(&self) -> livekit::TrackId;
    fn kind(&self) -> livekit::TrackType;
    fn name(&self) -> String;
    fn source(&self) -> livekit::TrackSource;
    fn stream(&self) -> String;

    fn update_track_info(&self, ti: &livekit::TrackInfo);
    fn update_audio_track(&self, update: &livekit::UpdateLocalAudioTrack);
    fn update_video_track(&self, update: &livekit::UpdateLocalVideoTrack);
    fn to_proto(&self) -> livekit::TrackInfo;

    fn publisher_id(&self) -> livekit::ParticipantId;
    fn publisher_identity(&self) -> livekit::ParticipantIdentity;
    fn publisher_version(&self) -> u32;
    fn logger(&self) -> Logger;

    fn is_muted(&self) -> bool;
    fn set_muted(&self, muted: bool);

    fn get_audio_level(&self) -> (f64, bool);

    fn close(&self, is_expected_to_resume: bool);
    fn is_open(&self) -> bool;

    // callbacks
    fn add_on_close(&self, f: Option<Box<dyn Fn(bool) + Send + Sync>>);

    // subscribers
    fn add_subscriber(
        &self,
        participant: Arc<dyn LocalParticipant>,
    ) -> Result<Option<Arc<dyn SubscribedTrack>>, Error>;
    fn remove_subscriber(
        &self,
        participant_id: &livekit::ParticipantId,
        is_expected_to_resume: bool,
    );
    fn is_subscriber(&self, sub_id: &livekit::ParticipantId) -> bool;
    fn revoke_disallowed_subscribers(
        &self,
        allowed_subscriber_identities: &[livekit::ParticipantIdentity],
    ) -> Vec<livekit::ParticipantIdentity>;
    fn get_all_subscribers(&self) -> Vec<livekit::ParticipantId>;
    fn get_num_subscribers(&self) -> usize;
    fn on_track_subscribed(&self);

    // returns quality information that's appropriate for width & height
    fn get_quality_for_dimension(
        &self,
        mime_type: MimeType,
        width: u32,
        height: u32,
    ) -> livekit::VideoQuality;

    // returns temporal layer that's appropriate for fps
    fn get_temporal_layer_for_spatial_fps(
        &self,
        mime_type: MimeType,
        spatial: i32,
        fps: u32,
    ) -> i32;

    fn receivers(&self) -> Vec<Arc<dyn sfu::TrackReceiver>>;
    fn clear_all_receivers(&self, is_expected_to_resume: bool);

    fn is_encrypted(&self) -> bool;
    fn has_packet_trailer(&self) -> bool;
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait LocalMediaTrack: MediaTrack {
    fn restart(&self);

    fn has_signal_cid(&self, cid: &str) -> bool;
    fn has_sdp_cid(&self, cid: &str) -> bool;

    fn set_migrated(&self, migrated: bool);
    fn migrated(&self) -> bool;
    fn set_published(&self, published: bool);
    fn published(&self) -> bool;

    fn get_connection_score_and_quality(&self) -> (f32, livekit::ConnectionQuality);
    fn get_track_stats(&self) -> Option<livekit::RtpStats>;

    fn set_rtt(&self, rtt: u32);

    fn notify_subscriber_node_max_quality(
        &self,
        node_id: &livekit::NodeId,
        qualities: &[SubscribedCodecQuality],
    );
    fn notify_subscription_node(
        &self,
        node_id: &livekit::NodeId,
        codecs: &[livekit::SubscribedAudioCodec],
    );
    fn clear_subscriber_nodes(&self);
    fn notify_subscriber_node_media_loss(&self, node_id: &livekit::NodeId, fractional_loss: u8);
}

// The fake of a media track is not a local media track, the fake of a local one is.
#[cfg(test)]
impl AsLocalMediaTrack for FakeMediaTrack {
    fn as_local_media_track(self: Arc<Self>) -> Option<Arc<dyn LocalMediaTrack>> {
        None
    }
}

#[cfg(test)]
impl AsLocalMediaTrack for FakeLocalMediaTrack {
    fn as_local_media_track(self: Arc<Self>) -> Option<Arc<dyn LocalMediaTrack>> {
        Some(self)
    }
}

// DataTrack represents a data track
#[cfg_attr(test, musivum_fakes::fake)]
pub trait DataTrack: Send + Sync {
    fn id(&self) -> livekit::TrackId;
    fn pub_handle(&self) -> u16;
    fn name(&self) -> String;
    fn to_proto(&self) -> livekit::DataTrackInfo;

    fn publisher_id(&self) -> livekit::ParticipantId;
    fn publisher_identity(&self) -> livekit::ParticipantIdentity;

    fn add_subscriber(
        &self,
        sub: Arc<dyn LocalParticipant>,
    ) -> Result<Option<Arc<dyn DataDownTrack>>, Error>;
    fn remove_subscriber(&self, participant_id: &livekit::ParticipantId);
    fn is_subscriber(&self, sub_id: &livekit::ParticipantId) -> bool;
    fn revoke_disallowed_subscribers(
        &self,
        allowed_subscriber_identities: &[livekit::ParticipantIdentity],
    ) -> Vec<livekit::ParticipantIdentity>;

    fn add_data_down_track(&self, sender: Arc<dyn DataTrackSender>) -> Result<(), Error>;
    fn delete_data_down_track(&self, subscriber_id: &livekit::ParticipantId);

    fn handle_packet(&self, data: &[u8], packet: &datatrack::Packet, arrival_time: i64);

    fn close(&self);
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait DataDownTrack: Any + Send + Sync {
    fn close(&self);
    fn on_close(&self, f: Option<Box<dyn Fn() + Send + Sync>>);

    fn handle(&self) -> u16;
    fn publish_data_track(&self) -> Option<Arc<dyn DataTrack>>;

    fn update_subscription_options(
        &self,
        subscription_options: Option<&livekit::DataTrackSubscriptionOptions>,
    );
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait DataTrackSender: Send + Sync {
    fn subscriber_id(&self) -> livekit::ParticipantId;

    fn write_packet(&self, data: &[u8], packet: &datatrack::Packet, arrival_time: i64);
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait DataTrackTransport: Send + Sync {
    fn send_data_track_message(&self, data: &[u8]) -> Result<(), Error>;
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait SubscribedTrack: Any + Send + Sync {
    fn add_on_bind(&self, f: Box<dyn Fn(Result<(), Error>) + Send + Sync>);
    fn is_bound(&self) -> bool;
    fn on_subscribe_stream_started(&self, f: Box<dyn Fn(Duration) + Send + Sync>);
    fn close(&self, is_expected_to_resume: bool);
    fn on_close(&self, f: Option<Box<dyn Fn(bool) + Send + Sync>>);
    fn id(&self) -> livekit::TrackId;
    fn publisher_id(&self) -> livekit::ParticipantId;
    fn publisher_identity(&self) -> livekit::ParticipantIdentity;
    fn publisher_version(&self) -> u32;
    fn subscriber_id(&self) -> livekit::ParticipantId;
    fn subscriber_identity(&self) -> livekit::ParticipantIdentity;
    fn subscriber(&self) -> Arc<dyn LocalParticipant>;
    fn down_track(&self) -> Option<Arc<sfu::DownTrack>>;
    fn media_track(&self) -> Option<Arc<dyn MediaTrack>>;
    fn rtp_sender(&self) -> Option<Arc<dyn webrtc::RtpSender>>;
    fn is_muted(&self) -> bool;
    fn set_publisher_muted(&self, muted: bool);
    fn update_subscriber_settings(
        &self,
        settings: &livekit::UpdateTrackSettings,
        is_immediate: bool,
    );
    // selects appropriate video layer according to subscriber preferences
    fn update_video_layer(&self);
    fn needs_negotiation(&self) -> bool;
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait ChangeNotifier: Send + Sync {
    fn add_observer(&self, key: &str, on_changed: Box<dyn Fn() + Send + Sync>);
    fn remove_observer(&self, key: &str);
    fn has_observers(&self) -> bool;
    fn notify_changed(&self);
}

#[derive(Clone, Default)]
pub struct MediaResolverResult {
    pub track_changed_notifier: Option<Arc<dyn ChangeNotifier>>,
    pub track_removed_notifier: Option<Arc<dyn ChangeNotifier>>,
    pub track: Option<Arc<dyn MediaTrack>>,
    // is permission given to the requesting participant
    pub has_permission: bool,
    pub publisher_id: livekit::ParticipantId,
    pub publisher_identity: livekit::ParticipantIdentity,
}

#[derive(Clone, Default)]
pub struct DataResolverResult {
    pub track_changed_notifier: Option<Arc<dyn ChangeNotifier>>,
    pub track_removed_notifier: Option<Arc<dyn ChangeNotifier>>,
    pub data_track: Option<Arc<dyn DataTrack>>,
    // is permission given to the requesting participant
    pub has_permission: bool,
    pub publisher_id: livekit::ParticipantId,
    pub publisher_identity: livekit::ParticipantIdentity,
}

// MediaTrackResolver locates a specific media track for a subscriber
pub type MediaTrackResolver =
    Box<dyn Fn(Arc<dyn LocalParticipant>, &livekit::TrackId) -> MediaResolverResult + Send + Sync>;

// DataTrackResolver locates a specific data track for a subscriber
pub type DataTrackResolver =
    Box<dyn Fn(Arc<dyn LocalParticipant>, &livekit::TrackId) -> DataResolverResult + Send + Sync>;

// Supervisor/operation monitor related definitions
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum OperationMonitorEvent {
    #[default]
    PublisherPeerConnectionConnected,
    AddPendingPublication,
    SetPublicationMute,
    SetPublishedTrack,
    ClearPublishedTrack,
}

impl fmt::Display for OperationMonitorEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OperationMonitorEvent::PublisherPeerConnectionConnected => {
                "PUBLISHER_PEER_CONNECTION_CONNECTED"
            }
            OperationMonitorEvent::AddPendingPublication => "ADD_PENDING_PUBLICATION",
            OperationMonitorEvent::SetPublicationMute => "SET_PUBLICATION_MUTE",
            OperationMonitorEvent::SetPublishedTrack => "SET_PUBLISHED_TRACK",
            OperationMonitorEvent::ClearPublishedTrack => "CLEAR_PUBLISHED_TRACK",
        })
    }
}

/// Go's `OperationMonitorData any`, as what is posted with an event: nothing (nil), a bool
/// (whether the publisher's connection is up, whether a publication is muted) or the track
/// that was published or cleared.
#[derive(Clone, Default)]
pub enum OperationMonitorData {
    #[default]
    None,
    Bool(bool),
    LocalMediaTrack(Arc<dyn LocalMediaTrack>),
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait OperationMonitor: Any + Send + Sync {
    fn post_event(&self, ome: OperationMonitorEvent, omd: OperationMonitorData);
    fn check(&self) -> Result<(), Error>;
    fn is_idle(&self) -> bool;
}

// -------------------------------------------------------------------

// The change notifier of `pkg/utils` is what the resolvers hand out as a `ChangeNotifier`:
// upstream it is one by having the methods.
impl ChangeNotifier for crate::utils::ChangeNotifier {
    fn add_observer(&self, key: &str, on_changed: Box<dyn Fn() + Send + Sync>) {
        crate::utils::ChangeNotifier::add_observer(self, key, on_changed);
    }

    fn remove_observer(&self, key: &str) {
        crate::utils::ChangeNotifier::remove_observer(self, key);
    }

    fn has_observers(&self) -> bool {
        crate::utils::ChangeNotifier::has_observers(self)
    }

    fn notify_changed(&self) {
        crate::utils::ChangeNotifier::notify_changed(self);
    }
}

#[cfg(test)]
#[path = "interfaces_test.rs"]
mod interfaces_test;
