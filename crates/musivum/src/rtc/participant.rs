// Ported from LiveKit, pkg/rtc/participant.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. participant_signal.go, participant_sdp.go and
// participant_data_track.go are methods of the same type: they are `impl ParticipantImpl`
// blocks in files of the same names, and the fields are visible in `rtc`. What else differs:
//
// - Making it. `NewParticipant` is `ParticipantImpl::new` and gives an `Arc`, made with
//   `Arc::new_cyclic`: the participant keeps a `Weak` of itself (`this`), which it hands to
//   its signal handler, signaller, subscription manager and data track manager (each takes the
//   participant weakly) and to every callback and task it leaves with others. The transport
//   manager, the two transport handlers and the TWCC responder are made before the
//   participant (making the manager can fail): they reach it through a cell that `new` fills
//   once it is made (`ParticipantCell`), and so does the data channel's `BytesTrackStats`. The
//   rest of `setupTransportManager`, `setupUpTrackManager` and `setupSubscriptionManager`
//   (setting callbacks) is done once it is made. `ParticipantParams` has the node its
//   connections are on (`node`, new, as `TransportManagerParams` has).
// - Locks. What `lock` guards is in `ParticipantImplInner`, with `p.params.ClientConf`, which
//   the original changes under that lock; what `pendingTracksLock`, `updateLock`,
//   `joiningMessageLock` and `rpcLock` guard is in a struct of each. The functions upstream
//   calls with `pendingTracksLock` held take that struct (`&mut PendingTracks`), or are its
//   methods where they use nothing else of the participant. Where the original changes pending
//   tracks under the read lock (`setTrackMuted`, `UpdateAudioTrack`, `UpdateVideoTrack`), the
//   write lock is taken. The atomic values (`id`, `closeReason`, `grants`, the listeners, the
//   helper, `migrateState`) are behind locks of their own; `state` is its number, swapped as
//   upstream; the times that are an `atomic.Pointer` are the zero time for nil. A pending
//   track's infos are values: where Go hands out a pointer into the pending track, a copy is
//   handed out, and where it changes a track info through such a pointer it changes the one in
//   the map.
// - Who holds whom. Whoever has the participant holds it (the room manager and the room). The
//   participant holds its managers, its signalling, its supervisor and its pending tracks.
//   Everything it hands out holds it weakly: its two transport handlers (held by the transport
//   manager's transports: participant, manager, transport, handler would be a cycle), the TWCC
//   responder's callback, the callbacks of the up track manager, the subscription manager and
//   the supervisor, the functions of a media or data track's params, a media track's close
//   callback (which holds the track weakly too), a subscribed track's bind callback, the
//   timers, the queued RTCP writes and the tasks; a call that comes when it is gone does
//   nothing (a handler answers as a closed participant answers). The tasks of `go p.X()` that
//   need the participant for their whole length hold it strongly while they run, as the
//   goroutine does: the state change, `onSubscriberReady`, a new track's publish, the
//   `PerformRpc` goroutine, and the closing of the managers in `Close` and in the migration
//   timer.
// - Goroutines. A goroutine is a task (`time::spawn`), except where it waits for workers to
//   end: the closing of the subscription and transport managers in `Close` and in the
//   migration timer runs on a thread of its own (`go_blocking`), since `TransportManager::close`
//   blocks until the transports' events workers have stopped. `Close` itself never blocks on
//   them, so it may be called from any callback (the transports' event workers included).
// - The connection layer. A remote track and its receiver come as the layer's seams
//   (`Arc<dyn webrtc::TrackRemote>`, `Arc<dyn webrtc::RtpReceiver>`); the transport manager
//   takes the layer's own receiver for `GetPublisherMid`, got back from the seam by downcasting
//   (`get_publisher_mid`; a receiver that is not the layer's has no mid). `AddTrackLocal` and
//   `AddTransceiverFromTrackLocal` hand the layer's sender and transceiver out behind the seam
//   traits; `RemoveTrackLocal` gets the layer's sender back the same way (another one is
//   `Error::NoSender`). `onMediaTrack`'s wait for a first packet to learn the codec
//   (`rtcTrack.Read` when the payload type is 0) has no counterpart: the layer announces a
//   track at its first packet, with its codec. `GetPacer` returns the subscriber transport's
//   pacer, which a sending transport always has (`TransportManager::get_subscriber_pacer` is an
//   `Option` for a transport that does not send).
// - Left out, because musivum has not ported what they belong to: LiveKit's observability
//   (`roomobs`, `observability`: the reporter, its resolver, the session timer, the logger's
//   deferred resolver, `disconnectedAt`, which only the reporter reads, and what `NewParticipant`
//   registers with the reporter), the metrics of `pkg/metric` (`MetricConfig`, the timestamper,
//   collector and reporter, `setupMetrics` and the three methods `pkg/metric` calls back), the
//   data cache of a participant that migrates in from another node (`migrateInPubDataCache`,
//   `MigrationDataCache`, `migrationWaitContinuousMsgDuration`: migrationdatacache.go is left
//   out with migration between nodes) and psrpc (`ErrMoveOldClientVersion` and the other error
//   of `SupportsMoving` are `Error::MoveOldClientVersion` and `Error::CannotBeMoved`, with the
//   texts of the original; their code, `FailedPrecondition`, has no counterpart). A comment
//   stands at the place of each.
// - Copies of what is private elsewhere: `isFirefox` and `isOBS` of clientinfo.go (private in
//   clientinfo.rs), `strings.EqualFold` (`equal_fold`, as other files have it). New for
//   musivum, as the port has no dependency for them: the LRU cache of hashicorp/golang-lru
//   (`UpdateCache`, as the participant uses it) and `uuid.NewString()` (`new_uuid_string`).
// - `PerformRpc`'s channels are tokio channels, written to with `try_send`: a send that the
//   original does without `select` blocks until it is received, here it is dropped when the
//   channel is full (the caller makes a channel of one for each answer). What the response
//   handler's `Resolve` does with `rpcPendingAcks` (its callers hold `rpcLock`) is done by its
//   callers. `psrpc.NewError(psrpc.Internal, err)` is the error itself.
// - `GetConnectionQuality` returns the info itself: it never returns nil upstream either.
//   `GetClientInfo().GetSdk()` and the other getters of a message that may be nil give the
//   zero value, as the generated getters do. A down track of a subscribed track and a media
//   track that the original dereferences without checking are `expect`ed with the message of
//   a nil dereference; so are the type assertions without `ok` (`.(*MediaTrack)`,
//   `.(types.LocalMediaTrack)`).
// - Logging: a value of `WithUnlikelyValues` is put on each line of that logger; values are
//   logged by their `Debug`. `IsEOF` on the error of an RTCP write is
//   `webrtc::Error::ClosedPipe` (the layer returns no `io.EOF`).
// - `PingIntervalSeconds` and `PingTimeoutSeconds` are `i32`, as `JoinResponse` has them; the
//   transport manager converts.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, OnceLock, Weak};

use musivum_protocol::Message as _;
use parking_lot::{Mutex, RwLock};
use rand::RngExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::signalling::{
    self, ParticipantSignalHandler, ParticipantSignaller, ParticipantSignalling,
};
use super::supervisor::{ParticipantSupervisor, ParticipantSupervisorParams};
use super::transport::{self as rtctransport};
use super::types::{self, LocalParticipant, LocalParticipantListener, MediaTrack as _};
use super::{
    BytesTrackStats, BytesTrackType, ClientInfo, DataTrack, DataTrackParams, Error, MediaTrack,
    MediaTrackParams, NEGOTIATION_FAILED_TIMEOUT, ParticipantDataBlob, ParticipantDataBlobHandler,
    ParticipantDataBlobParams, RtcpFeedbackConfig, SubscribePacer, SubscriptionManager,
    SubscriptionManagerParams, TransportManager, TransportManagerParams, UpDataTrackManager,
    UpDataTrackManagerParams, UpTrackManager, UpTrackManagerParams, WebRtcConfig,
    bytes_track_id_for_participant_id, is_codec_enabled, logger_with_track, recover_async,
    select_alternative_audio_codec, select_alternative_video_codec, stream_from_track_source,
    to_proto_track_kind,
};
use crate::config;
use crate::go::atomic;
use crate::go::time::{self, Duration, Time, Timer};
use crate::mediatransportutil::twcc;
use crate::protocol::auth;
use crate::protocol::codecs::mime;
use crate::protocol::datatrack;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::sdp as protosdp;
use crate::protocol::signalling as protosignalling;
use crate::protocol::utils::{
    self as putils, DataChannelRpcError, DataChannelRpcErrorCode, DataChannelRpcPendingAckHandler,
    DataChannelRpcPendingResponseHandler, TimedVersion, TimedVersionGenerator, guid,
};
use crate::routing::{Message, MessageSink};
use crate::sfu;
use crate::sfu::buffer;
use crate::sfu::connectionquality;
use crate::sfu::interceptor::SimulcastTrackInfo;
use crate::sfu::pacer;
use crate::sfu::streamallocator;
use crate::telemetry;
use crate::telemetry::prometheus;
use crate::utils as sutils;
use crate::webrtc;
use crate::webrtc::peerconnection::{IceCandidate, Node, SessionDescription};
use crate::webrtc::rtcp;
use crate::webrtc::rtcp::receiver_report::ReceiverReport;
use crate::webrtc::rtcp::source_description::{SourceDescription, SourceDescriptionChunk};
use crate::webrtc::sdp;
use crate::{debugw, errorw, infow, warnw};

// var _ types.LocalParticipant = (*ParticipantImpl)(nil): the implementation is at the end of
// this file

const SD_BATCH_SIZE: usize = 30;
const RTT_UPDATE_INTERVAL: Duration = Duration::from_secs(5);

const PUBLISHER_ANSWER_DYNACAST_RESEND_DELAY: Duration = Duration::from_secs(2);
const PUBLISHER_ANSWER_DYNACAST_RESEND_MAX_DELAY: Duration = Duration::from_secs(8);

const DISCONNECT_CLEANUP_DURATION: Duration = Duration::from_secs(5);
const MIGRATION_WAIT_DURATION: Duration = Duration::from_secs(3);
// `migrationWaitContinuousMsgDuration` (2 s): only for the data cache of a participant that
// migrates in from another node, which is left out (see the header)

pub(super) const C_MAX_PENDING_TRACKS: usize = 20;
const C_MAX_PENDING_QUEUED_TRACKS: usize = 3;
pub(super) const C_MAX_RELEASED_TRACKS: usize = 32;

// unsequenced reliable data (server API sends) cannot be recovered from the
// data message cache, so it is held here until the reliable data channel is writable
pub(super) const C_MAX_JOINING_UNSEQUENCED_RELIABLE_BYTES: usize = 100_000;

pub const PING_INTERVAL_SECONDS: i32 = 5;
pub const PING_TIMEOUT_SECONDS: i32 = 15;

// `ErrMoveOldClientVersion`, a `FailedPrecondition` of psrpc upstream, is
// `Error::MoveOldClientVersion` (errors.rs).

// -------------------------------------------------

#[derive(Clone, Debug, Default)]
pub(super) struct PendingTrackInfo {
    pub(super) track_infos: Vec<livekit::TrackInfo>,
    pub(super) sdp_rids: buffer::VideoLayersRid,
    pub(super) migrated: bool,
    pub(super) created_at: Time,

    // indicates if this track is queued for publishing to avoid a track has been published
    // before the previous track is unpublished(closed) because client is allowed to negotiate
    // webrtc track before AddTrackRequest return to speed up the publishing process
    pub(super) queued: bool,
}

// --------------------------------------------------

pub(super) struct PendingRemoteTrack {
    track: Arc<dyn webrtc::TrackRemote>,
    receiver: Arc<dyn webrtc::RtpReceiver>,
}

struct DownTrackStateEntry {
    transceiver: Arc<dyn webrtc::RtpTransceiver>,
    down_track: sfu::DownTrackState,
}

// `postRtcpOp` embeds the participant: here it holds it weakly, as the queue belongs to it.
pub(super) struct PostRtcpOp {
    p: Weak<ParticipantImpl>,
    pkts: Vec<Box<dyn rtcp::Packet>>,
}

// ---------------------------------------------------------------

#[derive(Clone)]
pub(super) struct ParticipantUpdateInfo {
    pub(super) identity: livekit::ParticipantIdentity,
    pub(super) version: u32,
    pub(super) state: livekit::participant_info::State,
    pub(super) updated_at: Time,
}

// `String()`, which is how it is logged
impl fmt::Debug for ParticipantUpdateInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "identity: {}, version: {}, state: {}, updatedAt: {}",
            self.identity,
            self.version,
            self.state.as_str_name(),
            self.updated_at
        )
    }
}

// What `joiningMessageLock` guards.
#[derive(Default)]
pub(super) struct JoiningMessages {
    pub(super) first_seqs: HashMap<livekit::ParticipantId, u32>,
    pub(super) last_written_seqs: HashMap<livekit::ParticipantId, u32>,
    pub(super) unsequenced_messages: Vec<Vec<u8>>,
    pub(super) unsequenced_bytes: usize,
    pub(super) unsequenced_dropped: usize,
    pub(super) can_write_reliable: bool,
}

pub(super) struct ReliableDataInfo {
    pub(super) joining_message_lock: Mutex<JoiningMessages>,
    pub(super) last_pub_reliable_seq: atomic::Uint32,
    pub(super) stop_reliable_by_migrate_out: atomic::Bool,
    // `migrateInPubDataCache`: left out with migration between nodes (see the header)
}

// ---------------------------------------------------------------

pub struct ParticipantParams {
    pub identity: livekit::ParticipantIdentity,
    pub name: livekit::ParticipantName,
    pub sid: livekit::ParticipantId,
    pub config: Arc<WebRtcConfig>,
    pub sink: Option<Arc<dyn MessageSink>>,
    pub audio_config: sfu::AudioConfig,
    pub video_config: config::VideoConfig,
    pub limit_config: config::LimitConfig,
    pub protocol_version: types::ProtocolVersion,
    pub session_start_time: Time,
    // `SessionTimer *observability.SessionTimer`: LiveKit's observability, not ported
    pub telemetry_listener: Option<Arc<dyn types::ParticipantTelemetryListener>>,
    pub trailer: Vec<u8>,
    pub pli_throttle_config: sfu::PliThrottleConfig,
    pub congestion_control_config: config::CongestionControlConfig,
    // codecs that are enabled for this room
    pub publish_enabled_codecs: Vec<livekit::Codec>,
    pub subscribe_enabled_codecs: Vec<livekit::Codec>,
    pub logger: Logger,
    // `LoggerResolver logger.DeferredFieldResolver`: the logger here has no deferred fields
    // `Reporter`, `ReporterResolver`: LiveKit's observability (`roomobs`), not ported
    pub sim_tracks: HashMap<u32, SimulcastTrackInfo>,
    pub grants: Option<Arc<auth::ClaimGrants>>,
    pub token_expires_at: Time,
    pub initial_version: u32,
    pub client_conf: Option<livekit::ClientConfiguration>,
    pub client_info: ClientInfo,
    pub region: String,
    pub migration: bool,
    pub reconnect: bool,
    pub adaptive_stream: bool,
    pub allow_tcp_fallback: bool,
    pub tcp_fallback_rtt_threshold: i64,
    pub allow_udp_unstable_fallback: bool,
    pub turns_enabled: bool,
    pub participant_listener: Option<Arc<dyn types::LocalParticipantListener>>,
    pub participant_helper: Option<Arc<dyn types::LocalParticipantHelper>>,
    pub disable_supervisor: bool,
    pub reconnect_on_publication_error: bool,
    pub reconnect_on_subscription_error: bool,
    pub reconnect_on_data_channel_error: bool,
    pub version_generator: Arc<dyn TimedVersionGenerator>,
    pub disable_dynacast: bool,
    pub subscriber_allow_pause: bool,
    pub subscription_limit_audio: i32,
    pub subscription_limit_video: i32,
    pub subscribe_pacer: Option<Arc<SubscribePacer>>,
    pub playout_delay: Option<livekit::PlayoutDelay>,
    pub sync_streams: bool,
    pub forward_stats: Option<Arc<sfu::ForwardStats>>,
    pub disable_sender_report_pass_through: bool,
    // `MetricConfig metric.MetricConfig`: `pkg/metric` is not ported
    pub use_one_shot_signalling_mode: bool,
    // read only to leave the metrics out: `pkg/metric` is not ported
    pub enable_metrics: bool,
    pub data_channel_max_buffered_amount: u64,
    pub datachannel_slow_threshold: i64,
    pub datachannel_lossy_target_latency: Duration,
    pub datachannel_data_track_target_latency: Duration,
    pub fire_on_track_by_sdp: bool,
    pub disable_codec_regression: bool,
    pub last_pub_reliable_seq: u32,
    pub country: String,
    pub prefer_video_size_from_media: bool,
    pub use_single_peer_connection: bool,
    pub enable_data_tracks: bool,
    pub enable_rtp_stream_restart_detection: bool,
    pub force_backup_codec_policy_simulcast: bool,
    pub disable_transceiver_reuse_for_e2ee: bool,
    pub enable_participant_data_blob: bool,
    pub enable_start_at_desired_quality: bool,
    pub migration_wait_duration: Duration,
    pub exclude_ipv6_local_candidates: bool,
    pub enable_warp: bool,
    pub media_batch_io_enabled: bool,

    // New: the node the participant's peer connections are on (see the header).
    pub node: Node,
}

pub struct ParticipantImpl {
    pub(super) params: ParticipantParams,

    participant_listener: RwLock<Option<Arc<dyn types::LocalParticipantListener>>>,
    telemetry_listener: RwLock<Option<Arc<dyn types::ParticipantTelemetryListener>>>,
    participant_helper: RwLock<Option<Arc<dyn types::LocalParticipantHelper>>>,
    id: RwLock<livekit::ParticipantId>,

    pub(super) is_closed: atomic::Bool,
    close_reason: RwLock<types::ParticipantCloseReason>,

    // livekit.ParticipantInfo_State, as its number
    pub(super) state: atomic::Int32,
    disconnected: CancellationToken,

    // a migrating in participant resumes on a reconnect response, the client takes it
    // only as the first message on the resumed signal connection
    pub(super) reconnect_response_sent: atomic::Bool,

    grants: RwLock<Arc<auth::ClaimGrants>>,
    pub(super) is_publisher: atomic::Bool,

    session_start_recorded: atomic::Bool,
    // the zero time for nil
    last_active_at: Mutex<Time>,
    // when first connected
    connected_at: Time,
    // `disconnectedAt`: read only by the observability reporter, which is not ported

    // the zero time for nil
    migrated_in_at: atomic::Time,

    pub_rtcp_queue: sutils::TypedOpsQueue<PostRtcpOp>,

    // hold reference for MediaTrack
    #[allow(dead_code)]
    twcc: Arc<twcc::Responder>,

    // client intended to publish, yet to be reconciled
    pub(super) pending_tracks_lock: RwLock<PendingTracks>,

    // supported codecs
    pub(super) enabled_publish_codecs: Vec<livekit::Codec>,
    pub(super) enabled_subscribe_codecs: Vec<livekit::Codec>,

    pub(super) transport_manager: Arc<TransportManager>,
    pub(super) up_track_manager: Arc<UpTrackManager>,
    pub(super) up_data_track_manager: UpDataTrackManager,
    pub(super) subscription_manager: Arc<SubscriptionManager>,

    // `icQueue`, indexed by the signal target
    pub(super) ic_queue: [Mutex<Option<IceCandidate>>; 2],

    // queued participant updates before join response is sent, and the cache of recently
    // sent updates, to ensure ordering by version: what `updateLock` guards
    pub(super) update_lock: Mutex<ParticipantUpdates>,

    data_channel_stats: Arc<BytesTrackStats>,

    pub(super) reliable_data_info: ReliableDataInfo,

    pub(super) lock: RwLock<ParticipantImplInner>,

    pub(super) dirty: atomic::Bool,
    version: atomic::Uint32,

    migrate_state: RwLock<types::MigrateState>,
    migrated_tracks_published_fuse: CancellationToken,

    pub(super) supervisor: Option<Arc<ParticipantSupervisor>>,

    pub(super) signalling: Arc<dyn ParticipantSignalling>,
    signal_handler: Arc<dyn ParticipantSignalHandler>,
    pub(super) signaller: Arc<dyn ParticipantSignaller>,

    // loggers for publisher and subscriber
    pub(super) pub_logger: Logger,
    pub(super) sub_logger: Logger,

    rpc_lock: Mutex<RpcPending>,

    data_blob: ParticipantDataBlob,

    // New: the participant itself, for what it hands out (see the header)
    pub(super) this: Weak<ParticipantImpl>,
}

// What `pendingTracksLock` guards.
#[derive(Default)]
pub(super) struct PendingTracks {
    pub(super) pending_tracks: HashMap<String, PendingTrackInfo>,
    pub(super) pending_publishing_tracks: HashMap<livekit::TrackId, PendingTrackInfo>,
    pub(super) pending_remote_tracks: Vec<PendingRemoteTrack>,
    // client ids (signal, codec and SDP) of each pending track released without media, so that
    // media or an offer for them is dropped instead of being matched by type to another pending
    // track, bounded to cMaxReleasedTracks, oldest out first
    pub(super) released_track_cids: Vec<Vec<String>>,
}

// What `updateLock` guards.
pub(super) struct ParticipantUpdates {
    // queued participant updates before join response is sent
    pub(super) queued_updates: Vec<livekit::ParticipantInfo>,
    // cache of recently sent updates, to ensure ordering by version
    pub(super) update_cache: UpdateCache,
}

#[allow(clippy::type_complexity)]
type OnIceConfigChanged = Arc<dyn Fn(Arc<dyn LocalParticipant>, &livekit::IceConfig) + Send + Sync>;
type OnParticipant = Arc<dyn Fn(Arc<dyn LocalParticipant>) + Send + Sync>;

// What `lock utils.RWMutex` guards.
pub(super) struct ParticipantImplInner {
    // utils.TimedVersion
    timed_version: TimedVersion,

    // timer that's set when disconnect is detected on primary PC
    disconnect_timer: Option<Timer>,
    migration_timer: Option<Timer>,
    // re-send of the subscribed qualities pending after publisher answers
    dynacast_resend_timer: Option<Timer>,
    dynacast_resend_deadline: Time,

    require_broadcast: bool,

    rtt_updated_at: Time,
    last_rtt: u32,

    // idempotent reference guard for telemetry stats worker
    telemetry_guard: Arc<telemetry::ReferenceGuard>,

    on_close: HashMap<String, OnParticipant>,
    on_claims_changed: Option<OnParticipant>,
    on_ice_config_changed: Option<OnIceConfigChanged>,

    cached_down_tracks: HashMap<livekit::TrackId, DownTrackStateEntry>,
    forwarder_state: HashMap<livekit::TrackId, livekit::RtpForwarderState>,

    connection_quality: livekit::ConnectionQuality,

    // `p.params.ClientConf`, which the original changes under this lock
    client_conf: Option<livekit::ClientConfiguration>,

    pub(super) next_subscribed_data_track_handle: u16,
}

// What `rpcLock` guards.
#[derive(Default)]
struct RpcPending {
    rpc_pending_acks: HashMap<String, DataChannelRpcPendingAckHandler>,
    rpc_pending_responses: HashMap<String, DataChannelRpcPendingResponseHandler>,
}

// The participant as its transport handlers and the TWCC responder reach it: they are made
// before it is, and hold it weakly once it is (see the header).
type ParticipantCell = Arc<OnceLock<Weak<ParticipantImpl>>>;

fn participant_of(cell: &ParticipantCell) -> Option<Arc<ParticipantImpl>> {
    cell.get().and_then(Weak::upgrade)
}

impl ParticipantImpl {
    pub fn new(params: ParticipantParams) -> Result<Arc<ParticipantImpl>, Error> {
        if params.identity.is_empty() {
            return Err(Error::EmptyIdentity);
        }
        if params.sid.is_empty() {
            return Err(Error::EmptyParticipantId);
        }
        let grants = match &params.grants {
            Some(grants) if grants.video.is_some() => Arc::new((**grants).clone()),
            _ => return Err(Error::MissingGrants),
        };

        let require_broadcast = !grants.metadata.is_empty() || !grants.attributes.is_empty();

        let cell: ParticipantCell = Arc::new(OnceLock::new());

        let pub_logger = params.logger.with_component(sutils::COMPONENT_PUB);
        let sub_logger = params.logger.with_component(sutils::COMPONENT_SUB);

        let supervisor = if !params.disable_supervisor {
            Some(ParticipantSupervisor::new(ParticipantSupervisorParams {
                logger: params.logger.clone(),
            }))
        } else {
            None
        };
        let mut timed_version = TimedVersion::default();
        timed_version.update(params.version_generator.next());

        let data_channel_stats = {
            let cell = cell.clone();
            BytesTrackStats::new(
                &params.country,
                &bytes_track_id_for_participant_id(BytesTrackType::Data, &params.sid),
                &params.sid,
                move || telemetry_listener_of(&cell),
            )
        };

        let (enabled_publish_codecs, enabled_subscribe_codecs) = Self::setup_enabled_codecs(
            &params.logger,
            &params.publish_enabled_codecs,
            &params.subscribe_enabled_codecs,
            params
                .client_conf
                .as_ref()
                .and_then(|c| c.disabled_codecs.as_ref()),
        );

        // `params.Reporter.RegisterFunc(...)`: what the participant reports of its session to
        // LiveKit's observability (`roomobs`), which is not ported, is not here.

        // keep last participants and when updates were sent
        let update_cache = UpdateCache::new(128);

        let (twcc, transport_manager) = Self::setup_transport_manager(
            &params,
            &cell,
            &data_channel_stats,
            &enabled_publish_codecs,
            &enabled_subscribe_codecs,
        )?;

        let up_track_manager = UpTrackManager::new(UpTrackManagerParams {
            logger: pub_logger.clone(),
            version_generator: params.version_generator.clone(),
        });

        let p = Arc::new_cyclic(|this: &Weak<ParticipantImpl>| {
            let lp: Weak<dyn LocalParticipant> = this.clone();
            let (signalling, signal_handler, signaller) =
                Self::setup_signalling(&params.logger, this);
            let up_data_track_manager = Self::setup_up_data_track_manager(&pub_logger, this);
            let subscription_manager =
                Self::setup_subscription_manager(&params, &sub_logger, this, lp);

            let client_conf = params.client_conf.clone();
            ParticipantImpl {
                participant_listener: RwLock::new(params.participant_listener.clone()),
                telemetry_listener: RwLock::new(params.telemetry_listener.clone()),
                participant_helper: RwLock::new(params.participant_helper.clone()),
                id: RwLock::new(params.sid.clone()),
                is_closed: atomic::Bool::new(false),
                close_reason: RwLock::new(types::ParticipantCloseReason::None),
                state: atomic::Int32::new(livekit::participant_info::State::Joining as i32),
                disconnected: CancellationToken::new(),
                reconnect_response_sent: atomic::Bool::new(false),
                grants: RwLock::new(grants),
                is_publisher: atomic::Bool::new(false),
                session_start_recorded: atomic::Bool::new(false),
                last_active_at: Mutex::new(Time::ZERO),
                connected_at: truncate_to_millisecond(time::now()),
                migrated_in_at: atomic::Time::new(Time::ZERO),
                pub_rtcp_queue: sutils::TypedOpsQueue::new(sutils::OpsQueueParams {
                    name: "pub-rtcp".to_string(),
                    min_size: 64,
                    logger: params.logger.clone(),
                    ..Default::default()
                }),
                twcc,
                pending_tracks_lock: RwLock::new(PendingTracks::default()),
                enabled_publish_codecs,
                enabled_subscribe_codecs,
                transport_manager,
                up_track_manager,
                up_data_track_manager,
                subscription_manager,
                ic_queue: [Mutex::new(None), Mutex::new(None)],
                update_lock: Mutex::new(ParticipantUpdates {
                    queued_updates: Vec::new(),
                    update_cache,
                }),
                data_channel_stats,
                reliable_data_info: ReliableDataInfo {
                    joining_message_lock: Mutex::new(JoiningMessages::default()),
                    last_pub_reliable_seq: atomic::Uint32::new(params.last_pub_reliable_seq),
                    stop_reliable_by_migrate_out: atomic::Bool::new(false),
                },
                lock: RwLock::new(ParticipantImplInner {
                    timed_version,
                    disconnect_timer: None,
                    migration_timer: None,
                    dynacast_resend_timer: None,
                    dynacast_resend_deadline: Time::ZERO,
                    require_broadcast,
                    rtt_updated_at: time::now(),
                    last_rtt: 0,
                    telemetry_guard: Arc::new(telemetry::ReferenceGuard::default()),
                    on_close: HashMap::new(),
                    on_claims_changed: None,
                    on_ice_config_changed: None,
                    cached_down_tracks: HashMap::new(),
                    forwarder_state: HashMap::new(),
                    connection_quality: livekit::ConnectionQuality::Excellent,
                    client_conf,
                    next_subscribed_data_track_handle: rand::rng().random_range(0..256u16),
                }),
                dirty: atomic::Bool::new(false),
                version: atomic::Uint32::new(params.initial_version),
                migrate_state: RwLock::new(types::MigrateState::Init),
                migrated_tracks_published_fuse: CancellationToken::new(),
                supervisor,
                signalling,
                signal_handler,
                signaller,
                pub_logger,
                sub_logger,
                rpc_lock: Mutex::new(RpcPending::default()),
                data_blob: ParticipantDataBlob::new(ParticipantDataBlobParams {
                    logger: params.logger.clone(),
                }),
                this: this.clone(),
                params,
            }
        });
        let _ = cell.set(Arc::downgrade(&p));

        p.swap_response_sink(p.params.sink.clone(), types::SignallingCloseReason::Unknown);

        if let Some(supervisor) = &p.supervisor {
            let weak = p.this.clone();
            supervisor.on_publication_error(move |track_id| {
                if let Some(p) = weak.upgrade() {
                    p.on_publication_error(track_id);
                }
            });
        }

        p.setup_transport_manager_callbacks();
        p.setup_up_track_manager();
        p.setup_subscription_manager_callbacks();
        // `p.setupMetrics()`: `pkg/metric` is not ported (see the header)

        Ok(p)
    }

    // The participant as it is handed to listeners and callbacks: it is held by whoever
    // calls one of its methods.
    pub(super) fn arc(&self) -> Arc<ParticipantImpl> {
        self.this
            .upgrade()
            .expect("a participant whose method is called is held")
    }

    fn set_listener(&self, listener: Option<Arc<dyn types::LocalParticipantListener>>) {
        *self.participant_listener.write() = listener;
    }

    pub(super) fn listener(&self) -> Arc<dyn types::LocalParticipantListener> {
        if let Some(l) = self.participant_listener.read().as_ref() {
            return l.clone();
        }
        Arc::new(types::NullLocalParticipantListener::default())
    }

    pub fn get_participant_listener(&self) -> Arc<dyn types::ParticipantListener> {
        self.listener()
    }

    pub fn clear_participant_listener(&self) {
        self.set_listener(None);
    }

    fn set_telemetry_listener(
        &self,
        listener: Option<Arc<dyn types::ParticipantTelemetryListener>>,
    ) {
        *self.telemetry_listener.write() = listener;
    }

    pub fn get_telemetry_listener(&self) -> Arc<dyn types::ParticipantTelemetryListener> {
        if let Some(l) = self.telemetry_listener.read().as_ref() {
            return l.clone();
        }
        Arc::new(types::NullParticipantTelemetryListener::default())
    }

    pub fn clear_telemetry_listener(&self) {
        self.set_telemetry_listener(None);
    }

    pub fn get_country(&self) -> String {
        self.params.country.clone()
    }

    pub fn get_trailer(&self) -> Vec<u8> {
        self.params.trailer.clone()
    }

    pub fn get_logger(&self) -> Logger {
        self.params.logger.clone()
    }

    // `GetLoggerResolver`, `GetReporter`, `GetReporterResolver`: not here (see the header)

    pub fn get_adaptive_stream(&self) -> bool {
        self.params.adaptive_stream
    }

    pub fn get_enable_start_at_desired_quality(&self) -> bool {
        self.params.enable_start_at_desired_quality
    }

    pub fn get_pacer(&self) -> Arc<dyn pacer::Pacer> {
        // the transport that subscribes always sends, and so has a pacer (see the header)
        self.transport_manager
            .get_subscriber_pacer()
            .expect("the subscriber transport has a pacer")
    }

    pub fn get_disable_sender_report_pass_through(&self) -> bool {
        self.params.disable_sender_report_pass_through
    }

    pub fn id(&self) -> livekit::ParticipantId {
        self.id.read().clone()
    }

    pub fn identity(&self) -> livekit::ParticipantIdentity {
        self.params.identity.clone()
    }

    pub fn state(&self) -> livekit::participant_info::State {
        livekit::participant_info::State::try_from(self.state.load()).unwrap_or_default()
    }

    pub fn kind(&self) -> livekit::participant_info::Kind {
        self.grants.read().get_participant_kind()
    }

    pub fn kind_details(&self) -> Vec<livekit::participant_info::KindDetail> {
        self.grants.read().get_kind_details()
    }

    pub fn is_recorder(&self) -> bool {
        let grants = self.grants.read().clone();
        grants.get_participant_kind() == livekit::participant_info::Kind::Egress
            || video_grant(&grants).recorder
    }

    pub fn is_agent(&self) -> bool {
        let grants = self.grants.read().clone();
        grants.get_participant_kind() == livekit::participant_info::Kind::Agent
            || video_grant(&grants).agent
    }

    pub fn is_dependent(&self) -> bool {
        let grants = self.grants.read().clone();
        match grants.get_participant_kind() {
            livekit::participant_info::Kind::Agent | livekit::participant_info::Kind::Egress => {
                true
            }
            _ => video_grant(&grants).agent || video_grant(&grants).recorder,
        }
    }

    pub fn protocol_version(&self) -> types::ProtocolVersion {
        self.params.protocol_version
    }

    pub fn is_ready(&self) -> bool {
        let state = self.state();

        // when migrating, there is no JoinResponse, state transitions from JOINING -> ACTIVE -> DISCONNECTED
        // so JOINING is considered ready. The ReconnectResponse takes the place of the JoinResponse
        // as the message the resumed signal connection opens with, so readiness waits for it.
        if self.params.migration {
            return state != livekit::participant_info::State::Disconnected
                && self.reconnect_response_sent.load();
        }

        // when not migrating, there is a JoinResponse, state transitions from JOINING -> JOINED -> ACTIVE -> DISCONNECTED
        state == livekit::participant_info::State::Joined
            || state == livekit::participant_info::State::Active
    }

    pub fn is_disconnected(&self) -> bool {
        self.state() == livekit::participant_info::State::Disconnected
    }

    pub fn disconnected(&self) -> CancellationToken {
        self.disconnected.clone()
    }

    pub fn is_idle(&self) -> bool {
        // check if there are any published tracks that are subscribed
        for t in self.up_track_manager.get_published_tracks() {
            if t.get_num_subscribers() > 0 {
                return false;
            }
        }

        !self.subscription_manager.has_subscriptions()
    }

    pub fn connected_at(&self) -> Time {
        self.connected_at
    }

    pub fn active_at(&self) -> Time {
        // the zero time when it never was
        *self.last_active_at.lock()
    }

    pub fn get_client_info(&self) -> Option<Arc<livekit::ClientInfo>> {
        let _guard = self.lock.read();
        self.params.client_info.client_info.clone()
    }

    pub fn get_client_configuration(&self) -> Option<livekit::ClientConfiguration> {
        let inner = self.lock.read();
        inner.client_conf.clone()
    }

    pub fn get_buffer_factory(&self) -> Arc<buffer::Factory> {
        // a nil pointer upstream when the configuration has none
        self.params
            .config
            .buffer_factory
            .clone()
            .expect("WebRTCConfig without BufferFactory")
    }

    // checkMetadataLimits check if name/metadata/attributes of a participant is within configured limits
    fn check_metadata_limits(
        &self,
        name: &str,
        metadata: &str,
        attributes: &HashMap<String, String>,
    ) -> Result<(), signalling::Error> {
        if !self.params.limit_config.check_participant_name_length(name) {
            return Err(signalling::Error::NameExceedsLimits);
        }

        if !self.params.limit_config.check_metadata_size(metadata) {
            return Err(signalling::Error::MetadataExceedsLimits);
        }

        if !self.params.limit_config.check_attributes_size(attributes) {
            return Err(signalling::Error::AttributesExceedsLimits);
        }

        Ok(())
    }

    pub fn update_metadata(
        &self,
        update: &livekit::UpdateParticipantMetadata,
        from_admin: bool,
    ) -> Result<(), Error> {
        // `lgr := p.params.Logger.WithUnlikelyValues(...)`: the two values are on the lines
        debugw!(self.params.logger, "updating participant metadata", "update" => update, "fromAdmin" => from_admin);

        let mut request_response = livekit::RequestResponse {
            request_id: update.request_id,
            ..Default::default()
        };
        let send_request_response =
            |mut request_response: livekit::RequestResponse, err: Option<signalling::Error>| {
                if !from_admin || (update.request_id != 0 || err.is_some()) {
                    request_response.request = Some(
                        livekit::request_response::Request::UpdateMetadata(update.clone()),
                    );
                    let _ = self.send_request_response(request_response);
                }
                if let Some(err) = &err {
                    warnw!(self.params.logger, "could not update metadata", err, "update" => update, "fromAdmin" => from_admin);
                }
                match err {
                    Some(err) => Err(Error::from(err)),
                    None => Ok(()),
                }
            };

        if !from_admin && !video_grant(&self.claim_grants()).get_can_update_own_metadata() {
            request_response.reason = livekit::request_response::Reason::NotAllowed as i32;
            request_response.message =
                "does not have permission to update own metadata".to_string();
            return send_request_response(
                request_response,
                Some(signalling::Error::UpdateOwnMetadataNotAllowed),
            );
        }

        if let Err(err) =
            self.check_metadata_limits(&update.name, &update.metadata, &update.attributes)
        {
            match err {
                signalling::Error::NameExceedsLimits => {
                    request_response.reason =
                        livekit::request_response::Reason::LimitExceeded as i32;
                    request_response.message = "exceeds name length limit".to_string();
                }

                signalling::Error::MetadataExceedsLimits => {
                    request_response.reason =
                        livekit::request_response::Reason::LimitExceeded as i32;
                    request_response.message = "exceeds metadata size limit".to_string();
                }

                signalling::Error::AttributesExceedsLimits => {
                    request_response.reason =
                        livekit::request_response::Reason::LimitExceeded as i32;
                    request_response.message = "exceeds attributes size limit".to_string();
                }
                _ => {}
            }
            return send_request_response(request_response, Some(err));
        }

        if !update.name.is_empty() {
            self.set_name(&update.name);
        }
        if !update.metadata.is_empty() {
            self.set_metadata(&update.metadata);
        }
        // `update.Attributes != nil`: a map that was not sent is empty here, and
        // `SetAttributes` does nothing with an empty one
        if !update.attributes.is_empty() {
            self.set_attributes(&update.attributes);
        }
        send_request_response(request_response, None)
    }

    // SetName attaches name to the participant
    pub fn set_name(&self, name: &str) {
        let on_claims_changed = {
            let inner = self.lock.write();
            let grants = self.grants.read().clone();
            if grants.name == name {
                return;
            }

            let mut grants = (*grants).clone();
            grants.name = name.to_string();
            *self.grants.write() = Arc::new(grants);
            self.dirty.store(true);

            inner.on_claims_changed.clone()
        };

        let p = self.arc();
        self.listener().on_participant_update(p.clone());

        if let Some(on_claims_changed) = on_claims_changed {
            on_claims_changed(p);
        }
    }

    // SetMetadata attaches metadata to the participant
    pub fn set_metadata(&self, metadata: &str) {
        let on_claims_changed = {
            let mut inner = self.lock.write();
            let grants = self.grants.read().clone();
            if grants.metadata == metadata {
                return;
            }

            let mut grants = (*grants).clone();
            grants.metadata = metadata.to_string();
            *self.grants.write() = Arc::new(grants);
            inner.require_broadcast = inner.require_broadcast || !metadata.is_empty();
            self.dirty.store(true);

            inner.on_claims_changed.clone()
        };

        let p = self.arc();
        self.listener().on_participant_update(p.clone());

        if let Some(on_claims_changed) = on_claims_changed {
            on_claims_changed(p);
        }
    }

    pub fn set_attributes(&self, attrs: &HashMap<String, String>) {
        if attrs.is_empty() {
            return;
        }
        let on_claims_changed = {
            let mut inner = self.lock.write();
            let mut grants = (**self.grants.read()).clone();
            let mut keys_to_delete = Vec::new();
            for (k, v) in attrs {
                if v.is_empty() {
                    keys_to_delete.push(k);
                } else {
                    grants.attributes.insert(k.clone(), v.clone());
                }
            }
            for k in keys_to_delete {
                grants.attributes.remove(k);
            }

            *self.grants.write() = Arc::new(grants);
            inner.require_broadcast = true; // already checked above
            self.dirty.store(true);

            inner.on_claims_changed.clone()
        };

        let p = self.arc();
        self.listener().on_participant_update(p.clone());

        if let Some(on_claims_changed) = on_claims_changed {
            on_claims_changed(p);
        }
    }

    pub fn claim_grants(&self) -> Arc<auth::ClaimGrants> {
        self.grants.read().clone()
    }

    pub fn token_expires_at(&self) -> Time {
        self.params.token_expires_at
    }

    pub fn set_permission(&self, permission: Option<&livekit::ParticipantPermission>) -> bool {
        let Some(permission) = permission else {
            return false;
        };
        let (grants, on_claims_changed, is_publisher) = {
            let mut inner = self.lock.write();
            let grants = self.grants.read().clone();

            if video_grant(&grants).matches_permission(Some(permission)) {
                return false;
            }

            infow!(self.params.logger, "updating participant permission", "permission" => permission);

            let mut grants = (*grants).clone();
            if let Some(video) = grants.video.as_mut() {
                video.update_from_permission(Some(permission));
            }
            let grants = Arc::new(grants);
            *self.grants.write() = grants.clone();
            self.dirty.store(true);

            let can_publish = video_grant(&grants).get_can_publish();

            let on_claims_changed = inner.on_claims_changed.clone();

            let is_publisher = can_publish && self.transport_manager.is_publisher_established();
            inner.require_broadcast = inner.require_broadcast || is_publisher;
            (grants, on_claims_changed, is_publisher)
        };
        let video = video_grant(&grants);
        let can_subscribe = video.get_can_subscribe();

        // publish permission has been revoked then remove offending tracks
        for track in self.up_track_manager.get_published_tracks() {
            if !video.get_can_publish_source(track.source()) {
                self.remove_published_track(track);
            }
        }

        if can_subscribe {
            // reconcile everything
            self.subscription_manager.reconcile_all();
        } else {
            // revoke all subscriptions
            let id = self.id();
            for st in self.subscription_manager.get_subscribed_tracks() {
                // a nil media track would be dereferenced upstream
                st.media_track()
                    .expect("nil pointer dereference: subscribed track without media track")
                    .remove_subscriber(&id, false);
            }
            for ddt in self.subscription_manager.get_subscribed_data_tracks() {
                ddt.publish_data_track()
                    .expect("nil pointer dereference: data down track without data track")
                    .remove_subscriber(&id);
            }
        }

        if !video.get_can_publish_data() {
            for dt in self.up_data_track_manager.get_published_data_tracks() {
                self.up_data_track_manager.remove_published_data_track(dt);
            }
        }

        // update isPublisher attribute
        self.is_publisher.store(is_publisher);

        let p = self.arc();
        self.listener().on_participant_update(p.clone());

        if let Some(on_claims_changed) = on_claims_changed {
            on_claims_changed(p);
        }
        true
    }

    pub fn can_skip_broadcast(&self) -> bool {
        let inner = self.lock.read();
        !inner.require_broadcast
    }

    fn maybe_inc_version(&self) {
        if self.dirty.load() {
            let mut inner = self.lock.write();
            if self.dirty.swap(false) {
                self.version.inc();
                inner
                    .timed_version
                    .update(self.params.version_generator.next());
            }
        }
    }

    pub fn version(&self) -> TimedVersion {
        self.maybe_inc_version();

        let inner = self.lock.read();
        inner.timed_version
    }

    pub fn to_proto_with_version(&self) -> (livekit::ParticipantInfo, TimedVersion) {
        self.maybe_inc_version();

        let (mut pi, piv) = {
            let inner = self.lock.read();
            let grants = self.grants.read().clone();
            let v = self.version.load();
            let piv = inner.timed_version;

            let mut client_protocol = 0;
            if let Some(client_info) = &self.params.client_info.client_info {
                client_protocol = client_info.client_protocol;
            }
            let pi = livekit::ParticipantInfo {
                sid: self.id().as_str().to_string(),
                identity: self.params.identity.as_str().to_string(),
                name: grants.name.clone(),
                state: self.state() as i32,
                joined_at: self.connected_at().unix_secs(),
                joined_at_ms: self.connected_at().unix_milli(),
                version: v,
                permission: Some(video_grant(&grants).to_permission()),
                metadata: grants.metadata.clone(),
                attributes: grants.attributes.clone(),
                region: self.params.region.clone(),
                is_publisher: self.is_publisher(),
                kind: grants.get_participant_kind() as i32,
                kind_details: grants
                    .get_kind_details()
                    .into_iter()
                    .map(|d| d as i32)
                    .collect(),
                disconnect_reason: self.close_reason().to_disconnect_reason() as i32,
                client_protocol,
                // `p.params.ClientInfo.GetCapabilities()`
                capabilities: self
                    .params
                    .client_info
                    .client_info
                    .as_ref()
                    .map(|c| c.capabilities.clone())
                    .unwrap_or_default(),
                ..Default::default()
            };
            (pi, piv)
        };

        {
            let pt = self.pending_tracks_lock.read();
            pi.tracks = self.up_track_manager.to_proto();

            // add any pending migrating tracks, else an update could delete/unsubscribe from yet to be published, migrating tracks
            let mut maybe_add = |pti: &PendingTrackInfo| {
                if !pti.migrated {
                    return;
                }

                let mut found = false;
                for ti in &pi.tracks {
                    if ti.sid == pti.track_infos[0].sid {
                        found = true;
                        break;
                    }
                }

                if !found {
                    pi.tracks.push(pti.track_infos[0].clone());
                }
            };

            for pt in pt.pending_tracks.values() {
                maybe_add(pt);
            }
            for ppt in pt.pending_publishing_tracks.values() {
                maybe_add(ppt);
            }
        }

        pi.data_tracks = self.up_data_track_manager.to_proto();

        (pi, piv)
    }

    pub fn to_proto(&self) -> livekit::ParticipantInfo {
        let (pi, _) = self.to_proto_with_version();
        pi
    }

    pub fn telemetry_guard(&self) -> Option<Arc<telemetry::ReferenceGuard>> {
        let inner = self.lock.read();

        Some(inner.telemetry_guard.clone())
    }

    #[allow(clippy::type_complexity)]
    pub fn add_on_close(
        &self,
        key: &str,
        callback: Option<Box<dyn Fn(Arc<dyn LocalParticipant>) + Send + Sync>>,
    ) {
        if self.is_closed.load() {
            if let Some(callback) = callback {
                let p: Arc<dyn LocalParticipant> = self.arc();
                time::spawn(async move { callback(p) });
            }
            return;
        }

        let mut inner = self.lock.write();
        match callback {
            None => {
                inner.on_close.remove(key);
            }
            Some(callback) => {
                inner.on_close.insert(key.to_string(), Arc::from(callback));
            }
        }
    }

    pub fn on_claims_changed(
        &self,
        callback: Box<dyn Fn(Arc<dyn LocalParticipant>) + Send + Sync>,
    ) {
        let mut inner = self.lock.write();
        inner.on_claims_changed = Some(Arc::from(callback));
    }

    pub fn handle_signal_source_close(&self) {
        self.transport_manager.set_signal_source_valid(false);

        if !self.has_ice_connected() {
            let _ = self.close(
                false,
                types::ParticipantCloseReason::SignalSourceClose,
                false,
            );
        }
    }

    fn synthesize_add_track_requests(
        &self,
        parsed_offer: &sdp::SessionDescription,
    ) -> Result<(), Error> {
        for m in &parsed_offer.media_descriptions {
            if !equal_fold(&m.media_name.media, "audio")
                && !equal_fold(&m.media_name.media, "video")
            {
                continue;
            }

            let mut cid = protosdp::get_media_stream_track(m);
            if cid.is_empty() {
                cid = guid::new(putils::TRACK_PREFIX);
            }

            let rids = protosdp::get_simulcast_rids(m);

            let (name, track_source, track_type) = if equal_fold(&m.media_name.media, "audio") {
                (
                    "synthesized-microphone",
                    livekit::TrackSource::Microphone,
                    livekit::TrackType::Audio,
                )
            } else {
                (
                    "synthesized-camera",
                    livekit::TrackSource::Camera,
                    livekit::TrackType::Video,
                )
            };
            #[allow(deprecated)]
            let mut req = livekit::AddTrackRequest {
                cid,
                name: name.to_string(),
                source: track_source as i32,
                r#type: track_type as i32,
                disable_dtx: true,
                stereo: false,
                stream: "camera".to_string(),
                ..Default::default()
            };
            if equal_fold(&m.media_name.media, "video") {
                if let Some(rids) = rids {
                    // add simulcast layers, NOTE: only quality can be set as dimensions/fps is not available
                    let n = rids
                        .len()
                        .min(buffer::DEFAULT_MAX_LAYER_SPATIAL as usize + 1);
                    for i in 0..n {
                        // WARN: casting int -> protobuf enum
                        req.layers.push(livekit::VideoLayer {
                            quality: i as i32,
                            ..Default::default()
                        });
                    }
                } else {
                    // dummy layer to ensure at least one layer is available
                    req.layers = vec![livekit::VideoLayer::default()];
                }
            }
            self.add_track(&req);
        }
        Ok(())
    }

    fn update_rids_from_sdp(
        &self,
        parsed: &sdp::SessionDescription,
        unmatch_videos: &[&sdp::MediaDescription],
    ) {
        for m in &parsed.media_descriptions {
            if m.media_name.media != "video" || !unmatch_videos.iter().any(|u| std::ptr::eq(*u, m))
            {
                continue;
            }

            let mst = protosdp::get_media_stream_track(m);
            if mst.is_empty() {
                continue;
            }

            let get_rids = |in_rids: &buffer::VideoLayersRid| -> buffer::VideoLayersRid {
                let mut out_rids = buffer::VideoLayersRid::default();
                if let Some(rids) = protosdp::get_simulcast_rids(m) {
                    let n = rids.len().min(in_rids.len());
                    for i in 0..n {
                        // disabled layers will have a `~` prefix, remove it while determining actual rid
                        if let Some(stripped) = rids[i].strip_prefix('~') {
                            out_rids[i] = stripped.to_string();
                        } else {
                            out_rids[i] = rids[i].clone();
                        }
                    }
                    for rid in out_rids.iter_mut().take(in_rids.len()).skip(n) {
                        *rid = String::new();
                    }
                    out_rids = buffer::normalize_video_layers_rid(&out_rids);
                } else {
                    for rid in out_rids.iter_mut().take(in_rids.len()) {
                        *rid = String::new();
                    }
                }

                out_rids
            };

            let found_pending = {
                let mut pt = self.pending_tracks_lock.write();
                match pt.get_pending_track_primary_by_sdp_cid(&mst) {
                    Some(pti) => {
                        pti.sdp_rids = get_rids(&pti.sdp_rids);
                        debugw!(
                            self.pub_logger,
                            "pending track rids updated",
                            "trackID" => pti.track_infos[0].sid,
                            "pendingTrack" => pti,
                        );

                        // the layers are set from a copy of the track info: what the two
                        // functions read of it, the qualities, does not change in the loop
                        let ti_before = pti.track_infos[0].clone();
                        let sdp_rids = pti.sdp_rids.clone();
                        let ti = &mut pti.track_infos[0];
                        for codec in &mut ti.codecs {
                            if codec.cid == mst || codec.sdp_cid == mst {
                                let mime_type = mime::normalize_mime_type(&codec.mime_type);
                                for layer in &mut codec.layers {
                                    layer.spatial_layer = buffer::video_quality_to_spatial_layer(
                                        mime_type,
                                        layer.quality(),
                                        Some(&ti_before),
                                    );
                                    layer.rid = buffer::video_quality_to_rid(
                                        mime_type,
                                        layer.quality(),
                                        Some(&ti_before),
                                        &sdp_rids,
                                    );
                                }
                            }
                        }
                        true
                    }
                    None => false,
                }
            };

            if !found_pending {
                // track could already be published, but this could be back up codec offer,
                // so check in published tracks also
                if let Some(mt) = self.get_published_track_by_sdp_cid(&mst) {
                    let mt = as_media_track(mt);
                    let mime_type = mt.get_mime_type_for_sdp_cid(&mst);
                    if mime_type != mime::MimeType::Unknown {
                        let rids = get_rids(&buffer::DEFAULT_VIDEO_LAYERS_RID);
                        mt.update_codec_rids(mime_type, &rids);
                        debugw!(
                            self.pub_logger,
                            "published track rids updated",
                            "trackID" => mt.id(),
                            "mime" => mime_type,
                            "track" => mt.to_proto(),
                        );
                    } else {
                        warnw!(
                            self.pub_logger,
                            "could not get mime type for sdp cid", nil,
                            "trackID" => mt.id(),
                            "sdpCid" => mst,
                            "track" => mt.to_proto(),
                        );
                    }
                }
            }
        }
    }

    pub fn handle_ice_trickle(&self, trickle_request: &livekit::TrickleRequest) {
        let candidate_init = match protosignalling::from_proto_trickle(trickle_request) {
            Ok(candidate_init) => candidate_init,
            Err(err) => {
                warnw!(self.params.logger, "could not decode trickle", err);
                let _ = self.send_request_response(livekit::RequestResponse {
                    reason: livekit::request_response::Reason::UnclassifiedError as i32,
                    message: err.to_string(),
                    request: Some(livekit::request_response::Request::Trickle(
                        trickle_request.clone(),
                    )),
                    ..Default::default()
                });
                return;
            }
        };

        // the target is a number on the wire: the original's manager logs one it does not
        // know, which its enum here cannot be handed
        match livekit::SignalTarget::try_from(trickle_request.target) {
            Ok(target) => self
                .transport_manager
                .add_ice_candidate(candidate_init, target),
            Err(_) => {
                warnw!(
                    self.params.logger.with_component(sutils::COMPONENT_TRANSPORT),
                    "ice candidate for unknown signal target",
                    "unknown signal target",
                    "target" => trickle_request.target,
                );
            }
        }
    }

    // HandleOffer an offer from remote participant, used when clients make the initial connection
    pub fn handle_offer(&self, sd: &livekit::SessionDescription) -> Result<(), Error> {
        let (offer, offer_id, _) = protosignalling::from_proto_session_description(sd);
        // `lgr := p.pubLogger.WithUnlikelyValues(...)`: the three values are on the lines

        debugw!(
            self.pub_logger,
            "received offer",
            "transport" => livekit::SignalTarget::Publisher,
            "offer" => offer,
            "offerId" => offer_id,
        );

        if self.params.use_one_shot_signalling_mode {
            let parsed_offer = match offer.unmarshal() {
                Ok(parsed_offer) => parsed_offer,
                Err(err) => {
                    warnw!(
                        self.pub_logger,
                        "could not parse offer", err,
                        "transport" => livekit::SignalTarget::Publisher,
                        "offer" => offer,
                        "offerId" => offer_id,
                    );
                    return Err(err.into());
                }
            };
            if let Err(err) = self.synthesize_add_track_requests(&parsed_offer) {
                warnw!(
                    self.pub_logger,
                    "could not synthesize add track requests", err,
                    "transport" => livekit::SignalTarget::Publisher,
                    "offer" => offer,
                    "offerId" => offer_id,
                );
                return Err(err);
            }
        }

        if let Err(err) = self.transport_manager.handle_offer(
            offer.clone(),
            offer_id,
            self.migrate_state() == types::MigrateState::Init,
        ) {
            warnw!(
                self.pub_logger,
                "could not handle offer", err,
                "transport" => livekit::SignalTarget::Publisher,
                "offer" => offer,
                "offerId" => offer_id,
                "mungedOffer" => offer,
            );
            return Err(err);
        }

        if self.params.use_one_shot_signalling_mode {
            let listener = self.listener();
            let p = self.arc();
            time::spawn(async move { listener.on_subscriber_ready(p) });
        }

        self.handle_pending_remote_tracks();
        Ok(())
    }

    fn on_publisher_set_remote_description(&self) {
        let Some(parsed_offer) = self.transport_manager.last_publisher_offer_parsed() else {
            return;
        };

        // set publish codec preferences after remote description is set
        // and required transceivers are created
        let (unmatch_audios, unmatch_videos) = self.populate_sdp_cid(&parsed_offer);
        self.set_codec_preferences_for_publisher(&parsed_offer, &unmatch_audios, &unmatch_videos);
        self.update_rids_from_sdp(&parsed_offer, &unmatch_videos);
    }

    fn on_publisher_answer(
        &self,
        answer: SessionDescription,
        answer_id: u32,
        mid_to_track_id: &HashMap<String, String>,
    ) -> Result<(), Error> {
        if self.is_closed() || self.is_disconnected() {
            return Ok(());
        }

        let answer = self.configure_publisher_answer(answer);
        debugw!(
            self.pub_logger,
            "sending answer",
            "transport" => livekit::SignalTarget::Publisher,
            "answer" => answer,
            "answerId" => answer_id,
            "midToTrackID" => mid_to_track_id,
        );

        self.send_sdp_answer(answer, answer_id, mid_to_track_id.clone())?;

        // The answer does not carry the pause state of the simulcast layers (SetIgnoreRidPauseForRecv),
        // so applying it re-enables in the publisher the layers paused by dynacast. Send the subscribed
        // qualities again with a delay to ensure the answer has been applied, once for a burst of answers:
        // the delay restarts with each answer, up to a maximum from the first one
        let mut inner = self.lock.write();
        match &inner.dynacast_resend_timer {
            None => {
                inner.dynacast_resend_deadline =
                    time::now().add(PUBLISHER_ANSWER_DYNACAST_RESEND_MAX_DELAY);
                let weak = self.this.clone();
                inner.dynacast_resend_timer = Some(time::after_func(
                    PUBLISHER_ANSWER_DYNACAST_RESEND_DELAY,
                    move || {
                        if let Some(p) = weak.upgrade() {
                            p.resend_subscribed_qualities();
                        }
                    },
                ));
            }
            Some(timer) => {
                timer.reset(
                    PUBLISHER_ANSWER_DYNACAST_RESEND_DELAY
                        .min(time::until(inner.dynacast_resend_deadline)),
                );
            }
        }
        Ok(())
    }

    fn resend_subscribed_qualities(&self) {
        self.lock.write().dynacast_resend_timer = None;

        if self.is_closed() || self.is_disconnected() {
            return;
        }
        for track in self.up_track_manager.get_published_tracks() {
            if let Some(mt) = downcast_media_track(track) {
                mt.resend_subscribed_quality();
            }
        }
    }

    pub fn get_answer(&self) -> Result<(SessionDescription, u32), Error> {
        if self.is_closed() || self.is_disconnected() {
            return Err(Error::ParticipantSessionClosed);
        }

        let (answer, answer_id) = self.transport_manager.get_answer()?;

        let answer = self.configure_publisher_answer(answer);
        debugw!(
            self.pub_logger,
            "returning answer",
            "transport" => livekit::SignalTarget::Publisher,
            "answer" => answer,
            "answerId" => answer_id,
        );
        Ok((answer, answer_id))
    }

    // HandleAnswer handles a client answer response, with subscriber PC, server initiates the
    // offer and client answers
    pub fn handle_answer(&self, sd: &livekit::SessionDescription) {
        let (answer, answer_id, _) = protosignalling::from_proto_session_description(sd);
        debugw!(
            self.sub_logger,
            "received answer",
            "transport" => livekit::SignalTarget::Subscriber,
            "answer" => answer,
            "answerId" => answer_id,
        );

        /* from server received join request to client answer
         * 1. server send join response & offer
         * ... swap candidates
         * 2. client send answer
         */
        let signal_conn_cost = time::since(self.connected_at()).milliseconds();
        self.transport_manager
            .update_signaling_rtt(signal_conn_cost as u32);

        self.transport_manager.handle_answer(answer, answer_id);
    }

    fn handle_migrate_tracks(&self) -> Vec<Arc<MediaTrack>> {
        // muted track won't send rtp packet, so it is required to add mediatrack manually.
        // But, synthesising track publish for unmuted tracks also keeps a consistent path.
        // In both cases (muted and unmuted), when publisher sends media packets, OnTrack would register and go from there.
        let mut added_tracks = Vec::new();
        let mut pt = self.pending_tracks_lock.write();
        // the pending tracks are collected first: adding one removes it from the map, which
        // Go's range allows while it runs
        let migrated: Vec<(String, livekit::TrackInfo, usize)> = pt
            .pending_tracks
            .iter()
            .filter(|(_, pti)| pti.migrated)
            .map(|(cid, pti)| {
                (
                    cid.clone(),
                    pti.track_infos[0].clone(),
                    pti.track_infos.len(),
                )
            })
            .collect();
        for (cid, ti, count) in migrated {
            if count > 1 {
                warnw!(
                    self.pub_logger,
                    "too many pending migrated tracks", nil,
                    "trackID" => ti.sid,
                    "count" => count,
                    "cid" => cid,
                );
            }

            match self.add_migrated_track(&mut pt, &cid, &ti) {
                Some(mt) => added_tracks.push(mt),
                None => {
                    warnw!(self.pub_logger, "could not find migrated track, migration failed", nil, "cid" => cid);
                    drop(pt);
                    self.issue_full_reconnect(types::ParticipantCloseReason::MigrateCodecMismatch);
                    return Vec::new();
                }
            }
        }

        if !added_tracks.is_empty() {
            self.dirty.store(true);
        }
        drop(pt);

        added_tracks
    }

    // AddTrack is called when client intends to publish track.
    // records track details and lets client know it's ok to proceed
    pub fn add_track(&self, req: &livekit::AddTrackRequest) {
        debugw!(self.params.logger, "add track request", "trackID" => req.cid, "request" => req);
        if !self.can_publish_source(req.source()) {
            warnw!(self.pub_logger, "no permission to publish track", nil, "trackID" => req.sid, "kind" => track_type_string(req.r#type));
            let _ = self.send_request_response(livekit::RequestResponse {
                reason: livekit::request_response::Reason::NotAllowed as i32,
                request: Some(livekit::request_response::Request::AddTrack(req.clone())),
                ..Default::default()
            });
            return;
        }

        if req.r#type != livekit::TrackType::Audio as i32
            && req.r#type != livekit::TrackType::Video as i32
        {
            warnw!(self.pub_logger, "unsupported track type", nil, "trackID" => req.sid, "kind" => track_type_string(req.r#type));
            let _ = self.send_request_response(livekit::RequestResponse {
                reason: livekit::request_response::Reason::UnsupportedType as i32,
                request: Some(livekit::request_response::Request::AddTrack(req.clone())),
                ..Default::default()
            });
            return;
        }

        let Some(ti) = self.add_pending_track(req) else {
            return;
        };

        let _ = self.send_track_published(&req.cid, &ti);

        self.handle_pending_remote_tracks();
    }

    pub fn set_migrate_info(
        &self,
        previous_offer: Option<&SessionDescription>,
        previous_answer: Option<&SessionDescription>,
        media_tracks: &[livekit::TrackPublishedResponse],
        data_channels: &[livekit::DataChannelInfo],
        data_channel_receive_state: &[livekit::DataChannelReceiveState],
        data_tracks: &[livekit::PublishDataTrackResponse],
    ) {
        let now = time::now();
        self.migrated_in_at.store(now);

        {
            let mut pt = self.pending_tracks_lock.write();
            for t in media_tracks {
                let ti = t
                    .track
                    .clone()
                    .expect("nil pointer dereference: TrackPublishedResponse without track");

                if let Some(supervisor) = &self.supervisor {
                    let track_id = livekit::TrackId::from(ti.sid.as_str());
                    supervisor.add_publication(&track_id);
                    supervisor.set_publication_mute(&track_id, ti.muted);
                }

                let track_id = ti.sid.clone();
                pt.pending_tracks.insert(
                    t.cid.clone(),
                    PendingTrackInfo {
                        track_infos: vec![ti],
                        migrated: true,
                        created_at: now,
                        ..Default::default()
                    },
                );
                infow!(
                    self.pub_logger,
                    "pending track added (migration)",
                    "trackID" => track_id,
                    "cid" => t.cid,
                    "pendingTrack" => pt.pending_tracks.get(&t.cid),
                );
            }
        }

        // for migrating in tracks, there is no AddTrack, so record a synthetic publish request
        for t in media_tracks {
            self.get_telemetry_listener().on_track_publish_requested(
                &self.id(),
                &self.identity(),
                t.track
                    .as_ref()
                    .expect("nil pointer dereference: TrackPublishedResponse without track"),
                false,
            );
        }

        for t in data_tracks {
            let dti = t
                .info
                .as_ref()
                .expect("nil pointer dereference: PublishDataTrackResponse without info");
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
                dti,
            );
            self.up_data_track_manager.add_published_data_track(dt);
        }

        if !media_tracks.is_empty() || !data_tracks.is_empty() {
            self.set_is_publisher(true);
        }

        {
            let mut joining = self.reliable_data_info.joining_message_lock.lock();
            for state in data_channel_receive_state {
                joining.first_seqs.insert(
                    livekit::ParticipantId::from(state.publisher_sid.as_str()),
                    state.last_seq.wrapping_add(1),
                );
            }
        }

        self.transport_manager
            .set_migrate_info(previous_offer, previous_answer, data_channels);
    }

    pub fn is_reconnect(&self) -> bool {
        self.params.reconnect
    }

    pub fn is_migration(&self) -> bool {
        self.params.migration
    }

    fn record_rtc_state(&self, close_reason: types::ParticipantCloseReason) {
        if self.has_ice_connected() {
            return;
        }

        if self.is_connection_canceled(close_reason) {
            prometheus::increment_participant_rtc_canceled(
                1,
                self.params.enable_warp,
                self.client_sdk(),
            );
        } else {
            prometheus::increment_participant_rtc_failure(
                1,
                self.params.enable_warp,
                self.client_sdk(),
            );
        }
    }

    pub fn is_connection_canceled(&self, close_reason: types::ParticipantCloseReason) -> bool {
        use types::ParticipantCloseReason as R;
        close_reason == R::JoinFailed
            || close_reason == R::ClientRequestLeave
            || close_reason == R::DuplicateIdentity
            || close_reason == R::RoomClosed
            || close_reason == R::MigrationRequested
            || close_reason == R::MigrationComplete
            // client closing signal connection too quickly, a quick close could be an indication of client leaving before a timeout
            // something longer could be clients timing out without sending a leave message and is usually a sign of failed connection
            || (time::since(self.params.session_start_time) < Duration::from_secs(3)
                && close_reason == R::SignalSourceClose)
    }

    pub fn close(
        &self,
        send_leave: bool,
        reason: types::ParticipantCloseReason,
        is_expected_to_resume: bool,
    ) -> Result<(), Error> {
        if self.is_closed.swap(true) {
            // already closed
            return Ok(());
        }

        self.record_rtc_state(reason);

        let mut session_duration = Duration::ZERO;
        let active_at = self.active_at();
        if !active_at.is_zero() {
            session_duration = time::since(active_at);
        }
        infow!(
            self.params.logger,
            "participant closing",
            "sendLeave" => send_leave,
            "reason" => reason.to_string(),
            "isExpectedToResume" => is_expected_to_resume,
            "clientInfo" => sutils::client_info_without_address(self.get_client_info().as_deref()),
            "kind" => self.kind(),
            "country" => self.params.country,
            "sessionDuration" => session_duration,
        );
        *self.close_reason.write() = reason;
        self.clear_disconnect_timer();
        self.clear_migration_timer();

        if send_leave {
            let _ = self.send_leave_request(
                reason,
                is_expected_to_resume,
                false, // isExpectedToReconnect
                false, // sendOnlyIfSupportingLeaveRequestWithAction
            );
        }

        if let Some(supervisor) = &self.supervisor {
            supervisor.stop();
        }

        let sdk = self.client_sdk();
        {
            let mut pt = self.pending_tracks_lock.write();
            if self.is_connection_canceled(reason) {
                for pti in pt.pending_tracks.values() {
                    if pti.track_infos.is_empty() {
                        continue;
                    }
                    prometheus::record_track_publish_cancels(
                        pti.track_infos[0].r#type().as_str_name(),
                        pti.track_infos.len() as i32,
                    );
                }
            } else {
                // record close time as publish time for non canceled pending tracks,
                // this will inflate publish time and can signal a node wide issue if
                // enough participants are closing without publishing tracks.
                for pti in pt.pending_tracks.values() {
                    if pti.track_infos.is_empty() || pti.migrated {
                        continue;
                    }
                    prometheus::record_publish_time(
                        &self.params.country,
                        pti.track_infos[0].source(),
                        pti.track_infos[0].r#type(),
                        time::since(pti.created_at),
                        sdk,
                        self.kind(),
                    );
                }
            }
            pt.pending_tracks = HashMap::new();
            pt.pending_publishing_tracks = HashMap::new();
        }

        self.up_track_manager.close(is_expected_to_resume);
        self.up_data_track_manager.close();

        {
            let mut rpc = self.rpc_lock.lock();
            rpc.rpc_pending_acks.clear();
            for handler in rpc.rpc_pending_responses.values() {
                (handler.resolve)(
                    "",
                    Some(putils::data_channel_rpc_error_from_built_in_codes(
                        putils::DATA_CHANNEL_RPC_RECIPIENT_DISCONNECTED,
                        "",
                    )),
                );
            }
            rpc.rpc_pending_responses = HashMap::new();
        }

        self.update_state(livekit::participant_info::State::Disconnected);
        self.disconnected.cancel();

        // ensure this is synchronized
        self.close_signal_connection(types::SignallingCloseReason::ParticipantClose);
        let on_close: Vec<OnParticipant> = self.lock.read().on_close.values().cloned().collect();
        let p: Arc<dyn LocalParticipant> = self.arc();
        for cb in on_close {
            cb(p.clone());
        }

        // Close peer connections without blocking participant Close. If peer connections are gathering candidates
        // Close will block.
        {
            // the goroutine holds the participant until both have closed: what the
            // subscription manager does for it on the way (the unsubscribes it tells telemetry
            // and the transport of) needs it there
            let p = self.arc();
            go_blocking(move || {
                p.subscription_manager.close(is_expected_to_resume);
                p.transport_manager.close();

                // `p.metricsCollector.Stop()`, `p.metricsReporter.Stop()`: `pkg/metric` is not
                // ported
            });
        }

        self.data_channel_stats.stop();
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load()
    }

    pub fn close_reason(&self) -> types::ParticipantCloseReason {
        *self.close_reason.read()
    }

    // Negotiate subscriber SDP with client, if force is true, will cancel pending
    // negotiate task and negotiate immediately
    pub fn negotiate(&self, force: bool) {
        if self.params.use_one_shot_signalling_mode {
            return;
        }

        if self.migrate_state() != types::MigrateState::Init {
            self.transport_manager.negotiate_subscriber(force);
        }
    }

    fn clear_migration_timer(&self) {
        let mut inner = self.lock.write();
        if let Some(timer) = inner.migration_timer.take() {
            timer.stop();
        }
    }

    fn setup_migration_timer_locked(&self, inner: &mut ParticipantImplInner) {
        if self.params.use_single_peer_connection {
            return;
        }

        //
        // On subscriber peer connection, remote side will try ICE on both
        // pre- and post-migration ICE candidates as the migrating out
        // peer connection leaves itself open to enable transition of
        // media with as less disruption as possible.
        //
        // But, sometimes clients could delay the migration because of
        // pinging the incorrect ICE candidates. Give the remote some time
        // to try and succeed. If not, close the subscriber peer connection
        // and help the remote side to narrow down its ICE candidate pool.
        //
        let weak = self.this.clone();
        inner.migration_timer = Some(time::after_func(
            self.params
                .migration_wait_duration
                .max(MIGRATION_WAIT_DURATION),
            move || {
                let Some(p) = weak.upgrade() else {
                    return;
                };
                p.clear_migration_timer();

                if p.is_closed() || p.is_disconnected() {
                    return;
                }
                debugw!(
                    p.sub_logger,
                    "closing subscriber peer connection to aid migration"
                );

                // the two closes wait for workers to end: off the runtime, on a thread of
                // their own, as the goroutine of `time.AfterFunc` may wait
                go_blocking(move || {
                    //
                    // Close all down tracks before closing subscriber peer connection.
                    // Closing subscriber peer connection will call `Unbind` on all down tracks.
                    // DownTrack close has checks to handle the case of closing before bind.
                    // So, an `Unbind` before close would bypass that logic.
                    //
                    p.subscription_manager.close(true);

                    p.transport_manager.subscriber_close();
                });
            },
        ));
    }

    pub fn maybe_start_migration(
        &self,
        force: bool,
        on_start: Option<Box<dyn Fn() + Send + Sync>>,
    ) -> bool {
        if self.is_closed() || self.params.use_one_shot_signalling_mode {
            return false;
        }

        let mut all_transport_connected = self.transport_manager.has_subscriber_ever_connected();
        if self.is_publisher() {
            all_transport_connected =
                all_transport_connected && self.transport_manager.has_publisher_ever_connected();
        }
        if !force && !all_transport_connected {
            return false;
        }

        if let Some(on_start) = on_start {
            on_start();
        }

        let _ = self.send_leave_request(
            types::ParticipantCloseReason::MigrationRequested,
            true,  // isExpectedToResume
            false, // isExpectedToReconnect
            true,  // sendOnlyIfSupportingLeaveRequestWithAction
        );
        self.close_signal_connection(types::SignallingCloseReason::Migration);

        self.clear_migration_timer();

        let mut inner = self.lock.write();
        self.setup_migration_timer_locked(&mut inner);
        drop(inner);

        true
    }

    pub fn notify_migration(&self) {
        let mut inner = self.lock.write();

        if inner.migration_timer.is_some() {
            // already set up
            return;
        }

        self.setup_migration_timer_locked(&mut inner);
    }

    pub fn set_migrate_state(&self, s: types::MigrateState) {
        let pre_state = self.migrate_state();
        if pre_state == types::MigrateState::Complete || pre_state == s {
            return;
        }

        debugw!(self.params.logger, "SetMigrateState", "state" => s);
        let mut migrated_tracks = Vec::new();
        if s == types::MigrateState::Complete {
            migrated_tracks = self.handle_migrate_tracks();
        }
        *self.migrate_state.write() = s;
        self.dirty.store(true);

        match s {
            types::MigrateState::Sync => {
                self.transport_manager.process_pending_publisher_offer();
            }

            types::MigrateState::Complete => {
                if pre_state == types::MigrateState::Sync {
                    infow!(self.params.logger, "migration complete");

                    // `if p.params.LastPubReliableSeq > 0`: the data cache of a participant
                    // that migrates in from another node is left out (see the header)
                }
                self.transport_manager
                    .process_pending_publisher_data_channels();
                let p = self.this.clone();
                time::spawn(async move {
                    if let Some(p) = p.upgrade() {
                        p.cache_forwarder_state();
                    }
                });
            }

            types::MigrateState::Init => {}
        }

        let weak = self.this.clone();
        let fuse = self.migrated_tracks_published_fuse.clone();
        time::spawn(async move {
            // launch callbacks in goroutine since they could block.
            // callbacks handle webhooks as well as db persistence
            if let Some(p) = weak.upgrade() {
                for t in migrated_tracks {
                    p.handle_track_published(t, true, true);
                }
            }

            if s == types::MigrateState::Complete {
                // wait for all migrated tracks to be published,
                // it is possible that synthesized track publish above could
                // race with actual publish from client and the above synthesized
                // one could actually be a no-op because the actual publish path is active.
                //
                // if the actual publish path has not finished, the migration state change
                // callback could close the remote participant/tracks before the local track
                // is fully active.
                //
                // that could lead to subscribers unsubscribing due to source
                // track going away, i. e. in this case, the remote track close would have
                // notified the subscription manager, the subscription manager would
                // re-resolve to check if the track is still active and unsubscribe if none
                // is active, as local track is in the process of completing publish,
                // the check would have resolved to an empty track leading to unsubscription.
                {
                    let weak = weak.clone();
                    let fuse = fuse.clone();
                    time::spawn(async move {
                        let start_time = time::now();
                        loop {
                            let done = match weak.upgrade() {
                                Some(p) => !p.has_pending_migrated_track() || p.is_disconnected(),
                                None => true,
                            };
                            if done || time::since(start_time) > Duration::from_secs(15) {
                                // a time out just to be safe, but it should not be needed
                                fuse.cancel();
                                return;
                            }

                            time::sleep(Duration::from_millis(20)).await;
                        }
                    });
                }

                fuse.cancelled().await;
            }

            if let Some(p) = weak.upgrade() {
                p.listener().on_migrate_state_change(p.clone(), s);
            }
        });
    }

    pub fn migrate_state(&self) -> types::MigrateState {
        *self.migrate_state.read()
    }

    // ICERestart restarts subscriber ICE connections
    pub fn ice_restart(&self, ice_config: Option<&livekit::IceConfig>) {
        if self.params.use_one_shot_signalling_mode {
            return;
        }

        self.clear_disconnect_timer();
        self.clear_migration_timer();

        for t in self.up_track_manager.get_published_tracks() {
            as_local_media_track(t).restart();
        }

        if self.transport_manager.ice_restart(ice_config).is_err() {
            self.issue_full_reconnect(types::ParticipantCloseReason::NegotiateFailed);
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn on_ice_config_changed(
        &self,
        f: Box<dyn Fn(Arc<dyn LocalParticipant>, &livekit::IceConfig) + Send + Sync>,
    ) {
        let mut inner = self.lock.write();
        inner.on_ice_config_changed = Some(Arc::from(f));
    }

    pub fn get_connection_quality(&self) -> livekit::ConnectionQualityInfo {
        let mut min_quality = livekit::ConnectionQuality::Excellent;
        let mut min_score = connectionquality::MAX_MOS;

        for pt in self.up_track_manager.get_published_tracks() {
            let (score, quality) = as_local_media_track(pt).get_connection_score_and_quality();
            if putils::is_connection_quality_lower(min_quality, quality) {
                min_quality = quality;
                min_score = score;
            } else if quality == min_quality && score < min_score {
                min_score = score;
            }
        }

        let subscribed_tracks = self.subscription_manager.get_subscribed_tracks();
        for sub_track in subscribed_tracks {
            let (score, quality) = sub_track
                .down_track()
                .expect("nil pointer dereference: subscribed track without down track")
                .get_connection_score_and_quality();
            if putils::is_connection_quality_lower(min_quality, quality) {
                min_quality = quality;
                min_score = score;
            } else if quality == min_quality && score < min_score {
                min_score = score;
            }
        }

        prometheus::record_quality(min_quality, min_score);

        if min_quality == livekit::ConnectionQuality::Lost
            && !self.protocol_version().supports_connection_quality_lost()
        {
            min_quality = livekit::ConnectionQuality::Poor;
        }

        {
            let mut inner = self.lock.write();
            if min_quality != inner.connection_quality {
                debugw!(self.params.logger, "connection quality changed", "from" => inner.connection_quality, "to" => min_quality);
            }
            inner.connection_quality = min_quality;
        }

        livekit::ConnectionQualityInfo {
            participant_sid: self.id().as_str().to_string(),
            quality: min_quality as i32,
            score: min_score,
        }
    }

    pub fn is_publisher(&self) -> bool {
        self.is_publisher.load()
    }

    pub fn can_publish(&self) -> bool {
        video_grant(&self.grants.read()).get_can_publish()
    }

    pub fn can_publish_source(&self, source: livekit::TrackSource) -> bool {
        video_grant(&self.grants.read()).get_can_publish_source(source)
    }

    pub fn can_subscribe(&self) -> bool {
        video_grant(&self.grants.read()).get_can_subscribe()
    }

    pub fn can_publish_data(&self) -> bool {
        video_grant(&self.grants.read()).get_can_publish_data()
    }

    pub fn hidden(&self) -> bool {
        video_grant(&self.grants.read()).hidden
    }

    pub fn can_subscribe_metrics(&self) -> bool {
        video_grant(&self.grants.read()).get_can_subscribe_metrics()
    }

    pub fn verify(&self) -> bool {
        let state = self.state();
        let mut is_active = state != livekit::participant_info::State::Joining
            && state != livekit::participant_info::State::Joined;
        if self.params.use_one_shot_signalling_mode {
            is_active = is_active && self.transport_manager.has_publisher_ever_connected();
        }

        is_active
    }

    pub fn verify_subscribe_participant_info(&self, p_id: &livekit::ParticipantId, version: u32) {
        if !self.is_ready() {
            // we have not sent a JoinResponse yet. metadata would be covered in JoinResponse
            return;
        }
        // the cache is safe for concurrent use upstream: here it is behind `updateLock`
        if let Some(info) = self.update_lock.lock().update_cache.get(p_id)
            && info.version >= version
        {
            return;
        }

        if let Some(info) = self.helper().get_participant_info(p_id) {
            let _ = self.send_participant_update(&[info]);
        }
    }

    // onTrackSubscribed handles post-processing after a track is subscribed
    fn on_track_subscribed(&self, sub_track: Arc<dyn types::SubscribedTrack>) {
        if self.params.client_info.fire_track_by_rtp_packet() {
            sub_track
                .down_track()
                .expect("nil pointer dereference: subscribed track without down track")
                .set_active_padding_on_mute_up_track();
        }

        let weak = self.this.clone();
        let weak_sub_track = Arc::downgrade(&sub_track);
        sub_track.add_on_bind(Box::new(move |err| {
            if err.is_err() {
                return;
            }
            let (Some(p), Some(sub_track)) = (weak.upgrade(), weak_sub_track.upgrade()) else {
                return;
            };
            if p.params.use_one_shot_signalling_mode {
                if p.transport_manager.has_publisher_ever_connected() {
                    let dt = sub_track
                        .down_track()
                        .expect("nil pointer dereference: subscribed track without down track");
                    dt.seed_state(sfu::DownTrackState {
                        forwarder_state: p.get_and_delete_forwarder_state(&sub_track.id()),
                        ..Default::default()
                    });
                    dt.set_connected();
                }
                // ONE-SHOT-SIGNALLING-MODE-TODO: video support should add to publisher PC for congestion control
            } else {
                if p.transport_manager.has_subscriber_ever_connected() {
                    let dt = sub_track
                        .down_track()
                        .expect("nil pointer dereference: subscribed track without down track");
                    dt.seed_state(sfu::DownTrackState {
                        forwarder_state: p.get_and_delete_forwarder_state(&sub_track.id()),
                        ..Default::default()
                    });
                    dt.set_connected();
                }
                p.transport_manager.add_subscribed_track(sub_track.as_ref());
            }
        }));
    }

    // onTrackUnsubscribed handles post-processing after a track is unsubscribed
    fn on_track_unsubscribed(&self, sub_track: Arc<dyn types::SubscribedTrack>) {
        self.transport_manager
            .remove_subscribed_track(sub_track.as_ref());
    }

    pub fn update_media_rtt(&self, rtt: u32) {
        let now = time::now();
        {
            let mut inner = self.lock.write();
            if now.sub(inner.rtt_updated_at) < RTT_UPDATE_INTERVAL || inner.last_rtt == rtt {
                return;
            }
            inner.rtt_updated_at = now;
            inner.last_rtt = rtt;
        }
        self.transport_manager.update_media_rtt(rtt);

        for pt in self.up_track_manager.get_published_tracks() {
            as_local_media_track(pt).set_rtt(rtt);
        }
    }
}

// ----------------------------------------------------------

// var _ transport.Handler = (*AnyTransportHandler)(nil)

// The handlers hold the participant weakly (see the header): what one is told after the
// participant is gone is dropped, and what it answers is then what a closed participant answers.
#[derive(Clone)]
pub struct AnyTransportHandler {
    p: ParticipantCell,
}

impl rtctransport::Handler for AnyTransportHandler {
    fn on_failed(&self, _is_short_lived: bool, _ici: &types::IceConnectionInfo) {
        if let Some(p) = participant_of(&self.p) {
            p.on_any_transport_failed();
        }
    }

    fn on_negotiation_failed(&self) {
        if let Some(p) = participant_of(&self.p) {
            p.on_any_transport_negotiation_failed();
        }
    }

    fn on_ice_candidate(
        &self,
        c: Option<&IceCandidate>,
        target: livekit::SignalTarget,
    ) -> Result<(), Error> {
        match participant_of(&self.p) {
            Some(p) => p.on_ice_candidate(c, target),
            None => Ok(()),
        }
    }
}

// ----------------------------------------------------------

pub struct PublisherTransportHandler {
    any_transport_handler: AnyTransportHandler,
}

impl rtctransport::Handler for PublisherTransportHandler {
    // the methods of the embedded `AnyTransportHandler`

    fn on_failed(&self, is_short_lived: bool, ici: &types::IceConnectionInfo) {
        self.any_transport_handler.on_failed(is_short_lived, ici);
    }

    fn on_negotiation_failed(&self) {
        self.any_transport_handler.on_negotiation_failed();
    }

    fn on_ice_candidate(
        &self,
        c: Option<&IceCandidate>,
        target: livekit::SignalTarget,
    ) -> Result<(), Error> {
        self.any_transport_handler.on_ice_candidate(c, target)
    }

    fn on_set_remote_description_offer(&self) {
        if let Some(p) = participant_of(&self.any_transport_handler.p) {
            p.on_publisher_set_remote_description();
        }
    }

    fn on_answer(
        &self,
        sd: SessionDescription,
        answer_id: u32,
        mid_to_track_id: &HashMap<String, String>,
    ) -> Result<(), Error> {
        match participant_of(&self.any_transport_handler.p) {
            Some(p) => p.on_publisher_answer(sd, answer_id, mid_to_track_id),
            None => Ok(()),
        }
    }

    fn on_track(
        &self,
        track: Arc<dyn webrtc::TrackRemote>,
        rtp_receiver: Arc<dyn webrtc::RtpReceiver>,
    ) {
        if let Some(p) = participant_of(&self.any_transport_handler.p) {
            p.on_media_track(track, rtp_receiver);
        }
    }

    fn on_initial_connected(&self) {
        if let Some(p) = participant_of(&self.any_transport_handler.p) {
            p.on_publisher_initial_connected();
        }
    }

    fn on_data_message(&self, kind: livekit::data_packet::Kind, data: &[u8]) {
        if let Some(p) = participant_of(&self.any_transport_handler.p) {
            p.on_received_data_message(kind, data);
        }
    }

    fn on_data_message_unlabeled(&self, data: &[u8]) {
        if let Some(p) = participant_of(&self.any_transport_handler.p) {
            p.on_received_data_message_unlabeled(data);
        }
    }

    fn on_data_track_message(&self, data: &[u8], arrival_time: i64) {
        if let Some(p) = participant_of(&self.any_transport_handler.p) {
            p.on_received_data_track_message(data, arrival_time);
        }
    }

    fn on_data_send_error(&self, err: Error) {
        if let Some(p) = participant_of(&self.any_transport_handler.p) {
            p.on_data_send_error(err);
        }
    }

    fn on_unmatched_media(&self, num_audios: u32, num_videos: u32) -> Result<(), Error> {
        match participant_of(&self.any_transport_handler.p) {
            Some(p) => p.send_media_sections_requirement(num_audios, num_videos),
            None => Ok(()),
        }
    }
}

// ----------------------------------------------------------

pub struct SubscriberTransportHandler {
    any_transport_handler: AnyTransportHandler,
}

impl rtctransport::Handler for SubscriberTransportHandler {
    // the methods of the embedded `AnyTransportHandler`

    fn on_failed(&self, is_short_lived: bool, ici: &types::IceConnectionInfo) {
        self.any_transport_handler.on_failed(is_short_lived, ici);
    }

    fn on_negotiation_failed(&self) {
        self.any_transport_handler.on_negotiation_failed();
    }

    fn on_ice_candidate(
        &self,
        c: Option<&IceCandidate>,
        target: livekit::SignalTarget,
    ) -> Result<(), Error> {
        self.any_transport_handler.on_ice_candidate(c, target)
    }

    fn on_offer(
        &self,
        sd: SessionDescription,
        offer_id: u32,
        mid_to_track_id: &HashMap<String, String>,
    ) -> Result<(), Error> {
        match participant_of(&self.any_transport_handler.p) {
            Some(p) => p.on_subscriber_offer(sd, offer_id, mid_to_track_id),
            None => Ok(()),
        }
    }

    fn on_stream_state_change(
        &self,
        update: &streamallocator::StreamStateUpdate,
    ) -> Result<(), Error> {
        match participant_of(&self.any_transport_handler.p) {
            Some(p) => p.on_stream_state_change(update),
            None => Ok(()),
        }
    }

    fn on_initial_connected(&self) {
        if let Some(p) = participant_of(&self.any_transport_handler.p) {
            p.on_subscriber_initial_connected();
        }
    }

    fn on_data_send_error(&self, err: Error) {
        if let Some(p) = participant_of(&self.any_transport_handler.p) {
            p.on_data_send_error(err);
        }
    }
}

// ----------------------------------------------------------

// `PrimaryTransportHandler` embeds the `transport.Handler` it wraps: every method is handed on
// to it, the two of its own apart.
pub struct PrimaryTransportHandler {
    handler: Arc<dyn rtctransport::Handler>,
    p: ParticipantCell,
}

impl rtctransport::Handler for PrimaryTransportHandler {
    fn on_initial_connected(&self) {
        self.handler.on_initial_connected();
        if let Some(p) = participant_of(&self.p) {
            p.on_primary_transport_initial_connected();
        }
    }

    fn on_fully_established(&self) {
        if let Some(p) = participant_of(&self.p) {
            p.on_primary_transport_fully_established();
        }
    }

    // the methods of the embedded handler

    fn on_ice_candidate(
        &self,
        c: Option<&IceCandidate>,
        target: livekit::SignalTarget,
    ) -> Result<(), Error> {
        self.handler.on_ice_candidate(c, target)
    }

    fn on_failed(&self, is_short_lived: bool, ice_connection_info: &types::IceConnectionInfo) {
        self.handler.on_failed(is_short_lived, ice_connection_info);
    }

    fn on_track(
        &self,
        track: Arc<dyn webrtc::TrackRemote>,
        rtp_receiver: Arc<dyn webrtc::RtpReceiver>,
    ) {
        self.handler.on_track(track, rtp_receiver);
    }

    fn on_data_message(&self, kind: livekit::data_packet::Kind, data: &[u8]) {
        self.handler.on_data_message(kind, data);
    }

    fn on_data_message_unlabeled(&self, data: &[u8]) {
        self.handler.on_data_message_unlabeled(data);
    }

    fn on_data_track_message(&self, data: &[u8], arrival_time: i64) {
        self.handler.on_data_track_message(data, arrival_time);
    }

    fn on_data_send_error(&self, err: Error) {
        self.handler.on_data_send_error(err);
    }

    fn on_offer(
        &self,
        sd: SessionDescription,
        offer_id: u32,
        mid_to_track_id: &HashMap<String, String>,
    ) -> Result<(), Error> {
        self.handler.on_offer(sd, offer_id, mid_to_track_id)
    }

    fn on_set_remote_description_offer(&self) {
        self.handler.on_set_remote_description_offer();
    }

    fn on_answer(
        &self,
        sd: SessionDescription,
        answer_id: u32,
        mid_to_track_id: &HashMap<String, String>,
    ) -> Result<(), Error> {
        self.handler.on_answer(sd, answer_id, mid_to_track_id)
    }

    fn on_negotiation_state_changed(&self, state: rtctransport::NegotiationState) {
        self.handler.on_negotiation_state_changed(state);
    }

    fn on_negotiation_failed(&self) {
        self.handler.on_negotiation_failed();
    }

    fn on_stream_state_change(
        &self,
        update: &streamallocator::StreamStateUpdate,
    ) -> Result<(), Error> {
        self.handler.on_stream_state_change(update)
    }

    fn on_unmatched_media(&self, num_audios: u32, num_videos: u32) -> Result<(), Error> {
        self.handler.on_unmatched_media(num_audios, num_videos)
    }
}

// ----------------------------------------------------------

impl ParticipantImpl {
    #[allow(clippy::type_complexity)]
    fn setup_signalling(
        logger: &Logger,
        this: &Weak<ParticipantImpl>,
    ) -> (
        Arc<dyn ParticipantSignalling>,
        Arc<dyn ParticipantSignalHandler>,
        Arc<dyn ParticipantSignaller>,
    ) {
        let participant: Weak<dyn LocalParticipant> = this.clone();
        let signalling = signalling::Signalling::new(signalling::SignallingParams {
            logger: logger.clone(),
        });
        let signal_handler = signalling::SignalHandler::new(signalling::SignalHandlerParams {
            logger: logger.clone(),
            participant: participant.clone(),
        });
        let weak = this.clone();
        let signaller = signalling::SignallerAsync::new(signalling::SignallerAsyncParams {
            logger: logger.clone(),
            participant,
            on_handshake_opened: Some(Box::new(move || {
                if let Some(p) = weak.upgrade() {
                    p.flush_queued_updates();
                }
            })),
        });
        (signalling, signal_handler, signaller)
    }

    fn setup_transport_manager(
        params: &ParticipantParams,
        cell: &ParticipantCell,
        data_channel_stats: &Arc<BytesTrackStats>,
        enabled_publish_codecs: &[livekit::Codec],
        enabled_subscribe_codecs: &[livekit::Codec],
    ) -> Result<(Arc<twcc::Responder>, Arc<TransportManager>), Error> {
        let twcc = twcc::new_transport_wide_cc_responder();
        {
            let cell = cell.clone();
            twcc.on_feedback(move |pkts| {
                if let Some(p) = participant_of(&cell) {
                    p.post_rtcp(pkts);
                }
            });
        }
        let ath = AnyTransportHandler { p: cell.clone() };
        let mut pth: Arc<dyn rtctransport::Handler> = Arc::new(PublisherTransportHandler {
            any_transport_handler: ath.clone(),
        });
        let mut sth: Arc<dyn rtctransport::Handler> = Arc::new(SubscriberTransportHandler {
            any_transport_handler: ath,
        });

        // `p.CanSubscribe()`, of the grants the participant is made with
        let can_subscribe = params
            .grants
            .as_ref()
            .is_some_and(|grants| video_grant(grants).get_can_subscribe());
        let subscriber_as_primary = !params.use_one_shot_signalling_mode
            && (params.protocol_version.subscriber_as_primary() && can_subscribe)
            && !params.use_single_peer_connection;
        if subscriber_as_primary {
            sth = Arc::new(PrimaryTransportHandler {
                handler: sth,
                p: cell.clone(),
            });
        } else {
            pth = Arc::new(PrimaryTransportHandler {
                handler: pth,
                p: cell.clone(),
            });
        }

        let playout_delay_enabled = params.playout_delay.as_ref().is_some_and(|d| d.enabled);
        let mut tm_params = TransportManagerParams {
            // primary connection does not change, canSubscribe can change if permission was updated
            // after the participant has joined
            subscriber_as_primary,
            use_single_peer_connection: params.use_single_peer_connection,
            config: params.config.clone(),
            twcc: Some(twcc.clone()),
            protocol_version: params.protocol_version,
            congestion_control_config: params.congestion_control_config.clone(),
            enabled_publish_codecs: enabled_publish_codecs.to_vec(),
            enabled_subscribe_codecs: enabled_subscribe_codecs.to_vec(),
            sim_tracks: params.sim_tracks.clone(),
            client_info: params.client_info.clone(),
            migration: params.migration,
            allow_tcp_fallback: params.allow_tcp_fallback,
            tcp_fallback_rtt_threshold: params.tcp_fallback_rtt_threshold,
            allow_udp_unstable_fallback: params.allow_udp_unstable_fallback,
            turns_enabled: params.turns_enabled,
            allow_playout_delay: playout_delay_enabled,
            data_channel_max_buffered_amount: params.data_channel_max_buffered_amount,
            datachannel_slow_threshold: params.datachannel_slow_threshold,
            datachannel_lossy_target_latency: params.datachannel_lossy_target_latency,
            datachannel_data_track_target_latency: params.datachannel_data_track_target_latency,
            logger: params.logger.with_component(sutils::COMPONENT_TRANSPORT),
            publisher_handler: pth,
            subscriber_handler: sth,
            data_channel_stats: data_channel_stats.clone(),
            use_one_shot_signalling_mode: params.use_one_shot_signalling_mode,
            fire_on_track_by_sdp: params.fire_on_track_by_sdp,
            enable_data_tracks: params.enable_data_tracks,
            exclude_ipv6_local_candidates: params.exclude_ipv6_local_candidates,
            enable_warp: params.enable_warp,
            node: params.node.clone(),
        };
        if params.sync_streams
            && playout_delay_enabled
            && client_info_is_firefox(&params.client_info)
        {
            // we will disable playout delay for Firefox if the user is expecting
            // the streams to be synced. Firefox doesn't support SyncStreams
            tm_params.allow_playout_delay = false;
        }
        let tm = TransportManager::new(tm_params)?;

        Ok((twcc, tm))
    }

    // The rest of `setupTransportManager`, once the participant is made.
    fn setup_transport_manager_callbacks(&self) {
        let weak = self.this.clone();
        self.transport_manager.on_ice_config_changed(Box::new(
            move |ice_config: &livekit::IceConfig| {
                let Some(p) = weak.upgrade() else {
                    return;
                };
                let on_ice_config_changed = {
                    let mut inner = p.lock.write();
                    let on_ice_config_changed = inner.on_ice_config_changed.clone();

                    let client_conf = inner.client_conf.get_or_insert_with(Default::default);
                    if ice_config.preference_subscriber == livekit::IceCandidateType::IctTls as i32
                    {
                        client_conf.force_relay = livekit::ClientConfigSetting::Enabled as i32;
                    } else {
                        // UNSET indicates that clients could override RTCConfiguration to forceRelay
                        client_conf.force_relay = livekit::ClientConfigSetting::Unset as i32;
                    }
                    on_ice_config_changed
                };

                if let Some(on_ice_config_changed) = on_ice_config_changed {
                    on_ice_config_changed(p.clone(), ice_config);
                }
            },
        ));

        self.transport_manager
            .set_subscriber_allow_pause(self.params.subscriber_allow_pause);
    }

    // The callbacks of `setupUpTrackManager`, once the participant is made.
    fn setup_up_track_manager(&self) {
        let weak = self.this.clone();
        self.up_track_manager.on_published_track_updated(
            move |track: Arc<dyn types::MediaTrack>| {
                if let Some(p) = weak.upgrade() {
                    p.dirty.store(true);
                    p.listener().on_track_updated(p.clone(), track);
                }
            },
        );

        let weak = self.this.clone();
        self.up_track_manager.on_up_track_manager_close(move || {
            if let Some(p) = weak.upgrade() {
                p.on_up_track_manager_close();
            }
        });
    }

    fn setup_up_data_track_manager(
        pub_logger: &Logger,
        this: &Weak<ParticipantImpl>,
    ) -> UpDataTrackManager {
        let participant: Weak<dyn types::Participant> = this.clone();
        UpDataTrackManager::new(UpDataTrackManagerParams {
            logger: pub_logger.clone(),
            participant,
        })
    }

    fn setup_subscription_manager(
        params: &ParticipantParams,
        sub_logger: &Logger,
        this: &Weak<ParticipantImpl>,
        participant: Weak<dyn LocalParticipant>,
    ) -> Arc<SubscriptionManager> {
        let resolver_of = this.clone();
        let data_resolver_of = this.clone();
        let subscribed_of = this.clone();
        let unsubscribed_of = this.clone();
        let error_of = this.clone();
        SubscriptionManager::new(SubscriptionManagerParams {
            participant,
            logger: sub_logger.without_sampler(),
            track_resolver: Box::new(move |lp, ti| match resolver_of.upgrade() {
                Some(p) => p.helper().resolve_media_track(lp, ti),
                None => types::MediaResolverResult::default(),
            }),
            data_track_resolver: Box::new(move |lp, ti| match data_resolver_of.upgrade() {
                Some(p) => p.helper().resolve_data_track(lp, ti),
                None => types::DataResolverResult::default(),
            }),
            on_track_subscribed: Box::new(move |sub_track| {
                if let Some(p) = subscribed_of.upgrade() {
                    p.on_track_subscribed(sub_track);
                }
            }),
            on_track_unsubscribed: Box::new(move |sub_track| {
                if let Some(p) = unsubscribed_of.upgrade() {
                    p.on_track_unsubscribed(sub_track);
                }
            }),
            on_subscription_error: Box::new(move |track_id, fatal, err| {
                if let Some(p) = error_of.upgrade() {
                    p.on_subscription_error(track_id, fatal, err);
                }
            }),
            subscription_limit_video: params.subscription_limit_video,
            subscription_limit_audio: params.subscription_limit_audio,
            subscribe_pacer: params.subscribe_pacer.clone(),
            use_one_shot_signalling_mode: params.use_one_shot_signalling_mode,
        })
    }

    // The callback of `setupSubscriptionManager`, once the participant is made.
    fn setup_subscription_manager_callbacks(&self) {
        let weak = self.this.clone();
        self.subscription_manager
            .on_subscribe_status_changed(Box::new(move |publisher_id, subscribed| {
                if let Some(p) = weak.upgrade() {
                    p.listener()
                        .on_subscribe_status_changed(p.clone(), publisher_id, subscribed);
                }
            }));
    }

    // `MetricsCollectorTimeToCollectMetrics`, `MetricsCollectorBatchReady`,
    // `MetricsReporterBatchReady` and `setupMetrics`: what the participant is to the metrics of
    // `pkg/metric` (their provider and consumer), which is not ported, is not here.

    pub(super) fn update_state(&self, state: livekit::participant_info::State) {
        let mut old_state;
        loop {
            old_state = self.state.load();
            if state as i32 <= old_state {
                debugw!(self.params.logger, "ignoring out of order participant state", "state" => state.as_str_name());
                return;
            }
            if state == livekit::participant_info::State::Active {
                // `lastActiveAt.CompareAndSwap(nil, now)`
                let mut last_active_at = self.last_active_at.lock();
                if last_active_at.is_zero() {
                    *last_active_at = time::now();
                }
            }
            if self.state.cas(old_state, state as i32) {
                break;
            }
        }

        debugw!(self.params.logger, "updating participant state", "state" => state.as_str_name());
        self.dirty.store(true);

        let listener = self.listener();
        let p = self.arc();
        time::spawn(async move { listener.on_state_change(p) });

        if state == livekit::participant_info::State::Disconnected
            && old_state == livekit::participant_info::State::Active as i32
        {
            // `p.disconnectedAt.Store(...)`: read only by the observability reporter
            prometheus::record_session_duration(
                self.protocol_version().0 as i64,
                time::since(*self.last_active_at.lock()),
            );
        }
    }

    pub(super) fn set_is_publisher(&self, is_publisher: bool) {
        if self.is_publisher.swap(is_publisher) == is_publisher {
            return;
        }

        self.lock.write().require_broadcast = true;

        self.dirty.store(true);

        // trigger update as well if participant is already fully connected
        if self.state() == livekit::participant_info::State::Active {
            self.listener().on_participant_update(self.arc());
        }
    }

    // when the server has an offer for participant
    fn on_subscriber_offer(
        &self,
        offer: SessionDescription,
        offer_id: u32,
        mid_to_track_id: &HashMap<String, String>,
    ) -> Result<(), Error> {
        debugw!(
            self.sub_logger,
            "sending offer",
            "transport" => livekit::SignalTarget::Subscriber,
            "offer" => offer,
            "offerId" => offer_id,
            "midToTrackID" => mid_to_track_id,
        );
        self.send_sdp_offer(offer, offer_id, mid_to_track_id.clone())
    }

    fn remove_published_track(&self, track: Arc<dyn types::MediaTrack>) {
        let track_id = track.id();
        self.up_track_manager.remove_published_track(track, false);
        if self.protocol_version().supports_unpublish() {
            self.send_track_unpublished(&track_id);
        } else {
            // for older clients that don't support unpublish, mute to avoid them sending data
            self.send_track_muted(&track_id, true);
        }
    }

    // when a new remoteTrack is created, creates a Track and adds it to room
    fn on_media_track(
        &self,
        rtc_track: Arc<dyn webrtc::TrackRemote>,
        rtp_receiver: Arc<dyn webrtc::RtpReceiver>,
    ) {
        if self.is_disconnected() {
            return;
        }

        let codec;
        let mut from_sdp = false;
        if rtc_track.kind() == webrtc::RtpCodecType::Video
            && self.params.client_info.fire_track_by_rtp_packet()
        {
            // `if rtcTrack.Codec().PayloadType == 0`: the original reads the first packet in a
            // goroutine to determine the codec, and calls this again once it has it. The
            // connection layer announces a track at its first packet, with its codec: there
            // is nothing to wait for (see the header).
            codec = rtc_track.codec();
        } else {
            // track fired by sdp
            let codecs = rtp_receiver.get_parameters().codecs;
            if codecs.is_empty() {
                errorw!(
                    self.pub_logger,
                    "no negotiated codecs for track, track will be ignored", nil,
                    "trackID" => rtc_track.id(),
                    "StreamID" => rtc_track.stream_id(),
                );
                return;
            }
            codec = codecs[0].clone();
            from_sdp = true;
        }
        debugw!(
            self.params.logger,
            "onMediaTrack",
            "codec" => codec,
            "payloadType" => codec.payload_type,
            "fromSdp" => from_sdp,
            "parameters" => rtp_receiver.get_parameters(),
        );

        let track: Arc<dyn sfu::TrackRemote> =
            sfu::TrackRemoteFromSdp::new(rtc_track, codec.clone());
        let (published_track, is_new_track, is_receiver_added, sdp_rids) =
            self.media_track_received(track.clone(), rtp_receiver.clone());
        let Some(published_track) = published_track else {
            debugw!(
                self.pub_logger,
                "webrtc track published but can't find MediaTrack in pendingTracks",
                "kind" => track.kind().to_string(),
                "webrtcTrackID" => track.id(),
                "rid" => track.rid(),
                "ssrc" => track.ssrc(),
                "rtxSsrc" => track.rtx_ssrc(),
                "mime" => mime::normalize_mime_type(&codec.mime_type),
                "isNewTrack" => is_new_track,
                "isReceiverAdded" => is_receiver_added,
                "sdpRids" => sdp_rids,
            );
            return;
        };

        if !self.can_publish_source(published_track.source()) {
            warnw!(self.pub_logger, "no permission to publish mediaTrack", nil,
                "source" => published_track.source(),
            );
            self.remove_published_track(published_track);
            return;
        }

        self.transport_manager.rtp_stream_published(
            track.ssrc(),
            &self.get_publisher_mid(&rtp_receiver),
            &track.rid(),
        );

        self.set_is_publisher(true);
        self.dirty.store(true);

        infow!(
            self.pub_logger,
            "mediaTrack published",
            "kind" => track.kind().to_string(),
            "trackID" => published_track.id(),
            "webrtcTrackID" => track.id(),
            "rid" => track.rid(),
            "ssrc" => track.ssrc(),
            "rtxSsrc" => track.rtx_ssrc(),
            "mime" => mime::normalize_mime_type(&codec.mime_type),
            "trackInfo" => published_track.to_proto(),
            "fromSdp" => from_sdp,
            "isReceiverAdded" => is_receiver_added,
            "sdpRids" => sdp_rids,
        );

        if !is_new_track && !published_track.has_pending_codec() && self.is_ready() {
            self.dirty.store(true);
            self.listener()
                .on_track_updated(self.arc(), published_track);
        }
    }

    fn handle_pending_remote_tracks(&self) {
        let pending_tracks =
            std::mem::take(&mut self.pending_tracks_lock.write().pending_remote_tracks);
        for rt in pending_tracks {
            self.on_media_track(rt.track, rt.receiver);
        }
    }

    fn on_received_data_message(&self, kind: livekit::data_packet::Kind, data: &[u8]) {
        if self.is_disconnected() || !self.can_publish_data() {
            return;
        }

        self.data_channel_stats.add_bytes(data.len() as u64, false);

        let mut dp = match livekit::DataPacket::decode(data) {
            Ok(dp) => dp,
            Err(err) => {
                warnw!(self.pub_logger, "could not parse data packet", err);
                return;
            }
        };

        dp.participant_sid = self.id().as_str().to_string();
        if kind == livekit::data_packet::Kind::Reliable
            && dp.sequence > 0
            && self.reliable_data_info.stop_reliable_by_migrate_out.load()
        {
            return;
        }
        // `migrateInPubDataCache`: the cache of a participant that migrates in from another
        // node, whose reliable messages are held until they continue those of the other
        // node, is left out with migration between nodes (see the header). Without it,
        // every message is handled as it comes.

        self.handle_received_data_message(kind, dp);
    }

    fn handle_received_data_message(
        &self,
        kind: livekit::data_packet::Kind,
        mut dp: livekit::DataPacket,
    ) {
        if kind == livekit::data_packet::Kind::Reliable && dp.sequence > 0 {
            if self.reliable_data_info.last_pub_reliable_seq.load() >= dp.sequence {
                infow!(
                    self.params.logger,
                    "received out of order reliable data packet",
                    "lastPubReliableSeq" => self.reliable_data_info.last_pub_reliable_seq.load(),
                    "dpSequence" => dp.sequence,
                );
                return;
            }

            self.reliable_data_info
                .last_pub_reliable_seq
                .store(dp.sequence);
        }

        // trust the channel that it came in as the source of truth
        #[allow(deprecated)]
        {
            dp.kind = kind as i32;
        }

        let mut should_forward_data = true;
        let mut should_forward_metrics = false;
        let mut override_sender_identity = true;
        let identity = self.params.identity.as_str();
        // only forward on user payloads
        //
        // (a message of a oneof is never nil here: the checks of the original for a nil
        // payload, which return, have no counterpart)
        match &mut dp.value {
            #[allow(deprecated)]
            Some(livekit::data_packet::Value::User(u)) => {
                if self.hidden() {
                    u.participant_sid = String::new();
                    u.participant_identity = String::new();
                } else {
                    u.participant_sid = self.id().as_str().to_string();
                    u.participant_identity = identity.to_string();
                }
                if !dp.destination_identities.is_empty() {
                    u.destination_identities = dp.destination_identities.clone();
                } else {
                    dp.destination_identities = u.destination_identities.clone();
                }
            }
            Some(livekit::data_packet::Value::SipDtmf(_)) => {}
            Some(livekit::data_packet::Value::Transcription(_)) => {
                if !self.is_agent() {
                    should_forward_data = false;
                }
            }
            Some(livekit::data_packet::Value::ChatMessage(chat_message)) => {
                if self.is_agent()
                    && !dp.participant_identity.is_empty()
                    && identity != dp.participant_identity
                {
                    override_sender_identity = false;
                    chat_message.generated = true;
                }
            }
            Some(livekit::data_packet::Value::Metrics(_)) => {
                should_forward_data = false;
                should_forward_metrics = true;
                // `p.metricTimestamper.Process(payload.Metrics)`: `pkg/metric` is not ported
            }
            Some(livekit::data_packet::Value::RpcRequest(rpc_request)) => {
                debugw!(
                    self.pub_logger,
                    "received RPC request",
                    "method" => rpc_request.method,
                    "rpc_request_id" => rpc_request.id,
                    "destinationIdentities" => dp.destination_identities,
                );
            }
            Some(livekit::data_packet::Value::RpcResponse(rpc_response)) => {
                debugw!(
                    self.pub_logger,
                    "received RPC response",
                    "rpc_request_id" => rpc_response.request_id,
                );

                match &rpc_response.value {
                    Some(livekit::rpc_response::Value::Payload(payload)) => {
                        should_forward_data = !self.handle_incoming_rpc_response(
                            &rpc_response.request_id,
                            payload,
                            None,
                        );
                    }
                    Some(livekit::rpc_response::Value::Error(err)) => {
                        should_forward_data = !self.handle_incoming_rpc_response(
                            &rpc_response.request_id,
                            "",
                            Some(DataChannelRpcError {
                                code: DataChannelRpcErrorCode(err.code),
                                message: err.message.clone(),
                                data: err.data.clone(),
                            }),
                        );
                    }
                    _ => {
                        // Unknown response encoding: still forwarded downstream, logged as a canary.
                        warnw!(self.pub_logger, "unhandled RPC response value type", nil, "rpc_request_id" => rpc_response.request_id);
                    }
                }
            }
            Some(livekit::data_packet::Value::RpcAck(rpc_ack)) => {
                debugw!(
                    self.pub_logger,
                    "received RPC ack",
                    "rpc_request_id" => rpc_ack.request_id,
                );

                should_forward_data = !self.handle_incoming_rpc_ack(&rpc_ack.request_id);
            }
            Some(livekit::data_packet::Value::StreamHeader(stream_header)) => {
                prometheus::record_data_packet_stream(
                    stream_header,
                    dp.destination_identities.len(),
                );

                if self.is_agent()
                    && !dp.participant_identity.is_empty()
                    && identity != dp.participant_identity
                {
                    match &mut stream_header.content_header {
                        Some(livekit::data_stream::header::ContentHeader::TextHeader(
                            text_header,
                        )) => {
                            text_header.generated = true;
                            override_sender_identity = false;
                        }
                        _ => {
                            override_sender_identity = true;
                        }
                    }
                }
            }
            Some(livekit::data_packet::Value::StreamChunk(_)) => {}
            Some(livekit::data_packet::Value::StreamTrailer(_)) => {}
            Some(livekit::data_packet::Value::EncryptedPacket(_)) => {}
            payload => {
                warnw!(self.pub_logger, "received unsupported data packet", nil, "payload" => payload);
            }
        }

        // SFU typically asserts the sender's identity. However, agents are able to
        // publish data on behalf of the participant in case of transcriptions/text streams
        // in those cases we'd leave the existing identity on the data packet alone.
        if override_sender_identity {
            if self.hidden() {
                dp.participant_identity = String::new();
            } else {
                dp.participant_identity = identity.to_string();
            }
        }

        if should_forward_data {
            self.listener().on_data_message(self.arc(), kind, &dp);
        }
        if should_forward_metrics {
            self.listener().on_metrics(self.arc(), &dp);
        }
    }

    fn on_received_data_message_unlabeled(&self, data: &[u8]) {
        if self.is_disconnected() || !self.can_publish_data() {
            return;
        }

        self.data_channel_stats.add_bytes(data.len() as u64, false);

        self.listener().on_data_message_unlabeled(self.arc(), data);
    }

    fn on_ice_candidate(
        &self,
        c: Option<&IceCandidate>,
        target: livekit::SignalTarget,
    ) -> Result<(), Error> {
        if self.is_disconnected() || self.is_closed() {
            return Ok(());
        }

        if target == livekit::SignalTarget::Subscriber
            && self.migrate_state() == types::MigrateState::Init
        {
            return Ok(());
        }

        self.send_ice_candidate(c, target)
    }

    fn on_publisher_initial_connected(&self) {
        self.set_migrate_state(types::MigrateState::Complete);

        if let Some(supervisor) = &self.supervisor {
            supervisor.set_publisher_peer_connection_connected(true);
        }

        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            time::spawn(Self::subscriber_rtcp_worker(
                self.this.clone(),
                self.get_logger(),
            ));

            self.set_down_tracks_connected();
        }

        self.pub_rtcp_queue.start();
    }

    fn on_subscriber_initial_connected(&self) {
        time::spawn(Self::subscriber_rtcp_worker(
            self.this.clone(),
            self.get_logger(),
        ));

        self.set_down_tracks_connected();
    }

    fn on_primary_transport_initial_connected(&self) {
        if !self.has_pending_migrated_track()
            && self.up_track_manager.get_published_tracks().is_empty()
        {
            // if there are no published tracks, declare migration complete on primary transport initial connect,
            // else, wait for all tracks to be published and publisher peer connection established
            self.set_migrate_state(types::MigrateState::Complete);
        }

        if !self.session_start_recorded.swap(true) {
            prometheus::record_session_start_time(
                self.protocol_version().0 as i64,
                self.params.enable_warp,
                self.client_sdk(),
                time::since(self.params.session_start_time),
            );
            prometheus::increment_participant_rtc_success(
                1,
                self.params.enable_warp,
                self.client_sdk(),
            );
        }
        self.update_state(livekit::participant_info::State::Active);
    }

    fn on_primary_transport_fully_established(&self) {
        self.replay_joining_reliable_messages();
    }

    fn clear_disconnect_timer(&self) {
        let mut inner = self.lock.write();
        if let Some(timer) = inner.disconnect_timer.take() {
            timer.stop();
        }
    }

    fn setup_disconnect_timer(&self) {
        self.clear_disconnect_timer();

        let mut inner = self.lock.write();
        let weak = self.this.clone();
        inner.disconnect_timer = Some(time::after_func(DISCONNECT_CLEANUP_DURATION, move || {
            let Some(p) = weak.upgrade() else {
                return;
            };
            p.clear_disconnect_timer();

            if p.is_closed() || p.is_disconnected() {
                return;
            }
            let _ = p.close(
                true,
                types::ParticipantCloseReason::PeerConnectionDisconnected,
                false,
            );
        }));
    }

    fn on_any_transport_failed(&self) {
        if self.params.use_one_shot_signalling_mode {
            // as there is no way to notify participant, close the participant on transport failure
            let _ = self.close(
                false,
                types::ParticipantCloseReason::PeerConnectionDisconnected,
                false,
            );
            return;
        }

        let _ = self.send_leave_request(
            types::ParticipantCloseReason::PeerConnectionDisconnected,
            true,  // isExpectedToResume
            false, // isExpectedToReconnect
            true,  // sendOnlyIfSupportingLeaveRequestWithAction
        );

        // clients support resuming of connections when signalling becomes disconnected
        self.close_signal_connection(types::SignallingCloseReason::TransportFailure);

        // detect when participant has actually left.
        self.setup_disconnect_timer();
    }

    // subscriberRTCPWorker sends SenderReports periodically when the participant is subscribed to
    // other publishedTracks in the room.
    async fn subscriber_rtcp_worker(p: Weak<ParticipantImpl>, logger: Logger) {
        let worker = async move {
            loop {
                {
                    let Some(p) = p.upgrade() else {
                        return;
                    };
                    if p.is_disconnected() {
                        return;
                    }

                    let subscribed_tracks = p.subscription_manager.get_subscribed_tracks();

                    // send in batches of sdBatchSize
                    let mut batch_size = 0;
                    let mut pkts: Vec<Box<dyn rtcp::Packet>> = Vec::new();
                    let mut sd: Vec<SourceDescriptionChunk> = Vec::new();
                    for sub_track in subscribed_tracks {
                        let Some(dt) = sub_track.down_track() else {
                            // a nil down track is dereferenced upstream
                            panic!("nil pointer dereference: subscribed track without down track");
                        };
                        let sr = dt.create_sender_report();
                        let chunks = dt.create_source_description_chunks();
                        // `chunks == nil`: a down track that is not bound gives none
                        let Some(sr) = sr else {
                            continue;
                        };
                        if chunks.is_empty() {
                            continue;
                        }

                        pkts.push(Box::new(sr));
                        let mut num_items = 0;
                        for chunk in &chunks {
                            num_items += chunk.items.len();
                        }
                        sd.extend(chunks);
                        batch_size = batch_size + 1 + num_items;
                        if batch_size >= SD_BATCH_SIZE {
                            if !sd.is_empty() {
                                pkts.push(Box::new(SourceDescription {
                                    chunks: std::mem::take(&mut sd),
                                }));
                            }
                            if let Err(err) = p.transport_manager.write_subscriber_rtcp(&pkts) {
                                if is_eof(&err) {
                                    return;
                                }
                                errorw!(p.sub_logger, "could not send down track reports", err);
                            }

                            pkts.clear();
                            sd.clear();
                            batch_size = 0;
                        }
                    }

                    if !pkts.is_empty() || !sd.is_empty() {
                        if !sd.is_empty() {
                            pkts.push(Box::new(SourceDescription { chunks: sd }));
                        }
                        if let Err(err) = p.transport_manager.write_subscriber_rtcp(&pkts) {
                            if is_eof(&err) {
                                return;
                            }
                            errorw!(p.sub_logger, "could not send down track reports", err);
                        }
                    }
                }

                time::sleep(Duration::from_secs(3)).await;
            }
        };
        if recover_async(Some(&logger), worker).await.is_err() {
            std::process::exit(1);
        }
    }

    fn on_stream_state_change(
        &self,
        update: &streamallocator::StreamStateUpdate,
    ) -> Result<(), Error> {
        if update.stream_states.is_empty() {
            return Ok(());
        }

        let mut stream_state_update = livekit::StreamStateUpdate::default();
        for stream_state_info in &update.stream_states {
            let mut state = livekit::StreamState::Active;
            if stream_state_info.state == streamallocator::StreamState::Paused {
                state = livekit::StreamState::Paused;
            }
            stream_state_update
                .stream_states
                .push(livekit::StreamStateInfo {
                    participant_sid: stream_state_info.participant_id.as_str().to_string(),
                    track_sid: stream_state_info.track_id.as_str().to_string(),
                    state: state as i32,
                });
        }

        self.send_stream_state_update(stream_state_update)
    }

    fn on_subscribed_max_quality_change(
        &self,
        track_id: &livekit::TrackId,
        track_info: &livekit::TrackInfo,
        subscribed_qualities: &[livekit::SubscribedCodec],
        max_subscribed_qualities: &[types::SubscribedCodecQuality],
    ) -> Result<(), Error> {
        if self.params.disable_dynacast {
            return Ok(());
        }

        if subscribed_qualities.is_empty() {
            return Ok(());
        }

        // send layer info about max subscription changes to telemetry
        for max_subscribed_quality in max_subscribed_qualities {
            let mut ti = livekit::TrackInfo {
                sid: track_info.sid.clone(),
                r#type: track_info.r#type,
                ..Default::default()
            };
            for layer in buffer::get_video_layers_for_mime_type(
                max_subscribed_quality.codec_mime,
                Some(track_info),
            ) {
                if layer.quality == max_subscribed_quality.quality as i32 {
                    ti.width = layer.width;
                    ti.height = layer.height;
                    break;
                }
            }
            self.get_telemetry_listener()
                .on_track_max_subscribed_video_quality(
                    &self.id(),
                    &ti,
                    max_subscribed_quality.codec_mime,
                    max_subscribed_quality.quality,
                );
        }

        // normalize the codec name
        let mut subscribed_qualities = subscribed_qualities.to_vec();
        for subscribed_quality in &mut subscribed_qualities {
            subscribed_quality.codec = subscribed_quality
                .codec
                .strip_prefix(mime::MIME_TYPE_PREFIX_VIDEO)
                .unwrap_or(&subscribed_quality.codec)
                .to_lowercase();
        }

        let subscribed_quality_update = livekit::SubscribedQualityUpdate {
            track_sid: track_id.as_str().to_string(),
            #[allow(deprecated)]
            subscribed_qualities: subscribed_qualities[0].qualities.clone(), // for compatible with old client
            subscribed_codecs: subscribed_qualities,
        };

        debugw!(
            self.pub_logger,
            "sending max subscribed quality",
            "trackID" => track_id,
            "qualities" => subscribed_quality_update.subscribed_codecs,
            "max" => max_subscribed_qualities,
        );
        self.send_subscribed_quality_update(subscribed_quality_update)
    }

    fn on_subscribed_audio_codec_change(
        &self,
        track_id: &livekit::TrackId,
        codecs: &[livekit::SubscribedAudioCodec],
    ) -> Result<(), Error> {
        if self.params.disable_dynacast {
            return Ok(());
        }

        if codecs.is_empty() {
            return Ok(());
        }

        // normalize the codec name
        let mut codecs = codecs.to_vec();
        for codec in &mut codecs {
            codec.codec = codec
                .codec
                .strip_prefix(mime::MIME_TYPE_PREFIX_AUDIO)
                .unwrap_or(&codec.codec)
                .to_lowercase();
        }

        let subscribed_audio_codec_update = livekit::SubscribedAudioCodecUpdate {
            track_sid: track_id.as_str().to_string(),
            subscribed_audio_codecs: codecs,
        };
        debugw!(
            self.pub_logger,
            "sending subscribed audio codec update",
            "trackID" => track_id,
            "update" => subscribed_audio_codec_update,
        );
        self.send_subscribed_audio_codec_update(subscribed_audio_codec_update)
    }

    // releasePendingTrackLocked drops every pending entry of a track whose media never arrived,
    // so it stops counting against cMaxPendingTracks. Must be called with pendingTracksLock held.
    pub(super) fn release_pending_track_locked(
        &self,
        pt: &mut PendingTracks,
        track_id: &livekit::TrackId,
    ) {
        let mut released: Option<livekit::TrackInfo> = None;
        let mut released_cids: Vec<String> = Vec::new();
        pt.pending_tracks.retain(|cid, pti| {
            let mut remaining = Vec::with_capacity(pti.track_infos.len());
            for ti in pti.track_infos.drain(..) {
                if ti.sid == track_id.as_str() {
                    released_cids.push(cid.clone());
                    for c in &ti.codecs {
                        released_cids.push(c.cid.clone());
                        released_cids.push(c.sdp_cid.clone());
                    }
                    released = Some(ti);
                    continue;
                }
                remaining.push(ti);
            }
            pti.track_infos = remaining;
            !pti.track_infos.is_empty()
        });
        let Some(released) = released else {
            return;
        };
        pt.add_released_track_cids_locked(released_cids);

        infow!(
            self.pub_logger,
            "releasing pending track that never received media",
            "trackID" => track_id,
            "trackInfo" => released,
            "numPendingTracks" => pt.pending_tracks.len(),
        );
        if let Some(supervisor) = &self.supervisor {
            supervisor.remove_publication(track_id);
        }
        if self.protocol_version().supports_unpublish() {
            self.send_track_unpublished(track_id);
        } else {
            self.send_track_muted(track_id, true);
        }
    }

    fn add_pending_track(&self, req: &livekit::AddTrackRequest) -> Option<livekit::TrackInfo> {
        let mut pt = self.pending_tracks_lock.write();
        if !req.sid.is_empty() {
            let Some(track) = self
                .up_track_manager
                .get_published_track(&livekit::TrackId::from(req.sid.as_str()))
            else {
                infow!(self.pub_logger, "could not find existing track for multi-codec simulcast", "trackID" => req.sid);
                let _ = self.send_request_response(livekit::RequestResponse {
                    reason: livekit::request_response::Reason::NotFound as i32,
                    request: Some(livekit::request_response::Request::AddTrack(req.clone())),
                    ..Default::default()
                });
                return None;
            };

            as_media_track(track.clone()).update_codec_info(&req.simulcast_codecs);
            let ti = track.to_proto();
            return Some(ti);
        }

        if pt.pending_tracks.len() >= C_MAX_PENDING_TRACKS
            && let Some(supervisor) = &self.supervisor
        {
            // a client can abandon a publish without any signal the server acts on,
            // so before rejecting, release pending tracks the supervisor reports as timed out
            for track_id in supervisor.timed_out_publications() {
                self.release_pending_track_locked(&mut pt, &track_id);
            }
        }

        if pt.pending_tracks.len() >= C_MAX_PENDING_TRACKS {
            infow!(self.pub_logger, "too many pending tracks, rejecting new track", "numPendingTracks" => pt.pending_tracks.len());
            let _ = self.send_request_response(livekit::RequestResponse {
                reason: livekit::request_response::Reason::LimitExceeded as i32,
                message: format!(
                    "too many pending tracks, rejecting new track, numPendingTracks: {}",
                    pt.pending_tracks.len()
                ),
                request: Some(livekit::request_response::Request::AddTrack(req.clone())),
                ..Default::default()
            });
            return None;
        }

        pt.clear_released_track_cids_locked(req);

        let mut backup_codec_policy = req.backup_codec_policy;

        // enable simulcast codec for audio by default
        if (backup_codec_policy != livekit::BackupCodecPolicy::Regression as i32
            && req.r#type == livekit::TrackType::Audio as i32)
            || (backup_codec_policy != livekit::BackupCodecPolicy::Simulcast as i32
                && self.params.disable_codec_regression)
        {
            backup_codec_policy = livekit::BackupCodecPolicy::Simulcast as i32;
        }

        let clone_layers = |layers: &[livekit::VideoLayer]| -> Vec<livekit::VideoLayer> {
            if layers.is_empty() {
                return Vec::new();
            }

            let mut cloned_layers = layers.to_vec();
            cloned_layers.sort_by_key(|l| l.quality);
            cloned_layers
        };

        #[allow(deprecated)]
        let mut ti = livekit::TrackInfo {
            r#type: req.r#type,
            name: req.name.clone(),
            width: req.width,
            height: req.height,
            muted: req.muted,
            disable_dtx: req.disable_dtx,
            source: req.source,
            layers: clone_layers(&req.layers),
            disable_red: req.disable_red,
            stereo: req.stereo,
            encryption: req.encryption,
            stream: req.stream.clone(),
            backup_codec_policy,
            audio_features: sutils::dedupe_slice(req.audio_features.clone()),
            packet_trailer_features: sutils::dedupe_slice(req.packet_trailer_features.clone()),
            ..Default::default()
        };
        #[allow(deprecated)]
        let (stereo, disable_dtx) = (req.stereo, req.disable_dtx);
        if stereo
            && !ti
                .audio_features
                .contains(&(livekit::AudioTrackFeature::TfStereo as i32))
        {
            ti.audio_features
                .push(livekit::AudioTrackFeature::TfStereo as i32);
        }
        if disable_dtx
            && !ti
                .audio_features
                .contains(&(livekit::AudioTrackFeature::TfNoDtx as i32))
        {
            ti.audio_features
                .push(livekit::AudioTrackFeature::TfNoDtx as i32);
        }
        if ti.stream.is_empty() {
            ti.stream = stream_from_track_source(ti.source()).to_string();
        }
        pt.set_track_id(&req.cid, &mut ti);

        if req.simulcast_codecs.is_empty() {
            // clients not supporting simulcast codecs, synthesise a codec
            let mut video_layer_mode = livekit::video_layer::Mode::OneSpatialLayerPerStream;
            if client_info_is_obs(&self.params.client_info) {
                video_layer_mode =
                    livekit::video_layer::Mode::OneSpatialLayerPerStreamIncompleteRtcpSr;
            }
            ti.codecs.push(livekit::SimulcastCodecInfo {
                cid: req.cid.clone(),
                layers: clone_layers(&req.layers),
                video_layer_mode: video_layer_mode as i32,
                ..Default::default()
            });
        } else {
            let mut seen_codecs: HashMap<String, ()> = HashMap::new();
            for codec in &req.simulcast_codecs {
                if codec.codec.is_empty() {
                    warnw!(
                        self.pub_logger,
                        "simulcast codec without mime type", nil,
                        "trackID" => ti.sid,
                        "track" => ti,
                        "addTrackRequest" => req,
                    );
                }

                let mut mime_type = codec.codec.clone();
                let mut video_layer_mode = codec.video_layer_mode;
                if req.r#type == livekit::TrackType::Video as i32 {
                    if !mime::is_mime_type_string_video(&mime_type) {
                        mime_type = format!("{}{}", mime::MIME_TYPE_PREFIX_VIDEO, mime_type);
                    }
                    if !is_codec_enabled(
                        &self.enabled_publish_codecs,
                        &webrtc::RtpCodecCapability {
                            mime_type: mime_type.clone(),
                            ..Default::default()
                        },
                    ) {
                        let alt_codec =
                            select_alternative_video_codec(&self.enabled_publish_codecs);
                        infow!(
                            self.pub_logger,
                            "falling back to alternative video codec",
                            "codec" => mime_type,
                            "altCodec" => alt_codec,
                            "enabledPublishCodecs" => self.enabled_publish_codecs,
                            "trackID" => ti.sid,
                        );
                        // select an alternative MIME type that's generally supported
                        mime_type = alt_codec;
                    }
                    if video_layer_mode == livekit::video_layer::Mode::Unused as i32 {
                        if mime::is_mime_type_string_svc_capable(&mime_type) {
                            video_layer_mode =
                                livekit::video_layer::Mode::MultipleSpatialLayersPerStream as i32;
                        } else if client_info_is_obs(&self.params.client_info) {
                            video_layer_mode =
                                livekit::video_layer::Mode::OneSpatialLayerPerStreamIncompleteRtcpSr
                                    as i32;
                        } else {
                            video_layer_mode =
                                livekit::video_layer::Mode::OneSpatialLayerPerStream as i32;
                        }
                    }
                } else if req.r#type == livekit::TrackType::Audio as i32 {
                    if !mime::is_mime_type_string_audio(&mime_type) {
                        mime_type = format!("{}{}", mime::MIME_TYPE_PREFIX_AUDIO, mime_type);
                    }
                    if !is_codec_enabled(
                        &self.enabled_publish_codecs,
                        &webrtc::RtpCodecCapability {
                            mime_type: mime_type.clone(),
                            ..Default::default()
                        },
                    ) {
                        let alt_codec =
                            select_alternative_audio_codec(&self.enabled_publish_codecs);
                        infow!(
                            self.pub_logger,
                            "falling back to alternative audio codec",
                            "codec" => mime_type,
                            "altCodec" => alt_codec,
                            "enabledPublishCodecs" => self.enabled_publish_codecs,
                            "trackID" => ti.sid,
                        );
                        // select an alternative MIME type that's generally supported
                        mime_type = alt_codec;
                    }
                }

                if seen_codecs.contains_key(&mime_type) || mime_type.is_empty() {
                    continue;
                }
                seen_codecs.insert(mime_type.clone(), ());

                ti.codecs.push(livekit::SimulcastCodecInfo {
                    mime_type,
                    cid: codec.cid.clone(),
                    video_layer_mode,
                    ..Default::default()
                });
            }

            // set up layers with codec specific layers,
            // fall back to common layers if codec specific layer is not available
            let mut simulcast = false;
            for (idx, codec) in ti.codecs.iter_mut().enumerate() {
                let mut found = false;
                for simulcast_codec in &req.simulcast_codecs {
                    if mime::get_mime_type_codec(&codec.mime_type)
                        != mime::normalize_mime_type_codec(&simulcast_codec.codec)
                    {
                        continue;
                    }

                    if !simulcast_codec.layers.is_empty() {
                        codec.layers = clone_layers(&simulcast_codec.layers);
                    } else {
                        codec.layers = clone_layers(&req.layers);
                    }
                    found = true;
                    break;
                }

                if !found {
                    // could happen if an alternate codec is selected and that is not in the simulcast codecs list
                    codec.layers = clone_layers(&req.layers);
                }

                // populate simulcast flag for compatibility, true if primary codec is not SVC and has multiple layers
                if idx == 0
                    && codec.video_layer_mode
                        != livekit::video_layer::Mode::MultipleSpatialLayersPerStream as i32
                    && codec.layers.len() > 1
                {
                    simulcast = true;
                }
            }
            if simulcast {
                #[allow(deprecated)]
                {
                    ti.simulcast = true;
                }
            }
        }

        if self.get_published_track_by_signal_cid(&req.cid).is_some()
            || self.get_published_track_by_sdp_cid(&req.cid).is_some()
            || pt.pending_tracks.contains_key(&req.cid)
        {
            if !pt.pending_tracks.contains_key(&req.cid) {
                let mut pti = PendingTrackInfo {
                    track_infos: vec![ti.clone()],
                    created_at: time::now(),
                    queued: true,
                    ..Default::default()
                };
                if ti.r#type == livekit::TrackType::Video as i32 {
                    pti.sdp_rids = buffer::DEFAULT_VIDEO_LAYERS_RID.clone(); // could get updated from SDP
                }
                pt.pending_tracks.insert(req.cid.clone(), pti);
            } else {
                let num_queued = pt.pending_tracks[&req.cid].track_infos.len();
                if num_queued >= C_MAX_PENDING_QUEUED_TRACKS {
                    infow!(self.pub_logger, "too many pending queued tracks, rejecting new track", "trackID" => ti.sid, "numPendingTracksQueued" => num_queued);
                    let _ = self.send_request_response(livekit::RequestResponse {
                        reason: livekit::request_response::Reason::LimitExceeded as i32,
                        message: format!(
                            "too many pending queued tracks, rejecting new track, numPendingTracksQueued: {num_queued}"
                        ),
                        request: Some(livekit::request_response::Request::AddTrack(req.clone())),
                        ..Default::default()
                    });
                    return None;
                }

                if let Some(pti) = pt.pending_tracks.get_mut(&req.cid) {
                    pti.track_infos.push(ti.clone());
                }
            }
            infow!(
                self.pub_logger,
                "pending track queued",
                "trackID" => ti.sid,
                "request" => req,
                "pendingTrack" => pt.pending_tracks.get(&req.cid),
            );
            let _ = self.send_request_response(livekit::RequestResponse {
                reason: livekit::request_response::Reason::Queued as i32,
                request: Some(livekit::request_response::Request::AddTrack(req.clone())),
                ..Default::default()
            });

            if let Some(supervisor) = &self.supervisor {
                let track_id = livekit::TrackId::from(ti.sid.as_str());
                supervisor.add_publication(&track_id);
                supervisor.set_publication_mute(&track_id, ti.muted);
            }
            drop(pt);

            self.get_telemetry_listener().on_track_publish_requested(
                &self.id(),
                &self.identity(),
                &ti,
                true,
            );
            return None;
        }

        let mut pti = PendingTrackInfo {
            track_infos: vec![ti.clone()],
            created_at: time::now(),
            ..Default::default()
        };
        if ti.r#type == livekit::TrackType::Video as i32 {
            pti.sdp_rids = buffer::DEFAULT_VIDEO_LAYERS_RID.clone(); // could get updated from SDP
        }
        pt.pending_tracks.insert(req.cid.clone(), pti);
        debugw!(
            self.pub_logger,
            "pending track added",
            "trackID" => ti.sid,
            "request" => req,
            "pendingTrack" => pt.pending_tracks.get(&req.cid),
        );

        if let Some(supervisor) = &self.supervisor {
            let track_id = livekit::TrackId::from(ti.sid.as_str());
            supervisor.add_publication(&track_id);
            supervisor.set_publication_mute(&track_id, ti.muted);
        }
        drop(pt);

        self.get_telemetry_listener().on_track_publish_requested(
            &self.id(),
            &self.identity(),
            &ti,
            true,
        );
        Some(ti)
    }

    pub fn get_pending_track(&self, track_id: &livekit::TrackId) -> Option<livekit::TrackInfo> {
        let pt = self.pending_tracks_lock.read();

        for t in pt.pending_tracks.values() {
            if t.track_infos[0].sid == track_id.as_str() {
                return Some(t.track_infos[0].clone());
            }
        }

        None
    }

    pub fn has_ice_connected(&self) -> bool {
        self.transport_manager.has_subscriber_ice_ever_connected()
            || self.transport_manager.has_publisher_ice_ever_connected()
    }

    pub fn has_connected(&self) -> bool {
        self.transport_manager.has_subscriber_ever_connected()
            || self.transport_manager.has_publisher_ever_connected()
    }

    pub fn set_track_muted(
        &self,
        mute: &livekit::MuteTrackRequest,
        from_admin: bool,
    ) -> Option<livekit::TrackInfo> {
        // when request is coming from admin, send message to current participant
        if from_admin {
            self.send_track_muted(&livekit::TrackId::from(mute.sid.as_str()), mute.muted);
        }

        self.set_track_muted_internal(mute, from_admin)
    }

    // `setTrackMuted`: the original has two methods whose names differ in case only.
    fn set_track_muted_internal(
        &self,
        mute: &livekit::MuteTrackRequest,
        from_admin: bool,
    ) -> Option<livekit::TrackInfo> {
        let track_id = livekit::TrackId::from(mute.sid.as_str());
        self.dirty.store(true);
        if let Some(supervisor) = &self.supervisor {
            supervisor.set_publication_mute(&track_id, mute.muted);
        }

        let (track, mut changed) = self
            .up_track_manager
            .set_published_track_muted(&track_id, mute.muted);
        let mut track_info = track.map(|track| track.to_proto());

        // update mute status in any pending/queued add track requests too
        {
            // the write lock, as the pending tracks are changed (the original takes the read
            // lock)
            let mut pt = self.pending_tracks_lock.write();
            for pti in pt.pending_tracks.values_mut() {
                for ti in &mut pti.track_infos {
                    if ti.sid == track_id.as_str() {
                        changed = changed || ti.muted != mute.muted;
                        ti.muted = mute.muted;
                        if track_info.is_none() {
                            track_info = Some(ti.clone());
                        }
                    }
                }
            }
        }

        if let Some(track_info) = &track_info
            && changed
        {
            if mute.muted {
                self.get_telemetry_listener()
                    .on_track_muted(&self.id(), track_info);
            } else {
                self.get_telemetry_listener()
                    .on_track_unmuted(&self.id(), track_info);
            }
        }

        if track_info.is_none() && !from_admin {
            let _ = self.send_request_response(livekit::RequestResponse {
                reason: livekit::request_response::Reason::NotFound as i32,
                request: Some(livekit::request_response::Request::Mute(mute.clone())),
                ..Default::default()
            });
        }

        track_info
    }

    fn media_track_received(
        &self,
        track: Arc<dyn sfu::TrackRemote>,
        rtp_receiver: Arc<dyn webrtc::RtpReceiver>,
    ) -> (Option<Arc<MediaTrack>>, bool, bool, buffer::VideoLayersRid) {
        let mut pt = self.pending_tracks_lock.write();

        let mid = self.get_publisher_mid(&rtp_receiver);
        debugw!(
            self.pub_logger,
            "media track received",
            "kind" => track.kind().to_string(),
            "trackID" => track.id(),
            "rid" => track.rid(),
            "ssrc" => track.ssrc(),
            "rtxSsrc" => track.rtx_ssrc(),
            "mime" => mime::normalize_mime_type(&track.codec().mime_type),
            "mid" => mid,
        );
        if mid.is_empty() {
            pt.pending_remote_tracks.push(PendingRemoteTrack {
                track: track.rtc_track(),
                receiver: rtp_receiver,
            });
            drop(pt);
            warnw!(self.pub_logger, "could not get mid for track", nil, "trackID" => track.id());
            return (None, false, false, buffer::VideoLayersRid::default());
        }

        // use existing media track to handle simulcast
        let mut created_at = Time::ZERO;
        let mut is_new_track = false;
        let mut is_migrated = false;
        let mut rids_from_sdp = buffer::VideoLayersRid::default();
        let mut pub_time = Duration::ZERO;
        let mt = match self
            .get_published_track_by_sdp_cid(&track.id())
            .and_then(downcast_media_track)
        {
            Some(mt) => mt,
            None => {
                if pt.is_released_track_cid_locked(&track.id()) {
                    drop(pt);
                    infow!(
                        self.pub_logger,
                        "dropping media for a released pending track",
                        "trackID" => track.id(),
                        "mid" => mid,
                    );
                    return (None, false, false, buffer::VideoLayersRid::default());
                }

                let (signal_cid, ti, sdp_rids, migrated, pending_created_at) =
                    pt.get_pending_track(&track.id(), to_proto_track_kind(track.kind()), true);
                rids_from_sdp = sdp_rids;
                is_migrated = migrated;
                created_at = pending_created_at;
                let Some(mut ti) = ti else {
                    pt.pending_remote_tracks.push(PendingRemoteTrack {
                        track: track.rtc_track(),
                        receiver: rtp_receiver,
                    });
                    drop(pt);
                    return (None, false, false, rids_from_sdp);
                };

                // check if the migrated track has correct codec
                if is_migrated && !ti.codecs.is_empty() {
                    let parameters = rtp_receiver.get_parameters();
                    let mut codec_found = 0;
                    for c in &ti.codecs {
                        for nc in &parameters.codecs {
                            if mime::is_mime_type_string_equal(&nc.mime_type, &c.mime_type) {
                                codec_found += 1;
                                break;
                            }
                        }
                    }
                    if codec_found != ti.codecs.len() {
                        warnw!(
                            self.pub_logger,
                            "migrated track codec mismatched", nil,
                            "trackID" => ti.sid,
                            "track" => ti,
                            "webrtcCodec" => parameters,
                            "codecFound" => codec_found,
                            "codecCount" => ti.codecs.len(),
                        );
                        drop(pt);
                        self.issue_full_reconnect(
                            types::ParticipantCloseReason::MigrateCodecMismatch,
                        );
                        return (None, false, false, rids_from_sdp);
                    }
                }

                ti.mime_type = track.codec().mime_type.clone();
                // set mime_type for tracks that the AddTrackRequest do not have simulcast_codecs set
                if ti.codecs.len() == 1 && ti.codecs[0].mime_type.is_empty() {
                    ti.codecs[0].mime_type = track.codec().mime_type.clone();
                }
                if putils::timed_version_from_proto(ti.version.as_ref()).is_zero() {
                    // only assign version on a fresh publish, i. e. avoid updating version in scenarios like migration
                    ti.version = Some(self.params.version_generator.next().to_proto());
                }

                let mime_type = mime::normalize_mime_type(&ti.mime_type);
                // the layers are set from a copy of the track info: what the two functions
                // read of it, the qualities, does not change in the loop
                let ti_before = ti.clone();
                #[allow(deprecated)]
                for layer in &mut ti.layers {
                    layer.spatial_layer = buffer::video_quality_to_spatial_layer(
                        mime_type,
                        layer.quality(),
                        Some(&ti_before),
                    );
                    layer.rid = buffer::video_quality_to_rid(
                        mime_type,
                        layer.quality(),
                        Some(&ti_before),
                        &rids_from_sdp,
                    );
                }

                let mt = self.add_media_track(&mut pt, &signal_cid, &ti);
                is_new_track = true;
                mt
            }
        };

        // a track might have been set up in migrate-in path and won't show up as a new track here,
        // so we need to check if it's migrated and published separately
        if !is_migrated {
            is_migrated = mt.migrated();
            if is_migrated {
                let migrated_in_at = self.migrated_in_at.load();
                if !migrated_in_at.is_zero() {
                    created_at = migrated_in_at;
                }
            }
        }
        if !is_new_track {
            is_new_track = !mt.published();
        }
        mt.set_published(true);

        if is_new_track {
            // if the addTrackRequest is sent before publisher peer connection is established, then it means the client tries to publish
            // before fully connected, in this case we only record the time when publisher peer connection is established since
            // we want this metric to represent the time cost by publishing.
            let connected_at = self.transport_manager.publisher_first_connected_at();
            if !connected_at.is_zero() && created_at.before(connected_at) {
                created_at = connected_at;
            }
            pub_time = time::since(created_at);
            self.dirty.store(true);
        }
        drop(pt);

        let (_, is_receiver_added) = mt.add_receiver(rtp_receiver, track.clone(), &mid);

        if is_new_track {
            let p = self.arc();
            let mt = mt.clone();
            time::spawn(async move {
                // TODO: remove this after we know where the high delay is coming from
                if pub_time > Duration::from_secs(3) {
                    infow!(
                        p.pub_logger,
                        "track published with high delay",
                        "trackID" => mt.id(),
                        "track" => mt.to_proto(),
                        "cost" => pub_time.milliseconds(),
                        "rid" => track.rid(),
                        "mime" => track.codec().mime_type,
                        "isMigrated" => is_migrated,
                    );
                } else {
                    debugw!(
                        p.pub_logger,
                        "track published",
                        "trackID" => mt.id(),
                        "track" => mt.to_proto(),
                        "cost" => pub_time.milliseconds(),
                        "isMigrated" => is_migrated,
                    );
                }

                prometheus::record_publish_time(
                    &p.params.country,
                    mt.source(),
                    mt.kind(),
                    pub_time,
                    p.client_sdk(),
                    p.kind(),
                );

                p.handle_track_published(mt, is_migrated, false);
            });
        }

        (Some(mt), is_new_track, is_receiver_added, rids_from_sdp)
    }

    fn add_migrated_track(
        &self,
        pt: &mut PendingTracks,
        cid: &str,
        ti: &livekit::TrackInfo,
    ) -> Option<Arc<MediaTrack>> {
        infow!(self.pub_logger, "add migrated track", "cid" => cid, "trackID" => ti.sid, "track" => ti);
        let Some(rtp_receiver) = self.transport_manager.get_publisher_rtp_receiver(&ti.mid) else {
            errorw!(
                self.pub_logger,
                "could not find receiver for migrated track", nil,
                "trackID" => ti.sid,
                "mid" => ti.mid,
            );
            return None;
        };

        // check if the migrated track has correct codec
        if !ti.codecs.is_empty() {
            let parameters = rtp_receiver.get_parameters();
            let mut codec_found = 0;
            for c in &ti.codecs {
                for nc in &parameters.codecs {
                    if mime::is_mime_type_string_equal(&nc.mime_type, &c.mime_type) {
                        codec_found += 1;
                        break;
                    }
                }
            }
            if codec_found != ti.codecs.len() {
                warnw!(
                    self.pub_logger,
                    "migrated track codec mismatched", nil,
                    "trackID" => ti.sid,
                    "track" => ti,
                    "webrtcCodec" => parameters,
                    "codecFound" => codec_found,
                    "codecCount" => ti.codecs.len(),
                );
                return None;
            }
        }

        let mt = self.add_media_track(pt, cid, ti);
        mt.set_migrated(true);

        let mut potential_codecs: Vec<webrtc::RtpCodecParameters> =
            Vec::with_capacity(ti.codecs.len());
        let parameters = rtp_receiver.get_parameters();
        for c in &ti.codecs {
            for nc in &parameters.codecs {
                if mime::is_mime_type_string_equal(&nc.mime_type, &c.mime_type) {
                    potential_codecs.push(nc.clone());
                    break;
                }
            }
        }
        // check for mime_type for tracks that do not have simulcast_codecs set
        if !ti.mime_type.is_empty() {
            for nc in &parameters.codecs {
                if mime::is_mime_type_string_equal(&nc.mime_type, &ti.mime_type) {
                    let mut already_added = false;
                    for pc in &potential_codecs {
                        if mime::is_mime_type_string_equal(&pc.mime_type, &ti.mime_type) {
                            already_added = true;
                            break;
                        }
                    }
                    if !already_added {
                        potential_codecs.push(nc.clone());
                    }
                    break;
                }
            }
        }
        mt.set_potential_codecs(&potential_codecs, &parameters.header_extensions);

        for codec in &ti.codecs {
            for (ssrc, info) in &self.params.sim_tracks {
                if info.mid == codec.mid && !info.is_repair_stream {
                    mt.set_layer_ssrcs_for_rid(
                        mime::normalize_mime_type(&codec.mime_type),
                        &info.stream_id,
                        *ssrc,
                        info.repair_ssrc,
                    );
                }
            }
        }

        Some(mt)
    }

    fn add_media_track(
        &self,
        pt: &mut PendingTracks,
        signal_cid: &str,
        ti: &livekit::TrackInfo,
    ) -> Arc<MediaTrack> {
        let weak = self.this.clone();
        let track_id = livekit::TrackId::from(ti.sid.as_str());
        let mt = MediaTrack::new(
            MediaTrackParams {
                participant_id: Some(Arc::from(self.participant_id_func())),
                participant_identity: self.params.identity.clone(),
                participant_version: self.version.load(),
                participant_country: self.params.country.clone(),
                participant_kind: self.kind(),
                participant_kind_details: self.kind_details(),
                buffer_factory: self.params.config.buffer_factory.clone(),
                receiver_config: self.params.config.receiver,
                audio_config: self.params.audio_config,
                video_config: self.params.video_config.clone(),
                telemetry_listener: Some(Arc::from(self.telemetry_listener_func())),
                logger: logger_with_track(&self.pub_logger, &track_id, false),
                // `Reporter`: LiveKit's observability, not ported
                subscriber_config: self.params.config.subscriber.clone(),
                pli_throttle_config: self.params.pli_throttle_config,
                sim_tracks: self.params.sim_tracks.clone(),
                on_rtcp: Some({
                    let weak = weak.clone();
                    Arc::new(move |pkts| {
                        if let Some(p) = weak.upgrade() {
                            p.post_rtcp(pkts);
                        }
                    })
                }),
                forward_stats: self.params.forward_stats.clone(),
                on_track_ever_subscribed: Some({
                    let weak = weak.clone();
                    Arc::new(move |track_id| {
                        if let Some(p) = weak.upgrade() {
                            p.send_track_has_been_subscribed(&track_id);
                        }
                    })
                }),
                should_regress_codec: Some({
                    let weak = weak.clone();
                    Arc::new(move || match weak.upgrade() {
                        Some(p) => p.helper().should_regress_codec(),
                        None => false,
                    })
                }),
                prefer_video_size_from_media: self.params.prefer_video_size_from_media,
                enable_rtp_stream_restart_detection: self
                    .params
                    .enable_rtp_stream_restart_detection,
                update_track_info_by_video_size_change: self.params.use_one_shot_signalling_mode,
                force_backup_codec_policy_simulcast: self
                    .params
                    .force_backup_codec_policy_simulcast,
                media_batch_io_enabled: self.params.media_batch_io_enabled,
                on_subscribed_max_quality_change: Some({
                    let weak = weak.clone();
                    Arc::new(
                        move |track_id: &livekit::TrackId,
                              track_info: &livekit::TrackInfo,
                              subscribed_qualities: &[livekit::SubscribedCodec],
                              max_subscribed_qualities: &[types::SubscribedCodecQuality]| {
                            match weak.upgrade() {
                                Some(p) => p.on_subscribed_max_quality_change(
                                    track_id,
                                    track_info,
                                    subscribed_qualities,
                                    max_subscribed_qualities,
                                ),
                                None => Ok(()),
                            }
                        },
                    )
                }),
                on_subscribed_audio_codec_change: Some({
                    let weak = weak.clone();
                    Arc::new(
                        move |track_id: &livekit::TrackId,
                              codecs: &[livekit::SubscribedAudioCodec]| {
                            match weak.upgrade() {
                                Some(p) => p.on_subscribed_audio_codec_change(track_id, codecs),
                                None => Ok(()),
                            }
                        },
                    )
                }),
            },
            ti,
        );

        // add to published and clean up pending
        if let Some(supervisor) = &self.supervisor {
            supervisor.set_published_track(&track_id, mt.clone());
        }
        self.up_track_manager.add_published_track(mt.clone());

        if let Some(pti) = pt.pending_tracks.get(signal_cid) {
            if pt.pending_publishing_tracks.contains_key(&track_id) {
                infow!(self.pub_logger, "unexpected pending publish track", "trackID" => ti.sid);
            }
            let publishing = PendingTrackInfo {
                track_infos: vec![pti.track_infos[0].clone()],
                migrated: pti.migrated,
                ..Default::default()
            };
            pt.pending_publishing_tracks
                .insert(track_id.clone(), publishing);
        }

        {
            // a nil pointer upstream when there is no pending track for the signal cid
            let pti = pt
                .pending_tracks
                .get_mut(signal_cid)
                .expect("nil pointer dereference: no pending track for the signal cid");
            pti.track_infos.remove(0);
            if pti.track_infos.is_empty() {
                pt.pending_tracks.remove(signal_cid);
            } else {
                pti.queued = true;
                pti.created_at = time::now();
            }
        }

        let weak_mt = Arc::downgrade(&mt);
        let signal_cid = signal_cid.to_string();
        let ti = ti.clone();
        mt.add_on_close(Some(Box::new(move |is_expected_to_resume| {
            let (Some(p), Some(mt)) = (weak.upgrade(), weak_mt.upgrade()) else {
                return;
            };
            if let Some(supervisor) = &p.supervisor {
                supervisor.clear_published_track(&track_id, mt.clone());
            }

            p.get_telemetry_listener().on_track_unpublished(
                &p.id(),
                &p.identity(),
                &mt.to_proto(),
                mt.published(),
                !is_expected_to_resume,
            );

            {
                let mut pt = p.pending_tracks_lock.write();
                if let Some(pti) = pt.pending_tracks.get_mut(&signal_cid) {
                    let _ = p.send_track_published(&signal_cid, &pti.track_infos[0]);
                    pti.queued = false;
                }
            }
            p.handle_pending_remote_tracks();

            p.dirty.store(true);

            debugw!(
                p.pub_logger,
                "track unpublished",
                "trackID" => ti.sid,
                "expectedToResume" => is_expected_to_resume,
                "track" => ti,
            );
            p.listener().on_track_unpublished(p.clone(), mt);
        })));

        mt
    }

    pub(super) fn handle_track_published(
        &self,
        track: Arc<dyn types::MediaTrack>,
        is_migrated: bool,
        is_synthetic: bool,
    ) {
        self.listener()
            .on_track_published(self.arc(), track.clone());

        if !is_synthetic {
            // send webhook after callbacks are complete, persistence and state handling happens
            // in `onTrackPublished` cb
            self.get_telemetry_listener().on_track_published(
                &self.id(),
                &self.identity(),
                &track.to_proto(),
                !is_migrated,
            );
        }

        self.pending_tracks_lock
            .write()
            .pending_publishing_tracks
            .remove(&track.id());
    }

    fn has_pending_migrated_track(&self) -> bool {
        let pt = self.pending_tracks_lock.read();

        for t in pt.pending_tracks.values() {
            if t.migrated {
                return true;
            }
        }

        for t in pt.pending_publishing_tracks.values() {
            if t.migrated {
                return true;
            }
        }

        false
    }

    fn on_up_track_manager_close(&self) {
        // `Stop()` returns the channel the end of the worker closes, which is not waited for
        let _ = self.pub_rtcp_queue.stop();
    }

    pub(super) fn get_published_track_by_signal_cid(
        &self,
        client_id: &str,
    ) -> Option<Arc<dyn types::MediaTrack>> {
        for published_track in self.up_track_manager.get_published_tracks() {
            if as_local_media_track(published_track.clone()).has_signal_cid(client_id) {
                debugw!(self.pub_logger, "found track by signal cid", "signalCid" => client_id, "trackID" => published_track.id());
                return Some(published_track);
            }
        }

        None
    }

    pub(super) fn get_published_track_by_sdp_cid(
        &self,
        client_id: &str,
    ) -> Option<Arc<dyn types::MediaTrack>> {
        for published_track in self.up_track_manager.get_published_tracks() {
            if as_local_media_track(published_track.clone()).has_sdp_cid(client_id) {
                debugw!(self.pub_logger, "found track by SDP cid", "sdpCid" => client_id, "trackID" => published_track.id());
                return Some(published_track);
            }
        }

        None
    }

    pub fn debug_info(&self) -> sfu::DebugInfo {
        let mut info = sfu::DebugInfo::new();
        info.insert("ID".to_string(), self.id().as_str().into());
        info.insert("State".to_string(), self.state().as_str_name().into());

        let mut pending_track_info = serde_json::Map::new();
        {
            let pt = self.pending_tracks_lock.read();
            for (client_id, pti) in &pt.pending_tracks {
                let track_infos: Vec<String> =
                    pti.track_infos.iter().map(|ti| format!("{ti:?}")).collect();

                pending_track_info.insert(
                    client_id.clone(),
                    serde_json::json!({
                        "TrackInfos": track_infos,
                        "Migrated": pti.migrated,
                    }),
                );
            }
        }
        info.insert(
            "PendingTracks".to_string(),
            serde_json::Value::Object(pending_track_info),
        );

        info.insert(
            "UpTrackManager".to_string(),
            serde_json::Value::Object(self.up_track_manager.debug_info()),
        );

        info
    }

    fn post_rtcp(&self, pkts: Vec<Box<dyn rtcp::Packet>>) {
        let migration_timer_active = self.lock.read().migration_timer.is_some();

        // Once migration out is active, layers getting added would not be communicated to
        // where the publisher is migrating to. Without SSRC, `UnhandleSimulcastInterceptor`
        // cannot be set up on the migrating in node. Without that interceptor, simulcast
        // probing will fail.
        //
        // Clients usually send `rid` RTP header extension till they get an RTCP Receiver Report
        // from the remote side. So, by curbing RTCP when migration is active, even if a new layer
        // get published to this node, client should continue to send `rid` to the new node
        // post migration and the new node can do regular simulcast probing (without the
        // `UnhandleSimulcastInterceptor`) to fire `OnTrack` on that layer. And when the new node
        // sends RTCP Receiver Report back to the client, client will stop `rid`.
        if migration_timer_active {
            return;
        }

        self.pub_rtcp_queue.enqueue(
            |op: PostRtcpOp| {
                let Some(p) = op.p.upgrade() else {
                    return;
                };
                if let Err(err) = p.transport_manager.write_publisher_rtcp(&op.pkts)
                    && !is_eof(&err)
                {
                    errorw!(p.pub_logger, "could not write RTCP to participant", err);
                }
            },
            PostRtcpOp {
                p: self.this.clone(),
                pkts,
            },
        );
    }

    fn set_down_tracks_connected(&self) {
        for t in self.subscription_manager.get_subscribed_tracks() {
            if let Some(dt) = t.down_track() {
                dt.seed_state(sfu::DownTrackState {
                    forwarder_state: self.get_and_delete_forwarder_state(&t.id()),
                    ..Default::default()
                });
                dt.set_connected();
            }
        }
    }

    fn cache_forwarder_state(&self) {
        // if migrating in, get forwarder state from migrating out node to facilitate resume
        //
        // (`fs != nil`: an empty map is taken for nil, which the helper's map cannot be)
        if let Ok(fs) = self.helper().get_subscriber_forwarder_state(self.arc())
            && !fs.is_empty()
        {
            self.lock.write().forwarder_state = fs;

            for t in self.subscription_manager.get_subscribed_tracks() {
                if let Some(dt) = t.down_track() {
                    dt.seed_state(sfu::DownTrackState {
                        forwarder_state: self.get_and_delete_forwarder_state(&t.id()),
                        ..Default::default()
                    });
                }
            }
        }
    }

    fn get_and_delete_forwarder_state(
        &self,
        track_id: &livekit::TrackId,
    ) -> Option<livekit::RtpForwarderState> {
        self.lock.write().forwarder_state.remove(track_id)
    }

    pub fn cache_down_track(
        &self,
        track_id: &livekit::TrackId,
        rtp_transceiver: Arc<dyn webrtc::RtpTransceiver>,
        down_track: sfu::DownTrackState,
    ) {
        let mut inner = self.lock.write();
        if let Some(existing) = inner.cached_down_tracks.get(track_id)
            && !Arc::ptr_eq(&existing.transceiver, &rtp_transceiver)
        {
            warnw!(self.sub_logger, "cached transceiver changed", nil, "trackID" => track_id);
        }
        inner.cached_down_tracks.insert(
            track_id.clone(),
            DownTrackStateEntry {
                transceiver: rtp_transceiver,
                down_track,
            },
        );
        debugw!(self.sub_logger, "caching downtrack", "trackID" => track_id);
    }

    pub fn uncache_down_track(&self, rtp_transceiver: Arc<dyn webrtc::RtpTransceiver>) {
        let mut inner = self.lock.write();
        let mut found = None;
        for (track_id, dts) in &inner.cached_down_tracks {
            if Arc::ptr_eq(&dts.transceiver, &rtp_transceiver) {
                found = Some(track_id.clone());
                break;
            }
        }
        if let Some(track_id) = found {
            if inner.cached_down_tracks.contains_key(&track_id) {
                debugw!(self.sub_logger, "uncaching downtrack", "trackID" => track_id);
            }
            inner.cached_down_tracks.remove(&track_id);
        }
    }

    pub fn get_cached_down_track(
        &self,
        track_id: &livekit::TrackId,
    ) -> (Option<Arc<dyn webrtc::RtpTransceiver>>, sfu::DownTrackState) {
        let inner = self.lock.read();

        if let Some(dts) = inner.cached_down_tracks.get(track_id) {
            return (Some(dts.transceiver.clone()), dts.down_track.clone());
        }

        (None, sfu::DownTrackState::default())
    }

    pub fn issue_full_reconnect(&self, reason: types::ParticipantCloseReason) {
        let _ = self.send_leave_request(
            reason, false, // isExpectedToResume
            true,  // isExpectedToReconnect
            false, // sendOnlyIfSupportingLeaveRequestWithAction
        );

        let scr = match reason {
            types::ParticipantCloseReason::PublicationError
            | types::ParticipantCloseReason::MigrateCodecMismatch => {
                types::SignallingCloseReason::FullReconnectPublicationError
            }
            types::ParticipantCloseReason::SubscriptionError => {
                types::SignallingCloseReason::FullReconnectSubscriptionError
            }
            types::ParticipantCloseReason::DataChannelError => {
                types::SignallingCloseReason::FullReconnectDataChannelError
            }
            types::ParticipantCloseReason::NegotiateFailed => {
                types::SignallingCloseReason::FullReconnectNegotiateFailed
            }
            _ => types::SignallingCloseReason::Unknown,
        };
        self.close_signal_connection(scr);

        // a full reconnect == client should connect back with a new session, close current one
        let _ = self.close(false, reason, false);
    }

    fn on_publication_error(&self, track_id: &livekit::TrackId) {
        if self.params.reconnect_on_publication_error {
            infow!(self.pub_logger, "issuing full reconnect on publication error", "trackID" => track_id);
            self.issue_full_reconnect(types::ParticipantCloseReason::PublicationError);
        }
    }

    fn on_subscription_error(&self, track_id: &livekit::TrackId, fatal: bool, err: &Error) {
        let signal_err = match error_cause(err) {
            Error::Webrtc(webrtc::Error::UnsupportedCodec) => {
                livekit::SubscriptionError::SeCodecUnsupported
            }
            Error::TrackNotFound => livekit::SubscriptionError::SeTrackNotfound,
            _ => livekit::SubscriptionError::SeUnknown,
        };

        let _ = self.send_subscription_response(track_id, signal_err);

        if self.params.reconnect_on_subscription_error && fatal {
            infow!(self.sub_logger, "issuing full reconnect on subscription error", "trackID" => track_id);
            self.issue_full_reconnect(types::ParticipantCloseReason::SubscriptionError);
        }
    }

    fn on_any_transport_negotiation_failed(&self) {
        if self.transport_manager.since_last_signal() < NEGOTIATION_FAILED_TIMEOUT / 2 {
            infow!(
                self.params.logger,
                "negotiation failed, starting full reconnect"
            );
        }
        self.issue_full_reconnect(types::ParticipantCloseReason::NegotiateFailed);
    }

    pub fn update_subscribed_quality(
        &self,
        node_id: &livekit::NodeId,
        track_id: &livekit::TrackId,
        max_qualities: &[types::SubscribedCodecQuality],
    ) -> Result<(), Error> {
        let Some(track) = self.up_track_manager.get_published_track(track_id) else {
            debugw!(self.pub_logger, "could not find track", "trackID" => track_id);
            return Err(Error::CouldNotFindPublishedTrack);
        };

        as_local_media_track(track).notify_subscriber_node_max_quality(node_id, max_qualities);
        Ok(())
    }

    pub fn update_subscribed_audio_codecs(
        &self,
        node_id: &livekit::NodeId,
        track_id: &livekit::TrackId,
        codecs: &[livekit::SubscribedAudioCodec],
    ) -> Result<(), Error> {
        let Some(track) = self.up_track_manager.get_published_track(track_id) else {
            debugw!(self.pub_logger, "could not find track", "trackID" => track_id);
            return Err(Error::CouldNotFindPublishedTrack);
        };

        as_local_media_track(track).notify_subscription_node(node_id, codecs);
        Ok(())
    }

    pub fn update_media_loss(
        &self,
        node_id: &livekit::NodeId,
        track_id: &livekit::TrackId,
        fractional_loss: u32,
    ) -> Result<(), Error> {
        let Some(track) = self.up_track_manager.get_published_track(track_id) else {
            debugw!(self.pub_logger, "could not find track", "trackID" => track_id);
            return Err(Error::CouldNotFindPublishedTrack);
        };

        as_local_media_track(track)
            .notify_subscriber_node_media_loss(node_id, fractional_loss as u8);
        Ok(())
    }

    pub fn get_playout_delay_config(&self) -> Option<livekit::PlayoutDelay> {
        self.params.playout_delay
    }

    pub fn supports_sync_stream_id(&self) -> bool {
        self.protocol_version().supports_sync_stream_id()
            && !client_info_is_firefox(&self.params.client_info)
            && self.params.sync_streams
    }

    pub fn supports_transceiver_reuse(&self, mt: Arc<dyn types::MediaTrack>) -> bool {
        if !self.params.client_info.supports_transceiver_reuse() {
            return false;
        }

        if self.params.use_one_shot_signalling_mode {
            return self.protocol_version().supports_transceiver_reuse();
        }

        self.protocol_version().supports_transceiver_reuse()
            && !self.supports_sync_stream_id()
            && (!mt.is_encrypted() || !self.params.disable_transceiver_reuse_for_e2ee)
    }

    pub fn send_data_message(
        &self,
        kind: livekit::data_packet::Kind,
        data: &[u8],
        sender: &livekit::ParticipantId,
        seq: u32,
    ) -> Result<(), Error> {
        if kind != livekit::data_packet::Kind::Reliable {
            if self.state() != livekit::participant_info::State::Active {
                return Err(Error::DataChannelUnavailable);
            }
            return self.transport_manager.send_data_message(kind, data);
        }

        if sender.is_empty() || seq == 0 {
            // Unsequenced reliable data, i. e. not published by a participant, room service
            // SendData for example. Such a message cannot be recovered by
            // replayJoiningReliableMessages as the data message cache is keyed on
            // sender/sequence number, so hold on to the message itself here till the
            // reliable data channel is writable.
            {
                let mut joining = self.reliable_data_info.joining_message_lock.lock();
                if !joining.can_write_reliable {
                    if joining.unsequenced_bytes + data.len()
                        > C_MAX_JOINING_UNSEQUENCED_RELIABLE_BYTES
                    {
                        joining.unsequenced_dropped += 1;
                        return Err(Error::DataChannelUnavailable);
                    }

                    joining.unsequenced_messages.push(data.to_vec());
                    joining.unsequenced_bytes += data.len();
                    return Ok(());
                }
            }

            if self.state() != livekit::participant_info::State::Active {
                return Err(Error::DataChannelUnavailable);
            }
            return self.transport_manager.send_data_message(kind, data);
        }

        {
            let mut joining = self.reliable_data_info.joining_message_lock.lock();
            if !joining.can_write_reliable {
                joining.first_seqs.entry(sender.clone()).or_insert(seq);
                return Ok(());
            }

            if let Some(&last_written_seq) = joining.last_written_seqs.get(sender) {
                if seq <= last_written_seq {
                    // already sent by replayJoiningReliableMessages
                    return Ok(());
                } else {
                    joining.last_written_seqs.remove(sender);
                }
            }
        }

        self.transport_manager.send_data_message(kind, data)
    }

    pub fn send_data_message_unlabeled(
        &self,
        data: &[u8],
        use_raw: bool,
        sender: &livekit::ParticipantIdentity,
    ) -> Result<(), Error> {
        if self.state() != livekit::participant_info::State::Active {
            return Err(Error::DataChannelUnavailable);
        }

        self.transport_manager
            .send_data_message_unlabeled(data, use_raw, sender)
    }

    fn on_data_send_error(&self, err: Error) {
        if self.params.reconnect_on_data_channel_error {
            infow!(self.params.logger, "issuing full reconnect on data channel error", "error" => format_args!("{err}"));
            self.issue_full_reconnect(types::ParticipantCloseReason::DataChannelError);
        }
    }

    // `setupEnabledCodecs` sets the participant's two lists; here it gives them, as they are
    // needed before the participant is made.
    pub(super) fn setup_enabled_codecs(
        logger: &Logger,
        publish_enabled_codecs: &[livekit::Codec],
        subscribe_enabled_codecs: &[livekit::Codec],
        disabled_codecs: Option<&livekit::DisabledCodecs>,
    ) -> (Vec<livekit::Codec>, Vec<livekit::Codec>) {
        let should_disable = |c: &livekit::Codec, disabled: &[livekit::Codec]| -> bool {
            for disable_codec in disabled {
                // disable codec's fmtp is empty means disable this codec entirely
                if mime::is_mime_type_string_equal(&c.mime, &disable_codec.mime) {
                    return true;
                }
            }
            false
        };
        // `disabledCodecs.GetCodecs()`, `disabledCodecs.GetPublish()`
        let disabled_all = disabled_codecs.map(|d| d.codecs.as_slice()).unwrap_or(&[]);
        let disabled_publish = disabled_codecs.map(|d| d.publish.as_slice()).unwrap_or(&[]);

        let mut enabled_publish_codecs: Vec<livekit::Codec> = Vec::new();
        let mut publish_codecs_audio = Vec::with_capacity(publish_enabled_codecs.len());
        let mut publish_codecs_video = Vec::with_capacity(publish_enabled_codecs.len());
        for c in publish_enabled_codecs {
            if should_disable(c, disabled_all) || should_disable(c, disabled_publish) {
                continue;
            }

            // sort by compatibility, since we will look for backups in these.
            if mime::is_mime_type_string_vp8(&c.mime) {
                if !enabled_publish_codecs.is_empty() {
                    enabled_publish_codecs.insert(0, c.clone());
                } else {
                    enabled_publish_codecs.push(c.clone());
                }
            } else if mime::is_mime_type_string_h264(&c.mime) {
                enabled_publish_codecs.push(c.clone());
            } else if mime::is_mime_type_string_audio(&c.mime) {
                publish_codecs_audio.push(c.clone());
            } else {
                publish_codecs_video.push(c.clone());
            }
        }
        // list all video first and then audio to work around a client side issue with Flutter SDK 2.4.2
        enabled_publish_codecs.extend(publish_codecs_video);
        enabled_publish_codecs.extend(publish_codecs_audio);

        let mut subscribe_codecs = Vec::with_capacity(subscribe_enabled_codecs.len());
        for c in subscribe_enabled_codecs {
            if should_disable(c, disabled_all) {
                continue;
            }
            subscribe_codecs.push(c.clone());
        }
        let enabled_subscribe_codecs = subscribe_codecs;
        debugw!(
            logger,
            "setup enabled codecs",
            "publish" => enabled_publish_codecs,
            "subscribe" => enabled_subscribe_codecs,
            "disabled" => disabled_codecs,
        );
        (enabled_publish_codecs, enabled_subscribe_codecs)
    }

    pub(super) fn replay_joining_reliable_messages(&self) {
        let mut joining = self.reliable_data_info.joining_message_lock.lock();
        for msg_cache in self
            .helper()
            .get_cached_reliable_data_message(&joining.first_seqs)
        {
            if !msg_cache.dest_identities.is_empty()
                && !msg_cache.dest_identities.contains(&self.identity())
            {
                continue;
            }
            match joining.last_written_seqs.get(&msg_cache.sender_id) {
                Some(&last_seq) if last_seq >= msg_cache.seq => {}
                _ => {
                    joining
                        .last_written_seqs
                        .insert(msg_cache.sender_id.clone(), msg_cache.seq);
                }
            }

            let _ = self
                .transport_manager
                .send_data_message(livekit::data_packet::Kind::Reliable, &msg_cache.data);
        }

        for msg in &joining.unsequenced_messages {
            let _ = self
                .transport_manager
                .send_data_message(livekit::data_packet::Kind::Reliable, msg);
        }
        if joining.unsequenced_dropped != 0 {
            warnw!(
                self.params.logger,
                "dropped unsequenced reliable data messages while joining", nil,
                "numDropped" => joining.unsequenced_dropped,
                "numReplayed" => joining.unsequenced_messages.len(),
            );
        }
        joining.unsequenced_messages = Vec::new();
        joining.unsequenced_bytes = 0;
        joining.unsequenced_dropped = 0;

        joining.first_seqs = HashMap::new();
        joining.can_write_reliable = true;
    }

    pub fn get_enabled_publish_codecs(&self) -> Vec<livekit::Codec> {
        let mut codecs = Vec::with_capacity(self.enabled_publish_codecs.len());
        for c in &self.enabled_publish_codecs {
            if mime::is_mime_type_string_rtx(&c.mime) {
                continue;
            }
            codecs.push(c.clone());
        }
        codecs
    }

    pub fn update_audio_track(&self, update: &livekit::UpdateLocalAudioTrack) -> Result<(), Error> {
        if self
            .up_track_manager
            .update_published_audio_track(update)
            .is_some()
        {
            return Ok(());
        }

        let mut is_pending = false;
        {
            // the write lock, as the pending tracks are changed (the original takes the read
            // lock)
            let mut pt = self.pending_tracks_lock.write();
            for pti in pt.pending_tracks.values_mut() {
                for ti in &mut pti.track_infos {
                    if ti.sid == update.track_sid {
                        is_pending = true;

                        ti.audio_features = sutils::dedupe_slice(update.features.clone());
                        #[allow(deprecated)]
                        {
                            ti.stereo = false;
                            ti.disable_dtx = false;
                            for feature in &update.features {
                                if *feature == livekit::AudioTrackFeature::TfStereo as i32 {
                                    ti.stereo = true;
                                } else if *feature == livekit::AudioTrackFeature::TfNoDtx as i32 {
                                    ti.disable_dtx = true;
                                }
                            }
                        }

                        debugw!(self.pub_logger, "updated pending track", "trackID" => ti.sid, "trackInfo" => ti);
                    }
                }
            }
        }
        if is_pending {
            return Ok(());
        }

        debugw!(self.pub_logger, "could not locate track", "trackID" => update.track_sid);
        let _ = self.send_request_response(livekit::RequestResponse {
            reason: livekit::request_response::Reason::NotFound as i32,
            request: Some(livekit::request_response::Request::UpdateAudioTrack(
                update.clone(),
            )),
            ..Default::default()
        });
        Err(Error::CouldNotFindTrack)
    }

    pub fn update_video_track(&self, update: &livekit::UpdateLocalVideoTrack) -> Result<(), Error> {
        if self
            .up_track_manager
            .update_published_video_track(update)
            .is_some()
        {
            return Ok(());
        }

        let mut is_pending = false;
        {
            // the write lock, as the pending tracks are changed (the original takes the read
            // lock)
            let mut pt = self.pending_tracks_lock.write();
            for pti in pt.pending_tracks.values_mut() {
                for ti in &mut pti.track_infos {
                    if ti.sid == update.track_sid {
                        is_pending = true;

                        ti.width = update.width;
                        ti.height = update.height;
                    }
                }
            }
        }
        if is_pending {
            return Ok(());
        }

        debugw!(self.pub_logger, "could not locate track", "trackID" => update.track_sid);
        let _ = self.send_request_response(livekit::RequestResponse {
            reason: livekit::request_response::Reason::NotFound as i32,
            request: Some(livekit::request_response::Request::UpdateVideoTrack(
                update.clone(),
            )),
            ..Default::default()
        });
        Err(Error::CouldNotFindTrack)
    }

    pub fn handle_metrics(
        &self,
        sender_participant_id: &livekit::ParticipantId,
        _metrics: &livekit::MetricsBatch,
    ) -> Result<(), Error> {
        if self.state() != livekit::participant_info::State::Active {
            return Err(Error::DataChannelUnavailable);
        }

        if !self.can_subscribe_metrics() {
            return Err(Error::NoSubscribeMetricsPermission);
        }

        if *sender_participant_id != self.id()
            && !self
                .subscription_manager
                .is_subscribed_to(sender_participant_id)
        {
            return Ok(());
        }

        // `p.metricsReporter.Merge(metrics)`: `pkg/metric` is not ported
        Ok(())
    }

    pub fn supports_codec_change(&self) -> bool {
        self.params.client_info.supports_codec_change()
    }

    pub fn supports_moving(&self) -> Result<(), Error> {
        if !self.protocol_version().supports_moving() {
            return Err(Error::MoveOldClientVersion);
        }

        let kind = self.kind();
        if kind == livekit::participant_info::Kind::Egress
            || kind == livekit::participant_info::Kind::Agent
            || self.params.use_one_shot_signalling_mode
        {
            return Err(Error::CannotBeMoved {
                kind: kind.as_str_name().to_string(),
                one_shot: self.params.use_one_shot_signalling_mode,
            });
        }

        Ok(())
    }

    // LeaveSession takes the participant out of its session in its current room without
    // closing the participant, for it to join another session (see JoinSession): its
    // published tracks are reported unpublished, the session end is reported, and the
    // telemetry guard and deferred resolvers are reset for the next session. `leave` runs in
    // between, while the left session's guard is still in place, for the caller to report the
    // leave.
    pub fn leave_session(&self, leave: Option<Box<dyn Fn() + Send + Sync>>) {
        for track in self.up_track_manager.get_published_tracks() {
            self.get_telemetry_listener().on_track_unpublished(
                &self.id(),
                &self.identity(),
                &track.to_proto(),
                as_local_media_track(track.clone()).published(),
                true,
            );
        }

        // `p.params.Reporter.ReportEndTime(time.Now())`: LiveKit's observability, not ported

        if let Some(leave) = leave {
            leave();
        }

        self.lock.write().telemetry_guard = Arc::new(telemetry::ReferenceGuard::default());

        // `p.params.LoggerResolver.Reset()`, `p.params.ReporterResolver.Reset()`: neither is
        // here (see the header)
    }

    // JoinSession takes the participant, after LeaveSession, into the session its current
    // room now serves: `join` runs first, for the caller to report the join, then the
    // published tracks are reported published again, in that order as on a fresh join.
    pub fn join_session(&self, join: Option<Box<dyn Fn() + Send + Sync>>) {
        if let Some(join) = join {
            join();
        }

        for track in self.up_track_manager.get_published_tracks() {
            self.get_telemetry_listener().on_track_published(
                &self.id(),
                &self.identity(),
                &track.to_proto(),
                true,
            );
        }
    }

    pub fn move_to_room(&self, params: types::MoveToRoomParams) {
        for track in self.up_track_manager.get_published_tracks() {
            for sub in track.get_all_subscribers() {
                track.remove_subscriber(&sub, false);
            }

            // clear the subscriber node max quality/audio codecs as the remote quality notify
            // from source room would not reach the moving out participant.
            as_local_media_track(track).clear_subscriber_nodes();
        }

        let weak = self.this.clone();
        self.leave_session(Some(Box::new(move || {
            let Some(p) = weak.upgrade() else {
                return;
            };
            p.subscription_manager.clear_all_subscriptions();

            // fire onClose callback for original room
            let on_close = std::mem::take(&mut p.lock.write().on_close);
            let lp: Arc<dyn LocalParticipant> = p.clone();
            for cb in on_close.values() {
                cb(lp.clone());
            }
        })));

        infow!(self.params.logger, "move participant to new room", "newRoomName" => params.room_name, "newID" => params.participant_id);

        self.set_listener(params.listener);
        self.set_telemetry_listener(params.telemetry_listener);
        *self.participant_helper.write() = params.helper;

        *self.id.write() = params.participant_id;

        let mut grants = (**self.grants.read()).clone();
        if let Some(video) = grants.video.as_mut() {
            video.room = params.room_name.as_str().to_string();
        }
        *self.grants.write() = Arc::new(grants);
    }

    pub(super) fn helper(&self) -> Arc<dyn types::LocalParticipantHelper> {
        // `p.participantHelper.Load().(types.LocalParticipantHelper)` panics on nil
        self.participant_helper
            .read()
            .clone()
            .expect("participant without helper")
    }

    pub fn get_last_reliable_sequence(&self, migrate_out: bool) -> u32 {
        if migrate_out {
            self.reliable_data_info
                .stop_reliable_by_migrate_out
                .store(true);
        }
        self.reliable_data_info.last_pub_reliable_seq.load()
    }

    pub fn handle_update_subscriptions(
        &self,
        track_ids: &[livekit::TrackId],
        participant_tracks: &[livekit::ParticipantTracks],
        subscribe: bool,
    ) {
        self.listener().on_update_subscriptions(
            self.arc(),
            track_ids,
            participant_tracks,
            subscribe,
        );
    }

    pub fn handle_update_subscription_permission(
        &self,
        subscription_permission: &livekit::SubscriptionPermission,
    ) -> Result<(), Error> {
        self.listener()
            .on_update_subscription_permission(self.arc(), subscription_permission)
    }

    pub fn handle_sync_state(&self, sync_state: &livekit::SyncState) -> Result<(), Error> {
        self.listener().on_sync_state(self.arc(), sync_state)
    }

    pub fn handle_simulate_scenario(
        &self,
        simulate_scenario: &livekit::SimulateScenario,
    ) -> Result<(), Error> {
        self.listener()
            .on_simulate_scenario(self.arc(), simulate_scenario)
    }

    pub fn handle_leave_request(&self, reason: types::ParticipantCloseReason) {
        self.listener().on_leave(self.arc(), reason);
    }

    pub fn handle_signal_message(&self, msg: Message) -> Result<(), Error> {
        Ok(self.signal_handler.handle_message(msg)?)
    }

    pub fn is_using_single_peer_connection(&self) -> bool {
        self.params.use_single_peer_connection
    }

    #[allow(clippy::type_complexity)]
    pub fn add_track_local(
        &self,
        track_local: Arc<dyn webrtc::TrackLocal>,
        params: types::AddTrackParams,
    ) -> Result<(Arc<dyn webrtc::RtpSender>, Arc<dyn webrtc::RtpTransceiver>), Error> {
        let (sender, transceiver) = if self.params.use_single_peer_connection {
            self.transport_manager.add_track_local(
                track_local,
                params,
                &self.enabled_subscribe_codecs,
                &self.params.config.subscriber.rtcp_feedback,
            )?
        } else {
            self.transport_manager.add_track_local(
                track_local,
                params,
                &[],
                &RtcpFeedbackConfig::default(),
            )?
        };
        Ok((sender, transceiver))
    }

    #[allow(clippy::type_complexity)]
    pub fn add_transceiver_from_track_local(
        &self,
        track_local: Arc<dyn webrtc::TrackLocal>,
        params: types::AddTrackParams,
    ) -> Result<(Arc<dyn webrtc::RtpSender>, Arc<dyn webrtc::RtpTransceiver>), Error> {
        let (sender, transceiver) = if self.params.use_single_peer_connection {
            self.transport_manager.add_transceiver_from_track_local(
                track_local,
                params,
                &self.enabled_subscribe_codecs,
                &self.params.config.subscriber.rtcp_feedback,
            )?
        } else {
            self.transport_manager.add_transceiver_from_track_local(
                track_local,
                params,
                &[],
                &RtcpFeedbackConfig::default(),
            )?
        };
        Ok((sender, transceiver))
    }

    fn handle_incoming_rpc_ack(&self, request_id: &str) -> bool {
        let mut rpc = self.rpc_lock.lock();

        let Some(handler) = rpc.rpc_pending_acks.remove(request_id) else {
            return false;
        };

        (handler.resolve)();
        true
    }

    fn handle_incoming_rpc_response(
        &self,
        request_id: &str,
        payload: &str,
        err: Option<DataChannelRpcError>,
    ) -> bool {
        let mut rpc = self.rpc_lock.lock();

        let Some(handler) = rpc.rpc_pending_responses.remove(request_id) else {
            return false;
        };

        // what the original's `Resolve` does with `rpcPendingAcks` (it is called with
        // `rpcLock` held): the pending ack, if any, is resolved too
        if let Some(ack) = rpc.rpc_pending_acks.get(request_id) {
            (ack.resolve)();
        }
        (handler.resolve)(payload, err);
        true
    }

    pub fn perform_rpc(
        &self,
        req: &livekit::PerformRpcRequest,
        result_ch: mpsc::Sender<String>,
        error_ch: mpsc::Sender<Error>,
    ) {
        let mut response_timeout = req.response_timeout_ms;
        if response_timeout == 0 {
            response_timeout =
                putils::DATA_CHANNEL_RPC_DEFAULT_RESPONSE_TIMEOUT.milliseconds() as u32;
        }

        let p = self.arc();
        let req = req.clone();
        time::spawn(async move {
            if req.payload.len() > putils::DATA_CHANNEL_RPC_MAX_PAYLOAD_BYTES {
                let _ = error_ch.try_send(
                    putils::data_channel_rpc_error_from_built_in_codes(
                        putils::DATA_CHANNEL_RPC_REQUEST_PAYLOAD_TOO_LARGE,
                        "",
                    )
                    .into(),
                );
                return;
            }

            let id = new_uuid_string();

            let response_timer = {
                let weak = p.this.clone();
                let id = id.clone();
                let error_ch = error_ch.clone();
                time::after_func(Duration::from_millis(response_timeout as i64), move || {
                    if let Some(p) = weak.upgrade() {
                        p.rpc_lock.lock().rpc_pending_responses.remove(&id);
                    }

                    let _ = error_ch.try_send(
                        putils::data_channel_rpc_error_from_built_in_codes(
                            putils::DATA_CHANNEL_RPC_RESPONSE_TIMEOUT,
                            "",
                        )
                        .into(),
                    );
                })
            };
            let ack_timer = {
                let weak = p.this.clone();
                let id = id.clone();
                let error_ch = error_ch.clone();
                let response_timer = response_timer.clone();
                time::after_func(putils::DATA_CHANNEL_RPC_MAX_ROUND_TRIP_LATENCY, move || {
                    if let Some(p) = weak.upgrade() {
                        let mut rpc = p.rpc_lock.lock();
                        rpc.rpc_pending_acks.remove(&id);
                        rpc.rpc_pending_responses.remove(&id);
                    }
                    response_timer.stop();

                    let _ = error_ch.try_send(
                        putils::data_channel_rpc_error_from_built_in_codes(
                            putils::DATA_CHANNEL_RPC_CONNECTION_TIMEOUT,
                            "",
                        )
                        .into(),
                    );
                })
            };

            let last_rtt = p.lock.read().last_rtt;
            #[allow(deprecated)]
            let rpc_request = livekit::DataPacket {
                kind: livekit::data_packet::Kind::Reliable as i32,
                participant_identity: id.clone(),
                value: Some(livekit::data_packet::Value::RpcRequest(
                    livekit::RpcRequest {
                        id: id.clone(),
                        method: req.method.clone(),
                        payload: req.payload.clone(),
                        response_timeout_ms: response_timeout.wrapping_sub(last_rtt),
                        version: 1,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            };
            // `proto.Marshal` cannot fail here
            let data = rpc_request.encode_to_vec();

            // using RPC ID as the unique ID for server to identify the response
            if let Err(err) = p.send_data_message(
                livekit::data_packet::Kind::Reliable,
                &data,
                &livekit::ParticipantId::from(id.as_str()),
                0,
            ) {
                ack_timer.stop();
                response_timer.stop();
                // `psrpc.NewError(psrpc.Internal, err)`: the error itself, psrpc is not ported
                let _ = error_ch.try_send(err);
                return;
            }

            let mut rpc = p.rpc_lock.lock();
            {
                let ack_timer = ack_timer.clone();
                rpc.rpc_pending_acks.insert(
                    id.clone(),
                    DataChannelRpcPendingAckHandler {
                        resolve: Box::new(move || {
                            ack_timer.stop();
                        }),
                        participant_identity: req.destination_identity.clone(),
                    },
                );
            }
            rpc.rpc_pending_responses.insert(
                id.clone(),
                DataChannelRpcPendingResponseHandler {
                    resolve: Box::new(move |payload: &str, error: Option<DataChannelRpcError>| {
                        response_timer.stop();
                        // the pending ack is resolved by the caller, which holds `rpcLock`
                        // (see `handle_incoming_rpc_response`)

                        match error {
                            Some(error) => {
                                let _ = error_ch.try_send(error.into());
                            }
                            None => {
                                let _ = result_ch.try_send(payload.to_string());
                            }
                        }
                    }),
                    participant_identity: req.destination_identity.clone(),
                },
            );
        });
    }

    pub fn is_warp_enabled(&self) -> bool {
        self.params.enable_warp
    }

    // ---- New for musivum: what the port needs that the original has from Go itself

    // `p.ID`, as a function value of the params of a track: the participant's ID while it is
    // there, the one it had when the function was made after.
    pub(super) fn participant_id_func(
        &self,
    ) -> Box<dyn Fn() -> livekit::ParticipantId + Send + Sync> {
        let weak = self.this.clone();
        let id = self.id();
        Box::new(move || match weak.upgrade() {
            Some(p) => p.id(),
            None => id.clone(),
        })
    }

    // `p.GetTelemetryListener`, as a function value.
    pub(super) fn telemetry_listener_func(
        &self,
    ) -> Box<dyn Fn() -> Arc<dyn types::ParticipantTelemetryListener> + Send + Sync> {
        let weak = self.this.clone();
        Box::new(move || match weak.upgrade() {
            Some(p) => p.get_telemetry_listener(),
            None => Arc::new(types::NullParticipantTelemetryListener::default()),
        })
    }

    // `p.GetClientInfo().GetSdk()`
    fn client_sdk(&self) -> livekit::client_info::Sdk {
        self.get_client_info().map(|c| c.sdk()).unwrap_or_default()
    }

    // `p.TransportManager.GetPublisherMid(rtpReceiver)`: the manager takes the connection
    // layer's receiver, which is behind the seam; another has no mid.
    fn get_publisher_mid(&self, rtp_receiver: &Arc<dyn webrtc::RtpReceiver>) -> String {
        let any: Arc<dyn Any + Send + Sync> = rtp_receiver.clone();
        match any.downcast::<webrtc::peerconnection::RtpReceiver>() {
            Ok(r) => self.transport_manager.get_publisher_mid(&r),
            Err(_) => String::new(),
        }
    }
}

// --------------------------------------------------------------------------

// The functions upstream calls with `pendingTracksLock` held that use nothing else of the
// participant.
impl PendingTracks {
    // must be called with pendingTracksLock held
    pub(super) fn add_released_track_cids_locked(&mut self, mut cids: Vec<String>) {
        cids.retain(|cid| !cid.is_empty());
        cids.sort();
        cids.dedup();
        if cids.is_empty() {
            return;
        }
        self.released_track_cids.push(cids);
        if self.released_track_cids.len() > C_MAX_RELEASED_TRACKS {
            self.released_track_cids.remove(0);
        }
    }

    // must be called with pendingTracksLock held
    pub(super) fn is_released_track_cid_locked(&self, cid: &str) -> bool {
        for cids in &self.released_track_cids {
            if cids.iter().any(|c| c == cid) {
                return true;
            }
        }
        false
    }

    // clearReleasedTrackCidsLocked drops every released track that shares a client id with
    // this request. A client re-adding a released track intends to publish on it again, so all
    // of its ids are cleared, including an SDP id a reused transceiver keeps.
    // Must be called with pendingTracksLock held.
    pub(super) fn clear_released_track_cids_locked(&mut self, req: &livekit::AddTrackRequest) {
        self.released_track_cids.retain(|cids| {
            if cids.contains(&req.cid) {
                return false;
            }
            for sc in &req.simulcast_codecs {
                if !sc.cid.is_empty() && cids.contains(&sc.cid) {
                    return false;
                }
            }
            true
        });
    }

    pub(super) fn get_pending_track(
        &self,
        client_id: &str,
        kind: livekit::TrackType,
        skip_queued: bool,
    ) -> (
        String,
        Option<livekit::TrackInfo>,
        buffer::VideoLayersRid,
        bool,
        Time,
    ) {
        let mut signal_cid = client_id.to_string();
        if self.is_released_track_cid_locked(client_id) {
            // do not fall back to matching by type, it would hand this to another pending track
            return (
                signal_cid,
                None,
                buffer::VideoLayersRid::default(),
                false,
                Time::ZERO,
            );
        }

        let mut pending_info = self.pending_tracks.get(client_id);
        if pending_info.is_none() {
            'track_loop: for (cid, pti) in &self.pending_tracks {
                let ti = &pti.track_infos[0];
                for c in &ti.codecs {
                    if c.cid == client_id {
                        pending_info = Some(pti);
                        signal_cid = cid.clone();
                        break 'track_loop;
                    }
                }
            }

            if pending_info.is_none() {
                //
                // If no match on client id, find first one matching type
                // as MediaStreamTrack can change client id when transceiver
                // is added to peer connection.
                //
                for (cid, pti) in &self.pending_tracks {
                    let ti = &pti.track_infos[0];
                    if ti.r#type == kind as i32 {
                        pending_info = Some(pti);
                        signal_cid = cid.clone();
                        break;
                    }
                }
            }
        }

        // if still not found, we are done
        let Some(pending_info) = pending_info.filter(|pi| !(skip_queued && pi.queued)) else {
            return (
                signal_cid,
                None,
                buffer::VideoLayersRid::default(),
                false,
                Time::ZERO,
            );
        };

        (
            signal_cid,
            Some(pending_info.track_infos[0].clone()),
            pending_info.sdp_rids.clone(),
            pending_info.migrated,
            pending_info.created_at,
        )
    }

    pub(super) fn get_pending_track_primary_by_sdp_cid(
        &mut self,
        sdp_cid: &str,
    ) -> Option<&mut PendingTrackInfo> {
        for pti in self.pending_tracks.values_mut() {
            let ti = &pti.track_infos[0];
            if ti.codecs.is_empty() {
                continue;
            }
            if ti.codecs[0].cid == sdp_cid || ti.codecs[0].sdp_cid == sdp_cid {
                return Some(pti);
            }
        }

        None
    }

    // setTrackID either generates a new TrackID for an AddTrackRequest
    fn set_track_id(&self, cid: &str, info: &mut livekit::TrackInfo) {
        let mut track_id = String::new();
        // if already pending, use the same SID
        // it is possible to have multiple AddTrackRequests for the same track
        if let Some(pti) = self.pending_tracks.get(cid) {
            track_id = pti.track_infos[0].sid.clone();
        }

        // otherwise generate
        if track_id.is_empty() {
            let mut track_prefix = putils::TRACK_PREFIX.to_string();
            if info.r#type == livekit::TrackType::Video as i32 {
                track_prefix += "V";
            } else if info.r#type == livekit::TrackType::Audio as i32 {
                track_prefix += "A";
            }

            if info.source == livekit::TrackSource::Camera as i32 {
                track_prefix += "C";
            } else if info.source == livekit::TrackSource::Microphone as i32 {
                track_prefix += "M";
            } else if info.source == livekit::TrackSource::ScreenShare as i32 {
                track_prefix += "S";
            } else if info.source == livekit::TrackSource::ScreenShareAudio as i32 {
                track_prefix += "s";
            }
            track_id = guid::new(&track_prefix);
        }
        info.sid = track_id;
    }
}

// --------------------------------------------------------------------------

/// `lru.Cache[livekit.ParticipantID, participantUpdateInfo]` of hashicorp/golang-lru/v2, as
/// the participant uses it: `Get` makes an entry the most recently used, `Peek` does not,
/// `Add` makes it the most recent and evicts the least recently used one when the cache is
/// full, `Keys` are from the oldest to the newest. New for musivum (the port has no such
/// cache): a list, which for 128 entries is as quick as a map with a list would be.
pub(super) struct UpdateCache {
    size: usize,
    // the oldest first
    entries: VecDeque<(livekit::ParticipantId, ParticipantUpdateInfo)>,
}

impl UpdateCache {
    fn new(size: usize) -> UpdateCache {
        UpdateCache {
            size,
            entries: VecDeque::with_capacity(size),
        }
    }

    pub(super) fn get(&mut self, key: &livekit::ParticipantId) -> Option<ParticipantUpdateInfo> {
        let i = self.entries.iter().position(|(k, _)| k == key)?;
        let entry = self.entries.remove(i)?;
        let value = entry.1.clone();
        self.entries.push_back(entry);
        Some(value)
    }

    pub(super) fn peek(&self, key: &livekit::ParticipantId) -> Option<&ParticipantUpdateInfo> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub(super) fn add(&mut self, key: livekit::ParticipantId, value: ParticipantUpdateInfo) {
        if let Some(i) = self.entries.iter().position(|(k, _)| *k == key) {
            self.entries.remove(i);
        }
        self.entries.push_back((key, value));
        if self.entries.len() > self.size {
            self.entries.pop_front();
        }
    }

    pub(super) fn keys(&self) -> Vec<livekit::ParticipantId> {
        self.entries.iter().map(|(k, _)| k.clone()).collect()
    }
}

// --------------------------------------------------------------------------

// `grants.Video`, which `NewParticipant` checks is there.
fn video_grant(grants: &auth::ClaimGrants) -> &auth::VideoGrant {
    grants
        .video
        .as_ref()
        .expect("the grants of a participant have a VideoGrant")
}

// `track.(*MediaTrack)`, which the original asserts without checking.
pub(super) fn as_media_track(track: Arc<dyn types::MediaTrack>) -> Arc<MediaTrack> {
    downcast_media_track(track)
        .expect("interface conversion: types.MediaTrack is not *rtc.MediaTrack")
}

// `track.(*MediaTrack)` with `ok`.
pub(super) fn downcast_media_track(track: Arc<dyn types::MediaTrack>) -> Option<Arc<MediaTrack>> {
    let any: Arc<dyn Any + Send + Sync> = track;
    any.downcast::<MediaTrack>().ok()
}

// `track.(types.LocalMediaTrack)`, which the original asserts without checking.
fn as_local_media_track(track: Arc<dyn types::MediaTrack>) -> Arc<dyn types::LocalMediaTrack> {
    track
        .as_local_media_track()
        .expect("interface conversion: types.MediaTrack is not types.LocalMediaTrack")
}

// `p.GetTelemetryListener()` for what is made before the participant (see the header).
fn telemetry_listener_of(cell: &ParticipantCell) -> Arc<dyn types::ParticipantTelemetryListener> {
    match participant_of(cell) {
        Some(p) => p.get_telemetry_listener(),
        None => Arc::new(types::NullParticipantTelemetryListener::default()),
    }
}

// Go's `time.Now().Truncate(time.Millisecond)`.
fn truncate_to_millisecond(t: Time) -> Time {
    let n = t.unix_nano();
    Time::from_unix_nano(n - n.rem_euclid(time::MILLISECOND.0))
}

// `errors.Is` looks through a wrapped error.
fn error_cause(err: &Error) -> &Error {
    match err {
        Error::Wrap { cause, .. } => error_cause(cause),
        err => err,
    }
}

// `livekit.TrackType.String()` of a value as the request has it: the name, or the number for
// one the enum does not know.
fn track_type_string(t: i32) -> String {
    match livekit::TrackType::try_from(t) {
        Ok(t) => t.as_str_name().to_string(),
        Err(_) => t.to_string(),
    }
}

// `IsEOF(err)` of utils.go on the errors of a connection: `io.ErrClosedPipe`, which the
// connection layer returns for a connection that is gone (`io.EOF` it does not return).
fn is_eof(err: &Error) -> bool {
    matches!(error_cause(err), Error::Webrtc(webrtc::Error::ClosedPipe))
}

// Go's `go f()` for a function that blocks (waits for workers to end): a thread of its own
// that enters the runtime of its caller, as the queues and timers of what it closes run there.
fn go_blocking(f: impl FnOnce() + Send + 'static) {
    let handle = time::runtime();
    std::thread::spawn(move || {
        let _guard = handle.enter();
        f();
    });
}

// `uuid.NewString()` of google/uuid: a random (version 4) UUID in its text form. New: the
// port has no dependency for UUIDs.
fn new_uuid_string() -> String {
    let mut b: [u8; 16] = rand::rng().random();
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant is 10
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0],
        b[1],
        b[2],
        b[3],
        b[4],
        b[5],
        b[6],
        b[7],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15]
    )
}

// `isFirefox` and `isOBS` of clientinfo.go, which are private to clientinfo.rs: copies (see the
// header).
fn client_info_is_firefox(c: &ClientInfo) -> bool {
    c.client_info.as_ref().is_some_and(|c| {
        equal_fold(&c.browser, "firefox") || equal_fold(&c.browser, "firefox mobile")
    })
}

fn client_info_is_obs(c: &ClientInfo) -> bool {
    c.client_info
        .as_ref()
        .is_some_and(|c| c.browser.contains("OBS"))
}

// Go's strings.EqualFold(s, t) for a `t` that is ASCII, as every name is that this file
// compares with: whether the two are equal under Unicode's simple case folding. That folding
// takes, next to the ASCII letters in their other case, two more characters to an ASCII
// letter: 'ſ' (U+017F) to 's' and the Kelvin sign (U+212A) to 'k'. (A copy of the function of
// clientinfo.rs, which is private there.)
pub(super) fn equal_fold(s: &str, t: &str) -> bool {
    let mut t = t.chars();
    for sr in s.chars() {
        let Some(tr) = t.next() else {
            return false;
        };
        if sr.eq_ignore_ascii_case(&tr) {
            continue;
        }
        match (sr, tr) {
            ('\u{17f}', 's' | 'S') | ('\u{212a}', 'k' | 'K') => continue,
            _ => return false,
        }
    }
    t.next().is_none()
}

// --------------------------------------------------------------------------

// The traits the participant is. Each method hands on to the method of the same name above
// or in the other files of the participant (participant_signal.rs, participant_sdp.rs,
// participant_data_track.rs), or, where Go has the method from the manager the participant
// embeds (`*TransportManager`, `*UpTrackManager`, `*UpDataTrackManager`,
// `*SubscriptionManager`), to that manager.

impl types::AsLocalParticipant for ParticipantImpl {
    fn as_local_participant(self: Arc<Self>) -> Option<Arc<dyn LocalParticipant>> {
        Some(self)
    }
}

impl types::Participant for ParticipantImpl {
    fn id(&self) -> livekit::ParticipantId {
        ParticipantImpl::id(self)
    }

    fn identity(&self) -> livekit::ParticipantIdentity {
        ParticipantImpl::identity(self)
    }

    fn state(&self) -> livekit::participant_info::State {
        ParticipantImpl::state(self)
    }

    fn connected_at(&self) -> Time {
        ParticipantImpl::connected_at(self)
    }

    fn close_reason(&self) -> types::ParticipantCloseReason {
        ParticipantImpl::close_reason(self)
    }

    fn kind(&self) -> livekit::participant_info::Kind {
        ParticipantImpl::kind(self)
    }

    fn kind_details(&self) -> Vec<livekit::participant_info::KindDetail> {
        ParticipantImpl::kind_details(self)
    }

    fn is_recorder(&self) -> bool {
        ParticipantImpl::is_recorder(self)
    }

    fn is_dependent(&self) -> bool {
        ParticipantImpl::is_dependent(self)
    }

    fn is_agent(&self) -> bool {
        ParticipantImpl::is_agent(self)
    }

    fn get_logger(&self) -> Logger {
        ParticipantImpl::get_logger(self)
    }

    fn can_skip_broadcast(&self) -> bool {
        ParticipantImpl::can_skip_broadcast(self)
    }

    fn version(&self) -> TimedVersion {
        ParticipantImpl::version(self)
    }

    fn to_proto(&self) -> livekit::ParticipantInfo {
        ParticipantImpl::to_proto(self)
    }

    fn to_proto_with_version(&self) -> (livekit::ParticipantInfo, TimedVersion) {
        ParticipantImpl::to_proto_with_version(self)
    }

    fn is_publisher(&self) -> bool {
        ParticipantImpl::is_publisher(self)
    }

    fn get_published_track(
        &self,
        track_id: &livekit::TrackId,
    ) -> Option<Arc<dyn types::MediaTrack>> {
        self.up_track_manager.get_published_track(track_id)
    }

    fn get_published_tracks(&self) -> Vec<Arc<dyn types::MediaTrack>> {
        self.up_track_manager.get_published_tracks()
    }

    fn remove_published_track(
        &self,
        track: Arc<dyn types::MediaTrack>,
        is_expected_to_resume: bool,
    ) {
        self.up_track_manager
            .remove_published_track(track, is_expected_to_resume);
    }

    fn get_published_data_tracks(&self) -> Vec<Arc<dyn types::DataTrack>> {
        self.up_data_track_manager.get_published_data_tracks()
    }

    fn get_published_data_track(&self, handle: u16) -> Option<Arc<dyn types::DataTrack>> {
        self.up_data_track_manager.get_published_data_track(handle)
    }

    fn remove_published_data_track(&self, track: Arc<dyn types::DataTrack>) {
        self.up_data_track_manager
            .remove_published_data_track(track);
    }

    fn get_audio_level(&self) -> (f64, bool) {
        self.up_track_manager.get_audio_level()
    }

    fn has_permission(
        &self,
        track_id: &livekit::TrackId,
        sub_identity: &livekit::ParticipantIdentity,
    ) -> bool {
        self.up_track_manager.has_permission(track_id, sub_identity)
    }

    fn hidden(&self) -> bool {
        ParticipantImpl::hidden(self)
    }

    fn migrate_state(&self) -> types::MigrateState {
        ParticipantImpl::migrate_state(self)
    }

    fn close(
        &self,
        send_leave: bool,
        reason: types::ParticipantCloseReason,
        is_expected_to_resume: bool,
    ) -> Result<(), Error> {
        ParticipantImpl::close(self, send_leave, reason, is_expected_to_resume)
    }

    fn is_closed(&self) -> bool {
        ParticipantImpl::is_closed(self)
    }

    fn is_disconnected(&self) -> bool {
        ParticipantImpl::is_disconnected(self)
    }

    fn subscription_permission(
        &self,
    ) -> (Option<Arc<livekit::SubscriptionPermission>>, TimedVersion) {
        self.up_track_manager.subscription_permission()
    }

    fn update_subscription_permission(
        &self,
        subscription_permission: Option<&livekit::SubscriptionPermission>,
        timed_version: TimedVersion,
        resolver_by_sid: Option<
            Box<dyn Fn(&livekit::ParticipantId) -> Option<Arc<dyn LocalParticipant>> + Send + Sync>,
        >,
    ) -> Result<(), Error> {
        ParticipantImpl::update_subscription_permission(
            self,
            subscription_permission,
            timed_version,
            resolver_by_sid,
        )
    }

    fn debug_info(&self) -> sfu::DebugInfo {
        ParticipantImpl::debug_info(self)
    }

    fn handle_received_data_track_message(
        &self,
        data: &[u8],
        packet: &datatrack::Packet,
        arrival_time: i64,
    ) {
        self.up_data_track_manager
            .handle_received_data_track_message(data, packet, arrival_time);
    }

    fn get_participant_listener(&self) -> Arc<dyn types::ParticipantListener> {
        ParticipantImpl::get_participant_listener(self)
    }

    fn add_data_blob(&self, data_blob: &livekit::DataBlob) {
        ParticipantDataBlobHandler::add_data_blob(self, data_blob);
    }

    fn get_data_blob(&self, key: Option<&livekit::DataBlobKey>) -> Option<Arc<livekit::DataBlob>> {
        ParticipantDataBlobHandler::get_data_blob(self, key)
    }
}

impl types::LocalParticipant for ParticipantImpl {
    fn telemetry_guard(&self) -> Option<Arc<telemetry::ReferenceGuard>> {
        ParticipantImpl::telemetry_guard(self)
    }

    fn get_telemetry_listener(&self) -> Arc<dyn types::ParticipantTelemetryListener> {
        ParticipantImpl::get_telemetry_listener(self)
    }

    fn get_country(&self) -> String {
        ParticipantImpl::get_country(self)
    }

    fn get_trailer(&self) -> Vec<u8> {
        ParticipantImpl::get_trailer(self)
    }

    fn get_adaptive_stream(&self) -> bool {
        ParticipantImpl::get_adaptive_stream(self)
    }

    fn get_enable_start_at_desired_quality(&self) -> bool {
        ParticipantImpl::get_enable_start_at_desired_quality(self)
    }

    fn protocol_version(&self) -> types::ProtocolVersion {
        ParticipantImpl::protocol_version(self)
    }

    fn supports_sync_stream_id(&self) -> bool {
        ParticipantImpl::supports_sync_stream_id(self)
    }

    fn supports_transceiver_reuse(&self, mt: Arc<dyn types::MediaTrack>) -> bool {
        ParticipantImpl::supports_transceiver_reuse(self, mt)
    }

    fn is_using_single_peer_connection(&self) -> bool {
        ParticipantImpl::is_using_single_peer_connection(self)
    }

    fn is_ready(&self) -> bool {
        ParticipantImpl::is_ready(self)
    }

    fn active_at(&self) -> Time {
        ParticipantImpl::active_at(self)
    }

    fn disconnected(&self) -> CancellationToken {
        ParticipantImpl::disconnected(self)
    }

    fn is_connection_canceled(&self, close_reason: types::ParticipantCloseReason) -> bool {
        ParticipantImpl::is_connection_canceled(self, close_reason)
    }

    fn is_idle(&self) -> bool {
        ParticipantImpl::is_idle(self)
    }

    fn subscriber_as_primary(&self) -> bool {
        self.transport_manager.subscriber_as_primary()
    }

    fn get_client_info(&self) -> Option<Arc<livekit::ClientInfo>> {
        ParticipantImpl::get_client_info(self)
    }

    fn get_client_configuration(&self) -> Option<livekit::ClientConfiguration> {
        ParticipantImpl::get_client_configuration(self)
    }

    fn get_buffer_factory(&self) -> Arc<buffer::Factory> {
        ParticipantImpl::get_buffer_factory(self)
    }

    fn get_playout_delay_config(&self) -> Option<livekit::PlayoutDelay> {
        ParticipantImpl::get_playout_delay_config(self)
    }

    fn get_pending_track(&self, track_id: &livekit::TrackId) -> Option<livekit::TrackInfo> {
        ParticipantImpl::get_pending_track(self, track_id)
    }

    fn get_ice_connection_info(&self) -> Vec<types::IceConnectionInfo> {
        self.transport_manager.get_ice_connection_info()
    }

    fn has_ice_connected(&self) -> bool {
        ParticipantImpl::has_ice_connected(self)
    }

    fn has_connected(&self) -> bool {
        ParticipantImpl::has_connected(self)
    }

    fn get_enabled_publish_codecs(&self) -> Vec<livekit::Codec> {
        ParticipantImpl::get_enabled_publish_codecs(self)
    }

    fn get_publisher_ice_session_ufrag(&self) -> Result<String, Error> {
        self.transport_manager.get_publisher_ice_session_ufrag()
    }

    fn supports_moving(&self) -> Result<(), Error> {
        ParticipantImpl::supports_moving(self)
    }

    fn get_last_reliable_sequence(&self, migrate_out: bool) -> u32 {
        ParticipantImpl::get_last_reliable_sequence(self, migrate_out)
    }

    fn is_warp_enabled(&self) -> bool {
        ParticipantImpl::is_warp_enabled(self)
    }

    fn swap_response_sink(
        &self,
        sink: Option<Arc<dyn MessageSink>>,
        reason: types::SignallingCloseReason,
    ) {
        ParticipantImpl::swap_response_sink(self, sink, reason);
    }

    fn get_response_sink(&self) -> Option<Arc<dyn MessageSink>> {
        ParticipantImpl::get_response_sink(self)
    }

    fn close_signal_connection(&self, reason: types::SignallingCloseReason) {
        ParticipantImpl::close_signal_connection(self, reason);
    }

    fn update_last_seen_signal(&self) {
        self.transport_manager.update_last_seen_signal();
    }

    fn set_signal_source_valid(&self, valid: bool) {
        self.transport_manager.set_signal_source_valid(valid);
    }

    fn handle_signal_source_close(&self) {
        ParticipantImpl::handle_signal_source_close(self);
    }

    fn update_metadata(
        &self,
        update: &livekit::UpdateParticipantMetadata,
        from_admin: bool,
    ) -> Result<(), Error> {
        ParticipantImpl::update_metadata(self, update, from_admin)
    }

    fn set_name(&self, name: &str) {
        ParticipantImpl::set_name(self, name);
    }

    fn set_metadata(&self, metadata: &str) {
        ParticipantImpl::set_metadata(self, metadata);
    }

    fn set_attributes(&self, attributes: &HashMap<String, String>) {
        ParticipantImpl::set_attributes(self, attributes);
    }

    fn update_audio_track(&self, update: &livekit::UpdateLocalAudioTrack) -> Result<(), Error> {
        ParticipantImpl::update_audio_track(self, update)
    }

    fn update_video_track(&self, update: &livekit::UpdateLocalVideoTrack) -> Result<(), Error> {
        ParticipantImpl::update_video_track(self, update)
    }

    fn claim_grants(&self) -> Arc<auth::ClaimGrants> {
        ParticipantImpl::claim_grants(self)
    }

    fn token_expires_at(&self) -> Time {
        ParticipantImpl::token_expires_at(self)
    }

    fn set_permission(&self, permission: Option<&livekit::ParticipantPermission>) -> bool {
        ParticipantImpl::set_permission(self, permission)
    }

    fn can_publish(&self) -> bool {
        ParticipantImpl::can_publish(self)
    }

    fn can_publish_source(&self, source: livekit::TrackSource) -> bool {
        ParticipantImpl::can_publish_source(self, source)
    }

    fn can_subscribe(&self) -> bool {
        ParticipantImpl::can_subscribe(self)
    }

    fn can_publish_data(&self) -> bool {
        ParticipantImpl::can_publish_data(self)
    }

    fn handle_ice_trickle(&self, trickle_request: &livekit::TrickleRequest) {
        ParticipantImpl::handle_ice_trickle(self, trickle_request);
    }

    fn handle_offer(&self, sd: &livekit::SessionDescription) -> Result<(), Error> {
        ParticipantImpl::handle_offer(self, sd)
    }

    fn get_answer(&self) -> Result<(SessionDescription, u32), Error> {
        ParticipantImpl::get_answer(self)
    }

    fn handle_ice_trickle_sdp_fragment(&self, sdp_fragment: &str) -> Result<(), Error> {
        self.transport_manager
            .handle_ice_trickle_sdp_fragment(sdp_fragment)
    }

    fn handle_ice_restart_sdp_fragment(&self, sdp_fragment: &str) -> Result<String, Error> {
        self.transport_manager
            .handle_ice_restart_sdp_fragment(sdp_fragment)
    }

    fn add_track(&self, req: &livekit::AddTrackRequest) {
        ParticipantImpl::add_track(self, req);
    }

    fn set_track_muted(
        &self,
        mute: &livekit::MuteTrackRequest,
        from_admin: bool,
    ) -> Option<livekit::TrackInfo> {
        ParticipantImpl::set_track_muted(self, mute, from_admin)
    }

    fn handle_answer(&self, sd: &livekit::SessionDescription) {
        ParticipantImpl::handle_answer(self, sd);
    }

    fn negotiate(&self, force: bool) {
        ParticipantImpl::negotiate(self, force);
    }

    fn ice_restart(&self, ice_config: Option<&livekit::IceConfig>) {
        ParticipantImpl::ice_restart(self, ice_config);
    }

    fn add_track_local(
        &self,
        track_local: Arc<dyn webrtc::TrackLocal>,
        params: types::AddTrackParams,
    ) -> Result<(Arc<dyn webrtc::RtpSender>, Arc<dyn webrtc::RtpTransceiver>), Error> {
        ParticipantImpl::add_track_local(self, track_local, params)
    }

    fn add_transceiver_from_track_local(
        &self,
        track_local: Arc<dyn webrtc::TrackLocal>,
        params: types::AddTrackParams,
    ) -> Result<(Arc<dyn webrtc::RtpSender>, Arc<dyn webrtc::RtpTransceiver>), Error> {
        ParticipantImpl::add_transceiver_from_track_local(self, track_local, params)
    }

    fn remove_track_local(&self, sender: Arc<dyn webrtc::RtpSender>) -> Result<(), Error> {
        // the manager takes the connection layer's sender, which is the one behind the seam
        // that `add_track_local` gave (see the header)
        let any: Arc<dyn Any + Send + Sync> = sender;
        match any.downcast::<webrtc::peerconnection::RtpSender>() {
            Ok(sender) => self.transport_manager.remove_track_local(&sender),
            Err(_) => Err(Error::NoSender),
        }
    }

    fn write_subscriber_rtcp(&self, pkts: &[Box<dyn rtcp::Packet>]) -> Result<(), Error> {
        self.transport_manager.write_subscriber_rtcp(pkts)
    }

    fn subscribe_to_track(&self, track_id: &livekit::TrackId, is_sync: bool) {
        self.subscription_manager
            .subscribe_to_track(track_id, is_sync);
    }

    fn unsubscribe_from_track(&self, track_id: &livekit::TrackId) {
        self.subscription_manager.unsubscribe_from_track(track_id);
    }

    fn update_subscribed_track_settings(
        &self,
        track_id: &livekit::TrackId,
        settings: &livekit::UpdateTrackSettings,
    ) {
        self.subscription_manager
            .update_subscribed_track_settings(track_id, settings);
    }

    fn get_subscribed_tracks(&self) -> Vec<Arc<dyn types::SubscribedTrack>> {
        self.subscription_manager.get_subscribed_tracks()
    }

    fn get_subscribed_data_tracks(&self) -> Vec<Arc<dyn types::DataDownTrack>> {
        self.subscription_manager.get_subscribed_data_tracks()
    }

    fn is_track_name_subscribed(
        &self,
        publisher_identity: &livekit::ParticipantIdentity,
        track_name: &str,
    ) -> bool {
        self.subscription_manager
            .is_track_name_subscribed(publisher_identity, track_name)
    }

    fn subscribe_to_data_track(&self, track_id: &livekit::TrackId) {
        self.subscription_manager.subscribe_to_data_track(track_id);
    }

    fn unsubscribe_from_data_track(&self, track_id: &livekit::TrackId) {
        self.subscription_manager
            .unsubscribe_from_data_track(track_id);
    }

    fn update_data_track_subscription_options(
        &self,
        track_id: &livekit::TrackId,
        subscription_options: Option<&livekit::DataTrackSubscriptionOptions>,
    ) {
        self.subscription_manager
            .update_data_track_subscription_options(track_id, subscription_options);
    }

    fn verify(&self) -> bool {
        ParticipantImpl::verify(self)
    }

    fn verify_subscribe_participant_info(&self, p_id: &livekit::ParticipantId, version: u32) {
        ParticipantImpl::verify_subscribe_participant_info(self, p_id, version);
    }

    fn wait_until_subscribed(&self, timeout: Duration) -> Result<(), Error> {
        self.subscription_manager.wait_until_subscribed(timeout)
    }

    fn stop_and_get_subscribed_tracks_forwarder_state(
        &self,
    ) -> HashMap<livekit::TrackId, livekit::RtpForwarderState> {
        self.subscription_manager
            .stop_and_get_subscribed_tracks_forwarder_state()
    }

    fn supports_codec_change(&self) -> bool {
        ParticipantImpl::supports_codec_change(self)
    }

    fn get_subscribed_participants(&self) -> Vec<livekit::ParticipantId> {
        self.subscription_manager.get_subscribed_participants()
    }

    fn is_subscribed_to(&self, sid: &livekit::ParticipantId) -> bool {
        self.subscription_manager.is_subscribed_to(sid)
    }

    fn get_connection_quality(&self) -> livekit::ConnectionQualityInfo {
        ParticipantImpl::get_connection_quality(self)
    }

    fn send_join_response(&self, join_response: &livekit::JoinResponse) -> Result<(), Error> {
        ParticipantImpl::send_join_response(self, join_response)
    }

    fn send_participant_update(
        &self,
        participants: &[livekit::ParticipantInfo],
    ) -> Result<(), Error> {
        ParticipantImpl::send_participant_update(self, participants)
    }

    fn send_speaker_update(
        &self,
        speakers: &[livekit::SpeakerInfo],
        force: bool,
    ) -> Result<(), Error> {
        ParticipantImpl::send_speaker_update(self, speakers, force)
    }

    fn send_data_message(
        &self,
        kind: livekit::data_packet::Kind,
        data: &[u8],
        sender_id: &livekit::ParticipantId,
        seq: u32,
    ) -> Result<(), Error> {
        ParticipantImpl::send_data_message(self, kind, data, sender_id, seq)
    }

    fn send_data_message_unlabeled(
        &self,
        data: &[u8],
        use_raw: bool,
        sender: &livekit::ParticipantIdentity,
    ) -> Result<(), Error> {
        ParticipantImpl::send_data_message_unlabeled(self, data, use_raw, sender)
    }

    fn send_room_update(&self, room: &livekit::Room) -> Result<(), Error> {
        ParticipantImpl::send_room_update(self, room)
    }

    fn send_connection_quality_update(
        &self,
        update: &livekit::ConnectionQualityUpdate,
    ) -> Result<(), Error> {
        ParticipantImpl::send_connection_quality_update(self, update)
    }

    fn send_subscription_permission_update(
        &self,
        publisher_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        allowed: bool,
    ) -> Result<(), Error> {
        ParticipantImpl::send_subscription_permission_update(self, publisher_id, track_id, allowed)
    }

    fn send_refresh_token(&self, token: &str) -> Result<(), Error> {
        ParticipantImpl::send_refresh_token(self, token)
    }

    fn handle_reconnect_and_send_response(
        &self,
        reconnect_reason: livekit::ReconnectReason,
        reconnect_response: &livekit::ReconnectResponse,
        recently_disconnected_participants: &[livekit::ParticipantInfo],
    ) -> Result<(), Error> {
        ParticipantImpl::handle_reconnect_and_send_response(
            self,
            reconnect_reason,
            reconnect_response,
            recently_disconnected_participants,
        )
    }

    fn issue_full_reconnect(&self, reason: types::ParticipantCloseReason) {
        ParticipantImpl::issue_full_reconnect(self, reason);
    }

    fn send_room_moved_response(&self, moved: &livekit::RoomMovedResponse) -> Result<(), Error> {
        ParticipantImpl::send_room_moved_response(self, moved)
    }

    fn send_data_track_subscriber_handles(
        &self,
        handles: &HashMap<u32, livekit::data_track_subscriber_handles::PublishedDataTrack>,
    ) -> Result<(), Error> {
        ParticipantImpl::send_data_track_subscriber_handles(self, handles)
    }

    fn get_recently_disconnected_participants(&self) -> Vec<livekit::ParticipantInfo> {
        ParticipantImpl::get_recently_disconnected_participants(self)
    }

    fn add_on_close(
        &self,
        key: &str,
        callback: Option<Box<dyn Fn(Arc<dyn LocalParticipant>) + Send + Sync>>,
    ) {
        ParticipantImpl::add_on_close(self, key, callback);
    }

    fn on_claims_changed(&self, callback: Box<dyn Fn(Arc<dyn LocalParticipant>) + Send + Sync>) {
        ParticipantImpl::on_claims_changed(self, callback);
    }

    fn handle_receiver_report(&self, dt: &sfu::DownTrack, report: &ReceiverReport) {
        self.transport_manager.handle_receiver_report(dt, report);
    }

    fn maybe_start_migration(
        &self,
        force: bool,
        on_start: Option<Box<dyn Fn() + Send + Sync>>,
    ) -> bool {
        ParticipantImpl::maybe_start_migration(self, force, on_start)
    }

    fn notify_migration(&self) {
        ParticipantImpl::notify_migration(self);
    }

    fn set_migrate_state(&self, s: types::MigrateState) {
        ParticipantImpl::set_migrate_state(self, s);
    }

    fn set_migrate_info(
        &self,
        previous_offer: Option<&SessionDescription>,
        previous_answer: Option<&SessionDescription>,
        media_tracks: &[livekit::TrackPublishedResponse],
        data_channels: &[livekit::DataChannelInfo],
        data_channel_receive_state: &[livekit::DataChannelReceiveState],
        data_tracks: &[livekit::PublishDataTrackResponse],
    ) {
        ParticipantImpl::set_migrate_info(
            self,
            previous_offer,
            previous_answer,
            media_tracks,
            data_channels,
            data_channel_receive_state,
            data_tracks,
        );
    }

    fn is_reconnect(&self) -> bool {
        ParticipantImpl::is_reconnect(self)
    }

    fn is_migration(&self) -> bool {
        ParticipantImpl::is_migration(self)
    }

    fn leave_session(&self, leave: Option<Box<dyn Fn() + Send + Sync>>) {
        ParticipantImpl::leave_session(self, leave);
    }

    fn join_session(&self, join: Option<Box<dyn Fn() + Send + Sync>>) {
        ParticipantImpl::join_session(self, join);
    }

    fn move_to_room(&self, params: types::MoveToRoomParams) {
        ParticipantImpl::move_to_room(self, params);
    }

    fn update_media_rtt(&self, rtt: u32) {
        ParticipantImpl::update_media_rtt(self, rtt);
    }

    fn update_signaling_rtt(&self, rtt: u32) {
        self.transport_manager.update_signaling_rtt(rtt);
    }

    fn cache_down_track(
        &self,
        track_id: &livekit::TrackId,
        rtp_transceiver: Arc<dyn webrtc::RtpTransceiver>,
        down_track_state: sfu::DownTrackState,
    ) {
        ParticipantImpl::cache_down_track(self, track_id, rtp_transceiver, down_track_state);
    }

    fn uncache_down_track(&self, rtp_transceiver: Arc<dyn webrtc::RtpTransceiver>) {
        ParticipantImpl::uncache_down_track(self, rtp_transceiver);
    }

    fn get_cached_down_track(
        &self,
        track_id: &livekit::TrackId,
    ) -> (Option<Arc<dyn webrtc::RtpTransceiver>>, sfu::DownTrackState) {
        ParticipantImpl::get_cached_down_track(self, track_id)
    }

    fn set_ice_config(&self, ice_config: Option<&livekit::IceConfig>) {
        self.transport_manager.set_ice_config(ice_config);
    }

    fn get_ice_config(&self) -> Option<livekit::IceConfig> {
        self.transport_manager.get_ice_config()
    }

    fn on_ice_config_changed(
        &self,
        callback: Box<dyn Fn(Arc<dyn LocalParticipant>, &livekit::IceConfig) + Send + Sync>,
    ) {
        ParticipantImpl::on_ice_config_changed(self, callback);
    }

    fn update_subscribed_quality(
        &self,
        node_id: &livekit::NodeId,
        track_id: &livekit::TrackId,
        max_qualities: &[types::SubscribedCodecQuality],
    ) -> Result<(), Error> {
        ParticipantImpl::update_subscribed_quality(self, node_id, track_id, max_qualities)
    }

    fn update_subscribed_audio_codecs(
        &self,
        node_id: &livekit::NodeId,
        track_id: &livekit::TrackId,
        codecs: &[livekit::SubscribedAudioCodec],
    ) -> Result<(), Error> {
        ParticipantImpl::update_subscribed_audio_codecs(self, node_id, track_id, codecs)
    }

    fn update_media_loss(
        &self,
        node_id: &livekit::NodeId,
        track_id: &livekit::TrackId,
        fractional_loss: u32,
    ) -> Result<(), Error> {
        ParticipantImpl::update_media_loss(self, node_id, track_id, fractional_loss)
    }

    fn set_subscriber_allow_pause(&self, allow_pause: bool) {
        self.transport_manager
            .set_subscriber_allow_pause(allow_pause);
    }

    fn set_subscriber_channel_capacity(&self, channel_capacity: i64) {
        self.transport_manager
            .set_subscriber_channel_capacity(channel_capacity);
    }

    fn get_pacer(&self) -> Arc<dyn pacer::Pacer> {
        ParticipantImpl::get_pacer(self)
    }

    fn get_disable_sender_report_pass_through(&self) -> bool {
        ParticipantImpl::get_disable_sender_report_pass_through(self)
    }

    fn handle_metrics(
        &self,
        sender_participant_id: &livekit::ParticipantId,
        batch: &livekit::MetricsBatch,
    ) -> Result<(), Error> {
        ParticipantImpl::handle_metrics(self, sender_participant_id, batch)
    }

    fn handle_update_subscriptions(
        &self,
        track_ids: &[livekit::TrackId],
        participant_tracks: &[livekit::ParticipantTracks],
        subscribe: bool,
    ) {
        ParticipantImpl::handle_update_subscriptions(
            self,
            track_ids,
            participant_tracks,
            subscribe,
        );
    }

    fn handle_update_subscription_permission(
        &self,
        subscription_permission: &livekit::SubscriptionPermission,
    ) -> Result<(), Error> {
        ParticipantImpl::handle_update_subscription_permission(self, subscription_permission)
    }

    fn handle_sync_state(&self, sync_state: &livekit::SyncState) -> Result<(), Error> {
        ParticipantImpl::handle_sync_state(self, sync_state)
    }

    fn handle_simulate_scenario(
        &self,
        simulate_scenario: &livekit::SimulateScenario,
    ) -> Result<(), Error> {
        ParticipantImpl::handle_simulate_scenario(self, simulate_scenario)
    }

    fn handle_leave_request(&self, reason: types::ParticipantCloseReason) {
        ParticipantImpl::handle_leave_request(self, reason);
    }

    fn handle_publish_data_track_request(&self, req: &livekit::PublishDataTrackRequest) {
        ParticipantImpl::handle_publish_data_track_request(self, req);
    }

    fn handle_unpublish_data_track_request(&self, req: &livekit::UnpublishDataTrackRequest) {
        ParticipantImpl::handle_unpublish_data_track_request(self, req);
    }

    fn handle_update_data_subscription(&self, req: &livekit::UpdateDataSubscription) {
        ParticipantImpl::handle_update_data_subscription(self, req);
    }

    fn handle_store_data_blob_request(&self, req: &livekit::StoreDataBlobRequest) {
        ParticipantDataBlobHandler::handle_store_data_blob_request(self, req);
    }

    fn handle_get_data_blob_request(&self, req: &livekit::GetDataBlobRequest) {
        ParticipantDataBlobHandler::handle_get_data_blob_request(self, req);
    }

    fn process_get_data_blob_request(
        &self,
        req: &livekit::GetDataBlobRequest,
        publisher: Option<Arc<dyn types::Participant>>,
    ) {
        ParticipantDataBlobHandler::process_get_data_blob_request(self, req, publisher);
    }

    fn handle_signal_message(&self, msg: Message) -> Result<(), Error> {
        ParticipantImpl::handle_signal_message(self, msg)
    }

    fn perform_rpc(
        &self,
        req: &livekit::PerformRpcRequest,
        result_ch: mpsc::Sender<String>,
        error_ch: mpsc::Sender<Error>,
    ) {
        ParticipantImpl::perform_rpc(self, req, result_ch, error_ch);
    }

    fn get_data_track_transport(&self) -> Arc<dyn types::DataTrackTransport> {
        self.transport_manager.get_data_track_transport()
    }

    fn clear_participant_listener(&self) {
        ParticipantImpl::clear_participant_listener(self);
    }

    fn get_next_subscribed_data_track_handle(&self) -> u16 {
        ParticipantImpl::get_next_subscribed_data_track_handle(self)
    }

    fn get_all_data_blob(&self) -> Vec<Arc<livekit::DataBlob>> {
        ParticipantDataBlobHandler::get_all_data_blob(self)
    }
}

// What the methods of participant_data_blob_handler.go use of the participant (see the header
// of participant_data_blob_handler.rs).
impl ParticipantDataBlobHandler for ParticipantImpl {
    fn data_blob_participant(&self) -> Arc<dyn LocalParticipant> {
        self.arc()
    }

    fn enable_participant_data_blob(&self) -> bool {
        self.params.enable_participant_data_blob
    }

    fn limit_config(&self) -> &config::LimitConfig {
        &self.params.limit_config
    }

    fn pub_logger(&self) -> &Logger {
        &self.pub_logger
    }

    fn data_blob(&self) -> &ParticipantDataBlob {
        &self.data_blob
    }

    fn listener(&self) -> Arc<dyn LocalParticipantListener> {
        ParticipantImpl::listener(self)
    }

    fn send_request_response(
        &self,
        request_response: livekit::RequestResponse,
    ) -> Result<(), Error> {
        ParticipantImpl::send_request_response(self, request_response)
    }

    fn send_store_data_blob_response(
        &self,
        request_id: u32,
        key: Option<livekit::DataBlobKey>,
    ) -> Result<(), Error> {
        ParticipantImpl::send_store_data_blob_response(self, request_id, key)
    }

    fn send_get_data_blob_response(
        &self,
        request_id: u32,
        data_blob: Option<livekit::DataBlob>,
    ) -> Result<(), Error> {
        ParticipantImpl::send_get_data_blob_response(self, request_id, data_blob)
    }
}

#[cfg(test)]
#[path = "participant_internal_test.rs"]
mod participant_internal_test;
