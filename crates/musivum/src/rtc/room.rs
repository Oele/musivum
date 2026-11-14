// Ported from LiveKit, pkg/rtc/room.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - Left out, because musivum has no agents, no egress and not LiveKit's observability:
//   `AgentStore`, `agentDispatch`, `agentJob`, `ErrJobShutdownTimeout`, the methods
//   `GetAgentDispatches`, `AddAgentDispatch`, `DeleteAgentDispatch`, `launchRoomAgents`,
//   `launchTargetAgents`, `handleNewJobs`, `createAgentDispatch`,
//   `createAgentDispatchFromRoomDispatch` and `createAgentDispatchesFromRoomAgent`, the
//   fields that serve them (`agentDispatches`, `agentClient`, `agentStore`,
//   `agentParticpants`, `egressLauncher`, `hasPublished`, `launchedTrackEgresses`) and what
//   reports to the participant's session reporter (`GetReporter().Tx`, `ReporterType`). A
//   comment stands at the first place of each kind where the room calls into them. psrpc
//   and `protocol/rpc` are used by room.go only there: for the agents' errors
//   (`ErrJobShutdownTimeout`, the "dispatch ID not found" of `DeleteAgentDispatch`) and the
//   reasons of `TerminateJob`, the RPC between the server and the agent workers, so nothing
//   of them is here. The RPC between participants (`RpcRequest`, `RpcAck`, `RpcResponse`) is
//   a kind of data packet and goes through `onDataMessage` and `BroadcastDataPacketForRoom`
//   like any other: that is ported.
// - `NewRoom` is `Room::new` and gives an `Arc`; the room keeps a `Weak` of itself for its
//   tasks, timers and listeners. What `lock` guards (`protoRoom`, the participants with
//   their options and request sources, `trailer`) is in an inner struct. The name and the
//   SID of `protoRoom`, which upstream reads without the lock (`Name()`, `ID()`), are copies
//   outside it: they never change, and the telemetry listener asks for them from calls a
//   participant may make while the room holds its lock. The proxy of the room's message is
//   made after the room (`OnceLock`), because its update function reads the room. A function
//   that "assumes lock is already acquired" takes the inner struct. `batchedUpdatesMu`,
//   `simulationLock` and `onStateChangeMu` are locks of their own with what they guard
//   inside. The three callbacks, set without a lock upstream, are behind a lock each and are
//   copied out before they are called.
// - The workers (`audioUpdateWorker`, `connectionQualityWorker`, `changeUpdateWorker`,
//   `simulationCleanupWorker`) and the goroutines are tasks; `closed` is a
//   `CancellationToken`. `roomUpdateInterval`, which the test file overrides in `init()`,
//   is a constant with the test value in a test build.
// - The two listeners (`localParticipantListener`, `participantTelemetryListener`) are made
//   with the room and hold it weakly; a call that arrives after the room is gone does
//   nothing (and returns no error).
// - `GetConnectionQuality` cannot give nil here (`types::LocalParticipant` returns the info
//   itself), so the two checks for nil always pass.
// - A data packet that a participant sends is handed to the listener by reference: the room
//   caches it as it came and then broadcasts a copy, which `BroadcastDataPacketForRoom`
//   changes where the original changes the caller's packet. The caller only reads its metrics
//   afterwards, which that function does not touch. `proto.Marshal` cannot fail here
//   (prost's encoding does not), so its error branches have no counterpart.
//   `BroadcastDataMessageForRoom` takes a source that is not optional: upstream checks it for
//   nil and then dereferences it anyway.
// - A request source may be nil (upstream's tests pass nil): the map holds an `Option`, and
//   `ReplaceParticipantRequestSource` closes the old one only if there is one, where Go would
//   call `Close` on nil.
// - `PingIntervalSeconds` and `PingTimeoutSeconds` are constants of participant.go, which is
//   not ported yet: their values are written out here, private, until it is.
// - `connectionDetailsFields` gives pairs of key and value for a log line, which
//   `infow_fields` writes. `GetOtherParticipantInfo`, `PushAndDequeueUpdates`, `CompareParticipant`
//   and the broadcast functions take the participants and the room they only read as
//   references (`&dyn Trait`, `Option<&dyn Trait>` where nil is passed).
// - The workers end at the first chance once the room is closed or gone. New: a room that
//   is dropped without having been closed cancels `closed` too, so its workers end.
// Who holds whom: the room manager holds the room. The room holds its participants (until
//   they are removed), the track manager with the published tracks, the proxy of its
//   message and its two listeners. The participants hold the listeners, which hold the room
//   weakly; so do the tasks, the timer of `Join`, the proxy's update function and the
//   resolver handed to `UpdateSubscriptionPermission`. The timer of `Join` holds its
//   participant weakly as well: a participant that is gone is no longer in the room, where
//   `RemoveParticipant` would do nothing anyway. The callbacks `OnClose`,
//   `OnParticipantChanged` and `OnRoomUpdated` are held by the room: whoever sets one must
//   not have it hold the room strongly.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock, Weak};

use bytes::Bytes;
use musivum_protocol::Message;
use parking_lot::{Mutex, RwLock};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use super::config::WebRtcConfig;
use super::roomtrackmanager::RoomTrackManager;
use super::user_packet_deduper::UserPacketDeduper;
use super::utils::{chunk_proto_batch, logger_with_room, maybe_truncate_ip};
use crate::config;
use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;
use crate::protocol::logger::{self, Logger};
use crate::protocol::utils as putils;
use crate::routing;
use crate::rtc::Error;
use crate::rtc::types;
use crate::sfu;
use crate::sfu::buffer;
use crate::sfu::connectionquality;
use crate::telemetry;
use crate::telemetry::prometheus;
use crate::utils as sutils;
use crate::{debugw, errorw, infow, warnw};

pub const AUDIO_LEVEL_QUANTIZATION: u32 = 8; // ideally power of 2 to minimize float decimal
const INV_AUDIO_LEVEL_QUANTIZATION: f64 = 1.0 / AUDIO_LEVEL_QUANTIZATION as f64;
const SUBSCRIBER_UPDATE_INTERVAL: Duration = Duration::from_secs(3);

const DATA_FORWARD_LOAD_BALANCE_THRESHOLD: u64 = 4;

const SIMULATE_DISCONNECT_SIGNAL_TIMEOUT: Duration = Duration::from_secs(5);

const DATA_MESSAGE_CACHE_TTL: Duration = Duration::from_secs(10);
const DATA_MESSAGE_CACHE_SIZE: usize = 100_000;

// var to allow unit test override
// frequency to update room participant counts
#[cfg(not(test))]
const ROOM_UPDATE_INTERVAL: Duration = Duration::from_secs(5);
// `roomUpdateInterval = defaultDelay` of the test file's `init()`
#[cfg(test)]
const ROOM_UPDATE_INTERVAL: Duration = Duration::from_millis(10);

// Not here: `ErrJobShutdownTimeout` and `AgentStore`, of the agents.

// `PingIntervalSeconds` and `PingTimeoutSeconds` of participant.go, which is not ported yet:
// these go when participant.rs has them.
const PING_INTERVAL_SECONDS: i32 = 5;
const PING_TIMEOUT_SECONDS: i32 = 15;

#[derive(Clone, Copy, Debug, Default)]
struct BroadcastOptions {
    skip_source: bool,
    immediate: bool,
}

struct DisconnectSignalOnResumeNoMessages {
    expiry: Time,
    closed_count: usize,
}

type ParticipantChangedCallback = Arc<dyn Fn(Arc<dyn types::Participant>) + Send + Sync>;
type RoomUpdatedCallback = Arc<dyn Fn() + Send + Sync>;
type CloseCallback = Arc<dyn Fn(types::RoomCloseReason) + Send + Sync>;

pub struct Room {
    // New: the room as its tasks, timers and listeners hold it.
    this: Weak<Room>,

    // atomics always need to be 64bit/8byte aligned
    // on 32bit arch only the beginning of the struct
    // starts at such a boundary.
    // time the first participant joined the room
    joined_at: atomic::Int64,
    // time that the last participant left the room
    left_at: atomic::Int64,
    holds: atomic::Int32,

    lock: RwLock<RoomInner>,

    // `protoRoom.Name` and `protoRoom.Sid`: see the header
    name: livekit::RoomName,
    id: livekit::RoomId,
    internal: Option<livekit::RoomInternal>,
    proto_proxy: OnceLock<Arc<putils::ProtoProxy<livekit::Room>>>,
    logger: Logger,

    #[allow(dead_code)] // not read upstream either
    config: WebRtcConfig,
    room_config: config::RoomConfig,
    audio_config: sfu::AudioConfig,
    server_info: livekit::ServerInfo,
    telemetry: Arc<dyn telemetry::TelemetryService>,
    // Not here: `egressLauncher`, egress is not ported.
    track_manager: RoomTrackManager,
    // Not here: `agentDispatches`, `agentClient` and `agentStore`, agents are not ported.
    buffer_factory: buffer::FactoryOfBufferFactory,

    // batch update participant info for non-publishers
    batched_updates: Mutex<HashMap<livekit::ParticipantIdentity, ParticipantUpdate>>,

    closed: CancellationToken,

    on_participant_changed: RwLock<Option<ParticipantChangedCallback>>,
    on_room_updated: RwLock<Option<RoomUpdatedCallback>>,
    on_close: RwLock<Option<CloseCallback>>,

    simulation_lock: Mutex<SimulationState>,

    user_packet_deduper: UserPacketDeduper,

    data_message_cache: putils::TimeSizeCache<types::DataMessageCache>,

    on_state_change_mu: Mutex<()>,
    local_participant_listener: Arc<LocalParticipantListener>,
    participant_telemetry_listener: Arc<ParticipantTelemetryListener>,
}

// What `lock sync.RWMutex` guards upstream.
struct RoomInner {
    proto_room: livekit::Room,

    // map of identity -> Participant
    participants: HashMap<livekit::ParticipantIdentity, Arc<dyn types::LocalParticipant>>,
    participant_opts: HashMap<livekit::ParticipantIdentity, Option<ParticipantOptions>>,
    participant_request_sources:
        HashMap<livekit::ParticipantIdentity, Option<Arc<dyn routing::MessageSource>>>,
    // Not here: `hasPublished` and `launchedTrackEgresses`, which only decide whether agents
    // and egress are launched, and `agentParticpants`.
    trailer: Vec<u8>,
}

// What `simulationLock sync.Mutex` guards upstream.
#[derive(Default)]
struct SimulationState {
    disconnect_signal_on_resume_participants: HashMap<livekit::ParticipantIdentity, Time>,
    disconnect_signal_on_resume_no_messages_participants:
        HashMap<livekit::ParticipantIdentity, DisconnectSignalOnResumeNoMessages>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ParticipantOptions {
    pub auto_subscribe: bool,
    pub auto_subscribe_data_track: bool,
}

// Not here: `agentDispatch`, `agentJob`, `newAgentDispatch`, `jobsLaunching`,
// `waitForPendingJobs`, `newAgentJob`, `participantLeft`, `waitForParticipantLeaving`: agents
// are not ported.

impl Room {
    // Not here: the parameters `agentClient`, `agentStore` and `egressLauncher`.
    pub fn new(
        room: &livekit::Room,
        internal: Option<&livekit::RoomInternal>,
        config: WebRtcConfig,
        room_config: config::RoomConfig,
        audio_config: &sfu::AudioConfig,
        server_info: &livekit::ServerInfo,
        telemetry: Arc<dyn telemetry::TelemetryService>,
    ) -> Arc<Room> {
        let mut proto_room = room.clone();
        let name = livekit::RoomName::from(&room.name);
        let id = livekit::RoomId::from(&room.sid);
        let logger = logger_with_room(
            &logger::get_logger().with_component(sutils::COMPONENT_ROOM),
            &name,
            &id,
        );

        if proto_room.empty_timeout == 0 {
            proto_room.empty_timeout = room_config.empty_timeout;
        }
        if proto_room.departure_timeout == 0 {
            proto_room.departure_timeout = room_config.departure_timeout;
        }
        if proto_room.creation_time == 0 {
            let now = time::now();
            proto_room.creation_time = now.unix_secs();
            proto_room.creation_time_ms = now.unix_milli();
        }

        let r = Arc::new_cyclic(|this: &Weak<Room>| Room {
            this: this.clone(),
            joined_at: atomic::Int64::new(0),
            left_at: atomic::Int64::new(0),
            holds: atomic::Int32::new(0),
            lock: RwLock::new(RoomInner {
                proto_room,
                participants: HashMap::new(),
                participant_opts: HashMap::new(),
                participant_request_sources: HashMap::new(),
                trailer: putils::random_secret().into_bytes(),
            }),
            name,
            id,
            internal: internal.cloned(),
            proto_proxy: OnceLock::new(),
            track_manager: RoomTrackManager::new(logger.clone()),
            logger,
            buffer_factory: buffer::FactoryOfBufferFactory::new(
                config.receiver.packet_buffer_size_video,
                config.receiver.packet_buffer_size_audio,
            ),
            config,
            room_config,
            audio_config: *audio_config,
            server_info: server_info.clone(),
            telemetry,
            batched_updates: Mutex::new(HashMap::new()),
            closed: CancellationToken::new(),
            on_participant_changed: RwLock::new(None),
            on_room_updated: RwLock::new(None),
            on_close: RwLock::new(None),
            simulation_lock: Mutex::new(SimulationState::default()),
            user_packet_deduper: UserPacketDeduper::new(),
            data_message_cache: putils::TimeSizeCache::new(putils::TimeSizeCacheParams {
                ttl: DATA_MESSAGE_CACHE_TTL,
                max_size: DATA_MESSAGE_CACHE_SIZE,
            }),
            on_state_change_mu: Mutex::new(()),
            local_participant_listener: Arc::new(LocalParticipantListener { room: this.clone() }),
            participant_telemetry_listener: Arc::new(ParticipantTelemetryListener {
                room: this.clone(),
            }),
        });

        let weak = Arc::downgrade(&r);
        let proto_proxy = putils::ProtoProxy::new(ROOM_UPDATE_INTERVAL, move || {
            weak.upgrade().map(|r| r.update_proto()).unwrap_or_default()
        });
        if r.proto_proxy.set(proto_proxy.clone()).is_err() {
            unreachable!("the proxy is set once");
        }

        // Not here: `createAgentDispatchesFromRoomAgent` and `launchRoomAgents`, agents are
        // not ported.

        time::spawn(Self::audio_update_worker(Arc::downgrade(&r)));
        time::spawn(Self::connection_quality_worker(Arc::downgrade(&r)));
        time::spawn(Self::change_update_worker(
            Arc::downgrade(&r),
            r.closed.clone(),
            proto_proxy,
        ));
        time::spawn(Self::simulation_cleanup_worker(Arc::downgrade(&r)));

        r
    }

    fn proto_proxy(&self) -> &Arc<putils::ProtoProxy<livekit::Room>> {
        self.proto_proxy
            .get()
            .expect("the proxy is set by Room::new")
    }

    pub fn logger(&self) -> Logger {
        self.logger.clone()
    }

    pub fn to_proto(&self) -> livekit::Room {
        self.proto_proxy().get()
    }

    pub fn name(&self) -> livekit::RoomName {
        self.name.clone()
    }

    pub fn id(&self) -> livekit::RoomId {
        self.id.clone()
    }

    pub fn trailer(&self) -> Vec<u8> {
        let r = self.lock.read();

        r.trailer.clone()
    }

    pub fn get_participant(
        &self,
        identity: &livekit::ParticipantIdentity,
    ) -> Option<Arc<dyn types::LocalParticipant>> {
        let r = self.lock.read();

        r.participants.get(identity).cloned()
    }

    pub fn get_participant_by_id(
        &self,
        participant_id: &livekit::ParticipantId,
    ) -> Option<Arc<dyn types::LocalParticipant>> {
        let r = self.lock.read();

        for p in r.participants.values() {
            if p.id() == *participant_id {
                return Some(p.clone());
            }
        }

        None
    }

    pub fn get_participants(&self) -> Vec<Arc<dyn types::LocalParticipant>> {
        let r = self.lock.read();

        r.participants.values().cloned().collect()
    }

    pub fn get_local_participants(&self) -> Vec<Arc<dyn types::LocalParticipant>> {
        self.get_participants()
    }

    pub fn get_participant_count(&self) -> usize {
        let r = self.lock.read();

        r.participants.len()
    }

    pub fn get_active_speakers(&self) -> Vec<livekit::SpeakerInfo> {
        let participants = self.get_participants();
        let mut speakers = Vec::with_capacity(participants.len());
        for p in &participants {
            let (level, active) = p.get_audio_level();
            if !active {
                continue;
            }
            speakers.push(livekit::SpeakerInfo {
                sid: p.id().as_str().to_string(),
                level: level as f32,
                active,
            });
        }

        speakers.sort_by(|a, b| sutils::signum(b.level - a.level).cmp(&0));

        // quantize to smooth out small changes
        for speaker in &mut speakers {
            speaker.level = (((speaker.level * AUDIO_LEVEL_QUANTIZATION as f32) as f64).ceil()
                * INV_AUDIO_LEVEL_QUANTIZATION) as f32;
        }

        speakers
    }

    pub fn get_buffer_factory(&self) -> Arc<buffer::Factory> {
        self.buffer_factory.create_buffer_factory()
    }

    pub fn first_joined_at(&self) -> i64 {
        self.joined_at.load()
    }

    pub fn last_left_at(&self) -> i64 {
        self.left_at.load()
    }

    pub fn internal(&self) -> Option<livekit::RoomInternal> {
        self.internal.clone()
    }

    pub fn hold(&self) -> bool {
        let _r = self.lock.write();

        if self.is_closed() {
            return false;
        }

        self.holds.inc();
        true
    }

    pub fn release(&self) {
        self.holds.dec();
    }

    pub fn join(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
        request_source: Option<Arc<dyn routing::MessageSource>>,
        opts: Option<ParticipantOptions>,
        ice_servers: &[livekit::IceServer],
    ) -> Result<(), Error> {
        let mut r = self.lock.write();

        if self.is_closed() {
            return Err(Error::RoomClosed);
        }

        if r.participants.contains_key(&participant.identity()) {
            return Err(Error::AlreadyJoined);
        }
        if r.proto_room.max_participants > 0 && !participant.is_dependent() {
            let mut num_participants = 0u32;
            for p in r.participants.values() {
                if !p.is_dependent() {
                    num_participants += 1;
                }
            }
            if num_participants >= r.proto_room.max_participants {
                return Err(Error::MaxParticipantsExceeded);
            }
        }

        if self.first_joined_at() == 0 && !participant.is_dependent() {
            self.joined_at.store(time::now().unix_secs());
        }

        // Not here: `launchTargetAgents` for a participant job, agents are not ported.

        debugw!(
            self.logger,
            "new participant joined",
            "participantID" => participant.id(),
            "participant" => participant.identity(),
            "clientInfo" => participant.get_client_info(),
            "options" => opts,
            "numParticipants" => r.participants.len(),
        );

        if participant.is_recorder() && !r.proto_room.active_recording {
            r.proto_room.active_recording = true;
            self.proto_proxy().mark_dirty(true);
        } else {
            self.proto_proxy().mark_dirty(false);
        }

        let identity = participant.identity();
        r.participants.insert(identity.clone(), participant.clone());
        r.participant_opts.insert(identity.clone(), opts);
        r.participant_request_sources
            .insert(identity, request_source);

        let on_participant_changed = self.on_participant_changed.read().clone();
        if let Some(on_participant_changed) = on_participant_changed {
            on_participant_changed(participant.clone());
        }

        {
            let room = self.this.clone();
            let participant = Arc::downgrade(&participant);
            time::after_func(time::MINUTE, move || {
                let (Some(room), Some(participant)) = (room.upgrade(), participant.upgrade())
                else {
                    return;
                };
                if !participant.verify() {
                    room.remove_participant(
                        &participant.identity(),
                        &participant.id(),
                        types::ParticipantCloseReason::JoinTimeout,
                    );
                }
            });
        }

        let join_response = self.create_join_response_locked(&r, &participant, ice_servers);
        if let Err(err) = participant.send_join_response(&join_response) {
            prometheus::record_service_operation_error("participant_join", "send_response");
            return Err(err);
        }

        participant.set_migrate_state(types::MigrateState::Complete);

        if participant.subscriber_as_primary() {
            // initiates sub connection as primary
            if participant.protocol_version().support_fast_start() {
                let room = self.this.clone();
                let participant = participant.clone();
                time::spawn(async move {
                    if let Some(room) = room.upgrade() {
                        room.subscribe_to_existing_tracks(&participant, false);
                    }
                    participant.negotiate(true);
                });
            } else {
                participant.negotiate(true);
            }
        } else if participant.is_using_single_peer_connection() {
            let room = self.this.clone();
            let participant = participant.clone();
            time::spawn(async move {
                if let Some(room) = room.upgrade() {
                    room.subscribe_to_existing_tracks(&participant, false);
                }
            });
        }

        prometheus::record_service_operation_success("participant_join");

        Ok(())
    }

    pub fn replace_participant_request_source(
        &self,
        identity: &livekit::ParticipantIdentity,
        req_source: Option<Arc<dyn routing::MessageSource>>,
    ) {
        let mut r = self.lock.write();
        if let Some(Some(rs)) = r.participant_request_sources.get(identity) {
            rs.close();
        }
        r.participant_request_sources
            .insert(identity.clone(), req_source);
    }

    pub fn get_participant_request_source(
        &self,
        identity: &livekit::ParticipantIdentity,
    ) -> Option<Arc<dyn routing::MessageSource>> {
        let r = self.lock.read();
        r.participant_request_sources
            .get(identity)
            .cloned()
            .flatten()
    }

    pub fn resume_participant(
        &self,
        p: Arc<dyn types::LocalParticipant>,
        request_source: Option<Arc<dyn routing::MessageSource>>,
        response_sink: Option<Arc<dyn routing::MessageSink>>,
        ice_config: Option<&livekit::IceConfig>,
        ice_servers: &[livekit::IceServer],
        reason: livekit::ReconnectReason,
    ) -> Result<(), Error> {
        self.replace_participant_request_source(&p.identity(), request_source);
        // close previous sink, and link to new one
        p.swap_response_sink(response_sink, types::SignallingCloseReason::Resume);

        p.set_signal_source_valid(true);

        // check for simulated signal disconnect on resume before sending any signal response messages
        {
            let mut s = self.simulation_lock.lock();
            let identity = p.identity();
            if let Some(state) = s
                .disconnect_signal_on_resume_no_messages_participants
                .get_mut(&identity)
            {
                // WARNING: this uses knowledge that service layer tries internally
                let mut simulated = false;
                if time::now().before(state.expiry) {
                    state.closed_count += 1;
                    p.close_signal_connection(
                        types::SignallingCloseReason::DisconnectOnResumeNoMessages,
                    );
                    simulated = true;
                }
                if state.closed_count == 3 {
                    s.disconnect_signal_on_resume_no_messages_participants
                        .remove(&identity);
                }
                if simulated {
                    return Ok(());
                }
            }
        }

        p.handle_reconnect_and_send_response(
            reason,
            &livekit::ReconnectResponse {
                ice_servers: ice_servers.to_vec(),
                client_configuration: p.get_client_configuration(),
                server_info: Some(self.server_info.clone()),
                last_message_seq: p.get_last_reliable_sequence(false),
            },
            &[],
        )?;

        // include the local participant's info as well, since metadata could have been changed
        let updates = get_other_participant_info(
            None,
            false,
            &to_participants(self.get_participants()),
            false,
        );
        p.send_participant_update(&updates)?;

        let _ = p.send_room_update(&self.to_proto());
        p.ice_restart(ice_config);

        // check for simulated signal disconnect on resume
        {
            let mut s = self.simulation_lock.lock();
            let identity = p.identity();
            if let Some(timeout) = s
                .disconnect_signal_on_resume_participants
                .get(&identity)
                .copied()
            {
                if time::now().before(timeout) {
                    p.close_signal_connection(types::SignallingCloseReason::DisconnectOnResume);
                }
                s.disconnect_signal_on_resume_participants.remove(&identity);
            }
        }

        Ok(())
    }

    pub fn handle_sync_state(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
        state: Option<&livekit::SyncState>,
    ) -> Result<(), Error> {
        if let Some(state) = state {
            return self.on_sync_state(participant, state);
        }

        Ok(())
    }

    fn on_sync_state(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
        state: &livekit::SyncState,
    ) -> Result<(), Error> {
        let p_logger = participant.get_logger();
        infow!(p_logger, "setting sync state", "state" => state);

        let mut should_reconnect = false;
        let pub_tracks = &state.publish_tracks;
        let existing_pub_tracks = participant.get_published_tracks();
        for pub_track in pub_tracks {
            // client may not have sent TrackInfo for each published track
            let Some(ti) = &pub_track.track else {
                warnw!(p_logger, "TrackInfo not sent during resume", nil);
                should_reconnect = true;
                break;
            };

            let ti_sid = livekit::TrackId::from(&ti.sid);
            let mut found = false;
            for existing_pub_track in &existing_pub_tracks {
                if existing_pub_track.id() == ti_sid {
                    found = true;
                    break;
                }
            }
            if !found {
                // is there a pending track?
                found = participant.get_pending_track(&ti_sid).is_some();
            }
            if !found {
                warnw!(p_logger, "unknown track during resume", nil, "trackID" => ti.sid);
                should_reconnect = true;
                break;
            }
        }

        let pub_data_tracks = &state.publish_data_tracks;
        let existing_pub_data_tracks = participant.get_published_data_tracks();
        for pub_data_track in pub_data_tracks {
            // client may not have sent DataTrackInfo for each published data track
            let Some(dti) = &pub_data_track.info else {
                warnw!(p_logger, "DataTrackInfo not sent during resume", nil);
                should_reconnect = true;
                break;
            };

            let dti_sid = livekit::TrackId::from(&dti.sid);
            let mut found = false;
            for existing_pub_data_track in &existing_pub_data_tracks {
                if existing_pub_data_track.id() == dti_sid {
                    found = true;
                    break;
                }
            }
            if !found {
                warnw!(p_logger, "unknown data track during resume", nil, "trackID" => dti.sid);
                should_reconnect = true;
                break;
            }
        }

        if should_reconnect {
            warnw!(
                p_logger,
                "unable to resume due to missing published tracks, starting full reconnect",
                nil
            );
            participant.issue_full_reconnect(types::ParticipantCloseReason::PublicationError);
            return Ok(());
        }

        // synthesize a track setting for each disabled track,
        // can be set before adding subscriptions,
        // in fact it is done before so that setting can be updated immediately upon subscription.
        for track_sid in &state.track_sids_disabled {
            participant.update_subscribed_track_settings(
                &livekit::TrackId::from(track_sid),
                &livekit::UpdateTrackSettings {
                    disabled: true,
                    ..Default::default()
                },
            );
        }

        let subscription = state.subscription.clone().unwrap_or_default();
        participant.handle_update_subscriptions(
            &livekit::strings_as_ids::<livekit::TrackId>(&subscription.track_sids),
            &subscription.participant_tracks,
            subscription.subscribe,
        );
        Ok(())
    }

    fn on_update_subscription_permission(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
        subscription_permission: &livekit::SubscriptionPermission,
    ) -> Result<(), Error> {
        let room = self.this.clone();
        participant.update_subscription_permission(
            Some(subscription_permission),
            putils::TimedVersion(0),
            Some(Box::new(move |participant_id: &livekit::ParticipantId| {
                room.upgrade()
                    .and_then(|room| room.get_participant_by_id(participant_id))
            })),
        )?;
        for track in participant.get_published_tracks() {
            self.track_manager.notify_track_changed(&track.id());
        }
        for track in participant.get_published_data_tracks() {
            self.track_manager.notify_track_changed(&track.id());
        }
        Ok(())
    }

    pub fn resolve_media_track_for_subscriber(
        &self,
        sub: Arc<dyn types::LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> types::MediaResolverResult {
        let mut res = types::MediaResolverResult::default();

        let info = self.track_manager.get_track_info(track_id);
        res.track_changed_notifier = Some(
            self.track_manager
                .get_or_create_track_change_notifier(track_id),
        );

        let Some(info) = info else {
            return res;
        };

        res.track = Some(info.track);
        res.track_removed_notifier = Some(
            self.track_manager
                .get_or_create_track_remove_notifier(track_id),
        );
        res.publisher_identity = info.publisher_identity;
        res.publisher_id = info.publisher_id;

        let publisher = self.get_participant_by_id(&res.publisher_id);
        // when publisher is not found, we will assume it doesn't have permission to access
        if let Some(publisher) = publisher {
            res.has_permission = is_participant_exempt_from_track_permissions_restrictions(&*sub)
                || publisher.has_permission(track_id, &sub.identity());
        }

        res
    }

    pub fn resolve_data_track_for_subscriber(
        &self,
        sub: Arc<dyn types::LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> types::DataResolverResult {
        let mut res = types::DataResolverResult::default();

        let info = self.track_manager.get_data_track_info(track_id);
        res.track_changed_notifier = Some(
            self.track_manager
                .get_or_create_track_change_notifier(track_id),
        );

        let Some(info) = info else {
            return res;
        };

        res.data_track = Some(info.data_track);
        res.track_removed_notifier = Some(
            self.track_manager
                .get_or_create_track_remove_notifier(track_id),
        );
        res.publisher_identity = info.publisher_identity;
        res.publisher_id = info.publisher_id;

        let publisher = self.get_participant_by_id(&res.publisher_id);
        // when publisher is not found, we will assume it doesn't have permission to access
        if let Some(publisher) = publisher {
            res.has_permission = is_participant_exempt_from_track_permissions_restrictions(&*sub)
                || publisher.has_permission(track_id, &sub.identity());
        }

        res
    }

    pub fn is_closed(&self) -> bool {
        self.closed.is_cancelled()
    }

    // CloseIfEmpty closes the room if all participants had left, or it's still empty past timeout
    pub fn close_if_empty(&self) {
        let r = self.lock.write();

        if self.is_closed() || self.holds.load() > 0 {
            return;
        }

        for p in r.participants.values() {
            if !p.is_dependent() {
                return;
            }
        }

        let timeout: u32;
        let elapsed: i64;
        let reason: &str;
        if self.first_joined_at() > 0 && self.last_left_at() > 0 {
            elapsed = time::now().unix_secs() - self.last_left_at();
            // need to give time in case participant is reconnecting
            timeout = r.proto_room.departure_timeout;
            reason = "departure timeout";
        } else {
            elapsed = time::now().unix_secs() - r.proto_room.creation_time;
            timeout = r.proto_room.empty_timeout;
            reason = "empty timeout";
        }
        drop(r);

        if elapsed >= timeout as i64 {
            self.close(types::RoomCloseReason::IdleTimeout);
            infow!(self.logger, "closing idle room", "reason" => reason);
        }
    }

    pub fn close(&self, reason: types::RoomCloseReason) {
        let r = self.lock.write();
        if self.closed.is_cancelled() {
            return;
        }
        self.closed.cancel();
        drop(r);

        infow!(self.logger, "closing room", "reason" => reason);
        let participant_close_reason = reason.to_participant_close_reason();
        for p in self.get_participants() {
            let _ = p.close(true, participant_close_reason, false);
        }

        self.proto_proxy().stop();

        let on_close = self.on_close.read().clone();
        if let Some(on_close) = on_close {
            on_close(reason);
        }
    }

    pub fn on_close(&self, f: impl Fn(types::RoomCloseReason) + Send + Sync + 'static) {
        *self.on_close.write() = Some(Arc::new(f));
    }

    pub fn on_participant_changed(
        &self,
        f: impl Fn(Arc<dyn types::Participant>) + Send + Sync + 'static,
    ) {
        *self.on_participant_changed.write() = Some(Arc::new(f));
    }

    pub fn send_data_packet(&self, dp: &livekit::DataPacket, kind: livekit::data_packet::Kind) {
        self.on_data_message(None, kind, dp);
    }

    pub fn set_metadata(&self, metadata: &str) -> CancellationToken {
        let mut r = self.lock.write();
        r.proto_room.metadata = metadata.to_string();
        drop(r);
        self.proto_proxy().mark_dirty(true)
    }

    fn send_room_update(&self) {
        let room_info = self.to_proto();
        // Send update to participants
        for p in self.get_participants() {
            if !p.is_ready() {
                continue;
            }

            if let Err(err) = p.send_room_update(&room_info) {
                warnw!(self.logger, "failed to send room update", err, "participant" => p.identity());
            }
        }
    }

    // Not here: `GetAgentDispatches`, `AddAgentDispatch` and `DeleteAgentDispatch`, agents are
    // not ported.

    pub fn on_room_updated(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.on_room_updated.write() = Some(Arc::new(f));
    }

    fn on_simulate_scenario(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
        simulate_scenario: &livekit::SimulateScenario,
    ) -> Result<(), Error> {
        use livekit::simulate_scenario::Scenario;

        match &simulate_scenario.scenario {
            Some(Scenario::SpeakerUpdate(speaker_update)) => {
                infow!(
                    self.logger,
                    "simulating speaker update",
                    "participant" => participant.identity(),
                    "duration" => speaker_update,
                );
                {
                    let room = self.this.clone();
                    let participant_id = participant.id();
                    let after = Duration::from_secs(*speaker_update as i64);
                    time::spawn(async move {
                        time::sleep(after).await;
                        if let Some(room) = room.upgrade() {
                            room.send_speaker_changes(&[livekit::SpeakerInfo {
                                sid: participant_id.as_str().to_string(),
                                active: false,
                                level: 0.0,
                            }]);
                        }
                    });
                }
                self.send_speaker_changes(&[livekit::SpeakerInfo {
                    sid: participant.id().as_str().to_string(),
                    active: true,
                    level: 0.9,
                }]);
            }
            Some(Scenario::Migration(_)) => {
                infow!(self.logger, "simulating migration", "participant" => participant.identity());
                // drop participant without necessarily cleaning up
                participant.close(
                    false,
                    types::ParticipantCloseReason::SimulateMigration,
                    true,
                )?;
            }
            Some(Scenario::NodeFailure(_)) => {
                infow!(self.logger, "simulating node failure", "participant" => participant.identity());
                // drop participant without necessarily cleaning up
                participant.close(
                    false,
                    types::ParticipantCloseReason::SimulateNodeFailure,
                    true,
                )?;
            }
            Some(Scenario::ServerLeave(_)) => {
                infow!(self.logger, "simulating server leave", "participant" => participant.identity());
                participant.close(
                    true,
                    types::ParticipantCloseReason::SimulateServerLeave,
                    false,
                )?;
            }
            Some(Scenario::SwitchCandidateProtocol(switch_candidate_protocol)) => {
                infow!(
                    self.logger,
                    "simulating switch candidate protocol",
                    "participant" => participant.identity(),
                );
                participant.ice_restart(Some(&livekit::IceConfig {
                    preference_subscriber: *switch_candidate_protocol,
                    preference_publisher: *switch_candidate_protocol,
                }));
            }
            Some(Scenario::SubscriberBandwidth(subscriber_bandwidth)) => {
                if *subscriber_bandwidth > 0 {
                    infow!(
                        self.logger,
                        "simulating subscriber bandwidth start",
                        "participant" => participant.identity(),
                        "bandwidth" => subscriber_bandwidth,
                    );
                } else {
                    infow!(
                        self.logger,
                        "simulating subscriber bandwidth end",
                        "participant" => participant.identity(),
                    );
                }
                participant.set_subscriber_channel_capacity(*subscriber_bandwidth);
            }
            Some(Scenario::DisconnectSignalOnResume(_)) => {
                infow!(
                    participant.get_logger(),
                    "simulating disconnect signal on resume"
                );
                let mut s = self.simulation_lock.lock();
                s.disconnect_signal_on_resume_participants.insert(
                    participant.identity(),
                    time::now().add(SIMULATE_DISCONNECT_SIGNAL_TIMEOUT),
                );
            }
            Some(Scenario::DisconnectSignalOnResumeNoMessages(_)) => {
                infow!(
                    participant.get_logger(),
                    "simulating disconnect signal on resume before sending any response messages"
                );
                let mut s = self.simulation_lock.lock();
                s.disconnect_signal_on_resume_no_messages_participants
                    .insert(
                        participant.identity(),
                        DisconnectSignalOnResumeNoMessages {
                            expiry: time::now().add(SIMULATE_DISCONNECT_SIGNAL_TIMEOUT),
                            closed_count: 0,
                        },
                    );
            }
            Some(Scenario::LeaveRequestFullReconnect(_)) | None => {}
        }
        Ok(())
    }

    fn create_join_response_locked(
        &self,
        r: &RoomInner,
        participant: &Arc<dyn types::LocalParticipant>,
        ice_servers: &[livekit::IceServer],
    ) -> livekit::JoinResponse {
        let ice_config = participant.get_ice_config();
        let has_ice_fallback = ice_config.as_ref().map_or(0, |c| c.preference_publisher)
            != livekit::IceCandidateType::IctNone as i32
            || ice_config.as_ref().map_or(0, |c| c.preference_subscriber)
                != livekit::IceCandidateType::IctNone as i32;
        livekit::JoinResponse {
            room: Some(self.to_proto()),
            participant: Some(participant.to_proto()),
            other_participants: get_other_participant_info(
                Some(&**participant),
                false, // isMigratingIn
                &to_participants(r.participants.values().cloned().collect()),
                false, // skipSubscriberBroadcast
            ),
            ice_servers: ice_servers.to_vec(),
            // indicates both server and client support subscriber as primary
            subscriber_primary: participant.subscriber_as_primary(),
            client_configuration: participant.get_client_configuration(),
            // sane defaults for ping interval & timeout
            ping_interval: PING_INTERVAL_SECONDS,
            ping_timeout: PING_TIMEOUT_SECONDS,
            server_info: Some(self.server_info.clone()),
            server_version: self.server_info.version.clone(),
            server_region: self.server_info.region.clone(),
            sif_trailer: r.trailer.clone(),
            enabled_publish_codecs: participant.get_enabled_publish_codecs(),
            fast_publish: participant.can_publish() && !has_ice_fallback,
            ..Default::default()
        }
    }

    // a ParticipantImpl in the room added a new track, subscribe other participants to it
    fn on_track_published(
        &self,
        participant: Arc<dyn types::Participant>,
        track: Arc<dyn types::MediaTrack>,
    ) {
        self.track_manager
            .add_track(track.clone(), &participant.identity(), &participant.id());

        // publish participant update, since track state is changed
        self.broadcast_participant_state(
            participant.clone(),
            BroadcastOptions {
                skip_source: true,
                ..Default::default()
            },
        );

        let r = self.lock.read();
        // subscribe all existing participants to this MediaTrack
        for existing_participant in r.participants.values() {
            if std::ptr::addr_eq(Arc::as_ptr(existing_participant), Arc::as_ptr(&participant)) {
                // skip publishing participant
                continue;
            }
            if existing_participant.state() != livekit::participant_info::State::Active {
                // not fully joined. don't subscribe yet
                continue;
            }
            if !r.auto_subscribe(&**existing_participant) {
                continue;
            }

            debugw!(
                existing_participant.get_logger(),
                "subscribing to new track",
                "publisher" => participant.identity(),
                "publisherID" => participant.id(),
                "trackID" => track.id(),
            );
            existing_participant.subscribe_to_track(&track.id(), false);
        }
        let on_participant_changed = self.on_participant_changed.read().clone();
        drop(r);

        if let Some(on_participant_changed) = on_participant_changed {
            on_participant_changed(participant);
        }

        // Not here: what follows upstream ("launch jobs") launches the agents of a publisher
        // and the participant's and the track's egress, once per participant and per track:
        // neither agents nor egress are ported.
    }

    fn on_track_updated(&self, p: Arc<dyn types::Participant>, _track: Arc<dyn types::MediaTrack>) {
        // send track updates to everyone, especially if track was updated by admin
        self.broadcast_participant_state(p.clone(), BroadcastOptions::default());
        let on_participant_changed = self.on_participant_changed.read().clone();
        if let Some(on_participant_changed) = on_participant_changed {
            on_participant_changed(p);
        }
    }

    fn on_track_unpublished(
        &self,
        p: Arc<dyn types::Participant>,
        track: Arc<dyn types::MediaTrack>,
    ) {
        self.track_manager.remove_track(track);
        if !p.is_closed() {
            self.broadcast_participant_state(
                p.clone(),
                BroadcastOptions {
                    skip_source: true,
                    ..Default::default()
                },
            );
        }
        let on_participant_changed = self.on_participant_changed.read().clone();
        if let Some(on_participant_changed) = on_participant_changed {
            on_participant_changed(p);
        }
    }

    fn on_data_track_published(
        &self,
        participant: Arc<dyn types::Participant>,
        dt: Arc<dyn types::DataTrack>,
    ) {
        self.track_manager
            .add_data_track(dt.clone(), &participant.identity(), &participant.id());

        // publish participant update, since a new data track was published
        self.broadcast_participant_state(
            participant.clone(),
            BroadcastOptions {
                skip_source: true,
                ..Default::default()
            },
        );

        let r = self.lock.read();
        // subscribe all existing participants to this DataTrack
        for existing_participant in r.participants.values() {
            if std::ptr::addr_eq(Arc::as_ptr(existing_participant), Arc::as_ptr(&participant)) {
                // skip publishing participant
                continue;
            }
            if existing_participant.state() != livekit::participant_info::State::Active {
                // not fully joined. don't subscribe yet
                continue;
            }
            if !r.auto_subscribe_data_track(&**existing_participant) {
                continue;
            }

            debugw!(
                existing_participant.get_logger(),
                "subscribing to new data track",
                "publisher" => participant.identity(),
                "publisherID" => participant.id(),
                "trackID" => dt.id(),
            );
            existing_participant.subscribe_to_data_track(&dt.id());
        }
        let on_participant_changed = self.on_participant_changed.read().clone();
        drop(r);

        if let Some(on_participant_changed) = on_participant_changed {
            on_participant_changed(participant);
        }
    }

    fn on_data_track_unpublished(
        &self,
        p: Arc<dyn types::Participant>,
        dt: Arc<dyn types::DataTrack>,
    ) {
        self.track_manager.remove_data_track(dt);
        if !p.is_closed() {
            self.broadcast_participant_state(
                p.clone(),
                BroadcastOptions {
                    skip_source: true,
                    ..Default::default()
                },
            );
        }
        let on_participant_changed = self.on_participant_changed.read().clone();
        if let Some(on_participant_changed) = on_participant_changed {
            on_participant_changed(p);
        }
    }

    fn on_participant_update(&self, p: Arc<dyn types::Participant>) {
        self.proto_proxy().mark_dirty(false);
        // immediately notify when permissions or metadata changed
        self.broadcast_participant_state(
            p.clone(),
            BroadcastOptions {
                immediate: true,
                ..Default::default()
            },
        );
        let on_participant_changed = self.on_participant_changed.read().clone();
        if let Some(on_participant_changed) = on_participant_changed {
            on_participant_changed(p);
        }
    }

    fn on_state_change(&self, p: Arc<dyn types::LocalParticipant>) {
        let on_participant_changed = self.on_participant_changed.read().clone();
        if let Some(on_participant_changed) = on_participant_changed {
            on_participant_changed(p.clone());
        }
        self.broadcast_participant_state(
            p.clone(),
            BroadcastOptions {
                skip_source: true,
                ..Default::default()
            },
        );

        let _on_state_change = self.on_state_change_mu.lock();

        match p.state() {
            livekit::participant_info::State::Active => {
                // subscribe participant to existing published tracks
                self.subscribe_to_existing_tracks(&p, false);

                let connect_time = time::since(p.connected_at());
                let mut meta = livekit::AnalyticsClientMeta {
                    client_connect_time: connect_time.milliseconds() as u32,
                    ..Default::default()
                };
                let infos = p.get_ice_connection_info();
                // Not here: the connection type for the reporter (`ReporterType()`), of
                // LiveKit's observability, which is not ported.
                for info in &infos {
                    if info.r#type != types::IceConnectionType::Unknown {
                        meta.connection_type = info.r#type.to_string();
                        break;
                    }
                }
                self.telemetry.participant_active(
                    &self.to_proto(),
                    &p.to_proto(),
                    &meta,
                    false,
                    p.is_warp_enabled(),
                    p.telemetry_guard(),
                );

                // Not here: `p.GetReporter().Tx(...)`, which reports the connect time, the
                // result and the connection type to LiveKit's observability.

                let mut fields = connection_details_fields(&infos);
                fields.push((
                    "clientInfo".to_string(),
                    Box::new(sutils::client_info_without_address(
                        p.get_client_info().as_deref(),
                    )),
                ));
                fields.push(("connectTime".to_string(), Box::new(connect_time)));
                infow_fields(&p.get_logger(), "participant active", &fields);
            }
            livekit::participant_info::State::Disconnected => {
                // remove participant from room
                let (identity, id, close_reason) = (p.identity(), p.id(), p.close_reason());
                let room = self.this.clone();
                time::spawn(async move {
                    if let Some(room) = room.upgrade() {
                        room.remove_participant(&identity, &id, close_reason);
                    }
                });
            }
            _ => {}
        }
    }

    fn on_data_message(
        &self,
        source: Option<Arc<dyn types::LocalParticipant>>,
        kind: livekit::data_packet::Kind,
        dp: &livekit::DataPacket,
    ) {
        if let Some(source) = &source
            && kind == livekit::data_packet::Kind::Reliable
            && dp.sequence > 0
        {
            // `proto.Marshal` cannot fail here: no "failed to marshal data packet for cache"
            let data = dp.encode_to_vec();
            let size = data.len();
            self.data_message_cache.add(
                Arc::new(types::DataMessageCache {
                    sender_id: source.id(),
                    seq: dp.sequence,
                    data: Bytes::from(data),
                    dest_identities: livekit::strings_as_ids(&dp.destination_identities),
                }),
                size,
            );
        }
        // a copy, which the broadcast changes: see the header
        let mut dp = dp.clone();
        broadcast_data_packet_for_room(self, source.as_deref(), kind, &mut dp, &self.logger);
    }

    fn on_data_message_unlabeled(&self, source: Arc<dyn types::LocalParticipant>, data: &[u8]) {
        broadcast_data_message_for_room(self, &*source, data, &self.logger);
    }

    fn on_metrics(&self, source: Arc<dyn types::Participant>, dp: &livekit::DataPacket) {
        broadcast_metrics_for_room(self, &*source, dp, &self.logger);
    }

    fn on_subscribe_status_changed(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
        publisher_id: &livekit::ParticipantId,
        subscribed: bool,
    ) {
        if subscribed {
            let publisher = self.get_participant_by_id(publisher_id);
            if let Some(publisher) = publisher
                && publisher.state() == livekit::participant_info::State::Active
            {
                // when a participant subscribes to another participant,
                // send speaker update if the subscribed to participant is active.
                let (level, active) = publisher.get_audio_level();
                if active {
                    let _ = participant.send_speaker_update(
                        &[livekit::SpeakerInfo {
                            sid: publisher.id().as_str().to_string(),
                            level: level as f32,
                            active,
                        }],
                        false,
                    );
                }

                // never nil here: see the header
                let cq = publisher.get_connection_quality();
                let update = livekit::ConnectionQualityUpdate { updates: vec![cq] };
                let _ = participant.send_connection_quality_update(&update);
            }
        } else {
            // no longer subscribed to the publisher, clear speaker status
            let _ = participant.send_speaker_update(
                &[livekit::SpeakerInfo {
                    sid: publisher_id.as_str().to_string(),
                    level: 0.0,
                    active: false,
                }],
                true,
            );
        }
    }

    fn on_update_subscriptions(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
        track_ids: &[livekit::TrackId],
        participant_tracks: &[livekit::ParticipantTracks],
        subscribe: bool,
    ) {
        self.update_subscriptions(participant, track_ids, participant_tracks, subscribe);
    }

    pub fn update_subscriptions(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
        track_ids: &[livekit::TrackId],
        participant_tracks: &[livekit::ParticipantTracks],
        subscribe: bool,
    ) {
        for track_id in track_ids {
            if subscribe {
                participant.subscribe_to_track(track_id, false);
            } else {
                participant.unsubscribe_from_track(track_id);
            }
        }

        for pt in participant_tracks {
            for track_id in livekit::strings_as_ids::<livekit::TrackId>(&pt.track_sids) {
                if subscribe {
                    participant.subscribe_to_track(&track_id, false);
                } else {
                    participant.unsubscribe_from_track(&track_id);
                }
            }
        }
    }

    fn on_update_data_subscriptions(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
        req: &livekit::UpdateDataSubscription,
    ) {
        for update in &req.updates {
            let track_id = livekit::TrackId::from(&update.track_sid);
            if update.subscribe {
                participant.subscribe_to_data_track(&track_id);
                participant
                    .update_data_track_subscription_options(&track_id, update.options.as_ref());
            } else {
                participant.unsubscribe_from_data_track(&track_id);
                participant.update_data_track_subscription_options(&track_id, None);
            }
        }
    }

    fn on_get_data_blob(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
        req: &livekit::GetDataBlobRequest,
    ) {
        let publisher = self.get_participant(&livekit::ParticipantIdentity::from(
            &req.participant_identity,
        ));
        participant.process_get_data_blob_request(
            req,
            publisher.map(|p| p as Arc<dyn types::Participant>),
        );
    }

    fn on_leave(&self, p: Arc<dyn types::LocalParticipant>, reason: types::ParticipantCloseReason) {
        self.remove_participant(&p.identity(), &p.id(), reason);
    }

    pub fn remove_participant(
        &self,
        identity: &livekit::ParticipantIdentity,
        p_id: &livekit::ParticipantId,
        reason: types::ParticipantCloseReason,
    ) {
        let mut r = self.lock.write();
        let Some(p) = r.participants.get(identity).cloned() else {
            return;
        };

        if !p_id.is_empty() && p.id() != *p_id {
            // participant session has been replaced
            return;
        }

        // Not here: the participant's agent job (`agentParticpants`), agents are not ported.

        r.participants.remove(identity);
        r.participant_opts.remove(identity);
        r.participant_request_sources.remove(identity);
        // Not here: `hasPublished`, `launchedTrackEgresses`, `agentParticpants`.
        // recorded while the participant is being removed, not after it is closed:
        // the room is already empty to CloseIfEmpty, which runs every second
        self.left_at.store(time::now().unix_secs());
        if !p.hidden() {
            r.proto_room.num_participants = r.proto_room.num_participants.wrapping_sub(1);
        }

        let mut immediate_change = false;
        if p.is_recorder() {
            let mut active_recording = false;
            for op in r.participants.values() {
                if op.is_recorder() {
                    active_recording = true;
                    break;
                }
            }

            if r.proto_room.active_recording != active_recording {
                r.proto_room.active_recording = active_recording;
                immediate_change = true;
            }
        }
        drop(r);
        self.proto_proxy().mark_dirty(immediate_change);

        if !p.has_connected() {
            let mut fields = connection_details_fields(&p.get_ice_connection_info());
            fields.push(("reason".to_string(), Box::new(reason.to_string())));
            fields.push((
                "clientInfo".to_string(),
                Box::new(sutils::client_info_without_address(
                    p.get_client_info().as_deref(),
                )),
            ));
            infow_fields(
                &p.get_logger(),
                "removing participant without connection",
                &fields,
            );
        }

        // send broadcast only if it's not already closed
        let send_updates = !p.is_disconnected();

        // remove all published tracks
        for t in p.get_published_tracks() {
            self.track_manager.remove_track(t);
        }

        // remove all published data tracks
        for t in p.get_published_data_tracks() {
            self.track_manager.remove_data_track(t);
        }

        // Not here: telling the agent's job that its participant left and terminating it.

        p.clear_participant_listener();

        // close participant as well
        let _ = p.close(true, reason, false);

        if send_updates {
            let on_participant_changed = self.on_participant_changed.read().clone();
            if let Some(on_participant_changed) = on_participant_changed {
                on_participant_changed(p.clone());
            }
            self.broadcast_participant_state(
                p,
                BroadcastOptions {
                    skip_source: true,
                    ..Default::default()
                },
            );
        }
    }

    fn subscribe_to_existing_tracks(&self, p: &Arc<dyn types::LocalParticipant>, is_sync: bool) {
        let r = self.lock.read();
        let auto_subscribe = r.auto_subscribe(&**p);
        let auto_subscribe_data_track = r.auto_subscribe_data_track(&**p);
        drop(r);

        let mut track_ids: Vec<livekit::TrackId> = Vec::new();
        for op in self.get_participants() {
            if p.id() == op.id() {
                // don't send to itself
                continue;
            }

            // subscribe to all
            if auto_subscribe {
                for track in op.get_published_tracks() {
                    track_ids.push(track.id());
                    p.subscribe_to_track(&track.id(), is_sync);
                }
            }

            if auto_subscribe_data_track {
                for track in op.get_published_data_tracks() {
                    track_ids.push(track.id());
                    p.subscribe_to_data_track(&track.id());
                }
            }
        }
        if !track_ids.is_empty() {
            debugw!(p.get_logger(), "subscribed participant to existing tracks", "trackID" => track_ids);
        }
    }

    // broadcast an update about participant p
    fn broadcast_participant_state(&self, p: Arc<dyn types::Participant>, opts: BroadcastOptions) {
        let pi = p.to_proto();

        // send it to the same participant immediately (`defer` upstream: after the block)
        let mut self_sent = false;

        'others: {
            if p.hidden() {
                // hidden participant updates are sent only to the hidden participant itself,
                // these could be things like metadata update
                break 'others;
            }

            let mut batched_updates = self.batched_updates.lock();
            let updates = push_and_dequeue_updates(
                pi.clone(),
                p.close_reason(),
                opts.immediate,
                self.get_participant(&livekit::ParticipantIdentity::from(&pi.identity))
                    .as_deref()
                    .map(|p| p as &dyn types::Participant),
                &mut batched_updates,
            );
            drop(batched_updates);
            if !updates.is_empty() {
                self_sent = true;
                send_participant_updates(
                    &updates,
                    &self.get_participants(),
                    self.room_config.update_batch_target_size as usize,
                );
            }
        }

        if !opts.skip_source
            && !self_sent
            && let Some(lp) = p.as_local_participant()
            && let Err(err) = lp.send_participant_update(&[pi])
        {
            errorw!(lp.get_logger(), "could not send update to participant", err);
        }
    }

    // for protocol 3, send only changed updates
    fn send_speaker_changes(&self, speakers: &[livekit::SpeakerInfo]) {
        for p in self.get_participants() {
            if p.protocol_version().supports_speaker_changed() {
                let _ = p.send_speaker_update(speakers, false);
            }
        }
    }

    fn update_proto(&self) -> livekit::Room {
        let r = self.lock.read();
        let mut room = r.proto_room.clone();
        drop(r);

        room.num_publishers = 0;
        room.num_participants = 0;
        for p in self.get_participants() {
            if !p.is_dependent() {
                room.num_participants += 1;
            }
            if p.is_publisher() {
                room.num_publishers += 1;
            }
        }

        room
    }

    async fn change_update_worker(
        r: Weak<Room>,
        closed: CancellationToken,
        proto_proxy: Arc<putils::ProtoProxy<livekit::Room>>,
    ) {
        let mut sub_ticker = new_ticker(SUBSCRIBER_UPDATE_INTERVAL);

        let mut clean_data_message_ticker = new_ticker(DATA_MESSAGE_CACHE_TTL);

        while !closed.is_cancelled() {
            tokio::select! {
                _ = closed.cancelled() => {
                    return;
                }
                _ = proto_proxy.updated().notified() => {
                    let Some(r) = r.upgrade() else {
                        return;
                    };
                    let on_room_updated = r.on_room_updated.read().clone();
                    if let Some(on_room_updated) = on_room_updated {
                        on_room_updated();
                    }
                    r.send_room_update();
                }
                _ = sub_ticker.tick() => {
                    let Some(r) = r.upgrade() else {
                        return;
                    };
                    let mut batched_updates = r.batched_updates.lock();
                    if batched_updates.is_empty() {
                        continue;
                    }
                    let updates_map = std::mem::take(&mut *batched_updates);
                    drop(batched_updates);

                    send_participant_updates(
                        &updates_map.into_values().collect::<Vec<_>>(),
                        &r.get_participants(),
                        r.room_config.update_batch_target_size as usize,
                    );
                }
                _ = clean_data_message_ticker.tick() => {
                    let Some(r) = r.upgrade() else {
                        return;
                    };
                    r.data_message_cache.prune();
                }
            }
        }
    }

    async fn audio_update_worker(r: Weak<Room>) {
        let mut last_active_map: HashMap<livekit::ParticipantId, livekit::SpeakerInfo> =
            HashMap::new();
        loop {
            let update_interval = {
                let Some(r) = r.upgrade() else {
                    return;
                };
                if r.is_closed() {
                    return;
                }

                let active_speakers = r.get_active_speakers();
                let mut changed_speakers = Vec::with_capacity(active_speakers.len());
                let mut next_active_map = HashMap::with_capacity(active_speakers.len());
                for speaker in active_speakers {
                    let sid = livekit::ParticipantId::from(&speaker.sid);
                    let prev = last_active_map.get(&sid);
                    if prev.is_none_or(|prev| prev.level != speaker.level) {
                        changed_speakers.push(speaker.clone());
                    }
                    next_active_map.insert(sid, speaker);
                }

                // changedSpeakers need to include previous speakers that are no longer speaking
                for (sid, speaker) in &last_active_map {
                    if !next_active_map.contains_key(sid) {
                        let mut inactive_speaker = speaker.clone();
                        inactive_speaker.level = 0.0;
                        inactive_speaker.active = false;
                        changed_speakers.push(inactive_speaker);
                    }
                }

                // see if an update is needed
                if !changed_speakers.is_empty() {
                    r.send_speaker_changes(&changed_speakers);
                }

                last_active_map = next_active_map;

                Duration::from_millis(r.audio_config.audio_level_config.update_interval as i64)
            };

            time::sleep(update_interval).await;
        }
    }

    async fn connection_quality_worker(r: Weak<Room>) {
        let mut ticker = new_ticker(connectionquality::UPDATE_INTERVAL);

        let mut prev_connection_infos: HashMap<
            livekit::ParticipantId,
            livekit::ConnectionQualityInfo,
        > = HashMap::new();
        // send updates to only users that are subscribed to each other
        loop {
            match r.upgrade() {
                Some(r) if !r.is_closed() => {}
                _ => return,
            }
            ticker.tick().await;
            let Some(r) = r.upgrade() else {
                return;
            };

            let participants = r.get_participants();
            let mut now_connection_infos = HashMap::with_capacity(participants.len());

            for p in &participants {
                if p.state() != livekit::participant_info::State::Active {
                    continue;
                }

                // never nil here: see the header
                let q = p.get_connection_quality();
                now_connection_infos.insert(p.id(), q);
            }

            // send an update if there is a change
            //   - new participant
            //   - quality change
            // NOTE: participant leaving is explicitly omitted as `leave` signal notifies that a participant is not in the room anymore
            let mut send_update = false;
            for p in &participants {
                let p_id = p.id();
                let prev_info = prev_connection_infos.get(&p_id);
                let Some(now_info) = now_connection_infos.get(&p_id) else {
                    // participant is not ACTIVE any more
                    continue;
                };
                if prev_info.is_none_or(|prev_info| now_info.quality != prev_info.quality) {
                    // new entrant OR change in quality
                    send_update = true;
                    break;
                }
            }

            if !send_update {
                prev_connection_infos = now_connection_infos;
                continue;
            }

            let maybe_add_to_update =
                |p_id: &livekit::ParticipantId, update: &mut livekit::ConnectionQualityUpdate| {
                    if let Some(now_info) = now_connection_infos.get(p_id) {
                        update.updates.push(now_info.clone());
                    }
                };

            for op in &participants {
                if !op.protocol_version().supports_connection_quality()
                    || op.state() != livekit::participant_info::State::Active
                {
                    continue;
                }
                let mut update = livekit::ConnectionQualityUpdate::default();

                // send to user itself
                maybe_add_to_update(&op.id(), &mut update);

                // add connection quality of other participants its subscribed to
                for sid in op.get_subscribed_participants() {
                    maybe_add_to_update(&sid, &mut update);
                }
                if update.updates.is_empty() {
                    // no change
                    continue;
                }
                if let Err(err) = op.send_connection_quality_update(&update) {
                    warnw!(
                        r.logger,
                        "could not send connection quality update", err,
                        "participant" => op.identity(),
                    );
                }
            }

            prev_connection_infos = now_connection_infos;
        }
    }

    async fn simulation_cleanup_worker(r: Weak<Room>) {
        loop {
            {
                let Some(r) = r.upgrade() else {
                    return;
                };
                if r.is_closed() {
                    return;
                }

                let now = time::now();
                let mut s = r.simulation_lock.lock();
                s.disconnect_signal_on_resume_participants
                    .retain(|_, timeout| !now.after(*timeout));

                s.disconnect_signal_on_resume_no_messages_participants
                    .retain(|_, state| !now.after(state.expiry));
            }

            time::sleep(Duration::from_secs(10)).await;
        }
    }

    // Not here: `launchRoomAgents`, `launchTargetAgents` and `handleNewJobs`, agents are not
    // ported.

    pub fn debug_info(&self) -> sfu::DebugInfo {
        let mut info = sfu::DebugInfo::new();
        info.insert("Name".to_string(), self.name.as_str().into());
        info.insert("Sid".to_string(), self.id.as_str().into());
        info.insert(
            "CreatedAt".to_string(),
            self.lock.read().proto_room.creation_time.into(),
        );

        let participants = self.get_participants();
        let mut participant_info = serde_json::Map::new();
        for p in &participants {
            participant_info.insert(
                p.identity().as_str().to_string(),
                serde_json::Value::Object(p.debug_info()),
            );
        }
        info.insert(
            "Participants".to_string(),
            serde_json::Value::Object(participant_info),
        );

        info
    }

    // Not here: `createAgentDispatch`, `createAgentDispatchFromRoomDispatch` and
    // `createAgentDispatchesFromRoomAgent`, agents are not ported.

    pub fn is_data_message_user_packet_duplicate(&self, up: &livekit::UserPacket) -> bool {
        self.user_packet_deduper.is_duplicate(up)
    }

    pub fn get_cached_reliable_data_message(
        &self,
        seqs: &HashMap<livekit::ParticipantId, u32>,
    ) -> Vec<Arc<types::DataMessageCache>> {
        let mut msgs = Vec::with_capacity(seqs.len() * 10);
        for msg in self.data_message_cache.get() {
            if let Some(seq) = seqs.get(&msg.sender_id)
                && msg.seq >= *seq
            {
                msgs.push(msg);
            }
        }
        msgs
    }

    pub fn local_participant_listener(&self) -> Arc<dyn types::LocalParticipantListener> {
        self.local_participant_listener.clone()
    }

    pub fn participant_telemetry_listener(&self) -> Arc<dyn types::ParticipantTelemetryListener> {
        self.participant_telemetry_listener.clone()
    }
}

impl RoomInner {
    // checks if participant should be auto subscribed to new tracks, assumes lock is already acquired
    fn auto_subscribe(&self, participant: &dyn types::LocalParticipant) -> bool {
        let opts = self.participant_opts.get(&participant.identity());
        // default to true if no options are set
        if let Some(Some(opts)) = opts
            && !opts.auto_subscribe
        {
            return false;
        }
        true
    }

    // checks if participant should be auto subscribed to new data tracks, assumes lock is already acquired
    fn auto_subscribe_data_track(&self, participant: &dyn types::LocalParticipant) -> bool {
        let opts = self.participant_opts.get(&participant.identity());
        // default to true if no options are set
        if let Some(Some(opts)) = opts
            && !opts.auto_subscribe_data_track
        {
            return false;
        }
        true
    }
}

// New, see the header.
impl Drop for Room {
    fn drop(&mut self) {
        self.closed.cancel();
    }
}

impl types::Room for Room {
    fn name(&self) -> livekit::RoomName {
        Room::name(self)
    }

    fn id(&self) -> livekit::RoomId {
        Room::id(self)
    }

    fn remove_participant(
        &self,
        identity: &livekit::ParticipantIdentity,
        p_id: &livekit::ParticipantId,
        reason: types::ParticipantCloseReason,
    ) {
        Room::remove_participant(self, identity, p_id, reason);
    }

    fn update_subscriptions(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
        track_ids: &[livekit::TrackId],
        participant_tracks: &[livekit::ParticipantTracks],
        subscribe: bool,
    ) {
        Room::update_subscriptions(self, participant, track_ids, participant_tracks, subscribe);
    }

    fn resolve_media_track_for_subscriber(
        &self,
        sub: Arc<dyn types::LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> types::MediaResolverResult {
        Room::resolve_media_track_for_subscriber(self, sub, track_id)
    }

    fn resolve_data_track_for_subscriber(
        &self,
        sub: Arc<dyn types::LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> types::DataResolverResult {
        Room::resolve_data_track_for_subscriber(self, sub, track_id)
    }

    fn get_local_participants(&self) -> Vec<Arc<dyn types::LocalParticipant>> {
        Room::get_local_participants(self)
    }

    fn is_data_message_user_packet_duplicate(&self, up: &livekit::UserPacket) -> bool {
        Room::is_data_message_user_packet_duplicate(self, up)
    }
}

// A Go ticker: the first tick after one period, ticks that were missed are skipped.
fn new_ticker(period: Duration) -> tokio::time::Interval {
    let period = period.to_std();
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker
}

// ------------------------------------------------------------

struct LocalParticipantListener {
    room: Weak<Room>,
}

impl types::ParticipantListener for LocalParticipantListener {
    fn on_participant_update(&self, p: Arc<dyn types::Participant>) {
        if let Some(room) = self.room.upgrade() {
            room.on_participant_update(p);
        }
    }

    fn on_track_published(
        &self,
        p: Arc<dyn types::Participant>,
        track: Arc<dyn types::MediaTrack>,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.on_track_published(p, track);
        }
    }

    fn on_track_updated(&self, p: Arc<dyn types::Participant>, track: Arc<dyn types::MediaTrack>) {
        if let Some(room) = self.room.upgrade() {
            room.on_track_updated(p, track);
        }
    }

    fn on_track_unpublished(
        &self,
        p: Arc<dyn types::Participant>,
        track: Arc<dyn types::MediaTrack>,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.on_track_unpublished(p, track);
        }
    }

    fn on_data_track_published(
        &self,
        p: Arc<dyn types::Participant>,
        track: Arc<dyn types::DataTrack>,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.on_data_track_published(p, track);
        }
    }

    fn on_data_track_unpublished(
        &self,
        p: Arc<dyn types::Participant>,
        track: Arc<dyn types::DataTrack>,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.on_data_track_unpublished(p, track);
        }
    }

    fn on_data_track_message(
        &self,
        _p: Arc<dyn types::Participant>,
        _data: &[u8],
        _packet: &crate::protocol::datatrack::Packet,
    ) {
    }

    fn on_metrics(&self, p: Arc<dyn types::Participant>, dp: &livekit::DataPacket) {
        if let Some(room) = self.room.upgrade() {
            room.on_metrics(p, dp);
        }
    }
}

impl types::LocalParticipantListener for LocalParticipantListener {
    fn on_state_change(&self, p: Arc<dyn types::LocalParticipant>) {
        if let Some(room) = self.room.upgrade() {
            room.on_state_change(p);
        }
    }

    fn on_subscriber_ready(&self, p: Arc<dyn types::LocalParticipant>) {
        if let Some(room) = self.room.upgrade() {
            room.subscribe_to_existing_tracks(&p, false);
        }
    }

    fn on_migrate_state_change(
        &self,
        _p: Arc<dyn types::LocalParticipant>,
        _migrate_state: types::MigrateState,
    ) {
    }

    fn on_data_message(
        &self,
        p: Arc<dyn types::LocalParticipant>,
        kind: livekit::data_packet::Kind,
        dp: &livekit::DataPacket,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.on_data_message(Some(p), kind, dp);
        }
    }

    fn on_data_message_unlabeled(&self, p: Arc<dyn types::LocalParticipant>, data: &[u8]) {
        if let Some(room) = self.room.upgrade() {
            room.on_data_message_unlabeled(p, data);
        }
    }

    fn on_subscribe_status_changed(
        &self,
        p: Arc<dyn types::LocalParticipant>,
        publisher_id: &livekit::ParticipantId,
        subscribed: bool,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.on_subscribe_status_changed(p, publisher_id, subscribed);
        }
    }

    fn on_update_subscriptions(
        &self,
        p: Arc<dyn types::LocalParticipant>,
        track_ids: &[livekit::TrackId],
        participant_tracks: &[livekit::ParticipantTracks],
        subscribe: bool,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.on_update_subscriptions(p, track_ids, participant_tracks, subscribe);
        }
    }

    fn on_update_subscription_permission(
        &self,
        p: Arc<dyn types::LocalParticipant>,
        subscription_permission: &livekit::SubscriptionPermission,
    ) -> Result<(), Error> {
        match self.room.upgrade() {
            Some(room) => room.on_update_subscription_permission(p, subscription_permission),
            None => Ok(()),
        }
    }

    fn on_update_data_subscriptions(
        &self,
        p: Arc<dyn types::LocalParticipant>,
        req: &livekit::UpdateDataSubscription,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.on_update_data_subscriptions(p, req);
        }
    }

    fn on_store_data_blob(
        &self,
        _p: Arc<dyn types::LocalParticipant>,
        _data_blob: &livekit::DataBlob,
    ) {
    }

    fn on_get_data_blob(
        &self,
        p: Arc<dyn types::LocalParticipant>,
        req: &livekit::GetDataBlobRequest,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.on_get_data_blob(p, req);
        }
    }

    fn on_sync_state(
        &self,
        p: Arc<dyn types::LocalParticipant>,
        state: &livekit::SyncState,
    ) -> Result<(), Error> {
        match self.room.upgrade() {
            Some(room) => room.on_sync_state(p, state),
            None => Ok(()),
        }
    }

    fn on_simulate_scenario(
        &self,
        p: Arc<dyn types::LocalParticipant>,
        simulate_scenario: &livekit::SimulateScenario,
    ) -> Result<(), Error> {
        match self.room.upgrade() {
            Some(room) => room.on_simulate_scenario(p, simulate_scenario),
            None => Ok(()),
        }
    }

    fn on_leave(
        &self,
        p: Arc<dyn types::LocalParticipant>,
        close_reason: types::ParticipantCloseReason,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.on_leave(p, close_reason);
        }
    }
}

// ------------------------------------------------------------

struct ParticipantTelemetryListener {
    room: Weak<Room>,
}

impl ParticipantTelemetryListener {
    fn event_room(&self, room: &Room) -> livekit::Room {
        if room.room_config.enable_full_room_in_webhooks {
            return room.to_proto();
        }

        livekit::Room {
            sid: room.id().as_str().to_string(),
            name: room.name().as_str().to_string(),
            ..Default::default()
        }
    }
}

impl types::ParticipantTelemetryListener for ParticipantTelemetryListener {
    fn on_track_publish_requested(
        &self,
        p_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        ti: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry.track_publish_requested(
                &self.event_room(&room),
                p_id,
                identity,
                ti,
                should_send_event,
            );
        }
    }

    fn on_track_published(
        &self,
        p_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        ti: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry.track_published(
                &self.event_room(&room),
                p_id,
                identity,
                ti,
                should_send_event,
            );
        }
    }

    fn on_track_unpublished(
        &self,
        p_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        ti: &livekit::TrackInfo,
        was_published_locally: bool,
        should_send_event: bool,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry.track_unpublished(
                &self.event_room(&room),
                p_id,
                identity,
                ti,
                was_published_locally,
                should_send_event,
            );
        }
    }

    fn on_track_subscribe_requested(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry
                .track_subscribe_requested(&self.event_room(&room), p_id, ti);
        }
    }

    fn on_track_subscribed(
        &self,
        p_id: &livekit::ParticipantId,
        ti: &livekit::TrackInfo,
        publisher_info: &livekit::ParticipantInfo,
        should_send_event: bool,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry.track_subscribed(
                &self.event_room(&room),
                p_id,
                ti,
                publisher_info,
                should_send_event,
            );
        }
    }

    fn on_track_unsubscribed(
        &self,
        p_id: &livekit::ParticipantId,
        ti: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry
                .track_unsubscribed(&self.event_room(&room), p_id, ti, should_send_event);
        }
    }

    fn on_track_subscribe_failed(
        &self,
        p_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        err: &Error,
        is_user_error: bool,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry.track_subscribe_failed(
                &self.event_room(&room),
                p_id,
                track_id,
                &err.to_string(),
                is_user_error,
            );
        }
    }

    fn on_track_subscribe_stream_started(
        &self,
        _p_id: &livekit::ParticipantId,
        _ti: &livekit::TrackInfo,
        _elapsed: Duration,
    ) {
    }

    fn on_track_muted(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry
                .track_muted(&self.event_room(&room), p_id, ti);
        }
    }

    fn on_track_unmuted(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry
                .track_unmuted(&self.event_room(&room), p_id, ti);
        }
    }

    fn on_track_published_update(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry
                .track_published_update(&self.event_room(&room), p_id, ti);
        }
    }

    fn on_track_max_subscribed_video_quality(
        &self,
        p_id: &livekit::ParticipantId,
        ti: &livekit::TrackInfo,
        mime: MimeType,
        max_quality: livekit::VideoQuality,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry.track_max_subscribed_video_quality(
                &self.event_room(&room),
                p_id,
                ti,
                mime,
                max_quality,
            );
        }
    }

    fn on_track_publish_rtp_stats(
        &self,
        p_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        layer: i32,
        stats: &livekit::RtpStats,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry.track_publish_rtp_stats(
                &self.event_room(&room),
                p_id,
                track_id,
                mime_type,
                layer,
                stats,
            );
        }
    }

    fn on_track_subscribe_rtp_stats(
        &self,
        p_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        stats: &livekit::RtpStats,
    ) {
        if let Some(room) = self.room.upgrade() {
            room.telemetry.track_subscribe_rtp_stats(
                &self.event_room(&room),
                p_id,
                track_id,
                mime_type,
                stats,
            );
        }
    }

    fn on_track_stats(&self, key: &telemetry::StatsKey, stat: &livekit::AnalyticsStat) {
        if let Some(room) = self.room.upgrade() {
            let (room_id, room_name) = (room.id(), room.name());
            // upstream sets the two fields on the stat it is handed
            let mut stat = stat.clone();
            (stat.room_id, stat.room_name) =
                (room_id.as_str().to_string(), room_name.as_str().to_string());
            room.telemetry.track_stats(&room_id, &room_name, key, &stat);
        }
    }
}

// ------------------------------------------------------------

pub fn broadcast_data_packet_for_room(
    r: &dyn types::Room,
    source: Option<&dyn types::LocalParticipant>,
    kind: livekit::data_packet::Kind,
    dp: &mut livekit::DataPacket,
    logger: &Logger,
) {
    dp.kind = kind as i32; // backward compatibility
    let dest = match &dp.value {
        Some(livekit::data_packet::Value::User(u)) => u.destination_sids.clone(),
        _ => Vec::new(),
    };
    if let Some(livekit::data_packet::Value::User(u)) = &mut dp.value {
        if r.is_data_message_user_packet_duplicate(u) {
            infow!(logger, "dropping duplicate data message", "nonce" => u.nonce);
            return;
        }
        if dp.destination_identities.is_empty() {
            dp.destination_identities = u.destination_identities.clone();
        } else {
            u.destination_identities = dp.destination_identities.clone();
        }
        if !dp.participant_identity.is_empty() {
            u.participant_identity = dp.participant_identity.clone();
        } else {
            dp.participant_identity = u.participant_identity.clone();
        }
    }
    let dest_identities = &dp.destination_identities;

    let participants = r.get_local_participants();
    let mut capacity = dest_identities.len();
    if capacity == 0 {
        capacity = dest.len();
    }
    if capacity == 0 {
        capacity = participants.len();
    }
    let mut dest_participants: Vec<Arc<dyn types::LocalParticipant>> = Vec::with_capacity(capacity);

    let mut dp_data: Option<Vec<u8>> = None;
    for op in &participants {
        if let Some(source) = source
            && op.id() == source.id()
        {
            continue;
        }
        if (!dest.is_empty() || !dest_identities.is_empty())
            && !dest.iter().any(|d| *d == op.id().as_str())
            && !dest_identities.iter().any(|d| *d == op.identity().as_str())
        {
            continue;
        }
        if dp_data.is_none() {
            // `proto.Marshal` cannot fail here: no "failed to marshal data packet"
            dp_data = Some(dp.encode_to_vec());
        }
        dest_participants.push(op.clone());
    }

    let dp_data = dp_data.unwrap_or_default();
    let sender_id = livekit::ParticipantId::from(&dp.participant_sid);
    putils::parallel_exec(
        &dest_participants,
        DATA_FORWARD_LOAD_BALANCE_THRESHOLD,
        1,
        |op| {
            let _ = op.send_data_message(kind, &dp_data, &sender_id, dp.sequence);
        },
    );
}

pub fn broadcast_data_message_for_room(
    r: &dyn types::Room,
    source: &dyn types::LocalParticipant,
    data: &[u8],
    _logger: &Logger,
) {
    putils::parallel_exec(
        &r.get_local_participants(),
        DATA_FORWARD_LOAD_BALANCE_THRESHOLD,
        1,
        |op| {
            if op.id() == source.id() {
                return;
            }

            let _ = op.send_data_message_unlabeled(data, false, &source.identity());
        },
    );
}

pub fn broadcast_metrics_for_room(
    r: &dyn types::Room,
    source: &dyn types::Participant,
    dp: &livekit::DataPacket,
    _logger: &Logger,
) {
    if let Some(livekit::data_packet::Value::Metrics(metrics)) = &dp.value {
        putils::parallel_exec(
            &r.get_local_participants(),
            DATA_FORWARD_LOAD_BALANCE_THRESHOLD,
            1,
            |op| {
                // echoing back to sender too
                let _ = op.handle_metrics(&source.id(), metrics);
            },
        );
    }
}

pub fn is_close_notify_skippable(close_reason: types::ParticipantCloseReason) -> bool {
    close_reason == types::ParticipantCloseReason::DuplicateIdentity
}

pub fn is_participant_exempt_from_track_permissions_restrictions(
    p: &dyn types::LocalParticipant,
) -> bool {
    // egress/recorder participants bypass permissions as auto-egress does not
    // have enough context to check permissions
    p.is_recorder()
}

pub fn compare_participant(pi1: &livekit::ParticipantInfo, pi2: &livekit::ParticipantInfo) -> i64 {
    if pi1.joined_at != pi2.joined_at {
        if pi1.joined_at < pi2.joined_at {
            return -1;
        } else {
            return 1;
        }
    }

    if pi1.joined_at_ms != 0 && pi2.joined_at_ms != 0 && pi1.joined_at_ms != pi2.joined_at_ms {
        if pi1.joined_at_ms < pi2.joined_at_ms {
            return -1;
        } else {
            return 1;
        }
    }

    // all join times being equal, it is not possible to really know which one is newer,
    // pick the higher pID to be consistent
    if pi1.sid != pi2.sid {
        if pi1.sid < pi2.sid {
            return -1;
        } else {
            return 1;
        }
    }

    0
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ParticipantUpdate {
    pub participant_info: livekit::ParticipantInfo,
    pub is_synthesized_disconnect: bool,
    pub close_reason: types::ParticipantCloseReason,
}

// push a participant update for batched broadcast, optionally returning immediate updates to broadcast.
// it handles the following scenarios
// * subscriber-only updates will be queued for batch updates
// * publisher & immediate updates will be returned without queuing
// * when the SID changes, it will return both updates, with the earlier participant set to disconnected
pub fn push_and_dequeue_updates(
    pi: livekit::ParticipantInfo,
    close_reason: types::ParticipantCloseReason,
    is_immediate: bool,
    existing_participant: Option<&dyn types::Participant>,
    cache: &mut HashMap<livekit::ParticipantIdentity, ParticipantUpdate>,
) -> Vec<ParticipantUpdate> {
    let mut updates = Vec::new();
    let identity = livekit::ParticipantIdentity::from(&pi.identity);
    let mut should_send = is_immediate || pi.is_publisher;

    if let Some(existing) = cache.get(&identity) {
        if pi.sid == existing.participant_info.sid {
            // same participant session
            if pi.version < existing.participant_info.version {
                // out of order update
                return Vec::new();
            }
        } else {
            // different participant sessions
            if compare_participant(&existing.participant_info, &pi) < 0 {
                // existing is older, synthesize a DISCONNECT for older and
                // send immediately along with newer session to signal switch
                should_send = true;
                // taken out of the cache, as the update of the newer session that is sent now
                // takes it out below
                let mut existing = cache.remove(&identity).expect("the update is in the cache");
                existing.participant_info.state =
                    livekit::participant_info::State::Disconnected as i32;
                existing.is_synthesized_disconnect = true;
                updates.push(existing);
            } else {
                // older session update, newer session has already become active, so nothing to do
                return Vec::new();
            }
        }
    } else if let Some(existing_participant) = existing_participant {
        let epi = existing_participant.to_proto();
        if compare_participant(&epi, &pi) > 0 {
            // older session update, newer session has already become active, so nothing to do
            return Vec::new();
        }
    }

    if should_send {
        // include any queued update, and return
        cache.remove(&identity);
        updates.push(ParticipantUpdate {
            participant_info: pi,
            close_reason,
            ..Default::default()
        });
    } else {
        // enqueue for batch
        cache.insert(
            identity,
            ParticipantUpdate {
                participant_info: pi,
                close_reason,
                ..Default::default()
            },
        );
    }

    updates
}

pub fn send_participant_updates(
    updates: &[ParticipantUpdate],
    participants: &[Arc<dyn types::LocalParticipant>],
    batch_target_size: usize,
) {
    if updates.is_empty() {
        return;
    }

    // For filtered updates, skip
    // 1. synthesized DISCONNECT - this happens on SID change
    // 2. close reasons of DUPLICATE_IDENTITY/STALE  - A newer session for that identity exists.
    //
    // Filtered updates are used with clients that can handle identity based reconnect and hence those
    // conditions can be skipped.
    let mut filtered_updates: Vec<livekit::ParticipantInfo> = Vec::new();
    for update in updates {
        if update.is_synthesized_disconnect || is_close_notify_skippable(update.close_reason) {
            continue;
        }
        filtered_updates.push(update.participant_info.clone());
    }

    let mut full_updates: Vec<livekit::ParticipantInfo> = Vec::new();
    for update in updates {
        full_updates.push(update.participant_info.clone());
    }

    let filtered_update_chunks = chunk_proto_batch(&filtered_updates, batch_target_size);
    let full_update_chunks = chunk_proto_batch(&full_updates, batch_target_size);

    for op in participants {
        let mut update_chunks = &full_update_chunks;
        if op.protocol_version().supports_identity_based_reconnection() {
            update_chunks = &filtered_update_chunks;
        }
        for chunk in update_chunks {
            if let Err(err) = op.send_participant_update(chunk) {
                errorw!(op.get_logger(), "could not send update to participant", err);
                break;
            }
        }
    }
}

// GetOtherParticipantInfo returns ParticipantInfo for everyone in the room except for the participant identified by lp.Identity()
pub fn get_other_participant_info(
    lp: Option<&dyn types::LocalParticipant>,
    is_migrating_in: bool,
    all_participants: &[Arc<dyn types::Participant>],
    skip_subscriber_broadcast: bool,
) -> Vec<livekit::ParticipantInfo> {
    let mut lp_identity = livekit::ParticipantIdentity::default();
    if let Some(lp) = lp {
        lp_identity = lp.identity();
    }

    let mut p_infos = Vec::with_capacity(all_participants.len());
    for op in all_participants {
        if (!skip_subscriber_broadcast || !op.can_skip_broadcast())
            && !op.hidden()
            && op.identity() != lp_identity
            && !is_migrating_in
        {
            p_infos.push(op.to_proto());
        }
    }

    p_infos
}

// The fields of a log line: pairs of key and value, where upstream has `[]any`.
pub(crate) type LogFields = Vec<(String, Box<dyn fmt::Debug>)>;

pub(crate) fn connection_details_fields(infos: &[types::IceConnectionInfo]) -> LogFields {
    let mut fields: LogFields = Vec::new();
    let mut connection_type = types::IceConnectionType::Unknown;
    for info in infos {
        let mut candidates: Vec<String> = Vec::with_capacity(info.remote.len() + info.local.len());
        for c in &info.local {
            let mut c_str = "[local]".to_string();
            if c.selected_order != 0 {
                c_str += &format!("[selected:{}]", c.selected_order);
            } else if c.filtered {
                c_str += "[filtered]";
            }
            if c.trickle {
                c_str += "[trickle]";
            }
            c_str += &format!(" {}", c.candidate);
            candidates.push(c_str);
        }
        for c in &info.remote {
            let mut c_str = "[remote]".to_string();
            if c.selected_order != 0 {
                c_str += &format!("[selected:{}]", c.selected_order);
            } else if c.filtered {
                c_str += "[filtered]";
            }
            if c.trickle {
                c_str += "[trickle]";
            }
            c_str += &format!(
                " {} {} {}:{}",
                c.candidate.protocol,
                c.candidate.typ,
                maybe_truncate_ip(&c.candidate.address),
                c.candidate.port
            );
            let related_address = &c.candidate.related_address;
            if !related_address.is_empty() {
                let related_addr = maybe_truncate_ip(related_address);
                if !related_addr.is_empty() {
                    c_str += &format!("  related {}:{}", related_addr, c.candidate.related_port);
                }
            }
            candidates.push(c_str);
        }
        if !candidates.is_empty() {
            fields.push((
                format!("{}Candidates", info.transport.as_str_name().to_lowercase()),
                Box::new(candidates),
            ));
        }
        if info.r#type != types::IceConnectionType::Unknown {
            connection_type = info.r#type;
        }
    }
    fields.push((
        "connectionType".to_string(),
        Box::new(connection_type.to_string()),
    ));
    fields
}

// `logger.Infow(msg, fields...)` with the fields of `connectionDetailsFields` and more.
fn infow_fields(logger: &Logger, msg: &str, fields: &LogFields) {
    let kvs: Vec<(&str, &dyn fmt::Debug)> = fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_ref()))
        .collect();
    logger.infow(msg, &kvs);
}

fn to_participants(lps: Vec<Arc<dyn types::LocalParticipant>>) -> Vec<Arc<dyn types::Participant>> {
    let mut participants: Vec<Arc<dyn types::Participant>> = Vec::with_capacity(lps.len());
    for lp in lps {
        participants.push(lp);
    }
    participants
}

#[cfg(test)]
#[path = "room_test.rs"]
mod room_test;
