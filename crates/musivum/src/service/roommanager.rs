// Ported from LiveKit, pkg/service/roommanager.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for one node and without psrpc. The context parameters
// are dropped.
// - Making it. `NewLocalRoomManager` is `RoomManager::new_local` and gives an `Arc`; the room
//   manager keeps a `Weak` of itself (`this`) for the tasks it starts. It registers itself on
//   the bus as the node's room manager server (`RegisterAllNodeTopics`, see `routing/bus.rs`),
//   which `Stop` undoes (`Kill`). It takes the WebRTC node its participants' connections are
//   on (`node`, new: made once per process by `rtcconfig::WebRtcConfig::new_node`), and closes
//   it in `Stop` where the original closes the UDP mux and the TCP listener of its WebRTC
//   configuration. It takes that WebRTC configuration (`rtc_config`, as an `Arc`) where the
//   original makes it (`rtc.NewWebRTCConfig(conf)`): the server makes it once, for the node's
//   sockets and the room manager both. It does not take `agentClient`, `agentStore`,
//   `egressLauncher` (agents and egress are not ported). Its `turnAuthHandler` is an `Option`
//   (the original's tests pass nil; one that is used is there).
//   `version.Version` and `agent.CurrentProtocol`, of packages that are not ported, are the
//   constants `VERSION` and `AGENT_CURRENT_PROTOCOL` here, with the original's values.
// - The servers of psrpc that the original makes per room and per participant
//   (`roomServers`, `participantServers`: the room API's `rpc.RoomServer` and
//   `rpc.ParticipantServer`) are the room manager itself, registered on the bus under the
//   room's and the participant's topic (`routing::RoomServerImpl`,
//   `routing::ParticipantServerImpl`, implemented at the end of this file). The bus replaces
//   a server on the same topic as `MultitonService.Replace` does; the two maps keep what was
//   registered, so that `Stop` can kill them all. Not here at all: the WHIP servers (`whipServer`,
//   `whipParticipantServers`, `httpSignalParticipantServers`; WHIP is not ported), the agents'
//   (`agentDispatchServers`, `ListDispatch`, `CreateDispatch`, `DeleteDispatch`), the start
//   of a room composite egress at the end of `getOrCreateRoom`, the session reporter and the
//   session timer of LiveKit's observability, and `MetricConfig` (`pkg/metric`).
// - The room manager holds the rooms. The callbacks a room holds (`OnClose`,
//   `OnRoomUpdated`, `OnParticipantChanged`) hold the room and the room manager weakly, and
//   so do the callbacks a participant holds (its `OnClose` uses the participant it is
//   handed, the same one the original uses by name) and the participant's helper
//   (`roomManagerParticipantHelper`); one that comes when the room is gone does what needs no
//   room (see each). `rtcSessionWorker` is a task that holds the room manager, the room, the
//   participant and the request source until it ends, as the goroutine does.
// - `deleteRoom`, whose name in Rust's spelling is `DeleteRoom`'s, is `delete_room_state`. It
//   clears the routing information and then deletes the room from the store,
//   where the original does both at once on two goroutines and waits for both; the error is
//   the same (the store's if both fail).
// - `StartSession` takes the request source and the response sink as `Arc`s, never nil
//   (upstream's test passes nil without an identity, where neither is used).
//   `pi.Client.Protocol` and `pi.CreateRoom.Name`, which the original reads without checking
//   for nil, are `expect`ed with the message of a nil dereference; `pi.Client.GetSdk()` and the
//   other getters give the zero value of a client that is not there.
// - `PerformRpc` and `UpdateRoomMetadata` wait as the original does, blocking the caller's
//   thread (`block_on`, which tells a multi-threaded runtime so); the channels of `PerformRpc`
//   are tokio's (see the header of `rtc/participant.rs`). When the participant drops both
//   without an answer, the wait does not end, as Go's `select` on the two channels does not.
// - `rtc.Recover` in `rtcSessionWorker` is `rtc::recover_async` around the worker's loop.
// - `participantReq` is the trait `ParticipantReq`, implemented for the four requests the
//   original passes as one.
// - New, a deliberate difference: `set_advertised_turn_tls_port`. The `turns:` URL of the ICE
//   servers names port 443 whatever `turn.tls_port` is, as upstream (which expects a load
//   balancer on 443), unless a server that builds musivum in names another port: it has no
//   proxy in front of it to rewrite the URL (`musivum_server::embed`).
// - New: `get_rooms`, every room of the node, for a server that builds musivum in and sends
//   the participants back for a full reconnect when it restarts (`musivum_server::embed`).

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Weak};

use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use parking_lot::{Mutex, RwLock};
use sha1::Sha1;
use tokio::sync::mpsc;

use super::{Error, ObjectStore, RoomAllocator, TurnAuthHandler};
use crate::clientconfiguration::{
    ClientConfigurationManager, STATIC_CONFIGURATIONS, StaticClientConfigurationManager,
};
use crate::config::{self, Config};
use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::mediatransportutil::rtcconfig;
use crate::protocol::auth::AccessToken;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::protocol::utils::{self as putils, TimedVersionGenerator, guid};
use crate::routing::{self, LocalNode, MessageBus, MessageSink, MessageSource, Router};
use crate::rtc::{self, Room, types};
use crate::sfu;
use crate::telemetry::{self, prometheus};
use crate::utils as sutils;
use crate::webrtc::peerconnection::Node;
use crate::{debugw, errorw, infow, warnw};

const TOKEN_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);
const TOKEN_DEFAULT_TTL: Duration = Duration::from_secs(10 * 60);

// `version.Version` of the server the original is built as
const VERSION: &str = "1.13.11";
// `agent.CurrentProtocol` (pkg/agent is not ported)
const AGENT_CURRENT_PROTOCOL: i32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct IceConfigCacheKey {
    room_name: livekit::RoomName,
    participant_identity: livekit::ParticipantIdentity,
}

// RoomManager manages rooms and its interaction with participants.
// It's responsible for creating, deleting rooms, as well as running sessions for participants
pub struct RoomManager {
    // New: the room manager as its tasks hold it.
    this: Weak<RoomManager>,

    // what `lock sync.RWMutex` guards: `rooms`
    lock: RwLock<HashMap<livekit::RoomName, Arc<Room>>>,

    config: Arc<Config>,
    rtc_config: Arc<rtc::WebRtcConfig>,
    server_info: livekit::ServerInfo,
    current_node: Arc<dyn LocalNode>,
    router: Arc<dyn Router>,
    room_allocator: Arc<dyn RoomAllocator>,
    // `roomManagerServer`: the room manager itself, registered on `bus` (see the header)
    // Not here: `whipServer`, WHIP is not ported.
    room_store: Arc<dyn ObjectStore>,
    telemetry: Arc<dyn telemetry::TelemetryService>,
    client_conf_manager: Box<dyn ClientConfigurationManager>,
    // Not here: `agentClient`, `agentStore` and `egressLauncher`, agents and egress are not
    // ported.
    version_generator: Arc<dyn TimedVersionGenerator>,
    turn_auth_handler: Option<Arc<TurnAuthHandler>>,
    bus: Arc<MessageBus>,

    // `roomServers`, `participantServers`: what is registered on the bus under each topic
    // (see the header). Not here: `agentDispatchServers`, `httpSignalParticipantServers`,
    // `whipParticipantServers`.
    room_servers: Mutex<HashMap<routing::RoomTopic, routing::ServerId>>,
    participant_servers: Mutex<HashMap<routing::ParticipantTopic, routing::ServerId>>,
    ice_config_cache: sutils::IceConfigCache<IceConfigCacheKey>,

    forward_stats: Option<Arc<sfu::ForwardStats>>,

    subscribe_pacer: Option<Arc<rtc::SubscribePacer>>,

    // New: the node every participant's connections are on (see the header).
    node: Node,

    // New: the port the `turns:` URL names, 0 for upstream's 443 (see the header).
    advertised_turn_tls_port: atomic::Uint32,
}

// `defer room.Release()`
struct ReleaseOnDrop<'a>(&'a Room);

impl Drop for ReleaseOnDrop<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl RoomManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new_local(
        conf: Arc<Config>,
        room_store: Arc<dyn ObjectStore>,
        current_node: Arc<dyn LocalNode>,
        router: Arc<dyn Router>,
        room_allocator: Arc<dyn RoomAllocator>,
        telemetry: Arc<dyn telemetry::TelemetryService>,
        version_generator: Arc<dyn TimedVersionGenerator>,
        turn_auth_handler: Option<Arc<TurnAuthHandler>>,
        bus: Arc<MessageBus>,
        forward_stats: Option<Arc<sfu::ForwardStats>>,
        rtc_conf: Arc<rtc::WebRtcConfig>,
        node: Node,
    ) -> Result<Arc<RoomManager>, Error> {
        // `rtc.NewWebRTCConfig(conf)`: the server's (see the header)

        let r = Arc::new_cyclic(|this| RoomManager {
            this: this.clone(),
            lock: RwLock::new(HashMap::new()),
            rtc_config: rtc_conf,
            current_node: current_node.clone(),
            router,
            room_allocator,
            room_store,
            telemetry,
            client_conf_manager: Box::new(StaticClientConfigurationManager::new(
                STATIC_CONFIGURATIONS.clone(),
            )),
            version_generator,
            turn_auth_handler,
            bus: bus.clone(),
            room_servers: Mutex::new(HashMap::new()),
            participant_servers: Mutex::new(HashMap::new()),
            forward_stats,
            subscribe_pacer: rtc::SubscribePacer::new(&conf.limit.subscribe_pace_shadow_rates),

            ice_config_cache: sutils::IceConfigCache::new(Duration::ZERO),

            server_info: livekit::ServerInfo {
                edition: livekit::server_info::Edition::Standard as i32,
                version: VERSION.to_string(),
                protocol: types::CURRENT_PROTOCOL,
                agent_protocol: AGENT_CURRENT_PROTOCOL,
                region: conf.region.clone(),
                node_id: current_node.node_id().to_string(),
                ..Default::default()
            },
            config: conf,
            node,
            advertised_turn_tls_port: atomic::Uint32::new(0),
        });

        // `rpc.NewTypedRoomManagerServer(r, bus, ...)` and its `RegisterAllNodeTopics`
        bus.register_room_manager_server(&current_node.node_id(), r.clone());

        // Not here: `newWhipService` and its server, WHIP is not ported.

        Ok(r)
    }

    /// New: the port the `turns:` URL of the ICE servers names; `None` for 443, as upstream
    /// names whatever `turn.tls_port` is (see the header).
    pub fn set_advertised_turn_tls_port(&self, port: Option<u16>) {
        self.advertised_turn_tls_port
            .store(port.map_or(0, u32::from));
    }

    fn arc(&self) -> Arc<RoomManager> {
        self.this
            .upgrade()
            .expect("the room manager is held by whoever calls it")
    }

    pub fn get_room(&self, room_name: &livekit::RoomName) -> Option<Arc<Room>> {
        let rooms = self.lock.read();
        rooms.get(room_name).cloned()
    }

    /// New: every room of the node (see the header).
    pub fn get_rooms(&self) -> Vec<Arc<Room>> {
        self.lock.read().values().cloned().collect()
    }

    // deleteRoom completely deletes all room information, including active sessions, room store, and routing info
    fn delete_room_state(&self, room_name: &livekit::RoomName) -> Result<(), Error> {
        infow!(logger::get_logger(), "deleting room state", "room" => room_name);
        self.lock.write().remove(room_name);

        // clear routing information
        let err = self.router.clear_room_state(room_name).map_err(Error::from);
        // also delete room from db
        let err2 = self.room_store.delete_room(room_name);

        // both at once upstream: see the header
        err2?;
        err
    }

    pub fn close_idle_rooms(&self) {
        let rooms: Vec<Arc<Room>> = self.lock.read().values().cloned().collect();

        for room in rooms {
            room.close_if_empty();
        }
    }

    pub fn has_participants(&self) -> bool {
        let rooms = self.lock.read();

        for room in rooms.values() {
            if !room.get_participants().is_empty() {
                return true;
            }
        }
        false
    }

    pub fn stop(&self) {
        // disconnect all clients
        let rooms: Vec<Arc<Room>> = self.lock.read().values().cloned().collect();

        for room in rooms {
            room.close(types::RoomCloseReason::ServerShutdown);
        }

        // `r.roomManagerServer.Kill()`
        self.bus
            .deregister_room_manager_server(&self.current_node.node_id());
        // Not here: the WHIP servers.
        // `r.roomServers.Kill()`
        for (topic, id) in self.room_servers.lock().drain() {
            self.bus.deregister_room_server(&topic, id);
        }
        // Not here: the agent dispatch servers.
        // `r.participantServers.Kill()`
        for (topic, id) in self.participant_servers.lock().drain() {
            self.bus.deregister_participant_server(&topic, id);
        }
        // Not here: the HTTP signal and WHIP participant servers.

        // `r.rtcConfig.UDPMux.Close()` and `r.rtcConfig.TCPMuxListener.Close()`: the node's
        // sockets
        self.node.close();

        self.ice_config_cache.stop();

        if let Some(forward_stats) = &self.forward_stats {
            forward_stats.stop();
        }
    }

    pub fn create_room(&self, req: &livekit::CreateRoomRequest) -> Result<livekit::Room, Error> {
        let room = self.get_or_create_room(Some(req))?;
        let _release = ReleaseOnDrop(&room);

        Ok(room.to_proto())
    }

    // StartSession starts WebRTC session when a new participant is connected, takes place on RTC node
    pub fn start_session(
        &self,
        pi: routing::ParticipantInit,
        request_source: Arc<dyn MessageSource>,
        response_sink: Arc<dyn MessageSink>,
        use_one_shot_signalling_mode: bool,
    ) -> Result<(), Error> {
        let session_start_time = time::now();

        if !pi.identity.is_empty()
            && let Some(grants) = &pi.grants
        {
            if !self.config.limit.check_metadata_size(&grants.metadata) {
                return Err(Error::MetadataExceedsLimits);
            }
            if !self.config.limit.check_attributes_size(&grants.attributes) {
                return Err(Error::AttributeExceedsLimits);
            }
        }

        let create_room = pi.create_room.as_ref();
        let room = match self.get_or_create_room(create_room) {
            Ok(room) => room,
            Err(err) => {
                if !pi.identity.is_empty() {
                    prometheus::increment_participant_rtc_canceled(1, false, client_sdk(&pi));
                }
                return Err(err);
            }
        };
        let _release = ReleaseOnDrop(&room);

        let (proto_room, room_internal) = (room.to_proto(), room.internal());

        // only create the room, but don't start a participant session
        if pi.identity.is_empty() {
            return Ok(());
        }

        // should not error out, error is logged in iceServersForParticipant even if it fails
        // since this is used for TURN server credentials, we don't want to fail the request even if there's no TURN for the session
        let api_key = self
            .get_first_key_pair()
            .map(|(key, _)| key)
            .unwrap_or_default();

        let participant = room.get_participant(&pi.identity);
        if let Some(participant) = participant {
            // When reconnecting, it means WS has interrupted but underlying peer connection is still ok in this state,
            // we'll keep the participant SID, and just swap the sink for the underlying connection
            if pi.reconnect {
                if participant.is_closed() {
                    // Send leave request if participant is closed, i. e. handle the case of client trying to resume crossing wires with
                    // server closing the participant due to some irrecoverable condition. Such a condition would have triggered
                    // a full reconnect when that condition occurred.
                    //
                    // It is possible that the client did not get that send request. So, send it again.
                    infow!(logger::get_logger(), "cannot restart a closed participant",
                        "room" => room.name(),
                        "nodeID" => self.current_node.node_id(),
                        "participant" => pi.identity,
                        "reason" => pi.reconnect_reason,
                    );

                    let pv = types::ProtocolVersion(client_protocol(&pi));
                    let leave = if pv.supports_regions_in_leave_request() {
                        livekit::LeaveRequest {
                            reason: livekit::DisconnectReason::StateMismatch as i32,
                            action: livekit::leave_request::Action::Reconnect as i32,
                            ..Default::default()
                        }
                    } else {
                        livekit::LeaveRequest {
                            can_reconnect: true,
                            reason: livekit::DisconnectReason::StateMismatch as i32,
                            ..Default::default()
                        }
                    };
                    let _ = response_sink.write_message(
                        livekit::SignalResponse {
                            message: Some(livekit::signal_response::Message::Leave(leave)),
                        }
                        .into(),
                    );
                    prometheus::increment_participant_rtc_canceled(
                        1,
                        participant.is_warp_enabled(),
                        participant_sdk(&participant),
                    );
                    return Err(Error::CouldNotRestartClosedParticipant);
                }

                infow!(participant.get_logger(), "resuming RTC session",
                    "nodeID" => self.current_node.node_id(),
                    "participantInit" => pi,
                    "numParticipants" => room.get_participant_count(),
                );
                let ice_config = self.get_ice_config(&room.name(), &participant);
                if let Err(err) = room.resume_participant(
                    participant.clone(),
                    Some(request_source.clone()),
                    Some(response_sink.clone()),
                    Some(&ice_config),
                    &self.ice_servers_for_participant(
                        &api_key,
                        &participant,
                        ice_config.preference_subscriber() == livekit::IceCandidateType::IctTls,
                    ),
                    pi.reconnect_reason,
                ) {
                    warnw!(
                        participant.get_logger(),
                        "could not resume participant",
                        err
                    );
                    prometheus::increment_participant_rtc_canceled(
                        1,
                        participant.is_warp_enabled(),
                        participant_sdk(&participant),
                    );
                    return Err(err.into());
                }
                self.telemetry.participant_resumed(
                    &room.to_proto(),
                    &participant.to_proto(),
                    &self.current_node.node_id(),
                    pi.reconnect_reason,
                );
                prometheus::increment_participant_rtc_active(1, participant.is_warp_enabled());

                {
                    let room = room.clone();
                    let participant = participant.clone();
                    let sync_state = pi.sync_state.clone();
                    time::spawn(async move {
                        let _ = room.handle_sync_state(participant, sync_state.as_ref());
                    });
                }

                time::spawn(self.arc().rtc_session_worker(
                    room.clone(),
                    participant,
                    request_source,
                ));
                return Ok(());
            }

            // we need to clean up the existing participant, so a new one can join
            infow!(participant.get_logger(), "removing duplicate participant");
            room.remove_participant(
                &participant.identity(),
                &participant.id(),
                types::ParticipantCloseReason::DuplicateIdentity,
            );
        } else if pi.reconnect {
            // send leave request if participant is trying to reconnect without keep subscribe state
            // but missing from the room
            let pv = types::ProtocolVersion(client_protocol(&pi));
            let leave = if pv.supports_regions_in_leave_request() {
                livekit::LeaveRequest {
                    reason: livekit::DisconnectReason::StateMismatch as i32,
                    action: livekit::leave_request::Action::Reconnect as i32,
                    ..Default::default()
                }
            } else {
                livekit::LeaveRequest {
                    can_reconnect: true,
                    reason: livekit::DisconnectReason::StateMismatch as i32,
                    ..Default::default()
                }
            };
            let _ = response_sink.write_message(
                livekit::SignalResponse {
                    message: Some(livekit::signal_response::Message::Leave(leave)),
                }
                .into(),
            );
            prometheus::increment_participant_rtc_canceled(1, false, client_sdk(&pi));
            return Err(Error::CouldNotRestartParticipant);
        }

        let sid = livekit::ParticipantId::from(guid::new(putils::PARTICIPANT_PREFIX));
        let p_logger = rtc::logger_with_participant(
            &rtc::logger_with_room(&logger::get_logger(), &room.name(), &room.id()),
            &pi.identity,
            &sid,
            false,
        );
        infow!(p_logger, "starting RTC session",
            "room" => room.name(),
            "nodeID" => self.current_node.node_id(),
            "numParticipants" => room.get_participant_count(),
            "participantInit" => pi,
            "apiKey" => pi.api_key,
        );

        let client = pi
            .client
            .as_ref()
            .expect("invalid memory address or nil pointer dereference");
        let client_conf = self.client_conf_manager.get_configuration(client);

        let pv = types::ProtocolVersion(client.protocol);
        let mut rtc_conf = (*self.rtc_config).clone();
        rtc_conf.set_buffer_factory(room.get_buffer_factory());
        if pi.disable_ice_lite {
            rtc_conf.web_rtc_config.setting_engine.lite = false;
        }
        rtc_conf.update_publisher_config(pi.use_single_peer_connection);

        // default allow forceTCP
        let mut allow_fallback = true;
        if let Some(allow_tcp_fallback) = self.config.rtc.allow_tcp_fallback {
            allow_fallback = allow_tcp_fallback;
        }

        // default do not force full reconnect on a publication error
        let mut reconnect_on_publication_error = false;
        if let Some(v) = self.config.rtc.reconnect_on_publication_error {
            reconnect_on_publication_error = v;
        }

        // default do not force full reconnect on a subscription error
        let mut reconnect_on_subscription_error = false;
        if let Some(v) = self.config.rtc.reconnect_on_subscription_error {
            reconnect_on_subscription_error = v;
        }

        // default do not force full reconnect on a data channel error
        let mut reconnect_on_data_channel_error = false;
        if let Some(v) = self.config.rtc.reconnect_on_data_channel_error {
            reconnect_on_data_channel_error = v;
        }

        let mut subscriber_allow_pause = self.config.rtc.congestion_control.allow_pause;
        if let Some(v) = pi.subscriber_allow_pause {
            subscriber_allow_pause = v;
        }

        let mut enabled_codecs = proto_room.enabled_codecs.clone();
        if !enabled_codecs
            .iter()
            .any(|codec| mime::is_mime_type_string_rtx(&codec.mime))
        {
            enabled_codecs.push(livekit::Codec {
                mime: mime::MimeType::Rtx.as_str().to_string(),
                ..Default::default()
            });
        }

        let enable_warp = !use_one_shot_signalling_mode && self.config.rtc.enable_warp;
        let rtc_config = &self.config.rtc.rtc_config;
        let participant: Arc<dyn types::LocalParticipant> =
            match rtc::ParticipantImpl::new(rtc::ParticipantParams {
                identity: pi.identity.clone(),
                name: pi.name.clone(),
                sid,
                config: Arc::new(rtc_conf),
                sink: Some(response_sink.clone()),
                audio_config: self.config.audio,
                video_config: self.config.video.clone(),
                limit_config: self.config.limit.clone(),
                protocol_version: pv,
                session_start_time,
                // `SessionTimer`: LiveKit's observability, not ported
                telemetry_listener: Some(room.participant_telemetry_listener()),
                trailer: room.trailer(),
                pli_throttle_config: self.config.rtc.pli_throttle,
                congestion_control_config: self.config.rtc.congestion_control.clone(),
                publish_enabled_codecs: enabled_codecs.clone(),
                subscribe_enabled_codecs: enabled_codecs,
                grants: pi.grants.clone().map(Arc::new),
                token_expires_at: pi.token_expires_at,
                reconnect: pi.reconnect,
                logger: p_logger.clone(),
                // `Reporter`: LiveKit's observability, not ported
                client_conf,
                client_info: rtc::ClientInfo {
                    client_info: pi.client.clone().map(Arc::new),
                },
                region: pi.region.clone(),
                adaptive_stream: pi.adaptive_stream,
                allow_tcp_fallback: allow_fallback,
                tcp_fallback_rtt_threshold: self.config.rtc.tcp_fallback_rtt_threshold,
                allow_udp_unstable_fallback: self.config.rtc.allow_udp_unstable_fallback,
                turns_enabled: self.config.is_turns_enabled(),
                participant_listener: Some(room.local_participant_listener()),
                participant_helper: Some(Arc::new(RoomManagerParticipantHelper {
                    room: Arc::downgrade(&room),
                    codec_regression_threshold: self.config.video.codec_regression_threshold,
                })),
                reconnect_on_publication_error,
                reconnect_on_subscription_error,
                reconnect_on_data_channel_error,
                version_generator: self.version_generator.clone(),
                subscriber_allow_pause,
                subscription_limit_audio: self.config.limit.subscription_limit_audio,
                subscription_limit_video: self.config.limit.subscription_limit_video,
                subscribe_pacer: self.subscribe_pacer.clone(),
                playout_delay: room_internal
                    .as_ref()
                    .and_then(|internal| internal.playout_delay),
                sync_streams: room_internal
                    .as_ref()
                    .is_some_and(|internal| internal.sync_streams),
                forward_stats: self.forward_stats.clone(),
                // `MetricConfig`: `pkg/metric` is not ported
                use_one_shot_signalling_mode,
                data_channel_max_buffered_amount: self.config.rtc.data_channel_max_buffered_amount,
                datachannel_slow_threshold: self.config.rtc.datachannel_slow_threshold,
                datachannel_lossy_target_latency: self.config.rtc.datachannel_lossy_target_latency,
                datachannel_data_track_target_latency: self
                    .config
                    .rtc
                    .datachannel_data_track_target_latency,
                fire_on_track_by_sdp: true,
                use_single_peer_connection: pi.use_single_peer_connection,
                enable_data_tracks: self.config.enable_data_tracks,
                enable_participant_data_blob: self.config.enable_participant_data_blob,
                enable_rtp_stream_restart_detection: self
                    .config
                    .rtc
                    .enable_rtp_stream_restart_detection,
                enable_warp,
                media_batch_io_enabled: rtc_config.batch_io.batch_size > 0
                    && (rtc_config.ice_port_range_start == 0 || rtc_config.ice_port_range_end == 0)
                    && rtc_config.udp_port.valid(),
                node: self.node.clone(),
                // Go's zero values for the fields the original does not set
                sim_tracks: HashMap::new(),
                initial_version: 0,
                migration: false,
                disable_supervisor: false,
                disable_dynacast: false,
                disable_sender_report_pass_through: false,
                enable_metrics: false,
                disable_codec_regression: false,
                last_pub_reliable_seq: 0,
                country: String::new(),
                prefer_video_size_from_media: false,
                force_backup_codec_policy_simulcast: false,
                disable_transceiver_reuse_for_e2ee: false,
                enable_start_at_desired_quality: false,
                migration_wait_duration: Duration::ZERO,
                exclude_ipv6_local_candidates: false,
            }) {
                Ok(participant) => participant,
                Err(err) => {
                    prometheus::increment_participant_rtc_canceled(1, enable_warp, client_sdk(&pi));
                    return Err(err.into());
                }
            };
        let ice_config = self.set_ice_config(&room.name(), &participant);

        // join room
        let mut opts = rtc::ParticipantOptions {
            auto_subscribe: pi.auto_subscribe,
            ..Default::default()
        };
        if let Some(auto_subscribe_data_track) = pi.auto_subscribe_data_track {
            opts.auto_subscribe_data_track = auto_subscribe_data_track;
        }
        let ice_servers = self.ice_servers_for_participant(
            &api_key,
            &participant,
            ice_config.preference_subscriber() == livekit::IceCandidateType::IctTls,
        );
        if let Err(err) = room.join(
            participant.clone(),
            Some(request_source.clone()),
            Some(opts),
            &ice_servers,
        ) {
            errorw!(p_logger, "could not join room", err);
            let _ = participant.close(true, types::ParticipantCloseReason::JoinFailed, false);
            prometheus::increment_participant_rtc_canceled(1, enable_warp, client_sdk(&pi));
            return Err(err.into());
        }

        let participant_topic =
            routing::format_participant_topic(&room.name(), &participant.identity());
        // `participantServers.Replace(participantTopic, participantServer)` with its
        // `RegisterAllParticipantTopics`, which cannot fail on the bus of one process. Not here:
        // the WHIP participant server of one-shot signalling mode (see the header).
        let participant_server_id = self.replace_participant_server(&participant_topic);

        if let Err(err) = self
            .room_store
            .store_participant(&room.name(), &participant.to_proto())
        {
            errorw!(p_logger, "could not store participant", err);
        }

        // update room store with new numParticipants
        persist_room_for_participant_count(
            &*self.room_store,
            &room,
            &*participant,
            &room.to_proto(),
        );

        let client_meta = livekit::AnalyticsClientMeta {
            region: self.current_node.region(),
            node: self.current_node.node_id().to_string(),
            ..Default::default()
        };
        self.telemetry.participant_joined(
            &proto_room,
            &participant.to_proto(),
            pi.client.as_ref(),
            Some(&client_meta),
            true,
            participant.telemetry_guard(),
        );
        {
            let room_store = self.room_store.clone();
            let telemetry = self.telemetry.clone();
            let room_name = room.name();
            let weak_room = Arc::downgrade(&room);
            let p_logger = p_logger.clone();
            let weak_r = self.this.clone();
            participant.add_on_close(
                types::PARTICIPANT_CLOSE_KEY_NORMAL,
                Some(Box::new(move |p: Arc<dyn types::LocalParticipant>| {
                    // `participantServerClosers.Close()`
                    if let Some(r) = weak_r.upgrade() {
                        r.kill_participant_server(&participant_topic, participant_server_id);
                    }

                    if let Err(err) = room_store.delete_participant(&room_name, &p.identity()) {
                        errorw!(p_logger, "could not delete participant", err);
                    }

                    // the room is there while it closes its participants; one that is gone
                    // has nothing to store or tell
                    let Some(room) = weak_room.upgrade() else {
                        return;
                    };
                    // update room store with new numParticipants
                    let proto = room.to_proto();
                    persist_room_for_participant_count(&*room_store, &room, &*p, &proto);
                    telemetry.participant_left(&proto, &p.to_proto(), true, p.telemetry_guard());
                })),
            );
        }
        {
            let weak_r = self.this.clone();
            let p_logger = p_logger.clone();
            participant.on_claims_changed(Box::new(
                move |participant: Arc<dyn types::LocalParticipant>| {
                    debugw!(p_logger, "refreshing client token after claims change");
                    let Some(r) = weak_r.upgrade() else {
                        return;
                    };
                    if let Err(err) = r.refresh_token(&participant) {
                        errorw!(p_logger, "could not refresh token", err);
                    }
                },
            ));
        }
        {
            let weak_r = self.this.clone();
            let room_name = room.name();
            participant.on_ice_config_changed(Box::new(
                move |participant: Arc<dyn types::LocalParticipant>,
                      ice_config: &livekit::IceConfig| {
                    let Some(r) = weak_r.upgrade() else {
                        return;
                    };
                    r.ice_config_cache.put(
                        IceConfigCacheKey {
                            room_name: room_name.clone(),
                            participant_identity: participant.identity(),
                        },
                        *ice_config,
                    );
                },
            ));
        }

        for add_track_request in &pi.add_track_requests {
            participant.add_track(add_track_request);
        }
        if let Some(publisher_offer) = &pi.publisher_offer {
            let _ = participant.handle_offer(publisher_offer);
        }

        time::spawn(
            self.arc()
                .rtc_session_worker(room.clone(), participant, request_source),
        );
        Ok(())
    }

    // create the actual room object, to be used on RTC node
    fn get_or_create_room(
        &self,
        create_room: Option<&livekit::CreateRoomRequest>,
    ) -> Result<Arc<Room>, Error> {
        let create_room = create_room.expect("invalid memory address or nil pointer dereference");
        let room_name = livekit::RoomName::from(create_room.name.as_str());

        let mut last_seen_room = self.lock.read().get(&room_name).cloned();

        if let Some(last_seen_room) = &last_seen_room
            && last_seen_room.hold()
        {
            return Ok(last_seen_room.clone());
        }

        // create new room, get details first
        // createRoom is reassigned to the request with the room preset applied, so that a room
        // composite egress from the preset, which RoomInternal has no field for, is started
        // (the request and `created` are read only for that egress, which is not ported)
        let (ri, internal, _create_room, _created) =
            self.room_allocator.create_room(create_room, true)?;

        let mut rooms = self.lock.write();

        let mut current_room = rooms.get(&room_name).cloned();
        while !same_room(&current_room, &last_seen_room) {
            drop(rooms);
            if let Some(current_room) = &current_room
                && current_room.hold()
            {
                return Ok(current_room.clone());
            }

            last_seen_room = current_room;
            rooms = self.lock.write();
            current_room = rooms.get(&room_name).cloned();
        }

        // construct ice servers
        let new_room = Room::new(
            &ri,
            internal.as_ref(),
            (*self.rtc_config).clone(),
            self.config.room.clone(),
            &self.config.audio,
            &self.server_info,
            self.telemetry.clone(),
        );

        let room_topic = routing::format_room_topic(&room_name);
        // `roomServers.Replace(roomTopic, roomServer)` with its `RegisterAllRoomTopics`, which
        // cannot fail on the bus of one process. Not here: the agent dispatch server.
        let room_server_id = self.replace_room_server(&room_topic);

        {
            let weak_r = self.this.clone();
            let weak_room = Arc::downgrade(&new_room);
            let room_name = room_name.clone();
            new_room.on_close(move |reason: types::RoomCloseReason| {
                // `killRoomServer()`; not here: `killDispServer()`
                if let Some(r) = weak_r.upgrade() {
                    r.kill_room_server(&room_topic, room_server_id);
                }

                let (Some(r), Some(new_room)) = (weak_r.upgrade(), weak_room.upgrade()) else {
                    return;
                };
                let room_info = new_room.to_proto();
                r.telemetry.room_ended(&room_info, reason.to_proto());
                prometheus::room_ended(Time::unix(room_info.creation_time, 0));
                if let Err(err) = r.delete_room_state(&room_name) {
                    errorw!(new_room.logger(), "could not delete room", err);
                }

                infow!(new_room.logger(), "room closed");
            });
        }

        {
            let room_store = self.room_store.clone();
            let weak_room = Arc::downgrade(&new_room);
            new_room.on_room_updated(move || {
                let Some(new_room) = weak_room.upgrade() else {
                    return;
                };
                if let Err(err) =
                    room_store.store_room(&new_room.to_proto(), new_room.internal().as_ref())
                {
                    errorw!(new_room.logger(), "could not handle metadata update", err);
                }
            });
        }

        {
            let room_store = self.room_store.clone();
            let room_logger = new_room.logger();
            let room_name = room_name.clone();
            new_room.on_participant_changed(move |p: Arc<dyn types::Participant>| {
                if !p.is_disconnected()
                    && let Err(err) = room_store.store_participant(&room_name, &p.to_proto())
                {
                    errorw!(room_logger, "could not handle participant change", err);
                }
            });
        }

        rooms.insert(room_name, new_room.clone());

        drop(rooms);

        new_room.hold();

        self.telemetry.room_started(&new_room.to_proto());
        prometheus::room_started();

        // Not here: the room composite egress of the request or its preset (`createRoom.Egress`
        // and `r.egressLauncher.StartEgress`), egress is not ported.

        Ok(new_room)
    }

    // manages an RTC session for a participant, runs on the RTC node
    async fn rtc_session_worker(
        self: Arc<Self>,
        room: Arc<Room>,
        participant: Arc<dyn types::LocalParticipant>,
        request_source: Arc<dyn MessageSource>,
    ) {
        let p_logger = participant.get_logger();

        let r = rtc::recover_async(
            Some(&p_logger),
            self.rtc_session_worker_loop(&room, &participant, &request_source, &p_logger),
        )
        .await;
        // the deferred `rtc.Recover`, which runs first
        if r.is_err() {
            std::process::exit(1);
        }

        debugw!(p_logger, "RTC session finishing", "connID" => request_source.connection_id());
        request_source.close();
    }

    // The body of `rtcSessionWorker` after its deferred functions.
    async fn rtc_session_worker_loop(
        &self,
        room: &Room,
        participant: &Arc<dyn types::LocalParticipant>,
        request_source: &Arc<dyn MessageSource>,
        p_logger: &logger::Logger,
    ) {
        // send first refresh for cases when client token is close to expiring
        let _ = self.refresh_token(participant);
        let mut token_ticker = new_ticker(TOKEN_REFRESH_INTERVAL);
        let disconnected = participant.disconnected();
        let read_chan = request_source.read_chan();
        loop {
            tokio::select! {
                _ = disconnected.cancelled() => {
                    return;
                }

                _ = token_ticker.tick() => {
                    // refresh token with the first API Key/secret pair
                    if let Err(err) = self.refresh_token(participant) {
                        errorw!(p_logger, "could not refresh token", err,
                            "connID" => request_source.connection_id(),
                        );
                    }
                }

                obj = read_chan.recv() => {
                    let Some(obj) = obj else {
                        if room
                            .get_participant_request_source(&participant.identity())
                            .is_some_and(|rs| same_source(&rs, request_source))
                        {
                            participant.handle_signal_source_close();
                        }
                        return;
                    };

                    if participant.handle_signal_message(obj).is_err() {
                        // more specific errors are already logged
                        // treat errors returned as fatal
                        return;
                    }
                }
            }
        }
    }

    fn room_and_participant_for_req(
        &self,
        req: &dyn ParticipantReq,
    ) -> Result<(Arc<Room>, Arc<dyn types::LocalParticipant>), Error> {
        let Some(room) = self.get_room(&livekit::RoomName::from(req.get_room())) else {
            return Err(Error::RoomNotFound);
        };

        let Some(participant) =
            room.get_participant(&livekit::ParticipantIdentity::from(req.get_identity()))
        else {
            return Err(Error::ParticipantNotFound);
        };

        Ok((room, participant))
    }

    pub fn list_participants(
        &self,
        req: &livekit::ListParticipantsRequest,
    ) -> Result<livekit::ListParticipantsResponse, Error> {
        let Some(room) = self.get_room(&livekit::RoomName::from(&req.room)) else {
            return Err(Error::RoomNotFound);
        };

        let participants = room.get_participants();
        let mut items = Vec::with_capacity(participants.len());
        for p in &participants {
            items.push(p.to_proto());
        }

        Ok(livekit::ListParticipantsResponse {
            participants: items,
        })
    }

    pub fn get_participant(
        &self,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::ParticipantInfo, Error> {
        let Some(room) = self.get_room(&livekit::RoomName::from(&req.room)) else {
            return Err(Error::RoomNotFound);
        };

        let Some(participant) =
            room.get_participant(&livekit::ParticipantIdentity::from(&req.identity))
        else {
            return Err(Error::ParticipantNotFound);
        };

        Ok(participant.to_proto())
    }

    pub fn remove_participant(
        &self,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::RemoveParticipantResponse, Error> {
        let (room, participant) = self.room_and_participant_for_req(req)?;

        infow!(participant.get_logger(), "removing participant");
        room.remove_participant(
            &livekit::ParticipantIdentity::from(&req.identity),
            &livekit::ParticipantId::default(),
            types::ParticipantCloseReason::ServiceRequestRemoveParticipant,
        );
        Ok(livekit::RemoveParticipantResponse {})
    }

    pub fn mute_published_track(
        &self,
        req: &livekit::MuteRoomTrackRequest,
    ) -> Result<livekit::MuteRoomTrackResponse, Error> {
        let (_, participant) = self.room_and_participant_for_req(req)?;

        debugw!(participant.get_logger(), "setting track muted",
            "trackID" => req.track_sid, "muted" => req.muted);
        if !req.muted && !self.config.room.enable_remote_unmute {
            errorw!(
                participant.get_logger(),
                "cannot unmute track, remote unmute is disabled",
                nil
            );
            return Err(Error::RemoteUnmuteNoteEnabled);
        }
        let track = participant.set_track_muted(
            &livekit::MuteTrackRequest {
                sid: req.track_sid.clone(),
                muted: req.muted,
            },
            true,
        );
        Ok(livekit::MuteRoomTrackResponse { track })
    }

    pub fn update_participant(
        &self,
        req: &livekit::UpdateParticipantRequest,
    ) -> Result<livekit::ParticipantInfo, Error> {
        let (_, participant) = self.room_and_participant_for_req(req)?;

        participant.update_metadata(
            &livekit::UpdateParticipantMetadata {
                name: req.name.clone(),
                metadata: req.metadata.clone(),
                attributes: req.attributes.clone(),
                ..Default::default()
            },
            true,
        )?;

        if let Some(permission) = &req.permission {
            debugw!(
                participant.get_logger(),
                "updating participant permission",
                "permission" => permission,
            );

            participant.set_permission(Some(permission));
        }

        Ok(participant.to_proto())
    }

    pub fn forward_participant(
        &self,
        _req: &livekit::ForwardParticipantRequest,
    ) -> Result<livekit::ForwardParticipantResponse, Error> {
        Err(Error::NotImplemented)
    }

    pub fn move_participant(
        &self,
        _req: &livekit::MoveParticipantRequest,
    ) -> Result<livekit::MoveParticipantResponse, Error> {
        Err(Error::NotImplemented)
    }

    pub fn perform_rpc(
        &self,
        req: &livekit::PerformRpcRequest,
    ) -> Result<livekit::PerformRpcResponse, Error> {
        let Some(room) = self.get_room(&livekit::RoomName::from(&req.room)) else {
            return Err(Error::RoomNotFound);
        };

        let Some(participant) = room.get_participant(&livekit::ParticipantIdentity::from(
            &req.destination_identity,
        )) else {
            return Err(Error::ParticipantNotFound);
        };

        let (result_chan_tx, mut result_chan) = mpsc::channel(1);
        let (error_chan_tx, mut error_chan) = mpsc::channel(1);

        participant.perform_rpc(req, result_chan_tx, error_chan_tx);

        block_on(async {
            tokio::select! {
                Some(result) = result_chan.recv() => {
                    Ok(livekit::PerformRpcResponse { payload: result })
                }
                Some(err) = error_chan.recv() => Err(err.into()),
                // both dropped without an answer: Go's `select` waits for ever (see the header)
                else => std::future::pending().await,
            }
        })
    }

    pub fn delete_room(
        &self,
        req: &livekit::DeleteRoomRequest,
    ) -> Result<livekit::DeleteRoomResponse, Error> {
        if let Some(room) = self.get_room(&livekit::RoomName::from(&req.room)) {
            infow!(room.logger(), "deleting room");
            room.close(types::RoomCloseReason::ApiDelete);
        } else {
            // special case of a non-RTC room e.g. room created but no participants joined
            debugw!(
                logger::get_logger(),
                "Deleting non-rtc room, loading from roomstore"
            );
            if let Err(err) = self
                .room_store
                .delete_room(&livekit::RoomName::from(&req.room))
            {
                debugw!(logger::get_logger(), "Error deleting non-rtc room", "err" => err);
                return Err(err);
            }
        }
        Ok(livekit::DeleteRoomResponse {})
    }

    pub fn update_subscriptions(
        &self,
        req: &livekit::UpdateSubscriptionsRequest,
    ) -> Result<livekit::UpdateSubscriptionsResponse, Error> {
        let (room, participant) = self.room_and_participant_for_req(req)?;

        debugw!(
            participant.get_logger(),
            "updating participant subscriptions"
        );
        room.update_subscriptions(
            participant,
            &livekit::strings_as_ids::<livekit::TrackId>(&req.track_sids),
            &req.participant_tracks,
            req.subscribe,
        );
        Ok(livekit::UpdateSubscriptionsResponse {})
    }

    pub fn send_data(
        &self,
        req: &livekit::SendDataRequest,
    ) -> Result<livekit::SendDataResponse, Error> {
        let Some(room) = self.get_room(&livekit::RoomName::from(&req.room)) else {
            return Err(Error::RoomNotFound);
        };

        debugw!(room.logger(), "api send data", "size" => req.data.len());
        room.send_data_packet(
            &livekit::DataPacket {
                kind: req.kind,
                destination_identities: req.destination_identities.clone(),
                value: Some(livekit::data_packet::Value::User(livekit::UserPacket {
                    payload: req.data.clone(),
                    destination_sids: req.destination_sids.clone(),
                    destination_identities: req.destination_identities.clone(),
                    topic: req.topic.clone(),
                    nonce: req.nonce.clone(),
                    ..Default::default()
                })),
                ..Default::default()
            },
            req.kind(),
        );
        Ok(livekit::SendDataResponse {})
    }

    pub fn update_room_metadata(
        &self,
        req: &livekit::UpdateRoomMetadataRequest,
    ) -> Result<livekit::Room, Error> {
        let Some(room) = self.get_room(&livekit::RoomName::from(&req.room)) else {
            return Err(Error::RoomNotFound);
        };

        debugw!(room.logger(), "updating room");
        let done = room.set_metadata(&req.metadata);
        // wait till the update is applied
        block_on(done.cancelled());
        Ok(room.to_proto())
    }

    // Not here: `ListDispatch`, `CreateDispatch` and `DeleteDispatch`, agents are not ported.

    fn ice_servers_for_participant(
        &self,
        api_key: &str,
        participant: &Arc<dyn types::LocalParticipant>,
        mut tls_only: bool,
    ) -> Vec<livekit::IceServer> {
        let mut ice_servers = Vec::new();
        let rtc_conf = &self.config.rtc;

        if tls_only && self.config.turn.tls_port == 0 {
            warnw!(
                logger::get_logger(),
                "tls only enabled but no turn tls config",
                nil
            );
            tls_only = false;
        }

        let mut has_stun = false;
        if self.config.turn.enabled {
            let mut urls = Vec::new();
            if self.config.turn.udp_port > 0 && !tls_only {
                // UDP TURN is used as STUN
                has_stun = true;
                for ip in self.config.rtc.rtc_config.node_ip.to_string_slice() {
                    let host = if ip.contains(':') {
                        format!("[{ip}]")
                    } else {
                        ip
                    };
                    urls.push(format!(
                        "turn:{host}:{}?transport=udp",
                        self.config.turn.udp_port
                    ));
                }
            }
            if self.config.turn.tls_port > 0 {
                // 443 upstream; another port where one is set (see the header)
                let port = match self.advertised_turn_tls_port.load() {
                    0 => 443,
                    port => port,
                };
                urls.push(format!(
                    "turns:{}:{port}?transport=tcp",
                    self.config.turn.domain
                ));
            }
            if !urls.is_empty() {
                let turn_auth_handler = self
                    .turn_auth_handler
                    .as_ref()
                    .expect("invalid memory address or nil pointer dereference");
                let (username, expiry) = turn_auth_handler.create_username(
                    api_key,
                    &participant.id(),
                    self.config.turn.ttl_seconds,
                );
                match turn_auth_handler.create_password(api_key, &participant.id(), expiry) {
                    Err(err) => {
                        warnw!(
                            participant.get_logger(),
                            "could not create turn password",
                            err
                        );
                        has_stun = false;
                    }
                    Ok(password) => ice_servers.push(livekit::IceServer {
                        urls,
                        username,
                        credential: password,
                    }),
                }
            }
        }

        if !rtc_conf.turn_servers.is_empty() {
            has_stun = true;
            for s in &self.config.rtc.turn_servers {
                let mut scheme = "turn";
                let mut transport = "tcp";
                match s.protocol.as_str() {
                    "tls" => scheme = "turns",
                    "udp" => transport = "udp",
                    _ => {}
                }

                let username: String;
                let credential: String;
                if !s.secret.is_empty() {
                    // Generate dynamic credentials using TURN static auth secrets.
                    // 0 means use the default; clamp the rest so a negative or overflowing TTL
                    // cannot produce a wrong-lifetime credential.
                    let mut ttl = s.ttl;
                    if ttl == 0 {
                        ttl = config::DEFAULT_EXTERNAL_TURN_TTL_SECONDS;
                    }
                    (ttl, _) = config::clamp_turn_ttl_seconds(ttl);

                    let expiry = time::now().add(ttl * time::SECOND).unix_secs();
                    let participant_id = participant.id();
                    username = format!("{expiry}:{participant_id}");

                    // HMAC-SHA1 signature
                    let mut h = Hmac::<Sha1>::new_from_slice(s.secret.as_bytes())
                        .expect("HMAC takes a key of any length");
                    h.update(username.as_bytes());
                    credential =
                        base64::engine::general_purpose::STANDARD.encode(h.finalize().into_bytes());
                } else {
                    // Use static credentials
                    username = s.username.clone();
                    credential = s.credential.clone();
                }

                let is = livekit::IceServer {
                    urls: vec![format!(
                        "{scheme}:{}:{}?transport={transport}",
                        s.host, s.port
                    )],
                    username,
                    credential,
                };
                ice_servers.push(is);
            }
        }

        if !rtc_conf.rtc_config.stun_servers.is_empty() {
            has_stun = true;
            ice_servers.push(ice_server_for_stun_servers(
                &self.config.rtc.rtc_config.stun_servers,
            ));
        }

        if !has_stun {
            ice_servers.push(ice_server_for_stun_servers(
                &rtcconfig::DEFAULT_STUN_SERVERS,
            ));
        }
        ice_servers
    }

    fn refresh_token(&self, participant: &Arc<dyn types::LocalParticipant>) -> Result<(), Error> {
        let (key, secret) = self.get_first_key_pair()?;

        let grants = participant.claim_grants();

        // Preserve the original token's expiry
        let mut valid_for = TOKEN_DEFAULT_TTL;
        let expires_at = participant.token_expires_at();
        if !expires_at.is_zero() {
            let remaining = time::until(expires_at);
            if remaining > valid_for {
                valid_for = remaining;
            }
        }

        let token = AccessToken::new(&key, &secret)
            .set_name(&grants.name)
            .set_identity(participant.identity().as_str())
            .set_kind(grants.get_participant_kind())
            .set_valid_for(valid_for)
            .set_metadata(&grants.metadata)
            .set_attributes(&grants.attributes)
            .set_video_grant(grants.video.clone())
            .set_room_config(grants.get_room_configuration().cloned())
            .set_room_preset(&grants.room_preset);
        let jwt = token.to_jwt()?;
        participant.send_refresh_token(&jwt)?;

        Ok(())
    }

    fn set_ice_config(
        &self,
        room_name: &livekit::RoomName,
        participant: &Arc<dyn types::LocalParticipant>,
    ) -> livekit::IceConfig {
        let ice_config = self.get_ice_config(room_name, participant);
        participant.set_ice_config(Some(&ice_config));
        ice_config
    }

    fn get_ice_config(
        &self,
        room_name: &livekit::RoomName,
        participant: &Arc<dyn types::LocalParticipant>,
    ) -> livekit::IceConfig {
        self.ice_config_cache.get(&IceConfigCacheKey {
            room_name: room_name.clone(),
            participant_identity: participant.identity(),
        })
    }

    // Go ranges over the map and returns its first pair, which is any of them; so does this.
    fn get_first_key_pair(&self) -> Result<(String, String), Error> {
        if let Some((key, secret)) = self.config.keys.iter().next() {
            return Ok((key.clone(), secret.clone()));
        }
        Err(Error::NoApiKeysConfigured)
    }
}

impl RoomManager {
    // `MultitonService.Replace` for the room's topic: registers the room manager as the room's
    // server, in place of one registered before.
    fn replace_room_server(&self, topic: &routing::RoomTopic) -> routing::ServerId {
        let this = self
            .this
            .upgrade()
            .expect("the room manager is alive while it works");
        let id = self.bus.register_room_server(topic, this);
        self.room_servers.lock().insert(topic.clone(), id);
        id
    }

    // The kill function that `Replace` returns: it kills only the server it registered.
    fn kill_room_server(&self, topic: &routing::RoomTopic, id: routing::ServerId) {
        self.bus.deregister_room_server(topic, id);
        let mut servers = self.room_servers.lock();
        if servers.get(topic) == Some(&id) {
            servers.remove(topic);
        }
    }

    fn replace_participant_server(&self, topic: &routing::ParticipantTopic) -> routing::ServerId {
        let this = self
            .this
            .upgrade()
            .expect("the room manager is alive while it works");
        let id = self.bus.register_participant_server(topic, this);
        self.participant_servers.lock().insert(topic.clone(), id);
        id
    }

    fn kill_participant_server(&self, topic: &routing::ParticipantTopic, id: routing::ServerId) {
        self.bus.deregister_participant_server(topic, id);
        let mut servers = self.participant_servers.lock();
        if servers.get(topic) == Some(&id) {
            servers.remove(topic);
        }
    }
}

// `rpc.RoomServerImpl`, which the room manager implements upstream with the methods above.
impl routing::RoomServerImpl for RoomManager {
    fn delete_room(
        &self,
        req: &livekit::DeleteRoomRequest,
    ) -> Result<livekit::DeleteRoomResponse, routing::PsrpcError> {
        RoomManager::delete_room(self, req).map_err(Into::into)
    }

    fn send_data(
        &self,
        req: &livekit::SendDataRequest,
    ) -> Result<livekit::SendDataResponse, routing::PsrpcError> {
        RoomManager::send_data(self, req).map_err(Into::into)
    }

    fn update_room_metadata(
        &self,
        req: &livekit::UpdateRoomMetadataRequest,
    ) -> Result<livekit::Room, routing::PsrpcError> {
        RoomManager::update_room_metadata(self, req).map_err(Into::into)
    }

    fn list_participants(
        &self,
        req: &livekit::ListParticipantsRequest,
    ) -> Result<livekit::ListParticipantsResponse, routing::PsrpcError> {
        RoomManager::list_participants(self, req).map_err(Into::into)
    }

    fn get_participant(
        &self,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::ParticipantInfo, routing::PsrpcError> {
        RoomManager::get_participant(self, req).map_err(Into::into)
    }
}

// `rpc.ParticipantServerImpl`, likewise.
impl routing::ParticipantServerImpl for RoomManager {
    fn remove_participant(
        &self,
        req: &livekit::RoomParticipantIdentity,
    ) -> Result<livekit::RemoveParticipantResponse, routing::PsrpcError> {
        RoomManager::remove_participant(self, req).map_err(Into::into)
    }

    fn mute_published_track(
        &self,
        req: &livekit::MuteRoomTrackRequest,
    ) -> Result<livekit::MuteRoomTrackResponse, routing::PsrpcError> {
        RoomManager::mute_published_track(self, req).map_err(Into::into)
    }

    fn update_participant(
        &self,
        req: &livekit::UpdateParticipantRequest,
    ) -> Result<livekit::ParticipantInfo, routing::PsrpcError> {
        RoomManager::update_participant(self, req).map_err(Into::into)
    }

    fn update_subscriptions(
        &self,
        req: &livekit::UpdateSubscriptionsRequest,
    ) -> Result<livekit::UpdateSubscriptionsResponse, routing::PsrpcError> {
        RoomManager::update_subscriptions(self, req).map_err(Into::into)
    }

    fn forward_participant(
        &self,
        req: &livekit::ForwardParticipantRequest,
    ) -> Result<livekit::ForwardParticipantResponse, routing::PsrpcError> {
        RoomManager::forward_participant(self, req).map_err(Into::into)
    }

    fn move_participant(
        &self,
        req: &livekit::MoveParticipantRequest,
    ) -> Result<livekit::MoveParticipantResponse, routing::PsrpcError> {
        RoomManager::move_participant(self, req).map_err(Into::into)
    }

    fn perform_rpc(
        &self,
        req: &livekit::PerformRpcRequest,
    ) -> Result<livekit::PerformRpcResponse, routing::PsrpcError> {
        RoomManager::perform_rpc(self, req).map_err(Into::into)
    }
}

// `rpc.RoomManagerServerImpl`, which the bus calls for the router's `CreateRoom`.
impl routing::RoomManagerServerImpl for RoomManager {
    fn create_room(
        &self,
        req: &livekit::CreateRoomRequest,
    ) -> Result<livekit::Room, routing::Error> {
        RoomManager::create_room(self, req).map_err(routing::Error::from)
    }
}

// `persistRoomForParticipantCount`, a closure of `StartSession` upstream, which `OnClose` of
// the participant calls too.
fn persist_room_for_participant_count(
    room_store: &dyn ObjectStore,
    room: &Room,
    participant: &dyn types::LocalParticipant,
    proto: &livekit::Room,
) {
    if !participant.hidden() && !room.is_closed() {
        let err = room_store.store_room(proto, room.internal().as_ref());
        if let Err(err) = err {
            errorw!(logger::get_logger(), "could not store room", err);
        }
    }
}

// `r == lastSeenRoom` of two rooms that may be nil: the same one, or none both times.
fn same_room(a: &Option<Arc<Room>>, b: &Option<Arc<Room>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

// `room.GetParticipantRequestSource(...) == requestSource`: the same source.
fn same_source(a: &Arc<dyn MessageSource>, b: &Arc<dyn MessageSource>) -> bool {
    std::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b))
}

// `pi.Client.GetSdk()`
fn client_sdk(pi: &routing::ParticipantInit) -> livekit::client_info::Sdk {
    pi.client
        .as_ref()
        .map(|client| client.sdk())
        .unwrap_or_default()
}

// `pi.Client.Protocol`, which the original reads without checking for nil
fn client_protocol(pi: &routing::ParticipantInit) -> i32 {
    pi.client
        .as_ref()
        .expect("invalid memory address or nil pointer dereference")
        .protocol
}

// `participant.GetClientInfo().GetSdk()`
fn participant_sdk(participant: &Arc<dyn types::LocalParticipant>) -> livekit::client_info::Sdk {
    participant
        .get_client_info()
        .map(|client| client.sdk())
        .unwrap_or_default()
}

// Go's `time.NewTicker(d)`: the first tick comes after `d`, and ticks that the receiver is
// too slow for are dropped.
fn new_ticker(d: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + d.to_std(), d.to_std());
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker
}

// Waits for `f` on the caller's thread, as the original's goroutine waits on a channel; a
// worker of a multi-threaded runtime tells the runtime so.
fn block_on<F: Future>(f: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(f))
        }
        _ => time::runtime().block_on(f),
    }
}

// ------------------------------------

fn ice_server_for_stun_servers(servers: &[impl AsRef<str>]) -> livekit::IceServer {
    let mut ice_server = livekit::IceServer::default();
    for stun_server in servers {
        ice_server
            .urls
            .push(format!("stun:{}", stun_server.as_ref()));
    }
    ice_server
}

// ------------------------------------

// `participantReq`: what the requests that name a room and a participant have in common.
trait ParticipantReq {
    fn get_room(&self) -> &str;
    fn get_identity(&self) -> &str;
}

macro_rules! participant_req {
    ($($t:ty),*) => {
        $(impl ParticipantReq for $t {
            fn get_room(&self) -> &str {
                &self.room
            }
            fn get_identity(&self) -> &str {
                &self.identity
            }
        })*
    };
}

participant_req!(
    livekit::RoomParticipantIdentity,
    livekit::MuteRoomTrackRequest,
    livekit::UpdateParticipantRequest,
    livekit::UpdateSubscriptionsRequest
);

// ------------------------------------

struct RoomManagerParticipantHelper {
    // the room holds the participant, which holds its helper: see the header
    room: Weak<Room>,
    codec_regression_threshold: i64,
}

impl types::LocalParticipantHelper for RoomManagerParticipantHelper {
    fn get_participant_info(
        &self,
        p_id: &livekit::ParticipantId,
    ) -> Option<livekit::ParticipantInfo> {
        let room = self.room.upgrade()?;
        if let Some(p) = room.get_participant_by_id(p_id) {
            return Some(p.to_proto());
        }
        None
    }

    fn get_region_settings(&self, _ip: &str) -> Option<livekit::RegionSettings> {
        None
    }

    fn get_subscriber_forwarder_state(
        &self,
        _lp: Arc<dyn types::LocalParticipant>,
    ) -> Result<HashMap<livekit::TrackId, livekit::RtpForwarderState>, rtc::Error> {
        Ok(HashMap::new())
    }

    // a room that is gone resolves nothing
    fn resolve_media_track(
        &self,
        lp: Arc<dyn types::LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> types::MediaResolverResult {
        match self.room.upgrade() {
            Some(room) => room.resolve_media_track_for_subscriber(lp, track_id),
            None => types::MediaResolverResult::default(),
        }
    }

    fn resolve_data_track(
        &self,
        lp: Arc<dyn types::LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> types::DataResolverResult {
        match self.room.upgrade() {
            Some(room) => room.resolve_data_track_for_subscriber(lp, track_id),
            None => types::DataResolverResult::default(),
        }
    }

    // a room that is gone has no participants
    fn should_regress_codec(&self) -> bool {
        let participant_count = self
            .room
            .upgrade()
            .map(|room| room.get_participant_count())
            .unwrap_or_default();
        self.codec_regression_threshold == 0
            || (participant_count as i64) < self.codec_regression_threshold
    }

    fn get_cached_reliable_data_message(
        &self,
        seqs: &HashMap<livekit::ParticipantId, u32>,
    ) -> Vec<Arc<types::DataMessageCache>> {
        match self.room.upgrade() {
            Some(room) => room.get_cached_reliable_data_message(seqs),
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
#[path = "roommanager_test.rs"]
mod roommanager_test;
