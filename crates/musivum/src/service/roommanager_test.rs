// Tests of the room manager, new for musivum. The original's one test function,
// `TestRoomCompositeEgressOnCreateRoom` of roommanager_test.go, is about the room composite
// egress that `CreateRoom` and `StartSession` start, which musivum leaves out with egress: it
// is not ported, and neither are its helpers (`fakeEgressLauncher`, `newTestRoomManager`
// with an egress launcher, `roomPresetConfig`).
//
// Every test runs a real room manager with real rooms and participants, on a node of its own
// (its connections on the loopback interface), reached as the server reaches it: sessions are
// started through the local router, over the bus, and arrive at the node's signal server
// (`SignalServer::new_default`), which hands them to `StartSession`. A client is the two ends
// the router gives: the sink its requests are written to and the source of its responses,
// which a task reads (handing SDP answers and candidates to the client's connection, when it
// has one) and keeps. Where a connection is needed (data sent through the room API), the
// client's connection is a transport of `rtc` made as `rtc/transportmanager_test.rs` makes
// its client, whose helpers are private there and are written out again here.
//
// The join responses are compared with what the original sent for the same `StartSession` and
// configuration: a Go test of its own (not kept in the repository) laid over `pkg/service` with
// `go test -overlay` made a room manager as `newTestRoomManager` of roommanager_test.go does,
// with the configuration of `new_test_node`, started the session of `participant_init` with
// client protocols 16 and 18, and printed the join response with the fields that are random
// or of the machine replaced as `masked_join` replaces them; it also printed what the
// original answered for a room or participant that does not exist.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration as StdDuration;

use musivum_protocol::Message as _;
use parking_lot::Mutex;

use super::*;
use crate::config::Config;
use crate::protocol::auth::{ClaimGrants, VideoGrant};
use crate::protocol::signalling::{
    from_proto_session_description, from_proto_trickle, to_proto_session_description,
    to_proto_trickle,
};
use crate::protocol::utils::new_default_timed_version_generator;
use crate::routing::{
    LocalNodeImpl, Message, NodeStatsConfig, ParticipantInit, create_router, new_local_message_bus,
    new_room_manager_client, new_signal_client,
};
use crate::rtc::transport::FakeHandler;
use crate::rtc::{LOSSY_DATA_CHANNEL, PcTransport, RELIABLE_DATA_CHANNEL, TransportParams};
use crate::service::{LocalStore, OssServiceStore, ServiceStore, SignalServer, new_room_allocator};
use crate::telemetry::FakeTelemetryService;
use crate::testutils::{CONNECT_TIMEOUT, eventually_async};
use crate::webrtc::peerconnection::{DataChannelInit, DtlsRole, NodeConfig};

const WAIT: StdDuration = StdDuration::from_secs(5);
const TEN_MS: StdDuration = StdDuration::from_millis(10);

// ------------------------------------------------
// The node

// What a server makes, in the order it makes it: the node, the bus, the router with its two
// clients on the bus, the store and the room allocator, the room manager (which registers on
// the bus), and the signal server, started.
struct TestNode {
    rm: Arc<RoomManager>,
    router: Arc<dyn Router>,
    store: Arc<LocalStore>,
    telemetry: Arc<FakeTelemetryService>,
    signal_server: SignalServer,
}

// `t.Cleanup(rm.Stop)`
impl Drop for TestNode {
    fn drop(&mut self) {
        self.signal_server.stop();
        self.rm.stop();
    }
}

fn new_test_node(change: impl FnOnce(&mut Config)) -> TestNode {
    let mut conf = Config::new("", true).unwrap();
    // no fixed RTC ports, so that the test does not bind the defaults
    conf.rtc.rtc_config.tcp_port = 0;
    conf.rtc.rtc_config.udp_port = Default::default();
    conf.keys = HashMap::from([("devkey".to_string(), "secret".to_string())]);
    conf.region = "testregion".to_string();
    change(&mut conf);
    let conf = Arc::new(conf);

    let node = LocalNodeImpl::new(Some(&conf)).unwrap();
    let bus = new_local_message_bus();
    let router = create_router(
        node.clone(),
        new_signal_client(node.node_id(), bus.clone()).unwrap(),
        new_room_manager_client(bus.clone(), conf.room.clone()).unwrap(),
        NodeStatsConfig::default(),
    );

    let store = LocalStore::new();
    let ra = new_room_allocator(conf.clone(), store.clone());
    let telemetry = Arc::new(FakeTelemetryService::default());

    let rm = RoomManager::new_local(
        conf,
        store.clone(),
        node.clone(),
        router.clone(),
        ra,
        telemetry.clone(),
        new_default_timed_version_generator(),
        bus.clone(),
        None,
        Node::new(NodeConfig::default()).unwrap(),
    )
    .unwrap();

    let signal_server = SignalServer::new_default(node, bus, router.clone(), rm.clone()).unwrap();
    signal_server.start().unwrap();

    TestNode {
        rm,
        router,
        store,
        telemetry,
        signal_server,
    }
}

impl TestNode {
    fn participant(&self, room: &str, identity: &str) -> Arc<dyn types::LocalParticipant> {
        self.rm
            .get_room(&room.into())
            .unwrap()
            .get_participant(&identity.into())
            .unwrap()
    }
}

// The session of a JS client of the given protocol, as the signalling endpoint starts it.
fn participant_init(room: &str, identity: &str, protocol: i32) -> ParticipantInit {
    ParticipantInit {
        identity: identity.into(),
        name: "Alice".into(),
        auto_subscribe: true,
        client: Some(livekit::ClientInfo {
            sdk: livekit::client_info::Sdk::Js as i32,
            version: "2.1.0".to_string(),
            protocol,
            ..Default::default()
        }),
        grants: Some(ClaimGrants {
            identity: identity.to_string(),
            name: "Alice".to_string(),
            metadata: "md".to_string(),
            video: Some(VideoGrant {
                room_join: true,
                room: room.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        create_room: Some(livekit::CreateRoomRequest {
            name: room.to_string(),
            ..Default::default()
        }),
        adaptive_stream: true,
        ..Default::default()
    }
}

// ------------------------------------------------
// The client

struct Client {
    req_sink: Arc<dyn MessageSink>,
    responses: Arc<Mutex<Vec<livekit::SignalResponse>>>,
    closed: Arc<AtomicBool>,
    pc: Arc<OnceLock<Arc<PcTransport>>>,
}

impl Client {
    fn start(node: &TestNode, pi: ParticipantInit) -> Client {
        let room_name = livekit::RoomName::from(pi.create_room.as_ref().unwrap().name.as_str());
        let res = node
            .router
            .start_participant_signal(&room_name, pi)
            .unwrap();
        let res_source = res.response_source.unwrap();

        let client = Client {
            req_sink: res.request_sink.unwrap(),
            responses: Arc::new(Mutex::new(Vec::new())),
            closed: Arc::new(AtomicBool::new(false)),
            pc: Arc::new(OnceLock::new()),
        };
        let responses = client.responses.clone();
        let closed = client.closed.clone();
        let pc = client.pc.clone();
        tokio::spawn(async move {
            let read_chan = res_source.read_chan();
            while let Some(msg) = read_chan.recv().await {
                let Message::SignalResponse(res) = msg else {
                    panic!("a request on the response source");
                };
                if let Some(pc) = pc.get() {
                    match &res.message {
                        Some(livekit::signal_response::Message::Answer(sd)) => {
                            let (sd, id, _) = from_proto_session_description(sd);
                            let _ = pc.handle_remote_description(sd, id);
                        }
                        Some(livekit::signal_response::Message::Trickle(trickle)) => {
                            if let Ok(candidate) = from_proto_trickle(trickle) {
                                pc.add_ice_candidate(candidate);
                            }
                        }
                        _ => {}
                    }
                }
                responses.lock().push(*res);
            }
            closed.store(true, Ordering::SeqCst);
        });
        client
    }

    fn send(&self, msg: livekit::signal_request::Message) {
        self.req_sink
            .write_message(livekit::SignalRequest { message: Some(msg) }.into())
            .unwrap();
    }

    // The first response that `matches` gives a value for, waiting for it.
    async fn wait_for<T>(
        &self,
        what: &str,
        matches: impl Fn(&livekit::signal_response::Message) -> Option<T>,
    ) -> T {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            {
                let responses = self.responses.lock();
                if let Some(found) = responses
                    .iter()
                    .filter_map(|r| r.message.as_ref())
                    .find_map(&matches)
                {
                    return found;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "{what} did not come, only {:?}",
                    self.responses.lock().clone()
                );
            }
            tokio::time::sleep(TEN_MS).await;
        }
    }

    async fn join_response(&self) -> livekit::JoinResponse {
        self.wait_for("the join response", |m| match m {
            livekit::signal_response::Message::Join(jr) => Some(jr.clone()),
            _ => None,
        })
        .await
    }

    async fn until_closed(&self) {
        let closed = self.closed.clone();
        eventually_async(move || closed.load(Ordering::SeqCst), WAIT, TEN_MS).await;
    }

    // A connection in the single-connection mode, negotiated through the signalling: the
    // client offers with the two data channels of protocol 18, the participant answers.
    async fn connect(&self) -> Arc<FakeHandler> {
        let handler = Arc::new(FakeHandler::default());
        let pc = PcTransport::new(client_params(&handler)).unwrap();
        create_client_data_channels(&pc);

        handler.on_offer_calls({
            let sink = self.req_sink.clone();
            move |offer, offer_id, mid_to_track_id| {
                if let Some(sd) =
                    to_proto_session_description(offer, offer_id, mid_to_track_id.clone())
                {
                    let _ = sink.write_message(
                        livekit::SignalRequest {
                            message: Some(livekit::signal_request::Message::Offer(sd)),
                        }
                        .into(),
                    );
                }
                Ok(())
            }
        });
        handler.on_ice_candidate_calls({
            let sink = self.req_sink.clone();
            move |candidate, _target| {
                if let Some(candidate) = candidate {
                    let trickle = to_proto_trickle(
                        &candidate.to_json().unwrap(),
                        livekit::SignalTarget::Publisher,
                        false,
                    );
                    let _ = sink.write_message(
                        livekit::SignalRequest {
                            message: Some(livekit::signal_request::Message::Trickle(trickle)),
                        }
                        .into(),
                    );
                }
                Ok(())
            }
        });
        assert!(self.pc.set(pc.clone()).is_ok());

        pc.negotiate(true);
        let established = handler.clone();
        // "the client's connection did not become fully established"
        eventually_async(
            move || established.on_fully_established_call_count() > 0,
            CONNECT_TIMEOUT,
            TEN_MS,
        )
        .await;
        handler
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(pc) = self.pc.get() {
            pc.close();
        }
    }
}

// `&WebRTCConfig{}` of rtc/transport_test.rs: a configuration with nothing set that the
// transport reads.
fn web_rtc_config() -> Arc<rtc::WebRtcConfig> {
    static CONFIG: LazyLock<Arc<rtc::WebRtcConfig>> = LazyLock::new(|| {
        let mut conf = Config::new("", true).unwrap();
        conf.rtc.rtc_config.tcp_port = 0;
        let mut c = rtc::WebRtcConfig::new(&conf).unwrap();
        c.web_rtc_config.setting_engine = Default::default();
        c.web_rtc_config.ice_servers = Vec::new();
        c.web_rtc_config.nat_1to1_ips = Vec::new();
        c.web_rtc_config.use_mdns = false;
        c.buffer_factory = None;
        c.answering_dtls_role = DtlsRole::Unspecified;
        Arc::new(c)
    });
    CONFIG.clone()
}

// The client's publisher, which offers, on a node of its own, with Go's zero values where
// nothing is said (`client_params` of rtc/transportmanager_test.rs).
fn client_params(handler: &Arc<FakeHandler>) -> TransportParams {
    TransportParams {
        handler: handler.clone(),
        protocol_version: Default::default(),
        config: web_rtc_config(),
        twcc: None,
        direction_config: Default::default(),
        congestion_control_config: Default::default(),
        enabled_publish_codecs: Vec::new(),
        enabled_subscribe_codecs: Vec::new(),
        logger: Default::default(),
        transport: livekit::SignalTarget::Publisher,
        sim_tracks: HashMap::new(),
        client_info: Default::default(),
        is_offerer: true,
        is_send_side: false,
        allow_playout_delay: false,
        use_one_shot_signalling_mode: false,
        exclude_ipv6_local_candidates: false,
        fire_on_track_by_sdp: false,
        data_channel_max_buffered_amount: 0,
        datachannel_slow_threshold: 0,
        datachannel_lossy_target_latency: Duration::ZERO,
        datachannel_data_track_target_latency: Duration::ZERO,
        datachannel_max_receiver_buffer_size: 0,
        enable_data_tracks: false,
        enable_warp: false,
        node: Node::new(NodeConfig::default()).unwrap(),
    }
}

// What the client's publisher creates before its first offer, as LiveKit's SDKs do for a
// single connection: the two channels, negotiated with the ids of protocol 18.
fn create_client_data_channels(client: &PcTransport) {
    let id = |id: livekit::NegotiatedDataChannelId| Some(id as i32 as u16);
    client
        .create_data_channel(
            RELIABLE_DATA_CHANNEL,
            Some(DataChannelInit {
                ordered: Some(true),
                negotiated: Some(true),
                id: id(livekit::NegotiatedDataChannelId::NdciReliable),
                ..Default::default()
            }),
        )
        .unwrap();
    client
        .create_data_channel(
            LOSSY_DATA_CHANNEL,
            Some(DataChannelInit {
                ordered: Some(false),
                max_retransmits: Some(0),
                negotiated: Some(true),
                id: id(livekit::NegotiatedDataChannelId::NdciLossy),
                ..Default::default()
            }),
        )
        .unwrap();
}

// ------------------------------------------------
// What the original sent

// The join response with what is random or of the machine replaced, as the Go test replaced
// it (and the room's TURN password, which is random too).
fn masked_join(jr: &livekit::JoinResponse) -> livekit::JoinResponse {
    let mut jr = jr.clone();
    let room = jr.room.as_mut().unwrap();
    assert!(!room.sid.is_empty());
    assert_ne!(0, room.creation_time);
    assert!(!room.turn_password.is_empty());
    room.sid = "RM_x".to_string();
    room.creation_time = 1;
    room.creation_time_ms = 1;
    room.turn_password = "x".to_string();
    let participant = jr.participant.as_mut().unwrap();
    assert!(!participant.sid.is_empty());
    assert_ne!(0, participant.joined_at);
    participant.sid = "PA_x".to_string();
    participant.joined_at = 1;
    participant.joined_at_ms = 1;
    assert!(!jr.sif_trailer.is_empty());
    jr.sif_trailer = b"x".to_vec();
    jr.server_info.as_mut().unwrap().node_id = "ND_x".to_string();
    jr
}

fn codecs(mimes: &[&str]) -> Vec<livekit::Codec> {
    mimes
        .iter()
        .map(|mime| livekit::Codec {
            mime: mime.to_string(),
            ..Default::default()
        })
        .collect()
}

// What the original sent, for client protocols 16 and 18 alike.
fn expected_join(room: &str) -> livekit::JoinResponse {
    livekit::JoinResponse {
        room: Some(livekit::Room {
            sid: "RM_x".to_string(),
            name: room.to_string(),
            empty_timeout: 300,
            departure_timeout: 20,
            creation_time: 1,
            creation_time_ms: 1,
            turn_password: "x".to_string(),
            enabled_codecs: codecs(&[
                "audio/PCMU",
                "audio/PCMA",
                "audio/opus",
                "audio/red",
                "video/VP8",
                "video/H264",
                "video/VP9",
                "video/AV1",
                "video/H265",
                "video/rtx",
            ]),
            ..Default::default()
        }),
        participant: Some(livekit::ParticipantInfo {
            sid: "PA_x".to_string(),
            identity: "alice".to_string(),
            metadata: "md".to_string(),
            joined_at: 1,
            joined_at_ms: 1,
            name: "Alice".to_string(),
            permission: Some(livekit::ParticipantPermission {
                can_subscribe: true,
                can_publish: true,
                can_publish_data: true,
                ..Default::default()
            }),
            // The Go test called `StartSession` with no region; a session that comes through
            // the signal server has the node's (`ParticipantInitFromStartSession(ss,
            // r.region)`), in the original as here.
            region: "testregion".to_string(),
            ..Default::default()
        }),
        server_version: "1.13.11".to_string(),
        ice_servers: vec![livekit::IceServer {
            urls: vec![
                "stun:global.stun.twilio.com:3478".to_string(),
                "stun:stun.l.google.com:19302".to_string(),
                "stun:stun1.l.google.com:19302".to_string(),
            ],
            ..Default::default()
        }],
        subscriber_primary: true,
        server_region: "testregion".to_string(),
        ping_timeout: 15,
        ping_interval: 5,
        server_info: Some(livekit::ServerInfo {
            version: "1.13.11".to_string(),
            protocol: 18,
            region: "testregion".to_string(),
            node_id: "ND_x".to_string(),
            agent_protocol: 1,
            ..Default::default()
        }),
        sif_trailer: b"x".to_vec(),
        enabled_publish_codecs: codecs(&[
            "video/VP8",
            "video/H264",
            "video/VP9",
            "video/AV1",
            "video/H265",
            "audio/PCMU",
            "audio/PCMA",
            "audio/opus",
            "audio/red",
        ]),
        fast_publish: true,
        ..Default::default()
    }
}

// ------------------------------------------------
// The tests

// A session started through the local router creates the room and joins the participant,
// which is sent the join response the original sends, then (its subscriber being the primary
// connection) the subscriber's offer and a fresh token.
#[tokio::test(flavor = "multi_thread")]
async fn test_start_session_creates_the_room_and_joins_the_participant() {
    let node = new_test_node(|_| {});

    for protocol in [16, 18] {
        let room = format!("room-{protocol}");
        let client = Client::start(&node, participant_init(&room, "alice", protocol));

        let jr = client.join_response().await;
        assert_eq!(
            expected_join(&room),
            masked_join(&jr),
            "protocol {protocol}"
        );

        client
            .wait_for("the subscriber's offer", |m| match m {
                livekit::signal_response::Message::Offer(_) => Some(()),
                _ => None,
            })
            .await;
        let token = client
            .wait_for("the token", |m| match m {
                livekit::signal_response::Message::RefreshToken(token) => Some(token.clone()),
                _ => None,
            })
            .await;
        assert!(!token.is_empty());

        // the room and the participant are the node's and the store's
        let r = node.rm.get_room(&room.as_str().into()).unwrap();
        assert_eq!(jr.room.as_ref().unwrap().sid, r.id().as_str());
        let p = node.participant(&room, "alice");
        assert_eq!(jr.participant.as_ref().unwrap().sid, p.id().as_str());
        let (stored, _) = node.store.load_room(&room.as_str().into(), false).unwrap();
        assert_eq!(r.id().as_str(), stored.sid);
        let stored = node
            .store
            .load_participant(&room.as_str().into(), &"alice".into())
            .unwrap();
        assert_eq!(p.id().as_str(), stored.sid);
    }

    assert_eq!(2, node.telemetry.room_started_call_count());
    assert_eq!(2, node.telemetry.participant_joined_call_count());
    let (room, participant, client_info, client_meta, should_send_event, _) =
        node.telemetry.participant_joined_args_for_call(1);
    assert_eq!("room-18", room.name);
    assert_eq!("alice", participant.identity);
    assert_eq!(18, client_info.unwrap().protocol);
    assert_eq!("testregion", client_meta.unwrap().region);
    assert!(should_send_event);
}

// A second participant's join is announced to the first; the second is told of the first in
// its join response.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_second_participant_is_announced_to_the_first() {
    let node = new_test_node(|_| {});

    let alice = Client::start(&node, participant_init("a-room", "alice", 16));
    let alice_sid = alice.join_response().await.participant.unwrap().sid;

    let bob = Client::start(&node, participant_init("a-room", "bob", 16));
    let jr = bob.join_response().await;
    let others: Vec<_> = jr
        .other_participants
        .iter()
        .map(|p| p.sid.clone())
        .collect();
    assert_eq!(vec![alice_sid], others);
    let bob_sid = jr.participant.unwrap().sid;

    alice
        .wait_for("bob in an update", |m| match m {
            livekit::signal_response::Message::Update(update)
                if update.participants.iter().any(|p| p.sid == bob_sid) =>
            {
                Some(())
            }
            _ => None,
        })
        .await;
    assert_eq!(
        2,
        node.rm
            .get_room(&"a-room".into())
            .unwrap()
            .get_participant_count()
    );
}

// A request the client writes is read from the request source by the session worker and
// handled by the participant: a track is added, and muted.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_signal_request_reaches_the_participant() {
    let node = new_test_node(|_| {});

    let alice = Client::start(&node, participant_init("a-room", "alice", 16));
    alice.join_response().await;

    alice.send(livekit::signal_request::Message::AddTrack(
        livekit::AddTrackRequest {
            cid: "mic".to_string(),
            name: "mic".to_string(),
            r#type: livekit::TrackType::Audio as i32,
            source: livekit::TrackSource::Microphone as i32,
            ..Default::default()
        },
    ));
    let track = alice
        .wait_for("the track published", |m| match m {
            livekit::signal_response::Message::TrackPublished(published)
                if published.cid == "mic" =>
            {
                published.track.clone()
            }
            _ => None,
        })
        .await;
    assert!(!track.muted);

    let p = node.participant("a-room", "alice");
    let track_id = livekit::TrackId::from(track.sid.as_str());
    assert!(!p.get_pending_track(&track_id).unwrap().muted);

    alice.send(livekit::signal_request::Message::Mute(
        livekit::MuteTrackRequest {
            sid: track.sid.clone(),
            muted: true,
        },
    ));
    // "the track was not muted"
    eventually_async(
        || p.get_pending_track(&track_id).unwrap().muted,
        WAIT,
        TEN_MS,
    )
    .await;
}

// Closing the client's end closes the request source: the participant, which never
// connected, is closed and leaves the room, and the room, empty past its timeout (the
// departure timeout, set to 0, once a participant has been there), is closed when the server
// closes its idle rooms; it is no longer the node's or the store's.
#[tokio::test(flavor = "multi_thread")]
async fn test_closing_the_source_closes_the_participant_and_the_room() {
    let node = new_test_node(|conf| conf.room.departure_timeout = 0);

    let alice = Client::start(&node, participant_init("a-room", "alice", 16));
    alice.join_response().await;
    let p = node.participant("a-room", "alice");

    // not while the participant is there
    node.rm.close_idle_rooms();
    assert!(node.rm.get_room(&"a-room".into()).is_some());
    assert!(node.rm.has_participants());

    alice.req_sink.close();
    // "the participant was not closed"
    eventually_async(|| p.is_closed(), WAIT, TEN_MS).await;
    assert_eq!(
        types::ParticipantCloseReason::SignalSourceClose,
        p.close_reason()
    );
    let room = node.rm.get_room(&"a-room".into()).unwrap();
    // "the participant did not leave the room"
    eventually_async(|| room.get_participant_count() == 0, WAIT, TEN_MS).await;
    assert_eq!(1, node.telemetry.participant_left_call_count());
    assert!(!node.rm.has_participants());

    // what the server does every second
    let rm = node.rm.clone();
    // "the room was not closed"
    eventually_async(
        move || {
            rm.close_idle_rooms();
            rm.get_room(&"a-room".into()).is_none()
        },
        WAIT,
        TEN_MS,
    )
    .await;
    assert!(room.is_closed());
    assert_eq!(
        Err(Error::RoomNotFound),
        node.store.load_room(&"a-room".into(), false).map(|_| ())
    );
    assert_eq!(1, node.telemetry.room_ended_call_count());
    let (_, reason) = node.telemetry.room_ended_args_for_call(0);
    assert_eq!(livekit::RoomEndReason::RoomEndIdleTimeout, reason);
}

// A session started again with the same identity, as a resume, keeps the participant and
// swaps its signalling: the new client is sent the reconnect response, the old one's
// response source is closed. A resume of a participant that is not there is answered with a
// leave and fails.
#[tokio::test(flavor = "multi_thread")]
async fn test_resume_with_the_same_identity() {
    let node = new_test_node(|_| {});

    let first = Client::start(&node, participant_init("a-room", "alice", 16));
    let sid = first.join_response().await.participant.unwrap().sid;

    let second = Client::start(
        &node,
        ParticipantInit {
            reconnect: true,
            reconnect_reason: livekit::ReconnectReason::RrSignalDisconnected,
            ..participant_init("a-room", "alice", 16)
        },
    );
    second
        .wait_for("the reconnect response", |m| match m {
            livekit::signal_response::Message::Reconnect(_) => Some(()),
            _ => None,
        })
        .await;
    second
        .wait_for("alice in an update", |m| match m {
            livekit::signal_response::Message::Update(update)
                if update.participants.iter().any(|p| p.sid == sid) =>
            {
                Some(())
            }
            _ => None,
        })
        .await;
    first.until_closed().await;

    let p = node.participant("a-room", "alice");
    assert_eq!(sid, p.id().as_str());
    assert!(!p.is_closed());
    assert_eq!(
        1,
        node.rm
            .get_room(&"a-room".into())
            .unwrap()
            .get_participant_count()
    );
    assert_eq!(1, node.telemetry.participant_resumed_call_count());
    assert_eq!(1, node.telemetry.participant_joined_call_count());

    // the session is the second client's: a request of it reaches the participant
    second.send(livekit::signal_request::Message::AddTrack(
        livekit::AddTrackRequest {
            cid: "mic".to_string(),
            name: "mic".to_string(),
            r#type: livekit::TrackType::Audio as i32,
            ..Default::default()
        },
    ));
    second
        .wait_for("the track published", |m| match m {
            livekit::signal_response::Message::TrackPublished(published)
                if published.cid == "mic" =>
            {
                Some(())
            }
            _ => None,
        })
        .await;

    // a resume of a participant that is not there
    let carol = Client::start(
        &node,
        ParticipantInit {
            reconnect: true,
            ..participant_init("a-room", "carol", 16)
        },
    );
    let leave = carol
        .wait_for("the leave", |m| match m {
            livekit::signal_response::Message::Leave(leave) => Some(leave.clone()),
            _ => None,
        })
        .await;
    assert_eq!(
        livekit::LeaveRequest {
            reason: livekit::DisconnectReason::StateMismatch as i32,
            action: livekit::leave_request::Action::Reconnect as i32,
            ..Default::default()
        },
        leave
    );
    carol.until_closed().await;
    assert!(
        node.rm
            .get_room(&"a-room".into())
            .unwrap()
            .get_participant(&"carol".into())
            .is_none()
    );
}

// The room-side operations of the room API, each once on a room with a connected
// participant, and with the original's errors for a room or a participant that is not there.
#[tokio::test(flavor = "multi_thread")]
async fn test_room_side_operations() {
    let node = new_test_node(|_| {});
    let room = "a-room".to_string();

    let alice = Client::start(
        &node,
        ParticipantInit {
            use_single_peer_connection: true,
            ..participant_init(&room, "alice", 18)
        },
    );
    let sid = alice.join_response().await.participant.unwrap().sid;
    let handler = alice.connect().await;
    let p = node.participant(&room, "alice");
    // "the participant did not become active"
    eventually_async(
        || p.state() == livekit::participant_info::State::Active,
        CONNECT_TIMEOUT,
        TEN_MS,
    )
    .await;

    // list
    let res = node
        .rm
        .list_participants(&livekit::ListParticipantsRequest { room: room.clone() })
        .unwrap();
    let sids: Vec<_> = res.participants.iter().map(|p| p.sid.clone()).collect();
    assert_eq!(vec![sid.clone()], sids);
    let err = node
        .rm
        .list_participants(&livekit::ListParticipantsRequest {
            room: "none".to_string(),
        })
        .unwrap_err();
    assert_eq!(Error::RoomNotFound, err);
    assert_eq!("requested room does not exist", err.to_string());
    assert_eq!(Some(http::StatusCode::NOT_FOUND), err.to_http());

    // get
    let identity = |room: &str, identity: &str| livekit::RoomParticipantIdentity {
        room: room.to_string(),
        identity: identity.to_string(),
        ..Default::default()
    };
    let info = node.rm.get_participant(&identity(&room, "alice")).unwrap();
    assert_eq!(sid, info.sid);
    assert_eq!(
        Err(Error::RoomNotFound),
        node.rm.get_participant(&identity("none", "alice"))
    );
    let err = node
        .rm
        .get_participant(&identity(&room, "none"))
        .unwrap_err();
    assert_eq!(Error::ParticipantNotFound, err);
    assert_eq!("participant does not exist", err.to_string());
    assert_eq!(Some(http::StatusCode::NOT_FOUND), err.to_http());

    // mute: a track the client has asked to publish
    alice.send(livekit::signal_request::Message::AddTrack(
        livekit::AddTrackRequest {
            cid: "mic".to_string(),
            name: "mic".to_string(),
            r#type: livekit::TrackType::Audio as i32,
            ..Default::default()
        },
    ));
    let track_sid = alice
        .wait_for("the track published", |m| match m {
            livekit::signal_response::Message::TrackPublished(published) => {
                published.track.as_ref().map(|t| t.sid.clone())
            }
            _ => None,
        })
        .await;
    let mute = |muted: bool| livekit::MuteRoomTrackRequest {
        room: room.clone(),
        identity: "alice".to_string(),
        track_sid: track_sid.clone(),
        muted,
    };
    let res = node.rm.mute_published_track(&mute(true)).unwrap();
    let track = res.track.unwrap();
    assert_eq!(track_sid, track.sid);
    assert!(track.muted);
    alice
        .wait_for("the mute", |m| match m {
            livekit::signal_response::Message::Mute(mute) if mute.sid == track_sid => {
                Some(mute.muted)
            }
            _ => None,
        })
        .await;
    let err = node.rm.mute_published_track(&mute(false)).unwrap_err();
    assert_eq!(Error::RemoteUnmuteNoteEnabled, err);
    assert_eq!("remote unmute not enabled", err.to_string());
    assert_eq!(
        Err(Error::ParticipantNotFound),
        node.rm
            .mute_published_track(&livekit::MuteRoomTrackRequest {
                identity: "none".to_string(),
                ..mute(true)
            })
    );
    // of a track that is not there: no track and no error, as the original answers
    let res = node
        .rm
        .mute_published_track(&livekit::MuteRoomTrackRequest {
            track_sid: "TR_none".to_string(),
            ..mute(true)
        })
        .unwrap();
    assert_eq!(None, res.track);

    // update
    let info = node
        .rm
        .update_participant(&livekit::UpdateParticipantRequest {
            room: room.clone(),
            identity: "alice".to_string(),
            metadata: "new metadata".to_string(),
            permission: Some(livekit::ParticipantPermission {
                can_subscribe: true,
                can_publish: false,
                can_publish_data: true,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
    assert_eq!("new metadata", info.metadata);
    assert!(!info.permission.unwrap().can_publish);
    assert!(!p.can_publish());
    assert_eq!(
        Err(Error::RoomNotFound),
        node.rm
            .update_participant(&livekit::UpdateParticipantRequest {
                room: "none".to_string(),
                identity: "alice".to_string(),
                ..Default::default()
            })
    );

    // send data: it arrives over the client's connection
    node.rm
        .send_data(&livekit::SendDataRequest {
            room: room.clone(),
            data: b"hello".to_vec(),
            kind: livekit::data_packet::Kind::Reliable as i32,
            topic: Some("a-topic".to_string()),
            ..Default::default()
        })
        .unwrap();
    let received = handler.clone();
    // "the data did not arrive"
    eventually_async(
        move || received.on_data_message_call_count() > 0,
        WAIT,
        TEN_MS,
    )
    .await;
    let (kind, data) = handler.on_data_message_args_for_call(0);
    assert_eq!(livekit::data_packet::Kind::Reliable, kind);
    let dp = livekit::DataPacket::decode(data.as_slice()).unwrap();
    let Some(livekit::data_packet::Value::User(user)) = dp.value else {
        panic!("not a user packet: {dp:?}");
    };
    assert_eq!(b"hello".to_vec(), user.payload);
    assert_eq!(Some("a-topic".to_string()), user.topic);
    assert_eq!(
        Err(Error::RoomNotFound),
        node.rm.send_data(&livekit::SendDataRequest {
            room: "none".to_string(),
            ..Default::default()
        })
    );

    // update metadata: the room returned is the updated one
    let res = node
        .rm
        .update_room_metadata(&livekit::UpdateRoomMetadataRequest {
            room: room.clone(),
            metadata: "room metadata".to_string(),
        })
        .unwrap();
    assert_eq!("room metadata", res.metadata);
    assert_eq!(
        Err(Error::RoomNotFound),
        node.rm
            .update_room_metadata(&livekit::UpdateRoomMetadataRequest {
                room: "none".to_string(),
                ..Default::default()
            })
    );

    // remove: the client is told to leave
    node.rm
        .remove_participant(&identity(&room, "alice"))
        .unwrap();
    alice
        .wait_for("the leave", |m| match m {
            livekit::signal_response::Message::Leave(_) => Some(()),
            _ => None,
        })
        .await;
    assert!(p.is_closed());
    assert_eq!(
        Err(Error::ParticipantNotFound),
        node.rm.get_participant(&identity(&room, "alice"))
    );
    assert_eq!(
        Err(Error::ParticipantNotFound),
        node.rm.remove_participant(&identity(&room, "alice"))
    );

    // delete: the room is closed and gone; a room that is not there is no error
    let r = node.rm.get_room(&room.as_str().into()).unwrap();
    node.rm
        .delete_room(&livekit::DeleteRoomRequest { room: room.clone() })
        .unwrap();
    assert!(r.is_closed());
    assert!(node.rm.get_room(&room.as_str().into()).is_none());
    let (_, reason) = node.telemetry.room_ended_args_for_call(0);
    assert_eq!(livekit::RoomEndReason::RoomEndApiDelete, reason);
    node.rm
        .delete_room(&livekit::DeleteRoomRequest {
            room: "none".to_string(),
        })
        .unwrap();
    assert_eq!(
        Err(Error::RoomNotFound),
        node.rm
            .list_participants(&livekit::ListParticipantsRequest { room })
    );
}
