// New for musivum: the original has no test of transportmanager.go. Every test runs a manager
// on real transports over the loopback interface, against a client side made of the same
// transports (each on a node of its own, as in transport_test.rs, whose helpers are private
// to transport_impl.rs and are written out again here).
//
// The expected values of the fallback decisions are what the Go original printed for the
// same inputs: a test file of its own (not kept in the repository) laid over `pkg/rtc` with
// `go test -overlay` made the three managers of `test_fallback_decisions_as_the_original`
// with the same params in the single-connection mode, set their publisher's connection type
// to UDP (as a connection over the loopback interface has it here), and for each row set
// the manager's fields as `run_failed_rows` and `run_loss_rows` do, ran
// `handleConnectionFailed` or `onMediaLossUpdate` of v1.13.11 and printed the fields
// after. Each row carries the name of its line of that output.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration as StdDuration;

use parking_lot::Mutex;

use super::*;
use crate::rtc::transport::{FakeHandler, Handler};
use crate::rtc::types::{FakeParticipantTelemetryListener, ParticipantTelemetryListener};
use crate::testutils::eventually_async;
use crate::webrtc::peerconnection::{DtlsRole, NodeConfig};

const SECOND: StdDuration = StdDuration::from_secs(1);
const TEN_MS: StdDuration = StdDuration::from_millis(10);

const ICT_NONE: i32 = livekit::IceCandidateType::IctNone as i32;
const ICT_TCP: i32 = livekit::IceCandidateType::IctTcp as i32;
const ICT_TLS: i32 = livekit::IceCandidateType::IctTls as i32;

// `&WebRTCConfig{}` of transport_test.rs: a configuration with nothing set that the transport
// reads.
fn web_rtc_config() -> Arc<WebRtcConfig> {
    static CONFIG: LazyLock<Arc<WebRtcConfig>> = LazyLock::new(|| {
        let mut conf = config::Config::new("", true).unwrap();
        conf.rtc.rtc_config.tcp_port = 0;
        let mut c = WebRtcConfig::new(&conf).unwrap();
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

fn new_node() -> Node {
    Node::new(NodeConfig::default()).unwrap()
}

// A transport of the client side, with Go's zero values where nothing is said.
fn client_params(
    handler: &Arc<FakeHandler>,
    transport: livekit::SignalTarget,
    is_offerer: bool,
) -> TransportParams {
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
        transport,
        sim_tracks: HashMap::new(),
        client_info: Default::default(),
        is_offerer,
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
        node: new_node(),
    }
}

// The manager's params, Go's zero values where nothing is said.
fn manager_params(
    publisher_handler: &Arc<FakeHandler>,
    subscriber_handler: &Arc<FakeHandler>,
) -> TransportManagerParams {
    TransportManagerParams {
        subscriber_as_primary: false,
        use_single_peer_connection: false,
        config: web_rtc_config(),
        twcc: None,
        protocol_version: Default::default(),
        congestion_control_config: Default::default(),
        enabled_subscribe_codecs: Vec::new(),
        enabled_publish_codecs: Vec::new(),
        sim_tracks: HashMap::new(),
        client_info: Default::default(),
        migration: false,
        allow_tcp_fallback: false,
        tcp_fallback_rtt_threshold: 0,
        allow_udp_unstable_fallback: false,
        turns_enabled: false,
        allow_playout_delay: false,
        data_channel_max_buffered_amount: 0,
        datachannel_slow_threshold: 0,
        datachannel_lossy_target_latency: Duration::ZERO,
        datachannel_data_track_target_latency: Duration::ZERO,
        logger: Default::default(),
        publisher_handler: publisher_handler.clone(),
        subscriber_handler: subscriber_handler.clone(),
        data_channel_stats: BytesTrackStats::new("", &"TR_data".into(), &"PA_1".into(), || {
            let l: Arc<dyn ParticipantTelemetryListener> =
                Arc::new(FakeParticipantTelemetryListener::default());
            l
        }),
        use_one_shot_signalling_mode: false,
        fire_on_track_by_sdp: false,
        enable_data_tracks: false,
        exclude_ipv6_local_candidates: false,
        enable_warp: false,
        node: new_node(),
    }
}

fn client_info(sdk: livekit::client_info::Sdk) -> ClientInfo {
    ClientInfo {
        client_info: Some(Arc::new(livekit::ClientInfo {
            sdk: sdk as i32,
            ..Default::default()
        })),
    }
}

// What the client's publisher creates before its first offer, as LiveKit's SDKs do: the two
// channels, in band, or negotiated with the ids of protocol 18 for a single connection.
fn create_client_data_channels(client: &PcTransport, negotiated: bool) {
    let id = |id: livekit::NegotiatedDataChannelId| negotiated.then_some(id as i32 as u16);
    client
        .create_data_channel(
            RELIABLE_DATA_CHANNEL,
            Some(DataChannelInit {
                ordered: Some(true),
                negotiated: Some(negotiated),
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
                negotiated: Some(negotiated),
                id: id(livekit::NegotiatedDataChannelId::NdciLossy),
                ..Default::default()
            }),
        )
        .unwrap();
}

// Candidates of one side handed to a transport of the other.
fn send_candidates_to(handler: &Arc<FakeHandler>, to: &Arc<PcTransport>) {
    let to = Arc::downgrade(to);
    handler.on_ice_candidate_calls(move |candidate, _target| {
        if let (Some(candidate), Some(to)) = (candidate, to.upgrade()) {
            to.add_ice_candidate(candidate.to_json().unwrap());
        }
        Ok(())
    });
}

// Candidates of the client handed to the manager with their target.
fn send_candidates_to_manager(handler: &Arc<FakeHandler>, to: &Arc<TransportManager>) {
    let to = Arc::downgrade(to);
    handler.on_ice_candidate_calls(move |candidate, target| {
        if let (Some(candidate), Some(to)) = (candidate, to.upgrade()) {
            to.add_ice_candidate(candidate.to_json().unwrap(), target);
        }
        Ok(())
    });
}

// The client's offers go to the manager (`HandleOffer`, pended while `pend` is set), the
// publisher's answers come back to the client through the participant's publisher handler.
fn route_publisher_negotiation(
    manager: &Arc<TransportManager>,
    publisher_handler: &Arc<FakeHandler>,
    client: &Arc<PcTransport>,
    client_handler: &Arc<FakeHandler>,
    pend: &Arc<AtomicBool>,
) {
    client_handler.on_offer_calls({
        let manager = Arc::downgrade(manager);
        let pend = pend.clone();
        move |offer, offer_id, _mid_to_track_id| {
            if let Some(manager) = manager.upgrade() {
                manager.handle_offer(offer, offer_id, pend.load(Ordering::SeqCst))?;
            }
            Ok(())
        }
    });
    publisher_handler.on_answer_calls({
        let client = Arc::downgrade(client);
        move |answer, answer_id, _mid_to_track_id| {
            if let Some(client) = client.upgrade() {
                let _ = client.handle_remote_description(answer, answer_id);
            }
            Ok(())
        }
    });
    send_candidates_to(publisher_handler, client);
    send_candidates_to_manager(client_handler, manager);
}

async fn until_fully_established(handlers: &[&Arc<FakeHandler>]) {
    // "transports did not become fully established"
    eventually_async(
        || {
            handlers
                .iter()
                .all(|h| h.on_fully_established_call_count() > 0)
        },
        10 * SECOND,
        TEN_MS,
    )
    .await;
}

// A manager in the single-connection mode with its client, connected.
struct SingleConnection {
    manager: Arc<TransportManager>,
    publisher_handler: Arc<FakeHandler>,
    subscriber_handler: Arc<FakeHandler>,
    client: Arc<PcTransport>,
    client_handler: Arc<FakeHandler>,
    ice_configs: Arc<Mutex<Vec<livekit::IceConfig>>>,
}

impl SingleConnection {
    async fn connect(change: impl FnOnce(&mut TransportManagerParams)) -> SingleConnection {
        let publisher_handler = Arc::new(FakeHandler::default());
        let subscriber_handler = Arc::new(FakeHandler::default());
        let mut params = manager_params(&publisher_handler, &subscriber_handler);
        params.use_single_peer_connection = true;
        params.protocol_version = types::ProtocolVersion(18);
        change(&mut params);
        let manager = TransportManager::new(params).unwrap();
        let ice_configs = Arc::new(Mutex::new(Vec::new()));
        manager.on_ice_config_changed(Box::new({
            let ice_configs = ice_configs.clone();
            move |ice_config| ice_configs.lock().push(*ice_config)
        }));

        let client_handler = Arc::new(FakeHandler::default());
        let client = PcTransport::new(client_params(
            &client_handler,
            livekit::SignalTarget::Publisher,
            true,
        ))
        .unwrap();
        create_client_data_channels(&client, true);
        route_publisher_negotiation(
            &manager,
            &publisher_handler,
            &client,
            &client_handler,
            &Arc::new(AtomicBool::new(false)),
        );

        client.negotiate(true);
        until_fully_established(&[&publisher_handler, &client_handler]).await;

        SingleConnection {
            manager,
            publisher_handler,
            subscriber_handler,
            client,
            client_handler,
            ice_configs,
        }
    }

    fn close(&self) {
        self.client.close();
        self.manager.close();
    }
}

// The two modes negotiate with a client and reach "fully established"; an offer of the
// client goes to the publisher transport and its answer comes back through the publisher
// handler; a data message sent through the manager arrives over the primary connection, on
// the channel of its kind.
#[tokio::test(flavor = "multi_thread")]
async fn test_two_connections_negotiate_route_and_send() {
    let publisher_handler = Arc::new(FakeHandler::default());
    let subscriber_handler = Arc::new(FakeHandler::default());
    let mut params = manager_params(&publisher_handler, &subscriber_handler);
    params.subscriber_as_primary = true;
    let manager = TransportManager::new(params).unwrap();
    assert!(manager.subscriber.is_some());

    let client_pub_handler = Arc::new(FakeHandler::default());
    let client_pub = PcTransport::new(client_params(
        &client_pub_handler,
        livekit::SignalTarget::Publisher,
        true,
    ))
    .unwrap();
    create_client_data_channels(&client_pub, false);
    let client_sub_handler = Arc::new(FakeHandler::default());
    let client_sub = PcTransport::new(client_params(
        &client_sub_handler,
        livekit::SignalTarget::Subscriber,
        false,
    ))
    .unwrap();

    // publisher: the client offers, the first offer pended as during a migration
    let pend = Arc::new(AtomicBool::new(true));
    route_publisher_negotiation(
        &manager,
        &publisher_handler,
        &client_pub,
        &client_pub_handler,
        &pend,
    );
    // subscriber: the manager offers through the subscriber handler, the client answers
    // through `HandleAnswer`
    subscriber_handler.on_offer_calls({
        let client_sub = Arc::downgrade(&client_sub);
        move |offer, offer_id, _mid_to_track_id| {
            if let Some(client_sub) = client_sub.upgrade() {
                let _ = client_sub.handle_remote_description(offer, offer_id);
            }
            Ok(())
        }
    });
    client_sub_handler.on_answer_calls({
        let manager = Arc::downgrade(&manager);
        move |answer, answer_id, _mid_to_track_id| {
            if let Some(manager) = manager.upgrade() {
                manager.handle_answer(answer, answer_id);
            }
            Ok(())
        }
    });
    send_candidates_to(&subscriber_handler, &client_sub);
    send_candidates_to_manager(&client_sub_handler, &manager);

    client_pub.negotiate(true);
    manager.negotiate_subscriber(true);

    // the client's offer is pended, the publisher has not seen it
    // "offer not pended"
    eventually_async(
        || manager.lock.read().pending_offer_publisher.is_some(),
        10 * SECOND,
        TEN_MS,
    )
    .await;
    assert_eq!(1, client_pub_handler.on_offer_call_count());
    assert!(manager.publisher.current_remote_description().is_none());
    assert_eq!(0, publisher_handler.on_answer_call_count());

    pend.store(false, Ordering::SeqCst);
    manager.process_pending_publisher_offer();
    assert!(manager.lock.read().pending_offer_publisher.is_none());

    until_fully_established(&[
        &publisher_handler,
        &subscriber_handler,
        &client_pub_handler,
        &client_sub_handler,
    ])
    .await;

    // the answer to the client's offer came through the publisher handler, the offer to the
    // client through the subscriber handler, and nothing else crossed
    assert_eq!(1, publisher_handler.on_answer_call_count());
    assert_eq!(0, publisher_handler.on_offer_call_count());
    assert_eq!(0, subscriber_handler.on_answer_call_count());
    assert!(subscriber_handler.on_offer_call_count() >= 1);
    assert!(manager.publisher.current_remote_description().is_some());
    assert!(manager.has_publisher_ever_connected());
    assert!(manager.has_subscriber_ever_connected());
    assert_eq!(2, manager.get_ice_connection_info().len());

    // the subscriber is the primary connection: data goes over it
    let data_track_transport = manager.get_data_track_transport();
    assert!(std::ptr::addr_eq(
        Arc::as_ptr(&data_track_transport),
        Arc::as_ptr(manager.subscriber.as_ref().unwrap())
    ));
    manager
        .send_data_message(livekit::data_packet::Kind::Reliable, b"reliable")
        .unwrap();
    manager
        .send_data_message(livekit::data_packet::Kind::Lossy, b"lossy")
        .unwrap();
    // "data messages did not arrive"
    eventually_async(
        || client_sub_handler.on_data_message_call_count() == 2,
        10 * SECOND,
        TEN_MS,
    )
    .await;
    let got: HashSet<_> = (0..2)
        .map(|i| client_sub_handler.on_data_message_args_for_call(i))
        .collect();
    assert_eq!(
        HashSet::from([
            (livekit::data_packet::Kind::Reliable, b"reliable".to_vec()),
            (livekit::data_packet::Kind::Lossy, b"lossy".to_vec()),
        ]),
        got
    );
    assert_eq!(0, client_pub_handler.on_data_message_call_count());

    client_pub.close();
    client_sub.close();
    manager.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_connection_negotiates_routes_and_sends() {
    let s = SingleConnection::connect(|_| {}).await;
    assert!(s.manager.subscriber.is_none());

    // one connection: the client's offer was answered through the publisher handler, and
    // nothing went through the subscriber handler
    assert_eq!(1, s.client_handler.on_offer_call_count());
    assert_eq!(1, s.publisher_handler.on_answer_call_count());
    assert_eq!(0, s.subscriber_handler.on_answer_call_count());
    assert_eq!(0, s.subscriber_handler.on_offer_call_count());
    assert_eq!(0, s.subscriber_handler.on_fully_established_call_count());
    assert!(s.manager.has_subscriber_ever_connected());
    assert_eq!(
        types::IceConnectionType::Udp,
        s.manager.get_lowest_priority_connection_type()
    );

    // over the channels the manager made as negotiated ones
    s.manager
        .send_data_message(livekit::data_packet::Kind::Reliable, b"reliable")
        .unwrap();
    // "data message did not arrive"
    eventually_async(
        || s.client_handler.on_data_message_call_count() == 1,
        10 * SECOND,
        TEN_MS,
    )
    .await;
    assert_eq!(
        (livekit::data_packet::Kind::Reliable, b"reliable".to_vec()),
        s.client_handler.on_data_message_args_for_call(0)
    );
    s.manager
        .send_data_message(livekit::data_packet::Kind::Lossy, b"lossy")
        .unwrap();
    // "data message did not arrive"
    eventually_async(
        || s.client_handler.on_data_message_call_count() == 2,
        10 * SECOND,
        TEN_MS,
    )
    .await;
    assert_eq!(
        (livekit::data_packet::Kind::Lossy, b"lossy".to_vec()),
        s.client_handler.on_data_message_args_for_call(1)
    );

    s.close();
}

// A connection that fails is reported upward and moves the ICE configuration on as the
// original's table says (row a1: UDP, short-lived, no preference yet, a client that can do
// ICE/TCP below the RTT threshold: TCP on both). The manager remembers that it reconfigured:
// a second failure changes nothing (row a8). The client's next attempt, an ICE restart after
// a short connection, starts the decision afresh (`HandleClientReconnect` resets it), and on
// a connection that is UDP again, with TCP preferred, the original goes back to no preference
// (row a3).
#[tokio::test(flavor = "multi_thread")]
async fn test_failed_connection_is_reported_and_changes_ice_config() {
    let s = SingleConnection::connect(|p| {
        p.allow_tcp_fallback = true;
        p.turns_enabled = true;
        p.client_info = client_info(livekit::client_info::Sdk::Js);
        p.tcp_fallback_rtt_threshold = 100;
        p.allow_udp_unstable_fallback = true;
    })
    .await;

    s.manager.update_last_seen_signal();
    s.client.close();

    // the participant sees signal meanwhile, as its pings keep coming
    // "failure not reported"
    let manager = s.manager.clone();
    let publisher_handler = s.publisher_handler.clone();
    tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now() + 40 * SECOND;
        while publisher_handler.on_failed_call_count() == 0 {
            assert!(std::time::Instant::now() < deadline, "failure not reported");
            manager.update_last_seen_signal();
            std::thread::sleep(StdDuration::from_millis(500));
        }
    })
    .await
    .unwrap();

    let (is_short_lived, info) = s.publisher_handler.on_failed_args_for_call(0);
    assert!(is_short_lived);
    assert_eq!(livekit::SignalTarget::Publisher, info.transport);
    assert_eq!(types::IceConnectionType::Udp, info.r#type);
    let tcp = livekit::IceConfig {
        preference_subscriber: ICT_TCP,
        preference_publisher: ICT_TCP,
    };
    assert_eq!(Some(tcp), s.manager.get_ice_config());
    assert_eq!(vec![tcp], *s.ice_configs.lock());
    assert!(s.manager.lock.read().is_transport_reconfigured);

    // a second failure of the same attempt: remembered, nothing changes
    let handler = TransportManagerTransportHandler {
        handler: s.publisher_handler.clone(),
        t: Arc::new(OnceLock::from(Arc::downgrade(&s.manager))),
        logger: Default::default(),
    };
    let before = s.publisher_handler.on_failed_call_count();
    handler.on_failed(true, &info);
    assert_eq!(before + 1, s.publisher_handler.on_failed_call_count());
    assert_eq!(Some(tcp), s.manager.get_ice_config());
    assert_eq!(1, s.ice_configs.lock().len());

    // the next attempt
    s.manager
        .handle_client_reconnect(livekit::ReconnectReason::RrPublisherFailed);
    let none = livekit::IceConfig::default();
    assert_eq!(Some(none), s.manager.get_ice_config());
    assert_eq!(vec![tcp, none], *s.ice_configs.lock());
    assert!(s.manager.lock.read().is_transport_reconfigured);

    s.close();
}

// One row of the table of `handleConnectionFailed`: the state before, what the original left.
struct FailedRow {
    name: &'static str,
    short: bool,
    pref: i32,
    failure_count: i64,
    last_failure_ago: Option<Duration>,
    reconfigured: bool,
    signal_ago: Duration,
    signal_invalid: bool,
    signaling_rtt: u32,
    // sub, pub, reconfigured, failureCount, lastFailure, changed
    expected: (i32, i32, bool, i64, &'static str, usize),
}

impl Default for FailedRow {
    fn default() -> Self {
        FailedRow {
            name: "",
            short: true,
            pref: ICT_NONE,
            failure_count: 0,
            last_failure_ago: None,
            reconfigured: false,
            signal_ago: Duration::ZERO,
            signal_invalid: false,
            signaling_rtt: 0,
            expected: (ICT_NONE, ICT_NONE, false, 0, "zero", 0),
        }
    }
}

fn last_failure_string(at: Time) -> &'static str {
    if at.is_zero() {
        "zero"
    } else if time::since(at) < time::SECOND {
        "now"
    } else {
        "earlier"
    }
}

fn run_failed_rows(s: &SingleConnection, rows: &[FailedRow]) {
    let t = &s.manager;
    for r in rows {
        s.ice_configs.lock().clear();
        {
            let mut inner = t.lock.write();
            inner.ice_config = livekit::IceConfig {
                preference_subscriber: r.pref,
                preference_publisher: r.pref,
            };
            inner.failure_count = r.failure_count;
            inner.last_failure = match r.last_failure_ago {
                Some(ago) => time::now().add(-ago),
                None => Time::ZERO,
            };
            inner.is_transport_reconfigured = r.reconfigured;
            inner.last_signal_at = time::now().add(-r.signal_ago);
            inner.signaling_rtt = r.signaling_rtt;
        }
        t.signal_source_valid.store(!r.signal_invalid);

        t.handle_connection_failed(r.short);

        let inner = t.lock.read();
        assert_eq!(
            r.expected,
            (
                inner.ice_config.preference_subscriber,
                inner.ice_config.preference_publisher,
                inner.is_transport_reconfigured,
                inner.failure_count,
                last_failure_string(inner.last_failure),
                s.ice_configs.lock().len(),
            ),
            "{}",
            r.name
        );
    }
}

// One row of the table of `onMediaLossUpdate`.
struct LossRow {
    name: &'static str,
    losses: usize,
    loss: u8,
    media_rtts: &'static [u32],
    signaling_rtt: u32,
    // udpRTT, unstableCount, sub, reconfigured, changed, onFailed
    expected: (u32, u32, i32, bool, usize, usize),
}

fn run_loss_rows(s: &SingleConnection, rows: &[LossRow]) {
    let t = &s.manager;
    for r in rows {
        s.ice_configs.lock().clear();
        let before = s.publisher_handler.on_failed_call_count();
        {
            let mut inner = t.lock.write();
            inner.ice_config = livekit::IceConfig::default();
            inner.is_transport_reconfigured = false;
            inner.udp_loss_unstable_count = 0;
            inner.udp_rtt = 0;
            inner.last_signal_at = time::now();
        }
        for rtt in r.media_rtts {
            t.update_media_rtt(*rtt);
        }
        t.update_signaling_rtt(r.signaling_rtt);
        for _ in 0..r.losses {
            t.on_media_loss_update(r.loss);
        }
        let inner = t.lock.read();
        assert_eq!(
            r.expected,
            (
                inner.udp_rtt,
                inner.udp_loss_unstable_count,
                inner.ice_config.preference_subscriber,
                inner.is_transport_reconfigured,
                s.ice_configs.lock().len(),
                s.publisher_handler.on_failed_call_count() - before,
            ),
            "{}",
            r.name
        );
    }
}

// The fallback decisions on connected managers (UDP), row by row against the Go original's
// output for the same rows (see the header). Group A: TCP fallback and TURN allowed, a JS
// client, an RTT threshold of 100 ms, the unstable-UDP fallback on. Group B: TCP fallback
// allowed, no TURN, a Go client (no ICE/TCP), no threshold. Group C: as A without the TCP
// fallback.
#[tokio::test(flavor = "multi_thread")]
async fn test_fallback_decisions_as_the_original() {
    let group_a = SingleConnection::connect(|p| {
        p.allow_tcp_fallback = true;
        p.turns_enabled = true;
        p.client_info = client_info(livekit::client_info::Sdk::Js);
        p.tcp_fallback_rtt_threshold = 100;
        p.allow_udp_unstable_fallback = true;
    })
    .await;
    let group_b = SingleConnection::connect(|p| {
        p.allow_tcp_fallback = true;
        p.turns_enabled = false;
        p.client_info = client_info(livekit::client_info::Sdk::Go);
    })
    .await;
    let group_c = SingleConnection::connect(|p| {
        p.allow_tcp_fallback = false;
        p.turns_enabled = true;
        p.client_info = client_info(livekit::client_info::Sdk::Js);
        p.tcp_fallback_rtt_threshold = 100;
        p.allow_udp_unstable_fallback = true;
    })
    .await;
    for s in [&group_a, &group_b, &group_c] {
        assert_eq!(
            types::IceConnectionType::Udp,
            s.manager.get_lowest_priority_connection_type()
        );
    }

    let ten_seconds = Some(Duration::from_secs(10));
    run_failed_rows(
        &group_a,
        &[
            FailedRow {
                name: "a1",
                expected: (ICT_TCP, ICT_TCP, true, 0, "zero", 1),
                ..Default::default()
            },
            FailedRow {
                name: "a2",
                signaling_rtt: 150,
                expected: (ICT_TLS, ICT_TLS, true, 0, "zero", 1),
                ..Default::default()
            },
            FailedRow {
                name: "a3",
                pref: ICT_TCP,
                expected: (ICT_NONE, ICT_NONE, true, 0, "zero", 1),
                ..Default::default()
            },
            FailedRow {
                name: "a4",
                pref: ICT_TLS,
                expected: (ICT_NONE, ICT_NONE, true, 0, "zero", 1),
                ..Default::default()
            },
            FailedRow {
                name: "a5",
                short: false,
                expected: (ICT_NONE, ICT_NONE, false, 1, "now", 0),
                ..Default::default()
            },
            FailedRow {
                name: "a6",
                short: false,
                failure_count: 1,
                last_failure_ago: ten_seconds,
                expected: (ICT_TCP, ICT_TCP, true, 0, "zero", 1),
                ..Default::default()
            },
            FailedRow {
                name: "a7",
                short: false,
                failure_count: 1,
                last_failure_ago: Some(Duration::from_secs(120)),
                expected: (ICT_NONE, ICT_NONE, false, 2, "now", 0),
                ..Default::default()
            },
            FailedRow {
                name: "a8",
                reconfigured: true,
                expected: (ICT_NONE, ICT_NONE, true, 0, "zero", 0),
                ..Default::default()
            },
            FailedRow {
                name: "a9",
                failure_count: 1,
                last_failure_ago: ten_seconds,
                signal_ago: Duration::from_secs(20),
                expected: (ICT_NONE, ICT_NONE, false, 0, "zero", 0),
                ..Default::default()
            },
            FailedRow {
                name: "a10",
                failure_count: 1,
                last_failure_ago: ten_seconds,
                signal_invalid: true,
                expected: (ICT_NONE, ICT_NONE, false, 0, "zero", 0),
                ..Default::default()
            },
            FailedRow {
                name: "a11",
                short: false,
                pref: ICT_TCP,
                failure_count: 1,
                last_failure_ago: ten_seconds,
                expected: (ICT_NONE, ICT_NONE, true, 0, "zero", 1),
                ..Default::default()
            },
        ],
    );
    run_failed_rows(
        &group_b,
        &[
            FailedRow {
                name: "b1",
                expected: (ICT_NONE, ICT_NONE, false, 0, "zero", 0),
                ..Default::default()
            },
            FailedRow {
                name: "b2",
                pref: ICT_TCP,
                expected: (ICT_NONE, ICT_NONE, true, 0, "zero", 1),
                ..Default::default()
            },
        ],
    );
    run_failed_rows(
        &group_c,
        &[FailedRow {
            name: "c1",
            failure_count: 1,
            last_failure_ago: ten_seconds,
            expected: (ICT_NONE, ICT_NONE, false, 1, "earlier", 0),
            ..Default::default()
        }],
    );

    run_loss_rows(
        &group_a,
        &[
            LossRow {
                name: "m1",
                losses: 20,
                loss: 63,
                media_rtts: &[60, 41],
                signaling_rtt: 64,
                expected: (51, 0, ICT_TCP, true, 1, 1),
            },
            LossRow {
                name: "m2",
                losses: 20,
                loss: 62,
                media_rtts: &[60, 41],
                signaling_rtt: 64,
                expected: (51, 0, ICT_NONE, false, 0, 0),
            },
            LossRow {
                name: "m3",
                losses: 20,
                loss: 63,
                media_rtts: &[50],
                signaling_rtt: 65,
                expected: (50, 0xfffff, ICT_NONE, false, 0, 0),
            },
            LossRow {
                name: "m4",
                losses: 20,
                loss: 63,
                media_rtts: &[50],
                signaling_rtt: 64,
                expected: (50, 0, ICT_TCP, true, 1, 1),
            },
            LossRow {
                name: "m5",
                losses: 20,
                loss: 63,
                media_rtts: &[100],
                signaling_rtt: 120,
                expected: (100, 0xfffff, ICT_NONE, false, 0, 0),
            },
            LossRow {
                name: "m6",
                losses: 19,
                loss: 255,
                media_rtts: &[60, 41],
                signaling_rtt: 64,
                expected: (51, 0x7ffff, ICT_NONE, false, 0, 0),
            },
            LossRow {
                name: "m7",
                losses: 33,
                loss: 63,
                media_rtts: &[],
                signaling_rtt: 1,
                expected: (0, 0xffffffff, ICT_NONE, false, 0, 0),
            },
        ],
    );
    run_loss_rows(
        &group_b,
        &[LossRow {
            name: "m8",
            losses: 20,
            loss: 255,
            media_rtts: &[60, 41],
            signaling_rtt: 64,
            expected: (51, 0, ICT_NONE, false, 0, 0),
        }],
    );

    for s in [&group_a, &group_b, &group_c] {
        s.close();
    }
}

// What a send that fails tells the participant: a closed channel goes to the primary
// connection's handler (`OnDataSendError`), a missing one only to the caller.
#[tokio::test(flavor = "multi_thread")]
async fn test_send_errors_are_handed_on() {
    let publisher_handler = Arc::new(FakeHandler::default());
    let subscriber_handler = Arc::new(FakeHandler::default());
    let mut params = manager_params(&publisher_handler, &subscriber_handler);
    params.subscriber_as_primary = true;
    let manager = TransportManager::new(params).unwrap();

    // nothing is open before negotiation: the channel is not available
    assert_eq!(
        Err(Error::DataChannelUnavailable),
        manager.send_data_message(livekit::data_packet::Kind::Reliable, b"x")
    );
    assert_eq!(0, subscriber_handler.on_data_send_error_call_count());

    let closed = Error::DataChannelWriter(datachannel::Error::Channel(webrtc::Error::ClosedPipe));
    for (res, to_handler) in [
        (Err(closed.clone()), true),
        (
            Err(Error::Wrap {
                message: "wrapped",
                cause: Box::new(Error::Webrtc(webrtc::Error::StreamClosed)),
            }),
            true,
        ),
        (Err(Error::TransportFailure), false),
        (Err(Error::DataChannelBufferFull), false),
        (Ok(()), false),
    ] {
        let before = subscriber_handler.on_data_send_error_call_count();
        assert_eq!(res, manager.handle_send_data_result(res.clone(), "kind", 1));
        assert_eq!(
            usize::from(to_handler),
            subscriber_handler.on_data_send_error_call_count() - before,
            "{res:?}"
        );
    }
    assert_eq!(0, publisher_handler.on_data_send_error_call_count());
    assert_eq!(
        closed,
        subscriber_handler.on_data_send_error_args_for_call(0)
    );
    // counted: the error that is not one of the expected ones
    assert_eq!(1, manager.data_channel_send_error_count.load());

    manager.close();
}
