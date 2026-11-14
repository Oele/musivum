// Ported from LiveKit, pkg/rtc/transport_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Every transport is on a node of its own (on the
// loopback interface, a port of the system's choosing), where the original's has sockets of
// its own; a plain pion peer connection of a test is a connection of musivum's layer.
// `&WebRTCConfig{}` is a configuration with nothing set that the transport reads
// (`web_rtc_config`); `registerCodecs(&me, codecs, fb, false)` is `create_media_engine` with
// that feedback and nothing else, which registers the same (`register_codecs` is private to
// mediaengine.rs). pion's default direction of `AddTransceiverFromKind` without an init is
// written out (send and receive). What a fake's stub sees is recorded and asserted on the
// test's thread, where the original calls `require` inside the stub. `untilTransportsConnected`
// waits at most `CONNECT_TIMEOUT` where the original's wait group waits for ever.
// `TestDTLSHandshakeSkipsHelloVerify` is ignored: the layer has no hook on the DTLS
// ClientHello and `rtc` no setting to skip HelloVerify. `TestConfigureAudioTransceiver` has
// an assertion more than the original, that an Opus codec was looked at, without which it
// would pass without testing anything.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration as StdDuration;

use parking_lot::Mutex;

use super::*;
use crate::rtc::transport::FakeHandler;
use crate::testutils::{CONNECT_TIMEOUT, eventually, eventually_async};
use crate::webrtc::peerconnection::{DtlsRole, NodeConfig, SignalingState};
use crate::webrtc::{MIME_TYPE_H264, MIME_TYPE_OPUS, MIME_TYPE_VP8};

const SECOND: StdDuration = StdDuration::from_secs(1);
const TEN_MS: StdDuration = StdDuration::from_millis(10);

// `&WebRTCConfig{}`: a configuration of the node with nothing set that the transport reads.
pub(super) fn web_rtc_config() -> Arc<WebRtcConfig> {
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

// A node of its own, as a pion connection has sockets of its own.
pub(super) fn new_node() -> Node {
    Node::new(NodeConfig::default()).unwrap()
}

// `TransportParams{Config: &WebRTCConfig{}, Handler: handler}`: Go's zero values for the rest.
pub(super) fn transport_params(handler: &Arc<FakeHandler>) -> TransportParams {
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
        transport: Default::default(),
        sim_tracks: HashMap::new(),
        client_info: Default::default(),
        is_offerer: false,
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

// `webrtc.NewAPI(webrtc.WithMediaEngine(me)).NewPeerConnection(webrtc.Configuration{})`, on a
// node of its own.
pub(super) fn new_peer_connection_with(me: MediaEngine) -> PeerConnection {
    PeerConnection::new(
        &new_node(),
        Configuration {
            media_engine: me,
            ..Default::default()
        },
    )
    .unwrap()
}

// `registerCodecs(&me, codecs, rtcpFeedback, false)`
pub(super) fn register_codecs(
    codecs: &[livekit::Codec],
    rtcp_feedback: RtcpFeedbackConfig,
) -> MediaEngine {
    create_media_engine(
        codecs,
        &DirectionConfig {
            rtcp_feedback,
            ..Default::default()
        },
        false,
    )
    .unwrap()
}

pub(super) fn codec(mime: &str) -> livekit::Codec {
    livekit::Codec {
        mime: mime.to_string(),
        ..Default::default()
    }
}

// pion's default direction of `AddTransceiverFromKind` without an init
pub(super) fn sendrecv() -> RtpTransceiverInit {
    RtpTransceiverInit {
        direction: RtpTransceiverDirection::Sendrecv,
    }
}

fn nack() -> webrtc::RtcpFeedback {
    webrtc::RtcpFeedback {
        r#type: webrtc::TYPE_RTCP_FB_NACK.to_string(),
        parameter: String::new(),
    }
}

// What a transport's negotiation state was set to last, as `OnNegotiationStateChanged` tells.
fn watch_negotiation_state(t: &PcTransport) -> Arc<Mutex<Option<NegotiationState>>> {
    let negotiation_state = Arc::new(Mutex::new(None));
    t.on_negotiation_state_changed(Some(Box::new({
        let negotiation_state = negotiation_state.clone();
        move |state| *negotiation_state.lock() = Some(state)
    })));
    negotiation_state
}

// New for musivum: the counters LiveKit's transport reports to (`RecordPeerConnectionState`,
// `RecordServiceOperationSuccess`) are reached when two transports negotiate and connect.
// The counters are shared by every test of the crate, which run at the same time: each is
// compared before and after, and only "at least" is asserted.
#[tokio::test(flavor = "multi_thread")]
async fn test_negotiation_reports_to_the_counters() {
    use crate::telemetry::prometheus::{PROM_PEER_CONNECTION, PROM_SERVICE_OPERATION_COUNTER};
    let state = |transport: livekit::SignalTarget, state: &str| {
        PROM_PEER_CONNECTION
            .with_label_values([transport.as_str_name(), state])
            .get()
    };
    let success = |op: &str| {
        PROM_SERVICE_OPERATION_COUNTER
            .with_label_values([op, "success", ""])
            .get()
    };

    let handler_a = Arc::new(FakeHandler::default());
    let mut params_a = transport_params(&handler_a);
    params_a.is_offerer = true;
    params_a.transport = livekit::SignalTarget::Subscriber;
    let transport_a = PcTransport::new(params_a).unwrap();
    transport_a
        .pc
        .create_data_channel(RELIABLE_DATA_CHANNEL, None)
        .unwrap();

    let handler_b = Arc::new(FakeHandler::default());
    let mut params_b = transport_params(&handler_b);
    params_b.transport = livekit::SignalTarget::Publisher;
    let transport_b = PcTransport::new(params_b).unwrap();

    let before = (
        success("offer"),
        success("answer"),
        success("peer_connection"),
        state(livekit::SignalTarget::Subscriber, "started"),
        state(livekit::SignalTarget::Subscriber, "connected"),
        state(livekit::SignalTarget::Publisher, "connected"),
    );

    handle_ice_exchange(&transport_a, &transport_b, &handler_a, &handler_b);
    connect_transports(
        &transport_a,
        &transport_b,
        &handler_a,
        &handler_b,
        false,
        1,
        1,
    )
    .await;

    let after = (
        success("offer"),
        success("answer"),
        success("peer_connection"),
        state(livekit::SignalTarget::Subscriber, "started"),
        state(livekit::SignalTarget::Subscriber, "connected"),
        state(livekit::SignalTarget::Publisher, "connected"),
    );
    assert!(after.0 > before.0, "offer: {before:?} -> {after:?}");
    assert!(after.1 > before.1, "answer: {before:?} -> {after:?}");
    assert!(
        after.2 >= before.2 + 2,
        "peer_connection: {before:?} -> {after:?}"
    );
    assert!(after.3 > before.3, "started: {before:?} -> {after:?}");
    assert!(
        after.4 > before.4,
        "subscriber connected: {before:?} -> {after:?}"
    );
    assert!(
        after.5 > before.5,
        "publisher connected: {before:?} -> {after:?}"
    );

    transport_a.close();
    transport_b.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_missing_answer_during_ice_restart() {
    let handler_a = Arc::new(FakeHandler::default());
    let mut params_a = transport_params(&handler_a);
    params_a.is_offerer = true;
    let transport_a = PcTransport::new(params_a).unwrap();
    transport_a
        .pc
        .create_data_channel(RELIABLE_DATA_CHANNEL, None)
        .unwrap();

    let handler_b = Arc::new(FakeHandler::default());
    let params_b = transport_params(&handler_b);
    let transport_b = PcTransport::new(params_b).unwrap();

    // exchange ICE
    handle_ice_exchange(&transport_a, &transport_b, &handler_a, &handler_b);

    connect_transports(
        &transport_a,
        &transport_b,
        &handler_a,
        &handler_b,
        false,
        1,
        1,
    )
    .await;
    assert!(ice_connected(&transport_a.pc));
    assert!(ice_connected(&transport_b.pc));

    let negotiation_state = watch_negotiation_state(&transport_a);

    // offer again, but missed
    let offer_received = Arc::new(AtomicBool::new(false));
    let seen = Arc::new(Mutex::new(None));
    handler_a.on_offer_calls({
        let transport_a = Arc::downgrade(&transport_a);
        let negotiation_state = negotiation_state.clone();
        let offer_received = offer_received.clone();
        let seen = seen.clone();
        move |_sd, _offer_id, _mid_to_track_id| {
            if let Some(transport_a) = transport_a.upgrade() {
                *seen.lock() = Some((transport_a.pc.signaling_state(), *negotiation_state.lock()));
            }
            offer_received.store(true, Ordering::SeqCst);
            Ok(())
        }
    });
    transport_a.negotiate(true);
    // "transportA offer not received"
    eventually_async(
        || offer_received.load(Ordering::SeqCst),
        10 * SECOND,
        TEN_MS,
    )
    .await;
    assert_eq!(
        Some((
            SignalingState::HaveLocalOffer,
            Some(NegotiationState::Remote)
        )),
        *seen.lock()
    );

    connect_transports(
        &transport_a,
        &transport_b,
        &handler_a,
        &handler_b,
        true,
        1,
        1,
    )
    .await;
    assert!(ice_connected(&transport_a.pc));
    assert!(ice_connected(&transport_b.pc));

    transport_a.close();
    transport_b.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_negotiation_timing() {
    let handler_a = Arc::new(FakeHandler::default());
    let mut params_a = transport_params(&handler_a);
    params_a.is_offerer = true;
    let transport_a = PcTransport::new(params_a).unwrap();
    transport_a
        .pc
        .create_data_channel(LOSSY_DATA_CHANNEL, None)
        .unwrap();

    let handler_b = Arc::new(FakeHandler::default());
    let transport_b = PcTransport::new(transport_params(&handler_b)).unwrap();

    assert!(!transport_a.is_established());
    assert!(!transport_b.is_established());

    handle_ice_exchange(&transport_a, &transport_b, &handler_a, &handler_b);
    let first_offer = Arc::new(Mutex::new(None));
    let first_offer_id = Arc::new(AtomicU32::new(0));
    let second_offer = Arc::new(Mutex::new(None));
    handler_a.on_offer_calls({
        let first_offer = first_offer.clone();
        let first_offer_id = first_offer_id.clone();
        let second_offer = second_offer.clone();
        move |sd: SessionDescription, offer_id, _mid_to_track_id| {
            let mut first = first_offer.lock();
            if first.is_none() {
                *first = Some(sd);
                first_offer_id.store(offer_id, Ordering::SeqCst);
            } else {
                *second_offer.lock() = Some(sd);
            }
            Ok(())
        }
    });

    let negotiation_state = watch_negotiation_state(&transport_a);
    let state_is = |want: NegotiationState| {
        let negotiation_state = negotiation_state.clone();
        move || *negotiation_state.lock() == Some(want)
    };

    // initial offer
    transport_a.negotiate(true);
    // "negotiation state does not match NegotiateStateRemote"
    eventually_async(state_is(NegotiationState::Remote), 10 * SECOND, TEN_MS).await;

    // second try, should've flipped transport status to retry
    transport_a.negotiate(true);
    // "negotiation state does not match NegotiateStateRetry"
    eventually_async(state_is(NegotiationState::Retry), 10 * SECOND, TEN_MS).await;

    // third try, should've stayed at retry
    transport_a.negotiate(true);
    tokio::time::sleep(StdDuration::from_millis(100)).await; // some time to process the negotiate event
    // "negotiation state does not match NegotiateStateRetry"
    eventually_async(state_is(NegotiationState::Retry), 10 * SECOND, TEN_MS).await;

    // "first offer not received yet"
    eventually_async(
        || {
            if first_offer.lock().is_none() {
                return false;
            }
            if first_offer_id.load(Ordering::SeqCst) == 0 {
                return false;
            }
            true
        },
        10 * SECOND,
        TEN_MS,
    )
    .await;

    handler_b.on_answer_calls({
        let transport_a = Arc::downgrade(&transport_a);
        move |answer, answer_id, _mid_to_track_id| {
            if let Some(transport_a) = transport_a.upgrade() {
                let _ = transport_a.handle_remote_description(answer, answer_id);
            }
            Ok(())
        }
    });
    let offer = first_offer.lock().clone().unwrap();
    let _ = transport_b.handle_remote_description(offer, first_offer_id.load(Ordering::SeqCst));

    // "transportA is not established"
    eventually_async(|| transport_a.is_established(), 10 * SECOND, TEN_MS).await;
    // "transportB is not established"
    eventually_async(|| transport_b.is_established(), 10 * SECOND, TEN_MS).await;

    // offerer should send another offer after processing the answer
    // as there were forced negotiations a couple of time above
    // "negotiation state does not match NegotiateStateRemote"
    eventually_async(state_is(NegotiationState::Remote), 10 * SECOND, TEN_MS).await;
    assert!(second_offer.lock().is_some());

    transport_a.close();
    transport_b.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_first_negotiation_uses_fast_debounce() {
    let (offered_tx, mut offered) = tokio::sync::mpsc::channel::<&'static str>(2);
    let mut transports = Vec::new();
    let mut new_offerer = |name: &'static str| {
        let handler = Arc::new(FakeHandler::default());
        let offered_tx = offered_tx.clone();
        handler.on_offer_calls(move |_, _, _| {
            let _ = offered_tx.try_send(name);
            Ok(())
        });
        let mut params = transport_params(&handler);
        params.is_offerer = true;
        let transport = PcTransport::new(params).unwrap();
        // `t.Cleanup(transport.Close)`: at the end of the test
        transports.push(transport.clone());
        transport
            .pc
            .create_data_channel(RELIABLE_DATA_CHANNEL, None)
            .unwrap();
        transport
    };

    // a transport that negotiated just now waits for the full debounce
    let recent = new_offerer("recent");
    recent.lock.write().update_last_negotiate_locked();
    recent.negotiate(false);

    // a new transport starts later, but its first offer uses the fast debounce and goes out first
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    new_offerer("fresh").negotiate(false);

    for want in ["fresh", "recent"] {
        match tokio::time::timeout(5 * SECOND, offered.recv()).await {
            Ok(Some(got)) => assert_eq!(want, got),
            _ => panic!("no offer from {want}"),
        }
    }

    for transport in transports {
        transport.close();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "the connection layer has no hook on the DTLS ClientHello (pion's \
            SetDTLSClientHelloMessageHook) and rtc no setting to skip HelloVerify \
            (SetDTLSInsecureSkipHelloVerify)"]
async fn test_dtls_handshake_skips_hello_verify() {
    // the SFU offers as on the subscriber connection, and a plain pion peer answers active like a browser,
    // so the SFU is the DTLS server and only the peer sends ClientHello
    let handler = Arc::new(FakeHandler::default());
    let mut params = transport_params(&handler);
    params.is_offerer = true;
    let sfu = PcTransport::new(params).unwrap();
    sfu.pc
        .create_data_channel(RELIABLE_DATA_CHANNEL, None)
        .unwrap();

    // `se.SetDTLSClientHelloMessageHook(...)`, which counts the ClientHellos the peer sends:
    // the layer has no such hook, so nothing counts them
    let client_hellos = AtomicI32::new(0);
    let mut se = SettingEngine::default();
    se.set_answering_dtls_role(DtlsRole::Client).unwrap();
    let client = PeerConnection::new(
        &new_node(),
        Configuration {
            setting_engine: se,
            ..Default::default()
        },
    )
    .unwrap();

    let state = Arc::new(Mutex::new((Vec::new(), false)));
    handler.on_ice_candidate_calls({
        let state = state.clone();
        let client = client.clone();
        move |c, _| {
            let Some(c) = c else {
                return Ok(());
            };
            let mut state = state.lock();
            let (pending, has_offer) = &mut *state;
            if !*has_offer {
                pending.push(c.to_json().unwrap());
                return Ok(());
            }
            Ok(client.add_ice_candidate(c.to_json().unwrap())?)
        }
    });
    client.on_ice_candidate({
        let sfu = Arc::downgrade(&sfu);
        move |c| {
            if let (Some(c), Some(sfu)) = (c, sfu.upgrade()) {
                sfu.add_ice_candidate(c.to_json().unwrap());
            }
        }
    });
    handler.on_offer_calls({
        let state = state.clone();
        let client = client.clone();
        let sfu = Arc::downgrade(&sfu);
        move |offer, offer_id, _| {
            {
                let mut state = state.lock();
                client.set_remote_description(offer)?;
                state.1 = true;
                for c in state.0.drain(..) {
                    let _ = client.add_ice_candidate(c);
                }
            }

            let answer = client.create_answer()?;
            client.set_local_description(answer.clone())?;
            match sfu.upgrade() {
                Some(sfu) => sfu.handle_remote_description(answer, offer_id),
                None => Ok(()),
            }
        }
    });
    sfu.negotiate(true);

    // "transports did not connect"
    eventually_async(
        || sfu.is_established() && client.connection_state() == PeerConnectionState::Connected,
        10 * SECOND,
        TEN_MS,
    )
    .await;

    // a HelloVerifyRequest from the SFU makes the peer send its ClientHello a second time, with the cookie
    assert_eq!(1, client_hellos.load(Ordering::SeqCst));

    sfu.close();
    let _ = client.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_first_offer_missed_during_ice_restart() {
    let handler_a = Arc::new(FakeHandler::default());
    let mut params_a = transport_params(&handler_a);
    params_a.is_offerer = true;
    let transport_a = PcTransport::new(params_a).unwrap();
    transport_a
        .pc
        .create_data_channel(RELIABLE_DATA_CHANNEL, None)
        .unwrap();

    let handler_b = Arc::new(FakeHandler::default());
    let transport_b = PcTransport::new(transport_params(&handler_b)).unwrap();

    // exchange ICE
    handle_ice_exchange(&transport_a, &transport_b, &handler_a, &handler_b);

    // first offer missed
    let first_offer_received = Arc::new(AtomicBool::new(false));
    handler_a.on_offer_calls({
        let first_offer_received = first_offer_received.clone();
        move |_sd, _offer_id, _mid_to_track_id| {
            first_offer_received.store(true, Ordering::SeqCst);
            Ok(())
        }
    });
    transport_a.negotiate(true);
    // "first offer not received"
    eventually_async(
        || first_offer_received.load(Ordering::SeqCst),
        10 * SECOND,
        TEN_MS,
    )
    .await;

    // set offer/answer with restart ICE, will negotiate twice,
    // first one is recover from missed offer
    // second one is restartICE
    handler_b.on_answer_calls({
        let transport_a = Arc::downgrade(&transport_a);
        move |answer, answer_id, _mid_to_track_id| {
            if let Some(transport_a) = transport_a.upgrade() {
                let _ = transport_a.handle_remote_description(answer, answer_id);
            }
            Ok(())
        }
    });

    let offer_count = Arc::new(AtomicI32::new(0));
    handler_a.on_offer_calls({
        let transport_b = Arc::downgrade(&transport_b);
        let offer_count = offer_count.clone();
        move |sd, offer_id, _mid_to_track_id| {
            offer_count.fetch_add(1, Ordering::SeqCst);
            let Some(transport_b) = transport_b.upgrade() else {
                return Ok(());
            };

            // the second offer is a ice restart offer, so we wait transportB complete the ice gathering
            if transport_b.pc.ice_gathering_state() == IceGatheringState::Gathering {
                eventually(
                    || transport_b.pc.ice_gathering_state() == IceGatheringState::Complete,
                    10 * SECOND,
                    TEN_MS,
                );
            }

            let _ = transport_b.handle_remote_description(sd, offer_id);
            Ok(())
        }
    });

    // first establish connection
    let _ = transport_a.ice_restart();

    // ensure we are connected
    // "transport did not connect"
    eventually_async(
        || {
            ice_connected(&transport_a.pc)
                && ice_connected(&transport_b.pc)
                && offer_count.load(Ordering::SeqCst) == 2
        },
        CONNECT_TIMEOUT,
        TEN_MS,
    )
    .await;

    transport_a.close();
    transport_b.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_first_answer_missed_during_ice_restart() {
    let handler_a = Arc::new(FakeHandler::default());
    let mut params_a = transport_params(&handler_a);
    params_a.is_offerer = true;
    let transport_a = PcTransport::new(params_a).unwrap();
    transport_a
        .pc
        .create_data_channel(LOSSY_DATA_CHANNEL, None)
        .unwrap();

    let handler_b = Arc::new(FakeHandler::default());
    let transport_b = PcTransport::new(transport_params(&handler_b)).unwrap();

    // exchange ICE
    handle_ice_exchange(&transport_a, &transport_b, &handler_a, &handler_b);

    // first answer missed
    let first_answer_received = Arc::new(AtomicBool::new(false));
    handler_b.on_answer_calls({
        let transport_a = Arc::downgrade(&transport_a);
        let first_answer_received = first_answer_received.clone();
        move |sd, answer_id, _mid_to_track_id| {
            if first_answer_received.load(Ordering::SeqCst) {
                if let Some(transport_a) = transport_a.upgrade() {
                    let _ = transport_a.handle_remote_description(sd, answer_id);
                }
            } else {
                // do not send first answer so that remote misses the first answer
                first_answer_received.store(true, Ordering::SeqCst);
            }
            Ok(())
        }
    });
    handler_a.on_offer_calls({
        let transport_b = Arc::downgrade(&transport_b);
        move |sd, offer_id, _mid_to_track_id| {
            if let Some(transport_b) = transport_b.upgrade() {
                let _ = transport_b.handle_remote_description(sd, offer_id);
            }
            Ok(())
        }
    });

    transport_a.negotiate(true);
    // "transportB signaling state did not go to stable"
    eventually_async(
        || {
            transport_b.pc.signaling_state() == SignalingState::Stable
                && first_answer_received.load(Ordering::SeqCst)
        },
        SECOND,
        TEN_MS,
    )
    .await;

    // set offer/answer with restart ICE, will negotiate twice,
    // first one is recover from missed offer
    // second one is restartICE
    let offer_count = Arc::new(AtomicI32::new(0));
    handler_a.on_offer_calls({
        let transport_b = Arc::downgrade(&transport_b);
        let offer_count = offer_count.clone();
        move |sd, offer_id, _mid_to_track_id| {
            offer_count.fetch_add(1, Ordering::SeqCst);
            let Some(transport_b) = transport_b.upgrade() else {
                return Ok(());
            };

            // the second offer is a ice restart offer, so we wait for transportB to complete ICE gathering
            if transport_b.pc.ice_gathering_state() == IceGatheringState::Gathering {
                eventually(
                    || transport_b.pc.ice_gathering_state() == IceGatheringState::Complete,
                    10 * SECOND,
                    TEN_MS,
                );
            }

            let _ = transport_b.handle_remote_description(sd, offer_id);
            Ok(())
        }
    });

    // first establish connection
    let _ = transport_a.ice_restart();

    // ensure we are connected
    // "transport did not connect"
    eventually_async(
        || {
            ice_connected(&transport_a.pc)
                && ice_connected(&transport_b.pc)
                && offer_count.load(Ordering::SeqCst) == 2
        },
        CONNECT_TIMEOUT,
        TEN_MS,
    )
    .await;

    transport_a.close();
    transport_b.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_negotiation_failed() {
    let handler_a = Arc::new(FakeHandler::default());
    let mut params_a = transport_params(&handler_a);
    params_a.is_offerer = true;
    let transport_a = PcTransport::new(params_a).unwrap();
    transport_a
        .pc
        .create_data_channel(RELIABLE_DATA_CHANNEL, None)
        .unwrap();

    let handler_b = Arc::new(FakeHandler::default());
    let transport_b = PcTransport::new(transport_params(&handler_b)).unwrap();

    // exchange ICE
    handle_ice_exchange(&transport_a, &transport_b, &handler_a, &handler_b);

    // wait for transport to be connected before maiming the signalling channel
    connect_transports(
        &transport_a,
        &transport_b,
        &handler_a,
        &handler_b,
        false,
        1,
        1,
    )
    .await;

    // reset OnOffer to force a negotiation failure
    handler_a.on_offer_calls(|_sd, _offer_id, _mid_to_track_id| Ok(()));
    let failed = Arc::new(AtomicI32::new(0));
    handler_a.on_negotiation_failed_calls({
        let failed = failed.clone();
        move || {
            failed.fetch_add(1, Ordering::SeqCst);
        }
    });
    transport_a.negotiate(true);
    // "negotiation failed"
    eventually_async(
        || failed.load(Ordering::SeqCst) == 1,
        (NEGOTIATION_FAILED_TIMEOUT + time::SECOND).to_std(),
        TEN_MS,
    )
    .await;

    transport_a.close();
    // as the original, which leaves it to the end of the test
    transport_b.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filtering_candidates() {
    let handler = Arc::new(FakeHandler::default());
    let mut params = transport_params(&handler);
    params.enabled_publish_codecs = vec![
        codec(MIME_TYPE_OPUS),
        codec(MIME_TYPE_VP8),
        codec(MIME_TYPE_H264),
    ];
    let transport = PcTransport::new(params).unwrap();

    transport
        .pc
        .create_data_channel(RELIABLE_DATA_CHANNEL, None)
        .unwrap();

    transport
        .pc
        .add_transceiver_from_kind(RtpCodecType::Audio, Some(sendrecv()))
        .unwrap();

    transport
        .pc
        .add_transceiver_from_kind(RtpCodecType::Video, Some(sendrecv()))
        .unwrap();

    let offer = transport.pc.create_offer(None).unwrap();

    // `webrtc.GatheringCompletePromise(transport.pc)`: gathering is complete when the local
    // description has been set
    transport.pc.set_local_description(offer).unwrap();
    assert_eq!(
        IceGatheringState::Complete,
        transport.pc.ice_gathering_state()
    );

    // should not filter out UDP candidates if TCP is not preferred
    let mut offer = transport.pc.local_description().unwrap();
    let filtered_offer = transport.filter_candidates(offer.clone(), false, true);
    assert_eq!(offer.sdp, filtered_offer.sdp);

    let mut parsed = offer.unmarshal().unwrap();

    // add a couple of TCP candidates
    let mut done = false;
    for m in &mut parsed.media_descriptions {
        let has_candidate = m
            .attributes
            .iter()
            .any(|a| a.key == sdp::ATTR_KEY_CANDIDATE);
        if has_candidate {
            for idx in 0..m.attributes.len() {
                if m.attributes[idx].key == sdp::ATTR_KEY_END_OF_CANDIDATES {
                    let mut modified_attributes = m.attributes[..idx].to_vec();
                    modified_attributes.extend([
                        sdp::Attribute {
                            key: sdp::ATTR_KEY_CANDIDATE.to_string(),
                            value: Some(
                                "054225987 1 tcp 2124414975 159.203.70.248 7881 typ host tcptype passive"
                                    .to_string(),
                            ),
                        },
                        sdp::Attribute {
                            key: sdp::ATTR_KEY_CANDIDATE.to_string(),
                            value: Some(
                                "054225987 2 tcp 2124414975 159.203.70.248 7881 typ host tcptype passive"
                                    .to_string(),
                            ),
                        },
                    ]);
                    modified_attributes.extend_from_slice(&m.attributes[idx..]);
                    m.attributes = modified_attributes;
                    done = true;
                    break;
                }
            }
        }
        if done {
            break;
        }
    }
    offer.sdp = parsed.marshal();

    let parsed = offer.unmarshal().unwrap();

    let get_num_transport_type_candidates = |sd: &sdp::SessionDescription| {
        let mut num_udp_candidates = 0;
        let mut num_tcp_candidates = 0;
        let mut count = |attrs: &[sdp::Attribute]| {
            for a in attrs {
                if a.key == sdp::ATTR_KEY_CANDIDATE {
                    let value = a.value.as_deref().unwrap_or("");
                    if value.contains("udp") {
                        num_udp_candidates += 1;
                    }
                    if value.contains("tcp") {
                        num_tcp_candidates += 1;
                    }
                }
            }
        };
        count(&sd.attributes);
        for m in &sd.media_descriptions {
            count(&m.attributes);
        }
        (num_udp_candidates, num_tcp_candidates)
    };
    let (udp, tcp) = get_num_transport_type_candidates(&parsed);
    assert_ne!(0, udp);
    assert_eq!(2, tcp);

    transport.set_prefer_tcp(true);
    let filtered_offer = transport.filter_candidates(offer, true, true);
    let parsed = filtered_offer.unmarshal().unwrap();
    let (udp, tcp) = get_num_transport_type_candidates(&parsed);
    assert_eq!(0, udp);
    assert_eq!(2, tcp);

    transport.close();
}

// iceConnected reports whether a peer connection's ICE has established. The
// controlling agent (the offerer) advances Connected -> Completed once its
// connectivity checks finish, so asserting exactly Connected races the poll
// against that transition; both states mean the transport is up.
fn ice_connected(pc: &PeerConnection) -> bool {
    let s = pc.ice_connection_state();
    s == IceConnectionState::Connected || s == IceConnectionState::Completed
}

fn handle_ice_exchange(
    a: &Arc<PcTransport>,
    b: &Arc<PcTransport>,
    ah: &Arc<FakeHandler>,
    bh: &Arc<FakeHandler>,
) {
    ah.on_ice_candidate_calls({
        let b = Arc::downgrade(b);
        move |candidate, _target| {
            let Some(candidate) = candidate else {
                return Ok(());
            };
            println!("got ICE candidate from A: {candidate}");
            if let Some(b) = b.upgrade() {
                b.add_ice_candidate(candidate.to_json().unwrap());
            }
            Ok(())
        }
    });
    bh.on_ice_candidate_calls({
        let a = Arc::downgrade(a);
        move |candidate, _target| {
            let Some(candidate) = candidate else {
                return Ok(());
            };
            println!("got ICE candidate from B: {candidate}");
            if let Some(a) = a.upgrade() {
                a.add_ice_candidate(candidate.to_json().unwrap());
            }
            Ok(())
        }
    });
}

async fn connect_transports(
    offerer: &Arc<PcTransport>,
    answerer: &Arc<PcTransport>,
    offerer_handler: &Arc<FakeHandler>,
    answerer_handler: &Arc<FakeHandler>,
    is_ice_restart: bool,
    expected_offer_count: i32,
    expected_answer_count: i32,
) {
    let offer_count = Arc::new(AtomicI32::new(0));
    let answer_count = Arc::new(AtomicI32::new(0));
    answerer_handler.on_answer_calls({
        let offerer = Arc::downgrade(offerer);
        let answer_count = answer_count.clone();
        move |answer, answer_id, _mid_to_track_id| {
            answer_count.fetch_add(1, Ordering::SeqCst);
            if let Some(offerer) = offerer.upgrade() {
                let _ = offerer.handle_remote_description(answer, answer_id);
            }
            Ok(())
        }
    });

    offerer_handler.on_offer_calls({
        let answerer = Arc::downgrade(answerer);
        let offer_count = offer_count.clone();
        move |offer, offer_id, _mid_to_track_id| {
            offer_count.fetch_add(1, Ordering::SeqCst);
            if let Some(answerer) = answerer.upgrade() {
                let _ = answerer.handle_remote_description(offer, offer_id);
            }
            Ok(())
        }
    });

    if is_ice_restart {
        let _ = offerer.ice_restart();
    } else {
        offerer.negotiate(true);
    }

    // "offer count mismatch, expected: %d, actual: %d"
    eventually_async(
        || offer_count.load(Ordering::SeqCst) == expected_offer_count,
        10 * SECOND,
        TEN_MS,
    )
    .await;

    // "offerer did not become connected"
    eventually_async(|| ice_connected(&offerer.pc), 10 * SECOND, TEN_MS).await;

    // "answer count mismatch, expected: %d, actual: %d"
    eventually_async(
        || answer_count.load(Ordering::SeqCst) == expected_answer_count,
        10 * SECOND,
        TEN_MS,
    )
    .await;

    // "answerer did not become connected"
    eventually_async(|| ice_connected(&answerer.pc), 10 * SECOND, TEN_MS).await;

    until_transports_connected(&[offerer_handler, answerer_handler]).await;
}

// `untilTransportsConnected(...).Wait()`
async fn until_transports_connected(transports: &[&Arc<FakeHandler>]) {
    let triggered = Arc::new(AtomicUsize::new(0));

    for t in transports {
        let done = Arc::new(AtomicBool::new(false));
        let hdlr = {
            let triggered = triggered.clone();
            move || {
                if !done.load(Ordering::SeqCst) {
                    done.store(true, Ordering::SeqCst);
                    triggered.fetch_add(1, Ordering::SeqCst);
                }
            }
        };

        if t.on_initial_connected_call_count() != 0 {
            hdlr();
        }
        t.on_initial_connected_calls(hdlr);
    }
    eventually_async(
        || triggered.load(Ordering::SeqCst) == transports.len(),
        CONNECT_TIMEOUT,
        TEN_MS,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_configure_audio_transceiver() {
    for (nack, stereo) in [(false, false), (true, false), (false, true), (true, true)] {
        let name = format!("nack={nack},stereo={stereo}");
        let me = register_codecs(
            &[codec(MIME_TYPE_OPUS)],
            RtcpFeedbackConfig {
                audio: vec![self::nack()],
                ..Default::default()
            },
        );
        let pc = new_peer_connection_with(me);
        let tr = pc
            .add_transceiver_from_kind(
                RtpCodecType::Audio,
                Some(RtpTransceiverInit {
                    direction: RtpTransceiverDirection::Sendonly,
                }),
            )
            .unwrap();

        configure_sender_audio(&tr, stereo, nack, None);
        let codecs = tr.sender().unwrap().get_parameters().rtp_parameters.codecs;
        // new: that there was a codec to look at
        let mut opus_found = false;
        for codec in &codecs {
            if mime::is_mime_type_string_opus(&codec.rtp_codec_capability.mime_type) {
                opus_found = true;
                assert_eq!(
                    stereo,
                    codec
                        .rtp_codec_capability
                        .sdp_fmtp_line
                        .contains("sprop-stereo=1"),
                    "{name}"
                );
                let mut nack_enabled = false;
                for fb in &codec.rtp_codec_capability.rtcp_feedback {
                    if fb.r#type == webrtc::TYPE_RTCP_FB_NACK {
                        nack_enabled = true;
                        break;
                    }
                }
                assert_eq!(nack, nack_enabled, "{name}");
            }
        }
        assert!(
            opus_found,
            "{name}: no Opus codec in the sender's parameters"
        );
        let _ = pc.close();
    }
}

// When answering a subscriber offer, the sender's audio payload type must echo
// the payload type the offer assigned (RFC 3264), otherwise Firefox decodes no
// audio. See https://github.com/livekit/livekit/issues/4599.
#[tokio::test(flavor = "multi_thread")]
async fn test_configure_audio_transceiver_echoes_offer_payload_type() {
    let me = register_codecs(
        &[codec(MIME_TYPE_OPUS)],
        RtcpFeedbackConfig {
            audio: vec![nack()],
            ..Default::default()
        },
    );
    let pc = new_peer_connection_with(me);
    let tr = pc
        .add_transceiver_from_kind(
            RtpCodecType::Audio,
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Sendonly,
            }),
        )
        .unwrap();

    // offer mapped Opus to a payload type different from the server MediaEngine's.
    const OFFERED_OPUS_PT: PayloadType = 109;
    configure_sender_audio(
        &tr,
        false,
        true,
        Some(&HashMap::from([(mime::MimeType::Opus, OFFERED_OPUS_PT)])),
    );

    let mut found = false;
    for codec in tr.sender().unwrap().get_parameters().rtp_parameters.codecs {
        if mime::is_mime_type_string_opus(&codec.rtp_codec_capability.mime_type) {
            assert_eq!(OFFERED_OPUS_PT, codec.payload_type);
            found = true;
        }
    }
    assert!(found, "opus codec must be present in sender preferences");
    let _ = pc.close();
}

// In single-PC mode the publisher PC carries both publish and subscribe
// directions. If the MediaEngine were built only from the publish codec list,
// the SDP offer would not advertise some codecs in the m-section even though
// the subscribe direction is supposed to support it. This regression-tests
// the union behavior in newPeerConnection: build the MediaEngine from publish +
// subscribe codec lists.
#[tokio::test(flavor = "multi_thread")]
async fn test_single_pc_media_engine_unions_codecs() {
    let video_m_section_codecs = |transport: &PcTransport| {
        transport
            .pc
            .add_transceiver_from_kind(RtpCodecType::Video, Some(sendrecv()))
            .unwrap();
        let offer = transport.pc.create_offer(None).unwrap();
        let parsed = offer.unmarshal().unwrap();
        let mut rtpmaps = Vec::new();
        for m in &parsed.media_descriptions {
            if m.media_name.media != "video" {
                continue;
            }
            for a in &m.attributes {
                if a.key == "rtpmap" {
                    rtpmaps.push(a.value.clone().unwrap_or_default());
                }
            }
        }
        rtpmaps
    };

    let sdp_has_h264 = |rtpmaps: &[String]| rtpmaps.iter().any(|r| r.contains("H264/"));

    let publish_only = vec![codec(MIME_TYPE_OPUS), codec(MIME_TYPE_VP8)];
    let subscribe_only = vec![
        codec(MIME_TYPE_OPUS),
        codec(MIME_TYPE_VP8),
        codec(MIME_TYPE_H264),
    ];

    // Control: only publish codecs set (dual-PC publisher PC). H.264 absent.
    let mut params = transport_params(&Arc::new(FakeHandler::default()));
    params.enabled_publish_codecs = publish_only.clone();
    let dual_pc = PcTransport::new(params).unwrap();
    assert!(
        !sdp_has_h264(&video_m_section_codecs(&dual_pc)),
        "dual-PC publisher must not advertise H.264 when it's stripped from the publish list"
    );

    // Single-PC publisher PC: both lists set. H.264 must appear.
    let mut params = transport_params(&Arc::new(FakeHandler::default()));
    params.enabled_publish_codecs = publish_only;
    params.enabled_subscribe_codecs = subscribe_only;
    params.is_send_side = true;
    let single_pc = PcTransport::new(params).unwrap();
    assert!(
        sdp_has_h264(&video_m_section_codecs(&single_pc)),
        "single-PC publisher must advertise H.264 from the subscribe list even when it's stripped from the publish list"
    );

    // the original leaves them open
    dual_pc.close();
    single_pc.close();
}

// Regression test for restrictReceiverCodecsToPublishList: subscribe-only
// codecs (e.g., H.264) registered for subscriptions must not leak into the
// recv-side m-section of an answer, or the peer could publish them.
#[tokio::test(flavor = "multi_thread")]
async fn test_single_pc_answer_strips_subscribe_only_codecs_from_recv_side() {
    let publish_codecs = vec![codec(MIME_TYPE_OPUS), codec(MIME_TYPE_VP8)];
    let subscribe_codecs = vec![
        codec(MIME_TYPE_OPUS),
        codec(MIME_TYPE_VP8),
        codec(MIME_TYPE_H264),
    ];

    let handler = Arc::new(FakeHandler::default());
    let mut params = transport_params(&handler);
    params.enabled_publish_codecs = publish_codecs;
    params.enabled_subscribe_codecs = subscribe_codecs.clone();
    params.is_send_side = true;
    let server = PcTransport::new(params).unwrap();

    let client_me = register_codecs(&subscribe_codecs, RtcpFeedbackConfig::default());
    let client = new_peer_connection_with(client_me);

    client
        .add_transceiver_from_kind(
            RtpCodecType::Video,
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Sendonly,
            }),
        )
        .unwrap();
    let offer = client.create_offer(None).unwrap();
    assert!(offer.sdp.contains("H264/"), "offer must advertise H.264");
    client.set_local_description(offer).unwrap();

    let answer = Arc::new(Mutex::new(None));
    handler.on_answer_calls({
        let answer = answer.clone();
        move |sd, _, _| {
            *answer.lock() = Some(sd);
            Ok(())
        }
    });
    server
        .handle_remote_description(client.local_description().unwrap(), 1)
        .unwrap();

    // "server did not produce answer"
    eventually_async(|| answer.lock().is_some(), 5 * SECOND, TEN_MS).await;

    let parsed = answer.lock().clone().unwrap().unmarshal().unwrap();

    let video_section = parsed
        .media_descriptions
        .iter()
        .find(|m| m.media_name.media == "video");
    let video_section = video_section.expect("answer missing video m-section");

    for a in &video_section.attributes {
        if a.key != "rtpmap" {
            continue;
        }
        let value = a.value.as_deref().unwrap_or("");
        assert!(
            !value.contains("H264/"),
            "answer must not advertise H.264 in recv-side m-section: {value}"
        );
    }

    server.close();
    let _ = client.close();
}

// the parsed remote offer kept by the transport must follow every accepted offer
// and survive an offer that does not parse
#[tokio::test(flavor = "multi_thread")]
async fn test_remote_offer_parsed_tracks_offers() {
    let codecs = vec![codec(MIME_TYPE_VP8)];
    let handler = Arc::new(FakeHandler::default());
    let mut params = transport_params(&handler);
    let mut config = (*web_rtc_config()).clone();
    // offers from a real client carry RTX pairs that the transport registers here
    config.buffer_factory =
        Some(buffer::FactoryOfBufferFactory::new(500, 200).create_buffer_factory());
    params.config = Arc::new(config);
    params.enabled_publish_codecs = codecs.clone();
    params.is_offerer = false;
    let tr = PcTransport::new(params).unwrap();

    let client_me = register_codecs(&codecs, RtcpFeedbackConfig::default());
    let client = new_peer_connection_with(client_me);

    handler.on_answer_calls({
        let client = client.clone();
        move |sd, _, _| Ok(client.set_remote_description(sd)?)
    });

    let mids = |parsed: &sdp::SessionDescription| -> Vec<String> {
        parsed
            .media_descriptions
            .iter()
            .map(|m| {
                sdp::attribute(&m.attributes, sdp::ATTR_KEY_MID)
                    .unwrap_or("")
                    .to_string()
            })
            .collect()
    };

    let mut offer_id = 0u32;
    let mut send_offer = async |num_sections: usize| {
        client
            .add_transceiver_from_kind(
                RtpCodecType::Video,
                Some(RtpTransceiverInit {
                    direction: RtpTransceiverDirection::Sendonly,
                }),
            )
            .unwrap();
        let offer = client.create_offer(None).unwrap();
        client.set_local_description(offer.clone()).unwrap();

        offer_id += 1;
        let set_calls = handler.on_set_remote_description_offer_call_count();
        tr.handle_remote_description(offer, offer_id).unwrap();
        eventually_async(
            || handler.on_set_remote_description_offer_call_count() > set_calls,
            5 * SECOND,
            TEN_MS,
        )
        .await;

        let parsed = tr.remote_offer_parsed();
        let parsed = parsed.expect("a parsed remote offer");
        assert_eq!(num_sections, parsed.media_descriptions.len());
        let want: Vec<String> = client
            .get_transceivers()
            .iter()
            .map(|tc| tc.mid())
            .collect();
        assert_eq!(want, mids(&parsed));

        // wait for the answer so that the client can offer again
        eventually_async(
            || client.signaling_state() == SignalingState::Stable,
            5 * SECOND,
            TEN_MS,
        )
        .await;
    };

    send_offer(1).await;
    send_offer(2).await;

    // an offer that does not parse fails the negotiation and leaves the last parse alone
    let before = tr.remote_offer_parsed();
    let failures = handler.on_negotiation_failed_call_count();
    tr.handle_remote_description(
        SessionDescription {
            r#type: SdpType::Offer,
            sdp: "not an sdp".to_string(),
        },
        offer_id + 1,
    )
    .unwrap();
    eventually_async(
        || handler.on_negotiation_failed_call_count() > failures,
        5 * SECOND,
        TEN_MS,
    )
    .await;
    let after = tr.remote_offer_parsed();
    assert!(Arc::ptr_eq(
        before.as_ref().unwrap(),
        after.as_ref().unwrap()
    ));

    tr.close();
    let _ = client.close();
}

// in one-shot signalling mode an ICE restart arrives as an SDP fragment that is patched into
// the remote offer and set on pion directly, the stored parse must follow it
#[tokio::test(flavor = "multi_thread")]
async fn test_remote_offer_parsed_follows_ice_restart_fragment() {
    let codecs = vec![codec(MIME_TYPE_VP8)];
    let handler = Arc::new(FakeHandler::default());
    let mut params = transport_params(&handler);
    let mut config = (*web_rtc_config()).clone();
    config.buffer_factory =
        Some(buffer::FactoryOfBufferFactory::new(500, 200).create_buffer_factory());
    params.config = Arc::new(config);
    params.enabled_publish_codecs = codecs.clone();
    params.is_offerer = false;
    params.use_one_shot_signalling_mode = true;
    let tr = PcTransport::new(params).unwrap();

    let client_me = register_codecs(&codecs, RtcpFeedbackConfig::default());
    let client = new_peer_connection_with(client_me);

    client
        .add_transceiver_from_kind(
            RtpCodecType::Video,
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Sendonly,
            }),
        )
        .unwrap();
    let offer = client.create_offer(None).unwrap();
    client.set_local_description(offer.clone()).unwrap();

    tr.handle_remote_description(offer, 1).unwrap();
    let before = tr.remote_offer_parsed();
    let before = before.expect("a parsed remote offer");
    let (ufrag, _) = lksdp::extract_ice_credential(&before).unwrap();
    assert_ne!("restartUfrag", ufrag);

    tr.get_answer().unwrap();

    let mid = client.get_transceivers()[0].mid();
    let fragment = format!(
        "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
         a=mid:{mid}\r\n\
         a=ice-ufrag:restartUfrag\r\n\
         a=ice-pwd:restartPwd0123456789abcdef\r\n\
         a=candidate:1 1 udp 2122260223 192.0.2.1 61764 typ host generation 0\r\n"
    );
    let answer = tr.handle_ice_restart_sdp_fragment(&fragment).unwrap();
    assert!(!answer.is_empty());

    let after = tr.remote_offer_parsed();
    let after = after.expect("a parsed remote offer");
    assert!(!Arc::ptr_eq(&before, &after));
    let (ufrag, pwd) = lksdp::extract_ice_credential(&after).unwrap();
    assert_eq!("restartUfrag", ufrag);
    assert_eq!("restartPwd0123456789abcdef", pwd);

    tr.close();
    let _ = client.close();
}
