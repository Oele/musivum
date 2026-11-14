// Ported from LiveKit, pkg/sfu/downtrack_downstream_integration_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The original binds a down track through two pion
// peer connections on pion's virtual network and reads what arrives on the far side. There
// is no virtual network here and the tests are about the down track, so the harness is the
// lightest that keeps their assertions: the test binds the down track itself, as the
// connection layer would, with a context of its own; the write stream of that context puts
// every packet on the "wire" as the connection would (header, payload, and the padding the
// header announces) and reads it back as the far side's pion would, so that a packet the
// far side could not parse does not arrive; the receiver is the fake; the pacer is the real
// pass-through pacer; RTCP is handed in where the RTCP reader would hand it in. The far
// side records the retransmission stream too, which pion's receiver takes for itself.
// `SetRTXBudgetForTest` is the budget in the parameters the down track is made with (see
// export_test.rs). The fake receiver is not made its own RED and primary receiver, which
// nothing here asks for and which would make it hold itself.
//
// Not ported, each for what it needs:
// - `TestPionVNetForwardingSpike`: pion's virtual network and peer connections; it has
//   nothing of the down track in it.
// - `TestDownTrackSendsProbePackets`: the stream allocator (`AddTrack`) and both bandwidth
//   estimators behind the pacer. What it asserts of the down track is asserted here, with a
//   fake stream allocator listener, by `probe_packets_go_out_on_the_rtx_stream_as_padding`,
//   which is new.
// - `TestDownTrackResumesAfterBandwidthPause`, with `testResumeAfterBandwidthPause` and
//   `countingPacer`: the stream allocator running, the remote bandwidth estimator, and a
//   receiver whose estimate is capped (the test's own REMB model).
// `TestDownTrackWriteRTPNoAlloc` is here without its assertion on allocations.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration as StdDuration;

use bytes::Bytes;
use parking_lot::Mutex;

use super::export_test::{
    DROPPED_SLOT_PADDING_SIZE_FOR_TEST, KEY_FRAME_INTERVAL_MAX_FOR_TEST,
    NACK_IGNORE_WINDOW_FOR_TEST,
};
use super::{DownTrack, DownTrackParams, RTP_PADDING_MAX_PAYLOAD_SIZE};
use crate::go::time::Duration;
use crate::mediatransportutil::codec;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::sfu::buffer;
use crate::sfu::bwe;
use crate::sfu::ccutils;
use crate::sfu::pacer;
use crate::sfu::packettrailer;
use crate::sfu::rtpextension::abscapturetime as act;
use crate::sfu::testutils;
use crate::sfu::{
    Bitrates, DownTrackListener, DownTrackStreamAllocatorListener, Error, FakeDownTrackListener,
    FakeDownTrackStreamAllocatorListener, FakeTrackReceiver, ReceiverCodecState, RtxBudgetConfig,
    RtxBudgetLimit, TrackReceiver,
};
use crate::testutils::eventually_async;
use crate::webrtc;
use crate::webrtc::rtcp;
use crate::webrtc::rtcp::receiver_report::ReceiverReport;
use crate::webrtc::rtcp::reception_report::ReceptionReport;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_nack::{
    NackPair, TransportLayerNack,
};
use crate::webrtc::rtp;
use crate::webrtc::sdp;

const TICK: StdDuration = StdDuration::from_millis(20);

fn secs(n: u64) -> StdDuration {
    StdDuration::from_secs(n)
}

// -----------------------------------------------------------------------------
// packet capture on the far side
// -----------------------------------------------------------------------------

#[derive(Default)]
struct PacketCapture {
    packets: Mutex<Vec<rtp::Packet>>,
}

impl PacketCapture {
    fn add(&self, p: rtp::Packet) {
        self.packets.lock().push(p);
    }

    fn count(&self) -> usize {
        self.packets.lock().len()
    }

    fn all(&self) -> Vec<rtp::Packet> {
        self.packets.lock().clone()
    }
}

// The wire and the far side in one: what the original has two peer connections and
// `captureTrack` for. A packet is serialized as the connection serializes it (pion's
// `rtp.MarshalPacketTo`: the padding comes from the header) and parsed as the far side
// parses it; one that does not parse does not arrive.
struct FarSide {
    capture: Arc<PacketCapture>,
}

impl webrtc::TrackLocalWriter for FarSide {
    fn write_rtp(&self, header: &rtp::Header, payload: &[u8]) -> Result<usize, webrtc::Error> {
        let mut buf = vec![0u8; rtp::packet_marshal_size(header, payload)];
        let n = rtp::marshal_packet_to(&mut buf, header, payload)
            .map_err(|err| webrtc::Error::Other(err.to_string()))?;
        buf.truncate(n);

        let mut pkt = rtp::Packet::default();
        if pkt.unmarshal(&Bytes::from(buf)).is_ok() {
            self.capture.add(pkt);
        }
        Ok(n)
    }
}

// -----------------------------------------------------------------------------
// fakes (from pkg/sfu/sfufakes) + a null BWE
// -----------------------------------------------------------------------------

// newFakeTrackReceiver returns a counterfeiter FakeTrackReceiver configured just
// enough for a DownTrack to bind and forward: it reports the given codec and fires
// its readiness callback immediately so Bind completes.
fn new_fake_track_receiver(codec_params: &webrtc::RtpCodecParameters) -> Arc<FakeTrackReceiver> {
    let r = Arc::new(FakeTrackReceiver::default());
    r.track_id_returns("fake-track".into());
    r.stream_id_returns("fake-stream".into());
    r.codec_returns(codec_params.clone());
    r.mime_returns(mime::normalize_mime_type(&codec_params.mime_type));
    r.video_layer_mode_returns(livekit::video_layer::Mode::Unused);
    r.header_extensions_returns(Vec::new());
    r.is_closed_returns(false);
    r.track_info_returns(Arc::new(livekit::TrackInfo::default()));
    r.codec_state_returns(ReceiverCodecState::Normal);
    r.read_rtp_returns(Err(Error::Eof));
    // AddOnReady must invoke its callback immediately so Bind reaches the bound state.
    r.add_on_ready_calls(|f| f());
    r
}

// nullBWE is a complete no-op bwe.BWE for tests that do not exercise estimation.
// bwe.NullBWE is meant to be embedded and lacks a couple of interface methods.
#[derive(Default)]
struct NullBwe {
    null_bwe: bwe::NullBwe,
}

fn new_null_bwe() -> Option<Arc<dyn bwe::Bwe>> {
    Some(Arc::new(NullBwe::default()))
}

impl bwe::Bwe for NullBwe {
    fn r#type(&self) -> bwe::BweType {
        bwe::BweType::None
    }

    fn set_bwe_listener(&self, bwe_listner: Option<Arc<dyn bwe::BweListener>>) {
        self.null_bwe.set_bwe_listener(bwe_listner);
    }

    fn reset(&self) {
        self.null_bwe.reset();
    }

    fn handle_remb(
        &self,
        received_estimate: i64,
        expected_bandwidth_usage: i64,
        sent_packets: u32,
        repeated_nacks: u32,
    ) {
        self.null_bwe.handle_remb(
            received_estimate,
            expected_bandwidth_usage,
            sent_packets,
            repeated_nacks,
        );
    }

    fn record_packet_send_and_get_sequence_number(
        &self,
        at_micro: i64,
        size: usize,
        is_rtx: bool,
        probe_cluster_id: ccutils::ProbeClusterId,
        is_probe: bool,
    ) -> u16 {
        self.null_bwe.record_packet_send_and_get_sequence_number(
            at_micro,
            size,
            is_rtx,
            probe_cluster_id,
            is_probe,
        )
    }

    fn handle_twcc_feedback(&self, report: &TransportLayerCc) {
        self.null_bwe.handle_twcc_feedback(report);
    }

    fn update_rtt(&self, rtt: f64) {
        self.null_bwe.update_rtt(rtt);
    }

    fn congestion_state(&self) -> bwe::CongestionState {
        self.null_bwe.congestion_state()
    }

    fn can_probe(&self) -> bool {
        self.null_bwe.can_probe()
    }

    fn probe_duration(&self) -> Duration {
        self.null_bwe.probe_duration()
    }

    fn probe_cluster_starting(&self, pci: ccutils::ProbeClusterInfo) {
        self.null_bwe.probe_cluster_starting(pci);
    }

    fn probe_cluster_done(&self, pci: ccutils::ProbeClusterInfo) {
        self.null_bwe.probe_cluster_done(pci);
    }

    fn probe_cluster_is_goal_reached(&self) -> bool {
        self.null_bwe.probe_cluster_is_goal_reached()
    }

    fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool) {
        self.null_bwe.probe_cluster_finalize()
    }
}

// -----------------------------------------------------------------------------
// DownTrack harness
// -----------------------------------------------------------------------------

// What `vnettest` registers with pion's media engine.
const VP8_PAYLOAD_TYPE: u8 = 96;
const RTX_PAYLOAD_TYPE: u8 = VP8_PAYLOAD_TYPE + 1;
const OPUS_PAYLOAD_TYPE: u8 = 111;

// What pion picks at random for a sender in the original.
const TEST_SSRC: u32 = 0x0d0d_0001;
const TEST_SSRC_RTX: u32 = 0x0d0d_0002;

fn video_rtcp_feedback() -> Vec<webrtc::RtcpFeedback> {
    let feedback = |r#type: &str, parameter: &str| webrtc::RtcpFeedback {
        r#type: r#type.to_string(),
        parameter: parameter.to_string(),
    };
    vec![
        feedback(webrtc::TYPE_RTCP_FB_NACK, ""),
        feedback(webrtc::TYPE_RTCP_FB_NACK, "pli"),
        feedback(webrtc::TYPE_RTCP_FB_TRANSPORT_CC, ""),
        feedback(webrtc::TYPE_RTCP_FB_GOOG_REMB, ""),
    ]
}

fn opus_codec_params() -> webrtc::RtpCodecParameters {
    webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: webrtc::MIME_TYPE_OPUS.to_string(),
            clock_rate: 48000,
            channels: 2,
            ..Default::default()
        },
        payload_type: OPUS_PAYLOAD_TYPE,
    }
}

fn vp8_codec_params() -> webrtc::RtpCodecParameters {
    webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: webrtc::MIME_TYPE_VP8.to_string(),
            clock_rate: 90000,
            rtcp_feedback: video_rtcp_feedback(),
            ..Default::default()
        },
        payload_type: VP8_PAYLOAD_TYPE,
    }
}

// `vnettest.MediaEngineConfig`, as far as these tests set it.
#[derive(Clone, Copy, Default)]
struct MediaEngineConfig {
    video: bool,             // VP8 and its RTX codec; otherwise opus
    header_extensions: bool, // abs-send-time + transport-cc
}

// What the two peer connections of the original negotiate for a media engine made by
// `vnettest.NewMediaEngine`: the codecs, the header extensions, and a retransmission
// stream where the codec has one.
fn negotiate(
    cfg: MediaEngineConfig,
) -> (
    Vec<webrtc::RtpCodecParameters>,
    Vec<webrtc::RtpHeaderExtensionParameter>,
    u32,
) {
    let mut codecs = Vec::new();
    let mut ssrc_rtx = 0;
    if cfg.video {
        codecs.push(vp8_codec_params());
        codecs.push(webrtc::RtpCodecParameters {
            rtp_codec_capability: webrtc::RtpCodecCapability {
                mime_type: webrtc::MIME_TYPE_RTX.to_string(),
                clock_rate: 90000,
                sdp_fmtp_line: format!("apt={VP8_PAYLOAD_TYPE}"),
                ..Default::default()
            },
            payload_type: RTX_PAYLOAD_TYPE,
        });
        ssrc_rtx = TEST_SSRC_RTX;
    } else {
        codecs.push(opus_codec_params());
    }

    let mut header_extensions = Vec::new();
    if cfg.header_extensions {
        header_extensions.push(webrtc::RtpHeaderExtensionParameter {
            uri: sdp::ABS_SEND_TIME_URI.to_string(),
            id: 1,
        });
        header_extensions.push(webrtc::RtpHeaderExtensionParameter {
            uri: sdp::TRANSPORT_CC_URI.to_string(),
            id: 2,
        });
    }
    (codecs, header_extensions, ssrc_rtx)
}

// The context the connection layer binds a track with.
struct TestContext {
    codecs: Vec<webrtc::RtpCodecParameters>,
    header_extensions: Vec<webrtc::RtpHeaderExtensionParameter>,
    ssrc: u32,
    ssrc_rtx: u32,
    write_stream: Arc<FarSide>,
}

impl webrtc::TrackLocalContext for TestContext {
    fn codec_parameters(&self) -> Vec<webrtc::RtpCodecParameters> {
        self.codecs.clone()
    }

    fn header_extensions(&self) -> Vec<webrtc::RtpHeaderExtensionParameter> {
        self.header_extensions.clone()
    }

    fn ssrc(&self) -> webrtc::Ssrc {
        self.ssrc
    }

    fn ssrc_retransmission(&self) -> webrtc::Ssrc {
        self.ssrc_rtx
    }

    fn write_stream(&self) -> Arc<dyn webrtc::TrackLocalWriter> {
        self.write_stream.clone()
    }
}

// The transceiver the sender connection has for the track, and its sender.
struct TestTransceiver {
    sender: Arc<TestSender>,
}

struct TestSender {
    parameters: webrtc::RtpSendParameters,
}

impl webrtc::RtpTransceiver for TestTransceiver {
    fn sender(&self) -> Option<Arc<dyn webrtc::RtpSender>> {
        Some(self.sender.clone())
    }

    fn mid(&self) -> String {
        "0".to_string()
    }
}

impl webrtc::RtpSender for TestSender {
    fn get_parameters(&self) -> webrtc::RtpSendParameters {
        self.parameters.clone()
    }
}

struct DownTrackHarness {
    dt: Arc<DownTrack>,
    receiver: Arc<FakeTrackReceiver>,
    capture: Arc<PacketCapture>,

    // what the down track holds weakly, or not at all, and the peer connections of the
    // original keep
    _listener: Arc<FakeDownTrackListener>,
    _transceiver: Arc<dyn webrtc::RtpTransceiver>,
    _factory: Arc<buffer::Factory>,
}

// newBoundDownTrack builds a real DownTrack, attaches it to a real sender PC,
// negotiates with a real subscriber PC over vnet, and makes it writable.
//
// Here: builds a real DownTrack, binds it as the connection would after the negotiation
// that `negotiate` stands for, and makes it writable. `rtx_budget` is the original's
// `SetRTXBudgetForTest`, which its tests call right after this.
async fn new_bound_down_track(
    factory: Arc<buffer::Factory>,
    codec_params: webrtc::RtpCodecParameters,
    p: Arc<dyn pacer::Pacer>,
    me_cfg: MediaEngineConfig,
    rtx_budget: Option<RtxBudgetLimit>,
    rcv_opt: impl FnOnce(&FakeTrackReceiver),
) -> DownTrackHarness {
    let capture = Arc::new(PacketCapture::default());

    let rcv = new_fake_track_receiver(&codec_params);
    rcv_opt(&rcv);
    let listener = Arc::new(FakeDownTrackListener::default());
    let dt = new_down_track(&factory, codec_params, p, rtx_budget, &rcv, &listener);

    let transceiver = new_transceiver(me_cfg);
    dt.set_transceiver(transceiver.clone());

    // vnettest.SignalPair: the connection binds the track when the negotiation is done
    let bound = dt
        .bind(new_context(me_cfg, &capture))
        .expect("the down track binds");
    assert!(!bound.mime_type.is_empty(), "the codec was negotiated");
    dt.set_connected();

    eventually_async(|| dt.is_writable_for_test(), secs(10), TICK).await;

    DownTrackHarness {
        dt,
        receiver: rcv,
        capture,
        _listener: listener,
        _transceiver: transceiver,
        _factory: factory,
    }
}

// The `NewDownTrack` of the original's `newBoundDownTrack`.
fn new_down_track(
    factory: &Arc<buffer::Factory>,
    codec_params: webrtc::RtpCodecParameters,
    p: Arc<dyn pacer::Pacer>,
    rtx_budget: Option<RtxBudgetLimit>,
    rcv: &Arc<FakeTrackReceiver>,
    listener: &Arc<FakeDownTrackListener>,
) -> Arc<DownTrack> {
    let weak_listener = Arc::downgrade(listener);
    let weak_listener: Weak<dyn DownTrackListener> = weak_listener;
    let receiver: Arc<dyn TrackReceiver> = rcv.clone();
    let rtx_budget = rtx_budget.map_or(RtxBudgetConfig::default(), |limit| RtxBudgetConfig {
        audio: limit,
        video: limit,
    });
    DownTrack::new(DownTrackParams {
        codecs: vec![codec_params],
        is_encrypted: false,
        source: livekit::TrackSource::Unknown,
        receiver,
        buffer_factory: factory.clone(),
        sub_id: "fake-sub".into(),
        stream_id: "fake-stream".to_string(),
        max_track: 500,
        playout_delay_limit: None,
        pacer: p,
        logger: logger::get_logger(),
        trailer: Bytes::new(),
        rtcp_writer: None,
        disable_sender_report_pass_through: false,
        supports_codec_change: false,
        strip_packet_trailer: false,
        enable_start_at_desired_quality: false,
        rtx_budget,
        listener: weak_listener,
    })
    .expect("the down track is made")
}

// The transceiver that `AddTransceiverFromTrack` gives in the original.
fn new_transceiver(me_cfg: MediaEngineConfig) -> Arc<dyn webrtc::RtpTransceiver> {
    let (codecs, header_extensions, _) = negotiate(me_cfg);
    Arc::new(TestTransceiver {
        sender: Arc::new(TestSender {
            parameters: webrtc::RtpSendParameters {
                rtp_parameters: webrtc::RtpParameters {
                    header_extensions,
                    codecs,
                },
                encodings: Vec::new(),
            },
        }),
    })
}

// What the sender connection binds its track with when the negotiation is done.
fn new_context(
    me_cfg: MediaEngineConfig,
    capture: &Arc<PacketCapture>,
) -> Arc<dyn webrtc::TrackLocalContext> {
    let (codecs, header_extensions, ssrc_rtx) = negotiate(me_cfg);
    Arc::new(TestContext {
        codecs,
        header_extensions,
        ssrc: TEST_SSRC,
        ssrc_rtx,
        write_stream: Arc::new(FarSide {
            capture: capture.clone(),
        }),
    })
}

fn new_buffer_factory() -> Arc<buffer::Factory> {
    buffer::FactoryOfBufferFactory::new(500, 500).create_buffer_factory()
}

// distinctivePayload builds a per-packet payload with recognizable bytes so payload
// fidelity can be asserted end-to-end.
fn distinctive_payload(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|j| seed.wrapping_add(j as u8)).collect()
}

// -----------------------------------------------------------------------------
// WriteRTP: media forwarding
// -----------------------------------------------------------------------------

// TestDownTrackForwardsMedia exercises the WriteRTP media forwarding path and
// verifies the full forwarded header/payload on the far side: SSRC, payload type,
// contiguous sequence numbers, timestamps, payload bytes, and that the padding bit
// is cleared regardless of the source packet's padding bit.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::explicit_counter_loop)] // the loop as the original has it
async fn test_down_track_forwards_media() {
    let factory = new_buffer_factory();
    let p = pacer::PassThrough::new(logger::get_logger(), new_null_bwe());

    let dh = new_bound_down_track(
        factory,
        opus_codec_params(),
        p.clone(),
        MediaEngineConfig::default(),
        None,
        |_| {},
    )
    .await;

    const NUM_PACKETS: u16 = 20;
    let mut sn = 23333u16;
    let mut ts = 0xabcdefu32;
    let mut want_payload: HashMap<u16, Vec<u8>> = HashMap::new();
    let mut want_ts: HashMap<u16, u32> = HashMap::new();
    for i in 0..NUM_PACKETS {
        let mut ep = testutils::get_test_ext_packet(&testutils::TestExtPacketParams {
            sequence_number: sn,
            timestamp: ts,
            ssrc: 0x12345678,
            payload_type: 111,
            payload_size: 40,
            ..Default::default()
        })
        .unwrap();
        // distinctive payload so payload fidelity can be checked on the far side
        let payload = distinctive_payload((i + 1) as u8, 40);
        ep.packet.payload = Bytes::from(payload.clone());
        // every other source packet carries the padding bit; the forwarded packet
        // must not inherit it.
        if i % 2 == 0 {
            ep.packet.header.padding = true;
        }
        want_payload.insert(sn, payload);
        want_ts.insert(sn, ts);

        assert_eq!(1, dh.dt.write_rtp(&ep, 0), "packet {i} should be forwarded");
        sn += 1;
        ts += 960;
    }

    // expected all forwarded media packets to arrive
    eventually_async(
        || dh.capture.count() >= NUM_PACKETS as usize,
        secs(10),
        TICK,
    )
    .await;

    let pkts = dh.capture.all();
    let mut seen = HashSet::new();
    for pkt in &pkts {
        assert_eq!(dh.dt.ssrc(), pkt.ssrc, "forwarded packet on wrong SSRC");
        assert_eq!(
            111, pkt.payload_type,
            "forwarded packet has wrong payload type"
        );
        assert!(
            !pkt.header.padding,
            "forwarded media packet must not carry padding bit (sn={})",
            pkt.sequence_number
        );

        let wp = want_payload.get(&pkt.sequence_number).unwrap_or_else(|| {
            panic!(
                "received unexpected sequence number {}",
                pkt.sequence_number
            )
        });
        assert_eq!(
            &wp[..],
            &pkt.payload[..],
            "forwarded payload mismatch (sn={})",
            pkt.sequence_number
        );
        assert_eq!(
            want_ts[&pkt.sequence_number], pkt.timestamp,
            "forwarded timestamp mismatch (sn={})",
            pkt.sequence_number
        );
        seen.insert(pkt.sequence_number);
    }

    // every source packet must have been forwarded, and the forwarded sequence
    // numbers must be contiguous.
    for want in 23333u16..23333 + NUM_PACKETS {
        assert!(
            seen.contains(&want),
            "sequence number {want} was not forwarded"
        );
    }

    pacer::Pacer::stop(&*p);
}

struct DiscardWriteStream;

impl webrtc::TrackLocalWriter for DiscardWriteStream {
    fn write_rtp(&self, _header: &rtp::Header, _payload: &[u8]) -> Result<usize, webrtc::Error> {
        Ok(0)
    }
}

struct DiscardPacer {
    pass_through: Arc<pacer::PassThrough>,
}

impl pacer::Pacer for DiscardPacer {
    fn enqueue(&self, mut p: pacer::Packet) {
        p.write_stream = Arc::new(DiscardWriteStream);
        self.pass_through.enqueue(p);
    }

    fn stop(&self) {
        self.pass_through.stop();
    }

    fn set_interval(&self, interval: Duration) {
        pacer::Pacer::set_interval(&*self.pass_through, interval);
    }

    fn set_bitrate(&self, bitrate: i64) {
        pacer::Pacer::set_bitrate(&*self.pass_through, bitrate);
    }

    fn time_since_last_sent_packet(&self) -> Duration {
        pacer::Pacer::time_since_last_sent_packet(&*self.pass_through)
    }

    fn set_pacer_probe_observer_listener(
        &self,
        listener: Option<Arc<dyn pacer::PacerProbeObserverListener>>,
    ) {
        pacer::Pacer::set_pacer_probe_observer_listener(&*self.pass_through, listener);
    }

    fn start_probe_cluster(&self, pci: ccutils::ProbeClusterInfo) {
        pacer::Pacer::start_probe_cluster(&*self.pass_through, pci);
    }

    fn end_probe_cluster(
        &self,
        probe_cluster_id: ccutils::ProbeClusterId,
    ) -> ccutils::ProbeClusterInfo {
        pacer::Pacer::end_probe_cluster(&*self.pass_through, probe_cluster_id)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_down_track_write_rtp_no_alloc() {
    let factory = new_buffer_factory();
    let p = Arc::new(DiscardPacer {
        pass_through: pacer::PassThrough::new(logger::get_logger(), new_null_bwe()),
    });

    let dh = new_bound_down_track(
        factory,
        opus_codec_params(),
        p.clone(),
        MediaEngineConfig::default(),
        None,
        |_| {},
    )
    .await;
    dh.dt.set_abs_capture_time_ext_id_for_test(5);

    let mut ep = testutils::get_test_ext_packet(&testutils::TestExtPacketParams {
        sequence_number: 1000,
        timestamp: 5000,
        ssrc: 0x12345678,
        payload_type: 111,
        payload_size: 40,
        ..Default::default()
    })
    .unwrap();
    ep.abs_capture_time_ext = Some(act::abs_capture_time_from_value(0x0102030405060708, 9));

    const RUNS: i32 = 1000;
    let mut forwarded = 0i32;
    // `testing.AllocsPerRun(runs, f)` calls `f` once to warm up and then `runs` times
    for _ in 0..RUNS + 1 {
        ep.packet.sequence_number += 1;
        ep.ext_sequence_number += 1;
        ep.packet.timestamp += 960;
        ep.ext_timestamp += 960;
        forwarded += dh.dt.write_rtp(&ep, 0);
    }
    assert_eq!(
        RUNS + 1,
        forwarded,
        "every write, including the AllocsPerRun warm up, should forward"
    );
    // The original requires zero allocations per `WriteRTP` here. That assertion is left
    // out: counting allocations needs `unsafe`, and a packet owns its parts in this port
    // (the header of downtrack.rs says what that costs per packet).

    pacer::Pacer::stop(&*p);
}

// -----------------------------------------------------------------------------
// retransmitPacket: RTX / retransmission
// -----------------------------------------------------------------------------

// TestDownTrackRetransmitsPacketsAsIs exercises the retransmit path when no RTX
// stream is negotiated, so the packet is re-sent as-is on the primary SSRC/payload
// type. It verifies the retransmitted header/payload on the far side: SSRC, payload
// type, sequence number, timestamp, payload bytes, and that the padding bit is
// cleared regardless of the source packet's padding bit.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::explicit_counter_loop)] // the loop as the original has it
async fn test_down_track_retransmits_packets_as_is() {
    let factory = new_buffer_factory();
    let p = pacer::PassThrough::new(logger::get_logger(), new_null_bwe());

    let dh = new_bound_down_track(
        factory,
        opus_codec_params(),
        p.clone(),
        MediaEngineConfig::default(),
        None,
        |_| {},
    )
    .await;

    const NUM_PACKETS: u16 = 10;
    let mut target_sn = 40000u16;
    let mut want_payload: HashMap<u16, Vec<u8>> = HashMap::new();
    let mut want_ts: HashMap<u16, u32> = HashMap::new();
    for i in 0..NUM_PACKETS {
        // build a source packet that carries the padding bit (valid padding trailer)
        // and a distinctive payload.
        let payload = distinctive_payload((i + 100) as u8, 30);
        let src = rtp::Packet {
            header: rtp::Header {
                version: 2,
                padding: true,
                padding_size: 20,
                payload_type: 111,
                sequence_number: 1000 + i,
                timestamp: 90000 + i as u32 * 960,
                ssrc: 0x22222222,
                ..Default::default()
            },
            payload: Bytes::from(payload.clone()),
            ..Default::default()
        };
        let raw = src.marshal().unwrap();

        let ts = 500000 + i as u32 * 960;
        want_payload.insert(target_sn, payload);
        want_ts.insert(target_sn, ts);

        dh.dt
            .retransmit_for_test(
                src.sequence_number as u64,
                target_sn,
                ts,
                ts as u64,
                0,
                &raw,
                0,
            )
            .unwrap();
        target_sn += 1;
    }

    // expected all retransmitted packets to arrive
    eventually_async(
        || dh.capture.count() >= NUM_PACKETS as usize,
        secs(10),
        TICK,
    )
    .await;

    let mut seen = HashSet::new();
    for pkt in &dh.capture.all() {
        assert_eq!(dh.dt.ssrc(), pkt.ssrc, "retransmitted packet on wrong SSRC");
        assert_eq!(
            111, pkt.payload_type,
            "retransmitted packet has wrong payload type"
        );
        assert!(
            !pkt.header.padding,
            "retransmitted packet must not carry padding bit (sn={})",
            pkt.sequence_number
        );

        let wp = want_payload.get(&pkt.sequence_number).unwrap_or_else(|| {
            panic!(
                "received unexpected sequence number {}",
                pkt.sequence_number
            )
        });
        assert_eq!(
            &wp[..],
            &pkt.payload[..],
            "retransmitted payload mismatch (sn={})",
            pkt.sequence_number
        );
        assert_eq!(
            want_ts[&pkt.sequence_number], pkt.timestamp,
            "retransmitted timestamp mismatch (sn={})",
            pkt.sequence_number
        );
        seen.insert(pkt.sequence_number);
    }

    for want in 40000u16..40000 + NUM_PACKETS {
        assert!(
            seen.contains(&want),
            "retransmitted sequence number {want} did not arrive"
        );
    }

    pacer::Pacer::stop(&*p);
}

// What the closures of the original's `TestDownTrackCapsRetransmissions` share: the
// sequence number and the time stamp go on from one sub-test to the next.
struct CapsRetransmissions {
    // extended, so the sequence number can wrap
    next_sn: u64,
    next_ts: u32,
}

const CAPS_NUM_PACKETS: usize = 20;

impl CapsRetransmissions {
    async fn setup(
        &self,
        rtx_budget: Option<RtxBudgetLimit>,
    ) -> (DownTrackHarness, Arc<CapturingPacer>, Vec<u8>) {
        let factory = new_buffer_factory();
        let cp = Arc::new(CapturingPacer::new(pacer::PassThrough::new(
            logger::get_logger(),
            new_null_bwe(),
        )));

        // every NACKed packet is read back from the receiver as this source packet
        let source = rtp::Packet {
            header: rtp::Header {
                version: 2,
                payload_type: 111,
                ssrc: 0x44444444,
                ..Default::default()
            },
            payload: Bytes::from(distinctive_payload(7, 600)),
            ..Default::default()
        }
        .marshal()
        .unwrap();
        let read_back = source.clone();
        let dh = new_bound_down_track(
            factory,
            opus_codec_params(),
            cp.clone(),
            MediaEngineConfig::default(),
            rtx_budget,
            move |r| {
                r.read_rtp_calls(move |buf: &mut [u8], _layer: u8, _esn: u64| {
                    let n = read_back.len().min(buf.len());
                    buf[..n].copy_from_slice(&read_back[..n]);
                    Ok(n)
                });
            },
        )
        .await;
        (dh, cp, source)
    }

    fn write_media(&mut self, dh: &DownTrackHarness, payload_size: usize) -> Vec<u16> {
        let mut sns = Vec::new();
        for _ in 0..CAPS_NUM_PACKETS {
            self.next_ts += 960;
            let ep = testutils::get_test_ext_packet(&testutils::TestExtPacketParams {
                sequence_number: self.next_sn as u16,
                sn_cycles: (self.next_sn >> 16) as i64,
                timestamp: self.next_ts,
                ssrc: 0x44444444,
                payload_type: 111,
                payload_size,
                ..Default::default()
            })
            .unwrap();
            assert_eq!(
                1,
                dh.dt.write_rtp(&ep, 0),
                "packet {} should be forwarded",
                self.next_sn
            );
            sns.push(self.next_sn as u16);
            self.next_sn += 1;
        }
        sns
    }

    // the budget floor of 1500 bytes sends three 612-byte packets: they must be the three
    // newest of the NACK, whatever its order
    async fn newest_of(&mut self, start: u64, nack_indexes: &[usize], newest_indexes: &[usize]) {
        let (dh, cp, _) = self
            .setup(Some(RtxBudgetLimit {
                max_ratio: 0.5,
                ..Default::default()
            }))
            .await;

        self.next_sn = start;
        let sns = self.write_media(&dh, 40);
        // expected the media to arrive
        eventually_async(|| dh.capture.count() >= CAPS_NUM_PACKETS, secs(10), TICK).await;
        // past the window the sequencer keeps after a packet is sent
        tokio::time::sleep((NACK_IGNORE_WINDOW_FOR_TEST + Duration::from_millis(20)).to_std())
            .await;

        let nacked: Vec<u16> = nack_indexes.iter().map(|&i| sns[i]).collect();
        send_nack(&dh, &nacked);

        let want: Vec<u16> = newest_indexes.iter().map(|&i| sns[i]).collect();
        // expected all but the newest to be capped
        eventually_async(
            || dh.dt.rtx_capped_for_test() == (nacked.len() - want.len()) as u64,
            secs(5),
            TICK,
        )
        .await;
        assert_eq!(want, rtx_seqs(&cp));

        cp.stop();
    }
}

fn send_nack(dh: &DownTrackHarness, sns: &[u16]) {
    let nack = TransportLayerNack {
        media_ssrc: dh.dt.ssrc(),
        nacks: sns
            .iter()
            .map(|&sn| NackPair {
                packet_id: sn,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let b = rtcp::packet::marshal(&[Box::new(nack)]).unwrap();
    dh.dt.handle_rtcp_for_test(&b);
}

fn rtx_seqs(cp: &CapturingPacer) -> Vec<u16> {
    cp.rtx_packets()
        .iter()
        .map(|p| p.hdr.sequence_number)
        .collect()
}

// require.ElementsMatch
fn assert_elements_match(mut expected: Vec<u16>, mut actual: Vec<u16>) {
    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(expected, actual);
}

// TestDownTrackCapsRetransmissions forwards media, NACKs all of it, and checks that an
// uncapped down track retransmits everything while a capped one retransmits only what its
// budget allows, newest first, without reading the capped packets from the buffer. Capped
// packets NACKed again keep failing while the budget is empty, past maxAck, and are
// retransmitted once media refills it. Repeat NACKs are spaced past the sequencer's
// duplicate window.
#[tokio::test(flavor = "multi_thread")]
async fn test_down_track_caps_retransmissions() {
    const NUM_PACKETS: usize = CAPS_NUM_PACKETS;

    let mut t = CapsRetransmissions {
        next_sn: 30000,
        next_ts: 1000,
    };

    // uncapped
    {
        let (dh, cp, _) = t.setup(None).await;
        let sns = t.write_media(&dh, 40);
        send_nack(&dh, &sns);

        // expected every packet to be retransmitted
        eventually_async(|| cp.rtx_packets().len() == NUM_PACKETS, secs(5), TICK).await;
        assert_eq!(
            sns,
            rtx_seqs(&cp),
            "uncapped: without a budget the order is unchanged"
        );

        cp.stop();
    }

    // capped
    {
        // two and a half retransmissions: the third overdraws the budget, a fourth is refused
        //
        // (the size of the source packet the receiver reads back, which `setup` makes: 612)
        const SOURCE_LEN: usize = 612;
        let (dh, cp, source) = t
            .setup(Some(RtxBudgetLimit {
                max_ratio: 0.5,
                burst_bytes: (5 * SOURCE_LEN / 2) as i64,
            }))
            .await;
        assert_eq!(SOURCE_LEN, source.len());

        let sns = t.write_media(&dh, 40);
        send_nack(&dh, &sns);

        // the capped count is added after the batch, so the retransmissions are all enqueued by then
        //
        // expected the batch to be capped
        eventually_async(
            || dh.dt.rtx_capped_for_test() == (NUM_PACKETS - 3) as u64,
            secs(5),
            TICK,
        )
        .await;
        // the newest packets go first
        assert_elements_match(sns[NUM_PACKETS - 3..].to_vec(), rtx_seqs(&cp));
        assert_eq!(
            3,
            dh.receiver.read_rtp_call_count(),
            "a capped retransmission must not read the publisher's buffer"
        );

        // with the budget empty, the capped packets fail again on every NACK, past maxAck
        let capped = &sns[..NUM_PACKETS - 3];
        for i in 0..4 {
            tokio::time::sleep((NACK_IGNORE_WINDOW_FOR_TEST + Duration::from_millis(20)).to_std())
                .await;
            send_nack(&dh, capped);
            let want = ((i + 2) * capped.len()) as u64;
            // expected NACK i+2 to be capped
            eventually_async(|| dh.dt.rtx_capped_for_test() == want, secs(5), TICK).await;
        }

        // media refills the budget, and the earlier capped packets are retransmitted
        t.write_media(&dh, 600);
        tokio::time::sleep((NACK_IGNORE_WINDOW_FOR_TEST + Duration::from_millis(20)).to_std())
            .await;
        send_nack(&dh, capped);
        // expected capped packets to be retransmitted after the refill
        eventually_async(|| cp.rtx_packets().len() == 6, secs(5), TICK).await;
        let mut want = sns[NUM_PACKETS - 3..].to_vec();
        want.extend_from_slice(&capped[capped.len() - 3..]);
        assert_elements_match(want, rtx_seqs(&cp));

        cp.stop();
    }

    // newest first for a NACK in mixed order
    t.newest_of(31000, &[10, 19, 0, 15, 5], &[19, 15, 10]).await;

    // newest first across the sequence number wrap
    //
    // packets 65530..65535 then 0..13: three before the wrap and three after it, out of order
    t.newest_of(65530, &[4, 8, 1, 6, 9, 2], &[9, 8, 6]).await;
}

// TestDownTrackRetransmitsPacketsViaRTX exercises the retransmit path when an RTX
// stream is negotiated. Each retransmitted packet must be emitted on the RTX SSRC
// with the RTX payload type, carry the original media sequence number as the OSN
// prefix (the first two payload bytes), draw its sequence number from the DownTrack's
// RTX sequence space, and clear the padding bit.
//
// RTX packets are captured at the pacer as the DownTrack builds them: pion's
// receiver-side RTX handling de-RTXes / consumes them, so the RTX-specific header
// fields are not observable on the far side. The packets are still handed to the real
// pacer (and written over pion).
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::explicit_counter_loop)] // the loop as the original has it
async fn test_down_track_retransmits_packets_via_rtx() {
    let factory = new_buffer_factory();
    let cp = Arc::new(CapturingPacer::new(pacer::PassThrough::new(
        logger::get_logger(),
        new_null_bwe(),
    )));

    // VP8 registers an RTX codec, so the bound DownTrack has an RTX SSRC / payload type.
    let dh = new_bound_down_track(
        factory,
        vp8_codec_params(),
        cp.clone(),
        MediaEngineConfig {
            video: true,
            ..Default::default()
        },
        None,
        |_| {},
    )
    .await;
    assert_ne!(0, dh.dt.ssrc_rtx(), "RTX ssrc should be negotiated");
    assert_ne!(
        0,
        dh.dt.payload_type_rtx_for_test(),
        "RTX payload type should be negotiated"
    );

    struct Want {
        payload: Vec<u8>,
        ts: u32,
    }
    const NUM_PACKETS: u16 = 10;
    let osn_base = 50000u16;
    let mut wants: HashMap<u16, Want> = HashMap::new(); // keyed by OSN (the original media sequence number)
    let mut target_sn = osn_base;
    for i in 0..NUM_PACKETS {
        // build a source packet that carries the padding bit (valid padding trailer)
        // and a distinctive payload.
        let payload = distinctive_payload((i + 100) as u8, 30);
        let src = rtp::Packet {
            header: rtp::Header {
                version: 2,
                padding: true,
                padding_size: 20,
                payload_type: 96,
                sequence_number: 2000 + i,
                timestamp: 180000 + i as u32 * 3000,
                ssrc: 0x33333333,
                ..Default::default()
            },
            payload: Bytes::from(payload.clone()),
            ..Default::default()
        };
        let raw = src.marshal().unwrap();

        let ts = 700000 + i as u32 * 3000;
        wants.insert(target_sn, Want { payload, ts });

        dh.dt
            .retransmit_for_test(
                src.sequence_number as u64,
                target_sn,
                ts,
                ts as u64,
                0,
                &raw,
                0,
            )
            .unwrap();
        target_sn += 1;
    }

    // expected RTX packets to be emitted
    eventually_async(
        || cp.rtx_packets().len() >= NUM_PACKETS as usize,
        secs(5),
        TICK,
    )
    .await;

    let mut seen = HashSet::new();
    let mut rtx_seqs = Vec::with_capacity(NUM_PACKETS as usize);
    for pr in &cp.rtx_packets() {
        assert_eq!(
            dh.dt.ssrc_rtx(),
            pr.hdr.ssrc,
            "retransmitted packet must be on the RTX SSRC"
        );
        assert_eq!(
            dh.dt.payload_type_rtx_for_test(),
            pr.hdr.payload_type as u32,
            "retransmitted packet must use the RTX payload type"
        );
        assert!(
            !pr.hdr.padding,
            "retransmitted packet must not carry padding bit"
        );
        assert!(
            pr.payload.len() >= 2,
            "RTX payload must contain the OSN prefix"
        );

        let osn = u16::from_be_bytes([pr.payload[0], pr.payload[1]]);
        let w = wants
            .get(&osn)
            .unwrap_or_else(|| panic!("unexpected OSN {osn}"));
        assert_eq!(
            &w.payload[..],
            &pr.payload[2..],
            "RTX media payload mismatch (osn={osn})"
        );
        assert_eq!(w.ts, pr.hdr.timestamp, "RTX timestamp mismatch (osn={osn})");
        seen.insert(osn);
        rtx_seqs.push(pr.hdr.sequence_number);
    }

    for osn in osn_base..osn_base + NUM_PACKETS {
        assert!(
            seen.contains(&osn),
            "OSN {osn} was not retransmitted via RTX"
        );
    }

    // RTX-stream sequence numbers come from the DownTrack's own RTX sequence space
    // and must advance by one per packet.
    assert_eq!(NUM_PACKETS as usize, rtx_seqs.len());
    for i in 1..rtx_seqs.len() {
        assert_eq!(
            rtx_seqs[i - 1].wrapping_add(1),
            rtx_seqs[i],
            "RTX sequence numbers must be contiguous"
        );
    }

    cp.stop();
}

// TestDownTrackReplaysPacketTrailerStripOnRetransmit verifies that a packet whose
// packet trailer was stripped when it was forwarded is retransmitted with the same
// bytes removed, i. e. the retransmitted media payload is byte identical to the
// original transmission.
#[tokio::test(flavor = "multi_thread")]
async fn test_down_track_replays_packet_trailer_strip_on_retransmit() {
    let factory = new_buffer_factory();
    let cp = Arc::new(CapturingPacer::new(pacer::PassThrough::new(
        logger::get_logger(),
        new_null_bwe(),
    )));

    let dh = new_bound_down_track(
        factory,
        vp8_codec_params(),
        cp.clone(),
        MediaEngineConfig {
            video: true,
            ..Default::default()
        },
        None,
        |_| {},
    )
    .await;
    assert_ne!(0, dh.dt.ssrc_rtx(), "RTX ssrc should be negotiated");

    let video = distinctive_payload(11, 40);
    let trailer = lkts_trailer();
    let mut payload = video.clone();
    payload.extend_from_slice(&trailer);
    let src = rtp::Packet {
        header: rtp::Header {
            version: 2,
            payload_type: 96,
            sequence_number: 3000,
            timestamp: 270000,
            ssrc: 0x44444444,
            ..Default::default()
        },
        payload: Bytes::from(payload),
        ..Default::default()
    };
    let raw = src.marshal().unwrap();

    let osn = 60000u16;
    dh.dt
        .retransmit_for_test(
            src.sequence_number as u64,
            osn,
            src.timestamp,
            src.timestamp as u64,
            0,
            &raw,
            trailer.len() as u8,
        )
        .unwrap();

    // expected RTX packet to be emitted
    eventually_async(|| !cp.rtx_packets().is_empty(), secs(5), TICK).await;

    let pr = &cp.rtx_packets()[0];
    assert_eq!(osn, u16::from_be_bytes([pr.payload[0], pr.payload[1]]));
    assert_eq!(
        &video[..],
        &pr.payload[2..],
        "retransmitted payload must not carry the packet trailer"
    );

    cp.stop();
}

// lktsTrailer builds a 15-byte LKTS packet trailer carrying a user timestamp TLV.
fn lkts_trailer() -> Vec<u8> {
    let mut trailer = vec![0x01 ^ 0xFF, 8 ^ 0xFF];
    for i in 0..8u8 {
        trailer.push(i ^ 0xFF);
    }
    trailer.push(15 ^ 0xFF);
    trailer.extend_from_slice(&packettrailer::MAGIC);
    trailer
}

// -----------------------------------------------------------------------------
// WritePaddingRTP: padding-only packets
// -----------------------------------------------------------------------------

// TestDownTrackSendsPaddingOnlyPackets exercises the WritePaddingRTP padding-only
// path. It first forwards a video keyframe (verifying the media header/payload),
// then emits padding-only packets and verifies each one on the far side: it is on
// the primary SSRC/payload type, carries the padding bit, and declares a full
// padding block.
//
// A padding-only packet is entirely padding: pion/srtp serializes the padding block
// from header.PaddingSize (via rtp.MarshalPacketTo) after the payload, so the packet
// has no payload. A receiver takes a packet with any payload left after the padding
// as media.
#[tokio::test(flavor = "multi_thread")]
async fn test_down_track_sends_padding_only_packets() {
    let factory = new_buffer_factory();
    let p = pacer::PassThrough::new(logger::get_logger(), new_null_bwe());

    let dh = new_bound_down_track(
        factory,
        vp8_codec_params(),
        p.clone(),
        MediaEngineConfig {
            video: true,
            ..Default::default()
        },
        None,
        |_| {},
    )
    .await;

    // force a valid target/current layer so the video forwarder will forward (test seam
    // also used by forwarder_test.go's disable()).
    dh.dt.force_forward_layer_for_test(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });

    // forward a keyframe so rtpStats becomes active and the stream is running
    let vp8 = codec::Vp8 {
        first_byte: 0x10,
        s: true,
        picture_id: 1,
        is_key_frame: true,
        ..Default::default()
    };
    let keyframe_payload = distinctive_payload(0x40, 50);
    let mut ep = testutils::get_test_ext_packet_vp8(
        &testutils::TestExtPacketParams {
            sequence_number: 5000,
            timestamp: 180000,
            ssrc: 0x33333333,
            payload_type: 96,
            payload_size: 50,
            is_key_frame: true,
            marker: true, // end of frame, so padding can be emitted on the frame boundary
            ..Default::default()
        },
        &vp8,
    )
    .unwrap();
    ep.packet.payload = Bytes::from(keyframe_payload.clone());
    assert_eq!(1, dh.dt.write_rtp(&ep, 0), "keyframe should forward");

    assert!(
        dh.dt.rtp_stats_active_for_test(),
        "rtpStats should be active after forwarding"
    );

    // wait for the media keyframe to arrive, then take the baseline count.
    eventually_async(|| dh.capture.count() >= 1, secs(10), TICK).await;
    let baseline = dh.capture.count();

    // emit padding-only packets. paddingOnMute=true bypasses the RR/active gating so
    // this stays deterministic; the header construction (Padding + PaddingSize) is the
    // same code path either way.
    const NUM_PADDING: usize = 3;
    let mut sent = 0;
    // expected WritePaddingRTP to emit padding packets
    eventually_async(
        || {
            if dh.dt.write_padding_rtp(500, true, false) > 0 {
                sent += 1;
            }
            sent >= NUM_PADDING
        },
        secs(5),
        TICK,
    )
    .await;

    // the padding-only packets must arrive on the far side.
    eventually_async(
        || dh.capture.count() >= baseline + NUM_PADDING,
        secs(10),
        TICK,
    )
    .await;

    let mut saw_keyframe = 0;
    let mut saw_padding = 0;
    for pkt in &dh.capture.all() {
        assert_eq!(dh.dt.ssrc(), pkt.ssrc, "packet on wrong SSRC");
        assert_eq!(96, pkt.payload_type, "packet has wrong payload type");

        if !pkt.header.padding {
            // the forwarded media keyframe
            assert!(
                pkt.header.marker,
                "forwarded keyframe should carry the marker bit"
            );
            // the forwarder prepends the VP8 payload descriptor, so the media bytes
            // appear as a suffix of the forwarded payload.
            assert!(
                pkt.payload.ends_with(&keyframe_payload),
                "forwarded keyframe payload mismatch"
            );
            saw_keyframe += 1;
            continue;
        }

        // a padding-only packet: it must declare a full padding block.
        assert_eq!(
            RTP_PADDING_MAX_PAYLOAD_SIZE, pkt.header.padding_size as usize,
            "padding-only packet must declare full padding size (sn={})",
            pkt.sequence_number
        );
        assert!(
            pkt.payload.is_empty(),
            "padding-only packet must have no payload (sn={})",
            pkt.sequence_number
        );
        saw_padding += 1;
    }
    assert_eq!(
        1, saw_keyframe,
        "expected exactly one forwarded media keyframe"
    );
    assert!(
        saw_padding >= NUM_PADDING,
        "expected the padding-only packets to be observed"
    );

    // Not in the original: what its far side has, from a run of the original with the
    // packets printed. The key frame goes out under its own sequence number and time
    // stamp, with a VP8 descriptor of one octet in front; every call for 500 octets of
    // padding gives two packets, which follow the key frame and each other in sequence
    // number, a tenth of a second apart in time stamp, without a marker.
    let all = dh.capture.all();
    assert_eq!(1 + 2 * NUM_PADDING, all.len());
    assert_eq!((5000, 180000), (all[0].sequence_number, all[0].timestamp));
    assert_eq!(
        [&[0x10u8][..], &keyframe_payload[..]].concat(),
        all[0].payload.to_vec()
    );
    for (i, pkt) in all[1..].iter().enumerate() {
        assert_eq!(
            (5001 + i as u16, 189000 + 9000 * i as u32, false),
            (pkt.sequence_number, pkt.timestamp, pkt.header.marker)
        );
    }

    // the padding in the slot of a dropped packet has no payload either
    dh.dt.write_dropped_slot_padding_for_test(6000, 190000);
    let mut found = None;
    // expected the dropped slot padding to arrive on the far side
    eventually_async(
        || {
            found = dh
                .capture
                .all()
                .into_iter()
                .find(|p| p.sequence_number == 6000);
            found.is_some()
        },
        secs(10),
        TICK,
    )
    .await;
    let pkt = found.unwrap();
    assert!(pkt.header.padding);
    assert_eq!(DROPPED_SLOT_PADDING_SIZE_FOR_TEST, pkt.header.padding_size);
    assert!(pkt.payload.is_empty());

    pacer::Pacer::stop(&*p);
}

// -----------------------------------------------------------------------------
// WriteProbePackets: RTX probe packets (with StreamAllocator + Pacer + BWE)
// -----------------------------------------------------------------------------

// capturingProbe records what the DownTrack handed the pacer for a probe packet.
#[derive(Clone)]
struct CapturingProbe {
    hdr: rtp::Header,
    payload_len: usize,
}

// capturedRTX records what the DownTrack handed the pacer for an RTX packet.
#[derive(Clone)]
struct CapturedRtx {
    hdr: rtp::Header,
    payload: Vec<u8>,
}

// capturingPacer wraps a real pacer.Pacer, records probe and RTX packets as the
// DownTrack constructs them (before the real pacer patches/marshals them), and
// delegates everything to the wrapped pacer so packets are still sent over real pion.
struct CapturingPacer {
    inner: Arc<dyn pacer::Pacer>,

    probes: Mutex<Vec<CapturingProbe>>,
    rtx: Mutex<Vec<CapturedRtx>>,
}

impl CapturingPacer {
    fn new(inner: Arc<dyn pacer::Pacer>) -> CapturingPacer {
        CapturingPacer {
            inner,
            probes: Mutex::new(Vec::new()),
            rtx: Mutex::new(Vec::new()),
        }
    }

    fn probe_packets(&self) -> Vec<CapturingProbe> {
        self.probes.lock().clone()
    }

    fn rtx_packets(&self) -> Vec<CapturedRtx> {
        self.rtx.lock().clone()
    }

    fn stop(&self) {
        self.inner.stop();
    }
}

impl pacer::Pacer for CapturingPacer {
    fn enqueue(&self, p: pacer::Packet) {
        if p.is_probe {
            self.probes.lock().push(CapturingProbe {
                hdr: p.header.clone(),
                payload_len: p.payload.len(),
            });
        } else if p.is_rtx {
            self.rtx.lock().push(CapturedRtx {
                hdr: p.header.clone(),
                payload: p.payload.to_vec(),
            });
        }
        self.inner.enqueue(p);
    }

    fn stop(&self) {
        self.inner.stop();
    }

    fn set_interval(&self, interval: Duration) {
        self.inner.set_interval(interval);
    }

    fn set_bitrate(&self, bitrate: i64) {
        self.inner.set_bitrate(bitrate);
    }

    fn time_since_last_sent_packet(&self) -> Duration {
        self.inner.time_since_last_sent_packet()
    }

    fn set_pacer_probe_observer_listener(
        &self,
        listener: Option<Arc<dyn pacer::PacerProbeObserverListener>>,
    ) {
        self.inner.set_pacer_probe_observer_listener(listener);
    }

    fn start_probe_cluster(&self, pci: ccutils::ProbeClusterInfo) {
        self.inner.start_probe_cluster(pci);
    }

    fn end_probe_cluster(
        &self,
        probe_cluster_id: ccutils::ProbeClusterId,
    ) -> ccutils::ProbeClusterInfo {
        self.inner.end_probe_cluster(probe_cluster_id)
    }
}

// feedReceiverReport hands the DownTrack a synthetic RTCP receiver report for its
// primary SSRC via the same handleRTCP path production uses. This deterministically
// sets LastReceiverReportTime() (a precondition for WriteProbePackets) without
// depending on the timing of the far side's periodic RRs.
fn feed_receiver_report(dt: &DownTrack, last_seq_no: u16) {
    let rr = ReceiverReport {
        ssrc: 0x9999,
        reports: vec![ReceptionReport {
            ssrc: dt.ssrc(),
            last_sender_report: 0,
            last_sequence_number: last_seq_no as u32,
            ..Default::default()
        }],
        ..Default::default()
    };
    let raw = rtcp::packet::marshal(&[Box::new(rr)]).unwrap();
    dt.handle_rtcp_for_test(&raw);
    assert_ne!(
        0,
        dt.last_receiver_report_time_for_test(),
        "synthetic RR should set LastReceiverReportTime"
    );
}

// New for musivum, in the place of `TestDownTrackSendsProbePackets`, which wires the down
// track to a real stream allocator and to both bandwidth estimators and is not ported (see
// the head of this file). What that test asserts of the down track is asserted here with a
// fake stream allocator listener in the allocator's place, once per type of estimator: the
// listener is what makes the down track take the id of the estimator's header extension,
// without which it sends no probes; a forwarded key frame arrives; a receiver report for
// the stream opens the way for probes; and every probe is padding only, on the RTX stream.
#[tokio::test(flavor = "multi_thread")]
async fn probe_packets_go_out_on_the_rtx_stream_as_padding() {
    for (name, bwe_type) in [
        ("sendside", bwe::BweType::SendSide),
        ("remote", bwe::BweType::Remote),
    ] {
        let factory = new_buffer_factory();
        let cp = Arc::new(CapturingPacer::new(pacer::PassThrough::new(
            logger::get_logger(),
            new_null_bwe(),
        )));

        // header extensions (abs-send-time for remote BWE, transport-cc for send-side)
        // are needed for the DownTrack to pick up an ext id, which WriteProbePackets
        // requires.
        let dh = new_bound_down_track(
            factory,
            vp8_codec_params(),
            cp.clone(),
            MediaEngineConfig {
                video: true,
                header_extensions: true,
            },
            None,
            |_| {},
        )
        .await;

        assert_ne!(0, dh.dt.ssrc_rtx(), "{name}: RTX ssrc should be negotiated");
        assert_ne!(
            0,
            dh.dt.payload_type_rtx_for_test(),
            "{name}: RTX payload type should be negotiated"
        );

        // without a stream allocator listener neither id is taken, and nothing probes
        dh.dt.set_rtp_header_extensions_for_test();
        assert_eq!((0, 0), dh.dt.ext_ids_for_test(), "{name}");

        let sal = Arc::new(FakeDownTrackStreamAllocatorListener::default());
        sal.is_bwe_enabled_returns(true);
        sal.bwe_type_returns(bwe_type);
        sal.is_subscribe_mutable_returns(true);
        let weak_sal = Arc::downgrade(&sal);
        let weak_sal: Weak<dyn DownTrackStreamAllocatorListener> = weak_sal;
        dh.dt.set_stream_allocator_listener(Some(weak_sal));
        assert_eq!(
            1,
            sal.on_subscription_changed_call_count(),
            "{name}: a new listener is told to allocate"
        );
        let want_ids = if bwe_type == bwe::BweType::Remote {
            (1, 0)
        } else {
            (0, 2)
        };
        assert_eq!(want_ids, dh.dt.ext_ids_for_test(), "{name}");

        // force a valid target/current layer so the video forwarder will forward.
        dh.dt.force_forward_layer_for_test(buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        });

        // forward a keyframe and verify it arrives with the expected header/payload.
        let vp8 = codec::Vp8 {
            first_byte: 0x10,
            s: true,
            picture_id: 1,
            is_key_frame: true,
            ..Default::default()
        };
        let keyframe_payload = distinctive_payload(0x50, 60);
        let mut ep = testutils::get_test_ext_packet_vp8(
            &testutils::TestExtPacketParams {
                sequence_number: 6000,
                timestamp: 270000,
                ssrc: 0x44444444,
                payload_type: 96,
                payload_size: 60,
                is_key_frame: true,
                marker: true,
                ..Default::default()
            },
            &vp8,
        )
        .unwrap();
        ep.packet.payload = Bytes::from(keyframe_payload.clone());

        // no probes before anything was sent
        assert_eq!(0, dh.dt.write_probe_packets(2000, true), "{name}");

        assert_eq!(
            1,
            dh.dt.write_rtp(&ep, 0),
            "{name}: keyframe should forward"
        );
        assert!(dh.dt.rtp_stats_active_for_test(), "{name}");

        eventually_async(|| dh.capture.count() >= 1, secs(10), TICK).await;

        let media = dh.capture.all()[0].clone();
        assert_eq!(
            dh.dt.ssrc(),
            media.ssrc,
            "{name}: forwarded media on wrong SSRC"
        );
        assert_eq!(
            96, media.payload_type,
            "{name}: forwarded media has wrong payload type"
        );
        assert!(
            !media.header.padding,
            "{name}: forwarded media must not carry padding bit"
        );
        // the forwarder prepends the VP8 payload descriptor, so the media bytes
        // appear as a suffix of the forwarded payload.
        assert!(
            media.payload.ends_with(&keyframe_payload),
            "{name}: forwarded media payload mismatch"
        );
        // the extension the estimator needs is on the packet, and only that one
        let (abs_send_time_id, transport_wide_id) = want_ids;
        assert_eq!(
            abs_send_time_id != 0,
            media.header.get_extension(1).is_some(),
            "{name}"
        );
        assert_eq!(
            transport_wide_id != 0,
            media.header.get_extension(2).is_some(),
            "{name}"
        );

        // no probes until the far side has reported on the stream: a report on another
        // stream does not count
        let other = ReceiverReport {
            ssrc: 0x9999,
            reports: vec![ReceptionReport {
                ssrc: dh.dt.ssrc() + 100,
                last_sequence_number: media.sequence_number as u32,
                ..Default::default()
            }],
            ..Default::default()
        };
        dh.dt
            .handle_rtcp_for_test(&rtcp::packet::marshal(&[Box::new(other)]).unwrap());
        assert_eq!(0, dh.dt.last_receiver_report_time_for_test(), "{name}");
        assert_eq!(0, dh.dt.write_probe_packets(2000, true), "{name}");
        assert!(cp.probe_packets().is_empty(), "{name}");

        // satisfy the RR precondition deterministically, referencing the
        // subscriber-facing sequence number of the forwarded keyframe.
        feed_receiver_report(&dh.dt, media.sequence_number);

        // emit RTX probe packets.
        const NUM_PROBE: usize = 3;
        // expected WriteProbePackets to emit RTX probe packets
        eventually_async(
            || {
                dh.dt.write_probe_packets(2000, true);
                cp.probe_packets().len() >= NUM_PROBE
            },
            secs(5),
            TICK,
        )
        .await;

        // every probe packet must be on the RTX stream, carry the padding bit, declare
        // the full padding size, and have no payload: pion serializes the padding from
        // the header.
        for pr in &cp.probe_packets() {
            assert_eq!(2, pr.hdr.version, "{name}: probe packet version");
            assert_eq!(
                dh.dt.ssrc_rtx(),
                pr.hdr.ssrc,
                "{name}: probe packet must be on the RTX stream"
            );
            assert_eq!(
                dh.dt.payload_type_rtx_for_test(),
                pr.hdr.payload_type as u32,
                "{name}: probe packet must use the RTX payload type"
            );
            assert!(
                pr.hdr.padding,
                "{name}: probe packet must carry padding bit"
            );
            assert_eq!(
                RTP_PADDING_MAX_PAYLOAD_SIZE, pr.hdr.padding_size as usize,
                "{name}: probe packet must set header.PaddingSize"
            );
            assert_eq!(
                0, pr.payload_len,
                "{name}: probe packet must have no payload"
            );
        }

        cp.stop();
    }
}

// -----------------------------------------------------------------------------
// keyFrameRequester: PLI only while a layer lock is pending
// -----------------------------------------------------------------------------

// TestDownTrackKeyFrameRequesterArmsOnlyWhilePending verifies that the key frame
// requester keeps sending PLIs while the target layer is not locked, and goes quiet
// once a key frame on the target layer locks it (no periodic wake-ups while locked).
#[tokio::test(flavor = "multi_thread")]
async fn test_down_track_key_frame_requester_arms_only_while_pending() {
    let factory = new_buffer_factory();
    let p = pacer::PassThrough::new(logger::get_logger(), new_null_bwe());

    let dh = new_bound_down_track(
        factory,
        vp8_codec_params(),
        p.clone(),
        MediaEngineConfig {
            video: true,
            ..Default::default()
        },
        None,
        |_| {},
    )
    .await;

    // allocate a target layer (spatial 0) without any media, so the lock is pending
    let mut brs = Bitrates::default();
    brs[0][0] = 100_000;
    dh.receiver.get_layered_bitrate_returns((vec![0], brs));
    dh.dt.up_track_max_published_layer_change(0);
    dh.dt.set_max_spatial_layer(0);
    let alloc = dh.dt.allocate_optimal(false, false);
    assert_eq!(
        0, alloc.target_layer.spatial,
        "expected target layer to be allocated"
    );

    // unlocked: PLIs must keep coming at the key frame interval
    //
    // expected repeated PLIs while layer lock is pending
    eventually_async(|| dh.receiver.send_pli_call_count() >= 3, secs(5), TICK).await;

    // a key frame on the target layer locks it
    let vp8 = codec::Vp8 {
        first_byte: 0x10,
        s: true,
        picture_id: 1,
        is_key_frame: true,
        ..Default::default()
    };
    let mut ep = testutils::get_test_ext_packet_vp8(
        &testutils::TestExtPacketParams {
            sequence_number: 5000,
            timestamp: 180000,
            ssrc: 0x33333333,
            payload_type: 96,
            payload_size: 50,
            is_key_frame: true,
            marker: true,
            ..Default::default()
        },
        &vp8,
    )
    .unwrap();
    ep.packet.payload = Bytes::from(distinctive_payload(0x40, 50));
    assert_eq!(1, dh.dt.write_rtp(&ep, 0), "keyframe should forward");

    // let an already armed interval fire, then the requester must stay asleep
    tokio::time::sleep((2 * KEY_FRAME_INTERVAL_MAX_FOR_TEST).to_std()).await;
    let before = dh.receiver.send_pli_call_count();
    let wakeups = dh.dt.key_frame_requester_wakeups_for_test();
    tokio::time::sleep((2 * KEY_FRAME_INTERVAL_MAX_FOR_TEST).to_std()).await;
    assert_eq!(
        wakeups,
        dh.dt.key_frame_requester_wakeups_for_test(),
        "key frame requester must not wake up once the layer is locked"
    );
    assert_eq!(
        before,
        dh.receiver.send_pli_call_count(),
        "no PLI expected once the layer is locked"
    );

    // a resync drops the lock and must wake the requester again
    dh.dt.resync();
    // expected PLI after resync
    eventually_async(|| dh.receiver.send_pli_call_count() > before, secs(5), TICK).await;

    pacer::Pacer::stop(&*p);
}

// -----------------------------------------------------------------------------
// bind, close, and what is left afterwards
// -----------------------------------------------------------------------------

// New for musivum: the original has no test of a bind that waits for the receiver, of
// closing, or of what holds a down track afterwards, which in Go is the garbage
// collector's matter. Here a closed down track must be free once its owner lets go of it:
// neither its workers nor what it registered elsewhere may keep it.
#[tokio::test(flavor = "multi_thread")]
async fn a_down_track_binds_when_its_receiver_is_ready_and_is_let_go_of_when_closed() {
    let factory = new_buffer_factory();
    let p = pacer::PassThrough::new(logger::get_logger(), new_null_bwe());
    let capture = Arc::new(PacketCapture::default());
    let me_cfg = MediaEngineConfig {
        video: true,
        ..Default::default()
    };

    // a receiver that is not ready yet: it keeps the callback
    let rcv = new_fake_track_receiver(&vp8_codec_params());
    type OnReady = Box<dyn Fn() + Send + Sync>;
    let on_ready: Arc<Mutex<Vec<OnReady>>> = Arc::default();
    let kept = on_ready.clone();
    rcv.add_on_ready_calls(move |f| kept.lock().push(f));
    let listener = Arc::new(FakeDownTrackListener::default());
    let dt = new_down_track(
        &factory,
        vp8_codec_params(),
        p.clone(),
        None,
        &rcv,
        &listener,
    );
    let transceiver = new_transceiver(me_cfg);
    dt.set_transceiver(transceiver.clone());
    let bindings = Arc::new(Mutex::new(Vec::new()));
    let b = bindings.clone();
    dt.on_binding(move |res| b.lock().push(res));

    // the connection binds the track: the codec is known, but the down track is not bound
    // and nothing can be written
    let ctx = new_context(me_cfg, &capture);
    let codec = webrtc::TrackLocal::bind(&*dt, ctx.clone()).unwrap();
    assert_eq!(VP8_PAYLOAD_TYPE, codec.payload_type);
    dt.set_connected();
    assert_eq!(1, listener.on_codec_negotiated_call_count());
    assert!(bindings.lock().is_empty());
    assert!(!dt.is_writable_for_test());
    assert_eq!(0, dt.ssrc());
    assert!(dt.create_source_description_chunks().is_empty());
    // a second bind is refused, with the original's text
    assert_eq!(
        Err(webrtc::Error::Other("already bound".to_string())),
        webrtc::TrackLocal::bind(&*dt, ctx.clone()).map(|_| ())
    );

    // the receiver is ready: the bind is finished
    let ready = on_ready.lock().pop().expect("the down track waits");
    ready();
    assert_eq!(vec![Ok(())], *bindings.lock());
    assert!(dt.is_writable_for_test());
    assert_eq!((TEST_SSRC, TEST_SSRC_RTX), (dt.ssrc(), dt.ssrc_rtx()));
    assert_eq!(RTX_PAYLOAD_TYPE as u32, dt.payload_type_rtx_for_test());
    eventually_async(
        || listener.on_bind_and_connected_call_count() == 1,
        secs(5),
        TICK,
    )
    .await;
    // being told again changes nothing
    ready();
    assert_eq!(1, bindings.lock().len());
    drop(ready);

    // a key frame is forwarded
    dt.force_forward_layer_for_test(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    let vp8 = codec::Vp8 {
        first_byte: 0x10,
        s: true,
        picture_id: 1,
        is_key_frame: true,
        ..Default::default()
    };
    let ep = testutils::get_test_ext_packet_vp8(
        &testutils::TestExtPacketParams {
            sequence_number: 5000,
            timestamp: 180000,
            ssrc: 0x33333333,
            payload_type: 96,
            payload_size: 50,
            is_key_frame: true,
            marker: true,
            ..Default::default()
        },
        &vp8,
    )
    .unwrap();
    assert_eq!(1, dt.write_rtp(&ep, 0));
    assert_eq!(1, capture.count());

    // RTCP for the stream reaches the down track through the reader it took from the
    // factory
    let reader = factory
        .get_rtcp_reader(TEST_SSRC)
        .expect("the down track took a reader for its stream");
    assert!(factory.get_rtcp_reader(TEST_SSRC_RTX).is_some());
    let rr = ReceiverReport {
        ssrc: 0x9999,
        reports: vec![ReceptionReport {
            ssrc: TEST_SSRC,
            last_sequence_number: 5000,
            ..Default::default()
        }],
        ..Default::default()
    };
    reader
        .write(&rtcp::packet::marshal(&[Box::new(rr)]).unwrap())
        .unwrap();
    assert_ne!(0, dt.last_receiver_report_time_for_test());
    let chunks = dt.create_source_description_chunks();
    assert_eq!(1, chunks.len());
    assert_eq!(TEST_SSRC, chunks[0].source);
    assert_eq!(&b"fake-stream"[..], &chunks[0].items[0].text[..]);

    // closing flushes the decoder with blank key frames, and waits for them
    dt.close();
    assert!(dt.is_closed());
    assert!(!dt.is_writable_for_test());
    let all = capture.all();
    // 0.2 s at 30 frames per second
    assert_eq!(1 + 6, all.len());
    for (i, pkt) in all[1..].iter().enumerate() {
        assert_eq!(5001 + i as u16, pkt.sequence_number);
        assert!(pkt.header.marker && !pkt.header.padding);
        assert!(pkt.payload.ends_with(&super::VP8_KEY_FRAME_8X8));
    }
    assert_eq!(0, dt.write_rtp(&ep, 0));

    // everybody is told, once
    assert_eq!(1, rcv.delete_down_track_call_count());
    assert_eq!(dt.subscriber_id(), rcv.delete_down_track_args_for_call(0));
    assert_eq!(1, listener.on_down_track_close_call_count());
    assert!(!listener.on_down_track_close_args_for_call(0));
    eventually_async(
        || {
            let n = listener.on_max_subscribed_layer_changed_call_count();
            n > 0
                && listener.on_max_subscribed_layer_changed_args_for_call(n - 1)
                    == buffer::INVALID_LAYER_SPATIAL
        },
        secs(5),
        TICK,
    )
    .await;
    assert!(factory.get_rtcp_reader(TEST_SSRC).is_none());
    assert!(factory.get_rtcp_reader(TEST_SSRC_RTX).is_none());
    dt.close();
    dt.set_connected();
    assert_eq!(1, listener.on_down_track_close_call_count());
    assert!(!dt.is_writable_for_test());

    // and nothing holds it
    let weak = Arc::downgrade(&dt);
    drop(dt);
    eventually_async(|| weak.upgrade().is_none(), secs(5), TICK).await;

    // nor one that is dropped without having been closed
    let rcv = new_fake_track_receiver(&vp8_codec_params());
    let dt = new_down_track(
        &factory,
        vp8_codec_params(),
        p.clone(),
        None,
        &rcv,
        &listener,
    );
    dt.set_transceiver(transceiver.clone());
    dt.bind(new_context(me_cfg, &capture)).unwrap();
    dt.set_connected();
    assert!(dt.is_writable_for_test());
    let weak = Arc::downgrade(&dt);
    drop(dt);
    eventually_async(|| weak.upgrade().is_none(), secs(5), TICK).await;

    // A publisher that changes its codec, for a subscriber that cannot follow: the down
    // track is given the new receiver and closes itself, from a task of its own. That close
    // runs on a thread of the runtime, and still its blank frames are written while it
    // waits for them.
    let capture = Arc::new(PacketCapture::default());
    let listener = Arc::new(FakeDownTrackListener::default());
    let rcv = new_fake_track_receiver(&vp8_codec_params());
    let dt = new_down_track(
        &factory,
        vp8_codec_params(),
        p.clone(),
        None,
        &rcv,
        &listener,
    );
    dt.set_transceiver(transceiver.clone());
    dt.bind(new_context(me_cfg, &capture)).unwrap();
    dt.set_connected();
    dt.force_forward_layer_for_test(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    assert_eq!(1, dt.write_rtp(&ep, 0));
    assert_eq!(1, capture.count());

    let other = new_fake_track_receiver(&webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: webrtc::MIME_TYPE_H264.to_string(),
            clock_rate: 90000,
            ..Default::default()
        },
        payload_type: 102,
    });
    let receiver: Arc<dyn TrackReceiver> = other.clone();
    dt.set_receiver(receiver);
    assert_eq!(1, rcv.delete_down_track_call_count());
    assert_eq!(1, other.add_down_track_call_count());
    assert_eq!("video/H264", dt.receiver().codec().mime_type);
    eventually_async(
        || listener.on_down_track_close_call_count() == 1,
        secs(5),
        TICK,
    )
    .await;
    assert!(dt.is_closed());
    assert_eq!(1, other.delete_down_track_call_count());
    assert_eq!(0, rcv.add_down_track_call_count());
    assert_eq!(1 + 6, capture.count());

    pacer::Pacer::stop(&*p);
}
