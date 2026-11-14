//! The experiment for step 3 of docs/PORTING.md, on webrtc-rs's `rtc`: a forwarder that does
//! what LiveKit does with a WebRTC library, small enough to read in one sitting.
//!
//! One browser page publishes video (VP8, three simulcast layers) and audio; another receives
//! one layer, on a peer connection the server offers. No interceptors are registered: the
//! program itself rewrites RTP, answers NACKs with RTX, sets the transport-wide sequence
//! number, passes PLIs on, sends REMB, pads on the RTX stream, and counts every RTP and RTCP
//! packet it is handed, so that the driver can check what the library passes through.
//!
//! All peer connections share one UDP socket, as LiveKit's do.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use axum::extract::State;
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::{Bytes, BytesMut};
use rtc::data_channel::RTCDataChannelInit;
use rtc::interceptor::{Attribute, Interceptor, Packet, Registry, Slot, StreamInfo, TaggedPacket};
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::media_engine::{
    MIME_TYPE_OPUS, MIME_TYPE_VP8, MediaEngine,
};
use rtc::peer_connection::configuration::setting_engine::SettingEngineBuilder;
use rtc::peer_connection::event::{RTCDataChannelEvent, RTCPeerConnectionEvent, RTCTrackEvent};
use rtc::peer_connection::message::{RTCMessage, TaggedRTCMessage};
use rtc::peer_connection::sdp::RTCSessionDescription;
use rtc::peer_connection::transport::{
    CandidateConfig, CandidateHostConfig, RTCDtlsRole, RTCIceCandidate,
};
use rtc::peer_connection::{RTCPeerConnection, RTCPeerConnectionBuilder};
use rtc::rtcp;
use rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest;
use rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use rtc::rtcp::payload_feedbacks::receiver_estimated_maximum_bitrate::ReceiverEstimatedMaximumBitrate;
use rtc::rtcp::receiver_report::ReceiverReport;
use rtc::rtcp::sender_report::SenderReport;
use rtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;
use rtc::rtcp::transport_feedbacks::transport_layer_nack::{NackPair, TransportLayerNack};
use rtc::rtp;
use rtc::rtp_transceiver::rtp_sender::{
    RTCPFeedback, RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters,
    RTCRtpEncodingParameters, RTCRtpHeaderExtensionCapability, RTCRtpRtxParameters, RtpCodecKind,
};
use rtc::rtp_transceiver::{RTCRtpReceiverId, RTCRtpSenderId};
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};

const MID_URI: &str = "urn:ietf:params:rtp-hdrext:sdes:mid";
const RID_URI: &str = "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id";
const RRID_URI: &str = "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id";
const TWCC_URI: &str = "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01";
const ABS_SEND_TIME_URI: &str = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time";
const AUDIO_LEVEL_URI: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    Publisher,
    Subscriber,
}

/// What the program was handed by the library and what it sent, for the driver to read.
#[derive(Default, Serialize, Clone)]
struct Counters {
    /// RTP received from the publisher, by "ssrc/pt" with the rid where the library knows it.
    rtp_in: BTreeMap<String, u64>,
    /// Header extension ids seen on RTP from the publisher, by id.
    rtp_in_extensions: BTreeMap<String, u64>,
    /// RTCP handed over, by peer role and packet type.
    rtcp_in: BTreeMap<String, u64>,
    rtp_forwarded: u64,
    rtp_dropped_on_purpose: u64,
    nack_sequence_numbers_requested: u64,
    rtx_sent: u64,
    rtx_not_in_cache: u64,
    padding_sent: u64,
    padding_wire_sizes: BTreeMap<String, u64>,
    pli_passed_on: u64,
    remb_sent: u64,
    twcc_feedback_packets: u64,
    twcc_feedback_statuses: u64,
    datachannel_messages: u64,
    write_errors: BTreeMap<String, u64>,
    /// What the tap right above SRTP saw, by peer: "Publisher 0", "Subscriber 1".
    wire: BTreeMap<String, WireView>,
    nacks_sent_to_publisher: u64,
    events: Vec<String>,
}

#[derive(Default, Serialize, Clone)]
struct WireView {
    rtp_in: BTreeMap<String, u64>,
    rtcp_in: u64,
    rtp_out: BTreeMap<String, u64>,
    rtcp_out: u64,
}

struct Peer {
    id: usize,
    role: Role,
    pc: RTCPeerConnection,
    wire: Arc<Mutex<WireCounts>>,
    ufrag: String,
    remote: Option<SocketAddr>,
    // Publisher: the tracks that opened.
    receivers: HashMap<String, RTCRtpReceiverId>,
    // Subscriber: what is sent.
    video_sender: Option<RTCRtpSenderId>,
    audio_sender: Option<RTCRtpSenderId>,
    video_ssrc: u32,
    video_rtx_ssrc: u32,
    audio_ssrc: u32,
    out: Option<Outgoing>,
}

/// The negotiated numbers of a subscriber's video sender, and the rewriting state.
struct Outgoing {
    video_pt: u8,
    rtx_pt: u8,
    audio_pt: u8,
    twcc_id: Option<u8>,
    next_seq: u16,
    next_rtx_seq: u16,
    next_audio_seq: u16,
    twcc_seq: u16,
    last_timestamp: u32,
    // The publisher's layers each count RTP time from a start of their own. What is sent has
    // to go on from where it was when the forwarded layer changes: a jump backwards and the
    // receiver throws the frames away as old, without a word. (LiveKit's RTPMunger does this
    // properly; this is the least that works.)
    timestamp_offset: u32,
    source_ssrc: Option<u32>,
    // What was sent, to answer NACKs from: sequence number to packet.
    cache: VecDeque<(u16, rtp::Packet)>,
}

enum Cmd {
    Publish(String, oneshot::Sender<Result<String>>),
    SubscribeOffer(oneshot::Sender<Result<(usize, String)>>),
    SubscribeAnswer(usize, String, oneshot::Sender<Result<()>>),
    Stats(oneshot::Sender<Counters>),
    Control(Control, oneshot::Sender<()>),
}

#[derive(Deserialize, Default, Clone)]
struct Control {
    /// Drop this share of the video packets that would be forwarded (0.0 to 1.0).
    drop: Option<f64>,
    /// Forward this simulcast layer.
    rid: Option<String>,
    /// Send this many padding-only packets on the subscriber's RTX stream.
    padding_packets: Option<u32>,
    /// Ask the publisher to send the last packets of the forwarded layer again (a NACK for
    /// this many of them), to see how its RTX arrives.
    nack_publisher: Option<u16>,
    reset_counters: Option<bool>,
}

struct Server {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddr,
    peers: Vec<Peer>,
    by_addr: HashMap<SocketAddr, usize>,
    counters: Counters,
    drop: f64,
    rid: String,
    // ssrc of the publisher's layer being forwarded, once known.
    forwarded_ssrc: Option<u32>,
    last_publisher_seq: Option<u16>,
    publisher_layer_ssrcs: HashMap<String, u32>,
    last_remb: Instant,
}

fn media_engine(role: Role) -> Result<MediaEngine> {
    let mut m = MediaEngine::default();
    let mut video_feedback = vec![
        RTCPFeedback { typ: "nack".into(), parameter: "".into() },
        RTCPFeedback { typ: "nack".into(), parameter: "pli".into() },
        RTCPFeedback { typ: "ccm".into(), parameter: "fir".into() },
    ];
    // Towards the publisher the program plays a receiver that estimates itself (REMB);
    // towards the subscriber it wants transport-wide feedback back.
    match role {
        Role::Publisher => {
            video_feedback.push(RTCPFeedback { typ: "goog-remb".into(), parameter: "".into() })
        }
        Role::Subscriber => {
            video_feedback.push(RTCPFeedback { typ: "transport-cc".into(), parameter: "".into() })
        }
    }
    m.register_codec(
        RTCRtpCodecParameters {
            rtp_codec: RTCRtpCodec {
                mime_type: MIME_TYPE_VP8.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "".to_owned(),
                rtcp_feedback: video_feedback,
            },
            payload_type: 96,
            ..Default::default()
        },
        RtpCodecKind::Video,
    )?;
    m.register_codec(
        RTCRtpCodecParameters {
            rtp_codec: RTCRtpCodec {
                mime_type: "video/rtx".to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "apt=96".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 97,
            ..Default::default()
        },
        RtpCodecKind::Video,
    )?;
    m.register_codec(
        RTCRtpCodecParameters {
            rtp_codec: RTCRtpCodec {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 111,
            ..Default::default()
        },
        RtpCodecKind::Audio,
    )?;
    let mut video_exts = vec![MID_URI, RID_URI, RRID_URI, ABS_SEND_TIME_URI];
    if role == Role::Subscriber {
        video_exts.push(TWCC_URI);
    }
    for uri in video_exts {
        m.register_header_extension(
            RTCRtpHeaderExtensionCapability { uri: uri.to_owned() },
            RtpCodecKind::Video,
            None,
        )?;
    }
    for uri in [MID_URI, AUDIO_LEVEL_URI] {
        m.register_header_extension(
            RTCRtpHeaderExtensionCapability { uri: uri.to_owned() },
            RtpCodecKind::Audio,
            None,
        )?;
    }
    Ok(m)
}

impl Server {
    fn new_peer(&mut self, role: Role) -> Result<usize> {
        let id = self.peers.len();
        let ufrag = format!("mus{id:04}{:08x}", rand::random::<u32>());
        let pwd = format!("{:032x}", rand::random::<u128>());
        let setting_engine = SettingEngineBuilder::new()
            .with_lite(true)
            .with_answering_dtls_role(RTCDtlsRole::Server)
            .with_ice_credentials(ufrag.clone(), pwd)
            .build();
        // None of the library's interceptors: whatever happens to RTP and RTCP, this program
        // does. Only its own tap, closest to the wire.
        let wire = Arc::new(Mutex::new(WireCounts::default()));
        let registry = Registry::new().with(
            Slot::Custom(100),
            Tap { counts: wire.clone(), read_queue: VecDeque::new(), write_queue: VecDeque::new() },
        );
        let pc = RTCPeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_setting_engine(setting_engine)
            .with_media_engine(media_engine(role)?)
            .with_interceptor_registry(registry)
            .build(Instant::now())?;
        self.peers.push(Peer {
            id,
            role,
            pc,
            wire,
            ufrag,
            remote: None,
            receivers: HashMap::new(),
            video_sender: None,
            audio_sender: None,
            video_ssrc: rand::random(),
            video_rtx_ssrc: rand::random(),
            audio_ssrc: rand::random(),
            out: None,
        });
        Ok(id)
    }

    fn add_local_candidate(&mut self, id: usize) -> Result<()> {
        let candidate = CandidateHostConfig {
            base_config: CandidateConfig {
                network: "udp".to_owned(),
                address: self.local_addr.ip().to_string(),
                port: self.local_addr.port(),
                component: 1,
                ..Default::default()
            },
            ..Default::default()
        }
        .new_candidate_host()?;
        self.peers[id].pc.add_local_candidate(RTCIceCandidate::from(&candidate).to_json()?)?;
        Ok(())
    }

    fn publish(&mut self, offer: String) -> Result<String> {
        let id = self.new_peer(Role::Publisher)?;
        let now = Instant::now();
        let peer = &mut self.peers[id];
        peer.pc.set_remote_description(now, RTCSessionDescription::offer(offer)?)?;
        self.add_local_candidate(id)?;
        let peer = &mut self.peers[id];
        let answer = peer.pc.create_answer(None)?;
        peer.pc.set_local_description(now, answer)?;
        let local = peer.pc.local_description().context("no local description")?;
        Ok(local.sdp)
    }

    fn subscribe_offer(&mut self) -> Result<(usize, String)> {
        let id = self.new_peer(Role::Subscriber)?;
        let now = Instant::now();
        let peer = &mut self.peers[id];
        let video = MediaStreamTrack::new(
            "musivum-stream".to_owned(),
            "video0".to_owned(),
            "video0".to_owned(),
            RtpCodecKind::Video,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(peer.video_ssrc),
                    rtx: Some(RTCRtpRtxParameters { ssrc: peer.video_rtx_ssrc }),
                    ..Default::default()
                },
                codec: RTCRtpCodec {
                    mime_type: MIME_TYPE_VP8.to_owned(),
                    clock_rate: 90000,
                    ..Default::default()
                },
                ..Default::default()
            }],
        );
        let audio = MediaStreamTrack::new(
            "musivum-stream".to_owned(),
            "audio0".to_owned(),
            "audio0".to_owned(),
            RtpCodecKind::Audio,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(peer.audio_ssrc),
                    ..Default::default()
                },
                codec: RTCRtpCodec {
                    mime_type: MIME_TYPE_OPUS.to_owned(),
                    clock_rate: 48000,
                    channels: 2,
                    ..Default::default()
                },
                ..Default::default()
            }],
        );
        peer.video_sender = Some(peer.pc.add_track(video)?);
        peer.audio_sender = Some(peer.pc.add_track(audio)?);
        // A data channel the server opens, as LiveKit does on the subscriber connection.
        peer.pc.create_data_channel(
            "_reliable",
            Some(RTCDataChannelInit { ordered: true, ..Default::default() }),
        )?;
        let offer = peer.pc.create_offer(None)?;
        peer.pc.set_local_description(now, offer)?;
        self.add_local_candidate(id)?;
        let local = self.peers[id].pc.local_description().context("no local description")?;
        Ok((id, local.sdp))
    }

    fn subscribe_answer(&mut self, id: usize, answer: String) -> Result<()> {
        let peer = self.peers.get_mut(id).ok_or_else(|| anyhow!("no such peer"))?;
        peer.pc.set_remote_description(Instant::now(), RTCSessionDescription::answer(answer)?)?;
        // What was negotiated for the senders.
        let mut out = Outgoing {
            video_pt: 0,
            rtx_pt: 0,
            audio_pt: 0,
            twcc_id: None,
            next_seq: rand::random(),
            next_rtx_seq: rand::random(),
            next_audio_seq: rand::random(),
            twcc_seq: 0,
            last_timestamp: 0,
            timestamp_offset: 0,
            source_ssrc: None,
            cache: VecDeque::new(),
        };
        if let Some(sender_id) = peer.video_sender {
            let mut sender = peer.pc.rtp_sender(sender_id).context("no video sender")?;
            let params = sender.get_parameters().clone();
            for c in &params.rtp_parameters.codecs {
                if c.rtp_codec.mime_type.eq_ignore_ascii_case(MIME_TYPE_VP8) {
                    out.video_pt = c.payload_type;
                }
                if c.rtp_codec.mime_type.eq_ignore_ascii_case("video/rtx") {
                    out.rtx_pt = c.payload_type;
                }
            }
            for e in &params.rtp_parameters.header_extensions {
                if e.uri == TWCC_URI {
                    out.twcc_id = Some(e.id as u8);
                }
            }
            self.counters.events.push(format!(
                "subscriber {id}: video pt {} rtx pt {} twcc ext {:?}; sender codecs {:?}; extensions {:?}",
                out.video_pt,
                out.rtx_pt,
                out.twcc_id,
                params
                    .rtp_parameters
                    .codecs
                    .iter()
                    .map(|c| format!("{}:{}", c.rtp_codec.mime_type, c.payload_type))
                    .collect::<Vec<_>>(),
                params
                    .rtp_parameters
                    .header_extensions
                    .iter()
                    .map(|e| format!("{}:{}", e.id, e.uri.rsplit([':', '/']).next().unwrap_or("")))
                    .collect::<Vec<_>>(),
            ));
        }
        if let Some(sender_id) = peer.audio_sender {
            let mut sender = peer.pc.rtp_sender(sender_id).context("no audio sender")?;
            for c in &sender.get_parameters().rtp_parameters.codecs {
                if c.rtp_codec.mime_type.eq_ignore_ascii_case(MIME_TYPE_OPUS) {
                    out.audio_pt = c.payload_type;
                }
            }
        }
        peer.out = Some(out);
        Ok(())
    }

    /// Finds the peer a datagram is for: by the address it comes from, or, the first time, by
    /// the ICE user name in its STUN binding request.
    fn route(&mut self, from: SocketAddr, data: &[u8]) -> Option<usize> {
        if let Some(&id) = self.by_addr.get(&from) {
            return Some(id);
        }
        let ufrag = stun_username(data)?;
        let local = ufrag.split(':').next()?;
        let id = self.peers.iter().position(|p| p.ufrag == local)?;
        self.by_addr.insert(from, id);
        self.peers[id].remote = Some(from);
        self.counters.events.push(format!("peer {id} is at {from}"));
        Some(id)
    }

    fn count_error(&mut self, what: &str, err: impl std::fmt::Display) {
        *self.counters.write_errors.entry(format!("{what}: {err}")).or_default() += 1;
    }

    /// Takes everything the peers have to say: datagrams to send, events, RTP and RTCP.
    async fn drain(&mut self) {
        for id in 0..self.peers.len() {
            loop {
                let mut progressed = false;
                while let Some(msg) = self.peers[id].pc.poll_write() {
                    progressed = true;
                    let _ = self.socket.send_to(&msg.message, msg.transport.peer_addr).await;
                }
                while let Some(event) = self.peers[id].pc.poll_event() {
                    progressed = true;
                    self.on_event(id, event);
                }
                let mut inbox = Vec::new();
                while let Some(TaggedRTCMessage { message, .. }) = self.peers[id].pc.poll_read() {
                    inbox.push(message);
                }
                for message in inbox {
                    progressed = true;
                    self.on_message(id, message);
                }
                if !progressed {
                    break;
                }
            }
        }
        // Whatever forwarding queued on other peers.
        for id in 0..self.peers.len() {
            while let Some(msg) = self.peers[id].pc.poll_write() {
                let _ = self.socket.send_to(&msg.message, msg.transport.peer_addr).await;
            }
        }
    }

    fn on_event(&mut self, id: usize, event: RTCPeerConnectionEvent) {
        let role = self.peers[id].role;
        match event {
            RTCPeerConnectionEvent::OnConnectionStateChangeEvent(state) => {
                self.counters.events.push(format!("peer {id} ({role:?}) connection: {state}"));
            }
            RTCPeerConnectionEvent::OnIceConnectionStateChangeEvent(state) => {
                self.counters.events.push(format!("peer {id} ({role:?}) ice: {state}"));
            }
            RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnOpen(init)) => {
                self.counters.events.push(format!(
                    "peer {id} track open: track {} ssrc {} rid {:?}",
                    init.track_id, init.ssrc, init.rid
                ));
                self.peers[id].receivers.insert(init.track_id.clone(), init.receiver_id);
                if let Some(rid) = init.rid.as_ref() {
                    self.publisher_layer_ssrcs.insert(rid.to_string(), init.ssrc);
                }
            }
            RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnOpen(dc)) => {
                self.counters.events.push(format!("peer {id} data channel open: {dc:?}"));
            }
            other => {
                let text = format!("{other:?}");
                let short: String = text.chars().take(90).collect();
                self.counters.events.push(format!("peer {id} event: {short}"));
            }
        }
    }

    fn on_message(&mut self, id: usize, message: RTCMessage) {
        let role = self.peers[id].role;
        match message {
            RTCMessage::RtpPacket(track_id, packet) => {
                if role == Role::Publisher {
                    self.on_publisher_rtp(id, track_id, packet);
                }
            }
            RTCMessage::RtcpPacket(_track_id, packets) => {
                for p in &packets {
                    self.on_rtcp(id, role, p.as_ref());
                }
            }
            RTCMessage::DataChannelMessage(channel, msg) => {
                self.counters.datachannel_messages += 1;
                // Echo it.
                let now = Instant::now();
                if let Some(mut dc) = self.peers[id].pc.data_channel(channel) {
                    let res = if msg.is_string {
                        dc.send_text(now, String::from_utf8_lossy(&msg.data).to_string())
                    } else {
                        dc.send(now, msg.data.clone())
                    };
                    if let Err(e) = res {
                        self.count_error("datachannel send", e);
                    }
                }
            }
            _ => {}
        }
    }

    fn on_publisher_rtp(&mut self, id: usize, track_id: String, packet: rtp::Packet) {
        let h = &packet.header;
        let rid = {
            let peer = &mut self.peers[id];
            peer.receivers.get(&track_id).copied().and_then(|rid| {
                peer.pc.rtp_receiver(rid).and_then(|r| r.track().rid(h.ssrc).map(|r| r.to_string()))
            })
        };
        let key = format!(
            "ssrc {} pt {} rid {}{}",
            h.ssrc,
            h.payload_type,
            rid.as_deref().unwrap_or("-"),
            if packet.payload.is_empty() { " (empty)" } else { "" }
        );
        *self.counters.rtp_in.entry(key).or_default() += 1;
        for e in &h.extensions {
            *self.counters.rtp_in_extensions.entry(format!("id {}", e.id)).or_default() += 1;
        }

        // By the track, not by the payload type: browsers number their codecs differently
        // (Chrome offers VP8 as 96 and Opus as 111, Firefox as 120 and 109). Only the video
        // is published with rids.
        let is_video = rid.is_some();
        let is_audio = !is_video;
        if is_video {
            if rid.as_deref() != Some(self.rid.as_str()) {
                return;
            }
            self.last_publisher_seq = Some(h.sequence_number);
            if self.forwarded_ssrc != Some(h.ssrc) {
                self.forwarded_ssrc = Some(h.ssrc);
                // A layer switch needs a key frame.
                self.send_pli_to_publisher();
            }
        } else if !is_audio {
            return;
        }

        if is_video && self.drop > 0.0 && rand::random::<f64>() < self.drop {
            // The sequence number is used up, so that the receiver sees a gap and NACKs it,
            // and the packet goes into the cache, so that the NACK can be answered.
            self.counters.rtp_dropped_on_purpose += 1;
            self.forward(packet, is_video, false);
            return;
        }
        self.forward(packet, is_video, true);
    }

    /// Rewrites a publisher's packet for each subscriber and writes it (or, with
    /// `really_send` off, only remembers it as sent).
    fn forward(&mut self, packet: rtp::Packet, is_video: bool, really_send: bool) {
        let now = Instant::now();
        let mut errors = Vec::new();
        for peer in self.peers.iter_mut().filter(|p| p.role == Role::Subscriber) {
            let Some(out) = peer.out.as_mut() else { continue };
            let Some(sender_id) = (if is_video { peer.video_sender } else { peer.audio_sender })
            else {
                continue;
            };
            let mut header = rtp::header::Header {
                version: 2,
                marker: packet.header.marker,
                timestamp: packet.header.timestamp,
                ..Default::default()
            };
            if is_video {
                if out.source_ssrc != Some(packet.header.ssrc) {
                    if out.source_ssrc.is_some() {
                        // One frame later than the last one sent, whatever the new layer says.
                        out.timestamp_offset = out
                            .last_timestamp
                            .wrapping_add(3000)
                            .wrapping_sub(packet.header.timestamp);
                    }
                    out.source_ssrc = Some(packet.header.ssrc);
                }
                header.timestamp = packet.header.timestamp.wrapping_add(out.timestamp_offset);
                header.payload_type = out.video_pt;
                header.ssrc = peer.video_ssrc;
                header.sequence_number = out.next_seq;
                out.next_seq = out.next_seq.wrapping_add(1);
                out.last_timestamp = header.timestamp;
            } else {
                header.payload_type = out.audio_pt;
                header.ssrc = peer.audio_ssrc;
                header.sequence_number = out.next_audio_seq;
                out.next_audio_seq = out.next_audio_seq.wrapping_add(1);
            }
            let mut pkt = rtp::Packet { header, payload: packet.payload.clone() };
            if is_video {
                out.cache.push_back((pkt.header.sequence_number, pkt.clone()));
                if out.cache.len() > 2000 {
                    out.cache.pop_front();
                }
            }
            if !really_send {
                continue;
            }
            if is_video {
                set_twcc(out, &mut pkt);
            }
            let Some(mut sender) = peer.pc.rtp_sender(sender_id) else { continue };
            match sender.write_rtp(now, pkt) {
                Ok(()) => self.counters.rtp_forwarded += 1,
                Err(e) => errors.push(e.to_string()),
            }
        }
        for e in errors {
            self.count_error("write_rtp", e);
        }
    }

    fn on_rtcp(&mut self, id: usize, role: Role, p: &dyn rtcp::Packet) {
        let any = p.as_any();
        let name = if any.is::<SenderReport>() {
            "SR"
        } else if any.is::<ReceiverReport>() {
            "RR"
        } else if any.is::<TransportLayerNack>() {
            "NACK"
        } else if any.is::<PictureLossIndication>() {
            "PLI"
        } else if any.is::<FullIntraRequest>() {
            "FIR"
        } else if any.is::<TransportLayerCc>() {
            "TWCC"
        } else if any.is::<ReceiverEstimatedMaximumBitrate>() {
            "REMB"
        } else if any.is::<rtcp::source_description::SourceDescription>() {
            "SDES"
        } else if any.is::<rtcp::extended_report::ExtendedReport>() {
            "XR"
        } else if any.is::<rtcp::goodbye::Goodbye>() {
            "BYE"
        } else {
            "other"
        };
        *self.counters.rtcp_in.entry(format!("{role:?} {name}")).or_default() += 1;

        if role != Role::Subscriber {
            return;
        }
        if any.is::<PictureLossIndication>() || any.is::<FullIntraRequest>() {
            self.send_pli_to_publisher();
        }
        if let Some(cc) = any.downcast_ref::<TransportLayerCc>() {
            self.counters.twcc_feedback_packets += 1;
            self.counters.twcc_feedback_statuses += cc.packet_status_count as u64;
        }
        if let Some(nack) = any.downcast_ref::<TransportLayerNack>() {
            let mut seqs = Vec::new();
            for pair in &nack.nacks {
                seqs.extend(pair.packet_list());
            }
            self.counters.nack_sequence_numbers_requested += seqs.len() as u64;
            self.retransmit(id, &seqs);
        }
    }

    /// Answers a NACK the way LiveKit's down track does: the packet again, wrapped as RTX
    /// (RFC 4588) on the repair stream, with a sequence number of that stream's own.
    fn retransmit(&mut self, id: usize, seqs: &[u16]) {
        let now = Instant::now();
        let mut errors = Vec::new();
        let peer = &mut self.peers[id];
        let (Some(out), Some(sender_id)) = (peer.out.as_mut(), peer.video_sender) else { return };
        for &seq in seqs {
            let Some((_, original)) = out.cache.iter().find(|(s, _)| *s == seq) else {
                self.counters.rtx_not_in_cache += 1;
                continue;
            };
            let mut payload = BytesMut::with_capacity(original.payload.len() + 2);
            payload.extend_from_slice(&seq.to_be_bytes());
            payload.extend_from_slice(&original.payload);
            let mut pkt = rtp::Packet {
                header: rtp::header::Header {
                    version: 2,
                    marker: original.header.marker,
                    payload_type: out.rtx_pt,
                    sequence_number: out.next_rtx_seq,
                    timestamp: original.header.timestamp,
                    ssrc: peer.video_rtx_ssrc,
                    ..Default::default()
                },
                payload: payload.freeze(),
            };
            out.next_rtx_seq = out.next_rtx_seq.wrapping_add(1);
            set_twcc(out, &mut pkt);
            // Through the connection: the sender object only takes its track's own SSRC, not
            // the repair stream's (`ErrSenderWithNoSSRCs`).
            let track_id = peer
                .pc
                .rtp_sender(sender_id)
                .map(|s| s.track().track_id().to_string())
                .unwrap_or_default();
            match peer
                .pc
                .handle_write(TaggedRTCMessage { now, message: RTCMessage::RtpPacket(track_id, pkt) })
            {
                Ok(()) => self.counters.rtx_sent += 1,
                Err(e) => errors.push(e.to_string()),
            }
        }
        for e in errors {
            self.count_error("retransmit", e);
        }
    }

    /// Padding-only packets on the RTX stream, which is how LiveKit probes for bandwidth.
    fn send_padding(&mut self, count: u32) {
        let now = Instant::now();
        let mut errors = Vec::new();
        for peer in self.peers.iter_mut().filter(|p| p.role == Role::Subscriber) {
            let (Some(out), Some(sender_id)) = (peer.out.as_mut(), peer.video_sender) else {
                continue;
            };
            for _ in 0..count {
                let mut pkt = rtp::Packet {
                    header: rtp::header::Header {
                        version: 2,
                        padding: true,
                        padding_size: 255,
                        payload_type: out.rtx_pt,
                        sequence_number: out.next_rtx_seq,
                        timestamp: out.last_timestamp,
                        ssrc: peer.video_rtx_ssrc,
                        ..Default::default()
                    },
                    payload: Bytes::new(),
                };
                out.next_rtx_seq = out.next_rtx_seq.wrapping_add(1);
                set_twcc(out, &mut pkt);
                let track_id = peer
                    .pc
                    .rtp_sender(sender_id)
                    .map(|s| s.track().track_id().to_string())
                    .unwrap_or_default();
                let res = peer
                    .pc
                    .handle_write(TaggedRTCMessage { now, message: RTCMessage::RtpPacket(track_id, pkt) });
                match res {
                    Ok(()) => self.counters.padding_sent += 1,
                    Err(e) => errors.push(e.to_string()),
                }
            }
            // How large they are on the wire says how much padding the library let through.
            let before = Instant::now();
            let mut sizes = Vec::new();
            while let Some(msg) = peer.pc.poll_write() {
                sizes.push((msg.message.len(), msg));
                if before.elapsed() > Duration::from_millis(50) {
                    break;
                }
            }
            for (len, msg) in sizes {
                *self.counters.padding_wire_sizes.entry(format!("{len} bytes")).or_default() += 1;
                let socket = self.socket.clone();
                tokio::spawn(async move {
                    let _ = socket.send_to(&msg.message, msg.transport.peer_addr).await;
                });
            }
        }
        for e in errors {
            self.count_error("padding", e);
        }
    }

    fn send_pli_to_publisher(&mut self) {
        let Some(media_ssrc) = self.forwarded_ssrc else { return };
        let now = Instant::now();
        let mut errors = Vec::new();
        for peer in self.peers.iter_mut().filter(|p| p.role == Role::Publisher) {
            let ids: Vec<RTCRtpReceiverId> = peer.receivers.values().copied().collect();
            for receiver_id in ids {
                let Some(mut receiver) = peer.pc.rtp_receiver(receiver_id) else { continue };
                if !receiver.track().ssrcs().any(|s| s == media_ssrc) {
                    continue;
                }
                match receiver.write_rtcp(
                    now,
                    vec![Box::new(PictureLossIndication { sender_ssrc: 1, media_ssrc })],
                ) {
                    Ok(()) => self.counters.pli_passed_on += 1,
                    Err(e) => errors.push(e.to_string()),
                }
                break;
            }
        }
        for e in errors {
            self.count_error("pli", e);
        }
    }

    fn nack_publisher(&mut self, count: u16) {
        let (Some(media_ssrc), Some(last)) = (self.forwarded_ssrc, self.last_publisher_seq) else {
            return;
        };
        let now = Instant::now();
        let mut errors = Vec::new();
        for peer in self.peers.iter_mut().filter(|p| p.role == Role::Publisher) {
            let ids: Vec<RTCRtpReceiverId> = peer.receivers.values().copied().collect();
            for receiver_id in ids {
                let Some(mut receiver) = peer.pc.rtp_receiver(receiver_id) else { continue };
                if !receiver.track().ssrcs().any(|s| s == media_ssrc) {
                    continue;
                }
                let nacks = (0..count)
                    .map(|i| NackPair { packet_id: last.wrapping_sub(5 + i * 2), lost_packets: 0 })
                    .collect();
                match receiver.write_rtcp(
                    now,
                    vec![Box::new(TransportLayerNack { sender_ssrc: 1, media_ssrc, nacks })],
                ) {
                    Ok(()) => self.counters.nacks_sent_to_publisher += count as u64,
                    Err(e) => errors.push(e.to_string()),
                }
                break;
            }
        }
        for e in errors {
            self.count_error("nack to publisher", e);
        }
    }

    /// Tells the publisher it may send a lot, as a receiver that estimates bandwidth itself
    /// does: without it a browser keeps to its starting bitrate and one simulcast layer.
    fn send_remb(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_remb) < Duration::from_millis(500) {
            return;
        }
        self.last_remb = now;
        let ssrcs: Vec<u32> = self.publisher_layer_ssrcs.values().copied().collect();
        if ssrcs.is_empty() {
            return;
        }
        let mut errors = Vec::new();
        for peer in self.peers.iter_mut().filter(|p| p.role == Role::Publisher) {
            let Some(&receiver_id) = peer.receivers.values().next() else { continue };
            let Some(mut receiver) = peer.pc.rtp_receiver(receiver_id) else { continue };
            match receiver.write_rtcp(
                now,
                vec![Box::new(ReceiverEstimatedMaximumBitrate {
                    sender_ssrc: 1,
                    bitrate: 5_000_000.0,
                    ssrcs: ssrcs.clone(),
                })],
            ) {
                Ok(()) => self.counters.remb_sent += 1,
                Err(e) => errors.push(e.to_string()),
            }
        }
        for e in errors {
            self.count_error("remb", e);
        }
    }

    async fn run(mut self, mut cmds: mpsc::Receiver<Cmd>) {
        let mut buf = vec![0u8; 2000];
        loop {
            self.drain().await;
            self.send_remb();

            let now = Instant::now();
            let mut next = now + Duration::from_millis(100);
            let mut due = false;
            for peer in self.peers.iter_mut() {
                if let Some(t) = peer.pc.poll_timeout() {
                    if t <= now {
                        let _ = peer.pc.handle_timeout(now);
                        due = true;
                    } else if t < next {
                        next = t;
                    }
                }
            }
            if due {
                continue;
            }

            tokio::select! {
                _ = tokio::time::sleep_until(next.into()) => {
                    let now = Instant::now();
                    for peer in self.peers.iter_mut() {
                        if peer.pc.poll_timeout().is_some_and(|t| t <= now) {
                            let _ = peer.pc.handle_timeout(now);
                        }
                    }
                }
                res = self.socket.recv_from(&mut buf) => {
                    let Ok((n, from)) = res else { continue };
                    let Some(id) = self.route(from, &buf[..n]) else { continue };
                    let local_addr = self.local_addr;
                    let res = self.peers[id].pc.handle_read(TaggedBytesMut {
                        now: Instant::now(),
                        transport: TransportContext {
                            local_addr,
                            peer_addr: from,
                            ecn: None,
                            transport_protocol: TransportProtocol::UDP,
                        },
                        message: BytesMut::from(&buf[..n]),
                    });
                    if let Err(e) = res {
                        self.count_error("handle_read", e);
                    }
                }
                cmd = cmds.recv() => {
                    let Some(cmd) = cmd else { return };
                    match cmd {
                        Cmd::Publish(offer, reply) => { let _ = reply.send(self.publish(offer)); }
                        Cmd::SubscribeOffer(reply) => { let _ = reply.send(self.subscribe_offer()); }
                        Cmd::SubscribeAnswer(id, answer, reply) => {
                            let _ = reply.send(self.subscribe_answer(id, answer));
                        }
                        Cmd::Stats(reply) => {
                            let mut counters = self.counters.clone();
                            for peer in &self.peers {
                                let w = peer.wire.lock().unwrap();
                                counters.wire.insert(format!("{:?} {}", peer.role, peer.id), WireView {
                                    rtp_in: w.rtp_in.clone(),
                                    rtcp_in: w.rtcp_in,
                                    rtp_out: w.rtp_out.clone(),
                                    rtcp_out: w.rtcp_out,
                                });
                            }
                            let _ = reply.send(counters);
                        }
                        Cmd::Control(c, reply) => {
                            if let Some(d) = c.drop { self.drop = d; }
                            if let Some(rid) = c.rid { self.rid = rid; }
                            if let Some(n) = c.padding_packets { self.send_padding(n); }
                            if let Some(n) = c.nack_publisher { self.nack_publisher(n); }
                            if c.reset_counters == Some(true) {
                                let events = std::mem::take(&mut self.counters.events);
                                self.counters = Counters { events, ..Default::default() };
                            }
                            let _ = reply.send(());
                        }
                    }
                }
            }
        }
    }
}

/// What the wire-most interceptor saw, shared with the program: every packet in both
/// directions as the library has it right above SRTP, before it does anything with it.
#[derive(Default)]
struct WireCounts {
    rtp_in: BTreeMap<String, u64>,
    rtcp_in: u64,
    rtp_out: BTreeMap<String, u64>,
    rtcp_out: u64,
}

/// An interceptor of the program's own, which is how `rtc` (like pion) lets an application
/// into the path of RTP and RTCP. This one counts what passes and marks all incoming RTCP
/// for delivery to the application: without that mark the library keeps RTCP to its own
/// interceptors.
struct Tap {
    counts: Arc<Mutex<WireCounts>>,
    read_queue: VecDeque<TaggedPacket>,
    write_queue: VecDeque<TaggedPacket>,
}

impl Protocol<TaggedPacket, TaggedPacket, ()> for Tap {
    type Rout = TaggedPacket;
    type Wout = TaggedPacket;
    type Eout = ();
    type Error = rtc::shared::error::Error;
    type Time = Instant;

    fn handle_read(&mut self, mut msg: TaggedPacket) -> std::result::Result<(), Self::Error> {
        {
            let mut counts = self.counts.lock().unwrap();
            match &msg.message.packet {
                Packet::Rtp(p) => {
                    let key = format!(
                        "ssrc {} pt {}{}{}",
                        p.header.ssrc,
                        p.header.payload_type,
                        if p.header.padding { " padding-flag" } else { "" },
                        if p.payload.is_empty() { " empty" } else { "" }
                    );
                    *counts.rtp_in.entry(key).or_default() += 1;
                }
                Packet::Rtcp(_) => counts.rtcp_in += 1,
                _ => {}
            }
        }
        if matches!(msg.message.packet, Packet::Rtcp(_)) {
            msg.message.add(Attribute::DeliverToApplication);
        }
        self.read_queue.push_back(msg);
        Ok(())
    }

    fn poll_read(&mut self) -> Option<Self::Rout> {
        self.read_queue.pop_front()
    }

    fn handle_write(&mut self, msg: TaggedPacket) -> std::result::Result<(), Self::Error> {
        {
            let mut counts = self.counts.lock().unwrap();
            match &msg.message.packet {
                Packet::Rtp(p) => {
                    let key = format!("ssrc {} pt {}", p.header.ssrc, p.header.payload_type);
                    *counts.rtp_out.entry(key).or_default() += 1;
                }
                Packet::Rtcp(_) => counts.rtcp_out += 1,
                _ => {}
            }
        }
        self.write_queue.push_back(msg);
        Ok(())
    }

    fn poll_write(&mut self) -> Option<Self::Wout> {
        self.write_queue.pop_front()
    }
}

impl Interceptor for Tap {
    fn bind_local_stream(&mut self, _info: &StreamInfo) {}
    fn unbind_local_stream(&mut self, _info: &StreamInfo) {}
    fn bind_remote_stream(&mut self, _info: &StreamInfo) {}
    fn unbind_remote_stream(&mut self, _info: &StreamInfo) {}
}

fn set_twcc(out: &mut Outgoing, pkt: &mut rtp::Packet) {
    if let Some(id) = out.twcc_id {
        let seq = out.twcc_seq;
        out.twcc_seq = out.twcc_seq.wrapping_add(1);
        let _ = pkt.header.set_extension(id, Bytes::copy_from_slice(&seq.to_be_bytes()));
    }
}

/// The USERNAME attribute of a STUN message, if this is one.
fn stun_username(data: &[u8]) -> Option<String> {
    if data.len() < 20 || data[0] & 0xC0 != 0 || data[4..8] != [0x21, 0x12, 0xA4, 0x42] {
        return None;
    }
    let len = u16::from_be_bytes([data[2], data[3]]) as usize;
    let mut rest = data.get(20..20 + len)?;
    while rest.len() >= 4 {
        let typ = u16::from_be_bytes([rest[0], rest[1]]);
        let alen = u16::from_be_bytes([rest[2], rest[3]]) as usize;
        let value = rest.get(4..4 + alen)?;
        if typ == 0x0006 {
            return String::from_utf8(value.to_vec()).ok();
        }
        let padded = (alen + 3) & !3;
        rest = rest.get(4 + padded..)?;
    }
    None
}

#[derive(Clone)]
struct App {
    cmds: mpsc::Sender<Cmd>,
}

#[derive(Deserialize)]
struct Sdp {
    sdp: String,
    id: Option<usize>,
}

#[derive(Serialize)]
struct SdpReply {
    sdp: String,
    id: usize,
}

type HttpResult<T> = std::result::Result<T, (axum::http::StatusCode, String)>;

fn internal(e: impl std::fmt::Display) -> (axum::http::StatusCode, String) {
    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

async fn publish(State(app): State<App>, Json(body): Json<Sdp>) -> HttpResult<Json<SdpReply>> {
    let (tx, rx) = oneshot::channel();
    app.cmds.send(Cmd::Publish(body.sdp, tx)).await.map_err(internal)?;
    let sdp = rx.await.map_err(internal)?.map_err(internal)?;
    Ok(Json(SdpReply { sdp, id: 0 }))
}

async fn subscribe_offer(State(app): State<App>) -> HttpResult<Json<SdpReply>> {
    let (tx, rx) = oneshot::channel();
    app.cmds.send(Cmd::SubscribeOffer(tx)).await.map_err(internal)?;
    let (id, sdp) = rx.await.map_err(internal)?.map_err(internal)?;
    Ok(Json(SdpReply { sdp, id }))
}

async fn subscribe_answer(State(app): State<App>, Json(body): Json<Sdp>) -> HttpResult<()> {
    let (tx, rx) = oneshot::channel();
    let id = body.id.ok_or_else(|| internal("no id"))?;
    app.cmds.send(Cmd::SubscribeAnswer(id, body.sdp, tx)).await.map_err(internal)?;
    rx.await.map_err(internal)?.map_err(internal)
}

async fn stats(State(app): State<App>) -> HttpResult<Json<Counters>> {
    let (tx, rx) = oneshot::channel();
    app.cmds.send(Cmd::Stats(tx)).await.map_err(internal)?;
    Ok(Json(rx.await.map_err(internal)?))
}

async fn control(State(app): State<App>, Json(body): Json<Control>) -> HttpResult<()> {
    let (tx, rx) = oneshot::channel();
    app.cmds.send(Cmd::Control(body, tx)).await.map_err(internal)?;
    rx.await.map_err(internal)
}

#[tokio::main]
async fn main() -> Result<()> {
    let http_port: u16 = std::env::var("HTTP_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(39400);
    let udp_port: u16 = std::env::var("UDP_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(39401);
    // Media on the loopback interface unless told otherwise (Firefox does not pair its own
    // addresses with a loopback one).
    let bind_ip = std::env::var("BIND_IP").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let socket = Arc::new(UdpSocket::bind((bind_ip.as_str(), udp_port)).await?);
    let local_addr = socket.local_addr()?;
    let (tx, rx) = mpsc::channel(64);
    let server = Server {
        socket,
        local_addr,
        peers: Vec::new(),
        by_addr: HashMap::new(),
        counters: Counters::default(),
        drop: 0.0,
        rid: "q".to_owned(),
        forwarded_ssrc: None,
        last_publisher_seq: None,
        publisher_layer_ssrcs: HashMap::new(),
        last_remb: Instant::now(),
    };
    tokio::spawn(server.run(rx));

    let app = Router::new()
        .route("/", get(|| async { Html(include_str!("../../driver/page.html")) }))
        .route("/publish", post(publish))
        .route("/subscribe-offer", post(subscribe_offer))
        .route("/subscribe-answer", post(subscribe_answer))
        .route("/stats", get(stats))
        .route("/control", post(control))
        .with_state(App { cmds: tx });
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", http_port)).await?;
    println!("http on 127.0.0.1:{http_port}, media on {local_addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
