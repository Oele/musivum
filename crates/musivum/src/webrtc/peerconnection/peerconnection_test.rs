//! Two of musivum's peer connections talking to each other in one process, over the loopback
//! interface on ports the system chooses: an ICE-lite "server" on a node socket, as musivum
//! is run, and a full-ICE "client" on a node of its own, standing in for a browser.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;

use super::*;
use crate::testutils::eventually_async;
use crate::webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use crate::webrtc::rtcp::receiver_report::ReceiverReport;
use crate::webrtc::rtcp::sender_report::SenderReport;
use crate::webrtc::sdp;
use crate::webrtc::{
    MIME_TYPE_H264, MIME_TYPE_OPUS, MIME_TYPE_RTX, MIME_TYPE_VP8, MIME_TYPE_VP9, RtcpFeedback,
    RtpCodecCapability, TrackLocalWriter,
};

const WAIT: Duration = Duration::from_secs(10);
const TICK: Duration = Duration::from_millis(5);

const VP8: u8 = 96;
const VP8_RTX: u8 = 97;
const VP9: u8 = 98;
const VP9_RTX: u8 = 99;
const OPUS: u8 = 111;

fn media_engine() -> MediaEngine {
    let mut m = MediaEngine::default();
    let video_feedback = vec![
        RtcpFeedback {
            r#type: "nack".to_string(),
            parameter: String::new(),
        },
        RtcpFeedback {
            r#type: "nack".to_string(),
            parameter: "pli".to_string(),
        },
        RtcpFeedback {
            r#type: "transport-cc".to_string(),
            parameter: String::new(),
        },
    ];
    let codecs = [
        (
            MIME_TYPE_VP8,
            90000,
            0,
            "",
            video_feedback,
            VP8,
            RtpCodecType::Video,
        ),
        (
            MIME_TYPE_RTX,
            90000,
            0,
            "apt=96",
            vec![],
            VP8_RTX,
            RtpCodecType::Video,
        ),
        (
            MIME_TYPE_VP9,
            90000,
            0,
            "profile-id=0",
            vec![],
            VP9,
            RtpCodecType::Video,
        ),
        (
            MIME_TYPE_RTX,
            90000,
            0,
            "apt=98",
            vec![],
            VP9_RTX,
            RtpCodecType::Video,
        ),
        (
            MIME_TYPE_OPUS,
            48000,
            2,
            "minptime=10;useinbandfec=1",
            vec![],
            OPUS,
            RtpCodecType::Audio,
        ),
    ];
    for (mime_type, clock_rate, channels, fmtp, rtcp_feedback, payload_type, kind) in codecs {
        register_codec(
            &mut m,
            &RtpCodecParameters {
                rtp_codec_capability: RtpCodecCapability {
                    mime_type: mime_type.to_string(),
                    clock_rate,
                    channels,
                    sdp_fmtp_line: fmtp.to_string(),
                    rtcp_feedback,
                },
                payload_type,
            },
            kind,
        )
        .unwrap();
    }
    for uri in [
        sdp::SDES_MID_URI,
        sdp::SDES_RTP_STREAM_ID_URI,
        sdp::SDES_REPAIR_RTP_STREAM_ID_URI,
        sdp::ABS_SEND_TIME_URI,
        sdp::TRANSPORT_CC_URI,
    ] {
        register_header_extension(&mut m, uri, RtpCodecType::Video, None).unwrap();
    }
    for uri in [sdp::SDES_MID_URI, sdp::AUDIO_LEVEL_URI] {
        register_header_extension(&mut m, uri, RtpCodecType::Audio, None).unwrap();
    }
    m
}

type Hook<T> = Mutex<Option<Box<dyn Fn(T) + Send + Sync>>>;
type Tracks = Arc<Mutex<Vec<(Arc<TrackRemote>, Arc<RtpReceiver>)>>>;

/// A call a connection made to its sink: packets by their SSRC, remote streams by theirs.
#[derive(Clone, Debug, PartialEq)]
enum SinkCall {
    Rtp(u32),
    Rtcp,
    BindRemote(u32),
    UnbindRemote(u32),
    BindLocal(StreamInfo),
    UnbindLocal(StreamInfo),
    Closed,
}

/// What a connection handed to its sink.
#[derive(Default)]
struct Recorder {
    rtp: Mutex<Vec<(u32, Bytes)>>,
    rtcp: Mutex<Vec<Vec<Box<dyn rtcp::Packet>>>>,
    bound: Mutex<Vec<StreamInfo>>,
    // Every call, in the order they were made.
    calls: Mutex<Vec<SinkCall>>,
    // Called for every RTP packet (with its SSRC) and for every RTCP packet, after it was
    // recorded.
    on_rtp: Hook<u32>,
    on_rtcp: Hook<()>,
    // Called when the sink is told of a local stream, bound (true) or unbound (false), after
    // that was recorded.
    on_local: Hook<bool>,
}

impl PacketSink for Recorder {
    fn on_rtp(&self, ssrc: u32, packet: Bytes, arrival: Time) {
        assert!(!arrival.is_zero());
        self.rtp.lock().push((ssrc, packet));
        self.calls.lock().push(SinkCall::Rtp(ssrc));
        if let Some(f) = self.on_rtp.lock().as_ref() {
            f(ssrc);
        }
    }

    fn on_rtcp(&self, packets: &[Box<dyn rtcp::Packet>], _arrival: Time) {
        self.rtcp.lock().push(packets.to_vec());
        self.calls.lock().push(SinkCall::Rtcp);
        if let Some(f) = self.on_rtcp.lock().as_ref() {
            f(());
        }
    }

    fn bind_remote_stream(&self, info: &StreamInfo) {
        self.bound.lock().push(info.clone());
        self.calls.lock().push(SinkCall::BindRemote(info.ssrc));
    }

    fn unbind_remote_stream(&self, info: &StreamInfo) {
        self.calls.lock().push(SinkCall::UnbindRemote(info.ssrc));
    }

    fn bind_local_stream(&self, info: &StreamInfo) {
        self.calls.lock().push(SinkCall::BindLocal(info.clone()));
        if let Some(f) = self.on_local.lock().as_ref() {
            f(true);
        }
    }

    fn unbind_local_stream(&self, info: &StreamInfo) {
        self.calls.lock().push(SinkCall::UnbindLocal(info.clone()));
        if let Some(f) = self.on_local.lock().as_ref() {
            f(false);
        }
    }

    fn closed(&self) {
        self.calls.lock().push(SinkCall::Closed);
    }
}

impl Recorder {
    /// The calls about local streams, in order.
    fn local_calls(&self) -> Vec<SinkCall> {
        self.calls
            .lock()
            .iter()
            .filter(|call| matches!(call, SinkCall::BindLocal(_) | SinkCall::UnbindLocal(_)))
            .cloned()
            .collect()
    }

    /// Where in the order of calls the first such call is.
    fn position(&self, call: &SinkCall) -> Option<usize> {
        self.calls.lock().iter().position(|c| c == call)
    }

    fn rtp_of(&self, ssrc: u32) -> Vec<Bytes> {
        self.rtp
            .lock()
            .iter()
            .filter(|(s, _)| *s == ssrc)
            .map(|(_, p)| p.clone())
            .collect()
    }

    /// The RTCP packets of one type that were handed over.
    fn rtcp_of<T: Clone + 'static>(&self) -> Vec<T> {
        self.rtcp
            .lock()
            .iter()
            .flatten()
            .filter_map(|p| p.as_any().downcast_ref::<T>().cloned())
            .collect()
    }
}

/// A local track that takes the first media codec it is offered and keeps what it was bound
/// with.
struct TestTrack {
    id: String,
    kind: RtpCodecType,
    context: Mutex<Option<Arc<dyn TrackLocalContext>>>,
    codec: Mutex<Option<RtpCodecParameters>>,
    // The payload types of the codecs the context had when the track was bound.
    offered: Mutex<Vec<u8>>,
    binds: AtomicUsize,
    unbinds: AtomicUsize,
}

impl TestTrack {
    fn new(id: &str, kind: RtpCodecType) -> Arc<TestTrack> {
        Arc::new(TestTrack {
            id: id.to_string(),
            kind,
            context: Mutex::new(None),
            codec: Mutex::new(None),
            offered: Mutex::new(Vec::new()),
            binds: AtomicUsize::new(0),
            unbinds: AtomicUsize::new(0),
        })
    }

    /// How often the track was bound and how often unbound.
    fn bound_and_unbound(&self) -> (usize, usize) {
        (
            self.binds.load(Ordering::SeqCst),
            self.unbinds.load(Ordering::SeqCst),
        )
    }

    fn is_bound(&self) -> bool {
        self.context.lock().is_some()
    }

    fn context(&self) -> Arc<dyn TrackLocalContext> {
        self.context.lock().clone().expect("the track is bound")
    }

    fn writer(&self) -> Arc<dyn TrackLocalWriter> {
        self.context().write_stream()
    }

    fn payload_type(&self) -> u8 {
        self.codec.lock().as_ref().expect("bound").payload_type
    }
}

impl TrackLocal for TestTrack {
    fn bind(&self, t: Arc<dyn TrackLocalContext>) -> Result<RtpCodecParameters, Error> {
        let offered = t.codec_parameters();
        *self.offered.lock() = offered.iter().map(|c| c.payload_type).collect();
        let codec = offered
            .into_iter()
            .find(|c| !c.mime_type.eq_ignore_ascii_case(MIME_TYPE_RTX))
            .ok_or(Error::UnsupportedCodec)?;
        *self.codec.lock() = Some(codec.clone());
        *self.context.lock() = Some(t);
        self.binds.fetch_add(1, Ordering::SeqCst);
        Ok(codec)
    }

    fn unbind(&self, _t: Arc<dyn TrackLocalContext>) -> Result<(), Error> {
        self.unbinds.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn id(&self) -> String {
        self.id.clone()
    }

    fn rid(&self) -> String {
        String::new()
    }

    fn stream_id(&self) -> String {
        "stream".to_string()
    }

    fn kind(&self) -> RtpCodecType {
        self.kind
    }
}

/// One end of a test: a node of its own, a connection on it, and what the connection
/// reported.
struct Peer {
    node: Node,
    pc: PeerConnection,
    sink: Arc<Recorder>,
    tracks: Tracks,
    states: Arc<Mutex<Vec<PeerConnectionState>>>,
}

impl Peer {
    /// A server's connection: ICE lite, and the DTLS server when it answers.
    fn server_on(node: &Node) -> Peer {
        let setting_engine = SettingEngine {
            lite: true,
            answering_dtls_role: DtlsRole::Server,
            ..Default::default()
        };
        Peer::new(node, setting_engine)
    }

    fn server() -> Peer {
        Peer::server_on(&Node::new(NodeConfig::default()).unwrap())
    }

    /// A client's connection, on a node of its own as a browser has a socket of its own.
    fn client() -> Peer {
        Peer::new(
            &Node::new(NodeConfig::default()).unwrap(),
            SettingEngine::default(),
        )
    }

    fn new(node: &Node, setting_engine: SettingEngine) -> Peer {
        Peer::with_servers(node, setting_engine, Vec::new())
    }

    fn with_servers(node: &Node, setting_engine: SettingEngine, servers: Vec<IceServer>) -> Peer {
        let sink = Arc::new(Recorder::default());
        let pc = PeerConnection::new(
            node,
            Configuration {
                media_engine: media_engine(),
                setting_engine,
                ice_servers: servers,
                packet_sink: Some(sink.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        let tracks = Arc::new(Mutex::new(Vec::new()));
        let states = Arc::new(Mutex::new(Vec::new()));
        pc.on_track({
            let tracks = tracks.clone();
            move |track, receiver| tracks.lock().push((track, receiver))
        });
        pc.on_connection_state_change({
            let states = states.clone();
            move |state| states.lock().push(state)
        });
        Peer {
            node: node.clone(),
            pc,
            sink,
            tracks,
            states,
        }
    }

    fn is_connected(&self) -> bool {
        self.pc.connection_state() == PeerConnectionState::Connected
    }

    fn track_of(&self, ssrc: u32) -> Option<Arc<TrackRemote>> {
        self.tracks
            .lock()
            .iter()
            .find(|(t, _)| t.ssrc() == ssrc)
            .map(|(t, _)| t.clone())
    }
}

/// An offer and its answer, carried over as the signalling would. The candidates travel in
/// the descriptions.
fn negotiate_with(
    offerer: &PeerConnection,
    answerer: &PeerConnection,
    options: Option<OfferOptions>,
) {
    let offer = offerer.create_offer(options).unwrap();
    assert_eq!(offer.r#type, SdpType::Offer);
    offerer.set_local_description(offer).unwrap();
    assert_eq!(offerer.signaling_state(), SignalingState::HaveLocalOffer);
    // Nothing is waited for where LiveKit waits for pion's `GatheringCompletePromise`: when
    // the local description is set, the candidates are in it.
    assert_eq!(offerer.ice_gathering_state(), IceGatheringState::Complete);
    answerer
        .set_remote_description(offerer.local_description().unwrap())
        .unwrap();
    assert_eq!(answerer.signaling_state(), SignalingState::HaveRemoteOffer);
    let answer = answerer.create_answer().unwrap();
    answerer.set_local_description(answer).unwrap();
    assert_eq!(answerer.signaling_state(), SignalingState::Stable);
    offerer
        .set_remote_description(answerer.local_description().unwrap())
        .unwrap();
    assert_eq!(offerer.signaling_state(), SignalingState::Stable);
}

fn negotiate(offerer: &PeerConnection, answerer: &PeerConnection) {
    negotiate_with(offerer, answerer, None);
}

async fn connected(a: &Peer, b: &Peer) {
    eventually_async(|| a.is_connected() && b.is_connected(), WAIT, TICK).await;
}

fn header(ssrc: u32, payload_type: u8, sequence_number: u16) -> rtp::Header {
    rtp::Header {
        version: 2,
        payload_type,
        sequence_number,
        timestamp: 3000 * sequence_number as u32,
        ssrc,
        ..Default::default()
    }
}

/// The bytes a packet has on the wire, before encryption.
fn marshalled(header: &rtp::Header, payload: &[u8]) -> Bytes {
    let packet = rtp::Packet {
        header: header.clone(),
        payload: Bytes::copy_from_slice(payload),
        ..Default::default()
    };
    Bytes::from(packet.marshal().unwrap())
}

fn extension_id(context: &dyn TrackLocalContext, uri: &str) -> u8 {
    context
        .header_extensions()
        .iter()
        .find(|e| e.uri == uri)
        .unwrap_or_else(|| panic!("{uri} was negotiated"))
        .id as u8
}

fn pli(media_ssrc: u32) -> Vec<Box<dyn rtcp::Packet>> {
    vec![Box::new(PictureLossIndication {
        sender_ssrc: 1,
        media_ssrc,
    })]
}

/// The client offers video (with a retransmission stream) and audio to send. What it writes
/// comes out at the server's sink as written, and RTCP goes both ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_client_offers_and_sends() {
    let server = Peer::server();
    let client = Peer::client();

    let video = TestTrack::new("video", RtpCodecType::Video);
    let audio = TestTrack::new("audio", RtpCodecType::Audio);
    let init = Some(RtpTransceiverInit {
        direction: RtpTransceiverDirection::Sendonly,
    });
    let video_transceiver = client
        .pc
        .add_transceiver_from_track(video.clone(), init)
        .unwrap();
    client
        .pc
        .add_transceiver_from_track(audio.clone(), init)
        .unwrap();
    assert_eq!(video_transceiver.kind(), RtpCodecType::Video);
    assert_eq!(
        video_transceiver.direction(),
        RtpTransceiverDirection::Sendonly
    );
    assert_eq!(video_transceiver.mid(), "");

    negotiate(&client.pc, &server.pc);
    connected(&client, &server).await;
    assert_eq!(video_transceiver.mid(), "0");

    // The tracks are bound once the connection is up, with what was negotiated.
    eventually_async(|| video.is_bound() && audio.is_bound(), WAIT, TICK).await;
    let context = video.context();
    let (ssrc, rtx_ssrc) = (context.ssrc(), context.ssrc_retransmission());
    assert_ne!(ssrc, 0);
    assert_ne!(rtx_ssrc, 0);
    assert_ne!(rtx_ssrc, ssrc);
    assert_eq!(video.payload_type(), VP8);
    assert_eq!(audio.payload_type(), OPUS);
    assert_eq!(audio.context().ssrc_retransmission(), 0);
    // After the bind the context has the codec the track chose.
    assert_eq!(context.codec_parameters().len(), 1);
    let sender = video_transceiver.sender().unwrap();
    let parameters = sender.get_parameters();
    assert_eq!(parameters.encodings[0].ssrc, ssrc);
    assert_eq!(parameters.encodings[0].rtx.ssrc, rtx_ssrc);
    assert!(parameters.codecs.iter().any(|c| c.payload_type == VP8_RTX));

    // RTP with header extensions, and a packet with a padding of its own size.
    let writer = video.writer();
    let abs_send_time = extension_id(context.as_ref(), sdp::ABS_SEND_TIME_URI);
    let twcc = extension_id(context.as_ref(), sdp::TRANSPORT_CC_URI);
    let mut expected = Vec::new();
    for seq in 0..5u16 {
        let mut h = header(ssrc, VP8, 1000 + seq);
        h.marker = seq == 4;
        h.set_extension(abs_send_time, Bytes::from_static(&[1, 2, 3]))
            .unwrap();
        h.set_extension(twcc, Bytes::copy_from_slice(&seq.to_be_bytes()))
            .unwrap();
        let mut payload = vec![seq as u8; 100 + seq as usize];
        if seq == 3 {
            // Padding after a payload: 7 octets, the last of which says so.
            h.padding = true;
            h.padding_size = 7;
            payload.truncate(50);
        }
        let n = writer.write_rtp(&h, &payload).unwrap();
        let bytes = marshalled(&h, &payload);
        assert_eq!(n, bytes.len());
        expected.push(bytes);
    }
    eventually_async(|| server.sink.rtp_of(ssrc).len() == 5, WAIT, TICK).await;
    assert_eq!(server.sink.rtp_of(ssrc), expected);

    // The server was told about the stream before its first packet, and about the track.
    eventually_async(|| server.track_of(ssrc).is_some(), WAIT, TICK).await;
    let track = server.track_of(ssrc).unwrap();
    assert_eq!(track.kind(), RtpCodecType::Video);
    assert_eq!(track.id(), "video");
    assert_eq!(track.stream_id(), "stream");
    assert_eq!(track.msid(), "stream video");
    assert_eq!(track.rid(), "");
    assert_eq!(track.rtx_ssrc(), rtx_ssrc);
    assert_eq!(track.codec().payload_type, VP8);
    assert!(track.codec().mime_type.eq_ignore_ascii_case(MIME_TYPE_VP8));
    let receiver = server.tracks.lock()[0].1.clone();
    assert_eq!(receiver.rtp_transceiver().unwrap().mid(), "0");
    assert_eq!(
        receiver.rtp_transceiver().unwrap().kind(),
        RtpCodecType::Video
    );
    assert!(
        receiver
            .get_parameters()
            .header_extensions
            .iter()
            .any(|e| e.uri == sdp::TRANSPORT_CC_URI)
    );
    {
        let bound = server.sink.bound.lock();
        let info = bound.iter().find(|i| i.ssrc == ssrc).expect("stream bound");
        assert_eq!(info.payload_type, VP8);
        assert_eq!(info.ssrc_retransmission, rtx_ssrc);
        assert_eq!(info.payload_type_retransmission, VP8_RTX);
        assert_eq!(info.clock_rate, 90000);
    }

    // Audio is a track of its own.
    let audio_ssrc = audio.context().ssrc();
    let h = header(audio_ssrc, OPUS, 7);
    audio.writer().write_rtp(&h, &[9; 40]).unwrap();
    eventually_async(|| server.track_of(audio_ssrc).is_some(), WAIT, TICK).await;
    assert_eq!(
        server.track_of(audio_ssrc).unwrap().kind(),
        RtpCodecType::Audio
    );
    assert_eq!(
        server.sink.rtp_of(audio_ssrc),
        vec![marshalled(&h, &[9; 40])]
    );

    // RTCP from the server (a key frame request) comes out at the client's sink.
    server
        .pc
        .write_rtcp(&[Box::new(PictureLossIndication {
            sender_ssrc: 1,
            media_ssrc: ssrc,
        })])
        .unwrap();
    eventually_async(
        || !client.sink.rtcp_of::<PictureLossIndication>().is_empty(),
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(
        client.sink.rtcp_of::<PictureLossIndication>(),
        vec![PictureLossIndication {
            sender_ssrc: 1,
            media_ssrc: ssrc
        }]
    );

    // And RTCP from the client (a sender report) at the server's.
    let report = SenderReport {
        ssrc,
        ntp_time: 0x0102030405060708,
        rtp_time: 90000,
        packet_count: 5,
        octet_count: 500,
        ..Default::default()
    };
    client.pc.write_rtcp(&[Box::new(report.clone())]).unwrap();
    eventually_async(
        || !server.sink.rtcp_of::<SenderReport>().is_empty(),
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(server.sink.rtcp_of::<SenderReport>(), vec![report]);

    // Nothing came twice, and nothing came to the wrong side.
    assert_eq!(server.sink.rtp.lock().len(), 6);
    assert!(client.sink.rtp.lock().is_empty());
    assert!(client.tracks.lock().is_empty());

    client.pc.close().unwrap();
    server.pc.close().unwrap();
}

/// The server offers a track it sends. Through the writer the track was bound with go media,
/// a retransmission on the repair stream and padding of 255 octets, and each arrives at the
/// client's sink as it was written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_server_offers_and_sends() {
    let server = Peer::server();
    let client = Peer::client();

    let video = TestTrack::new("video", RtpCodecType::Video);
    let sender = server.pc.add_track(video.clone()).unwrap();
    let transceivers = server.pc.get_transceivers();
    assert_eq!(transceivers.len(), 1);
    assert!(Arc::ptr_eq(&transceivers[0].sender().unwrap(), &sender));
    assert!(sender.track().is_some());
    assert_eq!(server.pc.get_senders().len(), 1);
    assert_eq!(transceivers[0].kind(), RtpCodecType::Video);

    // The transceiver is told what the track may be sent as, the way LiveKit configures a
    // sender: VP8 and its retransmission codec (as with pion, a repair codec that the
    // preferences leave out stays out). A codec the media engine does not have is refused.
    let codec = |mime_type: &str, fmtp: &str, payload_type| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: mime_type.to_string(),
            clock_rate: 90000,
            sdp_fmtp_line: fmtp.to_string(),
            ..Default::default()
        },
        payload_type,
    };
    assert!(
        transceivers[0]
            .set_codec_preferences(vec![codec(MIME_TYPE_H264, "", 102)])
            .is_err()
    );
    transceivers[0]
        .set_codec_preferences(vec![
            codec(MIME_TYPE_VP8, "", VP8),
            codec(MIME_TYPE_RTX, "apt=96", VP8_RTX),
        ])
        .unwrap();

    negotiate(&server.pc, &client.pc);
    // Negotiated, but not up yet: a track is bound when it can write.
    assert_eq!(transceivers[0].mid(), "0");
    let offer = server.pc.current_local_description().unwrap().sdp;
    assert!(offer.contains("a=rtpmap:96 VP8/90000"), "{offer}");
    assert!(offer.contains("a=rtpmap:97 rtx/90000"), "{offer}");
    assert!(!offer.contains("VP9"), "{offer}");
    connected(&server, &client).await;
    eventually_async(|| video.is_bound(), WAIT, TICK).await;
    assert_eq!(video.binds.load(Ordering::SeqCst), 1);

    let context = video.context();
    let (ssrc, rtx_ssrc) = (context.ssrc(), context.ssrc_retransmission());
    assert_ne!(rtx_ssrc, 0);
    let writer = context.write_stream();
    let negotiated: Vec<u8> = sender
        .get_parameters()
        .codecs
        .iter()
        .map(|c| c.payload_type)
        .collect();
    assert_eq!(negotiated, [VP8, VP8_RTX]);

    // Media on the track's own stream.
    let media = header(ssrc, VP8, 1);
    writer.write_rtp(&media, &[1; 200]).unwrap();

    // A retransmission as LiveKit's down track builds it (RFC 4588): on the repair stream,
    // with a sequence number of that stream's own, the original one in front of the payload.
    let rtx = header(rtx_ssrc, VP8_RTX, 500);
    let mut rtx_payload = vec![0, 1];
    rtx_payload.extend_from_slice(&[1; 200]);
    writer.write_rtp(&rtx, &rtx_payload).unwrap();

    // Padding only, as LiveKit probes for bandwidth: 255 octets on the repair stream.
    let mut padding = header(rtx_ssrc, VP8_RTX, 501);
    padding.padding = true;
    padding.padding_size = 255;
    assert_eq!(writer.write_rtp(&padding, &[]).unwrap(), 12 + 255);

    eventually_async(|| client.sink.rtp.lock().len() == 3, WAIT, TICK).await;
    assert_eq!(
        client.sink.rtp_of(ssrc),
        vec![marshalled(&media, &[1; 200])]
    );
    let repair = client.sink.rtp_of(rtx_ssrc);
    assert_eq!(repair.len(), 2);
    assert_eq!(repair[0], marshalled(&rtx, &rtx_payload));
    assert_eq!(repair[1], marshalled(&padding, &[]));
    assert_eq!(repair[1].len(), 12 + 255);
    assert_eq!(repair[1][repair[1].len() - 1], 255);

    // The client was told about the track: the stream of the description, with its repair
    // stream, on a transceiver the description made.
    eventually_async(|| client.track_of(ssrc).is_some(), WAIT, TICK).await;
    let track = client.track_of(ssrc).unwrap();
    assert_eq!(track.rtx_ssrc(), rtx_ssrc);
    assert_eq!(track.id(), "video");
    assert_eq!(client.tracks.lock().len(), 1);
    let transceivers = client.pc.get_transceivers();
    assert_eq!(transceivers.len(), 1);
    assert_eq!(transceivers[0].kind(), RtpCodecType::Video);
    assert_eq!(transceivers[0].mid(), "0");
    assert_eq!(
        transceivers[0].direction(),
        RtpTransceiverDirection::Recvonly
    );
    assert!(transceivers[0].sender().is_none());
    {
        let bound = client.sink.bound.lock();
        assert!(
            bound
                .iter()
                .any(|i| i.ssrc == ssrc && i.ssrc_retransmission == rtx_ssrc)
        );
        assert!(
            bound
                .iter()
                .any(|i| i.ssrc == rtx_ssrc && i.payload_type == VP8_RTX)
        );
    }

    // Nothing more comes of it later (what `rtc` unwraps is not handed over a second time).
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(client.sink.rtp.lock().len(), 3);

    server.pc.close().unwrap();
    client.pc.close().unwrap();
}

/// Two clients at one server node: one UDP port, two connections, and each gets its own
/// packets only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_connections_on_one_port() {
    let node = Node::new(NodeConfig::default()).unwrap();
    let servers = [Peer::server_on(&node), Peer::server_on(&node)];
    let clients = [Peer::client(), Peer::client()];
    assert_eq!(node.connection_count(), 2);

    let tracks = [
        TestTrack::new("first", RtpCodecType::Video),
        TestTrack::new("second", RtpCodecType::Video),
    ];
    for (client, track) in clients.iter().zip(&tracks) {
        client.pc.add_track(track.clone()).unwrap();
    }
    for (client, server) in clients.iter().zip(&servers) {
        negotiate(&client.pc, &server.pc);
    }
    // Both are told to send to the same address.
    let port = format!(" {} typ host", node.local_addr().port());
    for server in &servers {
        let sdp = server.pc.local_description().unwrap().sdp;
        assert!(sdp.contains(&port), "{sdp}");
    }
    for (client, server) in clients.iter().zip(&servers) {
        connected(client, server).await;
    }
    eventually_async(|| tracks.iter().all(|t| t.is_bound()), WAIT, TICK).await;

    // Both send at the same time.
    let ssrcs = [tracks[0].context().ssrc(), tracks[1].context().ssrc()];
    let mut expected = [Vec::new(), Vec::new()];
    for seq in 0..20u16 {
        for i in 0..2 {
            let h = header(ssrcs[i], VP8, seq);
            let payload = [i as u8 + 1; 50];
            tracks[i].writer().write_rtp(&h, &payload).unwrap();
            expected[i].push((ssrcs[i], marshalled(&h, &payload)));
        }
    }
    eventually_async(
        || servers.iter().all(|s| s.sink.rtp.lock().len() == 20),
        WAIT,
        TICK,
    )
    .await;
    for i in 0..2 {
        assert_eq!(*servers[i].sink.rtp.lock(), expected[i]);
        assert_eq!(servers[i].tracks.lock().len(), 1);
    }

    // And back: each server asks its own client for a key frame.
    for i in 0..2 {
        servers[i].pc.write_rtcp(&pli(ssrcs[i])).unwrap();
    }
    eventually_async(
        || {
            clients
                .iter()
                .all(|c| !c.sink.rtcp_of::<PictureLossIndication>().is_empty())
        },
        WAIT,
        TICK,
    )
    .await;
    for i in 0..2 {
        let got = clients[i].sink.rtcp_of::<PictureLossIndication>();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].media_ssrc, ssrcs[i]);
    }

    // The node knew where every datagram belonged.
    let stats = node.stats();
    assert_eq!(stats.datagrams_without_route, 0);
    assert_eq!(stats.datagrams_queue_full, 0);
    assert_eq!(stats.datagrams_not_sent, 0);

    for peer in servers.iter().chain(&clients) {
        peer.pc.close().unwrap();
    }
    eventually_async(|| node.connection_count() == 0, WAIT, TICK).await;
}

/// What a data channel told.
#[derive(Default)]
struct ChannelLog {
    opened: AtomicUsize,
    closed: AtomicUsize,
    low: AtomicUsize,
    messages: Mutex<Vec<DataChannelMessage>>,
}

impl ChannelLog {
    fn watch(channel: &Arc<DataChannel>) -> Arc<ChannelLog> {
        let log = Arc::new(ChannelLog::default());
        channel.on_open({
            let log = log.clone();
            move || {
                log.opened.fetch_add(1, Ordering::SeqCst);
            }
        });
        channel.on_message({
            let log = log.clone();
            move |message| log.messages.lock().push(message)
        });
        channel.on_buffered_amount_low({
            let log = log.clone();
            move || {
                log.low.fetch_add(1, Ordering::SeqCst);
            }
        });
        channel.on_close({
            let log = log.clone();
            move || {
                log.closed.fetch_add(1, Ordering::SeqCst);
            }
        });
        log
    }

    fn is_open(&self) -> bool {
        self.opened.load(Ordering::SeqCst) == 1
    }
}

/// Every channel the other side opens is kept, and echoes what it gets, as text or as bytes.
fn echo_incoming(peer: &Peer) -> Arc<Mutex<Vec<Arc<DataChannel>>>> {
    let incoming = Arc::new(Mutex::new(Vec::new()));
    peer.pc.on_data_channel({
        let incoming = incoming.clone();
        move |channel| {
            let echo = channel.clone();
            channel.on_message(move |message| {
                if message.is_string {
                    echo.send_text(std::str::from_utf8(&message.data).unwrap())
                        .unwrap();
                } else {
                    echo.send(&message.data).unwrap();
                }
            });
            incoming.lock().push(channel);
        }
    });
    incoming
}

/// Data channels: one the client creates, one the server creates, one both agreed on, with
/// messages both ways as text and as bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_data_channels() {
    let server = Peer::server();
    let client = Peer::client();
    let at_server = echo_incoming(&server);
    let at_client = echo_incoming(&client);

    // Created before the connection: lossy and unordered, as LiveKit's "_lossy".
    let from_client = client
        .pc
        .create_data_channel(
            "_lossy",
            Some(DataChannelInit {
                ordered: Some(false),
                max_retransmits: Some(0),
                ..Default::default()
            }),
        )
        .unwrap();
    assert_eq!(from_client.ready_state(), DataChannelState::Connecting);
    assert_eq!(from_client.id(), None);
    assert!(from_client.send(b"too early").is_err());
    let from_client_log = ChannelLog::watch(&from_client);

    // Agreed on outside of the connection: both sides make it, with the same id.
    let agreed = DataChannelInit {
        negotiated: Some(true),
        id: Some(5),
        ..Default::default()
    };
    let agreed_client = client
        .pc
        .create_data_channel("agreed", Some(agreed.clone()))
        .unwrap();
    let agreed_server = server
        .pc
        .create_data_channel("agreed", Some(agreed))
        .unwrap();
    let agreed_client_log = ChannelLog::watch(&agreed_client);
    let agreed_server_log = ChannelLog::watch(&agreed_server);
    assert!(
        client
            .pc
            .create_data_channel(
                "no id",
                Some(DataChannelInit {
                    negotiated: Some(true),
                    ..Default::default()
                })
            )
            .is_err()
    );

    negotiate(&client.pc, &server.pc);
    connected(&client, &server).await;
    eventually_async(
        || from_client_log.is_open() && agreed_client_log.is_open() && agreed_server_log.is_open(),
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(from_client.ready_state(), DataChannelState::Open);
    assert!(from_client.id().is_some());
    assert_eq!(agreed_client.id(), Some(5));
    assert_eq!(agreed_server.id(), Some(5));

    // The server was told about the client's channel as the client made it, and not about
    // the one both agreed on.
    eventually_async(|| at_server.lock().len() == 1, WAIT, TICK).await;
    {
        let channel = at_server.lock()[0].clone();
        assert_eq!(channel.label(), "_lossy");
        assert!(!channel.ordered());
        // `rtc` does not say how often a channel the other side opened retransmits.
        assert_eq!(channel.max_retransmits(), None);
        assert!(!channel.negotiated());
        assert_eq!(channel.id(), from_client.id());
    }

    // Bytes and text, there and back.
    from_client.send(&[0, 159, 146, 150]).unwrap();
    from_client.send_text("héllo").unwrap();
    eventually_async(|| from_client_log.messages.lock().len() == 2, WAIT, TICK).await;
    assert_eq!(
        *from_client_log.messages.lock(),
        vec![
            DataChannelMessage {
                is_string: false,
                data: Bytes::from_static(&[0, 159, 146, 150]),
            },
            DataChannelMessage {
                is_string: true,
                data: Bytes::from("héllo"),
            },
        ]
    );
    agreed_server.send_text("from the server").unwrap();
    agreed_client.send(b"from the client").unwrap();
    eventually_async(
        || {
            agreed_client_log.messages.lock().len() == 1
                && agreed_server_log.messages.lock().len() == 1
        },
        WAIT,
        TICK,
    )
    .await;
    assert!(agreed_client_log.messages.lock()[0].is_string);
    assert_eq!(
        &agreed_client_log.messages.lock()[0].data[..],
        b"from the server"
    );
    assert!(!agreed_server_log.messages.lock()[0].is_string);
    assert_eq!(
        &agreed_server_log.messages.lock()[0].data[..],
        b"from the client"
    );

    // A channel the server makes when the connection is up, reliable and ordered.
    let from_server = server.pc.create_data_channel("_reliable", None).unwrap();
    let from_server_log = ChannelLog::watch(&from_server);
    eventually_async(
        || from_server_log.is_open() && at_client.lock().len() == 1,
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(at_client.lock()[0].label(), "_reliable");
    assert!(at_client.lock()[0].ordered());
    assert_eq!(at_client.lock()[0].max_retransmits(), None);

    // More than goes out at once: what waits is counted, and the count comes back to 0 when
    // the other side has it all.
    from_server.set_buffered_amount_low_threshold(16 * 1024);
    assert_eq!(from_server.buffered_amount_low_threshold(), 16 * 1024);
    assert_eq!(from_server.buffered_amount(), 0);
    let message = vec![7u8; 4096];
    for _ in 0..64 {
        from_server.send(&message).unwrap();
    }
    assert!(from_server.buffered_amount() > 0);
    eventually_async(|| from_server_log.messages.lock().len() == 64, WAIT, TICK).await;
    assert!(
        from_server_log
            .messages
            .lock()
            .iter()
            .all(|m| m.data == message)
    );
    eventually_async(|| from_server.buffered_amount() == 0, WAIT, TICK).await;
    assert!(from_server_log.low.load(Ordering::SeqCst) >= 1);

    // A handler set on a channel that is open already is called.
    let late = Arc::new(AtomicUsize::new(0));
    at_client.lock()[0].on_open({
        let late = late.clone();
        move || {
            late.fetch_add(1, Ordering::SeqCst);
        }
    });
    eventually_async(|| late.load(Ordering::SeqCst) == 1, WAIT, TICK).await;

    // Closing a channel is told on both sides.
    let closed_at_server = Arc::new(AtomicUsize::new(0));
    at_server.lock()[0].on_close({
        let closed = closed_at_server.clone();
        move || {
            closed.fetch_add(1, Ordering::SeqCst);
        }
    });
    from_client.close().unwrap();
    eventually_async(
        || {
            from_client_log.closed.load(Ordering::SeqCst) == 1
                && closed_at_server.load(Ordering::SeqCst) == 1
        },
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(from_client.ready_state(), DataChannelState::Closed);
    assert!(from_client.send(b"too late").is_err());
    // The others go on.
    agreed_client.send_text("still here").unwrap();
    eventually_async(|| agreed_server_log.messages.lock().len() == 2, WAIT, TICK).await;

    client.pc.close().unwrap();
    server.pc.close().unwrap();
    // The channels that were open are told that it is over.
    eventually_async(
        || {
            agreed_client_log.closed.load(Ordering::SeqCst) == 1
                && agreed_server_log.closed.load(Ordering::SeqCst) == 1
        },
        WAIT,
        TICK,
    )
    .await;
}

/// A second track after the first negotiation, a track taken away, and an ICE restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_renegotiation_and_ice_restart() {
    let server = Peer::server();
    let client = Peer::client();
    let needed = Arc::new(AtomicUsize::new(0));
    server.pc.on_negotiation_needed({
        let needed = needed.clone();
        move || {
            needed.fetch_add(1, Ordering::SeqCst);
        }
    });
    let candidates = Arc::new(Mutex::new(Vec::new()));
    server.pc.on_ice_candidate({
        let candidates = candidates.clone();
        move |candidate| candidates.lock().push(candidate)
    });
    let gathering = Arc::new(Mutex::new(Vec::new()));
    server.pc.on_ice_gathering_state_change({
        let gathering = gathering.clone();
        move |state| gathering.lock().push(state)
    });

    let video = TestTrack::new("video", RtpCodecType::Video);
    server.pc.add_track(video.clone()).unwrap();
    eventually_async(|| needed.load(Ordering::SeqCst) == 1, WAIT, TICK).await;
    negotiate(&server.pc, &client.pc);
    connected(&server, &client).await;
    eventually_async(|| video.is_bound(), WAIT, TICK).await;

    // The node's address was told as the one candidate, and then that there are no more.
    eventually_async(|| candidates.lock().len() == 2, WAIT, TICK).await;
    {
        let candidates = candidates.lock();
        let candidate = candidates[0].as_ref().expect("a candidate");
        assert_eq!(candidate.port, server.node.local_addr().port());
        assert_eq!(candidate.address, "127.0.0.1");
        assert!(candidates[1].is_none());
    }
    assert_eq!(
        *gathering.lock(),
        vec![IceGatheringState::Gathering, IceGatheringState::Complete]
    );
    assert_eq!(server.pc.ice_gathering_state(), IceGatheringState::Complete);

    // A second track: negotiation is asked for, and the track is bound when it is done.
    let audio = TestTrack::new("audio", RtpCodecType::Audio);
    let audio_sender = server.pc.add_track(audio.clone()).unwrap();
    eventually_async(|| needed.load(Ordering::SeqCst) == 2, WAIT, TICK).await;
    assert!(!audio.is_bound());
    negotiate(&server.pc, &client.pc);
    eventually_async(|| audio.is_bound(), WAIT, TICK).await;
    assert_eq!(video.binds.load(Ordering::SeqCst), 1);
    assert_eq!(audio.payload_type(), OPUS);

    let mids: Vec<String> = server
        .pc
        .get_transceivers()
        .iter()
        .map(|t| t.mid())
        .collect();
    assert_eq!(mids, ["0", "1"]);
    // Without codec preferences a track is offered with all the media engine has.
    let offer = server.pc.current_local_description().unwrap().sdp;
    assert!(offer.contains("a=rtpmap:98 VP9/90000"), "{offer}");
    let kinds: Vec<RtpCodecType> = client
        .pc
        .get_transceivers()
        .iter()
        .map(|t| t.kind())
        .collect();
    assert_eq!(kinds, [RtpCodecType::Video, RtpCodecType::Audio]);

    let (video_ssrc, audio_ssrc) = (video.context().ssrc(), audio.context().ssrc());
    video
        .writer()
        .write_rtp(&header(video_ssrc, VP8, 1), &[1; 30])
        .unwrap();
    audio
        .writer()
        .write_rtp(&header(audio_ssrc, OPUS, 1), &[2; 30])
        .unwrap();
    eventually_async(
        || client.track_of(video_ssrc).is_some() && client.track_of(audio_ssrc).is_some(),
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(
        client.track_of(audio_ssrc).unwrap().kind(),
        RtpCodecType::Audio
    );
    assert_eq!(client.sink.rtp_of(audio_ssrc).len(), 1);

    // Nothing was gathered again for it.
    assert_eq!(candidates.lock().len(), 2);
    assert_eq!(gathering.lock().len(), 2);

    // The track is taken away: it is unbound, and its writer writes no more.
    let audio_writer = audio.writer();
    server.pc.remove_track(&audio_sender).unwrap();
    assert_eq!(
        audio_writer.write_rtp(&header(audio_ssrc, OPUS, 2), &[2; 30]),
        Err(Error::ClosedPipe)
    );
    eventually_async(|| audio.unbinds.load(Ordering::SeqCst) == 1, WAIT, TICK).await;
    assert!(server.pc.remove_track(&audio_sender).is_err());
    assert!(audio_sender.track().is_none());
    negotiate(&server.pc, &client.pc);
    assert_eq!(audio.binds.load(Ordering::SeqCst), 1);

    // An ICE restart: new credentials on both sides, the candidates told again, and media
    // flows afterwards.
    let ufrag = |peer: &Peer| {
        ice_ufrag(&peer.pc.local_description().unwrap().sdp)
            .unwrap()
            .to_string()
    };
    let before = (ufrag(&server), ufrag(&client));
    negotiate_with(
        &server.pc,
        &client.pc,
        Some(OfferOptions { ice_restart: true }),
    );
    let after = (ufrag(&server), ufrag(&client));
    assert_ne!(before.0, after.0);
    assert_ne!(before.1, after.1);
    connected(&server, &client).await;
    eventually_async(|| candidates.lock().len() == 4, WAIT, TICK).await;
    assert!(candidates.lock()[2].is_some());
    assert!(candidates.lock()[3].is_none());
    assert_eq!(gathering.lock().len(), 4);

    let received = client.sink.rtp_of(video_ssrc).len();
    let mut seq = 100;
    eventually_async(
        || {
            seq += 1;
            video
                .writer()
                .write_rtp(&header(video_ssrc, VP8, seq), &[3; 30])
                .unwrap();
            client.sink.rtp_of(video_ssrc).len() > received
        },
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(video.binds.load(Ordering::SeqCst), 1);
    assert_eq!(video.unbinds.load(Ordering::SeqCst), 0);

    server.pc.close().unwrap();
    client.pc.close().unwrap();
}

/// Callbacks call back into their connection: nothing of the connection is locked while
/// they run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_callbacks_may_call_back_in() {
    let server = Peer::server();
    let client = Peer::client();

    // When a track opens the server asks for a key frame, on the connection the track is on,
    // and looks around.
    let mids = Arc::new(Mutex::new(Vec::new()));
    server.pc.on_track({
        let pc = server.pc.clone();
        let mids = mids.clone();
        move |track, receiver| {
            assert_eq!(pc.signaling_state(), SignalingState::Stable);
            assert_eq!(pc.get_transceivers().len(), 1);
            assert!(!receiver.get_parameters().codecs.is_empty());
            pc.write_rtcp(&pli(track.ssrc())).unwrap();
            mids.lock().push(receiver.rtp_transceiver().unwrap().mid());
        }
    });
    // And its sink answers every packet with a report, as the buffer answers with NACKs.
    *server.sink.on_rtp.lock() = Some(Box::new({
        let pc = server.pc.clone();
        move |ssrc| {
            pc.write_rtcp(&[Box::new(ReceiverReport {
                ssrc,
                ..Default::default()
            })])
            .unwrap();
        }
    }));
    // A state callback that asks the connection for its state.
    let states = Arc::new(Mutex::new(Vec::new()));
    server.pc.on_connection_state_change({
        let pc = server.pc.clone();
        let states = states.clone();
        move |state| {
            let _ = pc.connection_state();
            let _ = pc.local_description();
            states.lock().push(state);
        }
    });

    let video = TestTrack::new("video", RtpCodecType::Video);
    client.pc.add_track(video.clone()).unwrap();
    negotiate(&client.pc, &server.pc);
    connected(&client, &server).await;
    eventually_async(|| video.is_bound(), WAIT, TICK).await;
    let ssrc = video.context().ssrc();
    for seq in 0..10 {
        video
            .writer()
            .write_rtp(&header(ssrc, VP8, seq), &[4; 60])
            .unwrap();
    }
    eventually_async(
        || {
            client.sink.rtcp_of::<ReceiverReport>().len() == 10
                && client.sink.rtcp_of::<PictureLossIndication>().len() == 1
        },
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(*mids.lock(), ["0"]);
    assert!(states.lock().contains(&PeerConnectionState::Connected));
    assert!(
        client
            .sink
            .rtcp_of::<ReceiverReport>()
            .iter()
            .all(|r| r.ssrc == ssrc)
    );

    // A callback may write on another connection (a subscriber's write happens from a
    // publisher's packet path) and may close its own.
    *client.sink.on_rtcp.lock() = Some(Box::new({
        let own = client.pc.clone();
        let other = server.pc.clone();
        move |()| {
            let _ = other.write_rtcp(&pli(1));
            own.close().unwrap();
        }
    }));
    server.pc.write_rtcp(&pli(ssrc)).unwrap();
    eventually_async(|| client.node.connection_count() == 0, WAIT, TICK).await;
    assert_eq!(client.pc.connection_state(), PeerConnectionState::Closed);

    server.pc.close().unwrap();
    eventually_async(|| server.node.connection_count() == 0, WAIT, TICK).await;
}

/// Closing a connection ends its task and its timers: the node counts no connection any
/// more, however the connection ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_close() {
    let server = Peer::server();
    let client = Peer::client();
    let video = TestTrack::new("video", RtpCodecType::Video);
    server.pc.add_track(video.clone()).unwrap();
    let channel = server.pc.create_data_channel("data", None).unwrap();
    let channel_log = ChannelLog::watch(&channel);
    negotiate(&server.pc, &client.pc);
    connected(&server, &client).await;
    eventually_async(|| video.is_bound() && channel_log.is_open(), WAIT, TICK).await;
    assert_eq!(server.node.connection_count(), 1);
    assert_eq!(client.node.connection_count(), 1);
    let writer = video.writer();
    let ssrc = video.context().ssrc();
    writer.write_rtp(&header(ssrc, VP8, 1), &[1; 10]).unwrap();

    server.pc.close().unwrap();
    // At once: the states, and nothing can be done with it any more.
    assert_eq!(server.pc.connection_state(), PeerConnectionState::Closed);
    assert_eq!(server.pc.signaling_state(), SignalingState::Closed);
    assert_eq!(server.pc.create_offer(None), Err(Error::ConnectionClosed));
    assert_eq!(
        server
            .pc
            .add_track(TestTrack::new("late", RtpCodecType::Audio))
            .err(),
        Some(Error::ConnectionClosed)
    );
    assert_eq!(
        writer.write_rtp(&header(ssrc, VP8, 2), &[1; 10]),
        Err(Error::ClosedPipe)
    );
    assert_eq!(server.pc.write_rtcp(&pli(ssrc)), Err(Error::ClosedPipe));
    assert!(channel.send(b"late").is_err());
    // Closing again is nothing.
    server.pc.close().unwrap();

    // From its task: the last things it tells, and then the task is gone.
    eventually_async(|| server.node.connection_count() == 0, WAIT, TICK).await;
    assert_eq!(video.unbinds.load(Ordering::SeqCst), 1);
    assert_eq!(channel_log.closed.load(Ordering::SeqCst), 1);
    assert_eq!(
        server.states.lock().last(),
        Some(&PeerConnectionState::Closed)
    );
    // Nothing is sent for it any more (no timer of it runs).
    let sent = server.node.stats().datagrams_sent;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(server.node.stats().datagrams_sent, sent);

    client.pc.close().unwrap();
    eventually_async(|| client.node.connection_count() == 0, WAIT, TICK).await;

    // A connection whose last handle is dropped is closed.
    let node = Node::new(NodeConfig::default()).unwrap();
    let pc = PeerConnection::new(&node, Configuration::default()).unwrap();
    let clone = pc.clone();
    assert_eq!(node.connection_count(), 1);
    drop(pc);
    assert_eq!(node.connection_count(), 1);
    drop(clone);
    eventually_async(|| node.connection_count() == 0, WAIT, TICK).await;

    // And so are the connections of a node that is closed.
    let pc = PeerConnection::new(&node, Configuration::default()).unwrap();
    assert_eq!(node.connection_count(), 1);
    node.close();
    eventually_async(|| node.connection_count() == 0, WAIT, TICK).await;
    assert_eq!(pc.connection_state(), PeerConnectionState::Closed);
}

/// The candidates are trickled: the descriptions go over as they were created, without
/// candidates, and each side is given the other's as they are told.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_trickled_candidates() {
    let server = Peer::server();
    let client = Peer::client();
    let trickled = |peer: &Peer| {
        let candidates = Arc::new(Mutex::new(Vec::new()));
        peer.pc.on_ice_candidate({
            let candidates = candidates.clone();
            move |candidate| {
                if let Some(candidate) = candidate {
                    candidates.lock().push(candidate.to_json().unwrap());
                }
            }
        });
        candidates
    };
    let (from_server, from_client) = (trickled(&server), trickled(&client));
    let channel = client.pc.create_data_channel("data", None).unwrap();
    let log = ChannelLog::watch(&channel);

    let offer = client.pc.create_offer(None).unwrap();
    assert!(!offer.sdp.contains("a=candidate"));
    client.pc.set_local_description(offer.clone()).unwrap();
    assert_eq!(
        client.pc.pending_local_description().unwrap().r#type,
        SdpType::Offer
    );
    assert!(client.pc.current_local_description().is_none());
    eventually_async(|| from_client.lock().len() == 1, WAIT, TICK).await;
    // A candidate cannot be taken before there is a description to take it for.
    assert!(
        server
            .pc
            .add_ice_candidate(from_client.lock()[0].clone())
            .is_err()
    );

    server.pc.set_remote_description(offer).unwrap();
    assert_eq!(
        server.pc.pending_remote_description().unwrap().r#type,
        SdpType::Offer
    );
    assert!(server.pc.remote_description().is_some());
    let answer = server.pc.create_answer().unwrap();
    server.pc.set_local_description(answer.clone()).unwrap();
    assert!(server.pc.pending_remote_description().is_none());
    assert_eq!(
        server.pc.current_remote_description().unwrap().r#type,
        SdpType::Offer
    );
    client.pc.set_remote_description(answer).unwrap();

    // Nothing connects until the client knows where the server is.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!client.is_connected());
    assert!(!server.is_connected());

    eventually_async(|| from_server.lock().len() == 1, WAIT, TICK).await;
    let candidate = from_server.lock()[0].clone();
    assert!(
        candidate
            .candidate
            .contains(&format!(" {} typ host", server.node.local_addr().port()))
    );
    client.pc.add_ice_candidate(candidate).unwrap();
    server
        .pc
        .add_ice_candidate(from_client.lock()[0].clone())
        .unwrap();
    connected(&client, &server).await;
    eventually_async(|| log.is_open(), WAIT, TICK).await;
    assert_eq!(
        client.pc.ice_connection_state(),
        IceConnectionState::Connected
    );

    // `rtc` keeps count of what went over the connection.
    let stats = server.pc.get_stats();
    assert!(stats.transport().is_some());
    assert!(stats.candidate_pairs().count() >= 1);

    client.pc.close().unwrap();
    server.pc.close().unwrap();
}

/// A publisher that sends simulcast, as browsers do it: no SSRCs in the description, the
/// layers named by their rid in a header extension. Each layer becomes a track of its own
/// on the one receiver; a retransmission stream, which names the layer it repairs, reaches
/// the sink like everything else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_simulcast_layers_are_tracks() {
    let server = Peer::server();
    let client = Peer::client();
    let video = TestTrack::new("video", RtpCodecType::Video);
    client.pc.add_track(video.clone()).unwrap();

    // The client's own description says one stream with its SSRC. The server is told what a
    // browser would say instead.
    let offer = client.pc.create_offer(None).unwrap();
    client.pc.set_local_description(offer).unwrap();
    let mut sdp = String::new();
    for line in client.pc.local_description().unwrap().sdp.lines() {
        if !line.starts_with("a=ssrc") {
            sdp.push_str(line);
            sdp.push_str("\r\n");
        }
    }
    sdp.push_str("a=rid:q send\r\na=rid:h send\r\na=simulcast:send q;h\r\n");
    server
        .pc
        .set_remote_description(SessionDescription {
            r#type: SdpType::Offer,
            sdp,
        })
        .unwrap();
    let answer = server.pc.create_answer().unwrap();
    assert!(
        answer.sdp.contains("a=simulcast:recv q;h"),
        "{}",
        answer.sdp
    );
    server.pc.set_local_description(answer).unwrap();
    client
        .pc
        .set_remote_description(server.pc.local_description().unwrap())
        .unwrap();
    connected(&client, &server).await;
    eventually_async(|| video.is_bound(), WAIT, TICK).await;

    let context = video.context();
    let mid = extension_id(context.as_ref(), sdp::SDES_MID_URI);
    let rid = extension_id(context.as_ref(), sdp::SDES_RTP_STREAM_ID_URI);
    let repaired = extension_id(context.as_ref(), sdp::SDES_REPAIR_RTP_STREAM_ID_URI);
    let writer = video.writer();
    let layer = |ssrc: u32, payload_type: u8, seq: u16, id: u8, name: &'static str| {
        let mut h = header(ssrc, payload_type, seq);
        h.set_extension(mid, Bytes::from_static(b"0")).unwrap();
        h.set_extension(id, Bytes::from_static(name.as_bytes()))
            .unwrap();
        h
    };
    let packets = [
        (layer(1111, VP8, 1, rid, "q"), vec![1u8; 20]),
        (layer(2222, VP8, 1, rid, "h"), vec![2u8; 20]),
        // A retransmission of the low layer's packet.
        (layer(3333, VP8_RTX, 1, repaired, "q"), vec![0, 1, 1, 1]),
        (layer(1111, VP8, 2, rid, "q"), vec![1u8; 20]),
    ];
    for (h, payload) in &packets {
        writer.write_rtp(h, payload).unwrap();
    }

    eventually_async(|| server.sink.rtp.lock().len() == 4, WAIT, TICK).await;
    let expected: Vec<(u32, Bytes)> = packets
        .iter()
        .map(|(h, payload)| (h.ssrc, marshalled(h, payload)))
        .collect();
    assert_eq!(*server.sink.rtp.lock(), expected);

    eventually_async(|| server.tracks.lock().len() == 2, WAIT, TICK).await;
    let tracks = server.tracks.lock().clone();
    assert_eq!(
        (tracks[0].0.ssrc(), tracks[0].0.rid()),
        (1111, "q".to_string())
    );
    assert_eq!(
        (tracks[1].0.ssrc(), tracks[1].0.rid()),
        (2222, "h".to_string())
    );
    for (track, receiver) in &tracks {
        assert_eq!(track.id(), "video");
        assert_eq!(track.kind(), RtpCodecType::Video);
        assert_eq!(track.codec().payload_type, VP8);
        assert!(Arc::ptr_eq(receiver, &tracks[0].1));
    }
    assert_eq!(tracks[0].1.tracks().len(), 2);
    assert_eq!(tracks[0].1.track().unwrap().ssrc(), 1111);

    // The layers were announced as streams. The retransmission stream was not: `rtc` does
    // not pair a stream that names what it repairs (and so the track knows no RTX SSRC).
    let bound: Vec<u32> = server.sink.bound.lock().iter().map(|i| i.ssrc).collect();
    assert_eq!(bound, [1111, 2222]);
    assert_eq!(tracks[0].0.rtx_ssrc(), 0);

    client.pc.close().unwrap();
    server.pc.close().unwrap();
}

/// What a connection told about its ICE transport, in the order it was told.
#[derive(Clone, Debug, PartialEq)]
enum IceEvent {
    State(IceConnectionState),
    Pair(Box<IceCandidatePair>),
}

fn ice_events(peer: &Peer) -> Arc<Mutex<Vec<IceEvent>>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    peer.pc.on_ice_connection_state_change({
        let events = events.clone();
        move |state| events.lock().push(IceEvent::State(state))
    });
    peer.pc.on_selected_candidate_pair_change({
        let events = events.clone();
        let pc = peer.pc.clone();
        move |pair| {
            // Nothing of the connection is locked: the callback asks it for the pair.
            assert_eq!(pc.get_selected_candidate_pair().as_ref(), Some(&pair));
            events.lock().push(IceEvent::Pair(Box::new(pair)));
        }
    });
    events
}

fn pairs_of(events: &Mutex<Vec<IceEvent>>) -> Vec<IceCandidatePair> {
    events
        .lock()
        .iter()
        .filter_map(|e| match e {
            IceEvent::Pair(pair) => Some((**pair).clone()),
            IceEvent::State(_) => None,
        })
        .collect()
}

/// The selected candidate pair: told once when the connection comes up, after the state that
/// came with it, told again after an ICE restart, and to be asked for in between.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_selected_candidate_pair() {
    let server = Peer::server();
    let client = Peer::client();
    let (at_server, at_client) = (ice_events(&server), ice_events(&client));
    let channel = client.pc.create_data_channel("data", None).unwrap();
    let log = ChannelLog::watch(&channel);
    assert_eq!(server.pc.get_selected_candidate_pair(), None);
    assert_eq!(client.pc.get_selected_candidate_pair(), None);

    negotiate(&client.pc, &server.pc);
    connected(&client, &server).await;
    eventually_async(
        || pairs_of(&at_server).len() == 1 && pairs_of(&at_client).len() == 1,
        WAIT,
        TICK,
    )
    .await;

    // Each side has its own node's address and the other's.
    let ports = (
        server.node.local_addr().port(),
        client.node.local_addr().port(),
    );
    let pair = pairs_of(&at_server)[0].clone();
    assert_eq!(
        (pair.local().address.as_str(), pair.local().port),
        ("127.0.0.1", ports.0)
    );
    assert_eq!(
        (pair.remote().address.as_str(), pair.remote().port),
        ("127.0.0.1", ports.1)
    );
    assert_eq!(pair.local().typ, IceCandidateType::Host);
    assert_eq!(pair.local().protocol, IceProtocol::Udp);
    assert_eq!(server.pc.get_selected_candidate_pair(), Some(pair));
    let pair = pairs_of(&at_client)[0].clone();
    assert_eq!((pair.local().port, pair.remote().port), (ports.1, ports.0));
    assert_eq!(client.pc.get_selected_candidate_pair(), Some(pair));

    // The pair was told after the state it came with.
    for events in [&at_server, &at_client] {
        let events = events.lock();
        let at = events
            .iter()
            .position(|e| matches!(e, IceEvent::Pair(_)))
            .unwrap();
        assert!(
            events[..at].contains(&IceEvent::State(IceConnectionState::Connected)),
            "{events:?}"
        );
    }

    // Traffic changes nothing: the pair is told when it changes, not when it is looked at.
    eventually_async(|| log.is_open(), WAIT, TICK).await;
    for _ in 0..20 {
        channel.send(&[1; 100]).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(pairs_of(&at_server).len(), 1);
    assert_eq!(pairs_of(&at_client).len(), 1);

    // An ICE restart drops the pair, and the one that is selected then is told, though its
    // addresses are the same.
    negotiate_with(
        &client.pc,
        &server.pc,
        Some(OfferOptions { ice_restart: true }),
    );
    eventually_async(
        || pairs_of(&at_server).len() == 2 && pairs_of(&at_client).len() == 2,
        WAIT,
        TICK,
    )
    .await;
    let pair = pairs_of(&at_server)[1].clone();
    assert_eq!((pair.local().port, pair.remote().port), ports);
    assert_eq!(server.pc.get_selected_candidate_pair(), Some(pair));
    assert!(
        at_server
            .lock()
            .contains(&IceEvent::State(IceConnectionState::Checking))
    );

    // A closed connection has no pair, and tells none.
    server.pc.close().unwrap();
    assert_eq!(server.pc.get_selected_candidate_pair(), None);
    client.pc.close().unwrap();
    eventually_async(|| server.node.connection_count() == 0, WAIT, TICK).await;
    assert_eq!(pairs_of(&at_server).len(), 2);
}

/// What a send on a data channel fails with when the channel is not open yet and when it was
/// closed, and more than one listener for the low threshold of a channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_data_channel_send_errors_and_listeners() {
    let server = Peer::server();
    let client = Peer::client();
    let at_server = Arc::new(Mutex::new(Vec::new()));
    server.pc.on_data_channel({
        let at_server = at_server.clone();
        move |channel| at_server.lock().push(channel)
    });

    let channel = client.pc.create_data_channel("data", None).unwrap();
    let other = client.pc.create_data_channel("other", None).unwrap();
    // The handler, and two that listen beside it.
    let log = ChannelLog::watch(&channel);
    let listeners = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
    for listener in &listeners {
        let listener = listener.clone();
        channel.add_buffered_amount_low_listener(move || {
            listener.fetch_add(1, Ordering::SeqCst);
        });
    }

    // Not open yet: pion's `Send` says a closed pipe.
    assert_eq!(channel.send(b"too early"), Err(Error::ClosedPipe));
    assert_eq!(channel.send_text("too early"), Err(Error::ClosedPipe));

    negotiate(&client.pc, &server.pc);
    connected(&client, &server).await;
    eventually_async(
        || {
            log.is_open()
                && other.ready_state() == DataChannelState::Open
                && at_server.lock().len() == 2
        },
        WAIT,
        TICK,
    )
    .await;

    // More than goes out at once: when it has drained, the handler and both listeners were
    // told, each as often as the others.
    channel.set_buffered_amount_low_threshold(16 * 1024);
    for _ in 0..64 {
        channel.send(&[7u8; 4096]).unwrap();
    }
    eventually_async(|| channel.buffered_amount() == 0, WAIT, TICK).await;
    eventually_async(
        || {
            let low = log.low.load(Ordering::SeqCst);
            low >= 1 && listeners.iter().all(|l| l.load(Ordering::SeqCst) == low)
        },
        WAIT,
        TICK,
    )
    .await;

    // Closed by this side: from then on the stream is closed, at once and for good.
    channel.close().unwrap();
    assert_eq!(channel.send(b"too late"), Err(Error::StreamClosed));
    assert_eq!(channel.send_text("too late"), Err(Error::StreamClosed));
    // And for the other side, once it has been told.
    let remote = at_server
        .lock()
        .iter()
        .find(|c| c.label() == "data")
        .cloned()
        .unwrap();
    eventually_async(
        || remote.ready_state() == DataChannelState::Closed,
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(remote.send(b"too late"), Err(Error::StreamClosed));
    eventually_async(|| log.closed.load(Ordering::SeqCst) == 1, WAIT, TICK).await;
    assert_eq!(channel.send(b"too late"), Err(Error::StreamClosed));
    // A listener that is added to a closed channel is not kept.
    channel.add_buffered_amount_low_listener(|| panic!("the channel is closed"));

    // The other channel goes on, until its connection is closed.
    other.send(b"still here").unwrap();
    client.pc.close().unwrap();
    assert_eq!(other.send(b"too late"), Err(Error::StreamClosed));
    server.pc.close().unwrap();
}

/// Two connections of one node with settings of their own: one as the node is set up (ICE
/// lite, the node's addresses), and one that is a full ICE agent, advertises an address of
/// its own and was given ICE servers, as LiveKit changes its copy of the setting engine for
/// some clients.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_settings_per_connection() {
    // pion's setters that do more than set a field.
    let mut settings = SettingEngine::default();
    assert!(settings.set_answering_dtls_role(DtlsRole::Auto).is_err());
    settings.set_answering_dtls_role(DtlsRole::Server).unwrap();
    settings.set_ice_timeouts(
        crate::go::time::Duration::from_secs(4),
        crate::go::time::Duration::from_secs(6),
        crate::go::time::Duration::from_secs(1),
    );
    assert_eq!(
        settings,
        SettingEngine {
            answering_dtls_role: DtlsRole::Server,
            ice_disconnected_timeout: Some(crate::go::time::Duration::from_secs(4)),
            ice_failed_timeout: Some(crate::go::time::Duration::from_secs(6)),
            ice_keepalive_interval: Some(crate::go::time::Duration::from_secs(1)),
            ..Default::default()
        }
    );

    // The node advertises two addresses. Its socket is at the second one only, and what
    // arrives there is told to `rtc` as having arrived at the first.
    let unused: std::net::IpAddr = "127.0.0.9".parse().unwrap();
    let own: std::net::IpAddr = "127.0.0.1".parse().unwrap();
    let node = Node::new(NodeConfig {
        nat_1to1_ips: vec![unused, own],
        ..Default::default()
    })
    .unwrap();
    let port = node.local_addr().port();
    assert_eq!(
        node.candidates_for(&["::1".parse().unwrap(), own]),
        vec![SocketAddr::new(own, port)]
    );

    // As the node is set up.
    let plain = Peer::server_on(&node);
    // A copy of those settings, changed for one connection.
    let servers = vec![IceServer {
        urls: vec!["stun:stun.example.org:3478".to_string()],
        ..Default::default()
    }];
    let changed = Peer::with_servers(
        &node,
        SettingEngine {
            lite: false,
            host_candidate_ips: Some(vec![own]),
            ..settings.clone()
        },
        servers.clone(),
    );
    assert!(plain.pc.ice_servers().is_empty());
    assert_eq!(changed.pc.ice_servers(), servers);
    // A server whose URL cannot be read is refused when the connection is made.
    assert!(
        PeerConnection::new(
            &node,
            Configuration {
                ice_servers: vec![IceServer {
                    urls: vec!["nonsense".to_string()],
                    ..Default::default()
                }],
                ..Default::default()
            }
        )
        .is_err()
    );

    let clients = [Peer::client(), Peer::client()];
    let channels = [
        clients[0].pc.create_data_channel("data", None).unwrap(),
        clients[1].pc.create_data_channel("data", None).unwrap(),
    ];
    let logs = [
        ChannelLog::watch(&channels[0]),
        ChannelLog::watch(&channels[1]),
    ];
    let _echo = [echo_incoming(&plain), echo_incoming(&changed)];
    negotiate(&clients[0].pc, &plain.pc);
    negotiate(&clients[1].pc, &changed.pc);

    // What each tells its client.
    let candidate = |ip: std::net::IpAddr| format!(" {ip} {port} typ host");
    let sdp = plain.pc.local_description().unwrap().sdp;
    assert!(sdp.contains("a=ice-lite"), "{sdp}");
    assert!(sdp.contains(&candidate(unused)), "{sdp}");
    assert!(sdp.contains(&candidate(own)), "{sdp}");
    let sdp = changed.pc.local_description().unwrap().sdp;
    assert!(!sdp.contains("a=ice-lite"), "{sdp}");
    assert!(!sdp.contains(&candidate(unused)), "{sdp}");
    assert!(sdp.contains(&candidate(own)), "{sdp}");
    assert!(
        sdp.lines()
            .filter(|line| line.starts_with("a=candidate"))
            .all(|line| line.contains(&candidate(own))),
        "{sdp}"
    );

    // Both come up, and both carry data.
    connected(&clients[0], &plain).await;
    connected(&clients[1], &changed).await;
    eventually_async(|| logs.iter().all(|l| l.is_open()), WAIT, TICK).await;
    for (i, channel) in channels.iter().enumerate() {
        channel.send(&[i as u8; 10]).unwrap();
    }
    eventually_async(
        || logs.iter().all(|l| l.messages.lock().len() == 1),
        WAIT,
        TICK,
    )
    .await;
    for (i, log) in logs.iter().enumerate() {
        assert_eq!(&log.messages.lock()[0].data[..], &[i as u8; 10]);
    }
    // The connection with an address of its own is at that address.
    let pair = changed.pc.get_selected_candidate_pair().unwrap();
    assert_eq!(
        (pair.local().address.as_str(), pair.local().port),
        ("127.0.0.1", port)
    );
    assert_eq!(node.stats().datagrams_without_route, 0);

    for peer in [&plain, &changed].into_iter().chain(&clients) {
        peer.pc.close().unwrap();
    }
    eventually_async(|| node.connection_count() == 0, WAIT, TICK).await;
}

/// A transceiver is used again for another track, as LiveKit does for a subscription that
/// comes back: the track of its sender is replaced without a negotiation. The first track is
/// unbound, the second is bound with what the media section has, and what it writes arrives
/// on the SSRC the first one had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_replace_track() {
    let server = Peer::server();
    let client = Peer::client();
    let needed = Arc::new(AtomicUsize::new(0));
    server.pc.on_negotiation_needed({
        let needed = needed.clone();
        move || {
            needed.fetch_add(1, Ordering::SeqCst);
        }
    });

    // A track that is replaced before its sender was ever bound is let go of, and the one
    // that took its place is bound when the negotiation is done.
    let never = TestTrack::new("never", RtpCodecType::Video);
    let first = TestTrack::new("first", RtpCodecType::Video);
    let sender = server.pc.add_track(never.clone()).unwrap();
    sender.replace_track(Some(first.clone())).unwrap();
    assert!(sender.track().is_some_and(|t| t.id() == "first"));
    negotiate(&server.pc, &client.pc);
    connected(&server, &client).await;
    eventually_async(|| first.is_bound(), WAIT, TICK).await;
    assert_eq!(never.bound_and_unbound(), (0, 0));
    assert_eq!(first.bound_and_unbound(), (1, 0));
    let offer = server.pc.current_local_description().unwrap().sdp;
    assert!(offer.contains("a=msid:stream first"), "{offer}");

    let context = first.context();
    let (ssrc, rtx_ssrc) = (context.ssrc(), context.ssrc_retransmission());
    assert_ne!(rtx_ssrc, 0);
    let first_writer = first.writer();
    let one = header(ssrc, VP8, 1);
    first_writer.write_rtp(&one, &[1; 50]).unwrap();
    eventually_async(|| client.sink.rtp_of(ssrc).len() == 1, WAIT, TICK).await;
    eventually_async(|| client.track_of(ssrc).is_some(), WAIT, TICK).await;
    let negotiations = needed.load(Ordering::SeqCst);

    // The second track, through the seam, as the ported code holds a sender.
    let second = TestTrack::new("second", RtpCodecType::Video);
    let seam: Arc<dyn crate::webrtc::RtpSender> = sender.clone();
    seam.replace_track(Some(second.clone())).unwrap();
    // At once: the sender has the new track, and the writer of the old one writes no more.
    assert!(sender.track().is_some_and(|t| t.id() == "second"));
    assert_eq!(
        first_writer.write_rtp(&header(ssrc, VP8, 2), &[1; 50]),
        Err(Error::ClosedPipe)
    );
    // From the connection's task: the first is unbound and the second bound, each once.
    eventually_async(|| second.is_bound(), WAIT, TICK).await;
    assert_eq!(first.bound_and_unbound(), (1, 1));
    assert_eq!(second.bound_and_unbound(), (1, 0));
    // It was bound with what the media section has: the stream's SSRCs, and the codecs that
    // were negotiated, not the one the first track chose of them.
    let context = second.context();
    assert_eq!(
        (context.ssrc(), context.ssrc_retransmission()),
        (ssrc, rtx_ssrc)
    );
    assert_eq!(*second.offered.lock(), *first.offered.lock());
    assert!(second.offered.lock().len() > 1);
    assert!(second.offered.lock().contains(&VP8_RTX));
    assert_eq!(second.payload_type(), VP8);
    assert_eq!(sender.get_parameters().encodings[0].ssrc, ssrc);

    // What it writes arrives on the same stream, and the client sees no other track.
    let two = header(ssrc, VP8, 2);
    second.writer().write_rtp(&two, &[2; 50]).unwrap();
    eventually_async(|| client.sink.rtp_of(ssrc).len() == 2, WAIT, TICK).await;
    assert_eq!(
        client.sink.rtp_of(ssrc),
        vec![marshalled(&one, &[1; 50]), marshalled(&two, &[2; 50])]
    );
    assert_eq!(client.tracks.lock().len(), 1);
    // Nothing was negotiated for it, and nothing is asked to be.
    assert_eq!(needed.load(Ordering::SeqCst), negotiations);
    assert_eq!(server.pc.signaling_state(), SignalingState::Stable);
    assert_eq!(server.pc.get_transceivers().len(), 1);

    // The sink was told of the stream of the first track, of its end, and of the stream of
    // the second, which is the same stream.
    let local = server.sink.local_calls();
    let info = match local.first() {
        Some(SinkCall::BindLocal(info)) => info.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(
        local,
        vec![
            SinkCall::BindLocal(info.clone()),
            SinkCall::UnbindLocal(info.clone()),
            SinkCall::BindLocal(info.clone()),
        ]
    );
    assert_eq!((info.ssrc, info.ssrc_retransmission), (ssrc, rtx_ssrc));
    assert_eq!(
        (info.payload_type, info.payload_type_retransmission),
        (VP8, VP8_RTX)
    );
    assert!(info.mime_type.eq_ignore_ascii_case(MIME_TYPE_VP8));
    assert_eq!(info.clock_rate, 90000);
    assert!(
        info.rtp_header_extensions
            .iter()
            .any(|e| e.uri == sdp::TRANSPORT_CC_URI && e.id != 0)
    );

    // A track of another kind is refused, and the sender keeps the one it has.
    let audio = TestTrack::new("audio", RtpCodecType::Audio);
    assert!(sender.replace_track(Some(audio.clone())).is_err());
    assert!(sender.track().is_some_and(|t| t.id() == "second"));
    let three = header(ssrc, VP8, 3);
    second.writer().write_rtp(&three, &[2; 50]).unwrap();

    // The track is taken away: it is unbound, and the sender has none. Then the sender is
    // given one again, which is bound as the second was.
    sender.replace_track(None).unwrap();
    assert!(sender.track().is_none());
    eventually_async(|| second.bound_and_unbound() == (1, 1), WAIT, TICK).await;
    let third = TestTrack::new("third", RtpCodecType::Video);
    sender.replace_track(Some(third.clone())).unwrap();
    eventually_async(|| third.is_bound(), WAIT, TICK).await;
    assert_eq!(third.context().ssrc(), ssrc);
    let four = header(ssrc, VP8, 4);
    third.writer().write_rtp(&four, &[3; 50]).unwrap();
    eventually_async(|| client.sink.rtp_of(ssrc).len() == 4, WAIT, TICK).await;
    assert_eq!(server.sink.local_calls().len(), 5);
    assert_eq!(audio.bound_and_unbound(), (0, 0));

    // The next negotiation names the track the sender has now, in the same media section
    // and on the same SSRC, and binds nothing again.
    negotiate(&server.pc, &client.pc);
    let offer = server.pc.current_local_description().unwrap().sdp;
    assert!(offer.contains("a=msid:stream third"), "{offer}");
    assert!(offer.contains(&format!("a=ssrc:{ssrc} ")), "{offer}");
    assert_eq!(offer.matches("m=video").count(), 1, "{offer}");
    let five = header(ssrc, VP8, 5);
    third.writer().write_rtp(&five, &[3; 50]).unwrap();
    eventually_async(|| client.sink.rtp_of(ssrc).len() == 5, WAIT, TICK).await;
    assert_eq!(third.bound_and_unbound(), (1, 0));

    // A sender whose track was removed takes no other, and neither does one of a connection
    // that was closed.
    server.pc.remove_track(&sender).unwrap();
    eventually_async(|| third.bound_and_unbound() == (1, 1), WAIT, TICK).await;
    assert_eq!(
        sender.replace_track(Some(second.clone())),
        Err(Error::Other("Sender has already been stopped".to_string()))
    );
    server.pc.close().unwrap();
    assert_eq!(sender.replace_track(None), Err(Error::ConnectionClosed));
    assert_eq!(second.bound_and_unbound(), (1, 1));
    client.pc.close().unwrap();
}

/// A transceiver that is stopped, as LiveKit stops one it kept for a track and could not
/// use: the track of its sender is unbound, it sends no more, and the next negotiation says
/// so. A track of its kind gets another transceiver from then on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_transceiver_stop() {
    let server = Peer::server();
    let client = Peer::client();
    let video = TestTrack::new("video", RtpCodecType::Video);
    let audio = TestTrack::new("audio", RtpCodecType::Audio);
    server.pc.add_track(video.clone()).unwrap();
    let audio_sender = server.pc.add_track(audio.clone()).unwrap();
    negotiate(&server.pc, &client.pc);
    connected(&server, &client).await;
    eventually_async(|| video.is_bound() && audio.is_bound(), WAIT, TICK).await;
    let (video_ssrc, audio_ssrc) = (video.context().ssrc(), audio.context().ssrc());
    let audio_writer = audio.writer();
    video
        .writer()
        .write_rtp(&header(video_ssrc, VP8, 1), &[1; 30])
        .unwrap();
    audio_writer
        .write_rtp(&header(audio_ssrc, OPUS, 1), &[2; 30])
        .unwrap();
    eventually_async(
        || client.track_of(video_ssrc).is_some() && client.track_of(audio_ssrc).is_some(),
        WAIT,
        TICK,
    )
    .await;

    // The audio transceiver is stopped, through the seam.
    let transceiver = server.pc.get_transceivers()[1].clone();
    assert_eq!(transceiver.kind(), RtpCodecType::Audio);
    let seam: Arc<dyn crate::webrtc::RtpTransceiver> = transceiver.clone();
    seam.stop().unwrap();
    // At once: it has no direction any more, and the track's writer writes no more.
    assert_eq!(transceiver.direction(), RtpTransceiverDirection::Inactive);
    assert_eq!(
        audio_writer.write_rtp(&header(audio_ssrc, OPUS, 2), &[2; 30]),
        Err(Error::ClosedPipe)
    );
    // From the connection's task: the track is unbound, and the sink is told.
    eventually_async(|| audio.bound_and_unbound() == (1, 1), WAIT, TICK).await;
    eventually_async(
        || {
            matches!(
                server.sink.local_calls().last(),
                Some(SinkCall::UnbindLocal(info)) if info.ssrc == audio_ssrc
            )
        },
        WAIT,
        TICK,
    )
    .await;
    // Stopping it again is nothing, and its sender takes no other track.
    seam.stop().unwrap();
    assert!(
        audio_sender
            .replace_track(Some(TestTrack::new("other", RtpCodecType::Audio)))
            .is_err()
    );
    assert_eq!(audio.bound_and_unbound(), (1, 1));
    // The other transceiver is as it was.
    assert_eq!(video.bound_and_unbound(), (1, 0));
    video
        .writer()
        .write_rtp(&header(video_ssrc, VP8, 2), &[1; 30])
        .unwrap();
    eventually_async(|| client.sink.rtp_of(video_ssrc).len() == 2, WAIT, TICK).await;

    // The next negotiation tells the other side: the media section is still there, inactive.
    negotiate(&server.pc, &client.pc);
    let offer = server.pc.current_local_description().unwrap().sdp;
    let audio_section = offer.split("m=").find(|m| m.starts_with("audio")).unwrap();
    assert!(audio_section.contains("a=inactive"), "{offer}");
    assert!(audio_section.contains("a=mid:1"), "{offer}");
    assert_eq!(
        client.pc.get_transceivers()[1].current_direction(),
        RtpTransceiverDirection::Inactive
    );

    // A track of that kind gets a transceiver of its own now, and a stream of its own.
    let again = TestTrack::new("again", RtpCodecType::Audio);
    server.pc.add_track(again.clone()).unwrap();
    assert_eq!(server.pc.get_transceivers().len(), 3);
    negotiate(&server.pc, &client.pc);
    eventually_async(|| again.is_bound(), WAIT, TICK).await;
    assert_ne!(again.context().ssrc(), audio_ssrc);
    assert_eq!(server.pc.get_transceivers()[2].mid(), "2");

    // The other side's sink was told that the stream of the stopped transceiver is over.
    eventually_async(
        || {
            client
                .sink
                .position(&SinkCall::UnbindRemote(audio_ssrc))
                .is_some()
        },
        WAIT,
        TICK,
    )
    .await;

    // What a stopped transceiver received is unbound at the sink. (A packet first: `rtc`
    // forgets what the stream of a receiver is at every negotiation and learns it again
    // from the stream's next packet. A transceiver that is stopped in between unbinds
    // nothing.)
    assert!(
        client
            .sink
            .position(&SinkCall::UnbindRemote(video_ssrc))
            .is_none()
    );
    video
        .writer()
        .write_rtp(&header(video_ssrc, VP8, 3), &[1; 30])
        .unwrap();
    eventually_async(|| client.sink.rtp_of(video_ssrc).len() == 3, WAIT, TICK).await;
    client.pc.get_transceivers()[0].stop().unwrap();
    eventually_async(
        || {
            client
                .sink
                .position(&SinkCall::UnbindRemote(video_ssrc))
                .is_some()
        },
        WAIT,
        TICK,
    )
    .await;

    // A transceiver of a connection that was closed is not stopped.
    server.pc.close().unwrap();
    assert_eq!(seam.stop(), Err(Error::ConnectionClosed));
    client.pc.close().unwrap();
}

/// The sink is told of the streams a connection sends, in order with everything else it is
/// told, and that the connection has closed: once, after everything else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sink_is_told_of_local_streams_and_of_the_end() {
    let server = Peer::server();
    let client = Peer::client();
    let video = TestTrack::new("video", RtpCodecType::Video);
    // What the track had been told when the sink was told of its stream.
    let track_then = Arc::new(Mutex::new(Vec::new()));
    *server.sink.on_local.lock() = Some(Box::new({
        let (video, track_then) = (video.clone(), track_then.clone());
        move |bound| track_then.lock().push((bound, video.bound_and_unbound()))
    }));
    server.pc.add_track(video.clone()).unwrap();
    negotiate(&server.pc, &client.pc);
    connected(&server, &client).await;
    eventually_async(|| video.is_bound(), WAIT, TICK).await;
    let (ssrc, rtx_ssrc) = (
        video.context().ssrc(),
        video.context().ssrc_retransmission(),
    );

    // The server's sink: the stream it sends, then RTCP about it from the client.
    eventually_async(|| !server.sink.local_calls().is_empty(), WAIT, TICK).await;
    client.pc.write_rtcp(&pli(ssrc)).unwrap();
    eventually_async(
        || server.sink.position(&SinkCall::Rtcp).is_some(),
        WAIT,
        TICK,
    )
    .await;
    let info = match server.sink.calls.lock().first() {
        Some(SinkCall::BindLocal(info)) => info.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!((info.ssrc, info.ssrc_retransmission), (ssrc, rtx_ssrc));
    assert_eq!(info.payload_type, video.payload_type());

    // The client's sink: the stream it receives before the stream's packets. The server
    // goes on writing while the client closes.
    let writer = video.writer();
    for seq in 0..200 {
        writer
            .write_rtp(&header(ssrc, VP8, seq), &[5; 100])
            .unwrap();
        if seq == 20 {
            eventually_async(|| !client.sink.rtp.lock().is_empty(), WAIT, TICK).await;
            client.pc.close().unwrap();
        }
    }
    eventually_async(
        || client.sink.calls.lock().last() == Some(&SinkCall::Closed),
        WAIT,
        TICK,
    )
    .await;
    assert!(
        client.sink.position(&SinkCall::BindRemote(ssrc)).unwrap()
            < client.sink.position(&SinkCall::Rtp(ssrc)).unwrap()
    );

    // The server closes: its track is unbound, then the sink is told of the stream's end,
    // and last of all that the connection has closed.
    server.pc.close().unwrap();
    eventually_async(
        || server.sink.calls.lock().last() == Some(&SinkCall::Closed),
        WAIT,
        TICK,
    )
    .await;
    {
        let calls = server.sink.calls.lock();
        let rtcp = calls.iter().filter(|c| **c == SinkCall::Rtcp).count();
        assert!(rtcp >= 1);
        let mut expected = vec![SinkCall::BindLocal(info.clone())];
        expected.extend(std::iter::repeat_n(SinkCall::Rtcp, rtcp));
        expected.push(SinkCall::UnbindLocal(info.clone()));
        expected.push(SinkCall::Closed);
        assert_eq!(*calls, expected);
    }
    assert_eq!(
        *track_then.lock(),
        vec![(true, (1, 0)), (false, (1, 1))],
        "the sink is told after the track"
    );

    // Nothing comes after it on either side, and the connections have let go of their sinks.
    tokio::time::sleep(Duration::from_millis(100)).await;
    for peer in [&server, &client] {
        let calls = peer.sink.calls.lock();
        assert_eq!(calls.last(), Some(&SinkCall::Closed));
        assert_eq!(
            calls.iter().filter(|c| **c == SinkCall::Closed).count(),
            1,
            "{calls:?}"
        );
    }
    eventually_async(
        || Arc::strong_count(&server.sink) == 1 && Arc::strong_count(&client.sink) == 1,
        WAIT,
        TICK,
    )
    .await;

    // A connection whose last handle is dropped tells its sink too.
    let node = Node::new(NodeConfig::default()).unwrap();
    let sink = Arc::new(Recorder::default());
    let pc = PeerConnection::new(
        &node,
        Configuration {
            packet_sink: Some(sink.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    drop(pc);
    eventually_async(
        || *sink.calls.lock() == [SinkCall::Closed] && Arc::strong_count(&sink) == 1,
        WAIT,
        TICK,
    )
    .await;
}

/// The codec a payload type stands for, asked of the connection as pion's media engine is
/// asked for a stream nobody announced: the codec as it was negotiated, with the header
/// extensions of its kind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_codec_of_a_payload_type() {
    let server = Peer::server();
    let client = Peer::client();
    let video = TestTrack::new("video", RtpCodecType::Video);
    let audio = TestTrack::new("audio", RtpCodecType::Audio);
    let init = Some(RtpTransceiverInit {
        direction: RtpTransceiverDirection::Sendonly,
    });
    for track in [&video, &audio] {
        client
            .pc
            .add_transceiver_from_track(track.clone(), init)
            .unwrap();
    }
    // Before anything was negotiated there is no telling.
    for peer in [&server, &client] {
        assert_eq!(
            peer.pc.get_rtp_parameters_by_payload_type(VP8).err(),
            Some(Error::CodecNotFound)
        );
    }
    negotiate(&client.pc, &server.pc);
    connected(&client, &server).await;
    eventually_async(|| video.is_bound() && audio.is_bound(), WAIT, TICK).await;

    // (uri, id), in an order of their own: the library keeps them in a map.
    let sorted = |extensions: Vec<crate::webrtc::RtpHeaderExtensionParameter>| {
        let mut extensions: Vec<(String, i32)> =
            extensions.into_iter().map(|e| (e.uri, e.id)).collect();
        extensions.sort();
        extensions
    };

    // The server, which receives: a retransmission codec says which codec it repairs, and
    // comes with the header extensions of video under the ids the sender uses.
    let rtx = server
        .pc
        .get_rtp_parameters_by_payload_type(VP8_RTX)
        .unwrap();
    assert_eq!(rtx.codecs.len(), 1);
    assert_eq!(rtx.codecs[0].payload_type, VP8_RTX);
    assert!(rtx.codecs[0].mime_type.eq_ignore_ascii_case(MIME_TYPE_RTX));
    assert_eq!(rtx.codecs[0].clock_rate, 90000);
    assert_eq!(rtx.codecs[0].sdp_fmtp_line, "apt=96");
    let video_extensions = sorted(video.context().header_extensions());
    assert_eq!(sorted(rtx.header_extensions.clone()), video_extensions);
    assert!(
        video_extensions
            .iter()
            .any(|(uri, _)| uri == sdp::SDES_REPAIR_RTP_STREAM_ID_URI)
    );

    let vp9 = server.pc.get_rtp_parameters_by_payload_type(VP9).unwrap();
    assert!(vp9.codecs[0].mime_type.eq_ignore_ascii_case(MIME_TYPE_VP9));
    assert_eq!(vp9.codecs[0].sdp_fmtp_line, "profile-id=0");

    // An audio codec comes with the header extensions of audio.
    let opus = server.pc.get_rtp_parameters_by_payload_type(OPUS).unwrap();
    assert_eq!(opus.codecs.len(), 1);
    assert!(
        opus.codecs[0]
            .mime_type
            .eq_ignore_ascii_case(MIME_TYPE_OPUS)
    );
    assert_eq!(
        (opus.codecs[0].clock_rate, opus.codecs[0].channels),
        (48000, 2)
    );
    let audio_extensions = sorted(audio.context().header_extensions());
    assert_eq!(sorted(opus.header_extensions.clone()), audio_extensions);
    assert_ne!(audio_extensions, video_extensions);

    // A payload type nothing was negotiated for.
    assert_eq!(
        server.pc.get_rtp_parameters_by_payload_type(77).err(),
        Some(Error::CodecNotFound)
    );

    // The side that only sends says the same.
    let sent = client
        .pc
        .get_rtp_parameters_by_payload_type(VP8_RTX)
        .unwrap();
    assert_eq!(sent.codecs, rtx.codecs);
    assert_eq!(sorted(sent.header_extensions), video_extensions);

    client.pc.close().unwrap();
    server.pc.close().unwrap();
}

fn feedback(r#type: &str, parameter: &str) -> RtcpFeedback {
    RtcpFeedback {
        r#type: r#type.to_string(),
        parameter: parameter.to_string(),
    }
}

fn opus(payload_type: u8, fmtp: &str, rtcp_feedback: Vec<RtcpFeedback>) -> RtpCodecParameters {
    RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MIME_TYPE_OPUS.to_string(),
            clock_rate: 48000,
            channels: 2,
            sdp_fmtp_line: fmtp.to_string(),
            rtcp_feedback,
        },
        payload_type,
    }
}

/// A connection with nothing but Opus, as the codec is given, on a node of its own. It is
/// never connected: it makes descriptions.
fn opus_connection(codec: &RtpCodecParameters) -> PeerConnection {
    let mut media_engine = MediaEngine::default();
    register_codec(&mut media_engine, codec, RtpCodecType::Audio).unwrap();
    PeerConnection::new(
        &Node::new(NodeConfig::default()).unwrap(),
        Configuration {
            media_engine,
            ..Default::default()
        },
    )
    .unwrap()
}

/// The lines of the audio section of a description that say which codecs it has, and its
/// direction: what the Go test the expected lines come from printed.
fn audio_codec_lines(sdp: &str) -> Vec<String> {
    codec_lines(sdp, "audio")
}

/// The same for the section of another kind.
fn codec_lines(sdp: &str, media: &str) -> Vec<String> {
    let section = format!("m={media}");
    let mut lines = Vec::new();
    let mut in_section = false;
    for line in sdp.lines().map(str::trim_end) {
        if line.starts_with("m=") {
            in_section = line.starts_with(&section);
        }
        let wanted = line.starts_with(&section)
            || line.starts_with("a=rtpmap:")
            || line.starts_with("a=fmtp:")
            || line.starts_with("a=rtcp-fb:")
            || matches!(
                line,
                "a=sendonly" | "a=recvonly" | "a=sendrecv" | "a=inactive"
            );
        if in_section && wanted {
            lines.push(line.to_string());
        }
    }
    lines
}

/// New for musivum. A sender's codecs, as pion's `RTPSender.GetParameters` gives them: the
/// transceiver's codecs, which are the media engine's of its kind until codec preferences are
/// set (`rtc` gives none until then), also for a transceiver that only sends and so has no
/// receiver to ask. Once preferences are set, the preferred codecs, with the media engine's
/// payload type where the preference has none and of the preference's feedback what the
/// media engine has for the codec (pion's `RTPTransceiver.getCodecs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sender_codecs_before_preferences() {
    let server = Peer::server();
    let video = server
        .pc
        .add_transceiver_from_track(
            TestTrack::new("video", RtpCodecType::Video),
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Sendonly,
            }),
        )
        .unwrap();
    let audio = server
        .pc
        .add_track(TestTrack::new("audio", RtpCodecType::Audio))
        .unwrap();

    let payload_types = |sender: &RtpSender| -> Vec<u8> {
        sender
            .get_parameters()
            .codecs
            .iter()
            .map(|c| c.payload_type)
            .collect()
    };
    let video_sender = video.sender().unwrap();
    assert_eq!(
        payload_types(&video_sender),
        vec![VP8, VP8_RTX, VP9, VP9_RTX]
    );
    let codecs = video_sender.get_parameters().rtp_parameters.codecs;
    assert_eq!(codecs[1].sdp_fmtp_line, "apt=96");
    assert_eq!(codecs[0].rtcp_feedback.len(), 3);
    assert_eq!(payload_types(&audio), vec![OPUS]);
    assert_eq!(
        audio.get_parameters().codecs[0].sdp_fmtp_line,
        "minptime=10;useinbandfec=1"
    );
    // What the receiver of a transceiver that sends and receives has, the sender has too.
    let receiver = server.pc.get_transceivers()[1].receiver().unwrap();
    assert_eq!(
        audio.get_parameters().codecs,
        receiver.get_parameters().codecs
    );

    // A preference without a payload type, with feedback of which the media engine has one.
    let mut vp8 = video_sender.get_parameters().codecs[0].clone();
    vp8.payload_type = 0;
    vp8.rtp_codec_capability.rtcp_feedback =
        vec![feedback("goog-remb", ""), feedback("nack", "pli")];
    video.set_codec_preferences(vec![vp8]).unwrap();
    let codecs = video_sender.get_parameters().rtp_parameters.codecs;
    assert_eq!(codecs[0].payload_type, VP8);
    assert!(codecs[0].mime_type.eq_ignore_ascii_case(MIME_TYPE_VP8));
    assert_eq!(codecs[0].rtcp_feedback, vec![feedback("nack", "pli")]);
    assert!(
        !codecs
            .iter()
            .any(|c| c.mime_type.eq_ignore_ascii_case(MIME_TYPE_VP9))
    );

    server.pc.close().unwrap();
}

/// New for musivum. A transceiver made from a kind to send has a sender, as pion's has.
/// pion gives it a track of its own that writes nothing; here it has none, as LiveKit leaves
/// it after `SetSender(sender, nil)`, and nothing is bound until it is given one. Its codecs
/// can be asked, and a track can take the place of none without a negotiation of its own:
/// it is bound when the transceiver has been negotiated, with the transceiver's stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sender_of_a_transceiver_from_kind() {
    let server = Peer::server();
    let client = Peer::client();
    let transceiver = server
        .pc
        .add_transceiver_from_kind(
            RtpCodecType::Audio,
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Sendonly,
            }),
        )
        .unwrap();
    let sender = transceiver
        .sender()
        .expect("a transceiver that sends has a sender");
    assert!(sender.track().is_none());
    let seam: Arc<dyn crate::webrtc::RtpTransceiver> = transceiver.clone();
    assert!(seam.sender().is_some());
    assert_eq!(server.pc.get_senders().len(), 1);
    let codecs = sender.get_parameters().rtp_parameters.codecs;
    assert_eq!(codecs.len(), 1);
    assert_eq!(codecs[0].payload_type, OPUS);

    let track = TestTrack::new("audio", RtpCodecType::Audio);
    sender.replace_track(Some(track.clone())).unwrap();
    assert!(sender.track().is_some_and(|t| t.id() == "audio"));
    negotiate(&server.pc, &client.pc);
    let offer = server.pc.current_local_description().unwrap().sdp;
    assert!(offer.contains("a=msid:stream audio"), "{offer}");
    connected(&server, &client).await;
    eventually_async(|| track.is_bound(), WAIT, TICK).await;
    assert_eq!(track.bound_and_unbound(), (1, 0));
    assert_eq!(track.payload_type(), OPUS);
    let ssrc = track.context().ssrc();
    assert_ne!(ssrc, 0);
    assert_eq!(sender.get_parameters().encodings[0].ssrc, ssrc);
    assert!(offer.contains(&format!("a=ssrc:{ssrc} ")), "{offer}");

    track
        .writer()
        .write_rtp(&header(ssrc, OPUS, 1), &[1; 20])
        .unwrap();
    eventually_async(|| client.sink.rtp_of(ssrc).len() == 1, WAIT, TICK).await;

    server.pc.close().unwrap();
    client.pc.close().unwrap();
}

/// New for musivum. What codec preferences say reaches the description, as with pion: the
/// fmtp line of the preferred codec (LiveKit adds `sprop-stereo=1` to Opus's), its feedback
/// (LiveKit takes NACK away) and its payload type (LiveKit's answer echoes the one the offer
/// gave). The expected lines are what pion at the pin writes for the same, in an offer and
/// in an answer (a Go test laid over `pkg/rtc` of the reference).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_codec_preferences_reach_the_description() {
    let fmtp = "minptime=10;useinbandfec=1";
    let stereo = "minptime=10;useinbandfec=1;sprop-stereo=1";
    // As LiveKit registers Opus, with NACK on.
    let server_opus = opus(
        OPUS,
        fmtp,
        vec![feedback("nack", ""), feedback("transport-cc", "")],
    );
    let send_only = Some(RtpTransceiverInit {
        direction: RtpTransceiverDirection::Sendonly,
    });

    // The offer: stereo on, NACK off.
    let server = opus_connection(&server_opus);
    let transceiver = server
        .add_transceiver_from_track(TestTrack::new("audio", RtpCodecType::Audio), send_only)
        .unwrap();
    transceiver
        .set_codec_preferences(vec![opus(OPUS, stereo, vec![feedback("transport-cc", "")])])
        .unwrap();
    let offer = server.create_offer(None).unwrap();
    assert_eq!(
        audio_codec_lines(&offer.sdp),
        vec![
            "m=audio 9 UDP/TLS/RTP/SAVPF 111",
            "a=rtpmap:111 opus/48000/2",
            "a=fmtp:111 minptime=10;useinbandfec=1;sprop-stereo=1",
            "a=rtcp-fb:111 transport-cc",
            "a=sendonly",
        ],
        "{}",
        offer.sdp
    );
    server.close().unwrap();

    // The answer to a client that offers Opus on 109: the payload type echoed, stereo on,
    // NACK off.
    let client = opus_connection(&opus(
        109,
        fmtp,
        vec![feedback("transport-cc", ""), feedback("nack", "")],
    ));
    client
        .add_transceiver_from_kind(
            RtpCodecType::Audio,
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Recvonly,
            }),
        )
        .unwrap();
    let client_offer = client.create_offer(None).unwrap();
    let server = opus_connection(&server_opus);
    let transceiver = server
        .add_transceiver_from_track(TestTrack::new("audio", RtpCodecType::Audio), send_only)
        .unwrap();
    server.set_remote_description(client_offer).unwrap();
    transceiver
        .set_codec_preferences(vec![opus(109, stereo, vec![feedback("transport-cc", "")])])
        .unwrap();
    let answer = server.create_answer().unwrap();
    assert_eq!(
        audio_codec_lines(&answer.sdp),
        vec![
            "m=audio 9 UDP/TLS/RTP/SAVPF 109",
            "a=rtpmap:109 opus/48000/2",
            "a=fmtp:109 minptime=10;useinbandfec=1;sprop-stereo=1",
            "a=rtcp-fb:109 transport-cc",
            "a=sendonly",
        ],
        "{}",
        answer.sdp
    );
    server.close().unwrap();
    client.close().unwrap();
}

/// What LiveKit's `configureSenderAudio` does with stereo on and NACK off: the sender's
/// codecs, Opus with `sprop-stereo=1`, without NACK and with the payload type an offer gave
/// it, set as the transceiver's codec preferences.
fn configure_sender_audio(transceiver: &RtpTransceiver, offer_payload_type: Option<u8>) {
    let sender = transceiver.sender().expect("a sender");
    let codecs = sender
        .get_parameters()
        .rtp_parameters
        .codecs
        .into_iter()
        .map(|mut c| {
            if c.mime_type.eq_ignore_ascii_case(MIME_TYPE_OPUS) {
                c.sdp_fmtp_line =
                    c.sdp_fmtp_line.replace(";sprop-stereo=1", "") + ";sprop-stereo=1";
                if let Some(i) = c.rtcp_feedback.iter().position(|fb| fb.r#type == "nack") {
                    c.rtcp_feedback.remove(i);
                }
                if let Some(payload_type) = offer_payload_type {
                    c.payload_type = payload_type;
                }
            }
            c
        })
        .collect();
    transceiver.set_codec_preferences(codecs).unwrap();
}

/// New for musivum. Codec preferences outlive the other side's answer, as with pion: a
/// sender configured as LiveKit configures one (stereo on, NACK off) is offered, the other
/// side answers without `sprop-stereo` (as a browser does), and the next offer still says
/// what the preferences say. `rtc` took the answer's codecs as the transceiver's preferences.
/// The expected lines are what pion at the pin writes for the same steps (a Go test laid over
/// `pkg/rtc` of the reference, which configures with LiveKit's own `configureSenderAudio`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_codec_preferences_outlive_an_answer() {
    let fmtp = "minptime=10;useinbandfec=1";
    let stereo = "minptime=10;useinbandfec=1;sprop-stereo=1";
    let server = opus_connection(&opus(
        OPUS,
        fmtp,
        vec![feedback("nack", ""), feedback("transport-cc", "")],
    ));
    let transceiver = server
        .add_transceiver_from_track(
            TestTrack::new("audio", RtpCodecType::Audio),
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Sendonly,
            }),
        )
        .unwrap();
    configure_sender_audio(&transceiver, None);
    let offer = server.create_offer(None).unwrap();
    server.set_local_description(offer.clone()).unwrap();
    let offered = vec![
        "m=audio 9 UDP/TLS/RTP/SAVPF 111",
        "a=rtpmap:111 opus/48000/2",
        "a=fmtp:111 minptime=10;useinbandfec=1;sprop-stereo=1",
        "a=rtcp-fb:111 transport-cc",
        "a=sendonly",
    ];
    assert_eq!(audio_codec_lines(&offer.sdp), offered, "{}", offer.sdp);

    let client = opus_connection(&opus(
        OPUS,
        fmtp,
        vec![feedback("transport-cc", ""), feedback("nack", "")],
    ));
    client.set_remote_description(offer).unwrap();
    let mut answer = client.create_answer().unwrap();
    client.set_local_description(answer.clone()).unwrap();
    answer.sdp = answer.sdp.replace(";sprop-stereo=1", "");
    assert_eq!(
        audio_codec_lines(&answer.sdp),
        vec![
            "m=audio 9 UDP/TLS/RTP/SAVPF 111",
            "a=rtpmap:111 opus/48000/2",
            "a=fmtp:111 minptime=10;useinbandfec=1",
            "a=rtcp-fb:111 transport-cc",
            "a=recvonly",
        ],
        "{}",
        answer.sdp
    );
    server.set_remote_description(answer).unwrap();

    let reoffer = server.create_offer(None).unwrap();
    assert_eq!(audio_codec_lines(&reoffer.sdp), offered, "{}", reoffer.sdp);
    assert_eq!(
        transceiver
            .sender()
            .unwrap()
            .get_parameters()
            .rtp_parameters
            .codecs,
        vec![opus(OPUS, stereo, vec![feedback("transport-cc", "")])]
    );
    server.close().unwrap();
    client.close().unwrap();
}

/// New for musivum. Codec preferences outlive the other side's next offer, as with pion: on a
/// single connection the client offers, the server answers with a sender configured as LiveKit
/// configures one (stereo on, NACK off, the offer's payload type), and when the client offers
/// again what it offered first (as a browser does: its own codecs, NACK and no stereo), the
/// answer is what it was. `rtc` took the offer's codecs as the transceiver's preferences, and
/// its answer lost `sprop-stereo` and carried NACK. The expected lines are pion's at the pin
/// for the same steps (the Go test of the test above).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_codec_preferences_outlive_an_offer() {
    let fmtp = "minptime=10;useinbandfec=1";
    let stereo = "minptime=10;useinbandfec=1;sprop-stereo=1";
    let client = opus_connection(&opus(
        109,
        fmtp,
        vec![feedback("transport-cc", ""), feedback("nack", "")],
    ));
    client
        .add_transceiver_from_kind(
            RtpCodecType::Audio,
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Recvonly,
            }),
        )
        .unwrap();
    let client_offer = client.create_offer(None).unwrap();
    client.set_local_description(client_offer.clone()).unwrap();
    assert_eq!(
        audio_codec_lines(&client_offer.sdp),
        vec![
            "m=audio 9 UDP/TLS/RTP/SAVPF 109",
            "a=rtpmap:109 opus/48000/2",
            "a=fmtp:109 minptime=10;useinbandfec=1",
            "a=rtcp-fb:109 transport-cc",
            "a=rtcp-fb:109 nack",
            "a=recvonly",
        ],
        "{}",
        client_offer.sdp
    );

    let server = opus_connection(&opus(
        OPUS,
        fmtp,
        vec![feedback("nack", ""), feedback("transport-cc", "")],
    ));
    let transceiver = server
        .add_transceiver_from_track(
            TestTrack::new("audio", RtpCodecType::Audio),
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Sendonly,
            }),
        )
        .unwrap();
    server.set_remote_description(client_offer.clone()).unwrap();
    configure_sender_audio(&transceiver, Some(109));
    let answer = server.create_answer().unwrap();
    server.set_local_description(answer.clone()).unwrap();
    let answered = vec![
        "m=audio 9 UDP/TLS/RTP/SAVPF 109",
        "a=rtpmap:109 opus/48000/2",
        "a=fmtp:109 minptime=10;useinbandfec=1;sprop-stereo=1",
        "a=rtcp-fb:109 transport-cc",
        "a=sendonly",
    ];
    assert_eq!(audio_codec_lines(&answer.sdp), answered, "{}", answer.sdp);
    client.set_remote_description(answer).unwrap();

    server.set_remote_description(client_offer).unwrap();
    let answer = server.create_answer().unwrap();
    server.set_local_description(answer.clone()).unwrap();
    assert_eq!(audio_codec_lines(&answer.sdp), answered, "{}", answer.sdp);
    assert_eq!(
        transceiver
            .sender()
            .unwrap()
            .get_parameters()
            .rtp_parameters
            .codecs,
        vec![opus(109, stereo, vec![feedback("transport-cc", "")])]
    );
    server.close().unwrap();
    client.close().unwrap();
}

/// New for musivum. Media arrives with its codec after a renegotiation that kept the codec
/// preferences, both ways on one connection as LiveKit's single connection has it. `rtc` maps
/// an incoming payload type through codecs the receiver keeps, which it used to take from the
/// preferences it set from every remote description, and which are now what the latest remote
/// description said for that media section. The client sends video on a transceiver its offer
/// made at the server, whose preferences the server sets (as LiveKit's
/// `configureReceiverCodecs` does); the server sends audio from a sender configured as
/// LiveKit configures one to a client transceiver that has no preferences. The client's
/// re-offer has no `sprop-stereo` (a browser's has none): the audio that arrives after it is
/// still the stereo Opus the server's answers give, with the payload type of its answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_media_after_a_renegotiation_has_its_codec() {
    let server = Peer::server();
    let client = Peer::client();
    let send_only = Some(RtpTransceiverInit {
        direction: RtpTransceiverDirection::Sendonly,
    });
    let audio = TestTrack::new("audio", RtpCodecType::Audio);
    let audio_transceiver = server
        .pc
        .add_transceiver_from_track(audio.clone(), send_only)
        .unwrap();
    let video = TestTrack::new("video", RtpCodecType::Video);
    client
        .pc
        .add_transceiver_from_track(video.clone(), send_only)
        .unwrap();
    client
        .pc
        .add_transceiver_from_kind(
            RtpCodecType::Audio,
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Recvonly,
            }),
        )
        .unwrap();

    // As `negotiate` carries them over: a local description once set has the candidates.
    let offer = client.pc.create_offer(None).unwrap();
    client.pc.set_local_description(offer).unwrap();
    server
        .pc
        .set_remote_description(client.pc.local_description().unwrap())
        .unwrap();
    configure_sender_audio(&audio_transceiver, Some(OPUS));
    let video_transceiver = server
        .pc
        .get_transceivers()
        .into_iter()
        .find(|t| t.kind() == RtpCodecType::Video)
        .expect("the offer made a transceiver for the video");
    let vp8 = video_transceiver
        .receiver()
        .unwrap()
        .get_parameters()
        .codecs
        .into_iter()
        .filter(|c| c.payload_type == VP8 || c.payload_type == VP8_RTX)
        .collect();
    video_transceiver.set_codec_preferences(vp8).unwrap();
    let answer = server.pc.create_answer().unwrap();
    server.pc.set_local_description(answer).unwrap();
    client
        .pc
        .set_remote_description(server.pc.local_description().unwrap())
        .unwrap();
    connected(&server, &client).await;
    eventually_async(|| audio.is_bound() && video.is_bound(), WAIT, TICK).await;
    let check =
        |round: usize, (audio_codec, video_codec): (RtpCodecParameters, RtpCodecParameters)| {
            assert_eq!(audio_codec.payload_type, OPUS, "{round}");
            assert!(
                audio_codec.mime_type.eq_ignore_ascii_case(MIME_TYPE_OPUS),
                "{round}"
            );
            assert_eq!(
                audio_codec.sdp_fmtp_line, "minptime=10;useinbandfec=1;sprop-stereo=1",
                "{round}"
            );
            assert_eq!(video_codec.payload_type, VP8, "{round}");
            assert!(
                video_codec.mime_type.eq_ignore_ascii_case(MIME_TYPE_VP8),
                "{round}"
            );
        };
    let mut sequence_number = 0;
    let streams = ((&server, &*audio), (&client, &*video));
    check(
        1,
        send_until_announced(1, &mut sequence_number, streams.0, streams.1).await,
    );

    // The client offers again, without stereo; the server's answer keeps its preferences.
    let reoffer = client.pc.create_offer(None).unwrap();
    client.pc.set_local_description(reoffer).unwrap();
    let mut reoffer = client.pc.local_description().unwrap();
    reoffer.sdp = reoffer.sdp.replace(";sprop-stereo=1", "");
    server.pc.set_remote_description(reoffer).unwrap();
    let answer = server.pc.create_answer().unwrap();
    server.pc.set_local_description(answer).unwrap();
    client
        .pc
        .set_remote_description(server.pc.local_description().unwrap())
        .unwrap();

    // `rtc` announces each stream again at its first packet after a negotiation.
    check(
        2,
        send_until_announced(2, &mut sequence_number, streams.0, streams.1).await,
    );

    client.pc.close().unwrap();
    server.pc.close().unwrap();
}

/// Writes the audio track's packets from one peer and the video track's from the other until
/// each stream has been announced `n` times at the other end; the codecs of the `n`-th
/// announcements, audio and video.
async fn send_until_announced(
    n: usize,
    sequence_number: &mut u16,
    (audio_peer, audio): (&Peer, &TestTrack),
    (video_peer, video): (&Peer, &TestTrack),
) -> (RtpCodecParameters, RtpCodecParameters) {
    let (audio_ssrc, video_ssrc) = (audio.context().ssrc(), video.context().ssrc());
    let announced = |peer: &Peer, ssrc: u32| {
        let tracks = peer.tracks.lock();
        let mine: Vec<_> = tracks.iter().filter(|(t, _)| t.ssrc() == ssrc).collect();
        (mine.len() >= n).then(|| mine[n - 1].0.codec())
    };
    for _ in 0..500 {
        *sequence_number = sequence_number.wrapping_add(1);
        audio
            .writer()
            .write_rtp(
                &header(audio_ssrc, audio.payload_type(), *sequence_number),
                &[1; 20],
            )
            .unwrap();
        video
            .writer()
            .write_rtp(
                &header(video_ssrc, video.payload_type(), *sequence_number),
                &[2; 20],
            )
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let (Some(a), Some(v)) = (
            announced(video_peer, audio_ssrc),
            announced(audio_peer, video_ssrc),
        ) {
            return (a, v);
        }
    }
    panic!(
        "the streams were not announced {n} times: {:?} at the audio's receiver, {:?} at the \
         video's",
        video_peer.sink.calls.lock(),
        audio_peer.sink.calls.lock()
    );
}

/// New for musivum. Codec preferences that leave repair codecs out leave them out of the
/// description and of the sender's codecs, as with pion: an RTX whose codec is not preferred
/// is dropped (pion's `filterUnattachedRTX`), and nothing is added: not the RTX of a
/// preferred codec, not FEC, not RED. `rtc` added the media engine's back. The expected lines
/// are pion's at the pin for the same (the Go test of the tests above).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_codec_preferences_leave_repair_codecs_out() {
    let mut media_engine = media_engine();
    for (mime_type, clock_rate, channels, fmtp, payload_type, kind) in [
        (
            "video/flexfec-03",
            90000,
            0,
            "repair-window=10000000",
            100,
            RtpCodecType::Video,
        ),
        ("audio/red", 48000, 2, "111/111", 63, RtpCodecType::Audio),
    ] {
        register_codec(
            &mut media_engine,
            &RtpCodecParameters {
                rtp_codec_capability: RtpCodecCapability {
                    mime_type: mime_type.to_string(),
                    clock_rate,
                    channels,
                    sdp_fmtp_line: fmtp.to_string(),
                    rtcp_feedback: vec![],
                },
                payload_type,
            },
            kind,
        )
        .unwrap();
    }
    let pc = PeerConnection::new(
        &Node::new(NodeConfig::default()).unwrap(),
        Configuration {
            media_engine,
            ..Default::default()
        },
    )
    .unwrap();
    let send_only = Some(RtpTransceiverInit {
        direction: RtpTransceiverDirection::Sendonly,
    });
    let video = pc
        .add_transceiver_from_track(TestTrack::new("video", RtpCodecType::Video), send_only)
        .unwrap();
    let audio = pc
        .add_transceiver_from_track(TestTrack::new("audio", RtpCodecType::Audio), send_only)
        .unwrap();
    let payload_types = |transceiver: &RtpTransceiver| -> Vec<u8> {
        transceiver
            .sender()
            .unwrap()
            .get_parameters()
            .codecs
            .iter()
            .map(|c| c.payload_type)
            .collect()
    };
    assert_eq!(payload_types(&video), vec![VP8, VP8_RTX, VP9, VP9_RTX, 100]);
    assert_eq!(payload_types(&audio), vec![OPUS, 63]);
    let keep = |transceiver: &RtpTransceiver, kept: &[u8]| {
        let codecs = transceiver
            .sender()
            .unwrap()
            .get_parameters()
            .rtp_parameters
            .codecs
            .into_iter()
            .filter(|c| kept.contains(&c.payload_type))
            .collect();
        transceiver.set_codec_preferences(codecs).unwrap();
    };
    keep(&video, &[VP9, VP8_RTX]);
    keep(&audio, &[OPUS]);

    let offer = pc.create_offer(None).unwrap();
    assert_eq!(
        codec_lines(&offer.sdp, "video"),
        vec![
            "m=video 9 UDP/TLS/RTP/SAVPF 98",
            "a=rtpmap:98 VP9/90000",
            "a=fmtp:98 profile-id=0",
            "a=sendonly",
        ],
        "{}",
        offer.sdp
    );
    assert_eq!(
        audio_codec_lines(&offer.sdp),
        vec![
            "m=audio 9 UDP/TLS/RTP/SAVPF 111",
            "a=rtpmap:111 opus/48000/2",
            "a=fmtp:111 minptime=10;useinbandfec=1",
            "a=sendonly",
        ],
        "{}",
        offer.sdp
    );
    assert_eq!(payload_types(&video), vec![VP9]);
    assert_eq!(payload_types(&audio), vec![OPUS]);
    pc.close().unwrap();
}

#[path = "tcp_test.rs"]
mod tcp_test;
