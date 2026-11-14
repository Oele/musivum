// New for musivum. The first half feeds a sink by hand, with a buffer factory and no
// connection. The second half runs two of musivum's peer connections against each other in
// one process, over the loopback interface on ports the system chooses, with helpers after
// those of `webrtc/peerconnection/peerconnection_test.rs`; its simulcast case follows the
// first case of upstream's `test/rtx_pairing_integration_test.go`, which tests the same
// through LiveKit's transport (the two other cases there are a migrated publisher, for which
// there is nothing here: see `unhandlesimulcast.rs`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;

use super::*;
use crate::go::time;
use crate::sfu::buffer::FactoryOfBufferFactory;
use crate::testutils::eventually_async;
use crate::webrtc::peerconnection::{
    Configuration, DtlsRole, MediaEngine, Node, NodeConfig, PeerConnection, PeerConnectionState,
    RtpReceiver, RtpTransceiverDirection, RtpTransceiverInit, SdpType, SessionDescription,
    SettingEngine, TrackRemote, register_codec, register_header_extension,
};
use crate::webrtc::rtcp::extended_report::{ExtendedReport, ReceiverReferenceTimeReportBlock};
use crate::webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use crate::webrtc::rtcp::receiver_report::ReceiverReport;
use crate::webrtc::rtcp::reception_report::ReceptionReport;
use crate::webrtc::rtcp::sender_report::SenderReport;
use crate::webrtc::rtp;
use crate::webrtc::{
    self, MIME_TYPE_VP8, RtcpFeedback, RtpCodecCapability, RtpCodecParameters, RtpCodecType,
    RtpHeaderExtensionParameter, RtpParameters, TrackLocal, TrackLocalContext,
};

const WAIT: Duration = Duration::from_secs(10);
const TICK: Duration = Duration::from_millis(5);

const VP8: u8 = 96;
const VP8_RTX: u8 = 97;

// The header extensions of the scripted streams.
const MID: u8 = 1;
const RID: u8 = 2;
const RSID: u8 = 3;
const TWCC: u8 = 4;

// The start of a VP8 key frame, as upstream's test has it.
const VP8_PAYLOAD: [u8; 10] = [0x10, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x40, 0x01, 0xf0, 0x00];

fn vp8_capability() -> RtpCodecCapability {
    RtpCodecCapability {
        mime_type: MIME_TYPE_VP8.to_string(),
        clock_rate: 90000,
        rtcp_feedback: ["nack", "transport-cc"]
            .into_iter()
            .map(|r#type| RtcpFeedback {
                r#type: r#type.to_string(),
                parameter: String::new(),
            })
            .collect(),
        ..Default::default()
    }
}

fn codecs() -> Vec<RtpCodecParameters> {
    vec![
        RtpCodecParameters {
            rtp_codec_capability: vp8_capability(),
            payload_type: VP8,
        },
        RtpCodecParameters {
            rtp_codec_capability: RtpCodecCapability {
                mime_type: MIME_TYPE_RTX.to_string(),
                clock_rate: 90000,
                sdp_fmtp_line: format!("apt={VP8}"),
                ..Default::default()
            },
            payload_type: VP8_RTX,
        },
    ]
}

const EXTENSIONS: [(&str, u8); 4] = [
    (sdp::SDES_MID_URI, MID),
    (sdp::SDES_RTP_STREAM_ID_URI, RID),
    (SDES_REPAIR_RTP_STREAM_ID_URI, RSID),
    (sdp::TRANSPORT_CC_URI, TWCC),
];

fn header_extensions() -> Vec<RtpHeaderExtension> {
    EXTENSIONS
        .iter()
        .map(|&(uri, id)| RtpHeaderExtension {
            uri: uri.to_string(),
            id: id as i32,
        })
        .collect()
}

/// What a receiver binds the buffer of a scripted stream with.
fn parameters() -> RtpParameters {
    RtpParameters {
        header_extensions: EXTENSIONS
            .iter()
            .map(|&(uri, id)| RtpHeaderExtensionParameter {
                uri: uri.to_string(),
                id: id as i32,
            })
            .collect(),
        codecs: codecs(),
    }
}

/// A VP8 stream as a connection announces it; `rtx_ssrc` is the retransmission stream the
/// description named for it, or 0.
fn vp8_stream(ssrc: u32, rtx_ssrc: u32) -> StreamInfo {
    StreamInfo {
        ssrc,
        ssrc_retransmission: rtx_ssrc,
        payload_type: VP8,
        payload_type_retransmission: if rtx_ssrc != 0 { VP8_RTX } else { 0 },
        rtp_header_extensions: header_extensions(),
        mime_type: MIME_TYPE_VP8.to_string(),
        clock_rate: 90000,
        rtcp_feedback: vp8_capability().rtcp_feedback,
        ..Default::default()
    }
}

/// A retransmission stream the description named, as `rtc` announces it: with its own SSRC
/// and payload type, and the codec of the stream it repairs.
fn declared_rtx_stream(ssrc: u32) -> StreamInfo {
    StreamInfo {
        payload_type: VP8_RTX,
        ..vp8_stream(ssrc, 0)
    }
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

fn with_extensions(mut header: rtp::Header, extensions: &[(u8, &str)]) -> rtp::Header {
    for (id, value) in extensions {
        header
            .set_extension(*id, Bytes::copy_from_slice(value.as_bytes()))
            .unwrap();
    }
    header
}

/// The payload of the media packet with this sequence number: every packet has its own.
fn media_payload(sequence_number: u16) -> Vec<u8> {
    let mut payload = VP8_PAYLOAD.to_vec();
    payload.extend_from_slice(&sequence_number.to_be_bytes());
    payload
}

/// The payload of the retransmission of that packet (RFC 4588): the original sequence number
/// in front.
fn rtx_payload(original_sequence_number: u16) -> Vec<u8> {
    let mut payload = original_sequence_number.to_be_bytes().to_vec();
    payload.extend_from_slice(&media_payload(original_sequence_number));
    payload
}

fn marshalled(header: &rtp::Header, payload: &[u8]) -> Bytes {
    let packet = rtp::Packet {
        header: header.clone(),
        payload: Bytes::copy_from_slice(payload),
        ..Default::default()
    };
    Bytes::from(packet.marshal().unwrap())
}

fn media(ssrc: u32, sequence_number: u16, extensions: &[(u8, &str)]) -> Bytes {
    marshalled(
        &with_extensions(header(ssrc, VP8, sequence_number), extensions),
        &media_payload(sequence_number),
    )
}

/// The header of the retransmission of a packet: a sequence number of the retransmission
/// stream's own, and the time of the packet it carries.
fn rtx_header(ssrc: u32, sequence_number: u16, original: u16) -> rtp::Header {
    rtp::Header {
        timestamp: header(0, 0, original).timestamp,
        ..header(ssrc, VP8_RTX, sequence_number)
    }
}

fn rtx(ssrc: u32, sequence_number: u16, original: u16, extensions: &[(u8, &str)]) -> Bytes {
    marshalled(
        &with_extensions(rtx_header(ssrc, sequence_number, original), extensions),
        &rtx_payload(original),
    )
}

/// Reads packets from a buffer as a receiver does, until it has `count` of them: their
/// sequence numbers, in the order the buffer gave them out.
async fn read(buffer: &Buffer, count: usize) -> Vec<u16> {
    let mut sequence_numbers = Vec::new();
    let mut buf = vec![0u8; 1500];
    while sequence_numbers.len() < count {
        let read = tokio::time::timeout(WAIT, buffer.read_extended_async(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("no packet after {sequence_numbers:?}"));
        if let Some(packet) = read.expect("the buffer is open") {
            assert_eq!(
                &media_payload(packet.packet.sequence_number)[..],
                &packet.packet.payload[..]
            );
            sequence_numbers.push(packet.packet.sequence_number);
        }
    }
    sequence_numbers
}

/// The packets a buffer has for retransmission, by the sequence numbers given: each is there,
/// with the payload it was sent with.
fn assert_has_in_order(buffer: &Buffer, first_ext_sequence_number: u64, sequence_numbers: &[u16]) {
    let mut buf = vec![0u8; 1500];
    for (i, &sequence_number) in sequence_numbers.iter().enumerate() {
        let n = buffer
            .get_packet(&mut buf, first_ext_sequence_number + i as u64)
            .unwrap_or_else(|err| panic!("packet {sequence_number}: {err}"));
        let mut packet = rtp::Packet::default();
        packet
            .unmarshal(&Bytes::copy_from_slice(&buf[..n]))
            .unwrap();
        assert_eq!(sequence_number, packet.sequence_number);
        assert_eq!(&media_payload(sequence_number)[..], &packet.payload[..]);
    }
}

type Found = Arc<Mutex<Vec<StreamInfo>>>;

struct Fixture {
    factory: Arc<buffer::Factory>,
    sink: Arc<BufferFactorySink>,
    // what the sink reported as found
    found: Found,
}

fn fixture_with(
    sim_tracks: HashMap<u32, SimulcastTrackInfo>,
    rtt_from_xr: Option<RttFromXrFactory>,
) -> Fixture {
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();
    let found = Found::default();
    let sink = BufferFactorySink::new(BufferFactorySinkParams {
        buffer_factory: factory.clone(),
        on_stream_found: Some(Box::new({
            let found = found.clone();
            move |info| found.lock().push(info.clone())
        })),
        sim_tracks,
        rtt_from_xr,
        logger: Logger::default(),
    });
    Fixture {
        factory,
        sink,
        found,
    }
}

fn fixture() -> Fixture {
    fixture_with(HashMap::new(), None)
}

impl Fixture {
    fn rtp(&self, packet: Bytes) {
        let ssrc = u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]);
        self.sink.on_rtp(ssrc, packet, time::now());
    }

    fn buffer(&self, ssrc: u32) -> Arc<Buffer> {
        self.factory.get_buffer(ssrc).expect("the buffer exists")
    }

    /// Binds the buffer of a stream as the receiver of its track does, and collects the
    /// pairings the buffer is told of.
    fn bind(&self, ssrc: u32) -> (Arc<Buffer>, Pairs) {
        let buffer = self.buffer(ssrc);
        let pairs = Pairs::default();
        buffer.on_notify_rtx({
            let pairs = pairs.clone();
            move |base, repair, rsid| pairs.lock().push((base, repair, rsid.to_string()))
        });
        buffer.bind(parameters(), &vp8_capability(), 0).unwrap();
        (buffer, pairs)
    }

    fn found_ssrcs(&self) -> Vec<u32> {
        self.found.lock().iter().map(|info| info.ssrc).collect()
    }
}

// (base, repair, rsid) as `Buffer::on_notify_rtx` tells it
type Pairs = Arc<Mutex<Vec<(u32, u32, String)>>>;

/// A retransmission stream the description named: the connection announces the stream with
/// it, and the stream on its own, before the first packet.
#[tokio::test(flavor = "multi_thread")]
async fn test_repair_stream_announced_by_the_description() {
    let f = fixture();

    f.sink.bind_remote_stream(&vp8_stream(1000, 2000));
    f.sink.bind_remote_stream(&declared_rtx_stream(2000));
    assert_eq!(vec![1000, 2000], f.found_ssrcs());
    // both streams have their buffer and their RTCP reader before a packet has come
    for ssrc in [1000, 2000] {
        let (buffer, reader) = f.factory.get_buffer_pair(ssrc);
        assert!(buffer.is_some() && reader.is_some(), "ssrc {ssrc}");
    }

    // Packets that come before the receiver has bound the buffer wait in it.
    f.rtp(media(1000, 1, &[]));
    f.rtp(media(1000, 2, &[]));
    let (buffer, pairs) = f.bind(1000);
    assert_eq!(vec![1, 2], read(&buffer, 2).await);

    // 3 and 4 are lost and come again on the retransmission stream, a padding-only packet
    // between them.
    f.rtp(media(1000, 5, &[]));
    f.rtp(media(1000, 6, &[]));
    f.rtp(rtx(2000, 1, 4, &[]));
    let mut padding = header(2000, VP8_RTX, 2);
    padding.padding = true;
    padding.padding_size = 200;
    f.rtp(marshalled(&padding, &[]));
    f.rtp(rtx(2000, 3, 3, &[]));
    assert_eq!(vec![5, 6, 4, 3], read(&buffer, 4).await);
    assert_has_in_order(
        &buffer,
        ext_sequence_number(&buffer, 1),
        &[1, 2, 3, 4, 5, 6],
    );

    // a pair the description named is not told to the receiver (it has it from the track)
    assert!(pairs.lock().is_empty());
    assert_eq!(vec![1000, 2000], f.found_ssrcs());
    f.sink.close();
}

/// The extended sequence number a buffer has given a sequence number of the stream's first
/// cycle.
fn ext_sequence_number(buffer: &Buffer, sequence_number: u16) -> u64 {
    let mut buf = vec![0u8; 1500];
    // the buffer starts counting at the first sequence number it sees or a cycle above it
    for cycle in 0..2u64 {
        let ext = (cycle << 16) + sequence_number as u64;
        if buffer.get_packet(&mut buf, ext).is_ok() {
            return ext;
        }
    }
    panic!("the buffer does not have packet {sequence_number}");
}

/// A simulcast layer and its retransmission stream, which is seen first: a browser probes
/// for bandwidth with padding on it before it has anything to retransmit. Nothing announces
/// that stream; what it is comes from its packets.
#[tokio::test(flavor = "multi_thread")]
async fn test_repair_stream_seen_before_its_stream() {
    let f = fixture();

    // Padding with the mid and the id of the stream it repairs, on an SSRC nobody knows, and
    // at a time when the connection has not said yet where those extensions are.
    for sequence_number in 1..=3 {
        let mut padding = with_extensions(
            header(2001, VP8_RTX, sequence_number),
            &[(MID, "0"), (RSID, "q")],
        );
        padding.padding = true;
        padding.padding_size = 100;
        f.rtp(marshalled(&padding, &[]));
    }
    let (repair_buffer, repair_reader) = f.factory.get_buffer_pair(2001);
    assert!(repair_buffer.is_some() && repair_reader.is_some());
    assert!(f.found.lock().is_empty());

    // The layer is announced, as `rtc` does it: without a retransmission stream.
    f.sink.bind_remote_stream(&vp8_stream(1001, 0));
    let (buffer, pairs) = f.bind(1001);
    f.rtp(media(1001, 1, &[(MID, "0"), (RID, "q")]));

    // That makes the pair. The receiver is told through the layer's buffer, and the
    // retransmission stream is reported as found: as such, with the layer's header
    // extensions and the payload type of its packets.
    eventually_async(|| !pairs.lock().is_empty(), WAIT, TICK).await;
    assert_eq!(vec![(1001, 2001, "q".to_string())], *pairs.lock());
    eventually_async(|| f.found.lock().len() == 2, WAIT, TICK).await;
    assert_eq!(
        vec![
            vp8_stream(1001, 0),
            StreamInfo {
                ssrc: 2001,
                payload_type: VP8_RTX,
                rtp_header_extensions: header_extensions(),
                mime_type: MIME_TYPE_RTX.to_string(),
                clock_rate: 90000,
                ..Default::default()
            }
        ],
        *f.found.lock()
    );

    // And a retransmission repairs the layer.
    f.rtp(media(1001, 2, &[]));
    f.rtp(media(1001, 4, &[]));
    f.rtp(rtx(2001, 4, 3, &[(MID, "0"), (RSID, "q")]));
    assert_eq!(vec![1, 2, 4, 3], read(&buffer, 4).await);
    assert_has_in_order(&buffer, ext_sequence_number(&buffer, 1), &[1, 2, 3, 4]);
    f.sink.close();
}

/// Three simulcast layers in one media section and a retransmission stream for each, told
/// apart by rid and repaired rid; and a second media section with a layer of the same rid.
#[tokio::test(flavor = "multi_thread")]
async fn test_simulcast_layers_with_rid_and_repaired_rid() {
    let f = fixture();
    // (ssrc, rtx ssrc, mid, rid)
    let layers = [
        (1001, 2001, "0", "q"),
        (1002, 2002, "0", "h"),
        (1003, 2003, "0", "f"),
        (1004, 2004, "1", "q"),
    ];

    let mut bound = Vec::new();
    for (ssrc, _, mid, rid) in layers {
        f.sink.bind_remote_stream(&vp8_stream(ssrc, 0));
        bound.push(f.bind(ssrc));
        f.rtp(media(ssrc, 1, &[(MID, mid), (RID, rid)]));
        f.rtp(media(ssrc, 2, &[(MID, mid), (RID, rid)]));
        f.rtp(media(ssrc, 4, &[]));
    }
    // The retransmission streams, in another order than the layers.
    for (_, rtx_ssrc, mid, rid) in layers.into_iter().rev() {
        f.rtp(rtx(rtx_ssrc, 1, 3, &[(MID, mid), (RSID, rid)]));
    }

    for ((ssrc, rtx_ssrc, _, rid), (buffer, pairs)) in layers.into_iter().zip(&bound) {
        assert_eq!(vec![1, 2, 4, 3], read(buffer, 4).await, "layer {ssrc}");
        assert_eq!(vec![(ssrc, rtx_ssrc, rid.to_string())], *pairs.lock());
    }
    eventually_async(|| f.found.lock().len() == 8, WAIT, TICK).await;
    let found = f.found.lock().clone();
    for (_, rtx_ssrc, _, _) in layers {
        let info = found.iter().find(|info| info.ssrc == rtx_ssrc).unwrap();
        assert_eq!(MIME_TYPE_RTX, info.mime_type);
        assert_eq!(VP8_RTX, info.payload_type);
    }
    f.sink.close();
}

/// What the description of a migrated publisher's streams says is taken over when the sink
/// is made, and holds for buffers that are made later. (That such streams are announced at
/// all is what there is no way for: see `unhandlesimulcast.rs`.)
#[tokio::test(flavor = "multi_thread")]
async fn test_pairs_of_migrated_streams_are_seeded() {
    let track = |repair_ssrc: u32, is_repair_stream: bool| SimulcastTrackInfo {
        mid: "0".to_string(),
        stream_id: "q".to_string(),
        repair_ssrc,
        is_repair_stream,
    };
    let f = fixture_with(
        HashMap::from([(1001, track(2001, false)), (2001, track(0, true))]),
        None,
    );

    // no extensions on the packets
    f.rtp(media(1001, 1, &[]));
    let (buffer, _) = f.bind(1001);
    f.rtp(media(1001, 2, &[]));
    f.rtp(media(1001, 4, &[]));
    f.rtp(rtx(2001, 1, 3, &[]));
    assert_eq!(vec![1, 2, 4, 3], read(&buffer, 4).await);
    // nothing was announced, so nothing was reported as found
    assert!(f.found.lock().is_empty());
    f.sink.close();
}

fn rtcp_bytes(packet: &dyn rtcp::Packet) -> Vec<u8> {
    rtcp::packet::marshal(&[packet.cloned()]).unwrap().to_vec()
}

/// RTCP goes to the reader of every SSRC it is addressed to, one packet at a time.
#[test]
fn test_rtcp_goes_to_the_readers_it_is_addressed_to() {
    let f = fixture();
    type Received = Arc<Mutex<Vec<Vec<u8>>>>;
    let reader = |ssrc: u32| -> Received {
        let received = Received::default();
        f.factory
            .get_or_new(BufferPacketType::Rtcp, ssrc)
            .into_rtcp_reader()
            .unwrap()
            .on_packet({
                let received = received.clone();
                move |packet| received.lock().push(packet.to_vec())
            });
        received
    };

    // A stream the connection receives, and two it sends (whose readers the down tracks
    // have asked of the factory).
    f.sink.bind_remote_stream(&vp8_stream(1000, 0));
    let (published, first, second) = (reader(1000), reader(5001), reader(5002));

    let sender_report = SenderReport {
        ssrc: 1000,
        ntp_time: 0x0102030405060708,
        rtp_time: 90000,
        packet_count: 5,
        octet_count: 500,
        ..Default::default()
    };
    let receiver_report = ReceiverReport {
        ssrc: 9,
        reports: [5001, 5002, 5001]
            .into_iter()
            .map(|ssrc| ReceptionReport {
                ssrc,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let pli = PictureLossIndication {
        sender_ssrc: 9,
        media_ssrc: 5002,
    };
    // addressed to nobody: it goes to everybody the compound packet is addressed to
    let reference_time = ExtendedReport {
        sender_ssrc: 9,
        reports: vec![Box::new(ReceiverReferenceTimeReportBlock {
            ntp_timestamp: 0x1122334455667788,
        })],
    };
    // addressed to an SSRC nobody has a reader for yet
    let stray = PictureLossIndication {
        sender_ssrc: 9,
        media_ssrc: 7777,
    };
    f.sink.on_rtcp(
        &[
            Box::new(sender_report.clone()),
            Box::new(receiver_report.clone()),
            Box::new(pli.clone()),
            Box::new(reference_time.clone()),
            Box::new(stray.clone()),
        ],
        time::now(),
    );

    assert_eq!(
        vec![rtcp_bytes(&sender_report), rtcp_bytes(&reference_time)],
        *published.lock()
    );
    // a packet that names an SSRC twice is given to its reader once
    assert_eq!(
        vec![rtcp_bytes(&receiver_report), rtcp_bytes(&reference_time)],
        *first.lock()
    );
    assert_eq!(
        vec![
            rtcp_bytes(&receiver_report),
            rtcp_bytes(&pli),
            rtcp_bytes(&reference_time)
        ],
        *second.lock()
    );
    // the reader that was made for the stray SSRC is the factory's, for whoever asks later
    assert!(f.factory.get_rtcp_reader(7777).is_some());

    // A reader its owner has closed does not keep the packet from the others.
    f.factory.get_rtcp_reader(5001).unwrap().close().unwrap();
    f.sink
        .on_rtcp(&[Box::new(receiver_report.clone())], time::now());
    assert_eq!(2, first.lock().len());
    assert_eq!(4, second.lock().len());

    // What comes after the sink is closed goes nowhere, and the readers are closed.
    f.sink.close();
    f.sink.on_rtcp(&[Box::new(pli.clone())], time::now());
    assert_eq!(4, second.lock().len());
    for ssrc in [1000, 5002, 7777] {
        assert!(f.factory.get_rtcp_reader(ssrc).is_none(), "ssrc {ssrc}");
    }
}

/// Buffers are closed when their stream is unbound, a retransmission stream's with the
/// stream it repairs, and all of them when the sink is closed.
#[tokio::test(flavor = "multi_thread")]
async fn test_buffers_are_closed_with_their_stream_and_with_the_sink() {
    let rtts = Arc::new(Mutex::new(Vec::<u32>::new()));
    let f = fixture_with(
        HashMap::new(),
        Some(RttFromXrFactory::new({
            let rtts = rtts.clone();
            move |rtt| rtts.lock().push(rtt)
        })),
    );
    let written = Arc::new(Mutex::new(Vec::<u32>::new()));
    f.sink.bind_rtcp_writer(Arc::new({
        let written = written.clone();
        move |pkts: &[Box<dyn rtcp::Packet>]| -> Result<(), webrtc::Error> {
            for pkt in pkts {
                if let Some(xr) = pkt.as_any().downcast_ref::<ExtendedReport>() {
                    written.lock().push(xr.sender_ssrc);
                }
            }
            Ok(())
        }
    }));

    // a layer with a retransmission stream that was learned, one with a stream the
    // description named, and a stream nobody announced
    f.sink.bind_remote_stream(&vp8_stream(1001, 0));
    let (_, pairs) = f.bind(1001);
    f.rtp(media(1001, 1, &[(MID, "0"), (RID, "q")]));
    f.rtp(rtx(2001, 1, 1, &[(MID, "0"), (RSID, "q")]));
    eventually_async(|| !pairs.lock().is_empty(), WAIT, TICK).await;
    f.sink.bind_remote_stream(&vp8_stream(1002, 2002));
    f.sink.bind_remote_stream(&declared_rtx_stream(2002));
    f.rtp(media(3003, 1, &[]));
    let open = |ssrcs: &[u32]| -> Vec<u32> {
        ssrcs
            .iter()
            .copied()
            .filter(|&ssrc| {
                let (buffer, reader) = f.factory.get_buffer_pair(ssrc);
                assert_eq!(buffer.is_some(), reader.is_some(), "ssrc {ssrc}");
                buffer.is_some()
            })
            .collect()
    };
    let all = [1001, 2001, 1002, 2002, 3003];
    assert_eq!(all.to_vec(), open(&all));

    // The sink asks for the round trip time in the name of the first stream that was
    // announced, and takes what comes back.
    eventually_async(|| !written.lock().is_empty(), WAIT, TICK).await;
    assert_eq!(1001, written.lock()[0]);
    let last_rr =
        ((crate::mediatransportutil::to_ntp_time(time::now()).0 >> 16) as u32).wrapping_sub(0x8000);
    f.sink.on_rtcp(
        &[Box::new(ExtendedReport {
            sender_ssrc: 9,
            reports: vec![Box::new(rtcp::extended_report::DLRRReportBlock {
                reports: vec![rtcp::extended_report::DLRRReport {
                    ssrc: 1001,
                    last_rr,
                    dlrr: 0,
                }],
            })],
        })],
        time::now(),
    );
    // half a second, give or take what passed in between
    assert_eq!(1, rtts.lock().len());
    assert!((500..600).contains(&rtts.lock()[0]), "{rtts:?}");

    // The layer is unbound: its buffers and those of its retransmission stream go.
    let closed = Arc::new(Mutex::new(Vec::new()));
    for ssrc in all {
        f.buffer(ssrc).on_final_rtp_stats({
            let closed = closed.clone();
            move |_| closed.lock().push(ssrc)
        });
    }
    f.sink.unbind_remote_stream(&vp8_stream(1001, 0));
    assert_eq!(vec![1002, 2002, 3003], open(&all));
    assert_eq!(vec![1001], *closed.lock(), "only a bound buffer has stats");
    // what still comes for the stream makes a buffer anew, as a stream nobody announced
    f.rtp(media(1001, 2, &[]));
    assert_eq!(vec![1001, 1002, 2002, 3003], open(&all));

    // `rtc` unbinds a retransmission stream it announced on its own; unbinding the stream
    // closes it already.
    f.sink.unbind_remote_stream(&vp8_stream(1002, 2002));
    assert_eq!(vec![1001, 3003], open(&all));
    f.sink.unbind_remote_stream(&declared_rtx_stream(2002));
    assert_eq!(vec![1001, 3003], open(&all));

    // Closing the sink closes the rest, and nothing is made after that.
    f.sink.close();
    assert!(open(&all).is_empty());
    f.rtp(media(1002, 9, &[]));
    f.sink.bind_remote_stream(&vp8_stream(4004, 0));
    f.sink.close();
    assert!(open(&[1001, 1002, 4004]).is_empty());
    let count = written.lock().len();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        count,
        written.lock().len(),
        "asked after the sink was closed"
    );
}

// ---------------------------------------------------------------------------------------
// Two connections
// ---------------------------------------------------------------------------------------

fn media_engine() -> MediaEngine {
    let mut m = MediaEngine::default();
    for codec in codecs() {
        register_codec(&mut m, &codec, RtpCodecType::Video).unwrap();
    }
    for (uri, _) in EXTENSIONS {
        register_header_extension(&mut m, uri, RtpCodecType::Video, None).unwrap();
    }
    m
}

/// The publisher's track: takes VP8 and keeps what it was bound with. What is written through
/// its writer goes out as written, on whatever SSRC.
struct TestTrack {
    context: Mutex<Option<Arc<dyn TrackLocalContext>>>,
}

impl TestTrack {
    fn context(&self) -> Arc<dyn TrackLocalContext> {
        self.context.lock().clone().expect("the track is bound")
    }

    fn extension(&self, uri: &str) -> u8 {
        self.context()
            .header_extensions()
            .iter()
            .find(|e| e.uri == uri)
            .unwrap_or_else(|| panic!("{uri} was negotiated"))
            .id as u8
    }
}

impl TrackLocal for TestTrack {
    fn bind(&self, t: Arc<dyn TrackLocalContext>) -> Result<RtpCodecParameters, webrtc::Error> {
        let codec = t
            .codec_parameters()
            .into_iter()
            .find(|c| c.payload_type == VP8)
            .ok_or(webrtc::Error::UnsupportedCodec)?;
        *self.context.lock() = Some(t);
        Ok(codec)
    }

    fn unbind(&self, _t: Arc<dyn TrackLocalContext>) -> Result<(), webrtc::Error> {
        Ok(())
    }

    fn id(&self) -> String {
        "video".to_string()
    }

    fn rid(&self) -> String {
        String::new()
    }

    fn stream_id(&self) -> String {
        "stream".to_string()
    }

    fn kind(&self) -> RtpCodecType {
        RtpCodecType::Video
    }
}

/// A track as the server's side of the test has taken it up: what `MediaTrack.AddReceiver`
/// of upstream's `pkg/rtc` does with the buffers of a published stream.
struct Published {
    track: Arc<TrackRemote>,
    buffer: Arc<Buffer>,
}

type Rtcp = Arc<Mutex<Vec<(u32, Vec<u8>)>>>;

/// The server's end: a connection with a sink on a buffer factory, and a receiver that binds
/// the buffer of every track that is announced.
struct Server {
    pc: PeerConnection,
    factory: Arc<buffer::Factory>,
    sink: Arc<BufferFactorySink>,
    found: Found,
    published: Arc<Mutex<Vec<Published>>>,
    pairs: Pairs,
    // the RTCP the readers of the published streams were given, by the stream's SSRC
    rtcp: Rtcp,
    _node: Node,
}

impl Server {
    fn new() -> Server {
        let node = Node::new(NodeConfig::default()).unwrap();
        let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();
        let found = Found::default();
        let sink = BufferFactorySink::new(BufferFactorySinkParams {
            buffer_factory: factory.clone(),
            on_stream_found: Some(Box::new({
                let found = found.clone();
                move |info| found.lock().push(info.clone())
            })),
            sim_tracks: HashMap::new(),
            rtt_from_xr: None,
            logger: Logger::default(),
        });
        let pc = PeerConnection::new(
            &node,
            Configuration {
                media_engine: media_engine(),
                setting_engine: SettingEngine {
                    lite: true,
                    answering_dtls_role: DtlsRole::Server,
                    ..Default::default()
                },
                packet_sink: Some(sink.clone()),
                ..Default::default()
            },
        )
        .unwrap();

        let published = Arc::new(Mutex::new(Vec::new()));
        let pairs = Pairs::default();
        let rtcp = Rtcp::default();
        pc.on_track({
            let (factory, published, pairs, rtcp) = (
                factory.clone(),
                published.clone(),
                pairs.clone(),
                rtcp.clone(),
            );
            move |track: Arc<TrackRemote>, receiver: Arc<RtpReceiver>| {
                let ssrc = track.ssrc();
                let (buffer, reader) = factory.get_buffer_pair(ssrc);
                let buffer = buffer.expect("the track's buffer is there when it is announced");
                let reader = reader.expect("and its RTCP reader");
                reader.on_packet({
                    let rtcp = rtcp.clone();
                    move |packet| rtcp.lock().push((ssrc, packet.to_vec()))
                });
                buffer.on_notify_rtx({
                    let pairs = pairs.clone();
                    move |base, repair, rsid| pairs.lock().push((base, repair, rsid.to_string()))
                });
                buffer
                    .bind(
                        receiver.get_parameters(),
                        &track.codec().rtp_codec_capability,
                        0,
                    )
                    .expect("the buffer binds");
                published.lock().push(Published { track, buffer });
            }
        });
        Server {
            pc,
            factory,
            sink,
            found,
            published,
            pairs,
            rtcp,
            _node: node,
        }
    }

    fn buffer_of(&self, ssrc: u32) -> Option<Arc<Buffer>> {
        self.published
            .lock()
            .iter()
            .find(|p| p.track.ssrc() == ssrc)
            .map(|p| p.buffer.clone())
    }

    /// The RTCP packets of one type that the reader of a stream was given.
    fn rtcp_of<T: Clone + 'static>(&self, ssrc: u32) -> Vec<T> {
        self.rtcp
            .lock()
            .iter()
            .filter(|(s, _)| *s == ssrc)
            .flat_map(|(_, bytes)| rtcp::packet::unmarshal(&mut &bytes[..]).unwrap())
            .filter_map(|p| p.as_any().downcast_ref::<T>().cloned())
            .collect()
    }

    fn close(&self) {
        self.pc.close().unwrap();
        self.sink.close();
    }
}

/// The publisher's end: a full-ICE connection on a node of its own, with one video track.
struct Client {
    pc: PeerConnection,
    track: Arc<TestTrack>,
    _node: Node,
}

impl Client {
    fn new() -> Client {
        let node = Node::new(NodeConfig::default()).unwrap();
        let pc = PeerConnection::new(
            &node,
            Configuration {
                media_engine: media_engine(),
                setting_engine: SettingEngine::default(),
                ..Default::default()
            },
        )
        .unwrap();
        let track = Arc::new(TestTrack {
            context: Mutex::new(None),
        });
        pc.add_transceiver_from_track(
            track.clone(),
            Some(RtpTransceiverInit {
                direction: RtpTransceiverDirection::Sendonly,
            }),
        )
        .unwrap();
        Client {
            pc,
            track,
            _node: node,
        }
    }

    fn write(&self, header: &rtp::Header, payload: &[u8]) {
        self.track
            .context()
            .write_stream()
            .write_rtp(header, payload)
            .unwrap();
    }

    fn write_media(&self, ssrc: u32, sequence_number: u16, extensions: &[(u8, &str)]) {
        self.write(
            &with_extensions(header(ssrc, VP8, sequence_number), extensions),
            &media_payload(sequence_number),
        );
    }

    fn write_rtx(&self, ssrc: u32, sequence_number: u16, original: u16, exts: &[(u8, &str)]) {
        self.write(
            &with_extensions(rtx_header(ssrc, sequence_number, original), exts),
            &rtx_payload(original),
        );
    }

    fn write_sender_report(&self, ssrc: u32) -> SenderReport {
        let report = SenderReport {
            ssrc,
            ntp_time: 0x0102030405060708,
            rtp_time: ssrc,
            packet_count: 5,
            octet_count: 500,
            ..Default::default()
        };
        self.pc.write_rtcp(&[Box::new(report.clone())]).unwrap();
        report
    }
}

/// The client offers, the server answers. `edit` is given the client's offer on its way to
/// the server: what a browser would have said instead.
async fn connect(client: &Client, server: &Server, edit: impl FnOnce(&str) -> String) {
    let offer = client.pc.create_offer(None).unwrap();
    client.pc.set_local_description(offer).unwrap();
    let sdp = edit(&client.pc.local_description().unwrap().sdp);
    server
        .pc
        .set_remote_description(SessionDescription {
            r#type: SdpType::Offer,
            sdp,
        })
        .unwrap();
    let answer = server.pc.create_answer().unwrap();
    server.pc.set_local_description(answer).unwrap();
    client
        .pc
        .set_remote_description(server.pc.local_description().unwrap())
        .unwrap();
    let connected = |pc: &PeerConnection| pc.connection_state() == PeerConnectionState::Connected;
    eventually_async(
        || connected(&client.pc) && connected(&server.pc),
        WAIT,
        TICK,
    )
    .await;
    eventually_async(|| client.track.context.lock().is_some(), WAIT, TICK).await;
}

/// The offer without the lines that name SSRCs, and with `lines` at the end of its one media
/// section.
fn without_ssrcs(sdp: &str, lines: &str) -> String {
    let mut out = String::new();
    for line in sdp.lines() {
        if !line.starts_with("a=ssrc") {
            out.push_str(line);
            out.push_str("\r\n");
        }
    }
    out.push_str(lines);
    out
}

/// A publisher with one video stream and its retransmission stream, both named in the
/// description. Media goes into the buffer the receiver has bound, retransmissions repair
/// what was lost, and RTCP for the stream comes out at its reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_published_stream_with_rtx() {
    let server = Server::new();
    let client = Client::new();
    connect(&client, &server, str::to_string).await;

    let context = client.track.context();
    let (ssrc, rtx_ssrc) = (context.ssrc(), context.ssrc_retransmission());
    assert_ne!(0, rtx_ssrc);

    // 3 and 4 are lost on the way.
    for sequence_number in [1, 2, 5, 6] {
        client.write_media(ssrc, sequence_number, &[]);
    }
    eventually_async(|| server.buffer_of(ssrc).is_some(), WAIT, TICK).await;
    let buffer = server.buffer_of(ssrc).unwrap();
    assert_eq!(vec![1, 2, 5, 6], read(&buffer, 4).await);

    // They come again on the retransmission stream, with a padding-only packet in between.
    client.write_rtx(rtx_ssrc, 100, 3, &[]);
    let mut padding = header(rtx_ssrc, VP8_RTX, 101);
    padding.padding = true;
    padding.padding_size = 255;
    client.write(&padding, &[]);
    client.write_rtx(rtx_ssrc, 102, 4, &[]);
    assert_eq!(vec![3, 4], read(&buffer, 2).await);
    assert_has_in_order(
        &buffer,
        ext_sequence_number(&buffer, 1),
        &[1, 2, 3, 4, 5, 6],
    );

    // The stream and its retransmission stream were reported as found, and each has its
    // buffer; the track knows its retransmission stream from the description.
    assert_eq!(
        vec![ssrc, rtx_ssrc],
        server
            .found
            .lock()
            .iter()
            .map(|i| i.ssrc)
            .collect::<Vec<_>>()
    );
    assert_eq!(rtx_ssrc, server.found.lock()[0].ssrc_retransmission);
    assert!(server.factory.get_buffer(rtx_ssrc).is_some());
    assert_eq!(rtx_ssrc, server.published.lock()[0].track.rtx_ssrc());
    assert_eq!(1, server.published.lock().len());

    // RTCP of the publisher about the stream arrives at the stream's reader.
    let report = client.write_sender_report(ssrc);
    eventually_async(
        || !server.rtcp_of::<SenderReport>(ssrc).is_empty(),
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(vec![report], server.rtcp_of::<SenderReport>(ssrc));

    client.pc.close().unwrap();
    server.close();
    assert!(server.factory.get_buffer(ssrc).is_none());
    assert!(server.factory.get_buffer(rtx_ssrc).is_none());
}

/// A simulcast publisher as browsers do it: no SSRCs in the description, three layers named
/// by their rid in a header extension, and a retransmission stream for each that names the
/// layer it repairs. `rtc` announces the layers and not the retransmission streams; they are
/// paired from their packets all the same, and repair their layers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_published_simulcast_with_rtx() {
    // (rid, ssrc, rtx ssrc)
    let layers = [("q", 1001, 2001), ("h", 1002, 2002), ("f", 1003, 2003)];

    let server = Server::new();
    let client = Client::new();
    connect(&client, &server, |sdp| {
        without_ssrcs(
            sdp,
            "a=rid:q send\r\na=rid:h send\r\na=rid:f send\r\na=simulcast:send q;h;f\r\n",
        )
    })
    .await;
    let mid = client.track.extension(sdp::SDES_MID_URI);
    let rid = client.track.extension(sdp::SDES_RTP_STREAM_ID_URI);
    let rsid = client.track.extension(SDES_REPAIR_RTP_STREAM_ID_URI);

    // Every layer is published; 3 is lost on each.
    for sequence_number in [1, 2, 4] {
        for (name, ssrc, _) in layers {
            client.write_media(ssrc, sequence_number, &[(mid, "0"), (rid, name)]);
        }
    }
    eventually_async(|| server.published.lock().len() == 3, WAIT, TICK).await;
    for (name, ssrc, _) in layers {
        let buffer = server.buffer_of(ssrc).unwrap();
        assert_eq!(vec![1, 2, 4], read(&buffer, 3).await, "layer {name}");
    }
    // `rtc` knows no retransmission stream for a layer
    for published in server.published.lock().iter() {
        assert_eq!(0, published.track.rtx_ssrc());
    }

    // The lost packets come again, each on the retransmission stream of its layer.
    for (name, _, rtx_ssrc) in layers {
        client.write_rtx(rtx_ssrc, 1, 3, &[(mid, "0"), (rsid, name)]);
    }
    for (name, ssrc, _) in layers {
        let buffer = server.buffer_of(ssrc).unwrap();
        assert_eq!(vec![3], read(&buffer, 1).await, "layer {name}");
        assert_has_in_order(&buffer, ext_sequence_number(&buffer, 1), &[1, 2, 3, 4]);
    }

    // All three pairs were made, and the receiver of each layer was told.
    let mut pairs = server.pairs.lock().clone();
    pairs.sort();
    assert_eq!(
        layers
            .iter()
            .map(|&(name, ssrc, rtx_ssrc)| (ssrc, rtx_ssrc, name.to_string()))
            .collect::<Vec<_>>(),
        pairs
    );
    // The retransmission streams were reported as found, after their layers.
    eventually_async(|| server.found.lock().len() == 6, WAIT, TICK).await;
    let found = server.found.lock().clone();
    for (name, ssrc, rtx_ssrc) in layers {
        let layer = found.iter().find(|i| i.ssrc == ssrc).unwrap();
        assert_eq!((VP8, 0), (layer.payload_type, layer.ssrc_retransmission));
        let repair = found.iter().find(|i| i.ssrc == rtx_ssrc).unwrap();
        assert_eq!(MIME_TYPE_RTX, repair.mime_type, "layer {name}");
        assert_eq!(VP8_RTX, repair.payload_type);
        assert_eq!(layer.rtp_header_extensions, repair.rtp_header_extensions);
    }

    // RTCP about a layer arrives at that layer's reader.
    for (_, ssrc, _) in layers {
        client.write_sender_report(ssrc);
    }
    for (_, ssrc, _) in layers {
        eventually_async(
            || !server.rtcp_of::<SenderReport>(ssrc).is_empty(),
            WAIT,
            TICK,
        )
        .await;
        let reports = server.rtcp_of::<SenderReport>(ssrc);
        assert_eq!(
            vec![ssrc],
            reports.iter().map(|r| r.ssrc).collect::<Vec<_>>()
        );
    }

    client.pc.close().unwrap();
    server.close();
    for (_, ssrc, rtx_ssrc) in layers {
        assert!(server.factory.get_buffer(ssrc).is_none());
        assert!(server.factory.get_buffer(rtx_ssrc).is_none());
    }
}

/// What `rtc` 0.21.1 makes of simulcast without rid, where the layers are SSRCs the
/// description groups (`a=ssrc-group:SIM`): it does not read the group, takes each SSRC as a
/// stream of its own for the one receiver of the media section, and the last one named
/// replaces the others. That one is announced, as a track without a rid; the others never
/// are. Their packets come out at the sink like everything else and wait in buffers nobody
/// takes. (This is the case upstream's `UnhandleSimulcastInterceptor` would be for, if
/// anything filled its list; here nothing could call it to effect.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_simulcast_by_ssrc_group_is_not_simulcast_to_rtc() {
    let server = Server::new();
    let client = Client::new();
    connect(&client, &server, |sdp| {
        let mut lines = String::from("a=ssrc-group:SIM 1001 1002 1003\r\n");
        for (ssrc, rtx_ssrc) in [(1001, 2001), (1002, 2002), (1003, 2003)] {
            lines.push_str(&format!("a=ssrc-group:FID {ssrc} {rtx_ssrc}\r\n"));
        }
        for ssrc in [1001, 2001, 1002, 2002, 1003, 2003] {
            lines.push_str(&format!("a=ssrc:{ssrc} cname:publisher\r\n"));
            lines.push_str(&format!("a=ssrc:{ssrc} msid:stream video\r\n"));
        }
        without_ssrcs(sdp, &lines)
    })
    .await;

    for sequence_number in 1..=3 {
        for ssrc in [1001, 1002, 1003] {
            client.write_media(ssrc, sequence_number, &[]);
        }
    }
    eventually_async(|| server.buffer_of(1003).is_some(), WAIT, TICK).await;
    assert_eq!(
        vec![1, 2, 3],
        read(&server.buffer_of(1003).unwrap(), 3).await
    );
    // the other layers have arrived by now too: the connection hands over in order
    eventually_async(
        || {
            [1001, 1002]
                .iter()
                .all(|&ssrc| server.factory.get_buffer(ssrc).is_some())
        },
        WAIT,
        TICK,
    )
    .await;

    {
        let published = server.published.lock();
        assert_eq!(1, published.len());
        assert_eq!(1003, published[0].track.ssrc());
        assert_eq!("", published[0].track.rid());
        assert_eq!(2003, published[0].track.rtx_ssrc());
    }
    assert_eq!(
        vec![1003, 2003],
        server
            .found
            .lock()
            .iter()
            .map(|i| i.ssrc)
            .collect::<Vec<_>>()
    );

    client.pc.close().unwrap();
    server.close();
}
