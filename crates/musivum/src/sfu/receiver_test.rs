// Ported from LiveKit, pkg/sfu/receiver_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `TestWebRTCReceiver_OnCloseHandler` makes an empty
// `&WebRTCReceiver{}`; a receiver cannot be made empty here, so the test makes one with `new`
// from a fake track. `receiverCloseTestSender` is a `FakeTrackSender` with the two methods
// stubbed that the original overrides; the channels that are closed are channels whose
// sender is dropped. `BenchmarkWriteRTP` and its helpers (the worker pool among them) are
// left out, as benchmarks are. The tests after the two ported ones are new for musivum, each
// marked so: they run a real `Buffer` behind a receiver.

use std::sync::mpsc;
use std::time::Duration as StdDuration;

use bytes::Bytes;

use super::*;
use crate::go::atomic;
use crate::go::time::Duration;
use crate::mediatransportutil::bucket;
use crate::protocol::logger;
use crate::sfu::streamtracker;
use crate::sfu::{FakeTrackRemote, FakeTrackSender, StreamTrackerConfig, StreamTrackerType};
use crate::testutils;
use crate::webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use crate::webrtc::rtp;

// A `*webrtc.RTPReceiver` without header extensions.
struct TestRtpReceiver;

impl webrtc::RtpReceiver for TestRtpReceiver {
    fn get_parameters(&self) -> webrtc::RtpParameters {
        webrtc::RtpParameters::default()
    }
}

fn opus_codec() -> webrtc::RtpCodecParameters {
    webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: webrtc::MIME_TYPE_OPUS.to_string(),
            clock_rate: 48000,
            channels: 2,
            ..Default::default()
        },
        payload_type: 111,
    }
}

fn audio_track_info() -> livekit::TrackInfo {
    livekit::TrackInfo {
        sid: "track".to_string(),
        r#type: livekit::TrackType::Audio as i32,
        source: livekit::TrackSource::Microphone as i32,
        ..Default::default()
    }
}

fn fake_track_remote(
    kind: webrtc::RtpCodecType,
    codec: webrtc::RtpCodecParameters,
    rid: &str,
    ssrc: u32,
) -> Arc<FakeTrackRemote> {
    let track = Arc::new(FakeTrackRemote::default());
    track.id_returns("track".to_string());
    track.stream_id_returns("stream".to_string());
    track.msid_returns("stream track".to_string());
    track.kind_returns(kind);
    track.codec_returns(codec);
    track.rid_returns(rid.to_string());
    track.ssrc_returns(ssrc);
    track
}

#[tokio::test(flavor = "multi_thread")]
async fn test_web_rtc_receiver_on_close_handler() {
    struct Args {
        f: Box<dyn Fn() + Send + Sync>,
    }
    struct Test {
        name: &'static str,
        args: Args,
    }
    let tests = vec![Test {
        name: "Must set on close handler function",
        args: Args { f: Box::new(|| {}) },
    }];
    for tt in tests {
        let track = fake_track_remote(webrtc::RtpCodecType::Audio, opus_codec(), "", 1234);
        let w = WebRtcReceiver::new(
            Arc::new(TestRtpReceiver),
            track.as_ref(),
            &audio_track_info(),
            logger::get_logger(),
            None,
            StreamTrackerManagerConfig::default(),
            Vec::new(),
        );
        w.on_close_handler(tt.args.f);
        assert!(w.on_close_handler.lock().is_some(), "{}", tt.name);
    }
}

// receiverCloseTestSender of the original
fn receiver_close_test_sender(
    subscriber_id: &str,
    up_track_max_published_layer_change: impl Fn(i32) + Send + Sync + 'static,
) -> Arc<FakeTrackSender> {
    let s = Arc::new(FakeTrackSender::default());
    s.subscriber_id_returns(subscriber_id.into());
    s.up_track_max_published_layer_change_calls(up_track_max_published_layer_change);
    s
}

#[test]
fn test_receiver_base_add_down_track_returns_closed_when_close_wins_race() {
    let receiver = ReceiverBase::new(
        ReceiverBaseParams {
            track_id: "track".into(),
            stream_id: "stream".to_string(),
            kind: webrtc::RtpCodecType::Audio,
            codec: opus_codec(),
            logger: logger::get_logger(),
            ..Default::default()
        },
        &audio_track_info(),
        ReceiverCodecState::Normal,
    );

    let (entered_add_tx, entered_add) = mpsc::channel::<()>();
    let (resume_add_tx, resume_add) = mpsc::channel::<()>();
    let entered_add_tx = Mutex::new(Some(entered_add_tx));
    let resume_add = Mutex::new(resume_add);
    let track = receiver_close_test_sender("subscriber", move |_| {
        // close(enteredAdd)
        drop(entered_add_tx.lock().take());
        // <-resumeAdd
        let _ = resume_add.lock().recv();
    });

    let (err_ch_tx, err_ch) = mpsc::channel();
    {
        let receiver = receiver.clone();
        std::thread::spawn(move || {
            let _ = err_ch_tx.send(receiver.add_down_track(track));
        });
    }

    // AddDownTrack has passed its initial IsClosed check and is paused
    // before reaching the final TryStore.
    let _ = entered_add.recv();

    // Close terminally drains/closes the spreader before AddDownTrack resumes.
    receiver.close("test", false);
    drop(resume_add_tx);

    let err = err_ch.recv().unwrap();
    assert_eq!(Err(Error::ReceiverClosed), err);
    assert!(receiver.is_closed());
    assert!(receiver.get_down_tracks().is_empty());
}

// BenchmarkWriteRTP, benchmarkNoPool, benchmarkPool, benchmarkGoroutine,
// benchmarkLoadBalanced, benchmarkLoadBalancedPool and writeRTP of the original are a
// benchmark: left out.

// -----------------------------------------------------------

// New for musivum: tests that run a real `Buffer` behind a receiver. The receiver is a
// `WebRtcReceiver` whose one up track has the rid of the middle layer of three, so its buffer
// is the buffer of layer 1. The expected sequence numbers, layers, counts, bytes and error
// texts are what the original gives for the same steps: they were taken from a Go test laid
// over `pkg/sfu` at the pin, which makes a `ReceiverBase` with the same parameters, adds a
// `buffer.Buffer` to it as layer 1 (`AddBuffer`, `OnRtcpFeedback`, `StartBuffer`, which is
// what `AddUpTrack` does) and counts what arrives at senders of its own. What is expected of
// `on_close_handler`, of the up tracks and of what holds the receiver after its close has no
// counterpart there and comes from the code's meaning; each place says so.

const SSRC: u32 = 0x1234;

fn vp8_codec() -> webrtc::RtpCodecParameters {
    webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: "video/VP8".to_string(),
            clock_rate: 90000,
            rtcp_feedback: vec![webrtc::RtcpFeedback {
                r#type: "nack".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        },
        payload_type: 96,
    }
}

// Three layers, one stream each.
fn simulcast_track_info() -> livekit::TrackInfo {
    let layer = |quality: livekit::VideoQuality,
                 width: u32,
                 height: u32,
                 bitrate: u32,
                 spatial_layer: i32,
                 rid: &str| livekit::VideoLayer {
        quality: quality as i32,
        width,
        height,
        bitrate,
        spatial_layer,
        rid: rid.to_string(),
        ..Default::default()
    };
    let layers = vec![
        layer(livekit::VideoQuality::Low, 320, 180, 100000, 0, "q"),
        layer(livekit::VideoQuality::Medium, 640, 360, 300000, 1, "h"),
        layer(livekit::VideoQuality::High, 1280, 720, 1000000, 2, "f"),
    ];
    livekit::TrackInfo {
        sid: "track".to_string(),
        r#type: livekit::TrackType::Video as i32,
        source: livekit::TrackSource::Camera as i32,
        layers: layers.clone(),
        codecs: vec![livekit::SimulcastCodecInfo {
            mime_type: "video/VP8".to_string(),
            layers,
            video_layer_mode: livekit::video_layer::Mode::OneSpatialLayerPerStream as i32,
            ..Default::default()
        }],
        ..Default::default()
    }
}

// A track without layers: its one stream is layer 0.
fn single_layer_track_info() -> livekit::TrackInfo {
    livekit::TrackInfo {
        sid: "track".to_string(),
        r#type: livekit::TrackType::Video as i32,
        source: livekit::TrackSource::Camera as i32,
        ..Default::default()
    }
}

// Trackers by packets that take a stream for live with its first packet and for stopped
// after a cycle of half a second without one.
fn stream_tracker_manager_config() -> StreamTrackerManagerConfig {
    let tracker = streamtracker::StreamTrackerPacketConfig {
        samples_required: 1,
        cycles_required: 1,
        cycle_duration: Duration::from_millis(500),
    };
    StreamTrackerManagerConfig {
        video: StreamTrackerConfig {
            stream_tracker_type: StreamTrackerType::Packet,
            bitrate_report_interval: [0, 1, 2]
                .into_iter()
                .map(|layer| (layer, Duration::from_secs(1)))
                .collect(),
            packet_tracker: [0, 1, 2]
                .into_iter()
                .map(|layer| (layer, tracker))
                .collect(),
            ..Default::default()
        },
        ..Default::default()
    }
}

// A receiver of a video track with one up track and its buffer, and what a test needs to
// feed it and to see what it sends to the publisher.
struct TestReceiver {
    w: Arc<WebRtcReceiver>,
    buff: Arc<buffer::Buffer>,
    // the PLIs that the receiver sent to the publisher
    plis: Arc<atomic::Uint32>,
    // the packets written, as they were written
    raws: Mutex<Vec<Bytes>>,
}

impl TestReceiver {
    // `rid` is the rid of the up track: "h" is layer 1 of `simulcast_track_info`, and a track
    // without rid is layer 0.
    fn new(track_info: &livekit::TrackInfo, rid: &str) -> TestReceiver {
        let track = fake_track_remote(webrtc::RtpCodecType::Video, vp8_codec(), rid, SSRC);
        let plis = Arc::new(atomic::Uint32::new(0));
        let on_rtcp = {
            let plis = plis.clone();
            move |pkts: Vec<Box<dyn rtcp::Packet>>| {
                for pkt in &pkts {
                    if pkt
                        .as_any()
                        .downcast_ref::<PictureLossIndication>()
                        .is_some()
                    {
                        plis.inc();
                    }
                }
            }
        };
        let w = WebRtcReceiver::new(
            Arc::new(TestRtpReceiver),
            track.as_ref(),
            track_info,
            logger::get_logger(),
            Some(Box::new(on_rtcp)),
            stream_tracker_manager_config(),
            vec![with_pli_throttle_config(PliThrottleConfig {
                low_quality: Duration::from_millis(100),
                mid_quality: Duration::from_millis(300),
                high_quality: Duration::from_millis(500),
            })],
        );

        let buff = buffer::Buffer::new(SSRC, 300, 70);
        assert!(
            buff.bind(
                webrtc::RtpParameters {
                    codecs: vec![vp8_codec()],
                    ..Default::default()
                },
                &vp8_codec().rtp_codec_capability,
                0,
            )
            .is_ok()
        );
        assert_eq!(Ok(()), w.add_up_track(track, buff.clone()));

        TestReceiver {
            w,
            buff,
            plis,
            raws: Mutex::new(Vec::new()),
        }
    }

    // Writes packet `i` of the stream into the buffer: sequence number 1000 + i.
    fn write(&self, i: usize) {
        let pkt = rtp::Packet {
            header: rtp::Header {
                version: 2,
                marker: true,
                payload_type: 96,
                sequence_number: (1000 + i) as u16,
                timestamp: (3000 * i) as u32,
                ssrc: SSRC,
                ..Default::default()
            },
            payload: Bytes::from(vec![0xff, 0xff, 0xff, 0xfd, 0xb4, 0x9f, 0x94, 0x1, i as u8]),
            ..Default::default()
        };
        let raw = Bytes::from(pkt.marshal().unwrap());
        self.raws.lock().push(raw.clone());
        assert!(self.buff.write(raw).is_ok());
    }

    fn raw(&self, i: usize) -> Bytes {
        self.raws.lock()[i].clone()
    }
}

// A down track that takes every packet.
fn new_sender(subscriber_id: &str) -> Arc<FakeTrackSender> {
    let dt = Arc::new(FakeTrackSender::default());
    dt.subscriber_id_returns(subscriber_id.into());
    dt.write_rtp_returns(1);
    dt
}

// The packets that a down track was given, each with the layer it was given with.
fn written(dt: &FakeTrackSender) -> Vec<(buffer::ExtPacket, i32)> {
    (0..dt.write_rtp_call_count())
        .map(|i| dt.write_rtp_args_for_call(i))
        .collect()
}

fn sequence_numbers(dt: &FakeTrackSender) -> Vec<u16> {
    written(dt)
        .iter()
        .map(|(pkt, _)| pkt.packet.sequence_number)
        .collect()
}

async fn eventually(condition: impl FnMut() -> bool) {
    testutils::eventually_async(
        condition,
        StdDuration::from_secs(10),
        StdDuration::from_millis(5),
    )
    .await;
}

// New for musivum.
#[tokio::test(flavor = "multi_thread")]
async fn packets_of_the_buffer_come_out_at_the_down_tracks_in_order_with_their_layer() {
    let r = TestReceiver::new(&simulcast_track_info(), "h");
    let dt1 = new_sender("sub1");
    let dt2 = new_sender("sub2");
    assert_eq!(Ok(()), r.w.add_down_track(dt1.clone()));
    assert_eq!(Ok(()), r.w.add_down_track(dt2.clone()));

    for i in 0..10 {
        r.write(i);
    }
    eventually(|| dt1.write_rtp_call_count() == 10 && dt2.write_rtp_call_count() == 10).await;
    // the layer went live, and the highest layer published became 1 (told from a task)
    eventually(|| {
        [&dt1, &dt2].iter().all(|dt| {
            dt.up_track_layers_change_call_count() == 1
                && dt.up_track_max_published_layer_change_call_count() == 2
        })
    })
    .await;

    // From the original: both senders get the ten packets in the order written, extended
    // sequence numbers equal to the sequence numbers, each with layer 1 (the buffer's), and a
    // packet that says no spatial layer and temporal layer 2.
    for (name, dt) in [("dt1", &dt1), ("dt2", &dt2)] {
        let pkts = written(dt);
        assert_eq!(10, pkts.len(), "{name}");
        for (i, (pkt, layer)) in pkts.iter().enumerate() {
            assert_eq!(1000 + i as u16, pkt.packet.sequence_number, "{name}");
            assert_eq!(1000 + i as u64, pkt.ext_sequence_number, "{name}");
            assert_eq!(1, *layer, "{name}");
            assert_eq!(-1, pkt.video_layer.spatial, "{name}");
            assert_eq!(2, pkt.video_layer.temporal, "{name}");
            assert_eq!(r.raw(i), pkt.raw_packet, "{name}");
        }
        // From the original: what a down track is told when it is added, and then when the
        // first packet of the layer came.
        assert_eq!(
            -1,
            dt.up_track_max_published_layer_change_args_for_call(0),
            "{name}"
        );
        assert_eq!(
            1,
            dt.up_track_max_published_layer_change_args_for_call(1),
            "{name}"
        );
        assert_eq!(
            1,
            dt.up_track_max_temporal_layer_seen_change_call_count(),
            "{name}"
        );
        assert_eq!(
            -1,
            dt.up_track_max_temporal_layer_seen_change_args_for_call(0),
            "{name}"
        );
        assert_eq!(0, dt.close_call_count(), "{name}");
    }

    r.w.close("test", true);
}

// New for musivum.
#[tokio::test(flavor = "multi_thread")]
async fn a_down_track_added_later_gets_only_later_packets() {
    let r = TestReceiver::new(&simulcast_track_info(), "h");
    let dt1 = new_sender("sub1");
    let dt2 = new_sender("sub2");
    assert_eq!(Ok(()), r.w.add_down_track(dt1.clone()));
    assert_eq!(Ok(()), r.w.add_down_track(dt2.clone()));

    for i in 0..10 {
        r.write(i);
    }
    eventually(|| dt1.write_rtp_call_count() == 10 && dt2.write_rtp_call_count() == 10).await;

    let dt3 = new_sender("sub3");
    assert_eq!(Ok(()), r.w.add_down_track(dt3.clone()));
    for i in 10..15 {
        r.write(i);
    }
    eventually(|| {
        dt1.write_rtp_call_count() == 15
            && dt2.write_rtp_call_count() == 15
            && dt3.write_rtp_call_count() == 5
    })
    .await;

    // From the original: the third sender has the five packets written after it was added,
    // and was told the highest layer published as it was then.
    assert_eq!(vec![1010, 1011, 1012, 1013, 1014], sequence_numbers(&dt3));
    assert!(written(&dt3).iter().all(|(_, layer)| *layer == 1));
    assert_eq!((1000..1015).collect::<Vec<u16>>(), sequence_numbers(&dt1));
    assert_eq!(1, dt3.up_track_max_published_layer_change_call_count());
    assert_eq!(1, dt3.up_track_max_published_layer_change_args_for_call(0));
    assert_eq!(0, dt3.up_track_layers_change_call_count());

    // From the original: the statistics of the track after fifteen packets of 21 octets.
    let stats = r.w.get_track_stats();
    assert!(stats.is_some());
    let stats = stats.unwrap();
    assert_eq!(15, stats.packets);
    assert_eq!(315, stats.bytes);
    assert_eq!(180, stats.header_bytes);

    // From the original: a sender that was deleted gets nothing more.
    r.w.delete_down_track(&"sub2".into());
    r.write(15);
    eventually(|| dt1.write_rtp_call_count() == 16 && dt3.write_rtp_call_count() == 6).await;
    assert_eq!(15, dt2.write_rtp_call_count());
    assert_eq!(2, r.w.get_down_tracks().len());

    r.w.close("test", true);
}

// New for musivum.
#[tokio::test(flavor = "multi_thread")]
async fn read_rtp_gives_a_packet_by_its_extended_sequence_number() {
    let r = TestReceiver::new(&simulcast_track_info(), "h");
    let dt1 = new_sender("sub1");
    assert_eq!(Ok(()), r.w.add_down_track(dt1.clone()));

    for i in 0..15 {
        r.write(i);
    }
    eventually(|| dt1.write_rtp_call_count() == 15).await;

    // From the original: the packet with extended sequence number 1003 is the fourth one
    // written, 21 octets, these.
    let esn = written(&dt1)[3].0.ext_sequence_number;
    assert_eq!(1003, esn);
    let mut buf = vec![0u8; 1500];
    let n = r.w.read_rtp(&mut buf, 1, esn);
    assert_eq!(Ok(21), n);
    assert_eq!(r.raw(3), buf[..21]);
    let hex: String = buf[..21].iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!("80e003eb0000232800001234fffffffdb49f940103", hex);

    // From the original: a layer without buffer, and a packet that the buffer has not seen.
    let err = r.w.read_rtp(&mut buf, 0, esn);
    assert_eq!(
        Err(Error::Bucket(bucket::Error::PacketMismatch(String::new()))),
        err
    );
    assert_eq!("sequence number mismatch", err.unwrap_err().to_string());
    let err = r.w.read_rtp(&mut buf, 1, esn + 1000);
    assert!(matches!(
        err,
        Err(Error::Bucket(bucket::Error::PacketTooNew(_)))
    ));
    assert_eq!(
        "received packet too new, headSN 1014, sn 2003",
        err.unwrap_err().to_string()
    );

    // From the original: a receiver that was closed has no buffers left to read from.
    r.w.close("test", true);
    let err = r.w.read_rtp(&mut buf, 1, esn);
    assert_eq!("sequence number mismatch", err.unwrap_err().to_string());
}

// New for musivum.
#[tokio::test(flavor = "multi_thread")]
async fn a_layer_that_stops_is_reported_and_plis_are_throttled() {
    let r = TestReceiver::new(&simulcast_track_info(), "h");
    let dt1 = new_sender("sub1");
    assert_eq!(Ok(()), r.w.add_down_track(dt1.clone()));

    for i in 0..10 {
        r.write(i);
    }
    eventually(|| dt1.write_rtp_call_count() == 10).await;
    eventually(|| dt1.up_track_layers_change_call_count() == 1).await;

    // From the original, with a throttle of 300 ms for the middle layer: of three PLIs in a
    // row one is sent, after the interval one of two more, a forced one always, and none for
    // a layer without buffer.
    r.w.send_pli(1, false);
    r.w.send_pli(1, false);
    r.w.send_pli(1, false);
    assert_eq!(1, r.plis.load());
    tokio::time::sleep(StdDuration::from_millis(400)).await;
    r.w.send_pli(1, false);
    r.w.send_pli(1, false);
    assert_eq!(2, r.plis.load());
    r.w.send_pli(1, true);
    assert_eq!(3, r.plis.load());
    r.w.send_pli(0, false);
    assert_eq!(3, r.plis.load());

    // From the original: the layer is reported once when it goes live and once more when its
    // tracker finds a cycle without packets.
    eventually(|| dt1.up_track_layers_change_call_count() == 2).await;
    tokio::time::sleep(StdDuration::from_millis(50)).await;
    assert_eq!(2, dt1.up_track_layers_change_call_count());
    assert_eq!(10, dt1.write_rtp_call_count());

    r.w.close("test", true);
}

// New for musivum.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_tells_the_down_tracks_and_forwarding_goes_on() {
    let r = TestReceiver::new(&simulcast_track_info(), "h");
    let dt1 = new_sender("sub1");
    assert_eq!(Ok(()), r.w.add_down_track(dt1.clone()));

    for i in 0..16 {
        r.write(i);
    }
    eventually(|| dt1.write_rtp_call_count() == 16).await;

    r.w.restart("test");
    r.write(16);
    r.write(17);
    eventually(|| dt1.write_rtp_call_count() == 18).await;

    // From the original: the sender is told of the restart once, the packets after it come
    // out with the sequence numbers they have, a packet from before the restart cannot be
    // read back, one from after it can.
    assert_eq!(1, dt1.receiver_restart_call_count());
    assert_eq!((1000..1018).collect::<Vec<u16>>(), sequence_numbers(&dt1));
    let pkts = written(&dt1);
    assert_eq!(1016, pkts[16].0.ext_sequence_number);
    assert_eq!(1017, pkts[17].0.ext_sequence_number);
    assert!(pkts.iter().all(|(_, layer)| *layer == 1));
    let mut buf = vec![0u8; 1500];
    let err = r.w.read_rtp(&mut buf, 1, 1003);
    assert_eq!(
        "invalid size, headSN 1017, sn 1003, size 65535",
        err.unwrap_err().to_string()
    );
    assert_eq!(Ok(21), r.w.read_rtp(&mut buf, 1, 1017));
    assert_eq!(r.raw(17), buf[..21]);

    // From the code's meaning: what the sender is handed is the base of the receiver.
    let restarted = dt1.receiver_restart_args_for_call(0);
    assert!(std::ptr::eq(
        Arc::as_ptr(&restarted) as *const (),
        Arc::as_ptr(r.w.receiver_base()) as *const ()
    ));

    r.w.close("test", true);
}

// New for musivum.
#[tokio::test(flavor = "multi_thread")]
async fn closing_the_receiver_closes_its_down_tracks_and_ends_its_forwarders() {
    let r = TestReceiver::new(&simulcast_track_info(), "h");
    let on_close_handler_calls = Arc::new(atomic::Uint32::new(0));
    {
        let on_close_handler_calls = on_close_handler_calls.clone();
        r.w.on_close_handler(move || {
            on_close_handler_calls.inc();
        });
    }
    let dt1 = new_sender("sub1");
    let dt2 = new_sender("sub2");
    let dt3 = new_sender("sub3");
    for dt in [&dt1, &dt2, &dt3] {
        assert_eq!(Ok(()), r.w.add_down_track(dt.clone()));
    }
    for i in 0..3 {
        r.write(i);
    }
    eventually(|| {
        [&dt1, &dt2, &dt3]
            .iter()
            .all(|dt| dt.write_rtp_call_count() == 3)
    })
    .await;
    r.w.delete_down_track(&"sub2".into());

    r.w.close("test", true);

    // From the original: the down tracks that the receiver still had are closed, once each;
    // the receiver is closed, has no down tracks and takes none; nothing can be read from it,
    // and its buffer is closed.
    assert_eq!(1, dt1.close_call_count());
    assert_eq!(0, dt2.close_call_count());
    assert_eq!(1, dt3.close_call_count());
    assert!(r.w.is_closed());
    assert!(r.w.get_down_tracks().is_empty());
    assert_eq!(
        Err(Error::ReceiverClosed),
        r.w.add_down_track(new_sender("sub4"))
    );
    assert!(r.w.can_close());
    let mut buf = vec![0u8; 1500];
    assert_eq!(
        "sequence number mismatch",
        r.w.read_rtp(&mut buf, 1, 1001).unwrap_err().to_string()
    );
    assert_eq!(
        Err(buffer::Error::Eof),
        r.buff.read_extended(&mut buf).map(|_| ())
    );
    // From the code's meaning: the handler of the WebRTC receiver is called once, and a second
    // close does nothing.
    assert_eq!(1, on_close_handler_calls.load());
    r.w.close("again", true);
    assert_eq!(1, on_close_handler_calls.load());
    assert_eq!(1, dt1.close_call_count());

    // From the code's meaning: nothing holds the receiver or its buffer afterwards. The
    // forwarder is what held the buffer besides the receiver, so a buffer that is gone is a
    // forwarder that ended.
    let TestReceiver { w, buff, .. } = r;
    let weak_w = Arc::downgrade(&w);
    let weak_base = Arc::downgrade(w.receiver_base());
    let weak_buff = Arc::downgrade(&buff);
    drop(w);
    drop(buff);
    eventually(|| {
        weak_w.upgrade().is_none() && weak_base.upgrade().is_none() && weak_buff.upgrade().is_none()
    })
    .await;
}

// New for musivum.
#[tokio::test(flavor = "multi_thread")]
async fn a_forwarder_that_waits_for_packets_does_not_hold_its_receiver() {
    let r = TestReceiver::new(&simulcast_track_info(), "h");
    let dt1 = new_sender("sub1");
    assert_eq!(Ok(()), r.w.add_down_track(dt1.clone()));
    for i in 0..3 {
        r.write(i);
    }
    eventually(|| dt1.write_rtp_call_count() == 3).await;

    // From the code's meaning: a receiver that is closed without closing its buffers leaves
    // its forwarder waiting at the buffer, which is not the receiver's to close. The
    // forwarder does not keep the receiver alive, and ends when the buffer is closed.
    r.w.close("test", false);
    assert_eq!(1, dt1.close_call_count());
    let TestReceiver { w, buff, .. } = r;
    let weak_w = Arc::downgrade(&w);
    let weak_base = Arc::downgrade(w.receiver_base());
    drop(w);
    eventually(|| weak_w.upgrade().is_none() && weak_base.upgrade().is_none()).await;

    let weak_buff = Arc::downgrade(&buff);
    assert!(buff.close().is_ok());
    drop(buff);
    eventually(|| weak_buff.upgrade().is_none()).await;
}

// New for musivum.
#[tokio::test(flavor = "multi_thread")]
async fn a_buffer_that_is_closed_closes_its_receiver() {
    let r = TestReceiver::new(&single_layer_track_info(), "");
    let on_close_handler_calls = Arc::new(atomic::Uint32::new(0));
    {
        let on_close_handler_calls = on_close_handler_calls.clone();
        r.w.on_close_handler(move || {
            on_close_handler_calls.inc();
        });
    }
    let dt1 = new_sender("sub1");
    assert_eq!(Ok(()), r.w.add_down_track(dt1.clone()));
    for i in 0..3 {
        r.write(i);
    }
    eventually(|| dt1.write_rtp_call_count() == 3).await;

    assert!(r.buff.close().is_ok());
    eventually(|| r.w.is_closed()).await;
    eventually(|| on_close_handler_calls.load() == 1).await;

    // From the original (a `ReceiverBase` that is self closing, as a WebRTC receiver's is):
    // the three packets came with layer 0, the forwarder that found its buffer closed closed
    // the receiver, which closed its down track and called `OnClosed` once; the buffer stays
    // where it was.
    assert_eq!(vec![1000, 1001, 1002], sequence_numbers(&dt1));
    assert!(written(&dt1).iter().all(|(_, layer)| *layer == 0));
    assert_eq!(1, dt1.close_call_count());
    assert!(r.w.get_down_tracks().is_empty());
    assert!(r.w.receiver_base().get_all_buffers()[0].is_some());
    assert_eq!(1, on_close_handler_calls.load());
    // From the code's meaning: the one up track is still counted, and a read from the buffer
    // that is closed says so with the error that a down track takes for the end.
    assert_eq!(1, r.w.num_up_tracks());
    let mut buf = vec![0u8; 1500];
    assert_eq!(Err(Error::Eof), r.w.read_rtp(&mut buf, 0, 1001));
}
