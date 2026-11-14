// Ported from LiveKit, pkg/rtc/mediatrack_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests of `TestGetQualityForDimension` are
// blocks that carry their names in the assertion messages. The tests after the two of the
// original are new for musivum, and say so.

use std::any::Any;
use std::time::Duration as StdDuration;

use parking_lot::Mutex as PlMutex;

use super::*;
use crate::protocol::logger;
use crate::rtc::DummyReceiver;
use crate::rtc::types::MediaTrack as _;
use crate::sfu::buffer::BufferPacketType;
use crate::testutils::eventually_async;

#[test]
fn test_track_info() {
    // ensures that persisted trackinfo is being returned
    let ti = livekit::TrackInfo {
        sid: "testsid".to_string(),
        name: "testtrack".to_string(),
        source: livekit::TrackSource::ScreenShare as i32,
        r#type: livekit::TrackType::Video as i32,
        simulcast: false,
        width: 100,
        height: 80,
        muted: true,
        ..Default::default()
    };

    let mt = MediaTrack::new(MediaTrackParams::default(), &ti);
    let out_info = mt.to_proto();
    assert_eq!(ti.muted, out_info.muted);
    assert_eq!(ti.name, out_info.name);
    assert_eq!(ti.name, mt.name());
    assert_eq!(livekit::TrackId::from(ti.sid.as_str()), mt.id());
    assert_eq!(ti.r#type, out_info.r#type);
    assert_eq!(ti.r#type(), mt.kind());
    assert_eq!(ti.source, out_info.source);
    assert_eq!(ti.width, out_info.width);
    assert_eq!(ti.height, out_info.height);
    assert_eq!(ti.simulcast, out_info.simulcast);
}

fn video_layer(quality: livekit::VideoQuality, width: u32, height: u32) -> livekit::VideoLayer {
    livekit::VideoLayer {
        quality: quality as i32,
        width,
        height,
        ..Default::default()
    }
}

#[test]
fn test_get_quality_for_dimension() {
    use livekit::VideoQuality::{High, Low, Medium};
    use mime::MimeType::{H264, Vp8};

    {
        let name = "landscape source";
        let mt = MediaTrack::new(
            MediaTrackParams {
                logger: logger::get_logger(),
                ..Default::default()
            },
            &livekit::TrackInfo {
                r#type: livekit::TrackType::Video as i32,
                width: 1080,
                height: 720,
                ..Default::default()
            },
        );

        assert_eq!(Low, mt.get_quality_for_dimension(Vp8, 120, 120), "{name}");
        assert_eq!(Low, mt.get_quality_for_dimension(Vp8, 300, 200), "{name}");
        assert_eq!(
            Medium,
            mt.get_quality_for_dimension(Vp8, 200, 250),
            "{name}"
        );
        assert_eq!(High, mt.get_quality_for_dimension(Vp8, 700, 480), "{name}");
        assert_eq!(High, mt.get_quality_for_dimension(Vp8, 500, 1000), "{name}");
    }

    {
        let name = "portrait source";
        let mt = MediaTrack::new(
            MediaTrackParams {
                logger: logger::get_logger(),
                ..Default::default()
            },
            &livekit::TrackInfo {
                r#type: livekit::TrackType::Video as i32,
                width: 540,
                height: 960,
                ..Default::default()
            },
        );

        assert_eq!(Low, mt.get_quality_for_dimension(Vp8, 200, 400), "{name}");
        assert_eq!(
            Medium,
            mt.get_quality_for_dimension(Vp8, 400, 400),
            "{name}"
        );
        assert_eq!(
            Medium,
            mt.get_quality_for_dimension(Vp8, 400, 700),
            "{name}"
        );
        assert_eq!(High, mt.get_quality_for_dimension(Vp8, 600, 900), "{name}");
    }

    {
        let name = "layers provided";
        let mt = MediaTrack::new(
            MediaTrackParams {
                logger: logger::get_logger(),
                ..Default::default()
            },
            &livekit::TrackInfo {
                r#type: livekit::TrackType::Video as i32,
                width: 1080,
                height: 720,
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: H264.to_string(),
                    layers: vec![
                        video_layer(Low, 480, 270),
                        video_layer(Medium, 960, 540),
                        video_layer(High, 1080, 720),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        assert_eq!(Low, mt.get_quality_for_dimension(H264, 120, 120), "{name}");
        assert_eq!(Low, mt.get_quality_for_dimension(H264, 300, 300), "{name}");
        assert_eq!(
            Medium,
            mt.get_quality_for_dimension(H264, 800, 500),
            "{name}"
        );
        assert_eq!(
            High,
            mt.get_quality_for_dimension(H264, 1000, 700),
            "{name}"
        );
    }

    {
        let name = "highest layer with smallest dimensions";
        let mut mt = MediaTrack::new(
            MediaTrackParams {
                logger: logger::get_logger(),
                ..Default::default()
            },
            &livekit::TrackInfo {
                r#type: livekit::TrackType::Video as i32,
                width: 1080,
                height: 720,
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: H264.to_string(),
                    layers: vec![
                        video_layer(Low, 480, 270),
                        video_layer(Medium, 1080, 720),
                        video_layer(High, 1080, 720),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        assert_eq!(Low, mt.get_quality_for_dimension(H264, 120, 120), "{name}");
        assert_eq!(Low, mt.get_quality_for_dimension(H264, 300, 300), "{name}");
        assert_eq!(High, mt.get_quality_for_dimension(H264, 800, 500), "{name}");
        assert_eq!(
            High,
            mt.get_quality_for_dimension(H264, 1000, 700),
            "{name}"
        );
        assert_eq!(
            High,
            mt.get_quality_for_dimension(H264, 1200, 800),
            "{name}"
        );

        mt = MediaTrack::new(
            MediaTrackParams {
                logger: logger::get_logger(),
                ..Default::default()
            },
            &livekit::TrackInfo {
                r#type: livekit::TrackType::Video as i32,
                width: 1080,
                height: 720,
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: H264.to_string(),
                    layers: vec![
                        video_layer(Low, 480, 270),
                        video_layer(Medium, 480, 270),
                        video_layer(High, 1080, 720),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        assert_eq!(
            Medium,
            mt.get_quality_for_dimension(H264, 120, 120),
            "{name}"
        );
        assert_eq!(
            Medium,
            mt.get_quality_for_dimension(H264, 300, 300),
            "{name}"
        );
        assert_eq!(High, mt.get_quality_for_dimension(H264, 800, 500), "{name}");
        assert_eq!(
            High,
            mt.get_quality_for_dimension(H264, 1000, 700),
            "{name}"
        );
        assert_eq!(
            High,
            mt.get_quality_for_dimension(H264, 1200, 800),
            "{name}"
        );
    }
}

// -------------------------------------------------------------------
// New for musivum: `add_receiver` with real buffers from a buffer factory, behind a fake
// `webrtc::RtpReceiver` and `sfu::TrackRemote`, and what follows from it (dynacast, muting,
// closing). The original has no test of these.
//
// The same steps were run against the Go original: a test laid over `pkg/rtc`, with pion's
// `RTPReceiver` made from an API whose media engine has the codecs the fake receiver here
// gives, a `sfu.TrackRemote` of its own and LiveKit's buffer factory, which printed a line
// after each step (`state` and the `on_subscribed_max_quality_change` of `params` below
// print the same fields the same way). Where a test says "the original printed", the
// expected text is that line. One thing the original was given that is not ported: a no-op
// reporter (`roomobs.NewNoopTrackReporter()`), without which it does not add a receiver.

struct TestRtpReceiver {
    parameters: webrtc::RtpParameters,
}

impl webrtc::RtpReceiver for TestRtpReceiver {
    fn get_parameters(&self) -> webrtc::RtpParameters {
        self.parameters.clone()
    }
}

struct TestTrackRemote {
    id: String,
    rid: String,
    ssrc: u32,
    kind: webrtc::RtpCodecType,
    codec: webrtc::RtpCodecParameters,
}

impl sfu::TrackRemote for TestTrackRemote {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn rid(&self) -> String {
        self.rid.clone()
    }
    fn msid(&self) -> String {
        format!("stream {}", self.id)
    }
    fn ssrc(&self) -> webrtc::Ssrc {
        self.ssrc
    }
    fn rtx_ssrc(&self) -> webrtc::Ssrc {
        0
    }
    fn stream_id(&self) -> String {
        "stream".to_string()
    }
    fn kind(&self) -> webrtc::RtpCodecType {
        self.kind
    }
    fn codec(&self) -> webrtc::RtpCodecParameters {
        self.codec.clone()
    }
    fn rtc_track(&self) -> Arc<dyn webrtc::TrackRemote> {
        unreachable!("not called by add_receiver")
    }
}

fn codec(mime_type: &str, pt: u8, clock: u32, ch: u16) -> webrtc::RtpCodecParameters {
    webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: mime_type.to_string(),
            clock_rate: clock,
            channels: ch,
            ..Default::default()
        },
        payload_type: pt,
    }
}

fn vp8() -> webrtc::RtpCodecParameters {
    codec("video/VP8", 96, 90000, 0)
}

fn h264() -> webrtc::RtpCodecParameters {
    codec("video/H264", 102, 90000, 0)
}

fn red() -> webrtc::RtpCodecParameters {
    codec("audio/red", 63, 48000, 2)
}

fn opus() -> webrtc::RtpCodecParameters {
    codec("audio/opus", 111, 48000, 2)
}

// What pion's receiver gives for its kind, from a media engine with these four codecs.
fn rtp_receiver(kind: webrtc::RtpCodecType) -> Arc<dyn webrtc::RtpReceiver> {
    let codecs = if kind == webrtc::RtpCodecType::Video {
        vec![vp8(), h264()]
    } else {
        vec![red(), opus()]
    };
    Arc::new(TestRtpReceiver {
        parameters: webrtc::RtpParameters {
            header_extensions: Vec::new(),
            codecs,
        },
    })
}

fn track_remote(
    id: &str,
    rid: &str,
    ssrc: u32,
    codec: webrtc::RtpCodecParameters,
) -> Arc<dyn sfu::TrackRemote> {
    let kind = if codec.mime_type.starts_with("video") {
        webrtc::RtpCodecType::Video
    } else {
        webrtc::RtpCodecType::Audio
    };
    Arc::new(TestTrackRemote {
        id: id.to_string(),
        rid: rid.to_string(),
        ssrc,
        kind,
        codec,
    })
}

// The buffers and RTCP readers of the SSRCs, as the connection's sink makes them before it
// announces a stream.
fn factory(ssrcs: &[u32]) -> Arc<buffer::Factory> {
    let f = buffer::FactoryOfBufferFactory::new(500, 200).create_buffer_factory();
    for &ssrc in ssrcs {
        f.get_or_new(BufferPacketType::Rtp, ssrc);
        f.get_or_new(BufferPacketType::Rtcp, ssrc);
    }
    f
}

fn layers() -> Vec<livekit::VideoLayer> {
    let layer =
        |quality: livekit::VideoQuality, width, height, bitrate, spatial_layer, rid: &str| {
            livekit::VideoLayer {
                quality: quality as i32,
                width,
                height,
                bitrate,
                spatial_layer,
                rid: rid.to_string(),
                ..Default::default()
            }
        };
    vec![
        layer(livekit::VideoQuality::Low, 320, 180, 150000, 0, "q"),
        layer(livekit::VideoQuality::Medium, 640, 360, 500000, 1, "h"),
        layer(livekit::VideoQuality::High, 1280, 720, 1500000, 2, "f"),
    ]
}

fn simulcast_info() -> livekit::TrackInfo {
    livekit::TrackInfo {
        sid: "TR_video".to_string(),
        name: "camera".to_string(),
        r#type: livekit::TrackType::Video as i32,
        source: livekit::TrackSource::Camera as i32,
        width: 1280,
        height: 720,
        codecs: vec![livekit::SimulcastCodecInfo {
            mime_type: "video/VP8".to_string(),
            cid: "cid-vp8".to_string(),
            layers: layers(),
            ..Default::default()
        }],
        layers: layers(),
        ..Default::default()
    }
}

type Lines = Arc<PlMutex<Vec<String>>>;

fn params(
    f: &Arc<buffer::Factory>,
    tl: &Arc<types::FakeParticipantTelemetryListener>,
    lines: &Lines,
) -> MediaTrackParams {
    let tl = tl.clone();
    let lines = lines.clone();
    MediaTrackParams {
        participant_id: Some(Arc::new(|| "PA_pub".into())),
        participant_identity: "pub".into(),
        participant_version: 1,
        buffer_factory: Some(f.clone()),
        receiver_config: ReceiverConfig {
            packet_buffer_size_video: 500,
            packet_buffer_size_audio: 200,
        },
        audio_config: sfu::DEFAULT_AUDIO_CONFIG,
        video_config: config::VideoConfig {
            stream_tracker_manager: sfu::DEFAULT_STREAM_TRACKER_MANAGER_CONFIG.clone(),
            ..Default::default()
        },
        telemetry_listener: Some(Arc::new(move || tl.clone())),
        logger: logger::get_logger(),
        should_regress_codec: Some(Arc::new(|| false)),
        on_subscribed_max_quality_change: Some(Arc::new(
            move |track_id: &livekit::TrackId,
                  ti: &livekit::TrackInfo,
                  subscribed_qualities: &[livekit::SubscribedCodec],
                  max_subscribed_qualities: &[types::SubscribedCodecQuality]| {
                let mut sq: Vec<String> = subscribed_qualities
                    .iter()
                    .map(|c| {
                        let qs: Vec<String> = c
                            .qualities
                            .iter()
                            .map(|q| format!("{}={}", q.quality().as_str_name(), q.enabled))
                            .collect();
                        format!("{}[{}]", c.codec, qs.join(","))
                    })
                    .collect();
                sq.sort();
                let mut mq: Vec<String> = max_subscribed_qualities
                    .iter()
                    .map(|q| format!("{}={}", q.codec_mime, q.quality.as_str_name()))
                    .collect();
                mq.sort();
                lines.lock().push(format!(
                    "track={} muted={} subscribed={} max={}",
                    track_id.as_str(),
                    ti.muted,
                    sq.join(";"),
                    mq.join(";")
                ));
                Ok(())
            },
        )),
        ..Default::default()
    }
}

fn web_rtc_receiver(r: &Arc<dyn sfu::TrackReceiver>) -> Option<Arc<sfu::WebRtcReceiver>> {
    let r: Arc<dyn Any + Send + Sync> = r.clone();
    r.downcast::<sfu::WebRtcReceiver>().ok()
}

fn primary(mt: &MediaTrack) -> Arc<sfu::WebRtcReceiver> {
    web_rtc_receiver(&mt.media_track_receiver().primary_receiver().unwrap()).unwrap()
}

fn state(mt: &MediaTrack) -> String {
    let ti = mt.to_proto();
    let mut rs = Vec::new();
    for r in mt.receivers() {
        let any: Arc<dyn Any + Send + Sync> = r.clone();
        if let Some(w) = web_rtc_receiver(&r) {
            rs.push(format!(
                "{}=web(up={},state={},closed={})",
                r.mime(),
                w.num_up_tracks(),
                w.codec_state() as i32,
                w.is_closed()
            ));
        } else if let Ok(d) = any.downcast::<DummyReceiver>() {
            rs.push(format!("{}=dummy({})", r.mime(), d.receiver().is_some()));
        } else {
            rs.push(format!("{}=other", r.mime()));
        }
    }
    let layers = |ls: &[livekit::VideoLayer]| {
        ls.iter()
            .map(|l| format!("{}:{}/{}", l.rid, l.ssrc, l.repair_ssrc))
            .collect::<Vec<_>>()
            .join(",")
    };
    let cs: Vec<String> = ti
        .codecs
        .iter()
        .map(|c| format!("{}|mid={}|{}", c.mime_type, c.mid, layers(&c.layers)))
        .collect();
    format!(
        "receivers={} mime={} mid={} simulcast={} muted={} open={} pending={} codecs={} layers={}",
        rs.join(","),
        ti.mime_type,
        ti.mid,
        ti.simulcast,
        ti.muted,
        mt.is_open(),
        mt.has_pending_codec(),
        cs.join(" "),
        layers(&ti.layers)
    )
}

fn buffers(mt: &MediaTrack) -> String {
    let layers: Vec<String> = primary(mt)
        .receiver_base()
        .get_all_buffers()
        .iter()
        .enumerate()
        .filter(|(_, b)| b.is_some())
        .map(|(i, _)| i.to_string())
        .collect();
    format!("buffers={}", layers.join(","))
}

// A camera track in VP8 with three simulcast layers, "q", "h" and "f" on 1001 to 1003, and
// what the description says of the streams of two mids.
fn simulcast_track(f: &Arc<buffer::Factory>) -> Arc<MediaTrack> {
    let tl = Arc::new(types::FakeParticipantTelemetryListener::default());
    let mut p = params(f, &tl, &Lines::default());
    let sim_track = |mid: &str, stream_id: &str, repair_ssrc: u32, is_repair_stream: bool| {
        interceptor::SimulcastTrackInfo {
            mid: mid.to_string(),
            stream_id: stream_id.to_string(),
            repair_ssrc,
            is_repair_stream,
        }
    };
    p.sim_tracks = HashMap::from([
        (1001, sim_track("0", "q", 1101, false)),
        (1101, sim_track("0", "q", 0, true)),
        (1002, sim_track("5", "h", 1102, false)),
    ]);
    MediaTrack::new(p, &simulcast_info())
}

// Expected values: the original printed them (lines 0 to 4 of its simulcast test). That the
// buffers are the factory's is not in what it printed.
#[tokio::test(flavor = "multi_thread")]
async fn add_receiver_makes_one_receiver_with_a_buffer_per_simulcast_layer() {
    let f = factory(&[1001, 1002, 1003]);
    let mt = simulcast_track(&f);
    let recv = rtp_receiver(webrtc::RtpCodecType::Video);
    let track = |rid: &str, ssrc: u32| track_remote("cid-vp8", rid, ssrc, vp8());

    assert_eq!(
        "receivers= mime= mid= simulcast=false muted=false open=true pending=true codecs=video/VP8|mid=|q:0/0,h:0/0,f:0/0 layers=q:0/0,h:0/0,f:0/0",
        state(&mt)
    );
    let (new_codec, added) = mt.add_receiver(recv.clone(), track("q", 1001), "0");
    assert_eq!(
        "new=true added=true receivers=video/VP8=dummy(true) mime=video/VP8 mid=0 simulcast=false muted=false open=true pending=false codecs=video/VP8|mid=0|q:1001/1101,h:0/0,f:0/0 layers=q:1001/1101,h:0/0,f:0/0 buffers=0",
        format!(
            "new={new_codec} added={added} {} {}",
            state(&mt),
            buffers(&mt)
        )
    );
    let (new_codec, added) = mt.add_receiver(recv.clone(), track("h", 1002), "0");
    assert_eq!(
        "new=false added=true receivers=video/VP8=dummy(true) mime=video/VP8 mid=0 simulcast=true muted=false open=true pending=false codecs=video/VP8|mid=0|q:1001/1101,h:1002/0,f:0/0 layers=q:1001/1101,h:1002/0,f:0/0 buffers=0,1",
        format!(
            "new={new_codec} added={added} {} {}",
            state(&mt),
            buffers(&mt)
        )
    );
    let (new_codec, added) = mt.add_receiver(recv.clone(), track("f", 1003), "0");
    assert_eq!(
        "new=false added=true receivers=video/VP8=dummy(true) mime=video/VP8 mid=0 simulcast=true muted=false open=true pending=false codecs=video/VP8|mid=0|q:1001/1101,h:1002/0,f:1003/0 layers=q:1001/1101,h:1002/0,f:1003/0 buffers=0,1,2",
        format!(
            "new={new_codec} added={added} {} {}",
            state(&mt),
            buffers(&mt)
        )
    );
    let (score, quality) = mt.get_connection_score_and_quality();
    let (sig, sdp) = mt.get_cids_for_mime_type(mime::MimeType::Vp8);
    assert_eq!(
        "score=4.5 quality=EXCELLENT signal=true sdp=true cids=cid-vp8/",
        format!(
            "score={score} quality={} signal={} sdp={} cids={sig}/{sdp}",
            quality.as_str_name(),
            mt.has_signal_cid("cid-vp8"),
            mt.has_sdp_cid("cid-vp8")
        )
    );

    // the layers' buffers are the factory's
    let all = primary(&mt).receiver_base().get_all_buffers();
    for (layer, ssrc) in [(0, 1001), (1, 1002), (2, 1003)] {
        let b: Arc<dyn buffer::BufferProvider> = f.get_buffer(ssrc).unwrap();
        assert!(
            Arc::ptr_eq(all[layer].as_ref().unwrap(), &b),
            "layer {layer}"
        );
    }

    mt.close(false);
}

// The connection layer announces a stream again after every renegotiation. Expected values:
// unchanged from line 3 of the original's simulcast test. The original itself differs here
// on purpose (see the header of mediatrack.rs): it printed "5: new=false added=false ..."
// and, half a second later, "6: receivers= ... open=false pending=true ...", the track
// closed.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_announced_again_is_left_as_it_is() {
    let f = factory(&[1001, 1002, 1003]);
    let mt = simulcast_track(&f);
    let recv = rtp_receiver(webrtc::RtpCodecType::Video);
    let track = |rid: &str, ssrc: u32| track_remote("cid-vp8", rid, ssrc, vp8());
    for (rid, ssrc) in [("q", 1001), ("h", 1002), ("f", 1003)] {
        assert_eq!(
            (rid == "q", true),
            mt.add_receiver(recv.clone(), track(rid, ssrc), "0")
        );
    }
    let after_three = "receivers=video/VP8=dummy(true) mime=video/VP8 mid=0 simulcast=true muted=false open=true pending=false codecs=video/VP8|mid=0|q:1001/1101,h:1002/0,f:1003/0 layers=q:1001/1101,h:1002/0,f:1003/0 buffers=0,1,2";
    assert_eq!(after_three, format!("{} {}", state(&mt), buffers(&mt)));

    for _ in 0..2 {
        for (rid, ssrc) in [("q", 1001), ("f", 1003)] {
            assert_eq!(
                (false, true),
                mt.add_receiver(recv.clone(), track(rid, ssrc), "0")
            );
        }
    }
    assert_eq!(after_three, format!("{} {}", state(&mt), buffers(&mt)));
    tokio::time::sleep(StdDuration::from_millis(500)).await;
    assert_eq!(after_three, format!("{} {}", state(&mt), buffers(&mt)));
    // the buffer still takes packets: it was not closed
    assert!(
        f.get_buffer(1001)
            .unwrap()
            .write(bytes::Bytes::from_static(&[
                0x80, 96, 0, 1, 0, 0, 0, 1, 0, 0, 3, 0xe9
            ]))
            .is_ok()
    );

    mt.close(false);
}

// A track that comes in VP8 and has H264 as the codec to regress to. Expected values: the
// original printed them (its backup codec test). The codec states behind the receivers are
// not in what it printed.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_codec_arriving_later_is_set_up_as_the_backup_receiver() {
    let f = factory(&[3001, 3002]);
    let tl = Arc::new(types::FakeParticipantTelemetryListener::default());
    let lines = Lines::default();
    let single = || {
        vec![livekit::VideoLayer {
            quality: livekit::VideoQuality::High as i32,
            width: 1280,
            height: 720,
            bitrate: 1500000,
            ..Default::default()
        }]
    };
    let codec_info = |mime_type: &str, cid: &str| livekit::SimulcastCodecInfo {
        mime_type: mime_type.to_string(),
        cid: cid.to_string(),
        layers: single(),
        ..Default::default()
    };
    let ti = livekit::TrackInfo {
        sid: "TR_backup".to_string(),
        r#type: livekit::TrackType::Video as i32,
        source: livekit::TrackSource::Camera as i32,
        width: 1280,
        height: 720,
        backup_codec_policy: livekit::BackupCodecPolicy::Regression as i32,
        codecs: vec![
            codec_info("video/VP8", "cid-vp8"),
            codec_info("video/H264", "cid-h264"),
        ],
        layers: single(),
        ..Default::default()
    };
    let mt = MediaTrack::new(params(&f, &tl, &lines), &ti);
    let recv = rtp_receiver(webrtc::RtpCodecType::Video);

    let (new_codec, added) =
        mt.add_receiver(recv.clone(), track_remote("cid-vp8", "", 3001, vp8()), "0");
    assert_eq!(
        "new=true added=true receivers=video/VP8=dummy(true),video/H264=dummy(false) mime=video/VP8 mid=0 simulcast=false muted=false open=true pending=false codecs=video/VP8|mid=0|:3001/0 video/H264|mid=|:0/0 layers=:3001/0",
        format!("new={new_codec} added={added} {}", state(&mt))
    );
    let (new_codec, added) = mt.add_receiver(
        recv.clone(),
        track_remote("cid-h264", "", 3002, h264()),
        "1",
    );
    assert_eq!(
        "new=true added=true receivers=video/VP8=dummy(true),video/H264=dummy(true) mime=video/VP8 mid=0 simulcast=false muted=false open=true pending=false codecs=video/VP8|mid=0|:3001/0 video/H264|mid=1|:3002/0 layers=:3001/0 active=video/VP8",
        format!(
            "new={new_codec} added={added} {} active={}",
            state(&mt),
            mt.media_track_receiver().active_receiver().unwrap().mime()
        )
    );
    let codec_state =
        |m: mime::MimeType| mt.media_track_receiver().receiver(m).unwrap().codec_state();
    assert_eq!(
        sfu::ReceiverCodecState::Suspended,
        codec_state(mime::MimeType::Vp8)
    );
    assert_eq!(
        sfu::ReceiverCodecState::Normal,
        codec_state(mime::MimeType::H264)
    );

    // the regression reached dynacast, which told the listener
    eventually_async(
        || !lines.lock().is_empty(),
        StdDuration::from_secs(5),
        StdDuration::from_millis(10),
    )
    .await;
    assert_eq!(
        "track=TR_backup muted=false subscribed=video/H264[LOW=true,MEDIUM=true,HIGH=true];video/VP8[LOW=false,MEDIUM=false,HIGH=false] max=video/H264=HIGH;video/VP8=OFF",
        lines.lock().join(" | ")
    );

    mt.close(false);
}

fn type_name(r: &Arc<dyn sfu::TrackReceiver>) -> &'static str {
    let any: Arc<dyn Any + Send + Sync> = r.clone();
    if any.is::<sfu::RedPrimaryReceiver>() {
        "*sfu.RedPrimaryReceiver"
    } else if any.is::<sfu::RedReceiver>() {
        "*sfu.RedReceiver"
    } else {
        "other"
    }
}

// An audio track published as RED, and one published as Opus. Expected values: the original
// printed them (its RED test).
#[tokio::test(flavor = "multi_thread")]
async fn red_and_opus_tracks_get_their_red_receivers() {
    let f = factory(&[4001, 4002]);
    let tl = Arc::new(types::FakeParticipantTelemetryListener::default());
    let recv = rtp_receiver(webrtc::RtpCodecType::Audio);
    let audio_info = |sid: &str, mime_type: &str, cid: &str| livekit::TrackInfo {
        sid: sid.to_string(),
        r#type: livekit::TrackType::Audio as i32,
        source: livekit::TrackSource::Microphone as i32,
        codecs: vec![livekit::SimulcastCodecInfo {
            mime_type: mime_type.to_string(),
            cid: cid.to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };

    let red_track = MediaTrack::new(
        params(&f, &tl, &Lines::default()),
        &audio_info("TR_red", "audio/red", "cid-red"),
    );
    let (new_codec, added) =
        red_track.add_receiver(recv.clone(), track_remote("cid-red", "", 4001, red()), "0");
    let p: Arc<dyn sfu::TrackReceiver> = primary(&red_track);
    assert_eq!(
        "new=true added=true receivers=audio/red=dummy(true) mime=audio/red mid=0 simulcast=false muted=false open=true pending=false codecs=audio/red|mid=0| layers= primaryForRed=*sfu.RedPrimaryReceiver same=true red=false",
        format!(
            "new={new_codec} added={added} {} primaryForRed={} same={} red={}",
            state(&red_track),
            type_name(&p.get_primary_receiver_for_red()),
            Arc::ptr_eq(
                &p.get_primary_receiver_for_red(),
                &p.get_primary_receiver_for_red()
            ),
            Arc::ptr_eq(&p.get_red_receiver(), &p),
        )
    );

    let opus_track = MediaTrack::new(
        params(&f, &tl, &Lines::default()),
        &audio_info("TR_opus", "audio/opus", "cid-opus"),
    );
    let (new_codec, added) = opus_track.add_receiver(
        recv.clone(),
        track_remote("cid-opus", "", 4002, opus()),
        "1",
    );
    let p: Arc<dyn sfu::TrackReceiver> = primary(&opus_track);
    assert_eq!(
        "new=true added=true receivers=audio/opus=dummy(true) mime=audio/opus mid=1 simulcast=false muted=false open=true pending=false codecs=audio/opus|mid=1| layers= red=*sfu.RedReceiver same=true primaryForRed=false",
        format!(
            "new={new_codec} added={added} {} red={} same={} primaryForRed={}",
            state(&opus_track),
            type_name(&p.get_red_receiver()),
            Arc::ptr_eq(&p.get_red_receiver(), &p.get_red_receiver()),
            Arc::ptr_eq(&p.get_primary_receiver_for_red(), &p),
        )
    );

    red_track.close(false);
    opus_track.close(false);
}

// The simulcast track with its three layers added, the lines its dynacast listener gets,
// and its receiver.
fn simulcast_track_with_layers() -> (
    Arc<buffer::Factory>,
    Arc<MediaTrack>,
    Lines,
    Arc<sfu::WebRtcReceiver>,
) {
    let f = factory(&[1001, 1002, 1003]);
    let tl = Arc::new(types::FakeParticipantTelemetryListener::default());
    let lines = Lines::default();
    let mt = MediaTrack::new(params(&f, &tl, &lines), &simulcast_info());
    let recv = rtp_receiver(webrtc::RtpCodecType::Video);
    for (i, rid) in ["q", "h", "f"].into_iter().enumerate() {
        mt.add_receiver(
            recv.clone(),
            track_remote("cid-vp8", rid, 1001 + i as u32, vp8()),
            "0",
        );
    }
    let wr = primary(&mt);
    (f, mt, lines, wr)
}

// Expected values: the original printed them (lines 1 and 2 of its mute and dynacast test).
#[tokio::test(flavor = "multi_thread")]
async fn muting_and_unmuting_reach_the_receiver_and_the_track_info() {
    let (_f, mt, _lines, wr) = simulcast_track_with_layers();
    let muted = || {
        format!(
            "muted={} info={} receiver={}",
            mt.is_muted(),
            mt.to_proto().muted,
            wr.track_info().muted
        )
    };

    mt.set_muted(true);
    assert_eq!("muted=true info=true receiver=true", muted());
    mt.set_muted(false);
    assert_eq!("muted=false info=false receiver=false", muted());

    mt.close(false);
}

// Expected values: the original printed them (lines 3 and 4 of its mute and dynacast test,
// whose steps this repeats). The receiver's expected layer is read by setting the next one,
// which gives the one before.
#[tokio::test(flavor = "multi_thread")]
async fn dynacast_tells_the_listener_the_maximum_subscribed_quality() {
    let (_f, mt, lines, wr) = simulcast_track_with_layers();
    mt.set_muted(true);
    mt.set_muted(false);

    mt.notify_subscriber_node_max_quality(
        &"ND_1".into(),
        &[types::SubscribedCodecQuality {
            codec_mime: mime::MimeType::Vp8,
            quality: livekit::VideoQuality::Medium,
        }],
    );
    eventually_async(
        || !lines.lock().is_empty(),
        StdDuration::from_secs(5),
        StdDuration::from_millis(10),
    )
    .await;
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    assert_eq!(
        "track=TR_video muted=false subscribed=video/VP8[LOW=true,MEDIUM=true,HIGH=false] max=video/VP8=MEDIUM expected=1",
        format!(
            "{} expected={}",
            lines.lock().join(" | "),
            wr.stream_tracker_manager()
                .set_max_expected_spatial_layer(1)
        )
    );

    // muted: the publisher is not told, the receiver is
    mt.set_muted(true);
    mt.notify_subscriber_node_max_quality(
        &"ND_1".into(),
        &[types::SubscribedCodecQuality {
            codec_mime: mime::MimeType::Vp8,
            quality: livekit::VideoQuality::High,
        }],
    );
    tokio::time::sleep(StdDuration::from_millis(300)).await;
    assert_eq!(
        "calls=1 expected=2",
        format!(
            "calls={} expected={}",
            lines.lock().len(),
            wr.stream_tracker_manager()
                .set_max_expected_spatial_layer(2)
        )
    );

    mt.close(false);
}

fn camera_info(sid: &str) -> livekit::TrackInfo {
    livekit::TrackInfo {
        sid: sid.to_string(),
        r#type: livekit::TrackType::Video as i32,
        source: livekit::TrackSource::Camera as i32,
        width: 1280,
        height: 720,
        codecs: vec![livekit::SimulcastCodecInfo {
            mime_type: "video/VP8".to_string(),
            cid: format!("cid-{sid}"),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn on_close_lines(mt: &MediaTrack) -> Lines {
    let lines = Lines::default();
    let l = lines.clone();
    mt.add_on_close(Some(Box::new(move |is_expected_to_resume: bool| {
        l.lock().push(format!("closed({is_expected_to_resume})"));
    })));
    lines
}

// The buffer of a track's only stream is closed (as the connection's sink does when the
// stream ends): its receiver closes, and with it the track. Expected values: the original
// printed them (lines 1 and 2 of its close test; a buffer that never had a packet has no
// final statistics to report).
#[tokio::test(flavor = "multi_thread")]
async fn closing_the_buffer_closes_the_receiver_and_the_track() {
    let f = factory(&[5001]);
    let tl = Arc::new(types::FakeParticipantTelemetryListener::default());
    let recv = rtp_receiver(webrtc::RtpCodecType::Video);
    let mt = MediaTrack::new(params(&f, &tl, &Lines::default()), &camera_info("TR_a"));
    let closed = on_close_lines(&mt);
    mt.add_receiver(recv, track_remote("cid-TR_a", "", 5001, vp8()), "0");
    let wr = primary(&mt);
    assert_eq!(
        "receivers=video/VP8=dummy(true) mime=video/VP8 mid=0 simulcast=false muted=false open=true pending=false codecs=video/VP8|mid=0| layers=",
        state(&mt)
    );

    f.get_buffer(5001).unwrap().close().unwrap();
    eventually_async(
        || !closed.lock().is_empty(),
        StdDuration::from_secs(5),
        StdDuration::from_millis(10),
    )
    .await;
    assert_eq!(
        "receivers= mime=video/VP8 mid=0 simulcast=false muted=false open=false pending=true codecs=video/VP8|mid=0| layers= receiverClosed=true onClose=closed(false) finalStats=0",
        format!(
            "{} receiverClosed={} onClose={} finalStats={}",
            state(&mt),
            wr.is_closed(),
            closed.lock().join(","),
            tl.on_track_publish_rtp_stats_call_count()
        )
    );
}

// Closing the track closes it and lets go of its receivers, which stay open, as upstream:
// they close when their buffers do. Expected values: the original printed them (line 3 of
// its close test).
#[tokio::test(flavor = "multi_thread")]
async fn closing_the_track_closes_it_and_lets_go_of_its_receivers() {
    let f = factory(&[5002]);
    let tl = Arc::new(types::FakeParticipantTelemetryListener::default());
    let recv = rtp_receiver(webrtc::RtpCodecType::Video);
    let mt = MediaTrack::new(params(&f, &tl, &Lines::default()), &camera_info("TR_b"));
    let closed = on_close_lines(&mt);
    mt.add_receiver(recv, track_remote("cid-TR_b", "", 5002, vp8()), "1");
    let wr = primary(&mt);

    mt.close(true);
    assert_eq!(
        "receivers= mime=video/VP8 mid=1 simulcast=false muted=false open=false pending=true codecs=video/VP8|mid=1| layers= receiverClosed=false onClose=closed(true)",
        format!(
            "{} receiverClosed={} onClose={}",
            state(&mt),
            wr.is_closed(),
            closed.lock().join(",")
        )
    );
}
