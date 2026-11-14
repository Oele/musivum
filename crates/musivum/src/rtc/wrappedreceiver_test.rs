// New for musivum: upstream has no test of wrappedreceiver.go.
//
// The steps of these tests were also run against the Go original (a test laid over
// `pkg/rtc`, with upstream's `typesfakes` and `sfufakes`), which printed a line after each
// step. Where a test says "the original printed", the expected text is that line. The first
// part of this file is what the four test files of this port share: the same fakes, set up
// the same way, and the same lines, as that Go test had.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use super::*;
use crate::protocol::logger;
use crate::rtc::testutils::new_mock_participant;
use crate::rtc::types;
use crate::sfu::pacer;
use crate::sfu::{FakeTrackReceiver, FakeTrackSender, TrackReceiver};
use crate::webrtc::rtp;

// -------------------------------------------------------------------
// shared with the other test files of the port

pub(in crate::rtc) fn codec(
    mime_type: &str,
    pt: u8,
    clock: u32,
    ch: u16,
) -> webrtc::RtpCodecParameters {
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

pub(in crate::rtc) fn opus() -> webrtc::RtpCodecParameters {
    codec("audio/opus", 111, 48000, 2)
}

pub(in crate::rtc) fn red() -> webrtc::RtpCodecParameters {
    codec("audio/red", 63, 48000, 2)
}

pub(in crate::rtc) fn vp8() -> webrtc::RtpCodecParameters {
    codec("video/VP8", 96, 90000, 0)
}

pub(in crate::rtc) fn h264() -> webrtc::RtpCodecParameters {
    codec("video/H264", 102, 90000, 0)
}

pub(in crate::rtc) fn av1() -> webrtc::RtpCodecParameters {
    codec("video/AV1", 35, 90000, 0)
}

pub(in crate::rtc) fn codecs_str(cs: &[webrtc::RtpCodecParameters]) -> String {
    cs.iter()
        .map(|c| format!("{}/{}", c.mime_type, c.payload_type))
        .collect::<Vec<_>>()
        .join(",")
}

// A receiver that answers for its codec and is ready at once.
pub(in crate::rtc) fn receiver(c: &webrtc::RtpCodecParameters) -> Arc<FakeTrackReceiver> {
    let r = Arc::new(FakeTrackReceiver::default());
    r.codec_returns(c.clone());
    r.mime_returns(mime::normalize_mime_type(&c.mime_type));
    r.track_id_returns("TR_fake".into());
    r.stream_id_returns("fake-stream".to_string());
    r.track_info_returns(Arc::new(livekit::TrackInfo::default()));
    r.add_on_ready_calls(|f| f());
    r
}

pub(in crate::rtc) fn sender(id: &str) -> Arc<FakeTrackSender> {
    let s = Arc::new(FakeTrackSender::default());
    s.subscriber_id_returns(id.into());
    s
}

pub(in crate::rtc) fn same_receiver(
    a: &Arc<dyn TrackReceiver>,
    b: &Arc<FakeTrackReceiver>,
) -> bool {
    let b: Arc<dyn TrackReceiver> = b.clone();
    Arc::ptr_eq(a, &b)
}

// Waits for the condition, as the Go test did: whether it came to hold.
pub(in crate::rtc) async fn wait(mut cond: impl FnMut() -> bool) -> bool {
    for _ in 0..2000 {
        if cond() {
            return true;
        }
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
    false
}

pub(in crate::rtc) fn layers() -> Vec<livekit::VideoLayer> {
    let layer = |quality: livekit::VideoQuality, width: u32, height: u32, spatial_layer: i32| {
        livekit::VideoLayer {
            quality: quality as i32,
            width,
            height,
            spatial_layer,
            ..Default::default()
        }
    };
    vec![
        layer(livekit::VideoQuality::Low, 320, 180, 0),
        layer(livekit::VideoQuality::Medium, 640, 360, 1),
        layer(livekit::VideoQuality::High, 1280, 720, 2),
    ]
}

pub(in crate::rtc) fn video_info() -> livekit::TrackInfo {
    livekit::TrackInfo {
        sid: "TR_vid".to_string(),
        r#type: livekit::TrackType::Video as i32,
        name: "cam".to_string(),
        source: livekit::TrackSource::Camera as i32,
        width: 1280,
        height: 720,
        stream: "camera".to_string(),
        codecs: vec![
            livekit::SimulcastCodecInfo {
                mime_type: "video/VP8".to_string(),
                cid: "cid-vp8".to_string(),
                layers: layers(),
                ..Default::default()
            },
            livekit::SimulcastCodecInfo {
                mime_type: "video/H264".to_string(),
                cid: "cid-h264".to_string(),
                layers: layers(),
                ..Default::default()
            },
        ],
        layers: layers(),
        ..Default::default()
    }
}

pub(in crate::rtc) fn audio_info() -> livekit::TrackInfo {
    livekit::TrackInfo {
        sid: "TR_aud".to_string(),
        r#type: livekit::TrackType::Audio as i32,
        name: "mic".to_string(),
        source: livekit::TrackSource::Microphone as i32,
        stream: "camera".to_string(),
        audio_features: vec![livekit::AudioTrackFeature::TfStereo as i32],
        ..Default::default()
    }
}

// A track info as one line: what can change in it.
pub(in crate::rtc) fn dump_ti(ti: &livekit::TrackInfo) -> String {
    let layers = |ls: &[livekit::VideoLayer]| {
        let parts: Vec<String> = ls
            .iter()
            .map(|l| {
                format!(
                    "{}:{}x{}:ssrc{}:rtx{}:rid{}:sl{}",
                    l.quality, l.width, l.height, l.ssrc, l.repair_ssrc, l.rid, l.spatial_layer
                )
            })
            .collect();
        format!("[{}]", parts.join(" "))
    };
    let features: Vec<String> = ti.audio_features.iter().map(|f| f.to_string()).collect();
    let mut s = format!(
        "sid={} mime={} mid={} muted={} simulcast={} {}x{} stereo={} nodtx={} features=[{}] layers={}",
        ti.sid,
        ti.mime_type,
        ti.mid,
        ti.muted,
        ti.simulcast,
        ti.width,
        ti.height,
        ti.stereo,
        ti.disable_dtx,
        features.join(" "),
        layers(&ti.layers)
    );
    for c in &ti.codecs {
        s += &format!(
            " | {} mid={} cid={} sdpcid={} {}",
            c.mime_type,
            c.mid,
            c.cid,
            c.sdp_cid,
            layers(&c.layers)
        );
    }
    s
}

// The sender and the transceiver a subscriber's transport gives for a track. (The fake of
// the Go test gave nil for both, which the original puts up with; here the fake has to be
// given something to return.)
pub(in crate::rtc) struct TestSender;

impl webrtc::RtpSender for TestSender {
    fn get_parameters(&self) -> webrtc::RtpSendParameters {
        webrtc::RtpSendParameters::default()
    }
}

pub(in crate::rtc) struct TestTransceiver {
    pub(in crate::rtc) sender: Option<Arc<TestSender>>,
}

impl webrtc::RtpTransceiver for TestTransceiver {
    fn sender(&self) -> Option<Arc<dyn webrtc::RtpSender>> {
        let sender = self.sender.clone()?;
        Some(sender)
    }

    fn mid(&self) -> String {
        "0".to_string()
    }
}

pub(in crate::rtc) fn new_transceiver() -> Arc<dyn webrtc::RtpTransceiver> {
    Arc::new(TestTransceiver {
        sender: Some(Arc::new(TestSender)),
    })
}

// A subscriber: upstream's mock participant, with what a subscription asks of it.
pub(in crate::rtc) fn new_participant(id: &str, hidden: bool) -> Arc<types::FakeLocalParticipant> {
    let sub = new_mock_participant(
        id.trim_start_matches("PA_").into(),
        types::ProtocolVersion(types::CURRENT_PROTOCOL),
        hidden,
        false,
        Arc::new(types::FakeLocalParticipantListener::default()),
    );
    sub.id_returns(id.into());
    sub.get_country_returns("NL".to_string());
    sub.supports_sync_stream_id_returns(true);
    sub.get_buffer_factory_returns(
        buffer::FactoryOfBufferFactory::new(500, 500).create_buffer_factory(),
    );
    sub.get_pacer_returns(pacer::PassThrough::new(logger::get_logger(), None));
    sub.get_telemetry_listener_returns(
        Arc::new(types::FakeParticipantTelemetryListener::default()),
    );
    let transceiver = new_transceiver();
    let rtp_sender: Arc<dyn webrtc::RtpSender> = Arc::new(TestSender);
    sub.add_track_local_returns(Ok((rtp_sender.clone(), transceiver.clone())));
    sub.add_transceiver_from_track_local_returns(Ok((rtp_sender, transceiver)));
    sub
}

pub(in crate::rtc) fn new_media_track(ti: &livekit::TrackInfo) -> Arc<types::FakeMediaTrack> {
    let mt = Arc::new(types::FakeMediaTrack::default());
    mt.id_returns(ti.sid.as_str().into());
    mt.kind_returns(ti.r#type());
    mt.source_returns(ti.source());
    mt.stream_returns(ti.stream.clone());
    mt.publisher_id_returns("PA_pub".into());
    mt.publisher_identity_returns("pub".into());
    mt.publisher_version_returns(7);
    mt.to_proto_returns(ti.clone());
    mt.get_quality_for_dimension_returns(livekit::VideoQuality::Medium);
    mt.get_temporal_layer_for_spatial_fps_returns(1);
    mt
}

// A down track as one line: what it was made with and what a subscription sets on it.
pub(in crate::rtc) fn dt_str(dt: &sfu::DownTrack) -> String {
    let di = dt.debug_info();
    let text = |key: &str| di[key].as_str().unwrap_or_default().to_string();
    let ml = dt.max_layer();
    format!(
        "sub={} track={} stream={} mime={} muted={} pubmuted={} max={}/{} closed={}",
        text("SubscriberID"),
        text("TrackID"),
        text("StreamID"),
        text("MimeType"),
        di["Muted"],
        di["PubMuted"],
        ml.spatial,
        ml.temporal,
        dt.is_closed()
    )
}

// The subscriber id of a down track without the time it was made at.
pub(in crate::rtc) fn bare_id(id: &livekit::ParticipantId) -> String {
    id.as_str()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_string()
}

pub(in crate::rtc) fn sub_calls(sub: &types::FakeLocalParticipant) -> String {
    format!(
        "addtrack={} addtransceiver={} verify={} uncache={} getcached={} removetrack={} cache={} reuse-asked={}",
        sub.add_track_local_call_count(),
        sub.add_transceiver_from_track_local_call_count(),
        sub.verify_subscribe_participant_info_call_count(),
        sub.uncache_down_track_call_count(),
        sub.get_cached_down_track_call_count(),
        sub.remove_track_local_call_count(),
        sub.cache_down_track_call_count(),
        sub.supports_transceiver_reuse_call_count()
    )
}

pub(in crate::rtc) fn sorted(ids: Vec<livekit::ParticipantId>) -> String {
    let mut out: Vec<String> = ids.iter().map(|id| id.as_str().to_string()).collect();
    out.sort();
    format!("[{}]", out.join(" "))
}

// What a connection binds a down track with when the codecs are negotiated.
struct TestWriter;

impl webrtc::TrackLocalWriter for TestWriter {
    fn write_rtp(&self, _header: &rtp::Header, payload: &[u8]) -> Result<usize, webrtc::Error> {
        Ok(payload.len())
    }
}

pub(in crate::rtc) struct TestContext {
    pub(in crate::rtc) codecs: Vec<webrtc::RtpCodecParameters>,
}

impl webrtc::TrackLocalContext for TestContext {
    fn codec_parameters(&self) -> Vec<webrtc::RtpCodecParameters> {
        self.codecs.clone()
    }

    fn header_extensions(&self) -> Vec<webrtc::RtpHeaderExtensionParameter> {
        Vec::new()
    }

    fn ssrc(&self) -> webrtc::Ssrc {
        0x1234
    }

    fn ssrc_retransmission(&self) -> webrtc::Ssrc {
        0
    }

    fn write_stream(&self) -> Arc<dyn webrtc::TrackLocalWriter> {
        Arc::new(TestWriter)
    }
}

pub(in crate::rtc) fn wrapped(
    receivers: &[(&Arc<FakeTrackReceiver>, usize)],
    track_id: &str,
    stream_id: &str,
    upstream_codecs: Vec<webrtc::RtpCodecParameters>,
    disable_red: bool,
    is_encrypted: bool,
) -> Arc<WrappedReceiver> {
    WrappedReceiver::new(WrappedReceiverParams {
        receivers: receivers
            .iter()
            .map(|(r, priority)| {
                let r: Arc<dyn TrackReceiver> = (*r).clone();
                SimulcastReceiver::new(r, *priority)
            })
            .collect(),
        track_id: track_id.into(),
        stream_id: stream_id.to_string(),
        upstream_codecs,
        logger: logger::get_logger(),
        disable_red,
        is_encrypted,
    })
}

// -------------------------------------------------------------------

// Expected values: the original printed them, for the same upstream codecs.
#[test]
fn wrapped_receiver_adds_red_or_opus_to_a_single_audio_codec() {
    let table: [(&str, Vec<webrtc::RtpCodecParameters>, bool, &str); 7] = [
        ("opus", vec![opus()], false, "audio/red/63,audio/opus/111"),
        ("opus-nored", vec![opus()], true, "audio/opus/111"),
        ("red", vec![red()], false, "audio/red/63,audio/opus/111"),
        (
            "red-nored",
            vec![red()],
            true,
            "audio/red/63,audio/opus/111",
        ),
        (
            "vp8+h264",
            vec![vp8(), h264()],
            false,
            "video/VP8/96,video/H264/102",
        ),
        ("vp8", vec![vp8()], false, "video/VP8/96"),
        (
            "opus+red",
            vec![opus(), red()],
            false,
            "audio/opus/111,audio/red/63",
        ),
    ];
    for (name, up, disable_red, expected) in table {
        let wr = wrapped(&[], "", "", up, disable_red, false);
        assert_eq!(expected, codecs_str(&wr.codecs()), "{name}");
    }

    // the two codecs that are added, in full
    let full = |c: &webrtc::RtpCodecParameters| {
        format!(
            "{} {} {} {:?} {} fb={}",
            c.mime_type,
            c.clock_rate,
            c.channels,
            c.sdp_fmtp_line,
            c.payload_type,
            c.rtcp_feedback.len()
        )
    };
    let wr = wrapped(&[], "", "", vec![opus()], false, false);
    assert_eq!(
        "audio/red 48000 2 \"111/111\" 63 fb=0",
        full(&wr.codecs()[0])
    );
    let wr = wrapped(&[], "", "", vec![red()], false, false);
    assert_eq!(
        "audio/opus 48000 2 \"minptime=10;useinbandfec=1\" 111 fb=0",
        full(&wr.codecs()[1])
    );
}

// Expected values: the original printed them.
#[test]
fn wrapped_receiver_picks_the_receiver_of_the_negotiated_codec() {
    let (vp8_r, h264_r) = (receiver(&vp8()), receiver(&h264()));
    let wr = wrapped(
        &[(&vp8_r, 0), (&h264_r, 1)],
        "TR_wrapped",
        "PA_pub|TR_wrapped",
        vec![vp8(), h264()],
        false,
        false,
    );
    assert_eq!(
        "TR_wrapped PA_pub|TR_wrapped",
        format!("{} {}", wr.track_id(), wr.stream_id())
    );

    // before a codec is negotiated a callback waits, and there is nothing to delete from
    let ready = Arc::new(crate::go::atomic::Int32::new(0));
    let on_ready = || {
        let ready = ready.clone();
        Box::new(move || {
            ready.inc();
        })
    };
    wr.add_on_ready(on_ready());
    wr.delete_down_track(&"PA_sub".into());
    assert_eq!(
        "onready vp8=0 h264=0 delete vp8=0 h264=0 ready=0",
        format!(
            "onready vp8={} h264={} delete vp8={} h264={} ready={}",
            vp8_r.add_on_ready_call_count(),
            h264_r.add_on_ready_call_count(),
            vp8_r.delete_down_track_call_count(),
            h264_r.delete_down_track_call_count(),
            ready.load()
        )
    );

    assert_eq!(
        (false, false),
        wr.determine_receiver(&av1().rtp_codec_capability)
    );
    assert_eq!(0, ready.load());

    assert_eq!(
        (true, true),
        wr.determine_receiver(&h264().rtp_codec_capability)
    );
    assert_eq!(mime::MimeType::H264, wr.mime());
    assert_eq!(1, ready.load(), "the callback that waited was handed on");

    wr.add_on_ready(on_ready());
    wr.delete_down_track(&"PA_sub".into());
    wr.send_pli(1, true);
    assert_eq!(
        "onready vp8=0 h264=2 delete vp8=0 h264=1 pli vp8=0 h264=1 ready=2",
        format!(
            "onready vp8={} h264={} delete vp8={} h264={} pli vp8={} h264={} ready={}",
            vp8_r.add_on_ready_call_count(),
            h264_r.add_on_ready_call_count(),
            vp8_r.delete_down_track_call_count(),
            h264_r.delete_down_track_call_count(),
            vp8_r.send_pli_call_count(),
            h264_r.send_pli_call_count(),
            ready.load()
        )
    );
    assert_eq!((1, true), h264_r.send_pli_args_for_call(0));

    // another codec is negotiated: the other receiver; one that nobody has: the receiver stays
    assert_eq!(
        (true, true),
        wr.determine_receiver(&vp8().rtp_codec_capability)
    );
    assert_eq!(mime::MimeType::Vp8, wr.mime());
    assert_eq!(
        (false, false),
        wr.determine_receiver(&av1().rtp_codec_capability)
    );
    assert_eq!(mime::MimeType::Vp8, wr.mime());
}

// Expected values: the original printed them, for a source that is encrypted and one that
// is not.
#[test]
fn wrapped_receiver_translates_between_opus_and_red_unless_the_source_is_encrypted() {
    for (enc, expected) in [
        (
            false,
            [
                "opus-receiver red-codec = true false getred=1",
                "pli red-of-opus=1 opus=0",
                "opus-receiver opus-codec = true true",
                "red-receiver opus-codec = true false getprimary=1",
                "pli primary-of-red=1 red=0",
                "red-receiver red-codec = true true",
            ],
        ),
        (
            true,
            [
                "opus-receiver red-codec = false false getred=0",
                "",
                "opus-receiver opus-codec = true true",
                "red-receiver opus-codec = false false getprimary=0",
                "",
                "red-receiver red-codec = true true",
            ],
        ),
    ] {
        let (opus_r, red_r) = (receiver(&opus()), receiver(&red()));
        let red_of_opus = receiver(&red());
        opus_r.get_red_receiver_returns(red_of_opus.clone());
        let primary_of_red = receiver(&opus());
        red_r.get_primary_receiver_for_red_returns(primary_of_red.clone());

        let wr = wrapped(&[(&opus_r, 0)], "", "", vec![opus()], false, enc);
        let (a, n) = wr.determine_receiver(&red().rtp_codec_capability);
        assert_eq!(
            expected[0],
            format!(
                "opus-receiver red-codec = {a} {n} getred={}",
                opus_r.get_red_receiver_call_count()
            )
        );
        if a {
            wr.send_pli(0, false);
            assert_eq!(
                expected[1],
                format!(
                    "pli red-of-opus={} opus={}",
                    red_of_opus.send_pli_call_count(),
                    opus_r.send_pli_call_count()
                )
            );
        }
        let (a, n) = wr.determine_receiver(&opus().rtp_codec_capability);
        assert_eq!(expected[2], format!("opus-receiver opus-codec = {a} {n}"));

        let wr = wrapped(&[(&red_r, 0)], "", "", vec![red()], false, enc);
        let (a, n) = wr.determine_receiver(&opus().rtp_codec_capability);
        assert_eq!(
            expected[3],
            format!(
                "red-receiver opus-codec = {a} {n} getprimary={}",
                red_r.get_primary_receiver_for_red_call_count()
            )
        );
        if a {
            wr.send_pli(0, false);
            assert_eq!(
                expected[4],
                format!(
                    "pli primary-of-red={} red={}",
                    primary_of_red.send_pli_call_count(),
                    red_r.send_pli_call_count()
                )
            );
        }
        let (a, n) = wr.determine_receiver(&red().rtp_codec_capability);
        assert_eq!(expected[5], format!("red-receiver red-codec = {a} {n}"));
    }
}

// The original dereferences the nil receiver it embeds (the first run of the Go test ended
// in that panic, in `OnStatsUpdate`, which asks the wrapped receiver for its track info).
#[test]
#[should_panic(expected = "nil pointer dereference")]
fn wrapped_receiver_panics_when_a_call_is_handed_on_before_a_codec_is_negotiated() {
    let vp8_r = receiver(&vp8());
    let wr = wrapped(&[(&vp8_r, 0)], "TR_wrapped", "", vec![vp8()], false, false);
    let _ = wr.track_info();
}

// Expected values: the original printed them.
#[test]
fn dummy_receiver_answers_for_itself_and_forwards_after_its_upgrade() {
    let ti = Arc::new(livekit::TrackInfo {
        sid: "TR_dummy".to_string(),
        ..Default::default()
    });
    let exts = [webrtc::RtpHeaderExtensionParameter {
        uri: "urn:test".to_string(),
        id: 5,
    }];
    let d = DummyReceiver::new(ti.clone(), "PA_pub", vp8(), &exts);
    let mut buf = [0u8; 10];
    let err = d.read_rtp(&mut buf, 0, 1);
    let (al, br) = d.get_layered_bitrate();
    let (lvl, active) = d.get_audio_level();
    assert_eq!(
        "id=TR_dummy stream=PA_pub codec=video/VP8/96 mime=video/VP8 exts=1 layers=0 brs-zero=true level=0/false closed=false state=0 downtracks=0 receiver-nil=true sizes=0 fps=0 stats-nil=true debug=0 mode=0 info-sid=TR_dummy",
        format!(
            "id={} stream={} codec={} mime={} exts={} layers={} brs-zero={} level={}/{} closed={} state={} downtracks={} receiver-nil={} sizes={} fps={} stats-nil={} debug={} mode={} info-sid={}",
            d.track_id(),
            d.stream_id(),
            codecs_str(&[d.codec()]),
            d.mime(),
            d.header_extensions().len(),
            al.len(),
            br == sfu::Bitrates::default(),
            lvl,
            active,
            d.is_closed(),
            d.codec_state() as i32,
            d.get_down_tracks().len(),
            d.receiver().is_none(),
            d.video_sizes().len(),
            d.get_temporal_layer_fps_for_spatial(0).len(),
            d.get_track_stats().is_none(),
            d.debug_info().len(),
            d.video_layer_mode() as i32,
            d.track_info().sid
        )
    );
    // The original's error says "no receiver"; `sfu::Error` has no such variant, see the
    // header of wrappedreceiver.rs for what stands for it.
    assert_eq!(
        Err(sfu::Error::Bucket(bucket::Error::PacketMismatch(
            String::new()
        ))),
        err
    );

    let (sa, sb) = (sender("PA_a"), sender("PA_b"));
    d.add_down_track(sa.clone()).unwrap();
    d.add_down_track(sb.clone()).unwrap();
    assert_eq!(2, d.get_down_tracks().len());
    d.delete_down_track(&"PA_a".into());
    assert_eq!(1, d.get_down_tracks().len());
    let ready = Arc::new(crate::go::atomic::Int32::new(0));
    let on_ready = || {
        let ready = ready.clone();
        Box::new(move || {
            ready.inc();
        })
    };
    d.add_on_ready(on_ready());
    d.add_on_codec_state_change(Box::new(|_, _| {}));
    d.set_max_expected_spatial_layer(1);
    d.send_pli(0, true);
    d.update_track_info(&ti);
    d.restart("x");

    let real = receiver(&codec("video/VP8", 97, 90000, 0));
    real.track_info_returns(Arc::new(livekit::TrackInfo {
        sid: "TR_real".to_string(),
        ..Default::default()
    }));
    d.upgrade(real.clone());
    assert_eq!(
        "adddowntrack=1 onready=1 oncodec=1 setmax=1 ready=1 pli=0 updateinfo=0 restart=0",
        format!(
            "adddowntrack={} onready={} oncodec={} setmax={} ready={} pli={} updateinfo={} restart={}",
            real.add_down_track_call_count(),
            real.add_on_ready_call_count(),
            real.add_on_codec_state_change_call_count(),
            real.set_max_expected_spatial_layer_call_count(),
            ready.load(),
            real.send_pli_call_count(),
            real.update_track_info_call_count(),
            real.restart_call_count()
        )
    );
    assert_eq!(
        "PA_b",
        real.add_down_track_args_for_call(0)
            .subscriber_id()
            .as_str()
    );
    assert_eq!(1, real.set_max_expected_spatial_layer_args_for_call(0));
    assert_eq!(
        "id=TR_dummy codec-pt=97 receiver-is-real=true info-sid=TR_real",
        format!(
            "id={} codec-pt={} receiver-is-real={} info-sid={}",
            d.track_id(),
            d.codec().payload_type,
            d.receiver().is_some_and(|r| same_receiver(&r, &real)),
            d.track_info().sid
        )
    );

    d.add_down_track(sa.clone()).unwrap();
    d.delete_down_track(&"PA_a".into());
    d.set_max_expected_spatial_layer(2);
    d.send_pli(0, true);
    d.add_on_ready(on_ready());
    d.add_on_codec_state_change(Box::new(|_, _| {}));
    let _ = d.read_rtp(&mut buf, 0, 1);
    d.update_track_info(&ti);
    d.restart("x");
    assert_eq!(
        "adddowntrack=2 delete=1 setmax=2 pli=1 onready=2 oncodec=2 readrtp=1 ready=2 updateinfo=1 restart=1",
        format!(
            "adddowntrack={} delete={} setmax={} pli={} onready={} oncodec={} readrtp={} ready={} updateinfo={} restart={}",
            real.add_down_track_call_count(),
            real.delete_down_track_call_count(),
            real.set_max_expected_spatial_layer_call_count(),
            real.send_pli_call_count(),
            real.add_on_ready_call_count(),
            real.add_on_codec_state_change_call_count(),
            real.read_rtp_call_count(),
            ready.load(),
            real.update_track_info_call_count(),
            real.restart_call_count()
        )
    );

    // a second upgrade is not one
    let other = receiver(&vp8());
    d.upgrade(other.clone());
    assert_eq!(
        "receiver-is-real=true other.adddowntrack=0 other.onready=0",
        format!(
            "receiver-is-real={} other.adddowntrack={} other.onready={}",
            d.receiver().is_some_and(|r| same_receiver(&r, &real)),
            other.add_down_track_call_count(),
            other.add_on_ready_call_count()
        )
    );
}

// Expected values: the original printed them.
#[test]
fn dummy_red_receivers_hold_down_tracks_until_the_upgrade() {
    let ti = Arc::new(livekit::TrackInfo {
        sid: "TR_dummy".to_string(),
        ..Default::default()
    });
    let (sa, sb) = (sender("PA_a"), sender("PA_b"));
    let d = DummyReceiver::new(ti.clone(), "PA_pub", opus(), &[]);
    let red_of_dummy = d.get_red_receiver();
    let primary_of_dummy = d.get_primary_receiver_for_red();
    assert!(Arc::ptr_eq(&d.get_red_receiver(), &red_of_dummy));
    assert!(Arc::ptr_eq(
        &d.get_primary_receiver_for_red(),
        &primary_of_dummy
    ));
    assert!(!Arc::ptr_eq(&red_of_dummy, &primary_of_dummy));

    red_of_dummy.add_down_track(sa.clone()).unwrap();
    red_of_dummy.add_down_track(sb.clone()).unwrap();
    red_of_dummy.delete_down_track(&"PA_b".into());
    primary_of_dummy.add_down_track(sb.clone()).unwrap();
    let mut buf = [0u8; 10];
    assert!(red_of_dummy.read_rtp(&mut buf, 0, 1).is_err());
    assert_eq!(
        "downtracks=1 mime=audio/opus id=TR_dummy base-downtracks=0 primary-downtracks=1",
        format!(
            "downtracks={} mime={} id={} base-downtracks={} primary-downtracks={}",
            red_of_dummy.get_down_tracks().len(),
            red_of_dummy.mime(),
            red_of_dummy.track_id(),
            d.get_down_tracks().len(),
            primary_of_dummy.get_down_tracks().len()
        )
    );

    let real_opus = receiver(&opus());
    let real_red = receiver(&red());
    let real_primary = receiver(&opus());
    real_opus.get_red_receiver_returns(real_red.clone());
    real_opus.get_primary_receiver_for_red_returns(real_primary.clone());
    d.upgrade(real_opus.clone());
    assert_eq!(
        "getred=1 getprimary=1 red.adddowntrack=1 primary.adddowntrack=1 base.adddowntrack=0",
        format!(
            "getred={} getprimary={} red.adddowntrack={} primary.adddowntrack={} base.adddowntrack={}",
            real_opus.get_red_receiver_call_count(),
            real_opus.get_primary_receiver_for_red_call_count(),
            real_red.add_down_track_call_count(),
            real_primary.add_down_track_call_count(),
            real_opus.add_down_track_call_count()
        )
    );
    assert_eq!(
        "PA_a",
        real_red
            .add_down_track_args_for_call(0)
            .subscriber_id()
            .as_str()
    );

    red_of_dummy.add_down_track(sb.clone()).unwrap();
    red_of_dummy.delete_down_track(&"PA_b".into());
    let _ = red_of_dummy.read_rtp(&mut buf, 0, 1);
    let _ = red_of_dummy.get_down_tracks();
    red_of_dummy.send_pli(0, true);
    assert_eq!(
        "red.adddowntrack=2 red.delete=1 red.readrtp=1 red.getdowntracks=1 red.pli=0 base.pli=1 same=true",
        format!(
            "red.adddowntrack={} red.delete={} red.readrtp={} red.getdowntracks={} red.pli={} base.pli={} same={}",
            real_red.add_down_track_call_count(),
            real_red.delete_down_track_call_count(),
            real_red.read_rtp_call_count(),
            real_red.get_down_tracks_call_count(),
            real_red.send_pli_call_count(),
            real_opus.send_pli_call_count(),
            Arc::ptr_eq(&d.get_red_receiver(), &red_of_dummy)
        )
    );

    // a RED receiver that is asked for after the upgrade is upgraded when it is made
    let d2 = DummyReceiver::new(ti, "PA_pub", opus(), &[]);
    d2.upgrade(real_opus.clone());
    let late = d2.get_red_receiver();
    late.add_down_track(sa.clone()).unwrap();
    assert_eq!(
        "getred=2 red.adddowntrack=3",
        format!(
            "getred={} red.adddowntrack={}",
            real_opus.get_red_receiver_call_count(),
            real_red.add_down_track_call_count()
        )
    );
}

// Not from the original, which leaves this to its garbage collector: a dummy receiver and
// its RED receivers do not keep each other alive.
#[test]
fn dummy_receiver_and_its_red_receivers_are_freed() {
    let d = DummyReceiver::new(
        Arc::new(livekit::TrackInfo::default()),
        "PA_pub",
        opus(),
        &[],
    );
    let red_of_dummy = d.get_red_receiver();
    let weak_d = Arc::downgrade(&d);
    let weak_red = Arc::downgrade(&red_of_dummy);

    // the RED receiver holds the receiver it embeds
    drop(d);
    assert!(weak_d.upgrade().is_some());
    assert_eq!(mime::MimeType::Opus, red_of_dummy.mime());

    drop(red_of_dummy);
    assert!(weak_red.upgrade().is_none());
    assert!(weak_d.upgrade().is_none());

    // and one that nobody holds any more is made anew
    let d = DummyReceiver::new(
        Arc::new(livekit::TrackInfo::default()),
        "PA_pub",
        opus(),
        &[],
    );
    drop(d.get_red_receiver());
    let again = d.get_red_receiver();
    assert_eq!(mime::MimeType::Opus, again.mime());
}
