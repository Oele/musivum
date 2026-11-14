// New for musivum: upstream has no test of mediatrackreceiver.go (`mediatrack_test.go`
// tests it through `MediaTrack`, which is not ported yet).
//
// The steps of these tests were also run against the Go original (see the top of
// wrappedreceiver_test.rs, which also has the helpers used here), on one receiver from
// "potential" to "closed"; here that run is cut in two tests, the second starting from the
// same receivers. Where a test says "the original printed", the expected text is the line
// the original printed after that step.

use std::sync::Arc;

use parking_lot::Mutex;

use super::*;
use crate::protocol::logger;
use crate::rtc::wrappedreceiver::wrappedreceiver_test::*;
use crate::sfu::{FakeTrackReceiver, TrackReceiver};

fn params(
    mt: &Arc<types::FakeMediaTrack>,
    tl: &Arc<types::FakeParticipantTelemetryListener>,
) -> MediaTrackReceiverParams {
    let media_track: Arc<dyn types::MediaTrack> = mt.clone();
    let tl = tl.clone();
    MediaTrackReceiverParams {
        media_track: Some(Arc::downgrade(&media_track)),
        participant_id: Some(Box::new(|| "PA_pub".into())),
        participant_identity: "pub".into(),
        participant_version: 3,
        receiver_config: ReceiverConfig {
            packet_buffer_size_video: 300,
            packet_buffer_size_audio: 200,
        },
        telemetry_listener: Some(Box::new(move || tl.clone())),
        logger: logger::get_logger(),
        regression_target_codec: mime::MimeType::H264,
        ..Default::default()
    }
}

fn mimes(rs: &[Arc<dyn TrackReceiver>]) -> String {
    rs.iter()
        .map(|r| r.mime().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

// A camera track of "PA_pub" that is to come in VP8 and H264, with both receivers set up.
struct Video {
    mtr: Arc<MediaTrackReceiver>,
    mt: Arc<types::FakeMediaTrack>,
    tl: Arc<types::FakeParticipantTelemetryListener>,
    vp8: Arc<FakeTrackReceiver>,
    h264: Arc<FakeTrackReceiver>,
}

// Expected values: the original printed them.
fn new_video() -> Video {
    let ti = video_info();
    let mt = new_media_track(&ti);
    let tl = Arc::new(types::FakeParticipantTelemetryListener::default());
    let mtr = MediaTrackReceiver::new(params(&mt, &tl), &ti);

    // potential codecs hold a place, and a receiver that comes takes it
    let exts = [webrtc::RtpHeaderExtensionParameter {
        uri: "urn:test".to_string(),
        id: 5,
    }];
    mtr.set_potential_codecs(&[vp8(), h264()], &exts);
    assert_eq!(
        "receivers=video/VP8,video/H264 primary-nil=true vp8-nil=true open=true stream-of-dummy=PA_pub",
        format!(
            "receivers={} primary-nil={} vp8-nil={} open={} stream-of-dummy={}",
            mimes(&mtr.receivers()),
            mtr.primary_receiver().is_none(),
            mtr.receiver(mime::MimeType::Vp8).is_none(),
            mtr.is_open(),
            mtr.receivers()[0].stream_id()
        )
    );
    let (vp8_r, h264_r) = (receiver(&vp8()), receiver(&h264()));
    mtr.setup_receiver(h264_r.clone(), 1, "mid1");
    assert_eq!(
        "receivers=video/VP8,video/H264 primary-nil=true h264-is-h264=true",
        format!(
            "receivers={} primary-nil={} h264-is-h264={}",
            mimes(&mtr.receivers()),
            mtr.primary_receiver().is_none(),
            mtr.receiver(mime::MimeType::H264)
                .is_some_and(|r| same_receiver(&r, &h264_r))
        )
    );
    assert_eq!(
        "sid=TR_vid mime= mid= muted=false simulcast=false 1280x720 stereo=false nodtx=false features=[] layers=[0:320x180:ssrc0:rtx0:rid:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc0:rtx0:rid:sl2] | video/VP8 mid= cid=cid-vp8 sdpcid= [0:320x180:ssrc0:rtx0:rid:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc0:rtx0:rid:sl2] | video/H264 mid=mid1 cid=cid-h264 sdpcid= [0:320x180:ssrc0:rtx0:rid:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc0:rtx0:rid:sl2]",
        dump_ti(&mtr.track_info())
    );
    mtr.setup_receiver(vp8_r.clone(), 0, "mid0");
    assert_eq!(
        "receivers=video/VP8,video/H264 primary-is-vp8=true",
        format!(
            "receivers={} primary-is-vp8={}",
            mimes(&mtr.receivers()),
            mtr.primary_receiver()
                .is_some_and(|r| same_receiver(&r, &vp8_r))
        )
    );
    assert_eq!(
        "sid=TR_vid mime=video/VP8 mid=mid0 muted=false simulcast=false 1280x720 stereo=false nodtx=false features=[] layers=[0:320x180:ssrc0:rtx0:rid:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc0:rtx0:rid:sl2] | video/VP8 mid=mid0 cid=cid-vp8 sdpcid= [0:320x180:ssrc0:rtx0:rid:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc0:rtx0:rid:sl2] | video/H264 mid=mid1 cid=cid-h264 sdpcid= [0:320x180:ssrc0:rtx0:rid:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc0:rtx0:rid:sl2]",
        dump_ti(&mtr.track_info())
    );
    Video {
        mtr,
        mt,
        tl,
        vp8: vp8_r,
        h264: h264_r,
    }
}

// Expected values: the original printed them.
#[tokio::test(flavor = "multi_thread")]
async fn media_track_receiver_answers_from_its_track_info_and_needs_a_receiver() {
    let ti = video_info();
    let mt = new_media_track(&ti);
    let tl = Arc::new(types::FakeParticipantTelemetryListener::default());
    let mtr = MediaTrackReceiver::new(params(&mt, &tl), &ti);
    let (lvl, active) = mtr.get_audio_level();
    assert_eq!(
        "id=TR_vid kind=VIDEO name=cam source=CAMERA stream=camera muted=false enc=false trailer=false pub=PA_pub/pub/3 open=true receivers=0 primary-nil=true active-nil=true level=0/false stats-nil=true",
        format!(
            "id={} kind={} name={} source={} stream={} muted={} enc={} trailer={} pub={}/{}/{} open={} receivers={} primary-nil={} active-nil={} level={}/{} stats-nil={}",
            mtr.id(),
            mtr.kind().as_str_name(),
            mtr.name(),
            mtr.source().as_str_name(),
            mtr.stream(),
            mtr.is_muted(),
            mtr.is_encrypted(),
            mtr.has_packet_trailer(),
            mtr.publisher_id(),
            mtr.publisher_identity(),
            mtr.publisher_version(),
            mtr.is_open(),
            mtr.receivers().len(),
            mtr.primary_receiver().is_none(),
            mtr.active_receiver().is_none(),
            lvl,
            active,
            mtr.get_track_stats().is_none()
        )
    );
    let err = mtr
        .add_subscriber(new_participant("PA_early", false))
        .err()
        .map(|e| e.to_string());
    assert_eq!(
        Some("cannot subscribe without a receiver in place".to_string()),
        err
    );
}

// Expected values: the original printed them.
#[tokio::test(flavor = "multi_thread")]
async fn media_track_receiver_orders_receivers_by_priority() {
    let ti = video_info();
    let mt = new_media_track(&ti);
    let tl = Arc::new(types::FakeParticipantTelemetryListener::default());
    let order = MediaTrackReceiver::new(params(&mt, &tl), &ti);
    let setups = Arc::new(Mutex::new(Vec::new()));
    {
        let setups = setups.clone();
        order.on_setup_receiver(move |m| setups.lock().push(m.to_string()));
    }
    let (o_av1, o_h264, o_vp8) = (receiver(&av1()), receiver(&h264()), receiver(&vp8()));
    order.setup_receiver(o_av1, 2, "");
    order.setup_receiver(o_h264.clone(), 1, "");
    order.setup_receiver(o_vp8.clone(), 0, "");
    order.setup_receiver(receiver(&vp8()), 0, ""); // a second one for the codec is refused
    assert_eq!(
        "receivers=video/VP8,video/H264,video/AV1 setups=[video/AV1 video/H264 video/VP8 video/VP8] primary-is-vp8=true h264-is-h264=true",
        format!(
            "receivers={} setups=[{}] primary-is-vp8={} h264-is-h264={}",
            mimes(&order.receivers()),
            setups.lock().join(" "),
            order
                .primary_receiver()
                .is_some_and(|r| same_receiver(&r, &o_vp8)),
            order
                .receiver(mime::MimeType::H264)
                .is_some_and(|r| same_receiver(&r, &o_h264))
        )
    );
    assert_eq!(dump_ti(&video_info()), dump_ti(&order.track_info()));
    order.clear_receiver(mime::MimeType::Vp8, false);
    assert_eq!("video/AV1,video/H264", mimes(&order.receivers()));
}

// Expected values: the original printed them.
#[tokio::test(flavor = "multi_thread")]
async fn media_track_receiver_updates_its_track_info() {
    let Video {
        mtr,
        tl,
        vp8: vp8_r,
        h264: h264_r,
        ..
    } = new_video();

    // mute
    mtr.set_muted(true);
    assert_eq!(
        "muted=true info-muted=true vp8.update=1 h264.update=1",
        format!(
            "muted={} info-muted={} vp8.update={} h264.update={}",
            mtr.is_muted(),
            mtr.track_info().muted,
            vp8_r.update_track_info_call_count(),
            h264_r.update_track_info_call_count()
        )
    );
    mtr.set_muted(false);

    // layers
    let layer = |quality: livekit::VideoQuality, width: u32, height: u32| livekit::VideoLayer {
        quality: quality as i32,
        width,
        height,
        ..Default::default()
    };
    mtr.update_codec_info(&[livekit::SimulcastCodec {
        codec: "vp8".to_string(),
        cid: "new-cid".to_string(),
        layers: vec![
            layer(livekit::VideoQuality::Low, 480, 270),
            layer(livekit::VideoQuality::High, 960, 540),
        ],
        ..Default::default()
    }]);
    assert_eq!(
        "sid=TR_vid mime=video/VP8 mid=mid0 muted=false simulcast=false 1280x720 stereo=false nodtx=false features=[] layers=[0:320x180:ssrc0:rtx0:rid:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc0:rtx0:rid:sl2] | video/VP8 mid=mid0 cid=new-cid sdpcid= [0:480x270:ssrc0:rtx0:ridq:sl0 2:960x540:ssrc0:rtx0:ridh:sl1] | video/H264 mid=mid1 cid=cid-h264 sdpcid= [0:320x180:ssrc0:rtx0:rid:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc0:rtx0:rid:sl2]",
        dump_ti(&mtr.track_info()),
        "codec info"
    );
    mtr.update_codec_sdp_cid(mime::MimeType::H264, "sdp-cid");
    mtr.update_codec_sdp_cid(mime::MimeType::Vp8, "new-cid");
    assert_eq!(
        "sid=TR_vid mime=video/VP8 mid=mid0 muted=false simulcast=false 1280x720 stereo=false nodtx=false features=[] layers=[0:320x180:ssrc0:rtx0:rid:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc0:rtx0:rid:sl2] | video/VP8 mid=mid0 cid=new-cid sdpcid= [0:480x270:ssrc0:rtx0:ridq:sl0 2:960x540:ssrc0:rtx0:ridh:sl1] | video/H264 mid=mid1 cid=cid-h264 sdpcid=sdp-cid [0:320x180:ssrc0:rtx0:rid:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc0:rtx0:rid:sl2]",
        dump_ti(&mtr.track_info()),
        "sdp cid"
    );
    mtr.update_codec_rids(
        mime::MimeType::H264,
        &["2".to_string(), "1".to_string(), "0".to_string()],
    );
    assert_eq!(
        "sid=TR_vid mime=video/VP8 mid=mid0 muted=false simulcast=false 1280x720 stereo=false nodtx=false features=[] layers=[0:320x180:ssrc0:rtx0:rid:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc0:rtx0:rid:sl2] | video/VP8 mid=mid0 cid=new-cid sdpcid= [0:480x270:ssrc0:rtx0:ridq:sl0 2:960x540:ssrc0:rtx0:ridh:sl1] | video/H264 mid=mid1 cid=cid-h264 sdpcid=sdp-cid [0:320x180:ssrc0:rtx0:rid2:sl0 1:640x360:ssrc0:rtx0:rid1:sl1 2:1280x720:ssrc0:rtx0:rid0:sl2]",
        dump_ti(&mtr.track_info()),
        "rids"
    );
    mtr.set_layer_ssrcs_for_rid(mime::MimeType::Vp8, "q", 1111, 2222);
    mtr.set_layer_ssrcs_for_rid(mime::MimeType::Vp8, "h", 3333, 0);
    mtr.set_layer_ssrcs_for_rid(mime::MimeType::H264, "0", 5555, 0);
    let ssrcs = "sid=TR_vid mime=video/VP8 mid=mid0 muted=false simulcast=false 1280x720 stereo=false nodtx=false features=[] layers=[0:480x270:ssrc1111:rtx2222:ridq:sl0 2:960x540:ssrc3333:rtx0:ridh:sl1] | video/VP8 mid=mid0 cid=new-cid sdpcid= [0:480x270:ssrc1111:rtx2222:ridq:sl0 2:960x540:ssrc3333:rtx0:ridh:sl1] | video/H264 mid=mid1 cid=cid-h264 sdpcid=sdp-cid [0:320x180:ssrc0:rtx0:rid2:sl0 1:640x360:ssrc0:rtx0:rid1:sl1 2:1280x720:ssrc5555:rtx0:rid0:sl2]";
    assert_eq!(ssrcs, dump_ti(&mtr.track_info()), "ssrcs");
    // a layer that has its ssrc keeps it
    mtr.set_layer_ssrcs_for_rid(mime::MimeType::Vp8, "q", 9999, 8888);
    assert_eq!(ssrcs, dump_ti(&mtr.track_info()), "ssrcs again");
    mtr.set_layer_rtx_info(3333, 4444, "h");
    mtr.set_layer_rtx_info(1111, 7777, "q");
    mtr.set_layer_rtx_info(5555, 6666, "wrong");
    mtr.set_layer_rtx_info(0, 6666, "q");
    assert_eq!(
        "sid=TR_vid mime=video/VP8 mid=mid0 muted=false simulcast=false 1280x720 stereo=false nodtx=false features=[] layers=[0:480x270:ssrc1111:rtx2222:ridq:sl0 2:960x540:ssrc3333:rtx4444:ridh:sl1] | video/VP8 mid=mid0 cid=new-cid sdpcid= [0:480x270:ssrc1111:rtx2222:ridq:sl0 2:960x540:ssrc3333:rtx4444:ridh:sl1] | video/H264 mid=mid1 cid=cid-h264 sdpcid=sdp-cid [0:320x180:ssrc0:rtx0:rid2:sl0 1:640x360:ssrc0:rtx0:rid1:sl1 2:1280x720:ssrc5555:rtx0:rid0:sl2]",
        dump_ti(&mtr.track_info()),
        "rtx"
    );

    // a new track info keeps the mids, ssrcs and rids there are
    let updates = tl.on_track_published_update_call_count();
    let mut nti = video_info();
    nti.name = "renamed".to_string();
    mtr.update_track_info(&nti);
    let updated = "sid=TR_vid mime= mid= muted=false simulcast=false 1280x720 stereo=false nodtx=false features=[] layers=[0:320x180:ssrc1111:rtx2222:ridq:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc3333:rtx4444:ridh:sl2] | video/VP8 mid=mid0 cid=cid-vp8 sdpcid= [0:320x180:ssrc1111:rtx2222:ridq:sl0 1:640x360:ssrc0:rtx0:rid:sl1 2:1280x720:ssrc3333:rtx4444:ridh:sl2] | video/H264 mid=mid1 cid=cid-h264 sdpcid= [0:320x180:ssrc0:rtx0:rid2:sl0 1:640x360:ssrc0:rtx0:rid1:sl1 2:1280x720:ssrc5555:rtx0:rid0:sl2]";
    assert_eq!(
        format!("name=renamed {updated}"),
        format!("name={} {}", mtr.name(), dump_ti(&mtr.track_info()))
    );
    mtr.update_video_track(&livekit::UpdateLocalVideoTrack {
        width: 640,
        height: 360,
        ..Default::default()
    });
    mtr.update_video_track(&livekit::UpdateLocalVideoTrack {
        width: 640,
        height: 360,
        ..Default::default()
    });
    mtr.update_audio_track(&livekit::UpdateLocalAudioTrack {
        features: vec![livekit::AudioTrackFeature::TfNoDtx as i32],
        ..Default::default()
    });
    assert_eq!(
        "640x360 updates=1",
        format!(
            "{}x{} updates={}",
            mtr.track_info().width,
            mtr.track_info().height,
            tl.on_track_published_update_call_count() - updates
        )
    );
    let sizes = [
        codec::VideoSize {
            width: 320,
            height: 180,
        },
        codec::VideoSize {
            width: 0,
            height: 0,
        },
        codec::VideoSize {
            width: 1280,
            height: 720,
        },
    ];
    mtr.update_video_size(mime::MimeType::Vp8, &sizes);
    mtr.update_video_size(mime::MimeType::Vp8, &sizes);
    assert_eq!(
        format!("updates=2 {updated}"),
        format!(
            "updates={} {}",
            tl.on_track_published_update_call_count() - updates,
            dump_ti(&mtr.track_info())
        )
    );
    mtr.notify_max_layer_change(mime::MimeType::Vp8, 1);
    let (_, nt) =
        tl.on_track_published_update_args_for_call(tl.on_track_published_update_call_count() - 1);
    assert_eq!(
        "sid=TR_vid mime= mid= muted=false simulcast=false 0x0 stereo=false nodtx=false features=[] layers=[1:640x360:ssrc0:rtx0:rid:sl0] type=VIDEO",
        format!("{} type={}", dump_ti(&nt), nt.r#type().as_str_name())
    );
    mtr.notify_max_layer_change(mime::MimeType::Vp8, buffer::INVALID_LAYER_SPATIAL);
    let (_, nt) =
        tl.on_track_published_update_args_for_call(tl.on_track_published_update_call_count() - 1);
    assert_eq!(
        "sid=TR_vid mime= mid= muted=false simulcast=false 0x0 stereo=false nodtx=false features=[] layers=[3:0x0:ssrc0:rtx0:rid:sl0]",
        dump_ti(&nt)
    );

    // temporal layers and qualities
    vp8_r.get_temporal_layer_fps_for_spatial_returns(vec![7.5, 15.0, 30.0]);
    let temporal = |m: mime::MimeType, fps: u32| mtr.get_temporal_layer_for_spatial_fps(m, 2, fps);
    assert_eq!(
        "5=0 8=0 9=1 16=1 30=2 34=3 h264=3 av1=3",
        format!(
            "5={} 8={} 9={} 16={} 30={} 34={} h264={} av1={}",
            temporal(mime::MimeType::Vp8, 5),
            temporal(mime::MimeType::Vp8, 8),
            temporal(mime::MimeType::Vp8, 9),
            temporal(mime::MimeType::Vp8, 16),
            temporal(mime::MimeType::Vp8, 30),
            temporal(mime::MimeType::Vp8, 34),
            temporal(mime::MimeType::H264, 5),
            temporal(mime::MimeType::Av1, 5)
        )
    );
    let quality = |w: u32, h: u32| {
        mtr.get_quality_for_dimension(mime::MimeType::Vp8, w, h)
            .as_str_name()
    };
    assert_eq!(
        "100x100=LOW 640x360=MEDIUM 700x400=MEDIUM 1280x720=HIGH",
        format!(
            "100x100={} 640x360={} 700x400={} 1280x720={}",
            quality(100, 100),
            quality(640, 360),
            quality(700, 400),
            quality(1280, 720)
        )
    );
    let size = |width: u32, height: u32| codec::VideoSize { width, height };
    vp8_r.video_sizes_returns(vec![size(160, 90), size(320, 180), size(640, 360)]);
    assert_eq!(
        "100x100=LOW 320x180=LOW 640x360=MEDIUM",
        format!(
            "100x100={} 320x180={} 640x360={}",
            quality(100, 100),
            quality(320, 180),
            quality(640, 360)
        )
    );
}

// Expected values: the original printed them.
#[tokio::test(flavor = "multi_thread")]
async fn media_track_receiver_adds_revokes_regresses_and_closes_subscribers() {
    let Video {
        mtr,
        mt: _mt,
        vp8: vp8_r,
        h264: h264_r,
        ..
    } = new_video();

    // subscribers
    let sub = new_participant("PA_sub", false);
    let st = mtr.add_subscriber(sub.clone()).unwrap().unwrap();
    assert_eq!(
        "num=1 is=true all=[PA_sub] addtrack=0 addtransceiver=1 verify=1 uncache=1 getcached=1 removetrack=0 cache=0 reuse-asked=1",
        format!(
            "num={} is={} all={} {}",
            mtr.get_num_subscribers(),
            mtr.is_subscriber(&"PA_sub".into()),
            sorted(mtr.get_all_subscribers()),
            sub_calls(&sub)
        )
    );
    assert_eq!(
        "sub=PA_sub track=TR_vid stream=PA_pub|camera mime=video/VP8 muted=false pubmuted=false max=-1/3 closed=false",
        dt_str(&st.down_track().unwrap())
    );
    let di = mtr.debug_info();
    let mut keys: Vec<&String> = di.keys().collect();
    keys.sort();
    assert_eq!(
        "keys=[DownTracks ID Kind PubMuted video/H264 video/VP8] id=TR_vid kind=VIDEO pubmuted=false downtracks=1",
        format!(
            "keys=[{}] id={} kind={} pubmuted={} downtracks={}",
            keys.iter()
                .map(|k| k.as_str())
                .collect::<Vec<_>>()
                .join(" "),
            di["ID"].as_str().unwrap_or_default(),
            di["Kind"].as_str().unwrap_or_default(),
            di["PubMuted"],
            di["DownTracks"].as_array().map_or(0, Vec::len)
        )
    );
    mtr.restart();
    mtr.maybe_set_simulcast();
    mtr.set_rtt(10);
    assert_eq!(
        "vp8.setmax=1/2 h264.setmax=1/2 simulcast=false",
        format!(
            "vp8.setmax={}/{} h264.setmax={}/{} simulcast={}",
            vp8_r.set_max_expected_spatial_layer_call_count(),
            vp8_r.set_max_expected_spatial_layer_args_for_call(0),
            h264_r.set_max_expected_spatial_layer_call_count(),
            h264_r.set_max_expected_spatial_layer_args_for_call(0),
            mtr.track_info().simulcast
        )
    );
    let err = mtr.add_subscriber(sub.clone()).err();
    assert_eq!(
        "err=already subscribed num=1",
        format!(
            "err={} num={}",
            err.map(|e| e.to_string()).unwrap_or_default(),
            mtr.get_num_subscribers()
        )
    );
    let rec = new_participant("PA_rec", false);
    rec.is_recorder_returns(true);
    assert!(mtr.add_subscriber(rec).is_ok());
    assert_eq!(2, mtr.get_num_subscribers());
    let ids = |ids: Vec<livekit::ParticipantIdentity>| {
        format!(
            "[{}]",
            ids.iter()
                .map(|id| id.as_str())
                .collect::<Vec<_>>()
                .join(" ")
        )
    };
    assert_eq!(
        "[]",
        ids(mtr.revoke_disallowed_subscribers(&["sub".into(), "someone".into()]))
    );
    let revoked = mtr.revoke_disallowed_subscribers(&["someone".into()]);
    let done = wait(|| mtr.get_num_subscribers() == 1).await;
    assert_eq!(
        "[sub] done=true left=[PA_rec]",
        format!(
            "{} done={} left={}",
            ids(revoked),
            done,
            sorted(mtr.get_all_subscribers())
        )
    );

    // a codec regression switches the subscribers of the codec to the backup codec
    let x = sender("PA_x");
    vp8_r.get_down_tracks_returns(vec![x.clone()]);
    let regressions = Arc::new(Mutex::new(Vec::new()));
    {
        let regressions = regressions.clone();
        mtr.on_codec_regression(move |o, n| {
            regressions
                .lock()
                .push(format!("{}>{}", o.mime_type, n.mime_type))
        });
    }
    let line = || {
        format!(
            "set={} regressions=[{}]",
            x.set_receiver_call_count(),
            regressions.lock().join(" ")
        )
    };
    let vp8_dyn: Arc<dyn TrackReceiver> = vp8_r.clone();
    mtr.handle_receiver_codec_change(&vp8_dyn, &vp8(), sfu::ReceiverCodecState::Normal);
    assert_eq!("set=0 regressions=[]", line(), "normal");
    let av1_dyn: Arc<dyn TrackReceiver> = receiver(&av1());
    mtr.handle_receiver_codec_change(&av1_dyn, &av1(), sfu::ReceiverCodecState::Suspended);
    assert_eq!("set=0 regressions=[]", line(), "unknown");
    mtr.handle_receiver_codec_change(&vp8_dyn, &vp8(), sfu::ReceiverCodecState::Suspended);
    // The down tracks are handed the place of the backup codec (a dummy receiver here, which
    // has H264's), not H264's receiver itself.
    assert_eq!(
        "set=1 regressions=[video/VP8>video/H264] to-h264=false active-is-vp8=false primary-is-vp8=true",
        format!(
            "{} to-h264={} active-is-vp8={} primary-is-vp8={}",
            line(),
            x.set_receiver_call_count() == 1
                && same_receiver(&x.set_receiver_args_for_call(0), &h264_r),
            mtr.active_receiver()
                .is_some_and(|r| same_receiver(&r, &vp8_r)),
            mtr.primary_receiver()
                .is_some_and(|r| same_receiver(&r, &vp8_r))
        )
    );
    // Not from the original: what they are handed is the H264 receiver's place.
    assert_eq!(mime::MimeType::H264, x.set_receiver_args_for_call(0).mime());
    mtr.handle_receiver_codec_change(&vp8_dyn, &vp8(), sfu::ReceiverCodecState::Suspended);
    assert_eq!("set=1 regressions=[video/VP8>video/H264]", line(), "again");

    // a subscriber that comes later gets the backup codec only
    let sub2 = new_participant("PA_sub2", false);
    let st2 = mtr.add_subscriber(sub2).unwrap().unwrap();
    let dt2 = st2.down_track().unwrap();
    assert_eq!(
        "sub=PA_sub2 track=TR_vid stream=PA_pub|camera mime=video/H264 muted=false pubmuted=false max=-1/3 closed=false",
        dt_str(&dt2)
    );
    let res = dt2.bind(Arc::new(TestContext {
        codecs: vec![vp8(), h264()],
    }));
    // (bound before its first settings are applied: see the same step in
    // mediatracksubscriptions_test.rs)
    let bound = wait(|| st2.is_bound() && dt2.max_layer().spatial == 2).await;
    assert_eq!(
        "ok=true bound=true vp8.add=0 h264.add=1 sub=PA_sub2 track=TR_vid stream=PA_pub|camera mime=video/H264 muted=false pubmuted=false max=2/3 closed=false",
        format!(
            "ok={} bound={} vp8.add={} h264.add={} {}",
            res.is_ok(),
            bound,
            vp8_r.add_down_track_call_count(),
            h264_r.add_down_track_call_count(),
            dt_str(&dt2)
        )
    );

    // closing
    let closes = Arc::new(Mutex::new(Vec::new()));
    mtr.add_on_close(None);
    {
        let closes = closes.clone();
        mtr.add_on_close(Some(Box::new(move |b| closes.lock().push(b))));
    }
    assert_eq!(
        "false open=true",
        format!("{} open={}", mtr.try_close(), mtr.is_open())
    );
    mtr.set_closing(true);
    assert!(!mtr.is_open());
    let err = mtr
        .add_subscriber(new_participant("PA_late", false))
        .err()
        .map(|e| e.to_string());
    assert_eq!(Some("track is not open".to_string()), err);
    mtr.clear_receiver(mime::MimeType::Vp8, true);
    mtr.clear_receiver(mime::MimeType::H264, true);
    assert_eq!(
        "receivers=0 num=0 try=true closes=[true] open=false",
        format!(
            "receivers={} num={} try={} closes={:?} open={}",
            mtr.receivers().len(),
            mtr.get_num_subscribers(),
            mtr.try_close(),
            closes.lock(),
            mtr.is_open()
        )
    );
    mtr.close(false);
    mtr.setup_receiver(receiver(&vp8()), 0, "");
    assert_eq!(
        "closes=[true] receivers=0",
        format!(
            "closes={:?} receivers={}",
            closes.lock(),
            mtr.receivers().len()
        )
    );
}

// Expected values: the original printed them.
#[tokio::test(flavor = "multi_thread")]
async fn media_track_receiver_is_closed_by_its_receivers() {
    let ti = video_info();
    let mt = new_media_track(&ti);
    let tl = Arc::new(types::FakeParticipantTelemetryListener::default());

    // a track of dummies only closes on TryClose
    let dm = MediaTrackReceiver::new(params(&mt, &tl), &ti);
    dm.set_potential_codecs(&[vp8(), h264()], &[]);
    assert_eq!(
        "try=true open=false",
        format!("try={} open={}", dm.try_close(), dm.is_open())
    );

    // a receiver that is closed closes the track
    let cl = MediaTrackReceiver::new(params(&mt, &tl), &ti);
    let cl_r = receiver(&vp8());
    cl.setup_receiver(cl_r.clone(), 0, "");
    assert!(cl.is_open());
    cl_r.is_closed_returns(true);
    assert!(!cl.is_open());
}

// Expected values: the original printed them.
#[tokio::test(flavor = "multi_thread")]
async fn media_track_receiver_of_an_audio_track() {
    let ati = audio_info();
    let amt = new_media_track(&ati);
    let atl = Arc::new(types::FakeParticipantTelemetryListener::default());
    let amtr = MediaTrackReceiver::new(
        MediaTrackReceiverParams {
            participant_version: 0,
            receiver_config: ReceiverConfig::default(),
            regression_target_codec: mime::MimeType::default(),
            audio_config: sfu::AudioConfig {
                active_red_encoding: true,
                ..Default::default()
            },
            ..params(&amt, &atl)
        },
        &ati,
    );
    assert_eq!(
        livekit::VideoQuality::High,
        amtr.get_quality_for_dimension(mime::MimeType::Opus, 10, 10)
    );

    // The feature of the update that is named twice makes the track stereo, as upstream
    // ("nodtx twice", "unsorted"): see the header of mediatrackreceiver.rs.
    let stereo = livekit::AudioTrackFeature::TfStereo as i32;
    let no_dtx = livekit::AudioTrackFeature::TfNoDtx as i32;
    let echo = livekit::AudioTrackFeature::TfEchoCancellation as i32;
    let table: [(&str, Vec<i32>, &str); 6] = [
        (
            "stereo",
            vec![stereo],
            "updates=1 sid=TR_aud mime= mid= muted=false simulcast=false 0x0 stereo=true nodtx=false features=[0] layers=[]",
        ),
        (
            "same",
            vec![stereo],
            "updates=1 sid=TR_aud mime= mid= muted=false simulcast=false 0x0 stereo=true nodtx=false features=[0] layers=[]",
        ),
        (
            "nodtx",
            vec![no_dtx],
            "updates=2 sid=TR_aud mime= mid= muted=false simulcast=false 0x0 stereo=false nodtx=true features=[1] layers=[]",
        ),
        (
            "nodtx twice",
            vec![no_dtx, no_dtx],
            "updates=3 sid=TR_aud mime= mid= muted=false simulcast=false 0x0 stereo=true nodtx=true features=[1] layers=[]",
        ),
        (
            "unsorted",
            vec![echo, no_dtx, echo],
            "updates=4 sid=TR_aud mime= mid= muted=false simulcast=false 0x0 stereo=true nodtx=true features=[1 3] layers=[]",
        ),
        (
            "none",
            vec![],
            "updates=5 sid=TR_aud mime= mid= muted=false simulcast=false 0x0 stereo=false nodtx=false features=[] layers=[]",
        ),
    ];
    for (name, features, expected) in table {
        amtr.update_audio_track(&livekit::UpdateLocalAudioTrack {
            features,
            ..Default::default()
        });
        assert_eq!(
            expected,
            format!(
                "updates={} {}",
                atl.on_track_published_update_call_count(),
                dump_ti(&amtr.track_info())
            ),
            "{name}"
        );
    }
    amtr.update_video_track(&livekit::UpdateLocalVideoTrack {
        width: 1,
        height: 1,
        ..Default::default()
    });
    assert_eq!(5, atl.on_track_published_update_call_count());

    let opus_r = receiver(&opus());
    opus_r.get_audio_level_returns((12.5, true));
    amtr.setup_receiver(opus_r, 0, "amid");
    let (lvl, active) = amtr.get_audio_level();
    let asub = new_participant("PA_asub", false);
    let ast = amtr.add_subscriber(asub.clone()).unwrap().unwrap();
    let (_, p) = asub.add_transceiver_from_track_local_args_for_call(0);
    assert_eq!(
        "level=12.5/true stereo=true red=true sub=PA_asub track=TR_aud stream=PA_pub|camera mime=audio/red muted=false pubmuted=false max=-1/-1 closed=false",
        format!(
            "level={}/{} stereo={} red={} {}",
            lvl,
            active,
            p.stereo,
            p.red,
            dt_str(&ast.down_track().unwrap())
        )
    );
    assert_eq!(
        "sid=TR_aud mime=audio/opus mid=amid muted=false simulcast=false 0x0 stereo=false nodtx=false features=[] layers=[]",
        dump_ti(&amtr.track_info())
    );
}
