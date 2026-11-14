// New for musivum: upstream has no test of subscribedtrack.go.
//
// The steps of these tests were also run against the Go original (see the top of
// wrappedreceiver_test.rs, which also has the helpers used here). Where a test says "the
// original printed", the expected text is the line the original printed after that step.
// The down tracks are real ones, not bound to a connection unless a test binds one.

use std::sync::Arc;

use parking_lot::Mutex;

use super::*;
use crate::go::time::MILLISECOND;
use crate::rtc::wrappedreceiver::wrappedreceiver_test::*;
use crate::sfu::FakeTrackReceiver;

#[derive(Default)]
struct Events {
    created: usize,
    closed: Vec<String>,
    maxq: Vec<String>,
    audioc: Vec<String>,
}

struct Sub {
    st: Arc<SubscribedTrack>,
    sub: Arc<types::FakeLocalParticipant>,
    mt: Arc<types::FakeMediaTrack>,
    tl: Arc<types::FakeParticipantTelemetryListener>,
    rcv: Arc<FakeTrackReceiver>,
    events: Arc<Mutex<Events>>,
}

impl Sub {
    fn events(&self) -> String {
        let e = self.events.lock();
        format!(
            "created={} closed=[{}] maxq=[{}] audioc=[{}]",
            e.created,
            e.closed.join(" "),
            e.maxq.join(" "),
            e.audioc.join(" ")
        )
    }

    fn dt(&self) -> Arc<sfu::DownTrack> {
        self.st
            .down_track()
            .expect("a subscribed track has a down track")
    }
}

// A subscription of "PA_sub" to a track of "PA_pub": a camera track with three layers in VP8,
// or a microphone track in opus.
fn new_sub(kind: livekit::TrackType, adaptive: bool, start_desired: bool, hidden: bool) -> Sub {
    let sub = new_participant("PA_sub", hidden);
    let tl = Arc::new(types::FakeParticipantTelemetryListener::default());
    let (ti, c) = if kind == livekit::TrackType::Audio {
        (audio_info(), opus())
    } else {
        (video_info(), vp8())
    };
    let mt = new_media_track(&ti);
    let rcv = receiver(&c);
    let wr = wrapped(
        &[(&rcv, 0)],
        &ti.sid,
        &format!("PA_pub|{}", ti.sid),
        vec![c],
        true,
        false,
    );
    let events = Arc::new(Mutex::new(Events::default()));
    let (created, closed, maxq, audioc) = (
        events.clone(),
        events.clone(),
        events.clone(),
        events.clone(),
    );
    let st = SubscribedTrack::new(SubscribedTrackParams {
        receiver_config: ReceiverConfig {
            packet_buffer_size_video: 300,
            packet_buffer_size_audio: 200,
        },
        subscriber_config: DirectionConfig::default(),
        subscriber: sub.clone(),
        media_track: mt.clone(),
        adaptive_stream: adaptive,
        enable_start_at_desired_quality: start_desired,
        telemetry_listener: tl.clone(),
        wrapped_receiver: wr,
        is_relayed: false,
        on_down_track_created: Some(Arc::new(move |_dt| {
            created.lock().created += 1;
        })),
        on_down_track_closed: Some(Box::new(move |id| {
            closed.lock().closed.push(id.to_string());
        })),
        on_subscriber_max_quality_change: Some(Arc::new(move |id, mime_type, layer| {
            maxq.lock()
                .maxq
                .push(format!("{}/{}/{}", bare_id(id), mime_type, layer));
        })),
        on_subscriber_audio_codec_change: Some(Arc::new(move |id, mime_type, enabled| {
            audioc
                .lock()
                .audioc
                .push(format!("{}/{}/{}", bare_id(id), mime_type, enabled));
        })),
    })
    .expect("the subscribed track is made");
    Sub {
        st,
        sub,
        mt,
        tl,
        rcv,
        events,
    }
}

fn settings(f: impl FnOnce(&mut livekit::UpdateTrackSettings)) -> livekit::UpdateTrackSettings {
    let mut s = livekit::UpdateTrackSettings::default();
    f(&mut s);
    s
}

const VIDEO_DT: &str = "sub=PA_sub track=TR_vid stream=PA_pub|camera mime=video/VP8";
const AUDIO_DT: &str = "sub=PA_sub track=TR_aud stream=PA_pub|camera mime=audio/opus";

// Expected values: the original printed them. (The original's line also says that there is
// no sender yet, which is so here too.)
#[tokio::test(flavor = "multi_thread")]
async fn subscribed_track_is_made_with_a_down_track_of_the_subscribers_parameters() {
    let m = new_sub(livekit::TrackType::Video, false, false, false);
    let st = &m.st;
    let dt = m.dt();
    assert_eq!(
        format!(
            "id=TR_vid pub=PA_pub/pub/7 sub=PA_sub/sub muted=false bound=false neg=false sender-nil=true kind=video dt-sub=PA_sub {VIDEO_DT} muted=false pubmuted=false max=-1/3 closed=false"
        ),
        format!(
            "id={} pub={}/{}/{} sub={}/{} muted={} bound={} neg={} sender-nil={} kind={} dt-sub={} {}",
            st.id(),
            st.publisher_id(),
            st.publisher_identity(),
            st.publisher_version(),
            st.subscriber_id(),
            st.subscriber_identity(),
            st.is_muted(),
            st.is_bound(),
            st.needs_negotiation(),
            st.rtp_sender().is_none(),
            dt.kind(),
            bare_id(&dt.subscriber_id()),
            dt_str(&dt)
        )
    );
    assert_eq!("created=1 closed=[] maxq=[] audioc=[]", m.events());
    // the wrapped receiver keeps the down track's "ready" callback until a codec is negotiated
    assert_eq!(0, m.rcv.add_on_ready_call_count());

    // Not from the original: the subscriber and the media track are the ones it was made with.
    let subscriber: Arc<dyn types::LocalParticipant> = m.sub.clone();
    assert!(Arc::ptr_eq(&subscriber, &st.subscriber()));
    let media_track: Arc<dyn types::MediaTrack> = m.mt.clone();
    assert!(
        st.media_track()
            .is_some_and(|mt| Arc::ptr_eq(&media_track, &mt))
    );
    st.set_needs_negotiation(true);
    assert!(st.needs_negotiation());
    let rtp_sender: Arc<dyn webrtc::RtpSender> = Arc::new(TestSender);
    st.set_rtp_sender(Some(rtp_sender.clone()));
    assert!(
        st.rtp_sender()
            .is_some_and(|s| Arc::ptr_eq(&s, &rtp_sender))
    );
}

// Expected values: the original printed them.
#[tokio::test(flavor = "multi_thread")]
async fn subscribed_track_applies_quality_dimensions_and_fps_to_its_down_track() {
    let m = new_sub(livekit::TrackType::Video, false, false, false);
    let st = &m.st;
    let dt = m.dt();

    st.update_subscriber_settings(
        &settings(|s| s.quality = livekit::VideoQuality::Low as i32),
        true,
    );
    assert_eq!(
        format!("{VIDEO_DT} muted=false pubmuted=false max=0/3 closed=false qfd=0 tfps=0"),
        format!(
            "{} qfd={} tfps={}",
            dt_str(&dt),
            m.mt.get_quality_for_dimension_call_count(),
            m.mt.get_temporal_layer_for_spatial_fps_call_count()
        )
    );

    // with dimensions the media track is asked for the quality (the fake says MEDIUM)
    st.update_subscriber_settings(
        &settings(|s| {
            s.quality = livekit::VideoQuality::High as i32;
            s.width = 640;
            s.height = 360;
        }),
        true,
    );
    assert_eq!(
        format!("{VIDEO_DT} muted=false pubmuted=false max=1/3 closed=false qfd=1"),
        format!(
            "{} qfd={}",
            dt_str(&dt),
            m.mt.get_quality_for_dimension_call_count()
        )
    );
    assert_eq!(
        (mime::MimeType::Vp8, 640, 360),
        m.mt.get_quality_for_dimension_args_for_call(0)
    );

    // with a frame rate the media track is asked for the temporal layer (the fake says 1)
    st.update_subscriber_settings(
        &settings(|s| {
            s.quality = livekit::VideoQuality::High as i32;
            s.fps = 15;
        }),
        true,
    );
    assert_eq!(
        format!("{VIDEO_DT} muted=false pubmuted=false max=2/1 closed=false tfps=1"),
        format!(
            "{} tfps={}",
            dt_str(&dt),
            m.mt.get_temporal_layer_for_spatial_fps_call_count()
        )
    );
    assert_eq!(
        (mime::MimeType::Vp8, 2, 15),
        m.mt.get_temporal_layer_for_spatial_fps_args_for_call(0)
    );

    // the same again changes nothing and asks nothing
    st.update_subscriber_settings(
        &settings(|s| {
            s.quality = livekit::VideoQuality::High as i32;
            s.fps = 15;
        }),
        true,
    );
    assert_eq!(1, m.mt.get_temporal_layer_for_spatial_fps_call_count());
    st.update_video_layer();
    assert_eq!(2, m.mt.get_temporal_layer_for_spatial_fps_call_count());
}

// Expected values: the original printed them.
#[tokio::test(flavor = "multi_thread")]
async fn subscribed_track_reports_mute_at_once_and_mutes_its_down_track_after_the_debounce() {
    let m = new_sub(livekit::TrackType::Video, false, false, false);
    let st = &m.st;
    let dt = m.dt();
    st.update_subscriber_settings(
        &settings(|s| {
            s.quality = livekit::VideoQuality::High as i32;
            s.fps = 15;
        }),
        true,
    );

    // mute is debounced, unmute is not
    st.update_subscriber_settings(&settings(|s| s.disabled = true), false);
    assert_eq!(
        format!("is-muted=true {VIDEO_DT} muted=false pubmuted=false max=2/1 closed=false"),
        format!("is-muted={} {}", st.is_muted(), dt_str(&dt))
    );
    assert!(wait(|| dt.debug_info()["Muted"] == true).await);
    assert_eq!(
        format!("is-muted=true {VIDEO_DT} muted=true pubmuted=false max=2/1 closed=false"),
        format!("is-muted={} {}", st.is_muted(), dt_str(&dt))
    );
    st.update_subscriber_settings(
        &settings(|s| s.quality = livekit::VideoQuality::Medium as i32),
        false,
    );
    assert_eq!(
        format!("is-muted=false {VIDEO_DT} muted=false pubmuted=false max=1/1 closed=false"),
        format!("is-muted={} {}", st.is_muted(), dt_str(&dt))
    );

    // a lower quality is debounced too
    st.update_subscriber_settings(
        &settings(|s| s.quality = livekit::VideoQuality::Low as i32),
        false,
    );
    assert_eq!(
        format!("{VIDEO_DT} muted=false pubmuted=false max=1/1 closed=false"),
        dt_str(&dt)
    );
    assert!(wait(|| dt.max_layer().spatial == 0).await);
    assert_eq!(
        format!("{VIDEO_DT} muted=false pubmuted=false max=0/1 closed=false"),
        dt_str(&dt)
    );

    st.set_publisher_muted(true);
    assert_eq!(
        format!("{VIDEO_DT} muted=false pubmuted=true max=0/1 closed=false"),
        dt_str(&dt)
    );
}

// Not from the original: only the last settings within the debounce interval are applied,
// and a subscribed track that is dropped is not kept alive by what waits to be applied.
#[tokio::test(flavor = "multi_thread")]
async fn subscribed_track_applies_the_last_settings_of_a_burst() {
    let m = new_sub(livekit::TrackType::Video, false, false, false);
    let dt = m.dt();
    m.st.update_subscriber_settings(
        &settings(|s| s.quality = livekit::VideoQuality::High as i32),
        true,
    );
    assert_eq!(2, dt.max_layer().spatial);
    for quality in [livekit::VideoQuality::Medium, livekit::VideoQuality::Low] {
        m.st.update_subscriber_settings(&settings(|s| s.quality = quality as i32), false);
    }
    assert_eq!(
        2,
        dt.max_layer().spatial,
        "nothing is applied before the time is over"
    );
    assert!(wait(|| dt.max_layer().spatial == 0).await);

    m.st.update_subscriber_settings(&settings(|s| s.disabled = true), false);
    let weak = Arc::downgrade(&m.st);
    let Sub { st, events, .. } = m;
    drop(st);
    drop(events);
    assert!(weak.upgrade().is_none());
    crate::go::time::sleep(300 * MILLISECOND).await;
    assert_eq!(
        serde_json::Value::Bool(false),
        dt.debug_info()["Muted"],
        "the track that was to apply it is gone"
    );
}

// Expected values: the original printed them, where it ran the same step; the rest is said
// at its place.
#[tokio::test(flavor = "multi_thread")]
async fn subscribed_track_passes_on_what_its_down_track_reports() {
    let m = new_sub(livekit::TrackType::Video, false, false, false);
    let st = &m.st;

    st.on_bind_and_connected();
    assert_eq!(1, m.mt.on_track_subscribed_call_count());

    // (The original printed this entry among those of its down track's own notifier.)
    st.on_max_subscribed_layer_changed(1);
    assert_eq!(
        "created=1 closed=[] maxq=[PA_sub/video/VP8/1] audioc=[]",
        m.events()
    );

    st.on_rtt_update(42);
    assert!(wait(|| m.sub.update_media_rtt_call_count() == 1).await);
    assert_eq!(42, m.sub.update_media_rtt_args_for_call(0));

    let started = Arc::new(Mutex::new(Vec::new()));
    {
        let started = started.clone();
        st.on_subscribe_stream_started(Box::new(move |d| started.lock().push(d)));
    }
    st.on_stream_started(5 * MILLISECOND);
    assert_eq!(1, m.tl.on_track_subscribe_stream_started_call_count());
    assert_eq!(vec![5 * MILLISECOND], *started.lock());
    let (id, ti, d) = m.tl.on_track_subscribe_stream_started_args_for_call(0);
    assert_eq!(
        "PA_sub TR_vid 5ms",
        format!("{} {} {:?}", id, ti.sid, d.to_std())
    );

    // a codec that the track does not have changes nothing
    st.on_codec_negotiated(av1().rtp_codec_capability);
    crate::go::time::sleep(50 * MILLISECOND).await;
    assert_eq!(
        "created=1 closed=[] maxq=[PA_sub/video/VP8/1] audioc=[]",
        m.events()
    );
    // one that it has: the highest layer of that codec is announced as wanted
    st.on_codec_negotiated(vp8().rtp_codec_capability);
    assert!(wait(|| m.events.lock().maxq.len() == 2).await);
    assert_eq!(
        "created=1 closed=[] maxq=[PA_sub/video/VP8/1 PA_sub/video/VP8/2] audioc=[]",
        m.events()
    );
    assert_eq!(1, m.rcv.add_on_ready_call_count());

    st.on_stats_update(&livekit::AnalyticsStat::default());
    assert_eq!(1, m.tl.on_track_stats_call_count());
    // Not from the original: the key the statistics are reported under.
    let (key, _) = m.tl.on_track_stats_args_for_call(0);
    assert_eq!(
        telemetry::stats_key_for_track(
            "NL",
            livekit::StreamType::Downstream,
            "PA_sub".into(),
            "TR_vid".into(),
            livekit::TrackSource::Camera,
            livekit::TrackType::Video,
        ),
        key
    );

    let closes = Arc::new(Mutex::new(Vec::new()));
    {
        let closes = closes.clone();
        st.on_close(Some(Box::new(move |b| closes.lock().push(b))));
    }
    st.on_down_track_close(true);
    st.on_down_track_close(false);
    assert_eq!(
        "closes=[true false] cache=0 created=1 closed=[PA_sub PA_sub] maxq=[PA_sub/video/VP8/1 PA_sub/video/VP8/2] audioc=[]",
        format!(
            "closes=[{}] cache={} {}",
            closes
                .lock()
                .iter()
                .map(|b: &bool| b.to_string())
                .collect::<Vec<_>>()
                .join(" "),
            m.sub.cache_down_track_call_count(),
            m.events()
        )
    );

    // Not from the original (its fake has no transceiver to give): a down track that closes
    // expecting to resume leaves its transceiver with the subscriber.
    let transceiver = new_transceiver();
    m.dt().set_transceiver(transceiver.clone());
    st.on_down_track_close(false);
    assert_eq!(0, m.sub.cache_down_track_call_count());
    st.on_down_track_close(true);
    assert_eq!(1, m.sub.cache_down_track_call_count());
    let (track_id, cached, _) = m.sub.cache_down_track_args_for_call(0);
    assert_eq!("TR_vid", track_id.as_str());
    assert!(Arc::ptr_eq(&transceiver, &cached));
    // and without a callback nothing is called
    st.on_close(None);
    st.close(false);
    assert_eq!(4, closes.lock().len());

    // hidden subscribers are not announced
    let h = new_sub(livekit::TrackType::Video, false, false, true);
    h.st.on_bind_and_connected();
    assert_eq!(0, h.mt.on_track_subscribed_call_count());
}

// Expected values: the original printed them.
#[tokio::test(flavor = "multi_thread")]
async fn subscribed_track_of_an_audio_track_has_no_layers() {
    let au = new_sub(livekit::TrackType::Audio, false, false, false);
    let dt = au.dt();
    assert_eq!(
        format!("{AUDIO_DT} muted=false pubmuted=false max=-1/-1 closed=false kind=audio"),
        format!("{} kind={}", dt_str(&dt), dt.kind())
    );
    au.st.on_codec_negotiated(opus().rtp_codec_capability);
    assert!(wait(|| au.events.lock().audioc.len() == 1).await);
    assert_eq!(
        "created=1 closed=[] maxq=[] audioc=[PA_sub/audio/opus/true]",
        au.events()
    );
    au.st
        .update_subscriber_settings(&settings(|s| s.disabled = true), true);
    assert_eq!(
        format!("{AUDIO_DT} muted=true pubmuted=false max=-1/-1 closed=false qfd=0"),
        format!(
            "{} qfd={}",
            dt_str(&dt),
            au.mt.get_quality_for_dimension_call_count()
        )
    );
    au.st.bound(Ok(()));
    crate::go::time::sleep(20 * MILLISECOND).await;
    assert_eq!(
        format!(
            "bound=true is-muted=true {AUDIO_DT} muted=true pubmuted=false max=-1/-1 closed=false"
        ),
        format!(
            "bound={} is-muted={} {}",
            au.st.is_bound(),
            au.st.is_muted(),
            dt_str(&dt)
        )
    );
}

// Expected values: the original printed them. The callbacks of the binding were given nil
// there ("<nil>" in its line) and `Ok` here.
#[tokio::test(flavor = "multi_thread")]
async fn subscribed_track_chooses_its_first_settings_when_it_is_bound() {
    let disabled = settings(|s| {
        s.disabled = true;
        s.quality = livekit::VideoQuality::Medium as i32;
    });
    let table: [(
        &str,
        bool,
        bool,
        Option<&livekit::UpdateTrackSettings>,
        &str,
    ); 6] = [
        (
            "plain",
            false,
            false,
            None,
            "bound=true is-muted=false binds=[<nil> <nil>] muted=false pubmuted=false max=2/3",
        ),
        (
            "adaptive",
            true,
            false,
            None,
            "bound=true is-muted=false binds=[<nil> <nil>] muted=false pubmuted=false max=0/3",
        ),
        (
            "adaptive+desired",
            true,
            true,
            None,
            "bound=true is-muted=false binds=[<nil> <nil>] muted=false pubmuted=false max=2/3",
        ),
        (
            "desired",
            false,
            true,
            None,
            "bound=true is-muted=false binds=[<nil> <nil>] muted=false pubmuted=false max=2/3",
        ),
        (
            "adaptive disabled before",
            true,
            false,
            Some(&disabled),
            "bound=true is-muted=false binds=[<nil> <nil>] muted=false pubmuted=false max=1/3",
        ),
        (
            "plain disabled before",
            false,
            false,
            Some(&disabled),
            "bound=true is-muted=true binds=[<nil> <nil>] muted=true pubmuted=false max=-1/3",
        ),
    ];
    let line = |b: &Sub, binds: &Mutex<Vec<String>>| {
        let dt = b.dt();
        let di = dt.debug_info();
        let ml = dt.max_layer();
        format!(
            "bound={} is-muted={} binds=[{}] muted={} pubmuted={} max={}/{}",
            b.st.is_bound(),
            b.st.is_muted(),
            binds.lock().join(" "),
            di["Muted"],
            di["PubMuted"],
            ml.spatial,
            ml.temporal
        )
    };
    let on_bind = |binds: &Arc<Mutex<Vec<String>>>| {
        let binds = binds.clone();
        Box::new(move |err: Result<(), Error>| {
            binds.lock().push(match err {
                Ok(()) => "<nil>".to_string(),
                Err(err) => err.to_string(),
            });
        })
    };
    for (name, adaptive, start_desired, before, expected) in table {
        let b = new_sub(livekit::TrackType::Video, adaptive, start_desired, false);
        if let Some(before) = before {
            b.st.update_subscriber_settings(before, true);
        }
        let binds = Arc::new(Mutex::new(Vec::new()));
        b.st.add_on_bind(on_bind(&binds));
        b.st.bound(Ok(()));
        assert!(wait(|| binds.lock().len() == 1).await, "{name}");
        // one that is added to a bound track is called at once
        b.st.add_on_bind(on_bind(&binds));
        assert!(wait(|| binds.lock().len() == 2).await, "{name}");
        assert_eq!(expected, line(&b, &binds), "{name}");
    }

    // an error of the binding: not bound, no settings, and the callbacks get the error (the
    // original printed the text of the error it was given, as this does)
    let e = new_sub(livekit::TrackType::Video, true, false, false);
    let binds = Arc::new(Mutex::new(Vec::new()));
    e.st.add_on_bind(on_bind(&binds));
    e.st.bound(Err(Error::TrackNotBound));
    assert!(wait(|| binds.lock().len() == 1).await);
    assert_eq!(
        "bound=false is-muted=false binds=[track not bound] muted=false pubmuted=false max=-1/3",
        line(&e, &binds)
    );
}
