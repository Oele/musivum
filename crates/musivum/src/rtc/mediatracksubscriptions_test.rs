// New for musivum: upstream has no test of mediatracksubscriptions.go.
//
// The steps of these tests were also run against the Go original (see the top of
// wrappedreceiver_test.rs, which also has the helpers used here). Where a test says "the
// original printed", the expected text is the line the original printed after that step;
// where the line here differs, the comment at it says why. The down tracks are real ones.

use std::sync::Arc;

use parking_lot::Mutex;

use super::*;
use crate::protocol::logger;
use crate::rtc::types::{FakeLocalParticipant, FakeMediaTrack};
use crate::rtc::wrappedreceiver::wrappedreceiver_test::*;
use crate::sfu::FakeTrackReceiver;

// The subscriptions of a camera track of "PA_pub" in VP8 and H264, with what they told.
struct Mts {
    mts: Arc<MediaTrackSubscriptions>,
    mt: Arc<FakeMediaTrack>,
    vp8: Arc<FakeTrackReceiver>,
    h264: Arc<FakeTrackReceiver>,
    created: Arc<Mutex<usize>>,
    maxq: Arc<Mutex<Vec<String>>>,
}

impl Mts {
    fn new_wr(&self) -> Arc<WrappedReceiver> {
        wrapped(
            &[(&self.vp8, 0), (&self.h264, 1)],
            "TR_vid",
            "PA_pub|TR_vid",
            vec![vp8(), h264()],
            false,
            false,
        )
    }

    fn created(&self) -> usize {
        *self.created.lock()
    }
}

fn new_subscriptions(mt: Arc<FakeMediaTrack>) -> Arc<MediaTrackSubscriptions> {
    let media_track: Arc<dyn types::MediaTrack> = mt;
    MediaTrackSubscriptions::new(MediaTrackSubscriptionsParams {
        media_track: Some(Arc::downgrade(&media_track)),
        receiver_config: ReceiverConfig {
            packet_buffer_size_video: 300,
            packet_buffer_size_audio: 200,
        },
        logger: logger::get_logger(),
        ..Default::default()
    })
}

fn new_mts() -> Mts {
    let mt = new_media_track(&video_info());
    let mts = new_subscriptions(mt.clone());
    let created = Arc::new(Mutex::new(0));
    {
        let created = created.clone();
        mts.on_down_track_created(move |_| *created.lock() += 1);
    }
    let maxq = Arc::new(Mutex::new(Vec::new()));
    {
        let maxq = maxq.clone();
        mts.on_subscriber_max_quality_change(move |id, mime_type, layer| {
            maxq.lock()
                .push(format!("{}/{}/{}", bare_id(id), mime_type, layer));
        });
    }
    Mts {
        mts,
        mt,
        vp8: receiver(&vp8()),
        h264: receiver(&h264()),
        created,
        maxq,
    }
}

const VIDEO_DT: &str = "sub=PA_sub track=TR_vid stream=PA_pub|camera";

// Expected values: the original printed them, except where said.
#[tokio::test(flavor = "multi_thread")]
async fn add_subscriber_makes_a_down_track_and_hands_it_to_the_transport() {
    let m = new_mts();
    let sub = new_participant("PA_sub", false);
    sub.supports_transceiver_reuse_returns(true);
    let st = m
        .mts
        .add_subscriber(sub.clone(), m.new_wr())
        .expect("the subscriber is added");
    assert_eq!(
        "created=1 addtrack=1 addtransceiver=0 verify=1 uncache=1 getcached=1 removetrack=0 cache=0 reuse-asked=1",
        format!("created={} {}", m.created(), sub_calls(&sub))
    );
    let dt = st
        .down_track()
        .expect("a subscribed track has a down track");
    // The original's fake transport gave no sender ("sender-nil=true" in its line); the one
    // here gives one, which the subscribed track keeps.
    assert_eq!(
        format!(
            "id=TR_vid sub=PA_sub pub=PA_pub neg=true sender-nil=false bound=false {VIDEO_DT} mime=video/VP8 muted=false pubmuted=false max=-1/3 closed=false"
        ),
        format!(
            "id={} sub={} pub={} neg={} sender-nil={} bound={} {}",
            st.id(),
            st.subscriber_id(),
            st.publisher_id(),
            st.needs_negotiation(),
            st.rtp_sender().is_none(),
            st.is_bound(),
            dt_str(&dt)
        )
    );
    // the down track is what the transport got, with the parameters of the track
    let (track_local, params) = sub.add_track_local_args_for_call(0);
    let dt_local: Arc<dyn webrtc::TrackLocal> = dt.clone();
    assert!(Arc::ptr_eq(&dt_local, &track_local));
    assert_eq!(
        "stereo=false red=false",
        format!("stereo={} red={}", params.stereo, params.red)
    );
    let (id, version) = sub.verify_subscribe_participant_info_args_for_call(0);
    assert_eq!("PA_pub 7", format!("{id} {version}"));
    // Not from the original: the transceiver the transport gave is the down track's, and
    // the one taken out of the cache.
    let transceiver = dt
        .get_transceiver()
        .expect("the down track has its transceiver");
    assert_eq!("0", transceiver.mid());
    assert!(Arc::ptr_eq(
        &transceiver,
        &sub.uncache_down_track_args_for_call(0)
    ));
    assert_eq!(
        "is=true other=false num=1 all=[PA_sub] vp8=[PA_sub] h264=[]",
        format!(
            "is={} other={} num={} all={} vp8={} h264={}",
            m.mts.is_subscriber(&"PA_sub".into()),
            m.mts.is_subscriber(&"PA_other".into()),
            m.mts.get_num_subscribers(),
            sorted(m.mts.get_all_subscribers()),
            sorted(m.mts.get_all_subscribers_for_mime(mime::MimeType::Vp8)),
            sorted(m.mts.get_all_subscribers_for_mime(mime::MimeType::H264))
        )
    );

    // a second time for the same subscriber is refused, before anything is made
    let err = m.mts.add_subscriber(sub.clone(), m.new_wr()).err();
    assert_eq!(
        "err=already subscribed created=1 num=1 addtrack=1 addtransceiver=0 verify=1 uncache=1 getcached=1 removetrack=0 cache=0 reuse-asked=1",
        format!(
            "err={} created={} num={} {}",
            err.map(|e| e.to_string()).unwrap_or_default(),
            m.created(),
            m.mts.get_num_subscribers(),
            sub_calls(&sub)
        )
    );
    assert_eq!(
        Some(Error::AlreadySubscribed),
        m.mts.add_subscriber(sub, m.new_wr()).err()
    );

    // binding: the down track is added to the receiver of the negotiated codec, and the
    // subscribed track is bound and takes the publisher's mute
    m.mt.is_muted_returns(true);
    let res = dt.bind(Arc::new(TestContext {
        codecs: vec![h264()],
    }));
    assert!(res.is_ok());
    // (The subscribed track is bound before it applies its first settings, and takes the
    // publisher's mute after: the original's test waited for the binding alone and printed
    // what it then saw, as the line below has it.)
    assert!(
        wait(|| st.is_bound()
            && dt.max_layer().spatial == 2
            && dt.debug_info()["PubMuted"] == true)
        .await
    );
    // the negotiated codec's highest layer is announced as wanted
    let announced = "PA_sub/video/H264/2".to_string();
    assert!(wait(|| m.maxq.lock().contains(&announced)).await);
    assert_eq!(
        format!(
            "vp8.add=0 h264.add=1 {VIDEO_DT} mime=video/H264 muted=false pubmuted=true max=2/3 closed=false"
        ),
        format!(
            "vp8.add={} h264.add={} {}",
            m.vp8.add_down_track_call_count(),
            m.h264.add_down_track_call_count(),
            dt_str(&dt)
        )
    );
    // The original printed "maxq=[PA_sub/video/H264/-1 PA_sub/video/H264/2
    // PA_sub/video/H264/2]": next to that announcement, what the down track's notifier
    // reported. The notifier reports the layer as it is when its task gets to it, so how
    // many entries it adds and which depends on timing, in the original too; they are of
    // the negotiated codec.
    assert!(
        m.maxq
            .lock()
            .iter()
            .all(|e| e.starts_with("PA_sub/video/H264/"))
    );
    assert_eq!(
        "vp8=[] h264=[PA_sub]",
        format!(
            "vp8={} h264={}",
            sorted(m.mts.get_all_subscribers_for_mime(mime::MimeType::Vp8)),
            sorted(m.mts.get_all_subscribers_for_mime(mime::MimeType::H264))
        )
    );

    m.mts.set_muted(false);
    assert_eq!(
        format!("{VIDEO_DT} mime=video/H264 muted=false pubmuted=false max=2/3 closed=false"),
        dt_str(&dt)
    );
    m.mts.update_video_layers();
    // Not from the original: the debug info is the down track's.
    let di = m.mts.debug_info();
    assert_eq!(1, di.len());
    assert_eq!(dt.debug_info(), di[0]);
}

// Expected values: the original printed them, except where said.
#[tokio::test(flavor = "multi_thread")]
async fn remove_subscriber_closes_the_down_track() {
    let m = new_mts();
    let sub = new_participant("PA_sub", false);
    sub.supports_transceiver_reuse_returns(true);
    let st = m.mts.add_subscriber(sub.clone(), m.new_wr()).unwrap();
    let dt = st.down_track().unwrap();
    dt.bind(Arc::new(TestContext {
        codecs: vec![h264()],
    }))
    .unwrap();
    assert!(wait(|| st.is_bound()).await);

    assert_eq!(
        Err(Error::NotFound),
        m.mts.remove_subscriber(&"PA_other".into(), false)
    );
    assert_eq!("not found", Error::NotFound.to_string());
    let res = m.mts.remove_subscriber(&"PA_sub".into(), false);
    let done = wait(|| {
        dt.is_closed()
            && m.mts.get_num_subscribers() == 0
            && sub.remove_track_local_call_count() == 1
    })
    .await;
    assert_eq!(
        "ok=true done=true num=0 h264.delete=1 addtrack=1 addtransceiver=0 verify=1 uncache=1 getcached=1 removetrack=1 cache=0 reuse-asked=1",
        format!(
            "ok={} done={} num={} h264.delete={} {}",
            res.is_ok(),
            done,
            m.mts.get_num_subscribers(),
            m.h264.delete_down_track_call_count(),
            sub_calls(&sub)
        )
    );
    // Not from the original: the sender taken off the connection is the subscription's.
    let sender = st.rtp_sender().unwrap();
    assert!(Arc::ptr_eq(
        &sender,
        &sub.remove_track_local_args_for_call(0)
    ));
    assert_eq!(
        Err(Error::NotFound),
        m.mts.remove_subscriber(&"PA_sub".into(), false)
    );

    // a subscriber without transceiver reuse, removed expecting to resume: closed at once,
    // and its sender stays
    let sub2 = new_participant("PA_sub2", false);
    let st2 = m.mts.add_subscriber(sub2.clone(), m.new_wr()).unwrap();
    assert_eq!(
        "created=2 addtrack=0 addtransceiver=1 verify=1 uncache=1 getcached=1 removetrack=0 cache=0 reuse-asked=1",
        format!("created={} {}", m.created(), sub_calls(&sub2))
    );
    let res = m.mts.remove_subscriber(&"PA_sub2".into(), true);
    // The original's fake transport gave no transceiver ("cache=0" in its line); the one here
    // gives one, which the down track that closes expecting to resume leaves in the cache.
    assert_eq!(
        "ok=true closed=true num=0 addtrack=0 addtransceiver=1 verify=1 uncache=1 getcached=1 removetrack=0 cache=1 reuse-asked=1",
        format!(
            "ok={} closed={} num={} {}",
            res.is_ok(),
            st2.down_track().unwrap().is_closed(),
            m.mts.get_num_subscribers(),
            sub_calls(&sub2)
        )
    );

    // the transport refuses the track: nothing is kept
    let sub3 = new_participant("PA_sub3", false);
    sub3.add_transceiver_from_track_local_returns(Err(Error::Webrtc(webrtc::Error::Other(
        "no transport".to_string(),
    ))));
    let res = m.mts.add_subscriber(sub3.clone(), m.new_wr());
    assert_eq!(
        "err=no transport nil=true num=0 created=3 addtrack=0 addtransceiver=1 verify=1 uncache=0 getcached=1 removetrack=0 cache=0 reuse-asked=1",
        format!(
            "err={} nil={} num={} created={} {}",
            res.as_ref()
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default(),
            res.is_err(),
            m.mts.get_num_subscribers(),
            m.created(),
            sub_calls(&sub3)
        )
    );
}

// What a cached transceiver gives: a sender that takes a new track or refuses it (pion's
// refuses one of another kind), and that says whether it was stopped.
struct CachedSender {
    refuse: bool,
    replaced: Mutex<Vec<Option<Arc<dyn webrtc::TrackLocal>>>>,
}

impl webrtc::RtpSender for CachedSender {
    fn get_parameters(&self) -> webrtc::RtpSendParameters {
        webrtc::RtpSendParameters::default()
    }

    fn replace_track(
        &self,
        track: Option<Arc<dyn webrtc::TrackLocal>>,
    ) -> Result<(), webrtc::Error> {
        if self.refuse {
            return Err(webrtc::Error::Other(
                "new track must be of the same kind as previous".to_string(),
            ));
        }
        self.replaced.lock().push(track);
        Ok(())
    }
}

struct CachedTransceiver {
    sender: Arc<CachedSender>,
    stopped: crate::go::atomic::Bool,
}

impl webrtc::RtpTransceiver for CachedTransceiver {
    fn sender(&self) -> Option<Arc<dyn webrtc::RtpSender>> {
        Some(self.sender.clone())
    }

    fn mid(&self) -> String {
        "1".to_string()
    }

    fn stop(&self) -> Result<(), webrtc::Error> {
        self.stopped.store(true);
        Ok(())
    }
}

fn cached_transceiver(refuse: bool) -> Arc<CachedTransceiver> {
    Arc::new(CachedTransceiver {
        sender: Arc::new(CachedSender {
            refuse,
            replaced: Mutex::new(Vec::new()),
        }),
        stopped: crate::go::atomic::Bool::default(),
    })
}

fn cached_subscriber(id: &str, transceiver: &Arc<CachedTransceiver>) -> Arc<FakeLocalParticipant> {
    let sub = new_participant(id, false);
    let cached: Arc<dyn webrtc::RtpTransceiver> = transceiver.clone();
    sub.get_cached_down_track_returns((Some(cached), sfu::DownTrackState::default()));
    sub
}

// Expected values: the original printed them, where its cached transceiver was pion's (a
// video one, and an audio one that refuses the video track); "stopped" stands for its
// direction ("sendrecv", and "inactive" once stopped).
#[tokio::test(flavor = "multi_thread")]
async fn add_subscriber_reuses_a_cached_transceiver_through_replace_track() {
    let m = new_mts();
    let cached = cached_transceiver(false);
    let sub4 = cached_subscriber("PA_sub4", &cached);
    sub4.supports_transceiver_reuse_returns(true);
    let st4 = m.mts.add_subscriber(sub4.clone(), m.new_wr()).unwrap();
    let dt4 = st4.down_track().unwrap();
    let cached_dyn: Arc<dyn webrtc::RtpTransceiver> = cached.clone();
    let cached_sender: Arc<dyn webrtc::RtpSender> = cached.sender.clone();
    let dt4_local: Arc<dyn webrtc::TrackLocal> = dt4.clone();
    assert_eq!(
        "neg=false sender-is-cached=true transceiver-is-cached=true replaced=true uncached-is-cached=true stopped=false num=1 addtrack=0 addtransceiver=0 verify=0 uncache=1 getcached=1 removetrack=0 cache=0 reuse-asked=0",
        format!(
            "neg={} sender-is-cached={} transceiver-is-cached={} replaced={} uncached-is-cached={} stopped={} num={} {}",
            st4.needs_negotiation(),
            st4.rtp_sender()
                .is_some_and(|s| Arc::ptr_eq(&s, &cached_sender)),
            dt4.get_transceiver()
                .is_some_and(|t| Arc::ptr_eq(&t, &cached_dyn)),
            cached.sender.replaced.lock().len() == 1
                && cached.sender.replaced.lock()[0]
                    .as_ref()
                    .is_some_and(|t| Arc::ptr_eq(t, &dt4_local)),
            Arc::ptr_eq(&sub4.uncache_down_track_args_for_call(0), &cached_dyn),
            cached.stopped.load(),
            m.mts.get_num_subscribers(),
            sub_calls(&sub4)
        )
    );

    // Not from the original, whose replaced track was not bound (pion binds it at once only
    // on a sender that has sent): the binding that follows seeds the down track with the
    // cached state and adds it to the receiver.
    dt4.bind(Arc::new(TestContext {
        codecs: vec![vp8()],
    }))
    .unwrap();
    assert!(wait(|| st4.is_bound()).await);
    assert_eq!(1, m.vp8.add_down_track_call_count());

    // one whose sender refuses the track: stopped, and the down track goes to a new one
    let refusing = cached_transceiver(true);
    let sub5 = cached_subscriber("PA_sub5", &refusing);
    let st5 = m.mts.add_subscriber(sub5.clone(), m.new_wr()).unwrap();
    let refusing_sender: Arc<dyn webrtc::RtpSender> = refusing.sender.clone();
    assert_eq!(
        "neg=true sender-is-cached=false replaced=false stopped=true num=2 addtrack=0 addtransceiver=1 verify=1 uncache=1 getcached=1 removetrack=0 cache=0 reuse-asked=1",
        format!(
            "neg={} sender-is-cached={} replaced={} stopped={} num={} {}",
            st5.needs_negotiation(),
            st5.rtp_sender()
                .is_some_and(|s| Arc::ptr_eq(&s, &refusing_sender)),
            !refusing.sender.replaced.lock().is_empty(),
            refusing.stopped.load(),
            m.mts.get_num_subscribers(),
            sub_calls(&sub5)
        )
    );
}

// Expected values: the original printed them.
#[tokio::test(flavor = "multi_thread")]
async fn add_subscriber_asks_for_stereo_and_red_as_the_track_has_them() {
    #[allow(clippy::type_complexity)]
    let table: [(&str, bool, fn(&mut livekit::TrackInfo), &str); 3] = [
        (
            "stereo red",
            false,
            |_| {},
            "stereo=true red=true sub=PA_asub track=TR_aud stream=PA_pub|camera mime=audio/red muted=false pubmuted=false max=-1/-1 closed=false",
        ),
        (
            "stereo opus only",
            true,
            |_| {},
            "stereo=true red=false sub=PA_asub track=TR_aud stream=PA_pub|camera mime=audio/opus muted=false pubmuted=false max=-1/-1 closed=false",
        ),
        (
            "mono red off in info",
            false,
            |ti| {
                ti.audio_features = Vec::new();
                ti.disable_red = true;
            },
            "stereo=false red=false sub=PA_asub track=TR_aud stream=PA_pub|camera mime=audio/red muted=false pubmuted=false max=-1/-1 closed=false",
        ),
    ];
    for (name, disable_red, change, expected) in table {
        let mut ati = audio_info();
        change(&mut ati);
        let amt = new_media_track(&ati);
        let amts = new_subscriptions(amt.clone());
        let opus_r = receiver(&opus());
        let wr = wrapped(
            &[(&opus_r, 0)],
            "TR_aud",
            "PA_pub",
            vec![opus()],
            disable_red,
            false,
        );
        let asub = new_participant("PA_asub", false);
        let ast = amts.add_subscriber(asub.clone(), wr).expect(name);
        let (_, p) = asub.add_transceiver_from_track_local_args_for_call(0);
        assert_eq!(
            expected,
            format!(
                "stereo={} red={} {}",
                p.stereo,
                p.red,
                dt_str(&ast.down_track().unwrap())
            ),
            "{name}"
        );
    }
}

// Not from the original, which leaves this to its garbage collector: the subscriptions do
// not keep their media track alive, and one that is gone takes no subscribers.
#[tokio::test(flavor = "multi_thread")]
async fn subscriptions_of_a_media_track_that_is_gone_take_no_subscribers() {
    let m = new_mts();
    let Mts {
        mts,
        mt,
        vp8: vp8_r,
        ..
    } = m;
    let weak = Arc::downgrade(&mt);
    drop(mt);
    assert!(weak.upgrade().is_none());
    let wr = wrapped(&[(&vp8_r, 0)], "TR_vid", "", vec![vp8()], false, false);
    let sub = new_participant("PA_sub", false);
    assert_eq!(
        Some(Error::NotOpen),
        mts.add_subscriber(sub.clone(), wr).err()
    );
    assert_eq!(0, sub.get_cached_down_track_call_count());
}
