// New for musivum: the original has no tests of the media loss proxy. The interval is the
// original's `downLostUpdateDelta`, a second; a loss is the `uint8` of an RTCP reception
// report, a fraction of 256, and is passed on as it is.

use std::sync::Weak;

use bytes::Bytes;

use super::*;

use crate::protocol::codecs::mime;
use crate::protocol::logger;
use crate::sfu::buffer;
use crate::sfu::pacer;
use crate::sfu::{FakeDownTrackListener, FakeTrackReceiver};
use crate::webrtc;
use crate::webrtc::rtcp::reception_report::ReceptionReport;

// The down track that a receiver report came in on. The proxy does not look at it.
struct TestDownTrack {
    dt: Arc<sfu::DownTrack>,
    _receiver: Arc<FakeTrackReceiver>,
    _listener: Arc<FakeDownTrackListener>,
}

fn new_down_track() -> TestDownTrack {
    let codec = webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: webrtc::MIME_TYPE_OPUS.to_string(),
            clock_rate: 48000,
            channels: 2,
            ..Default::default()
        },
        payload_type: 111,
    };

    let receiver = Arc::new(FakeTrackReceiver::default());
    receiver.track_id_returns("fake-track".into());
    receiver.stream_id_returns("fake-stream".into());
    receiver.codec_returns(codec.clone());
    receiver.mime_returns(mime::normalize_mime_type(&codec.mime_type));
    receiver.video_layer_mode_returns(livekit::video_layer::Mode::Unused);
    receiver.header_extensions_returns(Vec::new());
    receiver.is_closed_returns(false);
    receiver.track_info_returns(Arc::new(livekit::TrackInfo::default()));
    receiver.codec_state_returns(sfu::ReceiverCodecState::Normal);
    receiver.read_rtp_returns(Err(sfu::Error::Eof));

    let listener = Arc::new(FakeDownTrackListener::default());
    let weak_listener = Arc::downgrade(&listener);
    let weak_listener: Weak<dyn sfu::DownTrackListener> = weak_listener;

    let dt = sfu::DownTrack::new(sfu::DownTrackParams {
        codecs: vec![codec],
        is_encrypted: false,
        source: livekit::TrackSource::Microphone,
        receiver: receiver.clone(),
        buffer_factory: buffer::FactoryOfBufferFactory::new(500, 500).create_buffer_factory(),
        sub_id: "fake-sub".into(),
        stream_id: "fake-stream".to_string(),
        max_track: 500,
        playout_delay_limit: None,
        pacer: pacer::PassThrough::new(logger::get_logger(), None),
        logger: logger::get_logger(),
        trailer: Bytes::new(),
        rtcp_writer: None,
        disable_sender_report_pass_through: false,
        supports_codec_change: false,
        strip_packet_trailer: false,
        enable_start_at_desired_quality: false,
        rtx_budget: sfu::RtxBudgetConfig::default(),
        listener: weak_listener,
    })
    .expect("the down track is made");

    TestDownTrack {
        dt,
        _receiver: receiver,
        _listener: listener,
    }
}

fn receiver_report(fractions_lost: &[u8]) -> ReceiverReport {
    ReceiverReport {
        reports: fractions_lost
            .iter()
            .map(|&fraction_lost| ReceptionReport {
                fraction_lost,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

// A proxy and what it passed on.
fn new_media_loss_proxy() -> (Arc<MediaLossProxy>, Arc<Mutex<Vec<u8>>>) {
    let m = MediaLossProxy::new(MediaLossProxyParams {
        logger: logger::get_logger(),
    });
    let passed_on = Arc::new(Mutex::new(Vec::new()));
    let p = passed_on.clone();
    m.on_media_loss_update(Some(Box::new(move |fractional_loss| {
        p.lock().push(fractional_loss);
    })));
    (m, passed_on)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_worst_loss_of_an_interval_is_passed_on_once() {
    assert_eq!(time::SECOND, DOWN_LOST_UPDATE_DELTA);
    // more than the interval, by a margin for a loaded machine
    let interval = (DOWN_LOST_UPDATE_DELTA + Duration::from_millis(50)).to_std();

    let down_track = new_down_track();
    let dt = &down_track.dt;
    let node_id = livekit::NodeId::from("ND_other");
    let (m, passed_on) = new_media_loss_proxy();

    // nothing was reported: nothing is passed on, however long ago the last time was
    m.handle_max_loss_feedback(dt, &receiver_report(&[]));
    assert_eq!(Vec::<u8>::new(), *passed_on.lock());

    // the first losses are passed on at once, the worst of them: 51 of 256 is 20 %
    m.handle_max_loss_feedback(dt, &receiver_report(&[13, 51, 26]));
    assert_eq!(vec![51], *passed_on.lock());

    // within the interval the losses are collected, of subscribers here and on other nodes
    m.handle_max_loss_feedback(dt, &receiver_report(&[77]));
    m.notify_subscriber_node_media_loss(&node_id, 128);
    m.handle_max_loss_feedback(dt, &receiver_report(&[3, 102]));
    assert_eq!(vec![51], *passed_on.lock());

    // the interval passes: the proxy has no timer, it is the next report that passes the
    // worst loss on, which is not that report's own
    tokio::time::sleep(interval).await;
    assert_eq!(vec![51], *passed_on.lock());
    m.handle_max_loss_feedback(dt, &receiver_report(&[64]));
    assert_eq!(vec![51, 128], *passed_on.lock());

    // the next interval starts from nothing: its worst loss is 5, and a receiver report
    // without reception reports is enough to pass it on when the interval is over
    m.notify_subscriber_node_media_loss(&node_id, 5);
    assert_eq!(vec![51, 128], *passed_on.lock());
    tokio::time::sleep(interval).await;
    m.handle_max_loss_feedback(dt, &receiver_report(&[]));
    assert_eq!(vec![51, 128, 5], *passed_on.lock());
}

#[test]
fn a_loss_of_zero_is_a_report_and_is_passed_on() {
    let (m, passed_on) = new_media_loss_proxy();

    m.notify_subscriber_node_media_loss(&"ND_other".into(), 0);
    assert_eq!(vec![0], *passed_on.lock());
}

#[test]
fn the_callback_is_called_with_no_lock_held_and_can_be_taken_away() {
    let m = MediaLossProxy::new(MediaLossProxyParams {
        logger: logger::get_logger(),
    });
    let node_id = livekit::NodeId::from("ND_other");

    // a callback that reports a loss itself: that waits for the lock
    let passed_on = Arc::new(Mutex::new(Vec::new()));
    let p = passed_on.clone();
    let weak = Arc::downgrade(&m);
    m.on_media_loss_update(Some(Box::new(move |fractional_loss| {
        p.lock().push(fractional_loss);
        if let Some(m) = weak.upgrade() {
            m.notify_subscriber_node_media_loss(&"ND_third".into(), 7);
        }
    })));
    m.notify_subscriber_node_media_loss(&node_id, 200);
    assert_eq!(vec![200], *passed_on.lock());
    // what the callback reported waits for the interval to pass
    assert_eq!(7, m.lock.lock().max_down_frac_lost);
    assert!(m.lock.lock().max_down_frac_lost_valid);

    // without a callback an update happens all the same, and is lost
    m.on_media_loss_update(None);
    m.lock.lock().max_down_frac_lost_ts = Time::ZERO;
    m.notify_subscriber_node_media_loss(&node_id, 100);
    assert_eq!(vec![200], *passed_on.lock());
    assert_eq!(0, m.lock.lock().max_down_frac_lost);
    assert!(!m.lock.lock().max_down_frac_lost_valid);
}
