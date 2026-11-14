// Tests of `#[musivum_fakes::fake]` and of `testutils::fakes`, which its generated code is
// built from. Written new for musivum, except the two parts that say where they are ported
// from: they are there to show that a test written against counterfeiter's fakes translates
// line by line.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::fakes::{self, NotRecorded};
use crate::protocol::codecs::mime::{self, MimeType};
use crate::protocol::livekit::{
    self, ParticipantId, ParticipantIdentity, RtcpSenderReportState, TrackId, TrackInfo,
};
use crate::sfu::buffer::ExtPacket;
use crate::sfu::utils::{
    DownTrackSpreader, DownTrackSpreaderParams, RtpWriter, Sender, broadcast_rtp,
};
use crate::webrtc::{
    MIME_TYPE_OPUS, PayloadType, RtpCodecCapability, RtpCodecParameters,
    RtpHeaderExtensionParameter,
};

// ------------------------------------------------
// Stand-ins for what the traits below take and return and the port does not have yet

/// `sfu.Bitrates`.
pub type Bitrates = [[i64; 4]; 3];

/// `sfu.ReceiverCodecState`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReceiverCodecState {
    #[default]
    Normal,
    Suspended,
    Invalid,
}

/// An error that is not `Clone`, as one that wraps an `io::Error` is not.
#[derive(Debug)]
pub enum Error {
    Eof,
    Io(std::io::Error),
}

/// Something that is neither `Clone` nor `Default`.
#[derive(Debug, PartialEq)]
pub struct Token(u32);

fn assert_send_sync<T: Send + Sync>() {}

// ------------------------------------------------
// The upstream interfaces `TrackReceiver` and `TrackSender`, and what upstream's tests do
// with their fakes
//
// Ported from LiveKit, pkg/sfu/interfaces.go at v1.13.11 (the interfaces `TrackReceiver` and
// `TrackSender`).
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for this test only: the traits of the port are written
// with the modules that implement them. Types that are not ported yet are the stand-ins
// above, `DebugInfo` gives a map of strings, a receiver that Go may return as nil is an
// `Option`, and Go's `(int, error)` is a `Result`.

mod interfaces {
    use super::*;

    // TrackReceiver defines an interface receive media from remote peer
    #[musivum_fakes::fake]
    pub trait TrackReceiver: Send + Sync {
        fn track_id(&self) -> TrackId;
        fn stream_id(&self) -> String;

        // returns the initial codec of the receiver, it is determined by the track's codec
        // and will not change if the codec changes during the session (publisher changes codec)
        fn codec(&self) -> RtpCodecParameters;
        fn mime(&self) -> MimeType;
        fn video_layer_mode(&self) -> livekit::video_layer::Mode;
        fn header_extensions(&self) -> Vec<RtpHeaderExtensionParameter>;
        fn is_closed(&self) -> bool;

        fn read_rtp(&self, buf: &mut [u8], layer: u8, esn: u64) -> Result<usize, Error>;
        fn get_layered_bitrate(&self) -> (Vec<i32>, Bitrates);

        fn get_audio_level(&self) -> (f64, bool);

        fn send_pli(&self, layer: i32, force: bool);

        fn set_max_expected_spatial_layer(&self, layer: i32);

        fn add_down_track(&self, track: Arc<dyn TrackSender>) -> Result<(), Error>;
        fn delete_down_track(&self, participant_id: &ParticipantId);
        fn get_down_tracks(&self) -> Vec<Arc<dyn TrackSender>>;

        fn debug_info(&self) -> HashMap<String, String>;

        fn track_info(&self) -> Option<Arc<TrackInfo>>;
        fn update_track_info(&self, ti: &TrackInfo);

        // Get primary receiver if this receiver represents a RED codec; otherwise it will return itself
        fn get_primary_receiver_for_red(&self) -> Arc<dyn TrackReceiver>;

        // Get red receiver for primary codec, used by forward red encodings for opus only codec
        fn get_red_receiver(&self) -> Arc<dyn TrackReceiver>;

        fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32>;

        fn get_track_stats(&self) -> Option<livekit::RtpStats>;

        // AddOnReady adds a function to be called when the receiver is ready, the callback
        // could be called immediately if the receiver is ready when the callback is added
        fn add_on_ready(&self, f: Box<dyn FnOnce() + Send>);

        fn add_on_codec_state_change(
            &self,
            f: Box<dyn Fn(RtpCodecParameters, ReceiverCodecState) + Send + Sync>,
        );
        fn codec_state(&self) -> ReceiverCodecState;

        // closes all associated buffers and issues a resync to all attached downtracks so that
        // they can resync and have proper sequncing without gaps in sequence numbers / timestamps
        fn restart(&self, reason: &str);
    }

    // TrackSender defines an interface send media to remote peer
    #[musivum_fakes::fake]
    pub trait TrackSender: Send + Sync {
        fn up_track_layers_change(&self);
        fn up_track_bitrate_availability_change(&self);
        fn up_track_max_published_layer_change(&self, max_published_layer: i32);
        fn up_track_max_temporal_layer_seen_change(&self, max_temporal_layer_seen: i32);
        fn up_track_bitrate_report(&self, available_layers: &[i32], bitrates: Bitrates);
        fn write_rtp(&self, p: &ExtPacket, layer: i32) -> i32;
        fn close(&self);
        fn is_closed(&self) -> bool;
        // ID is the globally unique identifier for this Track.
        fn id(&self) -> String;
        fn subscriber_id(&self) -> ParticipantId;
        fn handle_rtcp_sender_report_data(
            &self,
            payload_type: PayloadType,
            layer: i32,
            publisher_sr_data: &RtcpSenderReportState,
        ) -> Result<(), Error>;
        fn resync(&self);
        fn set_receiver(&self, r: Arc<dyn TrackReceiver>);
        fn receiver_restart(&self, r: Arc<dyn TrackReceiver>);
    }
}

use interfaces::{FakeTrackReceiver, FakeTrackSender, TrackReceiver, TrackSender};

// Ported from LiveKit, pkg/sfu/downtrack_downstream_integration_test.go at v1.13.11: the
// helper `newFakeTrackReceiver` and the lines of the tests that configure and question the
// fake receiver. The down track they are about is not ported yet, so what stands between the
// lines here is the least that makes the fake do something.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The original's line is above each line here.

// newFakeTrackReceiver returns a counterfeiter FakeTrackReceiver configured just
// enough for a DownTrack to bind and forward: it reports the given codec and fires
// its readiness callback immediately so Bind completes.
fn new_fake_track_receiver(codec_params: RtpCodecParameters) -> Arc<FakeTrackReceiver> {
    // r := &sfufakes.FakeTrackReceiver{}
    let r = Arc::new(FakeTrackReceiver::default());
    // r.TrackIDReturns("fake-track")
    r.track_id_returns("fake-track".into());
    // r.StreamIDReturns("fake-stream")
    r.stream_id_returns("fake-stream".into());
    // r.CodecReturns(codecParams)
    r.codec_returns(codec_params.clone());
    // r.MimeReturns(mime.NormalizeMimeType(codecParams.MimeType))
    r.mime_returns(mime::normalize_mime_type(&codec_params.mime_type));
    // r.VideoLayerModeReturns(livekit.VideoLayer_MODE_UNUSED)
    r.video_layer_mode_returns(livekit::video_layer::Mode::Unused);
    // r.HeaderExtensionsReturns(nil)
    r.header_extensions_returns(Vec::new());
    // r.IsClosedReturns(false)
    r.is_closed_returns(false);
    // r.TrackInfoReturns(&livekit.TrackInfo{})
    r.track_info_returns(Some(Arc::new(TrackInfo::default())));
    // r.CodecStateReturns(ReceiverCodecStateNormal)
    r.codec_state_returns(ReceiverCodecState::Normal);
    // r.GetPrimaryReceiverForRedReturns(r)
    r.get_primary_receiver_for_red_returns(r.clone());
    // r.GetRedReceiverReturns(r)
    r.get_red_receiver_returns(r.clone());
    // r.ReadRTPReturns(0, io.EOF)
    r.read_rtp_returns_with(|| Err(Error::Eof));
    // AddOnReady must invoke its callback immediately so Bind reaches the bound state.
    // r.AddOnReadyCalls(func(f func()) { f() })
    r.add_on_ready_calls(|f| f());
    r
}

fn opus_codec_params() -> RtpCodecParameters {
    RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MIME_TYPE_OPUS.to_owned(),
            clock_rate: 48000,
            channels: 2,
            ..Default::default()
        },
        payload_type: 111,
    }
}

#[test]
fn a_fake_track_receiver_as_upstream_configures_it() {
    let r = new_fake_track_receiver(opus_codec_params());
    let receiver: Arc<dyn TrackReceiver> = r.clone();

    assert_eq!(receiver.track_id(), "fake-track");
    assert_eq!(receiver.stream_id(), "fake-stream");
    assert_eq!(receiver.codec(), opus_codec_params());
    assert_eq!(receiver.mime(), MimeType::Opus);
    assert_eq!(receiver.codec_state(), ReceiverCodecState::Normal);
    assert!(receiver.track_info().is_some());
    assert!(!receiver.is_closed());
    // The receiver is its own primary receiver and its own RED receiver.
    assert_eq!(
        receiver.get_primary_receiver_for_red().track_id(),
        "fake-track"
    );
    assert_eq!(
        receiver.get_red_receiver().get_red_receiver().stream_id(),
        "fake-stream"
    );
    // Every read ends, however often it is tried.
    let mut buf = [0u8; 1500];
    assert!(matches!(receiver.read_rtp(&mut buf, 0, 1), Err(Error::Eof)));
    assert!(matches!(receiver.read_rtp(&mut buf, 0, 2), Err(Error::Eof)));
    // The readiness callback is called at once.
    let ready = Arc::new(AtomicUsize::new(0));
    let counter = ready.clone();
    receiver.add_on_ready(Box::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    }));
    assert_eq!(ready.load(Ordering::SeqCst), 1);
    // What was not configured gives Go's zero values.
    assert_eq!(receiver.get_audio_level(), (0.0, false));
    assert_eq!(
        receiver.get_layered_bitrate(),
        (Vec::new(), Bitrates::default())
    );
    assert!(receiver.get_down_tracks().is_empty());
    assert!(receiver.get_track_stats().is_none());
    assert!(
        receiver
            .add_down_track(Arc::new(FakeTrackSender::default()))
            .is_ok()
    );

    // every NACKed packet is read back from the receiver as this source packet
    let source: Vec<u8> = (0..600).map(|i| (7 + i) as u8).collect();
    // r.ReadRTPCalls(func(buf []byte, _ uint8, _ uint64) (int, error) {
    //     return copy(buf, source), nil
    // })
    let packet = source.clone();
    r.read_rtp_calls(move |buf, _, _| {
        let n = packet.len().min(buf.len());
        buf[..n].copy_from_slice(&packet[..n]);
        Ok(n)
    });
    for esn in 30000..30003 {
        let n = receiver.read_rtp(&mut buf, 0, esn).unwrap();
        assert_eq!(&buf[..n], &source[..]);
    }
    // require.Equal(t, 3, dh.receiver.ReadRTPCallCount(), "a capped retransmission must not read the publisher's buffer")
    assert_eq!(
        5,
        r.read_rtp_call_count(),
        "the two reads that ended and the three that were stubbed"
    );
    // The buffer is not recorded, the layer and the sequence number are.
    assert_eq!(r.read_rtp_args_for_call(4), (NotRecorded, 0, 30002));

    // var brs Bitrates
    // brs[0][0] = 100_000
    let mut brs = Bitrates::default();
    brs[0][0] = 100_000;
    // dh.receiver.GetLayeredBitrateReturns([]int32{0}, brs)
    r.get_layered_bitrate_returns((vec![0], brs));
    assert_eq!(receiver.get_layered_bitrate(), (vec![0], brs));

    // before := dh.receiver.SendPLICallCount()
    let before = r.send_pli_call_count();
    receiver.send_pli(0, false);
    // return dh.receiver.SendPLICallCount() > before
    assert!(r.send_pli_call_count() > before);
    assert_eq!(r.send_pli_args_for_call(0), (0, false));
}

// Ported from LiveKit, pkg/sfu/utils/downtrackspreader_test.go at v1.13.11
// (`TestBroadcastRTP`, `TestBroadcastRTPEmpty`).
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, with a `FakeTrackSender` where the original has a
// sender written by hand for the test: its counter of writes is the fake's call count, its
// `accept` what the fake returns. The allocation count is not checked (as in
// `downtrackspreader_test.rs`, where the test itself is ported).

impl Sender for FakeTrackSender {
    fn subscriber_id(&self) -> ParticipantId {
        TrackSender::subscriber_id(self)
    }
}

impl RtpWriter<ExtPacket> for FakeTrackSender {
    fn write_rtp(&self, pkt: &ExtPacket, layer: i32) -> i32 {
        TrackSender::write_rtp(self, pkt, layer)
    }
}

fn new_test_spreader(
    num_down_tracks: usize,
    threshold: usize,
) -> (
    DownTrackSpreader<Arc<FakeTrackSender>>,
    Vec<Arc<FakeTrackSender>>,
) {
    let d = DownTrackSpreader::<Arc<FakeTrackSender>>::new(DownTrackSpreaderParams {
        threshold,
        ..Default::default()
    });
    let mut senders = Vec::with_capacity(num_down_tracks);
    for i in 0..num_down_tracks {
        // senders[i] = &testSender{id: livekit.ParticipantID(fmt.Sprintf("p%d", i)), accept: 1}
        let sender = Arc::new(FakeTrackSender::default());
        sender.subscriber_id_returns(ParticipantId::from(format!("p{i}")));
        sender.write_rtp_returns(1);
        senders.push(sender);
        d.store(senders[i].clone());
    }
    (d, senders)
}

#[test]
fn test_broadcast_rtp() {
    for (name, num_down_tracks, threshold) in [("serial", 5, 20), ("parallel", 50, 20)] {
        let (d, senders) = new_test_spreader(num_down_tracks, threshold);
        // senders[0].accept = 0 // one down track that drops
        senders[0].write_rtp_returns(0);
        let pkt = ExtPacket {
            ext_sequence_number: 9,
            ..Default::default()
        };

        let written = broadcast_rtp(&d, &pkt, 2);
        assert_eq!((num_down_tracks - 1) as i32, written, "{name}");
        for s in &senders {
            // require.EqualValues(t, 1, s.writes.Load())
            assert_eq!(1, s.write_rtp_call_count(), "{name}");
            // What a sender written by hand does not tell: what it was called with.
            let (written, layer) = s.write_rtp_args_for_call(0);
            assert_eq!((9, 2), (written.ext_sequence_number, layer), "{name}");
        }
        assert!(d.has_down_track(&ParticipantId::from("p3")), "{name}");
    }
}

#[test]
fn test_broadcast_rtp_empty() {
    let d = DownTrackSpreader::<Arc<FakeTrackSender>>::new(DownTrackSpreaderParams {
        threshold: 20,
        ..Default::default()
    });
    assert_eq!(0, broadcast_rtp(&d, &ExtPacket::default(), 0));
}

#[test]
fn the_rest_of_a_fake_track_sender() {
    assert_send_sync::<FakeTrackSender>();
    assert_send_sync::<FakeTrackReceiver>();

    let fake = Arc::new(FakeTrackSender::default());
    let sender: Arc<dyn TrackSender> = fake.clone();
    let receiver = new_fake_track_receiver(opus_codec_params());

    sender.up_track_layers_change();
    sender.up_track_bitrate_availability_change();
    sender.up_track_max_published_layer_change(2);
    sender.up_track_max_temporal_layer_seen_change(3);
    sender.up_track_bitrate_report(&[0, 1], Bitrates::default());
    sender.set_receiver(receiver.clone());
    sender.receiver_restart(receiver.clone());
    sender.resync();
    assert_eq!(sender.id(), "");
    assert!(!sender.is_closed());
    let report = RtcpSenderReportState {
        rtp_timestamp: 90000,
        ..Default::default()
    };
    assert!(
        sender
            .handle_rtcp_sender_report_data(96, 1, &report)
            .is_ok()
    );
    sender.close();

    assert_eq!(fake.up_track_layers_change_call_count(), 1);
    assert_eq!(fake.up_track_bitrate_availability_change_call_count(), 1);
    assert_eq!(fake.up_track_max_published_layer_change_args_for_call(0), 2);
    assert_eq!(
        fake.up_track_max_temporal_layer_seen_change_args_for_call(0),
        3
    );
    assert_eq!(
        fake.up_track_bitrate_report_args_for_call(0),
        (vec![0, 1], Bitrates::default())
    );
    assert_eq!(
        fake.handle_rtcp_sender_report_data_args_for_call(0),
        (96, 1, report)
    );
    // An `Arc<dyn Trait>` is recorded as it is: the same receiver, not a copy of it.
    let set: Arc<dyn TrackReceiver> = fake.set_receiver_args_for_call(0);
    assert_eq!(set.track_id(), "fake-track");
    assert_eq!(receiver.track_id_call_count(), 1);
    assert_eq!(fake.receiver_restart_call_count(), 1);
    assert_eq!(fake.resync_call_count(), 1);
    assert_eq!(fake.close_call_count(), 1);
    assert_eq!(format!("{fake:?}"), "FakeTrackSender");
}

// ------------------------------------------------
// What a call returns

#[musivum_fakes::fake]
trait Returning: Send + Sync {
    fn number(&self) -> i32;
    fn pair(&self) -> (f64, bool);
    fn maybe(&self) -> Option<Arc<TrackInfo>>;
    fn status(&self) -> Result<(), Error>;
    fn read(&self, at: u64) -> Result<Vec<u8>, Error>;
    fn open(&self) -> Result<Token, Error>;
    fn boxed(&self) -> Box<dyn fmt::Display + Send>;
    fn peer(&self) -> Arc<dyn Returning>;
    fn token(&self) -> Token;
    fn local(&self) -> Rc<i32>;
    fn nothing(&self);
}

#[test]
fn an_unconfigured_method_returns_the_zero_value() {
    let fake = FakeReturning::default();
    assert_eq!(fake.number(), 0);
    assert_eq!(fake.pair(), (0.0, false));
    assert!(fake.maybe().is_none());
    // Go's zero value of `error`, and of `([]byte, error)`.
    assert!(fake.status().is_ok());
    assert_eq!(fake.read(0).unwrap(), Vec::<u8>::new());
    fake.nothing();
    assert_eq!(fake.nothing_call_count(), 1);
}

#[test]
#[should_panic(
    expected = "FakeReturning::peer was called, but the test set no return value for it and \
                `alloc::sync::Arc<dyn musivum::testutils::fakes_test::Returning>` has no default: \
                use peer_returns(..), peer_returns_with(..) or peer_calls(..)"
)]
fn an_unconfigured_method_without_a_zero_value_says_so() {
    let fake = FakeReturning::default();
    fake.peer();
}

#[test]
#[should_panic(expected = "FakeReturning::open was called, but the test set no return value")]
fn a_result_without_a_zero_value_inside_says_so() {
    let fake = FakeReturning::default();
    let _ = fake.open();
}

#[test]
fn a_method_without_a_zero_value_can_be_configured() {
    let fake = FakeReturning::default();
    // The call is counted even when it panics afterwards.
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fake.token())).is_err());
    assert_eq!(fake.token_call_count(), 1);

    let other = Arc::new(FakeReturning::default());
    other.number_returns(5);
    // An `Arc` of a fake is taken where the method returns an `Arc<dyn Trait>`.
    fake.peer_returns(other.clone());
    assert_eq!(fake.peer().number(), 5);
    assert_eq!(fake.peer().number(), 5);
    assert_eq!(other.number_call_count(), 2);
}

#[test]
fn returns_returns_on_call_and_calls_in_counterfeiters_order() {
    let fake = FakeReturning::default();
    fake.number_returns(1);
    fake.number_returns_on_call(1, 10);
    fake.number_returns_on_call(5, 50);
    assert_eq!([fake.number(), fake.number(), fake.number()], [1, 10, 1]);

    // A stub goes before both.
    fake.number_calls(|| 100);
    assert_eq!(fake.number(), 100);
    // `x_returns` takes the stub away again, as in counterfeiter; the value for call 5 stays.
    fake.number_returns(2);
    assert_eq!([fake.number(), fake.number(), fake.number()], [2, 50, 2]);
    assert_eq!(fake.number_call_count(), 7);

    // So does `x_returns_on_call`.
    fake.number_stub(|| 100);
    fake.number_returns_on_call(8, 80);
    assert_eq!([fake.number(), fake.number()], [2, 80]);

    // A tuple where Go has two results.
    fake.pair_returns((20.0, true));
    assert_eq!(fake.pair(), (20.0, true));
    fake.maybe_returns(None);
    assert!(fake.maybe().is_none());
    let info = Arc::new(TrackInfo::default());
    fake.maybe_returns(Some(info.clone()));
    assert!(Arc::ptr_eq(&fake.maybe().unwrap(), &info));
}

#[test]
fn a_result_whose_error_is_not_clone() {
    let fake = FakeReturning::default();
    // An `Ok` is returned as often as the method is called: its value is `Clone`.
    fake.read_returns(Ok(vec![1, 2, 3]));
    assert_eq!(fake.read(0).unwrap(), [1, 2, 3]);
    assert_eq!(fake.read(1).unwrap(), [1, 2, 3]);

    // An `Err` that is not `Clone` once.
    fake.status_returns(Err(Error::Io(std::io::Error::other("gone"))));
    assert!(matches!(fake.status(), Err(Error::Io(_))));

    // A closure makes a new one for every call.
    fake.status_returns_with(|| Err(Error::Eof));
    assert!(matches!(fake.status(), Err(Error::Eof)));
    assert!(matches!(fake.status(), Err(Error::Eof)));

    // For single calls a value that is not `Clone` is just right.
    fake.read_returns_on_call(2, Err(Error::Eof));
    assert!(matches!(fake.read(2), Err(Error::Eof)));
    assert_eq!(fake.read(3).unwrap(), [1, 2, 3]);
    assert_eq!(fake.read_args_for_call(3), 3);
}

#[test]
#[should_panic(
    expected = "FakeReturning::status was called again, but the value the test set for it is a \
                `core::result::Result<(), musivum::testutils::fakes_test::Error>`, which is not \
                `Clone` and has been returned already: use status_returns_with(|| ..) to make \
                one for every call"
)]
fn a_value_that_is_not_clone_is_returned_once() {
    let fake = FakeReturning::default();
    fake.status_returns(Err(Error::Eof));
    assert!(fake.status().is_err());
    let _ = fake.status();
}

#[test]
fn values_that_are_not_clone() {
    let fake = FakeReturning::default();
    fake.token_returns(Token(7));
    assert_eq!(fake.token(), Token(7));
    fake.token_returns_with(|| Token(8));
    assert_eq!([fake.token(), fake.token()], [Token(8), Token(8)]);

    fake.boxed_returns(Box::new("once"));
    assert_eq!(fake.boxed().to_string(), "once");
    fake.boxed_returns_with(|| Box::new(12));
    assert_eq!(fake.boxed().to_string(), "12");
    assert_eq!(fake.boxed().to_string(), "12");

    fake.open_returns_on_call(0, Ok(Token(1)));
    fake.open_returns_on_call(1, Err(Error::Eof));
    assert_eq!(fake.open().unwrap(), Token(1));
    assert!(fake.open().is_err());

    // A value that is not `Send` cannot be kept, but can be made in a closure or by a stub.
    fake.local_returns_with(|| Rc::new(4));
    assert_eq!(*fake.local(), 4);
    fake.local_calls(|| Rc::new(5));
    assert_eq!(*fake.local(), 5);
}

#[test]
#[should_panic(
    expected = "FakeReturning::local_returns: a `alloc::rc::Rc<i32>` is not `Send`, so the fake, \
                which is shared between threads, cannot keep it: use local_returns_with(|| ..) \
                or local_calls(..)"
)]
fn a_value_that_is_not_send_cannot_be_kept() {
    let fake = FakeReturning::default();
    fake.local_returns(Rc::new(1));
}

// ------------------------------------------------
// What is recorded of the arguments

#[derive(Default)]
pub struct Counter(i32);

pub struct Guard;

#[musivum_fakes::fake]
trait Recording: Send + Sync {
    fn text(&self, s: &str);
    fn bytes(&self, b: &[u8], layer: u8);
    fn message(&self, info: &TrackInfo);
    fn maybe(&self, info: Option<&TrackInfo>, name: Option<&str>, payload: Option<&[u8]>);
    fn shared(&self, other: Arc<dyn Recording>, maybe_other: Option<Arc<dyn Recording>>);
    fn by_ref_to_shared(&self, other: &Arc<dyn Recording>);
    fn attributes(&self, attributes: &HashMap<String, String>, ids: Vec<TrackId>);
    fn callback(&self, f: Arc<dyn Fn(i32) -> i32 + Send + Sync>);
    fn boxed_callback(&self, id: u32, f: Box<dyn Fn(i32) -> i32 + Send + Sync>) -> i32;
    fn once(&self, f: Box<dyn FnOnce() -> u8 + Send>) -> u8;
    fn optional_callback(&self, f: Option<Box<dyn Fn() + Send + Sync>>);
    fn fill(&self, buf: &mut [u8], layer: u8) -> usize;
    fn bump(&self, counter: &mut Counter, by: i32);
    fn display(&self, what: &dyn fmt::Display, times: usize) -> String;
    fn names(&self, names: &[&str], n: u8);
    fn guarded(&self, #[fake(skip)] guard: &Guard, n: i32) -> i32;
    fn guarded_owned(&self, #[cfg_attr(test, fake(skip))] guard: Guard, n: i32);
    fn unseen(&self, #[fake(skip)] secret: String);
}

#[test]
fn arguments_are_recorded_as_owned_copies() {
    let fake = Arc::new(FakeRecording::default());

    fake.text("hello");
    fake.text("again");
    let first: String = fake.text_args_for_call(0);
    assert_eq!(first, "hello");
    assert_eq!(fake.text_args_for_call(1), "again");

    fake.bytes(&[1, 2, 3], 2);
    let (bytes, layer): (Vec<u8>, u8) = fake.bytes_args_for_call(0);
    assert_eq!((bytes.as_slice(), layer), (&[1u8, 2, 3][..], 2));

    let info = TrackInfo {
        sid: "TR_1".to_owned(),
        ..Default::default()
    };
    fake.message(&info);
    assert_eq!(fake.message_args_for_call(0), info);

    fake.maybe(Some(&info), Some("name"), None);
    fake.maybe(None, None, Some(&[4, 5]));
    assert_eq!(
        fake.maybe_args_for_call(0),
        (Some(info.clone()), Some("name".to_owned()), None)
    );
    assert_eq!(fake.maybe_args_for_call(1), (None, None, Some(vec![4, 5])));

    let other = Arc::new(FakeRecording::default());
    fake.shared(other.clone(), None);
    fake.shared(fake.clone(), Some(other.clone()));
    let (got, none) = fake.shared_args_for_call(0);
    got.text("through the recorded argument");
    assert_eq!(other.text_call_count(), 1);
    assert!(none.is_none());
    let (_, some) = fake.shared_args_for_call(1);
    some.unwrap().text("and again");
    assert_eq!(other.text_call_count(), 2);

    let as_dyn: Arc<dyn Recording> = other.clone();
    fake.by_ref_to_shared(&as_dyn);
    assert!(Arc::ptr_eq(
        &fake.by_ref_to_shared_args_for_call(0),
        &as_dyn
    ));

    let attributes = HashMap::from([("a".to_owned(), "b".to_owned())]);
    fake.attributes(&attributes, vec!["TR_1".into(), "TR_2".into()]);
    let (got_attributes, got_ids) = fake.attributes_args_for_call(0);
    assert_eq!(got_attributes, attributes);
    assert_eq!(got_ids, [TrackId::from("TR_1"), TrackId::from("TR_2")]);

    // A closure in an `Arc` is recorded, and the test can call it.
    fake.callback(Arc::new(|x| x * 2));
    let callback = fake.callback_args_for_call(0);
    assert_eq!(callback(21), 42);
}

#[test]
fn what_cannot_be_copied_is_not_recorded_and_a_stub_still_gets_it() {
    let fake = FakeRecording::default();

    // A closure in a `Box`.
    assert_eq!(fake.boxed_callback(1, Box::new(|x| x + 1)), 0);
    fake.boxed_callback_calls(|id, f| f(id as i32));
    assert_eq!(fake.boxed_callback(2, Box::new(|x| x + 1)), 3);
    let (id, f): (u32, NotRecorded) = fake.boxed_callback_args_for_call(1);
    assert_eq!((id, f), (2, NotRecorded));

    fake.once_calls(|f| f());
    assert_eq!(fake.once(Box::new(|| 9)), 9);
    assert_eq!(fake.once_args_for_call(0), NotRecorded);

    let called = Arc::new(AtomicUsize::new(0));
    fake.optional_callback_calls(|f| {
        if let Some(f) = f {
            f()
        }
    });
    let counter = called.clone();
    fake.optional_callback(Some(Box::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    })));
    fake.optional_callback(None);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(fake.optional_callback_call_count(), 2);

    // A `&mut`: the stub writes through it.
    let mut buf = [0u8; 4];
    assert_eq!(fake.fill(&mut buf, 1), 0);
    fake.fill_calls(|buf, layer| {
        buf.fill(layer);
        buf.len()
    });
    assert_eq!(fake.fill(&mut buf, 7), 4);
    assert_eq!(buf, [7; 4]);
    assert_eq!(fake.fill_args_for_call(1), (NotRecorded, 7));

    let mut counter = Counter::default();
    fake.bump_calls(|counter, by| counter.0 += by);
    fake.bump(&mut counter, 2);
    fake.bump(&mut counter, 3);
    assert_eq!(counter.0, 5);
    assert_eq!(fake.bump_args_for_call(1), (NotRecorded, 3));

    // A `&dyn`.
    fake.display_calls(|what, times| what.to_string().repeat(times));
    assert_eq!(fake.display(&12, 2), "1212");
    assert_eq!(fake.display_args_for_call(0), (NotRecorded, 2));

    // A type that still borrows when it is owned.
    fake.names(&["a", "b"], 2);
    assert_eq!(fake.names_args_for_call(0), (NotRecorded, 2));

    // What the trait says is not to be recorded: by reference, by value, and something that
    // could have been.
    fake.guarded_calls(|_guard, n| n * 2);
    assert_eq!(fake.guarded(&Guard, 4), 8);
    assert_eq!(fake.guarded_args_for_call(0), (NotRecorded, 4));
    fake.guarded_owned(Guard, 5);
    assert_eq!(fake.guarded_owned_args_for_call(0), (NotRecorded, 5));
    let seen = Arc::new(Mutex::new(String::new()));
    let seen_by_stub = seen.clone();
    fake.unseen_calls(move |secret| *seen_by_stub.lock().unwrap() = secret);
    fake.unseen("hunter2".to_owned());
    assert_eq!(fake.unseen_args_for_call(0), NotRecorded);
    assert_eq!(*seen.lock().unwrap(), "hunter2");
}

#[test]
#[should_panic(
    expected = "FakeRecording::text_args_for_call(2): there is no call 2, the method was called \
                1 time(s)"
)]
fn the_arguments_of_a_call_that_was_not_made() {
    let fake = FakeRecording::default();
    fake.text("one");
    fake.text_args_for_call(2);
}

// ------------------------------------------------
// Stubs that call back, and threads

#[test]
fn a_stub_may_call_back_into_the_fake() {
    let fake = Arc::new(FakeReturning::default());

    // Into the method it is the stub of: no lock of the fake is held while a stub runs.
    let again = fake.clone();
    fake.number_calls(move || {
        if again.number_call_count() < 3 {
            again.number() + 1
        } else {
            0
        }
    });
    assert_eq!(fake.number(), 2);
    assert_eq!(fake.number_call_count(), 3);

    // And to configure the fake, the method itself too.
    let again = fake.clone();
    fake.read_calls(move |at| {
        again.pair_returns((at as f64, true));
        again.read_returns(Ok(vec![9]));
        Ok(vec![at as u8])
    });
    assert_eq!(fake.read(4).unwrap(), [4]);
    assert_eq!(fake.pair(), (4.0, true));
    assert_eq!(fake.read(5).unwrap(), [9]);
    assert_eq!(fake.read_args_for_call(1), 5);

    // What a stub owns may ask the fake when it is dropped, which is when the test sets
    // something else: that happens with no lock held either.
    struct AsksWhenDropped(Arc<FakeReturning>, Arc<AtomicUsize>);
    impl Drop for AsksWhenDropped {
        fn drop(&mut self) {
            self.1.store(self.0.number_call_count(), Ordering::SeqCst);
        }
    }
    let asked = Arc::new(AtomicUsize::new(0));
    let owned = AsksWhenDropped(fake.clone(), asked.clone());
    fake.number_calls(move || {
        let _ = &owned;
        7
    });
    assert_eq!(fake.number(), 7);
    fake.number_returns(8);
    assert_eq!(asked.load(Ordering::SeqCst), 4);
    let owned = AsksWhenDropped(fake.clone(), asked.clone());
    fake.number_returns_with(move || {
        let _ = &owned;
        9
    });
    assert_eq!(fake.number(), 9);
    fake.number_calls(|| 10);
    assert_eq!(fake.number(), 10);
    fake.number_returns_on_call(7, 11);
    fake.number_returns(12);
    assert_eq!(asked.load(Ordering::SeqCst), 6);
    assert_eq!(fake.number(), 12);
    assert_eq!(fake.number(), 11);
}

#[test]
fn a_fake_is_shared_between_threads() {
    const THREADS: usize = 8;
    const CALLS: usize = 500;

    let fake = Arc::new(FakeTrackSender::default());
    let stubbed = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for thread in 0..THREADS {
            let sender: Arc<dyn TrackSender> = fake.clone();
            let fake = fake.clone();
            let stubbed = stubbed.clone();
            scope.spawn(move || {
                for call in 0..CALLS {
                    let pkt = ExtPacket {
                        ext_sequence_number: (thread * CALLS + call) as u64,
                        ..Default::default()
                    };
                    let written = sender.write_rtp(&pkt, thread as i32);
                    assert!(written == 0 || written == 1 || written == 2);
                    // The fake is configured while the others call it.
                    match call % 3 {
                        0 => fake.write_rtp_returns(1),
                        1 => {
                            let stubbed = stubbed.clone();
                            fake.write_rtp_calls(move |_, _| {
                                stubbed.fetch_add(1, Ordering::Relaxed);
                                2
                            })
                        }
                        _ => assert!(fake.write_rtp_call_count() > call),
                    }
                    sender.up_track_max_published_layer_change(call as i32);
                }
            });
        }
    });

    assert_eq!(fake.write_rtp_call_count(), THREADS * CALLS);
    assert_eq!(
        fake.up_track_max_published_layer_change_call_count(),
        THREADS * CALLS
    );
    // Every call was recorded once, with its own arguments.
    let mut seen = vec![false; THREADS * CALLS];
    for i in 0..THREADS * CALLS {
        let (pkt, layer) = fake.write_rtp_args_for_call(i);
        let n = pkt.ext_sequence_number as usize;
        assert_eq!(n / CALLS, layer as usize);
        assert!(!seen[n]);
        seen[n] = true;
    }
    assert!(stubbed.load(Ordering::Relaxed) <= THREADS * CALLS);
}

// ------------------------------------------------
// Supertraits: the traits are in modules that do not know each other's types

mod participant {
    use crate::protocol::livekit::{ParticipantId, ParticipantIdentity, TrackId};

    /// A type the modules of the subtraits do not import.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum CloseReason {
        #[default]
        None,
        ClientRequestLeave,
    }

    #[derive(Debug, PartialEq)]
    pub struct CloseError;

    #[musivum_fakes::fake]
    pub trait Participant: Send + Sync {
        fn id(&self) -> ParticipantId;
        fn identity(&self) -> ParticipantIdentity;
        fn state(&self) -> crate::protocol::livekit::participant_info::State;
        fn get_audio_level(&self) -> (f64, bool);
        fn has_permission(&self, track_id: &TrackId, sub_identity: &ParticipantIdentity) -> bool;
        fn close(
            &self,
            send_leave: bool,
            reason: CloseReason,
            is_expected_to_resume: bool,
        ) -> Result<(), CloseError>;
    }
}

mod local_participant {
    // Nothing of what the methods of `Participant` take and return is imported here.
    use std::any::Any;
    use std::fmt::Debug;

    use super::participant::Participant;
    use crate::protocol::livekit::SpeakerInfo;

    #[musivum_fakes::fake]
    pub trait LocalParticipant: Participant + Any + Debug + Send + Sync {
        fn is_ready(&self) -> bool;
        fn subscribe_to_track(&self, track_id: &str);
        fn send_speaker_update(&self, speakers: &[SpeakerInfo], force: bool) -> Result<(), ()>;
        fn send_data_message(&self, kind: i32, data: &[u8], sender: &str, sequence: u32);
    }
}

mod remote_participant {
    // A second subtrait of the same supertrait.
    #[musivum_fakes::fake]
    pub trait RemoteParticipant: super::participant::Participant {
        fn node(&self) -> String;
    }
}

mod agent {
    use super::local_participant::LocalParticipant;

    // Two levels deep.
    #[musivum_fakes::fake]
    pub trait Agent: LocalParticipant + 'static {
        fn job(&self) -> u32;
    }
}

mod listener {
    #[musivum_fakes::fake]
    pub trait Listener: Send + Sync {
        fn on_event(&self, name: &str) -> bool;
    }
}

mod supervised {
    use std::fmt;

    use super::listener::Listener;
    use super::participant::Participant;

    pub trait Describe {
        fn describe(&self) -> String;
    }

    // Two faked supertraits and one that is not a fake: `extends` says which are embedded.
    #[musivum_fakes::fake(extends(Participant, Listener))]
    pub trait Supervised: Participant + Listener + Describe + fmt::Display {
        fn supervisor(&self) -> String;
    }

    impl Describe for FakeSupervised {
        fn describe(&self) -> String {
            format!("supervised by {}", self.supervisor())
        }
    }

    impl fmt::Display for FakeSupervised {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a supervised participant")
        }
    }
}

use agent::{Agent, FakeAgent};
use listener::{FakeListenerState, Listener};
use local_participant::{FakeLocalParticipant, LocalParticipant};
use participant::{CloseError, CloseReason, FakeParticipant, Participant};
use remote_participant::{FakeRemoteParticipant, RemoteParticipant};
use supervised::{FakeSupervised, Supervised};

#[test]
fn the_fake_of_a_subtrait_is_a_fake_of_the_supertrait_too() {
    assert_send_sync::<FakeLocalParticipant>();
    let fake = Arc::new(FakeLocalParticipant::default());

    // The methods of the supertrait are configured on the fake of the subtrait, as
    // counterfeiter's FakeLocalParticipant has the methods of Participant too.
    // p.StateReturns(livekit.ParticipantInfo_ACTIVE)
    fake.state_returns(livekit::participant_info::State::Active);
    // p.GetAudioLevelReturns(20, true)
    fake.get_audio_level_returns((20.0, true));
    fake.id_returns("PA_1".into());
    fake.identity_returns("alice".into());
    fake.is_ready_returns(true);

    let local: Arc<dyn LocalParticipant> = fake.clone();
    assert!(local.is_ready());
    assert_eq!(local.id(), "PA_1");
    assert_eq!(local.get_audio_level(), (20.0, true));

    // Upcasting works as for any trait, and reaches the same fake.
    let participant: Arc<dyn Participant> = local.clone();
    assert_eq!(participant.identity(), "alice");
    assert_eq!(
        participant.state(),
        livekit::participant_info::State::Active
    );
    assert!(!participant.has_permission(&"TR_1".into(), &"bob".into()));
    assert_eq!(fake.identity_call_count(), 1);
    assert_eq!(fake.id_call_count(), 1);
    assert_eq!(
        fake.has_permission_args_for_call(0),
        (TrackId::from("TR_1"), ParticipantIdentity::from("bob"))
    );

    // p.CloseStub = func(bool, types.ParticipantCloseReason, bool) error {
    //     close(closing)
    //     <-release
    //     return nil
    // }
    let (closing, closed) = std::sync::mpsc::channel();
    let closing = Mutex::new(closing);
    fake.close_stub(move |_, _, _| {
        closing.lock().unwrap().send(()).unwrap();
        Ok(())
    });
    assert_eq!(
        participant.close(true, CloseReason::ClientRequestLeave, false),
        Ok(())
    );
    closed.try_recv().unwrap();
    assert_eq!(
        fake.close_args_for_call(0),
        (true, CloseReason::ClientRequestLeave, false)
    );
    fake.close_returns(Err(CloseError));
    assert_eq!(local.close(false, CloseReason::None, true), Err(CloseError));

    local.subscribe_to_track("TR_1");
    local.subscribe_to_track("TR_2");
    // require.Equal(t, numTracks, p.SubscribeToTrackCallCount())
    assert_eq!(2, fake.subscribe_to_track_call_count());

    let speakers = [livekit::SpeakerInfo {
        sid: "PA_1".to_owned(),
        level: 0.5,
        active: true,
    }];
    local.send_speaker_update(&speakers, false).unwrap();
    // numCalls := p.SendSpeakerUpdateCallCount()
    let num_calls = fake.send_speaker_update_call_count();
    let mut updates = Vec::new();
    // for i := range numCalls {
    for i in 0..num_calls {
        // infos, _ := p.SendSpeakerUpdateArgsForCall(i)
        let (infos, _) = fake.send_speaker_update_args_for_call(i);
        // updates = append(updates, infos)
        updates.push(infos);
    }
    assert_eq!(updates, [speakers.to_vec()]);

    let encoded = b"packet".to_vec();
    local.send_data_message(1, &encoded, "alice", 7);
    // require.Equal(t, 1, fp.SendDataMessageCallCount())
    assert_eq!(1, fake.send_data_message_call_count());
    // _, got, _, _ := fp.SendDataMessageArgsForCall(0)
    let (_, got, _, _) = fake.send_data_message_args_for_call(0);
    // require.Equal(t, encoded, got)
    assert_eq!(encoded, got);

    // The supertraits the fake did not have to do anything for.
    assert_eq!(format!("{local:?}"), "FakeLocalParticipant");
}

#[test]
fn a_fake_behind_a_trait_object_is_found_again() {
    let participants: Vec<Arc<dyn LocalParticipant>> = vec![
        Arc::new(FakeLocalParticipant::default()),
        Arc::new(FakeLocalParticipant::default()),
    ];
    // p := rm.GetParticipants()[0].(*typesfakes.FakeLocalParticipant)
    let p = fakes::downcast::<FakeLocalParticipant>(participants[0].clone());
    p.is_ready_returns(true);
    assert!(participants[0].is_ready());
    assert!(!participants[1].is_ready());

    let any: Arc<dyn Any + Send + Sync> = participants[1].clone();
    assert!(any.downcast::<FakeAgent>().is_err());
}

#[test]
fn the_fake_of_the_supertrait_alone_and_of_a_second_subtrait() {
    let participant = FakeParticipant::default();
    participant.id_returns("PA_2".into());
    assert_eq!(participant.id(), "PA_2");

    let remote = Arc::new(FakeRemoteParticipant::default());
    remote.id_returns("PA_3".into());
    remote.node_returns("node-1".to_owned());
    let as_remote: Arc<dyn RemoteParticipant> = remote.clone();
    let as_participant: Arc<dyn Participant> = as_remote.clone();
    assert_eq!(as_remote.node(), "node-1");
    assert_eq!(as_participant.id(), "PA_3");
    assert_eq!(remote.id_call_count(), 1);
}

#[test]
fn two_levels_of_supertraits() {
    let fake = Arc::new(FakeAgent::default());
    fake.job_returns(3);
    fake.is_ready_returns(true);
    fake.id_returns("PA_4".into());
    fake.has_permission_calls(|track_id, _| track_id == "TR_1");

    let agent: Arc<dyn Agent> = fake.clone();
    let local: Arc<dyn LocalParticipant> = agent.clone();
    let participant: Arc<dyn Participant> = local.clone();
    assert_eq!(agent.job(), 3);
    assert!(agent.is_ready());
    assert!(local.is_ready());
    assert_eq!(agent.id(), "PA_4");
    assert_eq!(participant.id(), "PA_4");
    assert!(participant.has_permission(&"TR_1".into(), &"bob".into()));
    assert!(!agent.has_permission(&"TR_2".into(), &"bob".into()));

    assert_eq!(fake.is_ready_call_count(), 2);
    assert_eq!(fake.id_call_count(), 2);
    assert_eq!(fake.has_permission_call_count(), 2);
    assert_eq!(format!("{fake:?}"), "FakeAgent");

    let found = fakes::downcast::<FakeAgent>(local);
    assert_eq!(found.job_call_count(), 1);
}

#[test]
fn several_faked_supertraits_and_one_that_is_implemented_by_hand() {
    let fake = Arc::new(FakeSupervised::default());
    fake.supervisor_returns("carol".to_owned());
    // The first supertrait is reached directly, as above.
    fake.id_returns("PA_5".into());
    // The others through `embedded`.
    fake.embedded::<FakeListenerState>().on_event_returns(true);

    let supervised: Arc<dyn Supervised> = fake.clone();
    assert_eq!(supervised.supervisor(), "carol");
    assert_eq!(supervised.id(), "PA_5");
    assert!(supervised.on_event("joined"));
    assert_eq!(supervised.describe(), "supervised by carol");
    assert_eq!(supervised.to_string(), "a supervised participant");

    let listener: Arc<dyn Listener> = supervised.clone();
    assert!(listener.on_event("left"));
    let state = fake.embedded::<FakeListenerState>();
    assert_eq!(state.on_event_call_count(), 2);
    assert_eq!(state.on_event_args_for_call(1), "left");
}

// ------------------------------------------------
// The form the library uses: behind `cfg_attr`, on traits with the types of the port
//
// The two traits are those of `sfu/bwe/bwe.rs`, copied: that file is ported from LiveKit,
// pkg/sfu/bwe/bwe.go at v1.13.11. Copyright 2023 LiveKit, Inc. Licensed under the Apache
// License, Version 2.0.

mod estimator {
    use std::sync::Arc;

    use crate::go::time::Duration;
    use crate::sfu::bwe::{BweType, CongestionState};
    use crate::sfu::ccutils;
    use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;

    #[cfg_attr(test, musivum_fakes::fake)]
    pub trait Bwe: Send + Sync {
        fn r#type(&self) -> BweType;

        fn set_bwe_listener(&self, bwe_listner: Option<Arc<dyn BweListener>>);

        fn reset(&self);

        fn handle_remb(
            &self,
            received_estimate: i64,
            expected_bandwidth_usage: i64,
            sent_packets: u32,
            repeated_nacks: u32,
        );

        // TWCC sequence number
        fn record_packet_send_and_get_sequence_number(
            &self,
            at_micro: i64,
            size: usize,
            is_rtx: bool,
            probe_cluster_id: ccutils::ProbeClusterId,
            is_probe: bool,
        ) -> u16;

        fn handle_twcc_feedback(&self, report: &TransportLayerCc);

        fn update_rtt(&self, rtt: f64);

        fn congestion_state(&self) -> CongestionState;

        fn can_probe(&self) -> bool;
        fn probe_duration(&self) -> Duration;
        fn probe_cluster_starting(&self, pci: ccutils::ProbeClusterInfo);
        fn probe_cluster_done(&self, pci: ccutils::ProbeClusterInfo);
        fn probe_cluster_is_goal_reached(&self) -> bool;
        fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool);
    }

    #[cfg_attr(test, musivum_fakes::fake)]
    pub trait BweListener: Send + Sync {
        fn on_congestion_state_change(
            &self,
            from_state: CongestionState,
            to_state: CongestionState,
            estimated_available_channel_capacity: i64,
        );
    }
}

#[test]
fn the_form_the_library_uses() {
    use crate::go::time::Duration;
    use crate::sfu::bwe::{BweType, CongestionState};
    use crate::sfu::ccutils::{ProbeClusterId, ProbeClusterInfo, ProbeSignal};
    use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;
    use estimator::{Bwe, BweListener, FakeBwe, FakeBweListener};

    let fake = Arc::new(FakeBwe::default());
    let listener = Arc::new(FakeBweListener::default());
    let bwe: Arc<dyn Bwe> = fake.clone();

    assert_eq!(bwe.r#type(), BweType::None);
    fake.type_returns(BweType::SendSide);
    assert_eq!(bwe.r#type(), BweType::SendSide);
    assert_eq!(bwe.congestion_state(), CongestionState::None);
    assert_eq!(bwe.probe_duration(), Duration::default());
    assert_eq!(
        bwe.probe_cluster_finalize(),
        (ProbeSignal::Inconclusive, 0, false)
    );
    fake.probe_cluster_finalize_returns((ProbeSignal::Congesting, 250_000, true));
    assert_eq!(
        bwe.probe_cluster_finalize(),
        (ProbeSignal::Congesting, 250_000, true)
    );
    assert!(!bwe.can_probe());
    assert!(!bwe.probe_cluster_is_goal_reached());

    // A listener that may be nil: recorded as the `Option` it is.
    bwe.set_bwe_listener(Some(listener.clone()));
    bwe.set_bwe_listener(None);
    let set: Arc<dyn BweListener> = fake.set_bwe_listener_args_for_call(0).unwrap();
    set.on_congestion_state_change(CongestionState::None, CongestionState::Congested, 500_000);
    assert_eq!(
        listener.on_congestion_state_change_args_for_call(0),
        (CongestionState::None, CongestionState::Congested, 500_000)
    );
    assert!(fake.set_bwe_listener_args_for_call(1).is_none());

    fake.record_packet_send_and_get_sequence_number_calls(|_, size, _, pci, _| {
        size as u16 + pci.0 as u16
    });
    assert_eq!(
        bwe.record_packet_send_and_get_sequence_number(1, 1200, false, ProbeClusterId(3), true),
        1203
    );
    let (at_micro, size, is_rtx, _, is_probe) =
        fake.record_packet_send_and_get_sequence_number_args_for_call(0);
    assert_eq!((at_micro, size, is_rtx, is_probe), (1, 1200, false, true));

    let report = TransportLayerCc {
        fb_pkt_count: 4,
        ..Default::default()
    };
    bwe.handle_twcc_feedback(&report);
    assert_eq!(fake.handle_twcc_feedback_args_for_call(0), report);
    bwe.handle_remb(1, 2, 3, 4);
    assert_eq!(fake.handle_remb_args_for_call(0), (1, 2, 3, 4));
    bwe.update_rtt(0.07);
    assert_eq!(fake.update_rtt_args_for_call(0), 0.07);
    let pci = ProbeClusterInfo::default();
    bwe.probe_cluster_starting(pci);
    bwe.probe_cluster_done(pci);
    assert_eq!(fake.probe_cluster_starting_args_for_call(0), pci);
    assert_eq!(fake.probe_cluster_done_call_count(), 1);
    bwe.reset();
    assert_eq!(fake.reset_call_count(), 1);
}

// ------------------------------------------------
// Other shapes of methods

#[musivum_fakes::fake]
trait Shapes: Send + Sync {
    const LIMIT: usize = 4;

    fn r#type(&self) -> u8;
    fn name(&self) -> &str;
    fn longest<'a>(&self, a: &'a str, b: &'a str) -> &'a str;
    fn lookup(&self, key: &str) -> Option<&TrackInfo>;
    fn set(&mut self, value: i32);
    fn into_number(self: Arc<Self>) -> u32;
    fn finish(self) -> bool
    where
        Self: Sized;

    // A method with a default body is faked like any other.
    fn with_default(&self, x: i32) -> i32 {
        x + 1
    }

    // Unless it says that it keeps its body.
    #[fake(keep)]
    fn kept(&self, x: i32) -> i32 {
        self.with_default(x) * 2
    }

    #[cfg_attr(test, fake(keep))]
    fn generic<T: Into<i32>>(&self, x: T) -> i32
    where
        Self: Sized,
    {
        self.with_default(x.into()) + Self::LIMIT as i32
    }

    #[cfg(test)]
    fn only_in_tests(&self, n: u8) -> bool;

    #[cfg(not(test))]
    fn never_built(&self) -> NoSuchType;
}

// A trait that is not there has no fake either.
#[musivum_fakes::fake]
#[cfg(not(test))]
trait NeverBuilt {
    fn never_built(&self) -> NoSuchType;
}

#[test]
fn other_shapes_of_methods() {
    let mut fake = FakeShapes::default();

    fake.type_returns(3);
    assert_eq!(fake.r#type(), 3);
    assert_eq!(fake.type_call_count(), 1);

    // What borrows from the fake when the trait is implemented for real is `'static` here.
    assert_eq!(fake.name(), "");
    fake.name_returns("a name");
    assert_eq!(fake.name(), "a name");
    fake.longest_calls(|a, b| if a.len() >= b.len() { "a" } else { "b" });
    assert_eq!(fake.longest("xx", "y"), "a");
    assert_eq!(
        fake.longest_args_for_call(0),
        ("xx".to_owned(), "y".to_owned())
    );
    assert!(fake.lookup("k").is_none());
    let info: &'static TrackInfo = Box::leak(Box::new(TrackInfo::default()));
    fake.lookup_returns(Some(info));
    assert!(std::ptr::eq(fake.lookup("k").unwrap(), info));

    fake.set(5);
    assert_eq!(fake.set_args_for_call(0), 5);

    assert_eq!(fake.with_default(1), 0);
    fake.with_default_returns(10);
    assert_eq!(fake.with_default(1), 10);
    assert_eq!(fake.kept(1), 20);
    assert_eq!(fake.generic(1u8), 14);
    assert_eq!(fake.with_default_call_count(), 4);

    fake.only_in_tests_returns(true);
    assert!(fake.only_in_tests(1));
    assert_eq!(fake.only_in_tests_args_for_call(0), 1);

    let fake = Arc::new(fake);
    fake.into_number_returns(8);
    assert_eq!(fake.clone().into_number(), 8);
    assert_eq!(fake.into_number_call_count(), 1);

    let fake = FakeShapes::default();
    fake.finish_returns(true);
    assert!(fake.finish());
}

#[musivum_fakes::fake]
trait Fetcher: Send + Sync {
    async fn fetch(&self, key: &str) -> Result<Vec<u8>, Error>;
}

#[tokio::test]
async fn an_async_method() {
    let fake = FakeFetcher::default();
    assert!(fake.fetch("a").await.unwrap().is_empty());
    fake.fetch_calls(|key| Ok(key.as_bytes().to_vec()));
    assert_eq!(fake.fetch("b").await.unwrap(), b"b");
    assert_eq!(fake.fetch_args_for_call(1), "b");
}
