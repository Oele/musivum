// Ported from LiveKit, pkg/rtc/mediatrackreceiver.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `NewMediaTrackReceiver` is `MediaTrackReceiver::new` and gives an `Arc`; the receiver
//   keeps a `Weak` of itself for the callbacks it leaves with its subscriptions and with
//   the down tracks. `MediaTrack` embeds it upstream: there it will be a field, and the
//   methods here that `types::MediaTrack` has (all but `to_proto`, `logger` and
//   `on_track_subscribed`, which the embedding type has, as it has its own `close` and
//   `set_muted` around the ones here) carry the trait's signatures, to be passed on to.
// - The `*MediaTrackSubscriptions` that the original embeds is the field
//   `media_track_subscriptions` (`media_track_subscriptions()` gives it), and the methods
//   after `get_track_stats` that only hand a call on to it are what the embedding was.
//   `simulcastReceiver` embeds an `sfu.TrackReceiver` the same way.
// - Who holds whom. The media track owns this receiver, which holds it weakly (it only
//   hands it on to its subscriptions), in an `Option` as the zero value of the params has
//   none. The receiver holds its subscriptions and the receivers of the track; what it
//   leaves with them (the callback of a new down track, the listener of receiver reports)
//   holds it weakly.
// - The params' two functions are `Option`s, nil in the zero value that upstream's tests
//   make a track from; calling one that is not there panics, as upstream.
// - `trackInfo` is an `atomic::Pointer`, never empty after `new`; `track_info()` gives the
//   shared message and `track_info_clone()` a copy to change. Where the original makes a
//   track's `Layers` the same slice as its first codec's (`trackInfo.Layers = ci.Layers`),
//   the layers are copied: nothing changes either afterwards, the info is stored whole.
//   Where it changes the layers of a codec while reading the track info they are in, the
//   layers are changed one by one in the same order, each from the info as it is then.
// - The receivers are a `Vec` that is replaced whole under the lock, as upstream;
//   `load_receivers` gives a copy of it. `slices.SortFunc`, which does not keep the order of
//   equal elements, is a sort that does.
// - `onCodecRegression` and `onMediaLossFeedback`, which the original sets without a lock,
//   are behind a lock each, as their setters take `&self`.
// - `r.TrackReceiver.(*DummyReceiver)` and `.(*sfu.WebRTCReceiver)` are the functions
//   `as_dummy_receiver` and `as_web_rtc_receiver` at the end of the file; `receiver ==
//   r` on two interfaces is `Arc::ptr_eq`.
// - `UpdateAudioTrack`: `DedupeSlice` sorts the features of the caller's update in place
//   and zeroes what it compacts away, and the original then looks for stereo and DTX in
//   what is left of them: a zero is `TF_STEREO`, so an update that names a feature twice
//   turns stereo on. That is kept (the update itself, which is borrowed, is not changed).
// - `GetQualityForDimension`: a size beyond the fourth would give a number that is no
//   `VideoQuality`; it is `HIGH` here.

use std::any::Any;
use std::fmt;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};
use serde_json::Value;

use super::{
    DirectionConfig, DummyReceiver, Error, MediaTrackSubscriptions, MediaTrackSubscriptionsParams,
    ReceiverConfig, WrappedReceiver, WrappedReceiverParams,
    is_participant_exempt_from_track_permissions_restrictions, is_red_enabled, logger_with_track,
    pack_stream_id,
};
use crate::go::atomic;
use crate::go::time;
use crate::mediatransportutil::codec;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::rtc::types;
use crate::sfu;
use crate::sfu::TrackReceiver as _;
use crate::sfu::buffer;
use crate::sfu::rtpstats;
use crate::utils as sutils;
use crate::webrtc;
use crate::webrtc::rtcp::receiver_report::ReceiverReport;
use crate::{debugw, errorw, infow, warnw};

const LAYER_SELECTION_TOLERANCE: f32 = 0.9;

// ErrNotOpen and ErrNoReceiver are `Error::NotOpen` and `Error::NoReceiver`.

// ------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum MediaTrackReceiverState {
    #[default]
    Open,
    Closing,
    Closed,
}

impl fmt::Display for MediaTrackReceiverState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            MediaTrackReceiverState::Open => "OPEN",
            MediaTrackReceiverState::Closing => "CLOSING",
            MediaTrackReceiverState::Closed => "CLOSED",
        })
    }
}

// -----------------------------------------------------

pub struct SimulcastReceiver {
    // the `sfu.TrackReceiver` that the original embeds
    track_receiver: Arc<dyn sfu::TrackReceiver>,
    priority: usize,
    // what `lock sync.Mutex` guards upstream: regressTo
    lock: Mutex<Option<Arc<dyn sfu::TrackReceiver>>>,
}

impl SimulcastReceiver {
    pub(super) fn new(
        track_receiver: Arc<dyn sfu::TrackReceiver>,
        priority: usize,
    ) -> Arc<SimulcastReceiver> {
        Arc::new(SimulcastReceiver {
            track_receiver,
            priority,
            lock: Mutex::new(None),
        })
    }

    pub fn priority(&self) -> usize {
        self.priority
    }

    pub fn regress_to(&self, receiver: Arc<dyn sfu::TrackReceiver>) {
        let mut regress_to = self.lock.lock();
        *regress_to = Some(receiver.clone());
        let dts = self.get_down_tracks();
        drop(regress_to);

        for dt in dts {
            dt.set_receiver(receiver.clone());
        }
    }

    pub fn is_regressed(&self) -> bool {
        let regress_to = self.lock.lock();
        regress_to.is_some()
    }
}

impl sfu::TrackReceiver for SimulcastReceiver {
    fn track_id(&self) -> livekit::TrackId {
        self.track_receiver.track_id()
    }

    fn stream_id(&self) -> String {
        self.track_receiver.stream_id()
    }

    fn codec(&self) -> webrtc::RtpCodecParameters {
        self.track_receiver.codec()
    }

    fn mime(&self) -> mime::MimeType {
        self.track_receiver.mime()
    }

    fn video_layer_mode(&self) -> livekit::video_layer::Mode {
        self.track_receiver.video_layer_mode()
    }

    fn header_extensions(&self) -> Vec<webrtc::RtpHeaderExtensionParameter> {
        self.track_receiver.header_extensions()
    }

    fn is_closed(&self) -> bool {
        self.track_receiver.is_closed()
    }

    fn read_rtp(&self, buf: &mut [u8], layer: u8, esn: u64) -> Result<usize, sfu::Error> {
        self.track_receiver.read_rtp(buf, layer, esn)
    }

    fn get_layered_bitrate(&self) -> (Vec<i32>, sfu::Bitrates) {
        self.track_receiver.get_layered_bitrate()
    }

    fn get_audio_level(&self) -> (f64, bool) {
        self.track_receiver.get_audio_level()
    }

    fn send_pli(&self, layer: i32, force: bool) {
        self.track_receiver.send_pli(layer, force);
    }

    fn set_max_expected_spatial_layer(&self, layer: i32) {
        self.track_receiver.set_max_expected_spatial_layer(layer);
    }

    fn add_down_track(&self, track: Arc<dyn sfu::TrackSender>) -> Result<(), sfu::Error> {
        let regress_to = self.lock.lock();
        if let Some(rt) = regress_to.clone() {
            drop(regress_to);
            // AddDownTrack could be called in downtrack.OnBinding callback, use a goroutine to avoid deadlock
            time::spawn(async move {
                track.set_receiver(rt);
            });
            return Ok(());
        }
        let err = self.track_receiver.add_down_track(track);
        drop(regress_to);
        err
    }

    fn delete_down_track(&self, participant_id: &livekit::ParticipantId) {
        self.track_receiver.delete_down_track(participant_id);
    }

    fn get_down_tracks(&self) -> Vec<Arc<dyn sfu::TrackSender>> {
        self.track_receiver.get_down_tracks()
    }

    fn debug_info(&self) -> sfu::DebugInfo {
        self.track_receiver.debug_info()
    }

    fn track_info(&self) -> Arc<livekit::TrackInfo> {
        self.track_receiver.track_info()
    }

    fn update_track_info(&self, ti: &livekit::TrackInfo) {
        self.track_receiver.update_track_info(ti);
    }

    fn get_primary_receiver_for_red(&self) -> Arc<dyn sfu::TrackReceiver> {
        self.track_receiver.get_primary_receiver_for_red()
    }

    fn get_red_receiver(&self) -> Arc<dyn sfu::TrackReceiver> {
        self.track_receiver.get_red_receiver()
    }

    fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32> {
        self.track_receiver
            .get_temporal_layer_fps_for_spatial(layer)
    }

    fn get_track_stats(&self) -> Option<livekit::RtpStats> {
        self.track_receiver.get_track_stats()
    }

    fn add_on_ready(&self, f: Box<dyn Fn() + Send + Sync>) {
        self.track_receiver.add_on_ready(f);
    }

    fn add_on_codec_state_change(
        &self,
        f: Box<dyn Fn(webrtc::RtpCodecParameters, sfu::ReceiverCodecState) + Send + Sync>,
    ) {
        self.track_receiver.add_on_codec_state_change(f);
    }

    fn codec_state(&self) -> sfu::ReceiverCodecState {
        self.track_receiver.codec_state()
    }

    fn video_sizes(&self) -> Vec<codec::VideoSize> {
        self.track_receiver.video_sizes()
    }

    fn restart(&self, reason: &str) {
        self.track_receiver.restart(reason);
    }
}

// -----------------------------------------------------

#[derive(Default)]
pub struct MediaTrackReceiverParams {
    pub media_track: Option<Weak<dyn types::MediaTrack>>,
    pub is_relayed: bool,
    pub participant_id: Option<Box<dyn Fn() -> livekit::ParticipantId + Send + Sync>>,
    pub participant_identity: livekit::ParticipantIdentity,
    pub participant_version: u32,
    pub receiver_config: ReceiverConfig,
    pub subscriber_config: DirectionConfig,
    pub audio_config: sfu::AudioConfig,
    #[allow(clippy::type_complexity)]
    pub telemetry_listener:
        Option<Box<dyn Fn() -> Arc<dyn types::ParticipantTelemetryListener> + Send + Sync>>,
    pub logger: Logger,
    pub regression_target_codec: mime::MimeType,
    pub prefer_video_size_from_media: bool,
}

type OnMediaLossFeedback = Arc<dyn Fn(&sfu::DownTrack, &ReceiverReport) + Send + Sync>;
type OnCodecRegression =
    Arc<dyn Fn(webrtc::RtpCodecParameters, webrtc::RtpCodecParameters) + Send + Sync>;

pub struct MediaTrackReceiver {
    params: MediaTrackReceiverParams,

    // what `lock sync.RWMutex` guards upstream
    lock: RwLock<MediaTrackReceiverInner>,
    track_info: atomic::Pointer<livekit::TrackInfo>,

    on_media_loss_feedback: RwLock<Option<OnMediaLossFeedback>>,
    on_codec_regression: RwLock<Option<OnCodecRegression>>,

    // the `*MediaTrackSubscriptions` that the original embeds
    media_track_subscriptions: Arc<MediaTrackSubscriptions>,

    // the receiver itself, for the callbacks it leaves with others
    this: Weak<MediaTrackReceiver>,
}

#[derive(Default)]
struct MediaTrackReceiverInner {
    receivers: Vec<Arc<SimulcastReceiver>>,
    potential_codecs: Vec<webrtc::RtpCodecParameters>,
    state: MediaTrackReceiverState,
    is_expected_to_resume: bool,

    on_setup_receiver: Option<Arc<dyn Fn(mime::MimeType) + Send + Sync>>,
    on_close: Vec<Arc<dyn Fn(bool) + Send + Sync>>,
}

impl MediaTrackReceiver {
    pub fn new(
        params: MediaTrackReceiverParams,
        ti: &livekit::TrackInfo,
    ) -> Arc<MediaTrackReceiver> {
        Arc::new_cyclic(|this: &Weak<MediaTrackReceiver>| {
            let media_track_subscriptions =
                MediaTrackSubscriptions::new(MediaTrackSubscriptionsParams {
                    media_track: params.media_track.clone(),
                    is_relayed: params.is_relayed,
                    receiver_config: params.receiver_config,
                    subscriber_config: params.subscriber_config.clone(),
                    logger: params.logger.clone(),
                });
            let t = this.clone();
            media_track_subscriptions.on_down_track_created(move |down_track| {
                if let Some(t) = t.upgrade() {
                    t.on_down_track_created(down_track);
                }
            });

            MediaTrackReceiver {
                params,
                lock: RwLock::new(MediaTrackReceiverInner {
                    state: MediaTrackReceiverState::Open,
                    ..Default::default()
                }),
                track_info: atomic::Pointer::new(Some(Arc::new(ti.clone()))),
                on_media_loss_feedback: RwLock::new(None),
                on_codec_regression: RwLock::new(None),
                media_track_subscriptions,
                this: this.clone(),
            }
        })
    }

    pub fn restart(&self) {
        for receiver in self.load_receivers() {
            let hq = buffer::get_spatial_layer_for_video_quality(
                receiver.mime(),
                livekit::VideoQuality::High,
                Some(&*self.track_info()),
            );
            receiver.set_max_expected_spatial_layer(hq);
        }
    }

    pub fn on_setup_receiver(&self, f: impl Fn(mime::MimeType) + Send + Sync + 'static) {
        let mut inner = self.lock.write();
        inner.on_setup_receiver = Some(Arc::new(f));
    }

    pub fn setup_receiver(
        &self,
        receiver: Arc<dyn sfu::TrackReceiver>,
        priority: usize,
        mid: &str,
    ) {
        let mut inner = self.lock.write();
        if inner.state != MediaTrackReceiverState::Open {
            warnw!(
                self.params.logger,
                "cannot set up receiver on a track not open",
                nil
            );
            return;
        }

        let mut receivers = inner.receivers.clone();

        // codec position maybe taken by DummyReceiver, check and upgrade to WebRTCReceiver
        let mut existing_receiver = false;
        for r in &receivers {
            if r.mime() == receiver.mime() {
                existing_receiver = true;
                if let Some(d) = as_dummy_receiver(&r.track_receiver) {
                    d.upgrade(receiver.clone());
                } else {
                    errorw!(
                        self.params.logger,
                        "receiver already exists, setup failed",
                        nil,
                        "mime" => receiver.mime(),
                    );
                }
                break;
            }
        }
        if !existing_receiver {
            receivers.push(SimulcastReceiver::new(receiver.clone(), priority));
        }

        receivers.sort_by(|a, b| sutils::signum(a.priority() as i64 - b.priority() as i64).cmp(&0));

        if !mid.is_empty() {
            let mut track_info = self.track_info_clone();
            if priority == 0 {
                track_info.mime_type = receiver.mime().to_string();
                track_info.mid = mid.to_string();
            }

            for (i, ci) in track_info.codecs.iter_mut().enumerate() {
                if i == priority {
                    ci.mime_type = receiver.mime().to_string();
                    ci.mid = mid.to_string();
                    break;
                }
            }
            self.track_info.store(Some(Arc::new(track_info)));
        }

        inner.receivers = receivers.clone();
        let on_setup_receiver = inner.on_setup_receiver.clone();
        drop(inner);

        let mut receiver_codecs = Vec::new();
        for r in &receivers {
            receiver_codecs.push(r.mime().to_string());
        }
        debugw!(
            self.params.logger,
            "setup receiver",
            "mime" => receiver.mime(),
            "priority" => priority,
            "receivers" => receiver_codecs,
            "mid" => mid,
        );

        if let Some(on_setup_receiver) = on_setup_receiver {
            on_setup_receiver(receiver.mime());
        }
    }

    pub fn handle_receiver_codec_change(
        &self,
        r: &Arc<dyn sfu::TrackReceiver>,
        codec: &webrtc::RtpCodecParameters,
        state: sfu::ReceiverCodecState,
    ) {
        // TODO: we only support codec regress to backup codec now, so the receiver will not be available
        // once fallback / regression happens.
        // We will support codec upgrade in the future then the primary receiver will be available again if
        // all subscribers of the track negotiate it.
        if state == sfu::ReceiverCodecState::Normal {
            return;
        }

        let mut inner = self.lock.write();
        // codec regression, find backup codec and switch all downtracks to it
        let mut old_receiver: Option<Arc<SimulcastReceiver>> = None;
        let mut backup_codec_receiver: Option<Arc<dyn sfu::TrackReceiver>> = None;
        for receiver in &inner.receivers {
            if Arc::ptr_eq(&receiver.track_receiver, r) {
                old_receiver = Some(receiver.clone());
                continue;
            }
            if let Some(d) = as_dummy_receiver(&receiver.track_receiver)
                && d.receiver().is_some_and(|dr| Arc::ptr_eq(&dr, r))
            {
                old_receiver = Some(receiver.clone());
                continue;
            }

            if receiver.mime() == self.params.regression_target_codec {
                backup_codec_receiver = Some(receiver.track_receiver.clone());
            }

            if old_receiver.is_some() && backup_codec_receiver.is_some() {
                break;
            }
        }

        let Some(old_receiver) = old_receiver else {
            // should not happen
            errorw!(
                self.params.logger,
                "could not find primary receiver for codec",
                nil,
                "codec" => codec.mime_type,
            );
            return;
        };

        if old_receiver.is_regressed() {
            infow!(self.params.logger, "codec already regressed", "codec" => codec.mime_type);
            return;
        }

        let Some(backup_codec_receiver) = backup_codec_receiver else {
            infow!(
                self.params.logger,
                "no backup codec found, can't regress codec"
            );
            return;
        };

        infow!(
            self.params.logger,
            "regressing codec",
            "from" => codec.mime_type,
            "to" => backup_codec_receiver.codec().mime_type,
        );

        // remove old codec from potential codecs
        for (i, c) in inner.potential_codecs.iter().enumerate() {
            if equal_fold(&c.mime_type, &codec.mime_type) {
                inner.potential_codecs.remove(i);
                break;
            }
        }
        let on_codec_regression = self.on_codec_regression.read().clone();
        drop(inner);
        old_receiver.regress_to(backup_codec_receiver.clone());

        if let Some(on_codec_regression) = on_codec_regression {
            on_codec_regression(codec.clone(), backup_codec_receiver.codec());
        }
    }

    pub fn on_codec_regression(
        &self,
        f: impl Fn(webrtc::RtpCodecParameters, webrtc::RtpCodecParameters) + Send + Sync + 'static,
    ) {
        *self.on_codec_regression.write() = Some(Arc::new(f));
    }

    pub fn set_potential_codecs(
        &self,
        codecs: &[webrtc::RtpCodecParameters],
        headers: &[webrtc::RtpHeaderExtensionParameter],
    ) {
        // The potential codecs have not published yet, so we can't get the actual Extensions, the client/browser uses same extensions
        // for all video codecs so we assume they will have same extensions as the primary codec.
        let mut inner = self.lock.write();
        let mut receivers = inner.receivers.clone();
        inner.potential_codecs = codecs.to_vec();
        for (i, c) in codecs.iter().enumerate() {
            let mut exist = false;
            for r in &receivers {
                if mime::normalize_mime_type(&c.mime_type) == r.mime() {
                    exist = true;
                    break;
                }
            }
            if !exist {
                receivers.push(SimulcastReceiver::new(
                    DummyReceiver::new(
                        self.track_info(),
                        self.publisher_id().as_str(),
                        c.clone(),
                        headers,
                    ),
                    i,
                ));
            }
        }
        receivers.sort_by(|a, b| sutils::signum(a.priority() as i64 - b.priority() as i64).cmp(&0));
        inner.receivers = receivers;
    }

    pub fn clear_receiver(&self, mime: mime::MimeType, is_expected_to_resume: bool) {
        let mut inner = self.lock.write();
        let mut receivers = inner.receivers.clone();
        for (idx, receiver) in receivers.iter().enumerate() {
            if receiver.mime() == mime {
                receivers.swap_remove(idx);
                break;
            }
        }
        inner.receivers = receivers;
        drop(inner);

        self.remove_all_subscribers_for_mime(mime, is_expected_to_resume);
    }

    pub fn clear_all_receivers(&self, is_expected_to_resume: bool) {
        debugw!(
            self.params.logger,
            "clearing all receivers",
            "isExpectedToResume" => is_expected_to_resume,
        );
        let mut inner = self.lock.write();
        let receivers = std::mem::take(&mut inner.receivers);

        inner.is_expected_to_resume = is_expected_to_resume;
        drop(inner);

        for r in receivers {
            self.remove_all_subscribers_for_mime(r.mime(), is_expected_to_resume);
        }
    }

    pub fn on_media_loss_feedback(
        &self,
        f: impl Fn(&sfu::DownTrack, &ReceiverReport) + Send + Sync + 'static,
    ) {
        *self.on_media_loss_feedback.write() = Some(Arc::new(f));
    }

    pub fn is_open(&self) -> bool {
        let inner = self.lock.read();
        if inner.state != MediaTrackReceiverState::Open {
            return false;
        }
        // If any one of the receivers has entered closed state, we would not consider the track open
        for receiver in &inner.receivers {
            if receiver.is_closed() {
                return false;
            }
        }
        true
    }

    pub fn set_closing(&self, is_expected_to_resume: bool) {
        let mut inner = self.lock.write();

        if inner.state == MediaTrackReceiverState::Open {
            inner.state = MediaTrackReceiverState::Closing;
        }

        inner.is_expected_to_resume = is_expected_to_resume;
    }

    pub fn try_close(&self) -> bool {
        let inner = self.lock.read();
        if inner.state == MediaTrackReceiverState::Closed {
            return true;
        }

        let mut num_active_receivers = 0;
        for receiver in &inner.receivers {
            let dr = as_dummy_receiver(&receiver.track_receiver);
            if dr.is_none_or(|dr| dr.receiver().is_some()) {
                // !ok means real receiver OR
                // dummy receiver with a regular receiver attached
                num_active_receivers += 1;
            }
        }

        let is_expected_to_resume = inner.is_expected_to_resume;
        drop(inner);
        if num_active_receivers != 0 {
            return false;
        }

        self.close(is_expected_to_resume);
        true
    }

    pub fn close(&self, is_expected_to_resume: bool) {
        self.clear_all_receivers(is_expected_to_resume);

        let mut inner = self.lock.write();
        if inner.state == MediaTrackReceiverState::Closed {
            return;
        }

        inner.state = MediaTrackReceiverState::Closed;
        let onclose = inner.on_close.clone();
        drop(inner);

        for f in onclose {
            f(is_expected_to_resume);
        }
    }

    pub fn id(&self) -> livekit::TrackId {
        livekit::TrackId::from(self.track_info().sid.as_str())
    }

    pub fn kind(&self) -> livekit::TrackType {
        self.track_info().r#type()
    }

    pub fn source(&self) -> livekit::TrackSource {
        self.track_info().source()
    }

    pub fn stream(&self) -> String {
        self.track_info().stream.clone()
    }

    pub fn publisher_id(&self) -> livekit::ParticipantId {
        let participant_id = self
            .params
            .participant_id
            .as_ref()
            .expect("nil pointer dereference: MediaTrackReceiverParams without ParticipantID");
        participant_id()
    }

    pub fn publisher_identity(&self) -> livekit::ParticipantIdentity {
        self.params.participant_identity.clone()
    }

    pub fn publisher_version(&self) -> u32 {
        self.params.participant_version
    }

    pub fn name(&self) -> String {
        self.track_info().name.clone()
    }

    pub fn is_muted(&self) -> bool {
        self.track_info().muted
    }

    pub fn set_muted(&self, muted: bool) {
        let inner = self.lock.write();
        let mut track_info = self.track_info_clone();
        track_info.muted = muted;
        self.track_info.store(Some(Arc::new(track_info)));
        drop(inner);

        self.update_track_info_of_receivers();
    }

    pub fn is_encrypted(&self) -> bool {
        self.track_info().encryption != livekit::encryption::Type::None as i32
    }

    pub fn has_packet_trailer(&self) -> bool {
        !self.track_info().packet_trailer_features.is_empty()
    }

    pub fn add_on_close(&self, f: Option<Box<dyn Fn(bool) + Send + Sync>>) {
        let Some(f) = f else {
            return;
        };

        let mut inner = self.lock.write();
        inner.on_close.push(Arc::from(f));
    }

    // AddSubscriber subscribes sub to current mediaTrack
    pub fn add_subscriber(
        &self,
        sub: Arc<dyn types::LocalParticipant>,
    ) -> Result<Option<Arc<dyn types::SubscribedTrack>>, Error> {
        let inner = self.lock.read();
        if inner.state != MediaTrackReceiverState::Open {
            return Err(Error::NotOpen);
        }

        let receivers = inner.receivers.clone();
        let mut potential_codecs = inner.potential_codecs.clone();
        drop(inner);

        if receivers.is_empty() {
            // cannot add, no receiver
            return Err(Error::NoReceiver);
        }

        for receiver in &receivers {
            if receiver.is_regressed() {
                continue;
            }

            let codec = receiver.codec();
            let mut found = false;
            for pc in &potential_codecs {
                if mime::is_mime_type_string_equal(&codec.mime_type, &pc.mime_type) {
                    found = true;
                    break;
                }
            }
            if !found {
                potential_codecs.push(codec);
            }
        }

        let mut stream_id = self.publisher_id().as_str().to_string();
        if sub.protocol_version().supports_packed_stream_id() {
            // when possible, pack both IDs in streamID to allow new streams to be generated
            // react-native-webrtc still uses stream based APIs and require this
            stream_id = pack_stream_id(&self.publisher_id(), &self.id());
        }

        let t_logger = logger_with_track(&sub.get_logger(), &self.id(), self.params.is_relayed);
        let wr = WrappedReceiver::new(WrappedReceiverParams {
            receivers,
            track_id: self.id(),
            stream_id,
            upstream_codecs: potential_codecs,
            logger: t_logger,
            disable_red: !is_red_enabled(&self.track_info())
                || !self.params.audio_config.active_red_encoding,
            is_encrypted: self.is_encrypted(),
        });
        let sub_id = sub.id();
        let res = self.media_track_subscriptions.add_subscriber(sub, wr);

        // media track could have been closed while adding subscription
        let mut remove = false;
        let mut is_expected_to_resume = false;
        let inner = self.lock.read();
        if inner.state != MediaTrackReceiverState::Open {
            is_expected_to_resume = inner.is_expected_to_resume;
            remove = true;
        }
        drop(inner);

        if remove {
            debugw!(
                self.params.logger,
                "removing subscriber on a not-open track",
                "subscriberID" => sub_id,
                "isExpectedToResume" => is_expected_to_resume,
            );
            let _ = self
                .media_track_subscriptions
                .remove_subscriber(&sub_id, is_expected_to_resume);
            return Err(Error::NotOpen);
        }

        res.map(Some)
    }

    // RemoveSubscriber removes participant from subscription
    // stop all forwarders to the client
    pub fn remove_subscriber(
        &self,
        subscriber_id: &livekit::ParticipantId,
        is_expected_to_resume: bool,
    ) {
        let _ = self
            .media_track_subscriptions
            .remove_subscriber(subscriber_id, is_expected_to_resume);
    }

    fn remove_all_subscribers_for_mime(&self, mime: mime::MimeType, is_expected_to_resume: bool) {
        debugw!(
            self.params.logger,
            "removing all subscribers for mime",
            "mime" => mime,
            "isExpectedToResume" => is_expected_to_resume,
        );
        for subscriber_id in self
            .media_track_subscriptions
            .get_all_subscribers_for_mime(mime)
        {
            self.remove_subscriber(&subscriber_id, is_expected_to_resume);
        }
    }

    pub fn revoke_disallowed_subscribers(
        &self,
        allowed_subscriber_identities: &[livekit::ParticipantIdentity],
    ) -> Vec<livekit::ParticipantIdentity> {
        let mut revoked_subscriber_identities = Vec::new();

        // LK-TODO: large number of subscribers needs to be solved for this loop
        for sub_track in self.media_track_subscriptions.get_all_subscribed_tracks() {
            if is_participant_exempt_from_track_permissions_restrictions(&*sub_track.subscriber()) {
                continue;
            }

            let found = allowed_subscriber_identities.contains(&sub_track.subscriber_identity());
            if !found {
                infow!(
                    self.params.logger,
                    "revoking subscription",
                    "subscriber" => sub_track.subscriber_identity(),
                    "subscriberID" => sub_track.subscriber_id(),
                );
                self.remove_subscriber(&sub_track.subscriber_id(), false);
                revoked_subscriber_identities.push(sub_track.subscriber_identity());
            }
        }

        revoked_subscriber_identities
    }

    fn update_track_info_of_receivers(&self) {
        let ti = self.track_info();
        for r in self.load_receivers() {
            r.update_track_info(&ti);
        }

        self.media_track_subscriptions.set_muted(ti.muted);
    }

    pub fn maybe_set_simulcast(&self) {
        // only primary receiver (i.e. receiver at index 0) for legacy use case
        let Some(primary_receiver) = self.primary_receiver() else {
            return;
        };
        if as_web_rtc_receiver(&primary_receiver).is_none_or(|wr| wr.num_up_tracks() < 2) {
            return;
        }

        let inner = self.lock.write();
        let mut track_info = self.track_info_clone();
        if track_info.simulcast {
            return;
        }
        track_info.simulcast = true;
        self.track_info.store(Some(Arc::new(track_info)));
        drop(inner);

        self.update_track_info_of_receivers();
    }

    pub fn set_layer_ssrcs_for_rid(
        &self,
        mime_type: mime::MimeType,
        rid: &str,
        ssrc: u32,
        repair_ssrc: u32,
    ) {
        let inner = self.lock.write();
        let mut track_info = self.track_info_clone();
        let mut layer = buffer::get_spatial_layer_for_rid(mime_type, rid, Some(&track_info));
        if layer == buffer::INVALID_LAYER_SPATIAL {
            // non-simulcast case will not have `rid`
            layer = 0;
        }
        let quality =
            buffer::get_video_quality_for_spatial_layer(mime_type, layer, Some(&track_info));
        // set video layer ssrc info
        for i in 0..track_info.codecs.len() {
            if mime::normalize_mime_type(&track_info.codecs[i].mime_type) != mime_type {
                continue;
            }

            // if origin layer has ssrc, don't override it
            let mut matching_layer: Option<usize> = None;
            let mut ssrc_found = false;
            for (li, l) in track_info.codecs[i].layers.iter().enumerate() {
                if l.quality == quality as i32 {
                    matching_layer = Some(li);
                    if l.ssrc != 0 {
                        ssrc_found = true;
                    }
                    break;
                }
            }
            if let Some(li) = matching_layer {
                if !ssrc_found {
                    let matching_layer = &mut track_info.codecs[i].layers[li];
                    matching_layer.ssrc = ssrc;
                    if repair_ssrc != 0 {
                        matching_layer.repair_ssrc = repair_ssrc;
                    }
                }
                let matching_layer = &track_info.codecs[i].layers[li];
                if ssrc_found
                    && (matching_layer.ssrc != ssrc || matching_layer.repair_ssrc != repair_ssrc)
                {
                    warnw!(
                        self.params.logger,
                        "not overriding ssrc",
                        nil,
                        "rid" => rid,
                        "ssrc" => ssrc,
                        "existingSSRC" => matching_layer.ssrc,
                        "repairSSRC" => repair_ssrc,
                        "existingRepairSSRC" => matching_layer.repair_ssrc,
                        "trackInfo" => track_info,
                    );
                }
            }

            // for client don't use simulcast codecs (old client version or single codec)
            if i == 0 {
                track_info.layers = track_info.codecs[i].layers.clone();
            }
            break;
        }
        self.track_info.store(Some(Arc::new(track_info)));
        drop(inner);

        self.update_track_info_of_receivers();
    }

    // Unexported upstream (`setLayerRtxInfo`), and public here: `mediatrack.go` hands it to
    // the buffer as what is told of a retransmission stream.
    pub fn set_layer_rtx_info(&self, ssrc: u32, repair_ssrc: u32, rsid: &str) {
        debugw!(
            self.params.logger,
            "rtx notification",
            "ssrc" => ssrc,
            "repairSSRC" => repair_ssrc,
            "rsid" => rsid,
        );
        if ssrc == 0 || repair_ssrc == 0 || rsid.is_empty() {
            return;
        }

        let inner = self.lock.write();
        let mut track_info = self.track_info_clone();

        'done: for ci in 0..track_info.codecs.len() {
            for li in 0..track_info.codecs[ci].layers.len() {
                let l = &track_info.codecs[ci].layers[li];
                if l.ssrc == ssrc {
                    if (l.repair_ssrc != 0 && l.repair_ssrc != repair_ssrc)
                        || (!l.rid.is_empty() && l.rid != rsid)
                    {
                        warnw!(
                            self.params.logger,
                            "not overriding rtx info",
                            nil,
                            "ssrc" => ssrc,
                            "repairSSRC" => repair_ssrc,
                            "existingRepairSSRC" => l.repair_ssrc,
                            "rsid" => rsid,
                            "existingRid" => l.rid,
                            "trackInfo" => track_info,
                        );
                    } else {
                        track_info.codecs[ci].layers[li].repair_ssrc = repair_ssrc;
                        debugw!(
                            self.params.logger,
                            "set rtx info",
                            "ssrc" => ssrc,
                            "repairSSRC" => repair_ssrc,
                            "rsid" => rsid,
                            "trackInfo" => track_info,
                        );
                    }
                    break 'done;
                }
            }
        }

        // backwards compatibility
        for li in 0..track_info.layers.len() {
            let l = &track_info.layers[li];
            if l.ssrc == ssrc {
                if (l.repair_ssrc != 0 && l.repair_ssrc != repair_ssrc)
                    || (!l.rid.is_empty() && l.rid != rsid)
                {
                    warnw!(
                        self.params.logger,
                        "not overriding rtx info",
                        nil,
                        "ssrc" => ssrc,
                        "repairSSRC" => repair_ssrc,
                        "existingRepairSSRC" => l.repair_ssrc,
                        "rsid" => rsid,
                        "existingRid" => l.rid,
                        "trackInfo" => track_info,
                    );
                } else {
                    track_info.layers[li].repair_ssrc = repair_ssrc;
                    debugw!(
                        self.params.logger,
                        "set rtx info",
                        "ssrc" => ssrc,
                        "repairSSRC" => repair_ssrc,
                        "rsid" => rsid,
                        "trackInfo" => track_info,
                    );
                }
                break;
            }
        }

        self.track_info.store(Some(Arc::new(track_info)));
        drop(inner);

        // change not propagated as it is internal
    }

    pub fn update_codec_info(&self, codecs: &[livekit::SimulcastCodec]) {
        let inner = self.lock.write();
        let mut track_info = self.track_info_clone();
        for c in codecs {
            for oi in 0..track_info.codecs.len() {
                if mime::get_mime_type_codec(&track_info.codecs[oi].mime_type)
                    == mime::normalize_mime_type_codec(&c.codec)
                {
                    track_info.codecs[oi].cid = c.cid.clone();

                    if !c.layers.is_empty() {
                        let mut cloned_layers = Vec::with_capacity(c.layers.len());
                        for l in &c.layers {
                            cloned_layers.push(l.clone());
                        }
                        track_info.codecs[oi].layers = cloned_layers;

                        let mime_type = mime::normalize_mime_type(&track_info.codecs[oi].mime_type);
                        for li in 0..track_info.codecs[oi].layers.len() {
                            let quality = track_info.codecs[oi].layers[li].quality();
                            let spatial_layer = buffer::video_quality_to_spatial_layer(
                                mime_type,
                                quality,
                                Some(&track_info),
                            );
                            track_info.codecs[oi].layers[li].spatial_layer = spatial_layer;
                            let rid = buffer::video_quality_to_rid(
                                mime_type,
                                quality,
                                Some(&track_info),
                                &buffer::DEFAULT_VIDEO_LAYERS_RID,
                            );
                            track_info.codecs[oi].layers[li].rid = rid;
                        }
                    }

                    break;
                }
            }
        }
        self.track_info.store(Some(Arc::new(track_info)));
        drop(inner);

        self.update_track_info_of_receivers();
    }

    pub fn update_codec_sdp_cid(&self, mime_type: mime::MimeType, sdp_cid: &str) {
        let inner = self.lock.write();
        let mut track_info = self.track_info_clone();
        for origin in &mut track_info.codecs {
            if mime::normalize_mime_type(&origin.mime_type) == mime_type && sdp_cid != origin.cid {
                origin.sdp_cid = sdp_cid.to_string();
            }
        }
        self.track_info.store(Some(Arc::new(track_info)));
        drop(inner);

        self.update_track_info_of_receivers();
    }

    pub fn update_codec_rids(&self, mime_type: mime::MimeType, rids: &buffer::VideoLayersRid) {
        let inner = self.lock.write();
        let mut track_info = self.track_info_clone();
        for oi in 0..track_info.codecs.len() {
            let origin_mime_type = mime::normalize_mime_type(&track_info.codecs[oi].mime_type);
            if origin_mime_type != mime_type {
                continue;
            }

            for li in 0..track_info.codecs[oi].layers.len() {
                let quality = track_info.codecs[oi].layers[li].quality();
                let spatial_layer =
                    buffer::video_quality_to_spatial_layer(mime_type, quality, Some(&track_info));
                track_info.codecs[oi].layers[li].spatial_layer = spatial_layer;
                let rid = buffer::video_quality_to_rid(mime_type, quality, Some(&track_info), rids);
                track_info.codecs[oi].layers[li].rid = rid;
            }
            break;
        }
        self.track_info.store(Some(Arc::new(track_info)));
        drop(inner);

        self.update_track_info_of_receivers();
    }

    pub fn update_track_info(&self, ti: &livekit::TrackInfo) {
        let mut cloned_info = ti.clone();

        let inner = self.lock.write();
        let track_info = self.track_info();
        // patch Mid/Rid and Ssrc/RtxSsrc of codecs/layers by keeping original if available
        for i in 0..cloned_info.codecs.len() {
            let ci = &mut cloned_info.codecs[i];
            for origin_ci in &track_info.codecs {
                if !mime::is_mime_type_string_equal(&ci.mime_type, &origin_ci.mime_type) {
                    continue;
                }

                if !origin_ci.mid.is_empty() {
                    ci.mid = origin_ci.mid.clone();
                }

                for layer in &mut ci.layers {
                    for origin_layer in &origin_ci.layers {
                        if layer.quality == origin_layer.quality {
                            if origin_layer.ssrc != 0 {
                                layer.ssrc = origin_layer.ssrc;
                            }
                            if !origin_layer.rid.is_empty() {
                                layer.rid = origin_layer.rid.clone();
                            }

                            if origin_layer.repair_ssrc != 0 {
                                layer.repair_ssrc = origin_layer.repair_ssrc;
                            }
                            break;
                        }
                    }
                }
                break;
            }

            // for clients that don't use simulcast codecs (old client version or single codec)
            if i == 0 {
                cloned_info.layers = cloned_info.codecs[i].layers.clone();
            }
        }
        self.track_info.store(Some(Arc::new(cloned_info)));
        drop(inner);

        self.update_track_info_of_receivers();
    }

    pub fn update_audio_track(&self, update: &livekit::UpdateLocalAudioTrack) {
        if self.kind() != livekit::TrackType::Audio {
            return;
        }

        let inner = self.lock.write();
        let track_info = self.track_info();
        let mut cloned_info = (*track_info).clone();

        cloned_info.audio_features = sutils::dedupe_slice(update.features.clone());
        // `update.Features` as the original's `DedupeSlice` leaves it, for the loop below:
        // sorted, without duplicates, and a zero for each one that was taken out.
        let mut features = cloned_info.audio_features.clone();
        features.resize(update.features.len(), 0);

        cloned_info.stereo = false;
        cloned_info.disable_dtx = false;
        for feature in features {
            if feature == livekit::AudioTrackFeature::TfStereo as i32 {
                cloned_info.stereo = true;
            } else if feature == livekit::AudioTrackFeature::TfNoDtx as i32 {
                cloned_info.disable_dtx = true;
            }
        }

        if *track_info == cloned_info {
            return;
        }

        let cloned_info = Arc::new(cloned_info);
        self.track_info.store(Some(cloned_info.clone()));
        drop(inner);

        self.update_track_info_of_receivers();

        self.telemetry_listener()
            .on_track_published_update(&self.publisher_id(), &cloned_info);
        debugw!(self.params.logger, "updated audio track", "before" => track_info, "after" => cloned_info);
    }

    pub fn update_video_track(&self, update: &livekit::UpdateLocalVideoTrack) {
        if self.kind() != livekit::TrackType::Video {
            return;
        }

        let inner = self.lock.write();
        let track_info = self.track_info();
        let mut cloned_info = (*track_info).clone();
        cloned_info.width = update.width;
        cloned_info.height = update.height;
        if *track_info == cloned_info {
            return;
        }

        let cloned_info = Arc::new(cloned_info);
        self.track_info.store(Some(cloned_info.clone()));
        drop(inner);

        self.update_track_info_of_receivers();

        self.telemetry_listener()
            .on_track_published_update(&self.publisher_id(), &cloned_info);
        debugw!(self.params.logger, "updated video track", "before" => track_info, "after" => cloned_info);
    }

    pub fn update_video_size(&self, mime_type: mime::MimeType, sizes: &[codec::VideoSize]) {
        let mut changed = false;
        let inner = self.lock.write();
        let track_info = self.track_info();
        let mut cloned_info = (*track_info).clone();
        let mut max_width: u32 = 0;
        let mut max_height: u32 = 0;
        for size in sizes {
            if size.width > max_width {
                max_width = size.width;
                max_height = size.height;
            }
        }

        if cloned_info.width != max_width || cloned_info.height != max_height {
            cloned_info.width = max_width;
            cloned_info.height = max_height;
            changed = true;
        }

        for c in &mut cloned_info.codecs {
            if mime::normalize_mime_type(&c.mime_type) == mime_type {
                for (i, l) in c.layers.iter_mut().enumerate() {
                    if i < sizes.len()
                        && (sizes[i].width != 0 || sizes[i].height != 0)
                        && (l.width != sizes[i].width || l.height != sizes[i].height)
                    {
                        l.width = sizes[i].width;
                        l.height = sizes[i].height;
                        changed = true;
                    }
                }
            }
        }

        if !changed {
            return;
        }

        let cloned_info = Arc::new(cloned_info);
        self.track_info.store(Some(cloned_info.clone()));
        drop(inner);

        self.update_track_info_of_receivers();

        self.telemetry_listener()
            .on_track_published_update(&self.publisher_id(), &cloned_info);
        debugw!(self.params.logger, "updated video sizes", "before" => track_info, "after" => cloned_info);
    }

    pub fn track_info(&self) -> Arc<livekit::TrackInfo> {
        self.track_info.load().unwrap_or_default()
    }

    pub fn track_info_clone(&self) -> livekit::TrackInfo {
        (*self.track_info()).clone()
    }

    pub fn notify_max_layer_change(&self, mime_type: mime::MimeType, max_layer: i32) {
        let track_info = self.track_info();
        let quality =
            buffer::get_video_quality_for_spatial_layer(mime_type, max_layer, Some(&*track_info));
        let mut ti = livekit::TrackInfo {
            sid: track_info.sid.clone(),
            r#type: track_info.r#type,
            layers: vec![livekit::VideoLayer {
                quality: quality as i32,
                ..Default::default()
            }],
            ..Default::default()
        };
        if quality != livekit::VideoQuality::Off {
            let layers = buffer::get_video_layers_for_mime_type(mime_type, Some(&*track_info));
            for layer in layers {
                if layer.quality == quality as i32 {
                    ti.layers[0].width = layer.width;
                    ti.layers[0].height = layer.height;
                    break;
                }
            }
        }

        self.telemetry_listener()
            .on_track_published_update(&self.publisher_id(), &ti);
    }

    // GetQualityForDimension finds the closest quality to use for desired dimensions
    // affords a 20% tolerance on dimension
    pub fn get_quality_for_dimension(
        &self,
        mime_type: mime::MimeType,
        width: u32,
        height: u32,
    ) -> livekit::VideoQuality {
        let mut quality = livekit::VideoQuality::High;
        if self.kind() == livekit::TrackType::Audio {
            return quality;
        }

        let track_info = self.track_info();

        let mut media_sizes: Vec<codec::VideoSize> = Vec::new();
        if let Some(receiver) = self.receiver(mime_type) {
            media_sizes = receiver.video_sizes();
        }

        if track_info.height == 0 && media_sizes.is_empty() {
            return quality;
        }
        let mut orig_size = track_info.height;
        let mut requested_size = height;
        if track_info.width < track_info.height {
            // for portrait videos
            orig_size = track_info.width;
            requested_size = width;
        }

        if orig_size == 0 {
            for i in (0..media_sizes.len()).rev() {
                if media_sizes[i].height > 0 {
                    orig_size = media_sizes[i].width.min(media_sizes[i].height);
                    break;
                }
            }
        }

        // default sizes representing qualities low - high
        let mut layer_sizes: Vec<u32> = vec![180, 360, orig_size];
        let mut provided_sizes: Vec<u32> = Vec::new();
        for layer in buffer::get_video_layers_for_mime_type(mime_type, Some(&*track_info)) {
            provided_sizes.push(layer.height);
        }

        if provided_sizes.is_empty()
            || provided_sizes[0] == 0
            || self.params.prefer_video_size_from_media
        {
            if !media_sizes.is_empty() {
                provided_sizes.clear();
                for size in &media_sizes {
                    provided_sizes.push(size.height);
                }
            } else {
                debugw!(
                    self.params.logger,
                    "no video sizes provided by receiver, using track info sizes"
                );
            }
        }

        if !provided_sizes.is_empty() {
            layer_sizes = provided_sizes;
            // comparing height always
            requested_size = height;
            layer_sizes.sort_unstable();
        }

        // finds the highest layer with smallest dimensions that still satisfy client demands
        requested_size = (requested_size as f32 * LAYER_SELECTION_TOLERANCE) as u32;
        for (i, &s) in layer_sizes.iter().enumerate() {
            quality =
                livekit::VideoQuality::try_from(i as i32).unwrap_or(livekit::VideoQuality::High);
            if i == layer_sizes.len() - 1 || (s >= requested_size && s != layer_sizes[i + 1]) {
                break;
            }
        }

        quality
    }

    pub fn get_audio_level(&self) -> (f64, bool) {
        let Some(receiver) = self.active_receiver() else {
            return (0.0, false);
        };

        receiver.get_audio_level()
    }

    fn on_down_track_created(&self, down_track: Arc<sfu::DownTrack>) {
        if self.kind() == livekit::TrackType::Audio {
            let t = self.this.clone();
            down_track.add_receiver_report_listener(move |dt, rr| {
                let Some(t) = t.upgrade() else {
                    return;
                };
                let on_media_loss_feedback = t.on_media_loss_feedback.read().clone();
                if let Some(on_media_loss_feedback) = on_media_loss_feedback {
                    on_media_loss_feedback(dt, rr);
                }
            });
        }
    }

    pub fn debug_info(&self) -> sfu::DebugInfo {
        let mut info = sfu::DebugInfo::new();
        info.insert("ID".to_string(), Value::from(self.id().as_str()));
        info.insert("Kind".to_string(), Value::from(self.kind().as_str_name()));
        info.insert("PubMuted".to_string(), Value::from(self.is_muted()));

        info.insert(
            "DownTracks".to_string(),
            Value::Array(
                self.media_track_subscriptions
                    .debug_info()
                    .into_iter()
                    .map(Value::Object)
                    .collect(),
            ),
        );

        for receiver in self.load_receivers() {
            info.insert(
                receiver.codec().mime_type.clone(),
                Value::Object(receiver.debug_info()),
            );
        }

        info
    }

    pub fn primary_receiver(&self) -> Option<Arc<dyn sfu::TrackReceiver>> {
        let receivers = self.load_receivers();
        if receivers.is_empty() {
            return None;
        }
        if let Some(dr) = as_dummy_receiver(&receivers[0].track_receiver) {
            return dr.receiver();
        }
        Some(receivers[0].track_receiver.clone())
    }

    pub fn active_receiver(&self) -> Option<Arc<dyn sfu::TrackReceiver>> {
        for r in self.load_receivers() {
            if r.is_regressed() {
                return Some(r.track_receiver.clone());
            }
        }

        self.primary_receiver()
    }

    pub fn receiver(&self, mime: mime::MimeType) -> Option<Arc<dyn sfu::TrackReceiver>> {
        for r in self.load_receivers() {
            if r.mime() == mime {
                if let Some(dr) = as_dummy_receiver(&r.track_receiver) {
                    return dr.receiver();
                }
                return Some(r.track_receiver.clone());
            }
        }
        None
    }

    pub fn receivers(&self) -> Vec<Arc<dyn sfu::TrackReceiver>> {
        let receivers = self.load_receivers();
        let mut track_receivers = Vec::with_capacity(receivers.len());
        for r in &receivers {
            track_receivers.push(r.track_receiver.clone());
        }
        track_receivers
    }

    fn load_receivers(&self) -> Vec<Arc<SimulcastReceiver>> {
        let inner = self.lock.read();
        inner.receivers.clone()
    }

    pub fn set_rtt(&self, rtt: u32) {
        for r in self.load_receivers() {
            if let Some(wr) = as_web_rtc_receiver(&r.track_receiver) {
                wr.set_rtt(rtt);
            }
        }
    }

    pub fn get_temporal_layer_for_spatial_fps(
        &self,
        mime_type: mime::MimeType,
        spatial: i32,
        fps: u32,
    ) -> i32 {
        let Some(receiver) = self.receiver(mime_type) else {
            return buffer::DEFAULT_MAX_LAYER_TEMPORAL;
        };

        let layer_fps = receiver.get_temporal_layer_fps_for_spatial(spatial);
        let request_fps = fps as f32 * LAYER_SELECTION_TOLERANCE;
        for (i, &f) in layer_fps.iter().enumerate() {
            if request_fps <= f {
                return i as i32;
            }
        }
        buffer::DEFAULT_MAX_LAYER_TEMPORAL
    }

    pub fn get_track_stats(&self) -> Option<livekit::RtpStats> {
        let receivers = self.load_receivers();
        let mut stats = Vec::with_capacity(receivers.len());
        for receiver in &receivers {
            let receiver_stats = receiver.get_track_stats();
            if let Some(receiver_stats) = receiver_stats {
                stats.push(receiver_stats);
            }
        }

        rtpstats::aggregate_rtp_stats(&stats)
    }

    // `t.params.TelemetryListener()`
    fn telemetry_listener(&self) -> Arc<dyn types::ParticipantTelemetryListener> {
        let telemetry_listener =
            self.params.telemetry_listener.as_ref().expect(
                "nil pointer dereference: MediaTrackReceiverParams without TelemetryListener",
            );
        telemetry_listener()
    }

    // The embedded `*MediaTrackSubscriptions`, and those of its methods that the original
    // reaches through the embedding (the others are the receiver's own, above).

    pub fn media_track_subscriptions(&self) -> &Arc<MediaTrackSubscriptions> {
        &self.media_track_subscriptions
    }

    pub fn on_subscriber_max_quality_change(
        &self,
        f: impl Fn(&livekit::ParticipantId, mime::MimeType, i32) + Send + Sync + 'static,
    ) {
        self.media_track_subscriptions
            .on_subscriber_max_quality_change(f);
    }

    pub fn on_subscriber_audio_codec_change(
        &self,
        f: impl Fn(&livekit::ParticipantId, mime::MimeType, bool) + Send + Sync + 'static,
    ) {
        self.media_track_subscriptions
            .on_subscriber_audio_codec_change(f);
    }

    pub fn is_subscriber(&self, sub_id: &livekit::ParticipantId) -> bool {
        self.media_track_subscriptions.is_subscriber(sub_id)
    }

    pub fn get_all_subscribers(&self) -> Vec<livekit::ParticipantId> {
        self.media_track_subscriptions.get_all_subscribers()
    }

    pub fn get_all_subscribers_for_mime(
        &self,
        mime: mime::MimeType,
    ) -> Vec<livekit::ParticipantId> {
        self.media_track_subscriptions
            .get_all_subscribers_for_mime(mime)
    }

    pub fn get_num_subscribers(&self) -> usize {
        self.media_track_subscriptions.get_num_subscribers()
    }

    pub fn update_video_layers(&self) {
        self.media_track_subscriptions.update_video_layers();
    }
}

// Go's `r.(*DummyReceiver)` on an `sfu.TrackReceiver`.
fn as_dummy_receiver(r: &Arc<dyn sfu::TrackReceiver>) -> Option<Arc<DummyReceiver>> {
    let r: Arc<dyn Any + Send + Sync> = r.clone();
    r.downcast::<DummyReceiver>().ok()
}

// Go's `r.(*sfu.WebRTCReceiver)` on an `sfu.TrackReceiver`.
fn as_web_rtc_receiver(r: &Arc<dyn sfu::TrackReceiver>) -> Option<Arc<sfu::WebRtcReceiver>> {
    let r: Arc<dyn Any + Send + Sync> = r.clone();
    r.downcast::<sfu::WebRtcReceiver>().ok()
}

// Go's strings.EqualFold(s, t) for a `t` that is ASCII, as a mime type is: whether the two
// are equal under Unicode's simple case folding. That folding takes, next to the ASCII
// letters in their other case, two more characters to an ASCII letter: 'ſ' (U+017F) to 's'
// and the Kelvin sign (U+212A) to 'k'. (The same function is in `clientinfo.rs` and in
// `protocol/codecs/mime/mimetype.rs`, private to each file.)
fn equal_fold(s: &str, t: &str) -> bool {
    let mut t = t.chars();
    for sr in s.chars() {
        let Some(tr) = t.next() else {
            return false;
        };
        if sr.eq_ignore_ascii_case(&tr) {
            continue;
        }
        match (sr, tr) {
            ('\u{17f}', 's' | 'S') | ('\u{212a}', 'k' | 'K') => continue,
            _ => return false,
        }
    }
    t.next().is_none()
}

#[cfg(test)]
#[path = "mediatrackreceiver_test.rs"]
mod mediatrackreceiver_test;
