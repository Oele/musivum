// Ported from LiveKit, pkg/rtc/wrappedreceiver.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The three receivers are made as `Arc`s. Each embeds an `sfu.TrackReceiver` upstream
//   (`DummyRedReceiver` a `*DummyReceiver`): here that is a field, and the methods of
//   `impl TrackReceiver` that only hand a call on to it are what the embedding was. The
//   original's own methods are inherent ones where the interface does not have them, and are
//   in the trait impl where it does: for `WrappedReceiver` in the trait's order (four of its
//   methods are the original's), for the two dummy receivers in the original's.
// - `WrappedReceiver`: the embedded receiver is nil until `determine_receiver` has chosen
//   one. It is behind the wrapper's lock with the callbacks (the original writes it under the
//   lock and reads it without). A call that is handed on before there is one panics, as the
//   call on the nil interface does in the original; the original's own methods (`track_id`,
//   `stream_id`, `delete_down_track`, `add_on_ready`, `codecs`) do not need one.
// - `DummyReceiver`: `receiver atomic.Value`, which is set once (`CompareAndSwap(nil, ..)`),
//   is a `OnceLock`. `maxExpectedLayer` is copied under the settings lock with its flag,
//   where the original reads it after unlocking. `ReadRTP`'s `errors.New("no receiver")` has
//   no variant in `sfu::Error`, which the trait returns: what stands for it is the error a
//   receiver without a buffer for the layer gives (the bucket's "sequence number mismatch");
//   the down track only tells the end of a buffer apart from every other error.
// - Who holds whom. A wrapped receiver holds the receivers of its track and the one it
//   chose; a dummy receiver holds the receiver it was upgraded to, and the down tracks and
//   callbacks that wait for one. A `DummyRedReceiver` holds its `DummyReceiver` (it is the
//   embedded one), so the `DummyReceiver` holds its two RED receivers weakly and makes one
//   anew when nobody holds the last: a RED receiver with down tracks waiting in it is held
//   by those down tracks through their wrapped receiver. Down tracks that wait in a dummy
//   receiver are held by it until the upgrade hands them on or they are deleted, as a real
//   receiver holds its down tracks.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::Mutex;

use super::SimulcastReceiver;
use crate::go::atomic;
use crate::mediatransportutil::bucket;
use crate::mediatransportutil::codec;
use crate::protocol::codecs as proto_codecs;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu;
use crate::sfu::buffer;
use crate::warnw;
use crate::webrtc;

// wrapper around WebRTC receiver, overriding its ID

pub struct WrappedReceiverParams {
    pub receivers: Vec<Arc<SimulcastReceiver>>,
    pub track_id: livekit::TrackId,
    pub stream_id: String,
    pub upstream_codecs: Vec<webrtc::RtpCodecParameters>,
    pub logger: Logger,
    pub disable_red: bool,
    pub is_encrypted: bool,
}

pub struct WrappedReceiver {
    // what `lock sync.Mutex` guards upstream
    lock: Mutex<WrappedReceiverInner>,

    params: WrappedReceiverParams,
    receivers: Vec<Arc<dyn sfu::TrackReceiver>>,
    codecs: Vec<webrtc::RtpCodecParameters>,
}

#[derive(Default)]
struct WrappedReceiverInner {
    // the `sfu.TrackReceiver` that the original embeds
    track_receiver: Option<Arc<dyn sfu::TrackReceiver>>,
    on_ready_callbacks: Vec<Box<dyn Fn() + Send + Sync>>,
}

impl WrappedReceiver {
    pub fn new(params: WrappedReceiverParams) -> Arc<WrappedReceiver> {
        let mut sfu_receivers: Vec<Arc<dyn sfu::TrackReceiver>> =
            Vec::with_capacity(params.receivers.len());
        for r in &params.receivers {
            sfu_receivers.push(r.clone());
        }

        let mut codecs = params.upstream_codecs.clone();
        if codecs.len() == 1 {
            let normalized_mime_type = mime::normalize_mime_type(&codecs[0].mime_type);
            if normalized_mime_type == mime::MimeType::Red {
                // if upstream is opus/red, then add opus to match clients that don't support red
                codecs.push(proto_codecs::OPUS_CODEC_PARAMETERS.clone());
            } else if !params.disable_red && normalized_mime_type == mime::MimeType::Opus {
                // if upstream is opus only and red enabled, add red to match clients that support red
                codecs.push(proto_codecs::RED_CODEC_PARAMETERS.clone());
                // prefer red codec
                codecs.swap(0, 1);
            }
        }

        Arc::new(WrappedReceiver {
            lock: Mutex::new(WrappedReceiverInner::default()),
            params,
            receivers: sfu_receivers,
            codecs,
        })
    }

    // DetermineReceiver determines the receiver of negotiated codec and returns
    //
    // isAvailable: returns true if given codec is a potential codec from publisher or if an existing published codec can be translated
    // needsPublish: indicates if the codec is needed from publisher, some combinations can be achieved via codec translation internally,
    //
    //	example: unencrypted opus -> RED translation and vice-versa can be done without the need for publisher to send the other codec.
    pub fn determine_receiver(&self, codec: &webrtc::RtpCodecCapability) -> (bool, bool) {
        let mut is_available = false;
        let mut needs_publish = false;
        let mut inner = self.lock.lock();

        let mut reason = "no matching receiver";
        let codec_mime_type = mime::normalize_mime_type(&codec.mime_type);
        let mut track_receiver: Option<Arc<dyn sfu::TrackReceiver>> = None;
        for receiver in &self.receivers {
            let receiver_mime_type = receiver.mime();
            if receiver_mime_type == codec_mime_type {
                track_receiver = Some(receiver.clone());
                is_available = true;
                needs_publish = true;
                break;
            }

            if receiver_mime_type == mime::MimeType::Red && codec_mime_type == mime::MimeType::Opus
            {
                // audio opus/red can match opus only
                if !self.params.is_encrypted {
                    // cannot match encrypted source
                    track_receiver = Some(receiver.get_primary_receiver_for_red());
                    is_available = true;
                    break;
                } else {
                    reason = "encrypted source";
                }
            } else if receiver_mime_type == mime::MimeType::Opus
                && codec_mime_type == mime::MimeType::Red
            {
                if !self.params.is_encrypted {
                    // cannot match encrypted source
                    track_receiver = Some(receiver.get_red_receiver());
                    is_available = true;
                    break;
                } else {
                    reason = "encrypted source";
                }
            }
        }
        let Some(track_receiver) = track_receiver else {
            drop(inner);
            warnw!(
                self.params.logger,
                "can't determine receiver for codec",
                nil,
                "codec" => codec.mime_type,
                "reason" => reason,
            );
            return (is_available, needs_publish);
        };
        inner.track_receiver = Some(track_receiver.clone());

        let on_ready_callbacks = std::mem::take(&mut inner.on_ready_callbacks);
        drop(inner);

        for f in on_ready_callbacks {
            track_receiver.add_on_ready(f);
        }

        (is_available, needs_publish)
    }

    pub fn codecs(&self) -> Vec<webrtc::RtpCodecParameters> {
        self.codecs.clone()
    }

    // The embedded receiver, for a call that is handed on to it. The original calls through
    // the embedded interface without looking: with none chosen yet that is a nil pointer
    // dereference there and this panic here.
    fn track_receiver(&self) -> Arc<dyn sfu::TrackReceiver> {
        self.lock
            .lock()
            .track_receiver
            .clone()
            .expect("nil pointer dereference: WrappedReceiver without a determined receiver")
    }
}

impl sfu::TrackReceiver for WrappedReceiver {
    fn track_id(&self) -> livekit::TrackId {
        self.params.track_id.clone()
    }

    fn stream_id(&self) -> String {
        self.params.stream_id.clone()
    }

    fn codec(&self) -> webrtc::RtpCodecParameters {
        self.track_receiver().codec()
    }

    fn mime(&self) -> mime::MimeType {
        self.track_receiver().mime()
    }

    fn video_layer_mode(&self) -> livekit::video_layer::Mode {
        self.track_receiver().video_layer_mode()
    }

    fn header_extensions(&self) -> Vec<webrtc::RtpHeaderExtensionParameter> {
        self.track_receiver().header_extensions()
    }

    fn is_closed(&self) -> bool {
        self.track_receiver().is_closed()
    }

    fn read_rtp(&self, buf: &mut [u8], layer: u8, esn: u64) -> Result<usize, sfu::Error> {
        self.track_receiver().read_rtp(buf, layer, esn)
    }

    fn get_layered_bitrate(&self) -> (Vec<i32>, sfu::Bitrates) {
        self.track_receiver().get_layered_bitrate()
    }

    fn get_audio_level(&self) -> (f64, bool) {
        self.track_receiver().get_audio_level()
    }

    fn send_pli(&self, layer: i32, force: bool) {
        self.track_receiver().send_pli(layer, force);
    }

    fn set_max_expected_spatial_layer(&self, layer: i32) {
        self.track_receiver().set_max_expected_spatial_layer(layer);
    }

    fn add_down_track(&self, track: Arc<dyn sfu::TrackSender>) -> Result<(), sfu::Error> {
        self.track_receiver().add_down_track(track)
    }

    fn delete_down_track(&self, participant_id: &livekit::ParticipantId) {
        let track_receiver = self.lock.lock().track_receiver.clone();

        if let Some(track_receiver) = track_receiver {
            track_receiver.delete_down_track(participant_id);
        }
    }

    fn get_down_tracks(&self) -> Vec<Arc<dyn sfu::TrackSender>> {
        self.track_receiver().get_down_tracks()
    }

    fn debug_info(&self) -> sfu::DebugInfo {
        self.track_receiver().debug_info()
    }

    fn track_info(&self) -> Arc<livekit::TrackInfo> {
        self.track_receiver().track_info()
    }

    fn update_track_info(&self, ti: &livekit::TrackInfo) {
        self.track_receiver().update_track_info(ti);
    }

    fn get_primary_receiver_for_red(&self) -> Arc<dyn sfu::TrackReceiver> {
        self.track_receiver().get_primary_receiver_for_red()
    }

    fn get_red_receiver(&self) -> Arc<dyn sfu::TrackReceiver> {
        self.track_receiver().get_red_receiver()
    }

    fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32> {
        self.track_receiver()
            .get_temporal_layer_fps_for_spatial(layer)
    }

    fn get_track_stats(&self) -> Option<livekit::RtpStats> {
        self.track_receiver().get_track_stats()
    }

    fn add_on_ready(&self, f: Box<dyn Fn() + Send + Sync>) {
        let mut inner = self.lock.lock();
        let Some(track_receiver) = inner.track_receiver.clone() else {
            inner.on_ready_callbacks.push(f);
            return;
        };
        drop(inner);

        track_receiver.add_on_ready(f);
    }

    fn add_on_codec_state_change(
        &self,
        f: Box<dyn Fn(webrtc::RtpCodecParameters, sfu::ReceiverCodecState) + Send + Sync>,
    ) {
        self.track_receiver().add_on_codec_state_change(f);
    }

    fn codec_state(&self) -> sfu::ReceiverCodecState {
        self.track_receiver().codec_state()
    }

    fn video_sizes(&self) -> Vec<codec::VideoSize> {
        self.track_receiver().video_sizes()
    }

    fn restart(&self, reason: &str) {
        self.track_receiver().restart(reason);
    }
}

// --------------------------------------------

pub struct DummyReceiver {
    receiver: OnceLock<Arc<dyn sfu::TrackReceiver>>,
    track_info: Arc<livekit::TrackInfo>,
    stream_id: String,
    codec: webrtc::RtpCodecParameters,
    header_extensions: Vec<webrtc::RtpHeaderExtensionParameter>,

    // what `downTrackLock sync.Mutex` guards upstream
    down_track_lock: Mutex<DummyReceiverDownTracks>,

    // what `settingsLock sync.Mutex` guards upstream
    settings_lock: Mutex<DummyReceiverSettings>,

    // the receiver itself, which its RED receivers embed
    this: Weak<DummyReceiver>,
}

#[derive(Default)]
struct DummyReceiverDownTracks {
    down_tracks: HashMap<livekit::ParticipantId, Arc<dyn sfu::TrackSender>>,
    on_ready_callbacks: Vec<Box<dyn Fn() + Send + Sync>>,
    #[allow(clippy::type_complexity)]
    on_codec_state_change:
        Vec<Box<dyn Fn(webrtc::RtpCodecParameters, sfu::ReceiverCodecState) + Send + Sync>>,
}

#[derive(Default)]
struct DummyReceiverSettings {
    max_expected_layer_valid: bool,
    max_expected_layer: i32,

    red_receiver: Weak<DummyRedReceiver>,
    primary_receiver: Weak<DummyRedReceiver>,
}

impl DummyReceiver {
    pub fn new(
        track_info: Arc<livekit::TrackInfo>,
        stream_id: &str,
        codec: webrtc::RtpCodecParameters,
        header_extensions: &[webrtc::RtpHeaderExtensionParameter],
    ) -> Arc<DummyReceiver> {
        Arc::new_cyclic(|this| DummyReceiver {
            receiver: OnceLock::new(),
            track_info,
            stream_id: stream_id.to_string(),
            codec,
            header_extensions: header_extensions.to_vec(),
            down_track_lock: Mutex::new(DummyReceiverDownTracks::default()),
            settings_lock: Mutex::new(DummyReceiverSettings::default()),
            this: this.clone(),
        })
    }

    pub fn receiver(&self) -> Option<Arc<dyn sfu::TrackReceiver>> {
        self.receiver.get().cloned()
    }

    pub fn upgrade(&self, receiver: Arc<dyn sfu::TrackReceiver>) {
        if self.receiver.set(receiver.clone()).is_err() {
            return;
        }

        let mut down_tracks = self.down_track_lock.lock();
        for t in down_tracks.down_tracks.values() {
            let _ = receiver.add_down_track(t.clone());
        }
        down_tracks.down_tracks = HashMap::new();

        let on_ready_callbacks = std::mem::take(&mut down_tracks.on_ready_callbacks);

        let codec_change = std::mem::take(&mut down_tracks.on_codec_state_change);
        drop(down_tracks);

        for f in on_ready_callbacks {
            receiver.add_on_ready(f);
        }

        for f in codec_change {
            receiver.add_on_codec_state_change(f);
        }

        let mut settings = self.settings_lock.lock();
        let max_expected_layer_valid = settings.max_expected_layer_valid;
        let max_expected_layer = settings.max_expected_layer;
        settings.max_expected_layer_valid = false;
        drop(settings);

        if max_expected_layer_valid {
            receiver.set_max_expected_spatial_layer(max_expected_layer);
        }

        let settings = self.settings_lock.lock();
        if let Some(primary_receiver) = settings.primary_receiver.upgrade() {
            primary_receiver.upgrade(&receiver);
        }
        if let Some(red_receiver) = settings.red_receiver.upgrade() {
            red_receiver.upgrade(&receiver);
        }
    }

    fn get_receiver(&self) -> Option<Arc<dyn sfu::TrackReceiver>> {
        self.receiver.get().cloned()
    }

    // The receiver as its RED receivers embed it.
    fn this(&self) -> Arc<DummyReceiver> {
        self.this
            .upgrade()
            .expect("a receiver that is called is held by somebody")
    }
}

impl sfu::TrackReceiver for DummyReceiver {
    fn track_id(&self) -> livekit::TrackId {
        livekit::TrackId::from(self.track_info.sid.as_str())
    }

    fn stream_id(&self) -> String {
        self.stream_id.clone()
    }

    fn codec(&self) -> webrtc::RtpCodecParameters {
        if let Some(receiver) = self.get_receiver() {
            return receiver.codec();
        }
        self.codec.clone()
    }

    fn mime(&self) -> mime::MimeType {
        if let Some(receiver) = self.get_receiver() {
            return receiver.mime();
        }
        mime::normalize_mime_type(&self.codec.mime_type)
    }

    fn video_layer_mode(&self) -> livekit::video_layer::Mode {
        if let Some(receiver) = self.get_receiver() {
            return receiver.video_layer_mode();
        }
        buffer::get_video_layer_mode_for_mime_type(self.mime(), Some(&*self.track_info()))
    }

    fn header_extensions(&self) -> Vec<webrtc::RtpHeaderExtensionParameter> {
        if let Some(receiver) = self.get_receiver() {
            return receiver.header_extensions();
        }
        self.header_extensions.clone()
    }

    fn read_rtp(&self, buf: &mut [u8], layer: u8, esn: u64) -> Result<usize, sfu::Error> {
        if let Some(receiver) = self.get_receiver() {
            return receiver.read_rtp(buf, layer, esn);
        }
        Err(no_receiver())
    }

    fn get_layered_bitrate(&self) -> (Vec<i32>, sfu::Bitrates) {
        if let Some(receiver) = self.get_receiver() {
            return receiver.get_layered_bitrate();
        }
        (Vec::new(), sfu::Bitrates::default())
    }

    fn get_audio_level(&self) -> (f64, bool) {
        if let Some(receiver) = self.get_receiver() {
            return receiver.get_audio_level();
        }
        (0.0, false)
    }

    fn send_pli(&self, layer: i32, force: bool) {
        if let Some(receiver) = self.get_receiver() {
            receiver.send_pli(layer, force);
        }
    }

    fn set_max_expected_spatial_layer(&self, layer: i32) {
        let mut settings = self.settings_lock.lock();
        let receiver = self.get_receiver();
        if receiver.is_some() {
            settings.max_expected_layer_valid = false;
        } else {
            settings.max_expected_layer_valid = true;
            settings.max_expected_layer = layer;
        }
        drop(settings);

        if let Some(receiver) = receiver {
            receiver.set_max_expected_spatial_layer(layer);
        }
    }

    fn add_down_track(&self, track: Arc<dyn sfu::TrackSender>) -> Result<(), sfu::Error> {
        let mut down_tracks = self.down_track_lock.lock();

        if let Some(receiver) = self.get_receiver() {
            let _ = receiver.add_down_track(track);
        } else {
            down_tracks.down_tracks.insert(track.subscriber_id(), track);
        }
        Ok(())
    }

    fn delete_down_track(&self, subscriber_id: &livekit::ParticipantId) {
        let mut down_tracks = self.down_track_lock.lock();

        if let Some(receiver) = self.get_receiver() {
            receiver.delete_down_track(subscriber_id);
        } else {
            down_tracks.down_tracks.remove(subscriber_id);
        }
    }

    fn get_down_tracks(&self) -> Vec<Arc<dyn sfu::TrackSender>> {
        let down_tracks = self.down_track_lock.lock();

        if let Some(receiver) = self.get_receiver() {
            return receiver.get_down_tracks();
        }
        down_tracks.down_tracks.values().cloned().collect()
    }

    fn debug_info(&self) -> sfu::DebugInfo {
        if let Some(receiver) = self.get_receiver() {
            return receiver.debug_info();
        }
        sfu::DebugInfo::new()
    }

    fn get_temporal_layer_fps_for_spatial(&self, spatial: i32) -> Vec<f32> {
        if let Some(receiver) = self.get_receiver() {
            return receiver.get_temporal_layer_fps_for_spatial(spatial);
        }
        Vec::new()
    }

    fn track_info(&self) -> Arc<livekit::TrackInfo> {
        if let Some(receiver) = self.get_receiver() {
            return receiver.track_info();
        }
        self.track_info.clone()
    }

    fn update_track_info(&self, ti: &livekit::TrackInfo) {
        if let Some(receiver) = self.get_receiver() {
            receiver.update_track_info(ti);
        }
    }

    fn is_closed(&self) -> bool {
        if let Some(receiver) = self.get_receiver() {
            return receiver.is_closed();
        }
        false
    }

    fn get_primary_receiver_for_red(&self) -> Arc<dyn sfu::TrackReceiver> {
        let mut settings = self.settings_lock.lock();

        if let Some(primary_receiver) = settings.primary_receiver.upgrade() {
            return primary_receiver;
        }
        let primary_receiver = DummyRedReceiver::new(self.this(), false);
        if let Some(r) = self.receiver.get() {
            primary_receiver.upgrade(r);
        }
        settings.primary_receiver = Arc::downgrade(&primary_receiver);
        primary_receiver
    }

    fn get_red_receiver(&self) -> Arc<dyn sfu::TrackReceiver> {
        let mut settings = self.settings_lock.lock();
        if let Some(red_receiver) = settings.red_receiver.upgrade() {
            return red_receiver;
        }
        let red_receiver = DummyRedReceiver::new(self.this(), true);
        if let Some(r) = self.receiver.get() {
            red_receiver.upgrade(r);
        }
        settings.red_receiver = Arc::downgrade(&red_receiver);
        red_receiver
    }

    fn get_track_stats(&self) -> Option<livekit::RtpStats> {
        if let Some(receiver) = self.get_receiver() {
            return receiver.get_track_stats();
        }
        None
    }

    fn add_on_ready(&self, f: Box<dyn Fn() + Send + Sync>) {
        let mut down_tracks = self.down_track_lock.lock();
        let Some(receiver) = self.get_receiver() else {
            down_tracks.on_ready_callbacks.push(f);
            return;
        };
        drop(down_tracks);
        receiver.add_on_ready(f);
    }

    fn add_on_codec_state_change(
        &self,
        f: Box<dyn Fn(webrtc::RtpCodecParameters, sfu::ReceiverCodecState) + Send + Sync>,
    ) {
        let mut down_tracks = self.down_track_lock.lock();
        let Some(receiver) = self.get_receiver() else {
            down_tracks.on_codec_state_change.push(f);
            return;
        };
        drop(down_tracks);
        receiver.add_on_codec_state_change(f);
    }

    fn codec_state(&self) -> sfu::ReceiverCodecState {
        if let Some(receiver) = self.get_receiver() {
            return receiver.codec_state();
        }
        sfu::ReceiverCodecState::Normal
    }

    fn video_sizes(&self) -> Vec<codec::VideoSize> {
        if let Some(receiver) = self.get_receiver() {
            return receiver.video_sizes();
        }

        Vec::new()
    }

    fn restart(&self, reason: &str) {
        if let Some(receiver) = self.get_receiver() {
            receiver.restart(reason);
        }
    }
}

// `errors.New("no receiver")` of the two `ReadRTP`: see the header.
fn no_receiver() -> sfu::Error {
    bucket::Error::PacketMismatch(String::new()).into()
}

// --------------------------------------------

pub struct DummyRedReceiver {
    // the `*DummyReceiver` that the original embeds
    dummy_receiver: Arc<DummyReceiver>,
    red_receiver: atomic::Value<Arc<dyn sfu::TrackReceiver>>, // sfu.TrackReceiver
    // indicates this receiver is for RED encoding receiver of primary codec OR
    // primary decoding receiver of RED codec
    is_red_encoding: bool,

    // what `downTrackLock sync.Mutex` guards upstream
    down_track_lock: Mutex<HashMap<livekit::ParticipantId, Arc<dyn sfu::TrackSender>>>,
}

impl DummyRedReceiver {
    pub fn new(d: Arc<DummyReceiver>, is_red_encoding: bool) -> Arc<DummyRedReceiver> {
        Arc::new(DummyRedReceiver {
            dummy_receiver: d,
            red_receiver: atomic::Value::new(),
            is_red_encoding,
            down_track_lock: Mutex::new(HashMap::new()),
        })
    }

    fn upgrade(&self, receiver: &Arc<dyn sfu::TrackReceiver>) {
        let red_receiver = if self.is_red_encoding {
            receiver.get_red_receiver()
        } else {
            receiver.get_primary_receiver_for_red()
        };
        self.red_receiver.store(red_receiver.clone());

        let mut down_tracks = self.down_track_lock.lock();
        for t in down_tracks.values() {
            let _ = red_receiver.add_down_track(t.clone());
        }
        *down_tracks = HashMap::new();
    }
}

impl sfu::TrackReceiver for DummyRedReceiver {
    fn track_id(&self) -> livekit::TrackId {
        self.dummy_receiver.track_id()
    }

    fn stream_id(&self) -> String {
        self.dummy_receiver.stream_id()
    }

    fn codec(&self) -> webrtc::RtpCodecParameters {
        self.dummy_receiver.codec()
    }

    fn mime(&self) -> mime::MimeType {
        self.dummy_receiver.mime()
    }

    fn video_layer_mode(&self) -> livekit::video_layer::Mode {
        self.dummy_receiver.video_layer_mode()
    }

    fn header_extensions(&self) -> Vec<webrtc::RtpHeaderExtensionParameter> {
        self.dummy_receiver.header_extensions()
    }

    fn is_closed(&self) -> bool {
        self.dummy_receiver.is_closed()
    }

    fn read_rtp(&self, buf: &mut [u8], layer: u8, esn: u64) -> Result<usize, sfu::Error> {
        if let Some(r) = self.red_receiver.load() {
            return r.read_rtp(buf, layer, esn);
        }
        Err(no_receiver())
    }

    fn get_layered_bitrate(&self) -> (Vec<i32>, sfu::Bitrates) {
        self.dummy_receiver.get_layered_bitrate()
    }

    fn get_audio_level(&self) -> (f64, bool) {
        self.dummy_receiver.get_audio_level()
    }

    fn send_pli(&self, layer: i32, force: bool) {
        self.dummy_receiver.send_pli(layer, force);
    }

    fn set_max_expected_spatial_layer(&self, layer: i32) {
        self.dummy_receiver.set_max_expected_spatial_layer(layer);
    }

    fn add_down_track(&self, track: Arc<dyn sfu::TrackSender>) -> Result<(), sfu::Error> {
        let mut down_tracks = self.down_track_lock.lock();

        if let Some(r) = self.red_receiver.load() {
            let _ = r.add_down_track(track);
        } else {
            down_tracks.insert(track.subscriber_id(), track);
        }
        Ok(())
    }

    fn delete_down_track(&self, subscriber_id: &livekit::ParticipantId) {
        let mut down_tracks = self.down_track_lock.lock();

        if let Some(r) = self.red_receiver.load() {
            r.delete_down_track(subscriber_id);
        } else {
            down_tracks.remove(subscriber_id);
        }
    }

    fn get_down_tracks(&self) -> Vec<Arc<dyn sfu::TrackSender>> {
        let down_tracks = self.down_track_lock.lock();

        if let Some(r) = self.red_receiver.load() {
            return r.get_down_tracks();
        }
        down_tracks.values().cloned().collect()
    }

    fn debug_info(&self) -> sfu::DebugInfo {
        self.dummy_receiver.debug_info()
    }

    fn track_info(&self) -> Arc<livekit::TrackInfo> {
        self.dummy_receiver.track_info()
    }

    fn update_track_info(&self, ti: &livekit::TrackInfo) {
        self.dummy_receiver.update_track_info(ti);
    }

    fn get_primary_receiver_for_red(&self) -> Arc<dyn sfu::TrackReceiver> {
        self.dummy_receiver.get_primary_receiver_for_red()
    }

    fn get_red_receiver(&self) -> Arc<dyn sfu::TrackReceiver> {
        self.dummy_receiver.get_red_receiver()
    }

    fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32> {
        self.dummy_receiver
            .get_temporal_layer_fps_for_spatial(layer)
    }

    fn get_track_stats(&self) -> Option<livekit::RtpStats> {
        self.dummy_receiver.get_track_stats()
    }

    fn add_on_ready(&self, f: Box<dyn Fn() + Send + Sync>) {
        self.dummy_receiver.add_on_ready(f);
    }

    fn add_on_codec_state_change(
        &self,
        f: Box<dyn Fn(webrtc::RtpCodecParameters, sfu::ReceiverCodecState) + Send + Sync>,
    ) {
        self.dummy_receiver.add_on_codec_state_change(f);
    }

    fn codec_state(&self) -> sfu::ReceiverCodecState {
        self.dummy_receiver.codec_state()
    }

    fn video_sizes(&self) -> Vec<codec::VideoSize> {
        self.dummy_receiver.video_sizes()
    }

    fn restart(&self, reason: &str) {
        self.dummy_receiver.restart(reason);
    }
}

// (Visible in `rtc`: the test files of the files that stand on this one share its helpers.)
#[cfg(test)]
#[path = "wrappedreceiver_test.rs"]
pub(super) mod wrappedreceiver_test;
