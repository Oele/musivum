// Ported from LiveKit, pkg/sfu/receiver.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `WebRTCReceiver` embeds `*ReceiverBase`. Here the base is the field `receiver_base`
//   (`receiver_base()` gives it, for what is not handed on), the methods of the base that
//   other packages call on a receiver are handed on, and `impl TrackReceiver` at the end of
//   the file is what the embedding made of the receiver: the base's methods, with the three
//   that the receiver has of its own (`update_track_info`, `set_max_expected_spatial_layer`,
//   `debug_info`). As upstream, what the base hands out as itself (to a RED receiver, from
//   `get_red_receiver` of a receiver that is RED, to the down tracks on a restart) is the
//   base and not this receiver.
// - The receiver is made as an `Arc` and keeps a `Weak` of itself: it is the base's listener
//   and `on_closed`, the provider of its connection statistics and what the callbacks of its
//   buffers and statistics capture. All of those hold it weakly, as it owns them.
// - `ReceiverOpts` is a boxed function of the receiver, as upstream, without the return
//   value (upstream every option returns the receiver it was given); `new` takes the options
//   as a `Vec`. The connection statistics are made before the options are applied, where the
//   original makes them after: the receiver cannot be made without them, and no option
//   touches them.
// - `new` takes the track by reference (it only reads it) and the RTP receiver as the
//   connection layer's trait. `receiver` is kept as upstream, where nothing reads it either.
// - `on_close_handler` and `on_stats_update`, plain fields upstream, are behind a lock each,
//   as their setters take `&self`; they are called with no lock held.
// - `on_rtcp` gets the packets as the buffer hands them over, a list of boxed packets, and an
//   empty list is what `nil` is upstream.
// - `get_delta_stats` gives the map by value, and `get_last_sender_report_time` the zero
//   `Time` where the original gives a zero `time.Time`, as the provider's trait has them.
// - `notify_max_expected_layer`: a track info is never nil here.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::go::time::Time;
use crate::mediatransportutil::codec;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::connectionquality;
use crate::sfu::{
    AudioConfig, Bitrates, DebugInfo, Error, ForwardStats, PliThrottleConfig, ReceiverBase,
    ReceiverBaseParams, ReceiverCodecState, StreamTrackerManager, StreamTrackerManagerConfig,
    StreamTrackerManagerListener, TrackReceiver, TrackRemote, TrackSender,
};
use crate::warnw;
use crate::webrtc;
use crate::webrtc::rtcp;

// var _ TrackReceiver = (*WebRTCReceiver)(nil): the implementation is at the end of this file

// `len(w.upTracks)` of the original
const NUM_SPATIAL_LAYERS: usize = (buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize;

type OnCloseHandler = Arc<dyn Fn() + Send + Sync>;
/// `onRTCP func([]rtcp.Packet)` of the original.
pub type OnRtcp = Box<dyn Fn(Vec<Box<dyn rtcp::Packet>>) + Send + Sync>;
type OnStatsUpdate = Arc<dyn Fn(&WebRtcReceiver, &livekit::AnalyticsStat) + Send + Sync>;

// WebRTCReceiver receives a media track
pub struct WebRtcReceiver {
    // `*ReceiverBase`
    receiver_base: Arc<ReceiverBase>,

    #[allow(dead_code)]
    receiver: Arc<dyn webrtc::RtpReceiver>,
    on_close_handler: Mutex<Option<OnCloseHandler>>,

    on_rtcp: Option<OnRtcp>,

    up_tracks_mu: Mutex<[Option<Arc<dyn TrackRemote>>; NUM_SPATIAL_LAYERS]>,

    connection_stats: Arc<connectionquality::ConnectionStats>,
    on_stats_update: Mutex<Option<OnStatsUpdate>>,

    // the receiver itself: see the header
    this: Weak<WebRtcReceiver>,
}

pub type ReceiverOpts = Box<dyn Fn(&WebRtcReceiver) + Send + Sync>;

// WithPliThrottleConfig indicates minimum time(ms) between sending PLIs
pub fn with_pli_throttle_config(pli_throttle_config: PliThrottleConfig) -> ReceiverOpts {
    Box::new(move |w| {
        w.receiver_base.set_pli_throttle_config(pli_throttle_config);
    })
}

// WithAudioConfig sets up parameters for active speaker detection
pub fn with_audio_config(audio_config: AudioConfig) -> ReceiverOpts {
    Box::new(move |w| {
        w.receiver_base.set_audio_config(audio_config);
    })
}

pub fn with_enable_rtp_stream_restart_detection(enable: bool) -> ReceiverOpts {
    Box::new(move |w| {
        w.receiver_base
            .set_enable_rtp_stream_restart_detection(enable);
    })
}

// WithLoadBalanceThreshold enables parallelization of packet writes when downTracks exceeds threshold
// Value should be between 3 and 150.
// For a server handling a few large rooms, use a smaller value (required to handle very large (250+ participant) rooms).
// For a server handling many small rooms, use a larger value or disable.
// Set to 0 (disabled) by default.
pub fn with_load_balance_threshold(down_tracks: usize) -> ReceiverOpts {
    Box::new(move |w| {
        w.receiver_base.set_lb_threshold(down_tracks);
    })
}

pub fn with_forward_stats(forward_stats: Option<Arc<ForwardStats>>) -> ReceiverOpts {
    Box::new(move |w| {
        w.receiver_base.set_forward_stats(forward_stats.clone());
    })
}

impl WebRtcReceiver {
    // NewWebRTCReceiver creates a new webrtc track receiver
    pub fn new(
        receiver: Arc<dyn webrtc::RtpReceiver>,
        track: &dyn TrackRemote,
        track_info: &livekit::TrackInfo,
        logger: Logger,
        on_rtcp: Option<OnRtcp>,
        stream_tracker_manager_config: StreamTrackerManagerConfig,
        opts: Vec<ReceiverOpts>,
    ) -> Arc<WebRtcReceiver> {
        let w = Arc::new_cyclic(|this: &Weak<WebRtcReceiver>| {
            let listener: Weak<dyn StreamTrackerManagerListener> = this.clone();
            let on_closed = {
                let this = this.clone();
                move || {
                    if let Some(w) = this.upgrade() {
                        w.on_closed();
                    }
                }
            };
            let receiver_base = ReceiverBase::new(
                ReceiverBaseParams {
                    track_id: livekit::TrackId::from(track.id()),
                    stream_id: track.stream_id(),
                    kind: track.kind(),
                    codec: track.codec(),
                    header_extensions: receiver.get_parameters().header_extensions,
                    logger: logger.clone(),
                    stream_tracker_manager_config,
                    stream_tracker_manager_listener: Some(listener),
                    is_self_closing: true,
                    on_new_buffer_needed: None,
                    on_closed: Some(Box::new(on_closed)),
                },
                track_info,
                ReceiverCodecState::Normal,
            );

            let receiver_provider: Weak<dyn connectionquality::ConnectionStatsReceiverProvider> =
                this.clone();
            let connection_stats =
                connectionquality::ConnectionStats::new(connectionquality::ConnectionStatsParams {
                    receiver_provider: Some(receiver_provider),
                    logger: logger.with_values(&[("direction", &"up")]),
                    ..Default::default()
                });

            WebRtcReceiver {
                receiver_base,
                receiver,
                on_close_handler: Mutex::new(None),
                on_rtcp,
                up_tracks_mu: Mutex::new(Default::default()),
                connection_stats,
                on_stats_update: Mutex::new(None),
                this: this.clone(),
            }
        });

        for opt in &opts {
            opt(&w);
        }

        {
            let this = w.this.clone();
            w.connection_stats.on_stats_update(
                move |_cs: &connectionquality::ConnectionStats, stat: &livekit::AnalyticsStat| {
                    let Some(w) = this.upgrade() else {
                        return;
                    };
                    let on_stats_update = w.on_stats_update.lock().clone();
                    if let Some(on_stats_update) = on_stats_update {
                        on_stats_update(&w, stat);
                    }
                },
            );
        }
        let codec = track.codec();
        w.connection_stats.start(
            mime::normalize_mime_type(&codec.mime_type),
            // TODO: technically not correct to declare FEC on when RED. Need the primary codec's fmtp line to check.
            mime::is_mime_type_string_red(&codec.mime_type)
                || codec
                    .sdp_fmtp_line
                    .to_lowercase()
                    .contains("useinbandfec=1"),
        );

        w.update_track_info(track_info);

        w
    }

    /// The `*ReceiverBase` that the original embeds.
    pub fn receiver_base(&self) -> &Arc<ReceiverBase> {
        &self.receiver_base
    }

    pub fn on_stats_update(
        &self,
        f: impl Fn(&WebRtcReceiver, &livekit::AnalyticsStat) + Send + Sync + 'static,
    ) {
        *self.on_stats_update.lock() = Some(Arc::new(f));
    }

    pub fn get_connection_score_and_quality(&self) -> (f32, livekit::ConnectionQuality) {
        self.connection_stats.get_score_and_quality()
    }

    fn ssrc(&self, layer: usize) -> u32 {
        let up_tracks = self.up_tracks_mu.lock();

        if let Some(track) = &up_tracks[layer] {
            return track.ssrc();
        }
        0
    }

    pub fn add_up_track(
        &self,
        track: Arc<dyn TrackRemote>,
        buff: Arc<buffer::Buffer>,
    ) -> Result<(), Error> {
        if self.receiver_base.is_closed() {
            return Err(Error::ReceiverClosed);
        }

        let mut layer: i32 = 0;
        if self.receiver_base.kind() == webrtc::RtpCodecType::Video
            && self.receiver_base.video_layer_mode()
                != livekit::video_layer::Mode::MultipleSpatialLayersPerStream
        {
            layer = buffer::get_spatial_layer_for_rid(
                self.receiver_base.mime(),
                &track.rid(),
                Some(&self.receiver_base.track_info()),
            );
        }
        if layer < 0 {
            warnw!(
                self.receiver_base.logger(),
                "invalid layer", nil,
                "rid" => track.rid(),
                "trackInfo" => self.receiver_base.track_info(),
            );
            return Err(Error::InvalidLayer);
        }

        {
            let mut up_tracks = self.up_tracks_mu.lock();
            if up_tracks[layer as usize].is_some() {
                return Err(Error::DuplicateLayer);
            }
            up_tracks[layer as usize] = Some(track);
        }

        self.receiver_base.add_buffer(buff.clone(), layer);
        {
            let this = self.this.clone();
            buff.on_rtcp_feedback(move |packets: Vec<Box<dyn rtcp::Packet>>| {
                if let Some(w) = this.upgrade() {
                    w.send_rtcp(packets);
                }
            });
        }
        self.receiver_base.start_buffer(buff, layer);
        Ok(())
    }

    pub fn num_up_tracks(&self) -> usize {
        let mut num_up_tracks = 0;

        let up_tracks = self.up_tracks_mu.lock();
        for track in up_tracks.iter() {
            if track.is_some() {
                num_up_tracks += 1;
            }
        }
        drop(up_tracks);

        num_up_tracks
    }

    pub fn update_track_info(&self, ti: &livekit::TrackInfo) {
        self.receiver_base.update_track_info(ti);
        self.connection_stats.update_mute(ti.muted);
    }

    fn notify_max_expected_layer(&self, layer: i32) {
        let ti = self.receiver_base.track_info();

        if self.receiver_base.kind() == webrtc::RtpCodecType::Audio
            || ti.source() == livekit::TrackSource::ScreenShare
        {
            // screen share tracks have highly variable bitrate, do not use bit rate based quality for those
            return;
        }

        let mut expected_bitrate: i64 = 0;
        for vl in buffer::get_video_layers_for_mime_type(self.receiver_base.mime(), Some(&ti)) {
            if vl.spatial_layer <= layer {
                expected_bitrate += vl.bitrate as i64;
            }
        }

        self.connection_stats
            .add_bitrate_transition(expected_bitrate);
    }

    pub fn set_max_expected_spatial_layer(&self, layer: i32) {
        self.receiver_base.set_max_expected_spatial_layer(layer);

        self.notify_max_expected_layer(layer);

        if layer == buffer::INVALID_LAYER_SPATIAL {
            self.connection_stats.update_layer_mute(true);
        } else {
            self.connection_stats.update_layer_mute(false);
            self.connection_stats.add_layer_transition(
                self.receiver_base
                    .stream_tracker_manager()
                    .distance_to_desired(),
            );
        }
    }

    // StreamTrackerManagerListener.OnAvailableLayersChanged
    pub fn on_available_layers_changed(&self) {
        self.connection_stats.add_layer_transition(
            self.receiver_base
                .stream_tracker_manager()
                .distance_to_desired(),
        );
    }

    // StreamTrackerManagerListener.OnBitrateAvailabilityChanged
    pub fn on_bitrate_availability_changed(&self) {}

    // StreamTrackerManagerListener.OnMaxPublishedLayerChanged
    pub fn on_max_published_layer_changed(&self, max_published_layer: i32) {
        self.notify_max_expected_layer(max_published_layer);
        self.connection_stats.add_layer_transition(
            self.receiver_base
                .stream_tracker_manager()
                .distance_to_desired(),
        );
    }

    // StreamTrackerManagerListener.OnMaxTemporalLayerSeenChanged
    pub fn on_max_temporal_layer_seen_changed(&self, _max_temporal_layer_seen: i32) {
        self.connection_stats.add_layer_transition(
            self.receiver_base
                .stream_tracker_manager()
                .distance_to_desired(),
        );
    }

    // StreamTrackerManagerListener.OnMaxAvailableLayerChanged
    pub fn on_max_available_layer_changed(&self, _max_available_layer: i32) {}

    // StreamTrackerManagerListener.OnBitrateReport
    pub fn on_bitrate_report(&self, _available_layers: &[i32], _bitrates: Bitrates) {
        self.connection_stats.add_layer_transition(
            self.receiver_base
                .stream_tracker_manager()
                .distance_to_desired(),
        );
    }

    // OnCloseHandler method to be called on remote track removed
    pub fn on_close_handler(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.on_close_handler.lock() = Some(Arc::new(f));
    }

    fn send_rtcp(&self, packets: Vec<Box<dyn rtcp::Packet>>) {
        if packets.is_empty() || self.receiver_base.is_closed() {
            return;
        }

        if let Some(on_rtcp) = &self.on_rtcp {
            on_rtcp(packets);
        }
    }

    pub fn get_delta_stats(&self) -> HashMap<u32, buffer::StreamStatsWithLayers> {
        let buffers = self.receiver_base.get_all_buffers();
        let mut delta_stats = HashMap::with_capacity(buffers.len());
        for (layer, buff) in buffers.iter().enumerate() {
            let Some(buff) = buff else {
                continue;
            };

            let Some(mut sswl) = buff.get_delta_stats() else {
                continue;
            };

            // patch buffer stats with correct layer
            let mut patched = HashMap::with_capacity(1);
            patched.insert(layer as i32, sswl.layers.get(&0).cloned().flatten());
            sswl.layers = patched;

            delta_stats.insert(self.ssrc(layer), sswl);
        }

        delta_stats
    }

    pub fn get_last_sender_report_time(&self) -> Time {
        let buffers = self.receiver_base.get_all_buffers();
        let mut latest_sr_time = Time::ZERO;
        for buff in buffers.iter().flatten() {
            let sr_at = buff.get_last_sender_report_time();
            if sr_at.after(latest_sr_time) {
                latest_sr_time = sr_at;
            }
        }

        latest_sr_time
    }

    fn on_closed(&self) {
        self.connection_stats.close();

        let on_close_handler = self.on_close_handler.lock().clone();
        if let Some(on_close_handler) = on_close_handler {
            on_close_handler();
        }
    }

    pub fn debug_info(&self) -> DebugInfo {
        let mut info = self.receiver_base.debug_info();

        let up_tracks = self.up_tracks_mu.lock();
        let mut up_track_info = Vec::with_capacity(up_tracks.len());
        for (layer, ut) in up_tracks.iter().enumerate() {
            if let Some(ut) = ut {
                up_track_info.push(serde_json::json!({
                    "Layer": layer,
                    "SSRC": ut.ssrc(),
                    "Msid": ut.msid(),
                    "RID": ut.rid(),
                }));
            }
        }
        drop(up_tracks);
        info.insert("UpTracks".to_string(), up_track_info.into());

        info
    }
}

// -----------------------------------------------------------

// The methods of `*ReceiverBase` that other packages call on a receiver, which the original
// has by embedding. The rest is reached through `receiver_base()`.

impl WebRtcReceiver {
    pub fn close(&self, reason: &str, clear_buffers: bool) {
        self.receiver_base.close(reason, clear_buffers)
    }

    pub fn can_close(&self) -> bool {
        self.receiver_base.can_close()
    }

    pub fn logger(&self) -> &Logger {
        self.receiver_base.logger()
    }

    pub fn on_max_layer_change(&self, f: impl Fn(mime::MimeType, i32) + Send + Sync + 'static) {
        self.receiver_base.on_max_layer_change(f)
    }

    pub fn set_rtt(&self, rtt: u32) {
        self.receiver_base.set_rtt(rtt)
    }

    pub fn kind(&self) -> webrtc::RtpCodecType {
        self.receiver_base.kind()
    }

    pub fn stream_tracker_manager(&self) -> &Arc<StreamTrackerManager> {
        self.receiver_base.stream_tracker_manager()
    }

    pub fn set_codec_state(&self, state: ReceiverCodecState) {
        self.receiver_base.set_codec_state(state)
    }

    pub fn set_codec_with_state(
        &self,
        codec: &webrtc::RtpCodecParameters,
        header_extensions: &[webrtc::RtpHeaderExtensionParameter],
        codec_state: ReceiverCodecState,
    ) -> bool {
        self.receiver_base
            .set_codec_with_state(codec, header_extensions, codec_state)
    }

    pub fn on_video_size_changed(&self, f: impl Fn() + Send + Sync + 'static) {
        self.receiver_base.on_video_size_changed(f)
    }
}

impl connectionquality::ConnectionStatsReceiverProvider for WebRtcReceiver {
    fn get_delta_stats(&self) -> HashMap<u32, buffer::StreamStatsWithLayers> {
        WebRtcReceiver::get_delta_stats(self)
    }

    fn get_last_sender_report_time(&self) -> Time {
        WebRtcReceiver::get_last_sender_report_time(self)
    }
}

// StreamTrackerManagerListener: the methods of the original that carry its name

impl StreamTrackerManagerListener for WebRtcReceiver {
    fn on_available_layers_changed(&self) {
        WebRtcReceiver::on_available_layers_changed(self)
    }

    fn on_bitrate_availability_changed(&self) {
        WebRtcReceiver::on_bitrate_availability_changed(self)
    }

    fn on_max_published_layer_changed(&self, max_published_layer: i32) {
        WebRtcReceiver::on_max_published_layer_changed(self, max_published_layer)
    }

    fn on_max_temporal_layer_seen_changed(&self, max_temporal_layer_seen: i32) {
        WebRtcReceiver::on_max_temporal_layer_seen_changed(self, max_temporal_layer_seen)
    }

    fn on_max_available_layer_changed(&self, max_available_layer: i32) {
        WebRtcReceiver::on_max_available_layer_changed(self, max_available_layer)
    }

    fn on_bitrate_report(&self, available_layers: &[i32], bitrates: Bitrates) {
        WebRtcReceiver::on_bitrate_report(self, available_layers, bitrates)
    }
}

// TrackReceiver: what the receiver has from the base it embeds upstream, and its own
// `update_track_info`, `set_max_expected_spatial_layer` and `debug_info`

impl TrackReceiver for WebRtcReceiver {
    fn track_id(&self) -> livekit::TrackId {
        self.receiver_base.track_id()
    }

    fn stream_id(&self) -> String {
        self.receiver_base.stream_id()
    }

    fn codec(&self) -> webrtc::RtpCodecParameters {
        self.receiver_base.codec()
    }

    fn mime(&self) -> mime::MimeType {
        self.receiver_base.mime()
    }

    fn video_layer_mode(&self) -> livekit::video_layer::Mode {
        self.receiver_base.video_layer_mode()
    }

    fn header_extensions(&self) -> Vec<webrtc::RtpHeaderExtensionParameter> {
        self.receiver_base.header_extensions()
    }

    fn is_closed(&self) -> bool {
        self.receiver_base.is_closed()
    }

    fn read_rtp(&self, buf: &mut [u8], layer: u8, esn: u64) -> Result<usize, Error> {
        self.receiver_base.read_rtp(buf, layer, esn)
    }

    fn get_layered_bitrate(&self) -> (Vec<i32>, Bitrates) {
        self.receiver_base.get_layered_bitrate()
    }

    fn get_audio_level(&self) -> (f64, bool) {
        self.receiver_base.get_audio_level()
    }

    fn send_pli(&self, layer: i32, force: bool) {
        self.receiver_base.send_pli(layer, force)
    }

    fn set_max_expected_spatial_layer(&self, layer: i32) {
        WebRtcReceiver::set_max_expected_spatial_layer(self, layer)
    }

    fn add_down_track(&self, track: Arc<dyn TrackSender>) -> Result<(), Error> {
        self.receiver_base.add_down_track(track)
    }

    fn delete_down_track(&self, participant_id: &livekit::ParticipantId) {
        self.receiver_base.delete_down_track(participant_id)
    }

    fn get_down_tracks(&self) -> Vec<Arc<dyn TrackSender>> {
        self.receiver_base.get_down_tracks()
    }

    fn debug_info(&self) -> DebugInfo {
        WebRtcReceiver::debug_info(self)
    }

    fn track_info(&self) -> Arc<livekit::TrackInfo> {
        self.receiver_base.track_info()
    }

    fn update_track_info(&self, ti: &livekit::TrackInfo) {
        WebRtcReceiver::update_track_info(self, ti)
    }

    fn get_primary_receiver_for_red(&self) -> Arc<dyn TrackReceiver> {
        self.receiver_base.get_primary_receiver_for_red()
    }

    fn get_red_receiver(&self) -> Arc<dyn TrackReceiver> {
        self.receiver_base.get_red_receiver()
    }

    fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32> {
        self.receiver_base.get_temporal_layer_fps_for_spatial(layer)
    }

    fn get_track_stats(&self) -> Option<livekit::RtpStats> {
        self.receiver_base.get_track_stats()
    }

    fn add_on_ready(&self, f: Box<dyn Fn() + Send + Sync>) {
        self.receiver_base.add_on_ready(f)
    }

    fn add_on_codec_state_change(
        &self,
        f: Box<dyn Fn(webrtc::RtpCodecParameters, ReceiverCodecState) + Send + Sync>,
    ) {
        self.receiver_base.add_on_codec_state_change(f)
    }

    fn codec_state(&self) -> ReceiverCodecState {
        self.receiver_base.codec_state()
    }

    fn video_sizes(&self) -> Vec<codec::VideoSize> {
        self.receiver_base.video_sizes()
    }

    fn restart(&self, reason: &str) {
        self.receiver_base.restart(reason)
    }
}

#[cfg(test)]
#[path = "receiver_test.rs"]
mod receiver_test;
