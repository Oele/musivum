// Ported from LiveKit, pkg/rtc/subscribedtrack.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `NewSubscribedTrack` is `SubscribedTrack::new` and gives an `Arc`; the track keeps a
//   `Weak` of itself for the debouncer and the tasks it starts. Its methods are inherent
//   ones, in the original's order; the two traits it implements (`types::SubscribedTrack`,
//   `sfu::DownTrackListener`) pass on to them at the end of the file.
// - Who holds whom. The subscribed track holds its down track, its subscriber, the media
//   track, the telemetry listener and the wrapped receiver, as upstream: the subscriber and
//   the media track have to be there for as long as the subscription is (the subscriber's
//   subscription manager reaches the media track through it to unsubscribe). The
//   subscriber's subscription manager and the media track's subscriptions hold the
//   subscribed track, so these are cycles: they end when the down track closes, which takes
//   the track out of the media track's subscriptions, and when the subscription manager
//   lets go of it, as upstream. The down track holds the subscribed track weakly (it is its
//   listener), and what this file hands to the down track to keep (the RTCP writer, the
//   receiver report listener) holds the subscriber weakly: the down track is also held by
//   the publisher's receiver and by the subscriber's connection.
// - The RTCP writer the down track is given returns the WebRTC library's error: the
//   package's error of `write_subscriber_rtcp` is passed on as it is when it wraps one,
//   as its text otherwise; a subscriber that is gone gives `ClosedPipe`.
// - The down track is made after the subscribed track, which is its listener: it is in a
//   `OnceLock`, set before `new` returns.
// - `for _, c := range codecs { c.RTCPFeedback = rtcpFeedback }` of the original writes to
//   a copy of each codec and changes nothing; it is a comment here.
// - `applySettings` reads the settings without the lock between its two locked parts
//   upstream. Here it takes a copy of them in the first part, computes the layers from the
//   copy, and reads `disabled` again in the second.
// - `onClose` and `onSubscribeStreamStarted` (`atomic.Value` upstream) are behind a lock
//   each. `on_close(None)` takes the callback away; upstream a nil function would be stored
//   and called.
// - The reporter (`roomobs`) is left out: the field, and in `on_stats_update` everything
//   after the call on the telemetry listener, which only feeds it.
// - A callback that is given an identifier gets it by reference; one that is shared with
//   the media track's subscriptions is an `Arc`.

use std::sync::{Arc, OnceLock, Weak};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use super::{
    ClientInfo, DirectionConfig, Error, ReceiverConfig, WrappedReceiver, logger_with_track,
    pack_sync_stream_id,
};
use crate::debugw;
use crate::go::atomic;
use crate::go::time::{self, Duration};
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils;
use crate::rtc::types;
use crate::sfu;
use crate::sfu::TrackReceiver as _;
use crate::sfu::buffer;
use crate::sfu::utils::Debouncer;
use crate::telemetry;
use crate::utils as sutils;
use crate::warnw;
use crate::webrtc;

const SUBSCRIPTION_DEBOUNCE_INTERVAL: Duration = Duration::from_millis(100);

// var _ types.SubscribedTrack = (*SubscribedTrack)(nil): the implementation is at the end of this file

pub type OnDownTrackCreated = Arc<dyn Fn(Arc<sfu::DownTrack>) + Send + Sync>;
pub type OnSubscriberMaxQualityChange =
    Arc<dyn Fn(&livekit::ParticipantId, mime::MimeType, i32) + Send + Sync>;
pub type OnSubscriberAudioCodecChange =
    Arc<dyn Fn(&livekit::ParticipantId, mime::MimeType, bool) + Send + Sync>;

pub struct SubscribedTrackParams {
    pub receiver_config: ReceiverConfig,
    pub subscriber_config: DirectionConfig,
    pub subscriber: Arc<dyn types::LocalParticipant>,
    pub media_track: Arc<dyn types::MediaTrack>,
    pub adaptive_stream: bool,
    pub enable_start_at_desired_quality: bool,
    pub telemetry_listener: Arc<dyn types::ParticipantTelemetryListener>,
    pub wrapped_receiver: Arc<WrappedReceiver>,
    pub is_relayed: bool,
    pub on_down_track_created: Option<OnDownTrackCreated>,
    #[allow(clippy::type_complexity)]
    pub on_down_track_closed: Option<Box<dyn Fn(&livekit::ParticipantId) + Send + Sync>>,
    pub on_subscriber_max_quality_change: Option<OnSubscriberMaxQualityChange>,
    pub on_subscriber_audio_codec_change: Option<OnSubscriberAudioCodecChange>,
}

pub struct SubscribedTrack {
    params: SubscribedTrackParams,
    logger: Logger,
    down_track: OnceLock<Arc<sfu::DownTrack>>,
    sender: RwLock<Option<Arc<dyn webrtc::RtpSender>>>,
    needs_negotiation: atomic::Bool,

    version_generator: Arc<dyn utils::TimedVersionGenerator>,
    // what `settingsLock sync.Mutex` guards upstream
    settings_lock: Mutex<SubscribedTrackSettings>,

    // what `bindLock sync.Mutex` guards upstream
    bind_lock: Mutex<SubscribedTrackBind>,

    #[allow(clippy::type_complexity)]
    on_subscribe_stream_started: RwLock<Option<Arc<dyn Fn(Duration) + Send + Sync>>>, //func(time.Duration)
    #[allow(clippy::type_complexity)]
    on_close: RwLock<Option<Arc<dyn Fn(bool) + Send + Sync>>>, // func(bool)

    debouncer: Debouncer,

    stats_key: telemetry::StatsKey,
    // Not here: reporter roomobs.TrackReporter, of LiveKit's observability, which is not
    // ported.

    // the track itself, for the debouncer and the tasks it starts
    this: Weak<SubscribedTrack>,
}

#[derive(Default)]
struct SubscribedTrackSettings {
    settings: Option<livekit::UpdateTrackSettings>,
    settings_version: utils::TimedVersion,
}

#[derive(Default)]
struct SubscribedTrackBind {
    bound: bool,
    #[allow(clippy::type_complexity)]
    on_bind_callbacks: Vec<Box<dyn Fn(Result<(), Error>) + Send + Sync>>,
}

impl SubscribedTrack {
    pub fn new(params: SubscribedTrackParams) -> Result<Arc<SubscribedTrack>, Error> {
        let s = Arc::new_cyclic(|this| SubscribedTrack {
            logger: params
                .subscriber
                .get_logger()
                .with_component(sutils::COMPONENT_SUB)
                .with_values(&[
                    ("trackID", &params.media_track.id()),
                    ("publisherID", &params.media_track.publisher_id()),
                    ("publisher", &params.media_track.publisher_identity()),
                ]),
            down_track: OnceLock::new(),
            sender: RwLock::new(None),
            needs_negotiation: atomic::Bool::default(),
            version_generator: utils::new_default_timed_version_generator(),
            settings_lock: Mutex::new(SubscribedTrackSettings::default()),
            bind_lock: Mutex::new(SubscribedTrackBind::default()),
            on_subscribe_stream_started: RwLock::new(None),
            on_close: RwLock::new(None),
            debouncer: Debouncer::new(SUBSCRIPTION_DEBOUNCE_INTERVAL),
            stats_key: telemetry::stats_key_for_track(
                &params.subscriber.get_country(),
                livekit::StreamType::Downstream,
                params.subscriber.id(),
                params.media_track.id(),
                params.media_track.source(),
                params.media_track.kind(),
            ),
            this: this.clone(),
            params,
        });
        let params = &s.params;

        let mut max_track: usize = 0;
        match params.media_track.kind() {
            livekit::TrackType::Audio => {
                max_track = params.receiver_config.packet_buffer_size_audio;
            }
            livekit::TrackType::Video => {
                max_track = params.receiver_config.packet_buffer_size_video;
            }
            kind => {
                warnw!(s.logger, "unexpected track type", nil, "kind" => kind);
            }
        }
        let codecs = params.wrapped_receiver.codecs();
        // The original takes the feedback of the track's kind from the subscriber's
        // configuration (`params.SubscriberConfig.RTCPFeedback`) in the switch above and
        // assigns it here, to a copy of each codec (`for _, c := range codecs`), which
        // changes nothing: the codecs go to the down track as the wrapped receiver has them.

        let mut stream_id = params.wrapped_receiver.stream_id();
        if params.subscriber.supports_sync_stream_id() && !params.media_track.stream().is_empty() {
            stream_id = pack_sync_stream_id(
                &params.media_track.publisher_id(),
                &params.media_track.stream(),
            );
        }

        let is_encrypted = params.media_track.is_encrypted();
        let mut trailer = Vec::new();
        if is_encrypted {
            trailer = params.subscriber.get_trailer();
        }
        let sub_client_info = ClientInfo {
            client_info: params.subscriber.get_client_info(),
        };
        let sub_supports_packet_trailer = sub_client_info.supports_packet_trailer();
        // Strip packet trailer if track has packet trailer but subscriber does not have cap
        let strip_packet_trailer =
            params.media_track.has_packet_trailer() && !sub_supports_packet_trailer;
        let listener: Weak<SubscribedTrack> = s.this.clone();
        let rtcp_writer_subscriber = Arc::downgrade(&params.subscriber);
        let down_track = sfu::DownTrack::new(sfu::DownTrackParams {
            codecs,
            is_encrypted,
            source: params.media_track.source(),
            receiver: params.wrapped_receiver.clone(),
            buffer_factory: params.subscriber.get_buffer_factory(),
            sub_id: params.subscriber.id(),
            stream_id,
            max_track,
            playout_delay_limit: params.subscriber.get_playout_delay_config(),
            pacer: params.subscriber.get_pacer(),
            trailer: Bytes::from(trailer),
            strip_packet_trailer,
            logger: logger_with_track(
                &params
                    .subscriber
                    .get_logger()
                    .with_component(sutils::COMPONENT_SUB),
                &params.media_track.id(),
                params.is_relayed,
            ),
            rtcp_writer: Some(Box::new(move |pkts| {
                let Some(subscriber) = rtcp_writer_subscriber.upgrade() else {
                    return Err(webrtc::Error::ClosedPipe);
                };
                subscriber
                    .write_subscriber_rtcp(pkts)
                    .map_err(|err| match err {
                        Error::Webrtc(err) => err,
                        err => webrtc::Error::Other(err.to_string()),
                    })
            })),
            disable_sender_report_pass_through: params
                .subscriber
                .get_disable_sender_report_pass_through(),
            supports_codec_change: params.subscriber.supports_codec_change(),
            enable_start_at_desired_quality: params.enable_start_at_desired_quality,
            rtx_budget: params.subscriber_config.rtx_budget,
            listener,
        })?;

        if let Some(on_down_track_created) = &params.on_down_track_created {
            on_down_track_created(down_track.clone());
        }

        let subscriber = Arc::downgrade(&params.subscriber);
        down_track.add_receiver_report_listener(move |dt, report| {
            if let Some(subscriber) = subscriber.upgrade() {
                subscriber.handle_receiver_report(dt, report);
            }
        });

        let _ = s.down_track.set(down_track);
        Ok(s)
    }

    pub fn add_on_bind(&self, f: Box<dyn Fn(Result<(), Error>) + Send + Sync>) {
        let mut bind = self.bind_lock.lock();
        let bound = bind.bound;
        if !bound {
            bind.on_bind_callbacks.push(f);
            return;
        }
        drop(bind);

        // fire immediately, do not need to persist since bind is a one time event
        time::spawn(async move {
            f(Ok(()));
        });
    }

    // for DownTrack callback to notify us that it's bound
    pub fn bound(&self, err: Result<(), Error>) {
        let mut bind = self.bind_lock.lock();
        if err.is_ok() {
            bind.bound = true;
        }
        let callbacks = std::mem::take(&mut bind.on_bind_callbacks);
        drop(bind);

        if err.is_ok() && self.params.media_track.kind() == livekit::TrackType::Video {
            // When AdaptiveStream is enabled, default the subscriber to LOW quality stream
            // we would want LOW instead of OFF for a couple of reasons
            // 1. when a subscriber unsubscribes from a track, we would forget their previously defined settings
            //    depending on client implementation, subscription on/off is kept separately from adaptive stream
            //    So when there are no changes to desired resolution, but the user re-subscribes, we may leave stream at OFF
            // 2. when interacting with dynacast *and* adaptive stream. If the publisher was not publishing at the
            //    time of subscription, we might not be able to trigger adaptive stream updates on the client side
            //    (since there isn't any video frames coming through). this will leave the stream "stuck" on off, without
            //    a trigger to re-enable it
            let mut s = self.settings_lock.lock();
            if let Some(settings) = &mut s.settings {
                if self.params.adaptive_stream {
                    // remove `disabled` flag to force a visibility update
                    settings.disabled = false;
                    debugw!(self.logger, "enabling subscriber track settings on bind", "settings" => settings);
                }
            } else {
                let settings = if self.params.enable_start_at_desired_quality {
                    // default to HIGH quality so the subscriber acquires the top layer directly instead of
                    // ramping up from a lower layer. adaptive stream clients can still scale down afterwards
                    // based on viewport.
                    livekit::UpdateTrackSettings {
                        quality: livekit::VideoQuality::High as i32,
                        ..Default::default()
                    }
                } else if self.params.adaptive_stream {
                    livekit::UpdateTrackSettings {
                        quality: livekit::VideoQuality::Low as i32,
                        ..Default::default()
                    }
                } else {
                    livekit::UpdateTrackSettings {
                        quality: livekit::VideoQuality::High as i32,
                        ..Default::default()
                    }
                };
                debugw!(self.logger, "initializing subscriber track settings on bind", "settings" => settings);
                s.settings = Some(settings);
            }
            drop(s);
            self.apply_settings();
        }

        for cb in callbacks {
            let err = err.clone();
            time::spawn(async move {
                cb(err);
            });
        }
    }

    // for DownTrack callback to notify us that it's closed
    pub fn close(&self, is_expected_to_resume: bool) {
        let on_close = self.on_close.read().clone();
        if let Some(on_close) = on_close {
            on_close(is_expected_to_resume);
        }
    }

    pub fn on_subscribe_stream_started(&self, f: Box<dyn Fn(Duration) + Send + Sync>) {
        *self.on_subscribe_stream_started.write() = Some(Arc::from(f));
    }

    pub fn on_close(&self, f: Option<Box<dyn Fn(bool) + Send + Sync>>) {
        *self.on_close.write() = f.map(Arc::from);
    }

    pub fn is_bound(&self) -> bool {
        let bind = self.bind_lock.lock();

        bind.bound
    }

    pub fn id(&self) -> livekit::TrackId {
        livekit::TrackId::from(self.dt().id())
    }

    pub fn publisher_id(&self) -> livekit::ParticipantId {
        self.params.media_track.publisher_id()
    }

    pub fn publisher_identity(&self) -> livekit::ParticipantIdentity {
        self.params.media_track.publisher_identity()
    }

    pub fn publisher_version(&self) -> u32 {
        self.params.media_track.publisher_version()
    }

    pub fn subscriber_id(&self) -> livekit::ParticipantId {
        self.params.subscriber.id()
    }

    pub fn subscriber_identity(&self) -> livekit::ParticipantIdentity {
        self.params.subscriber.identity()
    }

    pub fn subscriber(&self) -> Arc<dyn types::LocalParticipant> {
        self.params.subscriber.clone()
    }

    pub fn down_track(&self) -> Option<Arc<sfu::DownTrack>> {
        self.down_track.get().cloned()
    }

    pub fn media_track(&self) -> Option<Arc<dyn types::MediaTrack>> {
        Some(self.params.media_track.clone())
    }

    // has subscriber indicated it wants to mute this track
    pub fn is_muted(&self) -> bool {
        let s = self.settings_lock.lock();

        s.is_muted_locked()
    }

    pub fn set_publisher_muted(&self, muted: bool) {
        self.dt().pub_mute(muted);
    }

    pub fn update_subscriber_settings(
        &self,
        settings: &livekit::UpdateTrackSettings,
        mut is_immediate: bool,
    ) {
        let mut s = self.settings_lock.lock();
        if s.settings.as_ref() == Some(settings) {
            debugw!(self.logger, "skipping subscriber track settings", "settings" => s.settings);
            return;
        }

        is_immediate =
            is_immediate || (!settings.disabled && settings.disabled != s.is_muted_locked());
        s.settings = Some(settings.clone());
        debugw!(self.logger, "saving subscriber track settings", "settings" => s.settings);
        drop(s);

        if is_immediate {
            self.apply_settings();
        } else {
            // avoid frequent changes to mute & video layers, unless it became visible
            let this = self.this.clone();
            self.debouncer.add(move || {
                if let Some(t) = this.upgrade() {
                    t.apply_settings();
                }
            });
        }
    }

    pub fn update_video_layer(&self) {
        self.apply_settings();
    }

    fn apply_settings(&self) {
        let mut s = self.settings_lock.lock();
        let Some(settings) = s.settings.clone() else {
            return;
        };

        s.settings_version = self.version_generator.next();
        let settings_version = s.settings_version;
        drop(s);

        let dt = self.dt();
        let mut spatial = buffer::INVALID_LAYER_SPATIAL;
        let mut temporal = buffer::INVALID_LAYER_TEMPORAL;
        if dt.kind() == webrtc::RtpCodecType::Video {
            let mt = &self.params.media_track;
            let mut quality = settings.quality();
            let mime_type = dt.mime();
            if settings.width > 0 {
                quality = mt.get_quality_for_dimension(mime_type, settings.width, settings.height);
            }

            spatial = buffer::get_spatial_layer_for_video_quality(
                mime_type,
                quality,
                Some(&mt.to_proto()),
            );
            if settings.fps > 0 {
                temporal = mt.get_temporal_layer_for_spatial_fps(mime_type, spatial, settings.fps);
            }
        }

        let s = self.settings_lock.lock();
        if settings_version != s.settings_version {
            // a newer settings has superseded this one
            return;
        }
        // the settings are never taken away once there are some
        let Some(settings) = &s.settings else {
            return;
        };

        debugw!(self.logger, "applying subscriber track settings", "settings" => settings);
        if settings.disabled {
            dt.mute(true);
            return;
        } else {
            dt.mute(false);
        }

        if dt.kind() == webrtc::RtpCodecType::Video {
            dt.set_max_spatial_layer(spatial);
            if temporal != buffer::INVALID_LAYER_TEMPORAL {
                dt.set_max_temporal_layer(temporal);
            }
        }
    }

    pub fn needs_negotiation(&self) -> bool {
        self.needs_negotiation.load()
    }

    pub fn set_needs_negotiation(&self, needs: bool) {
        self.needs_negotiation.store(needs);
    }

    pub fn rtp_sender(&self) -> Option<Arc<dyn webrtc::RtpSender>> {
        self.sender.read().clone()
    }

    pub fn set_rtp_sender(&self, sender: Option<Arc<dyn webrtc::RtpSender>>) {
        *self.sender.write() = sender;
    }

    // DownTrackListener implementation
    // var _ sfu.DownTrackListener = (*SubscribedTrack)(nil): at the end of this file

    pub fn on_bind_and_connected(&self) {
        if self.params.subscriber.hidden() {
            return;
        }

        self.params.media_track.on_track_subscribed();
    }

    pub fn on_stats_update(&self, stat: &livekit::AnalyticsStat) {
        self.params
            .telemetry_listener
            .on_track_stats(&self.stats_key, stat);

        // Not here: the condensed statistics (telemetry.CondenseStat) that the original
        // hands to the reporter, which is not ported.
    }

    pub fn on_max_subscribed_layer_changed(&self, layer: i32) {
        if let Some(on_subscriber_max_quality_change) =
            &self.params.on_subscriber_max_quality_change
        {
            let dt = self.dt();
            on_subscriber_max_quality_change(&dt.subscriber_id(), dt.mime(), layer);
        }
    }

    pub fn on_rtt_update(&self, rtt: u32) {
        let subscriber = self.params.subscriber.clone();
        time::spawn(async move {
            subscriber.update_media_rtt(rtt);
        });
    }

    pub fn on_codec_negotiated(&self, codec: webrtc::RtpCodecCapability) {
        let (is_available, needs_publish) = self.params.wrapped_receiver.determine_receiver(&codec);
        if !is_available || !needs_publish {
            return;
        }

        if (self.params.on_subscriber_max_quality_change.is_some()
            || self.params.on_subscriber_audio_codec_change.is_some())
            && let Some(t) = self.this.upgrade()
        {
            time::spawn(async move {
                let mime_type = mime::normalize_mime_type(&codec.mime_type);
                match t.params.media_track.kind() {
                    livekit::TrackType::Video => {
                        let spatial = buffer::get_spatial_layer_for_video_quality(
                            mime_type,
                            livekit::VideoQuality::High,
                            Some(&t.params.media_track.to_proto()),
                        );
                        if let Some(on_subscriber_max_quality_change) =
                            &t.params.on_subscriber_max_quality_change
                        {
                            on_subscriber_max_quality_change(
                                &t.dt().subscriber_id(),
                                mime_type,
                                spatial,
                            );
                        }
                    }

                    livekit::TrackType::Audio => {
                        if let Some(on_subscriber_audio_codec_change) =
                            &t.params.on_subscriber_audio_codec_change
                        {
                            on_subscriber_audio_codec_change(
                                &t.dt().subscriber_id(),
                                mime_type,
                                true,
                            );
                        }
                    }

                    _ => {}
                }
            });
        }
    }

    pub fn on_down_track_close(&self, is_expected_to_resume: bool) {
        // Cache transceiver for potential re-use on resume.
        // To ensure subscription manager does not re-subscribe before caching,
        // delete the subscribed track only after caching.
        if is_expected_to_resume {
            let dt = self.dt();
            if let Some(tr) = dt.get_transceiver() {
                self.params
                    .subscriber
                    .cache_down_track(&self.id(), tr, dt.get_state());
            }
        }

        if let Some(on_down_track_closed) = &self.params.on_down_track_closed {
            on_down_track_closed(&self.params.subscriber.id());
        }
        self.close(is_expected_to_resume);
    }

    pub fn on_stream_started(&self, elapsed: Duration) {
        self.params
            .telemetry_listener
            .on_track_subscribe_stream_started(
                &self.params.subscriber.id(),
                &self.params.media_track.to_proto(),
                elapsed,
            );
        let on_subscribe_stream_started = self.on_subscribe_stream_started.read().clone();
        if let Some(on_subscribe_stream_started) = on_subscribe_stream_started {
            on_subscribe_stream_started(elapsed);
        }
    }

    // `t.downTrack`, where the original uses it without looking: it is set before `new`
    // returns the track, and nothing calls the track before that.
    pub(super) fn dt(&self) -> &Arc<sfu::DownTrack> {
        self.down_track
            .get()
            .expect("nil pointer dereference: SubscribedTrack without a down track")
    }
}

impl SubscribedTrackSettings {
    fn is_muted_locked(&self) -> bool {
        let Some(settings) = &self.settings else {
            return false;
        };

        settings.disabled
    }
}

impl types::SubscribedTrack for SubscribedTrack {
    fn add_on_bind(&self, f: Box<dyn Fn(Result<(), Error>) + Send + Sync>) {
        SubscribedTrack::add_on_bind(self, f);
    }

    fn is_bound(&self) -> bool {
        SubscribedTrack::is_bound(self)
    }

    fn on_subscribe_stream_started(&self, f: Box<dyn Fn(Duration) + Send + Sync>) {
        SubscribedTrack::on_subscribe_stream_started(self, f);
    }

    fn close(&self, is_expected_to_resume: bool) {
        SubscribedTrack::close(self, is_expected_to_resume);
    }

    fn on_close(&self, f: Option<Box<dyn Fn(bool) + Send + Sync>>) {
        SubscribedTrack::on_close(self, f);
    }

    fn id(&self) -> livekit::TrackId {
        SubscribedTrack::id(self)
    }

    fn publisher_id(&self) -> livekit::ParticipantId {
        SubscribedTrack::publisher_id(self)
    }

    fn publisher_identity(&self) -> livekit::ParticipantIdentity {
        SubscribedTrack::publisher_identity(self)
    }

    fn publisher_version(&self) -> u32 {
        SubscribedTrack::publisher_version(self)
    }

    fn subscriber_id(&self) -> livekit::ParticipantId {
        SubscribedTrack::subscriber_id(self)
    }

    fn subscriber_identity(&self) -> livekit::ParticipantIdentity {
        SubscribedTrack::subscriber_identity(self)
    }

    fn subscriber(&self) -> Arc<dyn types::LocalParticipant> {
        SubscribedTrack::subscriber(self)
    }

    fn down_track(&self) -> Option<Arc<sfu::DownTrack>> {
        SubscribedTrack::down_track(self)
    }

    fn media_track(&self) -> Option<Arc<dyn types::MediaTrack>> {
        SubscribedTrack::media_track(self)
    }

    fn rtp_sender(&self) -> Option<Arc<dyn webrtc::RtpSender>> {
        SubscribedTrack::rtp_sender(self)
    }

    fn is_muted(&self) -> bool {
        SubscribedTrack::is_muted(self)
    }

    fn set_publisher_muted(&self, muted: bool) {
        SubscribedTrack::set_publisher_muted(self, muted);
    }

    fn update_subscriber_settings(
        &self,
        settings: &livekit::UpdateTrackSettings,
        is_immediate: bool,
    ) {
        SubscribedTrack::update_subscriber_settings(self, settings, is_immediate);
    }

    fn update_video_layer(&self) {
        SubscribedTrack::update_video_layer(self);
    }

    fn needs_negotiation(&self) -> bool {
        SubscribedTrack::needs_negotiation(self)
    }
}

impl sfu::DownTrackListener for SubscribedTrack {
    fn on_bind_and_connected(&self) {
        SubscribedTrack::on_bind_and_connected(self);
    }

    fn on_stats_update(&self, stat: &livekit::AnalyticsStat) {
        SubscribedTrack::on_stats_update(self, stat);
    }

    fn on_max_subscribed_layer_changed(&self, layer: i32) {
        SubscribedTrack::on_max_subscribed_layer_changed(self, layer);
    }

    fn on_rtt_update(&self, rtt: u32) {
        SubscribedTrack::on_rtt_update(self, rtt);
    }

    fn on_codec_negotiated(&self, codec: webrtc::RtpCodecCapability) {
        SubscribedTrack::on_codec_negotiated(self, codec);
    }

    fn on_down_track_close(&self, is_expected_to_resume: bool) {
        SubscribedTrack::on_down_track_close(self, is_expected_to_resume);
    }

    fn on_stream_started(&self, elapsed: Duration) {
        SubscribedTrack::on_stream_started(self, elapsed);
    }
}

#[cfg(test)]
#[path = "subscribedtrack_test.rs"]
mod subscribedtrack_test;
