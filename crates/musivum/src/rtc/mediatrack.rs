// Ported from LiveKit, pkg/rtc/mediatrack.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `NewMediaTrack` is `MediaTrack::new` and gives an `Arc`, made with `Arc::new_cyclic`: the
//   track is handed out as itself (to its receiver as `types::MediaTrack`, to its dynacast
//   manager as listener) before it is complete, and keeps a `Weak` of itself (`this`) for
//   the callbacks it leaves with others.
// - The `*MediaTrackReceiver` and `*MediaLossProxy` that the original embeds are the fields
//   `media_track_receiver` (`media_track_receiver()` gives it) and `media_loss_proxy` (none
//   for video, where upstream's is nil). The methods are the original's, in its order, in
//   one `impl`; the traits (`types::MediaTrack`, `types::LocalMediaTrack`,
//   `dynacast::DynacastManagerListener`) are implemented at the end of the file, with what
//   the original has from the receiver it embeds passed on to it. The methods of the
//   receiver that other files of the package call on a `*MediaTrack` and that no trait has
//   are handed on in an `impl` of their own after the original's methods.
//   `NotifySubscriberNodeMediaLoss`, which the track has from the loss proxy, panics on a
//   video track, as calling it on upstream's nil proxy does.
// - Who holds whom. The participant holds the track (upstream: its up track manager and its
//   supervisor). The track holds its receiver, its loss proxy, its dynacast manager and the
//   last buffer it was given. Everything the track hands out holds it weakly: the receiver
//   (`media_track` of its params), the dynacast manager (its listener), and every callback
//   left with the receiver, the loss proxy, a `WebRTCReceiver`, a buffer or an RTCP reader. A
//   callback of a `WebRTCReceiver` holds that receiver weakly too (the receiver owns it),
//   and a buffer's notice of a retransmission stream holds the track's receiver weakly (the
//   track's receiver holds the receivers, which hold the buffers). The one callback that
//   holds what it needs strongly is that of a buffer's final statistics: it keeps the two
//   functions of the params it calls, and the track's ID, read when the buffer is added
//   (a track's ID does not change), so that the statistics are reported also when the
//   buffer is closed after the track is gone.
// - The functions of the params are `Arc`s, `None` in the zero value: the track hands them
//   on to its receiver and to the callbacks it leaves with others. Calling one that is not
//   there panics, as calling upstream's nil func does; `on_rtcp`,
//   `on_track_ever_subscribed` and the two `on_subscribed_*_change` are checked for nil
//   upstream and here. `BufferFactory`, a pointer, is an `Option` too, and `add_receiver`
//   panics without it, as upstream does. `ForwardStats` is an `Option` as the receiver's
//   option takes it.
// - `AddReceiver` takes the connection layer's `RtpReceiver` and an `sfu::TrackRemote` in
//   `Arc`s. `wr.(*sfu.WebRTCReceiver)`, which upstream asserts without checking, is
//   `as_web_rtc_receiver` at the end of the file, and `expect`s.
// - New for musivum in `add_receiver`: a stream that is announced again, whose buffer the
//   open receiver of its codec has already, is left as it is (`(false, true)` is returned,
//   and the RTCP reader keeps the callback it was given the first time). The connection
//   layer announces every remote stream again after each renegotiation
//   (`docs/webrtc-library.md`); upstream, where pion announces a stream once, the second
//   `AddUpTrack` would fail on the layer that is taken and close the buffer, and with it
//   the receiver and, if it was the only one, the track.
// - The observability reporter (`Reporter roomobs.TrackReporter`, LiveKit's `roomobs`) is
//   not ported: the field is left out, and with it the layers reported to it, the condensed
//   statistics (`telemetry.CondenseStat`) and the transaction of `OnStatsUpdate`, each with
//   a comment at its place. `ParticipantKind` and `ParticipantKindDetails`, which only the
//   reporter reads, are kept in the params.
// - `lastRR` of the RTCP reader's callback, a variable the closure captures upstream, is an
//   atomic the callback owns.
// - The two `OnSubscribed...Change` functions of the params get the protocol messages and
//   the lists borrowed.
// - `getReceiverLBThreshold` is 300 also without batch IO: the threshold is where LiveKit's
//   receiver starts writing a packet to its down tracks on several threads, and the original
//   ties it to what a write costs (~26us with the send inline, ~3.3us with batch IO). Here a
//   write, send included, costs ~4.4us, as batched does there; at 20, a packet to 100
//   subscribers took 144us where LiveKit takes 426us, for a quarter more CPU of the server
//   and 10 to 25% more of a load generator that is all the subscribers (`docs/port-status.md`,
//   "Under load"). At 300 it takes 440us.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::dynacast::{
    self, DynacastManagerAudio, DynacastManagerAudioParams, DynacastManagerVideo,
    DynacastManagerVideoParams,
};
use super::{
    DirectionConfig, Error, MediaLossProxy, MediaLossProxyParams, MediaTrackReceiver,
    MediaTrackReceiverParams, ReceiverConfig, logger_with_codec_mime,
};
use crate::config;
use crate::go::atomic;
use crate::go::time;
use crate::mediatransportutil as util;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;
use crate::rtc::types;
use crate::sfu;
use crate::sfu::TrackReceiver as _;
use crate::sfu::buffer;
use crate::sfu::connectionquality;
use crate::sfu::interceptor;
use crate::telemetry;
use crate::telemetry::prometheus;
use crate::webrtc;
use crate::webrtc::rtcp;
use crate::webrtc::rtcp::extended_report::{DLRRReportBlock, ExtendedReport};
use crate::webrtc::rtcp::receiver_report::ReceiverReport;
use crate::webrtc::rtcp::sender_report::SenderReport;
use crate::webrtc::rtcp::source_description::SourceDescription;
use crate::{debugw, errorw, infow, warnw};

// var _ types.LocalMediaTrack = (*MediaTrack)(nil): the implementation is at the end of this
// file

// MediaTrack represents a WebRTC track that needs to be forwarded
// Implements MediaTrack and PublishedTrack interface
pub struct MediaTrack {
    params: MediaTrackParams,
    buffer: atomic::Pointer<buffer::Buffer>,
    ever_subscribed: atomic::Bool,

    // the `*MediaTrackReceiver` that the original embeds
    media_track_receiver: Arc<MediaTrackReceiver>,
    // the `*MediaLossProxy` that the original embeds, nil for video
    media_loss_proxy: Option<Arc<MediaLossProxy>>,

    dynacast_manager: Option<Arc<dyn dynacast::DynacastManager>>,

    // serialises receiver creation in AddReceiver, i. e. the check-then-create
    // of a receiver for a mime type. Held across receiver set up
    add_receiver_lock: Mutex<()>,

    regression_target_codec_received: atomic::Bool,

    migrated: atomic::Bool,
    published: atomic::Bool,

    rtt_from_xr: atomic::Bool,

    backup_codec_policy: livekit::BackupCodecPolicy,
    // immutable after construction
    regression_target_codec: mime::MimeType,

    // the track itself, for the callbacks it leaves with others
    this: Weak<MediaTrack>,
}

/// `OnSubscribedMaxQualityChange` of the params.
pub type OnSubscribedMaxQualityChange = Arc<
    dyn Fn(
            &livekit::TrackId,
            &livekit::TrackInfo,
            &[livekit::SubscribedCodec],
            &[types::SubscribedCodecQuality],
        ) -> Result<(), Error>
        + Send
        + Sync,
>;

/// `OnSubscribedAudioCodecChange` of the params.
pub type OnSubscribedAudioCodecChange = Arc<
    dyn Fn(&livekit::TrackId, &[livekit::SubscribedAudioCodec]) -> Result<(), Error> + Send + Sync,
>;

#[derive(Default)]
pub struct MediaTrackParams {
    pub participant_id: Option<Arc<dyn Fn() -> livekit::ParticipantId + Send + Sync>>,
    pub participant_identity: livekit::ParticipantIdentity,
    pub participant_version: u32,
    pub participant_country: String,
    pub participant_kind: livekit::participant_info::Kind,
    pub participant_kind_details: Vec<livekit::participant_info::KindDetail>,
    pub buffer_factory: Option<Arc<buffer::Factory>>,
    pub receiver_config: ReceiverConfig,
    pub subscriber_config: DirectionConfig,
    pub pli_throttle_config: sfu::PliThrottleConfig,
    pub audio_config: sfu::AudioConfig,
    pub video_config: config::VideoConfig,
    #[allow(clippy::type_complexity)]
    pub telemetry_listener:
        Option<Arc<dyn Fn() -> Arc<dyn types::ParticipantTelemetryListener> + Send + Sync>>,
    pub logger: Logger,
    // `Reporter roomobs.TrackReporter`: not ported, see the header
    pub sim_tracks: HashMap<u32, interceptor::SimulcastTrackInfo>,
    #[allow(clippy::type_complexity)]
    pub on_rtcp: Option<Arc<dyn Fn(Vec<Box<dyn rtcp::Packet>>) + Send + Sync>>,
    pub forward_stats: Option<Arc<sfu::ForwardStats>>,
    pub on_track_ever_subscribed: Option<Arc<dyn Fn(livekit::TrackId) + Send + Sync>>,
    pub should_regress_codec: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    pub prefer_video_size_from_media: bool,
    pub enable_rtp_stream_restart_detection: bool,
    pub update_track_info_by_video_size_change: bool,
    pub force_backup_codec_policy_simulcast: bool,
    pub media_batch_io_enabled: bool,
    pub on_subscribed_max_quality_change: Option<OnSubscribedMaxQualityChange>,
    pub on_subscribed_audio_codec_change: Option<OnSubscribedAudioCodecChange>,
}

impl MediaTrack {
    pub fn new(params: MediaTrackParams, ti: &livekit::TrackInfo) -> Arc<MediaTrack> {
        Arc::new_cyclic(|this: &Weak<MediaTrack>| {
            let mut backup_codec_policy = ti.backup_codec_policy();

            if params.force_backup_codec_policy_simulcast {
                backup_codec_policy = livekit::BackupCodecPolicy::Simulcast;
            }

            let mut regression_target_codec = mime::MimeType::Unknown;
            if backup_codec_policy != livekit::BackupCodecPolicy::Simulcast && ti.codecs.len() > 1 {
                regression_target_codec = mime::normalize_mime_type(&ti.codecs[1].mime_type);
                debugw!(
                    params.logger,
                    "track enabled codec regression",
                    "regressionCodec" => regression_target_codec,
                );
            }

            let media_track: Weak<dyn types::MediaTrack> = this.clone();
            let media_track_receiver = MediaTrackReceiver::new(
                MediaTrackReceiverParams {
                    media_track: Some(media_track),
                    is_relayed: false,
                    participant_id: params.participant_id.clone().map(
                        |f| -> Box<dyn Fn() -> livekit::ParticipantId + Send + Sync> {
                            Box::new(move || f())
                        },
                    ),
                    participant_identity: params.participant_identity.clone(),
                    participant_version: params.participant_version,
                    receiver_config: params.receiver_config,
                    subscriber_config: params.subscriber_config.clone(),
                    audio_config: params.audio_config,
                    telemetry_listener: params.telemetry_listener.clone().map(
                        |f| -> Box<
                            dyn Fn() -> Arc<dyn types::ParticipantTelemetryListener> + Send + Sync,
                        > { Box::new(move || f()) },
                    ),
                    logger: params.logger.clone(),
                    regression_target_codec,
                    prefer_video_size_from_media: params.prefer_video_size_from_media,
                },
                ti,
            );

            let mut media_loss_proxy = None;
            if ti.r#type() == livekit::TrackType::Audio {
                let mlp = MediaLossProxy::new(MediaLossProxyParams {
                    logger: params.logger.clone(),
                });
                let t = this.clone();
                mlp.on_media_loss_update(Some(Box::new(move |fractional_loss: u8| {
                    if let Some(t) = t.upgrade()
                        && let Some(buff) = t.buffer.load()
                    {
                        buff.set_last_fraction_lost_report(fractional_loss);
                    }
                })));
                let m = mlp.clone();
                media_track_receiver.on_media_loss_feedback(
                    move |dt: &sfu::DownTrack, report: &ReceiverReport| {
                        m.handle_max_loss_feedback(dt, report)
                    },
                );
                media_loss_proxy = Some(mlp);
            }

            let listener: Weak<dyn dynacast::DynacastManagerListener> = this.clone();
            let mut dynacast_manager: Option<Arc<dyn dynacast::DynacastManager>> = None;
            match ti.r#type() {
                livekit::TrackType::Video => {
                    dynacast_manager =
                        Some(DynacastManagerVideo::new(DynacastManagerVideoParams {
                            dynacast_pause_delay: params.video_config.dynacast_pause_delay,
                            listener: Some(listener),
                            logger: params.logger.clone(),
                        }));
                }

                // `case livekit.TrackType_AUDIO: if len(ti.Codecs) > 1 { ... }`
                livekit::TrackType::Audio if ti.codecs.len() > 1 => {
                    dynacast_manager =
                        Some(DynacastManagerAudio::new(DynacastManagerAudioParams {
                            listener: Some(listener),
                            logger: params.logger.clone(),
                        }));
                }

                _ => {}
            }
            {
                let t = this.clone();
                media_track_receiver.on_setup_receiver(move |mime: mime::MimeType| {
                    let Some(t) = t.upgrade() else {
                        return;
                    };
                    if let Some(dynacast_manager) = &t.dynacast_manager {
                        dynacast_manager.add_codec(mime);
                    }
                });
            }
            {
                let t = this.clone();
                media_track_receiver.on_subscriber_max_quality_change(
                    move |subscriber_id: &livekit::ParticipantId,
                          mime_type: mime::MimeType,
                          layer: i32| {
                        let Some(t) = t.upgrade() else {
                            return;
                        };
                        if let Some(dynacast_manager) = &t.dynacast_manager {
                            dynacast_manager.notify_subscriber_max_quality(
                                subscriber_id,
                                mime_type,
                                buffer::get_video_quality_for_spatial_layer(
                                    mime_type,
                                    layer,
                                    Some(&t.media_track_receiver.track_info()),
                                ),
                            );
                        }
                    },
                );
            }
            {
                let t = this.clone();
                media_track_receiver.on_subscriber_audio_codec_change(
                    move |subscriber_id: &livekit::ParticipantId,
                          mime_type: mime::MimeType,
                          enabled: bool| {
                        let Some(t) = t.upgrade() else {
                            return;
                        };
                        if let Some(dynacast_manager) = &t.dynacast_manager {
                            dynacast_manager.notify_subscription(subscriber_id, mime_type, enabled);
                        }
                    },
                );
            }
            {
                let t = this.clone();
                media_track_receiver.on_codec_regression(
                    move |old: webrtc::RtpCodecParameters, new: webrtc::RtpCodecParameters| {
                        let Some(t) = t.upgrade() else {
                            return;
                        };
                        if let Some(dynacast_manager) = &t.dynacast_manager {
                            dynacast_manager.handle_codec_regression(
                                mime::normalize_mime_type(&old.mime_type),
                                mime::normalize_mime_type(&new.mime_type),
                            );
                        }
                    },
                );
            }

            MediaTrack {
                params,
                buffer: atomic::Pointer::new(None),
                ever_subscribed: atomic::Bool::new(false),
                media_track_receiver,
                media_loss_proxy,
                dynacast_manager,
                add_receiver_lock: Mutex::new(()),
                regression_target_codec_received: atomic::Bool::new(false),
                migrated: atomic::Bool::new(false),
                published: atomic::Bool::new(false),
                rtt_from_xr: atomic::Bool::new(false),
                backup_codec_policy,
                regression_target_codec,
                this: this.clone(),
            }
        })
    }

    pub fn notify_subscriber_node_max_quality(
        &self,
        node_id: &livekit::NodeId,
        qualities: &[types::SubscribedCodecQuality],
    ) {
        if let Some(dynacast_manager) = &self.dynacast_manager {
            dynacast_manager.notify_subscriber_node_max_quality(node_id, qualities);
        }
    }

    pub fn notify_subscription_node(
        &self,
        node_id: &livekit::NodeId,
        codecs: &[livekit::SubscribedAudioCodec],
    ) {
        if let Some(dynacast_manager) = &self.dynacast_manager {
            dynacast_manager.notify_subscription_node(node_id, codecs);
        }
    }

    pub fn clear_subscriber_nodes(&self) {
        if let Some(dynacast_manager) = &self.dynacast_manager {
            dynacast_manager.clear_subscriber_nodes();
        }
    }

    pub fn has_signal_cid(&self, cid: &str) -> bool {
        if !cid.is_empty() {
            let ti = self.media_track_receiver.track_info_clone();
            for c in &ti.codecs {
                if c.cid == cid {
                    return true;
                }
            }
        }
        false
    }

    pub fn has_sdp_cid(&self, cid: &str) -> bool {
        if !cid.is_empty() {
            let ti = self.media_track_receiver.track_info_clone();
            for c in &ti.codecs {
                if c.cid == cid || c.sdp_cid == cid {
                    return true;
                }
            }
        }
        false
    }

    pub fn get_mime_type_for_sdp_cid(&self, cid: &str) -> mime::MimeType {
        if !cid.is_empty() {
            let ti = self.media_track_receiver.track_info_clone();
            for c in &ti.codecs {
                if c.cid == cid || c.sdp_cid == cid {
                    return mime::normalize_mime_type(&c.mime_type);
                }
            }
        }
        mime::MimeType::Unknown
    }

    pub fn get_cids_for_mime_type(&self, mime_type: mime::MimeType) -> (String, String) {
        let ti = self.media_track_receiver.track_info_clone();
        for c in &ti.codecs {
            if mime::normalize_mime_type(&c.mime_type) == mime_type {
                return (c.cid.clone(), c.sdp_cid.clone());
            }
        }
        (String::new(), String::new())
    }

    pub fn to_proto(&self) -> livekit::TrackInfo {
        self.media_track_receiver.track_info_clone()
    }

    fn get_receiver_lb_threshold(&self) -> usize {
        // With batch IO the send syscall moves to the BatchConn flush goroutine, so WriteRTP only
        // translates the packet and enqueues it: ~3.3us per downtrack, against ~26us when the send runs
        // inline. The last subscriber of 300 subscribers at a serial forwarder is enqueued ~1ms
        // after the first.
        if self.params.media_batch_io_enabled {
            return 300;
        }

        // musivum: 300 here too, see the header
        300
    }

    // AddReceiver adds a new RTP receiver to the track, returns true when receiver represents a new codec
    // and if a receiver was added successfully
    pub fn add_receiver(
        &self,
        receiver: Arc<dyn webrtc::RtpReceiver>,
        track: Arc<dyn sfu::TrackRemote>,
        mid: &str,
    ) -> (bool, bool) {
        let mut new_codec = false;
        let ssrc = track.ssrc();
        let (buff, rtcp_reader) = self
            .params
            .buffer_factory
            .as_ref()
            .expect("nil pointer dereference: MediaTrackParams without BufferFactory")
            .get_buffer_pair(ssrc);
        let (Some(buff), Some(rtcp_reader)) = (buff, rtcp_reader) else {
            errorw!(self.params.logger, "could not retrieve buffer pair", nil);
            return (new_codec, false);
        };

        // New for musivum: a stream that is announced again is left as it is, see the header.
        if self.has_up_track_buffer(mime::normalize_mime_type(&track.codec().mime_type), &buff) {
            debugw!(
                self.params.logger,
                "AddReceiver of a stream that is added already",
                "rid" => track.rid(),
                "ssrc" => track.ssrc(),
                "codec" => track.codec(),
            );
            return (new_codec, true);
        }

        {
            let logger = self.params.logger.clone();
            let buff = buff.clone();
            let track = track.clone();
            let t = self.this.clone();
            let last_rr = atomic::Uint32::new(0);
            rtcp_reader.on_packet(move |bytes: &[u8]| {
                let pkts = match rtcp::packet::unmarshal(&mut &bytes[..]) {
                    Ok(pkts) => pkts,
                    Err(err) => {
                        errorw!(logger, "could not unmarshal RTCP", err, "size" => bytes.len());
                        return;
                    }
                };

                for pkt in &pkts {
                    let pkt = pkt.as_any();
                    if pkt.is::<SourceDescription>() {
                        // nothing upstream either
                    } else if let Some(pkt) = pkt.downcast_ref::<SenderReport>() {
                        if pkt.ssrc == track.ssrc() {
                            buff.set_sender_report_data(Some(&livekit::RtcpSenderReportState {
                                rtp_timestamp: pkt.rtp_time,
                                ntp_timestamp: pkt.ntp_time,
                                packets: pkt.packet_count,
                                octets: pkt.octet_count as u64,
                                at: mono::unix_nano(),
                                ..Default::default()
                            }));
                        }
                    } else if let Some(pkt) = pkt.downcast_ref::<ExtendedReport>() {
                        'rtt_from_xr: for report in &pkt.reports {
                            if let Some(rr) = report.as_any().downcast_ref::<DLRRReportBlock>() {
                                for dlrr_report in &rr.reports {
                                    if dlrr_report.last_rr <= last_rr.load() {
                                        continue;
                                    }
                                    let now_ntp = util::to_ntp_time(time::now());
                                    let now_ntp32 = (now_ntp.0 >> 16) as u32;
                                    let ntp_diff = now_ntp32
                                        .wrapping_sub(dlrr_report.last_rr)
                                        .wrapping_sub(dlrr_report.dlrr);
                                    let rtt = (ntp_diff as f64 * 1000.0 / 65536.0).ceil() as u32;
                                    buff.set_rtt(rtt);
                                    if let Some(t) = t.upgrade() {
                                        t.rtt_from_xr.store(true);
                                    }
                                    last_rr.store(dlrr_report.last_rr);
                                    break 'rtt_from_xr;
                                }
                            }
                        }
                    }
                }
            });
        }

        let ti = self.media_track_receiver.track_info_clone();

        let mut regress_codec = false;
        let enable_regression = self.enable_regression();

        let add_receiver_lock = self.add_receiver_lock.lock();
        let mime_type = mime::normalize_mime_type(&track.codec().mime_type);
        let layer = buffer::get_spatial_layer_for_rid(mime_type, &track.rid(), Some(&ti));
        if layer < 0 {
            warnw!(
                self.params.logger,
                "AddReceiver failed due to negative layer",
                nil,
                "rid" => track.rid(),
                "layer" => layer,
                "ssrc" => track.ssrc(),
                "codec" => track.codec(),
                "trackInfo" => ti,
            );
            drop(add_receiver_lock);
            return (new_codec, false);
        }

        debugw!(
            self.params.logger,
            "AddReceiver",
            "rid" => track.rid(),
            "layer" => layer,
            "ssrc" => track.ssrc(),
            "codec" => track.codec(),
            "trackInfo" => ti,
        );
        let mut wr = self.media_track_receiver.receiver(mime_type);
        if wr.is_none() {
            let mut priority: i64 = -1;
            for (idx, c) in ti.codecs.iter().enumerate() {
                if mime::is_mime_type_string_equal(&track.codec().mime_type, &c.mime_type) {
                    priority = idx as i64;
                    break;
                }
            }
            if priority < 0 {
                match ti.codecs.len() {
                    0 => {
                        // audio track
                        warnw!(
                            self.params.logger,
                            "unexpected 0 codecs in track info",
                            nil,
                            "mime" => mime_type,
                            "track" => ti,
                        );
                        priority = 0;
                    }
                    // older clients or non simulcast-codec, mime type only set later
                    1 if ti.codecs[0].mime_type.is_empty() => {
                        priority = 0;
                    }
                    _ => {}
                }
            }
            if priority < 0 {
                warnw!(
                    self.params.logger,
                    "could not find codec for webrtc receiver",
                    nil,
                    "mime" => mime_type,
                    "track" => ti,
                );
                drop(add_receiver_lock);
                return (new_codec, false);
            }
            let priority = priority as usize;

            let new_wr = sfu::WebRtcReceiver::new(
                receiver.clone(),
                &*track,
                &ti,
                logger_with_codec_mime(&self.params.logger, mime_type),
                self.params.on_rtcp.clone().map(|f| -> sfu::OnRtcp {
                    Box::new(move |pkts: Vec<Box<dyn rtcp::Packet>>| f(pkts))
                }),
                self.params.video_config.stream_tracker_manager.clone(),
                vec![
                    sfu::with_pli_throttle_config(self.params.pli_throttle_config),
                    sfu::with_audio_config(self.params.audio_config),
                    sfu::with_load_balance_threshold(self.get_receiver_lb_threshold()),
                    sfu::with_forward_stats(self.params.forward_stats.clone()),
                    sfu::with_enable_rtp_stream_restart_detection(
                        self.params.enable_rtp_stream_restart_detection,
                    ),
                ],
            );
            {
                let t = self.this.clone();
                new_wr.on_close_handler(move || {
                    let Some(t) = t.upgrade() else {
                        return;
                    };
                    t.media_track_receiver.set_closing(false);
                    t.media_track_receiver.clear_receiver(mime_type, false);
                    if t.media_track_receiver.try_close()
                        && let Some(dynacast_manager) = &t.dynacast_manager
                    {
                        dynacast_manager.close();
                    }
                });
            }

            // SIMULCAST-CODEC-TODO: these need to be receiver/mime aware, setting it up only for primary now
            let stats_key = telemetry::stats_key_for_track(
                &self.params.participant_country,
                livekit::StreamType::Upstream,
                self.media_track_receiver.publisher_id(),
                self.media_track_receiver.id(),
                ti.source(),
                ti.r#type(),
            );
            // Here upstream reports the size of every layer of every codec to its reporter
            // (`t.params.Reporter.ReportLayer`): not ported, see the header.
            {
                let t = self.this.clone();
                new_wr.on_stats_update(
                    move |_w: &sfu::WebRtcReceiver, stat: &livekit::AnalyticsStat| {
                        let Some(t) = t.upgrade() else {
                            return;
                        };
                        // send for only one codec, either primary (priority == 0) OR regressed codec
                        if priority == 0 || t.regression_target_codec_received.load() {
                            t.telemetry_listener().on_track_stats(&stats_key, stat);

                            // Here upstream condenses the statistics (`telemetry.CondenseStat`)
                            // and reports them to its reporter: not ported, see the header.
                        }
                    },
                );
            }

            {
                let t = self.this.clone();
                new_wr.on_max_layer_change(move |mime_type: mime::MimeType, max_layer: i32| {
                    let Some(t) = t.upgrade() else {
                        return;
                    };
                    // send for only one codec, either primary (priority == 0) OR regressed codec
                    if priority == 0 || t.regression_target_codec_received.load() {
                        t.media_track_receiver
                            .notify_max_layer_change(mime_type, max_layer);
                    }
                });
            }
            // SIMULCAST-CODEC-TODO END: these need to be receiver/mime aware, setting it up only for primary now

            if self.media_track_receiver.primary_receiver().is_none() {
                // primary codec published, set potential codecs
                let mut potential_codecs: Vec<webrtc::RtpCodecParameters> =
                    Vec::with_capacity(ti.codecs.len());
                let parameters = receiver.get_parameters();
                for c in &ti.codecs {
                    for nc in &parameters.codecs {
                        if mime::is_mime_type_string_equal(&nc.mime_type, &c.mime_type) {
                            potential_codecs.push(nc.clone());
                            break;
                        }
                    }
                }

                if !potential_codecs.is_empty() {
                    debugw!(
                        self.params.logger,
                        "primary codec published, set potential codecs",
                        "potential" => potential_codecs,
                    );
                    self.media_track_receiver
                        .set_potential_codecs(&potential_codecs, &parameters.header_extensions);
                }
            }

            self.buffer.store(Some(buff.clone()));

            self.media_track_receiver
                .setup_receiver(new_wr.clone(), priority, mid);

            for (&ssrc, info) in &self.params.sim_tracks {
                if info.mid == mid && !info.is_repair_stream {
                    self.media_track_receiver.set_layer_ssrcs_for_rid(
                        mime_type,
                        &info.stream_id,
                        ssrc,
                        info.repair_ssrc,
                    );
                }
            }
            wr = Some(new_wr.clone());
            new_codec = true;

            {
                let t = self.this.clone();
                let w = Arc::downgrade(&new_wr);
                new_wr.add_on_codec_state_change(Box::new(
                    move |codec: webrtc::RtpCodecParameters, state: sfu::ReceiverCodecState| {
                        let (Some(t), Some(w)) = (t.upgrade(), w.upgrade()) else {
                            return;
                        };
                        let w: Arc<dyn sfu::TrackReceiver> = w;
                        t.media_track_receiver
                            .handle_receiver_codec_change(&w, &codec, state);
                    },
                ));
            }

            // update subscriber video layers when video size changes
            {
                let t = self.this.clone();
                let w = Arc::downgrade(&new_wr);
                new_wr.on_video_size_changed(move || {
                    let Some(t) = t.upgrade() else {
                        return;
                    };
                    if t.params.update_track_info_by_video_size_change
                        && let Some(w) = w.upgrade()
                    {
                        t.media_track_receiver
                            .update_video_size(mime_type, &w.video_sizes());
                    }

                    t.media_track_receiver
                        .media_track_subscriptions()
                        .update_video_layers();
                });
            }
        }

        if new_codec && enable_regression {
            if mime_type == self.regression_target_codec {
                infow!(
                    self.params.logger,
                    "regression target codec received",
                    "codec" => mime_type,
                );
                self.regression_target_codec_received.store(true);
                regress_codec = true;
            } else if self.regression_target_codec_received.load() {
                regress_codec = true;
            }
        }
        drop(add_receiver_lock);

        let wr = wr.expect("a receiver was found or made");
        if let Err(err) = as_web_rtc_receiver(&wr)
            .expect("interface conversion: sfu.TrackReceiver is not *sfu.WebRTCReceiver")
            .add_up_track(track.clone(), buff.clone())
        {
            warnw!(
                self.params.logger,
                "adding up track failed",
                err,
                "rid" => track.rid(),
                "layer" => layer,
                "ssrc" => track.ssrc(),
                "newCodec" => new_codec,
            );
            let _ = buff.close();
            return (new_codec, false);
        }

        let mut expected_bitrate: i64 = 0;
        let layers = buffer::get_video_layers_for_mime_type(mime_type, Some(&ti));
        if layer >= 0 && layers.len() > layer as usize {
            expected_bitrate = layers[layer as usize].bitrate as i64;
        }
        if let Err(err) = buff.bind(
            receiver.get_parameters(),
            &track.codec().rtp_codec_capability,
            expected_bitrate,
        ) {
            warnw!(
                self.params.logger,
                "binding buffer failed",
                err,
                "rid" => track.rid(),
                "layer" => layer,
                "ssrc" => track.ssrc(),
                "newCodec" => new_codec,
            );
            let _ = buff.close();
            return (new_codec, false);
        }

        self.media_track_receiver.maybe_set_simulcast();
        self.media_track_receiver
            .set_layer_ssrcs_for_rid(mime_type, &track.rid(), track.ssrc(), 0);

        if regress_codec {
            for c in &ti.codecs {
                if mime::normalize_mime_type(&c.mime_type) == self.regression_target_codec {
                    continue;
                }

                debugw!(
                    self.params.logger,
                    "suspending codec for codec regression",
                    "codec" => c.mime_type,
                );
                if let Some(r) = self
                    .media_track_receiver
                    .receiver(mime::normalize_mime_type(&c.mime_type))
                    && let Some(rtcreceiver) = as_web_rtc_receiver(&r)
                {
                    rtcreceiver.set_codec_state(sfu::ReceiverCodecState::Suspended);
                }
            }
        }

        {
            let t = Arc::downgrade(&self.media_track_receiver);
            buff.on_notify_rtx(move |ssrc: u32, repair_ssrc: u32, rsid: &str| {
                if let Some(t) = t.upgrade() {
                    t.set_layer_rtx_info(ssrc, repair_ssrc, rsid);
                }
            });
        }

        buff.on_fec_recovery(|received, recovered, discarded, bytes_received| {
            prometheus::record_fec_upstream(received, recovered, discarded, bytes_received as u64);
        });

        // if subscriber request fps before fps calculated, update them after fps updated.
        {
            let t = self.this.clone();
            buff.on_fps_changed(move || {
                if let Some(t) = t.upgrade() {
                    t.media_track_receiver
                        .media_track_subscriptions()
                        .update_video_layers();
                }
            });
        }

        {
            // what the callback needs, held by it: see the header
            let telemetry_listener = self.params.telemetry_listener.clone();
            let participant_id = self.params.participant_id.clone();
            let track_id = self.media_track_receiver.id();
            buff.on_final_rtp_stats(move |stats: livekit::RtpStats| {
                let telemetry_listener = telemetry_listener
                    .as_ref()
                    .expect("nil pointer dereference: MediaTrackParams without TelemetryListener");
                let participant_id = participant_id
                    .as_ref()
                    .expect("nil pointer dereference: MediaTrackParams without ParticipantID");
                telemetry_listener().on_track_publish_rtp_stats(
                    &participant_id(),
                    &track_id,
                    mime_type,
                    layer,
                    &stats,
                );
            });
        }
        (new_codec, true)
    }

    // New for musivum: whether the receiver of the codec has this buffer already, i. e. the
    // stream was added before. See the header.
    fn has_up_track_buffer(&self, mime_type: mime::MimeType, buff: &Arc<buffer::Buffer>) -> bool {
        let Some(wr) = self.media_track_receiver.receiver(mime_type) else {
            return false;
        };
        let Some(wr) = as_web_rtc_receiver(&wr) else {
            return false;
        };
        if wr.is_closed() {
            // what follows then is upstream's: adding fails and the buffer is closed
            return false;
        }
        let buff: Arc<dyn buffer::BufferProvider> = buff.clone();
        wr.receiver_base()
            .get_all_buffers()
            .iter()
            .flatten()
            .any(|b| Arc::ptr_eq(b, &buff))
    }

    pub fn get_connection_score_and_quality(&self) -> (f32, livekit::ConnectionQuality) {
        let receiver = self.media_track_receiver.active_receiver();
        if let Some(rtc_receiver) = receiver.as_ref().and_then(as_web_rtc_receiver) {
            return rtc_receiver.get_connection_score_and_quality();
        }

        (
            connectionquality::MAX_MOS,
            livekit::ConnectionQuality::Excellent,
        )
    }

    pub fn set_rtt(&self, rtt: u32) {
        if !self.rtt_from_xr.load() {
            self.media_track_receiver.set_rtt(rtt);
        }
    }

    pub fn has_pending_codec(&self) -> bool {
        self.media_track_receiver.primary_receiver().is_none()
    }

    pub fn restart(&self) {
        self.media_track_receiver.restart();

        if let Some(dynacast_manager) = &self.dynacast_manager {
            dynacast_manager.restart();
        }
    }

    pub fn close(&self, is_expected_to_resume: bool) {
        self.media_track_receiver.set_closing(is_expected_to_resume);
        if let Some(dynacast_manager) = &self.dynacast_manager {
            dynacast_manager.close();
        }
        self.media_track_receiver.close(is_expected_to_resume);
    }

    pub fn set_muted(&self, muted: bool) {
        // update quality based on subscription if unmuting.
        // This will queue up the current state, but subscriber
        // driven changes could update it.
        if !muted && let Some(dynacast_manager) = &self.dynacast_manager {
            dynacast_manager.force_update();
        }

        self.media_track_receiver.set_muted(muted);
    }

    // ResendSubscribedQuality sends the publisher the subscribed qualities of the track again.
    pub fn resend_subscribed_quality(&self) {
        if let Some(dynacast_manager) = &self.dynacast_manager {
            dynacast_manager.resend_committed_quality();
        }
    }

    // OnTrackSubscribed is called when the track is subscribed by a non-hidden subscriber
    // this allows the publisher to know when they should start sending data
    pub fn on_track_subscribed(&self) {
        if !self.ever_subscribed.swap(true)
            && let Some(on_track_ever_subscribed) = self.params.on_track_ever_subscribed.clone()
        {
            let track_id = self.media_track_receiver.id();
            time::spawn(async move { on_track_ever_subscribed(track_id) });
        }
    }

    fn enable_regression(&self) -> bool {
        self.backup_codec_policy == livekit::BackupCodecPolicy::Regression
            || (self.backup_codec_policy == livekit::BackupCodecPolicy::PreferRegression
                && (self.params.should_regress_codec.as_ref().expect(
                    "nil pointer dereference: MediaTrackParams without ShouldRegressCodec",
                ))())
    }

    pub fn logger(&self) -> Logger {
        self.params.logger.clone()
    }

    // dynacast.DynacastManagerListtener implementation: the trait is implemented at the end of
    // this file

    pub fn on_dynacast_subscribed_max_quality_change(
        &self,
        subscribed_qualities: Vec<livekit::SubscribedCodec>,
        max_subscribed_qualities: Vec<types::SubscribedCodecQuality>,
    ) {
        if let Some(on_subscribed_max_quality_change) =
            &self.params.on_subscribed_max_quality_change
            && !self.media_track_receiver.is_muted()
        {
            let _ = on_subscribed_max_quality_change(
                &self.media_track_receiver.id(),
                &self.to_proto(),
                &subscribed_qualities,
                &max_subscribed_qualities,
            );
        }

        for q in &max_subscribed_qualities {
            let receiver = self.media_track_receiver.receiver(q.codec_mime);
            if let Some(receiver) = receiver {
                receiver.set_max_expected_spatial_layer(
                    buffer::get_spatial_layer_for_video_quality(
                        q.codec_mime,
                        q.quality,
                        Some(&self.media_track_receiver.track_info()),
                    ),
                );
            }
        }
    }

    pub fn on_dynacast_subscribed_audio_codec_change(
        &self,
        codecs: Vec<livekit::SubscribedAudioCodec>,
    ) {
        if let Some(on_subscribed_audio_codec_change) =
            &self.params.on_subscribed_audio_codec_change
        {
            let _ = on_subscribed_audio_codec_change(&self.media_track_receiver.id(), &codecs);
        }
    }

    pub fn set_migrated(&self, migrated: bool) {
        self.migrated.store(migrated);
    }

    pub fn migrated(&self) -> bool {
        self.migrated.load()
    }

    pub fn set_published(&self, published: bool) {
        self.published.store(published);
    }

    pub fn published(&self) -> bool {
        self.published.load()
    }
}

// -----------------------------------------------------------

// The embedded `*MediaTrackReceiver` and `*MediaLossProxy`, and those methods of the receiver
// that other files of the package call on a `*MediaTrack` and that no trait has (the others
// are passed on by the trait implementations below).

impl MediaTrack {
    /// The `*MediaTrackReceiver` that the original embeds.
    pub fn media_track_receiver(&self) -> &Arc<MediaTrackReceiver> {
        &self.media_track_receiver
    }

    /// The `*MediaLossProxy` that the original embeds: `None` for a video track.
    pub fn media_loss_proxy(&self) -> Option<&Arc<MediaLossProxy>> {
        self.media_loss_proxy.as_ref()
    }

    pub fn set_potential_codecs(
        &self,
        codecs: &[webrtc::RtpCodecParameters],
        headers: &[webrtc::RtpHeaderExtensionParameter],
    ) {
        self.media_track_receiver
            .set_potential_codecs(codecs, headers);
    }

    pub fn set_layer_ssrcs_for_rid(
        &self,
        mime_type: mime::MimeType,
        rid: &str,
        ssrc: u32,
        repair_ssrc: u32,
    ) {
        self.media_track_receiver
            .set_layer_ssrcs_for_rid(mime_type, rid, ssrc, repair_ssrc);
    }

    pub fn update_codec_info(&self, codecs: &[livekit::SimulcastCodec]) {
        self.media_track_receiver.update_codec_info(codecs);
    }

    pub fn update_codec_sdp_cid(&self, mime_type: mime::MimeType, sdp_cid: &str) {
        self.media_track_receiver
            .update_codec_sdp_cid(mime_type, sdp_cid);
    }

    pub fn update_codec_rids(&self, mime_type: mime::MimeType, rids: &buffer::VideoLayersRid) {
        self.media_track_receiver.update_codec_rids(mime_type, rids);
    }

    pub fn debug_info(&self) -> sfu::DebugInfo {
        self.media_track_receiver.debug_info()
    }

    // `t.params.TelemetryListener()`
    fn telemetry_listener(&self) -> Arc<dyn types::ParticipantTelemetryListener> {
        let telemetry_listener = self
            .params
            .telemetry_listener
            .as_ref()
            .expect("nil pointer dereference: MediaTrackParams without TelemetryListener");
        telemetry_listener()
    }
}

// -----------------------------------------------------------

impl types::AsLocalMediaTrack for MediaTrack {
    fn as_local_media_track(self: Arc<Self>) -> Option<Arc<dyn types::LocalMediaTrack>> {
        Some(self)
    }
}

// types.MediaTrack: the track's own `to_proto`, `logger`, `on_track_subscribed`, `close` and
// `set_muted`, and the rest from the receiver it embeds upstream
impl types::MediaTrack for MediaTrack {
    fn id(&self) -> livekit::TrackId {
        self.media_track_receiver.id()
    }

    fn kind(&self) -> livekit::TrackType {
        self.media_track_receiver.kind()
    }

    fn name(&self) -> String {
        self.media_track_receiver.name()
    }

    fn source(&self) -> livekit::TrackSource {
        self.media_track_receiver.source()
    }

    fn stream(&self) -> String {
        self.media_track_receiver.stream()
    }

    fn update_track_info(&self, ti: &livekit::TrackInfo) {
        self.media_track_receiver.update_track_info(ti)
    }

    fn update_audio_track(&self, update: &livekit::UpdateLocalAudioTrack) {
        self.media_track_receiver.update_audio_track(update)
    }

    fn update_video_track(&self, update: &livekit::UpdateLocalVideoTrack) {
        self.media_track_receiver.update_video_track(update)
    }

    fn to_proto(&self) -> livekit::TrackInfo {
        MediaTrack::to_proto(self)
    }

    fn publisher_id(&self) -> livekit::ParticipantId {
        self.media_track_receiver.publisher_id()
    }

    fn publisher_identity(&self) -> livekit::ParticipantIdentity {
        self.media_track_receiver.publisher_identity()
    }

    fn publisher_version(&self) -> u32 {
        self.media_track_receiver.publisher_version()
    }

    fn logger(&self) -> Logger {
        MediaTrack::logger(self)
    }

    fn is_muted(&self) -> bool {
        self.media_track_receiver.is_muted()
    }

    fn set_muted(&self, muted: bool) {
        MediaTrack::set_muted(self, muted)
    }

    fn get_audio_level(&self) -> (f64, bool) {
        self.media_track_receiver.get_audio_level()
    }

    fn close(&self, is_expected_to_resume: bool) {
        MediaTrack::close(self, is_expected_to_resume)
    }

    fn is_open(&self) -> bool {
        self.media_track_receiver.is_open()
    }

    fn add_on_close(&self, f: Option<Box<dyn Fn(bool) + Send + Sync>>) {
        self.media_track_receiver.add_on_close(f)
    }

    fn add_subscriber(
        &self,
        participant: Arc<dyn types::LocalParticipant>,
    ) -> Result<Option<Arc<dyn types::SubscribedTrack>>, Error> {
        self.media_track_receiver.add_subscriber(participant)
    }

    fn remove_subscriber(
        &self,
        participant_id: &livekit::ParticipantId,
        is_expected_to_resume: bool,
    ) {
        self.media_track_receiver
            .remove_subscriber(participant_id, is_expected_to_resume)
    }

    fn is_subscriber(&self, sub_id: &livekit::ParticipantId) -> bool {
        self.media_track_receiver.is_subscriber(sub_id)
    }

    fn revoke_disallowed_subscribers(
        &self,
        allowed_subscriber_identities: &[livekit::ParticipantIdentity],
    ) -> Vec<livekit::ParticipantIdentity> {
        self.media_track_receiver
            .revoke_disallowed_subscribers(allowed_subscriber_identities)
    }

    fn get_all_subscribers(&self) -> Vec<livekit::ParticipantId> {
        self.media_track_receiver.get_all_subscribers()
    }

    fn get_num_subscribers(&self) -> usize {
        self.media_track_receiver.get_num_subscribers()
    }

    fn on_track_subscribed(&self) {
        MediaTrack::on_track_subscribed(self)
    }

    fn get_quality_for_dimension(
        &self,
        mime_type: mime::MimeType,
        width: u32,
        height: u32,
    ) -> livekit::VideoQuality {
        self.media_track_receiver
            .get_quality_for_dimension(mime_type, width, height)
    }

    fn get_temporal_layer_for_spatial_fps(
        &self,
        mime_type: mime::MimeType,
        spatial: i32,
        fps: u32,
    ) -> i32 {
        self.media_track_receiver
            .get_temporal_layer_for_spatial_fps(mime_type, spatial, fps)
    }

    fn receivers(&self) -> Vec<Arc<dyn sfu::TrackReceiver>> {
        self.media_track_receiver.receivers()
    }

    fn clear_all_receivers(&self, is_expected_to_resume: bool) {
        self.media_track_receiver
            .clear_all_receivers(is_expected_to_resume)
    }

    fn is_encrypted(&self) -> bool {
        self.media_track_receiver.is_encrypted()
    }

    fn has_packet_trailer(&self) -> bool {
        self.media_track_receiver.has_packet_trailer()
    }
}

// types.LocalMediaTrack: the track's own, with `get_track_stats` from the receiver and
// `notify_subscriber_node_media_loss` from the loss proxy it embeds upstream
impl types::LocalMediaTrack for MediaTrack {
    fn restart(&self) {
        MediaTrack::restart(self)
    }

    fn has_signal_cid(&self, cid: &str) -> bool {
        MediaTrack::has_signal_cid(self, cid)
    }

    fn has_sdp_cid(&self, cid: &str) -> bool {
        MediaTrack::has_sdp_cid(self, cid)
    }

    fn set_migrated(&self, migrated: bool) {
        MediaTrack::set_migrated(self, migrated)
    }

    fn migrated(&self) -> bool {
        MediaTrack::migrated(self)
    }

    fn set_published(&self, published: bool) {
        MediaTrack::set_published(self, published)
    }

    fn published(&self) -> bool {
        MediaTrack::published(self)
    }

    fn get_connection_score_and_quality(&self) -> (f32, livekit::ConnectionQuality) {
        MediaTrack::get_connection_score_and_quality(self)
    }

    fn get_track_stats(&self) -> Option<livekit::RtpStats> {
        self.media_track_receiver.get_track_stats()
    }

    fn set_rtt(&self, rtt: u32) {
        MediaTrack::set_rtt(self, rtt)
    }

    fn notify_subscriber_node_max_quality(
        &self,
        node_id: &livekit::NodeId,
        qualities: &[types::SubscribedCodecQuality],
    ) {
        MediaTrack::notify_subscriber_node_max_quality(self, node_id, qualities)
    }

    fn notify_subscription_node(
        &self,
        node_id: &livekit::NodeId,
        codecs: &[livekit::SubscribedAudioCodec],
    ) {
        MediaTrack::notify_subscription_node(self, node_id, codecs)
    }

    fn clear_subscriber_nodes(&self) {
        MediaTrack::clear_subscriber_nodes(self)
    }

    fn notify_subscriber_node_media_loss(&self, node_id: &livekit::NodeId, fractional_loss: u8) {
        self.media_loss_proxy
            .as_ref()
            .expect("nil pointer dereference: the MediaLossProxy of a video track")
            .notify_subscriber_node_media_loss(node_id, fractional_loss)
    }
}

// var _ dynacast.DynacastManagerListener = (*MediaTrack)(nil)
impl dynacast::DynacastManagerListener for MediaTrack {
    fn on_dynacast_subscribed_max_quality_change(
        &self,
        subscribed_qualities: Vec<livekit::SubscribedCodec>,
        max_subscribed_qualities: Vec<types::SubscribedCodecQuality>,
    ) {
        MediaTrack::on_dynacast_subscribed_max_quality_change(
            self,
            subscribed_qualities,
            max_subscribed_qualities,
        )
    }

    fn on_dynacast_subscribed_audio_codec_change(
        &self,
        codecs: Vec<livekit::SubscribedAudioCodec>,
    ) {
        MediaTrack::on_dynacast_subscribed_audio_codec_change(self, codecs)
    }
}

// Go's `r.(*sfu.WebRTCReceiver)` on an `sfu.TrackReceiver`.
fn as_web_rtc_receiver(r: &Arc<dyn sfu::TrackReceiver>) -> Option<Arc<sfu::WebRtcReceiver>> {
    let r: Arc<dyn Any + Send + Sync> = r.clone();
    r.downcast::<sfu::WebRtcReceiver>().ok()
}

#[cfg(test)]
#[path = "mediatrack_test.rs"]
mod mediatrack_test;
