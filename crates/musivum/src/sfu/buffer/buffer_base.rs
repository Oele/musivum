// Ported from LiveKit, pkg/sfu/buffer/buffer_base.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The lock. `Buffer` embeds `*BufferBase` upstream and guards its own fields with the
//   base's lock. Here there is one lock too, and it is `Buffer`'s (`buffer.rs`): `BufferBase`
//   is the base's state inside that lock (the field `buffer_base` of `BufferInner`), the
//   functions the original calls with the lock held are its methods, and the methods of the
//   base that take the lock, which `Buffer` has by embedding, are written here as methods of
//   `Buffer` (`impl Buffer`), each at the place of its original: `b.Lock()` is
//   `self.lock.write()`, and `b.field` is `b.field` of `let b = &mut lock.buffer_base`. What
//   the base uses without its lock (the parameters, the condition variable, `isClosed`, the
//   generation of the key frame seeder, `closeOnce`) is `BufferBaseShared`, which `Buffer`
//   holds as `buffer_base` and the state as `shared`. `BufferBase.Bind`, which `Buffer.Bind`
//   hides, has no counterpart; `bind_locked` is the one that binds.
// - `BufferBaseParams.SendPLI` and `.OnStreamRestartLocked` are how the base calls the
//   `Buffer` around it. They are not fields: `send_pli` calls `Buffer::send_pli_packet`
//   (`Buffer.sendPLI` upstream, whose name in Rust's spelling is taken by `SendPLI`), and
//   `restart_stream_locked`, which cannot reach the fields of `Buffer` from the base's state,
//   sets `stream_restarted` where the original calls `OnStreamRestartLocked`; whoever called
//   into the base (`handle_incoming_packet`, `restart_stream`, `Buffer`'s `ingest_locked`)
//   calls `BufferInner::on_stream_restart_locked` right after, still under the lock. Nothing
//   in between looks at what that function changes (the FEC decoder).
// - `readCond` is a condition variable on a read-write lock, which parking_lot does not
//   have: `ReadCond` counts the broadcasts under a small lock of its own, and a waiter notes
//   the count while it still holds the buffer's lock. `read_extended` waits on it as a
//   thread does; `read_extended_async` is the same read for a task, which must not block its
//   worker thread. `WaitRead`, which only `Buffer.Read` calls, is `ReadCond::wait`.
// - A packet holds its parts by value, absent where the original's pointer is nil (the
//   original points into backing storage inside the same struct and takes packets from a
//   pool, both to keep the receive path from allocating: `ExtPacketFactory`,
//   `rtpPacketFactory` and `ReleaseExtPacket` have no counterpart, a packet is dropped when
//   its owner is done with it). `packet` is never nil in the original and is a plain value
//   here. The embedded `VideoLayer` is the field `video_layer`. A packet given to
//   `handle_incoming_packet` is parsed from a copy of the bytes if the caller did not parse
//   it. `read_extended` fills the caller's buffer as the original does, and the packet it
//   returns has its own copy of those bytes (`raw_packet`), which its header's extensions
//   and its payload are slices of.
// - `BufferProvider` is a trait. Its callbacks are boxed; `Buffer`'s own methods of the same
//   names take any function. `read_extended_async` is new in it.
// - `GetNACKPairs` takes the lock for writing where the original takes it for reading: the
//   NACK queue is changed by being asked. `StartKeyFrameSeeder` takes it for reading where
//   the original reads `codecType` without it. A line that the original logs before it takes
//   the lock is logged with the lock held, as the logger is behind it.
// - The goroutines are tasks that hold the buffer weakly: the key frame seeder, the check of
//   the bucket's capacity, the flush on close. A callback that the original calls on a
//   goroutine of its own (`go f()`) is called from a task.
// - The four counters that limit log lines are plain numbers under the lock.
// - Where the original would stop on a nil pointer the port panics too, with a message:
//   a packet handed to a buffer that is not bound (no statistics) or that was bound to a
//   codec that is neither audio nor video (no bucket), and a restart of such a buffer.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Once, Weak};

use bytes::Bytes;
use parking_lot::{Condvar, Mutex, RwLock, RwLockWriteGuard};
use tokio::sync::Notify;

use super::{
    Buffer, DEFAULT_MAX_LAYER_SPATIAL, DependencyDescriptorParser, Error, ExtDependencyDescriptor,
    FrameRateCalculator, FrameRateCalculatorDd, FrameRateCalculatorH26x, FrameRateCalculatorVp8,
    FrameRateCalculatorVp9, INIT_PACKET_BUFFER_SIZE_AUDIO, INIT_PACKET_BUFFER_SIZE_VIDEO,
    INVALID_LAYER_SPATIAL, INVALID_LAYER_TEMPORAL, StreamStatsWithLayers, VideoLayer,
    extract_dependency_descriptor_video_size,
};
use crate::go::atomic;
use crate::go::time::{self, Time};
use crate::mediatransportutil::bucket;
use crate::mediatransportutil::codec;
use crate::mediatransportutil::nack;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::{self, Logger};
use crate::protocol::utils::mono;
use crate::sfu::audio;
use crate::sfu::rtpextension::abscapturetime as act;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::sfu::rtpstats;
use crate::sfu::utils;
use crate::telemetry::prometheus;
use crate::webrtc;
use crate::webrtc::rtcp::reception_report::ReceptionReport;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_nack::NackPair;
use crate::webrtc::rtp;
use crate::webrtc::rtp::codecs;
use crate::webrtc::sdp;
use crate::{debugw, errorw, infow, warnw};

// --------------------------------------

#[derive(Clone, Debug, Default)]
pub struct ExtPacket {
    pub video_layer: VideoLayer,
    pub arrival: i64,
    pub ext_sequence_number: u64,
    pub ext_timestamp: u64,
    pub packet: rtp::Packet,
    pub vp8: Option<codec::Vp8>,        // None when not parsed
    pub vp9: Option<codecs::Vp9Packet>, // None when not parsed
    pub is_key_frame: bool,
    pub raw_packet: Bytes,
    pub dependency_descriptor: Option<ExtDependencyDescriptor>,
    pub abs_capture_time_ext: Option<act::AbsCaptureTime>,
    pub is_out_of_order: bool,
    pub is_buffered: bool,
}

/// What `BufferProvider::read_extended_async` returns: `read_extended` as something a task
/// awaits.
pub type ReadExtendedFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ExtPacket>, Error>> + Send + 'a>>;

#[cfg_attr(test, musivum_fakes::fake)]
pub trait BufferProvider: Send + Sync {
    fn set_logger(&self, lgr: Logger);
    fn set_audio_level_config(&self, audio_level_config: audio::AudioLevelConfig);
    fn set_stream_restart_detection(&self, enable: bool);
    fn set_pli_throttle(&self, duration: i64);
    fn set_rtt(&self, rtt: u32);
    fn set_paused(&self, paused: bool);

    fn send_pli(&self, force: bool);

    // Waits, as a thread does, until there is a packet (`Ok(Some(..))`), the stream was
    // restarted (`Ok(None)`: nil and no error upstream) or the buffer is closed
    // (`Error::Eof`). Not for a task: see `read_extended_async`.
    fn read_extended(&self, buf: &mut [u8]) -> Result<Option<ExtPacket>, Error>;
    // New for musivum: `read_extended` for a task, which waits without blocking the thread
    // it runs on.
    fn read_extended_async<'a>(&'a self, buf: &'a mut [u8]) -> ReadExtendedFuture<'a>;
    fn get_packet(&self, buf: &mut [u8], esn: u64) -> Result<usize, Error>;

    fn get_audio_level(&self) -> (f64, bool);
    fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32>;
    fn get_stats(&self) -> Option<livekit::RtpStats>;
    fn get_delta_stats(&self) -> Option<StreamStatsWithLayers>;
    fn get_delta_stats_lite(&self) -> Option<rtpstats::RtpDeltaInfoLite>;
    fn get_last_sender_report_time(&self) -> Time;
    fn get_nack_pairs(&self) -> Vec<NackPair>;

    fn set_sender_report_data(&self, sr_data: Option<&livekit::RtcpSenderReportState>);
    fn get_sender_report_data(&self) -> Option<livekit::RtcpSenderReportState>;

    fn on_rtcp_sender_report(&self, f: Box<dyn Fn() + Send + Sync>);
    fn on_fps_changed(&self, f: Box<dyn Fn() + Send + Sync>);
    fn on_video_size_changed(&self, f: Box<dyn Fn(Vec<codec::VideoSize>) + Send + Sync>);
    fn on_codec_change(&self, f: Box<dyn Fn(webrtc::RtpCodecParameters) + Send + Sync>);
    fn on_stream_restart(&self, f: Box<dyn Fn(&str) + Send + Sync>);

    fn start_key_frame_seeder(&self);
    fn stop_key_frame_seeder(&self);

    #[allow(clippy::too_many_arguments)]
    fn handle_incoming_packet(
        &self,
        raw_pkt: &[u8],
        rtp_packet: Option<&mut rtp::Packet>,
        arrival_time: i64,
        is_buffered: bool,
        is_rtx: bool,
        skipped_seqs: &[u16],
        oob_sequence_number: u16,
    ) -> Result<u64, Error>;

    fn mark_for_restart_stream(&self, reason: &str);
    fn restart_stream(&self, reason: &str);

    fn close_with_reason(&self, reason: &str) -> Result<Option<livekit::RtpStats>, Error>;
}

const BUCKET_CAP_CHECK_INTERVAL: i64 = 1_000_000_000; // 1e9

#[derive(Clone, Debug, Default)]
pub struct BufferBaseParams {
    pub ssrc: u32,
    pub max_video_pkts: usize,
    pub max_audio_pkts: usize,
    pub logger_components: Vec<String>,
    // `SendPLI` and `OnStreamRestartLocked` of the original: see the header
    pub is_reporting_enabled: bool,
    pub is_oob_sequence_number: bool,
    pub is_dd_restart_enabled: bool,
}

/// `readCond *sync.Cond` of the original, for the buffer's read-write lock: see the header.
pub(super) struct ReadCond {
    // how many broadcasts there were
    generation: Mutex<u64>,
    cond: Condvar,
    notify: Notify,
}

impl ReadCond {
    fn new() -> Self {
        Self {
            generation: Mutex::new(0),
            cond: Condvar::new(),
            notify: Notify::new(),
        }
    }

    /// `Broadcast()`.
    pub(super) fn broadcast(&self) {
        let mut generation = self.generation.lock();
        *generation = generation.wrapping_add(1);
        self.cond.notify_all();
        self.notify.notify_waiters();
    }

    /// `Wait()` for a thread: gives up the buffer's lock, waits for a broadcast and takes the
    /// lock again. The count is read before the buffer's lock is given up, so a broadcast
    /// that follows is not missed.
    pub(super) fn wait<'a, T>(
        &self,
        lock: &'a RwLock<T>,
        guard: RwLockWriteGuard<'a, T>,
    ) -> RwLockWriteGuard<'a, T> {
        let mut generation = self.generation.lock();
        let seen = *generation;
        drop(guard);
        while *generation == seen {
            self.cond.wait(&mut generation);
        }
        drop(generation);
        lock.write()
    }

    /// The count of broadcasts, for `wait_async`: to be read with the buffer's lock held.
    fn generation(&self) -> u64 {
        *self.generation.lock()
    }

    /// `Wait()` for a task, which has given up the buffer's lock already: waits until there
    /// was a broadcast after the count `seen`.
    async fn wait_async(&self, seen: u64) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let generation = self.generation();
        if generation != seen {
            return;
        }
        notified.await;
    }
}

/// What `BufferBase` uses without its lock upstream: see the header.
pub(super) struct BufferBaseShared {
    pub(super) params: BufferBaseParams,

    pub(super) read_cond: ReadCond,

    close_once: Once,

    key_frame_seeder_generation: atomic::Int32,

    is_closed: atomic::Bool,
}

type OnRtcpSenderReport = Arc<dyn Fn() + Send + Sync>;
type OnFpsChanged = Arc<dyn Fn() + Send + Sync>;
type OnVideoSizeChanged = Arc<dyn Fn(Vec<codec::VideoSize>) + Send + Sync>;
type OnCodecChange = Arc<dyn Fn(webrtc::RtpCodecParameters) + Send + Sync>;
type OnStreamRestart = Arc<dyn Fn(&str) + Send + Sync>;

/// What the lock guards of `BufferBase` upstream: see the header.
pub(super) struct BufferBase {
    // the buffer this is the base of, for the tasks it starts
    this: Weak<Buffer>,
    pub(super) shared: Arc<BufferBaseShared>,

    pub(super) bucket: Option<bucket::Bucket<u64, u16>>,
    last_bucket_cap_check_at: i64,

    pub(super) nacker: Option<Box<dyn nack::NackQueueInterface>>,
    rtp_stats_lite: Option<rtpstats::RtpStatsReceiverLite>,
    lite_stats_snapshot_id: u32,

    pub(super) ext_packets: VecDeque<ExtPacket>,

    pub(super) codec_type: webrtc::RtpCodecType,
    clock_rate: u32,
    mime: mime::MimeType,

    rtp_parameters: webrtc::RtpParameters,
    pub(super) payload_type: u8,
    pub(super) rtx_payload_type: u8,
    pub(super) fec_payload_type: u8,

    pub(super) sn_range_map: utils::RangeMap<u64, u64>,

    audio_level_config: audio::AudioLevelConfig,
    audio_level: Option<audio::AudioLevel>,
    audio_level_ext_id: u8,

    enable_stream_restart_detection: bool,

    pli_throttle: i64,

    pub(super) rtp_stats: Option<Arc<rtpstats::RtpStatsReceiver>>,
    pps_snapshot_id: u32,
    rr_snapshot_id: u32,
    delta_stats_snapshot_id: u32,

    // callbacks
    on_rtcp_sender_report: Option<OnRtcpSenderReport>,
    on_fps_changed: Option<OnFpsChanged>,
    on_video_size_changed: Option<OnVideoSizeChanged>,
    on_codec_change: Option<OnCodecChange>,
    on_stream_restart: Option<OnStreamRestart>,

    // video size tracking for multiple spatial layers
    current_video_size: [codec::VideoSize; DEFAULT_MAX_LAYER_SPATIAL as usize + 1],

    pub(super) logger: Logger,

    // dependency descriptor
    dd_ext_id: u8,
    dd_parser: Option<DependencyDescriptorParser>,

    is_paused: bool,
    frame_rate_calculator:
        [Option<Box<dyn FrameRateCalculator>>; DEFAULT_MAX_LAYER_SPATIAL as usize + 1],
    frame_rate_calculated: bool,

    packet_not_found_count: u32,
    packet_too_old_count: u32,
    ext_packet_too_much_count: u32,
    codec_payload_type_not_found_count: u32,

    abs_capture_time_ext_id: u8,

    pub(super) is_restart_pending: bool,

    // set where the original calls `params.OnStreamRestartLocked`: see the header
    pub(super) stream_restarted: bool,
}

impl BufferBase {
    pub(super) fn new(params: BufferBaseParams, this: Weak<Buffer>) -> Self {
        let mut l = logger::get_logger(); // will be reset with correct context via SetLogger
        for component in &params.logger_components {
            l = l.with_component(component);
        }
        l = l.with_values(&[("ssrc", &params.ssrc)]);

        Self {
            this,
            shared: Arc::new(BufferBaseShared {
                params,
                read_cond: ReadCond::new(),
                close_once: Once::new(),
                key_frame_seeder_generation: atomic::Int32::new(0),
                is_closed: atomic::Bool::new(false),
            }),
            bucket: None,
            last_bucket_cap_check_at: mono::unix_nano(),
            nacker: None,
            rtp_stats_lite: None,
            lite_stats_snapshot_id: 0,
            ext_packets: VecDeque::with_capacity(128),
            codec_type: webrtc::RtpCodecType::default(),
            clock_rate: 0,
            mime: mime::MimeType::default(),
            rtp_parameters: webrtc::RtpParameters::default(),
            payload_type: 0,
            rtx_payload_type: 0,
            fec_payload_type: 0,
            sn_range_map: utils::RangeMap::new(100),
            audio_level_config: audio::AudioLevelConfig::default(),
            audio_level: None,
            audio_level_ext_id: 0,
            enable_stream_restart_detection: false,
            pli_throttle: (500 * time::MILLISECOND).nanoseconds(),
            rtp_stats: None,
            pps_snapshot_id: 0,
            rr_snapshot_id: 0,
            delta_stats_snapshot_id: 0,
            on_rtcp_sender_report: None,
            on_fps_changed: None,
            on_video_size_changed: None,
            on_codec_change: None,
            on_stream_restart: None,
            current_video_size: [codec::VideoSize::default();
                DEFAULT_MAX_LAYER_SPATIAL as usize + 1],
            logger: l,
            dd_ext_id: 0,
            dd_parser: None,
            is_paused: false,
            frame_rate_calculator: [None, None, None],
            frame_rate_calculated: false,
            packet_not_found_count: 0,
            packet_too_old_count: 0,
            ext_packet_too_much_count: 0,
            codec_payload_type_not_found_count: 0,
            abs_capture_time_ext_id: 0,
            is_restart_pending: false,
            stream_restarted: false,
        }
    }

    pub(super) fn ssrc(&self) -> u32 {
        self.shared.params.ssrc
    }

    pub(super) fn max_video_pkts(&self) -> usize {
        self.shared.params.max_video_pkts
    }

    pub(super) fn max_audio_pkts(&self) -> usize {
        self.shared.params.max_audio_pkts
    }
}

impl Buffer {
    pub fn ssrc(&self) -> u32 {
        self.buffer_base.params.ssrc
    }

    pub fn max_video_pkts(&self) -> usize {
        self.buffer_base.params.max_video_pkts
    }

    pub fn max_audio_pkts(&self) -> usize {
        self.buffer_base.params.max_audio_pkts
    }

    pub fn set_logger(&self, lgr: Logger) {
        let mut lock = self.lock.write();
        let b = &mut lock.buffer_base;

        let mut lgr = lgr;
        for component in &b.shared.params.logger_components {
            lgr = lgr.with_component(component);
        }
        lgr = lgr.with_values(&[("ssrc", &b.shared.params.ssrc)]);
        b.logger = lgr;

        if let Some(rtp_stats) = &b.rtp_stats {
            rtp_stats.set_logger(b.logger.clone());
        }

        if let Some(rtp_stats_lite) = &b.rtp_stats_lite {
            rtp_stats_lite.set_logger(b.logger.clone());
        }
    }
}

impl BufferBase {
    pub(super) fn bind_locked(
        &mut self,
        rtp_parameters: webrtc::RtpParameters,
        codec: &webrtc::RtpCodecCapability,
        bitrate: i64,
    ) -> Result<(), Error> {
        debugw!(self.logger, "binding track");
        if codec.clock_rate == 0 {
            warnw!(self.logger, "invalid codec", nil, "rtpParameters" => rtp_parameters, "codec" => codec, "bitrate" => bitrate);
            return Err(Error::InvalidCodec);
        }

        self.setup_rtp_stats(codec.clock_rate);

        self.clock_rate = codec.clock_rate;
        self.mime = mime::normalize_mime_type(&codec.mime_type);
        self.rtp_parameters = rtp_parameters;
        for codec_parameter in &self.rtp_parameters.codecs {
            if mime::is_mime_type_string_equal(&codec_parameter.mime_type, &codec.mime_type) {
                self.payload_type = codec_parameter.payload_type;
                break;
            }
        }

        if self.payload_type == 0
            && !mime::is_mime_type_string_equal(&codec.mime_type, webrtc::MIME_TYPE_PCMU)
        {
            warnw!(
                self.logger,
                "could not find payload type for codec", nil,
                "codec" => codec.mime_type,
                "rtpParameters" => self.rtp_parameters,
            );
            self.payload_type = self.rtp_parameters.codecs[0].payload_type;
        }

        // find RTX payload type
        for codec in &self.rtp_parameters.codecs {
            if mime::is_mime_type_string_rtx(&codec.mime_type)
                && codec
                    .sdp_fmtp_line
                    .contains(&format!("apt={}", self.payload_type))
            {
                self.rtx_payload_type = codec.payload_type;
                break;
            }
        }

        // find FlexFEC payload type, the repair stream is codec agnostic (no apt)
        for codec in &self.rtp_parameters.codecs {
            // strings.EqualFold: no letter of the name has another that folds to it
            if codec
                .mime_type
                .eq_ignore_ascii_case(webrtc::MIME_TYPE_FLEX_FEC03)
            {
                self.fec_payload_type = codec.payload_type;
                break;
            }
        }

        for i in 0..self.rtp_parameters.header_extensions.len() {
            let ext = &self.rtp_parameters.header_extensions[i];
            let ext_id = ext.id;
            match ext.uri.as_str() {
                dd::EXTENSION_URI => {
                    if self.dd_ext_id != 0 {
                        warnw!(
                            self.logger,
                            "multiple dependency descriptor extensions found", nil,
                            "id" => ext_id,
                            "previous" => self.dd_ext_id,
                        );
                        continue;
                    }
                    self.dd_ext_id = ext_id as u8;
                    self.create_dd_parser_and_frame_rate_calculator();
                }

                sdp::AUDIO_LEVEL_URI => {
                    self.audio_level_ext_id = ext_id as u8;
                    let audio_level = audio::AudioLevel::new(audio::AudioLevelParams {
                        clock_rate: self.clock_rate,
                    });
                    audio_level.set_config(self.audio_level_config);
                    self.audio_level = Some(audio_level);
                }

                act::ABS_CAPTURE_TIME_URI => {
                    self.abs_capture_time_ext_id = ext_id as u8;
                }

                _ => {}
            }
        }

        if mime::is_mime_type_audio(self.mime) {
            self.codec_type = webrtc::RtpCodecType::Audio;
            self.bucket = bucket::Bucket::new(
                INIT_PACKET_BUFFER_SIZE_AUDIO,
                bucket::RTP_MAX_PKT_SIZE,
                bucket::RTP_SEQ_NUM_OFFSET,
            );
        } else if mime::is_mime_type_video(self.mime) {
            self.codec_type = webrtc::RtpCodecType::Video;
            self.bucket = bucket::Bucket::new(
                INIT_PACKET_BUFFER_SIZE_VIDEO,
                bucket::RTP_MAX_PKT_SIZE,
                bucket::RTP_SEQ_NUM_OFFSET,
            );

            if self.frame_rate_calculator[0].is_none() {
                self.create_frame_rate_calculator();
            }

            if bitrate > 0 {
                let pps = bitrate / 8 / 1200;
                let bucket = self.bucket.as_mut().expect("the bucket was just made");
                while pps > bucket.capacity() as i64 {
                    if bucket.grow() >= self.shared.params.max_video_pkts {
                        break;
                    }
                }
            }
        } else {
            self.codec_type = webrtc::RtpCodecType::Unknown;
        }

        for fb in &codec.rtcp_feedback {
            match fb.r#type.as_str() {
                webrtc::TYPE_RTCP_FB_GOOG_REMB => {
                    debugw!(self.logger, "Setting feedback", "type" => webrtc::TYPE_RTCP_FB_GOOG_REMB);
                    debugw!(
                        self.logger,
                        "REMB not supported, RTCP feedback will not be generated"
                    );
                }

                webrtc::TYPE_RTCP_FB_NACK => {
                    // pion uses a single mediaengine to manage negotiated codecs of peerconnection, that means we can't have different
                    // codec settings at track level for same codec type, so enable nack for all audio receivers but don't create nack queue
                    // for red codec.
                    if self.mime == mime::MimeType::Red {
                        continue;
                    }

                    debugw!(self.logger, "Setting feedback", "type" => webrtc::TYPE_RTCP_FB_NACK);
                    if self.nacker.is_none() {
                        self.nacker = Some(Box::new(nack::NackQueue::new(
                            nack::NACK_QUEUE_PARAMS_DEFAULT,
                        )));
                    }
                }

                _ => {}
            }
        }

        if self.nacker.is_none() && self.shared.params.is_oob_sequence_number {
            self.nacker = Some(Box::new(nack::NackQueue::new(
                nack::NACK_QUEUE_PARAMS_DEFAULT,
            )));
        }

        self.start_key_frame_seeder();

        Ok(())
    }
}

impl Buffer {
    pub fn close_with_reason(&self, reason: &str) -> Result<Option<livekit::RtpStats>, Error> {
        let mut stats = None;
        self.buffer_base.close_once.call_once(|| {
            self.buffer_base.is_closed.store(true);

            self.stop_key_frame_seeder();

            let this = {
                let mut lock = self.lock.write();
                let b = &mut lock.buffer_base;
                (stats, _) = b.stop_rtp_stats(reason);
                b.shared.read_cond.broadcast();
                b.this.clone()
            };

            // go b.flushExtPackets()
            time::spawn(async move {
                if let Some(b) = this.upgrade() {
                    b.flush_ext_packets();
                }
            });
        });
        Ok(stats)
    }

    pub fn is_closed(&self) -> bool {
        self.buffer_base.is_closed.load()
    }

    pub fn set_paused(&self, paused: bool) {
        let mut lock = self.lock.write();
        let b = &mut lock.buffer_base;

        b.is_paused = paused;
    }

    pub fn set_audio_level_config(&self, audio_level_config: audio::AudioLevelConfig) {
        let mut lock = self.lock.write();
        let b = &mut lock.buffer_base;

        b.audio_level_config = audio_level_config;
        if let Some(audio_level) = &b.audio_level {
            audio_level.set_config(b.audio_level_config);
        }
    }

    pub fn set_stream_restart_detection(&self, enable: bool) {
        let mut lock = self.lock.write();
        let b = &mut lock.buffer_base;

        b.enable_stream_restart_detection = enable;
    }
}

impl BufferBase {
    fn setup_rtp_stats(&mut self, clock_rate: u32) {
        let rtp_stats = rtpstats::RtpStatsReceiver::new(rtpstats::RtpStatsParams::default());
        rtp_stats.set_logger(self.logger.clone());
        rtp_stats.set_clock_rate(clock_rate);

        self.pps_snapshot_id = rtp_stats.new_snapshot_id();
        if self.shared.params.is_reporting_enabled {
            self.rr_snapshot_id = rtp_stats.new_snapshot_id();
            self.delta_stats_snapshot_id = rtp_stats.new_snapshot_id();
        }
        self.rtp_stats = Some(Arc::new(rtp_stats));

        self.setup_rtp_stats_lite(clock_rate);
    }

    fn setup_rtp_stats_lite(&mut self, clock_rate: u32) {
        if self.shared.params.is_oob_sequence_number {
            let rtp_stats_lite =
                rtpstats::RtpStatsReceiverLite::new(rtpstats::RtpStatsParams::default());
            rtp_stats_lite.set_logger(self.logger.clone());
            rtp_stats_lite.set_clock_rate(clock_rate);

            self.lite_stats_snapshot_id = rtp_stats_lite.new_snapshot_lite_id();
            self.rtp_stats_lite = Some(rtp_stats_lite);
        }
    }

    // Gives (stats, statsLite).
    pub(super) fn stop_rtp_stats(
        &mut self,
        reason: &str,
    ) -> (Option<livekit::RtpStats>, Option<livekit::RtpStats>) {
        let mut stats = None;
        if let Some(rtp_stats) = &self.rtp_stats {
            rtp_stats.stop();
            stats = rtp_stats.to_proto();

            debugw!(
                self.logger,
                "rtp stats",
                "direction" => "upstream",
                "stats" => rtp_stats,
                "reason" => reason,
            );
        }

        let stats_lite = self.stop_rtp_stats_lite(reason);
        (stats, stats_lite)
    }

    fn stop_rtp_stats_lite(&mut self, reason: &str) -> Option<livekit::RtpStats> {
        let mut stats_lite = None;
        if let Some(rtp_stats_lite) = &self.rtp_stats_lite {
            rtp_stats_lite.stop();
            stats_lite = rtp_stats_lite.to_proto();

            debugw!(
                self.logger,
                "rtp stats lite",
                "direction" => "upstream",
                "statsLite" => rtp_stats_lite,
                "reason" => reason,
            );
        }
        stats_lite
    }
}

impl Buffer {
    pub fn restart_oob_sequence_number(&self, reason: &str) {
        let mut lock = self.lock.write();
        let b = &mut lock.buffer_base;

        b.stop_rtp_stats_lite(reason);
        b.setup_rtp_stats_lite(b.clock_rate);

        if b.nacker.is_some() {
            b.nacker = Some(Box::new(nack::NackQueue::new(
                nack::NACK_QUEUE_PARAMS_DEFAULT,
            )));
        }
    }

    pub fn mark_for_restart_stream(&self, reason: &str) {
        let mut lock = self.lock.write();
        let b = &mut lock.buffer_base;
        debugw!(b.logger, "marking for stream restart", "reason" => reason);

        b.is_restart_pending = true;
        b.shared.read_cond.broadcast();
    }

    pub fn restart_stream(&self, reason: &str) {
        let mut lock = self.lock.write();
        debugw!(lock.buffer_base.logger, "stream restart", "reason" => reason);

        lock.buffer_base.restart_stream_locked(reason, false);
        lock.on_stream_restart_locked();
        lock.buffer_base.shared.read_cond.broadcast();
    }
}

impl BufferBase {
    fn restart_stream_locked(&mut self, reason: &str, is_detected: bool) {
        infow!(self.logger, "stream restart", "reason" => reason);

        // stop
        self.stop_key_frame_seeder();
        self.stop_rtp_stats("stream-restart");
        self.flush_ext_packets_locked();

        // restart
        self.sn_range_map = utils::RangeMap::new(100);
        self.setup_rtp_stats(self.clock_rate);

        self.bucket
            .as_mut()
            .expect("a stream that is restarted has a bucket")
            .resync_on_next_packet();
        self.last_bucket_cap_check_at = mono::unix_nano();

        if self.nacker.is_some() {
            self.nacker = Some(Box::new(nack::NackQueue::new(
                nack::NACK_QUEUE_PARAMS_DEFAULT,
            )));
        }

        if self.audio_level.is_some() {
            let audio_level = audio::AudioLevel::new(audio::AudioLevelParams {
                clock_rate: self.clock_rate,
            });
            audio_level.set_config(self.audio_level_config);
            self.audio_level = Some(audio_level);
        }

        if self.dd_ext_id != 0 {
            self.create_dd_parser_and_frame_rate_calculator();
        }

        self.frame_rate_calculated = false;
        if self.frame_rate_calculator[0].is_none() {
            self.create_frame_rate_calculator();
        }

        self.start_key_frame_seeder();

        // b.params.OnStreamRestartLocked: see the header
        self.stream_restarted = true;

        if is_detected {
            self.is_restart_pending = true;

            if let Some(f) = self.on_stream_restart.clone() {
                let reason = reason.to_string();
                time::spawn(async move { f(&reason) });
            }
        }
    }

    fn create_dd_parser_and_frame_rate_calculator(&mut self) {
        if mime::is_mime_type_svc_capable(self.mime) || self.mime == mime::MimeType::Vp8 {
            let frc = FrameRateCalculatorDd::new(self.clock_rate, self.logger.clone());
            for i in 0..self.frame_rate_calculator.len() {
                self.frame_rate_calculator[i] = Some(Box::new(
                    frc.get_frame_rate_calculator_for_spatial(i as i32),
                ));
            }
            self.dd_parser = Some(DependencyDescriptorParser::new(
                self.dd_ext_id,
                self.logger.clone(),
                move |spatial, temporal| {
                    frc.set_max_layer(spatial, temporal);
                },
                self.shared.params.is_dd_restart_enabled,
            ));
        }
    }

    fn create_frame_rate_calculator(&mut self) {
        match self.mime {
            mime::MimeType::Vp8 => {
                self.frame_rate_calculator[0] = Some(Box::new(FrameRateCalculatorVp8::new(
                    self.clock_rate,
                    self.logger.clone(),
                )));
            }

            mime::MimeType::Vp9 => {
                let frc = FrameRateCalculatorVp9::new(self.clock_rate, self.logger.clone());
                for i in 0..self.frame_rate_calculator.len() {
                    self.frame_rate_calculator[i] = Some(Box::new(
                        frc.get_frame_rate_calculator_for_spatial(i as i32),
                    ));
                }
            }

            mime::MimeType::H265 => {
                self.frame_rate_calculator[0] = Some(Box::new(FrameRateCalculatorH26x::new(
                    self.clock_rate,
                    self.logger.clone(),
                )));
            }

            _ => {}
        }
    }

    // What `ReadExtended` does with the lock held, up to where it would wait: `None` is "wait
    // for a broadcast and ask again".
    fn read_extended_locked(&mut self, buf: &mut [u8]) -> Option<Result<Option<ExtPacket>, Error>> {
        loop {
            if self.shared.is_closed.load() {
                return Some(Err(Error::Eof));
            }

            if self.is_restart_pending {
                self.is_restart_pending = false;
                return Some(Ok(None));
            }

            let ep = self.ext_packets.pop_front()?;
            if let Some(patched) = self.patch_ext_packet(ep, buf) {
                return Some(Ok(Some(patched)));
            }
        }
    }
}

impl Buffer {
    pub fn read_extended(&self, buf: &mut [u8]) -> Result<Option<ExtPacket>, Error> {
        let mut lock = self.lock.write();
        loop {
            if let Some(res) = lock.buffer_base.read_extended_locked(buf) {
                return res;
            }

            lock = self.buffer_base.read_cond.wait(&self.lock, lock);
        }
    }

    // New for musivum: `read_extended` for a task. See the header.
    pub async fn read_extended_async(&self, buf: &mut [u8]) -> Result<Option<ExtPacket>, Error> {
        loop {
            let seen = {
                let mut lock = self.lock.write();
                if let Some(res) = lock.buffer_base.read_extended_locked(buf) {
                    return res;
                }

                self.buffer_base.read_cond.generation()
            };
            self.buffer_base.read_cond.wait_async(seen).await;
        }
    }

    pub fn set_pli_throttle(&self, duration: i64) {
        let mut lock = self.lock.write();
        let b = &mut lock.buffer_base;

        b.pli_throttle = duration;
    }

    pub fn send_pli(&self, force: bool) {
        let (rtp_stats, pli_throttle) = {
            let lock = self.lock.read();
            let b = &lock.buffer_base;
            if b.codec_type != webrtc::RtpCodecType::Video {
                return;
            }

            (b.rtp_stats.clone(), b.pli_throttle)
        };

        let Some(rtp_stats) = rtp_stats else {
            return;
        };
        if !rtp_stats.check_and_update_pli(pli_throttle, force) {
            return;
        }

        // b.params.SendPLI: see the header
        self.send_pli_packet();
    }

    pub fn set_rtt(&self, rtt: u32) {
        let mut lock = self.lock.write();
        let b = &mut lock.buffer_base;

        if rtt == 0 {
            return;
        }

        if let Some(nacker) = b.nacker.as_mut() {
            nacker.set_rtt(rtt);
        }

        if let Some(rtp_stats) = &b.rtp_stats {
            rtp_stats.update_rtt(rtt);
        }
    }
}

impl BufferBase {
    // WaitRead: `ReadCond::wait`, see the header

    pub(super) fn notify_read(&self) {
        self.shared.read_cond.broadcast();
    }
}

impl Buffer {
    #[allow(clippy::too_many_arguments)]
    pub fn handle_incoming_packet(
        &self,
        raw_pkt: &[u8],
        rtp_packet: Option<&mut rtp::Packet>,
        arrival_time: i64,
        is_buffered: bool,
        is_rtx: bool,
        skipped_seqs: &[u16],
        oob_sequence_number: u16,
    ) -> Result<u64, Error> {
        let mut lock = self.lock.write();

        if self.buffer_base.is_closed.load() {
            return Err(Error::Eof);
        }

        let res = lock.buffer_base.handle_incoming_packet_locked(
            raw_pkt,
            rtp_packet,
            arrival_time,
            is_buffered,
            is_rtx,
            skipped_seqs,
            oob_sequence_number,
        );
        lock.on_stream_restart_locked();
        res
    }
}

impl BufferBase {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn handle_incoming_packet_locked(
        &mut self,
        raw_pkt: &[u8],
        rtp_packet: Option<&mut rtp::Packet>,
        arrival_time: i64,
        is_buffered: bool,
        is_rtx: bool,
        skipped_seqs: &[u16],
        oob_sequence_number: u16,
    ) -> Result<u64, Error> {
        let mut unmarshalled;
        let rtp_packet = match rtp_packet {
            Some(rtp_packet) => rtp_packet,
            None => {
                unmarshalled = rtp::Packet::default();
                if let Err(err) = unmarshalled.unmarshal(&Bytes::copy_from_slice(raw_pkt)) {
                    errorw!(self.logger, "could not unmarshal RTP packet", err);
                    return Err(err.into());
                }
                &mut unmarshalled
            }
        };

        self.process_audio_ssrc_level_header_extension(rtp_packet, arrival_time);

        if !skipped_seqs.is_empty() {
            let rtp_stats = self.rtp_stats.as_deref().expect(NOT_BOUND);
            // Use the current highest timestamp to prevent the case of old sequence number and newer timestamp.
            // It is possible that the skipped packet is older. An example sequence
            //   - Packet 10, skipped 6, 7, 9 -> Packet 8 is unknown at this point
            //   - Packet 11, skipped 8 -> this would cause sequence number to be older, but using timestamp from Packet 11 will make time stamp diff +ve
            let ts = rtp_stats.highest_timestamp();
            for &sn in skipped_seqs {
                let flow_state = rtp_stats.update(
                    arrival_time,
                    sn,
                    ts,
                    false, // no marker
                    0,     // no header for skipped packet, so 0 size
                    0,     // no payload
                    0,     // no padding
                );
                if flow_state.unhandled_reason == rtpstats::RtpFlowUnhandledReason::None
                    && !flow_state.is_out_of_order
                    && let Err(err) = self.sn_range_map.exclude_range(
                        flow_state.ext_sequence_number,
                        flow_state.ext_sequence_number.wrapping_add(1),
                    )
                {
                    errorw!(
                        self.logger,
                        "could not exclude range", err,
                        "sequenceNumber" => sn,
                        "extSequenceNumber" => flow_state.ext_sequence_number,
                        "rtpStats" => self.rtp_stats,
                        "rtpStatsLite" => self.rtp_stats_lite,
                        "snRangeMap" => self.sn_range_map,
                        "skipped" => skipped_seqs,
                    );
                }
            }
        }

        // do not start on an RTX packet
        if is_rtx && !self.rtp_stats.as_deref().expect(NOT_BOUND).is_active() {
            return Err(Error::CannotStartOnRtxPacket);
        }

        let mut flow_state = self.rtp_stats.as_deref().expect(NOT_BOUND).update(
            arrival_time,
            rtp_packet.header.sequence_number,
            rtp_packet.header.timestamp,
            rtp_packet.header.marker,
            rtp_packet.header.marshal_size(),
            rtp_packet.payload.len(),
            rtp_packet.header.padding_size as usize,
        );
        match flow_state.unhandled_reason {
            rtpstats::RtpFlowUnhandledReason::None => {}
            rtpstats::RtpFlowUnhandledReason::Restart => {
                if !self.enable_stream_restart_detection {
                    return Err(Error::UnhandledReason(flow_state.unhandled_reason));
                }

                self.restart_stream_locked("discontinuity", true);

                flow_state = self.rtp_stats.as_deref().expect(NOT_BOUND).update(
                    arrival_time,
                    rtp_packet.header.sequence_number,
                    rtp_packet.header.timestamp,
                    rtp_packet.header.marker,
                    rtp_packet.header.marshal_size(),
                    rtp_packet.payload.len(),
                    rtp_packet.header.padding_size as usize,
                );
            }
            _ => {
                return Err(Error::UnhandledReason(flow_state.unhandled_reason));
            }
        }

        if self.shared.params.is_oob_sequence_number {
            self.update_oob_nack_state(oob_sequence_number, arrival_time, raw_pkt.len());
        } else {
            self.update_nack_state(rtp_packet.sequence_number, flow_state);
        }

        if rtp_packet.payload.is_empty() && (!flow_state.is_out_of_order || flow_state.is_duplicate)
        {
            // drop padding only in-order or duplicate packet
            if !flow_state.is_out_of_order {
                // in-order packet - increment sequence number offset for subsequent packets
                // Example:
                //   40 - regular packet - pass through as sequence number 40
                //   41 - missing packet - don't know what it is, could be padding or not
                //   42 - padding only packet - in-order - drop - increment sequence number offset to 1 -
                //        range[0, 42] = 0 offset
                //   41 - arrives out of order - get offset 0 from cache - passed through as sequence number 41
                //   43 - regular packet - offset = 1 (running offset) - passes through as sequence number 42
                //   44 - padding only - in order - drop - increment sequence number offset to 2
                //        range[0, 42] = 0 offset, range[43, 44] = 1 offset
                //   43 - regular packet - out of order + duplicate - offset = 1 from cache -
                //        adjusted sequence number is 42, will be dropped by RTX buffer AddPacket method as duplicate
                //   45 - regular packet - offset = 2 (running offset) - passed through with adjusted sequence number as 43
                //   44 - padding only - out-of-order + duplicate - dropped as duplicate
                //
                if let Err(err) = self.sn_range_map.exclude_range(
                    flow_state.ext_sequence_number,
                    flow_state.ext_sequence_number.wrapping_add(1),
                ) {
                    errorw!(
                        self.logger,
                        "could not exclude range", err,
                        "sn" => rtp_packet.sequence_number,
                        "esn" => flow_state.ext_sequence_number,
                        "rtpStats" => self.rtp_stats,
                        "snRangeMap" => self.sn_range_map,
                    );
                }
            }
            return Err(Error::PaddingOnlyPacket);
        }

        if !flow_state.is_out_of_order
            && rtp_packet.payload_type != self.payload_type
            && self.codec_type == webrtc::RtpCodecType::Video
        {
            infow!(self.logger, "possible codec change", "oldPT" => self.payload_type, "receivedPT" => rtp_packet.payload_type);
            self.handle_codec_change(rtp_packet.payload_type);
        }

        // add to RTX buffer using sequence number after accounting for dropped padding only packets
        let sn_adjustment = match self.sn_range_map.get_value(flow_state.ext_sequence_number) {
            Ok(sn_adjustment) => sn_adjustment,
            Err(err) => {
                errorw!(
                    self.logger,
                    "could not get sequence number adjustment", err,
                    "sequenceNumber" => rtp_packet.sequence_number,
                    "extSequenceNumber" => flow_state.ext_sequence_number,
                    "timestamp" => rtp_packet.timestamp,
                    "extTimestamp" => flow_state.ext_timestamp,
                    "payloadSize" => rtp_packet.payload.len(),
                    "paddingSize" => rtp_packet.header.padding_size,
                    "rtpStats" => self.rtp_stats,
                    "rtpStatsLite" => self.rtp_stats_lite,
                    "snRangeMap" => self.sn_range_map,
                );
                return Err(err.into());
            }
        };

        flow_state.ext_sequence_number = flow_state.ext_sequence_number.wrapping_sub(sn_adjustment);
        rtp_packet.header.sequence_number = flow_state.ext_sequence_number as u16;
        if let Err(err) = self
            .bucket
            .as_mut()
            .expect(NO_BUCKET)
            .add_packet_with_sequence_number(raw_pkt, flow_state.ext_sequence_number)
            .map(|_| ())
        {
            if !flow_state.is_duplicate {
                if matches!(err, bucket::Error::PacketTooOld(_)) {
                    self.packet_too_old_count = self.packet_too_old_count.wrapping_add(1);
                    let packet_too_old_count = self.packet_too_old_count;
                    if packet_too_old_count.wrapping_sub(1).is_multiple_of(100) {
                        warnw!(
                            self.logger,
                            "could not add packet to bucket", err,
                            "count" => packet_too_old_count,
                            "flowState" => flow_state,
                            "snAdjustment" => sn_adjustment,
                            "incomingSequenceNumber" => flow_state.ext_sequence_number.wrapping_add(sn_adjustment),
                            "rtpStats" => self.rtp_stats,
                            "rtpStatsLite" => self.rtp_stats_lite,
                            "snRangeMap" => self.sn_range_map,
                            "skipped" => skipped_seqs,
                        );
                    }
                } else if err != bucket::Error::RtxPacket {
                    warnw!(
                        self.logger,
                        "could not add packet to bucket", err,
                        "flowState" => flow_state,
                        "snAdjustment" => sn_adjustment,
                        "incomingSequenceNumber" => flow_state.ext_sequence_number.wrapping_add(sn_adjustment),
                        "rtpStats" => self.rtp_stats,
                        "rtpStatsLite" => self.rtp_stats_lite,
                        "snRangeMap" => self.sn_range_map,
                        "skipped" => skipped_seqs,
                    );
                }
            }
            return Err(err.into());
        }

        let Some(ep) = self.get_ext_packet(rtp_packet, arrival_time, is_buffered, flow_state)
        else {
            return Err(Error::CouldNotGetExtPacket);
        };
        let ext_sequence_number = ep.ext_sequence_number;
        self.ext_packets.push_back(ep);
        self.shared.read_cond.broadcast();

        if self.ext_packets.len() > self.bucket.as_ref().expect(NO_BUCKET).capacity() {
            self.ext_packet_too_much_count = self.ext_packet_too_much_count.wrapping_add(1);
            if self
                .ext_packet_too_much_count
                .wrapping_sub(1)
                .is_multiple_of(100)
            {
                warnw!(self.logger, "too much ext packets", nil, "count" => self.ext_packets.len());
            }
        }

        self.maybe_grow_bucket(arrival_time);

        Ok(ext_sequence_number)
    }

    fn update_nack_state(&mut self, sequence_number: u16, flow_state: rtpstats::RtpFlowState) {
        let Some(nacker) = self.nacker.as_mut() else {
            return;
        };

        nacker.remove(sequence_number);

        let mut lost = flow_state.loss_start_inclusive;
        while lost != flow_state.loss_end_exclusive {
            nacker.push(lost as u16);
            lost = lost.wrapping_add(1);
        }
    }

    fn update_oob_nack_state(&mut self, sequence_number: u16, arrival_time: i64, size: usize) {
        if !self.shared.params.is_oob_sequence_number {
            return;
        }
        let Some(nacker) = self.nacker.as_mut() else {
            return;
        };

        let fs_lite = self
            .rtp_stats_lite
            .as_ref()
            .expect("a buffer with out-of-band sequence numbers has the statistics for them")
            .update(arrival_time, size, sequence_number);
        if fs_lite.is_not_handled {
            return;
        }

        nacker.remove(sequence_number);

        let mut lost = fs_lite.loss_start_inclusive;
        while lost != fs_lite.loss_end_exclusive {
            nacker.push(lost as u16);
            lost = lost.wrapping_add(1);
        }
    }

    fn process_audio_ssrc_level_header_extension(&self, p: &rtp::Packet, arrival_time: i64) {
        if self.audio_level_ext_id == 0 {
            return;
        }

        if let Some(e) = p.get_extension(self.audio_level_ext_id) {
            let mut ext = rtp::AudioLevelExtension::default();
            if ext.unmarshal(&e).is_ok() {
                self.audio_level
                    .as_ref()
                    .expect("a buffer with the audio level extension has an audio level")
                    .observe_with_rtp_timestamp(ext.level, p.timestamp, arrival_time);
            }
        }
    }

    fn handle_codec_change(&mut self, new_pt: u8) {
        let (mut codec_found, mut rtx_found) = (false, false);
        let mut rtx_pt = 0u8;
        let mut new_codec = webrtc::RtpCodecParameters::default();
        for codec in &self.rtp_parameters.codecs {
            if !codec_found && codec.payload_type == new_pt {
                new_codec = codec.clone();
                codec_found = true;
            }

            if mime::is_mime_type_string_rtx(&codec.mime_type)
                && codec.sdp_fmtp_line.contains(&format!("apt={new_pt}"))
            {
                rtx_found = true;
                rtx_pt = codec.payload_type;
            }

            if codec_found && rtx_found {
                break;
            }
        }
        if !codec_found {
            self.codec_payload_type_not_found_count =
                self.codec_payload_type_not_found_count.wrapping_add(1);
            let codec_not_found_count = self.codec_payload_type_not_found_count;
            if codec_not_found_count.wrapping_sub(1).is_multiple_of(100) {
                errorw!(
                    self.logger,
                    "could not find codec for new payload type", nil,
                    "pt" => new_pt,
                    "rtpParameters" => self.rtp_parameters,
                    "count" => codec_not_found_count,
                );
            }
            return;
        }

        infow!(
            self.logger,
            "codec changed",
            "oldPayload" => self.payload_type, "newPayload" => new_pt,
            "oldRtxPayload" => self.rtx_payload_type, "newRtxPayload" => rtx_pt,
            "oldMime" => self.mime, "newMime" => new_codec.mime_type,
        );
        self.payload_type = new_pt;
        self.rtx_payload_type = rtx_pt;
        self.mime = mime::normalize_mime_type(&new_codec.mime_type);

        if let Some(f) = self.on_codec_change.clone() {
            time::spawn(async move { f(new_codec) });
        }
    }

    fn get_ext_packet(
        &mut self,
        rtp_packet: &rtp::Packet,
        arrival_time: i64,
        is_buffered: bool,
        flow_state: rtpstats::RtpFlowState,
    ) -> Option<ExtPacket> {
        let mut ep = ExtPacket {
            arrival: arrival_time,
            ext_sequence_number: flow_state.ext_sequence_number,
            ext_timestamp: flow_state.ext_timestamp,
            packet: rtp_packet.clone(),
            video_layer: VideoLayer {
                spatial: INVALID_LAYER_SPATIAL,
                temporal: INVALID_LAYER_TEMPORAL,
            },
            is_out_of_order: flow_state.is_out_of_order,
            is_buffered,
            ..Default::default()
        };

        if ep.packet.payload.is_empty() {
            // padding only packet, nothing else to do
            return Some(ep);
        }

        if self.process_video_packet(&mut ep).is_err() {
            return None;
        }

        if self.abs_capture_time_ext_id != 0
            && let Some(ext_data) = rtp_packet.get_extension(self.abs_capture_time_ext_id)
            && !ext_data.is_empty()
        {
            let mut abs_capture_time = act::AbsCaptureTime::default();
            if abs_capture_time.unmarshal(&ext_data).is_ok() {
                ep.abs_capture_time_ext = Some(abs_capture_time);
            }
        }

        Some(ep)
    }

    fn process_video_packet(&mut self, ep: &mut ExtPacket) -> Result<(), Error> {
        if self.codec_type != webrtc::RtpCodecType::Video {
            return Ok(());
        }

        ep.video_layer.temporal = 0;
        let mut video_size: Vec<codec::VideoSize> = Vec::new();
        if let Some(dd_parser) = self.dd_parser.as_mut() {
            match dd_parser.parse(&ep.packet) {
                Err(err) => {
                    if err == Error::DdExtentionNotFound {
                        if self.mime == mime::MimeType::Vp8 || self.mime == mime::MimeType::Vp9 {
                            infow!(self.logger, "dd extension not found, disable dd parser");
                            self.dd_parser = None;
                            self.create_frame_rate_calculator();
                        }
                    } else {
                        return Err(err);
                    }
                }
                Ok((dd_val, video_layer)) => {
                    ep.video_layer = video_layer;
                    if let Some(descriptor) = &dd_val.descriptor {
                        video_size = extract_dependency_descriptor_video_size(descriptor);
                    }
                    ep.dependency_descriptor = Some(dd_val);
                    // DD-TODO : notify active decode target change if changed.
                }
            }
        }

        match self.mime {
            mime::MimeType::Vp8 => {
                let mut vp8_packet = codec::Vp8::default();
                if let Err(err) = vp8_packet.unmarshal(&ep.packet.payload) {
                    warnw!(self.logger, "could not unmarshal VP8 packet", err);
                    return Err(err.into());
                }
                ep.is_key_frame = vp8_packet.is_key_frame;
                if ep.dependency_descriptor.is_none() {
                    ep.video_layer.temporal = vp8_packet.tid as i32;

                    if ep.is_key_frame {
                        let sz = codec::extract_vp8_video_size(&vp8_packet, &ep.packet.payload);
                        if sz.width > 0 && sz.height > 0 {
                            video_size.push(sz);
                        }
                    }
                } else {
                    // vp8 with DependencyDescriptor enabled, use the TID from the descriptor
                    vp8_packet.tid = ep.video_layer.temporal as u8;
                }
                ep.vp8 = Some(vp8_packet);
                ep.video_layer.spatial = INVALID_LAYER_SPATIAL; // vp8 don't have spatial scalability, reset to invalid
            }

            mime::MimeType::Vp9 => {
                if ep.dependency_descriptor.is_none() {
                    let mut vp9_packet = codecs::Vp9Packet::default();
                    if let Err(err) = vp9_packet.unmarshal(&ep.packet.payload) {
                        warnw!(self.logger, "could not unmarshal VP9 packet", err);
                        return Err(err.into());
                    }
                    ep.video_layer = VideoLayer {
                        spatial: vp9_packet.sid as i32,
                        temporal: vp9_packet.tid as i32,
                    };
                    // codec.IsVP9KeyFrame(vp9Packet, ep.Packet.Payload): see `vp9keyframe.rs`
                    ep.is_key_frame = super::vp9_key_frame(&vp9_packet, &ep.packet.payload)?;

                    if ep.is_key_frame {
                        for i in 0..vp9_packet.width.len() {
                            video_size.push(codec::VideoSize {
                                width: vp9_packet.width[i] as u32,
                                height: vp9_packet.height[i] as u32,
                            });
                        }
                    }
                    ep.vp9 = Some(vp9_packet);
                } else {
                    ep.is_key_frame = codec::is_vp9_key_frame(None, &ep.packet.payload);
                }
            }

            mime::MimeType::H264 => {
                ep.is_key_frame = codec::is_h264_key_frame(&ep.packet.payload);
                ep.video_layer.spatial = INVALID_LAYER_SPATIAL; // h.264 don't have spatial scalability, reset to invalid

                // Check H264 key frame video size
                if ep.is_key_frame {
                    let sz = codec::extract_h264_video_size(&ep.packet.payload);
                    if sz.width > 0 && sz.height > 0 {
                        video_size.push(sz);
                    }
                }
            }

            mime::MimeType::Av1 => {
                ep.is_key_frame = codec::is_av1_key_frame(&ep.packet.payload);
            }

            mime::MimeType::H265 => {
                ep.is_key_frame = codec::is_h265_key_frame(&ep.packet.payload);
                if ep.dependency_descriptor.is_none() {
                    if ep.packet.payload.len() < 2 {
                        warnw!(self.logger, "invalid H265 packet", nil, "payloadLen" => ep.packet.payload.len());
                        return Err(Error::InvalidH265Packet);
                    }
                    ep.video_layer = VideoLayer {
                        spatial: 0,
                        temporal: (ep.packet.payload[1] & 0x07) as i32 - 1,
                    };
                    ep.video_layer.spatial = INVALID_LAYER_SPATIAL;

                    if ep.is_key_frame {
                        let sz = codec::extract_h265_video_size(&ep.packet.payload);
                        if sz.width > 0 && sz.height > 0 {
                            video_size.push(sz);
                        }
                    }
                }
            }

            _ => {}
        }

        if ep.is_key_frame {
            if let Some(rtp_stats) = &self.rtp_stats {
                rtp_stats.update_key_frame(1);
            }
            prometheus::increment_key_frames(prometheus::Direction::Incoming);
        }

        if !video_size.is_empty() {
            self.check_video_size_change(video_size);
        }

        self.do_fps_calc(ep);

        Ok(())
    }

    // Gives the packet with its bytes as they are in the bucket, which are also written to
    // `buf`, or nothing if they are not to be had: see the header.
    fn patch_ext_packet(&mut self, mut ep: ExtPacket, buf: &mut [u8]) -> Option<ExtPacket> {
        let n = match self.get_packet_locked(buf, ep.ext_sequence_number) {
            Ok(n) => n,
            Err(err) => {
                self.packet_not_found_count = self.packet_not_found_count.wrapping_add(1);
                let packet_not_found_count = self.packet_not_found_count;
                if packet_not_found_count.wrapping_sub(1).is_multiple_of(20) {
                    warnw!(
                        self.logger,
                        "could not get packet from bucket", err,
                        "sn" => ep.packet.sequence_number,
                        "headSN" => self.bucket.as_ref().map(|bucket| bucket.head_sequence_number()),
                        "count" => packet_not_found_count,
                        "rtpStats" => self.rtp_stats,
                        "rtpStatsLite" => self.rtp_stats_lite,
                        "snRangeMap" => self.sn_range_map,
                    );
                }
                return None;
            }
        };
        ep.raw_packet = Bytes::copy_from_slice(&buf[..n]);

        // parse the header again from the new buffer so extension and CSRC slices point into it,
        // then point the payload there too, ep owns its rtp.Packet
        let payload_start = match ep.packet.header.unmarshal(&ep.raw_packet) {
            Ok(payload_start) => payload_start,
            Err(err) => {
                warnw!(self.logger, "could not unmarshal header", err, "sn" => ep.packet.sequence_number);
                return None;
            }
        };
        let payload_end = payload_start + ep.packet.payload.len();
        if payload_end > n {
            warnw!(self.logger, "unexpected marshal size", nil, "max" => n, "need" => payload_end);
            return None;
        }
        ep.packet.payload = ep.raw_packet.slice(payload_start..payload_end);

        Some(ep)
    }
}

impl Buffer {
    fn flush_ext_packets(&self) {
        let mut lock = self.lock.write();
        lock.buffer_base.flush_ext_packets_locked();
    }
}

impl BufferBase {
    fn flush_ext_packets_locked(&mut self) {
        self.ext_packets.clear();
    }

    fn maybe_grow_bucket(&mut self, now: i64) {
        if now.wrapping_sub(self.last_bucket_cap_check_at) < BUCKET_CAP_CHECK_INTERVAL {
            return;
        }

        self.last_bucket_cap_check_at = now;

        // check and allocate in a go routine, away from the forwarding path
        let this = self.this.clone();
        time::spawn(async move {
            let Some(buffer) = this.upgrade() else {
                return;
            };
            let mut lock = buffer.lock.write();
            let b = &mut lock.buffer_base;

            let bucket = b.bucket.as_mut().expect(NO_BUCKET);
            let mut cap = bucket.capacity();
            let mut max_pkts = b.shared.params.max_video_pkts;
            if b.codec_type == webrtc::RtpCodecType::Audio {
                max_pkts = b.shared.params.max_audio_pkts;
            }
            if cap >= max_pkts {
                return;
            }

            let old_cap = cap;
            let rtp_stats = b.rtp_stats.as_deref().expect(NOT_BOUND);
            if let Some(delta_info) = rtp_stats.delta_info(b.pps_snapshot_id) {
                let duration = delta_info.end_time.sub(delta_info.start_time);
                if duration < 500 * time::MILLISECOND {
                    return;
                }

                // int(time.Duration(deltaInfo.Packets) * time.Second / duration)
                let pps = (delta_info.packets as i64).wrapping_mul(time::SECOND.nanoseconds())
                    / duration.nanoseconds();
                while pps > cap as i64 && cap < max_pkts {
                    cap = bucket.grow();
                }
                if cap > old_cap {
                    infow!(
                        b.logger,
                        "grow bucket",
                        "from" => old_cap,
                        "to" => cap,
                        "pps" => pps,
                        "deltaInfo" => delta_info,
                        "rtpStats" => rtp_stats,
                    );
                }
            }
        });
    }

    fn do_fps_calc(&mut self, ep: &ExtPacket) {
        if self.is_paused || self.frame_rate_calculated || ep.packet.payload.is_empty() {
            return;
        }

        let mut spatial = ep.video_layer.spatial;
        if spatial < 0 || spatial as usize >= self.frame_rate_calculator.len() {
            spatial = 0;
        }
        if let Some(fr) = self.frame_rate_calculator[spatial as usize].as_mut()
            && fr.recv_packet(ep)
        {
            let mut complete = true;
            for fr2 in self.frame_rate_calculator.iter().flatten() {
                if !fr2.completed() {
                    complete = false;
                    break;
                }
            }
            if complete {
                self.frame_rate_calculated = true;
                if let Some(f) = self.on_fps_changed.clone() {
                    time::spawn(async move { f() });
                }
            }
        }
    }
}

impl Buffer {
    pub fn set_sender_report_data(&self, sr_data: Option<&livekit::RtcpSenderReportState>) {
        let did_set = {
            let lock = self.lock.read();
            let b = &lock.buffer_base;
            let mut did_set = false;
            if let Some(rtp_stats) = &b.rtp_stats {
                did_set = rtp_stats.set_rtcp_sender_report_data(sr_data);
            }
            did_set
        };

        if did_set && let Some(cb) = self.get_on_rtcp_sender_report() {
            cb();
        }
    }

    pub fn get_sender_report_data(&self) -> Option<livekit::RtcpSenderReportState> {
        let lock = self.lock.read();
        let b = &lock.buffer_base;

        if let Some(rtp_stats) = &b.rtp_stats {
            return rtp_stats.get_rtcp_sender_report_data();
        }

        None
    }

    pub fn get_packet(&self, buff: &mut [u8], esn: u64) -> Result<usize, Error> {
        // read only, so retransmissions for many subscribers do not serialize
        let lock = self.lock.read();
        let b = &lock.buffer_base;

        b.get_packet_locked(buff, esn)
    }
}

impl BufferBase {
    fn get_packet_locked(&self, buff: &mut [u8], esn: u64) -> Result<usize, Error> {
        if self.shared.is_closed.load() {
            return Err(Error::Eof);
        }
        let Some(bucket) = &self.bucket else {
            return Err(bucket::Error::NoBucket.into());
        };
        Ok(bucket.get_packet(buff, esn)?)
    }
}

impl Buffer {
    pub fn get_stats(&self) -> Option<livekit::RtpStats> {
        let lock = self.lock.read();
        let b = &lock.buffer_base;

        b.rtp_stats.as_ref()?.to_proto()
    }

    pub fn get_delta_stats(&self) -> Option<StreamStatsWithLayers> {
        let lock = self.lock.read();
        let b = &lock.buffer_base;

        let delta_stats = Arc::new(
            b.rtp_stats
                .as_ref()?
                .delta_info(b.delta_stats_snapshot_id)?,
        );

        Some(StreamStatsWithLayers {
            rtp_stats: Some(delta_stats.clone()),
            layers: HashMap::from([(0, Some(delta_stats))]),
            ..Default::default()
        })
    }

    pub fn get_delta_stats_lite(&self) -> Option<rtpstats::RtpDeltaInfoLite> {
        let lock = self.lock.read();
        let b = &lock.buffer_base;

        b.rtp_stats_lite
            .as_ref()?
            .delta_info_lite(b.lite_stats_snapshot_id)
    }

    pub fn get_last_sender_report_time(&self) -> Time {
        let lock = self.lock.read();
        let b = &lock.buffer_base;

        let Some(rtp_stats) = &b.rtp_stats else {
            return Time::ZERO;
        };

        rtp_stats.last_sender_report_time()
    }

    pub fn get_audio_level(&self) -> (f64, bool) {
        let lock = self.lock.read();
        let b = &lock.buffer_base;

        let Some(audio_level) = &b.audio_level else {
            return (0.0, false);
        };

        audio_level.get_level(mono::unix_nano())
    }

    pub fn on_rtcp_sender_report(&self, f: impl Fn() + Send + Sync + 'static) {
        let mut lock = self.lock.write();
        lock.buffer_base.on_rtcp_sender_report = Some(Arc::new(f));
    }

    fn get_on_rtcp_sender_report(&self) -> Option<OnRtcpSenderReport> {
        let lock = self.lock.read();
        let b = &lock.buffer_base;

        b.on_rtcp_sender_report.clone()
    }

    pub fn on_fps_changed(&self, f: impl Fn() + Send + Sync + 'static) {
        let mut lock = self.lock.write();
        lock.buffer_base.on_fps_changed = Some(Arc::new(f));
    }

    pub fn on_video_size_changed(&self, f: impl Fn(Vec<codec::VideoSize>) + Send + Sync + 'static) {
        let mut lock = self.lock.write();
        lock.buffer_base.on_video_size_changed = Some(Arc::new(f));
    }

    pub fn on_codec_change(&self, f: impl Fn(webrtc::RtpCodecParameters) + Send + Sync + 'static) {
        let mut lock = self.lock.write();
        lock.buffer_base.on_codec_change = Some(Arc::new(f));
    }

    pub fn on_stream_restart(&self, f: impl Fn(&str) + Send + Sync + 'static) {
        let mut lock = self.lock.write();
        lock.buffer_base.on_stream_restart = Some(Arc::new(f));
    }
}

impl BufferBase {
    // checkVideoSizeChange checks if video size has changed for a specific spatial layer and fires callback
    fn check_video_size_change(&mut self, mut video_sizes: Vec<codec::VideoSize>) {
        if video_sizes.len() > self.current_video_size.len() {
            warnw!(
                self.logger,
                "video size index out of range", nil,
                "newSize" => video_sizes,
                "currentVideoSize" => self.current_video_size,
            );
            return;
        }

        if video_sizes.len() < self.current_video_size.len() {
            video_sizes.resize(self.current_video_size.len(), codec::VideoSize::default());
        }

        let mut changed = false;
        for (i, sz) in video_sizes.iter().enumerate() {
            if self.current_video_size[i].width != sz.width
                || self.current_video_size[i].height != sz.height
            {
                changed = true;
                break;
            }
        }

        if changed {
            debugw!(self.logger, "video size changed", "from" => self.current_video_size, "to" => video_sizes);
            self.current_video_size.copy_from_slice(&video_sizes);
            if let Some(f) = self.on_video_size_changed.clone() {
                time::spawn(async move { f(video_sizes) });
            }
        }
    }
}

impl Buffer {
    pub fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32> {
        let lock = self.lock.read();
        let b = &lock.buffer_base;

        if layer as i64 >= b.frame_rate_calculator.len() as i64 {
            return Vec::new();
        }

        // a negative layer is an index out of range, as in the original
        if let Some(fc) = &b.frame_rate_calculator[layer as usize] {
            return fc.get_frame_rate();
        }
        Vec::new()
    }

    pub fn start_key_frame_seeder(&self) {
        let lock = self.lock.read();
        lock.buffer_base.start_key_frame_seeder();
    }

    pub fn stop_key_frame_seeder(&self) {
        self.buffer_base.key_frame_seeder_generation.inc();
    }
}

impl BufferBase {
    fn start_key_frame_seeder(&self) {
        if self.codec_type == webrtc::RtpCodecType::Video {
            time::spawn(Buffer::seed_key_frame(
                self.this.clone(),
                self.shared.clone(),
                self.shared.key_frame_seeder_generation.inc(),
            ));
        }
    }

    fn stop_key_frame_seeder(&self) {
        self.shared.key_frame_seeder_generation.inc();
    }
}

impl Buffer {
    // The task holds the buffer weakly, and what the buffer shares without its lock: the
    // seeder ends at the tick after the buffer was closed or dropped.
    async fn seed_key_frame(
        this: Weak<Buffer>,
        shared: Arc<BufferBaseShared>,
        key_frame_seeder_generation: i32,
    ) {
        // a key frame is needed especially when using Dependency Descriptor
        // to get the DD structure which is used in parsing subsequent packets,
        // till then packets are dropped which results in stream tracker not
        // getting any data which means it does not declare layer start.
        //
        // send gratuitous PLIs for some time or until a key frame is seen to
        // get the engine rolling
        let timer = tokio::time::sleep((30 * time::SECOND).to_std());
        tokio::pin!(timer);

        let period = time::SECOND.to_std();
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let (rtp_stats, lgr) = {
            let Some(b) = this.upgrade() else {
                return;
            };
            let lock = b.lock.read();
            (
                lock.buffer_base.rtp_stats.clone(),
                lock.buffer_base.logger.clone(),
            )
        };
        debugw!(lgr, "starting key frame seeder", "generation" => key_frame_seeder_generation);
        let Some(rtp_stats) = rtp_stats else {
            debugw!(lgr, "cannot do key frame seeding without stats", "generation" => key_frame_seeder_generation);
            return;
        };
        let (initial_count, _) = rtp_stats.key_frame();

        loop {
            if shared.is_closed.load()
                || shared.key_frame_seeder_generation.load() != key_frame_seeder_generation
            {
                debugw!(
                    lgr,
                    "stopping key frame seeder: stopped",
                    "generation" => key_frame_seeder_generation,
                    "currentGeneration" => shared.key_frame_seeder_generation.load(),
                );
                return;
            }

            tokio::select! {
                _ = &mut timer => {
                    debugw!(lgr, "stopping key frame seeder: timeout", "generation" => key_frame_seeder_generation);
                    return;
                }

                _ = ticker.tick() => {
                    let (cnt, last) = rtp_stats.key_frame();
                    if cnt > initial_count {
                        debugw!(
                            lgr,
                            "stopping key frame seeder: received key frame",
                            "generation" => key_frame_seeder_generation,
                            "keyFrameCountInitial" => initial_count,
                            "keyFrameCount" => cnt,
                            "lastKeyFrame" => last,
                        );
                        return;
                    }

                    let Some(b) = this.upgrade() else {
                        return;
                    };
                    b.send_pli(false);
                }
            }
        }
    }

    pub fn get_nack_pairs(&self) -> Vec<NackPair> {
        let mut lock = self.lock.write();
        let b = &mut lock.buffer_base;

        b.get_nack_pairs_locked()
    }
}

impl BufferBase {
    pub(super) fn get_nack_pairs_locked(&mut self) -> Vec<NackPair> {
        let Some(nacker) = self.nacker.as_mut() else {
            return Vec::new();
        };

        let (pairs, num_seq_nums_nacked) = nacker.pairs();
        if !self.shared.params.is_oob_sequence_number {
            if let Some(rtp_stats) = &self.rtp_stats {
                rtp_stats.update_nack(num_seq_nums_nacked as u32);
            }
        } else if let Some(rtp_stats_lite) = &self.rtp_stats_lite {
            rtp_stats_lite.update_nack(num_seq_nums_nacked as u32);
        }

        pairs
    }

    pub(super) fn get_rtcp_reception_report_locked(
        &self,
        proxy_loss: u8,
    ) -> Option<ReceptionReport> {
        self.rtp_stats.as_ref()?.get_rtcp_reception_report(
            self.shared.params.ssrc,
            proxy_loss,
            self.rr_snapshot_id,
        )
    }
}

// what the original stops on with a nil pointer: see the header
const NOT_BOUND: &str = "a buffer that is handed packets is bound: it has statistics";
const NO_BUCKET: &str =
    "a buffer that is handed packets was bound to audio or video: it has a bucket";

// ---------------------------------------------------------------
