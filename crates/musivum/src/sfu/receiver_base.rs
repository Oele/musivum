// Ported from LiveKit, pkg/sfu/receiver_base.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The receiver is made as an `Arc` and keeps a `Weak` of itself (`this`): it is what it
//   hands to its stream tracker manager as listener, to its down tracks on a restart, to a
//   RED receiver as the receiver it wraps, and what `get_red_receiver` and
//   `get_primary_receiver_for_red` return where the original returns `r`. The methods of the
//   original are in one `impl`, in the original's order; the traits (`TrackReceiver`,
//   `StreamTrackerManagerListener`) are implemented at the end of the file by handing each
//   call to the method of the same name.
// - The locks are the original's, each with what it guards in a struct of its own:
//   `codec_state_lock`, `buffer_mu` (the buffers, their promises, the track info, and also
//   `rtt`, `on_max_layer_change`, the forwarders' wait group and `restart_in_progress`, which
//   the original reads and writes under it) and `video_size_mu`. What the options of the
//   receiver set (`pliThrottleConfig`, `audioConfig`, `enableRTPStreamRestartDetection`,
//   `lbThreshold`, `forwardStats`: plain fields upstream, written without a lock before the
//   receiver is used) is behind a lock of its own, `opts`, since the setters take `&self`.
// - `ErrReceiverClosed` and the other three errors are variants of the package's `Error`
//   (`errors.rs`). `read_rtp` gives the errors of the buffer as the package's: `Error::Eof`
//   for a buffer that is closed (the original's `io.EOF`, which the down track looks for),
//   `Error::Bucket` for what the bucket says, and `Error::Buffer` for anything else.
// - The config structs have their YAML names as doc comments, as nothing reads a
//   configuration file yet.
// - `ReceiverBaseParams`: the listener is a `Weak` (it is the receiver that owns this one),
//   the two functions are boxed, and `on_new_buffer_needed` gets the track info shared and
//   gives any error, as the original's is an `error` of whoever provides the buffers.
// - `bufferPromise.ready`, a channel that is closed, is a flag with a condition variable; the
//   `sync.WaitGroup` of the forwarders is `WaitGroup` here, a counter with a condition
//   variable. Its `wait` blocks the thread as the original blocks the goroutine, and tells
//   the runtime so when the thread is a worker of a multi-threaded one
//   (`tokio::task::block_in_place`). On a current-thread runtime a restart would wait for
//   forwarders that the waiting thread itself has to run: the receiver needs the
//   multi-threaded runtime.
// - The forwarder (`forward_rtp`) is a task per buffer that awaits the buffer's
//   `read_extended_async`. It holds the buffer, and the receiver only weakly: it takes the
//   receiver when a read returns and lets go of it before the next read, so that a receiver
//   that nobody else holds is dropped while its forwarders wait for packets, and a forwarder
//   that finds its receiver gone ends. It logs with a copy of the receiver's logger. What the
//   original defers is written out after the loop, which is a labelled block that `return`
//   of the original breaks out of; `wg.Done()` is a guard, so that it also happens when the
//   task is dropped. `buffer.ReleaseExtPacket` has no counterpart.
// - The callbacks that the receiver gives a buffer capture the receiver and the buffer
//   weakly (the receiver holds the buffer, the buffer the callbacks). `OnRtcpSenderReport`:
//   a sender report that is absent is not handed to the down tracks, as theirs is a
//   reference; the buffer calls back after it has set one.
// - The RED transformer is an `atomic::Pointer` of the trait object, as in the original. It
//   holds this receiver strongly (see `redreceiver.rs`), so `close` takes it out of its
//   field, under `buffer_mu` so that `get_red_receiver` cannot store a new one behind its
//   back, before it closes it: a closed receiver has no transformer, where the original
//   keeps the closed one (which has no down tracks left and forwards nothing). The type
//   assertions of `GetPrimaryReceiverForRed` and `GetRedReceiver` (`rt.(*RedReceiver)`), with
//   the `nil` they return for a transformer of the other kind, are gone: `is_red` never
//   changes, so a receiver's transformer is always of the one kind that it makes.
// - A track info is never nil here: `new` takes a reference, and the receiver keeps it in an
//   `Arc` that `track_info` hands out, where the original clones the message.
// - `on_max_layer_change`, `add_on_codec_state_change` and `on_video_size_changed` keep their
//   callbacks in `Arc`s, so that they are called with no lock held, as the original does.
// - `set_codec_with_state` logs the states as the numbers the original logs.
// - `debug_info` gives the JSON object of `interfaces.rs`.
// - `close_track_senders`: a goroutine per sender is a scoped thread per sender, and the end
//   of the scope is the wait that the `sync.WaitGroup` is.

use std::sync::{Arc, Weak};

use parking_lot::{Condvar, Mutex, RwLock};

use crate::go::atomic;
use crate::go::time::{self, Duration};
use crate::mediatransportutil::bucket;
use crate::mediatransportutil::codec;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils;
use crate::protocol::utils::mono;
use crate::sfu::audio;
use crate::sfu::buffer;
use crate::sfu::rtpstats;
use crate::sfu::streamtracker;
use crate::sfu::utils as sfuutils;
use crate::sfu::{
    DebugInfo, Error, ForwardStats, RedPrimaryReceiver, RedReceiver, RedTransformer,
    StreamTrackerManager, StreamTrackerManagerConfig, StreamTrackerManagerListener, TrackReceiver,
    TrackSender,
};
use crate::webrtc;
use crate::{debugw, errorw, infow, warnw};

// ErrReceiverClosed, ErrDownTrackAlreadyExist, ErrDuplicateLayer and ErrInvalidLayer are
// `Error::ReceiverClosed`, `Error::DownTrackAlreadyExist`, `Error::DuplicateLayer` and
// `Error::InvalidLayer`

// --------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PliThrottleConfig {
    /// `low_quality` in YAML.
    pub low_quality: Duration,
    /// `mid_quality` in YAML.
    pub mid_quality: Duration,
    /// `high_quality` in YAML.
    pub high_quality: Duration,
}

pub const DEFAULT_PLI_THROTTLE_CONFIG: PliThrottleConfig = PliThrottleConfig {
    low_quality: Duration::from_millis(500),
    mid_quality: time::SECOND,
    high_quality: time::SECOND,
};

// --------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AudioConfig {
    /// Inline in YAML: its fields are fields of this section.
    pub audio_level_config: audio::AudioLevelConfig,

    // enable red encoding downtrack for opus only audio up track
    /// `active_red_encoding` in YAML.
    pub active_red_encoding: bool,
    // enable proxying weakest subscriber loss to publisher in RTCP Receiver Report
    /// `enable_loss_proxying` in YAML.
    pub enable_loss_proxying: bool,
}

pub const DEFAULT_AUDIO_CONFIG: AudioConfig = AudioConfig {
    audio_level_config: audio::DEFAULT_AUDIO_LEVEL_CONFIG,
    active_red_encoding: false,
    enable_loss_proxying: false,
};

// --------------------------------------

pub type Bitrates = [[i64; (buffer::DEFAULT_MAX_LAYER_TEMPORAL + 1) as usize];
    (buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize];

// --------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ReceiverCodecState {
    #[default]
    Normal,
    Suspended,
    Invalid,
}

// --------------------------------------

// `len(r.buffers)` of the original
const NUM_SPATIAL_LAYERS: usize = (buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize;

struct BufferPromise {
    // `ready chan struct{}`: true once it is closed
    ready: Mutex<bool>,
    cond: Condvar,
}

impl BufferPromise {
    fn new() -> Arc<BufferPromise> {
        Arc::new(BufferPromise {
            ready: Mutex::new(false),
            cond: Condvar::new(),
        })
    }

    // <-bp.ready
    fn wait(&self) {
        let mut ready = self.ready.lock();
        while !*ready {
            self.cond.wait(&mut ready);
        }
    }
}

// `defer close(bp.ready)`
struct BufferPromiseReady(Arc<BufferPromise>);

impl Drop for BufferPromiseReady {
    fn drop(&mut self) {
        *self.0.ready.lock() = true;
        self.0.cond.notify_all();
    }
}

// Go's `sync.WaitGroup`, for the forwarders: see the header.
struct WaitGroup {
    count: Mutex<usize>,
    cond: Condvar,
}

impl WaitGroup {
    fn new() -> Arc<WaitGroup> {
        Arc::new(WaitGroup {
            count: Mutex::new(0),
            cond: Condvar::new(),
        })
    }

    fn add(&self, delta: usize) {
        *self.count.lock() += delta;
    }

    fn done(&self) {
        let mut count = self.count.lock();
        *count -= 1;
        if *count == 0 {
            self.cond.notify_all();
        }
    }

    fn wait(&self) {
        if *self.count.lock() == 0 {
            return;
        }

        let wait = || {
            let mut count = self.count.lock();
            while *count != 0 {
                self.cond.wait(&mut count);
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(wait)
            }
            _ => wait(),
        }
    }
}

// `defer wg.Done()`
struct WaitGroupDone(Arc<WaitGroup>);

impl Drop for WaitGroupDone {
    fn drop(&mut self) {
        self.0.done();
    }
}

/// `OnNewBufferNeeded` of the original.
pub type OnNewBufferNeeded = Box<
    dyn Fn(
            i32,
            Arc<livekit::TrackInfo>,
        )
            -> Result<Arc<dyn buffer::BufferProvider>, Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Sync,
>;

#[derive(Default)]
pub struct ReceiverBaseParams {
    pub track_id: livekit::TrackId,
    pub stream_id: String,
    pub kind: webrtc::RtpCodecType,
    pub codec: webrtc::RtpCodecParameters,
    pub header_extensions: Vec<webrtc::RtpHeaderExtensionParameter>,
    pub logger: Logger,
    pub stream_tracker_manager_config: StreamTrackerManagerConfig,
    pub stream_tracker_manager_listener: Option<Weak<dyn StreamTrackerManagerListener>>,
    pub is_self_closing: bool,
    pub on_new_buffer_needed: Option<OnNewBufferNeeded>,
    pub on_closed: Option<Box<dyn Fn() + Send + Sync>>,
}

type OnCodecStateChange = Arc<dyn Fn(webrtc::RtpCodecParameters, ReceiverCodecState) + Send + Sync>;
type OnMaxLayerChange = Arc<dyn Fn(mime::MimeType, i32) + Send + Sync>;
type OnVideoSizeChanged = Arc<dyn Fn() + Send + Sync>;

pub struct ReceiverBase {
    params: ReceiverBaseParams,

    // what the options of the receiver set: see the header
    opts: RwLock<ReceiverBaseOpts>,

    codec_state_lock: Mutex<ReceiverBaseCodecState>,

    is_red: bool,
    video_layer_mode: livekit::video_layer::Mode,

    buffer_mu: RwLock<ReceiverBaseBuffers>,

    video_size_mu: RwLock<ReceiverBaseVideoSizes>,

    stream_tracker_manager: Arc<StreamTrackerManager>,

    down_track_spreader: sfuutils::DownTrackSpreader<Arc<dyn TrackSender>>,

    red_transformer: atomic::Pointer<Arc<dyn RedTransformer>>,

    forwarders_generation: atomic::Uint32,

    is_closed: atomic::Bool,

    drop_payload_type_mismatch_count: atomic::Uint32,

    // the receiver itself: see the header
    this: Weak<ReceiverBase>,
}

#[derive(Default)]
struct ReceiverBaseOpts {
    pli_throttle_config: PliThrottleConfig,
    audio_config: AudioConfig,
    enable_rtp_stream_restart_detection: bool,
    lb_threshold: usize,
    forward_stats: Option<Arc<ForwardStats>>,
}

// What `codecStateLock` guards.
struct ReceiverBaseCodecState {
    codec_state: ReceiverCodecState,
    on_codec_state_change: Vec<OnCodecStateChange>,
}

// What `bufferMu` guards.
struct ReceiverBaseBuffers {
    buffers: [Option<Arc<dyn buffer::BufferProvider>>; NUM_SPATIAL_LAYERS],
    buffer_promises: [Option<Arc<BufferPromise>>; NUM_SPATIAL_LAYERS],
    track_info: Arc<livekit::TrackInfo>,

    rtt: u32,

    on_max_layer_change: Option<OnMaxLayerChange>,

    forwarders_wait_group: Arc<WaitGroup>,
    restart_in_progress: bool,
}

// What `videoSizeMu` guards.
#[derive(Default)]
struct ReceiverBaseVideoSizes {
    video_sizes: [codec::VideoSize; NUM_SPATIAL_LAYERS],
    on_video_size_changed: Option<OnVideoSizeChanged>,
}

impl ReceiverBase {
    pub fn new(
        params: ReceiverBaseParams,
        track_info: &livekit::TrackInfo,
        codec_state: ReceiverCodecState,
    ) -> Arc<ReceiverBase> {
        let opts = ReceiverBaseOpts::default();
        let mime_type = mime::normalize_mime_type(&params.codec.mime_type);

        let down_track_spreader =
            sfuutils::DownTrackSpreader::new(sfuutils::DownTrackSpreaderParams {
                threshold: opts.lb_threshold,
                logger: params.logger.clone(),
            });

        let stream_tracker_manager = StreamTrackerManager::new(
            params.logger.clone(),
            track_info,
            mime_type,
            params.codec.clock_rate,
            &params.stream_tracker_manager_config,
        );

        let r = Arc::new_cyclic(|this: &Weak<ReceiverBase>| ReceiverBase {
            opts: RwLock::new(opts),
            codec_state_lock: Mutex::new(ReceiverBaseCodecState {
                codec_state,
                on_codec_state_change: Vec::new(),
            }),
            is_red: mime::is_mime_type_string_red(&params.codec.mime_type),
            video_layer_mode: buffer::get_video_layer_mode_for_mime_type(
                mime_type,
                Some(track_info),
            ),
            buffer_mu: RwLock::new(ReceiverBaseBuffers {
                buffers: Default::default(),
                buffer_promises: Default::default(),
                track_info: Arc::new(track_info.clone()),
                rtt: 0,
                on_max_layer_change: None,
                forwarders_wait_group: WaitGroup::new(),
                restart_in_progress: false,
            }),
            video_size_mu: RwLock::new(ReceiverBaseVideoSizes::default()),
            stream_tracker_manager,
            down_track_spreader,
            red_transformer: atomic::Pointer::default(),
            forwarders_generation: atomic::Uint32::default(),
            is_closed: atomic::Bool::default(),
            drop_payload_type_mismatch_count: atomic::Uint32::default(),
            params,
            this: this.clone(),
        });

        let listener: Weak<dyn StreamTrackerManagerListener> = r.this.clone();
        r.stream_tracker_manager.set_listener(Some(listener));

        r.start_forwarders_generation();

        r
    }

    // `r` of the original, where it is handed out. The receiver is alive, as it is being
    // called.
    fn this(&self) -> Arc<ReceiverBase> {
        self.this
            .upgrade()
            .expect("a receiver that is called is held by somebody")
    }

    pub fn close(&self, reason: &str, clear_buffers: bool) {
        if self.is_closed.swap(true) {
            return;
        }

        if clear_buffers {
            self.clear_all_buffers(reason);
        }
        self.stream_tracker_manager.close();

        close_track_senders(&self.down_track_spreader.close_and_get_down_tracks());

        // taken out of its field, as it holds this receiver: see the header
        let rt = {
            let _b = self.buffer_mu.write();
            self.red_transformer.swap(None)
        };
        if let Some(rt) = rt {
            rt.close();
        }

        if let Some(on_closed) = &self.params.on_closed {
            on_closed();
        }
    }

    pub fn can_close(&self) -> bool {
        if self.is_closed() {
            return true;
        }

        if self.down_track_spreader.down_track_count() != 0 {
            return false;
        }

        if let Some(rt) = self.load_red_transformer() {
            return rt.can_close();
        }

        true
    }

    pub fn set_pli_throttle_config(&self, pli_throttle_config: PliThrottleConfig) {
        self.opts.write().pli_throttle_config = pli_throttle_config;
    }

    pub fn set_audio_config(&self, audio_config: AudioConfig) {
        self.opts.write().audio_config = audio_config;
    }

    pub fn set_enable_rtp_stream_restart_detection(
        &self,
        enable_rtp_strem_restart_detection: bool,
    ) {
        self.opts.write().enable_rtp_stream_restart_detection = enable_rtp_strem_restart_detection;
    }

    pub fn set_lb_threshold(&self, lb_threshold: usize) {
        self.opts.write().lb_threshold = lb_threshold;
        self.down_track_spreader.set_threshold(lb_threshold);
        if let Some(rt) = self.load_red_transformer() {
            rt.set_lb_threshold(lb_threshold);
        }
    }

    pub fn set_forward_stats(&self, forward_stats: Option<Arc<ForwardStats>>) {
        self.opts.write().forward_stats = forward_stats;
    }

    // `r.forwardStats` of the original, read
    fn forward_stats(&self) -> Option<Arc<ForwardStats>> {
        self.opts.read().forward_stats.clone()
    }

    // `r.lbThreshold` of the original, read
    fn lb_threshold(&self) -> usize {
        self.opts.read().lb_threshold
    }

    pub fn logger(&self) -> &Logger {
        &self.params.logger
    }

    pub fn track_info(&self) -> Arc<livekit::TrackInfo> {
        let b = self.buffer_mu.read();

        b.track_info.clone()
    }

    pub fn update_track_info(&self, ti: &livekit::TrackInfo) {
        let mut b = self.buffer_mu.write();
        let existing_version = utils::timed_version_from_proto(b.track_info.version.as_ref());
        let update_version = utils::timed_version_from_proto(ti.version.as_ref());
        if update_version.compare(existing_version) < 0 {
            let existing = b.track_info.clone();
            drop(b);
            debugw!(
                self.params.logger,
                "not updating to older version",
                "existing" => existing,
                "updated" => ti,
            );
            return;
        }

        let should_resync = utils::timed_version_from_proto(b.track_info.version.as_ref())
            != utils::timed_version_from_proto(ti.version.as_ref());
        if should_resync {
            debugw!(
                self.params.logger,
                "updating track info",
                "existing" => b.track_info,
                "updated" => ti,
                "shouldResync" => should_resync,
            );
        }
        b.track_info = Arc::new(ti.clone());

        let paused = b.track_info.muted;
        for buff in b.buffers.iter().flatten() {
            buff.set_paused(paused);
        }
        drop(b);

        self.stream_tracker_manager.update_track_info(Some(ti));

        if should_resync {
            self.restart("update-track-info");
        }
    }

    pub fn restart(&self, reason: &str) {
        infow!(self.params.logger, "restarting receiver", "reason" => reason);
        self.restart_internal(reason, false);
    }

    fn restart_internal(&self, reason: &str, is_detected: bool) {
        debugw!(
            self.params.logger,
            "restart receiver",
            "reason" => reason,
            "isDetected" => is_detected,
            "isClosed" => self.is_closed(),
        );

        if self.is_closed() {
            return;
        }

        // 1. guard against concurrent restarts
        {
            let mut b = self.buffer_mu.write();
            if b.restart_in_progress {
                debugw!(self.params.logger, "restart receiver, skipping duplicate");
                return;
            }
            b.restart_in_progress = true;

            // 2. advance forwarder generation
            self.forwarders_generation.inc();
            debugw!(
                self.params.logger,
                "restart receiver, advanced forwarder generation",
                "forwardersGeneration" => self.forwarders_generation.load(),
            );
        }

        // 3. mark for restart all the buffers
        // if a stream restart was detected, skip external restart
        //
        // NOTE: The case of external restart and detected restart (which usually comes from one buffer)
        //       racing will miss restart on all buffers if detected restart from one buffer adds the guard
        //       against concurrent restart. But, that condition should be very rare if at all.
        //       External restart happens when the underlying track changes or when seeking
        if !is_detected {
            for (layer, buff) in self.get_all_buffers().iter().enumerate() {
                let Some(buff) = buff else {
                    continue;
                };

                debugw!(self.params.logger, "restart receiver, marking buffer for restart", "layer" => layer);
                buff.mark_for_restart_stream(reason);
            }
            debugw!(
                self.params.logger,
                "restart receiver, marked buffers for restart"
            );
        }

        // 4. wait for the forwarders to finish
        self.wait_for_forwarders_stop();
        debugw!(self.params.logger, "restart receiver, forwarders stopped");

        // 5. restart all the buffers
        // Two phase restart - mark, followed by restart to ensure
        // a fresh start after existing forwarder is stopped
        if !is_detected {
            for (layer, buff) in self.get_all_buffers().iter().enumerate() {
                let Some(buff) = buff else {
                    continue;
                };

                debugw!(self.params.logger, "restart receiver, restarting buffer", "layer" => layer);
                buff.restart_stream(reason);
            }
            debugw!(self.params.logger, "restart receiver, restarted buffers");
        }

        // 6. reset stream tracker
        self.stream_tracker_manager.remove_all_trackers();
        debugw!(
            self.params.logger,
            "restart receiver, stream trackers removed"
        );

        // 7. signal attached downtracks to resync so that they can have proper sequencing on a receiver restart
        let r: Arc<dyn TrackReceiver> = self.this();
        self.down_track_spreader.broadcast(|dt| {
            dt.receiver_restart(r.clone());
        });
        if let Some(rt) = self.load_red_transformer() {
            rt.on_stream_restart();
        }
        debugw!(
            self.params.logger,
            "restart receiver, down tracks signalled"
        );

        // 8. move forwarder generation ahead
        self.start_forwarders_generation();
        debugw!(
            self.params.logger,
            "restart receiver, restarted forwarder generation",
            "forwardersGeneration" => self.forwarders_generation.load(),
        );

        let mut b = self.buffer_mu.write();
        // 9. release restart hold
        b.restart_in_progress = false;

        // 10. restart forwarders
        for (layer, buff) in b.buffers.clone().into_iter().enumerate() {
            let Some(buff) = buff else {
                continue;
            };

            debugw!(self.params.logger, "restart receiver, restarting forwarder", "layer" => layer);
            self.start_forwarder_for_buffer_locked(&b, layer as i32, buff);
        }
        debugw!(self.params.logger, "restart receiver, restarted forwarders");
    }

    pub fn on_max_layer_change(&self, f: impl Fn(mime::MimeType, i32) + Send + Sync + 'static) {
        let mut b = self.buffer_mu.write();
        b.on_max_layer_change = Some(Arc::new(f));
    }

    fn get_on_max_layer_change(&self) -> Option<OnMaxLayerChange> {
        let b = self.buffer_mu.read();

        b.on_max_layer_change.clone()
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load()
    }

    pub fn set_rtt(&self, rtt: u32) {
        let buffers = {
            let mut b = self.buffer_mu.write();
            if b.rtt == rtt || rtt == 0 {
                return;
            }

            b.rtt = rtt;
            b.buffers.clone()
        };

        for buff in buffers.iter().flatten() {
            buff.set_rtt(rtt);
        }
    }

    pub fn track_id(&self) -> livekit::TrackId {
        self.params.track_id.clone()
    }

    pub fn stream_id(&self) -> String {
        self.params.stream_id.clone()
    }

    pub fn codec(&self) -> webrtc::RtpCodecParameters {
        self.params.codec.clone()
    }

    pub fn mime(&self) -> mime::MimeType {
        mime::normalize_mime_type(&self.params.codec.mime_type)
    }

    pub fn video_layer_mode(&self) -> livekit::video_layer::Mode {
        self.video_layer_mode
    }

    pub fn header_extensions(&self) -> Vec<webrtc::RtpHeaderExtensionParameter> {
        self.params.header_extensions.clone()
    }

    pub fn kind(&self) -> webrtc::RtpCodecType {
        self.params.kind
    }

    pub fn stream_tracker_manager(&self) -> &Arc<StreamTrackerManager> {
        &self.stream_tracker_manager
    }

    pub fn add_down_track(&self, track: Arc<dyn TrackSender>) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::ReceiverClosed);
        }

        if self
            .down_track_spreader
            .has_down_track(&track.subscriber_id())
        {
            infow!(
                self.params.logger,
                "subscriberID already exists, replacing downtrack",
                "subscriberID" => track.subscriber_id(),
            );
        }

        track.up_track_max_published_layer_change(
            self.stream_tracker_manager.get_max_published_layer(),
        );
        track.up_track_max_temporal_layer_seen_change(
            self.stream_tracker_manager.get_max_temporal_layer_seen(),
        );

        if !self.down_track_spreader.try_store(track.clone()) {
            return Err(Error::ReceiverClosed);
        }
        debugw!(self.params.logger, "downtrack added", "subscriberID" => track.subscriber_id());
        Ok(())
    }

    pub fn delete_down_track(&self, subscriber_id: &livekit::ParticipantId) {
        self.down_track_spreader.free(subscriber_id);
        debugw!(self.params.logger, "downtrack deleted", "subscriberID" => subscriber_id);
    }

    pub fn get_down_tracks(&self) -> Vec<Arc<dyn TrackSender>> {
        let mut down_tracks = self.down_track_spreader.get_down_tracks().to_vec();
        if let Some(rt) = self.load_red_transformer() {
            down_tracks.extend(rt.get_down_tracks());
        }
        down_tracks
    }

    pub fn set_max_expected_spatial_layer(&self, layer: i32) {
        let prev_max = self
            .stream_tracker_manager
            .set_max_expected_spatial_layer(layer);
        debugw!(self.params.logger, "max expected layer change", "layer" => layer, "prevMax" => prev_max);

        let b = self.buffer_mu.read();
        // stop key frame seeders of stopped layers
        let mut idx = layer.wrapping_add(1);
        while idx <= prev_max {
            if let Some(buff) = &b.buffers[idx as usize] {
                buff.stop_key_frame_seeder();
            }
            idx += 1;
        }

        // start key frame seeders of newly expected layers
        let mut idx = prev_max.wrapping_add(1);
        while idx <= layer {
            if let Some(buff) = &b.buffers[idx as usize] {
                buff.start_key_frame_seeder();
            }
            idx += 1;
        }
    }

    // `r.params.StreamTrackerManagerListener` of the original, if it is still there
    fn stream_tracker_manager_listener(&self) -> Option<Arc<dyn StreamTrackerManagerListener>> {
        self.params
            .stream_tracker_manager_listener
            .as_ref()
            .and_then(Weak::upgrade)
    }

    // StreamTrackerManagerListener.OnAvailableLayersChanged
    pub fn on_available_layers_changed(&self) {
        self.down_track_spreader.broadcast(|dt| {
            dt.up_track_layers_change();
        });

        if let Some(listener) = self.stream_tracker_manager_listener() {
            listener.on_available_layers_changed();
        }
    }

    // StreamTrackerManagerListener.OnBitrateAvailabilityChanged
    pub fn on_bitrate_availability_changed(&self) {
        self.down_track_spreader.broadcast(|dt| {
            dt.up_track_bitrate_availability_change();
        });

        if let Some(listener) = self.stream_tracker_manager_listener() {
            listener.on_bitrate_availability_changed();
        }
    }

    // StreamTrackerManagerListener.OnMaxPublishedLayerChanged
    pub fn on_max_published_layer_changed(&self, max_published_layer: i32) {
        self.down_track_spreader.broadcast(|dt| {
            dt.up_track_max_published_layer_change(max_published_layer);
        });

        if let Some(listener) = self.stream_tracker_manager_listener() {
            listener.on_max_published_layer_changed(max_published_layer);
        }
    }

    // StreamTrackerManagerListener.OnMaxTemporalLayerSeenChanged
    pub fn on_max_temporal_layer_seen_changed(&self, max_temporal_layer_seen: i32) {
        self.down_track_spreader.broadcast(|dt| {
            dt.up_track_max_temporal_layer_seen_change(max_temporal_layer_seen);
        });

        if let Some(listener) = self.stream_tracker_manager_listener() {
            listener.on_max_temporal_layer_seen_changed(max_temporal_layer_seen);
        }
    }

    // StreamTrackerManagerListener.OnMaxAvailableLayerChanged
    pub fn on_max_available_layer_changed(&self, max_available_layer: i32) {
        if let Some(on_max_layer_change) = self.get_on_max_layer_change() {
            on_max_layer_change(self.mime(), max_available_layer);
        }

        if let Some(listener) = self.stream_tracker_manager_listener() {
            listener.on_max_available_layer_changed(max_available_layer);
        }
    }

    // StreamTrackerManagerListener.OnBitrateReport
    pub fn on_bitrate_report(&self, available_layers: &[i32], bitrates: Bitrates) {
        self.down_track_spreader.broadcast(|dt| {
            dt.up_track_bitrate_report(available_layers, bitrates);
        });

        if let Some(listener) = self.stream_tracker_manager_listener() {
            listener.on_bitrate_report(available_layers, bitrates);
        }
    }

    pub fn get_layered_bitrate(&self) -> (Vec<i32>, Bitrates) {
        self.stream_tracker_manager.get_layered_bitrate()
    }

    pub fn send_pli(&self, layer: i32, force: bool) {
        // SVC-TODO :  should send LRR (Layer Refresh Request) instead of PLI
        let Some(buff) = self.get_or_create_buffer(layer) else {
            return;
        };

        buff.send_pli(force);
    }

    fn get_buffer(&self, layer: i32) -> (Option<Arc<dyn buffer::BufferProvider>>, i32) {
        let b = self.buffer_mu.read();

        self.get_buffer_locked(&b, layer)
    }

    fn get_buffer_locked(
        &self,
        b: &ReceiverBaseBuffers,
        mut layer: i32,
    ) -> (Option<Arc<dyn buffer::BufferProvider>>, i32) {
        // for svc codecs, use layer = 0 always.
        // spatial layers are in-built and handled by single buffer
        if self.video_layer_mode == livekit::video_layer::Mode::MultipleSpatialLayersPerStream {
            layer = 0;
        }

        if layer < 0 || layer as usize >= b.buffers.len() {
            return (None, layer);
        }

        (b.buffers[layer as usize].clone(), layer)
    }

    pub fn get_or_create_buffer(&self, layer: i32) -> Option<Arc<dyn buffer::BufferProvider>> {
        let mut b = self.buffer_mu.write();

        if self.is_closed() {
            return None;
        }

        let (buff, layer) = self.get_buffer_locked(&b, layer);
        if buff.is_some() {
            return buff;
        }

        let on_new_buffer_needed = self.params.on_new_buffer_needed.as_ref()?;

        // a layer that no buffer can be for stops here, as in the original
        if let Some(bp) = b.buffer_promises[layer as usize].clone() {
            drop(b);
            bp.wait();

            let (buff, _) = self.get_buffer(layer);
            return buff;
        }

        let bp = BufferPromise::new();
        b.buffer_promises[layer as usize] = Some(bp.clone());

        let ti = b.track_info.clone();
        drop(b);

        let _ready = BufferPromiseReady(bp);

        let buff = match on_new_buffer_needed(layer, ti) {
            Ok(buff) => buff,
            Err(err) => {
                errorw!(self.params.logger, "could not create buffer", err);

                let mut b = self.buffer_mu.write();
                b.buffer_promises[layer as usize] = None;
                drop(b);

                return None;
            }
        };

        let (rtt, paused) = {
            let mut b = self.buffer_mu.write();
            b.buffers[layer as usize] = Some(buff.clone());
            (b.rtt, b.track_info.muted)
        };

        self.setup_buffer(&buff, layer, rtt, paused);
        Some(buff)
    }

    fn setup_buffer(
        &self,
        buff: &Arc<dyn buffer::BufferProvider>,
        layer: i32,
        rtt: u32,
        paused: bool,
    ) {
        let (audio_config, enable_rtp_stream_restart_detection, pli_throttle_config) = {
            let opts = self.opts.read();
            (
                opts.audio_config,
                opts.enable_rtp_stream_restart_detection,
                opts.pli_throttle_config,
            )
        };

        buff.set_logger(self.params.logger.with_values(&[("layer", &layer)]));
        buff.set_audio_level_config(audio_config.audio_level_config);
        buff.set_stream_restart_detection(enable_rtp_stream_restart_detection);
        {
            let this = self.this.clone();
            let weak_buff = Arc::downgrade(buff);
            buff.on_rtcp_sender_report(Box::new(move || {
                let (Some(r), Some(buff)) = (this.upgrade(), weak_buff.upgrade()) else {
                    return;
                };
                // a report that is absent: see the header
                let Some(sr_data) = buff.get_sender_report_data() else {
                    return;
                };
                r.down_track_spreader.broadcast(|dt| {
                    let _ = dt.handle_rtcp_sender_report_data(
                        r.params.codec.payload_type,
                        layer,
                        &sr_data,
                    );
                });

                if let Some(rt) = r.load_red_transformer() {
                    rt.forward_rtcp_sender_report(r.params.codec.payload_type, layer, &sr_data);
                }
            }));
        }
        {
            let this = self.this.clone();
            buff.on_video_size_changed(Box::new(move |video_size: Vec<codec::VideoSize>| {
                let Some(r) = this.upgrade() else {
                    return;
                };
                let cb = {
                    let mut v = r.video_size_mu.write();
                    if r.video_layer_mode
                        == livekit::video_layer::Mode::MultipleSpatialLayersPerStream
                    {
                        let n = v.video_sizes.len().min(video_size.len());
                        v.video_sizes[..n].copy_from_slice(&video_size[..n]);
                    } else {
                        v.video_sizes[layer as usize] = video_size[0];
                    }
                    debugw!(r.params.logger, "video size changed", "size" => v.video_sizes);
                    v.on_video_size_changed.clone()
                };

                if let Some(cb) = cb {
                    cb();
                }
            }));
        }
        if self.kind() == webrtc::RtpCodecType::Video && layer == 0 {
            let this = self.this.clone();
            buff.on_codec_change(Box::new(move |new_codec: webrtc::RtpCodecParameters| {
                if let Some(r) = this.upgrade() {
                    r.handle_codec_change(new_codec);
                }
            }));
        }
        {
            let this = self.this.clone();
            buff.on_stream_restart(Box::new(move |reason: &str| {
                if let Some(r) = this.upgrade() {
                    r.restart_internal(reason, true);
                }
            }));
        }

        let duration = match layer {
            2 => pli_throttle_config.high_quality,
            1 => pli_throttle_config.mid_quality,
            0 => pli_throttle_config.low_quality,
            _ => pli_throttle_config.mid_quality,
        };
        if duration != Duration::ZERO {
            buff.set_pli_throttle(duration.nanoseconds());
        }

        buff.set_rtt(rtt);
        buff.set_paused(paused);
    }

    pub fn add_buffer(&self, buff: Arc<dyn buffer::BufferProvider>, layer: i32) {
        let (rtt, paused) = {
            let mut b = self.buffer_mu.write();
            b.buffers[layer as usize] = Some(buff.clone());
            (b.rtt, b.track_info.muted)
        };

        self.setup_buffer(&buff, layer, rtt, paused);
    }

    pub fn start_buffer(&self, buff: Arc<dyn buffer::BufferProvider>, layer: i32) {
        let b = self.buffer_mu.write();
        self.start_forwarder_for_buffer_locked(&b, layer, buff);
    }

    pub fn get_all_buffers(&self) -> [Option<Arc<dyn buffer::BufferProvider>>; NUM_SPATIAL_LAYERS] {
        let b = self.buffer_mu.read();

        b.buffers.clone()
    }

    pub fn clear_all_buffers(&self, reason: &str) {
        let buffers = {
            let mut b = self.buffer_mu.write();
            let buffers = std::mem::take(&mut b.buffers);
            for bp in b.buffer_promises.iter_mut() {
                *bp = None;
            }
            buffers
        };

        for buff in buffers.iter().flatten() {
            let _ = buff.close_with_reason(reason);
        }

        self.stream_tracker_manager.remove_all_trackers();
    }

    pub fn read_rtp(&self, buf: &mut [u8], layer: u8, esn: u64) -> Result<usize, Error> {
        let (b, _) = self.get_buffer(layer as i32);
        let Some(b) = b else {
            return Err(bucket::Error::PacketMismatch(String::new()).into());
        };

        b.get_packet(buf, esn).map_err(|err| match err {
            buffer::Error::Eof => Error::Eof,
            buffer::Error::Bucket(err) => Error::Bucket(err),
            err => Error::Buffer(err),
        })
    }

    pub fn get_track_stats(&self) -> Option<livekit::RtpStats> {
        let b = self.buffer_mu.read();

        let mut all_stats = Vec::with_capacity(b.buffers.len());
        for buff in b.buffers.iter().flatten() {
            let Some(stats) = buff.get_stats() else {
                continue;
            };

            all_stats.push(stats);
        }

        rtpstats::aggregate_rtp_stats(&all_stats)
    }

    pub fn get_audio_level(&self) -> (f64, bool) {
        if self.kind() == webrtc::RtpCodecType::Video {
            return (0.0, false);
        }

        let b = self.buffer_mu.read();

        if let Some(buff) = b.buffers.iter().flatten().next() {
            return buff.get_audio_level();
        }

        (0.0, false)
    }

    fn start_forwarders_generation(&self) {
        let mut b = self.buffer_mu.write();

        self.forwarders_generation.inc();
        b.forwarders_wait_group = WaitGroup::new();
    }

    fn wait_for_forwarders_stop(&self) {
        let forwarders_wait_group = {
            let b = self.buffer_mu.write();
            b.forwarders_wait_group.clone()
        };

        forwarders_wait_group.wait();
    }

    fn start_forwarder_for_buffer_locked(
        &self,
        b: &ReceiverBaseBuffers,
        layer: i32,
        buff: Arc<dyn buffer::BufferProvider>,
    ) {
        if b.restart_in_progress {
            debugw!(self.params.logger, "restart in progress, deferring starting forwarder", "layer" => layer);
            return;
        }

        b.forwarders_wait_group.add(1);

        let forwarder_generation = self.forwarders_generation.load();
        debugw!(self.params.logger, "starting forwarder", "layer" => layer, "forwarderGeneration" => forwarder_generation);
        time::spawn(Self::forward_rtp(
            self.this.clone(),
            self.params.logger.clone(),
            layer,
            buff,
            forwarder_generation,
            b.forwarders_wait_group.clone(),
        ));
    }

    async fn forward_rtp(
        this: Weak<ReceiverBase>,
        logger: Logger,
        layer: i32,
        buff: Arc<dyn buffer::BufferProvider>,
        forwarder_generation: u32,
        wg: Arc<WaitGroup>,
    ) {
        let _done = WaitGroupDone(wg);

        let mut err: Option<buffer::Error> = None;

        let mut num_packets_forwarded: usize = 0;
        let mut num_packets_dropped: usize = 0;

        'forwarder: {
            let mut spatial_trackers: [Option<Arc<dyn streamtracker::StreamTrackerWorker>>;
                NUM_SPATIAL_LAYERS] = Default::default();
            if layer < 0 || layer as usize >= spatial_trackers.len() {
                errorw!(logger, "invalid layer", nil, "layer" => layer);
                break 'forwarder;
            }

            let mut pkt_buf = vec![0u8; bucket::RTP_MAX_PKT_SIZE];
            let Some(mut r) = this.upgrade() else {
                break 'forwarder;
            };
            debugw!(
                logger,
                "starting forwarding",
                "layer" => layer,
                "forwarderGeneration" => forwarder_generation,
                "forwardersGeneration" => r.forwarders_generation.load(),
            );

            while r.forwarders_generation.load() == forwarder_generation {
                // the receiver is not held while the forwarder waits for a packet: see the
                // header
                drop(r);
                let (ext_pkt, read_err) = match buff.read_extended_async(&mut pkt_buf).await {
                    Ok(ext_pkt) => (ext_pkt, None),
                    Err(err) => (None, Some(err)),
                };
                err = read_err;
                if err == Some(buffer::Error::Eof) {
                    break 'forwarder;
                }
                let Some(upgraded) = this.upgrade() else {
                    break 'forwarder;
                };
                r = upgraded;
                let Some(ext_pkt) = ext_pkt else {
                    continue;
                };
                let dequeued_at = mono::unix_nano();

                if ext_pkt.packet.payload_type != r.params.codec.payload_type {
                    // drop packets as we don't support codec fallback directly
                    let drop_payload_type_mismatch_count = r.drop_payload_type_mismatch_count.inc();
                    if drop_payload_type_mismatch_count.wrapping_sub(1) % 100 == 0 {
                        debugw!(
                            logger,
                            "dropping packet - payload mismatch",
                            "packetPayloadType" => ext_pkt.packet.payload_type,
                            "payloadType" => r.params.codec.payload_type,
                            "count" => drop_payload_type_mismatch_count,
                        );
                    }
                    num_packets_dropped += 1;
                    continue;
                }

                let mut spatial_layer = layer;
                if ext_pkt.video_layer.spatial >= 0
                    && !sfuutils::is_simulcast_mode(r.video_layer_mode)
                {
                    // svc packet, take spatial layer info from packet
                    spatial_layer = ext_pkt.video_layer.spatial;
                }
                if spatial_layer as i64 >= spatial_trackers.len() as i64 {
                    errorw!(
                        logger,
                        "unexpected spatial layer", nil,
                        "spatialLayer" => spatial_layer,
                        "pktSpatialLayer" => ext_pkt.video_layer.spatial,
                    );
                    num_packets_dropped += 1;
                    continue;
                }

                let mut write_count =
                    sfuutils::broadcast_rtp(&r.down_track_spreader, &ext_pkt, spatial_layer);
                if let Some(rt) = r.load_red_transformer() {
                    write_count = write_count.wrapping_add(rt.forward_rtp(&ext_pkt, spatial_layer));
                }

                // track delay/jitter
                //
                // Out-of-order packets (retransmissions/late arrivals) are excluded. They
                // tend to arrive in bursts (e.g. a NACK triggers a batch of retransmissions
                // delivered back-to-back) which the single forwarder goroutine drains
                // serially, inflating the measured transit for the tail of the burst. That
                // reflects loss recovery rather than steady-state forwarding health.
                if write_count > 0
                    && !ext_pkt.is_buffered
                    && !ext_pkt.is_out_of_order
                    && let Some(forward_stats) = r.forward_stats()
                {
                    let (latency, is_high) =
                        forward_stats.update(ext_pkt.arrival, mono::unix_nano());
                    if is_high {
                        debugw!(
                            logger,
                            "high forwarding latency",
                            "latency" => Duration(latency),
                            "queuingLatency" => Duration(dequeued_at.wrapping_sub(ext_pkt.arrival)),
                            "writeCount" => write_count,
                            "isOutOfOrder" => ext_pkt.is_out_of_order,
                            "layer" => layer,
                        );
                    }
                }

                // track video layers
                if r.kind() == webrtc::RtpCodecType::Video {
                    let tracker = &mut spatial_trackers[spatial_layer as usize];
                    if tracker.is_none() {
                        *tracker = r.stream_tracker_manager.get_tracker(spatial_layer);
                        if tracker.is_none() {
                            if r.video_layer_mode
                                == livekit::video_layer::Mode::MultipleSpatialLayersPerStream
                                && ext_pkt.dependency_descriptor.is_some()
                            {
                                r.stream_tracker_manager
                                    .add_dependency_descriptor_trackers();
                            }
                            *tracker = r.stream_tracker_manager.add_tracker(spatial_layer);
                        }
                    }
                    if let Some(tracker) = tracker {
                        tracker.observe(
                            ext_pkt.video_layer.temporal,
                            ext_pkt.raw_packet.len(),
                            ext_pkt.packet.payload.len(),
                            ext_pkt.packet.marker,
                            ext_pkt.packet.timestamp,
                            ext_pkt.dependency_descriptor.as_ref(),
                        );
                    }
                }

                num_packets_forwarded += 1;
            }
        }

        // what the original defers
        let r = this.upgrade();
        if err == Some(buffer::Error::Eof)
            && let Some(r) = &r
            && r.params.is_self_closing
        {
            r.close("forwarder-done", false);

            r.stream_tracker_manager.remove_tracker(layer);
            if r.video_layer_mode == livekit::video_layer::Mode::MultipleSpatialLayersPerStream {
                r.stream_tracker_manager.remove_all_trackers();
            }
        }

        debugw!(
            logger,
            "closing forwarder",
            "layer" => layer,
            "numPacketsForwarded" => num_packets_forwarded,
            "numPacketsDropped" => num_packets_dropped,
            "forwarderGeneration" => forwarder_generation,
            "forwardersGeneration" => r.as_ref().map(|r| r.forwarders_generation.load()),
            "error" => err,
        );
    }

    pub fn debug_info(&self) -> DebugInfo {
        let video_layer_mode =
            buffer::get_video_layer_mode_for_mime_type(self.mime(), Some(&self.track_info()));
        let mut info = DebugInfo::new();
        info.insert("Mime".to_string(), self.mime().to_string().into());
        info.insert(
            "VideoLayerMode".to_string(),
            video_layer_mode.as_str_name().into(),
        );

        info
    }

    pub fn get_primary_receiver_for_red(&self) -> Arc<dyn TrackReceiver> {
        let _b = self.buffer_mu.write();

        if !self.is_red || self.is_closed() {
            return self.this();
        }

        match self.load_red_transformer() {
            None => {
                let pr = RedPrimaryReceiver::new(
                    self.this(),
                    sfuutils::DownTrackSpreaderParams {
                        threshold: self.lb_threshold(),
                        logger: self.params.logger.clone(),
                    },
                );
                let rt: Arc<dyn RedTransformer> = pr.clone();
                self.red_transformer.store(Some(Arc::new(rt)));
                pr
            }
            // a `RedPrimaryReceiver`: see the header
            Some(rt) => rt,
        }
    }

    pub fn get_red_receiver(&self) -> Arc<dyn TrackReceiver> {
        let _b = self.buffer_mu.write();

        if self.is_red || self.is_closed() {
            return self.this();
        }

        match self.load_red_transformer() {
            None => {
                let pr = RedReceiver::new(
                    self.this(),
                    sfuutils::DownTrackSpreaderParams {
                        threshold: self.lb_threshold(),
                        logger: self.params.logger.clone(),
                    },
                );
                let rt: Arc<dyn RedTransformer> = pr.clone();
                self.red_transformer.store(Some(Arc::new(rt)));
                pr
            }
            // a `RedReceiver`: see the header
            Some(rt) => rt,
        }
    }

    pub fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32> {
        let (b, _) = self.get_buffer(layer);
        let Some(b) = b else {
            return Vec::new();
        };

        if self.video_layer_mode != livekit::video_layer::Mode::MultipleSpatialLayersPerStream {
            return b.get_temporal_layer_fps_for_spatial(0);
        }

        b.get_temporal_layer_fps_for_spatial(layer)
    }

    pub fn add_on_ready(&self, f: impl Fn()) {
        // receiver is always ready after created
        f();
    }

    fn handle_codec_change(&self, _new_codec: webrtc::RtpCodecParameters) {
        // codec fallback is not supported mid-session, i.e. change of codec via payload type change,
        // set the codec state to invalid once it happens
        self.set_codec_state(ReceiverCodecState::Invalid);
    }

    pub fn add_on_codec_state_change(
        &self,
        f: impl Fn(webrtc::RtpCodecParameters, ReceiverCodecState) + Send + Sync + 'static,
    ) {
        let mut c = self.codec_state_lock.lock();
        c.on_codec_state_change.push(Arc::new(f));
    }

    pub fn codec_state(&self) -> ReceiverCodecState {
        let c = self.codec_state_lock.lock();

        c.codec_state
    }

    pub fn set_codec_state(&self, state: ReceiverCodecState) {
        let fns = {
            let mut c = self.codec_state_lock.lock();
            if c.codec_state == state || c.codec_state == ReceiverCodecState::Invalid {
                return;
            }

            c.codec_state = state;
            c.on_codec_state_change.clone()
        };

        for f in fns {
            f(self.params.codec.clone(), state);
        }
    }

    // SetCodecWithState updates the codec state of the receiver. It returns true if the
    // given codec is incompatible with the codec the receiver was created with, see
    // `checkCodecChanged`.
    pub fn set_codec_with_state(
        &self,
        codec: &webrtc::RtpCodecParameters,
        header_extensions: &[webrtc::RtpHeaderExtensionParameter],
        codec_state: ReceiverCodecState,
    ) -> bool {
        let incompatible = self.check_codec_changed(codec, header_extensions);

        let mut c = self.codec_state_lock.lock();
        if codec_state == c.codec_state {
            return incompatible;
        }

        let on_codec_state_change = c.on_codec_state_change.clone();
        infow!(
            self.params.logger,
            "codec state changed",
            "from" => c.codec_state as i32,
            "to" => codec_state as i32,
        );
        let (fire_change, reason) = match codec_state {
            ReceiverCodecState::Normal => {
                // TODO: support codec recovery
                return incompatible;
            }

            // The original sets the reason to "codec suspended" for a suspended codec and
            // falls through to the case of an invalid one, which sets it again.
            ReceiverCodecState::Suspended | ReceiverCodecState::Invalid => {
                c.codec_state = codec_state;
                (true, "codec invalid")
            }
        };
        drop(c);

        if fire_change {
            self.clear_all_buffers(reason);

            for f in on_codec_state_change {
                f(self.params.codec.clone(), codec_state);
            }
        }

        incompatible
    }

    fn check_codec_changed(
        &self,
        codec: &webrtc::RtpCodecParameters,
        header_extensions: &[webrtc::RtpHeaderExtensionParameter],
    ) -> bool {
        let mut existing_fmtp: Vec<&str> = self.params.codec.sdp_fmtp_line.split(';').collect();
        existing_fmtp.sort();
        let mut check_fmtp: Vec<&str> = codec.sdp_fmtp_line.split(';').collect();
        check_fmtp.sort();
        let mut incompatible = false;
        if !mime::is_mime_type_string_equal(&self.params.codec.mime_type, &codec.mime_type)
            || existing_fmtp != check_fmtp
            || self.params.codec.clock_rate != codec.clock_rate
            || self.params.codec.payload_type != codec.payload_type
        {
            let err = format!(
                "mime: {} -> {}, fmtp: {} -> {}, clockRate: {} -> {}, pt: {} -> {}",
                self.params.codec.mime_type,
                codec.mime_type,
                self.params.codec.sdp_fmtp_line,
                codec.sdp_fmtp_line,
                self.params.codec.clock_rate,
                codec.clock_rate,
                self.params.codec.payload_type,
                codec.payload_type,
            );
            warnw!(self.params.logger, "unexpected change in codec", err);
            incompatible = self.params.codec.payload_type != codec.payload_type;
        }

        if self.params.header_extensions.len() != header_extensions.len() {
            let err = format!(
                "extensions: {} -> {}",
                self.params.header_extensions.len(),
                header_extensions.len()
            );
            errorw!(
                self.params.logger,
                "unexpected change in extensions length",
                err
            );
        }

        incompatible
    }

    pub fn video_sizes(&self) -> Vec<codec::VideoSize> {
        let mut sizes = Vec::new();
        let v = self.video_size_mu.read();
        for v in v.video_sizes.iter() {
            if v.width == 0 || v.height == 0 {
                break;
            }
            sizes.push(*v);
        }

        sizes
    }

    pub fn on_video_size_changed(&self, f: impl Fn() + Send + Sync + 'static) {
        let mut v = self.video_size_mu.write();
        v.on_video_size_changed = Some(Arc::new(f));
    }

    pub(super) fn load_red_transformer(&self) -> Option<Arc<dyn RedTransformer>> {
        if let Some(rt) = self.red_transformer.load() {
            return Some((*rt).clone());
        }

        None
    }
}

// -----------------------------------------------------------

// closes all track senders in parallel, returns when all are closed
pub(super) fn close_track_senders(senders: &[Arc<dyn TrackSender>]) {
    std::thread::scope(|s| {
        for dt in senders {
            s.spawn(move || {
                dt.close();
            });
        }
    });
}

// -----------------------------------------------------------

// StreamTrackerManagerListener: the methods of the original that carry its name

impl StreamTrackerManagerListener for ReceiverBase {
    fn on_available_layers_changed(&self) {
        ReceiverBase::on_available_layers_changed(self)
    }

    fn on_bitrate_availability_changed(&self) {
        ReceiverBase::on_bitrate_availability_changed(self)
    }

    fn on_max_published_layer_changed(&self, max_published_layer: i32) {
        ReceiverBase::on_max_published_layer_changed(self, max_published_layer)
    }

    fn on_max_temporal_layer_seen_changed(&self, max_temporal_layer_seen: i32) {
        ReceiverBase::on_max_temporal_layer_seen_changed(self, max_temporal_layer_seen)
    }

    fn on_max_available_layer_changed(&self, max_available_layer: i32) {
        ReceiverBase::on_max_available_layer_changed(self, max_available_layer)
    }

    fn on_bitrate_report(&self, available_layers: &[i32], bitrates: Bitrates) {
        ReceiverBase::on_bitrate_report(self, available_layers, bitrates)
    }
}

// TrackReceiver: the receiver is one upstream by having the methods

impl TrackReceiver for ReceiverBase {
    fn track_id(&self) -> livekit::TrackId {
        ReceiverBase::track_id(self)
    }

    fn stream_id(&self) -> String {
        ReceiverBase::stream_id(self)
    }

    fn codec(&self) -> webrtc::RtpCodecParameters {
        ReceiverBase::codec(self)
    }

    fn mime(&self) -> mime::MimeType {
        ReceiverBase::mime(self)
    }

    fn video_layer_mode(&self) -> livekit::video_layer::Mode {
        ReceiverBase::video_layer_mode(self)
    }

    fn header_extensions(&self) -> Vec<webrtc::RtpHeaderExtensionParameter> {
        ReceiverBase::header_extensions(self)
    }

    fn is_closed(&self) -> bool {
        ReceiverBase::is_closed(self)
    }

    fn read_rtp(&self, buf: &mut [u8], layer: u8, esn: u64) -> Result<usize, Error> {
        ReceiverBase::read_rtp(self, buf, layer, esn)
    }

    fn get_layered_bitrate(&self) -> (Vec<i32>, Bitrates) {
        ReceiverBase::get_layered_bitrate(self)
    }

    fn get_audio_level(&self) -> (f64, bool) {
        ReceiverBase::get_audio_level(self)
    }

    fn send_pli(&self, layer: i32, force: bool) {
        ReceiverBase::send_pli(self, layer, force)
    }

    fn set_max_expected_spatial_layer(&self, layer: i32) {
        ReceiverBase::set_max_expected_spatial_layer(self, layer)
    }

    fn add_down_track(&self, track: Arc<dyn TrackSender>) -> Result<(), Error> {
        ReceiverBase::add_down_track(self, track)
    }

    fn delete_down_track(&self, participant_id: &livekit::ParticipantId) {
        ReceiverBase::delete_down_track(self, participant_id)
    }

    fn get_down_tracks(&self) -> Vec<Arc<dyn TrackSender>> {
        ReceiverBase::get_down_tracks(self)
    }

    fn debug_info(&self) -> DebugInfo {
        ReceiverBase::debug_info(self)
    }

    fn track_info(&self) -> Arc<livekit::TrackInfo> {
        ReceiverBase::track_info(self)
    }

    fn update_track_info(&self, ti: &livekit::TrackInfo) {
        ReceiverBase::update_track_info(self, ti)
    }

    fn get_primary_receiver_for_red(&self) -> Arc<dyn TrackReceiver> {
        ReceiverBase::get_primary_receiver_for_red(self)
    }

    fn get_red_receiver(&self) -> Arc<dyn TrackReceiver> {
        ReceiverBase::get_red_receiver(self)
    }

    fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32> {
        ReceiverBase::get_temporal_layer_fps_for_spatial(self, layer)
    }

    fn get_track_stats(&self) -> Option<livekit::RtpStats> {
        ReceiverBase::get_track_stats(self)
    }

    fn add_on_ready(&self, f: Box<dyn Fn() + Send + Sync>) {
        ReceiverBase::add_on_ready(self, f)
    }

    fn add_on_codec_state_change(
        &self,
        f: Box<dyn Fn(webrtc::RtpCodecParameters, ReceiverCodecState) + Send + Sync>,
    ) {
        ReceiverBase::add_on_codec_state_change(self, f)
    }

    fn codec_state(&self) -> ReceiverCodecState {
        ReceiverBase::codec_state(self)
    }

    fn video_sizes(&self) -> Vec<codec::VideoSize> {
        ReceiverBase::video_sizes(self)
    }

    fn restart(&self, reason: &str) {
        ReceiverBase::restart(self, reason)
    }
}
