// Ported from LiveKit, pkg/sfu/downtrack.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `NewDownTrack` is `DownTrack::new` and gives an `Arc`; the down track keeps a `Weak` of
//   itself, which is what its workers, the callbacks it registers (the receiver's "ready",
//   the RTCP readers, the connection statistics) and the provider it hands to the
//   connection statistics hold. Its public methods are inherent ones, in the original's
//   order; the two traits it implements (`TrackSender`, `webrtc::TrackLocal`) pass on to
//   them at the end of the file.
// - Who holds whom. The down track holds its receiver (`Arc`): it reads from it for every
//   NACK and asks it for key frames. The receiver holds its down tracks and lets go of one
//   when that down track closes (`delete_down_track`, as in the original): that is the side
//   that is dropped, so a closed down track is free, and one that is still with its
//   receiver is held by it. The first receiver stays in `params` after `set_receiver`, as
//   in the original. The listener, the stream allocator listener and the transceiver own
//   the down track, directly or through the connection, and are held as `Weak`
//   (`set_transceiver` takes the `Arc` and keeps a `Weak` of it): one that is gone is one
//   that was not set. The pacer and the connection statistics are held by `Arc` and hold
//   nothing of the down track (the statistics hold their provider weakly).
// - Locks: each lock of the original is here for what it guards there. What `bindLock`
//   guards and nothing else reads is `DownTrackBind`. The fields that the original writes
//   under `bindLock` and reads without any lock on the packet path (the two SSRCs, the
//   payload types for RED, the ids of the header extensions, the write stream, the
//   sequencer) are atomics here, as are the two snapshot ids; a channel with the mutex and
//   the flag that guard its closing is a lock around the sending half, `None` when closed.
// - `Bind` defers part of its work in a closure (`doBind`): here it is the method
//   `do_bind`, and what the closure captures is the struct `DoBind`, kept in
//   `bind_on_receiver_ready` until the receiver is ready. `bind` returns the package's
//   error; the trait's `bind` gives its text as `webrtc::Error::Other`. `on_binding` is
//   given a `Result` where the original passes an error or nil.
// - Goroutines are tasks. The three workers (key frame requester, max layer notifier,
//   retransmit worker) hold the receiving half of their channel and the down track weakly:
//   they end when the down track closes, and also when it is dropped without `close`. The
//   max layer notifier holds the listener weakly too, for its last notification. The blank
//   frame writer and the padding on mute look the down track up at every tick. `go
//   d.Close()`, `go d.setRTPHeaderExtensions()` and `go listener.OnBindAndConnected()` are
//   tasks that run once.
// - `close_with_flush` waits for the blank frames (up to `FLUSH_TIMEOUT`) by blocking its
//   thread, as the original blocks its goroutine. On a multi-threaded runtime it tells the
//   runtime so (`block_in_place`): the writer was started from this thread and waits in
//   this worker's own queue, where no other worker would find it before the wait is over.
//   On a current-thread runtime the writer cannot run meanwhile: the wait ends by the
//   timeout and no blank frames are written.
// - No pools: a packet owns its header and its payload (see pacer/pacer.rs). Per forwarded
//   packet that costs one allocation for the payload, one for the header's list of
//   extensions if it has any, and one for each extension that the original copies into
//   the pacer packet's scratch (an inline dependency descriptor, the abs-capture-time);
//   a dependency descriptor that spilled is taken over from the translation parameters,
//   the playout delay and the two dummy extensions are shared. A retransmission costs two
//   allocations more: the packet read back is copied out of the batch's read buffer to be
//   parsed, and its extensions are copied out of the sequencer's entry. A batch of NACKs
//   and a probe allocate their read buffer. `PACKET_FACTORY_SIZE` is the size of the
//   original pool's buffers, which bounds the payload there and here.
// - `CreateSourceDescriptionChunks`: the chunk has the CNAME item only. The original adds
//   an item of type 15 with the mid, and `rtc-rtcp`'s `SdesType` has no such value (it
//   knows 0 to 8 and reads everything else as the end of the list).
// - `DownTrackParams`: the buffer factory, the pacer and the receiver are `Arc`s; the
//   RTCP writer is an `Option`, and an XR that asks for an answer gets none without one,
//   where the original calls a nil function; the trailer is `Bytes`; `MaxTrack` is the
//   size of the sequencer, a `usize`.
// - `DownTrackState` holds the two statistics objects by `Arc`; `MarshalLogObject` is
//   `Debug`. `bindState` is an enum, `String()` is `Display`.
// - `SetStreamAllocatorListener` takes an `Option<Weak<..>>`; the listeners are given the
//   down track by reference. A receiver report listener and the stats callback are
//   closures that are given the down track by reference.
// - `WritePaddingRTP` and `WriteProbePackets` count bytes in `i64` (Go's `int`), as the
//   prober that asks for them does.
// - `fmt.Sscanf(.., "%d/%d", ..)` of the RED parameters is `sscanf_two_ints`, at the end of
//   the file, which reads what the original reads from such a line.
// - `DebugInfo` is a JSON object; the bind state is in it as its number, which is what the
//   original's value becomes when it is written as JSON.
// - `io.EOF` from the receiver's `ReadRTP` is `Error::Eof`.

use std::fmt;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, LazyLock, Weak};

use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};
use rand::RngExt;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use super::redreceiver::OPUS_PT;
use super::rtxbudget::RtxBudget;
use super::sequencer::{ExtPacketMeta, Sequencer};
use super::{
    Bitrates, DebugInfo, DownTrackListener, DownTrackStreamAllocatorListener, Error, Forwarder,
    PlayoutDelayController, PlayoutDelayControllerState, RtxBudgetConfig, SequenceNumberOrdering,
    TrackReceiver, TrackSender, VideoAllocation, VideoPauseReason, VideoTransition,
};
use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;
use crate::sfu::buffer;
use crate::sfu::bwe;
use crate::sfu::ccutils;
use crate::sfu::connectionquality;
use crate::sfu::pacer;
use crate::sfu::packettrailer;
use crate::sfu::rtpextension::abscapturetime as act;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::sfu::rtpextension::playoutdelay as pd;
use crate::sfu::rtpstats;
use crate::sfu::utils;
use crate::telemetry::prometheus;
use crate::webrtc;
use crate::webrtc::rtcp;
use crate::webrtc::rtcp::extended_report::{
    DLRRReport, DLRRReportBlock, ExtendedReport, ReceiverReferenceTimeReportBlock,
};
use crate::webrtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest;
use crate::webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use crate::webrtc::rtcp::payload_feedbacks::receiver_estimated_maximum_bitrate::ReceiverEstimatedMaximumBitrate;
use crate::webrtc::rtcp::receiver_report::ReceiverReport;
use crate::webrtc::rtcp::sender_report::SenderReport;
use crate::webrtc::rtcp::source_description::{
    SdesType, SourceDescriptionChunk, SourceDescriptionItem,
};
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_nack::TransportLayerNack;
use crate::webrtc::rtp;
use crate::webrtc::sdp;
use crate::{debugw, errorw, infow, warnw};

// -------------------------------------------------------------------

pub const RTP_PADDING_MAX_PAYLOAD_SIZE: usize = 255;
pub const RTP_PADDING_ESTIMATED_HEADER_SIZE: usize = 20;
pub const RTP_BLANK_FRAMES_MUTE_SECONDS: f32 = 1.0;
pub const RTP_BLANK_FRAMES_CLOSE_SECONDS: f32 = 0.2;

pub const FLAG_STOP_RTX_ON_PLI: bool = true;

const KEY_FRAME_INTERVAL_MIN: u32 = 200;
const KEY_FRAME_INTERVAL_MAX: u32 = 1000;
const FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

// upper bound on NACKed sequence numbers buffered between retransmit worker runs
const MAX_PENDING_NACKS: usize = 5000;

const WAIT_BEFORE_SEND_PADDING_ON_MUTE: Duration = Duration::from_millis(100);
const MAX_PADDING_ON_MUTE_DURATION: Duration = Duration::from_secs(5);
const PADDING_ON_MUTE_INTERVAL: Duration = Duration::from_millis(100);

// The size of a buffer of the original's `PacketFactory` (sfu.go), which is not ported: a
// payload is built in one there, and a packet is read back from the receiver into one.
const PACKET_FACTORY_SIZE: usize = 1460;

// -------------------------------------------------------------------

// padding of a dropped packet's slot, one byte: the count byte itself
const DROPPED_SLOT_PADDING_SIZE: u8 = 1;

pub const VP8_KEY_FRAME_8X8: [u8; 31] = [
    0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x08, 0x00, //
    0x08, 0x00, 0x00, 0x47, 0x08, 0x85, 0x85, 0x88, //
    0x85, 0x84, 0x88, 0x02, 0x02, 0x00, 0x0c, 0x0d, //
    0x60, 0x00, 0xfe, 0xff, 0xab, 0x50, 0x80,
];

pub const H264_KEY_FRAME_2X2_SPS: [u8; 24] = [
    0x67, 0x42, 0xc0, 0x1f, 0x0f, 0xd9, 0x1f, 0x88, //
    0x88, 0x84, 0x00, 0x00, 0x03, 0x00, 0x04, 0x00, //
    0x00, 0x03, 0x00, 0xc8, 0x3c, 0x60, 0xc9, 0x20,
];
pub const H264_KEY_FRAME_2X2_PPS: [u8; 6] = [0x68, 0x87, 0xcb, 0x83, 0xcb, 0x20];
pub const H264_KEY_FRAME_2X2_IDR: [u8; 10] =
    [0x65, 0x88, 0x84, 0x0a, 0xf2, 0x62, 0x80, 0x00, 0xa7, 0xbe];
pub const H264_KEY_FRAME_2X2: [&[u8]; 3] = [
    &H264_KEY_FRAME_2X2_SPS,
    &H264_KEY_FRAME_2X2_PPS,
    &H264_KEY_FRAME_2X2_IDR,
];

pub const OPUS_SILENCE_FRAME: [u8; 80] = [
    0xf8, 0xff, 0xfe, 0x00, 0x00, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

// PCMU (G.711 µ-law) silence frame - 0xff represents zero amplitude
// 160 samples = 20ms at 8kHz sample rate
pub const PCMU_SILENCE_FRAME: [u8; 160] = [0xff; 160];

// PCMA (G.711 A-law) silence frame - 0xd5 represents zero amplitude
// 160 samples = 20ms at 8kHz sample rate
pub const PCMA_SILENCE_FRAME: [u8; 160] = [0xd5; 160];

static DUMMY_ABS_SEND_TIME_EXT: LazyLock<Bytes> = LazyLock::new(|| {
    Bytes::from(
        rtp::AbsSendTimeExtension::new(mono::now())
            .marshal()
            .unwrap_or_default(),
    )
});
static DUMMY_TRANSPORT_CC_EXT: LazyLock<Bytes> = LazyLock::new(|| {
    Bytes::from(
        rtp::TransportCcExtension {
            transport_sequence: 12345,
        }
        .marshal()
        .unwrap_or_default(),
    )
});

// -------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct DownTrackState {
    pub rtp_stats: Option<Arc<rtpstats::RtpStatsSender>>,
    pub delta_stats_sender_snapshot_id: u32,
    pub rtp_stats_rtx: Option<Arc<rtpstats::RtpStatsSender>>,
    pub delta_stats_rtx_sender_snapshot_id: u32,
    pub forwarder_state: Option<livekit::RtpForwarderState>,
    pub playout_delay_controller_state: PlayoutDelayControllerState,
}

impl fmt::Debug for DownTrackState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DownTrackState")
            .field("RTPStats", &self.rtp_stats)
            .field(
                "DeltaStatsSenderSnapshotId",
                &self.delta_stats_sender_snapshot_id,
            )
            .field("RTPStatsRTX", &self.rtp_stats_rtx)
            .field(
                "DeltaStatsRTXSenderSnapshotId",
                &self.delta_stats_rtx_sender_snapshot_id,
            )
            .field("ForwarderState", &self.forwarder_state)
            .field(
                "PlayoutDelayControllerState",
                &self.playout_delay_controller_state,
            )
            .finish()
    }
}

// -------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
enum BindState {
    Unbound = 0,
    // downtrack negotiated, but waiting for receiver to be ready to start forwarding
    WaitForReceiverReady,
    // downtrack is bound and ready to forward
    Bound,
}

impl BindState {
    // Go's conversion `bindState(v)`, of the values the down track stores.
    fn from_i32(v: i32) -> BindState {
        match v {
            1 => BindState::WaitForReceiverReady,
            2 => BindState::Bound,
            _ => BindState::Unbound,
        }
    }
}

impl fmt::Display for BindState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BindState::Unbound => "unbound",
            BindState::WaitForReceiverReady => "waitForReceiverReady",
            BindState::Bound => "bound",
        })
    }
}

// -------------------------------------------------------------------

pub type ReceiverReportListener = Arc<dyn Fn(&DownTrack, &ReceiverReport) + Send + Sync>;

type OnBinding = Arc<dyn Fn(Result<(), webrtc::Error>) + Send + Sync>;
type OnStatsUpdate = Arc<dyn Fn(&DownTrack, &livekit::AnalyticsStat) + Send + Sync>;
type RtcpWriter = Box<dyn Fn(&[Box<dyn rtcp::Packet>]) -> Result<(), webrtc::Error> + Send + Sync>;

pub struct DownTrackParams {
    pub codecs: Vec<webrtc::RtpCodecParameters>,
    pub is_encrypted: bool,
    pub source: livekit::TrackSource,
    pub receiver: Arc<dyn TrackReceiver>,
    pub buffer_factory: Arc<buffer::Factory>,
    pub sub_id: livekit::ParticipantId,
    pub stream_id: String,
    pub max_track: usize,
    pub playout_delay_limit: Option<livekit::PlayoutDelay>,
    pub pacer: Arc<dyn pacer::Pacer>,
    pub logger: Logger,
    pub trailer: Bytes,
    pub rtcp_writer: Option<RtcpWriter>,
    pub disable_sender_report_pass_through: bool,
    pub supports_codec_change: bool,
    pub strip_packet_trailer: bool,
    pub enable_start_at_desired_quality: bool,
    pub rtx_budget: RtxBudgetConfig,
    pub listener: Weak<dyn DownTrackListener>,
}

// What the closure `doBind` of the original's `Bind` captures.
struct DoBind {
    t: Arc<dyn webrtc::TrackLocalContext>,
    codec: webrtc::RtpCodecParameters,
    matched_upstream_codec: webrtc::RtpCodecParameters,
}

// What `bindLock` guards and nothing reads without it.
struct DownTrackBind {
    negotiated_codec_parameters: Vec<webrtc::RtpCodecParameters>,
    rtcp_reader: Option<Arc<buffer::RtcpReader>>,
    rtcp_reader_rtx: Option<Arc<buffer::RtcpReader>>,
    on_binding: Option<OnBinding>,
    bind_on_receiver_ready: Option<DoBind>,
    is_receiver_ready: bool,
}

// What `writeBlankFrameRTP` returns: the original's channel that is closed when the writer
// is done. The writer holds the sending half and drops it.
struct BlankFramesDone(std::sync::mpsc::Receiver<()>);

impl BlankFramesDone {
    // `select { case <-doneFlushing: case <-timer.C: }`: whether the writer was done in time.
    // It blocks the thread, as the original blocks its goroutine.
    fn wait(&self, timeout: Duration) -> bool {
        let wait = || {
            !matches!(
                self.0.recv_timeout(timeout.to_std()),
                Err(RecvTimeoutError::Timeout)
            )
        };
        match tokio::runtime::Handle::try_current() {
            // the writer may be waiting in this worker's own queue
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(wait)
            }
            _ => wait(),
        }
    }
}

type GetBlankFrame = Box<dyn Fn(&DownTrack, bool) -> Result<Vec<u8>, Error> + Send>;

// DownTrack implements webrtc.TrackLocal, is the track used to write packets
// to SFU Subscriber, the track handle the packets for simple, simulcast
// and SVC Publisher.
// A DownTrack has the following lifecycle
// - new
// - bound / unbound
// - closed
// once closed, a DownTrack cannot be re-used.
pub struct DownTrack {
    params: DownTrackParams,
    id: livekit::TrackId,
    kind: webrtc::RtpCodecType,
    ssrc: atomic::Uint32,
    ssrc_rtx: atomic::Uint32,
    payload_type: atomic::Uint32,
    payload_type_rtx: atomic::Uint32,
    sequencer: atomic::Pointer<Sequencer>,
    rtx_sequence_number: atomic::Uint64,
    rtx_budget: Option<RtxBudget>,

    // receiverLock and what it guards
    receiver: RwLock<Arc<dyn TrackReceiver>>,

    forwarder: Forwarder,

    upstream_codecs: Vec<webrtc::RtpCodecParameters>,
    codec: atomic::Value<webrtc::RtpCodecCapability>,
    clock_rate: u32,

    // payload types for red codec only
    is_red: atomic::Bool,
    upstream_primary_pt: atomic::Uint32,
    primary_pt: atomic::Uint32,

    abs_send_time_ext_id: atomic::Int32,
    transport_wide_ext_id: atomic::Int32,
    dependency_descriptor_ext_id: atomic::Int32,
    playout_delay_ext_id: atomic::Int32,
    abs_capture_time_ext_id: atomic::Int32,
    transceiver: RwLock<Option<Weak<dyn webrtc::RtpTransceiver>>>,
    write_stream: atomic::Value<Arc<dyn webrtc::TrackLocalWriter>>,

    // listenerLock and what it guards
    receiver_report_listeners: RwLock<Vec<ReceiverReportListener>>,

    bind_lock: Mutex<DownTrackBind>,
    bind_state: atomic::Int32,

    is_closed: atomic::Bool,
    connected: atomic::Bool,
    bind_and_connected_once: atomic::Bool,
    writable: atomic::Bool,
    writable_at: atomic::Time,
    write_stopped: atomic::Bool,

    rtp_stats: Arc<rtpstats::RtpStatsSender>,
    delta_stats_sender_snapshot_id: atomic::Uint32,

    rtp_stats_rtx: Arc<rtpstats::RtpStatsSender>,
    delta_stats_rtx_sender_snapshot_id: atomic::Uint32,

    total_repeated_nacks: atomic::Uint32,

    blank_frames_generation: atomic::Uint32,

    connection_stats: Arc<connectionquality::ConnectionStats>,
    on_stats_update: atomic::Value<OnStatsUpdate>, // func(d *DownTrack, stat *livekit.AnalyticsStat)

    is_nack_throttled: atomic::Bool,

    active_padding_on_mute_up_track: atomic::Bool,

    // streamAllocatorLock and what it guards
    stream_allocator_listener: RwLock<Option<Weak<dyn DownTrackStreamAllocatorListener>>>,
    probe_cluster_id: atomic::Uint32,

    playout_delay: Option<PlayoutDelayController>,

    pacer: Arc<dyn pacer::Pacer>,

    // maxLayerNotifierChMu, the channel and its closed flag: `None` when closed
    max_layer_notifier_ch: RwLock<Option<mpsc::Sender<&'static str>>>,

    // keyFrameRequesterChMu, the channel and its closed flag
    key_frame_requester_ch: RwLock<Option<mpsc::Sender<()>>>,
    key_frame_requester_wakeups: atomic::Uint32,

    // retransmitChMu, the channel and its closed flag
    retransmit_ch: RwLock<Option<mpsc::Sender<()>>>,
    // pendingNACKsMu and what it guards
    pending_nacks: Mutex<Vec<u16>>,

    created_at: i64,
    last_unmuted_at: atomic::Time,

    // the down track itself, for what it hands out and starts
    this: Weak<DownTrack>,
}

impl DownTrack {
    // NewDownTrack returns a DownTrack.
    pub fn new(mut params: DownTrackParams) -> Result<Arc<DownTrack>, Error> {
        let mime_type = mime::normalize_mime_type(&params.codecs[0].mime_type);
        let kind = if mime::is_mime_type_audio(mime_type) {
            webrtc::RtpCodecType::Audio
        } else if mime::is_mime_type_video(mime_type) {
            webrtc::RtpCodecType::Video
        } else {
            webrtc::RtpCodecType::Unknown
        };

        let codec = params.codecs[0].rtp_codec_capability.clone();
        let (max_layer_notifier_ch, max_layer_notifier_rx) = mpsc::channel(1);
        let (key_frame_requester_ch, key_frame_requester_rx) = mpsc::channel(1);
        let (retransmit_ch, retransmit_rx) = mpsc::channel(1);
        let created_at = time::now().unix_nano();
        let rtx_budget = RtxBudget::new(params.rtx_budget.for_kind(kind));

        // the playout delay controller gets the logger as it was handed in
        let params_logger = params.logger.clone();
        params.logger = params
            .logger
            .with_values(&[("subscriberID", &subscriber_id(&params.sub_id, created_at))]);

        let (md_cache_size, md_cache_size_rtx) = if kind == webrtc::RtpCodecType::Video {
            (8192, 8192)
        } else {
            (8192, 1024)
        };
        let rtp_stats = Arc::new(rtpstats::RtpStatsSender::new(
            rtpstats::RtpStatsParams::default(),
            md_cache_size,
        ));
        // clock rate will be set on bind or codec change with matching codec's clock rate
        rtp_stats.set_logger(params.logger.with_values(&[("stream", &"primary")]));
        let delta_stats_sender_snapshot_id = rtp_stats.new_sender_snapshot_id();

        let rtp_stats_rtx = Arc::new(rtpstats::RtpStatsSender::new(
            rtpstats::RtpStatsParams { is_rtx: true },
            md_cache_size_rtx,
        ));
        // clock rate will be set on bind or codec change with matching codec's clock rate
        rtp_stats_rtx.set_logger(params.logger.with_values(&[("stream", &"rtx")]));
        let delta_stats_rtx_sender_snapshot_id = rtp_stats_rtx.new_sender_snapshot_id();

        let forwarder = Forwarder::new(
            kind,
            params.logger.clone(),
            false, // skipReferenceTS
            false, // disableOpportunisticAllocation
            params.enable_start_at_desired_quality,
            Some(rtp_stats.clone()),
        );

        let mut playout_delay = None;
        if kind == webrtc::RtpCodecType::Video
            && let Some(delay) = &params.playout_delay_limit
            && delay.enabled
        {
            playout_delay = Some(PlayoutDelayController::new(
                delay.min,
                delay.max,
                params_logger,
                rtp_stats.clone(),
            )?);
        }

        let d = Arc::new_cyclic(|this: &Weak<DownTrack>| {
            let sender_provider: Weak<dyn connectionquality::ConnectionStatsSenderProvider> =
                this.clone();
            let connection_stats =
                connectionquality::ConnectionStats::new(connectionquality::ConnectionStatsParams {
                    sender_provider: Some(sender_provider),
                    logger: params.logger.with_values(&[("direction", &"down")]),
                    ..Default::default()
                });
            let d = this.clone();
            connection_stats.on_stats_update(move |_cs, stat| {
                let Some(d) = d.upgrade() else {
                    return;
                };
                if let Some(listener) = d.listener() {
                    listener.on_stats_update(stat);
                }
                if let Some(f) = d.on_stats_update.load() {
                    f(&d, stat);
                }
            });

            DownTrack {
                id: params.receiver.track_id(),
                upstream_codecs: params.codecs.clone(),
                kind,
                clock_rate: codec.clock_rate,
                pacer: params.pacer.clone(),
                max_layer_notifier_ch: RwLock::new(Some(max_layer_notifier_ch)),
                key_frame_requester_ch: RwLock::new(Some(key_frame_requester_ch)),
                retransmit_ch: RwLock::new(Some(retransmit_ch)),
                created_at,
                receiver: RwLock::new(params.receiver.clone()),
                rtx_budget,
                last_unmuted_at: atomic::Time::new(time::now()),
                codec: {
                    let v = atomic::Value::new();
                    v.store(codec);
                    v
                },
                bind_state: atomic::Int32::new(BindState::Unbound as i32),
                rtp_stats,
                delta_stats_sender_snapshot_id: atomic::Uint32::new(delta_stats_sender_snapshot_id),
                rtp_stats_rtx,
                delta_stats_rtx_sender_snapshot_id: atomic::Uint32::new(
                    delta_stats_rtx_sender_snapshot_id,
                ),
                forwarder,
                connection_stats,
                playout_delay,
                // a random number in third quartile of sequence number space
                rtx_sequence_number: atomic::Uint64::new(
                    rand::rng().random_range(0..(1u64 << 14)) + (1u64 << 15),
                ),

                ssrc: atomic::Uint32::new(0),
                ssrc_rtx: atomic::Uint32::new(0),
                payload_type: atomic::Uint32::new(0),
                payload_type_rtx: atomic::Uint32::new(0),
                sequencer: atomic::Pointer::new(None),
                is_red: atomic::Bool::new(false),
                upstream_primary_pt: atomic::Uint32::new(0),
                primary_pt: atomic::Uint32::new(0),
                abs_send_time_ext_id: atomic::Int32::new(0),
                transport_wide_ext_id: atomic::Int32::new(0),
                dependency_descriptor_ext_id: atomic::Int32::new(0),
                playout_delay_ext_id: atomic::Int32::new(0),
                abs_capture_time_ext_id: atomic::Int32::new(0),
                transceiver: RwLock::new(None),
                write_stream: atomic::Value::new(),
                receiver_report_listeners: RwLock::new(Vec::new()),
                bind_lock: Mutex::new(DownTrackBind {
                    negotiated_codec_parameters: Vec::new(),
                    rtcp_reader: None,
                    rtcp_reader_rtx: None,
                    on_binding: None,
                    bind_on_receiver_ready: None,
                    is_receiver_ready: false,
                }),
                is_closed: atomic::Bool::new(false),
                connected: atomic::Bool::new(false),
                bind_and_connected_once: atomic::Bool::new(false),
                writable: atomic::Bool::new(false),
                writable_at: atomic::Time::new(Time::ZERO),
                write_stopped: atomic::Bool::new(false),
                total_repeated_nacks: atomic::Uint32::new(0),
                blank_frames_generation: atomic::Uint32::new(0),
                on_stats_update: atomic::Value::new(),
                is_nack_throttled: atomic::Bool::new(false),
                active_padding_on_mute_up_track: atomic::Bool::new(false),
                stream_allocator_listener: RwLock::new(None),
                probe_cluster_id: atomic::Uint32::new(0),
                key_frame_requester_wakeups: atomic::Uint32::new(0),
                pending_nacks: Mutex::new(Vec::new()),
                this: this.clone(),
                params,
            }
        });

        if d.kind == webrtc::RtpCodecType::Video {
            time::spawn(Self::max_layer_notifier_worker(
                d.this.clone(),
                d.params.listener.clone(),
                d.params.logger.clone(),
                max_layer_notifier_rx,
            ));
            time::spawn(Self::key_frame_requester(
                d.this.clone(),
                key_frame_requester_rx,
            ));
        }

        time::spawn(Self::retransmit_worker(d.this.clone(), retransmit_rx));

        let this = d.this.clone();
        d.params.receiver.add_on_ready(Box::new(move || {
            if let Some(d) = this.upgrade() {
                d.handle_receiver_ready();
            }
        }));
        debugw!(d.params.logger, "downtrack created", "upstreamCodecs" => d.upstream_codecs);

        Ok(d)
    }

    // Bind is called by the PeerConnection after negotiation is complete
    // This asserts that the code requested is supported by the remote peer.
    // If so it sets up all the state (SSRC and PayloadType) to have a call
    pub fn bind(
        &self,
        t: Arc<dyn webrtc::TrackLocalContext>,
    ) -> Result<webrtc::RtpCodecParameters, Error> {
        let mut b = self.bind_lock.lock();
        if self.bind_state() != BindState::Unbound {
            return Err(Error::DownTrackAlreadyBound);
        }

        // the TrackLocalContext's codec parameters will be set to the bound codec after Bind returns,
        // so keep a copy of the codec parameters here to use it later
        b.negotiated_codec_parameters = t.codec_parameters();
        let mut codec = webrtc::RtpCodecParameters::default();
        let mut matched_upstream_codec = webrtc::RtpCodecParameters::default();
        for c in &self.upstream_codecs {
            match utils::codec_parameters_fuzzy_search(c, &b.negotiated_codec_parameters) {
                Ok(match_codec) => {
                    codec = match_codec;
                    matched_upstream_codec = c.clone();
                    break;
                }
                Err(_) => {
                    // for encrypyted tracks, should match on primary codec,
                    // i. e. codec at index 0 if the combination of upstream codecs is opus and RED
                    if self.params.is_encrypted {
                        let mut is_red_and_opus = true;
                        for u in &self.upstream_codecs {
                            if !mime::is_mime_type_string_opus(&u.mime_type)
                                || !mime::is_mime_type_string_red(&u.mime_type)
                            {
                                is_red_and_opus = false;
                                break;
                            }
                        }
                        if is_red_and_opus {
                            break;
                        }
                    }
                }
            }
        }

        if codec.mime_type.is_empty() {
            let err = webrtc::Error::UnsupportedCodec;
            let on_binding = b.on_binding.clone();
            let negotiated_codec_parameters = b.negotiated_codec_parameters.clone();
            drop(b);
            infow!(
                self.params.logger,
                "bind error for unsupported codec",
                "codecs" => self.upstream_codecs,
                "remoteParameters" => negotiated_codec_parameters,
            );
            if let Some(on_binding) = on_binding {
                on_binding(Err(err));
            }
            // don't return error here, as pion will not start transports if Bind fails at first answer
            return Ok(webrtc::RtpCodecParameters::default());
        }

        // if a downtrack is closed before bind, it already unsubscribed from client, don't do subsequent operation and return here.
        if self.is_closed() {
            debugw!(self.params.logger, "DownTrack closed before bind");
            return Ok(codec);
        }

        // Bind is called under RTPSender.mu lock,
        // call the RTPSender.GetParameters (which setRTPHeaderExtensions invokes)
        // in goroutine to avoid deadlock
        if let Some(d) = self.this.upgrade() {
            time::spawn(async move {
                d.set_rtp_header_extensions();
            });
        }

        let do_bind = DoBind {
            t,
            codec: codec.clone(),
            matched_upstream_codec,
        };

        let is_receiver_ready = b.is_receiver_ready;
        let mut do_bind = Some(do_bind);
        if !is_receiver_ready {
            debugw!(self.params.logger, "downtrack bound: receiver not ready", "codec" => codec);
            b.bind_on_receiver_ready = do_bind.take();
            self.set_bind_state_locked(&mut b, BindState::WaitForReceiverReady);
        }
        drop(b);

        if let Some(listener) = self.listener() {
            listener.on_codec_negotiated(codec.rtp_codec_capability.clone());
        }

        if let Some(do_bind) = do_bind {
            self.do_bind(do_bind);
        }
        Ok(codec)
    }

    // The closure `doBind` of the original's `Bind`.
    fn do_bind(&self, do_bind: DoBind) {
        let DoBind {
            t,
            codec,
            matched_upstream_codec,
        } = do_bind;

        let mut b = self.bind_lock.lock();
        if self.is_closed() {
            drop(b);
            debugw!(self.params.logger, "DownTrack closed before bind");
            return;
        }

        let mut is_fec_enabled = false;
        if mime::is_mime_type_string_red(&matched_upstream_codec.mime_type) {
            self.is_red.store(true);
            for c in &self.upstream_codecs {
                is_fec_enabled = c.sdp_fmtp_line.to_lowercase().contains("useinbandfec=1");

                // assume upstream primary codec is opus since we only support it for audio now
                if mime::is_mime_type_string_opus(&c.mime_type) {
                    self.upstream_primary_pt.store(c.payload_type as u32);
                    break;
                }
            }
            if self.upstream_primary_pt.load() == 0 {
                errorw!(
                    self.params.logger,
                    "failed to find upstream primary opus payload type for RED", nil,
                    "matchedCodec" => codec,
                    "upstreamCodec" => self.upstream_codecs,
                );
            }

            let (primary_pt, _secondary_pt, n) = sscanf_two_ints(&codec.sdp_fmtp_line);
            if n != 2 {
                errorw!(
                    self.params.logger,
                    "failed to parse primary and secondary payload type for RED",
                    "input does not match format",
                    "matchedCodec" => codec,
                );
            }
            self.primary_pt.store(primary_pt as u8 as u32);
        } else if mime::is_mime_type_string_audio(&matched_upstream_codec.mime_type) {
            is_fec_enabled = matched_upstream_codec
                .sdp_fmtp_line
                .to_lowercase()
                .contains("fec");
        }

        self.ssrc.store(t.ssrc());
        self.ssrc_rtx.store(t.ssrc_retransmission());
        self.payload_type.store(codec.payload_type as u32);
        self.payload_type_rtx.store(utils::find_rtx_payload_type(
            codec.payload_type,
            &b.negotiated_codec_parameters,
        ) as u32);
        if self.is_red.load() {
            debugw!(
                self.params.logger,
                "DownTrack.Bind",
                "codecs" => self.upstream_codecs,
                "matchCodec" => codec,
                "ssrc" => t.ssrc(),
                "ssrcRTX" => t.ssrc_retransmission(),
                "isFECEnabled" => is_fec_enabled,
                "isRED" => self.is_red.load(),
                "upstreamPrimaryPT" => self.upstream_primary_pt.load(),
                "primaryPT" => self.primary_pt.load(),
                "payloadType" => self.payload_type.load(),
                "payloadTypeRTX" => self.payload_type_rtx.load(),
                "codecParameters" => b.negotiated_codec_parameters,
            );
        } else {
            debugw!(
                self.params.logger,
                "DownTrack.Bind",
                "codecs" => self.upstream_codecs,
                "matchCodec" => codec,
                "ssrc" => t.ssrc(),
                "ssrcRTX" => t.ssrc_retransmission(),
                "isFECEnabled" => is_fec_enabled,
                "payloadType" => self.payload_type.load(),
                "payloadTypeRTX" => self.payload_type_rtx.load(),
                "codecParameters" => b.negotiated_codec_parameters,
            );
        }

        self.write_stream.store(t.write_stream());
        if let Some(rr) = self
            .params
            .buffer_factory
            .get_or_new(buffer::BufferPacketType::Rtcp, self.ssrc())
            .into_rtcp_reader()
        {
            let this = self.this.clone();
            rr.on_packet(move |pkt| {
                if let Some(d) = this.upgrade() {
                    d.handle_rtcp(pkt);
                }
            });
            b.rtcp_reader = Some(rr);
        }
        if self.ssrc_rtx() != 0
            && let Some(rr) = self
                .params
                .buffer_factory
                .get_or_new(buffer::BufferPacketType::Rtcp, self.ssrc_rtx())
                .into_rtcp_reader()
        {
            let this = self.this.clone();
            rr.on_packet(move |pkt| {
                if let Some(d) = this.upgrade() {
                    d.handle_rtcp_rtx(pkt);
                }
            });
            b.rtcp_reader_rtx = Some(rr);
        }

        self.sequencer.store(
            Sequencer::new(
                self.params.max_track,
                self.kind == webrtc::RtpCodecType::Video,
                self.params.logger.clone(),
            )
            .map(Arc::new),
        );

        self.codec.store(codec.rtp_codec_capability.clone());
        self.rtp_stats
            .set_clock_rate(codec.rtp_codec_capability.clock_rate);
        self.rtp_stats_rtx
            .set_clock_rate(codec.rtp_codec_capability.clock_rate);

        if let Some(on_binding) = &b.on_binding {
            on_binding(Ok(()));
        }
        self.set_bind_state_locked(&mut b, BindState::Bound);
        drop(b);

        let receiver = self.receiver();
        self.forwarder.determine_codec(
            &codec.rtp_codec_capability,
            &receiver.header_extensions(),
            receiver.video_layer_mode(),
        );
        self.connection_stats.start(self.mime(), is_fec_enabled);
        debugw!(self.params.logger, "downtrack bound");
    }

    fn bind_state(&self) -> BindState {
        BindState::from_i32(self.bind_state.load())
    }

    fn set_bind_state_locked(&self, b: &mut DownTrackBind, state: BindState) {
        if self.bind_state.swap(state as i32) == state as i32 {
            return;
        }

        if state == BindState::Bound || state == BindState::Unbound {
            b.bind_on_receiver_ready = None;
            self.on_bind_and_connected_change();
        }
    }

    fn handle_receiver_ready(&self) {
        let mut b = self.bind_lock.lock();
        if b.is_receiver_ready {
            return;
        }
        debugw!(self.params.logger, "downtrack receiver ready");
        b.is_receiver_ready = true;
        let do_bind = b.bind_on_receiver_ready.take();
        drop(b);

        if let Some(do_bind) = do_bind {
            self.do_bind(do_bind);
        }
    }

    fn handle_upstream_codec_change(&self, mime_type: &str) {
        let b = self.bind_lock.lock();
        let existing_mime_type = self.codec().mime_type;
        if mime::is_mime_type_string_equal(&existing_mime_type, mime_type) {
            return;
        }

        if !self.params.supports_codec_change {
            drop(b);
            infow!(
                self.params.logger,
                "client doesn't support codec change, renegotiate new codec"
            );
            if let Some(d) = self.this.upgrade() {
                time::spawn(async move {
                    d.close();
                });
            }
            return;
        }

        let (old_pt, old_rtx_pt, old_codec) = (
            self.payload_type.load(),
            self.payload_type_rtx.load(),
            self.codec(),
        );

        let mut codec = webrtc::RtpCodecParameters::default();
        for c in &self.upstream_codecs {
            if !mime::is_mime_type_string_equal(&c.mime_type, mime_type) {
                continue;
            }

            if let Ok(match_codec) =
                utils::codec_parameters_fuzzy_search(c, &b.negotiated_codec_parameters)
            {
                codec = match_codec;
                break;
            }
        }

        if codec.mime_type.is_empty() {
            // codec not found, should not happen since the upstream codec should only fall back to higher compatibility (vp8)
            errorw!(
                self.params.logger,
                "can't find matched codec for new upstream payload type", nil,
                "upstreamCodecs" => self.upstream_codecs,
                "remoteParameters" => b.negotiated_codec_parameters,
                "mime" => mime_type,
            );
            return;
        }

        self.payload_type.store(codec.payload_type as u32);
        self.payload_type_rtx.store(utils::find_rtx_payload_type(
            codec.payload_type,
            &b.negotiated_codec_parameters,
        ) as u32);

        self.codec.store(codec.rtp_codec_capability.clone());
        self.rtp_stats
            .set_clock_rate(codec.rtp_codec_capability.clock_rate);
        self.rtp_stats_rtx
            .set_clock_rate(codec.rtp_codec_capability.clock_rate);

        let is_fec_enabled = codec.sdp_fmtp_line.to_lowercase().contains("fec");
        drop(b);

        infow!(
            self.params.logger,
            "upstream codec changed",
            "oldPT" => old_pt, "newPT" => self.payload_type.load(),
            "oldRTXPT" => old_rtx_pt, "newRTXPT" => self.payload_type_rtx.load(),
            "oldCodec" => old_codec, "newCodec" => codec.rtp_codec_capability,
        );

        let receiver = self.receiver();
        self.forwarder.restart();
        self.flush_sequencer();
        self.forwarder.determine_codec(
            &codec.rtp_codec_capability,
            &receiver.header_extensions(),
            receiver.video_layer_mode(),
        );

        self.connection_stats
            .update_codec(self.mime(), is_fec_enabled);
    }

    // Unbind implements the teardown logic when the track is no longer needed. This happens
    // because a track has been stopped.
    pub fn unbind(&self, _t: Arc<dyn webrtc::TrackLocalContext>) -> Result<(), Error> {
        let mut b = self.bind_lock.lock();
        self.set_bind_state_locked(&mut b, BindState::Unbound);
        Ok(())
    }

    pub fn set_stream_allocator_listener(
        &self,
        listener: Option<Weak<dyn DownTrackStreamAllocatorListener>>,
    ) {
        let sal = listener.as_ref().and_then(Weak::upgrade);
        *self.stream_allocator_listener.write() = listener;

        self.set_rtp_header_extensions();

        if let Some(sal) = sal {
            // kick off a gratuitous allocation
            sal.on_subscription_changed(self);
        }
    }

    fn get_stream_allocator_listener(&self) -> Option<Arc<dyn DownTrackStreamAllocatorListener>> {
        self.stream_allocator_listener
            .read()
            .as_ref()
            .and_then(Weak::upgrade)
    }

    pub fn set_probe_cluster_id(&self, probe_cluster_id: ccutils::ProbeClusterId) {
        self.probe_cluster_id.store(probe_cluster_id.0);
    }

    pub fn swap_probe_cluster_id(
        &self,
        r#match: ccutils::ProbeClusterId,
        swap: ccutils::ProbeClusterId,
    ) {
        self.probe_cluster_id.cas(r#match.0, swap.0);
    }

    // ID is the unique identifier for this Track. This should be unique for the
    // stream, but doesn't have to globally unique. A common example would be 'audio' or 'video'
    // and StreamID would be 'desktop' or 'webcam'
    pub fn id(&self) -> String {
        self.id.to_string()
    }

    // Codec returns current track codec capability
    pub fn codec(&self) -> webrtc::RtpCodecCapability {
        self.codec.load().unwrap_or_default()
    }

    pub fn mime(&self) -> mime::MimeType {
        mime::normalize_mime_type(&self.codec().mime_type)
    }

    // StreamID is the group this track belongs too. This must be unique
    pub fn stream_id(&self) -> String {
        self.params.stream_id.clone()
    }

    pub fn subscriber_id(&self) -> livekit::ParticipantId {
        // add `createdAt` to ensure repeated subscriptions from same subscriber to same publisher does not collide
        subscriber_id(&self.params.sub_id, self.created_at)
    }

    pub fn receiver(&self) -> Arc<dyn TrackReceiver> {
        self.receiver.read().clone()
    }

    pub fn set_receiver(&self, r: Arc<dyn TrackReceiver>) {
        debugw!(self.params.logger, "downtrack set receiver", "codec" => r.codec());
        let b = self.bind_lock.lock();
        if self.is_closed() {
            return;
        }

        let old = {
            let mut receiver = self.receiver.write();
            std::mem::replace(&mut *receiver, r.clone())
        };

        old.delete_down_track(&self.subscriber_id());
        drop(b);

        let this = self.this.clone();
        r.add_on_ready(Box::new(move || {
            if let Some(d) = this.upgrade() {
                d.handle_receiver_ready();
            }
        }));
        self.handle_upstream_codec_change(&r.codec().mime_type);

        let b = self.bind_lock.lock();
        if let Some(d) = self.this.upgrade()
            && let Err(err) = r.add_down_track(d)
        {
            warnw!(
                self.params.logger,
                "failed to add downtrack to receiver",
                err
            );
        }
        drop(b);

        if let Some(sal) = self.get_stream_allocator_listener() {
            sal.on_subscribed_layer_changed(self, self.forwarder.max_layer());
        }
    }

    // Sets RTP header extensions for this track
    fn set_rtp_header_extensions(&self) {
        let mut is_bwe_enabled = false;
        let mut bwe_type = bwe::BweType::None;
        let sal = self.get_stream_allocator_listener();
        if let Some(sal) = &sal {
            is_bwe_enabled = sal.is_bwe_enabled(self);
            bwe_type = sal.bwe_type();
        }

        let Some(tr) = self.get_transceiver() else {
            return;
        };

        let mut extensions = Vec::new();
        if let Some(sender) = tr.sender() {
            extensions = sender.get_parameters().header_extensions.clone();
            debugw!(self.params.logger, "negotiated downtrack extensions", "extensions" => extensions);
        }

        let _b = self.bind_lock.lock();
        for ext in &extensions {
            match ext.uri.as_str() {
                sdp::ABS_SEND_TIME_URI => {
                    if sal.is_some() {
                        let current = self.abs_send_time_ext_id.load();
                        if is_bwe_enabled && bwe_type == bwe::BweType::Remote {
                            if current != 0 && current != ext.id {
                                infow!(self.params.logger, "absSendTimeExtID mismatch", "current" => current, "negotiated" => ext.id);
                            }
                            self.abs_send_time_ext_id.store(ext.id);
                        } else {
                            if current != 0 {
                                infow!(self.params.logger, "absSendTimeExtID disabled unexpectedly", "negotiated" => ext.id);
                            }
                            self.abs_send_time_ext_id.store(0);
                        }
                    }
                }

                dd::EXTENSION_URI => {
                    let current = self.dependency_descriptor_ext_id.load();
                    if current != 0 && current != ext.id {
                        infow!(self.params.logger, "dependencyDescriptorExtID mismatch", "current" => current, "negotiated" => ext.id);
                    }
                    self.dependency_descriptor_ext_id.store(ext.id);
                }

                pd::PLAYOUT_DELAY_URI => {
                    let current = self.playout_delay_ext_id.load();
                    if current != 0 && current != ext.id {
                        infow!(self.params.logger, "playoutDelayExtID mismatch", "current" => current, "negotiated" => ext.id);
                    }
                    self.playout_delay_ext_id.store(ext.id);
                }

                sdp::TRANSPORT_CC_URI => {
                    if sal.is_some() {
                        let current = self.transport_wide_ext_id.load();
                        if is_bwe_enabled && bwe_type == bwe::BweType::SendSide {
                            if current != 0 && current != ext.id {
                                infow!(self.params.logger, "transportWideExtID mismatch", "current" => current, "negotiated" => ext.id);
                            }
                            self.transport_wide_ext_id.store(ext.id);
                        } else {
                            if current != 0 {
                                infow!(self.params.logger, "transportWideExtID disabled unexpectedly", "negotiated" => ext.id);
                            }
                            self.transport_wide_ext_id.store(0);
                        }
                    }
                }

                act::ABS_CAPTURE_TIME_URI => {
                    let current = self.abs_capture_time_ext_id.load();
                    if current != 0 && current != ext.id {
                        infow!(self.params.logger, "absCaptureTimeExtID mismatch", "current" => current, "negotiated" => ext.id);
                    }
                    self.abs_capture_time_ext_id.store(ext.id);
                }

                _ => {}
            }
        }
        debugw!(
            self.params.logger,
            "negotiated extension ids",
            "absSendTimeExtID" => self.abs_send_time_ext_id.load(),
            "dependencyDescriptorExtID" => self.dependency_descriptor_ext_id.load(),
            "playoutDelayExtID" => self.playout_delay_ext_id.load(),
            "transportWideExtID" => self.transport_wide_ext_id.load(),
            "absCaptureTimeExtID" => self.abs_capture_time_ext_id.load(),
        );
    }

    // Kind controls if this TrackLocal is audio or video
    pub fn kind(&self) -> webrtc::RtpCodecType {
        self.kind
    }

    // RID is required by `webrtc.TrackLocal` interface
    pub fn rid(&self) -> String {
        String::new()
    }

    pub fn ssrc(&self) -> u32 {
        self.ssrc.load()
    }

    pub fn ssrc_rtx(&self) -> u32 {
        self.ssrc_rtx.load()
    }

    pub fn set_transceiver(&self, transceiver: Arc<dyn webrtc::RtpTransceiver>) {
        *self.transceiver.write() = Some(Arc::downgrade(&transceiver));
        self.set_rtp_header_extensions();
    }

    pub fn get_transceiver(&self) -> Option<Arc<dyn webrtc::RtpTransceiver>> {
        self.transceiver.read().as_ref().and_then(Weak::upgrade)
    }

    fn post_key_frame_request_event(&self) {
        if self.kind != webrtc::RtpCodecType::Video {
            return;
        }

        let ch = self.key_frame_requester_ch.read();
        if let Some(ch) = ch.as_ref() {
            let _ = ch.try_send(());
        }
    }

    async fn key_frame_requester(this: Weak<DownTrack>, mut ch: mpsc::Receiver<()>) {
        fn get_interval(d: &DownTrack) -> Duration {
            let interval = d
                .rtp_stats
                .get_rtt()
                .wrapping_mul(2)
                .clamp(KEY_FRAME_INTERVAL_MIN, KEY_FRAME_INTERVAL_MAX);
            Duration::from_millis(interval as i64)
        }

        // the original's timer, stopped: when it is to fire
        let mut timer: Option<tokio::time::Instant> = None;

        loop {
            // for !d.IsClosed()
            match this.upgrade() {
                Some(d) if !d.is_closed() => {}
                _ => return,
            }

            let fire_at = timer;
            tokio::select! {
                more = ch.recv() => {
                    if more.is_none() {
                        return;
                    }
                    timer = None;
                }
                _ = async {
                    if let Some(fire_at) = fire_at {
                        tokio::time::sleep_until(fire_at).await;
                    }
                }, if fire_at.is_some() => {
                    timer = None;
                }
            }

            let Some(d) = this.upgrade() else {
                return;
            };
            d.key_frame_requester_wakeups.inc();

            let (locked, layer) = d.forwarder.check_sync();
            if !locked && layer != buffer::INVALID_LAYER_SPATIAL && d.writable.load() {
                debugw!(d.params.logger, "sending PLI for layer lock", "layer" => layer);
                d.receiver().send_pli(layer, false);
                d.rtp_stats.update_layer_lock_pli_and_time(1);
            }

            // if the initial-acquisition grace expired without latching the requested layer, force a
            // re-allocation so the target falls back to the highest layer actually seen (rather than
            // stalling while waiting for a requested layer that never showed up)
            if d.forwarder.maybe_expire_acquire_grace()
                && let Some(sal) = d.get_stream_allocator_listener()
            {
                sal.on_available_layers_changed(&d);
            }

            // Re-arm only while there is something to wait for: a requested layer that is not locked yet,
            // or a lock state that can change without an event. Once locked, sleep until the
            // next event so a locked track does not wake up every interval.
            if (!locked && layer != buffer::INVALID_LAYER_SPATIAL)
                || d.forwarder.needs_periodic_sync_check()
            {
                timer = Some(tokio::time::Instant::now() + get_interval(&d).to_std());
            }
        }
    }

    fn post_max_layer_notifier_event(&self, event: &'static str) {
        if self.kind != webrtc::RtpCodecType::Video {
            return;
        }

        let ch = self.max_layer_notifier_ch.read();
        if let Some(ch) = ch.as_ref()
            && ch.try_send(event).is_err()
        {
            debugw!(self.params.logger, "max layer notifier channel busy", "event" => event);
        }
    }

    async fn max_layer_notifier_worker(
        this: Weak<DownTrack>,
        listener: Weak<dyn DownTrackListener>,
        logger: Logger,
        mut ch: mpsc::Receiver<&'static str>,
    ) {
        while let Some(event) = ch.recv().await {
            let Some(d) = this.upgrade() else {
                break;
            };
            let max_layer_spatial = d.forwarder.get_max_subscribed_spatial();
            debugw!(logger, "max subscribed layer processed", "layer" => max_layer_spatial, "event" => event);

            debugw!(
                logger,
                "notifying max subscribed layer",
                "layer" => max_layer_spatial,
                "event" => event,
            );
            if let Some(listener) = listener.upgrade() {
                listener.on_max_subscribed_layer_changed(max_layer_spatial);
            }
        }

        debugw!(
            logger,
            "notifying max subscribed layer",
            "layer" => buffer::INVALID_LAYER_SPATIAL,
            "event" => "close",
        );
        if let Some(listener) = listener.upgrade() {
            listener.on_max_subscribed_layer_changed(buffer::INVALID_LAYER_SPATIAL);
        }
    }

    // WriteRTP writes an RTP Packet to the DownTrack
    pub fn write_rtp(&self, ext_pkt: &buffer::ExtPacket, layer: i32) -> i32 {
        if !self.writable.load() {
            return 0;
        }

        let (mut tp, err) = self.forwarder.get_translation_params(ext_pkt, layer);
        if tp.should_drop {
            if let Err(err) = err {
                errorw!(self.params.logger, "could not get translation params", err);
            }
            if tp.pad_dropped_slot {
                self.write_dropped_slot_padding(
                    tp.rtp.ext_sequence_number,
                    tp.rtp.ext_timestamp,
                    tp.rtp.sn_ordering == SequenceNumberOrdering::OutOfOrder,
                );
            }
            return 0;
        }

        if tp.incoming_header_size > ext_pkt.packet.payload.len() {
            errorw!(
                self.params.logger,
                "incoming header size overflow", Error::PayloadOverflow,
                "incomingHeaderSize" => tp.incoming_header_size,
                "payloadSize" => ext_pkt.packet.payload.len(),
            );
            return 0;
        }

        let incoming_payload = &ext_pkt.packet.payload[tp.incoming_header_size..];
        // what a buffer of the original's pool has room for behind the codec header
        let n = incoming_payload
            .len()
            .min(PACKET_FACTORY_SIZE.saturating_sub(tp.codec_bytes_len));
        if n != incoming_payload.len() {
            errorw!(
                self.params.logger,
                "payload overflow", Error::PayloadOverflow,
                "want" => incoming_payload.len(),
                "have" => n,
            );
            return 0;
        }
        let mut payload = Vec::with_capacity(tp.codec_bytes_len + n);
        payload.extend_from_slice(tp.codec_header());
        payload.extend_from_slice(incoming_payload);

        let mut trailer_stripped = 0;
        if self.params.strip_packet_trailer {
            trailer_stripped =
                packettrailer::strip_trailer(&payload, tp.marker || tp.is_end_of_layer_frame);
            if trailer_stripped > 0 {
                payload.truncate(payload.len() - trailer_stripped);
            }
        }

        // translate RTP header
        let mut hdr = rtp::Header {
            version: ext_pkt.packet.version,
            padding: false,
            marker: tp.marker,
            payload_type: self.get_translated_payload_type(ext_pkt.packet.payload_type),
            sequence_number: tp.rtp.ext_sequence_number as u16,
            timestamp: tp.rtp.ext_timestamp as u32,
            ssrc: self.ssrc(),
            ..Default::default()
        };

        let mut pacer_packet = pacer::Packet::new(self.write_stream());
        pacer_packet.probe_cluster_id = ccutils::ProbeClusterId(self.probe_cluster_id.load());
        pacer_packet.abs_send_time_ext_id = self.abs_send_time_ext_id.load() as u8;
        pacer_packet.transport_wide_ext_id = self.transport_wide_ext_id.load() as u8;

        // add extensions
        //
        // the header refers to extensions until the pacer marshals them, so hold the ones on the stack in the pacer packet, which one send owns
        let dd_bytes_spill = Bytes::from(std::mem::take(&mut tp.dd_bytes_spill));
        let (dd_bytes, dd_ext): (&[u8], Option<Bytes>) =
            if dd_bytes_spill.is_empty() && !tp.dd_inline().is_empty() {
                let inline = tp.dd_inline();
                (inline, pacer_packet.hold_extension(inline))
            } else {
                (&dd_bytes_spill, Some(dd_bytes_spill.clone()))
            };
        let dependency_descriptor_ext_id = self.dependency_descriptor_ext_id.load();
        if dependency_descriptor_ext_id != 0
            && let Some(dd_ext) = dd_ext
            && !dd_ext.is_empty()
        {
            let _ = hdr.set_extension(dependency_descriptor_ext_id as u8, dd_ext);
        }
        let playout_delay_ext_id = self.playout_delay_ext_id.load();
        if playout_delay_ext_id != 0
            && let Some(playout_delay) = &self.playout_delay
            && let Some(val) = playout_delay.get_delay_extension(hdr.sequence_number)
        {
            let _ = hdr.set_extension(playout_delay_ext_id as u8, val);

            // NOTE: play out delay extension is not cached in sequencer,
            // i. e. they will not be added to retransmitted packet.
            // But, it is okay as the extension is added till a RTCP Receiver Report for
            // the corresponding sequence number is received.
            // The extreme case is all packets containing the play out delay are lost and
            // all of them retransmitted and an RTCP Receiver Report received for those
            // retransmitted sequence numbers. But, that is highly improbable, if not impossible.
        }
        let mut act_buf = [0u8; act::MAX_SIZE];
        let mut act_bytes: &[u8] = &[];
        let abs_capture_time_ext_id = self.abs_capture_time_ext_id.load();
        if let Some(abs_capture_time_ext) = &ext_pkt.abs_capture_time_ext
            && abs_capture_time_ext_id != 0
        {
            let mut n = 0;
            if !self.params.disable_sender_report_pass_through {
                // pass through the original publisher capture time verbatim, consistent
                // with sender reports also being passed through unchanged.
                n = abs_capture_time_ext.marshal_to(&mut act_buf).unwrap_or(0);
            } else {
                // normalize capture time to SFU clock.
                // NOTE: even if there is estimated offset populated, just re-map the
                // absolute capture time stamp as it should be the same RTCP sender report
                // clock domain of publisher. SFU is normalising sender reports of publisher
                // to SFU clock before sending to subscribers. So, capture time should be
                // normalized to the same clock. Clear out any offset.
                let (_, _, _, ref_sender_report) = self.forwarder.get_sender_report_params();
                if let Some(ref_sender_report) = &ref_sender_report {
                    let mut act_ext_copy = *abs_capture_time_ext;
                    if act_ext_copy
                        .rewrite(rtpstats::rtcp_sender_report_propagation_delay(
                            ref_sender_report,
                        ))
                        .is_ok()
                    {
                        n = act_ext_copy.marshal_to(&mut act_buf).unwrap_or(0);
                    }
                }
            }
            if n != 0 {
                act_bytes = &act_buf[..n];
                if let Some(act_ext) = pacer_packet.hold_extension(act_bytes) {
                    let _ = hdr.set_extension(abs_capture_time_ext_id as u8, act_ext);
                }
            }
        }
        self.add_dummy_extensions(&mut hdr);

        // the sequencer copies from the translation params and the incoming packet, not from the pacer packet that Enqueue frees
        if let Some(sequencer) = self.sequencer.load() {
            sequencer.push(
                ext_pkt.arrival,
                ext_pkt.ext_sequence_number,
                tp.rtp.ext_sequence_number,
                tp.rtp.ext_timestamp,
                tp.marker,
                layer as i8,
                tp.codec_header(),
                tp.incoming_header_size,
                dd_bytes,
                act_bytes,
                trailer_stripped,
            );
        }

        let header_size = hdr.marshal_size();
        let payload_size = payload.len();
        pacer_packet.header_size = header_size;
        RtxBudget::on_media_sent(self.rtx_budget.as_ref(), header_size + payload_size);
        self.rtp_stats.update(
            ext_pkt.arrival,
            tp.rtp.ext_sequence_number,
            tp.rtp.ext_timestamp,
            hdr.marker,
            header_size,
            payload_size,
            0,
            ext_pkt.is_out_of_order,
        );
        pacer_packet.header = hdr;
        pacer_packet.payload = Bytes::from(payload);
        self.pacer.enqueue(pacer_packet);

        if ext_pkt.is_key_frame {
            self.is_nack_throttled.store(false);
            self.rtp_stats.update_key_frame(1);
            prometheus::increment_key_frames(prometheus::Direction::Outgoing);
            debugw!(
                self.params.logger,
                "forwarded key frame",
                "layer" => layer,
                "rtpsn" => tp.rtp.ext_sequence_number,
                "rtpts" => tp.rtp.ext_timestamp,
            );
        }

        if tp.is_switching {
            self.post_max_layer_notifier_event("switching");
            // the current layer moved on the packet path, so the layer lock may have changed
            self.post_key_frame_request_event();
        }

        if tp.is_resuming
            && let Some(sal) = self.get_stream_allocator_listener()
        {
            sal.on_resume(self);
        }

        if tp.is_starting {
            let writable_at = self.writable_at.load();
            let last_unmuted_at = self.last_unmuted_at.load();
            let mut anchor_to = last_unmuted_at;
            if !writable_at.is_zero() && !last_unmuted_at.is_zero() {
                if writable_at.after(last_unmuted_at) {
                    anchor_to = writable_at;
                }
                if let Some(listener) = self.listener() {
                    listener.on_stream_started(time::since(anchor_to));
                }
                if time::since(anchor_to) > time::SECOND {
                    debugw!(
                        self.params.logger,
                        "stream start high latency",
                        "latency" => time::since(anchor_to),
                        "createdAt" => Time::unix(0, self.created_at),
                        "sinceCreate" => time::since(Time::unix(0, self.created_at)),
                        "writableAt" => writable_at,
                        "sinceWritable" => time::since(writable_at),
                        "lastUnmutedAt" => last_unmuted_at,
                        "sinceLastUnmute" => time::since(last_unmuted_at),
                        "rtpStats" => self.rtp_stats,
                    );
                }
            }
        }
        1
    }

    // writeDroppedSlotPadding sends a padding only packet in the outgoing slot of a dropped
    // packet that is out of order or follows a gap. The receiver takes it as padding for that sequence number and
    // does not NACK it. The sequencer has no entry for the slot, so a NACK that still comes
    // finds nothing to retransmit.
    fn write_dropped_slot_padding(
        &self,
        ext_sequence_number: u64,
        ext_timestamp: u64,
        is_out_of_order: bool,
    ) {
        let mut hdr = rtp::Header {
            version: 2,
            padding: true,
            padding_size: DROPPED_SLOT_PADDING_SIZE,
            marker: false,
            payload_type: self.payload_type.load() as u8,
            sequence_number: ext_sequence_number as u16,
            timestamp: ext_timestamp as u32,
            ssrc: self.ssrc(),
            ..Default::default()
        };
        self.add_dummy_extensions(&mut hdr);

        let hdr_size = hdr.marshal_size();
        self.rtp_stats.update(
            mono::unix_nano(),
            ext_sequence_number,
            ext_timestamp,
            hdr.marker,
            hdr_size,
            0,
            DROPPED_SLOT_PADDING_SIZE as usize,
            is_out_of_order,
        );

        let mut pacer_packet = pacer::Packet::new(self.write_stream());
        pacer_packet.header = hdr;
        pacer_packet.header_size = hdr_size;
        pacer_packet.probe_cluster_id = ccutils::PROBE_CLUSTER_ID_INVALID; // not probe traffic
        pacer_packet.abs_send_time_ext_id = self.abs_send_time_ext_id.load() as u8;
        pacer_packet.transport_wide_ext_id = self.transport_wide_ext_id.load() as u8;
        self.pacer.enqueue(pacer_packet);
    }

    // WritePaddingRTP tries to write as many padding only RTP packets as necessary
    // to satisfy given size to the DownTrack
    pub fn write_padding_rtp(
        &self,
        bytes_to_send: i64,
        padding_on_mute: bool,
        force_marker: bool,
    ) -> i64 {
        if !self.writable.load() {
            return 0;
        }

        if !padding_on_mute {
            if !self.rtp_stats.is_active() {
                return 0;
            }

            // Ideally should look at header extensions negotiated for
            // track and decide if padding can be sent. But, browsers behave
            // in unexpected ways when using audio for bandwidth estimation and
            // padding is mainly used to probe for excess available bandwidth.
            // So, to be safe, limit to video tracks
            if self.kind == webrtc::RtpCodecType::Audio {
                return 0;
            }

            // LK-TODO-START
            // Potentially write padding even if muted. Given that padding
            // can be sent only on frame boundaries, writing on disabled tracks
            // will give more options.
            // LK-TODO-END
            if self.forwarder.is_muted() {
                return 0;
            }

            // Hold sending padding packets till first RTCP-RR is received for this RTP stream.
            // That is definitive proof that the remote side knows about this RTP stream.
            if self.rtp_stats.last_receiver_report_time() == 0 {
                return 0;
            }
        }

        // RTP padding maximum is 255 bytes. Break it up.
        // Use 20 byte as estimate of RTP header size (12 byte header + 8 byte extension)
        let num = (bytes_to_send
            + RTP_PADDING_MAX_PAYLOAD_SIZE as i64
            + RTP_PADDING_ESTIMATED_HEADER_SIZE as i64
            - 1)
            / (RTP_PADDING_MAX_PAYLOAD_SIZE as i64 + RTP_PADDING_ESTIMATED_HEADER_SIZE as i64);
        if num == 0 {
            return 0;
        }
        // a negative number panics in the original too, where the munger makes a slice of
        // that length
        let num = usize::try_from(num).expect("a negative number of padding packets");

        let mut frame_rate = 0u32;
        if padding_on_mute {
            // advance timestamps when sending dummy padding packets to start a stream
            // to ensure receiver sees proper timestamp and starts the stream
            frame_rate = (time::SECOND / PADDING_ON_MUTE_INTERVAL) as u32;
        }

        let Ok(snts) = self
            .forwarder
            .get_sn_ts_for_padding(num, frame_rate, force_marker)
        else {
            return 0;
        };

        //
        // Register with sequencer as padding only so that NACKs for these can be filtered out.
        // Retransmission is probably a sign of network congestion/badness.
        // So, retransmitting padding only packets is only going to make matters worse.
        //
        if let Some(sequencer) = self.sequencer.load() {
            sequencer.push_padding(
                snts[0].ext_sequence_number,
                snts[snts.len() - 1].ext_sequence_number,
            );
        }

        let mut bytes_sent = 0i64;
        for snt in &snts {
            let mut hdr = rtp::Header {
                version: 2,
                padding: true,
                padding_size: RTP_PADDING_MAX_PAYLOAD_SIZE as u8,
                marker: false,
                payload_type: self.payload_type.load() as u8,
                sequence_number: snt.ext_sequence_number as u16,
                timestamp: snt.ext_timestamp as u32,
                ssrc: self.ssrc(),
                ..Default::default()
            };
            self.add_dummy_extensions(&mut hdr);

            let hdr_size = hdr.marshal_size();
            self.rtp_stats.update(
                mono::unix_nano(),
                snt.ext_sequence_number,
                snt.ext_timestamp,
                hdr.marker,
                hdr_size,
                0,
                RTP_PADDING_MAX_PAYLOAD_SIZE,
                false,
            );

            let mut pacer_packet = pacer::Packet::new(self.write_stream());
            pacer_packet.header = hdr;
            pacer_packet.header_size = hdr_size;
            pacer_packet.probe_cluster_id = ccutils::ProbeClusterId(self.probe_cluster_id.load());
            pacer_packet.is_probe = true;
            pacer_packet.abs_send_time_ext_id = self.abs_send_time_ext_id.load() as u8;
            pacer_packet.transport_wide_ext_id = self.transport_wide_ext_id.load() as u8;
            self.pacer.enqueue(pacer_packet);

            bytes_sent += (hdr_size + RTP_PADDING_MAX_PAYLOAD_SIZE) as i64;
        }

        bytes_sent
    }

    // Mute enables or disables media forwarding - subscriber triggered
    pub fn mute(&self, muted: bool) {
        let mut is_subscribe_mutable = true;
        if let Some(sal) = self.get_stream_allocator_listener() {
            is_subscribe_mutable = sal.is_subscribe_mutable(self);
        }
        let changed = self.forwarder.mute(muted, is_subscribe_mutable);
        self.handle_mute(muted, changed);
    }

    // PubMute enables or disables media forwarding - publisher side
    pub fn pub_mute(&self, pub_muted: bool) {
        let changed = self.forwarder.pub_mute(pub_muted);
        self.handle_mute(pub_muted, changed);
    }

    fn handle_mute(&self, muted: bool, changed: bool) {
        if !changed {
            return;
        }

        self.connection_stats
            .update_mute(self.forwarder.is_any_muted());
        if muted {
            self.last_unmuted_at.store(Time::ZERO);
        } else {
            self.last_unmuted_at.store(time::now());
        }

        //
        // Subscriber mute changes trigger a max layer notification.
        // That could result in encoding layers getting turned on/off on publisher side
        // (depending on aggregate layer requirements of all subscribers of the track).
        //
        // Publisher mute changes should not trigger notification.
        // If publisher turns off all layers because of subscribers indicating
        // no layers required due to publisher mute (bit of circular dependency),
        // there will be a delay in layers turning back on when unmute happens.
        // Unmute path will require
        //   1. unmute signalling out-of-band from publisher received by downtrack(s)
        //   2. downtrack(s) notifying max layer
        //   3. out-of-band notification about max layer sent back to the publisher
        //   4. publisher starts layer(s)
        // Ideally, on publisher mute, whatever layers were active remain active and
        // can be restarted by publisher immediately on unmute.
        //
        // Note that while publisher mute is active, subscriber changes can also happen
        // and that could turn on/off layers on publisher side.
        //
        self.post_max_layer_notifier_event("mute");
        // mute resyncs the forwarder, so the layer lock has to be re-acquired on unmute
        self.post_key_frame_request_event();

        if let Some(sal) = self.get_stream_allocator_listener() {
            sal.on_subscription_changed(self);
        }

        // when muting, send a few silence frames to ensure residual noise does not
        // put the comfort noise generator on decoder side in a bad state where it
        // generates noise that is not so comfortable.
        //
        // One possibility is not to inject blank frames when publisher is muted
        // and let forwarding continue. When publisher is muted, unless the media
        // stream is stopped, publisher will send silence frames which should have
        // comfort noise information. But, in case the publisher stops at an
        // inopportune frame (due to media stream stop or injecting audio from a file),
        // the decoder could be in a noisy state. So, inject blank frames on publisher
        // mute too.
        self.blank_frames_generation.inc();
        if self.kind == webrtc::RtpCodecType::Audio && muted {
            self.write_blank_frame_rtp(
                RTP_BLANK_FRAMES_MUTE_SECONDS,
                self.blank_frames_generation.load(),
            );
        }
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load()
    }

    pub fn close(&self) {
        self.close_with_flush(true, true);
    }

    // CloseWithFlush - `flush` used to indicate whether send blank frame to flush
    // decoder of client.
    //  1. When transceiver of this track is reused by some other participant's video track,
    //     set flush=true to avoid previous video shows before new stream is displayed.
    //  2. in case of session migration, participant migrate from other node, video track should
    //     be resumed with same participant, set flush=false since we don't need to flush decoder.
    pub fn close_with_flush(&self, flush: bool, is_ending: bool) {
        let mut b = self.bind_lock.lock();
        if self.is_closed.swap(true) {
            // already closed
            return;
        }

        debugw!(self.params.logger, "close downtrack", "flushBlankFrame" => flush, "isEnding" => is_ending);
        if self.bind_state() == BindState::Bound {
            self.forwarder.mute(true, true);

            // write blank frames after disabling so that other frames do not interfere.
            // Idea here is to send blank key frames to flush the decoder buffer at the remote end.
            // Otherwise, with transceiver re-use last frame from previous stream is held in the
            // display buffer and there could be a brief moment where the previous stream is displayed.
            if flush {
                let done_flushing = self.write_blank_frame_rtp(
                    RTP_BLANK_FRAMES_CLOSE_SECONDS,
                    self.blank_frames_generation.inc(),
                );

                // The flush runs in its own goroutine (writeBlankFrameRTP) and is
                // cancelled via blankFramesGeneration, so bindLock guards nothing
                // during the wait. Release it: bindLock is a control-plane lock
                // (Bind/SetConnected/ReceiverRestart), and holding it across the
                // up-to-flushTimeout wait serializes all of those behind every
                // close. isClosed is already set, so no other close can enter.
                MutexGuard::unlocked(&mut b, || {
                    // wait a limited time to flush
                    if !done_flushing.wait(FLUSH_TIMEOUT) {
                        self.blank_frames_generation.inc(); // in case flush is still running
                    }
                });
            }

            debugw!(self.params.logger, "closing sender", "kind" => self.kind);
        }

        self.set_bind_state_locked(&mut b, BindState::Unbound);
        self.receiver().delete_down_track(&self.subscriber_id());

        if let Some(rtcp_reader) = &b.rtcp_reader
            && is_ending
        {
            debugw!(self.params.logger, "downtrack close rtcp reader");
            let _ = rtcp_reader.close();
            rtcp_reader.on_packet(|_| {});
        }
        if let Some(rtcp_reader_rtx) = &b.rtcp_reader_rtx
            && is_ending
        {
            debugw!(self.params.logger, "downtrack close rtcp rtx reader");
            let _ = rtcp_reader_rtx.close();
            rtcp_reader_rtx.on_packet(|_| {});
        }
        drop(b);

        self.connection_stats.close();

        self.rtp_stats.stop();
        self.rtp_stats_rtx.stop();
        debugw!(
            self.params.logger,
            "rtp stats",
            "direction" => "downstream",
            "mime" => self.mime().to_string(),
            "ssrc" => self.ssrc(),
            "stats" => self.rtp_stats,
            "statsRTX" => self.rtp_stats_rtx,
        );

        *self.max_layer_notifier_ch.write() = None;

        *self.key_frame_requester_ch.write() = None;

        *self.retransmit_ch.write() = None;

        if let Some(listener) = self.listener() {
            listener.on_down_track_close(!flush);
        }
    }

    pub fn set_max_spatial_layer(&self, spatial_layer: i32) {
        let (changed, max_layer) = self.forwarder.set_max_spatial_layer(spatial_layer);
        if !changed {
            return;
        }

        self.post_max_layer_notifier_event("max-subscribed");
        self.post_key_frame_request_event();

        if let Some(sal) = self.get_stream_allocator_listener() {
            sal.on_subscribed_layer_changed(self, max_layer);
        }
    }

    pub fn set_max_temporal_layer(&self, temporal_layer: i32) {
        let (changed, max_layer) = self.forwarder.set_max_temporal_layer(temporal_layer);
        if !changed {
            return;
        }

        if let Some(sal) = self.get_stream_allocator_listener() {
            sal.on_subscribed_layer_changed(self, max_layer);
        }
    }

    pub fn max_layer(&self) -> buffer::VideoLayer {
        self.forwarder.max_layer()
    }

    pub fn get_state(&self) -> DownTrackState {
        let mut dts = DownTrackState {
            rtp_stats: Some(self.rtp_stats.clone()),
            delta_stats_sender_snapshot_id: self.delta_stats_sender_snapshot_id.load(),
            rtp_stats_rtx: Some(self.rtp_stats_rtx.clone()),
            delta_stats_rtx_sender_snapshot_id: self.delta_stats_rtx_sender_snapshot_id.load(),
            forwarder_state: self.forwarder.get_state(),
            ..Default::default()
        };

        if let Some(playout_delay) = &self.playout_delay {
            dts.playout_delay_controller_state = playout_delay.get_state();
        }
        dts
    }

    pub fn seed_state(&self, state: DownTrackState) {
        if self.writable.load() {
            return;
        }

        if state.rtp_stats.is_some() || state.forwarder_state.is_some() {
            debugw!(self.params.logger, "seeding downtrack state", "state" => state);
        }
        if let Some(rtp_stats) = &state.rtp_stats {
            self.rtp_stats.seed(rtp_stats);
            self.delta_stats_sender_snapshot_id
                .store(state.delta_stats_sender_snapshot_id);
            if let Some(playout_delay) = &self.playout_delay {
                playout_delay.seed_state(state.playout_delay_controller_state);
            }
        }
        if let Some(rtp_stats_rtx) = &state.rtp_stats_rtx {
            self.rtp_stats_rtx.seed(rtp_stats_rtx);
            self.delta_stats_rtx_sender_snapshot_id
                .store(state.delta_stats_rtx_sender_snapshot_id);

            self.rtx_sequence_number
                .store(self.rtp_stats_rtx.ext_highest_sequence_number());
        }
        self.forwarder.seed_state(state.forwarder_state.as_ref());
    }

    pub fn stop_write_and_get_state(&self) -> DownTrackState {
        debugw!(self.params.logger, "stopping write");
        {
            let _b = self.bind_lock.lock();
            self.writable.store(false);
            self.write_stopped.store(true);
        }

        self.get_state()
    }

    pub fn up_track_layers_change(&self) {
        if let Some(sal) = self.get_stream_allocator_listener() {
            sal.on_available_layers_changed(self);
        }
    }

    pub fn up_track_bitrate_availability_change(&self) {
        if let Some(sal) = self.get_stream_allocator_listener() {
            sal.on_bitrate_availability_changed(self);
        }
    }

    pub fn up_track_max_published_layer_change(&self, max_published_layer: i32) {
        if self.forwarder.set_max_published_layer(max_published_layer) {
            // may arm the acquisition grace, which the key frame requester has to expire
            self.post_key_frame_request_event();
            if let Some(sal) = self.get_stream_allocator_listener() {
                sal.on_max_published_spatial_changed(self);
            }
        }
    }

    pub fn up_track_max_temporal_layer_seen_change(&self, max_temporal_layer_seen: i32) {
        if self
            .forwarder
            .set_max_temporal_layer_seen(max_temporal_layer_seen)
            && let Some(sal) = self.get_stream_allocator_listener()
        {
            sal.on_max_published_temporal_changed(self);
        }
    }

    fn maybe_add_transition(&self, bitrate: i64, distance: f64, pause_reason: VideoPauseReason) {
        if self.kind == webrtc::RtpCodecType::Audio {
            return;
        }

        if pause_reason == VideoPauseReason::Bandwidth {
            self.connection_stats.update_pause(true);
        } else {
            self.connection_stats.update_pause(false);
            self.connection_stats.add_layer_transition(distance);
            self.connection_stats.add_bitrate_transition(bitrate);
        }
    }

    pub fn up_track_bitrate_report(&self, available_layers: &[i32], bitrates: Bitrates) {
        self.maybe_add_transition(
            self.forwarder.get_optimal_bandwidth_needed(bitrates),
            self.forwarder
                .distance_to_desired(available_layers, bitrates),
            self.forwarder.pause_reason(),
        );
    }

    pub fn on_binding(&self, f: impl Fn(Result<(), webrtc::Error>) + Send + Sync + 'static) {
        let mut b = self.bind_lock.lock();

        b.on_binding = Some(Arc::new(f));
    }

    pub fn add_receiver_report_listener(
        &self,
        listener: impl Fn(&DownTrack, &ReceiverReport) + Send + Sync + 'static,
    ) {
        let mut listeners = self.receiver_report_listeners.write();

        listeners.push(Arc::new(listener));
    }

    pub fn is_deficient(&self) -> bool {
        self.forwarder.is_deficient()
    }

    pub fn bandwidth_requested(&self) -> i64 {
        let (_, brs) = self.receiver().get_layered_bitrate();
        self.forwarder.bandwidth_requested(brs)
    }

    pub fn distance_to_desired(&self) -> f64 {
        let (al, brs) = self.receiver().get_layered_bitrate();
        self.forwarder.distance_to_desired(&al, brs)
    }

    pub fn allocate_optimal(&self, allow_overshoot: bool, hold: bool) -> VideoAllocation {
        let (al, brs) = self.receiver().get_layered_bitrate();
        let allocation = self
            .forwarder
            .allocate_optimal(&al, brs, allow_overshoot, hold);
        self.post_key_frame_request_event();
        self.maybe_add_transition(
            allocation.bandwidth_needed,
            allocation.distance_to_desired,
            allocation.pause_reason,
        );
        allocation
    }

    pub fn provisional_allocate_prepare(&self) {
        let (al, brs) = self.receiver().get_layered_bitrate();
        self.forwarder.provisional_allocate_prepare(&al, brs);
    }

    pub fn provisional_allocate_reset(&self) {
        self.forwarder.provisional_allocate_reset();
    }

    pub fn provisional_allocate(
        &self,
        available_channel_capacity: i64,
        layers: buffer::VideoLayer,
        allow_pause: bool,
        allow_overshoot: bool,
    ) -> (bool, i64) {
        self.forwarder.provisional_allocate(
            available_channel_capacity,
            layers,
            allow_pause,
            allow_overshoot,
        )
    }

    pub fn provisional_allocate_get_cooperative_transition(
        &self,
        allow_overshoot: bool,
    ) -> VideoTransition {
        let (transition, available_layers, brs) = self
            .forwarder
            .provisional_allocate_get_cooperative_transition(allow_overshoot);
        debugw!(
            self.params.logger,
            "stream: cooperative transition",
            "transition" => transition,
            "availableLayers" => available_layers,
            "bitrates" => brs,
        );
        transition
    }

    pub fn provisional_allocate_get_best_weighted_transition(&self) -> VideoTransition {
        let (transition, available_layers, brs) = self
            .forwarder
            .provisional_allocate_get_best_weighted_transition();
        debugw!(
            self.params.logger,
            "stream: best weighted transition",
            "transition" => transition,
            "availableLayers" => available_layers,
            "bitrates" => brs,
        );
        transition
    }

    pub fn provisional_allocate_commit(&self) -> VideoAllocation {
        let allocation = self.forwarder.provisional_allocate_commit();
        self.post_key_frame_request_event();
        self.maybe_add_transition(
            allocation.bandwidth_needed,
            allocation.distance_to_desired,
            allocation.pause_reason,
        );
        allocation
    }

    pub fn allocate_next_higher(
        &self,
        available_channel_capacity: i64,
        allow_overshoot: bool,
    ) -> (VideoAllocation, bool) {
        let (al, brs) = self.receiver().get_layered_bitrate();
        let (allocation, available) = self.forwarder.allocate_next_higher(
            available_channel_capacity,
            &al,
            brs,
            allow_overshoot,
        );
        self.post_key_frame_request_event();
        self.maybe_add_transition(
            allocation.bandwidth_needed,
            allocation.distance_to_desired,
            allocation.pause_reason,
        );
        (allocation, available)
    }

    pub fn get_next_higher_transition(&self, allow_overshoot: bool) -> (VideoTransition, bool) {
        let (available_layers, brs) = self.receiver().get_layered_bitrate();
        let (transition, available) = self
            .forwarder
            .get_next_higher_transition(brs, allow_overshoot);
        debugw!(
            self.params.logger,
            "stream: get next higher layer",
            "transition" => transition,
            "available" => available,
            "availableLayers" => available_layers,
            "bitrates" => brs,
        );
        (transition, available)
    }

    pub fn pause(&self) -> VideoAllocation {
        let (al, brs) = self.receiver().get_layered_bitrate();
        let allocation = self.forwarder.pause(&al, brs);
        self.maybe_add_transition(
            allocation.bandwidth_needed,
            allocation.distance_to_desired,
            allocation.pause_reason,
        );
        allocation
    }

    pub fn resync(&self) {
        self.forwarder.resync();
        self.flush_sequencer();
        self.post_key_frame_request_event();
    }

    // flushSequencer discards recorded packet metadata on a stream restart so that NACK
    // retransmissions cannot use metadata that describes packets no longer in the receiver's
    // (resynced) bucket.
    fn flush_sequencer(&self) {
        if let Some(sequencer) = self.sequencer.load() {
            sequencer.flush();
        }
    }

    pub fn receiver_restart(&self, rcvr: Arc<dyn TrackReceiver>) {
        if rcvr.mime() != self.receiver().mime() {
            infow!(self.params.logger, "upstream receiver restart - skipped", "mime" => self.receiver().mime().to_string(), "newMime" => rcvr.mime().to_string());
            return;
        }

        let codec = {
            let _b = self.bind_lock.lock();
            if self.is_closed.load() {
                return;
            }
            self.codec()
        };

        let receiver = self.receiver();
        infow!(self.params.logger, "upstream receiver restart", "mime" => receiver.mime().to_string());
        self.forwarder.restart();
        self.flush_sequencer();
        self.forwarder.determine_codec(
            &codec,
            &receiver.header_extensions(),
            receiver.video_layer_mode(),
        );
    }

    pub fn create_source_description_chunks(&self) -> Vec<SourceDescriptionChunk> {
        let transceiver = self.get_transceiver();
        if self.bind_state() != BindState::Bound || transceiver.is_none() {
            return Vec::new();
        }
        vec![SourceDescriptionChunk {
            source: self.ssrc(),
            items: vec![
                SourceDescriptionItem {
                    sdes_type: SdesType::SdesCname,
                    text: Bytes::from(self.params.stream_id.clone()),
                },
                // The original has a second item, `rtcp.SDESType(15)` with the transceiver's
                // mid as its text. The library's `SdesType` cannot hold 15, so the item is
                // not here: whoever writes the chunks has the mid from `get_transceiver`.
            ],
        }]
    }

    pub fn create_sender_report(&self) -> Option<SenderReport> {
        if self.bind_state() != BindState::Bound {
            return None;
        }

        let (_, _, ts_offset, ref_sender_report) = self.forwarder.get_sender_report_params();
        self.rtp_stats.get_rtcp_sender_report(
            self.ssrc(),
            ref_sender_report.as_ref(),
            ts_offset,
            !self.params.disable_sender_report_pass_through,
        )

        // not sending RTCP Sender Report for RTX
    }

    fn write_blank_frame_rtp(&self, duration: f32, generation: u32) -> BlankFramesDone {
        let (done, done_rx) = std::sync::mpsc::channel::<()>();
        let this = self.this.clone();
        time::spawn(async move {
            // `close(done)` at every return: the sending half goes with the task
            let _done = done;

            let (get_blank_frame, frame_rate): (GetBlankFrame, u32) = {
                let Some(d) = this.upgrade() else {
                    return;
                };
                // don't send if not writable OR nothing has been sent
                if !d.writable.load() || !d.rtp_stats.is_active() {
                    return;
                }

                let mime_type = d.mime();
                let get_blank_frame: GetBlankFrame = match mime_type {
                    mime::MimeType::Opus => Box::new(|d: &DownTrack, frame_end_needed| {
                        d.get_audio_blank_frame_func(&OPUS_SILENCE_FRAME)(frame_end_needed)
                    }),
                    mime::MimeType::Red => Box::new(DownTrack::get_opus_red_blank_frame),
                    mime::MimeType::Pcmu => Box::new(|d: &DownTrack, frame_end_needed| {
                        d.get_audio_blank_frame_func(&PCMU_SILENCE_FRAME)(frame_end_needed)
                    }),
                    mime::MimeType::Pcma => Box::new(|d: &DownTrack, frame_end_needed| {
                        d.get_audio_blank_frame_func(&PCMA_SILENCE_FRAME)(frame_end_needed)
                    }),
                    mime::MimeType::Vp8 => Box::new(DownTrack::get_vp8_blank_frame),
                    mime::MimeType::H264 => Box::new(DownTrack::get_h264_blank_frame),
                    _ => {
                        return;
                    }
                };

                let mut frame_rate = 30u32;
                if mime_type == mime::MimeType::Opus
                    || mime_type == mime::MimeType::Red
                    || mime_type == mime::MimeType::Pcmu
                    || mime_type == mime::MimeType::Pcma
                {
                    frame_rate = 50;
                }
                (get_blank_frame, frame_rate)
            };

            // send a number of blank frames just in case there is loss.
            // Intentionally ignoring check for mute or bandwidth constrained mute
            // as this is used to clear client side buffer.
            let mut num_frames = (frame_rate as f32 * duration) as i64;
            let frame_duration = Duration::from_millis((1000 / frame_rate) as i64);

            let mut ticker = new_ticker(frame_duration);

            loop {
                {
                    let Some(d) = this.upgrade() else {
                        return;
                    };
                    if generation != d.blank_frames_generation.load()
                        || num_frames <= 0
                        || !d.writable.load()
                        || !d.rtp_stats.is_active()
                    {
                        return;
                    }

                    let (snts, mut frame_end_needed) =
                        match d.forwarder.get_sn_ts_for_blank_frames(frame_rate, 1) {
                            Ok(v) => v,
                            Err(err) => {
                                warnw!(d.params.logger, "could not get SN/TS for blank frame", err);
                                return;
                            }
                        };

                    for snt in &snts {
                        let mut hdr = rtp::Header {
                            version: 2,
                            padding: false,
                            marker: true,
                            payload_type: d.payload_type.load() as u8,
                            sequence_number: snt.ext_sequence_number as u16,
                            timestamp: snt.ext_timestamp as u32,
                            ssrc: d.ssrc(),
                            ..Default::default()
                        };
                        d.add_dummy_extensions(&mut hdr);

                        let payload = match get_blank_frame(&d, frame_end_needed) {
                            Ok(payload) => payload,
                            Err(err) => {
                                warnw!(d.params.logger, "could not get blank frame", err);
                                return;
                            }
                        };

                        let header_size = hdr.marshal_size();
                        d.rtp_stats.update(
                            mono::unix_nano(),
                            snt.ext_sequence_number,
                            snt.ext_timestamp,
                            hdr.marker,
                            header_size,
                            payload.len(),
                            0,
                            false,
                        );
                        let mut pacer_packet = pacer::Packet::new(d.write_stream());
                        pacer_packet.header = hdr;
                        pacer_packet.header_size = header_size;
                        pacer_packet.payload = Bytes::from(payload);
                        pacer_packet.probe_cluster_id =
                            ccutils::ProbeClusterId(d.probe_cluster_id.load());
                        pacer_packet.abs_send_time_ext_id = d.abs_send_time_ext_id.load() as u8;
                        pacer_packet.transport_wide_ext_id = d.transport_wide_ext_id.load() as u8;
                        d.pacer.enqueue(pacer_packet);

                        // only the first frame will need frameEndNeeded to close out the
                        // previous picture, rest are small key frames (for the video case)
                        frame_end_needed = false;
                    }
                }

                num_frames -= 1;
                ticker.tick().await;
            }
        });

        BlankFramesDone(done_rx)
    }

    fn maybe_add_trailer(&self, buf: &mut [u8]) -> usize {
        if buf.len() < self.params.trailer.len() {
            warnw!(self.params.logger, "trailer too big", nil, "bufLen" => buf.len(), "trailerLen" => self.params.trailer.len());
            return 0;
        }

        buf[..self.params.trailer.len()].copy_from_slice(&self.params.trailer);
        self.params.trailer.len()
    }

    fn get_audio_blank_frame_func(
        &self,
        silent_payload: &'static [u8],
    ) -> impl Fn(bool) -> Result<Vec<u8>, Error> + '_ {
        // silence frame
        // Used shortly after muting to ensure residual noise does not keep
        // generating noise at the decoder after the stream is stopped
        // i. e. comfort noise generation actually not producing something comfortable.
        move |_frame_end_needed: bool| {
            let mut payload = vec![0u8; 1000];
            payload[..silent_payload.len()].copy_from_slice(silent_payload);
            let trailer_len = self.maybe_add_trailer(&mut payload[silent_payload.len()..]);
            payload.truncate(silent_payload.len() + trailer_len);
            Ok(payload)
        }
    }

    fn get_opus_red_blank_frame(&self, _frame_end_needed: bool) -> Result<Vec<u8>, Error> {
        // primary only silence frame for opus/red, there is no need to contain redundant silent frames
        let mut payload = vec![0u8; 1000];

        // primary header
        //  0 1 2 3 4 5 6 7
        // +-+-+-+-+-+-+-+-+
        // |0|   Block PT  |
        // +-+-+-+-+-+-+-+-+
        payload[0] = OPUS_PT;
        payload[1..1 + OPUS_SILENCE_FRAME.len()].copy_from_slice(&OPUS_SILENCE_FRAME);
        let trailer_len = self.maybe_add_trailer(&mut payload[1 + OPUS_SILENCE_FRAME.len()..]);
        payload.truncate(1 + OPUS_SILENCE_FRAME.len() + trailer_len);
        Ok(payload)
    }

    fn get_vp8_blank_frame(&self, frame_end_needed: bool) -> Result<Vec<u8>, Error> {
        // 8x8 key frame
        // Used even when closing out a previous frame. Looks like receivers
        // do not care about content (it will probably end up being an undecodable
        // frame, but that should be okay as there are key frames following)
        let header = self.forwarder.get_padding(frame_end_needed)?;

        let mut payload = vec![0u8; 1000];
        payload[..header.len()].copy_from_slice(&header);
        payload[header.len()..header.len() + VP8_KEY_FRAME_8X8.len()]
            .copy_from_slice(&VP8_KEY_FRAME_8X8);
        let trailer_len =
            self.maybe_add_trailer(&mut payload[header.len() + VP8_KEY_FRAME_8X8.len()..]);
        payload.truncate(header.len() + VP8_KEY_FRAME_8X8.len() + trailer_len);
        Ok(payload)
    }

    fn get_h264_blank_frame(&self, _frame_end_needed: bool) -> Result<Vec<u8>, Error> {
        // TODO - Jie Zeng
        // now use STAP-A to compose sps, pps, idr together, most decoder support packetization-mode 1.
        // if client only support packetization-mode 0, use single nalu unit packet
        let mut buf = vec![0u8; 1000];
        let mut offset = 0;
        buf[0] = 0x18; // STAP-A
        offset += 1;
        for payload in H264_KEY_FRAME_2X2 {
            buf[offset..offset + 2].copy_from_slice(&(payload.len() as u16).to_be_bytes());
            offset += 2;
            buf[offset..offset + payload.len()].copy_from_slice(payload);
            offset += payload.len();
        }
        offset += self.maybe_add_trailer(&mut buf[offset..]);
        buf.truncate(offset);
        Ok(buf)
    }

    fn handle_rtcp(&self, bytes: &[u8]) {
        let pkts = match rtcp::packet::unmarshal(&mut &bytes[..]) {
            Ok(pkts) => pkts,
            Err(err) => {
                errorw!(
                    self.params.logger,
                    "could not unmarshal rtcp receiver packet",
                    err
                );
                return;
            }
        };

        let mut pli_once = true;
        let mut send_pli_once = || {
            let (_, layer) = self.forwarder.check_sync();
            if pli_once && layer != buffer::INVALID_LAYER_SPATIAL {
                debugw!(self.params.logger, "sending PLI RTCP", "layer" => layer);
                self.receiver().send_pli(layer, false);
                self.is_nack_throttled.store(true);
                self.rtp_stats.update_pli_time();
                pli_once = false;
            }
        };

        let mut rtt_to_report = 0u32;

        let mut num_nacks = 0u32;
        let mut num_plis = 0u32;
        let mut num_firs = 0u32;
        for pkt in &pkts {
            let pkt = pkt.as_any();
            if let Some(p) = pkt.downcast_ref::<PictureLossIndication>() {
                if p.media_ssrc == self.ssrc() {
                    num_plis += 1;
                    send_pli_once();
                }
            } else if let Some(p) = pkt.downcast_ref::<FullIntraRequest>() {
                if p.media_ssrc == self.ssrc() {
                    num_firs += 1;
                    send_pli_once();
                }
            } else if let Some(p) = pkt.downcast_ref::<ReceiverEstimatedMaximumBitrate>() {
                if let Some(sal) = self.get_stream_allocator_listener() {
                    sal.on_remb(self, p);
                }
            } else if let Some(p) = pkt.downcast_ref::<ReceiverReport>() {
                // create new receiver report w/ only valid reception reports
                let mut rr = ReceiverReport {
                    ssrc: p.ssrc,
                    profile_extensions: p.profile_extensions.clone(),
                    ..Default::default()
                };
                for r in &p.reports {
                    if r.ssrc != self.ssrc() {
                        continue;
                    }
                    rr.reports.push(r.clone());

                    let (rtt, is_rtt_changed) = self.rtp_stats.update_from_receiver_report(r);
                    if is_rtt_changed {
                        rtt_to_report = rtt;
                    }

                    if let Some(playout_delay) = &self.playout_delay {
                        playout_delay.on_seq_acked(r.last_sequence_number as u16);
                        // screen share track has inaccuracy jitter due to its low frame rate and bursty traffic
                        if self.params.source != livekit::TrackSource::ScreenShare {
                            let jitter_ms =
                                r.jitter.wrapping_mul(1000) as u64 / self.clock_rate as u64;
                            playout_delay.set_jitter(jitter_ms as u32);
                        }
                    }
                }
                // RTX-TODO: This is used for media loss proxying only as of 2024-12-15.
                // Ideally, this should keep deltas between previous RTCP Receiver Report
                // and current report, calculate the loss in the window and reconcile it with
                // data in a similar window from RTX stream (to ensure losses are discounted
                // for NACKs), but keeping this simple for several reasons
                //   - media loss proxying is a configurable setting and could be disabled
                //   - media loss proxying is used for audio only and audio may not have NACKing
                //   - to keep it simple
                if !rr.reports.is_empty() {
                    let rr_listeners = self.receiver_report_listeners.read().clone();
                    for l in &rr_listeners {
                        l(self, &rr);
                    }
                }
            } else if let Some(p) = pkt.downcast_ref::<TransportLayerNack>() {
                if p.media_ssrc == self.ssrc() {
                    let mut nacks = Vec::new();
                    for pair in &p.nacks {
                        let packet_list = pair.packet_list();
                        num_nacks = num_nacks.wrapping_add(packet_list.len() as u32);
                        nacks.extend_from_slice(&packet_list);
                    }
                    self.post_retransmit_packets(&nacks);
                }
            } else if let Some(p) = pkt.downcast_ref::<TransportLayerCc>() {
                if p.media_ssrc == self.ssrc()
                    && let Some(sal) = self.get_stream_allocator_listener()
                {
                    sal.on_transport_cc_feedback(self, p);
                }
            } else if let Some(p) = pkt.downcast_ref::<ExtendedReport>() {
                // SFU only responds with the DLRRReport for the track has the sender SSRC, the behavior is different with
                // browser's implementation, which includes all sent tracks. It is ok since all the tracks
                // use the same connection, and server-sdk-go can get the rtt from the first DLRRReport
                // (libwebrtc/browsers don't send XR to calculate rtt, it only responds)
                let mut last_rr = 0u32;
                for report in &p.reports {
                    if let Some(rr) = report
                        .as_any()
                        .downcast_ref::<ReceiverReferenceTimeReportBlock>()
                    {
                        last_rr = (rr.ntp_timestamp >> 16) as u32;
                        break;
                    }
                }

                if last_rr > 0
                    && let Some(rtcp_writer) = &self.params.rtcp_writer
                {
                    let _ = rtcp_writer(&[Box::new(ExtendedReport {
                        sender_ssrc: self.ssrc(),
                        reports: vec![Box::new(DLRRReportBlock {
                            reports: vec![DLRRReport {
                                ssrc: p.sender_ssrc,
                                last_rr,
                                dlrr: 0, // no delay
                            }],
                        })],
                    })]);
                }
            }
        }

        self.rtp_stats.update_nack(num_nacks);
        self.rtp_stats.update_pli(num_plis);
        self.rtp_stats.update_fir(num_firs);

        if rtt_to_report != 0 {
            if let Some(sequencer) = self.sequencer.load() {
                sequencer.set_rtt(rtt_to_report);
            }

            if let Some(listener) = self.listener() {
                listener.on_rtt_update(rtt_to_report);
            }
        }
    }

    fn handle_rtcp_rtx(&self, bytes: &[u8]) {
        let pkts = match rtcp::packet::unmarshal(&mut &bytes[..]) {
            Ok(pkts) => pkts,
            Err(err) => {
                errorw!(
                    self.params.logger,
                    "could not unmarshal rtcp rtx receiver packet",
                    err
                );
                return;
            }
        };

        for pkt in &pkts {
            let pkt = pkt.as_any();
            if let Some(p) = pkt.downcast_ref::<ReceiverReport>() {
                for r in &p.reports {
                    if r.ssrc != self.ssrc_rtx() {
                        continue;
                    }

                    self.rtp_stats_rtx.update_from_receiver_report(r);
                }
            } else if let Some(p) = pkt.downcast_ref::<ReceiverEstimatedMaximumBitrate>() {
                if let Some(sal) = self.get_stream_allocator_listener() {
                    sal.on_remb(self, p);
                }
            } else if let Some(p) = pkt.downcast_ref::<TransportLayerCc>()
                && p.media_ssrc == self.ssrc_rtx()
                && let Some(sal) = self.get_stream_allocator_listener()
            {
                sal.on_transport_cc_feedback(self, p);
            }
        }
    }

    pub fn set_connected(&self) {
        let _b = self.bind_lock.lock();
        if self.is_closed.load() {
            return;
        }
        if !self.connected.swap(true) {
            self.on_bind_and_connected_change();
        }
        debugw!(self.params.logger, "downtrack connected");
    }

    // SetActivePaddingOnMuteUpTrack will enable padding on the track when its uptrack is muted.
    // Pion will not fire OnTrack event until it receives packet for the track,
    // so we send padding packets to help pion client (go-sdk) to fire the event.
    pub fn set_active_padding_on_mute_up_track(&self) {
        self.active_padding_on_mute_up_track.store(true);
    }

    fn retransmit_packet(
        &self,
        epm: &ExtPacketMeta,
        source_pkt: &[u8],
        is_probe: bool,
    ) -> Result<usize, Error> {
        let mut pkt = rtp::Packet::default();
        if let Err(err) = pkt.unmarshal(&Bytes::copy_from_slice(source_pkt)) {
            errorw!(
                self.params.logger,
                "could not unmarshal rtp packet to send via RTX",
                err
            );
            return Err(err.into());
        }
        let pm = &epm.packet_meta;
        // Defensive panic-safety net: the codec header size was recorded when the packet was first
        // forwarded and is re-read here against a bucket packet. A stream restart flushes the
        // sequencer (see flushSequencer), so metadata and payload should always agree; guard
        // against a slice overflow regardless.
        if pm.num_codec_bytes_in as usize > pkt.payload.len() {
            warnw!(
                self.params.logger,
                "recorded codec header size overflows payload", Error::PayloadOverflow,
                "numCodecBytesIn" => pm.num_codec_bytes_in,
                "payloadSize" => pkt.payload.len(),
            );
            return Err(Error::PayloadOverflow);
        }
        let mut hdr = rtp::Header {
            version: pkt.header.version,
            padding: false,
            marker: pm.marker,
            payload_type: self.get_translated_payload_type(pkt.header.payload_type),
            sequence_number: pm.target_seq_no,
            timestamp: pm.timestamp,
            ssrc: self.ssrc(),
            ..Default::default()
        };
        let mut rtx_offset = 0;
        let mut rtx_ext_sequence_number = 0u64;
        let rtx_pt = self.payload_type_rtx.load();
        if rtx_pt != 0 && self.ssrc_rtx() != 0 {
            rtx_ext_sequence_number = self.rtx_sequence_number.inc();
            rtx_offset = 2;

            hdr.payload_type = rtx_pt as u8;
            hdr.sequence_number = rtx_ext_sequence_number as u16;
            hdr.ssrc = self.ssrc_rtx();
        }

        let dependency_descriptor_ext_id = self.dependency_descriptor_ext_id.load();
        if dependency_descriptor_ext_id != 0 {
            let dd_bytes: &[u8] = if !pm.dd_bytes_slice.is_empty() {
                &pm.dd_bytes_slice
            } else {
                &pm.dd_bytes[..pm.dd_bytes_size as usize]
            };
            if !dd_bytes.is_empty() {
                let _ = hdr.set_extension(
                    dependency_descriptor_ext_id as u8,
                    Bytes::copy_from_slice(dd_bytes),
                );
            }
        }
        let abs_capture_time_ext_id = self.abs_capture_time_ext_id.load();
        if abs_capture_time_ext_id != 0 && pm.act_bytes_size != 0 {
            let _ = hdr.set_extension(
                abs_capture_time_ext_id as u8,
                Bytes::copy_from_slice(&pm.act_bytes[..pm.act_bytes_size as usize]),
            );
        }
        self.add_dummy_extensions(&mut hdr);

        // Go's `copy` into what is left of a buffer of the original's pool
        fn copy(payload: &mut Vec<u8>, src: &[u8]) {
            let n = src.len().min(PACKET_FACTORY_SIZE - payload.len());
            payload.extend_from_slice(&src[..n]);
        }
        let incoming_payload = &pkt.payload[pm.num_codec_bytes_in as usize..];
        let mut payload = Vec::with_capacity(
            PACKET_FACTORY_SIZE
                .min(rtx_offset + pm.codec_bytes_slice.len().max(8) + incoming_payload.len()),
        );
        if rtx_offset != 0 {
            // write OSN (Original Sequence Number)
            payload.extend_from_slice(&pm.target_seq_no.to_be_bytes());
        }
        // (in its second branch the original takes the length from the sizes, which panics
        // for a payload that the buffer has no room for; there is none: what is read back
        // from the receiver fits a buffer of the same size)
        if !pm.codec_bytes_slice.is_empty() {
            copy(&mut payload, &pm.codec_bytes_slice);
            copy(&mut payload, incoming_payload);
        } else {
            copy(
                &mut payload,
                &pm.codec_bytes[..pm.num_codec_bytes_out as usize],
            );
            copy(&mut payload, incoming_payload);
        }

        // replay the strip done on the original transmission to keep the retransmitted
        // payload byte identical to it
        if pm.trailer_stripped != 0 {
            if pm.trailer_stripped as usize > payload.len() - rtx_offset {
                warnw!(
                    self.params.logger,
                    "recorded packet trailer size overflows payload", Error::PayloadOverflow,
                    "trailerStripped" => pm.trailer_stripped,
                    "payloadSize" => payload.len() - rtx_offset,
                );
            } else {
                payload.truncate(payload.len() - pm.trailer_stripped as usize);
            }
        }

        let header_size = hdr.marshal_size();
        let (payload_size, padding_size, is_out_of_order) = if is_probe {
            // although not padding only packets, marking it as padding for accounting as padding is used to signify probing,
            // also not marking them as out-of-order although sequence numbers in packets are out-of-order because of re-sending packets
            (0, payload.len(), false)
        } else {
            (payload.len(), 0, true)
        };
        if hdr.ssrc == self.ssrc_rtx() {
            self.rtp_stats_rtx.update(
                mono::unix_nano(),
                rtx_ext_sequence_number,
                0,
                hdr.marker,
                header_size,
                payload_size,
                padding_size,
                is_out_of_order,
            );
        } else {
            self.rtp_stats.update(
                mono::unix_nano(),
                epm.ext_sequence_number,
                epm.ext_timestamp,
                hdr.marker,
                header_size,
                payload_size,
                padding_size,
                is_out_of_order,
            );
        }
        let sent = header_size + payload.len();
        let mut pacer_packet = pacer::Packet::new(self.write_stream());
        pacer_packet.header = hdr;
        pacer_packet.header_size = header_size;
        pacer_packet.payload = Bytes::from(payload);
        pacer_packet.probe_cluster_id = ccutils::ProbeClusterId(self.probe_cluster_id.load());
        pacer_packet.is_probe = is_probe;
        pacer_packet.is_rtx = !is_probe;
        pacer_packet.abs_send_time_ext_id = self.abs_send_time_ext_id.load() as u8;
        pacer_packet.transport_wide_ext_id = self.transport_wide_ext_id.load() as u8;
        self.pacer.enqueue(pacer_packet);
        Ok(sent)
    }

    // postRetransmitPackets buffers the NACKed sequence numbers, capped at
    // maxPendingNACKs, and signals the retransmit worker without blocking.
    fn post_retransmit_packets(&self, nacks: &[u16]) {
        if self.sequencer.load().is_none() || nacks.is_empty() {
            return;
        }

        {
            let mut pending_nacks = self.pending_nacks.lock();
            pending_nacks.extend_from_slice(nacks);
            if pending_nacks.len() > MAX_PENDING_NACKS {
                // drop the oldest NACKs under a NACK flood
                let excess = pending_nacks.len() - MAX_PENDING_NACKS;
                pending_nacks.drain(..excess);
            }
        }

        let ch = self.retransmit_ch.read();
        if let Some(ch) = ch.as_ref() {
            let _ = ch.try_send(());
        }
    }

    async fn retransmit_worker(this: Weak<DownTrack>, mut ch: mpsc::Receiver<()>) {
        loop {
            if ch.recv().await.is_none() {
                return;
            }
            let Some(d) = this.upgrade() else {
                return;
            };

            let nacks = std::mem::take(&mut *d.pending_nacks.lock());

            if !nacks.is_empty() {
                d.retransmit_packets(&nacks);
            }
        }
    }

    fn retransmit_packets(&self, nacks: &[u16]) {
        let Some(sequencer) = self.sequencer.load() else {
            return;
        };

        if FLAG_STOP_RTX_ON_PLI && self.is_nack_throttled.load() {
            return;
        }

        let (filtered, disallowed_layers) = self.forwarder.filter_rtx(nacks);
        if filtered.is_empty() {
            return;
        }

        let mut src = vec![0u8; PACKET_FACTORY_SIZE];

        let receiver = self.receiver();

        let mut nack_acks = 0u32;
        let mut nack_misses = 0u32;
        let mut num_repeated_nacks = 0u32;
        let mut capped: Vec<ExtPacketMeta> = Vec::new();
        let mut epms = sequencer.get_ext_packet_metas(&filtered);
        if self.rtx_budget.is_some() {
            // newest first: when the budget runs short, the oldest packets are the least likely
            // to still arrive in time for the receiver's jitter buffer. A batch can hold several
            // NACK messages, so sort instead of relying on their order.
            epms.sort_by_key(|epm| std::cmp::Reverse(epm.ext_sequence_number));
        }
        for epm in &epms {
            if disallowed_layers[epm.packet_meta.layer as usize] {
                continue;
            }

            // over budget: skip without reading the publisher's buffer
            if !capped.is_empty() || !RtxBudget::allow(self.rtx_budget.as_ref()) {
                capped.push(epm.clone());
                continue;
            }

            nack_acks += 1;

            let n = match receiver.read_rtp(
                &mut src,
                epm.packet_meta.layer as u8,
                epm.packet_meta.source_seq_no,
            ) {
                Ok(n) => n,
                Err(err) => {
                    if err == Error::Eof {
                        break;
                    }
                    nack_misses += 1;
                    continue;
                }
            };

            if epm.packet_meta.nacked > 1 {
                num_repeated_nacks += 1;
            }

            if let Ok(sent) = self.retransmit_packet(epm, &src[..n], false) {
                RtxBudget::spend(self.rtx_budget.as_ref(), sent);
            }
        }
        if !capped.is_empty() {
            // a capped packet was not sent, so it does not count toward the sequencer's NACK limit
            sequencer.unmark_nacked(&capped);
            if let Some(rtx_budget) = &self.rtx_budget {
                rtx_budget.num_capped.add(capped.len() as u64);
            }
            prometheus::record_rtx_capped(&self.kind.to_string(), capped.len() as u32);
        }

        self.total_repeated_nacks.add(num_repeated_nacks);

        self.rtp_stats
            .update_nack_processed(nack_acks, nack_misses, num_repeated_nacks);
    }

    pub fn write_probe_packets(&self, bytes_to_send: i64, use_padding: bool) -> i64 {
        let rtx_pt = self.payload_type_rtx.load() as u8;
        if rtx_pt == 0 || self.ssrc_rtx() == 0 {
            return self.write_padding_rtp(bytes_to_send, false, false);
        }

        let sequencer = self.sequencer.load();
        if !self.writable.load()
            || !self.rtp_stats.is_active()
            || (self.abs_send_time_ext_id.load() == 0 && self.transport_wide_ext_id.load() == 0)
            || self.rtp_stats.last_receiver_report_time() == 0
        {
            return 0;
        }
        let Some(sequencer) = sequencer else {
            return 0;
        };

        let mut bytes_sent = 0i64;

        if use_padding {
            let num = (bytes_to_send
                + RTP_PADDING_MAX_PAYLOAD_SIZE as i64
                + RTP_PADDING_ESTIMATED_HEADER_SIZE as i64
                - 1)
                / (RTP_PADDING_MAX_PAYLOAD_SIZE as i64 + RTP_PADDING_ESTIMATED_HEADER_SIZE as i64);
            if num == 0 {
                return 0;
            }

            for _ in 0..num {
                let rtx_ext_sequence_number = self.rtx_sequence_number.inc();
                let mut hdr = rtp::Header {
                    version: 2,
                    padding: true,
                    padding_size: RTP_PADDING_MAX_PAYLOAD_SIZE as u8,
                    marker: false,
                    payload_type: rtx_pt,
                    sequence_number: rtx_ext_sequence_number as u16,
                    timestamp: 0,
                    ssrc: self.ssrc_rtx(),
                    ..Default::default()
                };
                self.add_dummy_extensions(&mut hdr);

                let hdr_size = hdr.marshal_size();
                self.rtp_stats_rtx.update(
                    mono::unix_nano(),
                    rtx_ext_sequence_number,
                    0,
                    hdr.marker,
                    hdr_size,
                    0,
                    RTP_PADDING_MAX_PAYLOAD_SIZE,
                    false,
                );
                let mut pacer_packet = pacer::Packet::new(self.write_stream());
                pacer_packet.header = hdr;
                pacer_packet.header_size = hdr_size;
                pacer_packet.probe_cluster_id =
                    ccutils::ProbeClusterId(self.probe_cluster_id.load());
                pacer_packet.is_probe = true;
                pacer_packet.abs_send_time_ext_id = self.abs_send_time_ext_id.load() as u8;
                pacer_packet.transport_wide_ext_id = self.transport_wide_ext_id.load() as u8;
                self.pacer.enqueue(pacer_packet);

                bytes_sent += (hdr_size + RTP_PADDING_MAX_PAYLOAD_SIZE) as i64;
            }
        } else {
            let mut src = vec![0u8; PACKET_FACTORY_SIZE];

            let receiver = self.receiver();

            let end_ext_highest_sequence_number = self.rtp_stats.ext_highest_sequence_number();
            let start_ext_highest_sequence_number = end_ext_highest_sequence_number.wrapping_sub(5);
            for esn in start_ext_highest_sequence_number..=end_ext_highest_sequence_number {
                let Some(epm) = sequencer.lookup_ext_packet_meta(esn) else {
                    continue;
                };

                let n = match receiver.read_rtp(
                    &mut src,
                    epm.packet_meta.layer as u8,
                    epm.packet_meta.source_seq_no,
                ) {
                    Ok(n) => n,
                    Err(err) => {
                        if err == Error::Eof {
                            break;
                        }
                        continue;
                    }
                };

                let sent = self.retransmit_packet(&epm, &src[..n], true).unwrap_or(0);
                bytes_sent += sent as i64;
                if bytes_sent >= bytes_to_send {
                    break;
                }
            }

            // recent packets may be gone from the publisher buffer, for example while
            // paused, so probe with padding to keep the probe cluster progressing
            if bytes_sent == 0 {
                return self.write_probe_packets(bytes_to_send, true);
            }
        }

        bytes_sent
    }

    fn add_dummy_extensions(&self, hdr: &mut rtp::Header) {
        // add dummy extensions (actual ones will be filed by pacer) to get header size
        let abs_send_time_ext_id = self.abs_send_time_ext_id.load();
        if abs_send_time_ext_id != 0 {
            let _ = hdr.set_extension(abs_send_time_ext_id as u8, DUMMY_ABS_SEND_TIME_EXT.clone());
        }
        let transport_wide_ext_id = self.transport_wide_ext_id.load();
        if transport_wide_ext_id != 0 {
            let _ = hdr.set_extension(transport_wide_ext_id as u8, DUMMY_TRANSPORT_CC_EXT.clone());
        }
    }

    fn get_translated_payload_type(&self, src_pt: u8) -> u8 {
        // send primary codec to subscriber if the publisher sent primary codec when red is negotiated,
        // this will happen when the payload is too large to encode into red payload (exceeds mtu).
        if self.is_red.load()
            && src_pt == self.upstream_primary_pt.load() as u8
            && self.primary_pt.load() != 0
        {
            return self.primary_pt.load() as u8;
        }
        self.payload_type.load() as u8
    }

    pub fn debug_info(&self) -> DebugInfo {
        let mut stats = DebugInfo::new();
        stats.insert("LastPli".to_string(), self.rtp_stats.last_pli().into());
        let munger = self.forwarder.rtp_munger_debug_info();
        let mut rtp_munger = DebugInfo::new();
        rtp_munger.insert(
            "ExtHighestIncomingSN".to_string(),
            munger.ext_highest_incoming_sn.into(),
        );
        rtp_munger.insert("ExtLastSN".to_string(), munger.ext_last_sn.into());
        rtp_munger.insert(
            "ExtSecondLastSN".to_string(),
            munger.ext_second_last_sn.into(),
        );
        rtp_munger.insert("SNOffset".to_string(), munger.sn_offset.into());
        rtp_munger.insert("ExtLastTS".to_string(), munger.ext_last_ts.into());
        rtp_munger.insert(
            "ExtSecondLastTS".to_string(),
            munger.ext_second_last_ts.into(),
        );
        rtp_munger.insert("TSOffset".to_string(), munger.ts_offset.into());
        rtp_munger.insert("LastMarker".to_string(), munger.last_marker.into());
        rtp_munger.insert(
            "SecondLastMarker".to_string(),
            munger.second_last_marker.into(),
        );
        stats.insert("RTPMunger".to_string(), rtp_munger.into());

        let sender_report = self.create_sender_report();
        if let Some(sender_report) = sender_report {
            stats.insert("NTPTime".to_string(), sender_report.ntp_time.into());
            stats.insert("RTPTime".to_string(), sender_report.rtp_time.into());
            stats.insert("PacketCount".to_string(), sender_report.packet_count.into());
        }

        let mut info = DebugInfo::new();
        info.insert(
            "SubscriberID".to_string(),
            self.params.sub_id.to_string().into(),
        );
        info.insert("TrackID".to_string(), self.id.to_string().into());
        info.insert("StreamID".to_string(), self.params.stream_id.clone().into());
        info.insert("SSRC".to_string(), self.ssrc().into());
        info.insert("MimeType".to_string(), self.mime().to_string().into());
        info.insert("BindState".to_string(), (self.bind_state() as i32).into());
        info.insert("Muted".to_string(), self.forwarder.is_muted().into());
        info.insert("PubMuted".to_string(), self.forwarder.is_pub_muted().into());
        info.insert(
            "CurrentSpatialLayer".to_string(),
            self.forwarder.current_layer().spatial.into(),
        );
        info.insert("Stats".to_string(), stats.into());
        info
    }

    pub fn get_connection_score_and_quality(&self) -> (f32, livekit::ConnectionQuality) {
        self.connection_stats.get_score_and_quality()
    }

    // OnStatsUpdate registers an additional callback that fires alongside the
    // configured DownTrackListener whenever connection-quality stats are produced.
    // Intended for tests and observers; the production listener path is unaffected.
    pub fn on_stats_update(
        &self,
        f: impl Fn(&DownTrack, &livekit::AnalyticsStat) + Send + Sync + 'static,
    ) {
        self.on_stats_update.store(Arc::new(f));
    }

    pub fn get_track_stats(&self) -> Option<livekit::RtpStats> {
        rtpstats::reconcile_rtp_stats_with_rtx(
            self.rtp_stats.to_proto(),
            self.rtp_stats_rtx.to_proto().as_ref(),
        )
    }

    fn delta_stats(
        &self,
        ds: Option<rtpstats::RtpDeltaInfo>,
        dsrv: Option<rtpstats::RtpDeltaInfo>,
    ) -> std::collections::HashMap<u32, buffer::StreamStatsWithLayers> {
        let mut stream_stats = std::collections::HashMap::with_capacity(1);
        if ds.is_none() && dsrv.is_none() {
            return stream_stats;
        }

        let ds = ds.map(Arc::new);
        stream_stats.insert(
            self.ssrc(),
            buffer::StreamStatsWithLayers {
                rtp_stats: ds.clone(),
                rtp_stats_remote_view: dsrv.map(Arc::new),
                layers: std::collections::HashMap::from([(0, ds)]),
            },
        );

        stream_stats
    }

    pub fn get_delta_stats_sender(
        &self,
    ) -> std::collections::HashMap<u32, buffer::StreamStatsWithLayers> {
        let (ds, dsrv) = self
            .rtp_stats
            .delta_info_sender(self.delta_stats_sender_snapshot_id.load());
        let (ds_rtx, dsrv_rtx) = self
            .rtp_stats_rtx
            .delta_info_sender(self.delta_stats_rtx_sender_snapshot_id.load());
        self.delta_stats(
            rtpstats::reconcile_rtp_delta_info_with_rtx(ds, ds_rtx.as_ref()),
            rtpstats::reconcile_rtp_delta_info_with_rtx(dsrv, dsrv_rtx.as_ref()),
        )
    }

    pub fn get_primary_stream_last_receiver_report_time(&self) -> Time {
        Time::unix(0, self.rtp_stats.last_receiver_report_time())
    }

    pub fn get_primary_stream_packets_sent(&self) -> u64 {
        self.rtp_stats.get_packets_seen_minus_padding()
    }

    // (totalPackets, totalRepeatedNACKs)
    pub fn get_nack_stats(&self) -> (u32, u32) {
        let total_packets = self.rtp_stats.get_packets_seen_minus_padding() as u32;
        let total_repeated_nacks = self.total_repeated_nacks.load();
        (total_packets, total_repeated_nacks)
    }

    fn on_bind_and_connected_change(&self) {
        if self.write_stopped.load() {
            return;
        }
        let writable = self.connected.load() && self.bind_state() == BindState::Bound;
        if writable && !self.writable.load() {
            self.writable_at.store(time::now());
        }
        self.writable.store(writable);
        if writable && !self.bind_and_connected_once.swap(true) {
            if let Some(listener) = self.listener() {
                time::spawn(async move {
                    listener.on_bind_and_connected();
                });
            }

            if self.active_padding_on_mute_up_track.load() {
                time::spawn(Self::send_padding_on_mute(self.this.clone()));
            }

            // kick off PLI request if allocation is pending
            self.post_key_frame_request_event();
        }
    }

    async fn send_padding_on_mute(this: Weak<DownTrack>) {
        // let uptrack have chance to send packet before we send padding
        time::sleep(WAIT_BEFORE_SEND_PADDING_ON_MUTE).await;

        let Some(kind) = this.upgrade().map(|d| d.kind) else {
            return;
        };
        if kind == webrtc::RtpCodecType::Video {
            Self::send_padding_on_mute_for_video(this).await;
        } else {
            Self::send_silent_frame_on_mute_for_audio(this).await;
        }
    }

    async fn send_padding_on_mute_for_video(this: Weak<DownTrack>) {
        let num_packets = MAX_PADDING_ON_MUTE_DURATION / PADDING_ON_MUTE_INTERVAL;
        for i in 0..num_packets {
            {
                let Some(d) = this.upgrade() else {
                    return;
                };
                if d.rtp_stats.is_active() || d.is_closed() {
                    return;
                }
                if i == 0 {
                    debugw!(d.params.logger, "sending padding on mute");
                }
                d.write_padding_rtp(20, true, true);
            }
            time::sleep(PADDING_ON_MUTE_INTERVAL).await;
        }
    }

    async fn send_silent_frame_on_mute_for_audio(this: Weak<DownTrack>) {
        let payload = {
            let Some(d) = this.upgrade() else {
                return;
            };
            let payload = match d.mime() {
                mime::MimeType::Opus => d.get_audio_blank_frame_func(&OPUS_SILENCE_FRAME)(false),
                mime::MimeType::Red => d.get_opus_red_blank_frame(false),
                mime::MimeType::Pcmu => d.get_audio_blank_frame_func(&PCMU_SILENCE_FRAME)(false),
                mime::MimeType::Pcma => d.get_audio_blank_frame_func(&PCMA_SILENCE_FRAME)(false),
                _ => {
                    infow!(d.params.logger, "unsupported mime type for silent frame on mute", "mimeType" => d.mime());
                    return;
                }
            };
            match payload {
                Ok(payload) => Bytes::from(payload),
                Err(err) => {
                    warnw!(d.params.logger, "could not get blank frame", err);
                    return;
                }
            }
        };

        let frame_rate = 50u32;
        let frame_duration = Duration::from_millis((1000 / frame_rate) as i64);
        let mut num_frames = frame_rate * (MAX_PADDING_ON_MUTE_DURATION / time::SECOND) as u32;
        let mut first = true;
        loop {
            {
                let Some(d) = this.upgrade() else {
                    return;
                };
                if d.rtp_stats.is_active() || d.is_closed() || num_frames == 0 {
                    return;
                }
                if first {
                    first = false;
                    debugw!(d.params.logger, "sending padding on mute");
                }
                let snts = match d.forwarder.get_sn_ts_for_blank_frames(frame_rate, 1) {
                    Ok((snts, _)) => snts,
                    Err(err) => {
                        warnw!(d.params.logger, "could not get SN/TS for blank frame", err);
                        return;
                    }
                };
                for snt in &snts {
                    let mut hdr = rtp::Header {
                        version: 2,
                        padding: false,
                        marker: true,
                        payload_type: d.payload_type.load() as u8,
                        sequence_number: snt.ext_sequence_number as u16,
                        timestamp: snt.ext_timestamp as u32,
                        ssrc: d.ssrc(),
                        ..Default::default()
                    };
                    d.add_dummy_extensions(&mut hdr);
                    let header_size = hdr.marshal_size();
                    d.rtp_stats.update(
                        mono::unix_nano(),
                        snt.ext_sequence_number,
                        snt.ext_timestamp,
                        hdr.marker,
                        header_size,
                        0,
                        payload.len(), // although this is using empty frames, mark as padding as these are used to trigger Pion OnTrack only
                        false,
                    );
                    let mut pacer_packet = pacer::Packet::new(d.write_stream());
                    pacer_packet.header = hdr;
                    pacer_packet.header_size = header_size;
                    pacer_packet.payload = payload.clone();
                    pacer_packet.probe_cluster_id =
                        ccutils::ProbeClusterId(d.probe_cluster_id.load());
                    pacer_packet.abs_send_time_ext_id = d.abs_send_time_ext_id.load() as u8;
                    pacer_packet.transport_wide_ext_id = d.transport_wide_ext_id.load() as u8;
                    d.pacer.enqueue(pacer_packet);
                }
            }

            num_frames -= 1;
            time::sleep(frame_duration).await;
        }
    }

    pub fn handle_rtcp_sender_report_data(
        &self,
        _payload_type: webrtc::PayloadType,
        layer: i32,
        publisher_sr_data: &livekit::RtcpSenderReportState,
    ) -> Result<(), Error> {
        self.forwarder
            .set_ref_sender_report(layer, publisher_sr_data);

        let (current_layer, is_single_stream, ts_offset, ref_sender_report) =
            self.forwarder.get_sender_report_params();
        if layer == current_layer || (layer == 0 && is_single_stream) {
            self.handle_rtcp_sender_report_data_inner(ref_sender_report.as_ref(), ts_offset);
        }
        Ok(())
    }

    // The original's unexported `handleRTCPSenderReportData`.
    fn handle_rtcp_sender_report_data_inner(
        &self,
        publisher_sr_data: Option<&livekit::RtcpSenderReportState>,
        ts_offset: u64,
    ) {
        self.rtp_stats
            .maybe_adjust_first_packet_time(publisher_sr_data, ts_offset);
    }

    // `d.params.Listener`, if it is still there.
    fn listener(&self) -> Option<Arc<dyn DownTrackListener>> {
        self.params.listener.upgrade()
    }

    // `d.writeStream`: set by the first bind, before anything can be written. The original
    // hands the pacer a nil write stream otherwise, which panics when the packet is sent.
    fn write_stream(&self) -> Arc<dyn webrtc::TrackLocalWriter> {
        self.write_stream
            .load()
            .expect("the down track writes before it was bound")
    }
}

// -------------------------------------------------------------------------------

// `fmt.Sprintf("%s:%d", d.params.SubID, d.createdAt)`
fn subscriber_id(sub_id: &livekit::ParticipantId, created_at: i64) -> livekit::ParticipantId {
    livekit::ParticipantId::from(format!("{sub_id}:{created_at}"))
}

// Go's `time.NewTicker(d)`: the first tick comes after `d`, and ticks that the receiver is
// too slow for are dropped.
fn new_ticker(d: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + d.to_std(), d.to_std());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker
}

// Go's `fmt.Sscanf(s, "%d/%d", &a, &b)` for the parameters of RED (`111/111`): the two
// numbers and how many of them were read. A number that was not read is zero, as the
// variable the original scans into stays. `%d` skips blanks, takes a sign and decimal
// digits; what follows the second number is not looked at.
fn sscanf_two_ints(s: &str) -> (i64, i64, usize) {
    fn scan_int(s: &[u8], pos: &mut usize) -> Option<i64> {
        while *pos < s.len() && (s[*pos] == b' ' || s[*pos] == b'\t') {
            *pos += 1;
        }
        let start = *pos;
        if *pos < s.len() && (s[*pos] == b'+' || s[*pos] == b'-') {
            *pos += 1;
        }
        let digits = *pos;
        while *pos < s.len() && s[*pos].is_ascii_digit() {
            *pos += 1;
        }
        if *pos == digits {
            return None;
        }
        std::str::from_utf8(&s[start..*pos]).ok()?.parse().ok()
    }

    let s = s.as_bytes();
    let mut pos = 0;
    let Some(a) = scan_int(s, &mut pos) else {
        return (0, 0, 0);
    };
    if pos >= s.len() || s[pos] != b'/' {
        return (a, 0, 1);
    }
    pos += 1;
    let Some(b) = scan_int(s, &mut pos) else {
        return (a, 0, 1);
    };
    (a, b, 2)
}

// -------------------------------------------------------------------------------

impl TrackSender for DownTrack {
    fn up_track_layers_change(&self) {
        DownTrack::up_track_layers_change(self);
    }

    fn up_track_bitrate_availability_change(&self) {
        DownTrack::up_track_bitrate_availability_change(self);
    }

    fn up_track_max_published_layer_change(&self, max_published_layer: i32) {
        DownTrack::up_track_max_published_layer_change(self, max_published_layer);
    }

    fn up_track_max_temporal_layer_seen_change(&self, max_temporal_layer_seen: i32) {
        DownTrack::up_track_max_temporal_layer_seen_change(self, max_temporal_layer_seen);
    }

    fn up_track_bitrate_report(&self, available_layers: &[i32], bitrates: Bitrates) {
        DownTrack::up_track_bitrate_report(self, available_layers, bitrates);
    }

    fn write_rtp(&self, p: &buffer::ExtPacket, layer: i32) -> i32 {
        DownTrack::write_rtp(self, p, layer)
    }

    fn close(&self) {
        DownTrack::close(self);
    }

    fn is_closed(&self) -> bool {
        DownTrack::is_closed(self)
    }

    fn id(&self) -> String {
        DownTrack::id(self)
    }

    fn subscriber_id(&self) -> livekit::ParticipantId {
        DownTrack::subscriber_id(self)
    }

    fn handle_rtcp_sender_report_data(
        &self,
        payload_type: webrtc::PayloadType,
        layer: i32,
        publisher_sr_data: &livekit::RtcpSenderReportState,
    ) -> Result<(), Error> {
        DownTrack::handle_rtcp_sender_report_data(self, payload_type, layer, publisher_sr_data)
    }

    fn resync(&self) {
        DownTrack::resync(self);
    }

    fn set_receiver(&self, r: Arc<dyn TrackReceiver>) {
        DownTrack::set_receiver(self, r);
    }

    fn receiver_restart(&self, r: Arc<dyn TrackReceiver>) {
        DownTrack::receiver_restart(self, r);
    }
}

impl webrtc::TrackLocal for DownTrack {
    fn bind(
        &self,
        t: Arc<dyn webrtc::TrackLocalContext>,
    ) -> Result<webrtc::RtpCodecParameters, webrtc::Error> {
        DownTrack::bind(self, t).map_err(|err| webrtc::Error::Other(err.to_string()))
    }

    fn unbind(&self, t: Arc<dyn webrtc::TrackLocalContext>) -> Result<(), webrtc::Error> {
        DownTrack::unbind(self, t).map_err(|err| webrtc::Error::Other(err.to_string()))
    }

    fn id(&self) -> String {
        DownTrack::id(self)
    }

    fn rid(&self) -> String {
        DownTrack::rid(self)
    }

    fn stream_id(&self) -> String {
        DownTrack::stream_id(self)
    }

    fn kind(&self) -> webrtc::RtpCodecType {
        DownTrack::kind(self)
    }
}

impl connectionquality::ConnectionStatsSenderProvider for DownTrack {
    fn get_delta_stats_sender(
        &self,
    ) -> std::collections::HashMap<u32, buffer::StreamStatsWithLayers> {
        DownTrack::get_delta_stats_sender(self)
    }

    fn get_primary_stream_last_receiver_report_time(&self) -> Time {
        DownTrack::get_primary_stream_last_receiver_report_time(self)
    }

    fn get_primary_stream_packets_sent(&self) -> u64 {
        DownTrack::get_primary_stream_packets_sent(self)
    }
}

#[cfg(test)]
#[path = "export_test.rs"]
mod export_test;

#[cfg(test)]
#[path = "downtrack_downstream_integration_test.rs"]
mod downtrack_downstream_integration_test;
