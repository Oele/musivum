// Ported from LiveKit, pkg/sfu/forwarder.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - What the lock guards is `ForwarderInner`, in an `RwLock`; the kind, the logger, the flag
//   that turns opportunistic allocation off and the statistics are never changed and stand
//   outside it. The functions the original calls with its lock held take the inner struct
//   as an argument (`f`), and so do the closures that the original lets capture the
//   forwarder.
// - Swapping the layer selector: the original's `New...FromOther(f.vls)` shares the old
//   selector's `*Base` with the new one. Here the new selector takes the old one over, so
//   it is taken out of its field with `take_vls`, which leaves a `videolayerselector::Null`
//   there until the new selector is assigned in the same statement. That is how it is done
//   in every place. The field is never empty, so the original's branches for `f.vls == nil`
//   are gone. The codec munger's `new_from_other` only reads the old munger and needs none
//   of this. The dependency descriptor forwarder and its seed are `Option`s.
// - Where the original returns a value next to an error that its caller still reads, the
//   two are a pair: `get_translation_params` gives `(TranslationParams, Result<(), Error>)`
//   (the down track reads `should_drop` and `pad_dropped_slot` first and the error after),
//   and so do the functions under it that give `starting` with an error. The other errors
//   are in a `Result`.
// - `TranslationParams` keeps its fields, visible to the package as in the original.
//   `dd_bytes_spill` is a `Vec`, empty where the original's is nil.
// - `VideoPauseReason` is an enum; `String()` is `Display`; `MarshalLogObject` is `Debug`,
//   derived for `VideoAllocation` and `VideoTransition` and written for `RefInfo`, whose
//   sender report is logged through `WrappedRtcpSenderReportStateLogger` as in the original.
// - `provisional` is an `Option`: the functions that need it panic before
//   `provisional_allocate_prepare`, where the original dereferences nil. The two functions
//   that give a transition return a copy of the available layers, where the original
//   returns the slice it keeps; a slice that is nil and one that is empty are the same here.
// - `determine_codec` takes the codec by reference. `set_ref_sender_report` takes the report
//   by reference and keeps a copy; `get_sender_report_params` gives a copy.
// - `get_state`: the sender reports are a list of messages, which cannot hold nil, so a
//   layer without one has the empty message (which `seed_state` takes for none, as the
//   original does with what arrives over the wire). `seed_state`: a state without
//   `rtp_munger` leaves the RTP munger as it is, where the original dereferences nil.
// - `get_sn_ts_for_blank_frames` gives `(snts, frame_end_needed)` in a `Result`;
//   `rtp_munger_debug_info` gives the munger's `RtpMungerDebugInfo`; `filter_rtx` returns a
//   vector also when it filters nothing.
// - The `break` out of the `switch` in `determine_codec` is an `else`.

use std::fmt;
use std::sync::Arc;

use parking_lot::RwLock;
use rand::RngExt;

use super::{
    Bitrates, Error, RtpMunger, RtpMungerDebugInfo, SequenceNumberOrdering, SnTs,
    TranslationParamsRtp,
};
use crate::go::time::{self, Duration, Time};
use crate::mediatransportutil::NtpTime;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;
use crate::sfu::buffer;
use crate::sfu::codecmunger;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::sfu::rtpstats;
use crate::sfu::utils as sfuutils;
use crate::sfu::videolayerselector;
use crate::sfu::videolayerselector::temporallayerselector;
use crate::webrtc;
use crate::webrtc::rtp;
use crate::{debugw, errorw, infow};

pub const FLAG_PAUSE_ON_DOWNGRADE: bool = true;
pub const FLAG_FILTER_RTX: bool = false;
pub const FLAG_FILTER_RTX_LAYERS: bool = true;
pub const TRANSITION_COST_SPATIAL: i32 = 10;

pub const RESUME_BEHIND_THRESHOLD_SECONDS: f64 = 0.2; // 200ms
pub const RESUME_BEHIND_HIGH_THRESHOLD_SECONDS: f64 = 2.0; // 2 seconds
pub const LAYER_SWITCH_BEHIND_THRESHOLD_SECONDS: f64 = 0.05; // 50ms
pub const SWITCH_AHEAD_THRESHOLD_SECONDS: f64 = 0.025; // 25ms

// While a subscriber is acquiring its first layer and the requested (max) layer has not been
// seen on the wire yet, aim straight for the requested layer for this long instead of latching
// onto a lower layer that is detected first. Avoids a visible low -> high quality ramp,
// notably when the subscriber joined before the publisher started (so layers are detected, and
// `maxSeen` climbs, gradually). If the requested layer does not show up within this window the
// grace expires and forwarding falls back to the highest layer actually seen.
// See Forwarder.opportunisticAlloc / withinAcquireGraceLocked / MaybeExpireAcquireGrace.
const INITIAL_LAYER_ACQUISITION_GRACE: Duration = time::SECOND;

// -------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VideoPauseReason {
    #[default]
    None,
    Muted,
    PubMuted,
    FeedDry,
    Bandwidth,
}

impl fmt::Display for VideoPauseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            VideoPauseReason::None => "NONE",
            VideoPauseReason::Muted => "MUTED",
            VideoPauseReason::PubMuted => "PUB_MUTED",
            VideoPauseReason::FeedDry => "FEED_DRY",
            VideoPauseReason::Bandwidth => "BANDWIDTH",
        })
    }
}

// -------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct VideoAllocation {
    pub pause_reason: VideoPauseReason,
    pub is_deficient: bool,
    pub bandwidth_requested: i64,
    pub bandwidth_delta: i64,
    pub bandwidth_needed: i64,
    pub bitrates: Bitrates,
    pub target_layer: buffer::VideoLayer,
    pub request_layer_spatial: i32,
    pub max_layer: buffer::VideoLayer,
    pub distance_to_desired: f64,
}

impl fmt::Display for VideoAllocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "VideoAllocation{{pause: {}, def: {}, bwr: {}, del: {}, bwn: {}, rates: {}, target: {}, req: {}, max: {}, dist: {:.2}}}",
            self.pause_reason,
            self.is_deficient,
            self.bandwidth_requested,
            self.bandwidth_delta,
            self.bandwidth_needed,
            BitratesDisplay(&self.bitrates),
            self.target_layer,
            self.request_layer_spatial,
            self.max_layer,
            self.distance_to_desired,
        )
    }
}

// Go's `%+v` of the bitrates: `[[1 2 3 4] [5 6 7 8] [9 10 11 12]]`.
struct BitratesDisplay<'a>(&'a Bitrates);

impl fmt::Display for BitratesDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (s, temporals) in self.0.iter().enumerate() {
            if s != 0 {
                f.write_str(" ")?;
            }
            f.write_str("[")?;
            for (t, bitrate) in temporals.iter().enumerate() {
                if t != 0 {
                    f.write_str(" ")?;
                }
                write!(f, "{bitrate}")?;
            }
            f.write_str("]")?;
        }
        f.write_str("]")
    }
}

pub const VIDEO_ALLOCATION_DEFAULT: VideoAllocation = VideoAllocation {
    pause_reason: VideoPauseReason::FeedDry, // start with no feed till feed is seen
    is_deficient: false,
    bandwidth_requested: 0,
    bandwidth_delta: 0,
    bandwidth_needed: 0,
    bitrates: [[0; (buffer::DEFAULT_MAX_LAYER_TEMPORAL + 1) as usize];
        (buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize],
    target_layer: buffer::INVALID_LAYER,
    request_layer_spatial: buffer::INVALID_LAYER_SPATIAL,
    max_layer: buffer::INVALID_LAYER,
    distance_to_desired: 0.0,
};

// -------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct VideoAllocationProvisional {
    muted: bool,
    pub_muted: bool,
    max_seen_layer: buffer::VideoLayer,
    available_layers: Vec<i32>,
    bitrates: Bitrates,
    max_layer: buffer::VideoLayer,
    current_layer: buffer::VideoLayer,
    allocated_layer: buffer::VideoLayer,
}

// -------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VideoTransition {
    pub from: buffer::VideoLayer,
    pub to: buffer::VideoLayer,
    pub bandwidth_delta: i64,
}

impl fmt::Display for VideoTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "VideoTransition{{from: {}, to: {}, del: {}}}",
            self.from, self.to, self.bandwidth_delta
        )
    }
}

// -------------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TranslationParams {
    pub(super) should_drop: bool,
    pub(super) is_starting: bool,
    pub(super) is_resuming: bool,
    pub(super) is_switching: bool,
    pub(super) rtp: TranslationParamsRtp,
    pub(super) dd_bytes: [u8; dd::MAX_INLINE_EXTENSION_SIZE],
    pub(super) dd_bytes_len: usize,
    pub(super) dd_bytes_spill: Vec<u8>,
    pub(super) incoming_header_size: usize,
    pub(super) codec_bytes: [u8; codecmunger::MAX_HEADER_SIZE],
    pub(super) codec_bytes_len: usize,
    pub(super) marker: bool,
    // end of the svc spatial layer frame
    pub(super) is_end_of_layer_frame: bool,
    // The packet is dropped, but the RTP munger gave its outgoing sequence number
    // away when it saw the gap, so the downtrack sends padding in that slot. Without
    // it the subscriber NACKs a sequence number nothing can answer, and a VP8
    // receiver without picture IDs stalls on the hole until a key frame.
    pub(super) pad_dropped_slot: bool,
}

impl TranslationParams {
    // codecHeader returns the munged codec header to prepend to the payload.
    #[allow(dead_code)] // the down track calls it, and it is not ported yet
    pub(super) fn codec_header(&self) -> &[u8] {
        &self.codec_bytes[..self.codec_bytes_len]
    }

    // ddInline returns the dependency descriptor extension held inline. It points
    // into tp, so it has to be copied before it is attached to anything that
    // outlives the write.
    #[allow(dead_code)] // the down track calls it, and it is not ported yet
    pub(super) fn dd_inline(&self) -> &[u8] {
        &self.dd_bytes[..self.dd_bytes_len]
    }
}

// -------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
struct RefInfo {
    sender_report: Option<livekit::RtcpSenderReportState>,
    ts_offset: u64,
    is_ts_offset_valid: bool,
}

impl fmt::Debug for RefInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RefInfo")
            .field(
                "sender_report",
                &rtpstats::WrappedRtcpSenderReportStateLogger {
                    rtcp_sender_report_state: self.sender_report,
                },
            )
            .field("ts_offset", &self.ts_offset)
            .field("is_ts_offset_valid", &self.is_ts_offset_valid)
            .finish()
    }
}

// -------------------------------------------------------------------

pub struct Forwarder {
    kind: webrtc::RtpCodecType,
    logger: Logger,
    disable_opportunistic_allocation: bool,
    rtp_stats: Option<Arc<rtpstats::RtpStatsSender>>,

    lock: RwLock<ForwarderInner>,
}

struct ForwarderInner {
    mime: mime::MimeType,
    clock_rate: u32,
    skip_reference_ts: bool,
    enable_start_at_desired_quality: bool,

    muted: bool,
    pub_muted: bool,
    resume_behind_threshold: f64,

    started: bool,
    pre_start_time: Time,
    start_pending: bool,
    acquire_deadline: i64, // mono nanos; initial-acquisition grace deadline, 0 = inactive
    ext_first_ts: u64,
    last_ssrc: u32,
    last_reference_payload_type: i8,
    last_switch_ext_incoming_ts: u64,
    reference_layer_spatial: i32,
    dummy_start_ts_offset: u64,
    ref_infos: [RefInfo; (buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize],
    ref_video_layer_mode: livekit::video_layer::Mode,
    is_dd_available: bool,

    provisional: Option<VideoAllocationProvisional>,

    last_allocation: VideoAllocation,

    rtp_munger: RtpMunger,

    vls: Box<dyn videolayerselector::VideoLayerSelector>,
    // set for VP8 with the dependency descriptor, forwards the descriptor to the subscriber
    dd_forwarder: Option<videolayerselector::DdForwarder>,
    // migrated descriptor state that arrived before the codec was known
    dd_forwarder_seed: Option<livekit::DependencyDescriptorForwarderState>,
    // temporal layer of the packet being translated, the selector's current layer is
    // already the next one on the last packet before a down switch
    packet_temporal: i32,

    codec_munger: Box<dyn codecmunger::CodecMunger>,
}

// What the original's nil pointer dereference says less clearly.
const PROVISIONAL_NOT_PREPARED: &str =
    "provisional allocation used before provisional_allocate_prepare";

impl Forwarder {
    pub fn new(
        kind: webrtc::RtpCodecType,
        logger: Logger,
        skip_reference_ts: bool,
        disable_opportunistic_allocation: bool,
        enable_start_at_desired_quality: bool,
        rtp_stats: Option<Arc<rtpstats::RtpStatsSender>>,
    ) -> Self {
        let mut vls: Box<dyn videolayerselector::VideoLayerSelector> =
            Box::new(videolayerselector::Null::new(logger.clone()));
        if kind == webrtc::RtpCodecType::Video {
            vls.set_max_temporal(buffer::DEFAULT_MAX_LAYER_TEMPORAL);
        }
        vls.set_enable_start_at_desired_quality(enable_start_at_desired_quality);

        Self {
            kind,
            disable_opportunistic_allocation,
            rtp_stats,
            lock: RwLock::new(ForwarderInner {
                mime: mime::MimeType::Unknown,
                clock_rate: 0,
                skip_reference_ts,
                enable_start_at_desired_quality,
                muted: false,
                pub_muted: false,
                resume_behind_threshold: 0.0,
                started: false,
                pre_start_time: Time::ZERO,
                start_pending: false,
                acquire_deadline: 0,
                ext_first_ts: 0,
                last_ssrc: 0,
                last_reference_payload_type: -1,
                last_switch_ext_incoming_ts: 0,
                reference_layer_spatial: buffer::INVALID_LAYER_SPATIAL,
                dummy_start_ts_offset: 0,
                ref_infos: Default::default(),
                ref_video_layer_mode: livekit::video_layer::Mode::Unused,
                is_dd_available: false,
                provisional: None,
                last_allocation: VIDEO_ALLOCATION_DEFAULT,
                rtp_munger: RtpMunger::new(logger.clone()),
                vls,
                dd_forwarder: None,
                dd_forwarder_seed: None,
                packet_temporal: 0,
                codec_munger: Box::new(codecmunger::Null::new(logger.clone())),
            }),
            logger,
        }
    }

    pub fn set_max_published_layer(&self, max_published_layer: i32) -> bool {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        let existing_max_seen = f.vls.get_max_seen();
        if max_published_layer <= existing_max_seen.spatial {
            return false;
        }

        f.vls.set_max_seen_spatial(max_published_layer);
        if f.enable_start_at_desired_quality && !f.vls.get_current().is_valid() {
            // A (higher) layer just became available while nothing is being forwarded yet.
            // (Re)start the initial-acquisition grace so the target aims for the requested layer
            // instead of ramping up gradually as more layers are detected. See opportunisticAlloc.
            f.acquire_deadline =
                mono::unix_nano().wrapping_add(INITIAL_LAYER_ACQUISITION_GRACE.nanoseconds());
        }
        debugw!(self.logger, "setting max published layer", "layer" => max_published_layer);
        true
    }

    // withinAcquireGraceLocked reports whether the initial-acquisition grace window is still open.
    fn within_acquire_grace_locked(&self, f: &ForwarderInner) -> bool {
        f.acquire_deadline != 0 && mono::unix_nano() < f.acquire_deadline
    }

    // MaybeExpireAcquireGrace returns true once when the initial-acquisition grace has expired while
    // the forwarder is still not streaming any layer. The caller should trigger a re-allocation so the
    // target falls back from the requested layer to the highest layer actually seen, avoiding a stall
    // if the requested layer never shows up. The deadline is cleared so it fires at most once.
    pub fn maybe_expire_acquire_grace(&self) -> bool {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        if f.acquire_deadline == 0 || mono::unix_nano() < f.acquire_deadline {
            return false;
        }
        f.acquire_deadline = 0;
        f.enable_start_at_desired_quality = false;
        f.vls.set_enable_start_at_desired_quality(false);
        !f.vls.get_current().is_valid()
    }

    // NeedsPeriodicSyncCheck reports whether the layer lock state can change without an event, so the
    // key frame requester has to keep polling it: an initial-acquisition grace deadline is armed
    // (MaybeExpireAcquireGrace may still fire), or layers are selected with the dependency descriptor,
    // which can lose the lock on the packet path (key frame mismatch, broken frame chain).
    pub fn needs_periodic_sync_check(&self) -> bool {
        let f = self.lock.read();

        if f.acquire_deadline != 0 {
            return true;
        }
        f.vls
            .as_any()
            .is::<videolayerselector::DependencyDescriptor>()
    }

    pub fn set_max_temporal_layer_seen(&self, max_temporal_layer_seen: i32) -> bool {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        let existing_max_seen = f.vls.get_max_seen();
        if max_temporal_layer_seen <= existing_max_seen.temporal {
            return false;
        }

        f.vls.set_max_seen_temporal(max_temporal_layer_seen);
        debugw!(
            self.logger,
            "setting max temporal layer seen",
            "maxTemporalLayerSeen" => max_temporal_layer_seen,
        );
        true
    }

    pub fn determine_codec(
        &self,
        codec: &webrtc::RtpCodecCapability,
        extensions: &[webrtc::RtpHeaderExtensionParameter],
        video_layer_mode: livekit::video_layer::Mode,
    ) {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        if video_layer_mode == livekit::video_layer::Mode::OneSpatialLayerPerStreamIncompleteRtcpSr
        {
            f.skip_reference_ts = true;
        }

        let to_mime_type = mime::normalize_mime_type(&codec.mime_type);
        let codec_changed = f.mime != mime::MimeType::Unknown && f.mime != to_mime_type;
        if codec_changed {
            debugw!(
                self.logger,
                "forwarder codec changed",
                "from" => f.mime,
                "to" => to_mime_type,
            );
        }
        f.mime = to_mime_type;
        f.clock_rate = codec.clock_rate;
        f.ref_video_layer_mode = video_layer_mode;

        let dd_available = |exts: &[webrtc::RtpHeaderExtensionParameter]| -> bool {
            exts.iter().any(|ext| ext.uri == dd::EXTENSION_URI)
        };

        let prev_dd_forwarder = f.dd_forwarder.take();
        match f.mime {
            mime::MimeType::Vp8 => {
                self.set_simulcast_selector(f);
                if dd_available(extensions) {
                    self.set_dd_temporal_filter(f, prev_dd_forwarder.as_ref());
                } else {
                    f.is_dd_available = false;
                    f.codec_munger = Box::new(codecmunger::Vp8::new_from_other(
                        f.codec_munger.as_ref(),
                        self.logger.clone(),
                    ));
                    f.vls.set_temporal_layer_selector(Some(Box::new(
                        temporallayerselector::Vp8::new(self.logger.clone()),
                    )));
                }
            }

            mime::MimeType::H264 | mime::MimeType::H265 => {
                self.set_simulcast_selector(f);
                self.set_unfiltered_temporal(f);
            }

            mime::MimeType::Vp9 => {
                if sfuutils::is_simulcast_mode(video_layer_mode) {
                    self.set_simulcast_selector(f);
                    if dd_available(extensions) {
                        self.set_dd_temporal_filter(f, prev_dd_forwarder.as_ref());
                    } else {
                        self.set_unfiltered_temporal(f);
                    }
                } else {
                    f.codec_munger = Box::new(codecmunger::Null::new(self.logger.clone()));
                    f.is_dd_available = dd_available(extensions);
                    if f.is_dd_available {
                        f.vls = Box::new(videolayerselector::DependencyDescriptor::new_from_other(
                            self.take_vls(f),
                        ));
                    } else {
                        f.vls = Box::new(videolayerselector::Vp9::new_from_other(self.take_vls(f)));
                    }
                    // the svc selectors pick temporal layers themselves
                    f.vls.set_temporal_layer_selector(None);
                }
            }

            mime::MimeType::Av1 => {
                if sfuutils::is_simulcast_mode(video_layer_mode) {
                    self.set_simulcast_selector(f);
                    if dd_available(extensions) {
                        self.set_dd_temporal_filter(f, prev_dd_forwarder.as_ref());
                    } else {
                        self.set_unfiltered_temporal(f);
                    }
                } else {
                    f.codec_munger = Box::new(codecmunger::Null::new(self.logger.clone()));
                    f.is_dd_available = dd_available(extensions);
                    if f.is_dd_available {
                        f.vls = Box::new(videolayerselector::DependencyDescriptor::new_from_other(
                            self.take_vls(f),
                        ));
                        f.vls.set_temporal_layer_selector(None);
                    } else {
                        // without the dependency descriptor nothing tells the temporal layers apart
                        self.set_simulcast_selector(f);
                        self.set_unfiltered_temporal(f);
                    }
                }
            }

            _ => {}
        }
    }

    // Not in the original: takes the selector out of the forwarder for a `new_from_other`,
    // which keeps its `Base`. A `Null` stands in its place until the caller assigns the new
    // selector, in the same statement.
    fn take_vls(&self, f: &mut ForwarderInner) -> Box<dyn videolayerselector::VideoLayerSelector> {
        std::mem::replace(
            &mut f.vls,
            Box::new(videolayerselector::Null::new(self.logger.clone())),
        )
    }

    // setSimulcastSelector switches to the simulcast selector, keeping the selector state
    // across a codec change
    fn set_simulcast_selector(&self, f: &mut ForwarderInner) {
        match videolayerselector::Simulcast::new_from_other(self.take_vls(f)) {
            Ok(vls) => f.vls = Box::new(vls),
            Err(vls) => {
                f.vls = vls;
                errorw!(
                    self.logger,
                    "failed to create simulcast on codec change",
                    nil
                );
            }
        }
    }

    // setDDTemporalFilter filters the temporal layers of a simulcast stream by frame with the
    // dependency descriptor, and forwards the descriptor to the subscriber
    fn set_dd_temporal_filter(
        &self,
        f: &mut ForwarderInner,
        prev_dd_forwarder: Option<&videolayerselector::DdForwarder>,
    ) {
        f.is_dd_available = true;
        f.codec_munger = Box::new(codecmunger::DdTemporal::new(f.mime, self.logger.clone()));
        f.vls.set_temporal_layer_selector(Some(Box::new(
            temporallayerselector::DependencyDescriptor::new(self.logger.clone()),
        )));
        // the downtrack keeps its SSRC, so the frame numbers continue
        let mut dd_forwarder =
            videolayerselector::DdForwarder::new_from_other(prev_dd_forwarder, self.logger.clone());
        dd_forwarder.seed_state(f.dd_forwarder_seed.as_ref());
        f.dd_forwarder = Some(dd_forwarder);
        f.dd_forwarder_seed = None;
    }

    // setUnfilteredTemporal forwards every temporal layer of a simulcast stream that has
    // nothing to filter them with
    fn set_unfiltered_temporal(&self, f: &mut ForwarderInner) {
        f.is_dd_available = false;
        f.codec_munger = Box::new(codecmunger::Null::new(self.logger.clone()));
        f.vls
            .set_temporal_layer_selector(Some(Box::new(temporallayerselector::Null::new())));
    }

    pub fn get_state(&self) -> Option<livekit::RtpForwarderState> {
        let f = self.lock.read();

        if !f.started {
            return None;
        }

        let mut state = livekit::RtpForwarderState {
            started: f.started,
            reference_layer_spatial: f.reference_layer_spatial,
            ext_first_timestamp: f.ext_first_ts,
            dummy_start_timestamp_offset: f.dummy_start_ts_offset,
            rtp_munger: Some(f.rtp_munger.get_state()),
            ..Default::default()
        };
        if !f.pre_start_time.is_zero() {
            state.pre_start_time = f.pre_start_time.unix_nano();
        }

        let codec_munger_state = f.codec_munger.get_state();
        if let Some(codecmunger::State::Vp8MungerState(vp8_munger_state)) = codec_munger_state {
            state.codec_munger = Some(livekit::rtp_forwarder_state::CodecMunger::Vp8Munger(
                vp8_munger_state,
            ));
        }

        state.sender_report_state = f
            .ref_infos
            .iter()
            .map(|ref_info| ref_info.sender_report.unwrap_or_default())
            .collect();
        if let Some(dd_forwarder) = &f.dd_forwarder {
            state.dependency_descriptor_forwarder = dd_forwarder.get_state();
        } else {
            // seeded, but migrating on before the codec was known
            state.dependency_descriptor_forwarder = f.dd_forwarder_seed;
        }
        Some(state)
    }

    pub fn seed_state(&self, state: Option<&livekit::RtpForwarderState>) {
        let Some(state) = state else {
            return;
        };
        if !state.started {
            return;
        }

        let mut guard = self.lock.write();
        let f = &mut *guard;

        for (layer, rtcp_sender_report_state) in state.sender_report_state.iter().enumerate() {
            f.ref_infos[layer] = RefInfo::default();
            if rtcp_sender_report_state.ntp_timestamp != 0 {
                f.ref_infos[layer].sender_report = Some(*rtcp_sender_report_state);
            }
        }

        if let Some(rtp_munger) = &state.rtp_munger {
            f.rtp_munger.seed_state(rtp_munger);
        }
        f.codec_munger
            .seed_state(state.codec_munger.map(codecmunger::State::CodecMunger));
        if let Some(dd_forwarder) = &mut f.dd_forwarder {
            dd_forwarder.seed_state(state.dependency_descriptor_forwarder.as_ref());
        } else {
            f.dd_forwarder_seed = state.dependency_descriptor_forwarder;
        }

        f.started = true;
        f.reference_layer_spatial = state.reference_layer_spatial;
        if state.pre_start_time != 0 {
            f.pre_start_time = Time::unix(0, state.pre_start_time);
            f.start_pending = true;
        }
        f.ext_first_ts = state.ext_first_timestamp;
        f.dummy_start_ts_offset = state.dummy_start_timestamp_offset;
    }

    pub fn mute(&self, muted: bool, is_subscribe_mutable: bool) -> bool {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        if f.muted == muted {
            return false;
        }

        // Do not mute when paused due to bandwidth limitation.
        // There are two issues
        //   1. Muting means probing cannot happen on this track.
        //   2. Muting also triggers notification to publisher about layers this forwarder needs.
        //      If this forwarder does not need any layer, publisher could turn off all layers.
        // So, muting could lead to not being able to restart the track.
        // To avoid that, ignore mute when paused due to bandwidth limitations.
        //
        // NOTE: The above scenario refers to mute getting triggered due
        // to video stream visibility changes. When a stream is paused, it is possible
        // that the receiver hides the video tile triggering subscription mute.
        // The work around here to ignore mute does ignore an intentional mute.
        // It could result in some bandwidth consumed for stream without visibility in
        // the case of intentional mute.
        if muted && !is_subscribe_mutable {
            infow!(
                self.logger,
                "ignoring forwarder mute, paused due to congestion",
                "targetLayers" => f.vls.get_target(),
                "currentLayers" => f.vls.get_current(),
                "lastAllocation" => f.last_allocation,
            );
            return false;
        }

        debugw!(self.logger, "setting forwarder mute", "muted" => muted);
        f.muted = muted;

        // resync when muted so that sequence numbers do not jump on unmute
        if muted {
            self.resync_locked(f);
        }

        true
    }

    pub fn is_muted(&self) -> bool {
        let f = self.lock.read();

        f.muted
    }

    pub fn pub_mute(&self, pub_muted: bool) -> bool {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        if f.pub_muted == pub_muted {
            return false;
        }

        debugw!(self.logger, "setting forwarder pub mute", "muted" => pub_muted);
        f.pub_muted = pub_muted;

        // resync when pub muted so that sequence numbers do not jump on unmute
        if pub_muted {
            self.resync_locked(f);
        }
        true
    }

    pub fn is_pub_muted(&self) -> bool {
        let f = self.lock.read();

        f.pub_muted
    }

    pub fn is_any_muted(&self) -> bool {
        let f = self.lock.read();

        f.muted || f.pub_muted
    }

    pub fn set_max_spatial_layer(&self, spatial_layer: i32) -> (bool, buffer::VideoLayer) {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        if self.kind == webrtc::RtpCodecType::Audio {
            return (false, buffer::INVALID_LAYER);
        }

        let existing_max = f.vls.get_max();
        if spatial_layer == existing_max.spatial {
            return (false, existing_max);
        }

        debugw!(self.logger, "setting max spatial layer", "layer" => spatial_layer);
        f.vls.set_max_spatial(spatial_layer);
        if self.disable_opportunistic_allocation {
            return (true, f.vls.get_max());
        }

        if f.vls.get_target().spatial != buffer::INVALID_LAYER_SPATIAL
            || self.is_deficient_locked(f)
            || f.last_allocation.pause_reason == VideoPauseReason::Muted
            || f.last_allocation.pause_reason == VideoPauseReason::PubMuted
        {
            return (true, f.vls.get_max());
        }

        debugw!(
            self.logger,
            "opportunistically setting target spatial layer",
            "layer" => spatial_layer,
        );

        let mut alloc = f.last_allocation;

        // bitrates are not known
        alloc.bandwidth_requested = 0;
        alloc.bandwidth_delta = 0;
        alloc.bitrates = Bitrates::default();

        alloc.target_layer = f.vls.get_max();
        alloc.request_layer_spatial = f.vls.get_max().spatial;
        alloc.max_layer = f.vls.get_max();

        alloc.distance_to_desired = get_distance_to_desired(
            f.muted,
            f.pub_muted,
            f.vls.get_max_seen(),
            &[],
            alloc.bitrates,
            alloc.target_layer,
            f.vls.get_max(),
        );

        self.update_allocation(f, alloc, "opportunistic");
        (true, f.vls.get_max())
    }

    pub fn set_max_temporal_layer(&self, temporal_layer: i32) -> (bool, buffer::VideoLayer) {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        if self.kind == webrtc::RtpCodecType::Audio {
            return (false, buffer::INVALID_LAYER);
        }

        let existing_max = f.vls.get_max();
        if temporal_layer == existing_max.temporal {
            return (false, existing_max);
        }

        debugw!(self.logger, "setting max temporal layer", "layer" => temporal_layer);
        f.vls.set_max_temporal(temporal_layer);
        (true, f.vls.get_max())
    }

    pub fn max_layer(&self) -> buffer::VideoLayer {
        let f = self.lock.read();

        f.vls.get_max()
    }

    pub fn current_layer(&self) -> buffer::VideoLayer {
        let f = self.lock.read();

        f.vls.get_current()
    }

    pub fn target_layer(&self) -> buffer::VideoLayer {
        let f = self.lock.read();

        f.vls.get_target()
    }

    pub fn get_max_subscribed_spatial(&self) -> i32 {
        let f = self.lock.read();

        let mut layer = buffer::INVALID_LAYER_SPATIAL; // covers muted case
        if !f.muted {
            // If current is higher, mark the current layer as max subscribed layer
            // to prevent the current layer from stopping before forwarder switches
            // to the new and lower max layer,
            layer = f.vls.get_max().spatial.max(f.vls.get_current().spatial);

            // if reference layer is higher, hold there until an RTCP Sender Report from
            // publisher is available as that is used for reference time stamp between layers.
            if f.reference_layer_spatial != buffer::INVALID_LAYER_SPATIAL
                && layer < f.reference_layer_spatial
                && f.ref_infos[f.reference_layer_spatial as usize]
                    .sender_report
                    .is_none()
            {
                layer = f.reference_layer_spatial;
            }
        }

        layer
    }

    fn get_ref_layer(&self, f: &ForwarderInner) -> (i32, i32) {
        if f.last_ssrc == 0 {
            return (buffer::INVALID_LAYER_SPATIAL, buffer::INVALID_LAYER_SPATIAL);
        }

        if self.kind == webrtc::RtpCodecType::Audio {
            return (0, 0);
        }

        let current_layer_spatial = f.vls.get_current().spatial;
        if !(0..=buffer::DEFAULT_MAX_LAYER_SPATIAL).contains(&current_layer_spatial) {
            return (buffer::INVALID_LAYER_SPATIAL, buffer::INVALID_LAYER_SPATIAL);
        }

        if f.ref_video_layer_mode == livekit::video_layer::Mode::MultipleSpatialLayersPerStream {
            return (0, current_layer_spatial);
        }

        (current_layer_spatial, current_layer_spatial)
    }

    pub fn set_ref_sender_report(&self, layer: i32, sr_data: &livekit::RtcpSenderReportState) {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        if layer >= 0 && (layer as usize) < f.ref_infos.len() {
            if layer == f.reference_layer_spatial
                && f.ref_infos[layer as usize].sender_report.is_none()
            {
                debugw!(
                    self.logger,
                    "received RTCP sender report for reference layer spatial",
                    "layer" => layer,
                    "srData" => rtpstats::WrappedRtcpSenderReportStateLogger {
                        rtcp_sender_report_state: Some(*sr_data),
                    },
                );
            }
            f.ref_infos[layer as usize] = RefInfo {
                sender_report: Some(*sr_data),
                ts_offset: 0,
                is_ts_offset_valid: false,
            };

            // Mark validity of time stamp offset.
            //
            // It is possible to implement mute using pause/unpause
            // which can be implemented using replaceTrack(null)/replaceTrack(track).
            // In those cases, the RTP time stamp may not jump across
            // the mute/pause valley (for the time it is replaced with null track).
            // So, relying on a report that happened before unmute/unpause
            // could result in incorrect RTCP sender report on subscriber side.
            //
            // It could happen like this
            //   1. Normal operation: publisher sending sender reports and
            //      subscribers use reports from publisher to calculate and send
            //      RTCP sender report.
            //   2. Publisher pauses: there are no more reports.
            //   3. When paused, subscriber can still use the publisher side sender
            //      report to send reports. Although the time since last publisher
            //      sender report is increasing, the reports would still be correct
            //      as they referencing a previous (albeit older) correct report.
            //   4. Publisher unpauses after 20 seconds. But, it may not have advanced
            //      RTP Timestamp by that much. Let us say, it advances only by 5 seconds.
            //   5. When subscriber starts forwarding packets, it will calculate
            //      a new time stamp offset to adjust to the new time stamp of publisher.
            //   6. But, when that same offset is used on an old publisher sender report
            //      (i. e. a report from before the pause), the subscriber side sender
            //      reports jumps ahead in time by 15 seconds.
            //
            // So, mark valid for reports after last switch.
            let (ref_layer, _) = self.get_ref_layer(f);
            if layer == ref_layer && sr_data.rtp_timestamp_ext >= f.last_switch_ext_incoming_ts {
                f.ref_infos[layer as usize].ts_offset = f.rtp_munger.get_ts_offset();
                f.ref_infos[layer as usize].is_ts_offset_valid = true;
            }
        }
    }

    pub fn get_sender_report_params(
        &self,
    ) -> (i32, bool, u64, Option<livekit::RtcpSenderReportState>) {
        let f = self.lock.read();

        let (ref_layer, current_layer_spatial) = self.get_ref_layer(&f);
        if ref_layer == buffer::INVALID_LAYER_SPATIAL
            || f.ref_infos[ref_layer as usize].sender_report.is_none()
            || !f.ref_infos[ref_layer as usize].is_ts_offset_valid
        {
            return (buffer::INVALID_LAYER_SPATIAL, false, 0, None);
        }

        (
            current_layer_spatial,
            f.ref_video_layer_mode == livekit::video_layer::Mode::MultipleSpatialLayersPerStream,
            f.ref_infos[ref_layer as usize].ts_offset,
            f.ref_infos[ref_layer as usize].sender_report,
        )
    }

    fn is_deficient_locked(&self, f: &ForwarderInner) -> bool {
        f.last_allocation.is_deficient
    }

    pub fn is_deficient(&self) -> bool {
        let f = self.lock.read();

        self.is_deficient_locked(&f)
    }

    pub fn pause_reason(&self) -> VideoPauseReason {
        let f = self.lock.read();

        f.last_allocation.pause_reason
    }

    pub fn bandwidth_requested(&self, brs: Bitrates) -> i64 {
        let f = self.lock.read();

        get_bandwidth_needed(
            brs,
            f.vls.get_target(),
            f.last_allocation.bandwidth_requested,
        )
    }

    pub fn distance_to_desired(&self, available_layers: &[i32], brs: Bitrates) -> f64 {
        let f = self.lock.read();

        get_distance_to_desired(
            f.muted,
            f.pub_muted,
            f.vls.get_max_seen(),
            available_layers,
            brs,
            f.vls.get_target(),
            f.vls.get_max(),
        )
    }

    pub fn get_optimal_bandwidth_needed(&self, brs: Bitrates) -> i64 {
        let f = self.lock.read();

        get_optimal_bandwidth_needed(
            f.muted,
            f.pub_muted,
            f.vls.get_max_seen().spatial,
            brs,
            f.vls.get_max(),
        )
    }

    pub fn allocate_optimal(
        &self,
        available_layers: &[i32],
        brs: Bitrates,
        allow_overshoot: bool,
        hold: bool,
    ) -> VideoAllocation {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        if self.kind == webrtc::RtpCodecType::Audio {
            return f.last_allocation;
        }

        let max_layer = f.vls.get_max();
        let max_seen_layer = f.vls.get_max_seen();
        let current_layer = f.vls.get_current();
        let request_spatial = f.vls.get_request_spatial();
        let mut alloc = VideoAllocation {
            pause_reason: VideoPauseReason::None,
            bitrates: brs,
            target_layer: buffer::INVALID_LAYER,
            request_layer_spatial: request_spatial,
            max_layer,
            ..Default::default()
        };
        let optimal_bandwidth_needed = get_optimal_bandwidth_needed(
            f.muted,
            f.pub_muted,
            max_seen_layer.spatial,
            brs,
            max_layer,
        );
        if optimal_bandwidth_needed == 0 {
            alloc.pause_reason = VideoPauseReason::FeedDry;
        }
        alloc.bandwidth_needed = optimal_bandwidth_needed;

        let get_max_temporal = || -> i32 {
            let mut max_temporal = max_layer.temporal;
            if max_seen_layer.temporal != buffer::INVALID_LAYER_TEMPORAL
                && max_seen_layer.temporal < max_temporal
            {
                max_temporal = max_seen_layer.temporal;
            }
            max_temporal
        };

        // inAcquireGrace reports that we are acquiring the first layer, the requested layer has not
        // been seen on the wire yet, and the grace window is still open. While true, aim straight for
        // the requested layer instead of the highest seen so far, so acquisition does not ramp up
        // gradually as layers are detected (`maxSeen` climbs). See initialLayerAcquisitionGrace.
        let in_acquire_grace = |f: &ForwarderInner, max_spatial: i32| -> bool {
            !current_layer.is_valid()
                && max_seen_layer.spatial < max_spatial
                && self.within_acquire_grace_locked(f)
        };

        // set when opportunisticAlloc aimed the target at the requested layer due to the acquisition
        // grace (as opposed to overshoot), so the key frame request can be pointed at it too
        let mut acquire_grace_applied = false;
        let mut opportunistic_alloc = |f: &ForwarderInner, alloc: &mut VideoAllocation| {
            // opportunistically latch on to anything
            let mut max_spatial = max_layer.spatial;
            if allow_overshoot && f.vls.is_overshoot_okay() && max_seen_layer.spatial > max_spatial
            {
                max_spatial = max_seen_layer.spatial;
            }

            let mut target_spatial = max_seen_layer.spatial.min(max_spatial);
            if in_acquire_grace(f, max_spatial) {
                target_spatial = max_spatial;
                acquire_grace_applied = true;
            }

            alloc.target_layer = buffer::VideoLayer {
                spatial: target_spatial,
                temporal: get_max_temporal(),
            };
        };

        if !max_layer.is_valid() || max_seen_layer.spatial == buffer::INVALID_LAYER_SPATIAL {
            // nothing to do when max layers are not valid OR max published layer is invalid
        } else if f.muted {
            alloc.pause_reason = VideoPauseReason::Muted;
        } else if f.pub_muted {
            alloc.pause_reason = VideoPauseReason::PubMuted;
        } else {
            // lots of different events could end up here
            //   1. Publisher side layer resuming/stopping
            //   2. Bitrate becoming available
            //   3. New max published spatial layer or max temporal layer seen
            //   4. Subscriber layer changes
            //
            // to handle all of the above
            //   1. Find highest that can be requested - takes into account available layers and overshoot.
            //      This should catch scenarios like layers resuming/stopping.
            //   2. If current is a valid layer, check against currently available layers and continue at current
            //      if possible. Else, choose the highest available layer as the next target.
            //   3. If current is not valid, set next target to be opportunistic.
            let max_layer_spatial_limit = max_layer.spatial.min(max_seen_layer.spatial);
            let mut highest_available_layer = buffer::INVALID_LAYER_SPATIAL;
            let mut lowest_available_layer = buffer::INVALID_LAYER_SPATIAL;
            let mut request_layer_spatial = buffer::INVALID_LAYER_SPATIAL;
            for &al in available_layers {
                if al > request_layer_spatial && al <= max_layer_spatial_limit {
                    request_layer_spatial = al;
                }
                if al > highest_available_layer {
                    highest_available_layer = al;
                }
                if lowest_available_layer == buffer::INVALID_LAYER_SPATIAL
                    || al < lowest_available_layer
                {
                    lowest_available_layer = al;
                }
            }
            if request_layer_spatial == buffer::INVALID_LAYER_SPATIAL
                && highest_available_layer != buffer::INVALID_LAYER_SPATIAL
                && allow_overshoot
                && f.vls.is_overshoot_okay()
            {
                request_layer_spatial = highest_available_layer;
            }

            if current_layer.is_valid() {
                if (request_layer_spatial == request_spatial
                    && current_layer.spatial == request_spatial)
                    || request_layer_spatial == buffer::INVALID_LAYER_SPATIAL
                {
                    // 1. current is locked to desired, stay there
                    // OR
                    // 2. feed may be dry, let it continue at current layer if valid.
                    // covers the cases of
                    //   1. mis-detection of layer stop - can continue streaming
                    //   2. current layer resuming - can latch on when it starts
                    alloc.target_layer = buffer::VideoLayer {
                        spatial: current_layer.spatial,
                        temporal: get_max_temporal(),
                    };
                } else {
                    // current layer has stopped, switch to lowest available if `hold`ing, else switch to highest available
                    if hold {
                        // if `hold` is requested, may be set due to early warning congestion
                        // signal, in that case layers are not increased as increasing layers
                        // will result in more load on the channel
                        alloc.target_layer = buffer::VideoLayer {
                            spatial: lowest_available_layer,
                            temporal: 0,
                        };
                    } else {
                        alloc.target_layer = buffer::VideoLayer {
                            spatial: request_layer_spatial,
                            temporal: get_max_temporal(),
                        };
                    }
                }
                alloc.request_layer_spatial = alloc.target_layer.spatial;
            } else if hold {
                // allocate minimal to make the stream active while `hold`ing.
                if lowest_available_layer == buffer::INVALID_LAYER_SPATIAL {
                    alloc.target_layer = buffer::VideoLayer {
                        spatial: 0,
                        temporal: 0,
                    };
                } else {
                    alloc.target_layer = buffer::VideoLayer {
                        spatial: lowest_available_layer,
                        temporal: 0,
                    };
                }
                alloc.request_layer_spatial = alloc.target_layer.spatial;
            } else {
                // opportunistically latch on to anything
                opportunistic_alloc(f, &mut alloc);
                if acquire_grace_applied {
                    // in the acquisition grace, request a key frame for the requested layer we
                    // are waiting for (above what has been seen so far)
                    alloc.request_layer_spatial = alloc.target_layer.spatial;
                } else if request_layer_spatial == buffer::INVALID_LAYER_SPATIAL {
                    alloc.request_layer_spatial = max_layer_spatial_limit;
                } else {
                    alloc.request_layer_spatial = request_layer_spatial;
                }
            }
        }

        if !alloc.target_layer.is_valid() {
            alloc.target_layer = buffer::INVALID_LAYER;
            alloc.request_layer_spatial = buffer::INVALID_LAYER_SPATIAL;
        }
        if alloc.target_layer.is_valid() {
            alloc.bandwidth_requested = get_optimal_bandwidth_needed(
                f.muted,
                f.pub_muted,
                max_seen_layer.spatial,
                brs,
                alloc.target_layer,
            );
        }
        alloc.bandwidth_delta = alloc.bandwidth_requested
            - get_bandwidth_needed(
                brs,
                f.vls.get_target(),
                f.last_allocation.bandwidth_requested,
            );
        alloc.distance_to_desired = get_distance_to_desired(
            f.muted,
            f.pub_muted,
            f.vls.get_max_seen(),
            available_layers,
            brs,
            alloc.target_layer,
            f.vls.get_max(),
        );

        self.update_allocation(f, alloc, "optimal")
    }

    pub fn provisional_allocate_prepare(&self, available_layers: &[i32], bitrates: Bitrates) {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        let p = f
            .provisional
            .get_or_insert_with(VideoAllocationProvisional::default);
        p.allocated_layer = buffer::INVALID_LAYER;
        p.muted = f.muted;
        p.pub_muted = f.pub_muted;
        p.max_seen_layer = f.vls.get_max_seen();
        p.bitrates = bitrates;
        p.max_layer = f.vls.get_max();
        p.current_layer = f.vls.get_current();
        p.available_layers.clear();
        p.available_layers.extend_from_slice(available_layers);
    }

    pub fn provisional_allocate_reset(&self) {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        f.provisional
            .as_mut()
            .expect(PROVISIONAL_NOT_PREPARED)
            .allocated_layer = buffer::INVALID_LAYER;
    }

    pub fn provisional_allocate(
        &self,
        available_channel_capacity: i64,
        layer: buffer::VideoLayer,
        allow_pause: bool,
        allow_overshoot: bool,
    ) -> (bool, i64) {
        let mut guard = self.lock.write();
        let f = &mut *guard;
        let provisional = f.provisional.as_mut().expect(PROVISIONAL_NOT_PREPARED);

        if provisional.muted
            || provisional.pub_muted
            || provisional.max_seen_layer.spatial == buffer::INVALID_LAYER_SPATIAL
            || !provisional.max_layer.is_valid()
            || ((!allow_overshoot || !f.vls.is_overshoot_okay())
                && layer.greater_than(provisional.max_layer))
        {
            return (false, 0);
        }

        let required_bitrate =
            provisional.bitrates[layer.spatial as usize][layer.temporal as usize];
        if required_bitrate == 0 {
            return (false, 0);
        }

        let mut already_allocated_bitrate = 0i64;
        if provisional.allocated_layer.is_valid() {
            already_allocated_bitrate = provisional.bitrates
                [provisional.allocated_layer.spatial as usize]
                [provisional.allocated_layer.temporal as usize];
        }

        // a layer under maximum fits, take it
        if !layer.greater_than(provisional.max_layer)
            && required_bitrate <= (available_channel_capacity + already_allocated_bitrate)
        {
            provisional.allocated_layer = layer;
            return (true, required_bitrate - already_allocated_bitrate);
        }

        //
        // Given layer does not fit.
        //
        // Could be one of
        //  1. a layer below maximum that does not fit
        //  2. a layer above maximum which may or may not fit, but overshoot is allowed.
        // In any of those cases, take the lowest possible layer if pause is not allowed
        //
        if !allow_pause
            && (!provisional.allocated_layer.is_valid()
                || !layer.greater_than(provisional.allocated_layer))
        {
            provisional.allocated_layer = layer;
            return (true, required_bitrate - already_allocated_bitrate);
        }

        (false, 0)
    }

    pub fn provisional_allocate_get_cooperative_transition(
        &self,
        allow_overshoot: bool,
    ) -> (VideoTransition, Vec<i32>, Bitrates) {
        //
        // This is called when a track needs a change (could be mute/unmute, subscribed layers changed, published layers changed)
        // when channel is congested.
        //
        // The goal is to provide a co-operative transition. Co-operative stream allocation aims to keep all the streams active
        // as much as possible.
        //
        // When channel is congested, effecting a transition which will consume more bits will lead to more congestion.
        // So, this routine does the following
        //   1. When muting, it is not going to increase consumption.
        //   2. If the stream is currently active and the transition needs more bits (higher layers = more bits), do not make the up move.
        //      The higher layer requirement could be due to a new published layer becoming available or subscribed layers changing.
        //   3. If the new target layers are lower than current target, take the move down and save bits.
        //   4. If not currently streaming, find the minimum layers that can unpause the stream.
        //
        // To summarize, co-operative streaming means
        //   - Try to keep tracks streaming, i.e. no pauses at the expense of some streams not being at optimal layers
        //   - Do not make an upgrade as it could affect other tracks
        //
        let mut guard = self.lock.write();
        let f = &mut *guard;
        let provisional = f.provisional.as_mut().expect(PROVISIONAL_NOT_PREPARED);

        let existing_target_layer = f.vls.get_target();
        if provisional.muted || provisional.pub_muted {
            provisional.allocated_layer = buffer::INVALID_LAYER;
            return (
                VideoTransition {
                    from: existing_target_layer,
                    to: provisional.allocated_layer,
                    bandwidth_delta: -get_bandwidth_needed(
                        provisional.bitrates,
                        existing_target_layer,
                        f.last_allocation.bandwidth_requested,
                    ),
                },
                provisional.available_layers.clone(),
                provisional.bitrates,
            );
        }

        // check if we should preserve current target
        if existing_target_layer.is_valid() {
            // what is the highest that is available
            let mut maximal_layer = buffer::INVALID_LAYER;
            let mut maximal_bandwidth_required = 0i64;
            for s in (0..=provisional.max_layer.spatial).rev() {
                for t in (0..=provisional.max_layer.temporal).rev() {
                    if provisional.bitrates[s as usize][t as usize] != 0 {
                        maximal_layer = buffer::VideoLayer {
                            spatial: s,
                            temporal: t,
                        };
                        maximal_bandwidth_required = provisional.bitrates[s as usize][t as usize];
                        break;
                    }
                }

                if maximal_bandwidth_required != 0 {
                    break;
                }
            }

            if maximal_layer.is_valid() {
                if !existing_target_layer.greater_than(maximal_layer)
                    && provisional.bitrates[existing_target_layer.spatial as usize]
                        [existing_target_layer.temporal as usize]
                        != 0
                {
                    // currently streaming and maybe wanting an upgrade (existingTargetLayer <= maximalLayer),
                    // just preserve current target in the cooperative scheme of things
                    provisional.allocated_layer = existing_target_layer;
                    return (
                        VideoTransition {
                            from: existing_target_layer,
                            to: existing_target_layer,
                            bandwidth_delta: 0,
                        },
                        provisional.available_layers.clone(),
                        provisional.bitrates,
                    );
                }

                if existing_target_layer.greater_than(maximal_layer) {
                    // maximalLayer < existingTargetLayer, make the down move
                    provisional.allocated_layer = maximal_layer;
                    return (
                        VideoTransition {
                            from: existing_target_layer,
                            to: maximal_layer,
                            bandwidth_delta: maximal_bandwidth_required
                                - get_bandwidth_needed(
                                    provisional.bitrates,
                                    existing_target_layer,
                                    f.last_allocation.bandwidth_requested,
                                ),
                        },
                        provisional.available_layers.clone(),
                        provisional.bitrates,
                    );
                }
            }
        }

        let find_next_layer = |min_spatial: i32,
                               max_spatial: i32,
                               min_temporal: i32,
                               max_temporal: i32|
         -> (buffer::VideoLayer, i64) {
            let mut layers = buffer::INVALID_LAYER;
            let mut bw = 0i64;
            for s in min_spatial..=max_spatial {
                for t in min_temporal..=max_temporal {
                    if provisional.bitrates[s as usize][t as usize] != 0 {
                        layers = buffer::VideoLayer {
                            spatial: s,
                            temporal: t,
                        };
                        bw = provisional.bitrates[s as usize][t as usize];
                        break;
                    }
                }

                if bw != 0 {
                    break;
                }
            }

            (layers, bw)
        };

        let mut target_layer = buffer::INVALID_LAYER;
        let mut bandwidth_required = 0i64;
        if !existing_target_layer.is_valid() {
            // currently not streaming, find minimal
            // NOTE: a layer in feed could have paused and there could be other options than going back to minimal,
            // but the cooperative scheme knocks things back to minimal
            (target_layer, bandwidth_required) = find_next_layer(
                0,
                provisional.max_layer.spatial,
                0,
                provisional.max_layer.temporal,
            );

            // could not find a minimal layer, overshoot if allowed
            if bandwidth_required == 0
                && provisional.max_layer.is_valid()
                && allow_overshoot
                && f.vls.is_overshoot_okay()
            {
                (target_layer, bandwidth_required) = find_next_layer(
                    provisional.max_layer.spatial + 1,
                    buffer::DEFAULT_MAX_LAYER_SPATIAL,
                    0,
                    buffer::DEFAULT_MAX_LAYER_TEMPORAL,
                );
            }
        }

        // if nothing available, just leave target at current to enable opportunistic forwarding in case current resumes
        if !target_layer.is_valid() {
            target_layer = provisional.current_layer;
            if target_layer.is_valid() {
                bandwidth_required = provisional.bitrates[target_layer.spatial as usize]
                    [target_layer.temporal as usize];
            }
        }

        provisional.allocated_layer = target_layer;
        (
            VideoTransition {
                from: f.vls.get_target(),
                to: target_layer,
                bandwidth_delta: bandwidth_required
                    - get_bandwidth_needed(
                        provisional.bitrates,
                        existing_target_layer,
                        f.last_allocation.bandwidth_requested,
                    ),
            },
            provisional.available_layers.clone(),
            provisional.bitrates,
        )
    }

    pub fn provisional_allocate_get_best_weighted_transition(
        &self,
    ) -> (VideoTransition, Vec<i32>, Bitrates) {
        //
        // This is called when a track needs a change (could be mute/unmute, subscribed layers changed, published layers changed)
        // when channel is congested. This is called on tracks other than the one needing the change. When the track
        // needing the change requires bits, this is called to check if this track can contribute some bits to the pool.
        //
        // The goal is to keep all tracks streaming as much as possible. So, the track that needs a change needs bandwidth to be unpaused.
        //
        // This tries to figure out how much this track can contribute back to the pool to enable the track that needs to be unpaused.
        //   1. Track muted OR feed dry - can contribute everything back in case it was using bandwidth.
        //   2. Look at all possible down transitions from current target and find the best offer.
        //      Best offer is calculated as bandwidth saved moving to a down layer divided by cost.
        //      Cost has two components
        //        a. Transition cost: Spatial layer switch is expensive due to key frame requirement, but temporal layer switch is free.
        //        b. Quality cost: The farther away from desired layers, the higher the quality cost.
        //
        let mut guard = self.lock.write();
        let f = &mut *guard;
        let provisional = f.provisional.as_mut().expect(PROVISIONAL_NOT_PREPARED);

        let target_layer = f.vls.get_target();
        if provisional.muted || provisional.pub_muted {
            provisional.allocated_layer = buffer::INVALID_LAYER;
            return (
                VideoTransition {
                    from: target_layer,
                    to: provisional.allocated_layer,
                    bandwidth_delta: 0 - get_bandwidth_needed(
                        provisional.bitrates,
                        target_layer,
                        f.last_allocation.bandwidth_requested,
                    ),
                },
                provisional.available_layers.clone(),
                provisional.bitrates,
            );
        }

        let mut max_reachable_layer_temporal = buffer::INVALID_LAYER_TEMPORAL;
        for t in (0..=provisional.max_layer.temporal).rev() {
            for s in (0..=provisional.max_layer.spatial).rev() {
                if provisional.bitrates[s as usize][t as usize] != 0 {
                    max_reachable_layer_temporal = t;
                    break;
                }
            }
            if max_reachable_layer_temporal != buffer::INVALID_LAYER_TEMPORAL {
                break;
            }
        }

        if max_reachable_layer_temporal == buffer::INVALID_LAYER_TEMPORAL {
            // feed has gone dry, just leave target at current to enable opportunistic forwarding in case current resumes.
            // Note that this is giving back bits and opportunistic forwarding resuming might trigger congestion again,
            // but that should be handled by stream allocator.
            provisional.allocated_layer = provisional.current_layer;
            return (
                VideoTransition {
                    from: target_layer,
                    to: provisional.allocated_layer,
                    bandwidth_delta: 0 - get_bandwidth_needed(
                        provisional.bitrates,
                        target_layer,
                        f.last_allocation.bandwidth_requested,
                    ),
                },
                provisional.available_layers.clone(),
                provisional.bitrates,
            );
        }

        // starting from minimum to target, find transition which gives the best
        // transition taking into account bits saved vs cost of such a transition
        let existing_bandwidth_needed = get_bandwidth_needed(
            provisional.bitrates,
            target_layer,
            f.last_allocation.bandwidth_requested,
        );
        let mut best_layer = buffer::INVALID_LAYER;
        let mut best_bandwidth_delta = 0i64;
        let mut best_value = 0f32;
        for s in 0..=target_layer.spatial {
            for t in 0..=target_layer.temporal {
                if s == target_layer.spatial && t == target_layer.temporal {
                    break;
                }

                let bandwidth_delta =
                    0.max(existing_bandwidth_needed - provisional.bitrates[s as usize][t as usize]);

                let mut transition_cost = 0i32;
                // SVC-TODO: SVC will need a different cost transition
                if target_layer.spatial != s {
                    transition_cost = TRANSITION_COST_SPATIAL;
                }

                let quality_cost = (max_reachable_layer_temporal + 1) * (target_layer.spatial - s)
                    + (target_layer.temporal - t);

                let mut value = 0f32;
                if (transition_cost + quality_cost) != 0 {
                    value = bandwidth_delta as f32 / (transition_cost + quality_cost) as f32;
                }
                if value > best_value
                    || (value == best_value && bandwidth_delta > best_bandwidth_delta)
                {
                    best_value = value;
                    best_bandwidth_delta = bandwidth_delta;
                    best_layer = buffer::VideoLayer {
                        spatial: s,
                        temporal: t,
                    };
                }
            }
        }

        provisional.allocated_layer = best_layer;
        (
            VideoTransition {
                from: target_layer,
                to: best_layer,
                bandwidth_delta: -best_bandwidth_delta,
            },
            provisional.available_layers.clone(),
            provisional.bitrates,
        )
    }

    pub fn provisional_allocate_commit(&self) -> VideoAllocation {
        let mut guard = self.lock.write();
        let f = &mut *guard;
        let provisional = f.provisional.as_mut().expect(PROVISIONAL_NOT_PREPARED);

        let optimal_bandwidth_needed = get_optimal_bandwidth_needed(
            provisional.muted,
            provisional.pub_muted,
            provisional.max_seen_layer.spatial,
            provisional.bitrates,
            provisional.max_layer,
        );
        let mut alloc = VideoAllocation {
            bandwidth_requested: 0,
            bandwidth_delta: 0 - get_bandwidth_needed(
                provisional.bitrates,
                f.vls.get_target(),
                f.last_allocation.bandwidth_requested,
            ),
            bitrates: provisional.bitrates,
            bandwidth_needed: optimal_bandwidth_needed,
            target_layer: provisional.allocated_layer,
            request_layer_spatial: provisional.allocated_layer.spatial,
            max_layer: provisional.max_layer,
            distance_to_desired: get_distance_to_desired(
                provisional.muted,
                provisional.pub_muted,
                provisional.max_seen_layer,
                &provisional.available_layers,
                provisional.bitrates,
                provisional.allocated_layer,
                provisional.max_layer,
            ),
            ..Default::default()
        };

        if provisional.muted {
            alloc.pause_reason = VideoPauseReason::Muted;
        } else if provisional.pub_muted {
            alloc.pause_reason = VideoPauseReason::PubMuted;
        } else if optimal_bandwidth_needed == 0 {
            if provisional.allocated_layer.is_valid() {
                // overshoot
                alloc.bandwidth_requested = provisional.bitrates
                    [provisional.allocated_layer.spatial as usize]
                    [provisional.allocated_layer.temporal as usize];
                alloc.bandwidth_delta = alloc.bandwidth_requested
                    - get_bandwidth_needed(
                        provisional.bitrates,
                        f.vls.get_target(),
                        f.last_allocation.bandwidth_requested,
                    );
            } else {
                alloc.pause_reason = VideoPauseReason::FeedDry;

                // leave target at current for opportunistic forwarding
                if provisional.current_layer.is_valid()
                    && provisional.current_layer.spatial <= provisional.max_layer.spatial
                {
                    provisional.allocated_layer = provisional.current_layer;
                    alloc.target_layer = provisional.allocated_layer;
                    alloc.request_layer_spatial = alloc.target_layer.spatial;
                }
            }
        } else {
            if provisional.allocated_layer.is_valid() {
                alloc.bandwidth_requested = provisional.bitrates
                    [provisional.allocated_layer.spatial as usize]
                    [provisional.allocated_layer.temporal as usize];
            }
            alloc.bandwidth_delta = alloc.bandwidth_requested
                - get_bandwidth_needed(
                    provisional.bitrates,
                    f.vls.get_target(),
                    f.last_allocation.bandwidth_requested,
                );

            if provisional
                .allocated_layer
                .greater_than(provisional.max_layer)
                || alloc.bandwidth_requested
                    >= get_optimal_bandwidth_needed(
                        provisional.muted,
                        provisional.pub_muted,
                        provisional.max_seen_layer.spatial,
                        provisional.bitrates,
                        provisional.max_layer,
                    )
            {
                // could be greater than optimal if overshooting
                alloc.is_deficient = false;
            } else {
                alloc.is_deficient = true;
                if !provisional.allocated_layer.is_valid() {
                    alloc.pause_reason = VideoPauseReason::Bandwidth;
                }
            }
        }

        self.update_allocation(f, alloc, "cooperative")
    }

    pub fn allocate_next_higher(
        &self,
        available_channel_capacity: i64,
        available_layers: &[i32],
        brs: Bitrates,
        allow_overshoot: bool,
    ) -> (VideoAllocation, bool) {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        if self.kind == webrtc::RtpCodecType::Audio {
            return (f.last_allocation, false);
        }

        // if not deficient, nothing to do
        if !self.is_deficient_locked(f) {
            return (f.last_allocation, false);
        }

        let max_layer = f.vls.get_max();
        let max_seen_layer = f.vls.get_max_seen();
        let optimal_bandwidth_needed = get_optimal_bandwidth_needed(
            f.muted,
            f.pub_muted,
            max_seen_layer.spatial,
            brs,
            max_layer,
        );

        let mut already_allocated = 0i64;
        let target_layer = f.vls.get_target();
        if target_layer.is_valid() {
            already_allocated = brs[target_layer.spatial as usize][target_layer.temporal as usize];
        }

        let do_allocation = |f: &mut ForwarderInner,
                             min_spatial: i32,
                             max_spatial: i32,
                             min_temporal: i32,
                             max_temporal: i32|
         -> (bool, VideoAllocation, bool) {
            for s in min_spatial..=max_spatial {
                for t in min_temporal..=max_temporal {
                    let bandwidth_requested = brs[s as usize][t as usize];
                    if bandwidth_requested == 0 {
                        continue;
                    }

                    if (!allow_overshoot || !f.vls.is_overshoot_okay())
                        && bandwidth_requested - already_allocated > available_channel_capacity
                    {
                        // next higher available layer does not fit, return
                        return (true, f.last_allocation, false);
                    }

                    let new_target_layer = buffer::VideoLayer {
                        spatial: s,
                        temporal: t,
                    };
                    let mut alloc = VideoAllocation {
                        is_deficient: true,
                        bandwidth_requested,
                        bandwidth_delta: bandwidth_requested - already_allocated,
                        bandwidth_needed: optimal_bandwidth_needed,
                        bitrates: brs,
                        target_layer: new_target_layer,
                        request_layer_spatial: new_target_layer.spatial,
                        max_layer,
                        distance_to_desired: get_distance_to_desired(
                            f.muted,
                            f.pub_muted,
                            max_seen_layer,
                            available_layers,
                            brs,
                            new_target_layer,
                            max_layer,
                        ),
                        ..Default::default()
                    };
                    if new_target_layer.greater_than(max_layer)
                        || bandwidth_requested >= optimal_bandwidth_needed
                    {
                        alloc.is_deficient = false;
                    }

                    return (true, self.update_allocation(f, alloc, "next-higher"), true);
                }
            }

            (false, VideoAllocation::default(), false)
        };

        // try moving temporal layer up in currently streaming spatial layer
        if target_layer.is_valid() {
            let (done, allocation, boosted) = do_allocation(
                f,
                target_layer.spatial,
                target_layer.spatial,
                target_layer.temporal + 1,
                max_layer.temporal,
            );
            if done {
                return (allocation, boosted);
            }
        }

        // try moving spatial layer up if temporal layer move up is not available
        let (done, allocation, boosted) = do_allocation(
            f,
            target_layer.spatial + 1,
            max_layer.spatial,
            0,
            max_layer.temporal,
        );
        if done {
            return (allocation, boosted);
        }

        if allow_overshoot && f.vls.is_overshoot_okay() && max_layer.is_valid() {
            let (done, allocation, boosted) = do_allocation(
                f,
                max_layer.spatial + 1,
                buffer::DEFAULT_MAX_LAYER_SPATIAL,
                0,
                buffer::DEFAULT_MAX_LAYER_TEMPORAL,
            );
            if done {
                return (allocation, boosted);
            }
        }

        (f.last_allocation, false)
    }

    pub fn get_next_higher_transition(
        &self,
        brs: Bitrates,
        allow_overshoot: bool,
    ) -> (VideoTransition, bool) {
        let guard = self.lock.write();
        let f = &*guard;

        if self.kind == webrtc::RtpCodecType::Audio {
            return (VideoTransition::default(), false);
        }

        // if not deficient, nothing to do
        if !self.is_deficient_locked(f) {
            return (VideoTransition::default(), false);
        }

        // if targets are still pending, don't increase
        let target_layer = f.vls.get_target();
        if target_layer.is_valid() && target_layer != f.vls.get_current() {
            return (VideoTransition::default(), false);
        }

        let mut already_allocated = 0i64;
        if target_layer.is_valid() {
            already_allocated = brs[target_layer.spatial as usize][target_layer.temporal as usize];
        }

        let find_next_higher = |min_spatial: i32,
                                max_spatial: i32,
                                min_temporal: i32,
                                max_temporal: i32|
         -> (bool, VideoTransition, bool) {
            for s in min_spatial..=max_spatial {
                for t in min_temporal..=max_temporal {
                    let bandwidth_requested = brs[s as usize][t as usize];
                    // traverse till finding a layer requiring more bits.
                    // NOTE: it possible that higher temporal layer of lower spatial layer
                    //       could use more bits than lower temporal layer of higher spatial layer.
                    if bandwidth_requested == 0 || bandwidth_requested < already_allocated {
                        continue;
                    }

                    let transition = VideoTransition {
                        from: target_layer,
                        to: buffer::VideoLayer {
                            spatial: s,
                            temporal: t,
                        },
                        bandwidth_delta: bandwidth_requested - already_allocated,
                    };

                    return (true, transition, true);
                }
            }

            (false, VideoTransition::default(), false)
        };

        // try moving temporal layer up in currently streaming spatial layer
        let max_layer = f.vls.get_max();
        if target_layer.is_valid() {
            let (done, transition, is_available) = find_next_higher(
                target_layer.spatial,
                target_layer.spatial,
                target_layer.temporal + 1,
                max_layer.temporal,
            );
            if done {
                return (transition, is_available);
            }
        }

        // try moving spatial layer up if temporal layer move up is not available
        let (done, transition, is_available) = find_next_higher(
            target_layer.spatial + 1,
            max_layer.spatial,
            0,
            max_layer.temporal,
        );
        if done {
            return (transition, is_available);
        }

        if allow_overshoot && f.vls.is_overshoot_okay() && max_layer.is_valid() {
            let (done, transition, is_available) = find_next_higher(
                max_layer.spatial + 1,
                buffer::DEFAULT_MAX_LAYER_SPATIAL,
                0,
                buffer::DEFAULT_MAX_LAYER_TEMPORAL,
            );
            if done {
                return (transition, is_available);
            }
        }

        (VideoTransition::default(), false)
    }

    pub fn pause(&self, available_layers: &[i32], brs: Bitrates) -> VideoAllocation {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        let max_layer = f.vls.get_max();
        let max_seen_layer = f.vls.get_max_seen();
        let optimal_bandwidth_needed = get_optimal_bandwidth_needed(
            f.muted,
            f.pub_muted,
            max_seen_layer.spatial,
            brs,
            max_layer,
        );
        let mut alloc = VideoAllocation {
            bandwidth_requested: 0,
            bandwidth_delta: 0 - get_bandwidth_needed(
                brs,
                f.vls.get_target(),
                f.last_allocation.bandwidth_requested,
            ),
            bitrates: brs,
            bandwidth_needed: optimal_bandwidth_needed,
            target_layer: buffer::INVALID_LAYER,
            request_layer_spatial: buffer::INVALID_LAYER_SPATIAL,
            max_layer,
            distance_to_desired: get_distance_to_desired(
                f.muted,
                f.pub_muted,
                max_seen_layer,
                available_layers,
                brs,
                buffer::INVALID_LAYER,
                max_layer,
            ),
            ..Default::default()
        };

        if f.muted {
            alloc.pause_reason = VideoPauseReason::Muted;
        } else if f.pub_muted {
            alloc.pause_reason = VideoPauseReason::PubMuted;
        } else if optimal_bandwidth_needed == 0 {
            alloc.pause_reason = VideoPauseReason::FeedDry;
        } else {
            // pausing due to lack of bandwidth
            alloc.is_deficient = true;
            alloc.pause_reason = VideoPauseReason::Bandwidth;
        }

        self.update_allocation(f, alloc, "pause")
    }

    fn update_allocation(
        &self,
        f: &mut ForwarderInner,
        mut alloc: VideoAllocation,
        reason: &str,
    ) -> VideoAllocation {
        // restrict target temporal to 0 if codec does not support temporal layers
        if alloc.target_layer.is_valid() && f.mime == mime::MimeType::H264 {
            alloc.target_layer.temporal = 0;
        }

        if alloc.is_deficient != f.last_allocation.is_deficient
            || alloc.pause_reason != f.last_allocation.pause_reason
            || alloc.target_layer != f.last_allocation.target_layer
            || alloc.request_layer_spatial != f.last_allocation.request_layer_spatial
        {
            debugw!(
                self.logger,
                &format!("stream allocation: {reason}"),
                "allocation" => alloc,
                "lastAllocation" => f.last_allocation,
            );
        }
        f.last_allocation = alloc;

        self.set_target_layer(
            f,
            f.last_allocation.target_layer,
            f.last_allocation.request_layer_spatial,
        );
        if !f.vls.get_target().is_valid() {
            self.resync_locked(f);
        }

        f.last_allocation
    }

    fn set_target_layer(
        &self,
        f: &mut ForwarderInner,
        target_layer: buffer::VideoLayer,
        request_layer_spatial: i32,
    ) {
        f.vls.set_target(target_layer);
        if target_layer.is_valid() {
            f.vls.set_request_spatial(request_layer_spatial);
        } else {
            f.vls.set_request_spatial(buffer::INVALID_LAYER_SPATIAL);
        }
    }

    pub fn resync(&self) {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        self.resync_locked(f);
    }

    fn resync_locked(&self, f: &mut ForwarderInner) {
        f.vls.set_current(buffer::INVALID_LAYER);
        f.last_ssrc = 0;
        if f.pub_muted {
            f.resume_behind_threshold = RESUME_BEHIND_THRESHOLD_SECONDS;
        }
    }

    pub fn check_sync(&self) -> (bool, i32) {
        let f = self.lock.read();

        f.vls.check_sync()
    }

    pub fn restart(&self) {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        self.resync_locked(f);
        self.set_target_layer(f, buffer::INVALID_LAYER, buffer::INVALID_LAYER_SPATIAL);
        f.reference_layer_spatial = buffer::INVALID_LAYER_SPATIAL;
        f.last_reference_payload_type = -1;
        // The next source switch re-derives the offset from the new reference layer's raw timestamp,
        // so that it maps onto the downtrack's existing timeline (extExpectedTS). Keeping the old
        // offset would fold it into the new one and shift every later switch by the old value.
        f.dummy_start_ts_offset = 0;

        for ref_info in f.ref_infos.iter_mut() {
            *ref_info = RefInfo::default();
        }
        f.last_switch_ext_incoming_ts = 0;
        f.ref_video_layer_mode = livekit::video_layer::Mode::Unused;
    }

    pub fn force_layer_for_test(&self, layer: buffer::VideoLayer) {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        f.vls.set_current(layer);
        f.vls.set_target(layer);
    }

    pub fn filter_rtx(
        &self,
        nacks: &[u16],
    ) -> (
        Vec<u16>,
        [bool; (buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize],
    ) {
        let f = self.lock.read();

        let mut disallowed_layers = [false; (buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize];
        let filtered = if !FLAG_FILTER_RTX {
            nacks.to_vec()
        } else {
            f.rtp_munger.filter_rtx(nacks)
        };

        //
        // Curb RTX when deficient for two cases
        //   1. Target layer is lower than current layer. When current hits target, a key frame should flush the decoder.
        //   2. Requested layer is higher than current. Current layer's key frame should have flushed encoder.
        //      Remote might ask for older layer because of its jitter buffer, but let it starve as channel is already congested.
        //
        // Without the curb, when congestion hits, RTX rate could be so high that it further congests the channel.
        //
        if FLAG_FILTER_RTX_LAYERS {
            let current_layer = f.vls.get_current();
            let target_layer = f.vls.get_target();
            for layer in 0..=buffer::DEFAULT_MAX_LAYER_SPATIAL {
                if self.is_deficient_locked(&f)
                    && (target_layer.spatial < current_layer.spatial
                        || layer > current_layer.spatial)
                {
                    disallowed_layers[layer as usize] = true;
                }
            }
        }
        (filtered, disallowed_layers)
    }

    pub fn get_translation_params(
        &self,
        ext_pkt: &buffer::ExtPacket,
        layer: i32,
    ) -> (TranslationParams, Result<(), Error>) {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        if f.muted || f.pub_muted {
            return (
                TranslationParams {
                    should_drop: true,
                    ..Default::default()
                },
                Ok(()),
            );
        }

        match self.kind {
            webrtc::RtpCodecType::Audio => self.get_translation_params_audio(f, ext_pkt, layer),

            webrtc::RtpCodecType::Video => self.get_translation_params_video(f, ext_pkt, layer),

            webrtc::RtpCodecType::Unknown => (
                TranslationParams {
                    should_drop: true,
                    ..Default::default()
                },
                Err(Error::UnknownKind),
            ),
        }
    }

    fn get_ref_layer_rtp_timestamp(
        &self,
        f: &ForwarderInner,
        ts: u32,
        ref_layer: i32,
        target_layer: i32,
    ) -> Result<u32, Error> {
        if ref_layer < 0
            || ref_layer as usize >= f.ref_infos.len()
            || target_layer < 0
            || target_layer as usize >= f.ref_infos.len()
        {
            return Err(Error::InvalidLayers {
                ref_layer,
                target_layer,
            });
        }

        if ref_layer == target_layer
            || f.ref_video_layer_mode == livekit::video_layer::Mode::MultipleSpatialLayersPerStream
        {
            return Ok(ts);
        }

        let sr_ref = f.ref_infos[ref_layer as usize].sender_report;
        let sr_target = f.ref_infos[target_layer as usize].sender_report;
        let Some(sr_ref) = sr_ref.filter(|sr_ref| sr_ref.ntp_timestamp != 0) else {
            return Err(Error::UnavailableLayerRef {
                ref_layer,
                target_layer,
            });
        };
        let Some(sr_target) = sr_target.filter(|sr_target| sr_target.ntp_timestamp != 0) else {
            return Err(Error::UnavailableLayerTarget {
                ref_layer,
                target_layer,
            });
        };

        let ntp_diff = NtpTime(sr_ref.ntp_timestamp)
            .time()
            .sub(NtpTime(sr_target.ntp_timestamp).time());
        let rtp_diff = ntp_diff.nanoseconds().wrapping_mul(f.clock_rate as i64) / 1_000_000_000;

        // calculate other layer's time stamp at the same time as ref layer's NTP time
        let normalized_other_ts = sr_target.rtp_timestamp.wrapping_add(rtp_diff as u32);

        // now both layers' time stamp refer to the same NTP time and the diff is the offset between the layers
        let offset = sr_ref.rtp_timestamp.wrapping_sub(normalized_other_ts);

        Ok(ts.wrapping_add(offset))
    }

    fn process_source_switch(
        &self,
        f: &mut ForwarderInner,
        ext_pkt: &buffer::ExtPacket,
        layer: i32,
    ) -> (bool, Result<(), Error>) {
        let mut starting = false;
        if !f.started {
            if ext_pkt.is_out_of_order {
                return (starting, Err(Error::SkipStartOnOutOfOrderPacket));
            }

            f.started = true;
            f.reference_layer_spatial = layer;
            f.rtp_munger.set_last_sn_ts(ext_pkt);
            f.codec_munger.set_last(ext_pkt);
            debugw!(
                self.logger,
                "starting forwarding",
                "sequenceNumber" => ext_pkt.packet.sequence_number,
                "extSequenceNumber" => ext_pkt.ext_sequence_number,
                "timestamp" => ext_pkt.packet.timestamp,
                "extTimestamp" => ext_pkt.ext_timestamp,
                "layer" => layer,
                "referenceLayerSpatial" => f.reference_layer_spatial,
            );
            starting = true;
            return (starting, Ok(()));
        } else if f.reference_layer_spatial == buffer::INVALID_LAYER_SPATIAL {
            if ext_pkt.is_out_of_order {
                return (starting, Err(Error::SkipStartOnOutOfOrderPacket));
            }

            f.reference_layer_spatial = layer;
            f.codec_munger.set_last(ext_pkt);
            debugw!(
                self.logger,
                "catch up forwarding",
                "sequenceNumber" => ext_pkt.packet.sequence_number,
                "extSequenceNumber" => ext_pkt.ext_sequence_number,
                "timestamp" => ext_pkt.packet.timestamp,
                "extTimestamp" => ext_pkt.ext_timestamp,
                "layer" => layer,
                "referenceLayerSpatial" => f.reference_layer_spatial,
            );

            if f.start_pending {
                f.start_pending = false;
                starting = true;
            }
        }

        let log_transition = |f: &ForwarderInner,
                              message: &str,
                              ext_expected_ts: u64,
                              ext_ref_ts: u64,
                              ext_last_ts: u64,
                              diff_seconds: f64| {
            debugw!(
                self.logger,
                message,
                "layer" => layer,
                "referenceLayerSpatial" => f.reference_layer_spatial,
                "extExpectedTS" => ext_expected_ts,
                "incomingTS" => ext_pkt.packet.timestamp,
                "extIncomingTS" => ext_pkt.ext_timestamp,
                "extRefTS" => ext_ref_ts,
                "extLastTS" => ext_last_ts,
                "diffSeconds" => diff_seconds.abs(),
                "refInfos" => f.ref_infos,
                "lastSwitchExtIncomingTS" => f.last_switch_ext_incoming_ts,
                "rtpStats" => self.rtp_stats,
            );
        };

        // Compute how much time passed between the previous forwarded packet
        // and the current incoming (to be forwarded) packet and calculate
        // timestamp offset on source change.
        //
        // There are three timestamps to consider here
        //   1. extLastTS -> timestamp of last sent packet
        //   2. extRefTS -> timestamp of this packet (after munging) calculated using feed's RTCP sender report
        //   3. extExpectedTS -> expected timestamp of this packet calculated based on elapsed time since first packet
        // Ideally, extRefTS and extExpectedTS should be very close and extLastTS should be before both of those.
        // But, cases like muting/unmuting, clock vagaries, pacing, etc. make them not satisfy those conditions always.
        let ext_last_ts = f.rtp_munger.get_ext_last_timestamp();
        let mut ext_expected_ts = ext_last_ts;
        let mut ext_ref_ts = ext_last_ts;
        let mut ref_ts = ext_ref_ts as u32;
        let switching_at = mono::now();
        if !f.skip_reference_ts {
            ref_ts = match self.get_ref_layer_rtp_timestamp(
                f,
                ext_pkt.packet.timestamp,
                f.reference_layer_spatial,
                layer,
            ) {
                Ok(ref_ts) => ref_ts,
                Err(err) => {
                    // error out if refTS is not available. It can happen when there is no sender report
                    // for the layer being switched to. Can especially happen at the start of the track when layer switches are
                    // potentially happening very quickly. Erroring out and waiting for a layer for which a sender report has been
                    // received will calculate a better offset, but may result in initial adaptation to take a bit longer depending
                    // on how often publisher/remote side sends RTCP sender report.
                    debugw!(
                        self.logger,
                        "could not get ref layer timestamp",
                        "referenceLayerSpatial" => f.reference_layer_spatial,
                        "layer" => layer,
                        "error" => format_args!("{err}"),
                    );
                    return (starting, Err(err));
                }
            };

            // adjust refTS to current packet's timestamp mapped to that of reference layer's.
            // Without a reference layer (skipReferenceTS), refTS is the last outgoing timestamp,
            // which is already on the downtrack's timeline and needs no offset.
            //
            // The dummy start offset is applied modulo 2^32, before the cycles are restored from
            // extLastTS. Adding it in 64 bits lets a borrow (negative offset) or a carry (positive offset)
            // reach the upper half of extRefTS, and the wrap correction below only compares the lower 32
            // bits, so it cannot undo it: extRefTS would end up 2^32 below (or above) the right value.
            // With a negative offset that rejects every switch point as too far behind until the
            // reference layer's 32-bit timestamp climbs past |dummyStartTSOffset|, which can take hours.
            ref_ts = (ref_ts as u64).wrapping_add(f.dummy_start_ts_offset) as u32;
        }
        ext_ref_ts = (ext_ref_ts & 0xFFFF_FFFF_0000_0000).wrapping_add(ref_ts as u64);
        let last_ts = ext_last_ts as u32;
        if ref_ts.wrapping_sub(last_ts) < (1 << 31) && ref_ts < last_ts {
            ext_ref_ts = ext_ref_ts.wrapping_add(1 << 32);
        }
        if last_ts.wrapping_sub(ref_ts) < (1 << 31) && last_ts < ref_ts && ext_ref_ts >= (1 << 32) {
            ext_ref_ts = ext_ref_ts.wrapping_sub(1 << 32);
        }

        if let Some(rtp_stats) = &self.rtp_stats {
            match rtp_stats.get_expected_rtp_timestamp(switching_at.unix_nano()) {
                Ok(ts_ext) => {
                    ext_expected_ts = ts_ext;
                    if f.last_reference_payload_type == -1 {
                        if !f.skip_reference_ts {
                            f.dummy_start_ts_offset = ext_expected_ts.wrapping_sub(ref_ts as u64);
                        }
                        ext_ref_ts = ext_expected_ts;
                    }
                }
                Err(_) => {
                    if !f.pre_start_time.is_zero() {
                        let time_since_first = time::since(f.pre_start_time);
                        let rtp_diff = (time_since_first
                            .nanoseconds()
                            .wrapping_mul(f.clock_rate as i64)
                            / 1_000_000_000) as u64;
                        ext_expected_ts = f.ext_first_ts.wrapping_add(rtp_diff);
                        if f.dummy_start_ts_offset == 0 {
                            if !f.skip_reference_ts {
                                f.dummy_start_ts_offset =
                                    ext_expected_ts.wrapping_sub(ref_ts as u64);
                            }
                            ext_ref_ts = ext_expected_ts;
                            infow!(
                                self.logger,
                                "calculating dummyStartTSOffset",
                                "preStartTime" => f.pre_start_time,
                                "extFirstTS" => f.ext_first_ts,
                                "timeSinceFirst" => time_since_first,
                                "rtpDiff" => rtp_diff,
                                "extRefTS" => ext_ref_ts,
                                "incomingTS" => ext_pkt.packet.timestamp,
                                "referenceLayerSpatial" => f.reference_layer_spatial,
                                "dummyStartTSOffset" => f.dummy_start_ts_offset,
                            );
                        }
                    }
                }
            }
        }

        let mut ext_next_ts: u64;
        if f.last_ssrc == 0 {
            // If resuming (e. g. on unmute), keep next timestamp close to expected timestamp.
            //
            // Rationale:
            // Case 1: If mute is implemented via something like stopping a track and resuming it on unmute,
            // the RTP timestamp may not have jumped across mute valley. In this case, old timestamp
            // should not be used.
            //
            // Case 2: OTOH, something like pacing may be adding latency in the publisher path (even if
            // the timestamps incremented correctly across the mute valley). In this case, reference
            // timestamp should be used as things will catch up to real time when channel capacity
            // increases and pacer starts sending at faster rate.
            //
            // But, the challenge is distinguishing between the two cases. As a compromise, the difference
            // between extExpectedTS and extRefTS is thresholded. Difference below the threshold is treated as Case 2
            // and above as Case 1.
            //
            // In the event of extRefTS > extExpectedTS, use extRefTS.
            // Ideally, extRefTS should not be ahead of extExpectedTS, but extExpectedTS uses the first packet's
            // wall clock time. So, if the first packet experienced abmormal latency, it is possible
            // for extRefTS > extExpectedTS
            let diff_seconds =
                (ext_expected_ts.wrapping_sub(ext_ref_ts) as i64) as f64 / f.clock_rate as f64;
            if diff_seconds >= 0.0 {
                if f.resume_behind_threshold > 0.0 && diff_seconds > f.resume_behind_threshold {
                    log_transition(
                        f,
                        "resume, reference too far behind",
                        ext_expected_ts,
                        ext_ref_ts,
                        ext_last_ts,
                        diff_seconds,
                    );
                    ext_next_ts = ext_expected_ts;
                } else if diff_seconds > RESUME_BEHIND_HIGH_THRESHOLD_SECONDS {
                    // could be due to incoming time stamp lagging a lot, like an unpause of the track
                    log_transition(
                        f,
                        "resume, reference very far behind",
                        ext_expected_ts,
                        ext_ref_ts,
                        ext_last_ts,
                        diff_seconds,
                    );
                    ext_next_ts = ext_expected_ts;
                } else {
                    ext_next_ts = ext_ref_ts;
                }
            } else {
                if diff_seconds.abs() > SWITCH_AHEAD_THRESHOLD_SECONDS {
                    log_transition(
                        f,
                        "resume, reference too far ahead",
                        ext_expected_ts,
                        ext_ref_ts,
                        ext_last_ts,
                        diff_seconds,
                    );
                }
                ext_next_ts = ext_ref_ts;
            }
            f.resume_behind_threshold = 0.0;
        } else {
            // switching between layers, check if extRefTS is too far behind the last sent
            let mut diff_seconds =
                (ext_ref_ts.wrapping_sub(ext_last_ts) as i64) as f64 / f.clock_rate as f64;
            if diff_seconds < 0.0 {
                if diff_seconds.abs() > LAYER_SWITCH_BEHIND_THRESHOLD_SECONDS {
                    // this could be due to pacer trickling out this layer. Error out and wait for a more opportune time.
                    // AVSYNC-TODO: Consider some forcing function to do the switch
                    // (like "have waited for too long for layer switch, nothing available, switch to whatever is available" kind of condition).
                    log_transition(
                        f,
                        "layer switch, reference too far behind",
                        ext_expected_ts,
                        ext_ref_ts,
                        ext_last_ts,
                        diff_seconds,
                    );

                    return (starting, Err(Error::SwitchPointTooFarBehind));
                }

                // use a nominal increase to ensure that timestamp is always moving forward
                log_transition(
                    f,
                    "layer switch, reference is slightly behind",
                    ext_expected_ts,
                    ext_ref_ts,
                    ext_last_ts,
                    diff_seconds,
                );
                ext_next_ts = ext_last_ts.wrapping_add(1);
            } else {
                diff_seconds =
                    (ext_ref_ts.wrapping_sub(ext_expected_ts) as i64) as f64 / f.clock_rate as f64;
                if diff_seconds > SWITCH_AHEAD_THRESHOLD_SECONDS {
                    log_transition(
                        f,
                        "layer switch, reference too far ahead",
                        ext_expected_ts,
                        ext_ref_ts,
                        ext_last_ts,
                        diff_seconds,
                    );
                }

                ext_next_ts = ext_ref_ts;
            }
        }

        if (ext_next_ts.wrapping_sub(ext_last_ts) as i64) <= 0 {
            debugw!(
                self.logger,
                "next timestamp is before last, adjusting",
                "extNextTS" => ext_next_ts,
                "extLastTS" => ext_last_ts,
            );
            // nominal increase
            ext_next_ts = ext_last_ts.wrapping_add(1);
        }
        debugw!(
            self.logger,
            "next timestamp on switch",
            "switchingAt" => switching_at,
            "layer" => layer,
            "extLastTS" => ext_last_ts,
            "lastMarker" => f.rtp_munger.get_state().last_marker,
            "extRefTS" => ext_ref_ts,
            "dummyStartTSOffset" => f.dummy_start_ts_offset,
            "referenceLayerSpatial" => f.reference_layer_spatial,
            "extExpectedTS" => ext_expected_ts,
            "extNextTS" => ext_next_ts,
            "tsJump" => ext_next_ts.wrapping_sub(ext_last_ts),
            "nextSN" => f.rtp_munger.get_state().ext_last_sequence_number.wrapping_add(1),
            "extIncomingSN" => ext_pkt.ext_sequence_number,
            "extIncomingTS" => ext_pkt.ext_timestamp,
            "rtpStats" => self.rtp_stats,
        );

        f.rtp_munger
            .update_sn_ts_offsets(ext_pkt, 1, ext_next_ts.wrapping_sub(ext_last_ts));
        f.codec_munger.update_offsets(ext_pkt);
        (starting, Ok(()))
    }

    // should be called with lock held
    fn get_translation_params_common(
        &self,
        f: &mut ForwarderInner,
        ext_pkt: &buffer::ExtPacket,
        layer: i32,
        tp: &mut TranslationParams,
    ) -> (bool, Result<(), Error>) {
        let mut starting = false;
        if f.last_ssrc != ext_pkt.packet.ssrc {
            let res;
            (starting, res) = self.process_source_switch(f, ext_pkt, layer);
            if let Err(err) = res {
                debugw!(
                    self.logger,
                    "could not switch feed",
                    "error" => format_args!("{err}"),
                    "layer" => layer,
                    "refInfos" => f.ref_infos,
                    "lastSwitchExtIncomingTS" => f.last_switch_ext_incoming_ts,
                    "rtpStats" => self.rtp_stats,
                    "currentLayer" => f.vls.get_current(),
                    "targetLayer" => f.vls.get_current(),
                    "maxLayer" => f.vls.get_max(),
                );
                tp.should_drop = true;
                f.vls.rollback();
                return (starting, Ok(()));
            }
            debugw!(
                self.logger,
                "switching feed",
                "fromSSRC" => f.last_ssrc,
                "toSSRC" => ext_pkt.packet.ssrc,
                "fromPayloadType" => f.last_reference_payload_type,
                "toPayloadType" => ext_pkt.packet.payload_type,
                "layer" => layer,
                "refInfos" => f.ref_infos,
                "lastSwitchExtIncomingTS" => f.last_switch_ext_incoming_ts,
                "currentLayer" => f.vls.get_current(),
                "targetLayer" => f.vls.get_current(),
                "maxLayer" => f.vls.get_max(),
            );
            f.last_ssrc = ext_pkt.packet.ssrc;
            f.last_reference_payload_type = ext_pkt.packet.payload_type as i8;
            f.last_switch_ext_incoming_ts = ext_pkt.ext_timestamp;
        }

        let (tp_rtp, res) = f.rtp_munger.update_and_get_sn_ts(ext_pkt, tp.marker);
        if let Err(err) = res {
            tp.should_drop = true;
            if err == Error::PaddingOnlyPacket
                || err == Error::DuplicatePacket
                || err == Error::OutOfOrderSequenceNumberCacheMiss
            {
                return (starting, Ok(()));
            }
            return (starting, Err(err));
        }

        tp.rtp = tp_rtp;

        if !ext_pkt.packet.payload.is_empty() {
            return (starting, self.translate_codec_header(f, ext_pkt, tp));
        }

        (starting, Ok(()))
    }

    // should be called with lock held
    fn get_translation_params_audio(
        &self,
        f: &mut ForwarderInner,
        ext_pkt: &buffer::ExtPacket,
        layer: i32,
    ) -> (TranslationParams, Result<(), Error>) {
        let mut tp = TranslationParams::default();
        let (starting, res) = self.get_translation_params_common(f, ext_pkt, layer, &mut tp);
        tp.is_starting = starting;
        if res.is_err() {
            tp.should_drop = true;
        }
        (tp, res)
    }

    // should be called with lock held
    fn is_end_of_layer_frame(&self, f: &ForwarderInner, ext_pkt: &buffer::ExtPacket) -> bool {
        if let Some(dependency_descriptor) = &ext_pkt.dependency_descriptor {
            return dependency_descriptor
                .descriptor
                .as_ref()
                .is_some_and(|descriptor| descriptor.last_packet_in_frame);
        }

        if f.mime == mime::MimeType::Vp9 {
            return ext_pkt.vp9.as_ref().is_some_and(|vp9| vp9.e);
        }

        false
    }

    // should be called with lock held
    fn get_translation_params_video(
        &self,
        f: &mut ForwarderInner,
        ext_pkt: &buffer::ExtPacket,
        layer: i32,
    ) -> (TranslationParams, Result<(), Error>) {
        let mut tp = TranslationParams::default();
        if f.dd_forwarder.is_some()
            && ext_pkt.dependency_descriptor.is_none()
            && !ext_pkt.packet.payload.is_empty()
        {
            // The buffer stopped parsing the dependency descriptor because the publisher does
            // not send it. VP8 switches temporal layers with its payload descriptor, other
            // codecs forward every temporal layer. Padding never carries the descriptor, so it
            // does not count.
            infow!(
                self.logger,
                "turning off dependency descriptor for simulcast temporal layers",
                "layer" => layer,
            );
            f.dd_forwarder = None;
            if f.mime == mime::MimeType::Vp8 {
                f.is_dd_available = false;
                f.codec_munger = Box::new(codecmunger::Vp8::new_from_other(
                    f.codec_munger.as_ref(),
                    self.logger.clone(),
                ));
                if f.started {
                    // the munger takes its picture ID and TL0PICIDX from the first packet it sees
                    f.codec_munger.set_last(ext_pkt);
                }
                f.vls
                    .set_temporal_layer_selector(Some(Box::new(temporallayerselector::Vp8::new(
                        self.logger.clone(),
                    ))));
            } else {
                self.set_unfiltered_temporal(f);
            }
        }
        if f.codec_munger.as_any().is::<codecmunger::DdTemporal>()
            && f.mime == mime::MimeType::Vp8
            && ext_pkt.dependency_descriptor.is_some()
            && ext_pkt.vp8.as_ref().is_some_and(|vp8| vp8.i)
        {
            // The publisher sends picture IDs with the dependency descriptor, so they need
            // rewriting. The VP8 munger does that, the temporal layers still switch with the
            // dependency descriptor.
            infow!(
                self.logger,
                "VP8 picture ID with dependency descriptor, munging picture IDs",
                "layer" => layer,
            );
            f.codec_munger = Box::new(codecmunger::Vp8::new_from_other(
                f.codec_munger.as_ref(),
                self.logger.clone(),
            ));
            if f.started {
                f.codec_munger.set_last(ext_pkt);
            }
        }

        if !f.vls.get_target().is_valid() {
            // stream is paused by streamallocator
            tp.should_drop = true;
            return (tp, Ok(()));
        }

        let result = f.vls.select(ext_pkt, layer);
        if !result.is_selected {
            // the svc selector, simulcast with the dependency descriptor turns it off above
            if f.is_dd_available
                && f.dd_forwarder.is_none()
                && ext_pkt.dependency_descriptor.is_none()
            {
                infow!(
                    self.logger,
                    "turning off dependency descriptor",
                    "layer" => layer,
                    "refInfos" => f.ref_infos,
                    "lastSwitchExtIncomingTS" => f.last_switch_ext_incoming_ts,
                    "currentLayer" => f.vls.get_current(),
                    "targetLayer" => f.vls.get_current(),
                    "maxLayer" => f.vls.get_max(),
                );
                f.is_dd_available = false;
                match f.mime {
                    mime::MimeType::Vp9 => {
                        f.vls = Box::new(videolayerselector::Vp9::new_from_other(self.take_vls(f)));
                    }
                    mime::MimeType::Av1 => {
                        self.set_simulcast_selector(f);
                        self.set_unfiltered_temporal(f);
                    }
                    _ => {}
                }
            }
            tp.should_drop = true;
            if f.started && result.is_relevant {
                // call to update highest incoming sequence number and other internal structures
                let (tp_rtp, res) = f
                    .rtp_munger
                    .update_and_get_sn_ts(ext_pkt, result.rtp_marker);
                if res.is_ok() {
                    match tp_rtp.sn_ordering {
                        SequenceNumberOrdering::Contiguous => {
                            f.rtp_munger.packet_dropped(ext_pkt);
                        }
                        SequenceNumberOrdering::Gap | SequenceNumberOrdering::OutOfOrder => {
                            // a rollback after a gap moves the last outgoing number below the missing
                            // packets' slots and their late copies are rejected, so pad the slot instead
                            tp.rtp = tp_rtp;
                            tp.pad_dropped_slot = true;
                        }
                        SequenceNumberOrdering::Duplicate => {}
                    }
                }
            }
            return (tp, Ok(()));
        }
        tp.is_resuming = result.is_resuming;
        tp.is_switching = result.is_switching;
        tp.dd_bytes = result.dd_bytes;
        tp.dd_bytes_len = result.dd_bytes_len;
        tp.dd_bytes_spill = result.dd_bytes_spill;
        tp.marker = result.rtp_marker;
        tp.is_end_of_layer_frame = self.is_end_of_layer_frame(f, ext_pkt);
        f.packet_temporal = f.vls.get_current().temporal;

        let (starting, res) = self.get_translation_params_common(f, ext_pkt, layer, &mut tp);
        tp.is_starting = starting;
        if tp.should_drop {
            return (tp, res);
        }

        if FLAG_PAUSE_ON_DOWNGRADE
            && self.is_deficient_locked(f)
            && f.vls.get_target().spatial < f.vls.get_current().spatial
        {
            //
            // If target layer is lower than both the current and
            // maximum subscribed layer, it is due to bandwidth
            // constraints that the target layer has been switched down.
            // Continuing to send higher layer will only exacerbate the
            // situation by putting more stress on the channel. So, drop it.
            //
            // In the other direction, it is okay to keep forwarding till
            // switch point to get a smoother stream till the higher
            // layer key frame arrives.
            //
            // Note that it is possible for client subscription layer restriction
            // to coincide with server restriction due to bandwidth limitation,
            // In the case of subscription change, higher should continue streaming
            // to ensure smooth transition.
            //
            // To differentiate between the two cases, drop only when in DEFICIENT state.
            //
            tp.should_drop = true;
            return (tp, Ok(()));
        }

        if let Some(dd_forwarder) = &mut f.dd_forwarder {
            // after translateCodecHeader, which picks the packet's temporal layer
            let mut dd_result = videolayerselector::VideoLayerSelectorResult::default();
            if dd_forwarder.forward(ext_pkt, f.packet_temporal, &mut dd_result) {
                tp.dd_bytes = dd_result.dd_bytes;
                tp.dd_bytes_len = dd_result.dd_bytes_len;
                tp.dd_bytes_spill = dd_result.dd_bytes_spill;
            }
        }

        (tp, Ok(()))
    }

    fn translate_codec_header(
        &self,
        f: &mut ForwarderInner,
        ext_pkt: &buffer::ExtPacket,
        tp: &mut TranslationParams,
    ) -> Result<(), Error> {
        // codec specific forwarding check and any needed packet munging
        let tl = f.vls.select_temporal(ext_pkt);
        f.packet_temporal = tl;
        let (input_size, codec_bytes, codec_bytes_len) = match f.codec_munger.update_and_get(
            ext_pkt,
            tp.rtp.sn_ordering == SequenceNumberOrdering::OutOfOrder,
            tp.rtp.sn_ordering == SequenceNumberOrdering::Gap,
            tl,
        ) {
            Ok(updated) => updated,
            Err(err) => {
                tp.should_drop = true;
                if err == codecmunger::Error::FilteredTemporalLayer
                    || err == codecmunger::Error::OutOfOrderVp8PictureIdCacheMiss
                    || err == codecmunger::Error::FilteredOutOfOrder
                {
                    match tp.rtp.sn_ordering {
                        SequenceNumberOrdering::Contiguous => {
                            if err == codecmunger::Error::FilteredTemporalLayer {
                                // filtered temporal layer, update sequence number offset to prevent holes
                                f.rtp_munger.packet_dropped(ext_pkt);
                            }
                        }
                        SequenceNumberOrdering::Gap | SequenceNumberOrdering::OutOfOrder => {
                            // no rollback after a gap, see getTranslationParamsVideo
                            tp.pad_dropped_slot = true;
                        }
                        SequenceNumberOrdering::Duplicate => {}
                    }
                    return Ok(());
                }

                return Err(err.into());
            }
        };
        tp.incoming_header_size = input_size;
        tp.codec_bytes = codec_bytes;
        tp.codec_bytes_len = codec_bytes_len;
        Ok(())
    }

    fn maybe_start(&self, f: &mut ForwarderInner) {
        if f.started {
            return;
        }

        f.started = true;
        f.pre_start_time = time::now();
        f.start_pending = true;

        let sequence_number =
            (rand::rng().random_range(0..(1u32 << 14)) as u16).wrapping_add(1 << 15); // a random number in third quartile of sequence number space
        let timestamp = rand::rng()
            .random_range(0..(1u32 << 30))
            .wrapping_add(1 << 31); // a random number in third quartile of timestamp space
        let ext_pkt = buffer::ExtPacket {
            packet: rtp::Packet {
                header: rtp::Header {
                    sequence_number,
                    timestamp,
                    ..Default::default()
                },
                ..Default::default()
            },
            ext_sequence_number: sequence_number as u64,
            ext_timestamp: timestamp as u64,
            ..Default::default()
        };
        f.rtp_munger.set_last_sn_ts(&ext_pkt);

        f.ext_first_ts = timestamp as u64;
        infow!(
            self.logger,
            "starting with dummy forwarding",
            "sequenceNumber" => ext_pkt.packet.sequence_number,
            "timestamp" => ext_pkt.packet.timestamp,
            "preStartTime" => f.pre_start_time,
        );
    }

    pub fn get_sn_ts_for_padding(
        &self,
        num: usize,
        frame_rate: u32,
        mut force_marker: bool,
    ) -> Result<Vec<SnTs>, Error> {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        self.maybe_start(f);

        // padding is used for probing. Padding packets should only
        // be at frame boundaries to ensure decoder sequencer does
        // not get out-of-sync. But, when a stream is paused,
        // force a frame marker as a restart of the stream will
        // start with a key frame which will reset the decoder.
        if !f.vls.get_target().is_valid() {
            force_marker = true;
        }
        f.rtp_munger.update_and_get_padding_sn_ts(
            num,
            f.clock_rate,
            frame_rate,
            force_marker,
            f.rtp_munger.get_ext_last_timestamp(),
        )
    }

    pub fn get_sn_ts_for_blank_frames(
        &self,
        frame_rate: u32,
        mut num_packets: usize,
    ) -> Result<(Vec<SnTs>, bool), Error> {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        self.maybe_start(f);

        let frame_end_needed = !f.rtp_munger.is_on_frame_boundary();
        if frame_end_needed {
            num_packets += 1;
        }

        let ext_last_ts = f.rtp_munger.get_ext_last_timestamp();
        let mut ext_expected_ts = ext_last_ts;
        if let Some(rtp_stats) = &self.rtp_stats
            && let Ok(ts_ext) = rtp_stats.get_expected_rtp_timestamp(mono::unix_nano())
        {
            ext_expected_ts = ts_ext;
        }
        if (ext_expected_ts.wrapping_sub(ext_last_ts) as i64) <= 0 {
            ext_expected_ts = ext_last_ts.wrapping_add(1);
        }
        let snts = f.rtp_munger.update_and_get_padding_sn_ts(
            num_packets,
            f.clock_rate,
            frame_rate,
            frame_end_needed,
            ext_expected_ts,
        )?;
        Ok((snts, frame_end_needed))
    }

    pub fn get_padding(&self, frame_end_needed: bool) -> Result<Vec<u8>, Error> {
        let mut guard = self.lock.write();
        let f = &mut *guard;

        Ok(f.codec_munger.update_and_get_padding(!frame_end_needed)?)
    }

    pub fn rtp_munger_debug_info(&self) -> RtpMungerDebugInfo {
        let f = self.lock.read();

        f.rtp_munger.debug_info()
    }
}

// -----------------------------------------------------------------------------

fn get_optimal_bandwidth_needed(
    muted: bool,
    pub_muted: bool,
    max_published_layer: i32,
    brs: Bitrates,
    max_layer: buffer::VideoLayer,
) -> i64 {
    if muted || pub_muted || max_published_layer == buffer::INVALID_LAYER_SPATIAL {
        return 0;
    }

    for i in (0..=max_layer.spatial).rev() {
        for j in (0..=max_layer.temporal).rev() {
            if brs[i as usize][j as usize] == 0 {
                continue;
            }

            return brs[i as usize][j as usize];
        }
    }

    // could be 0 due to either
    //   1. publisher has stopped all layers ==> feed dry.
    //   2. stream tracker has declared all layers stopped, functionally same as above.
    //      But, listed differently as this could be a mis-detection.
    //   3. Bitrate measurement is pending.
    0
}

fn get_bandwidth_needed(brs: Bitrates, layer: buffer::VideoLayer, fallback: i64) -> i64 {
    if layer.is_valid() && brs[layer.spatial as usize][layer.temporal as usize] > 0 {
        return brs[layer.spatial as usize][layer.temporal as usize];
    }

    fallback
}

fn get_distance_to_desired(
    muted: bool,
    pub_muted: bool,
    max_seen_layer: buffer::VideoLayer,
    available_layers: &[i32],
    brs: Bitrates,
    target_layer: buffer::VideoLayer,
    max_layer: buffer::VideoLayer,
) -> f64 {
    if muted || pub_muted || !max_seen_layer.is_valid() || !max_layer.is_valid() {
        return 0.0;
    }

    let mut adjusted_max_layer = max_layer;

    let mut max_available_spatial = buffer::INVALID_LAYER_SPATIAL;
    let mut max_available_temporal = buffer::INVALID_LAYER_TEMPORAL;

    // max available spatial is min(subscribedMax, publishedMax, availableMax)
    // subscribedMax = subscriber requested max spatial layer
    // publishedMax = max spatial layer ever published
    // availableMax = based on bit rate measurement, available max spatial layer
    'done: for s in (0..brs.len() as i32).rev() {
        for t in (0..brs[0].len() as i32).rev() {
            if brs[s as usize][t as usize] != 0 {
                max_available_spatial = s;
                break 'done;
            }
        }
    }

    // before bit rate measurement is available, stream tracker could declare layer seen, account for that
    for &layer in available_layers {
        if layer > max_available_spatial {
            max_available_spatial = layer;
            max_available_temporal = max_seen_layer.temporal; // till bit rate measurement is available, assume max seen as temporal
        }
    }

    if max_available_spatial < adjusted_max_layer.spatial {
        adjusted_max_layer.spatial = max_available_spatial;
    }

    if max_seen_layer.spatial < adjusted_max_layer.spatial {
        adjusted_max_layer.spatial = max_seen_layer.spatial;
    }

    // max available temporal is min(subscribedMax, temporalLayerSeenMax, availableMax)
    // subscribedMax = subscriber requested max temporal layer
    // temporalLayerSeenMax = max temporal layer ever published/seen
    // availableMax = based on bit rate measurement, available max temporal in the adjusted max spatial layer
    if adjusted_max_layer.spatial != buffer::INVALID_LAYER_SPATIAL {
        for t in (0..brs[0].len() as i32).rev() {
            if brs[adjusted_max_layer.spatial as usize][t as usize] != 0 {
                max_available_temporal = t;
                break;
            }
        }
    }
    if max_available_temporal < adjusted_max_layer.temporal {
        adjusted_max_layer.temporal = max_available_temporal;
    }

    if max_seen_layer.temporal < adjusted_max_layer.temporal {
        adjusted_max_layer.temporal = max_seen_layer.temporal;
    }

    if !adjusted_max_layer.is_valid() {
        adjusted_max_layer = buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        };
    }

    // adjust target layers if they are invalid, i. e. not streaming
    let mut adjusted_target_layer = target_layer;
    if !target_layer.is_valid() {
        adjusted_target_layer = buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        };
    }

    let mut distance = ((adjusted_max_layer.spatial - adjusted_target_layer.spatial)
        * (max_seen_layer.temporal + 1))
        + (adjusted_max_layer.temporal - adjusted_target_layer.temporal);
    if !target_layer.is_valid() {
        distance += max_seen_layer.temporal + 1;
    }

    distance as f64 / (max_seen_layer.temporal + 1) as f64
}

#[cfg(test)]
#[path = "forwarder_test.rs"]
mod forwarder_test;
