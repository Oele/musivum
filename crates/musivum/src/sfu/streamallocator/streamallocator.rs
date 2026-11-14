// Ported from LiveKit, pkg/sfu/streamallocator/streamallocator.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `NewStreamAllocator` is `StreamAllocator::new` and gives an `Arc`; the allocator keeps
//   a `Weak` of itself. Its public methods are inherent ones, in the original's order; the
//   four listener traits it implements (`DownTrackStreamAllocatorListener`,
//   `bwe::BweListener`, `pacer::PacerProbeObserverListener`, `ccutils::ProberListener`)
//   pass on to them at the end of the file.
// - Who holds whom. The allocator holds the estimator, the pacer and its prober (`Arc`),
//   and its tracks, which hold their down tracks (`Arc`), until `remove_track`. Nothing
//   holds the allocator but its owner: a down track is given a `Weak` of it; the
//   estimator, the pacer and the prober, whose traits take a listener as an `Arc`, are
//   given a `Listener`, which holds the allocator weakly and passes the calls on; an event
//   in the queue (`Event` embeds the `*StreamAllocator` upstream) and the ping worker hold
//   it weakly too. An allocator that is dropped without `stop` therefore goes: its queue
//   ends its worker (see utils/opsqueue.rs) and the ping worker ends at its next tick.
// - Events are as upstream: the same signals, one handler each, run one at a time by the
//   worker of a `TypedOpsQueue`, which is a thread (see utils/opsqueue.rs); the ping
//   goroutine is a task. `stop` waits for that worker by blocking, and as upstream waits
//   for ever on an allocator that was not started.
// - What `videoTracksMu` guards is `VideoTracks`. The fields that upstream only the events
//   worker writes, without a lock (`allowPause`, the two channel capacities, `state`, the
//   three of the active probe, `lastRTTTime`), are atomics, or have a lock of their own
//   that is held for the read or the write only: as upstream, a handler holds no lock
//   while it calls the estimator, the pacer, a down track or `on_stream_state_change`.
//   That function, set without a lock upstream, has a lock of its own too.
// - The shadow of the tracks is a shared slice (`Arc<[..]>`), which `get_video_tracks`
//   hands out as upstream hands out the slice.
// - Not carried over: pion's bandwidth estimator (`sendSideBWEInterceptor
//   cc.BandwidthEstimator`, behind a configuration switch that is off by default) with
//   `SetSendSideBWEInterceptor`, `onTargetBitrateChange` and the hand-over of the
//   transport-cc feedback to it in `handleSignalFeedback`.
// - `ProbeMode`, a string type, is an enum whose `Display` and `FromStr` have the texts.
//   The configuration therefore has no zero value (`Default`): the zero probe mode of the
//   original is a text that is neither mode, with which nothing probes. The `yaml` tags of
//   the configuration are doc comments on its fields, as nothing reads a configuration
//   file yet.
// - `RTTGetter` is an `Option` of a boxed function. The function given to
//   `on_stream_state_change` is handed the update by reference and returns a
//   `StreamStateChangeError`, any error: the allocator only logs it.
// - `OnREMB` and `OnTransportCCFeedback` are handed the RTCP packet by reference (see
//   sfu/interfaces.rs): the feedback is copied into its event. `OnSendProbe` and
//   `probeBytesToSend` count bytes in `i64` (Go's `int`), as the prober does.
// - `streamAllocatorState` and `streamAllocatorSignal` are enums and their `String()` is
//   `Display`; `Event`'s is too.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};
use tokio::time::MissedTickBehavior;

use crate::go::atomic;
use crate::go::time::{self, Duration};
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::bwe;
use crate::sfu::ccutils;
use crate::sfu::pacer;
use crate::sfu::{DownTrack, DownTrackStreamAllocatorListener, VideoAllocation, VideoPauseReason};
use crate::utils;
use crate::webrtc;
use crate::webrtc::rtcp::payload_feedbacks::receiver_estimated_maximum_bitrate::ReceiverEstimatedMaximumBitrate;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;
use crate::{debugw, errorw, infow, warnw};

use super::{
    MaxDistanceSorter, MinDistanceSorter, StreamState, StreamStateUpdate, Track, TrackSorter,
};

const C_CHANNEL_CAPACITY_INFINITY: i64 = 100 * 1000 * 1000; // 100 Mbps

const C_PRIORITY_MIN: u8 = 1;
const C_PRIORITY_MAX: u8 = 255;
pub(super) const C_PRIORITY_DEFAULT_SCREENSHARE: u8 = C_PRIORITY_MAX;
pub(super) const C_PRIORITY_DEFAULT_VIDEO: u8 = C_PRIORITY_MIN;

const C_FLAG_ALLOW_OVERSHOOT_WHILE_OPTIMAL: bool = true;
const C_FLAG_ALLOW_OVERSHOOT_WHILE_DEFICIENT: bool = false;
const C_FLAG_ALLOW_OVERSHOOT_EXEMPT_TRACK_WHILE_DEFICIENT: bool = true;
const C_FLAG_ALLOW_OVERSHOOT_IN_PROBE: bool = true;
const C_FLAG_ALLOW_OVERSHOOT_IN_CATCHUP: bool = false;
const C_FLAG_ALLOW_OVERSHOOT_IN_BOOST: bool = true;

const C_RTT_PULL_INTERVAL: Duration = Duration::from_secs(30);

const C_PING_LONG: Duration = Duration(C_RTT_PULL_INTERVAL.0 / 2);
const C_PING_SHORT: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum StreamAllocatorState {
    #[default]
    Stable,
    Deficient,
}

impl fmt::Display for StreamAllocatorState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            StreamAllocatorState::Stable => "STABLE",
            StreamAllocatorState::Deficient => "DEFICIENT",
        })
    }
}

// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum StreamAllocatorSignal {
    #[default]
    AllocateTrack,
    AllocateAllTracks,
    AdjustState,
    Estimate,
    Feedback,
    PeriodicPing,
    ProbeClusterSwitch,
    SendProbe,
    PacerProbeObserverClusterComplete,
    Resume,
    SetAllowPause,
    SetChannelCapacity,
    CongestionStateChange,
}

impl fmt::Display for StreamAllocatorSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            StreamAllocatorSignal::AllocateTrack => "ALLOCATE_TRACK",
            StreamAllocatorSignal::AllocateAllTracks => "ALLOCATE_ALL_TRACKS",
            StreamAllocatorSignal::AdjustState => "ADJUST_STATE",
            StreamAllocatorSignal::Estimate => "ESTIMATE",
            StreamAllocatorSignal::Feedback => "FEEDBACK",
            StreamAllocatorSignal::PeriodicPing => "PERIODIC_PING",
            StreamAllocatorSignal::ProbeClusterSwitch => "PROBE_CLUSTER_SWITCH",
            StreamAllocatorSignal::SendProbe => "SEND_PROBE",
            StreamAllocatorSignal::PacerProbeObserverClusterComplete => {
                "PACER_PROBE_OBSERVER_CLUSTER_COMPLETE"
            }
            StreamAllocatorSignal::Resume => "RESUME",
            StreamAllocatorSignal::SetAllowPause => "SET_ALLOW_PAUSE",
            StreamAllocatorSignal::SetChannelCapacity => "SET_CHANNEL_CAPACITY",
            StreamAllocatorSignal::CongestionStateChange => "CONGESTION_STATE_CHANGE",
        })
    }
}

// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct Event {
    stream_allocator: Weak<StreamAllocator>,
    signal: StreamAllocatorSignal,

    // only one of these fields will be set based on event type
    track_id: livekit::TrackId,
    allow_pause: bool,
    channel_capacity: i64,
    twcc_feedback: Option<TransportLayerCc>,
    congestion_state_change_data: CongestionStateChangeData,
    probe_bytes_to_send: i64,
    probe_cluster_id: ccutils::ProbeClusterId,
    probe_cluster_info: ccutils::ProbeClusterInfo,
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self.signal {
            StreamAllocatorSignal::AllocateTrack | StreamAllocatorSignal::Resume => {
                format!("trackID: {}", self.track_id)
            }
            StreamAllocatorSignal::Estimate | StreamAllocatorSignal::SetChannelCapacity => {
                format!("channelCapacity: {}", self.channel_capacity)
            }
            StreamAllocatorSignal::Feedback => {
                format!("twccFeedback: {:?}", self.twcc_feedback)
            }
            StreamAllocatorSignal::ProbeClusterSwitch => {
                format!("probeClusterInfo: {:?}", self.probe_cluster_info)
            }
            StreamAllocatorSignal::SendProbe => {
                format!("probeBytesToSend: {}", self.probe_bytes_to_send)
            }
            StreamAllocatorSignal::PacerProbeObserverClusterComplete => {
                format!("probeClusterId: {}", self.probe_cluster_id)
            }
            StreamAllocatorSignal::SetAllowPause => {
                format!("allowPause: {}", self.allow_pause)
            }
            StreamAllocatorSignal::CongestionStateChange => {
                format!(
                    "congestStatecChangeData: {:?}",
                    self.congestion_state_change_data
                )
            }
            _ => String::new(),
        };

        if value.is_empty() {
            return write!(f, "StreamAllocator:Event{{signal: {}}}", self.signal);
        }

        write!(
            f,
            "StreamAllocator:Event{{signal: {}, {}}}",
            self.signal, value
        )
    }
}

// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProbeMode {
    Padding,
    Media,
}

impl ProbeMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProbeMode::Padding => "padding",
            ProbeMode::Media => "media",
        }
    }
}

impl fmt::Display for ProbeMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// Go's `ProbeMode(s)` of a string from the configuration. Any other text, the empty one
// too, is an error here, where the original has a value that equals neither constant.
impl FromStr for ProbeMode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "padding" => Ok(ProbeMode::Padding),
            "media" => Ok(ProbeMode::Media),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamAllocatorConfig {
    /// `min_channel_capacity` in YAML.
    pub min_channel_capacity: i64,
    /// `disable_etimation_unmanaged_tracks` in YAML (the spelling is the original's).
    pub disable_estimation_unmanaged_tracks: bool,

    /// `probe_mode` in YAML.
    pub probe_mode: ProbeMode,
    /// `probe_overage_pct` in YAML.
    pub probe_overage_pct: i64,
    /// `probe_min_bps` in YAML.
    pub probe_min_bps: i64,

    /// `paused_min_wait` in YAML.
    pub paused_min_wait: Duration,
}

pub const DEFAULT_STREAM_ALLOCATOR_CONFIG: StreamAllocatorConfig = StreamAllocatorConfig {
    min_channel_capacity: 0,
    disable_estimation_unmanaged_tracks: false,

    probe_mode: ProbeMode::Padding,
    probe_overage_pct: 120,
    probe_min_bps: 200_000,

    paused_min_wait: Duration::from_secs(5),
};

// ---------------------------------------------------------------------------

pub struct StreamAllocatorParams {
    pub config: StreamAllocatorConfig,
    pub bwe: Arc<dyn bwe::Bwe>,
    pub pacer: Arc<dyn pacer::Pacer>,
    pub rtt_getter: Option<Box<dyn Fn() -> (f64, bool) + Send + Sync>>,
    pub logger: Logger,
}

/// Go's `error`, as the function given to `on_stream_state_change` returns it.
pub type StreamStateChangeError = Box<dyn std::error::Error + Send + Sync>;

type OnStreamStateChange =
    Arc<dyn Fn(&StreamStateUpdate) -> Result<(), StreamStateChangeError> + Send + Sync>;

pub struct StreamAllocator {
    params: StreamAllocatorParams,

    on_stream_state_change: RwLock<Option<OnStreamStateChange>>,

    enabled: bool,
    allow_pause: atomic::Bool,

    committed_channel_capacity: atomic::Int64,
    overridden_channel_capacity: atomic::Int64,

    prober: Arc<ccutils::Prober>,

    video_tracks_mu: RwLock<VideoTracks>,

    state: Mutex<StreamAllocatorState>,

    active_probe_cluster_id: Mutex<ccutils::ProbeClusterId>,
    active_probe_goal_reached: atomic::Bool,
    active_probe_congesting: atomic::Bool,

    events_queue: utils::TypedOpsQueue<Event>,

    last_rtt_time: atomic::Time,

    ping_generation: atomic::Uint32,

    is_stopped: atomic::Bool,

    this: Weak<StreamAllocator>,
}

// What `videoTracksMu` guards.
struct VideoTracks {
    video_tracks: HashMap<livekit::TrackId, Arc<Track>>,
    video_tracks_shadow: Arc<[Arc<Track>]>,
    is_allocate_all_pending: bool,
    remb_tracking_ssrc: u32,
}

// What the estimator, the pacer and the prober are given as their listener: the allocator,
// held weakly (see the head of this file).
struct Listener {
    stream_allocator: Weak<StreamAllocator>,
}

#[derive(Clone, Debug, Default)]
pub struct AddTrackParams {
    pub source: livekit::TrackSource,
    pub priority: u8,
    pub is_multi_layered: bool,
    pub publisher_id: livekit::ParticipantId,
}

// called when congestion state changes (send side bandwidth estimation)
#[derive(Clone, Copy, Debug, Default)]
struct CongestionStateChangeData {
    from_state: bwe::CongestionState,
    to_state: bwe::CongestionState,
    estimated_available_channel_capacity: i64,
}

// `s.state` and `s.activeProbeClusterId` of the original where it reads them: the lock of
// the field is let go of before anything else is called.
impl StreamAllocator {
    fn state(&self) -> StreamAllocatorState {
        *self.state.lock()
    }

    fn active_probe_cluster_id(&self) -> ccutils::ProbeClusterId {
        *self.active_probe_cluster_id.lock()
    }
}

impl StreamAllocator {
    pub fn new(
        params: StreamAllocatorParams,
        enabled: bool,
        allow_pause: bool,
    ) -> Arc<StreamAllocator> {
        let s = Arc::new_cyclic(|this: &Weak<StreamAllocator>| StreamAllocator {
            on_stream_state_change: RwLock::new(None),
            enabled,
            allow_pause: atomic::Bool::new(allow_pause),
            committed_channel_capacity: atomic::Int64::new(0),
            overridden_channel_capacity: atomic::Int64::new(0),
            prober: ccutils::Prober::new(ccutils::ProberParams {
                listener: Some(Arc::new(Listener {
                    stream_allocator: this.clone(),
                })),
                logger: params.logger.clone(),
            }),
            video_tracks_mu: RwLock::new(VideoTracks {
                video_tracks: HashMap::new(),
                video_tracks_shadow: Arc::from(Vec::new()),
                is_allocate_all_pending: false,
                remb_tracking_ssrc: 0,
            }),
            state: Mutex::new(StreamAllocatorState::Stable),
            active_probe_cluster_id: Mutex::new(ccutils::PROBE_CLUSTER_ID_INVALID),
            active_probe_goal_reached: atomic::Bool::new(false),
            active_probe_congesting: atomic::Bool::new(false),
            events_queue: utils::TypedOpsQueue::new(utils::OpsQueueParams {
                name: "stream-allocator".to_string(),
                min_size: 64,
                logger: params.logger.clone(),
                ..Default::default()
            }),
            last_rtt_time: atomic::Time::new(time::now().add(-C_RTT_PULL_INTERVAL)),
            ping_generation: atomic::Uint32::new(0),
            is_stopped: atomic::Bool::new(false),
            this: this.clone(),
            params,
        });

        s.params.bwe.set_bwe_listener(Some(Arc::new(Listener {
            stream_allocator: s.this.clone(),
        })));
        s.params
            .pacer
            .set_pacer_probe_observer_listener(Some(Arc::new(Listener {
                stream_allocator: s.this.clone(),
            })));

        s
    }

    pub fn start(&self) {
        self.events_queue.start();
        time::spawn(Self::ping(
            self.this.clone(),
            self.ping_generation.inc(),
            C_PING_LONG,
        ));
    }

    pub fn stop(&self) {
        if self.is_stopped.swap(true) {
            return;
        }

        // wait for eventsQueue to be done
        self.events_queue.stop().wait();

        self.maybe_stop_probe();
    }

    pub fn on_stream_state_change(
        &self,
        f: impl Fn(&StreamStateUpdate) -> Result<(), StreamStateChangeError> + Send + Sync + 'static,
    ) {
        *self.on_stream_state_change.write() = Some(Arc::new(f));
    }

    pub fn add_track(&self, down_track: Arc<DownTrack>, params: AddTrackParams) {
        if down_track.kind() != webrtc::RtpCodecType::Video {
            return;
        }

        let track = Track::new(
            down_track.clone(),
            params.source,
            params.is_multi_layered,
            params.publisher_id,
            self.params.logger.clone(),
        );
        track.set_priority(params.priority);

        let track_id = livekit::TrackId::from(down_track.id());
        let old_track = {
            let mut vt = self.video_tracks_mu.write();
            let old_track = vt.video_tracks.insert(track_id, track);
            vt.shadow_video_tracks_locked();
            old_track
        };

        if let Some(old_track) = old_track {
            old_track.down_track().set_stream_allocator_listener(None);
        }

        let listener: Weak<dyn DownTrackStreamAllocatorListener> = self.this.clone();
        down_track.set_stream_allocator_listener(Some(listener));
        down_track.set_probe_cluster_id(self.active_probe_cluster_id());

        self.maybe_post_event_allocate_track(&down_track);
    }

    pub fn remove_track(&self, down_track: &DownTrack) {
        {
            let mut vt = self.video_tracks_mu.write();
            let track_id = livekit::TrackId::from(down_track.id());
            if let Some(existing) = vt.video_tracks.get(&track_id)
                && std::ptr::eq(Arc::as_ptr(existing.down_track()), down_track)
            {
                vt.video_tracks.remove(&track_id);
                vt.shadow_video_tracks_locked();
            }
        }

        // STREAM-ALLOCATOR-TODO: use any saved bandwidth to re-distribute
        self.post_event(Event {
            signal: StreamAllocatorSignal::AdjustState,
            ..Default::default()
        });
    }

    pub fn set_track_priority(&self, down_track: &DownTrack, priority: u8) {
        let mut vt = self.video_tracks_mu.write();
        if let Some(track) = vt
            .video_tracks
            .get(&livekit::TrackId::from(down_track.id()))
        {
            let changed = track.set_priority(priority);
            if changed && !vt.is_allocate_all_pending {
                // do a full allocation on a track priority change to keep it simple
                vt.is_allocate_all_pending = true;
                self.post_event(Event {
                    signal: StreamAllocatorSignal::AllocateAllTracks,
                    ..Default::default()
                });
            }
        }
    }

    pub fn set_allow_pause(&self, allow_pause: bool) {
        self.post_event(Event {
            signal: StreamAllocatorSignal::SetAllowPause,
            allow_pause,
            ..Default::default()
        });
    }

    pub fn set_channel_capacity(&self, channel_capacity: i64) {
        self.post_event(Event {
            signal: StreamAllocatorSignal::SetChannelCapacity,
            channel_capacity,
            ..Default::default()
        });
    }

    // called when a new REMB is received (receive side bandwidth estimation)
    pub fn on_remb(&self, down_track: &DownTrack, remb: &ReceiverEstimatedMaximumBitrate) {
        //
        // Channel capacity is estimated at a peer connection level. All down tracks
        // in the peer connection will end up calling this for a REMB report with
        // the same estimated channel capacity. Use a tracking SSRC to lock onto to
        // one report. As SSRCs can be dropped over time, update tracking SSRC as needed
        //
        // A couple of things to keep in mind
        //   - REMB reports could be sent gratuitously as a way of providing
        //     periodic feedback, i.e. even if the estimated capacity does not
        //     change, there could be REMB packets on the wire. Those gratuitous
        //     REMBs should not trigger anything bad.
        //   - As each down track will issue this callback for the same REMB packet
        //     from the wire, theoretically it is possible that one down track's
        //     callback from previous REMB comes after another down track's callback
        //     from the new REMB. REMBs could fire very quickly especially when
        //     the network is entering congestion.
        // STREAM-ALLOCATOR-TODO-START
        // Need to check if the same SSRC reports can somehow race, i.e. does pion send
        // RTCP dispatch for same SSRC on different threads? If not, the tracking SSRC
        // should prevent racing
        // STREAM-ALLOCATOR-TODO-END
        //

        // if there are no video tracks, ignore any straggler REMB
        let mut vt = self.video_tracks_mu.write();
        if vt.video_tracks.is_empty() {
            return;
        }

        let mut down_track_ssrc = 0u32;
        let mut down_track_ssrc_rtx = 0u32;
        let track = vt
            .video_tracks
            .get(&livekit::TrackId::from(down_track.id()));
        if let Some(track) = track {
            down_track_ssrc = track.down_track().ssrc();
            down_track_ssrc_rtx = track.down_track().ssrc_rtx();
        }

        let mut found = remb.ssrcs.contains(&vt.remb_tracking_ssrc);
        if !found {
            if remb.ssrcs.is_empty() {
                warnw!(
                    self.params.logger,
                    "stream allocator: no SSRC to track REMB",
                    nil
                );
                return;
            }

            // try to lock to track which is sending this update
            for &ssrc in &remb.ssrcs {
                if ssrc == 0 {
                    continue;
                }

                if ssrc == down_track_ssrc {
                    vt.remb_tracking_ssrc = down_track_ssrc;
                    found = true;
                    break;
                }
                if ssrc == down_track_ssrc_rtx {
                    vt.remb_tracking_ssrc = down_track_ssrc_rtx;
                    found = true;
                    break;
                }
            }

            if !found {
                vt.remb_tracking_ssrc = remb.ssrcs[0];
            }
        }

        if vt.remb_tracking_ssrc == 0
            || (vt.remb_tracking_ssrc != down_track_ssrc
                && vt.remb_tracking_ssrc != down_track_ssrc_rtx)
        {
            return;
        }
        drop(vt);

        self.post_event(Event {
            signal: StreamAllocatorSignal::Estimate,
            channel_capacity: remb.bitrate as i64,
            ..Default::default()
        });
    }

    // called when a new transport-cc feedback is received
    pub fn on_transport_cc_feedback(&self, _down_track: &DownTrack, fb: &TransportLayerCc) {
        self.post_event(Event {
            signal: StreamAllocatorSignal::Feedback,
            twcc_feedback: Some(fb.clone()),
            ..Default::default()
        });
    }

    // BWEListener implementation
    pub fn on_congestion_state_change(
        &self,
        from_state: bwe::CongestionState,
        to_state: bwe::CongestionState,
        estimated_available_channel_capacity: i64,
    ) {
        self.post_event(Event {
            signal: StreamAllocatorSignal::CongestionStateChange,
            congestion_state_change_data: CongestionStateChangeData {
                from_state,
                to_state,
                estimated_available_channel_capacity,
            },
            ..Default::default()
        });
    }

    // called when feeding track's layer availability changes
    pub fn on_available_layers_changed(&self, down_track: &DownTrack) {
        self.maybe_post_event_allocate_track(down_track);
    }

    // called when feeding track's bitrate measurement of any layer is available
    pub fn on_bitrate_availability_changed(&self, down_track: &DownTrack) {
        self.maybe_post_event_allocate_track(down_track);
    }

    // called when feeding track's max published spatial layer changes
    pub fn on_max_published_spatial_changed(&self, down_track: &DownTrack) {
        self.maybe_post_event_allocate_track(down_track);
    }

    // called when feeding track's max published temporal layer changes
    pub fn on_max_published_temporal_changed(&self, down_track: &DownTrack) {
        self.maybe_post_event_allocate_track(down_track);
    }

    // called when subscription settings changes (muting/unmuting of track)
    pub fn on_subscription_changed(&self, down_track: &DownTrack) {
        self.maybe_post_event_allocate_track(down_track);
    }

    // called when subscribed layer changes (limiting max layer)
    pub fn on_subscribed_layer_changed(&self, down_track: &DownTrack, layer: buffer::VideoLayer) {
        let mut should_post = false;
        {
            let vt = self.video_tracks_mu.write();
            if let Some(track) = vt
                .video_tracks
                .get(&livekit::TrackId::from(down_track.id()))
                && track.set_max_layer(layer)
                && track.set_dirty(true)
            {
                should_post = true;
            }
        }

        if should_post {
            self.post_event(Event {
                signal: StreamAllocatorSignal::AllocateTrack,
                track_id: livekit::TrackId::from(down_track.id()),
                ..Default::default()
            });
        }
    }

    // called when forwarder resumes a track
    pub fn on_resume(&self, down_track: &DownTrack) {
        self.post_event(Event {
            signal: StreamAllocatorSignal::Resume,
            track_id: livekit::TrackId::from(down_track.id()),
            ..Default::default()
        });
    }

    // called when probe cluster changes
    pub fn on_probe_cluster_switch(&self, pci: ccutils::ProbeClusterInfo) {
        self.post_event(Event {
            signal: StreamAllocatorSignal::ProbeClusterSwitch,
            probe_cluster_info: pci,
            ..Default::default()
        });
    }

    // called when prober wants to send packet(s)
    pub fn on_send_probe(&self, bytes_to_send: i64) {
        self.post_event(Event {
            signal: StreamAllocatorSignal::SendProbe,
            probe_bytes_to_send: bytes_to_send,
            ..Default::default()
        });
    }

    // called when pacer probe observer observes a cluster completion
    pub fn on_pacer_probe_observer_cluster_complete(
        &self,
        probe_cluster_id: ccutils::ProbeClusterId,
    ) {
        self.post_event(Event {
            signal: StreamAllocatorSignal::PacerProbeObserverClusterComplete,
            probe_cluster_id,
            ..Default::default()
        });
    }

    // called to check if track should participate in BWE
    pub fn is_bwe_enabled(&self, down_track: &DownTrack) -> bool {
        if !self.params.config.disable_estimation_unmanaged_tracks {
            return true;
        }

        let vt = self.video_tracks_mu.write();

        if let Some(track) = vt
            .video_tracks
            .get(&livekit::TrackId::from(down_track.id()))
        {
            return track.is_managed();
        }

        true
    }

    pub fn bwe_type(&self) -> bwe::BweType {
        self.params.bwe.r#type()
    }

    // called to check if track subscription mute can be applied
    pub fn is_subscribe_mutable(&self, down_track: &DownTrack) -> bool {
        let vt = self.video_tracks_mu.write();

        if let Some(track) = vt
            .video_tracks
            .get(&livekit::TrackId::from(down_track.id()))
        {
            return track.is_subscribe_mutable();
        }

        true
    }

    fn maybe_post_event_allocate_track(&self, down_track: &DownTrack) {
        let mut should_post = false;
        {
            let vt = self.video_tracks_mu.write();
            if let Some(track) = vt
                .video_tracks
                .get(&livekit::TrackId::from(down_track.id()))
            {
                should_post = track.set_dirty(true);
            }
        }

        if should_post {
            self.post_event(Event {
                signal: StreamAllocatorSignal::AllocateTrack,
                track_id: livekit::TrackId::from(down_track.id()),
                ..Default::default()
            });
        }
    }

    async fn ping(this: Weak<StreamAllocator>, ping_generation: u32, interval: Duration) {
        let mut ticker = new_ticker(interval);

        loop {
            ticker.tick().await;
            let Some(s) = this.upgrade() else {
                return;
            };
            if s.is_stopped.load() || (ping_generation != s.ping_generation.load()) {
                return;
            }

            s.post_event(Event {
                signal: StreamAllocatorSignal::PeriodicPing,
                ..Default::default()
            });
        }
    }

    fn post_event(&self, mut event: Event) {
        event.stream_allocator = self.this.clone();
        self.events_queue.enqueue(
            |event: Event| {
                let Some(s) = event.stream_allocator.upgrade() else {
                    return;
                };
                match event.signal {
                    StreamAllocatorSignal::AllocateTrack => s.handle_signal_allocate_track(event),
                    StreamAllocatorSignal::AllocateAllTracks => {
                        s.handle_signal_allocate_all_tracks(event)
                    }
                    StreamAllocatorSignal::AdjustState => s.handle_signal_adjust_state(event),
                    StreamAllocatorSignal::Estimate => s.handle_signal_estimate(event),
                    StreamAllocatorSignal::Feedback => s.handle_signal_feedback(event),
                    StreamAllocatorSignal::PeriodicPing => s.handle_signal_periodic_ping(event),
                    StreamAllocatorSignal::ProbeClusterSwitch => {
                        s.handle_signal_probe_cluster_switch(event)
                    }
                    StreamAllocatorSignal::SendProbe => s.handle_signal_send_probe(event),
                    StreamAllocatorSignal::PacerProbeObserverClusterComplete => {
                        s.handle_signal_pacer_probe_observer_cluster_complete(event)
                    }
                    StreamAllocatorSignal::Resume => s.handle_signal_resume(event),
                    StreamAllocatorSignal::SetAllowPause => s.handle_signal_set_allow_pause(event),
                    StreamAllocatorSignal::SetChannelCapacity => {
                        s.handle_signal_set_channel_capacity(event)
                    }
                    StreamAllocatorSignal::CongestionStateChange => {
                        s.handle_signal_congestion_state_change(event)
                    }
                }
            },
            event,
        );
    }

    fn handle_signal_allocate_track(&self, event: Event) {
        let track = {
            let vt = self.video_tracks_mu.write();
            let track = vt.video_tracks.get(&event.track_id).cloned();
            if let Some(track) = &track {
                track.set_dirty(false);
            }
            track
        };

        if let Some(track) = track {
            self.allocate_track(&track);
        }
    }

    fn handle_signal_allocate_all_tracks(&self, _event: Event) {
        {
            let mut vt = self.video_tracks_mu.write();
            vt.is_allocate_all_pending = false;
        }

        if self.state() == StreamAllocatorState::Deficient {
            self.allocate_all_tracks();
        }
    }

    fn handle_signal_adjust_state(&self, _event: Event) {
        self.adjust_state();
    }

    fn handle_signal_estimate(&self, event: Event) {
        // always update NACKs
        let (packet_delta, repeated_nack_delta) = self.get_nack_delta();

        self.params.bwe.handle_remb(
            event.channel_capacity,
            self.get_expected_bandwidth_usage(),
            packet_delta,
            repeated_nack_delta,
        );
    }

    fn handle_signal_feedback(&self, event: Event) {
        if let Some(fb) = &event.twcc_feedback {
            self.params.bwe.handle_twcc_feedback(fb);
        }
    }

    fn handle_signal_periodic_ping(&self, _event: Event) {
        // if pause is allowed, there may be no packets sent and BWE could be in congested state,
        // reset BWE if that persists for a while
        if self.allow_pause.load()
            && self.state() == StreamAllocatorState::Deficient
            && self.params.bwe.congestion_state() != bwe::CongestionState::None
            && self.params.pacer.time_since_last_sent_packet() > self.params.config.paused_min_wait
        {
            infow!(
                self.params.logger,
                "stream allocator: resetting bwe to enable probing"
            );
            self.maybe_stop_probe();
            self.params.bwe.reset();

            // as BWE is reset, there is no finalizing for active cluster, so reset active cluster id
            *self.active_probe_cluster_id.lock() = ccutils::PROBE_CLUSTER_ID_INVALID;
        }

        if self.active_probe_cluster_id() != ccutils::PROBE_CLUSTER_ID_INVALID {
            if !self.active_probe_congesting.load()
                && !self.active_probe_goal_reached.load()
                && self.params.bwe.probe_cluster_is_goal_reached()
            {
                debugw!(
                    self.params.logger,
                    "stream allocator: probe goal reached",
                    "activeProbeClusterId" => self.active_probe_cluster_id(),
                );
                self.active_probe_goal_reached.store(true);
                self.maybe_stop_probe();
            }

            // finalize any probe that may have finished/aborted
            let (probe_signal, channel_capacity, is_finalized) =
                self.params.bwe.probe_cluster_finalize();
            if is_finalized {
                debugw!(
                    self.params.logger,
                    "stream allocator: probe result",
                    "activeProbeClusterId" => self.active_probe_cluster_id(),
                    "probeSignal" => probe_signal,
                    "channelCapacity" => channel_capacity,
                );

                *self.active_probe_cluster_id.lock() = ccutils::PROBE_CLUSTER_ID_INVALID;

                if probe_signal != ccutils::ProbeSignal::Congesting {
                    if channel_capacity > self.committed_channel_capacity.load() {
                        self.committed_channel_capacity.store(channel_capacity);
                    }

                    self.maybe_boost_deficient_tracks();
                }
            }
        }

        // probe if necessary and timing is right
        if self.state() == StreamAllocatorState::Deficient {
            self.maybe_probe();
        }

        if time::since(self.last_rtt_time.load()) > C_RTT_PULL_INTERVAL {
            self.last_rtt_time.store(time::now());

            if let Some(rtt_getter) = &self.params.rtt_getter {
                let (rtt, ok) = rtt_getter();
                if ok {
                    self.params.bwe.update_rtt(rtt);
                }
            }
        }
    }

    fn handle_signal_probe_cluster_switch(&self, event: Event) {
        let pci = event.probe_cluster_info;
        *self.active_probe_cluster_id.lock() = pci.id;
        self.active_probe_goal_reached.store(false);
        self.active_probe_congesting.store(false);

        self.params.bwe.probe_cluster_starting(pci);

        self.params.pacer.start_probe_cluster(pci);

        for t in self.get_video_tracks().iter() {
            t.down_track().set_probe_cluster_id(pci.id);
        }
    }

    fn handle_signal_send_probe(&self, event: Event) {
        let mut bytes_to_send = event.probe_bytes_to_send;
        if bytes_to_send <= 0 {
            return;
        }

        let mut bytes_sent = 0i64;
        for track in self.get_video_tracks().iter() {
            let sent = track.write_probe_packets(bytes_to_send);
            bytes_sent += sent;
            bytes_to_send -= sent;
            if bytes_to_send <= 0 {
                break;
            }
        }

        self.prober.probes_sent(bytes_sent);
    }

    fn handle_signal_pacer_probe_observer_cluster_complete(&self, event: Event) {
        let pci = self.params.pacer.end_probe_cluster(event.probe_cluster_id);

        for t in self.get_video_tracks().iter() {
            t.down_track()
                .swap_probe_cluster_id(pci.id, ccutils::PROBE_CLUSTER_ID_INVALID);
        }

        self.params.bwe.probe_cluster_done(pci);
        self.prober.cluster_done(pci);
    }

    fn handle_signal_resume(&self, event: Event) {
        let (track, updated) = {
            let vt = self.video_tracks_mu.write();
            let track = vt.video_tracks.get(&event.track_id).cloned();
            let updated = track
                .as_ref()
                .is_some_and(|track| track.set_stream_state(StreamState::Active));
            (track, updated)
        };

        if updated && let Some(track) = track {
            let mut update = StreamStateUpdate::new();
            update.handle_streaming_change(&track, StreamState::Active);
            self.maybe_send_update(update);
        }
    }

    fn handle_signal_set_allow_pause(&self, event: Event) {
        self.allow_pause.store(event.allow_pause);
    }

    fn handle_signal_set_channel_capacity(&self, event: Event) {
        self.overridden_channel_capacity
            .store(event.channel_capacity);
        if self.overridden_channel_capacity.load() > 0 {
            infow!(
                self.params.logger,
                "allocating on override channel capacity",
                "override" => self.overridden_channel_capacity.load(),
            );
            self.allocate_all_tracks();
        } else {
            infow!(self.params.logger, "clearing override channel capacity");
        }
    }

    fn handle_signal_congestion_state_change(&self, event: Event) {
        let cscd = event.congestion_state_change_data;
        if cscd.to_state != bwe::CongestionState::None {
            // end/abort any running probe if channel is not clear
            self.maybe_stop_probe();
        }

        // some tracks may have been held at sub-optimal allocation
        // during early warning hold (if there was one)
        if is_holdable_congestion_state(cscd.from_state)
            && cscd.to_state == bwe::CongestionState::None
            && self.state() == StreamAllocatorState::Stable
        {
            let mut update = StreamStateUpdate::new();
            for track in self.get_video_tracks().iter() {
                let allocation =
                    track.allocate_optimal(C_FLAG_ALLOW_OVERSHOOT_WHILE_OPTIMAL, false);
                update_stream_state_change(track, allocation, &mut update);
            }
            self.maybe_send_update(update);
        }

        if cscd.to_state == bwe::CongestionState::Congested {
            if self.active_probe_cluster_id() != ccutils::PROBE_CLUSTER_ID_INVALID {
                if !self.active_probe_congesting.load() {
                    self.active_probe_congesting.store(true);
                    debugw!(
                        self.params.logger,
                        "stream allocator: channel congestion detected, not updating channel capacity in active probe",
                        "old(bps)" => self.committed_channel_capacity.load(),
                        "new(bps)" => cscd.estimated_available_channel_capacity,
                        "expectedUsage(bps)" => self.get_expected_bandwidth_usage(),
                    );
                }
            } else {
                debugw!(
                    self.params.logger,
                    "stream allocator: channel congestion detected, updating channel capacity",
                    "old(bps)" => self.committed_channel_capacity.load(),
                    "new(bps)" => cscd.estimated_available_channel_capacity,
                    "expectedUsage(bps)" => self.get_expected_bandwidth_usage(),
                );
                self.committed_channel_capacity
                    .store(cscd.estimated_available_channel_capacity);

                self.allocate_all_tracks();
            }
        }
    }

    fn set_state(&self, state: StreamAllocatorState) {
        if self.state() == state {
            return;
        }

        infow!(
            self.params.logger,
            "stream allocator: state change",
            "from" => self.state(),
            "to" => state,
        );
        *self.state.lock() = state;

        // restart everything when state is STABLE
        if state == StreamAllocatorState::Stable {
            self.maybe_stop_probe();

            self.params.bwe.reset();

            *self.active_probe_cluster_id.lock() = ccutils::PROBE_CLUSTER_ID_INVALID;
            time::spawn(Self::ping(
                self.this.clone(),
                self.ping_generation.inc(),
                C_PING_LONG,
            ));
        } else {
            time::spawn(Self::ping(
                self.this.clone(),
                self.ping_generation.inc(),
                C_PING_SHORT,
            ));
        }
    }

    fn adjust_state(&self) {
        for track in self.get_video_tracks().iter() {
            if track.is_deficient() {
                self.set_state(StreamAllocatorState::Deficient);
                return;
            }
        }

        self.set_state(StreamAllocatorState::Stable);
    }

    fn allocate_track(&self, track: &Arc<Track>) {
        // end/abort any probe that may be running when a track specific change needs allocation
        self.maybe_stop_probe();

        // if not deficient, free pass allocate track
        let bwe_congestion_state = self.params.bwe.congestion_state();
        if !self.enabled
            || (self.state() == StreamAllocatorState::Stable
                && !is_deficient_congestion_state(bwe_congestion_state))
            || !track.is_managed()
        {
            let mut update = StreamStateUpdate::new();
            let allocation = track.allocate_optimal(
                C_FLAG_ALLOW_OVERSHOOT_WHILE_OPTIMAL,
                is_holdable_congestion_state(bwe_congestion_state),
            );
            update_stream_state_change(track, allocation, &mut update);
            self.maybe_send_update(update);
            return;
        }

        //
        // In DEFICIENT state,
        //   Two possibilities
        //   1. Available headroom is enough to accommodate track that needs change.
        //      Note that the track could be muted, hence stopping.
        //   2. Have to steal bits from other tracks currently streaming.
        //
        //   For both cases, do
        //     a. Find cooperative transition from track that needs allocation.
        //     b. If track is giving back bits, apply the transition and use bits given
        //        back to boost any deficient track(s).
        //
        //   If track needs more bits, i.e. upward transition (may need resume or higher layer subscription),
        //     a. Try to allocate using existing headroom. This can be tried to get the best
        //        possible fit for the available headroom.
        //     b. If there is not enough headroom to allocate anything, ask for best offer from
        //        other tracks that are currently streaming and try to use it. This is done only if the
        //        track needing change is not currently streaming, i. e. it has to be resumed.
        //
        track.provisional_allocate_prepare();
        let mut transition = track.provisional_allocate_get_cooperative_transition(
            C_FLAG_ALLOW_OVERSHOOT_WHILE_DEFICIENT,
        );

        // downgrade, giving back bits
        if transition.from.greater_than(transition.to) {
            let allocation = track.provisional_allocate_commit();

            let mut update = StreamStateUpdate::new();
            update_stream_state_change(track, allocation, &mut update);
            self.maybe_send_update(update);

            self.adjust_state();

            // Use the bits given back to boost deficient track(s).
            // Note layer downgrade may actually have positive delta (i.e. consume more bits)
            // because of when the measurement is done. But, only available headroom after
            // applying the transition will be used to boost deficient track(s).
            self.maybe_boost_deficient_tracks();
            return;
        }

        // a no-op transition
        if transition.from == transition.to {
            return;
        }

        // this track is currently not streaming and needs bits to start OR streaming at some layer and wants more bits.
        // NOTE: With co-operative transition, tracks should not be asking for more if already streaming, but handle that case any way.
        // first try an allocation using available headroom, current consumption of this track is discounted to calculate headroom.
        let available_channel_capacity =
            self.get_available_headroom_without_tracks(false, std::slice::from_ref(track));
        if available_channel_capacity > 0 {
            track.provisional_allocate_reset(); // to reset allocation from co-operative transition above and try fresh

            let mut best_layer = buffer::INVALID_LAYER;

            'alloc_loop: for spatial in 0..=buffer::DEFAULT_MAX_LAYER_SPATIAL {
                for temporal in 0..=buffer::DEFAULT_MAX_LAYER_TEMPORAL {
                    let layer = buffer::VideoLayer { spatial, temporal };

                    let (is_candidate, used_channel_capacity) = track.provisional_allocate(
                        available_channel_capacity,
                        layer,
                        self.allow_pause.load(),
                        C_FLAG_ALLOW_OVERSHOOT_WHILE_DEFICIENT,
                    );
                    if available_channel_capacity < used_channel_capacity {
                        break 'alloc_loop;
                    }

                    if is_candidate {
                        best_layer = layer;
                    }
                }
            }

            if best_layer.is_valid() {
                if best_layer.greater_than(transition.from) {
                    // found layer that can fit in available headroom, take it if it is better than existing
                    let mut update = StreamStateUpdate::new();
                    let allocation = track.provisional_allocate_commit();
                    update_stream_state_change(track, allocation, &mut update);
                    self.maybe_send_update(update);
                }

                self.adjust_state();
                return;
            }

            track.provisional_allocate_reset();
            transition = track.provisional_allocate_get_cooperative_transition(
                C_FLAG_ALLOW_OVERSHOOT_WHILE_DEFICIENT,
            ); // get transition again to reset above allocation attempt using available headroom
        }

        // if there is not enough headroom, try to redistribute starting with tracks that are closest to their desired.
        let mut bandwidth_acquired = 0i64;
        let mut contributing_tracks: Vec<Arc<Track>> = Vec::new();

        let min_distance_sorted = self.get_min_distance_sorted_video_tracks(track);
        for t in &min_distance_sorted.0 {
            t.provisional_allocate_prepare();
        }

        for t in &min_distance_sorted.0 {
            let tx = t.provisional_allocate_get_best_weighted_transition();
            if tx.bandwidth_delta < 0 {
                contributing_tracks.push(t.clone());

                bandwidth_acquired =
                    bandwidth_acquired.wrapping_add(tx.bandwidth_delta.wrapping_neg());
                if bandwidth_acquired >= transition.bandwidth_delta {
                    break;
                }
            }
        }

        let mut update = StreamStateUpdate::new();
        if bandwidth_acquired >= transition.bandwidth_delta {
            // commit the tracks that contributed
            for t in &contributing_tracks {
                let allocation = t.provisional_allocate_commit();
                update_stream_state_change(t, allocation, &mut update);
            }

            // STREAM-ALLOCATOR-TODO if got too much extra, can potentially give it to some deficient track
        }

        // commit the track that needs change if enough could be acquired or pause not allowed
        if !self.allow_pause.load() || bandwidth_acquired >= transition.bandwidth_delta {
            let allocation = track.provisional_allocate_commit();
            update_stream_state_change(track, allocation, &mut update);
        } else {
            // explicitly pause to ensure stream state update happens if a track coming out of mute cannot be allocated
            let allocation = track.pause();
            update_stream_state_change(track, allocation, &mut update);
        }

        self.maybe_send_update(update);

        self.adjust_state();
    }

    fn maybe_stop_probe(&self) {
        let active_probe_cluster_id = self.active_probe_cluster_id();
        if active_probe_cluster_id == ccutils::PROBE_CLUSTER_ID_INVALID {
            return;
        }

        let pci = self.params.pacer.end_probe_cluster(active_probe_cluster_id);

        for t in self.get_video_tracks().iter() {
            t.down_track()
                .swap_probe_cluster_id(pci.id, ccutils::PROBE_CLUSTER_ID_INVALID);
        }

        self.params.bwe.probe_cluster_done(pci);
        self.prober.reset(pci);
    }

    fn maybe_boost_deficient_tracks(&self) {
        let mut available_channel_capacity = self.get_available_headroom(false);
        if available_channel_capacity <= 0 {
            debugw!(
                self.params.logger,
                "stream allocator: no available headroom to boost deficient tracks",
                "committedChannelCapacity" => self.committed_channel_capacity.load(),
                "availableChannelCapacity" => available_channel_capacity,
                "expectedBandwidthUsage" => self.get_expected_bandwidth_usage(),
            );
            return;
        }

        let mut update = StreamStateUpdate::new();

        let mut sorted_tracks = self.get_max_distance_sorted_deficient_video_tracks();
        'boost_loop: loop {
            for (idx, track) in sorted_tracks.0.iter().enumerate() {
                let (allocation, boosted) = track.allocate_next_higher(
                    available_channel_capacity,
                    C_FLAG_ALLOW_OVERSHOOT_IN_CATCHUP,
                );
                if !boosted {
                    if idx == sorted_tracks.0.len() - 1 {
                        // all tracks tried
                        break 'boost_loop;
                    }
                    continue;
                }

                update_stream_state_change(track, allocation, &mut update);

                available_channel_capacity =
                    available_channel_capacity.wrapping_sub(allocation.bandwidth_delta);
                if available_channel_capacity <= 0 {
                    break 'boost_loop;
                }

                break; // sort again below as the track that was just boosted could still be farthest from its desired
            }
            sorted_tracks = self.get_max_distance_sorted_deficient_video_tracks();
            if sorted_tracks.0.is_empty() {
                break; // nothing available to boost
            }
        }

        self.maybe_send_update(update);

        self.adjust_state();
    }

    fn allocate_all_tracks(&self) {
        if !self.enabled {
            // nothing else to do when disabled
            return;
        }

        //
        // Goals:
        //   1. Stream as many tracks as possible, i.e. no pauses.
        //   2. Try to give fair allocation to all track.
        //
        // Start with the lowest layer and give each track a chance at that layer and keep going up.
        // As long as there is enough bandwidth for tracks to stream at the lowest layer, the first goal is achieved.
        //
        // Tracks that have higher subscribed layer can use any additional available bandwidth. This tried to achieve the second goal.
        //
        // If there is not enough bandwidth even for the lowest layer, tracks at lower priorities will be paused.
        //
        let mut update = StreamStateUpdate::new();

        let mut available_channel_capacity = self.get_available_channel_capacity(true);

        //
        // This pass is to find out if there is any leftover channel capacity after allocating exempt tracks.
        // Exempt tracks are given optimal allocation (i. e. no bandwidth constraint) so that they do not fail allocation.
        //
        let video_tracks = self.get_video_tracks();
        for track in video_tracks.iter() {
            if track.is_managed() {
                continue;
            }

            let allocation =
                track.allocate_optimal(C_FLAG_ALLOW_OVERSHOOT_EXEMPT_TRACK_WHILE_DEFICIENT, false);
            update_stream_state_change(track, allocation, &mut update);

            // STREAM-ALLOCATOR-TODO: optimistic allocation before bitrate is available will return 0. How to account for that?
            if !self.params.config.disable_estimation_unmanaged_tracks {
                available_channel_capacity =
                    available_channel_capacity.wrapping_sub(allocation.bandwidth_requested);
            }
        }

        if available_channel_capacity < 0 {
            available_channel_capacity = 0;
        }
        if available_channel_capacity == 0 && self.allow_pause.load() {
            // nothing left for managed tracks, pause them all
            for track in video_tracks.iter() {
                if !track.is_managed() {
                    continue;
                }

                let allocation = track.pause();
                update_stream_state_change(track, allocation, &mut update);
            }
        } else {
            let sorted = self.get_sorted_video_tracks();
            for track in &sorted.0 {
                track.provisional_allocate_prepare();
            }

            for spatial in 0..=buffer::DEFAULT_MAX_LAYER_SPATIAL {
                for temporal in 0..=buffer::DEFAULT_MAX_LAYER_TEMPORAL {
                    let layer = buffer::VideoLayer { spatial, temporal };

                    for track in &sorted.0 {
                        let (_, used_channel_capacity) = track.provisional_allocate(
                            available_channel_capacity,
                            layer,
                            self.allow_pause.load(),
                            C_FLAG_ALLOW_OVERSHOOT_WHILE_DEFICIENT,
                        );
                        available_channel_capacity =
                            available_channel_capacity.wrapping_sub(used_channel_capacity);
                        if available_channel_capacity < 0 {
                            available_channel_capacity = 0;
                        }
                    }
                }
            }

            for track in &sorted.0 {
                let allocation = track.provisional_allocate_commit();
                update_stream_state_change(track, allocation, &mut update);
            }
        }

        self.maybe_send_update(update);

        self.adjust_state();
    }

    fn maybe_send_update(&self, update: StreamStateUpdate) {
        if update.empty() {
            return;
        }

        // logging individual changes to make it easier for logging systems
        for stream_state in &update.stream_states {
            debugw!(
                self.params.logger,
                "streamed tracks changed",
                "trackID" => stream_state.track_id,
                "state" => stream_state.state,
            );
        }
        let on_stream_state_change = self.on_stream_state_change.read().clone();
        if let Some(on_stream_state_change) = on_stream_state_change
            && let Err(err) = on_stream_state_change(&update)
        {
            errorw!(
                self.params.logger,
                "could not send streamed tracks update",
                err
            );
        }
    }

    fn get_available_channel_capacity(&self, allow_override: bool) -> i64 {
        let mut available_channel_capacity = self.committed_channel_capacity.load();
        if self.params.config.min_channel_capacity > available_channel_capacity {
            available_channel_capacity = self.params.config.min_channel_capacity;
            debugw!(
                self.params.logger,
                "stream allocator: overriding channel capacity with min channel capacity",
                "actual" => self.committed_channel_capacity.load(),
                "override" => available_channel_capacity,
            );
        }
        if allow_override && self.overridden_channel_capacity.load() > 0 {
            available_channel_capacity = self.overridden_channel_capacity.load();
            debugw!(
                self.params.logger,
                "stream allocator: overriding channel capacity",
                "actual" => self.committed_channel_capacity.load(),
                "override" => available_channel_capacity,
            );
        }

        available_channel_capacity
    }

    fn get_expected_bandwidth_usage(&self) -> i64 {
        let mut expected = 0i64;
        for track in self.get_video_tracks().iter() {
            expected = expected.wrapping_add(track.bandwidth_requested());
        }

        expected
    }

    fn get_expected_bandwidth_usage_without_tracks(&self, filtered_tracks: &[Arc<Track>]) -> i64 {
        let mut expected = 0i64;
        for track in self.get_video_tracks().iter() {
            let filtered = filtered_tracks.iter().any(|t| Arc::ptr_eq(t, track));
            if !filtered {
                expected = expected.wrapping_add(track.bandwidth_requested());
            }
        }

        expected
    }

    fn get_available_headroom(&self, allow_override: bool) -> i64 {
        self.get_available_channel_capacity(allow_override)
            .wrapping_sub(self.get_expected_bandwidth_usage())
    }

    fn get_available_headroom_without_tracks(
        &self,
        allow_override: bool,
        filtered_tracks: &[Arc<Track>],
    ) -> i64 {
        self.get_available_channel_capacity(allow_override)
            .wrapping_sub(self.get_expected_bandwidth_usage_without_tracks(filtered_tracks))
    }

    fn get_nack_delta(&self) -> (u32, u32) {
        let mut agg_packet_delta = 0u32;
        let mut agg_repeated_nack_delta = 0u32;
        for track in self.get_video_tracks().iter() {
            let (packet_delta, nack_delta) = track.get_nack_delta();
            agg_packet_delta = agg_packet_delta.wrapping_add(packet_delta);
            agg_repeated_nack_delta = agg_repeated_nack_delta.wrapping_add(nack_delta);
        }

        (agg_packet_delta, agg_repeated_nack_delta)
    }

    fn maybe_probe(&self) {
        if self.overridden_channel_capacity.load() > 0 {
            // do not probe if channel capacity is overridden
            return;
        }

        if !self.params.bwe.can_probe() {
            return;
        }

        match self.params.config.probe_mode {
            ProbeMode::Media => {
                self.maybe_probe_with_media();
                self.adjust_state();
            }
            ProbeMode::Padding => {
                self.maybe_probe_with_padding();
            }
        }
    }

    fn maybe_probe_with_media(&self) {
        // boost deficient track farthest from desired layer
        for track in &self.get_max_distance_sorted_deficient_video_tracks().0 {
            let (allocation, boosted) = track
                .allocate_next_higher(C_CHANNEL_CAPACITY_INFINITY, C_FLAG_ALLOW_OVERSHOOT_IN_BOOST);
            if !boosted {
                continue;
            }

            let mut update = StreamStateUpdate::new();
            update_stream_state_change(track, allocation, &mut update);
            self.maybe_send_update(update);

            self.params.bwe.reset();
            break;
        }
    }

    fn maybe_probe_with_padding(&self) {
        // use deficient track farthest from desired layer to find how much to probe
        for track in &self.get_max_distance_sorted_deficient_video_tracks().0 {
            let (transition, available) =
                track.get_next_higher_transition(C_FLAG_ALLOW_OVERSHOOT_IN_PROBE);
            if !available || transition.bandwidth_delta < 0 {
                continue;
            }

            // overshoot a bit to account for noise (in measurement/estimate etc)
            let desired_increase_bps = (transition
                .bandwidth_delta
                .wrapping_mul(self.params.config.probe_overage_pct)
                / 100)
                .max(self.params.config.probe_min_bps);
            let expected_bandwidth_usage = self.get_expected_bandwidth_usage();
            let pci = self.prober.add_cluster(
                ccutils::ProbeClusterMode::Uniform,
                ccutils::ProbeClusterGoal {
                    available_bandwidth_bps: self.committed_channel_capacity.load(),
                    expected_usage_bps: expected_bandwidth_usage,
                    desired_bps: expected_bandwidth_usage.wrapping_add(desired_increase_bps),
                    duration: self.params.bwe.probe_duration(),
                    ..Default::default()
                },
            );
            debugw!(
                self.params.logger,
                "stream allocator: adding probe",
                "probeClusterInfo" => pci,
            );
            break;
        }
    }

    fn get_video_tracks(&self) -> Arc<[Arc<Track>]> {
        let vt = self.video_tracks_mu.read();

        vt.video_tracks_shadow.clone()
    }

    fn get_sorted_video_tracks(&self) -> TrackSorter {
        let mut track_sorter = TrackSorter::default();
        for track in self.get_video_tracks().iter() {
            if !track.is_managed() {
                continue;
            }

            track_sorter.0.push(track.clone());
        }

        track_sorter.sort();

        track_sorter
    }

    fn get_min_distance_sorted_video_tracks(&self, exclude: &Arc<Track>) -> MinDistanceSorter {
        let mut min_distance_sorter = MinDistanceSorter::default();
        for track in self.get_video_tracks().iter() {
            if !track.is_managed() || Arc::ptr_eq(track, exclude) {
                continue;
            }

            min_distance_sorter.0.push(track.clone());
        }

        min_distance_sorter.sort();

        min_distance_sorter
    }

    fn get_max_distance_sorted_deficient_video_tracks(&self) -> MaxDistanceSorter {
        let mut max_distance_sorter = MaxDistanceSorter::default();
        for track in self.get_video_tracks().iter() {
            if !track.is_managed() || !track.is_deficient() {
                continue;
            }

            max_distance_sorter.0.push(track.clone());
        }

        max_distance_sorter.sort();

        max_distance_sorter
    }
}

impl VideoTracks {
    fn shadow_video_tracks_locked(&mut self) {
        self.video_tracks_shadow = self.video_tracks.values().cloned().collect();
    }
}

// ------------------------------------------------

fn update_stream_state_change(
    track: &Track,
    allocation: VideoAllocation,
    update: &mut StreamStateUpdate,
) {
    let mut updated = false;
    let mut stream_state = StreamState::Inactive;
    match allocation.pause_reason {
        VideoPauseReason::Muted | VideoPauseReason::PubMuted => {
            stream_state = StreamState::Inactive;
            updated = track.set_stream_state(stream_state);
        }

        VideoPauseReason::Bandwidth => {
            stream_state = StreamState::Paused;
            updated = track.set_stream_state(stream_state);
        }

        _ => {}
    }

    if updated {
        update.handle_streaming_change(track, stream_state);
    }
}

fn is_holdable_congestion_state(bwe_congestion_state: bwe::CongestionState) -> bool {
    bwe_congestion_state == bwe::CongestionState::EarlyWarning
}

fn is_deficient_congestion_state(bwe_congestion_state: bwe::CongestionState) -> bool {
    bwe_congestion_state == bwe::CongestionState::Congested
}

// ------------------------------------------------

// Go's `time.NewTicker(d)`: the first tick comes after `d`, and ticks that the receiver is
// too slow for are dropped.
fn new_ticker(d: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + d.to_std(), d.to_std());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker
}

// ------------------------------------------------

// The four interfaces of the original that the allocator has the methods of.

impl DownTrackStreamAllocatorListener for StreamAllocator {
    fn on_remb(&self, dt: &DownTrack, remb: &ReceiverEstimatedMaximumBitrate) {
        StreamAllocator::on_remb(self, dt, remb);
    }

    fn on_transport_cc_feedback(&self, dt: &DownTrack, cc: &TransportLayerCc) {
        StreamAllocator::on_transport_cc_feedback(self, dt, cc);
    }

    fn on_available_layers_changed(&self, dt: &DownTrack) {
        StreamAllocator::on_available_layers_changed(self, dt);
    }

    fn on_bitrate_availability_changed(&self, dt: &DownTrack) {
        StreamAllocator::on_bitrate_availability_changed(self, dt);
    }

    fn on_max_published_spatial_changed(&self, dt: &DownTrack) {
        StreamAllocator::on_max_published_spatial_changed(self, dt);
    }

    fn on_max_published_temporal_changed(&self, dt: &DownTrack) {
        StreamAllocator::on_max_published_temporal_changed(self, dt);
    }

    fn on_subscription_changed(&self, dt: &DownTrack) {
        StreamAllocator::on_subscription_changed(self, dt);
    }

    fn on_subscribed_layer_changed(&self, dt: &DownTrack, layers: buffer::VideoLayer) {
        StreamAllocator::on_subscribed_layer_changed(self, dt, layers);
    }

    fn on_resume(&self, dt: &DownTrack) {
        StreamAllocator::on_resume(self, dt);
    }

    fn is_bwe_enabled(&self, dt: &DownTrack) -> bool {
        StreamAllocator::is_bwe_enabled(self, dt)
    }

    fn bwe_type(&self) -> bwe::BweType {
        StreamAllocator::bwe_type(self)
    }

    fn is_subscribe_mutable(&self, dt: &DownTrack) -> bool {
        StreamAllocator::is_subscribe_mutable(self, dt)
    }
}

impl bwe::BweListener for StreamAllocator {
    fn on_congestion_state_change(
        &self,
        from_state: bwe::CongestionState,
        to_state: bwe::CongestionState,
        estimated_available_channel_capacity: i64,
    ) {
        StreamAllocator::on_congestion_state_change(
            self,
            from_state,
            to_state,
            estimated_available_channel_capacity,
        );
    }
}

impl pacer::PacerProbeObserverListener for StreamAllocator {
    fn on_pacer_probe_observer_cluster_complete(&self, probe_cluster_id: ccutils::ProbeClusterId) {
        StreamAllocator::on_pacer_probe_observer_cluster_complete(self, probe_cluster_id);
    }
}

impl ccutils::ProberListener for StreamAllocator {
    fn on_probe_cluster_switch(&self, info: ccutils::ProbeClusterInfo) {
        StreamAllocator::on_probe_cluster_switch(self, info);
    }

    fn on_send_probe(&self, bytes_to_send: i64) {
        StreamAllocator::on_send_probe(self, bytes_to_send);
    }
}

// The same three for what the estimator, the pacer and the prober hold: an allocator that
// is gone is a listener that was not set.

impl bwe::BweListener for Listener {
    fn on_congestion_state_change(
        &self,
        from_state: bwe::CongestionState,
        to_state: bwe::CongestionState,
        estimated_available_channel_capacity: i64,
    ) {
        if let Some(s) = self.stream_allocator.upgrade() {
            s.on_congestion_state_change(
                from_state,
                to_state,
                estimated_available_channel_capacity,
            );
        }
    }
}

impl pacer::PacerProbeObserverListener for Listener {
    fn on_pacer_probe_observer_cluster_complete(&self, probe_cluster_id: ccutils::ProbeClusterId) {
        if let Some(s) = self.stream_allocator.upgrade() {
            s.on_pacer_probe_observer_cluster_complete(probe_cluster_id);
        }
    }
}

impl ccutils::ProberListener for Listener {
    fn on_probe_cluster_switch(&self, info: ccutils::ProbeClusterInfo) {
        if let Some(s) = self.stream_allocator.upgrade() {
            s.on_probe_cluster_switch(info);
        }
    }

    fn on_send_probe(&self, bytes_to_send: i64) {
        if let Some(s) = self.stream_allocator.upgrade() {
            s.on_send_probe(bytes_to_send);
        }
    }
}

#[cfg(test)]
#[path = "streamallocator_test.rs"]
mod streamallocator_test;
