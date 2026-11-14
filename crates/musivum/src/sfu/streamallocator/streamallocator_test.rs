// New for musivum: the original has no tests of this package (two tests of the down track
// cover it, see sfu/downtrack_downstream_integration_test.rs).
//
// The tests here drive an allocator through its public methods and through the listeners it
// registers, with real down tracks bound as the down track's own tests bind them, a fake
// pacer and an estimator that says what the test tells it to. Unless a test says otherwise,
// its expected values are the original's: the same steps were run against LiveKit v1.13.11
// with a Go test laid over `pkg/sfu` (the allocator with a scripted `bwe.BWE`, down tracks
// from the original's `newBoundDownTrack`), and what that printed is asserted here. What a
// test reads of the allocator's own state (`state`, the committed capacity, the active probe)
// is from reading the original, which a test outside its package cannot see.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use parking_lot::Mutex;

use super::*;
use crate::mediatransportutil::codec;
use crate::protocol::logger;
use crate::sfu::downtrack::downtrack_downstream_integration_test::{
    DownTrackHarness, MediaEngineConfig, new_bound_down_track_with_ssrcs, new_buffer_factory,
    vp8_codec_params,
};
use crate::sfu::pacer::FakePacer;
use crate::sfu::testutils;
use crate::sfu::{Bitrates, TrackReceiver};
use crate::testutils::eventually_async;

const TICK: StdDuration = StdDuration::from_millis(10);
const WAIT: StdDuration = StdDuration::from_secs(10);

// -----------------------------------------------------------------------------
// an estimator that does what the test says
// -----------------------------------------------------------------------------

#[derive(Default)]
struct ScriptedBwe {
    lock: Mutex<ScriptedBweInner>,
}

#[derive(Default)]
struct ScriptedBweInner {
    listener: Option<Arc<dyn bwe::BweListener>>,
    congestion_state: bwe::CongestionState,
    // How many times `can_probe` still says yes. The estimators say no from the start of a
    // probe to its end; a count does the same without depending on how soon the start of
    // the probe is handled.
    probes_allowed: usize,
    // what `probe_cluster_finalize` answers once, after a probe is done
    finalize_on_done: Option<(ccutils::ProbeSignal, i64)>,
    finalize: Option<(ccutils::ProbeSignal, i64)>,

    starting: Vec<ccutils::ProbeClusterInfo>,
    done: Vec<ccutils::ProbeClusterInfo>,
    resets: usize,
    rembs: Vec<(i64, i64, u32, u32)>,
    twccs: Vec<TransportLayerCc>,
    rtts: Vec<f64>,
}

impl ScriptedBwe {
    fn set_congestion_state(&self, congestion_state: bwe::CongestionState) {
        self.lock.lock().congestion_state = congestion_state;
    }

    // What an estimator does when its state changes: it tells its listener.
    fn notify(&self, from: bwe::CongestionState, to: bwe::CongestionState, capacity: i64) {
        let listener = self.lock.lock().listener.clone();
        listener
            .expect("the allocator set itself as the listener")
            .on_congestion_state_change(from, to, capacity);
    }

    fn allow_probes(&self, n: usize) {
        self.lock.lock().probes_allowed = n;
    }

    fn finalize_on_done(&self, signal: ccutils::ProbeSignal, capacity: i64) {
        self.lock.lock().finalize_on_done = Some((signal, capacity));
    }

    fn starting(&self) -> Vec<ccutils::ProbeClusterInfo> {
        self.lock.lock().starting.clone()
    }

    fn done(&self) -> Vec<ccutils::ProbeClusterInfo> {
        self.lock.lock().done.clone()
    }

    fn resets(&self) -> usize {
        self.lock.lock().resets
    }

    fn rembs(&self) -> Vec<(i64, i64, u32, u32)> {
        self.lock.lock().rembs.clone()
    }

    fn twccs(&self) -> Vec<TransportLayerCc> {
        self.lock.lock().twccs.clone()
    }

    fn rtts(&self) -> Vec<f64> {
        self.lock.lock().rtts.clone()
    }
}

impl bwe::Bwe for ScriptedBwe {
    fn r#type(&self) -> bwe::BweType {
        bwe::BweType::Remote
    }

    fn set_bwe_listener(&self, bwe_listner: Option<Arc<dyn bwe::BweListener>>) {
        self.lock.lock().listener = bwe_listner;
    }

    fn reset(&self) {
        self.lock.lock().resets += 1;
    }

    fn handle_remb(
        &self,
        received_estimate: i64,
        expected_bandwidth_usage: i64,
        sent_packets: u32,
        repeated_nacks: u32,
    ) {
        self.lock.lock().rembs.push((
            received_estimate,
            expected_bandwidth_usage,
            sent_packets,
            repeated_nacks,
        ));
    }

    fn record_packet_send_and_get_sequence_number(
        &self,
        _at_micro: i64,
        _size: usize,
        _is_rtx: bool,
        _probe_cluster_id: ccutils::ProbeClusterId,
        _is_probe: bool,
    ) -> u16 {
        0
    }

    fn handle_twcc_feedback(&self, report: &TransportLayerCc) {
        self.lock.lock().twccs.push(report.clone());
    }

    fn update_rtt(&self, rtt: f64) {
        self.lock.lock().rtts.push(rtt);
    }

    fn congestion_state(&self) -> bwe::CongestionState {
        self.lock.lock().congestion_state
    }

    fn can_probe(&self) -> bool {
        let mut inner = self.lock.lock();
        if inner.probes_allowed == 0 {
            return false;
        }
        inner.probes_allowed -= 1;
        true
    }

    fn probe_duration(&self) -> Duration {
        Duration::from_millis(200)
    }

    fn probe_cluster_starting(&self, pci: ccutils::ProbeClusterInfo) {
        self.lock.lock().starting.push(pci);
    }

    fn probe_cluster_done(&self, pci: ccutils::ProbeClusterInfo) {
        let mut inner = self.lock.lock();
        inner.done.push(pci);
        inner.finalize = inner.finalize_on_done.take();
    }

    fn probe_cluster_is_goal_reached(&self) -> bool {
        false
    }

    fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool) {
        match self.lock.lock().finalize.take() {
            Some((signal, capacity)) => (signal, capacity, true),
            None => (ccutils::ProbeSignal::Inconclusive, 0, false),
        }
    }
}

// -----------------------------------------------------------------------------
// the allocator under test, and what is around it
// -----------------------------------------------------------------------------

struct Env {
    bwe: Arc<ScriptedBwe>,
    pacer: Arc<FakePacer>,
    sa: Arc<StreamAllocator>,
    updates: Arc<Mutex<Vec<StreamStateUpdate>>>,
    num_tracks: Mutex<u32>,
}

const CAMERA: livekit::TrackSource = livekit::TrackSource::Camera;
const SCREEN_SHARE: livekit::TrackSource = livekit::TrackSource::ScreenShare;

use bwe::CongestionState::{Congested, EarlyWarning, None as Clear};

impl Env {
    // An allocator that is enabled and started, as the transport makes it.
    fn new(config: StreamAllocatorConfig, allow_pause: bool) -> Env {
        let bwe = Arc::new(ScriptedBwe::default());
        let pacer = Arc::new(FakePacer::default());
        let sa = StreamAllocator::new(
            StreamAllocatorParams {
                config,
                bwe: bwe.clone(),
                pacer: pacer.clone(),
                rtt_getter: Some(Box::new(|| (0.05, true))),
                logger: logger::get_logger(),
            },
            true,
            allow_pause,
        );
        let updates = Arc::new(Mutex::new(Vec::new()));
        let recorded = updates.clone();
        sa.on_stream_state_change(move |update| {
            recorded.lock().push(update.clone());
            Ok(())
        });
        sa.start();
        Env {
            bwe,
            pacer,
            sa,
            updates,
            num_tracks: Mutex::new(0),
        }
    }

    // A down track for a published track with one temporal layer and the given bitrates of
    // its spatial layers, subscribed at its highest layer and added to the allocator.
    async fn add_track(
        &self,
        id: &'static str,
        source: livekit::TrackSource,
        priority: u8,
        is_multi_layered: bool,
        rates: &[i64],
    ) -> DownTrackHarness {
        let n = {
            let mut num_tracks = self.num_tracks.lock();
            *num_tracks += 1;
            *num_tracks
        };
        let dh = new_bound_down_track_with_ssrcs(
            new_buffer_factory(),
            vp8_codec_params(),
            self.pacer.clone(),
            MediaEngineConfig {
                video: true,
                header_extensions: true,
            },
            None,
            (0x0a0a_0001 + 0x100 * n, 0x0a0a_0002 + 0x100 * n),
            |r| {
                r.track_id_returns(id.into());
                // layer switches without sender reports from the fake publisher
                r.video_layer_mode_returns(
                    livekit::video_layer::Mode::OneSpatialLayerPerStreamIncompleteRtcpSr,
                );
            },
        )
        .await;
        dh.receiver.get_layered_bitrate_returns(bitrates(rates));
        self.sa.add_track(
            dh.dt.clone(),
            AddTrackParams {
                source,
                priority,
                is_multi_layered,
                publisher_id: format!("pub-{id}").into(),
            },
        );
        let max_spatial = rates.len() as i32 - 1;
        dh.dt.up_track_max_published_layer_change(max_spatial);
        dh.dt.up_track_max_temporal_layer_seen_change(0);
        dh.dt.set_max_spatial_layer(max_spatial);
        dh.dt.set_max_temporal_layer(0);
        dh.dt.up_track_layers_change();
        dh.dt.up_track_bitrate_availability_change();
        self.flush();
        dh
    }

    // Waits until the events posted so far are handled.
    fn flush(&self) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.sa.events_queue.enqueue(
            move |_| {
                let _ = tx.send(());
            },
            Event::default(),
        );
        rx.recv_timeout(WAIT).expect("the events worker runs");
    }

    // The estimator's change of state, as the estimator tells it, handled.
    fn congestion_state_change(
        &self,
        from: bwe::CongestionState,
        to: bwe::CongestionState,
        capacity: i64,
    ) {
        self.bwe.notify(from, to, capacity);
        self.flush();
    }

    // The stream state updates since the last call, each as its (publisher, track, state).
    fn take_updates(&self) -> Vec<Vec<(String, String, StreamState)>> {
        self.flush();
        std::mem::take(&mut *self.updates.lock())
            .into_iter()
            .map(|update| {
                update
                    .stream_states
                    .into_iter()
                    .map(|info| {
                        (
                            info.participant_id.to_string(),
                            info.track_id.to_string(),
                            info.state,
                        )
                    })
                    .collect()
            })
            .collect()
    }

    fn state(&self) -> StreamAllocatorState {
        self.sa.state()
    }
}

fn bitrates(rates: &[i64]) -> (Vec<i32>, Bitrates) {
    let mut brs = Bitrates::default();
    let mut layers = Vec::new();
    for (i, &rate) in rates.iter().enumerate() {
        brs[i][0] = rate;
        layers.push(i as i32);
    }
    (layers, brs)
}

const LAYERS: [i64; 3] = [100_000, 300_000, 900_000];

// What a down track has of its allocation: the bitrate of its target layer, whether it is
// short of what it wants, and how far.
fn allocation(dh: &DownTrackHarness) -> (i64, bool, f64) {
    (
        dh.dt.bandwidth_requested(),
        dh.dt.is_deficient(),
        dh.dt.distance_to_desired(),
    )
}

fn update(id: &str, state: StreamState) -> Vec<(String, String, StreamState)> {
    vec![(format!("pub-{id}"), id.to_string(), state)]
}

// A key frame of the publisher's given layer, written to the down track as its receiver
// would: what the down track answers.
fn write_key_frame(dh: &DownTrackHarness, layer: i32) -> i32 {
    let vp8 = codec::Vp8 {
        first_byte: 0x10,
        s: true,
        picture_id: 1,
        is_key_frame: true,
        ..Default::default()
    };
    let ep = testutils::get_test_ext_packet_vp8(
        &testutils::TestExtPacketParams {
            sequence_number: 100,
            timestamp: 3000,
            ssrc: 0x1000 + layer as u32,
            payload_type: 96,
            payload_size: 100,
            is_key_frame: true,
            marker: true,
            arrival_time: time::now(),
            video_layer: buffer::VideoLayer {
                spatial: layer,
                temporal: 0,
            },
            ..Default::default()
        },
        &vp8,
    )
    .unwrap();
    dh.dt.write_rtp(&ep, layer)
}

// -----------------------------------------------------------------------------
// the tests
// -----------------------------------------------------------------------------

// From reading the original: the defaults of the configuration and the texts.
#[test]
fn the_configuration_and_the_texts_are_the_originals() {
    assert_eq!(
        StreamAllocatorConfig {
            min_channel_capacity: 0,
            disable_estimation_unmanaged_tracks: false,
            probe_mode: ProbeMode::Padding,
            probe_overage_pct: 120,
            probe_min_bps: 200_000,
            paused_min_wait: Duration::from_secs(5),
        },
        DEFAULT_STREAM_ALLOCATOR_CONFIG
    );

    for (mode, text) in [(ProbeMode::Padding, "padding"), (ProbeMode::Media, "media")] {
        assert_eq!(text, mode.to_string());
        assert_eq!(Ok(mode), text.parse());
    }
    assert_eq!(Err(()), "".parse::<ProbeMode>());

    assert_eq!("STABLE", StreamAllocatorState::Stable.to_string());
    assert_eq!("DEFICIENT", StreamAllocatorState::Deficient.to_string());

    for (signal, text) in [
        (StreamAllocatorSignal::AllocateTrack, "ALLOCATE_TRACK"),
        (
            StreamAllocatorSignal::AllocateAllTracks,
            "ALLOCATE_ALL_TRACKS",
        ),
        (StreamAllocatorSignal::AdjustState, "ADJUST_STATE"),
        (StreamAllocatorSignal::Estimate, "ESTIMATE"),
        (StreamAllocatorSignal::Feedback, "FEEDBACK"),
        (StreamAllocatorSignal::PeriodicPing, "PERIODIC_PING"),
        (
            StreamAllocatorSignal::ProbeClusterSwitch,
            "PROBE_CLUSTER_SWITCH",
        ),
        (StreamAllocatorSignal::SendProbe, "SEND_PROBE"),
        (
            StreamAllocatorSignal::PacerProbeObserverClusterComplete,
            "PACER_PROBE_OBSERVER_CLUSTER_COMPLETE",
        ),
        (StreamAllocatorSignal::Resume, "RESUME"),
        (StreamAllocatorSignal::SetAllowPause, "SET_ALLOW_PAUSE"),
        (
            StreamAllocatorSignal::SetChannelCapacity,
            "SET_CHANNEL_CAPACITY",
        ),
        (
            StreamAllocatorSignal::CongestionStateChange,
            "CONGESTION_STATE_CHANGE",
        ),
    ] {
        assert_eq!(text, signal.to_string());
    }

    assert_eq!(
        "StreamAllocator:Event{signal: ADJUST_STATE}",
        Event {
            signal: StreamAllocatorSignal::AdjustState,
            ..Default::default()
        }
        .to_string()
    );
    assert_eq!(
        "StreamAllocator:Event{signal: RESUME, trackID: TR_a}",
        Event {
            signal: StreamAllocatorSignal::Resume,
            track_id: "TR_a".into(),
            ..Default::default()
        }
        .to_string()
    );
    assert_eq!(
        "StreamAllocator:Event{signal: ESTIMATE, channelCapacity: 150000}",
        Event {
            signal: StreamAllocatorSignal::Estimate,
            channel_capacity: 150_000,
            ..Default::default()
        }
        .to_string()
    );
    assert_eq!(
        "StreamAllocator:Event{signal: SET_ALLOW_PAUSE, allowPause: true}",
        Event {
            signal: StreamAllocatorSignal::SetAllowPause,
            allow_pause: true,
            ..Default::default()
        }
        .to_string()
    );
}

// An estimate that suffices for all tracks gives each its optimal layer.
#[tokio::test(flavor = "multi_thread")]
async fn an_estimate_that_suffices_gives_each_track_its_optimal_layer() {
    let env = Env::new(DEFAULT_STREAM_ALLOCATOR_CONFIG, true);
    let a = env.add_track("TR_a", CAMERA, 10, false, &LAYERS).await;
    let b = env.add_track("TR_b", CAMERA, 5, false, &LAYERS).await;

    // without an estimate every track has its highest layer
    assert_eq!((900_000, false, 0.0), allocation(&a));
    assert_eq!((900_000, false, 0.0), allocation(&b));
    assert_eq!(StreamAllocatorState::Stable, env.state());

    // the channel is congested, and has room for both
    env.bwe.set_congestion_state(Congested);
    env.congestion_state_change(Clear, Congested, 2_000_000);
    assert_eq!((900_000, false, 0.0), allocation(&a));
    assert_eq!((900_000, false, 0.0), allocation(&b));
    assert!(env.take_updates().is_empty());
    assert_eq!(StreamAllocatorState::Stable, env.state());
    assert_eq!(2_000_000, env.sa.committed_channel_capacity.load());

    env.sa.stop();
}

// An estimate that does not suffice pauses the track of the lowest priority and says so; a
// higher estimate gives it a layer again, and the stream is reported active when the down
// track forwards again. Without leave to pause nothing is paused, and a capacity that is
// set from outside goes before the estimate.
#[tokio::test(flavor = "multi_thread")]
async fn an_estimate_that_does_not_suffice_pauses_the_lowest_priority_track() {
    let env = Env::new(DEFAULT_STREAM_ALLOCATOR_CONFIG, true);
    let a = env.add_track("TR_a", CAMERA, 10, false, &LAYERS).await;
    let b = env.add_track("TR_b", CAMERA, 5, false, &LAYERS).await;
    assert!(env.take_updates().is_empty());

    // room for the lowest layer of one track
    env.bwe.set_congestion_state(Congested);
    env.congestion_state_change(Clear, Congested, 150_000);
    assert_eq!((100_000, true, 2.0), allocation(&a));
    assert_eq!((0, true, 3.0), allocation(&b));
    assert_eq!(
        vec![update("TR_b", StreamState::Paused)],
        env.take_updates()
    );
    assert_eq!(StreamAllocatorState::Deficient, env.state());
    // a paused track cannot be muted by its subscriber
    assert!(env.sa.is_subscribe_mutable(&a.dt));
    assert!(!env.sa.is_subscribe_mutable(&b.dt));
    // when deficient the allocator is pinged ten times a second: the first ping takes the
    // round trip time to the estimator
    eventually_async(|| env.bwe.rtts() == vec![0.05], WAIT, TICK).await;

    // a higher estimate: the higher priority gets the higher layer, the paused track the
    // lowest
    env.congestion_state_change(Congested, Congested, 450_000);
    assert_eq!((300_000, true, 1.0), allocation(&a));
    assert_eq!((100_000, true, 2.0), allocation(&b));
    assert!(env.take_updates().is_empty());

    // the paused stream is active again when a key frame of its layer is forwarded
    assert_eq!(1, write_key_frame(&b, 0));
    assert_eq!(
        vec![update("TR_b", StreamState::Active)],
        env.take_updates()
    );
    assert!(env.sa.is_subscribe_mutable(&b.dt));
    // and so is one that starts
    assert_eq!(1, write_key_frame(&a, 1));
    assert_eq!(
        vec![update("TR_a", StreamState::Active)],
        env.take_updates()
    );

    // the end of the congestion by itself changes nothing: more comes from probing
    env.bwe.set_congestion_state(Clear);
    env.congestion_state_change(Congested, Clear, 450_000);
    assert_eq!((300_000, true, 1.0), allocation(&a));
    assert_eq!((100_000, true, 2.0), allocation(&b));
    assert_eq!(StreamAllocatorState::Deficient, env.state());

    // without leave to pause every track keeps its lowest layer, however little there is
    env.sa.set_allow_pause(false);
    env.bwe.set_congestion_state(Congested);
    env.congestion_state_change(Clear, Congested, 50_000);
    assert_eq!((100_000, true, 2.0), allocation(&a));
    assert_eq!((100_000, true, 2.0), allocation(&b));
    assert!(env.take_updates().is_empty());

    // a capacity set from outside overrides the estimate; with nothing deficient the
    // allocator is stable again and resets the estimator
    env.sa.set_channel_capacity(2_000_000);
    env.flush();
    assert_eq!((900_000, false, 0.0), allocation(&a));
    assert_eq!((900_000, false, 0.0), allocation(&b));
    assert_eq!(StreamAllocatorState::Stable, env.state());
    assert_eq!(1, env.bwe.resets());
    env.sa.set_channel_capacity(0);
    env.flush();
    assert_eq!((900_000, false, 0.0), allocation(&a));
    assert_eq!((900_000, false, 0.0), allocation(&b));
    assert!(env.take_updates().is_empty());
    assert_eq!(1, env.bwe.resets());
    assert_eq!(vec![0.05], env.bwe.rtts());

    env.sa.stop();
}

// A probe is started when a track is deficient and the estimator allows it, and it is ended
// by the pacer's observer; what the estimator makes of the probe is then given to the
// deficient tracks.
#[tokio::test(flavor = "multi_thread")]
async fn a_probe_is_started_for_a_deficient_track_and_ended_by_the_pacers_observer() {
    let env = Env::new(DEFAULT_STREAM_ALLOCATOR_CONFIG, true);
    let a = env.add_track("TR_a", CAMERA, 10, false, &LAYERS).await;
    let b = env.add_track("TR_b", CAMERA, 5, false, &LAYERS).await;

    env.congestion_state_change(Clear, Congested, 150_000);
    assert_eq!((100_000, true, 2.0), allocation(&a));
    assert_eq!((0, true, 3.0), allocation(&b));
    assert_eq!(
        vec![update("TR_b", StreamState::Paused)],
        env.take_updates()
    );
    assert_eq!(0, env.pacer.start_probe_cluster_call_count());

    // the estimator allows a probe: the next ping asks the prober for one. The track with
    // the higher priority waits for its layer still, so the probe is for what the paused
    // track needs to start (100 kbps, which is less than the least a probe adds).
    env.bwe
        .finalize_on_done(ccutils::ProbeSignal::NotCongesting, 500_000);
    env.bwe.allow_probes(1);
    eventually_async(|| env.bwe.starting().len() == 1, WAIT, TICK).await;
    env.flush();
    let pci = env.bwe.starting()[0];
    assert_eq!(ccutils::ProbeClusterId(1), pci.id);
    assert_eq!(
        ccutils::ProbeClusterGoal {
            available_bandwidth_bps: 150_000,
            expected_usage_bps: 100_000,
            desired_bps: 300_000,
            duration: Duration::from_millis(200),
            desired_bytes: 7501,
        },
        pci.goal
    );
    // the pacer is told to observe it
    assert_eq!(1, env.pacer.start_probe_cluster_call_count());
    assert_eq!(pci, env.pacer.start_probe_cluster_args_for_call(0));
    assert_eq!(pci.id, env.sa.active_probe_cluster_id());
    assert_eq!(0, env.pacer.end_probe_cluster_call_count());

    // the pacer's observer has seen enough: it tells the listener the allocator gave it
    let mut ended = pci;
    ended.result = ccutils::ProbeClusterResult {
        start_time: 1,
        end_time: 2,
        is_completed: true,
        ..Default::default()
    };
    env.pacer.end_probe_cluster_returns(ended);
    let observer = env
        .pacer
        .set_pacer_probe_observer_listener_args_for_call(0)
        .expect("the allocator set itself as the listener");
    observer.on_pacer_probe_observer_cluster_complete(pci.id);
    env.flush();
    assert_eq!(1, env.pacer.end_probe_cluster_call_count());
    assert_eq!(pci.id, env.pacer.end_probe_cluster_args_for_call(0));
    // the estimator is given what the pacer counted
    assert_eq!(vec![ended], env.bwe.done());
    assert!(!env.sa.prober.is_running());

    // the next ping takes the estimator's verdict: 500 kbps, and no congestion
    eventually_async(
        || allocation(&a) == (300_000, true, 1.0) && allocation(&b) == (100_000, true, 2.0),
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(500_000, env.sa.committed_channel_capacity.load());
    assert_eq!(
        ccutils::PROBE_CLUSTER_ID_INVALID,
        env.sa.active_probe_cluster_id()
    );
    assert!(env.take_updates().is_empty());
    assert_eq!(1, env.bwe.starting().len());
    assert_eq!(1, env.pacer.start_probe_cluster_call_count());
    assert_eq!(0, env.bwe.resets());

    env.sa.stop();
}

// With media as the probe, every ping gives the deficient track that is farthest from what
// it wants its next layer and resets the estimator, until nothing is deficient.
#[tokio::test(flavor = "multi_thread")]
async fn a_probe_with_media_raises_the_deficient_tracks_layer_by_layer() {
    let env = Env::new(
        StreamAllocatorConfig {
            probe_mode: ProbeMode::Media,
            ..DEFAULT_STREAM_ALLOCATOR_CONFIG
        },
        true,
    );
    let a = env.add_track("TR_a", CAMERA, 10, false, &LAYERS).await;
    let b = env.add_track("TR_b", CAMERA, 5, false, &LAYERS).await;

    env.congestion_state_change(Clear, Congested, 150_000);
    assert_eq!((100_000, true, 2.0), allocation(&a));
    assert_eq!((0, true, 3.0), allocation(&b));
    assert_eq!(
        vec![update("TR_b", StreamState::Paused)],
        env.take_updates()
    );

    // two layers for the one, three for the other: five pings, each with a reset of the
    // estimator, and one more reset when the allocator is stable again
    env.bwe.allow_probes(100);
    eventually_async(|| env.state() == StreamAllocatorState::Stable, WAIT, TICK).await;
    env.flush();
    assert_eq!((900_000, false, 0.0), allocation(&a));
    assert_eq!((900_000, false, 0.0), allocation(&b));
    assert_eq!(6, env.bwe.resets());
    assert!(env.take_updates().is_empty());
    assert_eq!(0, env.pacer.start_probe_cluster_call_count());

    assert_eq!(1, write_key_frame(&b, 0));
    assert_eq!(
        vec![update("TR_b", StreamState::Active)],
        env.take_updates()
    );

    env.sa.stop();
}

// A change of the subscribed layers (`on_subscribed_layer_changed`), of the layers the
// publisher has (`on_available_layers_changed`) and of the subscription (a mute) each lead
// to a new allocation of the track.
#[tokio::test(flavor = "multi_thread")]
async fn a_change_of_subscribed_or_available_layers_gives_a_new_allocation() {
    let env = Env::new(DEFAULT_STREAM_ALLOCATOR_CONFIG, true);
    let a = env.add_track("TR_a", CAMERA, 10, false, &LAYERS).await;
    assert_eq!((900_000, false, 0.0), allocation(&a));
    assert!(env.take_updates().is_empty());

    // the stream starts
    assert_eq!(1, write_key_frame(&a, 2));
    assert_eq!(
        vec![update("TR_a", StreamState::Active)],
        env.take_updates()
    );

    // the subscriber wants less, then all again
    a.dt.set_max_spatial_layer(1);
    env.flush();
    assert_eq!((300_000, false, 0.0), allocation(&a));
    a.dt.set_max_spatial_layer(2);
    env.flush();
    assert_eq!((900_000, false, 0.0), allocation(&a));

    // the publisher stops its higher layers, then has all again
    a.receiver.get_layered_bitrate_returns(bitrates(&[100_000]));
    a.dt.up_track_layers_change();
    env.flush();
    assert_eq!((100_000, false, 0.0), allocation(&a));
    a.receiver.get_layered_bitrate_returns(bitrates(&LAYERS));
    a.dt.up_track_layers_change();
    env.flush();
    assert_eq!((900_000, false, 0.0), allocation(&a));

    // the subscriber mutes and unmutes
    a.dt.mute(true);
    env.flush();
    assert_eq!((0, false, 0.0), allocation(&a));
    a.dt.mute(false);
    env.flush();
    assert_eq!((900_000, false, 0.0), allocation(&a));

    // a temporal layer more is no more bits where the publisher has one temporal layer
    a.dt.set_max_temporal_layer(1);
    env.flush();
    assert_eq!((900_000, false, 0.0), allocation(&a));
    assert_eq!(
        buffer::VideoLayer {
            spatial: 2,
            temporal: 1
        },
        a.dt.max_layer()
    );

    // none of that is a stream that pauses or resumes
    assert!(env.take_updates().is_empty());
    assert_eq!(StreamAllocatorState::Stable, env.state());

    env.sa.stop();
}

// While the estimator gives an early warning, a track that asks for more is held (at its
// lowest layer, as the original has it); when the channel is clear again, the tracks are
// given their optimal layers.
#[tokio::test(flavor = "multi_thread")]
async fn an_early_warning_holds_the_allocation_until_the_channel_is_clear() {
    let env = Env::new(DEFAULT_STREAM_ALLOCATOR_CONFIG, true);
    let a = env.add_track("TR_a", CAMERA, 10, false, &LAYERS).await;
    assert_eq!(1, write_key_frame(&a, 2));
    assert_eq!(
        vec![update("TR_a", StreamState::Active)],
        env.take_updates()
    );
    a.dt.set_max_spatial_layer(1);
    env.flush();
    assert_eq!((300_000, false, 0.0), allocation(&a));

    // the warning by itself changes nothing
    env.bwe.set_congestion_state(EarlyWarning);
    env.congestion_state_change(Clear, EarlyWarning, 2_000_000);
    assert_eq!((300_000, false, 0.0), allocation(&a));

    // the subscriber wants the highest layer: held
    a.dt.set_max_spatial_layer(2);
    env.flush();
    assert_eq!((100_000, false, 2.0), allocation(&a));
    assert_eq!(StreamAllocatorState::Stable, env.state());

    // clear again: the hold ends
    env.bwe.set_congestion_state(Clear);
    env.congestion_state_change(EarlyWarning, Clear, 2_000_000);
    assert_eq!((900_000, false, 0.0), allocation(&a));
    assert!(env.take_updates().is_empty());
    // a warning is no congestion: the capacity is not committed
    assert_eq!(0, env.sa.committed_channel_capacity.load());

    env.sa.stop();
}

// A screen share with one layer is not managed: it is given what it needs before the other
// tracks, whatever the estimate, and what it uses is taken off the estimate.
#[tokio::test(flavor = "multi_thread")]
async fn a_screen_share_with_one_layer_is_exempt() {
    let env = Env::new(DEFAULT_STREAM_ALLOCATOR_CONFIG, true);
    let s = env
        .add_track("TR_s", SCREEN_SHARE, 0, false, &[400_000])
        .await;
    let a = env.add_track("TR_a", CAMERA, 0, false, &LAYERS).await;
    assert_eq!((400_000, false, 0.0), allocation(&s));
    assert_eq!((900_000, false, 0.0), allocation(&a));
    assert!(env.sa.is_bwe_enabled(&s.dt));
    assert!(env.sa.is_bwe_enabled(&a.dt));

    // 50 kbps are left for the camera: paused
    env.congestion_state_change(Clear, Congested, 450_000);
    assert_eq!((400_000, false, 0.0), allocation(&s));
    assert_eq!((0, true, 3.0), allocation(&a));
    assert_eq!(
        vec![update("TR_a", StreamState::Paused)],
        env.take_updates()
    );

    // 250 kbps are left for the camera: its lowest layer
    env.congestion_state_change(Congested, Congested, 650_000);
    assert_eq!((400_000, false, 0.0), allocation(&s));
    assert_eq!((100_000, true, 2.0), allocation(&a));
    assert!(env.take_updates().is_empty());

    env.sa.stop();

    // with `disable_etimation_unmanaged_tracks` the screen share is kept out of the
    // estimation, and what it uses is not taken off the estimate
    let env = Env::new(
        StreamAllocatorConfig {
            disable_estimation_unmanaged_tracks: true,
            ..DEFAULT_STREAM_ALLOCATOR_CONFIG
        },
        true,
    );
    let s = env
        .add_track("TR_s", SCREEN_SHARE, 0, false, &[400_000])
        .await;
    let a = env.add_track("TR_a", CAMERA, 0, false, &LAYERS).await;
    assert!(!env.sa.is_bwe_enabled(&s.dt));
    assert!(env.sa.is_bwe_enabled(&a.dt));

    env.congestion_state_change(Clear, Congested, 450_000);
    assert_eq!((400_000, false, 0.0), allocation(&s));
    assert_eq!((300_000, true, 1.0), allocation(&a));
    assert!(env.take_updates().is_empty());

    env.sa.stop();
}

// A screen share with layers is managed, with the highest priority unless it is given
// another: it gets its layers before a camera. A change of priority leads to a new
// allocation of all tracks.
#[tokio::test(flavor = "multi_thread")]
async fn a_screen_share_with_layers_is_prioritised() {
    let env = Env::new(DEFAULT_STREAM_ALLOCATOR_CONFIG, true);
    let a = env.add_track("TR_a", CAMERA, 0, false, &LAYERS).await;
    let m = env.add_track("TR_m", SCREEN_SHARE, 0, true, &LAYERS).await;
    assert_eq!((900_000, false, 0.0), allocation(&a));
    assert_eq!((900_000, false, 0.0), allocation(&m));

    // room for the lowest layer of both and the middle layer of one: the screen share's
    env.congestion_state_change(Clear, Congested, 500_000);
    assert_eq!((100_000, true, 2.0), allocation(&a));
    assert_eq!((300_000, true, 1.0), allocation(&m));

    // a higher priority for the camera that is still below the screen share's
    env.sa.set_track_priority(&a.dt, 2);
    env.flush();
    assert_eq!((100_000, true, 2.0), allocation(&a));
    assert_eq!((300_000, true, 1.0), allocation(&m));

    // the screen share below the camera
    env.sa.set_track_priority(&m.dt, 1);
    env.flush();
    assert_eq!((300_000, true, 1.0), allocation(&a));
    assert_eq!((100_000, true, 2.0), allocation(&m));

    // the same priority again is no change
    env.sa.set_track_priority(&m.dt, 1);
    env.flush();
    assert_eq!((300_000, true, 1.0), allocation(&a));
    assert_eq!((100_000, true, 2.0), allocation(&m));

    // and back to its default
    env.sa.set_track_priority(&m.dt, 0);
    env.flush();
    assert_eq!((100_000, true, 2.0), allocation(&a));
    assert_eq!((300_000, true, 1.0), allocation(&m));

    assert!(env.take_updates().is_empty());

    env.sa.stop();
}

// When the allocator is deficient, a track that changes is allocated by itself: one that
// gives bits back (a mute, a lower subscribed layer) lets deficient tracks have them, and
// one that needs bits (an unmute) takes them from the headroom, or else from the tracks
// that can best spare them; if there are none to spare it is paused, with a report.
#[tokio::test(flavor = "multi_thread")]
async fn a_track_that_changes_when_deficient_gives_or_takes_bits() {
    let env = Env::new(DEFAULT_STREAM_ALLOCATOR_CONFIG, true);
    let a = env.add_track("TR_a", CAMERA, 10, false, &LAYERS).await;
    let b = env.add_track("TR_b", CAMERA, 5, false, &LAYERS).await;

    env.congestion_state_change(Clear, Congested, 450_000);
    assert_eq!((300_000, true, 1.0), allocation(&a));
    assert_eq!((100_000, true, 2.0), allocation(&b));

    // a mute gives 100 kbps back: not enough for the other track's next layer
    b.dt.mute(true);
    env.flush();
    assert_eq!((300_000, true, 1.0), allocation(&a));
    assert_eq!((0, false, 0.0), allocation(&b));

    // no headroom is left
    env.congestion_state_change(Congested, Congested, 300_000);
    assert_eq!((300_000, true, 1.0), allocation(&a));
    assert_eq!((0, false, 0.0), allocation(&b));

    // the unmuted track needs 100 kbps to start: the other track goes down a layer for it
    b.dt.mute(false);
    env.flush();
    assert_eq!((100_000, true, 2.0), allocation(&a));
    assert_eq!((100_000, true, 2.0), allocation(&b));
    assert!(env.take_updates().is_empty());

    // muted again: the other track takes its layer back
    b.dt.mute(true);
    env.flush();
    assert_eq!((300_000, true, 1.0), allocation(&a));
    assert_eq!((0, false, 0.0), allocation(&b));

    // room for one lowest layer only
    env.congestion_state_change(Congested, Congested, 100_000);
    assert_eq!((100_000, true, 2.0), allocation(&a));
    assert_eq!((0, false, 0.0), allocation(&b));
    assert!(env.take_updates().is_empty());

    // nothing to take for the track that is unmuted: it is paused, and reported so
    b.dt.mute(false);
    env.flush();
    assert_eq!((100_000, true, 2.0), allocation(&a));
    assert_eq!((0, true, 3.0), allocation(&b));
    assert_eq!(
        vec![update("TR_b", StreamState::Paused)],
        env.take_updates()
    );

    env.congestion_state_change(Congested, Congested, 450_000);
    assert_eq!((300_000, true, 1.0), allocation(&a));
    assert_eq!((100_000, true, 2.0), allocation(&b));

    // a track that asks for less gives its bits to the deficient one
    a.dt.set_max_spatial_layer(0);
    env.flush();
    assert_eq!((100_000, false, 0.0), allocation(&a));
    assert_eq!((300_000, true, 1.0), allocation(&b));
    assert_eq!(StreamAllocatorState::Deficient, env.state());
    assert_eq!(0, env.bwe.resets());

    // the deficient track leaves: stable again, with a reset of the estimator
    env.sa.remove_track(&b.dt);
    env.flush();
    assert_eq!((100_000, false, 0.0), allocation(&a));
    assert_eq!(StreamAllocatorState::Stable, env.state());
    assert_eq!(1, env.bwe.resets());
    assert!(env.take_updates().is_empty());
    assert_eq!(1, env.sa.get_video_tracks().len());

    env.sa.stop();
}

// A REMB is reported by every down track of the connection: the allocator takes it from the
// one whose stream the report names, and gives the estimator the estimate with what the
// tracks are expected to use. Transport-cc feedback goes to the estimator as it is.
#[tokio::test(flavor = "multi_thread")]
async fn a_remb_reaches_the_estimator_once() {
    let env = Env::new(DEFAULT_STREAM_ALLOCATOR_CONFIG, true);
    let a = env.add_track("TR_a", CAMERA, 10, false, &LAYERS).await;
    let b = env.add_track("TR_b", CAMERA, 5, false, &LAYERS).await;
    assert_ne!(a.dt.ssrc(), b.dt.ssrc());
    assert_ne!(a.dt.ssrc_rtx(), b.dt.ssrc_rtx());

    let remb = |bitrate: f32, ssrcs: &[u32]| ReceiverEstimatedMaximumBitrate {
        sender_ssrc: 1,
        bitrate,
        ssrcs: ssrcs.to_vec(),
    };

    // a's stream, reported by a: taken. The two "packets" are one per down track that has
    // sent nothing yet, as the original counts them.
    let first = (500_000, 1_800_000, 2, 0);
    env.sa.on_remb(&a.dt, &remb(500_000.0, &[a.dt.ssrc()]));
    env.flush();
    assert_eq!(vec![first], env.bwe.rembs());

    // the same, reported by b: not taken
    env.sa.on_remb(&b.dt, &remb(500_000.0, &[a.dt.ssrc()]));
    env.flush();
    assert_eq!(vec![first], env.bwe.rembs());

    // no stream named: not taken
    env.sa.on_remb(&a.dt, &remb(550_000.0, &[]));
    env.flush();
    assert_eq!(vec![first], env.bwe.rembs());

    // a stream nobody has: not taken, and now the one that is tracked
    env.sa.on_remb(&a.dt, &remb(600_000.0, &[12345]));
    env.flush();
    assert_eq!(vec![first], env.bwe.rembs());

    // b's retransmission stream, reported by b: taken, and tracked from here on
    let second = (700_000, 1_800_000, 0, 0);
    env.sa
        .on_remb(&b.dt, &remb(700_000.0, &[0, b.dt.ssrc_rtx()]));
    env.flush();
    assert_eq!(vec![first, second], env.bwe.rembs());

    // the same stream, reported by a: not taken
    env.sa.on_remb(&a.dt, &remb(800_000.0, &[b.dt.ssrc_rtx()]));
    env.flush();
    assert_eq!(vec![first, second], env.bwe.rembs());

    let fb = TransportLayerCc {
        media_ssrc: 7,
        ..Default::default()
    };
    env.sa.on_transport_cc_feedback(&a.dt, &fb);
    env.flush();
    assert_eq!(vec![fb], env.bwe.twccs());

    // an estimate is no congestion: nothing was allocated anew
    assert_eq!((900_000, false, 0.0), allocation(&a));
    assert_eq!((900_000, false, 0.0), allocation(&b));
    assert!(env.take_updates().is_empty());

    env.sa.stop();
}

// From reading: who holds whom. The estimator, the pacer and the down tracks have the
// allocator as their listener and do not keep it alive, nor do its own workers: when its
// owner lets go of it, stopped or not, it goes. A track that is added again takes the place
// of the first, and audio is not the allocator's business.
#[tokio::test(flavor = "multi_thread")]
async fn an_allocator_is_let_go_of_when_its_owner_drops_it() {
    for stop in [true, false] {
        let env = Env::new(DEFAULT_STREAM_ALLOCATOR_CONFIG, true);
        let a = env.add_track("TR_a", CAMERA, 10, false, &LAYERS).await;
        env.congestion_state_change(Clear, Congested, 100_000);
        assert_eq!((100_000, true, 2.0), allocation(&a));

        // the same down track again: it takes the place of the first
        env.sa.add_track(a.dt.clone(), AddTrackParams::default());
        env.flush();
        assert_eq!(1, env.sa.get_video_tracks().len());
        assert_eq!((100_000, true, 2.0), allocation(&a));

        // a track that is removed is gone, and a down track the allocator does not have
        // is not its business
        let other = env.add_track("TR_o", CAMERA, 0, false, &LAYERS).await;
        assert_eq!(2, env.sa.get_video_tracks().len());
        env.sa.remove_track(&other.dt);
        env.flush();
        assert_eq!(1, env.sa.get_video_tracks().len());
        env.sa.remove_track(&other.dt);
        env.sa.set_track_priority(&other.dt, 3);
        env.flush();
        assert_eq!(1, env.sa.get_video_tracks().len());
        assert!(env.sa.is_subscribe_mutable(&other.dt));
        assert!(env.sa.is_bwe_enabled(&other.dt));
        assert_eq!(bwe::BweType::Remote, env.sa.bwe_type());

        let Env {
            bwe,
            pacer,
            sa,
            updates: _updates,
            num_tracks: _,
        } = env;
        if stop {
            sa.stop();
            // a second stop does nothing
            sa.stop();
        }
        let weak = Arc::downgrade(&sa);
        drop(sa);
        // a ping or an event that is being handled holds it for a moment
        eventually_async(|| weak.upgrade().is_none(), WAIT, TICK).await;

        // what is left behind talks to nobody
        bwe.notify(Clear, Congested, 1);
        pacer
            .set_pacer_probe_observer_listener_args_for_call(0)
            .unwrap()
            .on_pacer_probe_observer_cluster_complete(ccutils::ProbeClusterId(1));
        a.dt.up_track_layers_change();
        assert_eq!((100_000, true, 2.0), allocation(&a));
    }
}

// From reading: `start` is what makes the allocator handle events, and `stop` what ends
// that; the listeners it is made with are set by `new`, and a track is known to it from
// `add_track` on.
#[tokio::test(flavor = "multi_thread")]
async fn an_allocator_handles_events_from_its_start_to_its_stop() {
    let bwe = Arc::new(ScriptedBwe::default());
    let pacer = Arc::new(FakePacer::default());
    let sa = StreamAllocator::new(
        StreamAllocatorParams {
            config: DEFAULT_STREAM_ALLOCATOR_CONFIG,
            bwe: bwe.clone(),
            pacer: pacer.clone(),
            rtt_getter: None,
            logger: logger::get_logger(),
        },
        true,
        false,
    );
    assert!(bwe.lock.lock().listener.is_some());
    assert_eq!(1, pacer.set_pacer_probe_observer_listener_call_count());

    let dh = new_bound_down_track_with_ssrcs(
        new_buffer_factory(),
        vp8_codec_params(),
        pacer.clone(),
        MediaEngineConfig {
            video: true,
            header_extensions: true,
        },
        None,
        (0x0a0a_0001, 0x0a0a_0002),
        |r| r.get_layered_bitrate_returns(bitrates(&LAYERS)),
    )
    .await;
    sa.add_track(
        dh.dt.clone(),
        AddTrackParams {
            source: CAMERA,
            publisher_id: "pub".into(),
            ..Default::default()
        },
    );
    assert_eq!(1, sa.get_video_tracks().len());
    let track = sa.get_video_tracks()[0].clone();
    assert_eq!("fake-track", track.id().to_string());
    assert_eq!("pub", track.publisher_id().to_string());
    assert_eq!(C_PRIORITY_DEFAULT_VIDEO, track.priority());
    assert!(track.is_managed());
    assert!(Arc::ptr_eq(&dh.dt, track.down_track()));
    let receiver: Arc<dyn TrackReceiver> = dh.receiver.clone();
    assert!(Arc::ptr_eq(&receiver, &dh.dt.receiver()));

    let remb = ReceiverEstimatedMaximumBitrate {
        sender_ssrc: 1,
        bitrate: 500_000.0,
        ssrcs: vec![dh.dt.ssrc()],
    };

    // not started: the event waits
    sa.on_remb(&dh.dt, &remb);
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    assert!(bwe.rembs().is_empty());

    sa.start();
    eventually_async(|| bwe.rembs().len() == 1, WAIT, TICK).await;

    // stopped: the event is dropped
    sa.stop();
    sa.on_remb(&dh.dt, &remb);
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    assert_eq!(1, bwe.rembs().len());
}
