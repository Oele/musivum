// Ported from LiveKit, pkg/sfu/bwe/remotebwe/remote_bwe.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NewRemoteBWE` is `RemoteBwe::new` and gives an `Arc`,
// as the estimator is shared; the methods are the type's own, in the order of the original,
// and the `Bwe` trait is implemented on top of those at the end, with the two calls the
// original leaves to the `bwe.NullBWE` it embeds passed on to a `NullBwe`; the functions the
// original calls with its lock held take what the lock guards as an argument; the probe
// controller and the channel observer cannot be nil, so `new` makes a first pair of them
// before `reset` makes them again; `new_channel_observer` gives back the observer it replaces,
// which `probe_cluster_finalize` goes on reading where the original keeps the pointer; the
// `yaml` tags of the configuration are doc comments on its fields, as nothing reads a
// configuration file yet. The tests are new: the original has none.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::go::time::{Duration, Time};
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;
use crate::sfu::bwe;
use crate::sfu::ccutils;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;
use crate::{debugw, infow};

use super::channel_observer::{
    ChannelCongestionReason, ChannelObserver, ChannelObserverConfig, ChannelObserverParams,
    ChannelTrend, DEFAULT_CHANNEL_OBSERVER_CONFIG_NON_PROBE, DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE,
};
use super::probe_controller::{
    DEFAULT_PROBE_CONTROLLER_CONFIG, ProbeController, ProbeControllerConfig, ProbeControllerParams,
};

// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RemoteBweConfig {
    /// `nack_ratio_attenuator` in YAML.
    pub nack_ratio_attenuator: f64,
    /// `expected_usage_threshold` in YAML.
    pub expected_usage_threshold: f64,
    /// `channel_observer_probe` in YAML.
    pub channel_observer_probe: ChannelObserverConfig,
    /// `channel_observer_non_probe` in YAML.
    pub channel_observer_non_probe: ChannelObserverConfig,
    /// `probe_controller` in YAML.
    pub probe_controller: ProbeControllerConfig,
}

pub const DEFAULT_REMOTE_BWE_CONFIG: RemoteBweConfig = RemoteBweConfig {
    nack_ratio_attenuator: 0.4,
    expected_usage_threshold: 0.95,
    channel_observer_probe: DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE,
    channel_observer_non_probe: DEFAULT_CHANNEL_OBSERVER_CONFIG_NON_PROBE,
    probe_controller: DEFAULT_PROBE_CONTROLLER_CONFIG,
};

// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct RemoteBweParams {
    pub config: RemoteBweConfig,
    pub logger: Logger,
}

pub struct RemoteBwe {
    null_bwe: bwe::NullBwe,

    params: RemoteBweParams,

    lock: RwLock<RemoteBweInner>,
}

// What `lock` guards.
struct RemoteBweInner {
    last_received_estimate: i64,
    last_expected_bandwidth_usage: i64,
    committed_channel_capacity: i64,

    probe_controller: ProbeController,

    channel_observer: ChannelObserver,

    congestion_state: bwe::CongestionState,
    #[allow(dead_code)] // not read upstream either
    congestion_state_switched_at: Time,

    bwe_listener: Option<Arc<dyn bwe::BweListener>>,
}

impl RemoteBwe {
    pub fn new(params: RemoteBweParams) -> Arc<Self> {
        let r = Arc::new(Self {
            null_bwe: bwe::NullBwe::default(),
            lock: RwLock::new(RemoteBweInner {
                last_received_estimate: 0,
                last_expected_bandwidth_usage: 0,
                committed_channel_capacity: 0,
                probe_controller: ProbeController::new(ProbeControllerParams {
                    config: params.config.probe_controller,
                    logger: params.logger.clone(),
                }),
                channel_observer: ChannelObserver::new(
                    ChannelObserverParams {
                        name: "non-probe".to_string(),
                        config: params.config.channel_observer_non_probe,
                    },
                    params.logger.clone(),
                ),
                congestion_state: bwe::CongestionState::None,
                congestion_state_switched_at: Time::ZERO,
                bwe_listener: None,
            }),
            params,
        });

        r.reset();
        r
    }

    pub fn r#type(&self) -> bwe::BweType {
        bwe::BweType::Remote
    }

    pub fn set_bwe_listener(&self, bwe_listener: Option<Arc<dyn bwe::BweListener>>) {
        let mut inner = self.lock.write();

        inner.bwe_listener = bwe_listener;
    }

    fn get_bwe_listener(&self) -> Option<Arc<dyn bwe::BweListener>> {
        let inner = self.lock.read();

        inner.bwe_listener.clone()
    }

    pub fn reset(&self) {
        let mut inner = self.lock.write();

        inner.last_received_estimate = 0;
        inner.last_expected_bandwidth_usage = 0;
        inner.committed_channel_capacity = 100_000_000;

        inner.congestion_state = bwe::CongestionState::None;
        inner.congestion_state_switched_at = mono::now();

        inner.probe_controller = ProbeController::new(ProbeControllerParams {
            config: self.params.config.probe_controller,
            logger: self.params.logger.clone(),
        });

        self.new_channel_observer(&mut inner);
    }

    pub fn handle_remb(
        &self,
        received_estimate: i64,
        expected_bandwidth_usage: i64,
        sent_packets: u32,
        repeated_nacks: u32,
    ) {
        let mut inner = self.lock.write();
        inner.last_received_estimate = received_estimate;
        inner.last_expected_bandwidth_usage = expected_bandwidth_usage;

        // in probe, freeze channel observer state if probe causes congestion till the probe is done,
        // this is to ensure that probe result is not marked a success,
        // an unsuccessful probe will not up allocate any tracks
        if inner.congestion_state != bwe::CongestionState::None
            && inner.probe_controller.is_in_probe()
        {
            return;
        }

        let last_received_estimate = inner.last_received_estimate;
        inner.channel_observer.add_estimate(last_received_estimate);
        inner
            .channel_observer
            .add_nack(sent_packets, repeated_nacks);

        let (should_notify, from_state, to_state, committed_channel_capacity) =
            self.congestion_detection_state_machine(&mut inner);
        drop(inner);

        if should_notify && let Some(bwe_listener) = self.get_bwe_listener() {
            bwe_listener.on_congestion_state_change(
                from_state,
                to_state,
                committed_channel_capacity,
            );
        }
    }

    pub fn update_rtt(&self, rtt: f64) {
        let mut inner = self.lock.write();

        inner.probe_controller.update_rtt(rtt);
    }

    fn congestion_detection_state_machine(
        &self,
        inner: &mut RemoteBweInner,
    ) -> (bool, bwe::CongestionState, bwe::CongestionState, i64) {
        let mut from_state = inner.congestion_state;
        let mut to_state = inner.congestion_state;
        let mut update = false;
        let (trend, reason) = inner.channel_observer.get_trend();

        match from_state {
            bwe::CongestionState::None => {
                if trend == ChannelTrend::Congesting
                    && (inner.probe_controller.is_in_probe()
                        || self.estimate_available_channel_capacity(inner, reason))
                {
                    // when in probe, if congested, stays there till probe is done,
                    // the estimate stays at pre-probe level
                    to_state = bwe::CongestionState::Congested;
                }
            }

            bwe::CongestionState::Congested => {
                if trend == ChannelTrend::Congesting {
                    if self.estimate_available_channel_capacity(inner, reason) {
                        // update state as this needs to reset switch time to wait for congestion min duration again
                        update = true;
                    }
                } else {
                    to_state = bwe::CongestionState::None;
                }
            }

            // the original's switch has no case for the early warning, which this estimator
            // does not get into
            bwe::CongestionState::EarlyWarning => {}
        }

        let mut should_notify = false;
        if to_state != from_state || update {
            (from_state, to_state) = self.update_congestion_state(inner, to_state, reason);
            should_notify = true;
        }

        (
            should_notify,
            from_state,
            to_state,
            inner.committed_channel_capacity,
        )
    }

    fn estimate_available_channel_capacity(
        &self,
        inner: &mut RemoteBweInner,
        reason: ChannelCongestionReason,
    ) -> bool {
        let mut estimate_to_commit = match reason {
            ChannelCongestionReason::Loss => {
                (inner.last_expected_bandwidth_usage as f64
                    * (1.0
                        - self.params.config.nack_ratio_attenuator
                            * inner.channel_observer.get_nack_ratio())) as i64
            }
            _ => inner.last_received_estimate,
        };
        if estimate_to_commit > inner.last_received_estimate {
            estimate_to_commit = inner.last_received_estimate;
        }

        let commit_threshold = (self.params.config.expected_usage_threshold
            * inner.last_expected_bandwidth_usage as f64) as i64;
        if estimate_to_commit > commit_threshold
            || inner.committed_channel_capacity == estimate_to_commit
        {
            return false;
        }

        infow!(
            self.params.logger,
            "remote bwe: channel congestion detected, applying channel capacity update",
            "reason" => reason,
            "old(bps)" => inner.committed_channel_capacity,
            "new(bps)" => estimate_to_commit,
            "lastReceived(bps)" => inner.last_received_estimate,
            "expectedUsage(bps)" => inner.last_expected_bandwidth_usage,
            "commitThreshold(bps)" => commit_threshold,
            "channel" => inner.channel_observer,
        );
        inner.committed_channel_capacity = estimate_to_commit;
        true
    }

    fn update_congestion_state(
        &self,
        inner: &mut RemoteBweInner,
        state: bwe::CongestionState,
        reason: ChannelCongestionReason,
    ) -> (bwe::CongestionState, bwe::CongestionState) {
        debugw!(
            self.params.logger,
            "remote bwe: congestion state change",
            "from" => inner.congestion_state,
            "to" => state,
            "reason" => reason,
            "committedChannelCapacity" => inner.committed_channel_capacity,
        );

        let from_state = inner.congestion_state;
        inner.congestion_state = state;
        inner.congestion_state_switched_at = mono::now();
        (from_state, inner.congestion_state)
    }

    pub fn congestion_state(&self) -> bwe::CongestionState {
        let inner = self.lock.write();

        inner.congestion_state
    }

    pub fn can_probe(&self) -> bool {
        let inner = self.lock.write();

        inner.congestion_state == bwe::CongestionState::None && inner.probe_controller.can_probe()
    }

    pub fn probe_duration(&self) -> Duration {
        let inner = self.lock.write();

        inner.probe_controller.probe_regulator.probe_duration()
    }

    pub fn probe_cluster_starting(&self, pci: ccutils::ProbeClusterInfo) {
        let mut inner = self.lock.write();

        inner.last_expected_bandwidth_usage = pci.goal.expected_usage_bps;

        debugw!(
            self.params.logger,
            "remote bwe: starting probe",
            "lastReceived" => inner.last_received_estimate,
            "expectedBandwidthUsage" => inner.last_expected_bandwidth_usage,
            "channel" => inner.channel_observer,
        );

        inner.probe_controller.probe_cluster_starting(pci);
        self.new_channel_observer(&mut inner);
    }

    pub fn probe_cluster_done(&self, pci: ccutils::ProbeClusterInfo) {
        let mut inner = self.lock.write();

        inner.probe_controller.probe_cluster_done(pci);
    }

    pub fn probe_cluster_is_goal_reached(&self) -> bool {
        let inner = self.lock.write();

        if !inner.probe_controller.is_in_probe()
            || inner.congestion_state != bwe::CongestionState::None
            || !inner.channel_observer.has_enough_estimate_samples()
        {
            return false;
        }

        inner
            .probe_controller
            .probe_cluster_is_goal_reached(inner.channel_observer.get_highest_estimate())
    }

    pub fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool) {
        let mut inner = self.lock.write();

        let (pci, is_finalized) = inner.probe_controller.maybe_finalize_probe();
        if !is_finalized {
            return (ccutils::ProbeSignal::Inconclusive, 0, is_finalized);
        }

        // switch to a non-probe channel observer on probe end,
        // reset congestion state to get a fresh trend
        let probe_congestion_state = inner.congestion_state;

        inner.congestion_state = bwe::CongestionState::None;
        let pco = self.new_channel_observer(&mut inner);

        infow!(
            self.params.logger,
            "remote bwe: probe finalized",
            "lastReceived" => inner.last_received_estimate,
            "expectedBandwidthUsage" => inner.last_expected_bandwidth_usage,
            "channel" => pco,
            "isSignalValid" => pco.has_enough_estimate_samples(),
            "probeClusterInfo" => pci,
            "rtt" => inner.probe_controller.get_rtt(),
        );

        let mut probe_signal = ccutils::ProbeSignal::NotCongesting;
        if probe_congestion_state != bwe::CongestionState::None {
            probe_signal = ccutils::ProbeSignal::Congesting;
        } else if !pco.has_enough_estimate_samples() {
            probe_signal = ccutils::ProbeSignal::Inconclusive;
        } else {
            let highest_estimate = pco.get_highest_estimate();
            if highest_estimate > inner.committed_channel_capacity {
                inner.committed_channel_capacity = highest_estimate;
            }
        }

        inner
            .probe_controller
            .probe_regulator
            .probe_signal(probe_signal, pci.created_at);
        (probe_signal, inner.committed_channel_capacity, true)
    }

    fn new_channel_observer(&self, inner: &mut RemoteBweInner) -> ChannelObserver {
        let params = if inner.probe_controller.is_in_probe() {
            ChannelObserverParams {
                name: "probe".to_string(),
                config: self.params.config.channel_observer_probe,
            }
        } else {
            ChannelObserverParams {
                name: "non-probe".to_string(),
                config: self.params.config.channel_observer_non_probe,
            }
        };

        std::mem::replace(
            &mut inner.channel_observer,
            ChannelObserver::new(params, self.params.logger.clone()),
        )
    }
}

// The interface, on top of the methods above and of the `NullBwe` for the two that the
// original leaves to the `bwe.NullBWE` it embeds.
impl bwe::Bwe for RemoteBwe {
    fn r#type(&self) -> bwe::BweType {
        RemoteBwe::r#type(self)
    }

    fn set_bwe_listener(&self, bwe_listner: Option<Arc<dyn bwe::BweListener>>) {
        RemoteBwe::set_bwe_listener(self, bwe_listner)
    }

    fn reset(&self) {
        RemoteBwe::reset(self)
    }

    fn handle_remb(
        &self,
        received_estimate: i64,
        expected_bandwidth_usage: i64,
        sent_packets: u32,
        repeated_nacks: u32,
    ) {
        RemoteBwe::handle_remb(
            self,
            received_estimate,
            expected_bandwidth_usage,
            sent_packets,
            repeated_nacks,
        )
    }

    fn record_packet_send_and_get_sequence_number(
        &self,
        at_micro: i64,
        size: usize,
        is_rtx: bool,
        probe_cluster_id: ccutils::ProbeClusterId,
        is_probe: bool,
    ) -> u16 {
        self.null_bwe.record_packet_send_and_get_sequence_number(
            at_micro,
            size,
            is_rtx,
            probe_cluster_id,
            is_probe,
        )
    }

    fn handle_twcc_feedback(&self, report: &TransportLayerCc) {
        self.null_bwe.handle_twcc_feedback(report)
    }

    fn update_rtt(&self, rtt: f64) {
        RemoteBwe::update_rtt(self, rtt)
    }

    fn congestion_state(&self) -> bwe::CongestionState {
        RemoteBwe::congestion_state(self)
    }

    fn can_probe(&self) -> bool {
        RemoteBwe::can_probe(self)
    }

    fn probe_duration(&self) -> Duration {
        RemoteBwe::probe_duration(self)
    }

    fn probe_cluster_starting(&self, pci: ccutils::ProbeClusterInfo) {
        RemoteBwe::probe_cluster_starting(self, pci)
    }

    fn probe_cluster_done(&self, pci: ccutils::ProbeClusterInfo) {
        RemoteBwe::probe_cluster_done(self, pci)
    }

    fn probe_cluster_is_goal_reached(&self) -> bool {
        RemoteBwe::probe_cluster_is_goal_reached(self)
    }

    fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool) {
        RemoteBwe::probe_cluster_finalize(self)
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::{OnceLock, Weak, mpsc};

    use parking_lot::Mutex;

    use super::*;
    use crate::go::time;
    use crate::sfu::bwe::Bwe;

    #[derive(Default)]
    struct Listener {
        r: OnceLock<Weak<RemoteBwe>>,
        events: Mutex<Vec<String>>,
    }

    impl Listener {
        fn take(&self) -> Vec<String> {
            std::mem::take(&mut *self.events.lock())
        }
    }

    impl bwe::BweListener for Listener {
        fn on_congestion_state_change(
            &self,
            from_state: bwe::CongestionState,
            to_state: bwe::CongestionState,
            estimated_available_channel_capacity: i64,
        ) {
            // the estimator can be called from the callback: its lock is not held. It is
            // asked from another thread, so that a lock that is held fails the test and does
            // not hang it.
            let r = self.r.get().and_then(Weak::upgrade).expect("the estimator");
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                let _ = tx.send(r.congestion_state());
            });
            let seen = match rx.recv_timeout(std::time::Duration::from_secs(2)) {
                Ok(state) => state.to_string(),
                Err(_) => "(the estimator is locked)".to_string(),
            };
            self.events.lock().push(format!(
                "{from_state}->{to_state} capacity={estimated_available_channel_capacity} seen={seen}"
            ));
        }
    }

    // An estimator with the original's configuration and a listener, called through the
    // trait as the rest of the server calls it. What its methods give is what the harness
    // that ran the original printed after each step.
    struct Session {
        r: Arc<RemoteBwe>,
        bwe: Arc<dyn Bwe>,
        l: Arc<Listener>,
    }

    impl Session {
        fn new() -> Self {
            let r = RemoteBwe::new(RemoteBweParams {
                config: DEFAULT_REMOTE_BWE_CONFIG,
                ..Default::default()
            });
            let l = Arc::new(Listener::default());
            l.r.set(Arc::downgrade(&r)).expect("set once");
            let bwe: Arc<dyn Bwe> = r.clone();
            bwe.set_bwe_listener(Some(l.clone()));
            Self { r, bwe, l }
        }

        // The congestion state after the report, and what the listener was told by it.
        fn remb(
            &self,
            received_estimate: i64,
            expected_bandwidth_usage: i64,
            sent_packets: u32,
            repeated_nacks: u32,
        ) -> String {
            self.bwe.handle_remb(
                received_estimate,
                expected_bandwidth_usage,
                sent_packets,
                repeated_nacks,
            );
            format!(
                "state={} events={:?}",
                self.bwe.congestion_state(),
                self.l.take()
            )
        }

        fn status(&self) -> String {
            let state = self.bwe.congestion_state();
            let can_probe = self.bwe.can_probe();
            let probe_duration = self.bwe.probe_duration();
            let goal_reached = self.bwe.probe_cluster_is_goal_reached();
            let events = self.l.take();

            let inner = self.r.lock.read();
            // ChannelObserver { name: "probe", ...
            let observer = format!("{:?}", inner.channel_observer);
            let observer = observer.split('"').nth(1).expect("a name");
            format!(
                "state={state} canProbe={can_probe} probeDuration={probe_duration} \
                 goalReached={goal_reached} events={events:?} \
                 lastReceived={} expectedUsage={} committed={} observer={observer}",
                inner.last_received_estimate,
                inner.last_expected_bandwidth_usage,
                inner.committed_channel_capacity,
            )
        }

        fn finalize(&self) -> String {
            let (probe_signal, capacity, ok) = self.bwe.probe_cluster_finalize();
            format!("signal={probe_signal} capacity={capacity} ok={ok}")
        }
    }

    fn pci(id: u32, expected_usage_bps: i64, desired_bps: i64) -> ccutils::ProbeClusterInfo {
        ccutils::ProbeClusterInfo {
            id: ccutils::ProbeClusterId(id),
            created_at: mono::now(),
            goal: ccutils::ProbeClusterGoal {
                available_bandwidth_bps: 1_200_000,
                expected_usage_bps,
                desired_bps,
                duration: 200 * time::MILLISECOND,
                desired_bytes: desired_bps / 40,
            },
            ..Default::default()
        }
    }

    fn result() -> ccutils::ProbeClusterResult {
        ccutils::ProbeClusterResult {
            start_time: 1_000_000_000,
            end_time: 1_200_000_000,
            packets_probe: 20,
            bytes_probe: 5000,
            is_completed: true,
            ..Default::default()
        }
    }

    fn sleep(d: Duration) {
        std::thread::sleep(d.to_std());
    }

    const QUIET: &str = "state=NONE events=[]";
    const STILL_CONGESTED: &str = "state=CONGESTED events=[]";

    // New for musivum: the original has no tests. The expectations of the tests here are
    // what the original gives for the same calls with the same sleeps between them (a Go
    // test laid over the package, which printed these lines).
    #[test]
    fn a_session_of_estimates_and_probes() {
        let s = Session::new();
        assert_eq!(s.bwe.r#type(), bwe::BweType::Remote);
        sleep(5 * time::MILLISECOND);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=true probeDuration=200ms goalReached=false events=[] \
             lastReceived=0 expectedUsage=0 committed=100000000 observer=non-probe"
        );
        assert_eq!(s.finalize(), "signal=INCONCLUSIVE capacity=0 ok=false");

        // the estimate rises
        for i in 0..12 {
            assert_eq!(s.remb(2_000_000 + i * 100_000, 1_500_000, 100, 0), QUIET);
        }

        // the estimate falls: congested once it is under 95% of what is expected to be
        // used, at the estimate, and again at every lower one
        for i in 0..=18 {
            let estimate = 3_000_000 - i * 100_000;
            let expected = match estimate {
                1_400_000 => {
                    r#"state=CONGESTED events=["NONE->CONGESTED capacity=1400000 seen=CONGESTED"]"#
                }
                1_300_000 => {
                    r#"state=CONGESTED events=["CONGESTED->CONGESTED capacity=1300000 seen=CONGESTED"]"#
                }
                1_200_000 => {
                    r#"state=CONGESTED events=["CONGESTED->CONGESTED capacity=1200000 seen=CONGESTED"]"#
                }
                _ => QUIET,
            };
            assert_eq!(
                s.remb(estimate, 1_500_000, 100, 0),
                expected,
                "estimate {estimate}"
            );
        }

        // the estimate stays
        assert_eq!(s.remb(1_200_000, 1_500_000, 100, 0), STILL_CONGESTED);
        assert_eq!(s.remb(1_200_000, 1_500_000, 100, 0), STILL_CONGESTED);

        // the estimate recovers: the capacity follows it up while the trend is still
        // falling, and the congestion ends when it is not
        for i in 0..12 {
            let estimate = 1_300_000 + i * 100_000;
            let expected = match estimate {
                1_300_000 => {
                    r#"state=CONGESTED events=["CONGESTED->CONGESTED capacity=1300000 seen=CONGESTED"]"#
                }
                1_400_000 => {
                    r#"state=CONGESTED events=["CONGESTED->CONGESTED capacity=1400000 seen=CONGESTED"]"#
                }
                1_500_000 => STILL_CONGESTED,
                1_600_000 => r#"state=NONE events=["CONGESTED->NONE capacity=1400000 seen=NONE"]"#,
                _ => QUIET,
            };
            assert_eq!(
                s.remb(estimate, 1_500_000, 100, 0),
                expected,
                "estimate {estimate}"
            );
        }

        // more is expected to be used than the estimate, which does not fall
        assert_eq!(s.remb(2_500_000, 4_000_000, 100, 0), QUIET);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=true probeDuration=200ms goalReached=false events=[] \
             lastReceived=2500000 expectedUsage=4000000 committed=1400000 observer=non-probe"
        );

        // a probe that reaches its goal
        let mut pci1 = pci(1, 2_000_000, 3_000_000);
        s.bwe.probe_cluster_starting(pci1);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=200ms goalReached=false events=[] \
             lastReceived=2500000 expectedUsage=2000000 committed=1400000 observer=probe"
        );
        assert_eq!(s.remb(2_600_000, 2_000_000, 50, 0), QUIET);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=200ms goalReached=false events=[] \
             lastReceived=2600000 expectedUsage=2000000 committed=1400000 observer=probe"
        );
        assert_eq!(s.remb(2_900_000, 2_000_000, 50, 0), QUIET);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=200ms goalReached=false events=[] \
             lastReceived=2900000 expectedUsage=2000000 committed=1400000 observer=probe"
        );
        assert_eq!(s.remb(3_100_000, 2_000_000, 50, 0), QUIET);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=200ms goalReached=true events=[] \
             lastReceived=3100000 expectedUsage=2000000 committed=1400000 observer=probe"
        );
        pci1.result = result();
        s.bwe.probe_cluster_done(pci1);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=200ms goalReached=true events=[] \
             lastReceived=3100000 expectedUsage=2000000 committed=1400000 observer=probe"
        );
        // the hangover lasts five round trips: 350 ms
        assert_eq!(s.finalize(), "signal=INCONCLUSIVE capacity=0 ok=false");
        sleep(400 * time::MILLISECOND);
        assert_eq!(
            s.finalize(),
            "signal=NOT_CONGESTING capacity=3100000 ok=true"
        );
        assert_eq!(s.finalize(), "signal=INCONCLUSIVE capacity=0 ok=false");
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=300ms goalReached=false events=[] \
             lastReceived=3100000 expectedUsage=2000000 committed=3100000 observer=non-probe"
        );

        // a probe that congests, with estimates under what is expected to be used and one
        // over the goal: the capacity stays what it was before the probe, the goal is not
        // reached, and nothing more is taken in until the probe is finalized, which ends
        // the congestion unannounced
        let mut pci2 = pci(2, 3_500_000, 4_000_000);
        s.bwe.probe_cluster_starting(pci2);
        assert_eq!(s.remb(4_100_000, 3_500_000, 50, 0), QUIET);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=300ms goalReached=false events=[] \
             lastReceived=4100000 expectedUsage=3500000 committed=3100000 observer=probe"
        );
        assert_eq!(s.remb(3_300_000, 3_500_000, 50, 0), QUIET);
        assert_eq!(
            s.remb(3_200_000, 3_500_000, 50, 0),
            r#"state=CONGESTED events=["NONE->CONGESTED capacity=3100000 seen=CONGESTED"]"#
        );
        assert_eq!(s.remb(1_000_000, 3_500_000, 50, 0), STILL_CONGESTED);
        assert_eq!(s.remb(5_000_000, 3_500_000, 50, 0), STILL_CONGESTED);
        assert_eq!(
            s.status(),
            "state=CONGESTED canProbe=false probeDuration=300ms goalReached=false events=[] \
             lastReceived=5000000 expectedUsage=3500000 committed=3100000 observer=probe"
        );
        pci2.result = result();
        s.bwe.probe_cluster_done(pci2);
        sleep(400 * time::MILLISECOND);
        assert_eq!(s.finalize(), "signal=CONGESTING capacity=3100000 ok=true");
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=200ms goalReached=false events=[] \
             lastReceived=5000000 expectedUsage=3500000 committed=3100000 observer=non-probe"
        );

        // a probe with too few estimates, and a longer round trip: a hangover of 1 s
        s.bwe.update_rtt(0.33);
        let mut pci3 = pci(3, 2_500_000, 4_000_000);
        s.bwe.probe_cluster_starting(pci3);
        assert_eq!(s.remb(4_500_000, 2_500_000, 50, 0), QUIET);
        assert_eq!(s.remb(4_600_000, 2_500_000, 50, 0), QUIET);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=200ms goalReached=false events=[] \
             lastReceived=4600000 expectedUsage=2500000 committed=3100000 observer=probe"
        );
        pci3.result = result();
        s.bwe.probe_cluster_done(pci3);
        sleep(400 * time::MILLISECOND);
        assert_eq!(s.finalize(), "signal=INCONCLUSIVE capacity=0 ok=false");
        sleep(700 * time::MILLISECOND);
        assert_eq!(s.finalize(), "signal=INCONCLUSIVE capacity=3100000 ok=true");
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=300ms goalReached=false events=[] \
             lastReceived=4600000 expectedUsage=2500000 committed=3100000 observer=non-probe"
        );

        // congested again, and reset
        for i in 0..12 {
            let estimate = 2_400_000 - i * 100_000;
            let expected = match estimate {
                1_300_000 => {
                    r#"state=CONGESTED events=["NONE->CONGESTED capacity=1300000 seen=CONGESTED"]"#
                }
                _ => QUIET,
            };
            assert_eq!(
                s.remb(estimate, 3_000_000, 100, 0),
                expected,
                "estimate {estimate}"
            );
        }
        s.bwe.reset();
        sleep(5 * time::MILLISECOND);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=true probeDuration=200ms goalReached=false events=[] \
             lastReceived=0 expectedUsage=0 committed=100000000 observer=non-probe"
        );
        // the capacity is the first one again
        let pci4 = pci(4, 2_500_000, 4_000_000);
        s.bwe.probe_cluster_starting(pci4);
        s.bwe.probe_cluster_done(pci4);
        sleep(400 * time::MILLISECOND);
        assert_eq!(
            s.finalize(),
            "signal=INCONCLUSIVE capacity=100000000 ok=true"
        );
        // and the listener is still there
        for i in 0..12 {
            let estimate = 2_400_000 - i * 100_000;
            let expected = match estimate {
                1_300_000 => {
                    r#"state=CONGESTED events=["NONE->CONGESTED capacity=1300000 seen=CONGESTED"]"#
                }
                _ => QUIET,
            };
            assert_eq!(
                s.remb(estimate, 3_000_000, 100, 0),
                expected,
                "estimate {estimate}"
            );
        }

        // no listener
        s.bwe.set_bwe_listener(None);
        for i in 0..3 {
            assert_eq!(
                s.remb(1_200_000 - i * 100_000, 3_000_000, 100, 0),
                STILL_CONGESTED
            );
        }
    }

    // New for musivum.
    #[test]
    fn lasting_loss_congests_at_what_is_expected_to_be_used_less_the_loss() {
        let s = Session::new();
        assert_eq!(s.remb(3_000_000, 1_500_000, 100, 0), QUIET);
        // repeated NACKs: without a probe they count after 2 s, for at most 3 s
        assert_eq!(s.remb(3_000_000, 1_500_000, 100, 30), QUIET);
        assert_eq!(s.remb(3_000_000, 1_500_000, 100, 30), QUIET);
        sleep(2100 * time::MILLISECOND);
        // 60 of 300: what is expected to be used, less 0.4 times that share of it
        assert_eq!(
            s.remb(3_000_000, 1_500_000, 100, 0),
            r#"state=CONGESTED events=["NONE->CONGESTED capacity=1380000 seen=CONGESTED"]"#
        );
        // 60 of 400
        assert_eq!(
            s.remb(3_000_000, 1_500_000, 100, 0),
            r#"state=CONGESTED events=["CONGESTED->CONGESTED capacity=1410000 seen=CONGESTED"]"#
        );
        // 60 of 500 is above 95% of what is expected to be used: not taken
        assert_eq!(s.remb(3_000_000, 1_500_000, 100, 0), STILL_CONGESTED);
        // an estimate below that is
        assert_eq!(
            s.remb(1_400_000, 1_500_000, 0, 0),
            r#"state=CONGESTED events=["CONGESTED->CONGESTED capacity=1400000 seen=CONGESTED"]"#
        );
        assert_eq!(s.remb(1_400_000, 1_500_000, 0, 0), STILL_CONGESTED);
        assert_eq!(
            s.status(),
            "state=CONGESTED canProbe=false probeDuration=200ms goalReached=false events=[] \
             lastReceived=1400000 expectedUsage=1500000 committed=1400000 observer=non-probe"
        );

        // the window of the NACKs ends, and the congestion with it
        sleep(1000 * time::MILLISECOND);
        assert_eq!(
            s.remb(3_000_000, 1_500_000, 100, 0),
            r#"state=NONE events=["CONGESTED->NONE capacity=1400000 seen=NONE"]"#
        );
        assert_eq!(
            s.status(),
            "state=NONE canProbe=true probeDuration=200ms goalReached=false events=[] \
             lastReceived=3000000 expectedUsage=1500000 committed=1400000 observer=non-probe"
        );
    }

    // New for musivum. A new probe controller has the invalid cluster, so that one being
    // done counts as its own: ported as it is.
    #[test]
    fn the_invalid_cluster_being_done_is_a_probe_without_estimates() {
        let s = Session::new();
        sleep(5 * time::MILLISECOND);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=true probeDuration=200ms goalReached=false events=[] \
             lastReceived=0 expectedUsage=0 committed=100000000 observer=non-probe"
        );
        s.bwe
            .probe_cluster_done(ccutils::PROBE_CLUSTER_INFO_INVALID);
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=200ms goalReached=false events=[] \
             lastReceived=0 expectedUsage=0 committed=100000000 observer=non-probe"
        );
        assert_eq!(s.remb(2_000_000, 1_500_000, 100, 0), QUIET);
        sleep(400 * time::MILLISECOND);
        assert_eq!(
            s.finalize(),
            "signal=INCONCLUSIVE capacity=100000000 ok=true"
        );
        assert_eq!(
            s.status(),
            "state=NONE canProbe=false probeDuration=300ms goalReached=false events=[] \
             lastReceived=2000000 expectedUsage=1500000 committed=100000000 observer=non-probe"
        );
    }

    // New for musivum. The values are the original's (printed by the harness).
    #[test]
    fn the_defaults_are_the_originals_and_the_rest_of_the_interface_does_nothing() {
        assert_eq!(
            DEFAULT_REMOTE_BWE_CONFIG,
            RemoteBweConfig {
                nack_ratio_attenuator: 0.4,
                expected_usage_threshold: 0.95,
                channel_observer_probe: DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE,
                channel_observer_non_probe: DEFAULT_CHANNEL_OBSERVER_CONFIG_NON_PROBE,
                probe_controller: DEFAULT_PROBE_CONTROLLER_CONFIG,
            }
        );
        // a Go struct that leaves everything out
        assert_eq!(RemoteBweConfig::default().nack_ratio_attenuator, 0.0);

        // what the original leaves to the bwe.NullBWE it embeds
        let s = Session::new();
        assert_eq!(
            s.bwe.record_packet_send_and_get_sequence_number(
                1_000,
                1200,
                false,
                ccutils::PROBE_CLUSTER_ID_INVALID,
                false
            ),
            0
        );
        s.bwe.handle_twcc_feedback(&TransportLayerCc::default());
        assert_eq!(s.bwe.congestion_state(), bwe::CongestionState::None);
        assert!(s.l.take().is_empty());
    }
}
