// Ported from LiveKit, pkg/sfu/bwe/remotebwe/probe_controller.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `probeControllerState` is an enum and its `String()` is
// `Display`; the `*ccutils.ProbeRegulator` the original embeds is the field `probe_regulator`,
// through which the two methods are called that the original has from it (`p.ProbeDuration()`
// is `p.probe_regulator.probe_duration()`, and so for `ProbeSignal`); the `yaml` tags of the
// configuration are doc comments on its fields, as nothing reads a configuration file yet.
// The tests at the end are new: the original has none.

use std::fmt;

use crate::go::time::{self, Duration, Time};
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;
use crate::sfu::bwe;
use crate::sfu::ccutils;
use crate::warnw;

// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(super) enum ProbeControllerState {
    #[default]
    None,
    Probing,
    Hangover,
}

impl fmt::Display for ProbeControllerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ProbeControllerState::None => "NONE",
            ProbeControllerState::Probing => "PROBING",
            ProbeControllerState::Hangover => "HANGOVER",
        })
    }
}

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ProbeControllerConfig {
    /// `probe_regulator` in YAML.
    pub probe_regulator: ccutils::ProbeRegulatorConfig,

    /// `settle_wait_num_rtt` in YAML.
    pub settle_wait_num_rtt: u32,
    /// `settle_wait_min` in YAML.
    pub settle_wait_min: Duration,
    /// `settle_wait_max` in YAML.
    pub settle_wait_max: Duration,
}

pub const DEFAULT_PROBE_CONTROLLER_CONFIG: ProbeControllerConfig = ProbeControllerConfig {
    probe_regulator: ccutils::DEFAULT_PROBE_REGULATOR_CONFIG,

    settle_wait_num_rtt: 5,
    settle_wait_min: Duration::from_millis(250),
    settle_wait_max: Duration::from_secs(5),
};

// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub(super) struct ProbeControllerParams {
    pub(super) config: ProbeControllerConfig,
    pub(super) logger: Logger,
}

pub(super) struct ProbeController {
    params: ProbeControllerParams,

    state: ProbeControllerState,
    state_switched_at: Time,

    pci: ccutils::ProbeClusterInfo,
    rtt: f64,

    pub(super) probe_regulator: ccutils::ProbeRegulator,
}

impl ProbeController {
    pub(super) fn new(params: ProbeControllerParams) -> Self {
        Self {
            state: ProbeControllerState::None,
            state_switched_at: mono::now(),
            pci: ccutils::PROBE_CLUSTER_INFO_INVALID,
            rtt: bwe::DEFAULT_RTT,
            probe_regulator: ccutils::ProbeRegulator::new(ccutils::ProbeRegulatorParams {
                config: params.config.probe_regulator,
                logger: params.logger.clone(),
            }),
            params,
        }
    }

    pub(super) fn update_rtt(&mut self, rtt: f64) {
        if rtt == 0.0 {
            self.rtt = bwe::DEFAULT_RTT;
        } else if self.rtt == 0.0 {
            self.rtt = rtt;
        } else {
            self.rtt =
                bwe::RTT_SMOOTHING_FACTOR * rtt + (1.0 - bwe::RTT_SMOOTHING_FACTOR) * self.rtt;
        }
    }

    pub(super) fn get_rtt(&self) -> f64 {
        self.rtt
    }

    pub(super) fn can_probe(&self) -> bool {
        self.state == ProbeControllerState::None && self.probe_regulator.can_probe()
    }

    pub(super) fn is_in_probe(&self) -> bool {
        self.state != ProbeControllerState::None
    }

    pub(super) fn probe_cluster_starting(&mut self, pci: ccutils::ProbeClusterInfo) {
        if self.state != ProbeControllerState::None {
            warnw!(
                self.params.logger,
                "unexpected probe controller state",
                nil,
                "state" => self.state,
            );
        }

        self.set_state(ProbeControllerState::Probing);
        self.pci = pci;
    }

    pub(super) fn probe_cluster_done(&mut self, pci: ccutils::ProbeClusterInfo) {
        if self.pci.id != pci.id {
            return;
        }

        self.pci.result = pci.result;
        self.set_state(ProbeControllerState::Hangover);
    }

    pub(super) fn probe_cluster_is_goal_reached(&self, estimate: i64) -> bool {
        if self.pci.id == ccutils::PROBE_CLUSTER_ID_INVALID {
            return false;
        }

        estimate > self.pci.goal.desired_bps
    }

    pub(super) fn maybe_finalize_probe(&mut self) -> (ccutils::ProbeClusterInfo, bool) {
        if self.state != ProbeControllerState::Hangover {
            return (ccutils::PROBE_CLUSTER_INFO_INVALID, false);
        }

        let mut settle_wait =
            Duration::from_secs_f64(self.params.config.settle_wait_num_rtt as f64 * self.rtt);
        if settle_wait < self.params.config.settle_wait_min {
            settle_wait = self.params.config.settle_wait_min;
        }
        if settle_wait > self.params.config.settle_wait_max {
            settle_wait = self.params.config.settle_wait_max;
        }
        if time::since(self.state_switched_at) < settle_wait {
            return (ccutils::PROBE_CLUSTER_INFO_INVALID, false);
        }

        self.set_state(ProbeControllerState::None);
        (self.pci, true)
    }

    fn set_state(&mut self, state: ProbeControllerState) {
        if state == self.state {
            return;
        }

        self.state = state;
        self.state_switched_at = mono::now();
    }
}

// ------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn new_controller() -> ProbeController {
        ProbeController::new(ProbeControllerParams {
            config: DEFAULT_PROBE_CONTROLLER_CONFIG,
            ..Default::default()
        })
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

    // What the harness that ran the original printed after each step.
    fn state(p: &ProbeController) -> String {
        format!(
            "state={} inProbe={} canProbe={} rtt={} probeDuration={}",
            p.state,
            p.is_in_probe(),
            p.can_probe(),
            p.get_rtt(),
            p.probe_regulator.probe_duration()
        )
    }

    // The same for an attempt to finalize, with the cluster it gave.
    fn finalize(p: &mut ProbeController) -> (String, ccutils::ProbeClusterInfo) {
        let (pci, ok) = p.maybe_finalize_probe();
        (
            format!(
                "ok={ok} id={} desiredBps={} bytesProbe={} isCompleted={}",
                pci.id, pci.goal.desired_bps, pci.result.bytes_probe, pci.result.is_completed
            ),
            pci,
        )
    }

    const NOT_FINALIZED: &str = "ok=false id=0 desiredBps=0 bytesProbe=0 isCompleted=false";

    fn sleep(d: Duration) {
        std::thread::sleep(d.to_std());
    }

    // New for musivum: the original has no tests. The expectations of the tests here are
    // what the original gives for the same calls with the same sleeps between them (a Go
    // test laid over the package, which printed these lines).
    #[test]
    fn a_probe_goes_from_probing_through_the_hangover_back_to_none() {
        let mut p = new_controller();
        sleep(5 * time::MILLISECOND);
        assert_eq!(
            state(&p),
            "state=NONE inProbe=false canProbe=true rtt=0.07 probeDuration=200ms"
        );
        // no cluster, no goal to reach and nothing to finalize
        assert!(!p.probe_cluster_is_goal_reached(10_000_000));
        assert_eq!(
            finalize(&mut p),
            (
                NOT_FINALIZED.to_string(),
                ccutils::PROBE_CLUSTER_INFO_INVALID
            )
        );

        let mut pci1 = pci(1, 600_000, 1_000_000);
        p.probe_cluster_starting(pci1);
        assert_eq!(
            state(&p),
            "state=PROBING inProbe=true canProbe=false rtt=0.07 probeDuration=200ms"
        );
        // the goal is reached above the desired rate
        assert!(!p.probe_cluster_is_goal_reached(1_000_000));
        assert!(p.probe_cluster_is_goal_reached(1_000_001));
        assert_eq!(finalize(&mut p).0, NOT_FINALIZED);

        // another cluster being done is not this one
        let mut other = pci(2, 600_000, 1_000_000);
        other.result = result();
        p.probe_cluster_done(other);
        assert_eq!(
            state(&p),
            "state=PROBING inProbe=true canProbe=false rtt=0.07 probeDuration=200ms"
        );

        pci1.result = result();
        p.probe_cluster_done(pci1);
        assert_eq!(
            state(&p),
            "state=HANGOVER inProbe=true canProbe=false rtt=0.07 probeDuration=200ms"
        );
        assert!(p.probe_cluster_is_goal_reached(1_000_001));

        // the hangover lasts five round trips: 350 ms
        assert_eq!(finalize(&mut p).0, NOT_FINALIZED);
        assert_eq!(
            state(&p),
            "state=HANGOVER inProbe=true canProbe=false rtt=0.07 probeDuration=200ms"
        );
        sleep(400 * time::MILLISECOND);
        assert_eq!(
            finalize(&mut p),
            (
                "ok=true id=1 desiredBps=1000000 bytesProbe=5000 isCompleted=true".to_string(),
                pci1
            )
        );
        assert_eq!(
            state(&p),
            "state=NONE inProbe=false canProbe=true rtt=0.07 probeDuration=200ms"
        );
        assert_eq!(finalize(&mut p).0, NOT_FINALIZED);
        // the cluster is kept
        assert!(p.probe_cluster_is_goal_reached(1_000_001));

        // the regulator behind the controller: the next probe waits 3 s from when this
        // cluster was made, and may be longer
        p.probe_regulator
            .probe_signal(ccutils::ProbeSignal::NotCongesting, pci1.created_at);
        assert_eq!(
            state(&p),
            "state=NONE inProbe=false canProbe=false rtt=0.07 probeDuration=300ms"
        );
        // and after a congesting one 4.5 s, here from 5 s ago
        p.probe_regulator.probe_signal(
            ccutils::ProbeSignal::Congesting,
            mono::now().add(-5 * time::SECOND),
        );
        assert_eq!(
            state(&p),
            "state=NONE inProbe=false canProbe=true rtt=0.07 probeDuration=200ms"
        );
    }

    // New for musivum.
    #[test]
    fn a_cluster_that_starts_while_another_is_probing_takes_its_place() {
        let mut p = new_controller();
        p.probe_cluster_starting(pci(1, 600_000, 1_000_000));
        let switched_at = p.state_switched_at;
        sleep(5 * time::MILLISECOND);
        // logs "unexpected probe controller state"
        p.probe_cluster_starting(pci(2, 600_000, 2_000_000));
        assert_eq!(
            state(&p),
            "state=PROBING inProbe=true canProbe=false rtt=0.07 probeDuration=200ms"
        );
        // the state did not change, so neither did its time
        assert_eq!(p.state_switched_at, switched_at);
        assert!(!p.probe_cluster_is_goal_reached(1_500_000));
        assert!(p.probe_cluster_is_goal_reached(2_000_001));
    }

    // New for musivum. A new controller has the invalid cluster, so that one being done
    // counts as its own: ported as it is.
    #[test]
    fn the_invalid_cluster_being_done_puts_a_new_controller_in_hangover() {
        let mut p = new_controller();
        sleep(5 * time::MILLISECOND);
        p.probe_cluster_done(ccutils::PROBE_CLUSTER_INFO_INVALID);
        assert_eq!(
            state(&p),
            "state=HANGOVER inProbe=true canProbe=false rtt=0.07 probeDuration=200ms"
        );
        assert!(!p.probe_cluster_is_goal_reached(1_000_001));
    }

    // New for musivum.
    #[test]
    fn the_round_trip_time_is_smoothed() {
        let mut p = new_controller();
        let mut rtts = Vec::new();
        // zero is "not known": the default again; a smoothed value of zero is replaced
        for rtt in [0.01, 0.2, 0.0, -0.07, 0.3, 0.1] {
            p.update_rtt(rtt);
            rtts.push(p.get_rtt().to_string());
        }
        assert_eq!(
            rtts,
            ["0.04", "0.12000000000000001", "0.07", "0", "0.3", "0.2"]
        );
    }

    // New for musivum. The hangover is set back in time, as the harness did, not waited for.
    #[test]
    fn the_hangover_lasts_five_round_trips_within_its_bounds() {
        for (rtt, ago, expected) in [
            // five round trips of 70 ms: 350 ms
            (0.07, 200 * time::MILLISECOND, NOT_FINALIZED),
            (
                0.07,
                400 * time::MILLISECOND,
                "ok=true id=7 desiredBps=1000000 bytesProbe=0 isCompleted=false",
            ),
            // at least 250 ms
            (0.001, 100 * time::MILLISECOND, NOT_FINALIZED),
            (
                0.001,
                300 * time::MILLISECOND,
                "ok=true id=7 desiredBps=1000000 bytesProbe=0 isCompleted=false",
            ),
            // five round trips of 500 ms: 2.5 s
            (0.5, 2 * time::SECOND, NOT_FINALIZED),
            (
                0.5,
                3 * time::SECOND,
                "ok=true id=7 desiredBps=1000000 bytesProbe=0 isCompleted=false",
            ),
            // at most 5 s
            (2.0, 4 * time::SECOND, NOT_FINALIZED),
            (
                2.0,
                6 * time::SECOND,
                "ok=true id=7 desiredBps=1000000 bytesProbe=0 isCompleted=false",
            ),
        ] {
            let mut p = new_controller();
            let pci7 = pci(7, 600_000, 1_000_000);
            p.probe_cluster_starting(pci7);
            p.probe_cluster_done(pci7);
            p.rtt = rtt;
            p.state_switched_at = mono::now().add(-ago);
            assert_eq!(
                finalize(&mut p).0,
                expected,
                "rtt {rtt}, hangover for {ago}"
            );
        }
    }

    // New for musivum. The texts and the values are the original's (printed by the harness).
    #[test]
    fn the_texts_and_the_defaults_are_the_originals() {
        assert_eq!(ProbeControllerState::None.to_string(), "NONE");
        assert_eq!(ProbeControllerState::Probing.to_string(), "PROBING");
        assert_eq!(ProbeControllerState::Hangover.to_string(), "HANGOVER");
        assert_eq!(ProbeControllerState::default(), ProbeControllerState::None);

        assert_eq!(
            DEFAULT_PROBE_CONTROLLER_CONFIG,
            ProbeControllerConfig {
                probe_regulator: ccutils::ProbeRegulatorConfig {
                    base_interval: 3 * time::SECOND,
                    backoff_factor: 1.5,
                    max_interval: 2 * time::MINUTE,
                    min_duration: 200 * time::MILLISECOND,
                    max_duration: 20 * time::SECOND,
                    duration_increase_factor: 1.5,
                },
                settle_wait_num_rtt: 5,
                settle_wait_min: 250 * time::MILLISECOND,
                settle_wait_max: 5 * time::SECOND,
            }
        );
    }
}
