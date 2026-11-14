// Ported from LiveKit, pkg/sfu/ccutils/probe_regulator.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the `yaml` tags of the configuration are left out, as
// nothing reads a configuration file yet. The tests at the end are new: the original has none.

use crate::go::time::{self, Duration, Time};
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;

use super::ProbeSignal;

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ProbeRegulatorConfig {
    pub base_interval: Duration,
    pub backoff_factor: f64,
    pub max_interval: Duration,

    pub min_duration: Duration,
    pub max_duration: Duration,
    pub duration_increase_factor: f64,
}

pub const DEFAULT_PROBE_REGULATOR_CONFIG: ProbeRegulatorConfig = ProbeRegulatorConfig {
    base_interval: Duration::from_secs(3),
    backoff_factor: 1.5,
    max_interval: Duration::from_secs(2 * 60),

    min_duration: Duration::from_millis(200),
    max_duration: Duration::from_secs(20),
    duration_increase_factor: 1.5,
};

// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct ProbeRegulatorParams {
    pub config: ProbeRegulatorConfig,
    pub logger: Logger,
}

#[derive(Clone, Debug)]
pub struct ProbeRegulator {
    params: ProbeRegulatorParams,

    probe_interval: Duration,
    probe_duration: Duration,
    next_probe_earliest_at: Time,
}

impl ProbeRegulator {
    pub fn new(params: ProbeRegulatorParams) -> Self {
        Self {
            probe_interval: params.config.base_interval,
            probe_duration: params.config.min_duration,
            next_probe_earliest_at: mono::now(),
            params,
        }
    }

    pub fn can_probe(&self) -> bool {
        mono::now().after(self.next_probe_earliest_at)
    }

    pub fn probe_duration(&self) -> Duration {
        self.probe_duration
    }

    pub fn probe_signal(&mut self, probe_signal: ProbeSignal, base_time: Time) {
        if probe_signal == ProbeSignal::Congesting {
            // wait longer till next probe
            self.probe_interval = (self.probe_interval.milliseconds() as f64
                * self.params.config.backoff_factor) as i64
                * time::MILLISECOND;
            if self.probe_interval > self.params.config.max_interval {
                self.probe_interval = self.params.config.max_interval;
            }

            // revert back to starting with shortest probe
            self.probe_duration = self.params.config.min_duration;
        } else {
            // probe can be started again after minimal interval as previous congestion signal indicated congestion clearing
            self.probe_interval = self.params.config.base_interval;

            // can do longer probe after a good probe
            self.probe_duration = (self.probe_duration.milliseconds() as f64
                * self.params.config.duration_increase_factor)
                as i64
                * time::MILLISECOND;
            if self.probe_duration > self.params.config.max_duration {
                self.probe_duration = self.params.config.max_duration;
            }
        }

        if base_time.is_zero() {
            self.next_probe_earliest_at = mono::now().add(self.probe_interval);
        } else {
            self.next_probe_earliest_at = base_time.add(self.probe_interval);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_regulator() -> ProbeRegulator {
        ProbeRegulator::new(ProbeRegulatorParams {
            config: DEFAULT_PROBE_REGULATOR_CONFIG,
            ..Default::default()
        })
    }

    // New for musivum: the original has no tests. The numbers are what the original gives
    // for its default configuration (checked by running it).
    #[test]
    fn a_congesting_probe_backs_the_interval_off_up_to_the_maximum() {
        let mut p = new_regulator();
        assert_eq!(p.probe_interval, 3 * time::SECOND);
        assert_eq!(p.probe_duration(), 200 * time::MILLISECOND);

        // a good probe first, so that there is a longer duration to revert
        p.probe_signal(ProbeSignal::NotCongesting, Time::ZERO);
        assert_eq!(p.probe_duration(), 300 * time::MILLISECOND);

        // the interval is multiplied in whole milliseconds: 15187.5 ms becomes 15187 ms
        let base_time = Time::unix(1000, 0);
        for expected_ms in [
            4500, 6750, 10125, 15187, 22780, 34170, 51255, 76882, 115323, 120000, 120000,
        ] {
            p.probe_signal(ProbeSignal::Congesting, base_time);
            assert_eq!(p.probe_interval, expected_ms * time::MILLISECOND);
            assert_eq!(p.probe_duration(), 200 * time::MILLISECOND);
            assert_eq!(
                p.next_probe_earliest_at,
                base_time.add(expected_ms * time::MILLISECOND)
            );
        }
    }

    // New for musivum.
    #[test]
    fn a_probe_that_does_not_congest_resets_the_interval_and_lengthens_the_probe() {
        let mut p = new_regulator();
        p.probe_signal(ProbeSignal::Congesting, Time::ZERO);
        p.probe_signal(ProbeSignal::Congesting, Time::ZERO);
        assert_eq!(p.probe_interval, 6750 * time::MILLISECOND);

        // an inconclusive probe counts as one that did not congest
        let base_time = Time::unix(1000, 0);
        p.probe_signal(ProbeSignal::Inconclusive, base_time);
        assert_eq!(p.probe_interval, 3 * time::SECOND);
        assert_eq!(p.probe_duration(), 300 * time::MILLISECOND);
        assert_eq!(p.next_probe_earliest_at, base_time.add(3 * time::SECOND));

        for expected_ms in [
            450, 675, 1012, 1518, 2277, 3415, 5122, 7683, 11524, 17286, 20000, 20000,
        ] {
            p.probe_signal(ProbeSignal::NotCongesting, base_time);
            assert_eq!(p.probe_interval, 3 * time::SECOND);
            assert_eq!(p.probe_duration(), expected_ms * time::MILLISECOND);
        }
    }

    // New for musivum.
    #[test]
    fn the_next_probe_waits_for_the_interval() {
        let mut p = new_regulator();
        // without a base time the interval starts now
        p.probe_signal(ProbeSignal::NotCongesting, Time::ZERO);
        assert!(!p.can_probe());
        let wait = p.next_probe_earliest_at.sub(mono::now());
        assert!(
            wait > 2 * time::SECOND && wait <= 3 * time::SECOND,
            "{wait}"
        );

        // with a base time it starts there: this one has passed
        p.probe_signal(
            ProbeSignal::NotCongesting,
            mono::now().add(-4 * time::SECOND),
        );
        assert!(p.can_probe());
    }
}
