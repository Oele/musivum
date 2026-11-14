// Ported from LiveKit, pkg/sfu/bwe/remotebwe/nack_tracker.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `MarshalLogObject` is `Debug`, written to read like a
// derived one, with the values the original adds to the fields; the `yaml` tags of the
// configuration are doc comments on its fields, as nothing reads a configuration file yet.
// The tests at the end are new: the original has none.

use std::fmt;

use crate::go::time::{self, Duration, Time};
use crate::protocol::logger::Logger;

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct NackTrackerConfig {
    /// `window_min_duration` in YAML.
    pub window_min_duration: Duration,
    /// `window_max_duration` in YAML.
    pub window_max_duration: Duration,
    /// `ratio_threshold` in YAML.
    pub ratio_threshold: f64,
}

pub(super) const DEFAULT_NACK_TRACKER_CONFIG_PROBE: NackTrackerConfig = NackTrackerConfig {
    window_min_duration: Duration::from_millis(500),
    window_max_duration: Duration::from_secs(1),
    ratio_threshold: 0.04,
};

pub(super) const DEFAULT_NACK_TRACKER_CONFIG_NON_PROBE: NackTrackerConfig = NackTrackerConfig {
    window_min_duration: Duration::from_secs(2),
    window_max_duration: Duration::from_secs(3),
    ratio_threshold: 0.08,
};

// ------------------------------------------------

#[derive(Clone, Debug, Default)]
pub(super) struct NackTrackerParams {
    pub(super) name: String,
    #[allow(dead_code)] // not read upstream either
    pub(super) logger: Logger,
    pub(super) config: NackTrackerConfig,
}

pub(super) struct NackTracker {
    params: NackTrackerParams,

    window_start_time: Time,
    packets: u32,
    repeated_nacks: u32,
}

impl NackTracker {
    pub(super) fn new(params: NackTrackerParams) -> Self {
        Self {
            params,
            window_start_time: Time::ZERO,
            packets: 0,
            repeated_nacks: 0,
        }
    }

    pub(super) fn add(&mut self, packets: u32, repeated_nacks: u32) {
        if self.params.config.window_max_duration != Duration::ZERO
            && !self.window_start_time.is_zero()
            && time::since(self.window_start_time) > self.params.config.window_max_duration
        {
            self.window_start_time = Time::ZERO;
            self.packets = 0;
            self.repeated_nacks = 0;
        }

        //
        // Start NACK monitoring window only when a repeated NACK happens.
        // This allows locking tightly to when NACKs start happening and
        // check if the NACKs keep adding up (potentially a sign of congestion)
        // or isolated losses
        //
        if self.repeated_nacks == 0 && repeated_nacks != 0 {
            self.window_start_time = time::now();
        }

        if !self.window_start_time.is_zero() {
            self.packets = self.packets.wrapping_add(packets);
            self.repeated_nacks = self.repeated_nacks.wrapping_add(repeated_nacks);
        }
    }

    pub(super) fn get_ratio(&self) -> f64 {
        let mut ratio = 0.0;
        if self.packets != 0 {
            ratio = self.repeated_nacks as f64 / self.packets as f64;
            if ratio > 1.0 {
                ratio = 1.0;
            }
        }

        ratio
    }

    pub(super) fn is_triggered(&self) -> bool {
        if self.params.config.window_min_duration != Duration::ZERO
            && !self.window_start_time.is_zero()
            && time::since(self.window_start_time) > self.params.config.window_min_duration
        {
            return self.get_ratio() > self.params.config.ratio_threshold;
        }

        false
    }
}

impl fmt::Debug for NackTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("NackTracker");
        s.field("name", &self.params.name);
        if self.window_start_time.is_zero() {
            s.field("window", &format_args!("inactive"));
        } else {
            s.field("window_start_time", &self.window_start_time);
            s.field("window_duration", &time::since(self.window_start_time));
            s.field("packets", &self.packets);
            s.field("repeated_nacks", &self.repeated_nacks);
            s.field("nack_ratio", &self.get_ratio());
        }
        s.finish()
    }
}

// ------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn new_tracker(config: NackTrackerConfig) -> NackTracker {
        NackTracker::new(NackTrackerParams {
            name: "test".to_string(),
            config,
            ..Default::default()
        })
    }

    // What the harness that ran the original printed after each step.
    fn state(n: &NackTracker) -> String {
        format!(
            "ratio={} triggered={} active={} packets={} repeatedNacks={}",
            n.get_ratio(),
            n.is_triggered(),
            !n.window_start_time.is_zero(),
            n.packets,
            n.repeated_nacks
        )
    }

    fn sleep(d: Duration) {
        std::thread::sleep(d.to_std());
    }

    // New for musivum: the original has no tests. The expectations of the tests here are
    // what the original gives for the same calls with the same sleeps between them (a Go
    // test laid over the package, which printed these lines).
    #[test]
    fn the_window_starts_at_a_repeated_nack_and_triggers_on_the_ratio_after_its_minimum() {
        // a window of at least 500 ms and at most 1 s, triggered above 0.04
        let mut n = new_tracker(DEFAULT_NACK_TRACKER_CONFIG_PROBE);
        assert_eq!(
            state(&n),
            "ratio=0 triggered=false active=false packets=0 repeatedNacks=0"
        );

        // packets do not count before the first repeated NACK
        n.add(100, 0);
        assert_eq!(
            state(&n),
            "ratio=0 triggered=false active=false packets=0 repeatedNacks=0"
        );
        n.add(100, 10);
        assert_eq!(
            state(&n),
            "ratio=0.1 triggered=false active=true packets=100 repeatedNacks=10"
        );
        n.add(100, 0);
        assert_eq!(
            state(&n),
            "ratio=0.05 triggered=false active=true packets=200 repeatedNacks=10"
        );

        // the ratio counts once the window has lasted its minimum
        sleep(600 * time::MILLISECOND);
        assert_eq!(
            state(&n),
            "ratio=0.05 triggered=true active=true packets=200 repeatedNacks=10"
        );
        n.add(300, 0);
        assert_eq!(
            state(&n),
            "ratio=0.02 triggered=false active=true packets=500 repeatedNacks=10"
        );
        n.add(0, 15);
        assert_eq!(
            state(&n),
            "ratio=0.05 triggered=true active=true packets=500 repeatedNacks=25"
        );

        // past its maximum the window is still read, and ends at the next report
        sleep(500 * time::MILLISECOND);
        assert_eq!(
            state(&n),
            "ratio=0.05 triggered=true active=true packets=500 repeatedNacks=25"
        );
        n.add(50, 0);
        assert_eq!(
            state(&n),
            "ratio=0 triggered=false active=false packets=0 repeatedNacks=0"
        );

        // more repeated NACKs than packets is a ratio of 1
        n.add(10, 20);
        assert_eq!(
            state(&n),
            "ratio=1 triggered=false active=true packets=10 repeatedNacks=20"
        );
        sleep(600 * time::MILLISECOND);
        assert_eq!(
            state(&n),
            "ratio=1 triggered=true active=true packets=10 repeatedNacks=20"
        );

        // a report with a repeated NACK that ends a window starts the next one
        sleep(500 * time::MILLISECOND);
        n.add(40, 2);
        assert_eq!(
            state(&n),
            "ratio=0.05 triggered=false active=true packets=40 repeatedNacks=2"
        );
    }

    // New for musivum.
    #[test]
    fn the_window_of_the_time_without_a_probe_is_longer() {
        // at least 2 s: what triggers after 500 ms while probing does not here
        let mut n = new_tracker(DEFAULT_NACK_TRACKER_CONFIG_NON_PROBE);
        n.add(100, 9);
        assert_eq!(
            state(&n),
            "ratio=0.09 triggered=false active=true packets=100 repeatedNacks=9"
        );
        sleep(600 * time::MILLISECOND);
        assert_eq!(
            state(&n),
            "ratio=0.09 triggered=false active=true packets=100 repeatedNacks=9"
        );
    }

    // New for musivum.
    #[test]
    fn without_a_minimum_nothing_triggers_and_without_a_maximum_the_window_does_not_end() {
        let mut n = new_tracker(NackTrackerConfig::default());
        n.add(10, 5);
        assert_eq!(
            state(&n),
            "ratio=0.5 triggered=false active=true packets=10 repeatedNacks=5"
        );
        sleep(50 * time::MILLISECOND);
        n.add(10, 0);
        assert_eq!(
            state(&n),
            "ratio=0.25 triggered=false active=true packets=20 repeatedNacks=5"
        );

        // only a maximum, of 20 ms
        let mut n = new_tracker(NackTrackerConfig {
            window_max_duration: 20 * time::MILLISECOND,
            ratio_threshold: 0.04,
            ..Default::default()
        });
        n.add(10, 5);
        assert_eq!(
            state(&n),
            "ratio=0.5 triggered=false active=true packets=10 repeatedNacks=5"
        );
        sleep(50 * time::MILLISECOND);
        assert_eq!(
            state(&n),
            "ratio=0.5 triggered=false active=true packets=10 repeatedNacks=5"
        );
        n.add(10, 0);
        assert_eq!(
            state(&n),
            "ratio=0 triggered=false active=false packets=0 repeatedNacks=0"
        );
    }

    // New for musivum.
    #[test]
    fn the_counts_wrap_as_the_originals_do() {
        let mut n = new_tracker(DEFAULT_NACK_TRACKER_CONFIG_NON_PROBE);
        n.add(4294967295, 4294967295);
        assert_eq!(
            state(&n),
            "ratio=1 triggered=false active=true packets=4294967295 repeatedNacks=4294967295"
        );
        n.add(3, 2);
        assert_eq!(
            state(&n),
            "ratio=0.5 triggered=false active=true packets=2 repeatedNacks=1"
        );
        n.add(2, 0);
        assert_eq!(
            state(&n),
            "ratio=0.25 triggered=false active=true packets=4 repeatedNacks=1"
        );
    }

    // New for musivum. The values are the original's (printed by the harness); how a tracker
    // is logged is this port's.
    #[test]
    fn the_defaults_are_the_originals_and_a_tracker_is_logged_with_its_window() {
        assert_eq!(
            DEFAULT_NACK_TRACKER_CONFIG_PROBE,
            NackTrackerConfig {
                window_min_duration: 500 * time::MILLISECOND,
                window_max_duration: time::SECOND,
                ratio_threshold: 0.04,
            }
        );
        assert_eq!(
            DEFAULT_NACK_TRACKER_CONFIG_NON_PROBE,
            NackTrackerConfig {
                window_min_duration: 2 * time::SECOND,
                window_max_duration: 3 * time::SECOND,
                ratio_threshold: 0.08,
            }
        );

        let mut n = new_tracker(DEFAULT_NACK_TRACKER_CONFIG_PROBE);
        assert_eq!(
            format!("{n:?}"),
            "NackTracker { name: \"test\", window: inactive }"
        );
        n.add(200, 10);
        let logged = format!("{n:?}");
        assert!(
            logged.starts_with("NackTracker { name: \"test\", window_start_time: "),
            "{logged}"
        );
        assert!(logged.contains(", window_duration: "), "{logged}");
        assert!(
            logged.ends_with(", packets: 200, repeated_nacks: 10, nack_ratio: 0.05 }"),
            "{logged}"
        );
    }
}
