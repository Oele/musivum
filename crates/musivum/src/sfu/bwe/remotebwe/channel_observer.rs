// Ported from LiveKit, pkg/sfu/bwe/remotebwe/channel_observer.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `channelTrend` and `channelCongestionReason` are enums
// and their `String()` is `Display`; `MarshalLogObject` is `Debug`, written to read like a
// derived one, with the values the original adds to the fields; the `yaml` tags of the
// configuration are doc comments on its fields, as nothing reads a configuration file yet.
// The tests at the end are new: the original has none.

use std::fmt;

use crate::go::time::Duration;
use crate::protocol::logger::Logger;
use crate::sfu::ccutils;

use super::nack_tracker::{
    DEFAULT_NACK_TRACKER_CONFIG_NON_PROBE, DEFAULT_NACK_TRACKER_CONFIG_PROBE, NackTracker,
    NackTrackerConfig, NackTrackerParams,
};

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(super) enum ChannelTrend {
    #[default]
    Inconclusive,
    Clearing,
    Congesting,
}

impl fmt::Display for ChannelTrend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ChannelTrend::Inconclusive => "INCONCLUSIVE",
            ChannelTrend::Clearing => "CLEARING",
            ChannelTrend::Congesting => "CONGESTING",
        })
    }
}

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(super) enum ChannelCongestionReason {
    #[default]
    None,
    Estimate,
    Loss,
}

impl fmt::Display for ChannelCongestionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ChannelCongestionReason::None => "NONE",
            ChannelCongestionReason::Estimate => "ESTIMATE",
            ChannelCongestionReason::Loss => "LOSS",
        })
    }
}

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ChannelObserverConfig {
    /// `estimate` in YAML.
    pub estimate: ccutils::TrendDetectorConfig,
    /// `nack` in YAML.
    pub nack: NackTrackerConfig,
}

pub(super) const DEFAULT_TREND_DETECTOR_CONFIG_PROBE: ccutils::TrendDetectorConfig =
    ccutils::TrendDetectorConfig {
        required_samples: 3,
        required_samples_min: 3,
        downward_trend_threshold: 0.0,
        downward_trend_max_wait: Duration::from_secs(5),
        collapse_threshold: Duration::ZERO,
        validity_window: Duration::from_secs(10),
    };

pub(super) const DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE: ChannelObserverConfig =
    ChannelObserverConfig {
        estimate: DEFAULT_TREND_DETECTOR_CONFIG_PROBE,
        nack: DEFAULT_NACK_TRACKER_CONFIG_PROBE,
    };

pub(super) const DEFAULT_TREND_DETECTOR_CONFIG_NON_PROBE: ccutils::TrendDetectorConfig =
    ccutils::TrendDetectorConfig {
        required_samples: 12,
        required_samples_min: 8,
        downward_trend_threshold: -0.6,
        downward_trend_max_wait: Duration::from_secs(5),
        collapse_threshold: Duration::from_millis(500),
        validity_window: Duration::from_secs(10),
    };

pub(super) const DEFAULT_CHANNEL_OBSERVER_CONFIG_NON_PROBE: ChannelObserverConfig =
    ChannelObserverConfig {
        estimate: DEFAULT_TREND_DETECTOR_CONFIG_NON_PROBE,
        nack: DEFAULT_NACK_TRACKER_CONFIG_NON_PROBE,
    };

// ------------------------------------------------

#[derive(Clone, Debug, Default)]
pub(super) struct ChannelObserverParams {
    pub(super) name: String,
    pub(super) config: ChannelObserverConfig,
}

pub(super) struct ChannelObserver {
    params: ChannelObserverParams,
    #[allow(dead_code)] // not read upstream either
    logger: Logger,

    estimate_trend: ccutils::TrendDetector<i64>,
    nack_tracker: NackTracker,
}

impl ChannelObserver {
    pub(super) fn new(params: ChannelObserverParams, logger: Logger) -> Self {
        Self {
            estimate_trend: ccutils::TrendDetector::<i64>::new(ccutils::TrendDetectorParams {
                name: params.name.clone() + "-estimate",
                logger: logger.clone(),
                config: params.config.estimate,
            }),
            nack_tracker: NackTracker::new(NackTrackerParams {
                name: params.name.clone() + "-nack",
                logger: logger.clone(),
                config: params.config.nack,
            }),
            params,
            logger,
        }
    }

    #[allow(dead_code)] // not called upstream either
    pub(super) fn seed_estimate(&mut self, estimate: i64) {
        self.estimate_trend.seed(estimate);
    }

    pub(super) fn add_estimate(&mut self, estimate: i64) {
        self.estimate_trend.add_value(estimate);
    }

    pub(super) fn add_nack(&mut self, packets: u32, repeated_nacks: u32) {
        self.nack_tracker.add(packets, repeated_nacks);
    }

    #[allow(dead_code)] // not called upstream either
    pub(super) fn get_lowest_estimate(&self) -> i64 {
        self.estimate_trend.get_lowest()
    }

    pub(super) fn get_highest_estimate(&self) -> i64 {
        self.estimate_trend.get_highest()
    }

    pub(super) fn has_enough_estimate_samples(&self) -> bool {
        self.estimate_trend.has_enough_samples()
    }

    pub(super) fn get_nack_ratio(&self) -> f64 {
        self.nack_tracker.get_ratio()
    }

    pub(super) fn get_trend(&self) -> (ChannelTrend, ChannelCongestionReason) {
        let estimate_direction = self.estimate_trend.get_direction();

        if estimate_direction == ccutils::TrendDirection::Downward {
            return (ChannelTrend::Congesting, ChannelCongestionReason::Estimate);
        } else if self.nack_tracker.is_triggered() {
            return (ChannelTrend::Congesting, ChannelCongestionReason::Loss);
        } else if estimate_direction == ccutils::TrendDirection::Upward {
            return (ChannelTrend::Clearing, ChannelCongestionReason::None);
        }

        (ChannelTrend::Inconclusive, ChannelCongestionReason::None)
    }
}

impl fmt::Debug for ChannelObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (channel_trend, channel_congestion_reason) = self.get_trend();
        f.debug_struct("ChannelObserver")
            .field("name", &self.params.name)
            .field("estimate", &self.estimate_trend)
            .field("nack", &self.nack_tracker)
            .field("channel_trend", &format_args!("{channel_trend}"))
            .field(
                "channel_congestion_reason",
                &format_args!("{channel_congestion_reason}"),
            )
            .finish()
    }
}

// ------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::go::time;

    fn new_observer(name: &str, config: ChannelObserverConfig) -> ChannelObserver {
        ChannelObserver::new(
            ChannelObserverParams {
                name: name.to_string(),
                config,
            },
            Logger::default(),
        )
    }

    // What the harness that ran the original printed after each step.
    fn state(c: &ChannelObserver) -> String {
        let (trend, reason) = c.get_trend();
        format!(
            "trend={trend} reason={reason} lowest={} highest={} enough={} nackRatio={}",
            c.get_lowest_estimate(),
            c.get_highest_estimate(),
            c.has_enough_estimate_samples(),
            c.get_nack_ratio()
        )
    }

    // Adds the estimates one after the other; the state of the new observer and the state
    // after each estimate have to be the lines of `expected`.
    #[track_caller]
    fn series(mut c: ChannelObserver, estimates: &[i64], expected: &str) {
        let expected: Vec<&str> = expected
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect();
        assert_eq!(estimates.len() + 1, expected.len());

        assert_eq!(state(&c), expected[0], "new");
        for (i, estimate) in estimates.iter().enumerate() {
            c.add_estimate(*estimate);
            assert_eq!(
                state(&c),
                expected[i + 1],
                "after estimate {} ({estimate})",
                i + 1
            );
        }
    }

    fn sleep(d: Duration) {
        std::thread::sleep(d.to_std());
    }

    // New for musivum: the original has no tests. The expectations of the tests here are
    // what the original gives for the same calls with the same sleeps between them (a Go
    // test laid over the package, which printed these lines).
    #[test]
    fn a_rising_estimate_is_clearing_once_there_are_the_required_samples() {
        // twelve of them when there is no probe
        let rising: Vec<i64> = (0..13).map(|i| 1_000_000 + i * 50_000).collect();
        series(
            new_observer("non-probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_NON_PROBE),
            &rising,
            "
            trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1050000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1100000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1150000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1200000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1250000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1300000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1350000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1400000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1450000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1500000 enough=false nackRatio=0
            trend=CLEARING reason=NONE lowest=1000000 highest=1550000 enough=true nackRatio=0
            trend=CLEARING reason=NONE lowest=1000000 highest=1600000 enough=true nackRatio=0
            ",
        );

        // three of them in a probe
        series(
            new_observer("probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE),
            &[1_000_000, 1_100_000, 1_200_000, 1_300_000],
            "
            trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1100000 enough=false nackRatio=0
            trend=CLEARING reason=NONE lowest=1000000 highest=1200000 enough=true nackRatio=0
            trend=CLEARING reason=NONE lowest=1000000 highest=1300000 enough=true nackRatio=0
            ",
        );
    }

    // New for musivum.
    #[test]
    fn a_falling_estimate_is_congesting_by_the_estimate() {
        let falling: Vec<i64> = (0..13).map(|i| 2_000_000 - i * 50_000).collect();
        series(
            new_observer("non-probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_NON_PROBE),
            &falling,
            "
            trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=2000000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1950000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1900000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1850000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1800000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1750000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1700000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1650000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1600000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1550000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=2000000 enough=false nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=1450000 highest=2000000 enough=true nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=1400000 highest=2000000 enough=true nackRatio=0
            ",
        );

        // falling with every second estimate a little up again is falling enough
        series(
            new_observer("non-probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_NON_PROBE),
            &[
                2_000_000, 2_050_000, 1_900_000, 1_950_000, 1_800_000, 1_850_000, 1_700_000,
                1_750_000, 1_600_000, 1_650_000, 1_500_000, 1_550_000, 1_400_000, 1_450_000,
            ],
            "
            trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=2000000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=2000000 highest=2050000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1900000 highest=2050000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1900000 highest=2050000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1800000 highest=2050000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1800000 highest=2050000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1700000 highest=2050000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1700000 highest=2050000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1600000 highest=2050000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1600000 highest=2050000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=2050000 enough=false nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=1500000 highest=2050000 enough=true nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=1400000 highest=2050000 enough=true nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=1400000 highest=2050000 enough=true nackRatio=0
            ",
        );

        // in a probe any fall over three estimates counts
        series(
            new_observer("probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE),
            &[1_300_000, 1_200_000, 1_100_000, 1_000_000],
            "
            trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1300000 highest=1300000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1200000 highest=1300000 enough=false nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=1100000 highest=1300000 enough=true nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=1000000 highest=1300000 enough=true nackRatio=0
            ",
        );
    }

    // New for musivum.
    #[test]
    fn a_flat_estimate_is_inconclusive() {
        series(
            new_observer("non-probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_NON_PROBE),
            &[1_500_000; 13],
            "
            trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=true nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=1500000 enough=true nackRatio=0
            ",
        );

        series(
            new_observer("probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE),
            &[1_000_000; 4],
            "
            trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1000000 enough=true nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1000000 enough=true nackRatio=0
            ",
        );
    }

    // New for musivum.
    #[test]
    fn the_trend_follows_an_estimate_that_turns() {
        // down, and up again: congesting, then nothing, then clearing
        series(
            new_observer("non-probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_NON_PROBE),
            &[
                2_000_000, 1_900_000, 1_800_000, 1_700_000, 1_600_000, 1_500_000, 1_400_000,
                1_300_000, 1_200_000, 1_100_000, 1_000_000, 900_000, 950_000, 1_000_000, 1_050_000,
                1_100_000, 1_150_000, 1_200_000, 1_250_000, 1_300_000, 1_350_000, 1_400_000,
                1_450_000, 1_500_000,
            ],
            "
            trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=2000000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1900000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1800000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1700000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1600000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1500000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1400000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1300000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1200000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1100000 highest=2000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=2000000 enough=false nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=CLEARING reason=NONE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=CLEARING reason=NONE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=CLEARING reason=NONE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=CLEARING reason=NONE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=CLEARING reason=NONE lowest=900000 highest=2000000 enough=true nackRatio=0
            trend=CLEARING reason=NONE lowest=900000 highest=2000000 enough=true nackRatio=0
            ",
        );

        series(
            new_observer("probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE),
            &[
                1_000_000, 1_200_000, 1_100_000, 1_300_000, 1_000_000, 900_000, 950_000,
            ],
            "
            trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1000000 enough=false nackRatio=0
            trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1200000 enough=false nackRatio=0
            trend=CLEARING reason=NONE lowest=1000000 highest=1200000 enough=true nackRatio=0
            trend=CLEARING reason=NONE lowest=1000000 highest=1300000 enough=true nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=1000000 highest=1300000 enough=true nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=900000 highest=1300000 enough=true nackRatio=0
            trend=CONGESTING reason=ESTIMATE lowest=900000 highest=1300000 enough=true nackRatio=0
            ",
        );
    }

    // New for musivum.
    #[test]
    fn lasting_loss_is_congesting_by_loss_unless_the_estimate_falls_too() {
        // in a probe: a window of at least 500 ms and at most 1 s, triggered above 0.04
        let mut c = new_observer("probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE);
        c.add_nack(100, 10);
        assert_eq!(
            state(&c),
            "trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0.1"
        );
        sleep(600 * time::MILLISECOND);
        assert_eq!(
            state(&c),
            "trend=CONGESTING reason=LOSS lowest=0 highest=0 enough=false nackRatio=0.1"
        );

        // an estimate that rises does not clear it
        c.add_estimate(1_000_000);
        assert_eq!(
            state(&c),
            "trend=CONGESTING reason=LOSS lowest=1000000 highest=1000000 enough=false nackRatio=0.1"
        );
        c.add_estimate(1_100_000);
        assert_eq!(
            state(&c),
            "trend=CONGESTING reason=LOSS lowest=1000000 highest=1100000 enough=false nackRatio=0.1"
        );
        c.add_estimate(1_200_000);
        assert_eq!(
            state(&c),
            "trend=CONGESTING reason=LOSS lowest=1000000 highest=1200000 enough=true nackRatio=0.1"
        );

        // one that falls is the reason before the loss
        c.add_estimate(1_100_000);
        assert_eq!(
            state(&c),
            "trend=CONGESTING reason=LOSS lowest=1000000 highest=1200000 enough=true nackRatio=0.1"
        );
        c.add_estimate(1_000_000);
        assert_eq!(
            state(&c),
            "trend=CONGESTING reason=ESTIMATE lowest=1000000 highest=1200000 enough=true nackRatio=0.1"
        );

        // the loss thins out below the threshold: the estimate alone decides
        c.add_nack(400, 0);
        assert_eq!(
            state(&c),
            "trend=CONGESTING reason=ESTIMATE lowest=1000000 highest=1200000 enough=true nackRatio=0.02"
        );
        c.add_estimate(1_100_000);
        assert_eq!(
            state(&c),
            "trend=INCONCLUSIVE reason=NONE lowest=1000000 highest=1200000 enough=true nackRatio=0.02"
        );
        c.add_estimate(1_200_000);
        assert_eq!(
            state(&c),
            "trend=CLEARING reason=NONE lowest=1000000 highest=1200000 enough=true nackRatio=0.02"
        );

        // and the window ends
        sleep(500 * time::MILLISECOND);
        c.add_nack(100, 0);
        assert_eq!(
            state(&c),
            "trend=CLEARING reason=NONE lowest=1000000 highest=1200000 enough=true nackRatio=0"
        );
    }

    // New for musivum.
    #[test]
    fn a_seed_is_a_sample_of_the_trend_but_not_an_estimate() {
        let mut c = new_observer("probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE);
        c.seed_estimate(1_000_000);
        assert_eq!(
            state(&c),
            "trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0"
        );
        // only the first seed is taken
        c.seed_estimate(3_000_000);
        assert_eq!(
            state(&c),
            "trend=INCONCLUSIVE reason=NONE lowest=0 highest=0 enough=false nackRatio=0"
        );
        c.add_estimate(1_100_000);
        assert_eq!(
            state(&c),
            "trend=INCONCLUSIVE reason=NONE lowest=1100000 highest=1100000 enough=false nackRatio=0"
        );
        c.add_estimate(1_200_000);
        assert_eq!(
            state(&c),
            "trend=CLEARING reason=NONE lowest=1100000 highest=1200000 enough=false nackRatio=0"
        );
    }

    // New for musivum. The texts and the values are the original's (printed by the harness);
    // how an observer is logged is this port's.
    #[test]
    fn the_texts_and_the_defaults_are_the_originals_and_an_observer_is_logged_with_its_trend() {
        assert_eq!(ChannelTrend::Inconclusive.to_string(), "INCONCLUSIVE");
        assert_eq!(ChannelTrend::Clearing.to_string(), "CLEARING");
        assert_eq!(ChannelTrend::Congesting.to_string(), "CONGESTING");
        assert_eq!(ChannelTrend::default(), ChannelTrend::Inconclusive);
        assert_eq!(ChannelCongestionReason::None.to_string(), "NONE");
        assert_eq!(ChannelCongestionReason::Estimate.to_string(), "ESTIMATE");
        assert_eq!(ChannelCongestionReason::Loss.to_string(), "LOSS");
        assert_eq!(
            ChannelCongestionReason::default(),
            ChannelCongestionReason::None
        );

        assert_eq!(
            DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE,
            ChannelObserverConfig {
                estimate: ccutils::TrendDetectorConfig {
                    required_samples: 3,
                    required_samples_min: 3,
                    downward_trend_threshold: 0.0,
                    downward_trend_max_wait: 5 * time::SECOND,
                    collapse_threshold: Duration::ZERO,
                    validity_window: 10 * time::SECOND,
                },
                nack: NackTrackerConfig {
                    window_min_duration: 500 * time::MILLISECOND,
                    window_max_duration: time::SECOND,
                    ratio_threshold: 0.04,
                },
            }
        );
        assert_eq!(
            DEFAULT_CHANNEL_OBSERVER_CONFIG_NON_PROBE,
            ChannelObserverConfig {
                estimate: ccutils::TrendDetectorConfig {
                    required_samples: 12,
                    required_samples_min: 8,
                    downward_trend_threshold: -0.6,
                    downward_trend_max_wait: 5 * time::SECOND,
                    collapse_threshold: 500 * time::MILLISECOND,
                    validity_window: 10 * time::SECOND,
                },
                nack: NackTrackerConfig {
                    window_min_duration: 2 * time::SECOND,
                    window_max_duration: 3 * time::SECOND,
                    ratio_threshold: 0.08,
                },
            }
        );

        let mut c = new_observer("probe", DEFAULT_CHANNEL_OBSERVER_CONFIG_PROBE);
        for estimate in [1_300_000, 1_200_000, 1_100_000] {
            c.add_estimate(estimate);
        }
        let logged = format!("{c:?}");
        assert!(
            logged.starts_with(
                "ChannelObserver { name: \"probe\", estimate: TrendDetector { name: \"probe-estimate\", "
            ),
            "{logged}"
        );
        assert!(
            logged.ends_with(
                ", nack: NackTracker { name: \"probe-nack\", window: inactive }, \
                 channel_trend: CONGESTING, channel_congestion_reason: ESTIMATE }"
            ),
            "{logged}"
        );
    }
}
