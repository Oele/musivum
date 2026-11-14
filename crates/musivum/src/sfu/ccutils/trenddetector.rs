// Ported from LiveKit, pkg/sfu/ccutils/trenddetector.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the type constraint `int64 | float64` is a trait;
// `TrendDirection` is an enum and its `String()` is `Display`; `MarshalLogObject` is `Debug`,
// written to read like a derived one, with the values the original adds to the fields; the
// `yaml` tags of the configuration are left out, as nothing reads a configuration file yet.
// The tests at the end are new: the original has none.

use std::fmt;

use crate::go::time::{self, Duration, Time};
use crate::protocol::logger::Logger;

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum TrendDirection {
    #[default]
    Inconclusive,
    Upward,
    Downward,
}

impl fmt::Display for TrendDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrendDirection::Inconclusive => "INCONCLUSIVE",
            TrendDirection::Upward => "UPWARD",
            TrendDirection::Downward => "DOWNWARD",
        })
    }
}

// ------------------------------------------------

/// What a trend is detected in: `int64 | float64` upstream.
pub trait TrendDetectorNumber: Copy + PartialOrd + Default + fmt::Debug {
    /// Go's conversion `float64(v)`.
    fn to_f64(self) -> f64;
}

impl TrendDetectorNumber for i64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl TrendDetectorNumber for f64 {
    fn to_f64(self) -> f64 {
        self
    }
}

// ------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct TrendDetectorSample<T: TrendDetectorNumber> {
    value: T,
    at: Time,
}

struct TrendDetectorSampleElapsed<T: TrendDetectorNumber> {
    value: T,
    since_first: Duration,
}

impl<T: TrendDetectorNumber> fmt::Debug for TrendDetectorSampleElapsed<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrendDetectorSampleElapsed")
            .field("value", &self.value.to_f64())
            .field("since_first", &self.since_first)
            .finish()
    }
}

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TrendDetectorConfig {
    pub required_samples: usize,
    pub required_samples_min: usize,
    pub downward_trend_threshold: f64,
    pub downward_trend_max_wait: Duration,
    pub collapse_threshold: Duration,
    pub validity_window: Duration,
}

// ------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct TrendDetectorParams {
    pub name: String,
    pub logger: Logger,
    pub config: TrendDetectorConfig,
}

pub struct TrendDetector<T: TrendDetectorNumber> {
    params: TrendDetectorParams,

    start_time: Time,
    num_samples: usize,
    samples: Vec<TrendDetectorSample<T>>,
    lowest_value: T,
    highest_value: T,

    direction: TrendDirection,
}

impl<T: TrendDetectorNumber> TrendDetector<T> {
    pub fn new(params: TrendDetectorParams) -> Self {
        Self {
            params,
            start_time: time::now(),
            num_samples: 0,
            samples: Vec::new(),
            lowest_value: T::default(),
            highest_value: T::default(),
            direction: TrendDirection::Inconclusive,
        }
    }

    pub fn seed(&mut self, value: T) {
        if !self.samples.is_empty() {
            return;
        }

        self.samples.push(TrendDetectorSample {
            value,
            at: time::now(),
        });
    }

    pub fn add_value(&mut self, value: T) {
        self.num_samples += 1;
        if self.lowest_value == T::default() || value < self.lowest_value {
            self.lowest_value = value;
        }
        if value > self.highest_value {
            self.highest_value = value;
        }

        // Ignore duplicate values in collapse window.
        //
        // Bandwidth estimate is received periodically. If the estimate does not change, it will be repeated.
        // When there is congestion, there are several estimates received with decreasing values.
        //
        // Using a sliding window, collapsing repeated values and waiting for falling trend to ensure that
        // the reaction is not too fast, i. e. reacting to falling values too quick could mean a lot of re-allocation
        // resulting in layer switches, key frames and more congestion.
        //
        // But, on the flip side, estimate could fall once or twice within a sliding window and stay there.
        // In those cases, using a collapse window to record a value even if it is duplicate. By doing that,
        // a trend could be detected eventually. It will be delayed, but that is fine with slow changing estimates.
        let last_sample = self.samples.last();
        if let Some(last_sample) = last_sample
            && last_sample.value == value
            && self.params.config.collapse_threshold > Duration::ZERO
            && time::since(last_sample.at) < self.params.config.collapse_threshold
        {
            return;
        }

        self.samples.push(TrendDetectorSample {
            value,
            at: time::now(),
        });
        self.prune();
        self.update_direction();
    }

    pub fn get_lowest(&self) -> T {
        self.lowest_value
    }

    pub fn get_highest(&self) -> T {
        self.highest_value
    }

    pub fn get_direction(&self) -> TrendDirection {
        self.direction
    }

    pub fn has_enough_samples(&self) -> bool {
        self.num_samples >= self.params.config.required_samples
    }
}

impl<T: TrendDetectorNumber> fmt::Debug for TrendDetector<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut samples = Vec::new();
        if !self.samples.is_empty() {
            let first_time = self.samples[0].at;
            for sample in &self.samples {
                samples.push(TrendDetectorSampleElapsed {
                    value: sample.value,
                    since_first: sample.at.sub(first_time),
                });
            }
        }

        f.debug_struct("TrendDetector")
            .field("name", &self.params.name)
            .field("start_time", &self.start_time)
            .field("elapsed", &time::since(self.start_time))
            .field("num_samples", &self.num_samples)
            .field("samples", &samples)
            .field("lowest_value", &self.lowest_value.to_f64())
            .field("highest_value", &self.highest_value.to_f64())
            .field("kendalls_tau", &self.kendalls_tau())
            .field("direction", &format_args!("{}", self.direction))
            .finish()
    }
}

impl<T: TrendDetectorNumber> TrendDetector<T> {
    fn prune(&mut self) {
        // prune based on a few rules

        //  1. If there are more than required samples
        if self.samples.len() > self.params.config.required_samples {
            let cutoff_index = self.samples.len() - self.params.config.required_samples;
            self.samples.drain(..cutoff_index);
        }

        // 2. drop samples that are too old
        if !self.samples.is_empty() && self.params.config.validity_window > Duration::ZERO {
            let cutoff_time = time::now().add(-self.params.config.validity_window);
            let mut cutoff_index = None;
            for (i, sample) in self.samples.iter().enumerate() {
                if sample.at.after(cutoff_time) {
                    cutoff_index = Some(i);
                    break;
                }
            }
            if let Some(cutoff_index) = cutoff_index {
                self.samples.drain(..cutoff_index);
            }
        }

        //  3. collapse same values at the front to just the last of those samples
        if !self.samples.is_empty() {
            let mut cutoff_index = None;
            let first_value = self.samples[0].value;
            for i in 1..self.samples.len() {
                if self.samples[i].value != first_value {
                    cutoff_index = Some(i - 1);
                    break;
                }
            }

            if let Some(cutoff_index) = cutoff_index {
                self.samples.drain(..cutoff_index);
            } else {
                // all values are the same, just keep the last one
                let last = self.samples.len() - 1;
                self.samples.drain(..last);
            }
        }
    }

    fn update_direction(&mut self) {
        if self.samples.len() < self.params.config.required_samples_min {
            self.direction = TrendDirection::Inconclusive;
            return;
        }

        // using Kendall's Tau to find trend
        let kt = self.kendalls_tau();

        self.direction = TrendDirection::Inconclusive;
        if kt > 0.0 && self.samples.len() >= self.params.config.required_samples {
            self.direction = TrendDirection::Upward;
        } else if kt < self.params.config.downward_trend_threshold
            && (self.samples.len() >= self.params.config.required_samples
                || self.samples[self.samples.len() - 1]
                    .at
                    .sub(self.samples[0].at)
                    > self.params.config.downward_trend_max_wait)
        {
            self.direction = TrendDirection::Downward;
        }
    }

    fn kendalls_tau(&self) -> f64 {
        let mut concordant_pairs = 0i64;
        let mut discordant_pairs = 0i64;

        // i < len(t.samples)-1 of the original is a signed comparison: no pairs of no samples
        for i in 0..self.samples.len().saturating_sub(1) {
            for j in i + 1..self.samples.len() {
                if self.samples[i].value < self.samples[j].value {
                    concordant_pairs += 1;
                } else if self.samples[i].value > self.samples[j].value {
                    discordant_pairs += 1;
                }
            }
        }

        if (concordant_pairs + discordant_pairs) == 0 {
            return 0.0;
        }

        (concordant_pairs as f64 - discordant_pairs as f64)
            / (concordant_pairs as f64 + discordant_pairs as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The thresholds the original configures for the trend of the congested packet groups
    // (sendsidebwe), without the collapse window.
    fn config() -> TrendDetectorConfig {
        TrendDetectorConfig {
            required_samples: 4,
            required_samples_min: 2,
            downward_trend_threshold: -0.5,
            downward_trend_max_wait: 2 * time::SECOND,
            collapse_threshold: Duration::ZERO,
            validity_window: 10 * time::SECOND,
        }
    }

    fn new_trend_detector<T: TrendDetectorNumber>(config: TrendDetectorConfig) -> TrendDetector<T> {
        TrendDetector::new(TrendDetectorParams {
            name: "test".to_string(),
            config,
            ..Default::default()
        })
    }

    fn values<T: TrendDetectorNumber>(t: &TrendDetector<T>) -> Vec<T> {
        t.samples.iter().map(|sample| sample.value).collect()
    }

    // New for musivum: the original has no tests. The expectations of the tests here are
    // what the original gives for the same values (checked by running it).
    #[test]
    fn a_rising_series_is_upward_once_there_are_the_required_samples() {
        let mut t = new_trend_detector::<i64>(config());
        assert_eq!(t.get_direction(), TrendDirection::Inconclusive);
        assert!(!t.has_enough_samples());

        for value in [100, 200, 300] {
            t.add_value(value);
            assert_eq!(t.get_direction(), TrendDirection::Inconclusive);
            assert!(!t.has_enough_samples());
        }
        t.add_value(400);
        assert_eq!(t.get_direction(), TrendDirection::Upward);
        assert!(t.has_enough_samples());
        assert_eq!(t.kendalls_tau(), 1.0);

        // only the required samples are kept
        t.add_value(500);
        assert_eq!(values(&t), vec![200, 300, 400, 500]);
        assert_eq!(t.get_direction(), TrendDirection::Upward);

        assert_eq!(t.get_lowest(), 100);
        assert_eq!(t.get_highest(), 500);
    }

    // New for musivum.
    #[test]
    fn a_flat_series_is_inconclusive() {
        let mut t = new_trend_detector::<i64>(config());
        for _ in 0..6 {
            t.add_value(300);
            assert_eq!(t.get_direction(), TrendDirection::Inconclusive);
        }
        // equal values at the front collapse to the last of them
        assert_eq!(values(&t), vec![300]);
        assert_eq!(t.kendalls_tau(), 0.0);
        assert!(t.has_enough_samples());

        // and in the collapse window a repeated value is not even recorded
        let mut t = new_trend_detector::<i64>(TrendDetectorConfig {
            collapse_threshold: 500 * time::MILLISECOND,
            ..config()
        });
        t.add_value(300);
        let at = t.samples[0].at;
        t.add_value(300);
        assert_eq!(values(&t), vec![300]);
        assert_eq!(t.samples[0].at, at);
        assert_eq!(t.get_direction(), TrendDirection::Inconclusive);
    }

    // New for musivum.
    #[test]
    fn a_falling_series_is_downward_below_the_threshold() {
        let mut t = new_trend_detector::<f64>(config());
        for value in [0.9, 0.8, 0.7] {
            t.add_value(value);
            // enough to look at (two samples), but neither the required samples nor the wait
            assert_eq!(t.get_direction(), TrendDirection::Inconclusive);
        }
        t.add_value(0.6);
        assert_eq!(t.get_direction(), TrendDirection::Downward);
        assert_eq!(t.kendalls_tau(), -1.0);

        // the thresholds the original has while probing (remotebwe): three samples, which
        // are also the fewest that are looked at, and any fall counts
        let mut t = new_trend_detector::<i64>(TrendDetectorConfig {
            required_samples: 3,
            required_samples_min: 3,
            downward_trend_threshold: 0.0,
            ..config()
        });
        for value in [300, 200] {
            t.add_value(value);
            assert_eq!(t.get_direction(), TrendDirection::Inconclusive);
        }
        t.add_value(100);
        assert_eq!(t.get_direction(), TrendDirection::Downward);

        // 4 of 6 pairs fall: Kendall's tau is -1/3, which is not below -0.5
        let mut t = new_trend_detector::<i64>(config());
        for value in [4, 1, 3, 2] {
            t.add_value(value);
        }
        assert_eq!(t.get_direction(), TrendDirection::Inconclusive);

        // but it is below -0.3
        let mut t = new_trend_detector::<i64>(TrendDetectorConfig {
            downward_trend_threshold: -0.3,
            ..config()
        });
        for value in [4, 1, 3, 2] {
            t.add_value(value);
        }
        assert_eq!(t.get_direction(), TrendDirection::Downward);

        // and a series that rises as much as it falls has no direction
        let mut t = new_trend_detector::<i64>(config());
        for value in [1, 2, 2, 1] {
            t.add_value(value);
        }
        assert_eq!(t.kendalls_tau(), 0.0);
        assert_eq!(t.get_direction(), TrendDirection::Inconclusive);
    }

    // New for musivum.
    #[test]
    fn a_falling_series_is_downward_early_when_it_has_lasted_the_max_wait() {
        let mut t = new_trend_detector::<i64>(TrendDetectorConfig {
            downward_trend_max_wait: 100 * time::MILLISECOND,
            ..config()
        });
        t.add_value(300);
        t.add_value(200);
        assert_eq!(t.get_direction(), TrendDirection::Inconclusive);
        std::thread::sleep((150 * time::MILLISECOND).to_std());
        t.add_value(100);
        assert_eq!(t.get_direction(), TrendDirection::Downward);
    }

    // New for musivum.
    #[test]
    fn samples_older_than_the_validity_window_are_dropped() {
        let mut t = new_trend_detector::<i64>(TrendDetectorConfig {
            validity_window: 100 * time::MILLISECOND,
            ..config()
        });
        t.add_value(100);
        t.add_value(200);
        t.add_value(300);
        std::thread::sleep((150 * time::MILLISECOND).to_std());
        t.add_value(400);
        assert_eq!(values(&t), vec![400]);
        assert_eq!(t.get_direction(), TrendDirection::Inconclusive);
    }

    // New for musivum.
    #[test]
    fn a_seed_is_only_taken_when_there_are_no_samples() {
        let mut t = new_trend_detector::<i64>(config());
        t.seed(100);
        t.seed(50);
        assert_eq!(values(&t), vec![100]);
        // a seed is a sample, but not a value that was added
        assert_eq!(t.get_lowest(), 0);
        assert_eq!(t.get_highest(), 0);
        for value in [200, 300, 400] {
            t.add_value(value);
        }
        assert_eq!(t.get_direction(), TrendDirection::Upward);
        assert!(!t.has_enough_samples());
        assert_eq!(t.get_lowest(), 200);

        assert_eq!(TrendDirection::Inconclusive.to_string(), "INCONCLUSIVE");
        assert_eq!(TrendDirection::Upward.to_string(), "UPWARD");
        assert_eq!(TrendDirection::Downward.to_string(), "DOWNWARD");
        let logged = format!("{t:?}");
        assert!(logged.contains("name: \"test\""), "{logged}");
        assert!(logged.contains("num_samples: 3"), "{logged}");
        assert!(
            logged.contains("kendalls_tau: 1.0, direction: UPWARD"),
            "{logged}"
        );
    }
}
