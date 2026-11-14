// Ported from LiveKit, pkg/sfu/audio/audiolevel.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; everything `lock` guards, which is all but the params,
// is in an inner struct, and the functions called with the lock held take it; the YAML tags
// of `AudioLevelConfig` are not carried over (the fields have the names of the keys).

use parking_lot::Mutex;

const SILENT_AUDIO_LEVEL: u8 = 127;
const NEG_INV_20: f64 = -1.0 / 20.0;

// --------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AudioLevelConfig {
    // minimum level to be considered active, 0-127, where 0 is loudest
    pub active_level: u8,
    // percentile to measure, a participant is considered active if it has exceeded the ActiveLevel more than
    // MinPercentile% of the time
    pub min_percentile: u8,
    // interval to update clients, in ms
    pub update_interval: u32,
    // smoothing for audioLevel values sent to the client.
    // audioLevel will be an average of `smooth_intervals`, 0 to disable
    pub smooth_intervals: u32,
}

pub const DEFAULT_AUDIO_LEVEL_CONFIG: AudioLevelConfig = AudioLevelConfig {
    active_level: 35, // -35dBov
    min_percentile: 40,
    update_interval: 400,
    smooth_intervals: 2,
};

// --------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AudioLevelParams {
    pub clock_rate: u32,
}

// keeps track of audio level for a participant
pub struct AudioLevel {
    params: AudioLevelParams,

    // what `lock sync.Mutex` guards upstream
    inner: Mutex<AudioLevelInner>,
}

struct AudioLevelInner {
    config: AudioLevelConfig,

    // min duration within an observe duration window to be considered active
    min_active_duration: u32,
    smooth_factor: f64,
    active_threshold: f64,

    smoothed_level: f64,

    loudest_observed_level: u8,
    active_duration: u32,   // ms
    observed_duration: u32, // ms
    last_observed_at: i64,

    highest_rtp_timestamp: u32,
    highest_rtp_timestamp_initialized: bool,
}

impl AudioLevel {
    pub fn new(params: AudioLevelParams) -> Self {
        Self {
            params,
            inner: Mutex::new(AudioLevelInner {
                config: AudioLevelConfig::default(),
                min_active_duration: 0,
                smooth_factor: 1.0,
                active_threshold: 0.0,
                smoothed_level: 0.0,
                loudest_observed_level: SILENT_AUDIO_LEVEL,
                active_duration: 0,
                observed_duration: 0,
                last_observed_at: 0,
                highest_rtp_timestamp: 0,
                highest_rtp_timestamp_initialized: false,
            }),
        }
    }

    pub fn set_config(&self, config: AudioLevelConfig) {
        let mut l = self.inner.lock();

        l.config = config;
        l.min_active_duration =
            (l.config.min_percentile as u32).wrapping_mul(l.config.update_interval) / 100;
        if l.config.smooth_intervals > 0 {
            // exponential moving average (EMA), same center of mass with simple moving average (SMA)
            l.smooth_factor = 2f64 / (l.config.smooth_intervals.wrapping_add(1) as f64);
        } else {
            l.smooth_factor = 1.0;
        }
        l.active_threshold = convert_audio_level(l.config.active_level as f64);
    }

    // Observes a new frame
    pub fn observe(&self, level: u8, duration_ms: u32, arrival_time: i64) {
        let mut l = self.inner.lock();

        Self::observe_locked(&mut l, level, duration_ms, arrival_time);
    }

    fn observe_locked(l: &mut AudioLevelInner, level: u8, duration_ms: u32, arrival_time: i64) {
        if l.config.update_interval == 0 {
            return;
        }

        l.last_observed_at = arrival_time;

        l.observed_duration = l.observed_duration.wrapping_add(duration_ms);

        if level <= l.config.active_level {
            l.active_duration = l.active_duration.wrapping_add(duration_ms);
            if l.loudest_observed_level > level {
                l.loudest_observed_level = level;
            }
        }

        if l.observed_duration >= l.config.update_interval {
            let mut smoothed_level = 0.0f64;
            // compute and reset
            if l.active_duration >= l.min_active_duration {
                // adjust loudest observed level by how much of the window was active.
                // Weight will be 0 if active the entire duration
                // > 0 if active for longer than observe duration
                // < 0 if active for less than observe duration
                let activity_weight =
                    20.0 * (l.active_duration as f64 / l.config.update_interval as f64).log10();
                let adjusted_level = l.loudest_observed_level as f64 - activity_weight;
                let linear_level = convert_audio_level(adjusted_level);

                // exponential smoothing to dampen transients
                smoothed_level =
                    l.smoothed_level + (linear_level - l.smoothed_level) * l.smooth_factor;
            }
            Self::reset_locked(l, smoothed_level);
        }
    }

    pub fn observe_with_rtp_timestamp(&self, level: u8, ts: u32, arrival_time: i64) {
        let mut l = self.inner.lock();

        if !l.highest_rtp_timestamp_initialized {
            l.highest_rtp_timestamp_initialized = true;
            l.highest_rtp_timestamp = ts;
        }

        if ts.wrapping_sub(l.highest_rtp_timestamp) < (1 << 31) {
            let duration_ms = ts.wrapping_sub(l.highest_rtp_timestamp).wrapping_mul(1000)
                / self.params.clock_rate;
            Self::observe_locked(&mut l, level, duration_ms, arrival_time);

            l.highest_rtp_timestamp = ts;
        }
    }

    // returns current smoothed audio level
    pub fn get_level(&self, now: i64) -> (f64, bool) {
        let mut l = self.inner.lock();

        if l.config.update_interval == 0 {
            return (0.0, false);
        }

        Self::reset_if_stale_locked(&mut l, now);

        (l.smoothed_level, l.smoothed_level >= l.active_threshold)
    }

    fn reset_if_stale_locked(l: &mut AudioLevelInner, arrival_time: i64) {
        if arrival_time.wrapping_sub(l.last_observed_at) / 1_000_000
            < 2u32.wrapping_mul(l.config.update_interval) as i64
        {
            return;
        }

        Self::reset_locked(l, 0.0);
    }

    fn reset_locked(l: &mut AudioLevelInner, smoothed_level: f64) {
        l.smoothed_level = smoothed_level;
        l.loudest_observed_level = SILENT_AUDIO_LEVEL;
        l.active_duration = 0;
        l.observed_duration = 0;
    }
}

// ---------------------------------------------------

// convert decibel back to linear
pub fn convert_audio_level(level: f64) -> f64 {
    10f64.powf(level * NEG_INV_20)
}

#[cfg(test)]
#[path = "audiolevel_test.rs"]
mod audiolevel_test;
