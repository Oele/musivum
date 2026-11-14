// Ported from LiveKit, pkg/sfu/audio/audiolevel_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks of the one test; the random
// first timestamp of `observe_samples_with_rtp_timestamp` comes from the standard library's
// randomly keyed hasher instead of `math/rand`.

use std::hash::{BuildHasher, RandomState};

use super::*;
use crate::go::time::{self, Time};

const SAMPLES_PER_BATCH: i64 = 25;
const DEFAULT_ACTIVE_LEVEL: u8 = 30;
// requires two noisy samples to count
const DEFAULT_PERCENTILE: u8 = 10;
const DEFAULT_OBSERVE_DURATION: u32 = 500; // ms

#[test]
fn test_audio_level() {
    {
        let name = "initially to return not noisy, within a few samples";
        let mut clock = time::now();
        let a = create_audio_level(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION,
        );

        let (_, noisy) = a.get_level(clock.unix_nano());
        assert!(!noisy, "{name}");

        observe_samples(&a, 28, 5, clock);
        clock = clock.add(5 * 20 * time::MILLISECOND);

        let (_, noisy) = a.get_level(clock.unix_nano());
        assert!(!noisy, "{name}");
    }

    {
        let name = "not noisy when all samples are below threshold";
        let mut clock = time::now();
        let a = create_audio_level(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION,
        );

        observe_samples(&a, 35, 100, clock);
        clock = clock.add(100 * 20 * time::MILLISECOND);

        let (_, noisy) = a.get_level(clock.unix_nano());
        assert!(!noisy, "{name}");
    }

    {
        let name = "not noisy when less than percentile samples are above threshold";
        let mut clock = time::now();
        let a = create_audio_level(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION,
        );

        observe_samples(&a, 35, SAMPLES_PER_BATCH - 2, clock);
        clock = clock.add((SAMPLES_PER_BATCH - 2) * 20 * time::MILLISECOND);
        observe_samples(&a, 25, 1, clock);
        clock = clock.add(20 * time::MILLISECOND);
        observe_samples(&a, 35, 1, clock);
        clock = clock.add(20 * time::MILLISECOND);

        let (_, noisy) = a.get_level(clock.unix_nano());
        assert!(!noisy, "{name}");
    }

    {
        let name = "noisy when higher than percentile samples are above threshold";
        let mut clock = time::now();
        let a = create_audio_level(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION,
        );

        observe_samples(&a, 35, SAMPLES_PER_BATCH - 16, clock);
        clock = clock.add((SAMPLES_PER_BATCH - 16) * 20 * time::MILLISECOND);
        observe_samples(&a, 25, 8, clock);
        clock = clock.add(8 * 20 * time::MILLISECOND);
        observe_samples(&a, 29, 8, clock);
        clock = clock.add(8 * 20 * time::MILLISECOND);

        let (level, noisy) = a.get_level(clock.unix_nano());
        assert!(noisy, "{name}");
        assert!(
            level > convert_audio_level(DEFAULT_ACTIVE_LEVEL as f64),
            "{name}: {level}"
        );
        assert!(level < convert_audio_level(25f64), "{name}: {level}");
    }

    {
        let name = "not noisy when samples are stale";
        let mut clock = time::now();
        let a = create_audio_level(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION,
        );

        observe_samples(&a, 25, 100, clock);
        clock = clock.add(100 * 20 * time::MILLISECOND);
        let (level, noisy) = a.get_level(clock.unix_nano());
        assert!(noisy, "{name}");
        assert!(
            level > convert_audio_level(DEFAULT_ACTIVE_LEVEL as f64),
            "{name}: {level}"
        );
        assert!(level < convert_audio_level(20f64), "{name}: {level}");

        // let enough time pass to make the samples stale
        clock = clock.add(1500 * time::MILLISECOND);
        let (level, noisy) = a.get_level(clock.unix_nano());
        assert_eq!(0.0f64, level, "{name}");
        assert!(!noisy, "{name}");
    }

    {
        let name = "not noisy when samples are stale - with RTP timestamp";
        let mut clock = time::now();
        let a = create_audio_level(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION,
        );

        observe_samples_with_rtp_timestamp(&a, 25, 100, clock);
        clock = clock.add(100 * 20 * time::MILLISECOND);
        let (level, noisy) = a.get_level(clock.unix_nano());
        assert!(noisy, "{name}");
        assert!(
            level > convert_audio_level(DEFAULT_ACTIVE_LEVEL as f64),
            "{name}: {level}"
        );
        assert!(level < convert_audio_level(20f64), "{name}: {level}");

        // let enough time pass to make the samples stale
        clock = clock.add(1500 * time::MILLISECOND);
        let (level, noisy) = a.get_level(clock.unix_nano());
        assert_eq!(0.0f64, level, "{name}");
        assert!(!noisy, "{name}");
    }
}

fn create_audio_level(active_level: u8, min_percentile: u8, observe_duration: u32) -> AudioLevel {
    let al = AudioLevel::new(AudioLevelParams { clock_rate: 48000 });
    al.set_config(AudioLevelConfig {
        active_level,
        min_percentile,
        update_interval: observe_duration,
        ..Default::default()
    });
    al
}

fn observe_samples(a: &AudioLevel, level: u8, count: i64, base_time: Time) {
    for i in 0..count {
        a.observe(
            level,
            20,
            base_time.add((i * 20) * time::MILLISECOND).unix_nano(),
        );
    }
}

fn observe_samples_with_rtp_timestamp(a: &AudioLevel, level: u8, count: i64, base_time: Time) {
    // rand.Intn(1 << 20)
    let mut sample_ts = (RandomState::new().hash_one(0u8) % (1 << 20)) as u32;
    let mut sample_time = base_time;
    for i in 0..count {
        if (i % 5) == 0 {
            // out-of-order sample
            a.observe_with_rtp_timestamp(
                level,
                sample_ts.wrapping_sub(1920),
                sample_time.unix_nano(),
            );
        }
        a.observe_with_rtp_timestamp(level, sample_ts, sample_time.unix_nano());
        sample_ts = sample_ts.wrapping_add(960); // 20 ms at 48 kHz
        sample_time = sample_time.add(20 * time::MILLISECOND);
    }
}
