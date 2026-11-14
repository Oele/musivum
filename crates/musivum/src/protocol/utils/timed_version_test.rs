// Ported from LiveKit's protocol, utils/timed_version_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the time before the epoch that one subtest starts from
// is the zero time of the port (1970), where the original takes the year 0, which a time
// here cannot hold. The tests after `test_timed_version` are new and say so.

use super::*;

#[test]
fn test_timed_version() {
    // timed versions are monotonic and comparable
    {
        let generator = new_default_timed_version_generator();
        let tv1 = generator.next();
        let mut tv2 = generator.next();
        let tv3 = generator.next();

        assert!(tv3.after(tv1));
        assert!(tv3.after(tv2));
        assert!(tv2.after(tv1));

        tv2.update(tv3);
        assert!(tv2.after(tv1));
        // tv3 and tv2 are equivalent after update
        assert!(!tv3.after(tv2));

        assert_eq!(0, tv1.compare(tv1));
        assert_eq!(-1, tv1.compare(tv2));
        assert_eq!(1, tv2.compare(tv1));
    }

    // protobuf roundtrip
    {
        let generator = new_default_timed_version_generator();
        let tv1 = generator.next();
        let tv2 = timed_version_from_proto(Some(&tv1.to_proto()));
        assert_eq!(tv1, tv2);
    }

    // from zero time yields epoch version
    {
        let generator = new_default_timed_version_generator();
        let tv1 = timed_version_from_time(Time::ZERO);
        let tv2 = generator.next();
        assert!(tv2.after(tv1));
    }

    // time.Time roundtrip
    {
        // time.Now().Round(time.Microsecond)
        let ts1 = Time::from_unix_nano((time::now().unix_nano() + 500) / 1000 * 1000);
        let tv1 = timed_version_from_time(ts1);
        let ts2 = tv1.time();
        let tv2 = timed_version_from_time(ts2);
        assert_eq!(ts1, ts2);
        assert_eq!(tv1, tv2);
    }

    // timed version from nil is zero
    {
        assert!(timed_version_from_proto(None).is_zero());
    }
}

// New for musivum: values the original gives (run at the pin), for the epoch and for how a
// time and its ticks are packed.
#[test]
fn a_version_is_packed_as_the_original_packs_it() {
    assert_eq!(943_920_000_000_000, EPOCH);

    let tv = timed_version_from_components(1_700_000_000_000_000, 5);
    assert_eq!(6_193_807_360_000_000_005, tv.0);
    assert_eq!("1700000000000000.5", tv.to_string());
    assert_eq!((1_700_000_000_000_000, 5), timed_version_components(tv));
    let proto = livekit::TimedVersion {
        unix_micro: 1_700_000_000_000_000,
        ticks: 5,
    };
    assert_eq!(tv, timed_version_from_proto(Some(&proto)));
    assert_eq!(proto, tv.to_proto());

    let t = timed_version_from_time(Time::unix(1_700_000_000, 123_456_789)).time();
    assert_eq!(1_700_000_000_123_456_000, t.unix_nano());

    // what comes before the epoch is the epoch
    assert!(timed_version_from_components(EPOCH - 1, 0).is_zero());
    assert_eq!(TimedVersion(1), TimedVersion(0).next());
    assert_eq!(TimedVersion(0), TimedVersion(0).prev());
    assert_eq!(TimedVersion(6), TimedVersion(7).prev());

    let mut tv = TimedVersion(5);
    assert!(!tv.downgrade(TimedVersion(6)));
    assert!(tv.downgrade(TimedVersion(4)));
    assert!(!tv.upgrade(TimedVersion(4)));
    tv.store(TimedVersion(9));
    assert_eq!(TimedVersion(9), tv.load());
}

// New for musivum: versions in a row, from one thread and from several at once.
#[test]
fn the_generator_never_repeats_or_goes_back() {
    let generator = new_default_timed_version_generator();
    let mut last = generator.next();
    for _ in 0..20_000 {
        let tv = generator.next();
        assert!(tv.after(last));
        last = tv;
    }

    let mut versions: Vec<TimedVersion> = std::thread::scope(|s| {
        let threads: Vec<_> = (0..8)
            .map(|_| s.spawn(|| (0..5_000).map(|_| generator.next()).collect::<Vec<_>>()))
            .collect();
        threads
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .collect()
    });
    assert!(versions.iter().all(|tv| tv.after(last)));
    versions.sort();
    versions.dedup();
    assert_eq!(8 * 5_000, versions.len());
}

// New for musivum: a generator whose last version is ahead of the clock (another thread read
// the clock later and got its version first) stays at that time and counts on.
#[test]
fn the_generator_does_not_go_back_to_an_earlier_time() {
    let generator = DefaultTimedVersionGenerator::default();
    let ahead = time::now().unix_micro() + 3_600_000_000;
    *generator.mu.lock() = DefaultTimedVersionGeneratorInner {
        ts: ahead,
        ticks: 5,
    };
    assert_eq!((ahead, 6), timed_version_components(generator.next()));
    assert_eq!((ahead, 7), timed_version_components(generator.next()));
}

// New for musivum: more versions in one microsecond than it has ticks for: the generator
// waits for the clock to move on.
#[test]
fn the_generator_waits_when_a_microsecond_is_used_up() {
    let generator = DefaultTimedVersionGenerator::default();
    let ahead = time::now().unix_micro() + 20_000;
    *generator.mu.lock() = DefaultTimedVersionGeneratorInner {
        ts: ahead,
        ticks: TICK_MASK - 1,
    };
    let last = generator.next();
    assert_eq!((ahead, TICK_MASK as i32), timed_version_components(last));

    let tv = generator.next();
    assert!(time::now().unix_micro() > ahead);
    assert!(tv.after(last));
    let (ts, ticks) = timed_version_components(tv);
    assert!(ts > ahead);
    assert_eq!(0, ticks);
}
