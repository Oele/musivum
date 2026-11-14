// Ported from LiveKit, pkg/rtc/subscribepacer_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks; the check of `Delays` on a
// nil pacer is left out, see there. The two tests at the end are new.

use super::*;

use crate::go::time::{self, MILLISECOND};

#[test]
fn test_subscribe_pacer() {
    {
        let name = "off without rates";
        assert!(SubscribePacer::new(&[]).is_none(), "{name}");
        assert!(SubscribePacer::new(&[0.0, -5.0]).is_none(), "{name}");

        // The original also checks that `Delays` on a nil pacer gives a nil map. That has no
        // counterpart: a pacer that is off is `None`, and there is nothing to call it on.
    }

    {
        let name = "one second of burst, then one token per interval";
        let p = SubscribePacer::new(&[10.0, 100.0]).unwrap();
        let now = time::now();

        for i in 0..10 {
            assert_eq!(
                HashMap::from([
                    ("10".to_string(), Duration::ZERO),
                    ("100".to_string(), Duration::ZERO)
                ]),
                p.delays(now),
                "{name}: subscribe {i}"
            );
        }
        assert_eq!(
            HashMap::from([
                ("10".to_string(), 100 * MILLISECOND),
                ("100".to_string(), Duration::ZERO)
            ]),
            p.delays(now),
            "{name}"
        );
        assert_eq!(
            HashMap::from([
                ("10".to_string(), 200 * MILLISECOND),
                ("100".to_string(), Duration::ZERO)
            ]),
            p.delays(now),
            "{name}"
        );
    }

    {
        let name = "a burst that outruns the rate waits in line";
        let p = SubscribePacer::new(&[50.0]).unwrap();
        let now = time::now();

        let mut last = Duration::ZERO;
        for _ in 0..150 {
            last = p.delays(now)["50"];
        }
        // 50 go at once, the other 100 drain at 50 per second
        assert_eq!(2 * SECOND, last, "{name}");
    }

    {
        let name = "a rate below one per second does not delay the first subscribe";
        let p = SubscribePacer::new(&[0.5]).unwrap();
        let now = time::now();

        assert_eq!(Duration::ZERO, p.delays(now)["0.5"], "{name}");
        assert_eq!(2 * SECOND, p.delays(now)["0.5"], "{name}");
    }

    {
        let name = "a sample taken earlier than the last one waits from its own time";
        let p = SubscribePacer::new(&[10.0]).unwrap();
        let now = time::now();

        for _ in 0..10 {
            p.delays(now);
        }
        // the next token is free at now + 1 s, and the burst covers 900 ms of it
        assert_eq!(
            600 * MILLISECOND,
            p.delays(now.add(-500 * MILLISECOND))["10"],
            "{name}"
        );
    }

    {
        let name = "duplicate rates make one bucket";
        let p = SubscribePacer::new(&[50.0, 10.0, 50.0]).unwrap();
        assert_eq!(2, p.buckets.lock().len(), "{name}");
    }

    {
        let name = "refills while idle";
        let p = SubscribePacer::new(&[10.0]).unwrap();
        let now = time::now();

        for _ in 0..12 {
            p.delays(now);
        }
        assert_eq!(
            Duration::ZERO,
            p.delays(now.add(2 * SECOND))["10"],
            "{name}"
        );
    }
}

// New for musivum: the buckets that rates make, in their order, each with its label, its
// interval and its burst in nanoseconds. The expected values are what the original gives for
// the same rates (printed by a Go test at the pin).
#[test]
fn rates_make_the_buckets_of_the_original() {
    // label, interval, burst
    type Bucket<'a> = (&'a str, i64, i64);
    let table: [(&[f64], &[Bucket]); 11] = [
        (
            &[10.0, 100.0],
            &[("10", 100000000, 900000000), ("100", 10000000, 990000000)],
        ),
        (&[0.5], &[("0.5", 2000000000, 0)]),
        (
            &[50.0, 10.0, 50.0],
            &[("10", 100000000, 900000000), ("50", 20000000, 980000000)],
        ),
        (&[0.001], &[("0.001", 1000000000000, 0)]),
        (&[1e21], &[("1000000000000000000000", 0, 1000000000)]),
        (&[1e-7], &[("0.0000001", 10000000000000000, 0)]),
        (
            &[2.5, 1.0 / 3.0],
            &[
                ("0.3333333333333333", 3000000000, 0),
                ("2.5", 400000000, 600000000),
            ],
        ),
        (&[f64::INFINITY], &[("+Inf", 0, 1000000000)]),
        (
            &[1.0, 1.5, 1e6, 123456789.125],
            &[
                ("1", 1000000000, 0),
                ("1.5", 666666666, 333333334),
                ("1000000", 1000, 999999000),
                ("123456789.125", 8, 999999992),
            ],
        ),
        (
            &[3.0, 1.0, 2.0],
            &[
                ("1", 1000000000, 0),
                ("2", 500000000, 500000000),
                ("3", 333333333, 666666667),
            ],
        ),
        (
            &[1e9, 2e9, 3e9, 0.1, 0.3, 7.0],
            &[
                ("0.1", 10000000000, 0),
                ("0.3", 3333333333, 0),
                ("7", 142857142, 857142858),
                ("1000000000", 1, 999999999),
                ("2000000000", 0, 1000000000),
                ("3000000000", 0, 1000000000),
            ],
        ),
    ];
    for (rates, expected) in table {
        let p = SubscribePacer::new(rates).unwrap();
        let buckets = p.buckets.lock();
        let got: Vec<(&str, i64, i64)> = buckets
            .iter()
            .map(|b| (b.rate_label.as_str(), b.interval.0, b.burst.0))
            .collect();
        assert_eq!(expected, got, "rates {rates:?}");
    }

    // A rate that is not a number is sorted before the others and gets a bucket, as upstream.
    // (Its interval is the conversion of a NaN to an integer, which Go leaves undefined.)
    let p = SubscribePacer::new(&[1.0, f64::NAN]).unwrap();
    let labels: Vec<String> = p
        .buckets
        .lock()
        .iter()
        .map(|b| b.rate_label.clone())
        .collect();
    assert_eq!(vec!["NaN", "1"], labels);
    let p = SubscribePacer::new(&[f64::NAN, f64::NAN]).unwrap();
    assert_eq!(2, p.buckets.lock().len());
}

// New for musivum: subscribes that come 100 ms apart, at rates of 3 and of 0.4 per second.
// The expected delays, in nanoseconds, are what the original gives.
#[test]
fn delays_of_spaced_subscribes_are_those_of_the_original() {
    let p = SubscribePacer::new(&[3.0, 0.4]).unwrap();
    let now = Time::unix(1700000000, 0);
    let expected: [(i64, i64); 6] = [
        (0, 0),
        (0, 2400000000),
        (0, 4800000000),
        (33333332, 7200000000),
        (266666665, 9600000000),
        (499999998, 12000000000),
    ];
    for (i, (at_3, at_0_4)) in expected.into_iter().enumerate() {
        let d = p.delays(now.add(i as i64 * 100 * MILLISECOND));
        assert_eq!(2, d.len());
        assert_eq!((at_3, at_0_4), (d["3"].0, d["0.4"].0), "subscribe {i}");
    }
}
