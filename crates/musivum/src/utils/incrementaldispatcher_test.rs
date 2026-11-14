// Ported from LiveKit, pkg/utils/incrementaldispatcher_test.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the goroutines are threads (`for_each` blocks);
// `testutils.WithTimeout` of pkg/testutils/timeout.go, which the test waits with, is ported
// here as `with_timeout`.

use std::sync::Arc;

use super::*;
use crate::go::atomic;
use crate::go::time::{self, Duration, MILLISECOND, SECOND};

// testutils.WithTimeout: asks every 10 ms until `f` returns no complaint, and fails with the
// last complaint when the time is up.
#[track_caller]
fn with_timeout(mut f: impl FnMut() -> String, timeout: Duration) {
    let deadline = time::now().add(timeout);
    loop {
        std::thread::sleep((10 * MILLISECOND).to_std());
        let last_err = f();
        if last_err.is_empty() {
            return;
        }
        if time::now().after(deadline) {
            panic!("did not reach expected state after {timeout}: {last_err}");
        }
    }
}

#[test]
fn test_for_each() {
    let producer = Arc::new(IncrementalDispatcher::<i64>::new());
    let p = producer.clone();
    std::thread::spawn(move || {
        p.add(1);
        p.add(2);
        p.add(3);
        p.done();
    });

    let mut sum = 0;
    producer.for_each(|item| {
        sum += item;
    });

    assert_eq!(6, sum);
}

#[test]
fn test_concurrent_consumption() {
    let producer = Arc::new(IncrementalDispatcher::<i64>::new());
    let num_consumers = 100;
    let sums: Arc<Vec<atomic::Int32>> =
        Arc::new((0..num_consumers).map(|_| atomic::Int32::new(0)).collect());
    let mut wg = Vec::new();

    for i in 0..num_consumers {
        let producer = producer.clone();
        let sums = sums.clone();
        wg.push(std::thread::spawn(move || {
            producer.for_each(|item| {
                sums[i].add(item as i32);
            });
        }));
    }

    // Add items
    let mut expected_sum = 0;
    for i in 0..20 {
        expected_sum += i;
        producer.add(i);
    }

    for (i, sum) in sums.iter().enumerate() {
        with_timeout(
            || {
                if sum.load() != expected_sum as i32 {
                    return format!(
                        "consumer {} did not consume all the items. expected {}, actual: {}",
                        i,
                        expected_sum,
                        sum.load()
                    );
                }
                String::new()
            },
            SECOND,
        );
    }

    // keep adding and ensure it's consumed
    for i in 20..30 {
        expected_sum += i;
        producer.add(i);
    }

    // wait for all consumers to finish
    producer.done();
    for consumer in wg {
        consumer.join().unwrap();
    }

    for (i, sum) in sums.iter().enumerate() {
        assert_eq!(
            expected_sum as i32,
            sum.load(),
            "consumer {i} did not match"
        );
    }
}
