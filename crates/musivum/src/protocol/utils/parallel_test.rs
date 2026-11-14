// Ported from LiveKit's protocol, utils/parallel_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the buffered channel is `std::sync::mpsc::sync_channel`.

use std::sync::mpsc;

use super::*;

#[test]
fn test_parallel() {
    for cs in [1usize, 10, 100, 10000] {
        let mut send_slice: Vec<i64> = Vec::with_capacity(cs);
        let mut recv_slice: Vec<i64> = Vec::with_capacity(cs);
        let (recv_ch_tx, recv_ch) = mpsc::sync_channel::<i64>(cs);
        for i in 0..cs {
            send_slice.push(i as i64);
        }

        parallel_exec(&send_slice, 10, 2, |i: &i64| {
            recv_ch_tx.send(*i).unwrap();
        });
        drop(recv_ch_tx);

        for i in recv_ch {
            recv_slice.push(i);
        }

        recv_slice.sort();

        assert_eq!(send_slice, recv_slice, "{cs}");
    }
}

// New for musivum: the workers of `parallel_exec` run on a fixed set of threads, as the
// original's goroutines run on Go's, and not on threads started for each call. Every call
// above the threshold has at least one worker that runs `f`, so threads of each call's own
// would show more than `num_cpu() + 1` of them over `num_cpu() + 2` calls.
#[test]
fn test_parallel_reuses_threads() {
    let vals: Vec<i64> = (0..100).collect();
    let threads = parking_lot::Mutex::new(std::collections::HashSet::new());
    let count = atomic::Uint64::new(0);

    let calls = num_cpu() + 2;
    for _ in 0..calls {
        parallel_exec(&vals, 10, 2, |_: &i64| {
            threads.lock().insert(std::thread::current().id());
            count.add(1);
        });
    }

    assert_eq!((calls * vals.len()) as u64, count.load());
    let num_threads = threads.lock().len();
    assert!(
        num_threads <= num_cpu() + 1,
        "{num_threads} threads ran f over {calls} calls, num_cpu is {}",
        num_cpu()
    );
}
