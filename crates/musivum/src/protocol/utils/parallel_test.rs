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

// New for musivum: the caller of `parallel_exec` is the first of its workers, as the thread
// of the original's caller runs the goroutines while the caller waits, and the others are
// started while there is work left: with calls of `f` that take a while, the caller runs the
// first step itself and other threads run `f` too; with calls that take no time, a call
// starts about one worker, not `numCPU` of them at once.
#[test]
fn test_parallel_starts_workers_while_there_is_work() {
    let vals: Vec<usize> = (0..50).collect();
    let me = std::thread::current().id();
    let threads: Vec<parking_lot::Mutex<Option<std::thread::ThreadId>>> =
        vals.iter().map(|_| parking_lot::Mutex::new(None)).collect();
    parallel_exec(&vals, 10, 2, |i: &usize| {
        *threads[*i].lock() = Some(std::thread::current().id());
        std::thread::sleep(std::time::Duration::from_micros(500));
    });
    assert_eq!(Some(me), *threads[0].lock());
    assert_eq!(Some(me), *threads[1].lock());
    let distinct: std::collections::HashSet<_> =
        threads.iter().map(|t| t.lock().unwrap()).collect();
    assert!(distinct.len() > 1, "only the caller ran f");

    const CALLS: u64 = 200;
    let num_workers = 16;
    let mut started = 0;
    for _ in 0..CALLS {
        let st = ParallelState {
            vals: &vals,
            f: |_: &usize| {},
            step: 2,
            end: vals.len() as u64,
            start: atomic::Uint64::new(0),
            to_start: atomic::Uint64::new(num_workers - 1),
            starting: atomic::Bool::new(false),
        };
        rayon::in_place_scope(|s| st.run(s));
        started += num_workers - 1 - st.to_start.load();
    }
    assert!(
        started <= 3 * CALLS,
        "{started} workers started over {CALLS} calls"
    );
}
