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
