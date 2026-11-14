// Ported from LiveKit's protocol, utils/parallel_bench_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the goroutines of the test are scoped threads, and a
// failure is a panic of the thread that found it. `BenchmarkParallelExec` and what only it
// uses (`benchSink`, `perItemWork`, `benchmarkParallelExec`) are left out: benchmarks are not
// ported.

use super::*;

// TestParallelExecConcurrent exercises the shared work-stealing state under many
// concurrent callers: every element must be visited exactly once per call.
#[test]
fn test_parallel_exec_concurrent() {
    const N: usize = 137;
    std::thread::scope(|wg| {
        for _g in 0..8 {
            wg.spawn(|| {
                for _iter in 0..300 {
                    let mut vals = vec![0usize; N];
                    let counts: Vec<atomic::Uint32> =
                        (0..N).map(|_| atomic::Uint32::new(0)).collect();
                    for (i, val) in vals.iter_mut().enumerate() {
                        *val = i;
                    }
                    parallel_exec(&vals, 1, 2, |i: &usize| {
                        counts[*i].add(1);
                    });
                    for (i, count) in counts.iter().enumerate() {
                        let got = count.load();
                        if got != 1 {
                            panic!("index {i} visited {got} times");
                        }
                    }
                }
            });
        }
    });
}
