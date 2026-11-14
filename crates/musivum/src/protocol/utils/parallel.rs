// Ported from LiveKit's protocol, utils/parallel.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the workers are scoped threads of the operating system
// where the original starts goroutines, and the end of the scope is the wait that the
// original's `sync.WaitGroup` is; the state is on the caller's stack and borrowed by the
// workers; `fn` (here `f`) gets each element by reference. `num_cpu` stands in for Go's
// `runtime.NumCPU()` and is written new.

use std::sync::LazyLock;

use crate::go::atomic;

/// Go's `runtime.NumCPU()`: the number of processors the process can use, read once.
pub(crate) fn num_cpu() -> usize {
    static NUM_CPU: LazyLock<usize> = LazyLock::new(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });
    *NUM_CPU
}

// parallelState holds all shared state for one parallel ParallelExec call in a
// single object, which the workers share through one reference.
struct ParallelState<'a, T, F> {
    vals: &'a [T],
    f: F,
    step: u64,
    end: u64,
    start: atomic::Uint64,
}

impl<T, F: Fn(&T)> ParallelState<'_, T, F> {
    fn run(&self) {
        loop {
            let n = self.start.add(self.step);
            if n >= self.end.wrapping_add(self.step) {
                return;
            }

            let mut i = n.wrapping_sub(self.step);
            while i < n && i < self.end {
                (self.f)(&self.vals[i as usize]);
                i += 1;
            }
        }
    }
}

/// ParallelExec will executes the given function with each element of vals, if len(vals) >= parallelThreshold,
/// will execute them in parallel, with the given step size. So fn must be thread-safe.
pub fn parallel_exec<T, F>(vals: &[T], parallel_threshold: u64, step: u64, f: F)
where
    T: Sync,
    F: Fn(&T) + Sync,
{
    if (vals.len() as u64) < parallel_threshold {
        for v in vals {
            f(v);
        }
        return;
    }

    // parallel - enables much more efficient multi-core utilization
    let mut num_cpu = num_cpu();
    if num_cpu > vals.len() {
        num_cpu = vals.len();
    }

    let st = ParallelState {
        vals,
        f,
        step,
        end: vals.len() as u64,
        start: atomic::Uint64::new(0),
    };

    std::thread::scope(|s| {
        for _ in 0..num_cpu {
            s.spawn(|| st.run());
        }
    });
}

#[cfg(test)]
#[path = "parallel_test.rs"]
mod parallel_test;

#[cfg(test)]
#[path = "parallel_bench_test.rs"]
mod parallel_bench_test;
