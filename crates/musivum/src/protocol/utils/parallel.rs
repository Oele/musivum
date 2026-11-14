// Ported from LiveKit's protocol, utils/parallel.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the workers are jobs on rayon's pool of threads where
// the original starts goroutines on Go's threads, and the end of a `rayon::in_place_scope` is
// the wait that the original's `sync.WaitGroup` is. They are started as Go's scheduler gets
// its threads to the goroutines, not all at once: the caller is the first worker (in Go the
// thread of the caller runs the goroutines while the caller waits), and a worker that takes
// work while there is more left starts one more, as long as no other is on its way and fewer
// than `numCPU` were started (Go wakes another thread when a woken one has found work, and
// only while none is looking for it); the state is on the caller's stack and borrowed by the
// workers; `fn` (here `f`) gets each element by reference. The workers stay on rayon, where
// those of `sfu/utils.BroadcastRTP` run on `go::sync::go`'s threads: `fn` borrows from the
// caller, which only a scoped pool can take without `unsafe`, and this runs far less often
// than for every packet. `num_cpu` stands in for Go's `runtime.NumCPU()` and is written new.

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
    // New: how many workers may still be started, and whether one was started that has not
    // begun yet (see the header)
    to_start: atomic::Uint64,
    starting: atomic::Bool,
}

impl<'a, T: Sync, F: Fn(&T) + Sync> ParallelState<'a, T, F> {
    fn run(&'a self, s: &rayon::Scope<'a>) {
        loop {
            let n = self.start.add(self.step);
            if n >= self.end.wrapping_add(self.step) {
                return;
            }
            if n < self.end {
                self.start_worker(s);
            }

            let mut i = n.wrapping_sub(self.step);
            while i < n && i < self.end {
                (self.f)(&self.vals[i as usize]);
                i += 1;
            }
        }
    }

    // New: starts one more worker, unless one is on its way or all were started
    fn start_worker(&'a self, s: &rayon::Scope<'a>) {
        if self.starting.load() || !self.starting.cas(false, true) {
            return;
        }
        if self.to_start.load() == 0 {
            // `starting` stays set: there is nothing more to start
            return;
        }
        self.to_start.dec();
        s.spawn(move |s| {
            self.starting.store(false);
            self.run(s);
        });
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
        to_start: atomic::Uint64::new(num_cpu.saturating_sub(1) as u64),
        starting: atomic::Bool::new(false),
    };

    rayon::in_place_scope(|s| st.run(s));
}

#[cfg(test)]
#[path = "parallel_test.rs"]
mod parallel_test;

#[cfg(test)]
#[path = "parallel_bench_test.rs"]
mod parallel_bench_test;
