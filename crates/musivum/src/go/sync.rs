//! Go's `go f()` for a function that runs and ends without waiting, and `sync.WaitGroup`:
//! for the workers that `sfu/utils.BroadcastRTP` starts for every packet that goes to many
//! down tracks.
//!
//! Go runs a goroutine on one of a few threads, and a thread that finds nothing to run sleeps
//! after a short look around. The workers ran on rayon's pool before: its threads look for
//! work for a while after every job (32 rounds of `sched_yield` and of stealing from every
//! other thread) before they sleep, and a thread that finds work wakes another one to go on
//! looking. For jobs that come hundreds of times a second and take tens of microseconds, the
//! looking cost more than the jobs. The threads here wait on a condition variable as soon as
//! the queue is empty, and a job wakes one of them.
//!
//! A function given to [`go`] is `'static`: what it works on is shared with it by an `Arc`,
//! as Go shares it by a pointer. There are up to [`num_cpu`] threads, started when jobs
//! first need them. A function that panics ends the program in Go; here the thread goes on
//! with the next job, and the panic is the function's to report (see [`WaitGroup`]).

use std::collections::VecDeque;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, LazyLock};

use parking_lot::{Condvar, Mutex};

use crate::protocol::utils::num_cpu;

type Job = Box<dyn FnOnce() + Send>;

/// The threads that run what [`go`] is given.
pub(crate) struct Pool {
    state: Mutex<PoolState>,
    wake: Condvar,
    max_threads: usize,
}

#[derive(Default)]
struct PoolState {
    jobs: VecDeque<Job>,
    threads: usize,
    // threads waiting on `wake`, and of them how many were notified and have not woken yet
    idle: usize,
    notified: usize,
    // how often a waiting thread woke up, for the tests
    wake_ups: u64,
}

static POOL: LazyLock<Arc<Pool>> = LazyLock::new(|| Pool::new(num_cpu()));

/// Go's `go f()`: runs `f` on one of the pool's threads, waking one that sleeps or starting
/// one more if there is none and fewer than [`num_cpu`] are running; otherwise `f` waits
/// for a thread that is done with its job.
pub fn go(f: impl FnOnce() + Send + 'static) {
    POOL.go(Box::new(f));
}

impl Pool {
    pub(crate) fn new(max_threads: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(PoolState::default()),
            wake: Condvar::new(),
            max_threads,
        })
    }

    pub(crate) fn go(self: &Arc<Self>, job: Job) {
        let mut state = self.state.lock();
        state.jobs.push_back(job);
        if state.idle > state.notified {
            state.notified += 1;
            drop(state);
            self.wake.notify_one();
            return;
        }
        if state.threads >= self.max_threads {
            return;
        }
        state.threads += 1;
        drop(state);
        let pool = self.clone();
        let started = std::thread::Builder::new()
            .name("musivum-worker".into())
            .spawn(move || pool.work());
        if started.is_err() {
            // No thread to be had: the job runs on the caller, as Go would end the program.
            let mut state = self.state.lock();
            state.threads -= 1;
            if let Some(job) = state.jobs.pop_front() {
                drop(state);
                job();
            }
        }
    }

    fn work(&self) {
        let mut state = self.state.lock();
        loop {
            if let Some(job) = state.jobs.pop_front() {
                drop(state);
                // The panic message is out already (the hook); the thread goes on.
                let _ = panic::catch_unwind(AssertUnwindSafe(job));
                state = self.state.lock();
                continue;
            }
            state.idle += 1;
            self.wake.wait(&mut state);
            state.idle -= 1;
            state.notified = state.notified.saturating_sub(1);
            state.wake_ups += 1;
        }
    }

    /// How many threads were started, and how often one woke up from waiting for a job.
    #[cfg(test)]
    pub(crate) fn counts(&self) -> (usize, u64) {
        let state = self.state.lock();
        (state.threads, state.wake_ups)
    }
}

/// Go's `sync.WaitGroup`, which also tells whether a worker it waited for panicked.
#[derive(Default)]
pub struct WaitGroup {
    state: Mutex<WaitGroupState>,
    done: Condvar,
}

#[derive(Default)]
struct WaitGroupState {
    count: usize,
    panicked: bool,
}

impl WaitGroup {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&self, delta: usize) {
        self.state.lock().count += delta;
    }

    pub fn done(&self) {
        self.done_after(false);
    }

    /// `defer wg.Done()`: `done` when the guard is dropped, also by a panic, which `wait`
    /// then tells.
    pub fn defer_done(&self) -> DeferDone<'_> {
        DeferDone(self)
    }

    fn done_after(&self, panicked: bool) {
        let mut state = self.state.lock();
        state.panicked |= panicked;
        state.count -= 1;
        if state.count == 0 {
            self.done.notify_all();
        }
    }

    /// Waits until the count is zero, and returns whether a worker panicked.
    pub fn wait(&self) -> bool {
        let mut state = self.state.lock();
        while state.count > 0 {
            self.done.wait(&mut state);
        }
        state.panicked
    }
}

/// What [`WaitGroup::defer_done`] returns.
pub struct DeferDone<'a>(&'a WaitGroup);

impl Drop for DeferDone<'_> {
    fn drop(&mut self) {
        self.0.done_after(std::thread::panicking());
    }
}

#[cfg(test)]
#[path = "sync_test.rs"]
mod sync_test;
