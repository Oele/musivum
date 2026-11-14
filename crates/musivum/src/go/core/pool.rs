// Ported from frostbyte73/core, pool.go at v0.1.1, with what it uses of fuse.go.
// Copyright (c) 2023 David Colburn. Licensed under the MIT License.
// Changes: translated from Go to Rust. It is the `core.QueuePool` that `URLNotifier` sends
// with. A job is a future, which the
// worker, a task, awaits where the original calls a function. The `next` channel is a tokio
// channel of one. `Fuse` is written out with what the pool needs of it: breaking it once,
// whether it is broken, a future that ends when it is, and, new, a blocking wait for
// `Drain`, whose wait is a goroutine's upstream. `drain` takes the jobs that are left out of
// the queue under the worker's lock and runs them after unlocking, where the original runs
// them with the lock held (a future cannot be awaited under a lock of parking_lot): a
// `Submit` meanwhile queues a job that is never run in both. `block_in_place` is new.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::go::time;

pub const DEFAULT_QUEUE_SIZE: i64 = 100;
pub const DEFAULT_CAPACITY: i64 = 10;

pub type Job = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Clone, Copy, Debug, Default)]
pub struct QueueWorkerParams {
    pub queue_size: i64,
    pub drop_when_full: bool,
}

pub struct QueuePool {
    inner: Mutex<QueuePoolInner>,

    capacity: usize,
    params: QueueWorkerParams,
    drain: Fuse,
    kill: Fuse,
}

struct QueuePoolInner {
    workers: Vec<Option<Arc<QueueWorker>>>,
}

pub fn new_queue_pool(max_workers: i64, mut params: QueueWorkerParams) -> QueuePool {
    let max_workers = if max_workers <= 0 {
        DEFAULT_CAPACITY
    } else {
        max_workers
    };
    if params.queue_size == 0 {
        params.queue_size = DEFAULT_QUEUE_SIZE;
    }

    QueuePool {
        inner: Mutex::new(QueuePoolInner {
            workers: (0..max_workers).map(|_| None).collect(),
        }),
        capacity: max_workers as usize,
        params,
        drain: Fuse::default(),
        kill: Fuse::default(),
    }
}

impl QueuePool {
    fn hash(&self, key: &str) -> usize {
        // FNV-1a, 32 bits
        let mut h: u32 = 0x811c_9dc5;
        for b in key.bytes() {
            h ^= b as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
        h as usize % self.capacity
    }

    pub fn submit(&self, key: &str, job: impl Future<Output = ()> + Send + 'static) -> bool {
        let mut p = self.inner.lock();
        if self.kill.is_broken() {
            drop(p);
            return false;
        }

        let idx = self.hash(key);
        let w = p.workers[idx]
            .get_or_insert_with(|| new_queue_worker(self.params))
            .clone();
        drop(p);

        w.submit(Box::pin(job))
    }

    pub fn drain(&self) {
        self.drain.once(|| {
            let p = self.inner.lock();

            block_in_place(|| {
                std::thread::scope(|wg| {
                    for w in p.workers.iter().flatten() {
                        wg.spawn(move || {
                            w.drain();
                        });
                    }
                });
            });

            drop(p);
        });
    }

    pub fn kill(&self) {
        self.kill.once(|| {
            let p = self.inner.lock();
            for w in p.workers.iter().flatten() {
                w.kill();
            }
            drop(p);
        });
    }
}

struct QueueWorker {
    inner: Mutex<QueueWorkerInner>,
    queue_worker_params: QueueWorkerParams,

    next: mpsc::Sender<Job>,
    draining: Fuse,
    done: Fuse,
    kill: Fuse,
}

struct QueueWorkerInner {
    active: bool,
    deque: VecDeque<Job>,
}

fn new_queue_worker(mut params: QueueWorkerParams) -> Arc<QueueWorker> {
    if params.queue_size == 0 {
        params.queue_size = DEFAULT_QUEUE_SIZE;
    }

    let (next, next_rx) = mpsc::channel(1);
    let w = Arc::new(QueueWorker {
        inner: Mutex::new(QueueWorkerInner {
            active: false,
            deque: VecDeque::with_capacity(params.queue_size.clamp(0, 1024) as usize),
        }),
        queue_worker_params: params,
        next,
        draining: Fuse::default(),
        done: Fuse::default(),
        kill: Fuse::default(),
    });
    let worker = w.clone();
    time::spawn(async move { worker.run(next_rx).await });
    w
}

impl QueueWorker {
    async fn run(&self, mut next: mpsc::Receiver<Job>) {
        let kill = self.kill.watch();
        let draining = self.draining.watch();
        loop {
            tokio::select! {
                _ = kill.cancelled() => {
                    return;
                }
                _ = draining.cancelled() => {
                    self.drain_jobs(&mut next).await;
                    return;
                }
                job = next.recv() => {
                    if let Some(job) = job {
                        job.await;
                    }

                    let mut w = self.inner.lock();
                    if let Some(job) = w.deque.pop_front() {
                        let _ = self.next.try_send(job);
                    } else {
                        w.active = false;
                    }
                    drop(w);
                }
            }
        }
    }

    fn submit(&self, job: Job) -> bool {
        let mut submitted = true;
        let mut w = self.inner.lock();
        if w.active {
            if self.queue_worker_params.drop_when_full
                && w.deque.len() as i64 == self.queue_worker_params.queue_size
            {
                submitted = false;
            } else {
                w.deque.push_back(job);
            }
        } else {
            w.active = true;
            let _ = self.next.try_send(job);
        }
        drop(w);
        submitted
    }

    fn drain(&self) {
        self.draining.break_fuse();
        // select on done and kill
        self.done.wait_or(&self.kill);
    }

    async fn drain_jobs(&self, next: &mut mpsc::Receiver<Job>) {
        let (job, count) = {
            let w = self.inner.lock();

            let job = next.try_recv().ok();
            (job, w.deque.len())
        };
        if let Some(job) = job {
            job.await;
        }
        let jobs: Vec<Job> = {
            let mut w = self.inner.lock();
            let count = count.min(w.deque.len());
            w.deque.drain(..count).collect()
        };
        for job in jobs {
            job.await;
        }

        self.done.break_fuse();
    }

    fn kill(&self) {
        self.kill.break_fuse();
    }
}

/// `core.Fuse`, with what the pool uses of it.
#[derive(Default)]
pub struct Fuse {
    token: CancellationToken,
    // the mutex of the original's `Once`; also what `wait_or` waits on
    m: Mutex<()>,
    broken: Condvar,
}

impl Fuse {
    // IsBroken returns true if the fuse has been broken
    pub fn is_broken(&self) -> bool {
        self.token.is_cancelled()
    }

    // Watch returns a channel which will close once the fuse is broken
    pub fn watch(&self) -> CancellationToken {
        self.token.clone()
    }

    // Break breaks the fuse. It returns true if it was broken by this call.
    pub fn break_fuse(&self) -> bool {
        let mut broken = false;
        self.once(|| {
            broken = true;
        });
        broken
    }

    // Once runs the callback and breaks the fuse
    pub fn once(&self, f: impl FnOnce()) {
        if self.token.is_cancelled() {
            return;
        }
        let m = self.m.lock();
        if self.token.is_cancelled() {
            return;
        }
        f();
        self.token.cancel();
        drop(m);
        self.broken.notify_all();
    }

    /// New: blocks until this fuse or `other` is broken, what a goroutine does upstream with
    /// a `select` on the two channels.
    fn wait_or(&self, other: &Fuse) {
        block_in_place(|| {
            let mut m = self.m.lock();
            while !self.is_broken() && !other.is_broken() {
                // `other` has a condition variable of its own: look again now and then.
                self.broken
                    .wait_for(&mut m, std::time::Duration::from_millis(10));
            }
        });
    }
}

/// New: runs `f`, which blocks, so that the tasks of the runtime the caller is on keep
/// running: on a thread of a multi-threaded runtime it hands that thread's tasks to another.
pub fn block_in_place<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

#[cfg(test)]
#[path = "pool_test.rs"]
mod pool_test;
