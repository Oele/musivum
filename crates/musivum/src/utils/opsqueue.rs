// Ported from LiveKit, pkg/utils/opsqueue.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The worker goroutine is a thread, not a task: an op is
// a function that may block, and `<-oq.Stop()` has to be able to wait for the worker from any
// thread, also from one of the runtime, which a task cannot promise. The thread enters the
// runtime of whoever called `start`, so that timers and tasks an op starts run where the
// caller's do. The channel `Stop()` returns is a `Done`: `<-oq.Stop()` is `oq.stop().wait()`.
// The wake channel is a flag under the queue's lock with a condition variable (it is written
// to under that lock upstream, and closed when `isStopped` is set). Go's embedding is written
// out: `OpsQueue` and `TypedOpsQueue` hand `start` and `stop` on to the base, which they
// share with the worker. New: a queue
// that is dropped without having been stopped lets its worker run what is queued and end,
// instead of leaving a thread behind as Go leaves a goroutine. New: an op that panics, which
// ends the program in Go, ends the worker here: the queue is then stopped, what was queued is
// dropped and the channel of `Stop()` is closed.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::go::time::{self, Duration};
use crate::protocol::logger::Logger;

#[derive(Clone, Debug, Default)]
pub struct OpsQueueParams {
    pub name: String,
    pub min_size: usize,
    pub flush_on_stop: bool,
    pub logger: Logger,
}

pub type UntypedQueueOp = Box<dyn FnOnce() + Send>;

impl OpsQueueItem for UntypedQueueOp {
    fn run(self) {
        self()
    }
}

pub struct OpsQueue {
    ops_queue_base: Arc<OpsQueueBase<UntypedQueueOp>>,
}

impl OpsQueue {
    pub fn new(params: OpsQueueParams) -> Self {
        Self {
            ops_queue_base: OpsQueueBase::new(params),
        }
    }

    pub fn start(&self) {
        self.ops_queue_base.start();
    }

    pub fn stop(&self) -> Done {
        self.ops_queue_base.stop()
    }

    pub fn enqueue(&self, op: impl FnOnce() + Send + 'static) {
        self.ops_queue_base.enqueue(Box::new(op));
    }
}

impl Drop for OpsQueue {
    fn drop(&mut self) {
        self.ops_queue_base.abandon();
    }
}

struct TypedQueueOp<T> {
    f: Box<dyn FnOnce(T) + Send>,
    arg: T,
}

impl<T: Send + 'static> OpsQueueItem for TypedQueueOp<T> {
    fn run(self) {
        (self.f)(self.arg)
    }
}

pub struct TypedOpsQueue<T: Send + 'static> {
    ops_queue_base: Arc<OpsQueueBase<TypedQueueOp<T>>>,
}

impl<T: Send + 'static> TypedOpsQueue<T> {
    pub fn new(params: OpsQueueParams) -> Self {
        Self {
            ops_queue_base: OpsQueueBase::new(params),
        }
    }

    pub fn start(&self) {
        self.ops_queue_base.start();
    }

    pub fn stop(&self) -> Done {
        self.ops_queue_base.stop()
    }

    pub fn enqueue(&self, f: impl FnOnce(T) + Send + 'static, arg: T) {
        self.ops_queue_base.enqueue(TypedQueueOp {
            f: Box::new(f),
            arg,
        });
    }
}

impl<T: Send + 'static> Drop for TypedOpsQueue<T> {
    fn drop(&mut self) {
        self.ops_queue_base.abandon();
    }
}

trait OpsQueueItem: Send + 'static {
    fn run(self);
}

struct OpsQueueBase<T: OpsQueueItem> {
    params: OpsQueueParams,

    lock: Mutex<OpsQueueInner<T>>,
    wake: Condvar,
    done_chan: Done,
}

struct OpsQueueInner<T> {
    ops: VecDeque<T>,
    // The wake channel upstream: whether a wake up is waiting in it. That it is closed is
    // `is_stopped`.
    wake: bool,
    is_started: bool,
    is_stopped: bool,
    // New: the queue was dropped, nothing more will be enqueued.
    is_abandoned: bool,
}

impl<T: OpsQueueItem> OpsQueueBase<T> {
    fn new(params: OpsQueueParams) -> Arc<Self> {
        let ops = VecDeque::with_capacity(params.min_size.min(128));
        Arc::new(Self {
            params,
            lock: Mutex::new(OpsQueueInner {
                ops,
                wake: false,
                is_started: false,
                is_stopped: false,
                is_abandoned: false,
            }),
            wake: Condvar::new(),
            done_chan: Done::new(),
        })
    }

    fn start(self: &Arc<Self>) {
        let mut inner = self.lock.lock();
        if inner.is_started {
            return;
        }

        inner.is_started = true;
        drop(inner);

        let oq = self.clone();
        let runtime = time::runtime();
        std::thread::Builder::new()
            .name(format!("opsqueue {}", self.params.name.replace('\0', "")))
            .spawn(move || {
                let _runtime = runtime.enter();
                oq.process();
            })
            .expect("the worker thread of an ops queue starts");
    }

    fn stop(&self) -> Done {
        let mut inner = self.lock.lock();
        if inner.is_stopped {
            return self.done_chan.clone();
        }

        inner.is_stopped = true;
        self.wake.notify_all();
        drop(inner);
        self.done_chan.clone()
    }

    fn enqueue(&self, op: T) {
        let mut inner = self.lock.lock();

        if inner.is_stopped {
            return;
        }

        inner.ops.push_back(op);
        if inner.ops.len() == 1 {
            inner.wake = true;
            self.wake.notify_one();
        }
    }

    fn process(&self) {
        // Closes the channel when this returns, also when an op panics.
        let _done = WorkerEnded(self);

        loop {
            {
                let mut inner = self.lock.lock();
                while !inner.wake && !inner.is_stopped && !inner.is_abandoned {
                    self.wake.wait(&mut inner);
                }
                inner.wake = false;
            }
            loop {
                let mut inner = self.lock.lock();
                if inner.is_stopped && (!self.params.flush_on_stop || inner.ops.is_empty()) {
                    return;
                }

                if inner.ops.is_empty() {
                    if inner.is_abandoned {
                        return;
                    }
                    break;
                }
                let op = inner.ops.pop_front().expect("the queue is not empty");
                drop(inner);

                op.run();
            }
        }
    }

    // New: see the header.
    fn abandon(&self) {
        let mut inner = self.lock.lock();
        inner.is_abandoned = true;
        self.wake.notify_all();
    }
}

/// The channel that `Stop()` returns upstream, which is closed when the worker has ended.
#[derive(Clone)]
pub struct Done {
    inner: Arc<DoneInner>,
}

struct DoneInner {
    closed: Mutex<bool>,
    cond: Condvar,
}

impl Done {
    fn new() -> Self {
        Self {
            inner: Arc::new(DoneInner {
                closed: Mutex::new(false),
                cond: Condvar::new(),
            }),
        }
    }

    fn close(&self) {
        *self.inner.closed.lock() = true;
        self.inner.cond.notify_all();
    }

    /// Go's `<-done`: blocks until the worker has ended. As upstream, that is forever for a
    /// queue that was stopped without having been started.
    pub fn wait(&self) {
        let mut closed = self.inner.closed.lock();
        while !*closed {
            self.inner.cond.wait(&mut closed);
        }
    }

    /// Go's `select` on the channel and `time.After(d)`: true if the worker has ended, false
    /// if `d` passed first.
    pub fn wait_timeout(&self, d: Duration) -> bool {
        let deadline = std::time::Instant::now() + d.to_std();
        let mut closed = self.inner.closed.lock();
        while !*closed {
            if self
                .inner
                .cond
                .wait_until(&mut closed, deadline)
                .timed_out()
            {
                return *closed;
            }
        }
        true
    }

    /// Go's `select` on the channel with a `default`: whether the worker has ended.
    pub fn is_done(&self) -> bool {
        *self.inner.closed.lock()
    }
}

// `defer close(oq.doneChan)` upstream.
struct WorkerEnded<'a, T: OpsQueueItem>(&'a OpsQueueBase<T>);

impl<T: OpsQueueItem> Drop for WorkerEnded<'_, T> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // New: see the header.
            let mut inner = self.0.lock.lock();
            inner.is_stopped = true;
            let ops = std::mem::take(&mut inner.ops);
            drop(inner);
            drop(ops);
        }
        self.0.done_chan.close();
    }
}

#[cfg(test)]
#[path = "opsqueue_test.rs"]
mod opsqueue_test;
