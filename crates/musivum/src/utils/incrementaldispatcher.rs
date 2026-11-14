// Ported from LiveKit, pkg/utils/incrementaldispatcher.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the lock is a `Mutex` where the original has a
// `RWMutex`, because the condition variable needs one (readers that copy items out take it one
// after the other); `for_each` blocks the calling thread while it waits, as the original
// blocks its goroutine, so it is for a thread, not for a task; the items are cloned out to
// each consumer.

use parking_lot::{Condvar, Mutex};
use tokio_util::sync::CancellationToken;

// IncrementalDispatcher is a dispatcher that allows multiple consumers to consume items as they become
// available, while producers can add items at anytime.
pub struct IncrementalDispatcher<T> {
    done: CancellationToken,
    lock: Mutex<IncrementalDispatcherInner<T>>,
    cond: Condvar,
}

struct IncrementalDispatcherInner<T> {
    items: Vec<T>,
}

impl<T: Clone> IncrementalDispatcher<T> {
    pub fn new() -> Self {
        Self {
            done: CancellationToken::new(),
            lock: Mutex::new(IncrementalDispatcherInner { items: Vec::new() }),
            cond: Condvar::new(),
        }
    }

    pub fn add(&self, item: T) {
        if self.done.is_cancelled() {
            return;
        }
        let mut d = self.lock.lock();
        d.items.push(item);
        self.cond.notify_all();
        drop(d);
    }

    pub fn done(&self) {
        let d = self.lock.lock();
        self.done.cancel();
        self.cond.notify_all();
        drop(d);
    }

    pub fn for_each(&self, mut f: impl FnMut(T)) {
        let mut idx = 0;
        let mut dispatch_from_idx = |idx: &mut usize| {
            let mut items_to_dispatch = Vec::new();
            let d = self.lock.lock();
            while *idx < d.items.len() {
                items_to_dispatch.push(d.items[*idx].clone());
                *idx += 1;
            }
            drop(d);
            for item in items_to_dispatch {
                f(item);
            }
        };
        while !self.done.is_cancelled() {
            dispatch_from_idx(&mut idx);
            let mut d = self.lock.lock();
            // need to check again because Done may have been called while dispatching
            if self.done.is_cancelled() {
                drop(d);
                break;
            }
            if idx == d.items.len() {
                self.cond.wait(&mut d);
            }
            drop(d);
        }

        dispatch_from_idx(&mut idx);
    }
}

impl<T: Clone> Default for IncrementalDispatcher<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "incrementaldispatcher_test.rs"]
mod incrementaldispatcher_test;
