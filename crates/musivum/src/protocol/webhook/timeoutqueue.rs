// Ported from LiveKit's protocol, utils/timeoutqueue.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. It belongs to `protocol::utils` and is here, with its
// test, because `ResourceURLNotifier` is the one user of it so far. An item is an `Arc`, which
// the queue and its owner share. Its time, its links and whether it was removed, which the
// queue's lock guards upstream, are under a lock of the item's own that is only taken with the
// queue's held; the links are `Arc`s both ways, which `remove` takes apart, and a queue that
// is dropped takes apart what is left in it (new). `Reset`, `Remove` and the iterator take an
// item's `Arc`, the iterator gives one.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::go::time::{self, Duration};

pub struct TimeoutQueueItem<T> {
    pub value: T,
    links: Mutex<TimeoutQueueItemLinks<T>>,
}

struct TimeoutQueueItemLinks<T> {
    time: i64,
    next: Option<Arc<TimeoutQueueItem<T>>>,
    prev: Option<Arc<TimeoutQueueItem<T>>>,
    removed: bool,
}

impl<T> TimeoutQueueItem<T> {
    /// `&TimeoutQueueItem[T]{Value: value}`.
    pub fn new(value: T) -> Arc<TimeoutQueueItem<T>> {
        Arc::new(TimeoutQueueItem {
            value,
            links: Mutex::new(TimeoutQueueItemLinks {
                time: 0,
                next: None,
                prev: None,
                removed: false,
            }),
        })
    }
}

pub struct TimeoutQueue<T> {
    mu: Mutex<TimeoutQueueInner<T>>,
}

struct TimeoutQueueInner<T> {
    head: Option<Arc<TimeoutQueueItem<T>>>,
    tail: Option<Arc<TimeoutQueueItem<T>>>,
}

impl<T> Default for TimeoutQueue<T> {
    fn default() -> Self {
        TimeoutQueue {
            mu: Mutex::new(TimeoutQueueInner {
                head: None,
                tail: None,
            }),
        }
    }
}

fn same<T>(a: &Option<Arc<TimeoutQueueItem<T>>>, b: &Arc<TimeoutQueueItem<T>>) -> bool {
    a.as_ref().is_some_and(|a| Arc::ptr_eq(a, b))
}

impl<T> TimeoutQueue<T> {
    pub fn reset(&self, i: &Arc<TimeoutQueueItem<T>>) -> bool {
        let t = time::now().unix_nano();

        let mut q = self.mu.lock();

        let removed = i.links.lock().removed;
        if !removed {
            i.links.lock().time = t;
            q.remove(i);
            q.push(i);
        }
        !removed
    }

    pub fn remove(&self, i: &Arc<TimeoutQueueItem<T>>) {
        let mut q = self.mu.lock();

        i.links.lock().removed = true;
        q.remove(i);
    }

    fn pop_before(&self, t: i64, remove: bool) -> Option<Arc<TimeoutQueueItem<T>>> {
        let mut q = self.mu.lock();

        let i = q.head.clone()?;
        if i.links.lock().time > t {
            return None;
        }

        {
            let mut links = i.links.lock();
            links.removed = links.removed || remove;
            links.time = 0;
        }
        q.remove(&i);

        Some(i)
    }

    pub fn iterate_after(&self, timeout: Duration) -> TimeoutQueueIterator<'_, T> {
        new_timeout_queue_iterator(self, timeout, false)
    }

    pub fn iterate_remove_after(&self, timeout: Duration) -> TimeoutQueueIterator<'_, T> {
        new_timeout_queue_iterator(self, timeout, true)
    }

    // The values from head to tail, for the tests: debugTimeoutQueueItems of the original's.
    #[cfg(test)]
    fn debug_items(&self) -> Vec<T>
    where
        T: Clone,
    {
        let q = self.mu.lock();
        let mut items = Vec::new();
        let mut i = q.head.clone();
        while let Some(item) = i {
            items.push(item.value.clone());
            i = item.links.lock().next.clone();
        }
        items
    }
}

impl<T> TimeoutQueueInner<T> {
    fn push(&mut self, i: &Arc<TimeoutQueueItem<T>>) {
        let prev = self.tail.clone();
        {
            let mut links = i.links.lock();
            links.prev = prev.clone();
            links.next = None;
        }

        if let Some(prev) = &prev {
            prev.links.lock().next = Some(i.clone());
        }

        self.tail = Some(i.clone());
        if self.head.is_none() {
            self.head = Some(i.clone());
        }
    }

    fn remove(&mut self, i: &Arc<TimeoutQueueItem<T>>) {
        let (prev, next) = {
            let links = i.links.lock();
            (links.prev.clone(), links.next.clone())
        };
        if same(&self.tail, i) {
            self.tail = prev.clone();
        }
        if same(&self.head, i) {
            self.head = next.clone();
        }
        if let Some(prev) = &prev {
            prev.links.lock().next = next.clone();
        }
        if let Some(next) = &next {
            next.links.lock().prev = prev.clone();
        }
        let mut links = i.links.lock();
        links.next = None;
        links.prev = None;
    }
}

// New: the links of the items left in the queue hold each other.
impl<T> Drop for TimeoutQueue<T> {
    fn drop(&mut self) {
        let q = self.mu.get_mut();
        let mut i = q.head.take();
        q.tail = None;
        while let Some(item) = i {
            let mut links = item.links.lock();
            links.prev = None;
            i = links.next.take();
        }
    }
}

pub struct TimeoutQueueIterator<'a, T> {
    q: &'a TimeoutQueue<T>,
    time: i64,
    remove: bool,
    item: Option<Arc<TimeoutQueueItem<T>>>,
}

fn new_timeout_queue_iterator<T>(
    q: &TimeoutQueue<T>,
    timeout: Duration,
    remove: bool,
) -> TimeoutQueueIterator<'_, T> {
    TimeoutQueueIterator {
        q,
        time: time::now().add(-timeout).unix_nano(),
        remove,
        item: None,
    }
}

impl<T> TimeoutQueueIterator<'_, T> {
    // the original's Next, which is not an iterator's
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        self.item = self.q.pop_before(self.time, self.remove);
        self.item.is_some()
    }

    pub fn item(&self) -> Option<&Arc<TimeoutQueueItem<T>>> {
        self.item.as_ref()
    }
}

#[cfg(test)]
#[path = "timeoutqueue_test.rs"]
mod timeoutqueue_test;
