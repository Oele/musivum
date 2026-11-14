// Ported from LiveKit's protocol, utils/time_size_cache.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0. (The original
// has no copyright line of its own; this is the notice of its repository.)
// Changes: translated from Go to Rust; the items, pointers upstream, are `Arc`s.

use std::sync::Arc;

use parking_lot::Mutex;

use super::mono;
use crate::go::time::Duration;

const DEFAULT_TTL: Duration = Duration::from_secs(3);

struct TimeSizeCacheItem<T> {
    added_at: i64,
    item: Arc<T>,
    size: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct TimeSizeCacheParams {
    pub ttl: Duration,
    pub max_size: usize,
}

pub struct TimeSizeCache<T> {
    params: TimeSizeCacheParams,

    lock: Mutex<TimeSizeCacheInner<T>>,
}

struct TimeSizeCacheInner<T> {
    items: Vec<TimeSizeCacheItem<T>>,
    items_size: usize,
}

impl<T> TimeSizeCache<T> {
    pub fn new(params: TimeSizeCacheParams) -> Self {
        Self {
            params,
            lock: Mutex::new(TimeSizeCacheInner {
                items: Vec::new(),
                items_size: 0,
            }),
        }
    }

    pub fn add(&self, item: Arc<T>, size: usize) {
        self.add_at(item, size, mono::unix_nano());
    }

    pub fn add_at(&self, item: Arc<T>, size: usize, at_nano: i64) {
        let mut t = self.lock.lock();

        t.items.push(TimeSizeCacheItem {
            added_at: at_nano,
            item,
            size,
        });
        t.items_size += size;

        // prune if size exceeds threshold
        if self.params.max_size != 0 && t.items_size > self.params.max_size {
            let mut idx = 0;
            while t.items_size > self.params.max_size {
                t.items_size -= t.items[idx].size;
                idx += 1;
            }
            t.items.drain(0..idx);
        }
    }

    pub fn get(&self) -> Vec<Arc<T>> {
        let t = self.lock.lock();

        let mut items = Vec::with_capacity(t.items.len());
        for tsci in &t.items {
            items.push(tsci.item.clone());
        }

        items
    }

    pub fn prune(&self) {
        self.prune_at(mono::unix_nano());
    }

    pub fn prune_at(&self, at_nano: i64) {
        let mut t = self.lock.lock();

        // age based pruning
        let threshold = at_nano.wrapping_sub(self.get_ttl().nanoseconds());
        for idx in 0..t.items.len() {
            if t.items[idx].added_at >= threshold {
                t.items.drain(0..idx);
                return;
            }
            t.items_size -= t.items[idx].size;
        }

        // all items have aged
        t.items = Vec::new();
        t.items_size = 0;
    }

    fn get_ttl(&self) -> Duration {
        let mut ttl = self.params.ttl;
        if ttl.is_zero() {
            ttl = DEFAULT_TTL;
        }
        ttl
    }
}

#[cfg(test)]
#[path = "time_size_cache_test.rs"]
mod time_size_cache_test;
