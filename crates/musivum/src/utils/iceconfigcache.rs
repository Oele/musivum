// Ported from LiveKit, pkg/utils/iceconfigcache.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0. (The original
// has no copyright line of its own; this is the notice of its repository.)
// Changes: translated from Go to Rust; the cache library of the original
// (jellydator/ttlcache) is replaced by `TtlCache` below, a map with expiry times written new
// for this file, with what the original asks of the library: one time to live for every item,
// set when the item is put and not prolonged by reading it; a worker that removes what has
// expired (here once a minute; the library does it at the moment of expiry, which shows only
// in memory); `stop` ends the worker without waiting for it. An ICE config is kept by value
// (the message is two numbers), so there is no nil config to put or to get. New: a cache that
// is dropped without having been stopped ends its worker, where Go leaves the goroutine
// running.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::go::time::{self, Duration, Time};
use crate::protocol::livekit;

const ICE_CONFIG_TTL_MIN: Duration = Duration::from_secs(5 * 60);

pub struct IceConfigCache<T> {
    c: Arc<TtlCache<T, livekit::IceConfig>>,
}

impl<T: Eq + Hash + Send + 'static> IceConfigCache<T> {
    pub fn new(ttl: Duration) -> Self {
        let cache = TtlCache::new(ttl.max(ICE_CONFIG_TTL_MIN));
        cache.clone().start();

        Self { c: cache }
    }

    pub fn stop(&self) {
        self.c.stop();
    }

    pub fn put(&self, key: T, ice_config: livekit::IceConfig) {
        self.c.set(key, ice_config);
    }

    pub fn get(&self, key: &T) -> livekit::IceConfig {
        if let Some(it) = self.c.get(key) {
            return it;
        }
        livekit::IceConfig::default()
    }
}

// New, see the header.
impl<T> Drop for IceConfigCache<T> {
    fn drop(&mut self) {
        self.c.stopped.cancel();
    }
}

// How often the worker looks for expired items.
const TTL_CACHE_CLEANUP_INTERVAL: Duration = time::MINUTE;

/// What the original takes from jellydator/ttlcache. Written new.
struct TtlCache<K, V> {
    ttl: Duration,
    items: Mutex<HashMap<K, TtlCacheItem<V>>>,
    stopped: CancellationToken,
}

struct TtlCacheItem<V> {
    value: V,
    expires_at: Time,
}

impl<K: Eq + Hash + Send + 'static, V: Clone + Send + 'static> TtlCache<K, V> {
    fn new(ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            ttl,
            items: Mutex::new(HashMap::new()),
            stopped: CancellationToken::new(),
        })
    }

    fn start(self: Arc<Self>) {
        time::spawn(async move {
            loop {
                tokio::select! {
                    _ = self.stopped.cancelled() => return,
                    _ = time::sleep(TTL_CACHE_CLEANUP_INTERVAL.min(self.ttl)) => self.delete_expired(),
                }
            }
        });
    }

    fn stop(&self) {
        self.stopped.cancel();
    }

    fn set(&self, key: K, value: V) {
        let expires_at = time::now().add(self.ttl);
        self.items
            .lock()
            .insert(key, TtlCacheItem { value, expires_at });
    }

    fn get(&self, key: &K) -> Option<V> {
        let items = self.items.lock();
        let item = items.get(key)?;
        if !item.expires_at.after(time::now()) {
            return None;
        }
        Some(item.value.clone())
    }

    fn delete_expired(&self) {
        let now = time::now();
        self.items
            .lock()
            .retain(|_, item| item.expires_at.after(now));
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.items.lock().len()
    }
}

#[cfg(test)]
#[path = "iceconfigcache_test.rs"]
mod iceconfigcache_test;
