// Ported from LiveKit, pkg/utils/iceconfigcache_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0. (The original
// has no copyright line of its own; this is the notice of its repository.)
// Changes: translated from Go to Rust. The original has one test, which only puts a config;
// the others here are new and say so.

use super::*;
use crate::go::time::{MILLISECOND, SECOND};
use crate::testutils::eventually_async;

struct StopOnDrop<'a, T: Eq + Hash + Send + 'static>(&'a IceConfigCache<T>);

impl<T: Eq + Hash + Send + 'static> Drop for StopOnDrop<'_, T> {
    fn drop(&mut self) {
        self.0.stop();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ice_config_cache() {
    let cache = IceConfigCache::<String>::new(10 * SECOND);
    let _cleanup = StopOnDrop(&cache);

    cache.put("test".to_string(), livekit::IceConfig::default());
    // The original ends with `require.NotNil(t, cache)`, which a value in Rust cannot fail.
}

// New for musivum: what is put is what is got, and a key that is not there gives the empty
// config.
#[tokio::test(flavor = "multi_thread")]
async fn a_config_is_kept_for_its_key() {
    let cache = IceConfigCache::<(String, String)>::new(Duration::ZERO);
    let _cleanup = StopOnDrop(&cache);
    // the time to live is never less than five minutes
    assert_eq!(ICE_CONFIG_TTL_MIN, cache.c.ttl);

    let key = ("room".to_string(), "alice".to_string());
    let config = livekit::IceConfig {
        preference_subscriber: livekit::IceCandidateType::IctTcp as i32,
        preference_publisher: livekit::IceCandidateType::IctTls as i32,
    };
    assert_eq!(livekit::IceConfig::default(), cache.get(&key));
    cache.put(key.clone(), config);
    assert_eq!(config, cache.get(&key));
    assert_eq!(
        livekit::IceConfig::default(),
        cache.get(&("room".to_string(), "bob".to_string()))
    );
}

// What the cache gives for the key, if the test got to look before `deadline`; `None` if the
// test was held up for longer, so that what it saw says nothing.
fn get_before<K: Eq + Hash + Send + 'static>(
    cache: &TtlCache<K, u32>,
    key: &K,
    deadline: Time,
) -> Option<Option<u32>> {
    let got = cache.get(key);
    time::now().before(deadline).then_some(got)
}

// New for musivum: an item is gone after its time to live, reading it does not prolong it,
// and the worker removes it.
#[tokio::test(flavor = "multi_thread")]
async fn an_item_expires() {
    let ttl = 200 * MILLISECOND;
    let cache = TtlCache::<&'static str, u32>::new(ttl);
    cache.clone().start();

    // the item is there until `ttl` after it was set, which is not before `expires_at`
    let expires_at = time::now().add(ttl);
    cache.set("a", 1);
    let set_at = time::now();
    if let Some(got) = get_before(&cache, &"a", expires_at) {
        assert_eq!(Some(1), got);
    }
    time::sleep(100 * MILLISECOND).await;
    if let Some(got) = get_before(&cache, &"a", expires_at) {
        assert_eq!(Some(1), got);
    }
    // and gone after that, although it was read in between
    time::sleep(time::until(set_at.add(ttl)) + MILLISECOND).await;
    assert_eq!(None, cache.get(&"a"));

    let c = cache.clone();
    eventually_async(
        move || c.len() == 0,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_millis(10),
    )
    .await;

    // putting it again gives it a new time
    let expires_at = time::now().add(ttl);
    cache.set("a", 2);
    if let Some(got) = get_before(&cache, &"a", expires_at) {
        assert_eq!(Some(2), got);
    }
    cache.stop();
}

// New for musivum: a cache that is dropped without having been stopped ends its worker, which
// is the last to let go of the items.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_cache_ends_its_worker() {
    let cache = IceConfigCache::<String>::new(10 * SECOND);
    cache.put("test".to_string(), livekit::IceConfig::default());
    let c = Arc::downgrade(&cache.c);
    assert!(c.upgrade().is_some());

    drop(cache);
    eventually_async(
        move || c.upgrade().is_none(),
        std::time::Duration::from_secs(5),
        std::time::Duration::from_millis(10),
    )
    .await;
}
