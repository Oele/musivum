// Ported from LiveKit's protocol, utils/time_size_cache_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0. (The original
// has no copyright line of its own; this is the notice of its repository.)
// Changes: translated from Go to Rust. The test after `test_time_size_cache` is new and says
// so.

use super::*;
use crate::go::time::SECOND;

fn items_size<T>(cache: &TimeSizeCache<T>) -> usize {
    cache.lock.lock().items_size
}

#[test]
fn test_time_size_cache() {
    let cache = TimeSizeCache::<String>::new(TimeSizeCacheParams {
        ttl: 5 * SECOND,
        max_size: 10,
    });

    let mut now = mono::now();
    let one = Arc::new("one".to_string());
    cache.add_at(one.clone(), one.len(), now.unix_nano());
    let items = cache.get();
    assert_eq!(1, items.len());
    assert_eq!(*one, *items[0]);
    assert_eq!(3, items_size(&cache));

    now = now.add(SECOND);
    let two = Arc::new("two".to_string());
    cache.add_at(two.clone(), two.len(), now.unix_nano());
    let items = cache.get();
    assert_eq!(2, items.len());
    assert_eq!(*one, *items[0]);
    assert_eq!(*two, *items[1]);
    assert_eq!(6, items_size(&cache));

    // this should evict the "one" based on size
    now = now.add(SECOND);
    let three = Arc::new("three".to_string());
    cache.add_at(three.clone(), three.len(), now.unix_nano());
    let items = cache.get();
    assert_eq!(2, items.len());
    assert_eq!(*two, *items[0]);
    assert_eq!(*three, *items[1]);
    assert_eq!(8, items_size(&cache));

    // this should evict the "two" based on size
    now = now.add(SECOND);
    let four = Arc::new("four".to_string());
    cache.add_at(four.clone(), four.len(), now.unix_nano());
    let items = cache.get();
    assert_eq!(2, items.len());
    assert_eq!(*three, *items[0]);
    assert_eq!(*four, *items[1]);
    assert_eq!(9, items_size(&cache));

    // this should not evict any element as existing elements should not have aged enough
    now = now.add(SECOND);
    cache.prune_at(now.unix_nano());
    let items = cache.get();
    assert_eq!(2, items.len());
    assert_eq!(9, items_size(&cache));

    // should evict "three" based on age
    now = now.add(4 * SECOND);
    cache.prune_at(now.unix_nano());
    let items = cache.get();
    assert_eq!(1, items.len());
    assert_eq!(4, items_size(&cache));

    // should evict all
    now = now.add(SECOND);
    cache.prune_at(now.unix_nano());
    let items = cache.get();
    assert_eq!(0, items.len());
    assert_eq!(0, items_size(&cache));

    // add something that fits
    cache.add(four.clone(), four.len());
    let items = cache.get();
    assert_eq!(1, items.len());
    assert_eq!(*four, *items[0]);
    assert_eq!(4, items_size(&cache));

    // add something which will exceed size threshold by itself,
    // should clean up all items in cache as items are FIFO and the "four" added above
    // gets removed first and the long item added also should get removed as it does not fit.
    let really_long = Arc::new("really-long".to_string());
    cache.add(really_long.clone(), really_long.len());
    let items = cache.get();
    assert_eq!(0, items.len());
    assert_eq!(0, items_size(&cache));
}

// New for musivum: what the original's test leaves open (values run at the pin): items that
// fill the cache to exactly its maximum stay, the time to live of a cache that names none is
// three seconds, and without a maximum nothing is evicted for its size.
#[test]
fn the_maximum_is_kept_to_the_byte_and_the_default_time_to_live_is_three_seconds() {
    let cache = TimeSizeCache::<String>::new(TimeSizeCacheParams {
        max_size: 10,
        ..Default::default()
    });
    let (a, b, c) = (
        Arc::new("a".to_string()),
        Arc::new("bbbbb".to_string()),
        Arc::new("ccccc".to_string()),
    );
    cache.add_at(a.clone(), 1, 1000);
    cache.add_at(b.clone(), 5, 1000);
    cache.add_at(c.clone(), 5, 1000);
    let items = cache.get();
    assert_eq!(2, items.len());
    assert_eq!(10, items_size(&cache));
    assert_eq!(*b, *items[0]);

    let cache = TimeSizeCache::<String>::new(TimeSizeCacheParams::default());
    let at = (10 * SECOND).nanoseconds();
    cache.add_at(a.clone(), 1, at);
    cache.add_at(b.clone(), 5, at + 1);
    cache.prune_at(at + (3 * SECOND).nanoseconds());
    assert_eq!(2, cache.get().len());
    assert_eq!(6, items_size(&cache));
    cache.prune_at(at + (3 * SECOND).nanoseconds() + 1);
    assert_eq!(1, cache.get().len());
    assert_eq!(5, items_size(&cache));
    cache.prune_at(at + (3 * SECOND).nanoseconds() + 2);
    assert_eq!(0, cache.get().len());
    assert_eq!(0, items_size(&cache));

    for _ in 0..100 {
        cache.add_at(c.clone(), 1000, at);
    }
    assert_eq!(100, cache.get().len());
    assert_eq!(100_000, items_size(&cache));
}
