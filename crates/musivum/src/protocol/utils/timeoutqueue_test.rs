// Ported from LiveKit's protocol, utils/timeoutqueue_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `debugTimeoutQueueItems` is `debug_items` of the queue.
// The subtests, which run in parallel upstream, run one after the other.

use super::*;

#[test]
fn test_timeout_queue() {
    // queue order matches insert order
    {
        let q = TimeoutQueue::<i32>::default();
        for i in 0..5 {
            q.reset(&TimeoutQueueItem::new(i));
        }
        assert_eq!(vec![0, 1, 2, 3, 4], q.debug_items(), "queue order");
    }

    // reset items do not affect order of other entries
    {
        let q = TimeoutQueue::<i32>::default();
        let mut its = Vec::new();
        for i in 0..5 {
            let it = TimeoutQueueItem::new(i);
            its.push(it.clone());
            q.reset(&it);
        }

        q.reset(&its[1]);
        assert_eq!(vec![0, 2, 3, 4, 1], q.debug_items(), "reset 1");
        q.reset(&its[3]);
        assert_eq!(vec![0, 2, 4, 1, 3], q.debug_items(), "reset 3");
        q.reset(&its[2]);
        assert_eq!(vec![0, 4, 1, 3, 2], q.debug_items(), "reset 2");
    }

    // popBefore removes items
    {
        let q = TimeoutQueue::<i32>::default();
        for i in 0..5 {
            let it = TimeoutQueueItem::new(i);
            q.reset(&it);
        }

        let ts = time::now().unix_nano();
        q.pop_before(ts, true);
        assert_eq!(vec![1, 2, 3, 4], q.debug_items(), "first pop");
        q.pop_before(ts, true);
        assert_eq!(vec![2, 3, 4], q.debug_items(), "second pop");
    }
}
