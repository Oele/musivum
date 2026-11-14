// Ported from frostbyte73/core, pool_test.go at v0.1.1.
// Copyright (c) 2023 David Colburn. Licensed under the MIT License.
// Changes: translated from Go to Rust; a job is a future, its sleep the runtime's.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use super::*;
use crate::go::time::MILLISECOND;

#[tokio::test(flavor = "multi_thread")]
async fn test_pool() {
    let p = new_queue_pool(
        2,
        QueueWorkerParams {
            queue_size: 10,
            ..Default::default()
        },
    );

    const KEY1: &str = "1";
    const KEY2: &str = "2";

    let val1 = Arc::new(AtomicU32::new(0));
    let val2 = Arc::new(AtomicU32::new(0));

    let val = |ind: &[u32]| -> u32 {
        let mut v = 0;
        for i in ind {
            v += 1 << i;
        }
        v
    };
    let add = |val: &Arc<AtomicU32>, i: u32| {
        val.fetch_add(1 << i, Ordering::SeqCst);
    };
    let get1 = || val1.load(Ordering::SeqCst);
    let get2 = || val2.load(Ordering::SeqCst);

    let v1 = val1.clone();
    let submitted = p.submit(KEY1, async move {
        time::sleep(500 * MILLISECOND).await;
        add(&v1, 1);
    });
    assert!(submitted);

    let v2 = val2.clone();
    let submitted = p.submit(KEY2, async move {
        add(&v2, 2);
    });
    assert!(submitted);

    let v1 = val1.clone();
    let submitted = p.submit(KEY1, async move {
        add(&v1, 3);
    });
    assert!(submitted);

    time::sleep(100 * MILLISECOND).await;
    assert_eq!(val(&[]), get1());
    assert_eq!(val(&[2]), get2());

    time::sleep(500 * MILLISECOND).await;
    assert_eq!(val(&[1, 3]), get1());

    let v1 = val1.clone();
    let submitted = p.submit(KEY1, async move {
        time::sleep(500 * MILLISECOND).await;
        add(&v1, 4);
    });
    assert!(submitted);

    let v2 = val2.clone();
    let submitted = p.submit(KEY2, async move {
        time::sleep(500 * MILLISECOND).await;
        add(&v2, 5);
    });
    assert!(submitted);

    p.drain();
    assert_eq!(val(&[1, 3, 4]), get1());
    assert_eq!(val(&[2, 5]), get2());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pool_overflow() {
    let queue_size = 2;
    let p = new_queue_pool(
        2,
        QueueWorkerParams {
            queue_size,
            drop_when_full: true,
        },
    );

    for i in 0..queue_size + 2 {
        let submitted = p.submit("key", async {
            time::sleep(500 * MILLISECOND).await;
        });
        if i < queue_size + 1 {
            assert!(submitted, "job {i}");
        } else {
            assert!(!submitted, "job {i}");
        }
    }

    p.drain();
}
