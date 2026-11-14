// Ported from LiveKit, pkg/service/turnquota_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. BenchmarkTURNAllocationQuota_Allow is left out.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicI32, Ordering};

use super::*;
use crate::service::LIVEKIT_REALM;
use crate::testutils::eventually_async;

fn udp_addr(port: u16) -> Option<Addr> {
    Some(Addr::Udp(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        port,
    )))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_turn_allocation_quota_limits_per_user() {
    let q = new_turn_allocation_quota(4);

    // four distinct source 5-tuples for the same participant are allowed
    for i in 0..4 {
        assert!(
            q.allow("participantA", LIVEKIT_REALM, udp_addr(5000 + i)),
            "allocation {i}"
        );
    }
    // the fifth is rejected (Pion returns 486 Allocation Quota Reached)
    assert!(!q.allow("participantA", LIVEKIT_REALM, udp_addr(6000)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_turn_allocation_quota_isolated_per_user() {
    let q = new_turn_allocation_quota(2);

    assert!(q.allow("A", LIVEKIT_REALM, udp_addr(5000)));
    assert!(q.allow("A", LIVEKIT_REALM, udp_addr(5001)));
    assert!(!q.allow("A", LIVEKIT_REALM, udp_addr(5002)));

    // a different participant has its own budget
    assert!(q.allow("B", LIVEKIT_REALM, udp_addr(5000)));
    assert!(q.allow("B", LIVEKIT_REALM, udp_addr(5001)));
    assert!(!q.allow("B", LIVEKIT_REALM, udp_addr(5002)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_turn_allocation_quota_release_frees_slot() {
    let q = new_turn_allocation_quota(1);

    let addr = udp_addr(5000);
    assert!(q.allow("A", LIVEKIT_REALM, addr));
    assert!(!q.allow("A", LIVEKIT_REALM, udp_addr(5001)));

    // once the allocation is deleted the slot is reclaimed
    q.on_deleted(addr, None, "udp", "A", LIVEKIT_REALM);
    assert!(q.allow("A", LIVEKIT_REALM, udp_addr(5001)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_turn_allocation_quota_retransmit_is_idempotent() {
    let q = new_turn_allocation_quota(1);

    let addr = udp_addr(5000);
    // same 5-tuple retried multiple times must consume only one slot
    assert!(q.allow("A", LIVEKIT_REALM, addr));
    assert!(q.allow("A", LIVEKIT_REALM, addr));
    assert!(q.allow("A", LIVEKIT_REALM, addr));

    // a different 5-tuple is still over quota
    assert!(!q.allow("A", LIVEKIT_REALM, udp_addr(5001)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_turn_allocation_quota_forgets_user_when_empty() {
    let q = new_turn_allocation_quota(1);

    let addr = udp_addr(5000);
    assert!(q.allow("A", LIVEKIT_REALM, addr));
    q.on_deleted(addr, None, "udp", "A", LIVEKIT_REALM);

    let tracked = q.mu.lock().contains_key("A");
    assert!(
        !tracked,
        "user with no active allocations should not be retained"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_turn_allocation_quota_unconfirmed_reservation_reclaimed() {
    // an Allocate that passes the quota but never produces an allocation (e.g.
    // relay ports exhausted) must not hold its slot forever
    let q = new_turn_allocation_quota(1);
    *q.reservation_ttl.lock() = Duration::from_millis(10);

    assert!(q.allow("A", LIVEKIT_REALM, udp_addr(5000)));
    assert!(
        !q.allow("A", LIVEKIT_REALM, udp_addr(5001)),
        "at quota while reservation pending"
    );

    // slot should be reclaimed after TTL
    eventually_async(
        || q.allow("A", LIVEKIT_REALM, udp_addr(5002)),
        std::time::Duration::from_secs(1),
        std::time::Duration::from_millis(5),
    )
    .await;

    // the reclaimed user should not linger once fully drained
    q.on_deleted(udp_addr(5002), None, "udp", "A", LIVEKIT_REALM);
    let tracked = q.mu.lock().contains_key("A");
    assert!(!tracked);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_turn_allocation_quota_confirmed_reservation_survives_ttl() {
    // a confirmed (live) allocation must never be reclaimed by the pending timer
    let q = new_turn_allocation_quota(1);
    *q.reservation_ttl.lock() = Duration::from_millis(10);

    let addr = udp_addr(5000);
    assert!(q.allow("A", LIVEKIT_REALM, addr));
    q.on_created(addr, None, "udp", "A", LIVEKIT_REALM, None, 0);

    tokio::time::sleep(std::time::Duration::from_millis(30)).await; // past the TTL

    assert!(
        !q.allow("A", LIVEKIT_REALM, udp_addr(5001)),
        "confirmed allocation must still hold its slot"
    );

    q.on_deleted(addr, None, "udp", "A", LIVEKIT_REALM);
    assert!(q.allow("A", LIVEKIT_REALM, udp_addr(5001)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_turn_allocation_quota_stale_reclaim_does_not_evict_replacement() {
    // a reclaim timer that fires after its slot was released must not delete a
    // fresh reservation created for the same participant + client address
    let q = new_turn_allocation_quota(1);
    *q.reservation_ttl.lock() = Duration::from_secs(3600); // keep real timers from firing during the test

    let addr = udp_addr(5000);
    let key = src_addr_key(addr);

    assert!(q.allow("A", LIVEKIT_REALM, addr));
    let stale = q.mu.lock()["A"][&key].clone();

    // the original allocation is torn down, then the same 5-tuple is reused and
    // its replacement allocation is confirmed
    q.on_deleted(addr, None, "udp", "A", LIVEKIT_REALM);
    assert!(q.allow("A", LIVEKIT_REALM, addr));
    q.on_created(addr, None, "udp", "A", LIVEKIT_REALM, None, 0);

    // the original slot's timer fires late: it must be a no-op
    q.reclaim_pending("A", &key, &stale);

    let live =
        q.mu.lock()
            .get("A")
            .is_some_and(|slots| slots.contains_key(&key));
    assert!(live, "replacement slot must survive a stale reclaim");
    assert!(
        !q.allow("A", LIVEKIT_REALM, udp_addr(5001)),
        "replacement must still count against quota"
    );
}

// TestTURNAllocationQuota_ConcurrentAllocatesRespectLimit is the core security
// property: a burst of concurrent Allocate requests reusing one credential must
// not be able to race past the limit.
#[tokio::test(flavor = "multi_thread")]
async fn test_turn_allocation_quota_concurrent_allocates_respect_limit() {
    const LIMIT: usize = 4;
    let q = new_turn_allocation_quota(LIMIT);

    let granted = Arc::new(AtomicI32::new(0));
    let mut wg = Vec::new();
    for i in 0..200u16 {
        let (q, granted) = (q.clone(), granted.clone());
        wg.push(std::thread::spawn(move || {
            if q.allow("attacker", LIVEKIT_REALM, udp_addr(7000 + i)) {
                granted.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for w in wg {
        w.join().unwrap();
    }

    assert_eq!(
        LIMIT as i32,
        granted.load(Ordering::SeqCst),
        "concurrent allocations must not exceed the quota"
    );
}

#[test]
fn test_turn_allocation_quota_src_addr_key_distinguishes_network() {
    let udp = Some(Addr::Udp(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        5000,
    )));
    let tcp = Some(Addr::Tcp(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        5000,
    )));
    assert_ne!(src_addr_key(udp), src_addr_key(tcp));
    assert_eq!("", src_addr_key(None));
}

// Not here: BenchmarkTURNAllocationQuota_Allow (a benchmark).
