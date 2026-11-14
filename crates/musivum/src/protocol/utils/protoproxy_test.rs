// Ported from LiveKit's protocol, utils/protoproxy_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the proxies are of `Option<livekit::Room>`, the
// counterpart of the pointer the original's are of. The original checks for a leak by counting
// goroutines before and after; here it is checked that the worker has ended.
//
// The original's test depends on how fast it runs: what it expects after making a proxy holds
// only as long as that proxy's next refresh is not due, which with the original's interval of
// 10 ms is as long as the test is not held up for 5 ms, and a busy machine holds it up for
// longer. So here the test proxy refreshes every 100 ms, the wait for an update is 2.5 s at
// most instead of 250 ms, the last wait for no update is 300 ms (three intervals) instead of
// 100 ms, and the last subtest waits for the await to resolve for up to 2 s instead of
// looking after 100 ms. And a subtest that finds that it was held up for an interval all the
// same starts again with a new proxy: what it saw before the first refresh was due is
// checked once it knows that it was in time.
//
// The tests after `test_proto_proxy` are new and say so.

use super::*;
use crate::go::atomic;
use crate::go::time::{MILLISECOND, SECOND};
use crate::protocol::livekit;

// The refresh interval of the test proxies.
const REFRESH_INTERVAL: Duration = Duration::from_millis(100);

// How often a subtest starts again when it was held up, before the test gives up.
const ATTEMPTS: usize = 100;

// Whether no refresh was due yet for a proxy that was made at `created`.
fn in_time(created: Time) -> bool {
    time::since(created) < REFRESH_INTERVAL
}

#[tokio::test(flavor = "multi_thread")]
async fn test_proto_proxy() {
    // basics
    for attempt in 1.. {
        assert!(attempt <= ATTEMPTS, "the test was held up every time");
        let created = time::now();
        let (proxy, num_participants, freeze) = create_test_proxy();

        tokio::select! {
            biased;
            _ = proxy.updated().notified() => panic!("should not have received an update"),
            _ = std::future::ready(()) => {}
        }

        // should not have changed, initial value should persist
        assert_eq!(0, proxy.get().unwrap().num_participants);

        // immediate change
        proxy.mark_dirty(true).cancelled().await;
        await_proxy_update(&proxy).await;

        let num_participants_after_change = num_participants.load();
        let value_after_change = proxy.get().unwrap().num_participants;

        // queue an update while the cached value is still fresh so the worker
        // picks it up on the next tick instead of triggering an immediate refresh.
        proxy.mark_dirty(false);
        let update_while_fresh = proxy_update_within(&proxy, 5 * MILLISECOND).await;
        let value_while_fresh = proxy.get().unwrap().num_participants;

        // freeze and ensure that updates are not triggered
        freeze.store(true);

        if !in_time(created) {
            proxy.stop();
            continue;
        }
        assert_eq!(2, num_participants_after_change);
        assert_eq!(1, value_after_change);
        assert!(!update_while_fresh, "should not have received an update");
        assert_eq!(1, value_while_fresh);

        await_proxy_update(&proxy).await;
        assert_eq!(2, proxy.get().unwrap().num_participants);

        // trigger another update, but should not get notification as freeze is in place and the model should not have changed
        proxy.mark_dirty(false);
        assert_no_proxy_update(&proxy, 300 * MILLISECOND).await;
        assert_eq!(2, proxy.get().unwrap().num_participants);

        // ensure we didn't leak
        proxy.stop();

        for _ in 0..10 {
            if proxy.done.is_cancelled() {
                break;
            }
            time::sleep(100 * MILLISECOND).await;
        }
        assert!(proxy.done.is_cancelled());
        break;
    }

    // await next update after marking dirty
    for attempt in 1.. {
        assert!(attempt <= ATTEMPTS, "the test was held up every time");
        let created = time::now();
        let (proxy, _, _) = create_test_proxy();
        assert_eq!(0, proxy.get().unwrap().num_participants);
        proxy.mark_dirty(true).cancelled().await;
        let value = proxy.get().unwrap().num_participants;
        if !in_time(created) {
            continue;
        }
        assert_eq!(1, value);
        break;
    }

    // await resolves when proxy is stopped
    {
        let (proxy, _, _) = create_test_proxy();
        let done = proxy.mark_dirty(true);
        proxy.stop();
        done.cancelled().await;
    }

    // multiple awaits resolve for one update
    for attempt in 1.. {
        assert!(attempt <= ATTEMPTS, "the test was held up every time");
        let created = time::now();
        let (proxy, _, _) = create_test_proxy();
        let done0 = proxy.mark_dirty(false);
        let done1 = proxy.mark_dirty(true);
        done0.cancelled().await;
        done1.cancelled().await;
        let value = proxy.get().unwrap().num_participants;
        if !in_time(created) {
            continue;
        }
        assert_eq!(1, value);
        break;
    }

    // await resolve when there is no change
    {
        let proxy = ProtoProxy::new(10 * MILLISECOND, || -> Option<livekit::Room> { None });
        let done = proxy.mark_dirty(true);
        tokio::select! {
            _ = done.cancelled() => {}
            _ = time::sleep(2 * SECOND) => panic!("the await did not resolve"),
        }
    }
}

async fn await_proxy_update(proxy: &ProtoProxy<Option<livekit::Room>>) {
    tokio::select! {
        _ = proxy.updated().notified() => {}
        _ = time::sleep(2500 * MILLISECOND) => panic!("timed out waiting for proxy update"),
    }
}

async fn assert_no_proxy_update(proxy: &ProtoProxy<Option<livekit::Room>>, d: Duration) {
    assert!(
        !proxy_update_within(proxy, d).await,
        "should not have received an update"
    );
}

// Whether the proxy says within `d` that it was updated.
async fn proxy_update_within(proxy: &ProtoProxy<Option<livekit::Room>>, d: Duration) -> bool {
    tokio::select! {
        _ = proxy.updated().notified() => true,
        _ = time::sleep(d) => false,
    }
}

fn create_test_proxy() -> (
    Arc<ProtoProxy<Option<livekit::Room>>>,
    Arc<atomic::Uint32>,
    Arc<atomic::Bool>,
) {
    // uses an update func that increments numParticipants each time
    let num_participants = Arc::new(atomic::Uint32::new(0));
    let freeze = Arc::new(atomic::Bool::new(false));
    let (n, f) = (num_participants.clone(), freeze.clone());
    (
        ProtoProxy::new(REFRESH_INTERVAL, move || {
            let room = livekit::Room {
                num_participants: n.load(),
                ..Default::default()
            };
            if !f.load() {
                n.add(1);
            }
            Some(room)
        }),
        num_participants,
        freeze,
    )
}

// New for musivum: holds what the original does without a refresh interval. Its comment says
// that the proxy then updates on `MarkDirty(true)` only; it does not update at all, because
// there is no worker to do it, and who waits is let go when the proxy is stopped.
#[tokio::test(flavor = "multi_thread")]
async fn without_a_refresh_interval_nothing_updates_as_upstream() {
    let (proxy, calls) = create_counting_proxy(Duration::ZERO);
    assert_eq!(1, proxy.get().unwrap().num_participants);

    let done = proxy.mark_dirty(true);
    time::sleep(50 * MILLISECOND).await;
    assert!(!done.is_cancelled());
    assert_eq!(1, proxy.get().unwrap().num_participants);
    assert_eq!(1, calls.load());

    proxy.stop();
    assert!(done.is_cancelled());
}

// A proxy of a room whose number of participants is the number of times the update function
// was called.
fn create_counting_proxy(
    refresh_interval: Duration,
) -> (Arc<ProtoProxy<Option<livekit::Room>>>, Arc<atomic::Uint32>) {
    let calls = Arc::new(atomic::Uint32::new(0));
    let c = calls.clone();
    (
        ProtoProxy::new(refresh_interval, move || {
            Some(livekit::Room {
                num_participants: c.add(1),
                ..Default::default()
            })
        }),
        calls,
    )
}

// New for musivum: a tick refreshes nothing that was not marked, and a refresh takes the mark
// away.
#[tokio::test(flavor = "multi_thread")]
async fn only_what_is_marked_is_refreshed() {
    let (proxy, calls) = create_counting_proxy(20 * MILLISECOND);
    time::sleep(150 * MILLISECOND).await;
    assert_eq!(1, calls.load());
    assert_eq!(1, proxy.get().unwrap().num_participants);
    proxy.stop();

    // With the next tick an hour away it is the mark alone that refreshes. (A tick that
    // comes between an immediate mark and its refresh refreshes as well, upstream too.)
    let (proxy, calls) = create_counting_proxy(time::HOUR);
    assert!(!proxy.lock.read().dirty);
    proxy.mark_dirty(true).cancelled().await;
    assert_eq!(2, calls.load());
    assert!(!proxy.lock.read().dirty);
    assert_eq!(2, proxy.get().unwrap().num_participants);
    proxy.stop();
}

// New for musivum: a mark that is not immediate waits for a tick while the value is fresh, and
// does not wait when the value is older than an interval.
#[tokio::test(flavor = "multi_thread")]
async fn a_mark_of_a_stale_value_does_not_wait_for_a_tick() {
    let (proxy, calls) = create_counting_proxy(time::HOUR);
    let done = proxy.mark_dirty(false);
    time::sleep(50 * MILLISECOND).await;
    assert!(!done.is_cancelled());
    assert_eq!(1, calls.load());

    proxy.lock.write().refreshed_at = time::now().add(-2 * time::HOUR);
    let done = proxy.mark_dirty(false);
    tokio::select! {
        _ = done.cancelled() => {}
        _ = time::sleep(5 * SECOND) => panic!("the await did not resolve"),
    }
    assert_eq!(2, calls.load());
    proxy.stop();
}

// New for musivum: a tick leaves a value alone that was refreshed less than an interval ago,
// and a later tick refreshes it.
#[tokio::test(flavor = "multi_thread")]
async fn a_tick_leaves_a_fresh_value_alone() {
    let interval = 300 * MILLISECOND;
    for attempt in 1.. {
        assert!(attempt <= ATTEMPTS, "the test was held up every time");
        let created = time::now();
        let (proxy, _) = create_counting_proxy(interval);

        // a refresh in the middle of the first interval, and a mark for the next one
        time::sleep(interval / 2).await;
        proxy.mark_dirty(true).cancelled().await;
        await_proxy_update(&proxy).await;
        let value = proxy.get().unwrap().num_participants;
        proxy.mark_dirty(false);
        // (all of that before the first tick, or the test starts again)
        if time::since(created) >= interval {
            proxy.stop();
            continue;
        }
        assert_eq!(2, value);

        // The first tick comes an interval after the worker started, which is about half an
        // interval after the refresh: too early for another one. (If the test is held up
        // here until another one is due, what it sees says nothing.)
        let due = created.add(interval / 2).add(interval);
        time::sleep(time::until(created.add(interval)) + 30 * MILLISECOND).await;
        let value = proxy.get().unwrap().num_participants;
        if time::now().before(due) {
            assert_eq!(2, value);
        }

        // the tick after that refreshes it
        await_proxy_update(&proxy).await;
        assert_eq!(3, proxy.get().unwrap().num_participants);
        proxy.stop();
        break;
    }
}

// New for musivum: a proxy that is dropped without having been stopped ends its worker, at
// once and not at its next tick, and lets go of who waits for an update.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_proxy_ends_its_worker() {
    let (proxy, calls) = create_counting_proxy(time::HOUR);
    let worker_done = proxy.done.clone();
    proxy.mark_dirty(true).cancelled().await;
    assert_eq!(2, calls.load());

    // an update that is due at the next tick, and somebody waiting for it
    let done = proxy.mark_dirty(false);
    assert!(!done.is_cancelled());
    assert!(!worker_done.is_cancelled());
    drop(proxy);

    tokio::select! {
        _ = worker_done.cancelled() => {}
        _ = time::sleep(5 * SECOND) => panic!("the worker did not end"),
    }
    assert!(done.is_cancelled());
    // the update function was not called again
    assert_eq!(2, calls.load());
}
