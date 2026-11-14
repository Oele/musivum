//! Tests of the ops queue. Upstream has none (pkg/utils has no opsqueue_test.go): these are
//! written new, for what `pkg/sfu` and `pkg/rtc` rely on.

use std::sync::mpsc;

use super::*;
use crate::go::time::{MILLISECOND, SECOND};

fn params(flush_on_stop: bool) -> OpsQueueParams {
    OpsQueueParams {
        name: "test".to_string(),
        min_size: 4,
        flush_on_stop,
        ..Default::default()
    }
}

#[test]
fn ops_run_in_order_on_the_worker() {
    let oq = OpsQueue::new(params(false));
    oq.start();

    let (tx, rx) = mpsc::channel();
    let caller = std::thread::current().id();
    for i in 0..100 {
        let tx = tx.clone();
        oq.enqueue(move || {
            tx.send((i, std::thread::current().id())).unwrap();
        });
    }
    for i in 0..100 {
        let (got, thread) = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        assert_eq!(i, got);
        assert_ne!(caller, thread);
    }

    let done = oq.stop();
    assert!(done.wait_timeout(5 * SECOND));
    assert!(done.is_done());
    // every call gives the same channel
    oq.stop().wait();
}

#[test]
fn a_second_start_does_not_start_a_second_worker() {
    let oq = OpsQueue::new(params(false));
    oq.start();
    oq.start();

    // While the worker is in an op, nobody runs the next one.
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let (tx, rx) = mpsc::channel();
    oq.enqueue(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    oq.enqueue(move || tx.send(()).unwrap());
    assert!(
        rx.recv_timeout(std::time::Duration::from_millis(50))
            .is_err()
    );

    release_tx.send(()).unwrap();
    rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    oq.stop().wait();
}

#[test]
fn ops_enqueued_before_the_start_run_after_it() {
    let oq = OpsQueue::new(params(false));
    let (tx, rx) = mpsc::channel();
    for i in 0..3 {
        let tx = tx.clone();
        oq.enqueue(move || tx.send(i).unwrap());
    }
    assert!(
        rx.recv_timeout(std::time::Duration::from_millis(50))
            .is_err()
    );

    oq.start();
    for i in 0..3 {
        assert_eq!(
            i,
            rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap()
        );
    }
    oq.stop().wait();
}

// Holds the worker in an op until the test lets go, with ops queued behind it.
fn stop_with_ops_queued(flush_on_stop: bool) -> Vec<i32> {
    let oq = OpsQueue::new(params(flush_on_stop));
    oq.start();

    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let (tx, rx) = mpsc::channel();
    oq.enqueue(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    for i in 0..5 {
        let tx = tx.clone();
        oq.enqueue(move || tx.send(i).unwrap());
    }

    let done = oq.stop();
    assert!(!done.is_done(), "the worker is still in the first op");
    assert!(!done.wait_timeout(20 * MILLISECOND));
    // not taken any more
    let tx_late = tx.clone();
    oq.enqueue(move || tx_late.send(100).unwrap());

    release_tx.send(()).unwrap();
    assert!(done.wait_timeout(5 * SECOND));
    drop(tx);
    drop(oq);
    rx.iter().collect()
}

#[test]
fn stop_drops_what_is_queued() {
    assert_eq!(Vec::<i32>::new(), stop_with_ops_queued(false));
}

#[test]
fn stop_with_flush_runs_what_is_queued() {
    assert_eq!(vec![0, 1, 2, 3, 4], stop_with_ops_queued(true));
}

#[test]
fn typed_ops_get_their_argument() {
    let oq = TypedOpsQueue::<(u32, &'static str)>::new(params(false));
    oq.start();

    let (tx, rx) = mpsc::channel();
    for (n, s) in [(1, "one"), (2, "two"), (3, "three")] {
        let tx = tx.clone();
        oq.enqueue(move |arg| tx.send(arg).unwrap(), (n, s));
    }
    for want in [(1, "one"), (2, "two"), (3, "three")] {
        assert_eq!(
            want,
            rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap()
        );
    }
    assert!(oq.stop().wait_timeout(5 * SECOND));
}

#[test]
fn an_op_can_start_a_timer() {
    // The worker thread is not one of the runtime's, and still `after_func` works from an op,
    // on the runtime the queue was started on.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("ops-queue-test")
        .enable_time()
        .build()
        .unwrap();
    let oq = OpsQueue::new(params(false));
    {
        let _runtime = runtime.enter();
        oq.start();
    }
    let (tx, rx) = mpsc::channel();
    oq.enqueue(move || {
        let tx = Mutex::new(tx);
        time::after_func(10 * MILLISECOND, move || {
            let thread = std::thread::current();
            let _ = tx.lock().send(thread.name().map(str::to_string));
        });
    });
    assert_eq!(
        Some("ops-queue-test".to_string()),
        rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap()
    );
    oq.stop().wait();
}

#[test]
fn a_dropped_queue_runs_what_is_queued_and_ends_its_worker() {
    let oq = OpsQueue::new(params(false));
    oq.start();
    let done = oq.ops_queue_base.done_chan.clone();

    let (tx, rx) = mpsc::channel();
    for i in 0..10 {
        let tx = tx.clone();
        oq.enqueue(move || {
            std::thread::sleep(std::time::Duration::from_millis(1));
            tx.send(i).unwrap();
        });
    }
    drop(tx);
    drop(oq);

    assert!(done.wait_timeout(5 * SECOND));
    assert_eq!((0..10).collect::<Vec<i32>>(), rx.iter().collect::<Vec<_>>());
}

#[test]
fn a_queue_can_be_dropped_by_its_own_op() {
    // Ops often hold what owns their queue. The last of them then drops the queue on the
    // worker itself.
    struct Owner {
        oq: OpsQueue,
    }
    let owner = Arc::new(Owner {
        oq: OpsQueue::new(params(false)),
    });
    owner.oq.start();
    let done = owner.oq.ops_queue_base.done_chan.clone();

    let (release_tx, release_rx) = mpsc::channel::<()>();
    let o = owner.clone();
    owner.oq.enqueue(move || {
        release_rx.recv().unwrap();
        assert_eq!(1, Arc::strong_count(&o));
        drop(o);
    });
    drop(owner);
    release_tx.send(()).unwrap();

    assert!(done.wait_timeout(5 * SECOND));
}

#[test]
fn an_op_that_panics_ends_the_worker_and_stops_the_queue() {
    let oq = OpsQueue::new(params(true));
    oq.start();
    let done = oq.ops_queue_base.done_chan.clone();

    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let (tx, rx) = mpsc::channel();
    oq.enqueue(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        panic!("this op panics, as the test wants it to");
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    for i in 0..3 {
        let tx = tx.clone();
        oq.enqueue(move || tx.send(i).unwrap());
    }
    assert!(!done.is_done());
    release_tx.send(()).unwrap();

    assert!(done.wait_timeout(5 * SECOND));
    // what was queued is dropped, and nothing more is taken
    let tx_late = tx.clone();
    oq.enqueue(move || tx_late.send(100).unwrap());
    assert!(oq.ops_queue_base.lock.lock().ops.is_empty());
    drop(tx);
    assert_eq!(Vec::<i32>::new(), rx.iter().collect::<Vec<_>>());
    assert!(oq.stop().is_done());
}
