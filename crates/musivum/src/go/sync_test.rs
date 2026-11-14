use std::sync::mpsc;
use std::time::Duration;

use super::*;
use crate::go::atomic;

#[test]
fn test_go_runs_every_job() {
    const JOBS: usize = 1000;
    let count = Arc::new(atomic::Uint64::new(0));
    let (tx, rx) = mpsc::channel();
    for _ in 0..JOBS {
        let count = count.clone();
        let tx = tx.clone();
        go(move || {
            count.add(1);
            tx.send(()).unwrap();
        });
    }
    for _ in 0..JOBS {
        rx.recv_timeout(Duration::from_secs(10)).unwrap();
    }
    assert_eq!(JOBS as u64, count.load());
}

// The pool's threads sleep while there is nothing to do: a thread wakes up once for a job
// that finds it waiting, and not at all while no job comes.
#[test]
fn test_pool_threads_sleep_between_jobs() {
    const JOBS: u64 = 50;
    let pool = Pool::new(4);
    let (tx, rx) = mpsc::channel();
    for _ in 0..JOBS {
        let tx = tx.clone();
        pool.go(Box::new(move || tx.send(()).unwrap()));
        rx.recv_timeout(Duration::from_secs(10)).unwrap();
    }

    let (threads, wake_ups) = pool.counts();
    assert!(threads <= 4, "{threads} threads");
    assert!(wake_ups <= JOBS, "{wake_ups} wake-ups for {JOBS} jobs");

    std::thread::sleep(Duration::from_millis(50));
    assert_eq!((threads, wake_ups), pool.counts(), "woke up without a job");
}

#[test]
fn test_wait_group_waits_and_tells_a_panic() {
    let pool = Pool::new(2);
    for panics in [false, true] {
        let wg = Arc::new(WaitGroup::new());
        let ran = Arc::new(atomic::Uint64::new(0));
        for _ in 0..4 {
            wg.add(1);
            let wg = wg.clone();
            let ran = ran.clone();
            pool.go(Box::new(move || {
                let _done = wg.defer_done();
                std::thread::sleep(Duration::from_millis(5));
                ran.add(1);
                if panics {
                    panic!("a job that panics, on purpose");
                }
            }));
        }
        assert_eq!(panics, wg.wait(), "panics {panics}");
        assert_eq!(4, ran.load(), "panics {panics}");
    }
    // The threads go on after a panic.
    let (tx, rx) = mpsc::channel();
    pool.go(Box::new(move || tx.send(()).unwrap()));
    rx.recv_timeout(Duration::from_secs(10)).unwrap();
}
