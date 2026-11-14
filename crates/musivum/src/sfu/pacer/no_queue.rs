// Ported from LiveKit, pkg/sfu/pacer/no_queue.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NewNoQueue` is `NoQueue::new` and gives an `Arc`,
// as a pacer is shared; the worker goroutine is a task that holds that `Arc`; the `wake`
// channel of one element is a `Notify`, which keeps one wake-up just so, and the fuse is a
// `CancellationToken`; the deque is a `VecDeque`, in an inner struct for what `lock`
// guards; the embedded `Base` is a field, and the methods of `Pacer` that the original gets
// from it by embedding are passed on to it in the `impl Pacer` at the end. The tests are
// new: the original has none.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::go::time::{self, Duration};
use crate::protocol::logger::Logger;
use crate::sfu::bwe::Bwe;
use crate::sfu::ccutils;

use super::{Base, Pacer, PacerProbeObserverListener, Packet};

pub struct NoQueue {
    pub base: Base,

    #[allow(dead_code)] // not read upstream either
    logger: Logger,

    lock: RwLock<NoQueueInner>,
    wake: Notify,
    stop: CancellationToken,
}

// What `lock` guards.
struct NoQueueInner {
    packets: VecDeque<Packet>,
}

impl NoQueue {
    pub fn new(logger: Logger, bwe: Option<Arc<dyn Bwe>>) -> Arc<NoQueue> {
        let n = Arc::new(NoQueue {
            base: Base::new(logger.clone(), bwe),
            logger,
            lock: RwLock::new(NoQueueInner {
                // n.packets.SetBaseCap(512)
                packets: VecDeque::with_capacity(512),
            }),
            wake: Notify::new(),
            stop: CancellationToken::new(),
        });

        time::spawn(n.clone().send_worker());
        n
    }

    pub fn stop(&self) {
        self.stop.cancel();

        self.wake.notify_one();
    }

    pub fn enqueue(&self, p: Packet) {
        let mut n = self.lock.write();
        n.packets.push_back(p);
        drop(n);

        self.wake.notify_one();
    }

    async fn send_worker(self: Arc<Self>) {
        loop {
            self.wake.notified().await;
            loop {
                if self.stop.is_cancelled() {
                    return;
                }

                let mut n = self.lock.write();
                let Some(p) = n.packets.pop_front() else {
                    break;
                };
                drop(n);

                let _ = self.base.send_packet(p);
            }
        }
    }
}

// ------------------------------------------------

impl Pacer for NoQueue {
    fn enqueue(&self, p: Packet) {
        NoQueue::enqueue(self, p);
    }

    fn stop(&self) {
        NoQueue::stop(self);
    }

    fn set_interval(&self, interval: Duration) {
        self.base.set_interval(interval);
    }

    fn set_bitrate(&self, bitrate: i64) {
        self.base.set_bitrate(bitrate);
    }

    fn time_since_last_sent_packet(&self) -> Duration {
        self.base.time_since_last_sent_packet()
    }

    fn set_pacer_probe_observer_listener(
        &self,
        listener: Option<Arc<dyn PacerProbeObserverListener>>,
    ) {
        self.base
            .probe_observer
            .set_pacer_probe_observer_listener(listener);
    }

    fn start_probe_cluster(&self, pci: ccutils::ProbeClusterInfo) {
        self.base.probe_observer.start_probe_cluster(pci);
    }

    fn end_probe_cluster(
        &self,
        probe_cluster_id: ccutils::ProbeClusterId,
    ) -> ccutils::ProbeClusterInfo {
        self.base.probe_observer.end_probe_cluster(probe_cluster_id)
    }
}

#[cfg(test)]
mod tests {
    use super::super::base::base_test::{RecWriter, plain_packet};
    use super::*;
    use crate::protocol::logger;
    use crate::testutils::eventually_async;

    const WAIT_FOR: std::time::Duration = std::time::Duration::from_secs(5);
    const TICK: std::time::Duration = std::time::Duration::from_millis(1);

    // New for musivum: the original has no tests. What is enqueued goes out from the worker
    // in the order it came, and `stop` ends the worker: nothing is sent after it.
    #[tokio::test(flavor = "multi_thread")]
    async fn packets_go_out_in_the_order_they_came_and_stop_ends_the_worker() {
        let w = RecWriter::new();
        let p = NoQueue::new(logger::get_logger(), None);
        // the worker holds the pacer
        assert_eq!(2, Arc::strong_count(&p));

        for sn in 0..300 {
            p.enqueue(plain_packet(sn, &w));
        }
        eventually_async(|| w.count() == 300, WAIT_FOR, TICK).await;
        let sns: Vec<u16> = w.written().iter().map(|x| x.sequence_number).collect();
        assert_eq!((0..300).collect::<Vec<u16>>(), sns);

        p.stop();
        eventually_async(|| Arc::strong_count(&p) == 1, WAIT_FOR, TICK).await;
        p.enqueue(plain_packet(300, &w));
        time::sleep(20 * time::MILLISECOND).await;
        assert_eq!(300, w.count());
    }
}
