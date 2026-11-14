// Ported from LiveKit, pkg/sfu/pacer/leaky_bucket.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NewLeakyBucket` is `LeakyBucket::new` and gives an
// `Arc`, as a pacer is shared; the worker goroutine is a task that holds that `Arc`; its
// timer, which the original resets to the interval on every way back to waiting for it, is
// a sleep of the interval at the top of the loop; the fuse is a `CancellationToken`; the
// deque is a `VecDeque`, with the interval and the bitrate in an inner struct for what
// `lock` guards; a bitrate and a count of bytes are `i64`; the embedded `Base` is a field,
// and the methods of `Pacer` that the original gets from it by embedding are passed on to
// it in the `impl Pacer` at the end. The tests are new: the original has none.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

use crate::go::time::{self, Duration};
use crate::protocol::logger::Logger;
use crate::sfu::bwe::Bwe;
use crate::sfu::ccutils;

use super::{Base, Pacer, PacerProbeObserverListener, Packet};

const MAX_OVERSHOOT_FACTOR: f64 = 2.0;

pub struct LeakyBucket {
    pub base: Base,

    #[allow(dead_code)] // not read upstream either
    logger: Logger,

    lock: RwLock<LeakyBucketInner>,
    stop: CancellationToken,
}

// What `lock` guards.
struct LeakyBucketInner {
    packets: VecDeque<Packet>,
    interval: Duration,
    bitrate: i64,
}

impl LeakyBucket {
    pub fn new(
        logger: Logger,
        bwe: Option<Arc<dyn Bwe>>,
        interval: Duration,
        bitrate: i64,
    ) -> Arc<LeakyBucket> {
        let l = Arc::new(LeakyBucket {
            base: Base::new(logger.clone(), bwe),
            logger,
            lock: RwLock::new(LeakyBucketInner {
                // l.packets.SetBaseCap(512)
                packets: VecDeque::with_capacity(512),
                interval,
                bitrate,
            }),
            stop: CancellationToken::new(),
        });

        time::spawn(l.clone().send_worker());
        l
    }

    pub fn set_interval(&self, interval: Duration) {
        let mut l = self.lock.write();

        l.interval = interval;
    }

    pub fn set_bitrate(&self, bitrate: i64) {
        let mut l = self.lock.write();

        l.bitrate = bitrate;
    }

    pub fn stop(&self) {
        self.stop.cancel();
    }

    pub fn enqueue(&self, p: Packet) {
        let mut l = self.lock.write();
        l.packets.push_back(p);
        drop(l);
    }

    async fn send_worker(self: Arc<Self>) {
        let mut interval = self.lock.read().interval;
        let mut bitrate: i64;

        // timer := time.NewTimer(interval)
        let mut overage: i64 = 0;

        loop {
            // <-timer.C, after the timer.Reset(interval) on each of the ways back here
            time::sleep(interval).await;

            {
                let l = self.lock.read();
                interval = l.interval;
                bitrate = l.bitrate;
            }

            // calculate number of bytes that can be sent in this interval
            // adjusting for overage.
            let interval_bytes = (interval.seconds() * bitrate as f64 / 8.0) as i64;
            let max_overshoot_bytes = (interval_bytes as f64 * MAX_OVERSHOOT_FACTOR) as i64;
            let mut to_send_bytes = interval_bytes.wrapping_sub(overage);
            if to_send_bytes < 0 {
                // too much overage, wait for next interval
                overage = to_send_bytes.wrapping_neg();
                continue;
            }

            // do not allow too much overshoot in an interval
            if to_send_bytes > max_overshoot_bytes {
                to_send_bytes = max_overshoot_bytes;
            }

            loop {
                if self.stop.is_cancelled() {
                    return;
                }

                let mut l = self.lock.write();
                let Some(p) = l.packets.pop_front() else {
                    drop(l);
                    // allow overshoot in next interval with shortage in this interval
                    overage = to_send_bytes.wrapping_neg();
                    break;
                };
                drop(l);

                let written = self.base.send_packet(p).unwrap_or(0);
                to_send_bytes = to_send_bytes.wrapping_sub(written as i64);
                if to_send_bytes < 0 {
                    // overage, wait for next interval
                    overage = to_send_bytes.wrapping_neg();
                    break;
                }
            }
        }
    }
}

// ------------------------------------------------

impl Pacer for LeakyBucket {
    fn enqueue(&self, p: Packet) {
        LeakyBucket::enqueue(self, p);
    }

    fn stop(&self) {
        LeakyBucket::stop(self);
    }

    fn set_interval(&self, interval: Duration) {
        LeakyBucket::set_interval(self, interval);
    }

    fn set_bitrate(&self, bitrate: i64) {
        LeakyBucket::set_bitrate(self, bitrate);
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
    use std::time::Instant;

    use super::super::base::base_test::{RecWriter, plain_packet};
    use super::*;
    use crate::protocol::logger;
    use crate::testutils::eventually_async;

    const WAIT_FOR: std::time::Duration = std::time::Duration::from_secs(5);
    const TICK: std::time::Duration = std::time::Duration::from_millis(1);
    // a wait may seem this much shorter than it was asked for: the clocks' doing
    const SLACK: std::time::Duration = std::time::Duration::from_millis(1);

    // A bucket of 1000 bytes per interval of 40 ms with nine packets waiting in it, each
    // written as 500 bytes, and when each was written, counted from the start. The bucket
    // is made with another bitrate, which `set_bitrate` puts right before the first
    // interval is over. `None` if this thread was held up while it set that up, so that
    // the first interval may have passed without all of it.
    async fn nine_packets_in_a_bucket()
    -> Option<(Arc<LeakyBucket>, Arc<RecWriter>, Vec<std::time::Duration>)> {
        let interval = 40 * time::MILLISECOND;
        let w = RecWriter::answering(Ok(500));

        let start = Instant::now();
        let l = LeakyBucket::new(logger::get_logger(), None, interval, 8_000);
        let p: Arc<dyn Pacer> = l.clone();
        p.set_bitrate(200_000);
        for sn in 0..9 {
            p.enqueue(plain_packet(sn, &w));
        }
        if start.elapsed() >= interval.to_std() / 2 {
            l.stop();
            return None;
        }

        eventually_async(|| w.count() == 9, WAIT_FOR, TICK).await;
        let at = w
            .written()
            .iter()
            .map(|x| x.at.duration_since(start))
            .collect();
        Some((l, w, at))
    }

    // New for musivum: the original has no tests. The bucket lets the bytes of an interval
    // through and the packet that takes it below zero, which the next interval pays for:
    // with 1000 bytes per interval and packets of 500 bytes that is three packets in the
    // first interval and two in each one after it. After intervals with nothing to send it
    // makes up for at most two intervals' bytes: five packets at once. The numbers are
    // those of a run of the original at this rate.
    #[tokio::test(flavor = "multi_thread")]
    async fn packets_are_spaced_at_the_bitrate() {
        let mut attempts = 0;
        let (l, w, at) = loop {
            attempts += 1;
            match nine_packets_in_a_bucket().await {
                Some(bucket) => break bucket,
                None => assert!(attempts < 10, "this thread is held up every time"),
            }
        };
        let sns: Vec<u16> = w.written().iter().map(|x| x.sequence_number).collect();
        assert_eq!((0..9).collect::<Vec<u16>>(), sns);

        let interval = (40 * time::MILLISECOND).to_std();
        // by the end of the n-th interval no more than 2n+1 packets are out
        for (i, at) in at.iter().enumerate() {
            let intervals = i.div_ceil(2).max(1) as u32;
            assert!(*at + SLACK >= interval * intervals, "{i}: {at:?}");
        }
        // three together, then two and two and two, an interval apart
        assert!(at[2] - at[0] < interval / 2, "{at:?}");
        for first in [3, 5, 7] {
            assert!(at[first] - at[first - 1] + SLACK >= interval, "{at:?}");
            assert!(at[first + 1] - at[first] < interval / 2, "{at:?}");
        }

        // a longer interval at the same 1000 bytes: it holds from the next one on
        let interval = 50 * time::MILLISECOND;
        l.set_interval(interval);
        l.set_bitrate(160_000);
        let interval = interval.to_std();
        // nothing to send for at least three intervals
        time::sleep(250 * time::MILLISECOND).await;
        assert_eq!(9, w.count());

        for sn in 100..109 {
            l.enqueue(plain_packet(sn, &w));
        }
        eventually_async(|| w.count() == 18, WAIT_FOR, TICK).await;
        let written = w.written()[9..].to_vec();
        let sns: Vec<u16> = written.iter().map(|x| x.sequence_number).collect();
        assert_eq!((100..109).collect::<Vec<u16>>(), sns);
        let at: Vec<Instant> = written.iter().map(|x| x.at).collect();
        // five together, then two and two, an interval apart
        assert!(at[4] - at[0] < interval / 2, "{at:?}");
        for first in [5, 7] {
            assert!(at[first] - at[first - 1] + SLACK >= interval, "{at:?}");
            assert!(at[first + 1] - at[first] < interval / 2, "{at:?}");
        }

        l.stop();
    }

    // New for musivum: `stop` ends the worker at its next interval, and nothing is sent
    // after that.
    #[tokio::test(flavor = "multi_thread")]
    async fn stop_ends_the_worker() {
        let w = RecWriter::answering(Ok(500));
        let l = LeakyBucket::new(logger::get_logger(), None, 5 * time::MILLISECOND, 800_000);
        // the worker holds the pacer
        assert_eq!(2, Arc::strong_count(&l));
        l.enqueue(plain_packet(0, &w));
        eventually_async(|| w.count() == 1, WAIT_FOR, TICK).await;

        l.stop();
        eventually_async(|| Arc::strong_count(&l) == 1, WAIT_FOR, TICK).await;
        l.enqueue(plain_packet(1, &w));
        time::sleep(30 * time::MILLISECOND).await;
        assert_eq!(1, w.count());
    }
}
