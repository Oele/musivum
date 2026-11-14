// Ported from LiveKit, pkg/sfu/forwardstats.go at v1.13.11.
// Copyright LiveKit, Inc. (the original file has no notice of its own). Licensed under the
// Apache License, Version 2.0.
// Changes: translated from Go to Rust. The two rings of a shard are allocated instead of
// being arrays inside the struct (the buffer is 2 MiB, more than a thread's stack may hold),
// and the read cursor of a shard, which only the consumer touches, is an atomic, since
// `drain` takes `&self`. What `lock` guards is in an inner struct. The worker goroutine is a
// task that holds the `Arc` which `new` returns; its tickers are the runtime's intervals,
// and `closeCh` is a `Notify`: a second `stop` does nothing, where the original panics on
// closing a closed channel, and an interval that is not positive panics in the worker task,
// where the original takes the process down. The counters it reports to are
// `crate::telemetry::prometheus`.

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::MissedTickBehavior;

use crate::go::atomic;
use crate::go::time::{self, Duration};
use crate::protocol::logger;
use crate::telemetry::prometheus;
use crate::{infow, warnw};

const C_HIGH_FORWARDING_LATENCY: Duration = Duration::from_millis(20);
const C_SKEW_FACTOR: i64 = 10;

// forwardLatencyPercentile is the transit quantile reported as the node's
// forwarding latency and read by the overload controller. p90 tracks a broad
// slowdown but ignores the sparse tail (a few packets stalled on goroutine
// scheduling), which shedding cannot fix.
const FORWARD_LATENCY_PERCENTILE: f64 = 0.90;

// A summary interval's worth of samples across all tracks must fit without
// dropping (ForwardStats is a singleton). Shard count spreads the per-packet
// atomic; shard capacity bounds memory (numShards*shardCap*16 bytes = 2MiB).
const FORWARD_SAMPLE_NUM_SHARDS: usize = 16;
const FORWARD_SAMPLE_SHARD_CAP: usize = 8192;
const FORWARD_SAMPLE_SHARD_MASK: u64 = FORWARD_SAMPLE_SHARD_CAP as u64 - 1;
const FORWARD_SAMPLE_SHARD_SEL: u64 = FORWARD_SAMPLE_NUM_SHARDS as u64 - 1;

// forwardSampleShard is a ring of transit samples with multiple producers and a
// single consumer. A producer reserves a slot, stores the value, then publishes
// the slot's epoch (reserved index + 1). The consumer reads a slot only once its
// epoch marks the value committed for that index.
struct ForwardSampleShard {
    write_idx: atomic::Uint64,  // advanced by producers to reserve a slot
    read_idx: atomic::Uint64,   // consumer-only cursor
    ring: Box<[atomic::Int64]>, // forwardSampleShardCap of them
    seq: Box<[atomic::Uint64]>, // per-slot publish epoch
}

impl Default for ForwardSampleShard {
    fn default() -> Self {
        Self {
            write_idx: atomic::Uint64::new(0),
            read_idx: atomic::Uint64::new(0),
            ring: (0..FORWARD_SAMPLE_SHARD_CAP)
                .map(|_| atomic::Int64::new(0))
                .collect(),
            seq: (0..FORWARD_SAMPLE_SHARD_CAP)
                .map(|_| atomic::Uint64::new(0))
                .collect(),
        }
    }
}

// forwardSampleBuffer holds per-packet transit samples produced on the packet
// path and consumed by the background worker, which performs metric emission.
#[derive(Default)]
struct ForwardSampleBuffer {
    shards: [ForwardSampleShard; FORWARD_SAMPLE_NUM_SHARDS],
    dropped: atomic::Uint64,
}

impl ForwardSampleBuffer {
    // push records a sample: reserve a slot, store the value, then publish the
    // slot's epoch. The shard is selected from arrival time bits.
    fn push(&self, arrival: i64, transit_ns: i64) {
        let sh = &self.shards[(((arrival as u64) >> 6) & FORWARD_SAMPLE_SHARD_SEL) as usize];
        let i = sh.write_idx.add(1).wrapping_sub(1);
        let slot = (i & FORWARD_SAMPLE_SHARD_MASK) as usize;
        sh.ring[slot].store(transit_ns);
        sh.seq[slot].store(i.wrapping_add(1));
    }

    // drain passes every committed sample to fn and advances the read cursor. Only
    // the background worker calls this.
    //
    // A slot holds index r's value once its epoch equals r+1. If the slot at the
    // cursor is still uncommitted (a producer reserved it but has not published),
    // draining stops and resumes from there on the next call, so no sample is read
    // stale or skipped. When producers get a shard's capacity ahead, or overwrite a
    // slot before it is read, the affected samples are counted as dropped.
    fn drain(&self, mut f: impl FnMut(i64)) {
        for sh in &self.shards {
            let w = sh.write_idx.load();
            let mut r = sh.read_idx.load();
            if w.wrapping_sub(r) > FORWARD_SAMPLE_SHARD_CAP as u64 {
                self.dropped.add(
                    w.wrapping_sub(r)
                        .wrapping_sub(FORWARD_SAMPLE_SHARD_CAP as u64),
                );
                r = w.wrapping_sub(FORWARD_SAMPLE_SHARD_CAP as u64);
            }
            while r < w {
                let slot = (r & FORWARD_SAMPLE_SHARD_MASK) as usize;
                if sh.seq[slot].load() < r.wrapping_add(1) {
                    // reserved but not yet published; resume here next drain
                    break;
                }
                let v = sh.ring[slot].load();
                if sh.seq[slot].load() != r.wrapping_add(1) {
                    // overwritten by a newer sample during the read; original lost
                    self.dropped.add(1);
                    r = r.wrapping_add(1);
                    continue;
                }
                f(v);
                r = r.wrapping_add(1);
            }
            sh.read_idx.store(r);
        }
    }

    fn take_dropped(&self) -> u64 {
        self.dropped.swap(0)
    }
}

// forwardSummary is a mergeable histogram of forwarding transit over an
// interval. Each power-of-two octave [2^e, 2^(e+1)) us is split into
// forwardHistSub linear sub-buckets, and each bucket keeps the observed
// [min, max] of the samples in it. A percentile interpolates within the
// crossing bucket's observed range rather than its nominal edges, so the
// estimate never leaves the samples and is exact when a bucket's samples
// cluster -- which keeps it accurate near an overload threshold that falls
// mid-bucket, without needing threshold-aware boundaries. Two summaries merge
// by adding their buckets, so the percentile covers the whole report window,
// and a few stalled packets (e.g. from goroutine scheduling latency) cannot
// drag it the way a mean does.
const FORWARD_HIST_SUB: usize = 8; // linear sub-buckets per octave
const FORWARD_HIST_OCTAVES: usize = 27; // up to 2^27 us (~134 s)
const FORWARD_HIST_BUCKETS: usize = FORWARD_HIST_OCTAVES * FORWARD_HIST_SUB; // 216

// bucketStat is one bucket: how many samples fell in it and their observed
// transit range in nanoseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BucketStat {
    count: i64,
    min_ns: i64,
    max_ns: i64,
}

impl BucketStat {
    fn add(&mut self, transit_ns: i64) {
        if self.count == 0 {
            (self.min_ns, self.max_ns) = (transit_ns, transit_ns);
        } else {
            self.min_ns = self.min_ns.min(transit_ns);
            self.max_ns = self.max_ns.max(transit_ns);
        }
        self.count += 1;
    }

    fn merge_in(&mut self, o: BucketStat) {
        if o.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = o;
            return;
        }
        self.count += o.count;
        self.min_ns = self.min_ns.min(o.min_ns);
        self.max_ns = self.max_ns.max(o.max_ns);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ForwardSummary {
    count: i64,
    min_ns: i64,
    max_ns: i64,
    buckets: [BucketStat; FORWARD_HIST_BUCKETS],
}

impl Default for ForwardSummary {
    fn default() -> Self {
        Self {
            count: 0,
            min_ns: 0,
            max_ns: 0,
            buckets: [BucketStat::default(); FORWARD_HIST_BUCKETS],
        }
    }
}

// forwardBucket returns the bucket index for a transit in nanoseconds: the
// octave floor(log2(us)) times forwardHistSub, plus the linear sub-bucket
// within the octave.
fn forward_bucket(transit_ns: i64) -> usize {
    let us = transit_ns / 1000;
    if us <= 0 {
        return 0;
    }
    let e = (u64::BITS - (us as u64).leading_zeros()) as usize - 1; // floor(log2(us)), us >= 1 so e >= 0
    if e >= FORWARD_HIST_OCTAVES {
        return FORWARD_HIST_BUCKETS - 1;
    }
    let octave = 1i64 << e;
    e * FORWARD_HIST_SUB + ((us - octave) * FORWARD_HIST_SUB as i64 / octave) as usize
}

impl ForwardSummary {
    fn add_sample(&mut self, transit_ns: i64) {
        if self.count == 0 {
            (self.min_ns, self.max_ns) = (transit_ns, transit_ns);
        } else {
            self.min_ns = self.min_ns.min(transit_ns);
            self.max_ns = self.max_ns.max(transit_ns);
        }
        self.count += 1;
        self.buckets[forward_bucket(transit_ns)].add(transit_ns);
    }

    fn merge(mut self, o: ForwardSummary) -> ForwardSummary {
        if o.count == 0 {
            return self;
        }
        if self.count == 0 {
            return o;
        }
        self.count += o.count;
        self.min_ns = self.min_ns.min(o.min_ns);
        self.max_ns = self.max_ns.max(o.max_ns);
        for (b, ob) in self.buckets.iter_mut().zip(o.buckets) {
            b.merge_in(ob);
        }
        self
    }

    // percentile returns the p-quantile (0..1) of transit, interpolated within the
    // observed [min, max] of the bucket the quantile falls in.
    fn percentile(&self, p: f64) -> Duration {
        if self.count == 0 {
            return Duration::ZERO;
        }
        let target = p * self.count as f64;
        let mut cum = 0.0f64;
        for b in &self.buckets {
            let c = b.count as f64;
            if c == 0.0 {
                continue;
            }
            if cum + c >= target {
                let (lo, hi) = (b.min_ns as f64, b.max_ns as f64);
                return Duration((lo + (hi - lo) * (target - cum) / c) as i64);
            }
            cum += c;
        }
        Duration(self.max_ns)
    }
}

pub struct ForwardStats {
    samples: ForwardSampleBuffer,

    // ring of per-summary-interval summaries covering the report window.
    // written by the background worker (flush) and read both by the worker
    // (report) and by external callers (GetStats), so it is guarded by lock.
    lock: Mutex<ForwardStatsInner>,

    summary_interval: Duration,
    report_interval: Duration,

    close_ch: Notify,
}

// What `lock` guards.
struct ForwardStatsInner {
    ring: Vec<ForwardSummary>,
    ring_head: usize,
    ring_len: usize,
}

impl ForwardStats {
    pub fn new(
        summary_interval: Duration,
        report_interval: Duration,
        report_window: Duration,
    ) -> Arc<ForwardStats> {
        let mut ring_cap = (report_window + summary_interval - time::NANOSECOND) / summary_interval;
        if ring_cap < 1 {
            ring_cap = 1;
        }

        let s = Arc::new(ForwardStats {
            samples: ForwardSampleBuffer::default(),
            lock: Mutex::new(ForwardStatsInner {
                ring: vec![ForwardSummary::default(); ring_cap as usize],
                ring_head: 0,
                ring_len: 0,
            }),
            summary_interval,
            report_interval,
            close_ch: Notify::new(),
        });

        time::spawn(s.clone().run());
        s
    }

    // Update records a forwarded packet's transit latency. It buffers the sample
    // and returns the transit and whether it exceeds the high-latency threshold.
    // The sample is aggregated and emitted by the background worker.
    pub fn update(&self, arrival: i64, left: i64) -> (i64, bool) {
        let transit = left.wrapping_sub(arrival);
        self.samples.push(arrival, transit);
        (transit, Duration(transit) > C_HIGH_FORWARDING_LATENCY)
    }

    pub fn stop(&self) {
        self.close_ch.notify_one();
    }

    async fn run(self: Arc<Self>) {
        let mut summary_ticker = new_ticker(self.summary_interval);
        let mut report_ticker = new_ticker(self.report_interval);

        loop {
            tokio::select! {
                _ = self.close_ch.notified() => {
                    return;
                }

                _ = summary_ticker.tick() => {
                    self.flush();
                }

                _ = report_ticker.tick() => {
                    // the summary ticker keeps the window ring current to within one
                    // summary interval; report over it without advancing the ring.
                    self.report();
                }
            }
        }
    }

    // flush drains the buffered samples, observes each into the Prometheus
    // histogram, and folds the interval summary into the window ring used for the
    // latency gauge.
    fn flush(&self) {
        let mut summ = ForwardSummary::default();
        self.samples.drain(|transit_ns| {
            prometheus::record_forward_latency_sample(transit_ns);
            summ.add_sample(transit_ns);
        });

        let mut inner = self.lock.lock();
        let ring_head = inner.ring_head;
        inner.ring[ring_head] = summ;
        inner.ring_head = (inner.ring_head + 1) % inner.ring.len();
        if inner.ring_len < inner.ring.len() {
            inner.ring_len += 1;
        }
    }

    // summarize merges the ring summaries covering the most recent window. A
    // window <= 0 (or >= the report window) covers the entire ring.
    fn summarize(&self, window: Duration) -> ForwardSummary {
        let inner = self.lock.lock();

        let mut n = inner.ring_len;
        if window > Duration::ZERO && self.summary_interval > Duration::ZERO {
            let mut want =
                (window + self.summary_interval - time::NANOSECOND) / self.summary_interval;
            if want < 1 {
                want = 1;
            }
            if want < n as i64 {
                n = want as usize;
            }
        }

        // walk backwards from the most recent entry (ringHead-1) over n entries.
        let mut w = ForwardSummary::default();
        for i in 0..n {
            // (ringHead - 1 - i + len(ring)) % len(ring), in an order that stays above zero
            let idx = (inner.ring_head + inner.ring.len() - 1 - i) % inner.ring.len();
            w = w.merge(inner.ring[idx]);
        }
        w
    }

    // GetStats returns the reported forwarding-latency percentile
    // (forwardLatencyPercentile) over the most recent duration. The duration is
    // rounded up to a whole number of summary intervals (the smallest bucket span
    // that covers it). A duration <= 0, or one that meets/exceeds the report window,
    // covers the full window.
    pub fn get_stats(&self, duration: Duration) -> Duration {
        self.summarize(duration)
            .percentile(FORWARD_LATENCY_PERCENTILE)
    }

    fn report(&self) {
        let w = self.summarize(Duration::ZERO);

        let p90 = w.percentile(FORWARD_LATENCY_PERCENTILE);
        let dropped = self.samples.take_dropped();
        if dropped > 0 {
            warnw!(
                logger::get_logger(),
                "forward stats sample buffer overflow",
                nil,
                "dropped" => dropped,
            );
        }
        // a max far above p90 means a few packets stalled (e.g. goroutine scheduling
        // latency) rather than a broad forwarding slowdown; p90 rides through it.
        if w.count > 0 && w.max_ns > p90.nanoseconds().wrapping_mul(C_SKEW_FACTOR) {
            infow!(
                logger::get_logger(),
                "high spread in forwarding path",
                "lowest" => Duration(w.min_ns),
                "highest" => Duration(w.max_ns),
                "count" => w.count,
                "p90" => p90,
            );
        }

        prometheus::record_forward_latency(p90.nanoseconds() as u32);
    }
}

// Go's `time.NewTicker(d)`: the first tick comes after `d`, and ticks that the receiver is
// too slow for are dropped.
fn new_ticker(d: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + d.to_std(), d.to_std());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker
}

#[cfg(test)]
#[path = "forwardstats_test.rs"]
mod forwardstats_test;
