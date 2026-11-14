// Ported from LiveKit, pkg/sfu/ccutils/prober.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the worker goroutine is a task, started by
// `add_cluster`, which for that takes the prober as the `Arc` that `new` returns; its ticker
// is a sleep; the deque is a `VecDeque`; `ProbeClusterMode` is an enum and its `String()` is
// `Display`; `MarshalLogObject` is `Debug`, derived or, where the original adds values to the
// fields, written to read like a derived one; where the original reads fields of a cluster
// without its lock (`Id`, `Start`, `MarshalLogObject`), the lock is taken for the read. The
// tests at the end are new: the original has none.

// Design of Prober
//
// Probing is used to check for existence of excess channel capacity.
// This is especially useful in the downstream direction of SFU.
// SFU forwards audio/video streams from one or more publishers to
// all the subscribers. But, the downstream channel of a subscriber
// may not be big enough to carry all the streams. It is also a time
// varying quantity.
//
// When there is not enough capacity, some streams will be paused.
// To resume a stream, SFU would need to know that the channel has
// enough capacity. That's where probing comes in. When conditions
// are favorable, SFU can send probe packets so that the bandwidth
// estimator has more data to estimate available channel capacity
// better.
// NOTE: What defines `favorable conditions` is implementation dependent.
//
// There are two options for probing
//   - Use padding only RTP packets: This one is preferable as
//     probe rate can be controlled more tightly.
//   - Resume a paused stream or forward a higher spatial layer:
//     Have to find a stream at probing rate. Also, a stream could
//     get a key frame unexpectedly boosting rate in the probing
//     window.
//
// The strategy used depends on stream allocator implementation.
// This module can be used if the stream allocator decides to use
// padding only RTP packets for probing purposes.
//
// Implementation:
// There are a couple of options
//   - Check prober in the forwarding path (pull from prober).
//     This is preferred for scalability reasons. But, this
//     suffers from not being able to probe when all streams
//     are paused (could be due to downstream bandwidth
//     constraints or the corresponding upstream tracks may
//     have paused due to upstream bandwidth constraints).
//     Another issue is not being able to have tight control on
//     probing window boundary as the packet forwarding path
//     may not have a packet to forward. But, it should not
//     be a major concern as long as some stream(s) is/are
//     forwarded as there should be a packet at least every
//     60 ms or so (forwarding only one stream at 15 fps).
//     Usually, it will be serviced much more frequently when
//     there are multiple streams getting forwarded.
//   - Run it a go routine. But, that would have to wake up
//     very often to prevent bunching up of probe
//     packets. So, a scalability concern as there is one prober
//     per subscriber peer connection. But, probe windows
//     should be very short (of the order of 100s of ms).
//     So, this approach might be fine.
//
// The implementation here follows the second approach of using a
// go routine.
//
// Pacing:
// ------
// Ideally, the subscriber peer connection should have a pacer which
// trickles data out at the estimated channel capacity rate (and
// estimated channel capacity + probing rate when actively probing).
//
// But, there a few significant challenges
//  1. Pacer will require buffering of forwarded packets. That means
//     more memory, more CPU (have to make copy of packets) and
//     more latency in the media stream.
//  2. Scalability concern as SFU may be handling hundreds of
//     subscriber peer connections and each one processing the pacing
//     loop at 5ms interval will add up.
//
// So, this module assumes that pacing is inherently provided by the
// publishers for media streams. That is a reasonable assumption given
// that publishing clients will run their own pacer and pacing data out
// at a steady rate.
//
// A further assumption is that if there are multiple publishers for
// a subscriber peer connection, all the publishers are not pacing
// in sync, i.e. each publisher's pacer is completely independent
// and SFU will be receiving the media packets with a good spread and
// not clumped together.
//
// Given those assumptions, this module monitors media send rate and
// adjusts probing packet sends accordingly. Although the probing may
// have a high enough wake up frequency, it is for short windows.
// For example, probing at 5 Mbps for 1/2 second and sending 1000 byte
// probe per iteration will wake up every 1.6 ms. That is very high,
// but should last for 1/2 second or so.
//
//	5 Mbps over 1/2 second = 2.5 Mbps
//	2.5 Mbps = 312500 bytes = 313 probes at 1000 byte probes
//	313 probes over 1/2 second = 1.6 ms between probes
//
// A few things to note
//  1. When a probe cluster is added, the expected media rate is provided.
//     So, the wake-up interval takes that into account. For example,
//     if probing at 5 Mbps for 1/2 second and if 4 Mbps of it is expected
//     to be provided by media traffic, the wake-up interval becomes 8 ms.
//  2. The amount of probing should actually be capped at some value to
//     avoid too much self-induced congestion. It maybe something like 500 kbps.
//     That will increase the wake-up interval to 16 ms in the above example.
//  3. In practice, the probing interval may also be shorter. Typically,
//     it can be run for 2 - 3 RTTs to get a good measurement. For
//     the longest hauls, RTT could be 250 ms or so leading to the probing
//     window being long(ish). But, RTT should be much shorter especially if
//     the subscriber peer connection of the client is able to connect to
//     the nearest data center.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::debugw;
use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;

pub trait ProberListener: Send + Sync {
    fn on_probe_cluster_switch(&self, info: ProbeClusterInfo);
    fn on_send_probe(&self, bytes_to_send: i64);
}

#[derive(Clone, Default)]
pub struct ProberParams {
    pub listener: Option<Arc<dyn ProberListener>>,
    pub logger: Logger,
}

pub struct Prober {
    params: ProberParams,

    cluster_id: atomic::Uint32,

    clusters_mu: RwLock<ProberInner>,
}

// What `clustersMu` guards.
struct ProberInner {
    clusters: VecDeque<Arc<Cluster>>,
    active_cluster: Option<Arc<Cluster>>,
}

impl Prober {
    pub fn new(params: ProberParams) -> Arc<Prober> {
        Arc::new(Prober {
            params,
            cluster_id: atomic::Uint32::new(0),
            clusters_mu: RwLock::new(ProberInner {
                // p.clusters.SetBaseCap(2)
                clusters: VecDeque::with_capacity(2),
                active_cluster: None,
            }),
        })
    }

    pub fn is_running(&self) -> bool {
        let inner = self.clusters_mu.read();

        !inner.clusters.is_empty()
    }

    pub fn reset(&self, info: ProbeClusterInfo) {
        let mut inner = self.clusters_mu.write();

        if let Some(active_cluster) = &inner.active_cluster
            && active_cluster.id() == info.id
        {
            active_cluster.mark_completed(info.result);
            debugw!(
                self.params.logger,
                "prober: resetting active cluster",
                "cluster" => active_cluster,
            );
        }

        inner.clusters.clear();
        inner.active_cluster = None;
    }

    pub fn add_cluster(
        self: &Arc<Self>,
        mode: ProbeClusterMode,
        pcg: ProbeClusterGoal,
    ) -> ProbeClusterInfo {
        if pcg.desired_bps <= 0 {
            return PROBE_CLUSTER_INFO_INVALID;
        }

        let cluster_id = ProbeClusterId(self.cluster_id.inc());
        let cluster = Cluster::new(cluster_id, mode, pcg, self.params.listener.clone());
        debugw!(self.params.logger, "cluster added", "cluster" => cluster);

        self.push_back_cluster_and_maybe_start(cluster.clone());

        cluster.info()
    }

    pub fn probes_sent(&self, bytes_sent: i64) {
        let Some(cluster) = self.get_front_cluster() else {
            return;
        };

        cluster.probes_sent(bytes_sent);
    }

    pub fn cluster_done(&self, info: ProbeClusterInfo) {
        let Some(cluster) = self.get_front_cluster() else {
            return;
        };

        if cluster.id() == info.id {
            cluster.mark_completed(info.result);
            debugw!(self.params.logger, "cluster done", "cluster" => cluster);
            self.pop_front_cluster(&cluster);
        }
    }

    pub fn get_active_cluster_id(&self) -> ProbeClusterId {
        let inner = self.clusters_mu.read();

        if let Some(active_cluster) = &inner.active_cluster {
            return active_cluster.id();
        }

        PROBE_CLUSTER_ID_INVALID
    }

    fn get_front_cluster(&self) -> Option<Arc<Cluster>> {
        let mut inner = self.clusters_mu.write();

        if inner.active_cluster.is_some() {
            return inner.active_cluster.clone();
        }

        if inner.clusters.is_empty() {
            inner.active_cluster = None;
        } else {
            inner.active_cluster = inner.clusters.front().cloned();
            if let Some(active_cluster) = &inner.active_cluster {
                active_cluster.start();
            }
        }
        inner.active_cluster.clone()
    }

    fn pop_front_cluster(&self, cluster: &Arc<Cluster>) {
        let mut inner = self.clusters_mu.write();

        if inner.clusters.is_empty() {
            inner.active_cluster = None;
            return;
        }

        if inner
            .clusters
            .front()
            .is_some_and(|front| Arc::ptr_eq(front, cluster))
        {
            inner.clusters.pop_front();
        }

        if inner
            .active_cluster
            .as_ref()
            .is_some_and(|active_cluster| Arc::ptr_eq(active_cluster, cluster))
        {
            inner.active_cluster = None;
        }
    }

    fn push_back_cluster_and_maybe_start(self: &Arc<Self>, cluster: Arc<Cluster>) {
        let mut inner = self.clusters_mu.write();
        inner.clusters.push_back(cluster);

        if inner.clusters.len() == 1 {
            time::spawn(self.clone().run());
        }
    }

    async fn run(self: Arc<Self>) {
        loop {
            let Some(cluster) = self.get_front_cluster() else {
                return;
            };

            let sleep_duration = cluster.process();
            if sleep_duration == Duration::ZERO {
                self.pop_front_cluster(&cluster);
                continue;
            }

            time::sleep(sleep_duration).await;
        }
    }
}

// ---------------------------------

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProbeClusterId(pub u32);

impl fmt::Display for ProbeClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

// Logged as the number it is.
impl fmt::Debug for ProbeClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

pub const PROBE_CLUSTER_ID_INVALID: ProbeClusterId = ProbeClusterId(0);

// padding only packets are 255 bytes max + 20 byte header = 4 packets per probe,
// when not using padding only packets, this is a min and actual sent could be higher
const C_BYTES_PER_PROBE: i64 = 1100;
const C_SLEEP_DURATION: Duration = Duration::from_millis(20);
const C_SLEEP_DURATION_MIN: Duration = Duration::from_millis(10);

// -----------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ProbeClusterMode {
    #[default]
    Uniform,
    LinearChirp,
}

impl fmt::Display for ProbeClusterMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ProbeClusterMode::Uniform => "UNIFORM",
            ProbeClusterMode::LinearChirp => "LINEAR_CHIRP",
        })
    }
}

// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProbeClusterGoal {
    pub available_bandwidth_bps: i64,
    pub expected_usage_bps: i64,
    pub desired_bps: i64,
    pub duration: Duration,
    pub desired_bytes: i64,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct ProbeClusterResult {
    pub start_time: i64,
    pub end_time: i64,
    pub packets_probe: i64,
    pub bytes_probe: i64,
    pub packets_non_probe_primary: i64,
    pub bytes_non_probe_primary: i64,
    pub packets_non_probe_rtx: i64,
    pub bytes_non_probe_rtx: i64,
    pub is_completed: bool,
}

impl ProbeClusterResult {
    pub fn bytes(&self) -> i64 {
        self.bytes_probe
            .wrapping_add(self.bytes_non_probe_primary)
            .wrapping_add(self.bytes_non_probe_rtx)
    }

    pub fn duration(&self) -> Duration {
        Duration(self.end_time.wrapping_sub(self.start_time))
    }

    pub fn bitrate(&self) -> f64 {
        let duration = self.duration().seconds();
        if duration != 0.0 {
            return (self.bytes().wrapping_mul(8)) as f64 / duration;
        }

        0.0
    }
}

impl fmt::Debug for ProbeClusterResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProbeClusterResult")
            .field("start_time", &Time::from_unix_nano(self.start_time))
            .field("end_time", &Time::from_unix_nano(self.end_time))
            .field("duration", &self.duration())
            .field("packets_probe", &self.packets_probe)
            .field("bytes_probe", &self.bytes_probe)
            .field("packets_non_probe_primary", &self.packets_non_probe_primary)
            .field("bytes_non_probe_primary", &self.bytes_non_probe_primary)
            .field("packets_non_probe_rtx", &self.packets_non_probe_rtx)
            .field("bytes_non_probe_rtx", &self.bytes_non_probe_rtx)
            .field("bytes", &self.bytes())
            .field("bitrate", &self.bitrate())
            .field("is_completed", &self.is_completed)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProbeClusterInfo {
    pub id: ProbeClusterId,
    pub created_at: Time,
    pub goal: ProbeClusterGoal,
    pub result: ProbeClusterResult,
}

pub const PROBE_CLUSTER_INFO_INVALID: ProbeClusterInfo = ProbeClusterInfo {
    id: PROBE_CLUSTER_ID_INVALID,
    created_at: Time::ZERO,
    goal: ProbeClusterGoal {
        available_bandwidth_bps: 0,
        expected_usage_bps: 0,
        desired_bps: 0,
        duration: Duration::ZERO,
        desired_bytes: 0,
    },
    result: ProbeClusterResult {
        start_time: 0,
        end_time: 0,
        packets_probe: 0,
        bytes_probe: 0,
        packets_non_probe_primary: 0,
        bytes_non_probe_primary: 0,
        packets_non_probe_rtx: 0,
        bytes_non_probe_rtx: 0,
        is_completed: false,
    },
};

// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Bucket {
    expected_elapsed_duration: Duration,
    expected_probe_bytes_sent: i64,
}

// ---------------------------------------------------------------------------

pub struct Cluster {
    mode: ProbeClusterMode,
    listener: Option<Arc<dyn ProberListener>>,

    lock: RwLock<ClusterInner>,
}

// What `lock` guards. The sleep duration and the buckets are not written after `init_probes`,
// but are read with the rest.
struct ClusterInner {
    info: ProbeClusterInfo,

    base_sleep_duration: Duration,
    buckets: Vec<Bucket>,
    bucket_idx: usize,

    probe_bytes_sent: i64,

    start_time: Time,
    is_complete: bool,
}

impl Cluster {
    fn new(
        id: ProbeClusterId,
        mode: ProbeClusterMode,
        pcg: ProbeClusterGoal,
        listener: Option<Arc<dyn ProberListener>>,
    ) -> Arc<Cluster> {
        let mut c = Cluster {
            mode,
            listener,
            lock: RwLock::new(ClusterInner {
                info: ProbeClusterInfo {
                    id,
                    created_at: mono::now(),
                    goal: pcg,
                    ..Default::default()
                },
                base_sleep_duration: Duration::ZERO,
                buckets: Vec::new(),
                bucket_idx: 0,
                probe_bytes_sent: 0,
                start_time: Time::ZERO,
                is_complete: false,
            }),
        };
        c.init_probes();
        Arc::new(c)
    }

    fn init_probes(&mut self) {
        let mode = self.mode;
        let c = self.lock.get_mut();

        c.info.goal.desired_bytes =
            (c.info.goal.desired_bps as f64 * c.info.goal.duration.seconds() / 8.0 + 0.5).round()
                as i64;

        let mut num_buckets =
            (c.info.goal.duration.seconds() / C_SLEEP_DURATION.seconds() + 0.5).round() as i64;
        if num_buckets < 1 {
            num_buckets = 1;
        }
        let mut num_intervals = num_buckets;

        // for linear chirp, group intervals with decreasing duration, i.e. incraasing bitrate,
        // by aiming to send same number of bytes in each interval, as intervals get shorter, the bitrate is higher
        if mode == ProbeClusterMode::LinearChirp {
            let mut sum = 0i64;
            let mut i = 1i64;
            loop {
                sum += i;
                if sum >= num_buckets {
                    break;
                }
                i += 1;
            }
            num_buckets = i;
            num_intervals = sum;
        }

        c.base_sleep_duration = c.info.goal.duration / num_intervals;
        if c.base_sleep_duration < C_SLEEP_DURATION_MIN {
            c.base_sleep_duration = C_SLEEP_DURATION_MIN;
        }

        num_intervals =
            (c.info.goal.duration.seconds() / c.base_sleep_duration.seconds() + 0.5).round() as i64;
        let desired_probe_bytes_per_interval = (((c.info.goal.duration.seconds()
            * c.info
                .goal
                .desired_bps
                .wrapping_sub(c.info.goal.expected_usage_bps) as f64
            / 8.0)
            + num_intervals as f64
            - 1.0)
            / num_intervals as f64
            + 0.5)
            .round() as i64;

        c.buckets = vec![Bucket::default(); num_buckets as usize];
        for i in 0..num_buckets as usize {
            match mode {
                ProbeClusterMode::Uniform => {
                    c.buckets[i] = Bucket {
                        expected_elapsed_duration: c.base_sleep_duration,
                        ..Default::default()
                    };
                }

                ProbeClusterMode::LinearChirp => {
                    c.buckets[i] = Bucket {
                        expected_elapsed_duration: (num_buckets - i as i64) * c.base_sleep_duration,
                        ..Default::default()
                    };
                }
            }
            if i > 0 {
                let previous = c.buckets[i - 1].expected_elapsed_duration;
                c.buckets[i].expected_elapsed_duration += previous;
            }
            c.buckets[i].expected_probe_bytes_sent =
                (i as i64 + 1).wrapping_mul(desired_probe_bytes_per_interval);
        }
    }

    pub fn start(&self) {
        if let Some(listener) = &self.listener {
            let info = self.lock.read().info;
            listener.on_probe_cluster_switch(info);
        }
    }

    pub fn id(&self) -> ProbeClusterId {
        self.lock.read().info.id
    }

    pub fn info(&self) -> ProbeClusterInfo {
        let c = self.lock.read();

        c.info
    }

    pub fn probes_sent(&self, bytes_sent: i64) {
        let mut c = self.lock.write();

        c.probe_bytes_sent = c.probe_bytes_sent.wrapping_add(bytes_sent);
    }

    pub fn mark_completed(&self, result: ProbeClusterResult) {
        let mut c = self.lock.write();

        c.is_complete = true;
        c.info.result = result;
    }

    pub fn process(&self) -> Duration {
        let mut bytes_to_send = 0;
        {
            let mut c = self.lock.write();
            if c.is_complete {
                return Duration::ZERO;
            }

            if c.start_time.is_zero() {
                c.start_time = mono::now();
                bytes_to_send = C_BYTES_PER_PROBE;
            } else {
                let since_start = time::since(c.start_time);
                if since_start > c.buckets[c.bucket_idx].expected_elapsed_duration {
                    c.bucket_idx += 1;
                    let mut overflow = false;
                    if c.bucket_idx >= c.buckets.len() {
                        // when overflowing, repeat the last bucket
                        c.bucket_idx = c.buckets.len() - 1;
                        overflow = true;
                    }
                    if c.buckets[c.bucket_idx].expected_probe_bytes_sent > c.probe_bytes_sent
                        || overflow
                    {
                        bytes_to_send = std::cmp::max(
                            C_BYTES_PER_PROBE,
                            c.buckets[c.bucket_idx]
                                .expected_probe_bytes_sent
                                .wrapping_sub(c.probe_bytes_sent),
                        );
                    }
                }
            }
        }

        if bytes_to_send != 0
            && let Some(listener) = &self.listener
        {
            listener.on_send_probe(bytes_to_send);
        }

        C_SLEEP_DURATION_MIN
    }
}

impl fmt::Debug for Cluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let c = self.lock.read();
        f.debug_struct("Cluster")
            .field("mode", &format_args!("{}", self.mode))
            .field("info", &c.info)
            .field("base_sleep_duration", &c.base_sleep_duration)
            .field("num_buckets", &c.buckets.len())
            .field("bucket_idx", &c.bucket_idx)
            .field("probe_bytes_sent", &c.probe_bytes_sent)
            .field("start_time", &c.start_time)
            .field("elapsed", &time::since(c.start_time))
            .field("is_complete", &c.is_complete)
            .finish()
    }
}

// ----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;

    use super::*;
    use crate::testutils::eventually_async;

    #[derive(Default)]
    struct Listener {
        switches: Mutex<Vec<ProbeClusterInfo>>,
        probes: Mutex<Vec<i64>>,
    }

    impl ProberListener for Listener {
        fn on_probe_cluster_switch(&self, info: ProbeClusterInfo) {
            self.switches.lock().push(info);
        }

        fn on_send_probe(&self, bytes_to_send: i64) {
            self.probes.lock().push(bytes_to_send);
        }
    }

    // 400 kbps more than is in use, for 100 ms.
    fn goal() -> ProbeClusterGoal {
        ProbeClusterGoal {
            available_bandwidth_bps: 800_000,
            expected_usage_bps: 600_000,
            desired_bps: 1_000_000,
            duration: 100 * time::MILLISECOND,
            ..Default::default()
        }
    }

    fn buckets(c: &Cluster) -> Vec<(i64, i64)> {
        c.lock
            .read()
            .buckets
            .iter()
            .map(|b| {
                (
                    b.expected_elapsed_duration.nanoseconds(),
                    b.expected_probe_bytes_sent,
                )
            })
            .collect()
    }

    // New for musivum: the original has no tests. The expectations of the two tests of the
    // clusters are what the original makes of the same goals (checked by running it).
    #[test]
    fn a_uniform_cluster_for_a_goal() {
        let c = Cluster::new(ProbeClusterId(7), ProbeClusterMode::Uniform, goal(), None);
        let info = c.info();
        assert_eq!(c.id(), ProbeClusterId(7));
        assert_eq!(info.id, ProbeClusterId(7));
        assert!(!info.created_at.is_zero());
        assert_eq!(
            info.goal,
            ProbeClusterGoal {
                desired_bytes: 12501,
                ..goal()
            }
        );
        assert_eq!(info.result, ProbeClusterResult::default());

        assert_eq!(c.lock.read().base_sleep_duration, Duration(16_666_666));
        // six buckets of a sixth of the duration, each to send a seventh of the bytes
        assert_eq!(
            buckets(&c),
            vec![
                (16_666_666, 716),
                (33_333_332, 1432),
                (49_999_998, 2148),
                (66_666_664, 2864),
                (83_333_330, 3580),
                (99_999_996, 4296),
            ]
        );
    }

    // New for musivum.
    #[test]
    fn a_linear_chirp_cluster_for_a_goal() {
        let c = Cluster::new(
            ProbeClusterId(8),
            ProbeClusterMode::LinearChirp,
            goal(),
            None,
        );
        assert_eq!(c.info().goal.desired_bytes, 12501);
        assert_eq!(c.lock.read().base_sleep_duration, Duration(16_666_666));
        // the same bytes in shorter and shorter buckets: 3, 2 and 1 intervals long
        assert_eq!(
            buckets(&c),
            vec![(49_999_998, 716), (83_333_330, 1432), (99_999_996, 2148)]
        );

        // a goal shorter than an interval is still one bucket, of the shortest sleep
        let c = Cluster::new(
            ProbeClusterId(9),
            ProbeClusterMode::LinearChirp,
            ProbeClusterGoal {
                duration: 5 * time::MILLISECOND,
                ..goal()
            },
            None,
        );
        assert_eq!(c.info().goal.desired_bytes, 626);
        assert_eq!(buckets(&c), vec![(10_000_000, 251)]);
    }

    // New for musivum.
    #[test]
    fn a_cluster_asks_for_what_its_buckets_are_behind() {
        let listener = Arc::new(Listener::default());
        let c = Cluster::new(
            ProbeClusterId(1),
            ProbeClusterMode::Uniform,
            goal(),
            Some(listener.clone()),
        );
        c.start();
        assert_eq!(*listener.switches.lock(), vec![c.info()]);

        // the first probe starts the clock
        assert_eq!(c.process(), C_SLEEP_DURATION_MIN);
        assert_eq!(*listener.probes.lock(), vec![1100]);
        c.probes_sent(100);

        // in the second bucket 1432 bytes are expected
        std::thread::sleep((20 * time::MILLISECOND).to_std());
        assert_eq!(c.process(), C_SLEEP_DURATION_MIN);
        assert_eq!(*listener.probes.lock(), vec![1100, 1332]);

        // ahead of the buckets nothing is asked for
        c.probes_sent(5000);
        std::thread::sleep((90 * time::MILLISECOND).to_std());
        for _ in 0..4 {
            assert_eq!(c.process(), C_SLEEP_DURATION_MIN);
        }
        assert_eq!(*listener.probes.lock(), vec![1100, 1332]);
        assert_eq!(c.lock.read().bucket_idx, 5);

        // past the last bucket it is repeated, with at least a probe every time
        assert_eq!(c.process(), C_SLEEP_DURATION_MIN);
        assert_eq!(c.lock.read().bucket_idx, 5);
        assert_eq!(*listener.probes.lock(), vec![1100, 1332, 1100]);

        let result = ProbeClusterResult {
            start_time: 1_000_000_000,
            end_time: 1_100_000_000,
            packets_probe: 20,
            bytes_probe: 5000,
            bytes_non_probe_primary: 7000,
            bytes_non_probe_rtx: 500,
            is_completed: true,
            ..Default::default()
        };
        assert_eq!(result.bytes(), 12500);
        assert_eq!(result.duration(), 100 * time::MILLISECOND);
        assert_eq!(result.bitrate(), 1_000_000.0);
        assert_eq!(ProbeClusterResult::default().bitrate(), 0.0);

        c.mark_completed(result);
        assert_eq!(c.process(), Duration::ZERO);
        assert_eq!(listener.probes.lock().len(), 3);
        assert_eq!(c.info().result, result);

        let logged = format!("{c:?}");
        assert!(
            logged.contains("mode: UNIFORM, info: ProbeClusterInfo { id: 1, "),
            "{logged}"
        );
        assert!(
            logged.contains("bytes: 12500, bitrate: 1000000.0, is_completed: true"),
            "{logged}"
        );
        assert!(
            logged.contains("num_buckets: 6, bucket_idx: 5, probe_bytes_sent: 5100, "),
            "{logged}"
        );
        assert_eq!(ProbeClusterMode::LinearChirp.to_string(), "LINEAR_CHIRP");
    }

    // New for musivum.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_prober_runs_its_clusters_one_after_the_other() {
        let wait_for = std::time::Duration::from_secs(2);
        let tick = std::time::Duration::from_millis(2);

        let listener = Arc::new(Listener::default());
        let p = Prober::new(ProberParams {
            listener: Some(listener.clone()),
            ..Default::default()
        });
        assert!(!p.is_running());
        assert_eq!(p.get_active_cluster_id(), PROBE_CLUSTER_ID_INVALID);

        // nothing desired, no cluster
        assert_eq!(
            p.add_cluster(ProbeClusterMode::Uniform, ProbeClusterGoal::default()),
            PROBE_CLUSTER_INFO_INVALID
        );
        assert!(!p.is_running());

        let first = p.add_cluster(ProbeClusterMode::Uniform, goal());
        assert_eq!(first.id, ProbeClusterId(1));
        assert_eq!(first.goal.desired_bytes, 12501);
        assert!(p.is_running());

        // the worker makes it the active one and sends the first probe
        eventually_async(|| !listener.probes.lock().is_empty(), wait_for, tick).await;
        assert_eq!(*listener.switches.lock(), vec![first]);
        assert_eq!(listener.probes.lock()[0], 1100);
        assert_eq!(p.get_active_cluster_id(), first.id);
        p.probes_sent(1100);

        // a second one waits behind it
        let second = p.add_cluster(ProbeClusterMode::LinearChirp, goal());
        assert_eq!(second.id, ProbeClusterId(2));
        p.cluster_done(second);
        assert_eq!(p.get_active_cluster_id(), first.id);

        p.cluster_done(first);
        eventually_async(|| p.get_active_cluster_id() == second.id, wait_for, tick).await;
        assert_eq!(*listener.switches.lock(), vec![first, second]);
        assert!(p.is_running());

        // a reset drops what there is, and the worker ends
        p.reset(second);
        assert!(!p.is_running());
        assert_eq!(p.get_active_cluster_id(), PROBE_CLUSTER_ID_INVALID);
        time::sleep(50 * time::MILLISECOND).await;
        let probes = listener.probes.lock().len();
        time::sleep(50 * time::MILLISECOND).await;
        assert_eq!(listener.probes.lock().len(), probes);
        assert_eq!(listener.switches.lock().len(), 2);
    }
}
