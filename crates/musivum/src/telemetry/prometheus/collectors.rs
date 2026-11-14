//! What the package counts into: a counter, a gauge, a histogram with fixed buckets, and a
//! set of one of these per combination of label values. All of them can be the value of a
//! `static`, count without a lock, and are read back with `get` or `snapshot`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

const ORDER: Ordering = Ordering::Relaxed;

// Adds to a float that is kept as its bits in an atomic.
fn add_f64(bits: &AtomicU64, v: f64) {
    let _ = bits.fetch_update(ORDER, ORDER, |old| {
        Some((f64::from_bits(old) + v).to_bits())
    });
}

/// A number that only goes up: Prometheus's `Counter`, for whole numbers.
#[derive(Debug, Default)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub const fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    pub fn inc(&self) {
        self.add(1);
    }

    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, ORDER);
    }

    pub fn get(&self) -> u64 {
        self.value.load(ORDER)
    }
}

/// A number that is set, or goes up and down: Prometheus's `Gauge`.
#[derive(Debug, Default)]
pub struct Gauge {
    bits: AtomicU64,
}

impl Gauge {
    pub const fn new() -> Self {
        Self {
            // the bits of 0.0
            bits: AtomicU64::new(0),
        }
    }

    pub fn set(&self, v: f64) {
        self.bits.store(v.to_bits(), ORDER);
    }

    pub fn add(&self, v: f64) {
        add_f64(&self.bits, v);
    }

    pub fn sub(&self, v: f64) {
        add_f64(&self.bits, -v);
    }

    pub fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(ORDER))
    }
}

/// Observations counted into `N` buckets with fixed upper bounds, and one more for what is
/// above the last: Prometheus's `Histogram`. An observation counts in the first bucket whose
/// bound it does not exceed.
#[derive(Debug)]
pub struct Histogram<const N: usize> {
    bounds: [f64; N],
    buckets: [AtomicU64; N],
    // above the last bound, or not a number
    above: AtomicU64,
    // the sum of all observations: the bits of a float
    sum: AtomicU64,
}

impl<const N: usize> Histogram<N> {
    /// `bounds` are the upper bounds of the buckets (upstream's `Buckets`), ascending: a
    /// `static` with bounds out of order does not compile.
    pub const fn new(bounds: [f64; N]) -> Self {
        let mut i = 1;
        while i < N {
            assert!(
                bounds[i - 1] < bounds[i],
                "the bounds of a histogram ascend"
            );
            i += 1;
        }
        Self {
            bounds,
            buckets: [const { AtomicU64::new(0) }; N],
            above: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, v: f64) {
        let bucket = match self.bounds.iter().position(|bound| v <= *bound) {
            Some(i) => &self.buckets[i],
            None => &self.above,
        };
        bucket.fetch_add(1, ORDER);
        add_f64(&self.sum, v);
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut count = 0;
        let mut buckets = Vec::with_capacity(N);
        for (bound, bucket) in self.bounds.iter().zip(&self.buckets) {
            count += bucket.load(ORDER);
            buckets.push((*bound, count));
        }
        count += self.above.load(ORDER);
        HistogramSnapshot {
            buckets,
            count,
            sum: f64::from_bits(self.sum.load(ORDER)),
        }
    }
}

/// What a [`Histogram`] held at one moment, in the form Prometheus reports it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HistogramSnapshot {
    /// Each upper bound with the number of observations that did not exceed it. The numbers
    /// are cumulative, as Prometheus's `le` buckets are.
    pub buckets: Vec<(f64, u64)>,
    /// The number of all observations: Prometheus's `+Inf` bucket.
    pub count: u64,
    /// The sum of all observations.
    pub sum: f64,
}

impl HistogramSnapshot {
    /// The number of observations that did not exceed the upper bound `le`, which must be
    /// one of the histogram's bounds.
    pub fn le(&self, le: f64) -> u64 {
        self.buckets
            .iter()
            .find(|(bound, _)| *bound == le)
            .map(|(_, count)| *count)
            .unwrap_or_else(|| panic!("the histogram has no bucket with the bound {le}"))
    }
}

/// One collector of the kind `M` for every combination of the values of `L` labels:
/// Prometheus's `CounterVec`, `GaugeVec` and `HistogramVec`. A collector is made when its
/// label values are first used.
pub struct MetricVec<M, const L: usize> {
    label_names: [&'static str; L],
    new: fn() -> M,
    children: RwLock<Vec<([String; L], Arc<M>)>>,
}

impl<M, const L: usize> MetricVec<M, L> {
    /// `new` makes the collector of one combination: `Counter::new`, `Gauge::new`, or
    /// `|| Histogram::new(BOUNDS)`.
    pub const fn new(label_names: [&'static str; L], new: fn() -> M) -> Self {
        Self {
            label_names,
            new,
            children: RwLock::new(Vec::new()),
        }
    }

    /// Prometheus's `WithLabelValues`: the collector for these values of the labels, in the
    /// order of the label names.
    pub fn with_label_values(&self, values: [&str; L]) -> Arc<M> {
        fn find<M, const L: usize>(
            children: &[([String; L], Arc<M>)],
            values: &[&str; L],
        ) -> Option<Arc<M>> {
            children
                .iter()
                .find(|(have, _)| have.iter().zip(values).all(|(a, b)| a == b))
                .map(|(_, child)| child.clone())
        }

        if let Some(child) = find(&self.children.read(), &values) {
            return child;
        }
        let mut children = self.children.write();
        if let Some(child) = find(&children, &values) {
            return child;
        }
        let child = Arc::new((self.new)());
        children.push((values.map(str::to_string), child.clone()));
        child
    }

    pub fn label_names(&self) -> &[&'static str; L] {
        &self.label_names
    }

    /// Every combination of label values used so far, with its collector.
    pub fn children(&self) -> Vec<([String; L], Arc<M>)> {
        self.children.read().clone()
    }
}

/// What a collector holds, read at one moment.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Counter(u64),
    Gauge(f64),
    Histogram(HistogramSnapshot),
}

/// A collector that can be read as a [`Value`]: [`Counter`], [`Gauge`], [`Histogram`].
pub trait Metric {
    fn value(&self) -> Value;
}

impl Metric for Counter {
    fn value(&self) -> Value {
        Value::Counter(self.get())
    }
}

impl Metric for Gauge {
    fn value(&self) -> Value {
        Value::Gauge(self.get())
    }
}

impl<const N: usize> Metric for Histogram<N> {
    fn value(&self) -> Value {
        Value::Histogram(self.snapshot())
    }
}

/// One metric as it is reported: its name, and a value for every combination of label
/// values (one value without label values for a collector that has no labels).
#[derive(Clone, Debug, PartialEq)]
pub struct MetricFamily {
    /// Prometheus's name of the metric: upstream's `Namespace`, `Subsystem` and `Name`
    /// joined by underscores.
    pub name: &'static str,
    /// Upstream's `Help`, empty where it has none.
    pub help: &'static str,
    pub label_names: Vec<&'static str>,
    pub samples: Vec<Sample>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    /// The values of the labels, in the order of `label_names`.
    pub label_values: Vec<String>,
    pub value: Value,
}

impl MetricFamily {
    /// A collector without labels.
    pub fn of(name: &'static str, help: &'static str, metric: &impl Metric) -> Self {
        Self {
            name,
            help,
            label_names: Vec::new(),
            samples: vec![Sample {
                label_values: Vec::new(),
                value: metric.value(),
            }],
        }
    }

    /// The collectors of a [`MetricVec`].
    pub fn of_vec<M: Metric, const L: usize>(
        name: &'static str,
        help: &'static str,
        vec: &MetricVec<M, L>,
    ) -> Self {
        Self {
            name,
            help,
            label_names: vec.label_names().to_vec(),
            samples: vec
                .children()
                .into_iter()
                .map(|(label_values, child)| Sample {
                    label_values: label_values.to_vec(),
                    value: child.value(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_counter_adds_up() {
        static COUNTER: Counter = Counter::new();
        assert_eq!(COUNTER.get(), 0);
        COUNTER.inc();
        COUNTER.add(41);
        assert_eq!(COUNTER.get(), 42);
        assert_eq!(COUNTER.value(), Value::Counter(42));
    }

    #[test]
    fn a_gauge_is_set_and_moved() {
        static GAUGE: Gauge = Gauge::new();
        assert_eq!(GAUGE.get(), 0.0);
        GAUGE.set(2.5);
        GAUGE.add(1.0);
        GAUGE.sub(4.0);
        assert_eq!(GAUGE.get(), -0.5);
        assert_eq!(GAUGE.value(), Value::Gauge(-0.5));
    }

    #[test]
    fn a_histogram_counts_an_observation_in_the_first_bucket_that_holds_it() {
        static HISTOGRAM: Histogram<3> = Histogram::new([1.0, 10.0, 100.0]);
        assert_eq!(
            HISTOGRAM.snapshot(),
            HistogramSnapshot {
                buckets: vec![(1.0, 0), (10.0, 0), (100.0, 0)],
                count: 0,
                sum: 0.0,
            }
        );

        // below the first bound, on a bound, between bounds, on the last bound, above it
        for v in [-3.0, 0.5, 1.0, 1.5, 10.0, 100.0, 100.5, 1e9] {
            HISTOGRAM.observe(v);
        }
        let snapshot = HISTOGRAM.snapshot();
        assert_eq!(snapshot.buckets, vec![(1.0, 3), (10.0, 5), (100.0, 6)]);
        assert_eq!(snapshot.count, 8);
        assert_eq!(snapshot.sum, 210.5 + 1e9);
        assert_eq!(snapshot.le(10.0), 5);

        // what is not a number counts, in no bucket
        HISTOGRAM.observe(f64::NAN);
        let snapshot = HISTOGRAM.snapshot();
        assert_eq!(snapshot.le(100.0), 6);
        assert_eq!(snapshot.count, 9);
    }

    #[test]
    #[should_panic(expected = "no bucket with the bound")]
    fn a_snapshot_is_asked_only_for_bounds_it_has() {
        Histogram::new([1.0, 2.0]).snapshot().le(1.5);
    }

    #[test]
    #[should_panic(expected = "ascend")]
    fn the_bounds_of_a_histogram_ascend() {
        // a `static` like this one does not compile
        let _ = Histogram::new(std::hint::black_box([1.0, 1.0]));
    }

    #[test]
    fn a_vec_has_one_collector_for_each_combination_of_label_values() {
        static VEC: MetricVec<Counter, 2> = MetricVec::new(["direction", "country"], Counter::new);
        VEC.with_label_values(["incoming", "nl"]).inc();
        VEC.with_label_values(["outgoing", "nl"]).add(5);
        VEC.with_label_values(["incoming", "nl"]).add(2);
        assert_eq!(VEC.with_label_values(["incoming", "nl"]).get(), 3);
        assert_eq!(VEC.with_label_values(["outgoing", "nl"]).get(), 5);
        // asking for a combination makes it
        assert_eq!(VEC.with_label_values(["incoming", ""]).get(), 0);
        assert_eq!(VEC.children().len(), 3);

        static HISTOGRAMS: MetricVec<Histogram<2>, 1> =
            MetricVec::new(["kind"], || Histogram::new([1.0, 2.0]));
        HISTOGRAMS.with_label_values(["video"]).observe(1.5);
        assert_eq!(
            HISTOGRAMS.with_label_values(["video"]).snapshot().buckets,
            vec![(1.0, 0), (2.0, 1)]
        );
    }

    #[test]
    fn a_vec_counts_from_many_threads() {
        static VEC: MetricVec<Counter, 1> = MetricVec::new(["direction"], Counter::new);
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for i in 0..1000 {
                        let direction = if i % 2 == 0 { "incoming" } else { "outgoing" };
                        VEC.with_label_values([direction]).inc();
                    }
                });
            }
        });
        assert_eq!(VEC.children().len(), 2);
        assert_eq!(VEC.with_label_values(["incoming"]).get(), 4000);
        assert_eq!(VEC.with_label_values(["outgoing"]).get(), 4000);
    }

    #[test]
    fn a_family_reports_every_combination() {
        let gauge = Gauge::new();
        gauge.set(7.0);
        assert_eq!(
            MetricFamily::of("livekit_x", "what x is", &gauge),
            MetricFamily {
                name: "livekit_x",
                help: "what x is",
                label_names: vec![],
                samples: vec![Sample {
                    label_values: vec![],
                    value: Value::Gauge(7.0),
                }],
            }
        );

        let vec: MetricVec<Counter, 1> = MetricVec::new(["direction"], Counter::new);
        assert!(
            MetricFamily::of_vec("livekit_y", "", &vec)
                .samples
                .is_empty()
        );
        vec.with_label_values(["incoming"]).add(2);
        vec.with_label_values(["outgoing"]).add(3);
        assert_eq!(
            MetricFamily::of_vec("livekit_y", "", &vec),
            MetricFamily {
                name: "livekit_y",
                help: "",
                label_names: vec!["direction"],
                samples: vec![
                    Sample {
                        label_values: vec!["incoming".to_string()],
                        value: Value::Counter(2),
                    },
                    Sample {
                        label_values: vec!["outgoing".to_string()],
                        value: Value::Counter(3),
                    },
                ],
            }
        );
    }
}
