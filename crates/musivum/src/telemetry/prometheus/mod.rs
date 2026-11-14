//! The counters of LiveKit's `pkg/telemetry/prometheus`, without Prometheus.
//!
//! LiveKit's code reports what it does by calling functions of that package
//! (`prometheus.RecordForwardLatency(p90)`, `prometheus.IncrementKeyFrames(direction)`), which
//! write to collectors of the Prometheus client library. The ported code calls the same
//! functions here, under the same names in Rust's spelling. They write to collectors of
//! musivum's own ([`Counter`], [`Gauge`], [`Histogram`] and [`MetricVec`], written new): each
//! is a `static` that counts into atomics from the start of the process, so nothing has to
//! be initialized (there is no `Init`) and the library has no dependency for it. A test reads
//! a collector back directly (`PROM_FORWARD_LATENCY_HIST.snapshot()`); whoever serves
//! metrics reads all of them with [`gather`] and adds the labels of the node (`node_id`,
//! `node_type`), which upstream attaches to every collector.
//!
//! # Adding a function that ported code calls
//!
//! Only what is called so far is here. When the file you port calls another one, say
//! `prometheus.IncrementKeyFrames`:
//!
//! 1. Find it in upstream's `pkg/telemetry/prometheus`. It goes into the file of the same
//!    name here (`packets.go` is `packets.rs`). A new file gets the header of a ported file,
//!    with "only the functions that ported code calls" among its changes, is declared below
//!    (`mod rooms; pub use rooms::*;`) and gets a line in [`gather`].
//! 2. For every collector the function writes to, add a `pub static` under upstream's name
//!    in Rust's spelling (`promKeyFrameTotal` is `PROM_KEY_FRAME_TOTAL`), unless it is there
//!    already:
//!
//!    | upstream | here |
//!    |---|---|
//!    | `prometheus.Counter` | `Counter::new()` |
//!    | `prometheus.Gauge` | `Gauge::new()` |
//!    | `prometheus.Histogram` | `Histogram::new([...])` with upstream's `Buckets`, written out if upstream computes them |
//!    | `*prometheus.CounterVec`, `GaugeVec` | `MetricVec::new(["direction"], Counter::new)` with upstream's label names |
//!    | `*prometheus.HistogramVec` | `MetricVec::new(["direction"], \|\| Histogram::new([...]))` |
//!    | a plain `atomic.Uint64` of the package | a `pub(super) static` of `crate::go::atomic` |
//!
//! 3. Write the function with upstream's body. `promX.WithLabelValues(a, b).Add(float64(n))`
//!    is `PROM_X.with_label_values([a, b]).add(n)`; `Inc`, `Set`, `Sub` and `Observe` keep
//!    their names. A value that upstream turns into a label with `String()` or
//!    `strconv.FormatBool` is turned into the same text here. A check for a nil collector
//!    (`if promX == nil { return }`) has no counterpart.
//! 4. Add the static to the `collect` function of its file, with the name Prometheus gives
//!    it (upstream's `Namespace`, `Subsystem` and `Name` joined by underscores:
//!    `livekit_keyframe_total`) and upstream's `Help`.
//! 5. Test it in the file's `tests` module. The statics are shared by all tests of the
//!    crate, which run at the same time: compare what a collector holds before and after,
//!    and expect "at least", not "exactly". The collectors themselves are tested in
//!    `collectors.rs`; keep what is general there and what is LiveKit's out of it.

mod collectors;
mod node;
mod packets;
mod rooms;

pub use collectors::*;
pub use node::*;
pub use packets::*;
pub use rooms::*;

/// Every metric of the package as it stands now, for whoever serves them.
pub fn gather() -> Vec<MetricFamily> {
    let mut families = Vec::new();
    node::collect(&mut families);
    packets::collect(&mut families);
    rooms::collect(&mut families);
    families
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gather_has_every_metric_under_the_name_prometheus_gives_it() {
        record_forward_latency_sample(1);
        let families = gather();
        let names: Vec<&str> = families.iter().map(|family| family.name).collect();
        assert_eq!(
            names,
            [
                "livekit_node_service_operation",
                "livekit_keyframe_total",
                "livekit_forward_latency",
                "livekit_forward_latency_ns",
                "livekit_rtx_capped_total",
                "livekit_fec_total",
                "livekit_fec_bytes",
                "livekit_track_subscribe_counter",
                "livekit_pubsubtime_ms",
                "livekit_subscribe_pace_shadow_delay_ms",
                "livekit_peer_connection_state",
            ]
        );
        assert_eq!(families[1].label_names, ["direction"]);

        let histogram = &families[3];
        assert!(histogram.label_names.is_empty());
        assert_eq!(histogram.samples.len(), 1);
        let Value::Histogram(snapshot) = &histogram.samples[0].value else {
            panic!("{:?} is not a histogram", histogram.samples[0].value);
        };
        assert!(snapshot.count >= 1);
        // upstream's buckets: 50us, 100us, 250us, 500us, 1ms, 2ms, 3ms, 5ms, 10ms, 20ms
        let bounds: Vec<f64> = snapshot.buckets.iter().map(|(bound, _)| *bound).collect();
        assert_eq!(
            bounds,
            [50e3, 100e3, 250e3, 500e3, 1e6, 2e6, 3e6, 5e6, 10e6, 20e6]
        );
    }
}
