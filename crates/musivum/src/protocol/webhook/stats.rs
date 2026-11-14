// Ported from LiveKit's protocol, webhook/stats.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The collectors are musivum's own
// (`crate::telemetry::prometheus`) and are statics, so `InitWebhookStats` with its `sync.Once`
// has no counterpart: the constant labels it takes are left to whoever serves the metrics, as
// those of LiveKit's other collectors are. `collect` is new: it hands the two collectors to
// `telemetry::prometheus::gather` under the names the original registers them with.

use crate::telemetry::prometheus::{Counter, Histogram, MetricFamily, MetricVec};

pub static PROM_WEBHOOK_DISPATCH_TOTAL: MetricVec<Counter, 2> =
    MetricVec::new(["status", "reason"], Counter::new);
pub static PROM_WEBHOOK_QUEUE_LENGTH_HISTOGRAM: Histogram<9> =
    Histogram::new([1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 40.0, 80.0]);

// Not here: InitWebhookStats(constLabels prometheus.Labels), see the header.

pub fn inc_dispatch_success() {
    PROM_WEBHOOK_DISPATCH_TOTAL
        .with_label_values(["success", ""])
        .inc();
}

pub fn inc_dispatch_failure() {
    PROM_WEBHOOK_DISPATCH_TOTAL
        .with_label_values(["failure", ""])
        .inc();
}

pub fn inc_dispatch_drop(reason: &str) {
    PROM_WEBHOOK_DISPATCH_TOTAL
        .with_label_values(["drop", reason])
        .inc();
}

pub fn record_queue_length(queue_length: usize) {
    PROM_WEBHOOK_QUEUE_LENGTH_HISTOGRAM.observe(queue_length as f64);
}

/// New: the collectors of this file under the names the original registers them with.
pub fn collect(families: &mut Vec<MetricFamily>) {
    families.push(MetricFamily::of_vec(
        "livekit_webhook_dispatch_total",
        "",
        &PROM_WEBHOOK_DISPATCH_TOTAL,
    ));
    families.push(MetricFamily::of(
        "livekit_webhook_queue_length",
        "",
        &PROM_WEBHOOK_QUEUE_LENGTH_HISTOGRAM,
    ));
}
