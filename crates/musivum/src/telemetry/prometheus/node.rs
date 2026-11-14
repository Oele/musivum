// Ported from LiveKit, pkg/telemetry/prometheus/node.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the functions that ported code calls are here,
// more are added as they are needed. The collectors are musivum's own (see the module's
// description) and are statics, so there is no `initNodeStats` to call first; the constant
// labels `node_id` and `node_type` are left to whoever serves the metrics. The node's
// statistics, which the rest of the original file gathers for a cluster, are not ported.

use super::{Counter, MetricFamily, MetricVec};

pub static PROM_SERVICE_OPERATION_COUNTER: MetricVec<Counter, 3> =
    MetricVec::new(["type", "status", "error_type"], Counter::new);

// The collectors of this file under the names upstream registers them with.
pub(super) fn collect(families: &mut Vec<MetricFamily>) {
    families.push(MetricFamily::of_vec(
        "livekit_node_service_operation",
        "",
        &PROM_SERVICE_OPERATION_COUNTER,
    ));
}

pub fn record_service_operation_success(op: &str) {
    PROM_SERVICE_OPERATION_COUNTER
        .with_label_values([op, "success", ""])
        .add(1);
}

pub fn record_service_operation_error(op: &str, error: &str) {
    PROM_SERVICE_OPERATION_COUNTER
        .with_label_values([op, "error", error])
        .add(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_operations_count_by_outcome() {
        let count = |status: &str, error: &str| {
            PROM_SERVICE_OPERATION_COUNTER
                .with_label_values(["an operation of this test", status, error])
                .get()
        };
        let (success, error) = (count("success", ""), count("error", "write_message"));
        record_service_operation_success("an operation of this test");
        record_service_operation_error("an operation of this test", "write_message");
        record_service_operation_error("an operation of this test", "write_message");
        assert_eq!(success + 1, count("success", ""));
        assert_eq!(error + 2, count("error", "write_message"));
    }
}
