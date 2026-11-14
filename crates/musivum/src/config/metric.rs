// Ported from LiveKit, pkg/metric/metric_config.go, with the configuration structs of
// pkg/metric/metric_timestamper.go, metrics_collector.go and metrics_reporter.go, at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; only the configuration structs and their defaults.
// The rest of pkg/metric is not ported yet; the structs are here, next to the configuration
// that holds them, until then.

use crate::go::time::{self, Duration};

use super::yaml::yaml_struct;

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricConfig {
    /// `timestamper_config` in YAML.
    pub timestamper: MetricTimestamperConfig,
    /// `collector` in YAML.
    pub collector: MetricsCollectorConfig,
    /// `reporter` in YAML.
    pub reporter: MetricsReporterConfig,
}

pub const DEFAULT_METRIC_CONFIG: MetricConfig = MetricConfig {
    timestamper: DEFAULT_METRIC_TIMESTAMPER_CONFIG,
    collector: DEFAULT_METRICS_COLLECTOR_CONFIG,
    reporter: DEFAULT_METRICS_REPORTER_CONFIG,
};

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricTimestamperConfig {
    /// `one_way_delay_estimator_min_interval` in YAML.
    pub one_way_delay_estimator_min_interval: Duration,
    /// `one_way_delay_estimator_max_batch` in YAML.
    pub one_way_delay_estimator_max_batch: i64,
}

pub const DEFAULT_METRIC_TIMESTAMPER_CONFIG: MetricTimestamperConfig = MetricTimestamperConfig {
    one_way_delay_estimator_min_interval: Duration(5 * time::SECOND.0),
    one_way_delay_estimator_max_batch: 100,
};

// --------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricsCollectorConfig {
    /// `sampling_interval_ms` in YAML.
    pub sampling_interval_ms: u32,
    /// `batch_interval_ms` in YAML.
    pub batch_interval_ms: u32,
}

pub const DEFAULT_METRICS_COLLECTOR_CONFIG: MetricsCollectorConfig = MetricsCollectorConfig {
    sampling_interval_ms: 3 * 1000,
    batch_interval_ms: 10 * 1000,
};

// --------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricsReporterConfig {
    /// `reporting_interval_ms` in YAML.
    pub reporting_interval_ms: u32,
}

pub const DEFAULT_METRICS_REPORTER_CONFIG: MetricsReporterConfig = MetricsReporterConfig {
    reporting_interval_ms: 10 * 1000,
};

yaml_struct!(MetricConfig, "metric.MetricConfig", {
    timestamper: "timestamper_config",
    collector: "collector",
    reporter: "reporter",
});
yaml_struct!(MetricTimestamperConfig, "metric.MetricTimestamperConfig", {
    one_way_delay_estimator_min_interval: "one_way_delay_estimator_min_interval",
    one_way_delay_estimator_max_batch: "one_way_delay_estimator_max_batch",
});
yaml_struct!(MetricsCollectorConfig, "metric.MetricsCollectorConfig", {
    sampling_interval_ms: "sampling_interval_ms",
    batch_interval_ms: "batch_interval_ms",
});
yaml_struct!(MetricsReporterConfig, "metric.MetricsReporterConfig", {
    reporting_interval_ms: "reporting_interval_ms",
});
