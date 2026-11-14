// Ported from LiveKit, pkg/routing/nodestats.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The configuration is pkg/config's `NodeStatsConfig`
// with its `DefaultNodeStatsConfig`, both here because musivum's configuration does not read
// `node_stats` (it is for the nodes of a cluster). The statistics come from
// `telemetry::prometheus::get_node_stats`, and their history is a
// ring of `Option`s, `None` where the original has nil. A configuration whose intervals give
// a history of no length panics on the first update, as the original does.

use parking_lot::RwLock;

use crate::errorw;
use crate::go::time::{self, Duration};
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::protocol::utils::hwstats;
use crate::telemetry::prometheus;

/// pkg/config's `NodeStatsConfig`.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeStatsConfig {
    /// `stats_update_interval` in YAML.
    pub stats_update_interval: Duration,
    /// `stats_rate_measurement_intervals` in YAML.
    pub stats_rate_measurement_intervals: Vec<Duration>,
    /// `stats_max_delay` in YAML.
    pub stats_max_delay: Duration,
}

/// pkg/config's `DefaultNodeStatsConfig`.
pub fn default_node_stats_config() -> NodeStatsConfig {
    NodeStatsConfig {
        stats_update_interval: 2 * time::SECOND,
        stats_rate_measurement_intervals: vec![10 * time::SECOND],
        stats_max_delay: 30 * time::SECOND,
    }
}

impl Default for NodeStatsConfig {
    fn default() -> Self {
        default_node_stats_config()
    }
}

pub struct NodeStats {
    started_at: i64,

    lock: RwLock<NodeStatsInner>,
}

struct NodeStatsInner {
    config: NodeStatsConfig,
    stats_history: Vec<Option<livekit::NodeStats>>,
    stats_history_write_ptr: usize,
}

impl NodeStats {
    pub fn new(conf: Option<&NodeStatsConfig>, started_at: i64) -> NodeStats {
        let n = NodeStats {
            started_at,
            lock: RwLock::new(NodeStatsInner {
                config: default_node_stats_config(),
                stats_history: Vec::new(),
                stats_history_write_ptr: 0,
            }),
        };
        n.update_config(conf);
        n
    }

    pub fn update_config(&self, conf: Option<&NodeStatsConfig>) {
        let mut inner = self.lock.write();

        let default_config = default_node_stats_config();
        let conf = conf.unwrap_or(&default_config);
        inner.config = conf.clone();

        // set up stats history to be able to measure different rate windows
        let mut max_interval = Duration::ZERO;
        for &rate_interval in &conf.stats_rate_measurement_intervals {
            if rate_interval > max_interval {
                max_interval = rate_interval;
            }
        }
        let len =
            (max_interval.0 + conf.stats_update_interval.0 - 1) / conf.stats_update_interval.0;
        inner.stats_history = vec![None; len as usize];
        inner.stats_history_write_ptr = 0;
    }

    pub fn update_and_get_node_stats(&self) -> Result<livekit::NodeStats, hwstats::Error> {
        let mut inner = self.lock.write();

        // append(n.statsHistory[n.statsHistoryWritePtr:], n.statsHistory[0:n.statsHistoryWritePtr]...)
        let ptr = inner.stats_history_write_ptr;
        let mut history = inner.stats_history[ptr..].to_vec();
        history.extend_from_slice(&inner.stats_history[..ptr]);

        let stats = match prometheus::get_node_stats(
            self.started_at,
            &history,
            &inner.config.stats_rate_measurement_intervals,
        ) {
            Ok(stats) => stats,
            Err(err) => {
                errorw!(logger::get_logger(), "could not update node stats", err);
                return Err(err);
            }
        };

        inner.stats_history[ptr] = Some(stats.clone());
        inner.stats_history_write_ptr = (ptr + 1) % inner.stats_history.len();
        Ok(stats)
    }

    pub fn get_latest_node_stats(&self) -> Option<livekit::NodeStats> {
        let inner = self.lock.read();

        let len = inner.stats_history.len();
        let latest_ptr = (inner.stats_history_write_ptr + len - 1) % len;
        inner.stats_history[latest_ptr].clone()
    }
}
