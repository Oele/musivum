// Ported from LiveKit, pkg/routing/selector/cpuload.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the nodes are copies (see utils.rs). A node without
// statistics panics, as in the original (see `stats` in utils.rs).

use super::{Error, NodeSelector, filter_nodes_by_criteria, select_sorted_node};
use crate::protocol::livekit;

// CPULoadSelector eliminates nodes that have CPU usage higher than CPULoadLimit
// then selects a node from nodes that are not overloaded
#[derive(Clone, Debug, Default)]
pub struct CpuLoadSelector {
    pub cpu_load_limit: f32,
    pub sort_by: String,
    pub algorithm: String,
}

impl CpuLoadSelector {
    fn filter_nodes(&self, nodes: &[livekit::Node]) -> Result<Vec<livekit::Node>, Error> {
        let nodes = filter_nodes_by_criteria(nodes, self.cpu_load_limit, |node| {
            node.stats
                .as_ref()
                .expect("invalid memory address or nil pointer dereference")
                .cpu_load
        })?;

        Ok(nodes)
    }
}

impl NodeSelector for CpuLoadSelector {
    fn select_node(&self, nodes: &[livekit::Node]) -> Result<livekit::Node, Error> {
        let mut nodes = self.filter_nodes(nodes)?;

        select_sorted_node(&mut nodes, &self.sort_by, &self.algorithm)
    }
}

#[cfg(test)]
#[path = "cpuload_test.rs"]
mod cpuload_test;
