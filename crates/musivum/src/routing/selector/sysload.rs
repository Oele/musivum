// Ported from LiveKit, pkg/routing/selector/sysload.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the nodes are copies (see utils.rs).

use super::{Error, NodeSelector, filter_nodes_by_criteria, get_node_sysload, select_sorted_node};
use crate::protocol::livekit;

// SystemLoadSelector eliminates nodes that surpass has a per-cpu node higher than SysloadLimit
// then selects a node from nodes that are not overloaded
#[derive(Clone, Debug, Default)]
pub struct SystemLoadSelector {
    pub sysload_limit: f32,
    pub sort_by: String,
    pub algorithm: String,
}

impl SystemLoadSelector {
    fn filter_nodes(&self, nodes: &[livekit::Node]) -> Result<Vec<livekit::Node>, Error> {
        let nodes = filter_nodes_by_criteria(nodes, self.sysload_limit, get_node_sysload)?;

        Ok(nodes)
    }
}

impl NodeSelector for SystemLoadSelector {
    fn select_node(&self, nodes: &[livekit::Node]) -> Result<livekit::Node, Error> {
        let mut nodes = self.filter_nodes(nodes)?;

        select_sorted_node(&mut nodes, &self.sort_by, &self.algorithm)
    }
}

#[cfg(test)]
#[path = "sysload_test.rs"]
pub(super) mod sysload_test;
