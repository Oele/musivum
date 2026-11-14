// Ported from LiveKit, pkg/routing/selector/any.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the nodes are copies (see utils.rs).

use super::{Error, NodeSelector, get_available_nodes, select_sorted_node};
use crate::protocol::livekit;

// AnySelector selects any available node with no limitations
#[derive(Clone, Debug, Default)]
pub struct AnySelector {
    pub sort_by: String,
    pub algorithm: String,
}

impl NodeSelector for AnySelector {
    fn select_node(&self, nodes: &[livekit::Node]) -> Result<livekit::Node, Error> {
        let mut nodes = get_available_nodes(nodes);
        if nodes.is_empty() {
            return Err(Error::NoAvailableNodes);
        }

        select_sorted_node(&mut nodes, &self.sort_by, &self.algorithm)
    }
}

#[cfg(test)]
#[path = "any_test.rs"]
mod any_test;
