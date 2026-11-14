// Ported from LiveKit, pkg/routing/selector/nodesfilter.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the nodes are copies (see utils.rs).

use super::{Error, get_available_nodes};
use crate::protocol::livekit;

pub fn filter_nodes_by_criteria(
    nodes: &[livekit::Node],
    criteria_threshold: f32,
    calculate_criteria_func: impl Fn(&livekit::Node) -> f32,
) -> Result<Vec<livekit::Node>, Error> {
    let nodes = get_available_nodes(nodes);
    if nodes.is_empty() {
        return Err(Error::NoAvailableNodes);
    }

    let filtered_nodes: Vec<livekit::Node> = nodes
        .iter()
        .filter(|node| calculate_criteria_func(node) < criteria_threshold)
        .cloned()
        .collect();
    if !filtered_nodes.is_empty() {
        return Ok(filtered_nodes);
    }
    Ok(nodes)
}
