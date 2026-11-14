// Ported from LiveKit, pkg/routing/selector/cpuload_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the nodes are those of sysload_test.rs, compared by
// value (see there).

use crate::protocol::livekit;
use crate::routing::selector::sysload::sysload_test::{node_load_high, node_load_low};
use crate::routing::selector::{self, NodeSelector};

#[test]
fn test_cpu_load_selector_select_node() {
    let sel = selector::CpuLoadSelector {
        cpu_load_limit: 0.8,
        sort_by: "random".to_string(),
        algorithm: "lowest".to_string(),
    };

    let nodes: Vec<livekit::Node> = Vec::new();
    let res = sel.select_node(&nodes);
    assert!(res.is_err(), "should error no available nodes");

    // Select a node with high load when no nodes with low load are available
    let nodes = vec![node_load_high()];
    if let Err(err) = sel.select_node(&nodes) {
        panic!("{err}");
    }

    // Select a node with low load when available
    let low = node_load_low();
    let nodes = vec![low.clone(), node_load_high()];
    for _ in 0..5 {
        let node = sel.select_node(&nodes).unwrap();
        assert_eq!(low, node, "selected the wrong node");
    }
}
