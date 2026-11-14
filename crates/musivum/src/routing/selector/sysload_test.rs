// Ported from LiveKit, pkg/routing/selector/sysload_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The three nodes, package variables of the original
// that `cpuload_test.go` and `sortby_test.go` use too, are functions that make them (with the
// time they are made at, where Go takes the time the package is loaded). The original asks
// whether the selected node is the same pointer as `nodeLoadLow`; here it is compared by
// value, which tells the three nodes apart as well.

use crate::go::time;
use crate::protocol::livekit;
use crate::routing::selector::{self, NodeSelector};

pub(in crate::routing::selector) fn node_load_low() -> livekit::Node {
    livekit::Node {
        state: livekit::NodeState::Serving as i32,
        stats: Some(livekit::NodeStats {
            updated_at: time::now().unix_secs(),
            num_cpus: 1,
            cpu_load: 0.1,
            load_avg_last1min: 0.0,
            num_rooms: 1,
            num_clients: 2,
            num_tracks_in: 4,
            num_tracks_out: 8,
            rates: vec![livekit::NodeStatsRate {
                bytes_in: 1000.0,
                bytes_out: 2000.0,
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub(in crate::routing::selector) fn node_load_medium() -> livekit::Node {
    livekit::Node {
        state: livekit::NodeState::Serving as i32,
        stats: Some(livekit::NodeStats {
            updated_at: time::now().unix_secs(),
            num_cpus: 1,
            cpu_load: 0.5,
            load_avg_last1min: 0.5,
            num_rooms: 5,
            num_clients: 10,
            num_tracks_in: 20,
            num_tracks_out: 200,
            rates: vec![livekit::NodeStatsRate {
                bytes_in: 5000.0,
                bytes_out: 10000.0,
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub(in crate::routing::selector) fn node_load_high() -> livekit::Node {
    livekit::Node {
        state: livekit::NodeState::Serving as i32,
        stats: Some(livekit::NodeStats {
            updated_at: time::now().unix_secs(),
            num_cpus: 1,
            cpu_load: 0.99,
            load_avg_last1min: 2.0,
            num_rooms: 10,
            num_clients: 20,
            num_tracks_in: 40,
            num_tracks_out: 800,
            rates: vec![livekit::NodeStatsRate {
                bytes_in: 10000.0,
                bytes_out: 40000.0,
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn test_system_load_selector_select_node() {
    let sel = selector::SystemLoadSelector {
        sysload_limit: 1.0,
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
