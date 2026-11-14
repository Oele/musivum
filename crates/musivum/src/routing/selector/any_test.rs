// Ported from LiveKit, pkg/routing/selector/any_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the tables are loops over arrays of structs, with the
// name of the case in the messages. `require.Nil(t, node)` after an error has no
// counterpart: the result is an error or a node.

use crate::go::time;
use crate::protocol::livekit;
use crate::routing::selector::{AnySelector, NodeSelector, get_available_nodes};

fn create_test_node(
    id: &str,
    cpu_load: f32,
    num_rooms: i32,
    num_clients: i32,
    state: livekit::NodeState,
) -> livekit::Node {
    livekit::Node {
        id: id.to_string(),
        state: state as i32,
        stats: Some(livekit::NodeStats {
            updated_at: time::now().unix_secs() - 1, // Recent update to be considered available
            cpu_load,
            num_rooms,
            num_clients,
            num_cpus: 4,
            load_avg_last1min: cpu_load * 4.0, // Simulate system load
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn test_any_selector_select_node_two_choice() {
    struct Test {
        name: &'static str,
        sort_by: &'static str,
        algorithm: &'static str,
        nodes: Vec<livekit::Node>,
        want_err: &'static str,
        expected: &'static str,
        not_expected: &'static str,
    }
    let tests = vec![
        Test {
            name: "successful selection with cpuload sorting",
            sort_by: "cpuload",
            algorithm: "twochoice",
            nodes: vec![
                create_test_node("node1", 0.8, 5, 10, livekit::NodeState::Serving),
                create_test_node("node2", 0.3, 2, 5, livekit::NodeState::Serving),
                create_test_node("node3", 0.6, 3, 8, livekit::NodeState::Serving),
                create_test_node("node4", 0.9, 6, 12, livekit::NodeState::Serving),
            ],
            want_err: "",
            expected: "", // Not deterministic selection, so no specific expected node
            not_expected: "node4", // Node with highest load should not be selected
        },
        Test {
            name: "successful selection with rooms sorting",
            sort_by: "rooms",
            algorithm: "twochoice",
            nodes: vec![
                create_test_node("node1", 0.5, 8, 15, livekit::NodeState::Serving),
                create_test_node("node2", 0.4, 2, 5, livekit::NodeState::Serving),
                create_test_node("node3", 0.6, 12, 20, livekit::NodeState::Serving),
            ],
            want_err: "",
            expected: "", // Not deterministic selection, so no specific expected node
            not_expected: "node3", // Node with highest room count should not be selected
        },
        Test {
            name: "successful selection with clients sorting",
            sort_by: "clients",
            algorithm: "twochoice",
            nodes: vec![
                create_test_node("node1", 0.5, 3, 25, livekit::NodeState::Serving),
                create_test_node("node2", 0.4, 2, 5, livekit::NodeState::Serving),
                create_test_node("node3", 0.6, 4, 30, livekit::NodeState::Serving),
            ],
            want_err: "",
            expected: "", // Not deterministic selection, so no specific expected node
            not_expected: "node3", // Node with highest clients should not be selected
        },
        Test {
            name: "empty nodes list",
            sort_by: "cpuload",
            algorithm: "twochoice",
            nodes: vec![],
            want_err: "could not find any available nodes",
            expected: "",
            not_expected: "",
        },
        Test {
            name: "no available nodes - all unavailable",
            sort_by: "cpuload",
            algorithm: "twochoice",
            nodes: vec![livekit::Node {
                id: "node1".to_string(),
                state: livekit::NodeState::Serving as i32,
                stats: Some(livekit::NodeStats {
                    updated_at: time::now().unix_secs() - 10, // Too old
                    cpu_load: 0.3,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            want_err: "could not find any available nodes",
            expected: "",
            not_expected: "",
        },
        Test {
            name: "no available nodes - not serving",
            sort_by: "cpuload",
            algorithm: "twochoice",
            nodes: vec![livekit::Node {
                id: "node1".to_string(),
                state: livekit::NodeState::ShuttingDown as i32,
                stats: Some(livekit::NodeStats {
                    updated_at: time::now().unix_secs() - 1,
                    cpu_load: 0.3,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            want_err: "could not find any available nodes",
            expected: "",
            not_expected: "",
        },
        Test {
            name: "single available node",
            sort_by: "cpuload",
            algorithm: "twochoice",
            nodes: vec![create_test_node(
                "node1",
                0.5,
                3,
                10,
                livekit::NodeState::Serving,
            )],
            want_err: "",
            expected: "node1", // Should select the only available node
            not_expected: "",  // No other nodes to compare against
        },
        Test {
            name: "two available nodes",
            sort_by: "cpuload",
            algorithm: "twochoice",
            nodes: vec![
                create_test_node("node1", 0.8, 5, 15, livekit::NodeState::Serving),
                create_test_node("node2", 0.3, 2, 5, livekit::NodeState::Serving),
            ],
            want_err: "",
            expected: "node2",     // Should select the node with lower load
            not_expected: "node1", // Should not select the node with higher load
        },
    ];

    for tt in tests {
        let selector = AnySelector {
            sort_by: tt.sort_by.to_string(),
            algorithm: tt.algorithm.to_string(),
        };

        let res = selector.select_node(&tt.nodes);

        if !tt.want_err.is_empty() {
            let err = res.expect_err(tt.name);
            assert!(err.to_string().contains(tt.want_err), "{}: {err}", tt.name);
        } else {
            let node = res.expect(tt.name);
            assert!(!node.id.is_empty(), "{}", tt.name);

            // Verify the selected node is one of the available nodes
            let mut found = false;
            let available_nodes = get_available_nodes(&tt.nodes);
            for available_node in &available_nodes {
                if available_node.id == node.id {
                    found = true;
                    break;
                }
            }
            assert!(
                found,
                "{}: Selected node should be one of the available nodes",
                tt.name
            );

            if !tt.expected.is_empty() {
                assert_eq!(
                    tt.expected, node.id,
                    "{}: Selected node should match expected",
                    tt.name
                );
            }
            if !tt.not_expected.is_empty() {
                assert_ne!(
                    tt.not_expected, node.id,
                    "{}: Selected node should not match not expected",
                    tt.name
                );
            }
        }
    }
}

#[test]
fn test_any_selector_select_node_two_choice_probabilistic_behavior() {
    // Test that two-choice algorithm favors nodes with lower metrics
    // This test runs multiple iterations to increase confidence in the probabilistic behavior
    let selector = AnySelector {
        sort_by: "cpuload".to_string(),
        algorithm: "twochoice".to_string(),
    };

    // Create nodes where node2 has significantly lower CPU load
    let nodes = vec![
        create_test_node("node1", 0.95, 10, 20, livekit::NodeState::Serving), // Very high load
        create_test_node("node2", 0.1, 1, 2, livekit::NodeState::Serving),    // Low load
        create_test_node("node3", 0.5, 9, 18, livekit::NodeState::Serving),   // Medium load
        create_test_node("node4", 0.85, 8, 16, livekit::NodeState::Serving),  // High load
    ];

    // Run multiple selections and count how often the low-load node is selected
    let iterations = 1000;
    let mut low_load_selections = 0;
    let mut higest_load_selections = 0;

    for _ in 0..iterations {
        let node = selector.select_node(&nodes).unwrap();

        if node.id == "node2" {
            low_load_selections += 1;
        }
        if node.id == "node1" {
            higest_load_selections += 1;
        }
    }

    // The low-load node should be selected more often than pure random (25%)
    // Due to the two-choice algorithm favoring the better node
    let selection_rate = low_load_selections as f64 / iterations as f64;
    assert!(
        selection_rate > 0.4,
        "Two-choice algorithm should favor the low-load node more than random selection"
    );
    assert_eq!(
        higest_load_selections, 0,
        "Two-choice algorithm should never favor the highest load node"
    );
}

#[test]
fn test_any_selector_select_node_invalid_parameters() {
    let nodes = vec![create_test_node(
        "node1",
        0.5,
        3,
        10,
        livekit::NodeState::Serving,
    )];

    struct Test {
        name: &'static str,
        sort_by: &'static str,
        algorithm: &'static str,
        want_err: &'static str,
    }
    let tests = [
        Test {
            name: "empty sortBy",
            sort_by: "",
            algorithm: "twochoice",
            want_err: "sort by option cannot be blank",
        },
        Test {
            name: "empty algorithm",
            sort_by: "cpuload",
            algorithm: "",
            want_err: "node selector algorithm option cannot be blank",
        },
        Test {
            name: "unknown sortBy",
            sort_by: "invalid",
            algorithm: "twochoice",
            want_err: "unknown sort by option",
        },
        Test {
            name: "unknown algorithm",
            sort_by: "cpuload",
            algorithm: "invalid",
            want_err: "unknown node selector algorithm option",
        },
    ];

    for tt in tests {
        let selector = AnySelector {
            sort_by: tt.sort_by.to_string(),
            algorithm: tt.algorithm.to_string(),
        };

        let err = selector.select_node(&nodes).expect_err(tt.name);

        assert!(err.to_string().contains(tt.want_err), "{}: {err}", tt.name);
    }
}
