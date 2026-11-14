// Ported from LiveKit, pkg/routing/selector/sortby_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. There is no `sortby.go`: what this tests is
// `SelectSortedNode` of utils.go, through the system load selector. The nodes are those of
// sysload_test.rs, compared by value (see there). `SortByTest`, a helper whose name does not
// start with `Test`, is `sort_by_test`, not a test of its own.

use crate::routing::selector::sysload::sysload_test::{
    node_load_high, node_load_low, node_load_medium,
};
use crate::routing::selector::{self, NodeSelector};

fn sort_by_test(sort_by: &str) {
    let sel = selector::SystemLoadSelector {
        sort_by: sort_by.to_string(),
        algorithm: "lowest".to_string(),
        ..Default::default()
    };
    let low = node_load_low();
    let nodes = vec![low.clone(), node_load_medium(), node_load_high()];

    for _ in 0..5 {
        let node = sel.select_node(&nodes).unwrap();
        assert_eq!(low, node, "selected the wrong node for SortBy: {sort_by}");
    }
}

#[test]
fn test_sort_by_errors() {
    let mut sel = selector::SystemLoadSelector {
        algorithm: "lowest".to_string(),
        ..Default::default()
    };
    let nodes = vec![node_load_low(), node_load_medium(), node_load_high()];

    // Test unset sort by option error
    let res = sel.select_node(&nodes);
    assert_eq!(
        Err(selector::Error::SortByNotSet),
        res,
        "shouldn't allow empty sortBy"
    );

    // Test unknown sort by option error
    sel.sort_by = "testFail".to_string();
    let res = sel.select_node(&nodes);
    assert_eq!(
        Err(selector::Error::SortByUnknown),
        res,
        "shouldn't allow unknown sortBy"
    );
}

#[test]
fn test_sort_by() {
    let sort_by_tests = [
        "sysload",
        "cpuload",
        "rooms",
        "clients",
        "tracks",
        "bytespersec",
    ];

    for sort_by in sort_by_tests {
        sort_by_test(sort_by);
    }
}
