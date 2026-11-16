// Ported from LiveKit, pkg/routing/selector/utils.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Nodes are taken as a slice of nodes and a node is
// returned as a copy, where the original passes pointers. `SelectSortedNode` sorts the slice
// it is given in place, as the original does; Rust's sort is stable, Go's `slices.SortFunc`
// is so only for slices of up to twelve nodes (it sorts those by insertion), so nodes that
// compare equal may come out in another order in a longer slice. A node without statistics
// that is sorted by them panics, as the original does when it reads through the nil pointer.
// `funk.RandomInt` and `rand.Perm` are the `rand` crate's.

use std::cmp::Ordering;

use rand::RngExt;

use super::Error;
use crate::config::LimitConfig;
use crate::go::time;
use crate::protocol::livekit;
use crate::utils::signum;

pub const AVAILABLE_SECONDS: i64 = 5;

// checks if a node has been updated recently to be considered for selection
pub fn is_available(node: &livekit::Node) -> bool {
    let Some(stats) = &node.stats else {
        // available till stats are available
        return true;
    };

    let delta = time::now().unix_secs().wrapping_sub(stats.updated_at);
    delta < AVAILABLE_SECONDS
}

pub fn get_available_nodes(nodes: &[livekit::Node]) -> Vec<livekit::Node> {
    nodes
        .iter()
        .filter(|node| is_available(node) && node.state() == livekit::NodeState::Serving)
        .cloned()
        .collect()
}

// `node.Stats` read through, which panics in Go when it is nil.
fn stats(node: &livekit::Node) -> &livekit::NodeStats {
    node.stats
        .as_ref()
        .expect("invalid memory address or nil pointer dereference")
}

pub fn get_node_sysload(node: &livekit::Node) -> f32 {
    let stats = stats(node);
    let mut num_cpus = stats.num_cpus;
    if num_cpus == 0 {
        num_cpus = 1;
    }
    stats.load_avg_last1min / num_cpus as f32
}

// TODO: check remote node configured limit, instead of this node's config
#[allow(clippy::needless_bool)] // the returns as the original has them
pub fn limits_reached(limit_config: &LimitConfig, node_stats: Option<&livekit::NodeStats>) -> bool {
    let Some(node_stats) = node_stats else {
        return false;
    };

    if limit_config.num_tracks > 0
        && limit_config.num_tracks
            <= node_stats
                .num_tracks_in
                .wrapping_add(node_stats.num_tracks_out)
    {
        return true;
    }

    let rate = node_stats.rates.first().cloned().unwrap_or_default();
    if limit_config.bytes_per_sec > 0.0
        && limit_config.bytes_per_sec <= rate.bytes_in + rate.bytes_out
    {
        return true;
    }

    false
}

pub fn select_sorted_node(
    nodes: &mut [livekit::Node],
    sort_by: &str,
    algorithm: &str,
) -> Result<livekit::Node, Error> {
    if sort_by.is_empty() {
        return Err(Error::SortByNotSet);
    }
    if algorithm.is_empty() {
        return Err(Error::AlgorithmNotSet);
    }

    match algorithm {
        // examine all nodes and select the lowest based on sort criteria
        "lowest" => select_lowest_sorted_node(nodes, sort_by),
        // randomly select two nodes and return the lowest based on sort criteria "Power of Two Random Choices"
        "twochoice" => select_two_choice_sorted_node(nodes, sort_by),
        _ => Err(Error::AlgorithmUnknown),
    }
}

fn select_two_choice_sorted_node(
    nodes: &mut [livekit::Node],
    sort_by: &str,
) -> Result<livekit::Node, Error> {
    if nodes.len() <= 2 {
        return select_lowest_sorted_node(nodes, sort_by);
    }

    // randomly select two nodes
    let (node1, node2) = select_two_random_nodes(nodes)?;

    // compare the two nodes based on the sort criteria (the original's check for a nil node,
    // which `selectTwoRandomNodes` never returns, has no counterpart)
    let selected_node = select_lowest_sorted_node(&mut [node1, node2], sort_by)?;

    Ok(selected_node)
}

// Go's `utils.Signum(x)` as the comparison `slices.SortFunc` takes.
fn sign<T: crate::utils::SignedNumber>(x: T) -> Ordering {
    signum(x).cmp(&0)
}

fn select_lowest_sorted_node(
    nodes: &mut [livekit::Node],
    sort_by: &str,
) -> Result<livekit::Node, Error> {
    // Return a node based on what it should be sorted by for priority
    match sort_by {
        "random" => {
            let idx = rand::rng().random_range(0..nodes.len());
            Ok(nodes[idx].clone())
        }
        "sysload" => {
            nodes.sort_by(|a, b| sign(get_node_sysload(a) - get_node_sysload(b)));
            Ok(nodes[0].clone())
        }
        "cpuload" => {
            nodes.sort_by(|a, b| sign(stats(a).cpu_load - stats(b).cpu_load));
            Ok(nodes[0].clone())
        }
        "rooms" => {
            nodes.sort_by(|a, b| sign(stats(a).num_rooms.wrapping_sub(stats(b).num_rooms)));
            Ok(nodes[0].clone())
        }
        "clients" => {
            nodes.sort_by(|a, b| sign(stats(a).num_clients.wrapping_sub(stats(b).num_clients)));
            Ok(nodes[0].clone())
        }
        "tracks" => {
            nodes.sort_by(|a, b| {
                let (a, b) = (stats(a), stats(b));
                sign(
                    a.num_tracks_in
                        .wrapping_add(a.num_tracks_out)
                        .wrapping_sub(b.num_tracks_in.wrapping_add(b.num_tracks_out)),
                )
            });
            Ok(nodes[0].clone())
        }
        "bytespersec" => {
            nodes.sort_by(|a, b| {
                let ratea = stats(a).rates.first().cloned().unwrap_or_default();
                let rateb = stats(b).rates.first().cloned().unwrap_or_default();
                sign((ratea.bytes_in + ratea.bytes_out) - (rateb.bytes_in + rateb.bytes_out))
            });
            Ok(nodes[0].clone())
        }
        _ => Err(Error::SortByUnknown),
    }
}

fn select_two_random_nodes(
    nodes: &[livekit::Node],
) -> Result<(livekit::Node, livekit::Node), Error> {
    if nodes.len() < 2 {
        return Err(Error::NoAvailableNodes);
    }

    // the first two of `rand.Perm(len(nodes))`: two different indices
    let shuffled_indices = rand::seq::index::sample(&mut rand::rng(), nodes.len(), 2);

    Ok((
        nodes[shuffled_indices.index(0)].clone(),
        nodes[shuffled_indices.index(1)].clone(),
    ))
}

#[cfg(test)]
#[path = "utils_test.rs"]
mod utils_test;
