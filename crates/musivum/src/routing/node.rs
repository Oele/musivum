// Ported from LiveKit, pkg/routing/node.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `Clone()` is `clone_node()`: a method `clone` would be
// shadowed by `Arc`'s own wherever the node is held as an `Arc<dyn LocalNode>`. The node's
// statistics are `None` for a node made from a message, as nil in the original, and updating
// them there panics as it does. `NewLocalNode` returns an `Arc` (the node is shared by the
// router, the room manager and the signal server) and takes the configuration as an `Option`;
// it takes the statistics' configuration from musivum's configuration, which does not read
// `node_stats`, so the defaults are used (`DefaultNodeStatsConfig`, see nodestats.rs).
// `runtime.NumCPU()` is `hwstats::num_cpu()`. A node whose statistics are nil panics in
// `SecondsSinceNodeStatsUpdate`, as the original does.

use std::sync::Arc;

use parking_lot::RwLock;

use super::{Error, NodeStats, hwstats};
use crate::config::Config;
use crate::go::time::{self, Time};
use crate::protocol::livekit;
use crate::protocol::utils::guid;

#[cfg_attr(test, musivum_fakes::fake)]
pub trait LocalNode: Send + Sync {
    // Clone() of the original
    fn clone_node(&self) -> livekit::Node;
    fn set_node_id(&self, node_id: &livekit::NodeId);
    fn node_id(&self) -> livekit::NodeId;
    fn node_type(&self) -> livekit::NodeType;
    fn node_ip(&self) -> String;
    fn region(&self) -> String;
    fn set_state(&self, state: livekit::NodeState);
    fn set_stats(&self, stats: &livekit::NodeStats);
    fn update_node_stats(&self) -> bool;
    fn seconds_since_node_stats_update(&self) -> f64;
}

pub struct LocalNodeImpl {
    lock: RwLock<livekit::Node>,

    node_stats: Option<NodeStats>,
}

impl LocalNodeImpl {
    pub fn new(conf: Option<&Config>) -> Result<Arc<LocalNodeImpl>, Error> {
        let node_id = guid::new(guid::NODE_PREFIX);
        if let Some(conf) = conf
            && conf.rtc.rtc_config.node_ip.is_empty()
        {
            return Err(Error::IpNotSet);
        }
        let now_unix = time::now().unix_secs();
        let mut node = livekit::Node {
            id: node_id,
            num_cpus: hwstats::num_cpu() as u32,
            state: livekit::NodeState::Serving as i32,
            stats: Some(livekit::NodeStats {
                started_at: now_unix,
                updated_at: now_unix,
                ..Default::default()
            }),
            ..Default::default()
        };
        if let Some(conf) = conf {
            node.ip = conf.rtc.rtc_config.node_ip.primary_ip();
            node.region = conf.region.clone();
        }
        // `nsc = &conf.NodeStats`: not read by musivum's configuration, the defaults
        let node_stats = NodeStats::new(None, now_unix);

        Ok(Arc::new(LocalNodeImpl {
            lock: RwLock::new(node),
            node_stats: Some(node_stats),
        }))
    }

    pub fn new_from_node_proto(node: &livekit::Node) -> Result<Arc<LocalNodeImpl>, Error> {
        Ok(Arc::new(LocalNodeImpl {
            lock: RwLock::new(node.clone()),
            node_stats: None,
        }))
    }
}

impl LocalNode for LocalNodeImpl {
    fn clone_node(&self) -> livekit::Node {
        self.lock.read().clone()
    }

    // for testing only
    fn set_node_id(&self, node_id: &livekit::NodeId) {
        self.lock.write().id = node_id.to_string();
    }

    fn node_id(&self) -> livekit::NodeId {
        livekit::NodeId::from(&self.lock.read().id)
    }

    fn node_type(&self) -> livekit::NodeType {
        self.lock.read().r#type()
    }

    fn node_ip(&self) -> String {
        self.lock.read().ip.clone()
    }

    fn region(&self) -> String {
        self.lock.read().region.clone()
    }

    fn set_state(&self, state: livekit::NodeState) {
        self.lock.write().set_state(state);
    }

    // for testing only
    fn set_stats(&self, stats: &livekit::NodeStats) {
        self.lock.write().stats = Some(stats.clone());
    }

    fn update_node_stats(&self) -> bool {
        let mut node = self.lock.write();

        let node_stats = self
            .node_stats
            .as_ref()
            .expect("invalid memory address or nil pointer dereference");
        let Ok(stats) = node_stats.update_and_get_node_stats() else {
            return false;
        };

        node.stats = Some(stats);
        true
    }

    fn seconds_since_node_stats_update(&self) -> f64 {
        let node = self.lock.read();

        let stats = node
            .stats
            .as_ref()
            .expect("invalid memory address or nil pointer dereference");
        time::since(Time::unix(stats.updated_at, 0)).seconds()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::prometheus;

    // New for musivum: the original has no test of the local node. A node made from the
    // configuration has its address and region, and serves; its statistics, once updated,
    // count the rooms and participants the room manager has counted, and have the system's
    // CPUs and memory. Other tests of the crate count rooms and participants at the same
    // time, so what is compared is the difference, at least what this test added.
    #[test]
    fn a_local_nodes_stats_count_rooms_and_participants() {
        let mut conf = Config::default();
        assert_eq!(
            Err(Error::IpNotSet),
            LocalNodeImpl::new(Some(&conf)).map(|_| ())
        );
        conf.rtc.rtc_config.node_ip.v4 = "192.0.2.1".to_string();
        conf.region = "here".to_string();
        let node = LocalNodeImpl::new(Some(&conf)).unwrap();

        let n = node.clone_node();
        assert!(n.id.starts_with("ND_"), "{}", n.id);
        assert_eq!("192.0.2.1", node.node_ip());
        assert_eq!("here", node.region());
        assert_eq!(livekit::NodeState::Serving, n.state());
        assert_eq!(livekit::NodeType::Server, node.node_type());
        assert!(n.num_cpus >= 1);
        let started_at = n.stats.as_ref().unwrap().started_at;
        assert!(node.seconds_since_node_stats_update() < 5.0);

        {
            // the first update makes the memory getter, which probes the cgroup's files
            #[cfg(target_os = "linux")]
            let _paths = hwstats::TEST_PATHS_LOCK.lock();
            assert!(node.update_node_stats());
        }
        let before = node.clone_node().stats.unwrap();

        prometheus::room_started();
        prometheus::room_started();
        prometheus::add_participant();
        prometheus::add_participant();
        prometheus::add_participant();
        prometheus::sub_participant();
        assert!(node.update_node_stats());
        let after = node.clone_node().stats.unwrap();

        assert!(after.num_rooms - before.num_rooms >= 2);
        assert!(after.num_clients - before.num_clients >= 2);
        assert_eq!(started_at, after.started_at);
        assert!(after.updated_at >= before.updated_at);
        assert!(after.num_cpus >= 1);
        assert!((0.0..=1.0).contains(&after.cpu_load));
        #[cfg(target_os = "linux")]
        {
            assert!(after.memory_total > 0);
            assert!(after.memory_used <= after.memory_total);
        }
        // no rate yet: the history does not go back ten seconds
        assert!(after.rates.is_empty());

        prometheus::room_ended(time::now());
        prometheus::room_ended(time::now());
        prometheus::sub_participant();
        prometheus::sub_participant();

        node.set_state(livekit::NodeState::ShuttingDown);
        assert_eq!(livekit::NodeState::ShuttingDown, node.clone_node().state());
        node.set_node_id(&"ND_test".into());
        assert_eq!(livekit::NodeId::from("ND_test"), node.node_id());

        // a node made from a message has no statistics of its own to update
        let copy = LocalNodeImpl::new_from_node_proto(&node.clone_node()).unwrap();
        assert_eq!(node.clone_node(), copy.clone_node());
    }
}
