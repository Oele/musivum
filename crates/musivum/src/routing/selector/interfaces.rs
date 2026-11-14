// Ported from LiveKit, pkg/routing/selector/interfaces.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `ErrUnsupportedSelector` is in errors.rs. A node is
// taken and returned by value: the original returns the pointer it was given, here a copy.
// `CreateNodeSelector` takes the selector's configuration, `NodeSelectorConfig`, which is
// pkg/config's and is here because musivum's configuration does not read `node_selector`
// (it is for a cluster); it has no `Regions`. The kind "regionaware" gives
// `UnsupportedSelector`: the region aware selector (`regionaware.go`) is not ported, since
// one node has no other region to prefer.

use super::{AnySelector, CpuLoadSelector, Error, SystemLoadSelector};
use crate::protocol::livekit;
use crate::warnw;

// NodeSelector selects an appropriate node to run the current session
pub trait NodeSelector: Send + Sync {
    fn select_node(&self, nodes: &[livekit::Node]) -> Result<livekit::Node, Error>;
}

/// pkg/config's `NodeSelectorConfig`, without `Regions`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeSelectorConfig {
    /// `kind` in YAML.
    pub kind: String,
    /// `sort_by` in YAML.
    pub sort_by: String,
    /// `algorithm` in YAML.
    pub algorithm: String,
    /// `cpu_load_limit` in YAML.
    pub cpu_load_limit: f32,
    /// `sysload_limit` in YAML.
    pub sysload_limit: f32,
}

impl NodeSelectorConfig {
    /// The `NodeSelector` of pkg/config's `DefaultConfig`.
    pub fn default_config() -> NodeSelectorConfig {
        NodeSelectorConfig {
            kind: "any".to_string(),
            sort_by: "random".to_string(),
            sysload_limit: 0.9,
            cpu_load_limit: 0.9,
            algorithm: "lowest".to_string(),
        }
    }
}

pub fn create_node_selector(conf: &NodeSelectorConfig) -> Result<Box<dyn NodeSelector>, Error> {
    let mut kind = conf.kind.as_str();
    if kind.is_empty() {
        kind = "any";
    }
    match kind {
        "any" => Ok(Box::new(AnySelector {
            sort_by: conf.sort_by.clone(),
            algorithm: conf.algorithm.clone(),
        })),
        "cpuload" => Ok(Box::new(CpuLoadSelector {
            cpu_load_limit: conf.cpu_load_limit,
            sort_by: conf.sort_by.clone(),
            algorithm: conf.algorithm.clone(),
        })),
        "sysload" => Ok(Box::new(SystemLoadSelector {
            sysload_limit: conf.sysload_limit,
            sort_by: conf.sort_by.clone(),
            algorithm: conf.algorithm.clone(),
        })),
        // "regionaware": not ported, see the header
        "random" => {
            warnw!(
                crate::protocol::logger::get_logger(),
                "random node selector is deprecated, please switch to \"any\" or another selector",
                nil
            );
            Ok(Box::new(AnySelector {
                sort_by: conf.sort_by.clone(),
                algorithm: conf.algorithm.clone(),
            }))
        }
        _ => Err(Error::UnsupportedSelector),
    }
}
