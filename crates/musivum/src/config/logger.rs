// Ported from LiveKit's protocol, logger/config.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; only the struct, with the fields that YAML sets. Its
// lock, the observers of an update (`Update`, `AddUpdateObserver`), `ResolveComponentLevel`
// and `MarshalYAML` are left out: they belong to the logger that is set up from this, which
// `crate::protocol::logger` does not have yet. The struct is here, next to the
// configuration that holds it, until then.

use std::collections::HashMap;

use super::yaml::yaml_struct;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Config {
    /// `json` in YAML.
    pub json: bool,
    /// `level` in YAML.
    pub level: String,
    // true to enable log sampling, where the same log message and level will be throttled.
    // we have two layers of sampling
    // 1. global sampling - within a second, it will log the first SampleInitial, then every SampleInterval messages.
    // 2. per participant/track sampling - to be used with Logger.WithItemSampler(). This would be used to throttle
    //    the logs for a particular participant/track.
    /// `sample` in YAML.
    pub sample: bool,

    /// `component_levels` in YAML.
    pub component_levels: HashMap<String, String>,

    // global sampling per server
    // when sampling, the first N logs will be logged
    /// `sample_initial` in YAML.
    pub sample_initial: i64,
    // when sampling, every Mth log will be logged
    /// `sample_interval` in YAML.
    pub sample_interval: i64,

    // participant/track level sampling
    /// `item_sample_seconds` in YAML.
    pub item_sample_seconds: i64,
    /// `item_sample_initial` in YAML.
    pub item_sample_initial: i64,
    /// `item_sample_interval` in YAML.
    pub item_sample_interval: i64,
}

yaml_struct!(Config, "logger.Config", {
    json: "json",
    level: "level",
    sample: "sample",
    component_levels: "component_levels",
    sample_initial: "sample_initial",
    sample_interval: "sample_interval",
    item_sample_seconds: "item_sample_seconds",
    item_sample_initial: "item_sample_initial",
    item_sample_interval: "item_sample_interval",
});
