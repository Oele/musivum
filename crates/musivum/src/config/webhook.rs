// Ported from LiveKit's protocol, webhook/notifier.go, webhook/url_notifier.go and
// webhook/resource_url_notifier.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; only the configuration structs and their defaults.
// The notifiers that read them are not ported yet; the structs are here, next to the
// configuration that holds them, until then.

use crate::go::time::{self, Duration};

use super::yaml::yaml_struct;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WebHookConfig {
    /// `urls` in YAML.
    pub urls: Vec<String>,
    /// `api_key` in YAML.
    pub api_key: String,
    /// `url_notifier` in YAML.
    pub url_notifier: UrlNotifierConfig,
    /// `resource_url_notifier` in YAML.
    pub resource_url_notifier: ResourceUrlNotifierConfig,
    /// `filter_params` in YAML.
    pub filter_params: FilterParams,
}

pub const DEFAULT_WEB_HOOK_CONFIG: WebHookConfig = WebHookConfig {
    urls: Vec::new(),
    api_key: String::new(),
    url_notifier: DEFAULT_URL_NOTIFIER_CONFIG,
    resource_url_notifier: DEFAULT_RESOURCE_URL_NOTIFIER_CONFIG,
    filter_params: FilterParams {
        include_events: Vec::new(),
        exclude_events: Vec::new(),
    },
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FilterParams {
    /// `include_events` in YAML.
    pub include_events: Vec<String>,
    /// `exclude_events` in YAML.
    pub exclude_events: Vec<String>,
}

// ---------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UrlNotifierConfig {
    /// `num_workers` in YAML.
    pub num_workers: i64,
    /// `queue_size` in YAML.
    pub queue_size: i64,
}

pub const DEFAULT_URL_NOTIFIER_CONFIG: UrlNotifierConfig = UrlNotifierConfig {
    num_workers: 10,
    queue_size: 100,
};

// ---------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceUrlNotifierConfig {
    /// `max_age` in YAML.
    pub max_age: Duration,
    /// `max_depth` in YAML.
    pub max_depth: i64,
}

pub const DEFAULT_RESOURCE_URL_NOTIFIER_CONFIG: ResourceUrlNotifierConfig =
    ResourceUrlNotifierConfig {
        max_age: Duration(30 * time::SECOND.0),
        max_depth: 200,
    };

yaml_struct!(WebHookConfig, "webhook.WebHookConfig", {
    urls: "urls",
    api_key: "api_key",
    url_notifier: "url_notifier",
    resource_url_notifier: "resource_url_notifier",
    filter_params: "filter_params",
});
yaml_struct!(FilterParams, "webhook.FilterParams", {
    include_events: "include_events",
    exclude_events: "exclude_events",
});
yaml_struct!(UrlNotifierConfig, "webhook.URLNotifierConfig", {
    num_workers: "num_workers",
    queue_size: "queue_size",
});
yaml_struct!(ResourceUrlNotifierConfig, "webhook.ResourceURLNotifierConfig", {
    max_age: "max_age",
    max_depth: "max_depth",
});
