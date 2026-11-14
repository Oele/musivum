// Ported from LiveKit's protocol, webhook/notifier.go, webhook/url_notifier.go and
// webhook/resource_url_notifier.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The configuration structs and their defaults are in
// `crate::protocol::webhook` with the notifiers that read them, as upstream; this module
// re-exports them under the names the configuration has used so far, and has their reading
// from YAML (the struct tags of the original), which is the configuration's.

pub use crate::protocol::webhook::{
    DEFAULT_RESOURCE_URL_NOTIFIER_CONFIG, DEFAULT_URL_NOTIFIER_CONFIG, DEFAULT_WEB_HOOK_CONFIG,
    FilterParams, ResourceUrlNotifierConfig, UrlNotifierConfig, WebHookConfig,
};

use super::yaml::yaml_struct;

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
