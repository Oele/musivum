// Ported from LiveKit, pkg/clientconfiguration/conf.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the expression of a rule, which the original hands to
// its script compiler as a string, is here the same text as Rust code behind `|c|`
// (`script_match!` in match.rs says why that gives the same), so `must.Get` has nothing to
// check; the table is made when it is first used.

use std::sync::{Arc, LazyLock};

use super::ConfigurationItem;
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;
use crate::script_match;

/// StaticConfigurations list specific device-side limitations that should be disabled at a global level
pub static STATIC_CONFIGURATIONS: LazyLock<Vec<ConfigurationItem>> = LazyLock::new(|| {
    vec![
        // {
        // 	Match:         must.Get(NewScriptMatch(`c.protocol <= 5 || c.browser == "firefox"`)),
        // 	Configuration: &livekit.ClientConfiguration{ResumeConnection: livekit.ClientConfigSetting_DISABLED},
        // 	Merge:         false,
        // },
        ConfigurationItem {
            r#match: Arc::new(script_match!(|c| c.browser == "safari")),
            configuration: Some(livekit::ClientConfiguration {
                disabled_codecs: Some(livekit::DisabledCodecs {
                    codecs: vec![livekit::Codec {
                        mime: MimeType::Av1.to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            merge: true,
        },
        ConfigurationItem {
            r#match: Arc::new(script_match!(
                |c| c.browser == "safari" && c.browser_version > "18.3"
            )),
            configuration: Some(livekit::ClientConfiguration {
                disabled_codecs: Some(livekit::DisabledCodecs {
                    publish: vec![livekit::Codec {
                        mime: MimeType::Vp9.to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            merge: true,
        },
        // Firefox enabled AV1 for WebRTC by default since 155, but we got reports of issues with AV1 decoding.
        // It does not publish AV1 either (no SVC encoding), so disable it in both directions.
        ConfigurationItem {
            r#match: Arc::new(script_match!(
                |c| c.browser == "firefox" || c.browser == "firefox mobile"
            )),
            configuration: Some(livekit::ClientConfiguration {
                disabled_codecs: Some(livekit::DisabledCodecs {
                    codecs: vec![livekit::Codec {
                        mime: MimeType::Av1.to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            merge: true,
        },
        ConfigurationItem {
            r#match: Arc::new(script_match!(|c| (c.device_model == "xiaomi 2201117ti"
                && c.os == "android")
                || ((c.browser == "firefox" || c.browser == "firefox mobile")
                    && c.os == "android"))),
            configuration: Some(livekit::ClientConfiguration {
                disabled_codecs: Some(livekit::DisabledCodecs {
                    publish: vec![livekit::Codec {
                        mime: MimeType::H264.to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            // merged so Firefox for Android keeps the AV1 rule above
            merge: true,
        },
    ]
});

#[cfg(test)]
#[path = "conf_test.rs"]
mod conf_test;
