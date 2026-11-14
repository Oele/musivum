// Ported from LiveKit, pkg/clientconfiguration/conf_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the expressions that the original hands to
// `NewScriptMatch` as strings are written through `script_match!`, as the rules of the table
// are. Two cases of `TestScriptMatch`, the ones that expect an error, cannot be written that
// way and are not here: see there. The last test is new.

use std::sync::Arc;

use crate::clientconfiguration::*;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::script_match;
use crate::testutils::capture_log;

#[test]
fn test_static_configuration_h264() {
    let cm = StaticClientConfigurationManager::new(STATIC_CONFIGURATIONS.clone());

    let h264_disabled = |info: &livekit::ClientInfo| -> bool {
        let Some(conf) = cm.get_configuration(info) else {
            return false;
        };
        let Some(disabled_codecs) = conf.disabled_codecs.as_ref() else {
            return false;
        };
        for c in &disabled_codecs.publish {
            if mime::is_mime_type_string_h264(&c.mime) {
                return true;
            }
        }
        false
    };

    // desktop Linux Firefox publishes H.264 through OpenH264, so it must not be disabled
    assert!(!h264_disabled(&livekit::ClientInfo {
        browser: "firefox".to_string(),
        os: "linux".to_string(),
        ..Default::default()
    }));
    // Firefox for Android is still worked around
    assert!(h264_disabled(&livekit::ClientInfo {
        browser: "firefox mobile".to_string(),
        os: "android".to_string(),
        ..Default::default()
    }));
}

#[test]
fn test_static_configuration_av1() {
    let cm = StaticClientConfigurationManager::new(STATIC_CONFIGURATIONS.clone());

    let av1_disabled = |info: &livekit::ClientInfo| -> bool {
        let Some(conf) = cm.get_configuration(info) else {
            return false;
        };
        let Some(disabled_codecs) = conf.disabled_codecs.as_ref() else {
            return false;
        };
        for c in &disabled_codecs.codecs {
            if mime::is_mime_type_string_equal(&c.mime, mime::MimeType::Av1.as_str()) {
                return true;
            }
        }
        false
    };

    assert!(av1_disabled(&livekit::ClientInfo {
        browser: "Firefox".to_string(),
        browser_version: "155.0".to_string(),
        os: "macOS".to_string(),
        ..Default::default()
    }));
    assert!(av1_disabled(&livekit::ClientInfo {
        browser: "firefox".to_string(),
        browser_version: "156.0.1".to_string(),
        os: "windows".to_string(),
        ..Default::default()
    }));
    assert!(av1_disabled(&livekit::ClientInfo {
        browser: "firefox".to_string(),
        browser_version: "158.0.2".to_string(),
        os: "windows".to_string(),
        ..Default::default()
    }));
    // an unknown version is treated as affected
    assert!(av1_disabled(&livekit::ClientInfo {
        browser: "firefox".to_string(),
        os: "linux".to_string(),
        ..Default::default()
    }));
    // should not be disabled for Chrome
    assert!(!av1_disabled(&livekit::ClientInfo {
        browser: "chrome".to_string(),
        os: "linux".to_string(),
        ..Default::default()
    }));

    // Firefox for Android keeps the AV1 rule alongside its H.264 publish rule
    let android = livekit::ClientInfo {
        browser: "firefox mobile".to_string(),
        browser_version: "156.0".to_string(),
        os: "android".to_string(),
        ..Default::default()
    };
    assert!(av1_disabled(&android));
    let conf = cm.get_configuration(&android).unwrap();
    let disabled_codecs = conf.disabled_codecs.as_ref().unwrap();
    assert_eq!(1, disabled_codecs.publish.len());
    assert!(mime::is_mime_type_string_h264(
        &disabled_codecs.publish[0].mime
    ));
}

#[test]
fn test_script_match_configuration() {
    {
        let name = "no merge";
        let confs = vec![ConfigurationItem {
            r#match: Arc::new(script_match!(|c| c.protocol > 5 && c.browser != "firefox")),
            configuration: Some(livekit::ClientConfiguration {
                resume_connection: livekit::ClientConfigSetting::Enabled as i32,
                ..Default::default()
            }),
            merge: false,
        }];

        let cm = StaticClientConfigurationManager::new(confs);

        let conf = cm.get_configuration(&livekit::ClientInfo {
            protocol: 4,
            ..Default::default()
        });
        assert_eq!(None, conf, "{name}");

        let conf = cm.get_configuration(&livekit::ClientInfo {
            protocol: 6,
            browser: "firefox".to_string(),
            ..Default::default()
        });
        assert_eq!(None, conf, "{name}");

        let conf = cm
            .get_configuration(&livekit::ClientInfo {
                protocol: 6,
                browser: "chrome".to_string(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            conf.resume_connection,
            livekit::ClientConfigSetting::Enabled as i32,
            "{name}"
        );
    }

    {
        let name = "merge";
        let confs = vec![
            ConfigurationItem {
                r#match: Arc::new(script_match!(|c| c.protocol > 5 && c.browser != "firefox")),
                configuration: Some(livekit::ClientConfiguration {
                    resume_connection: livekit::ClientConfigSetting::Enabled as i32,
                    ..Default::default()
                }),
                merge: true,
            },
            ConfigurationItem {
                r#match: Arc::new(script_match!(|c| c.sdk == "android")),
                configuration: Some(livekit::ClientConfiguration {
                    video: Some(livekit::VideoConfiguration {
                        hardware_encoder: livekit::ClientConfigSetting::Disabled as i32,
                    }),
                    ..Default::default()
                }),
                merge: true,
            },
        ];

        let cm = StaticClientConfigurationManager::new(confs);

        let conf = cm.get_configuration(&livekit::ClientInfo {
            protocol: 4,
            ..Default::default()
        });
        assert_eq!(None, conf, "{name}");

        let conf = cm.get_configuration(&livekit::ClientInfo {
            protocol: 6,
            browser: "firefox".to_string(),
            ..Default::default()
        });
        assert_eq!(None, conf, "{name}");

        let conf = cm
            .get_configuration(&livekit::ClientInfo {
                protocol: 6,
                browser: "chrome".to_string(),
                sdk: 3,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            conf.resume_connection,
            livekit::ClientConfigSetting::Enabled as i32,
            "{name}"
        );
        assert_eq!(
            conf.video.as_ref().unwrap().hardware_encoder,
            livekit::ClientConfigSetting::Disabled as i32,
            "{name}"
        );
    }
}

#[test]
fn test_script_match() {
    let client = livekit::ClientInfo {
        protocol: 6,
        browser: "chrome".to_string(),
        sdk: 3, // android
        device_model: "12345".to_string(),
        browser_version: "13.2".to_string(),
        version: "2.17.1".to_string(),
        ..Default::default()
    };

    struct Testcase {
        name: &'static str,
        expr: ScriptMatch,
        result: bool,
    }

    let cases = [
        Testcase {
            name: "simple match",
            expr: script_match!(|c| c.protocol > 5),
            result: true,
        },
        // {name: "invalid expr", expr: `cc.protocol > 5`, err: true},
        // {name: "unexist field", expr: `c.protocols > 5`, err: true},
        //
        // These two cannot be written: the original expects an error of them, the first
        // from `NewScriptMatch` (the script does not compile) and the second from `Match`
        // (it fails each time it runs), and here neither compiles. That they do not is
        // tested where `script_match!` is documented, in match.rs. With them the `err`
        // of a case is gone, which no other case sets.
        Testcase {
            name: "combined condition",
            expr: script_match!(|c| c.protocol > 5 && (c.sdk == "android" || c.sdk == "ios")),
            result: true,
        },
        Testcase {
            name: "combined condition2",
            expr: script_match!(
                |c| (c.device_model == "xiaomi 2201117ti" && c.os == "android")
                    || ((c.browser == "firefox" || c.browser == "firefox mobile")
                        && (c.os == "linux" || c.os == "android"))
            ),
            result: false,
        },
        Testcase {
            name: "string lesser",
            expr: script_match!(|c| c.browser_version < "11.3"),
            result: false,
        },
        Testcase {
            name: "string lesser eq",
            expr: script_match!(|c| c.browser_version <= "13.2"),
            result: true,
        },
        Testcase {
            name: "string greater",
            expr: script_match!(|c| c.browser_version > "11.3"),
            result: true,
        },
        Testcase {
            name: "string greater eq",
            expr: script_match!(|c| c.browser_version >= "13.2"),
            result: true,
        },
        Testcase {
            name: "semantic lesser",
            expr: script_match!(|c| c.version < "2.16.10"),
            result: false,
        },
        Testcase {
            name: "semantic lesser eq",
            expr: script_match!(|c| c.version <= "2.17.1"),
            result: true,
        },
        Testcase {
            name: "semantic greater",
            expr: script_match!(|c| c.version > "2.16.10"),
            result: true,
        },
        Testcase {
            name: "semantic greater eq",
            expr: script_match!(|c| c.version >= "2.17.1"),
            result: true,
        },
    ];

    for c in cases {
        let m = c.expr.r#match(&client);
        assert_eq!(Ok(c.result), m, "{}: {:?}", c.name, c.expr);
    }
}

// New for musivum: what the original's tests do not come to in `GetConfiguration`. A rule
// whose match fails is logged and passed over; a rule that is not merged is the answer on
// its own, also when merged rules matched before it; and a rule may have no configuration.
#[test]
fn a_failed_match_an_unmerged_rule_and_a_rule_without_configuration() {
    struct Fails;
    impl Match for Fails {
        fn r#match(&self, _client_info: &livekit::ClientInfo) -> Result<bool, Error> {
            Err(Error::InvalidMatchExpressionResult)
        }
    }

    let resume = |setting: livekit::ClientConfigSetting| {
        Some(livekit::ClientConfiguration {
            resume_connection: setting as i32,
            ..Default::default()
        })
    };
    let relay = Some(livekit::ClientConfiguration {
        force_relay: livekit::ClientConfigSetting::Enabled as i32,
        ..Default::default()
    });
    let client = livekit::ClientInfo {
        protocol: 6,
        address: "192.0.2.7".to_string(),
        ..Default::default()
    };

    let cm = StaticClientConfigurationManager::new(vec![
        ConfigurationItem {
            r#match: Arc::new(Fails),
            configuration: resume(livekit::ClientConfigSetting::Disabled),
            merge: false,
        },
        ConfigurationItem {
            r#match: Arc::new(script_match!(|c| c.protocol > 5)),
            configuration: resume(livekit::ClientConfigSetting::Enabled),
            merge: true,
        },
        ConfigurationItem {
            r#match: Arc::new(script_match!(|c| c.protocol > 5)),
            configuration: None,
            merge: true,
        },
        ConfigurationItem {
            r#match: Arc::new(script_match!(|c| c.protocol > 5)),
            configuration: relay.clone(),
            merge: true,
        },
        ConfigurationItem {
            r#match: Arc::new(script_match!(|c| c.protocol > 9)),
            configuration: None,
            merge: false,
        },
        ConfigurationItem {
            r#match: Arc::new(script_match!(|c| c.protocol > 8)),
            configuration: relay.clone(),
            merge: false,
        },
    ]);

    let mut conf = None;
    let log = capture_log(tracing::Level::ERROR, || {
        conf = cm.get_configuration(&client);
    });
    assert_eq!(
        Some(livekit::ClientConfiguration {
            resume_connection: livekit::ClientConfigSetting::Enabled as i32,
            force_relay: livekit::ClientConfigSetting::Enabled as i32,
            ..Default::default()
        }),
        conf
    );
    assert!(log.contains("matchrule failed"), "{log}");
    assert!(log.contains("invalid match expression result"), "{log}");
    assert!(!log.contains("192.0.2.7"), "{log}");

    let client = livekit::ClientInfo {
        protocol: 9,
        ..client
    };
    assert_eq!(relay, cm.get_configuration(&client));

    let client = livekit::ClientInfo {
        protocol: 10,
        ..client
    };
    assert_eq!(None, cm.get_configuration(&client));
}
