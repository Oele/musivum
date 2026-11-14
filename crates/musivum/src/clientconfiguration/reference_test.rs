//! New for musivum: the port held against the original, client by client.
//!
//! `testdata/reference_output.txt` is what LiveKit's own package at the pin says about
//! made-up clients and about pairs of versions; `testdata/reference_output_test.go` printed
//! it, and says how it is run. The test here says the same with the port and expects the
//! same lines. The parts of a line have tabs between them:
//!
//! - `REF client`, the nine fields of a client that rules can read (the number of its SDK,
//!   the SDK's version, the protocol, the operating system and its version, the model of the
//!   device, the browser and its version, the address), `=>`, the same nine as an expression
//!   sees them (`c.sdk`, `c.version`, ...), `=>`, the configuration that LiveKit's rules give
//!   this client, `=>`, the one that the rules of `ref_configurations` below give it: rules
//!   that are not LiveKit's, for what its own do not use.
//! - `REF version`, the field of the client that has the version (`version` or
//!   `browser_version`), the version, a string to compare it with, `=>`, whether the version
//!   is `<`, `<=`, `>`, `>=`, `==`, `!=` the string, to an expression, as six digits, then
//!   whether each of the two is a semantic version to Go's `semver` package behind a "v",
//!   and what that package's `Compare` says of them.
//!
//! `MUSIVUM_CLIENTCONF_REFERENCE` may name another file of such lines, which is then read as
//! well: the output of the Go test for the clients and pairs it makes from lists of values,
//! kept outside the repository.

use std::sync::Arc;

use super::*;
use crate::protocol::livekit;
use crate::protocol::livekit::ClientConfigSetting::{Disabled, Enabled, Unset};
use crate::script_match;

// `refConfigurations` of testdata/reference_output_test.go.
fn ref_configurations() -> Vec<ConfigurationItem> {
    vec![
        ConfigurationItem {
            r#match: Arc::new(script_match!(|c| c.protocol > 5 && c.browser != "firefox")),
            configuration: Some(livekit::ClientConfiguration {
                resume_connection: Enabled as i32,
                force_relay: Disabled as i32,
                ..Default::default()
            }),
            merge: true,
        },
        ConfigurationItem {
            r#match: Arc::new(script_match!(|c| c.sdk == "android"
                || c.sdk == "react_native"
                || c.sdk == "99")),
            configuration: Some(livekit::ClientConfiguration {
                video: Some(livekit::VideoConfiguration {
                    hardware_encoder: Disabled as i32,
                }),
                force_relay: Enabled as i32,
                ..Default::default()
            }),
            merge: true,
        },
        ConfigurationItem {
            r#match: Arc::new(script_match!(|c| c.version >= "2.17.1" && c.version != "3")),
            configuration: Some(livekit::ClientConfiguration {
                video: Some(livekit::VideoConfiguration::default()),
                screen: Some(livekit::VideoConfiguration {
                    hardware_encoder: Enabled as i32,
                }),
                resume_connection: Unset as i32,
                disabled_codecs: Some(livekit::DisabledCodecs {
                    codecs: vec![livekit::Codec {
                        mime: "video/VP8".to_string(),
                        fmtp_line: "x=1".to_string(),
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            merge: true,
        },
        ConfigurationItem {
            r#match: Arc::new(script_match!(
                |c| c.os_version == "14.5" || c.address == "192.0.2.7"
            )),
            configuration: Some(livekit::ClientConfiguration {
                disabled_codecs: Some(livekit::DisabledCodecs {
                    codecs: vec![livekit::Codec {
                        mime: "video/H265".to_string(),
                        ..Default::default()
                    }],
                    publish: vec![livekit::Codec {
                        mime: "video/VP8".to_string(),
                        ..Default::default()
                    }],
                }),
                ..Default::default()
            }),
            merge: true,
        },
        ConfigurationItem {
            r#match: Arc::new(script_match!(|c| c.protocol == 16
                || c.version < "1.0.0-beta"
                || c.browser_version == "18.3")),
            configuration: Some(livekit::ClientConfiguration {
                resume_connection: Disabled as i32,
                ..Default::default()
            }),
            merge: false,
        },
    ]
}

fn ref_codecs(codecs: &[livekit::Codec]) -> String {
    let s: Vec<String> = codecs
        .iter()
        .map(|c| {
            if c.fmtp_line.is_empty() {
                c.mime.clone()
            } else {
                format!("{};{}", c.mime, c.fmtp_line)
            }
        })
        .collect();
    format!("[{}]", s.join(","))
}

fn ref_video(v: Option<&livekit::VideoConfiguration>) -> String {
    match v {
        None => "nil".to_string(),
        Some(v) => v.hardware_encoder.to_string(),
    }
}

fn ref_configuration(conf: Option<&livekit::ClientConfiguration>) -> String {
    let Some(conf) = conf else {
        return "nil".to_string();
    };
    let disabled = match conf.disabled_codecs.as_ref() {
        None => "nil".to_string(),
        Some(d) => format!(
            "codecs={} publish={}",
            ref_codecs(&d.codecs),
            ref_codecs(&d.publish)
        ),
    };
    format!(
        "video={} screen={} resume={} disabled={{{}}} relay={}",
        ref_video(conf.video.as_ref()),
        ref_video(conf.screen.as_ref()),
        conf.resume_connection,
        disabled,
        conf.force_relay
    )
}

fn ref_bool(b: bool) -> &'static str {
    if b { "1" } else { "0" }
}

// Says with the port what each line of `reference` says, and expects the same line. Gives
// the number of clients and of pairs of versions that it read.
fn check_reference(reference: &str) -> (usize, usize) {
    let cm = StaticClientConfigurationManager::new(STATIC_CONFIGURATIONS.clone());
    let ref_cm = StaticClientConfigurationManager::new(ref_configurations());
    let (mut clients, mut versions) = (0, 0);
    for (n, line) in reference.lines().enumerate() {
        let n = n + 1;
        let f: Vec<&str> = line.split('\t').collect();
        let mut out: Vec<String> = Vec::new();
        match f[0] {
            "REF client" => {
                clients += 1;
                let info = livekit::ClientInfo {
                    sdk: f[1].parse().unwrap(),
                    version: f[2].to_string(),
                    protocol: f[3].parse().unwrap(),
                    os: f[4].to_string(),
                    os_version: f[5].to_string(),
                    device_model: f[6].to_string(),
                    browser: f[7].to_string(),
                    browser_version: f[8].to_string(),
                    address: f[9].to_string(),
                    ..Default::default()
                };
                out.extend(f[..10].iter().map(|s| s.to_string()));
                out.push("=>".to_string());
                let c = ClientObject::new(&info);
                out.extend([
                    c.sdk.clone(),
                    c.version.to_string(),
                    c.protocol.to_string(),
                    c.os.clone(),
                    c.os_version.to_string(),
                    c.device_model.clone(),
                    c.browser.clone(),
                    c.browser_version.to_string(),
                    c.address.to_string(),
                ]);
                out.push("=>".to_string());
                out.push(ref_configuration(cm.get_configuration(&info).as_ref()));
                out.push("=>".to_string());
                out.push(ref_configuration(ref_cm.get_configuration(&info).as_ref()));
            }
            "REF version" => {
                versions += 1;
                let (field, left, right) = (f[1], f[2], f[3]);
                let info = match field {
                    "version" => livekit::ClientInfo {
                        version: left.to_string(),
                        ..Default::default()
                    },
                    "browser_version" => livekit::ClientInfo {
                        browser_version: left.to_string(),
                        ..Default::default()
                    },
                    _ => panic!("line {n}: {line}"),
                };
                let c = ClientObject::new(&info);
                let v = match field {
                    "version" => c.version,
                    _ => c.browser_version,
                };
                out.extend(f[..4].iter().map(|s| s.to_string()));
                out.push("=>".to_string());
                out.push(
                    [
                        v < right,
                        v <= right,
                        v > right,
                        v >= right,
                        v == right,
                        v != right,
                    ]
                    .map(ref_bool)
                    .concat(),
                );
                let (left, right) = (format!("v{left}"), format!("v{right}"));
                out.push(
                    [semver::is_valid(&left), semver::is_valid(&right)]
                        .map(ref_bool)
                        .concat(),
                );
                out.push(semver::compare(&left, &right).to_string());
            }
            _ => panic!("line {n}: {line}"),
        }
        assert_eq!(line, out.join("\t"), "line {n}");
    }
    (clients, versions)
}

#[test]
fn test_reference_output() {
    let read = check_reference(include_str!("testdata/reference_output.txt"));
    assert_eq!((51, 42), read);

    if let Ok(path) = std::env::var("MUSIVUM_CLIENTCONF_REFERENCE") {
        let (clients, versions) = check_reference(&std::fs::read_to_string(&path).unwrap());
        println!("{path}: {clients} clients and {versions} pairs of versions as the original");
    }
}
