// Ported from LiveKit, cmd/server/main_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `TestMainExitCode` runs the program, and is in
// tests/main_test.rs, where the program is at hand. The files are written in a directory of
// the test's own under the build's `target`, where the original writes `file` into the
// directory it runs in. The tests after `test_should_return_error_if_config_file_does_not_exist`
// are new: the flags and the environment against the configuration, with the values
// LiveKit's `getConfig` gives for the same command lines at the pin (each run in a process
// of its own, through a test laid over cmd/server with `go test -overlay`).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use clap::Parser;

use super::*;

// `t.TempDir()`: a directory of the test's own, under the build's `target`, taken away
// when this is dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> TempDir {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir =
            PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/tmp")).join(format!(
                "main_test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }

    fn join(&self, name: &str) -> String {
        self.0.join(name).to_str().unwrap().to_string()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct TestStruct {
    config_file_name: &'static str,
    config_body: &'static str,

    expected_config_body: &'static str,
}

#[test]
fn test_get_config_string() {
    let dir = TempDir::new();
    let tests = [
        TestStruct {
            config_file_name: "",
            config_body: "",
            expected_config_body: "",
        },
        TestStruct {
            config_file_name: "",
            config_body: "configBody",
            expected_config_body: "configBody",
        },
        TestStruct {
            config_file_name: "file",
            config_body: "configBody",
            expected_config_body: "configBody",
        },
        TestStruct {
            config_file_name: "file",
            config_body: "",
            expected_config_body: "fileContent",
        },
    ];
    for test in tests {
        let config_file_name = write_config_file(&dir, &test);

        let config_body = get_config_string(&config_file_name, test.config_body).unwrap();
        assert_eq!(test.expected_config_body, config_body);

        if !config_file_name.is_empty() {
            std::fs::remove_file(&config_file_name).unwrap();
        }
    }
}

#[test]
fn test_should_return_error_if_config_file_does_not_exist() {
    let config_body = get_config_string("notExistingFile", "");
    assert!(config_body.is_err());
}

fn write_config_file(dir: &TempDir, test: &TestStruct) -> String {
    if test.config_file_name.is_empty() {
        return String::new();
    }
    let path = dir.join(test.config_file_name);
    std::fs::write(&path, test.expected_config_body).unwrap();
    path
}

// What `getConfig` makes of a command line and an environment, in the shape the test laid
// over cmd/server printed it.
#[derive(Debug, PartialEq)]
struct Got {
    development: bool,
    port: u32,
    bind_addresses: Vec<&'static str>,
    keys: Vec<&'static str>,
    key_file: &'static str,
    region: &'static str,
    node_ip: &'static str,
    udp_port: &'static str,
    ips_includes: Vec<&'static str>,
    turn_cert: &'static str,
    turn_key: &'static str,
    log_level: &'static str,
}

impl Default for Got {
    fn default() -> Got {
        Got {
            development: false,
            port: 7880,
            bind_addresses: Vec::new(),
            keys: Vec::new(),
            key_file: "",
            region: "",
            node_ip: "127.0.0.1",
            udp_port: "0-0",
            ips_includes: Vec::new(),
            turn_cert: "",
            turn_key: "",
            log_level: "",
        }
    }
}

fn run(args: &[&str], env: &[(&str, &str)]) -> Result<Config, Error> {
    let cli =
        Cli::try_parse_from(std::iter::once("musivum-server").chain(args.iter().copied())).unwrap();
    let env: HashMap<String, String> = env
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let flags = cli.flags.with_env(|name| env.get(name).cloned());
    get_config(&flags, |_| {})
}

fn check(name: &str, args: &[&str], env: &[(&str, &str)], want: Got) {
    let conf = run(args, env).unwrap_or_else(|err| panic!("{name}: {err}"));
    let mut keys: Vec<String> = conf.keys.iter().map(|(k, v)| format!("{k}={v}")).collect();
    keys.sort();
    let rtc = &conf.rtc.rtc_config;
    assert_eq!(want.development, conf.development, "{name}: development");
    assert_eq!(want.port, conf.port, "{name}: port");
    assert_eq!(
        want.bind_addresses, conf.bind_addresses,
        "{name}: bind_addresses"
    );
    assert_eq!(want.keys, keys, "{name}: keys");
    assert_eq!(want.key_file, conf.key_file, "{name}: key_file");
    assert_eq!(want.region, conf.region, "{name}: region");
    assert_eq!(want.node_ip, rtc.node_ip.v4, "{name}: node_ip");
    assert_eq!(
        want.udp_port,
        format!("{}-{}", rtc.udp_port.start, rtc.udp_port.end),
        "{name}: udp_port"
    );
    assert_eq!(want.ips_includes, rtc.ips.includes, "{name}: ips_includes");
    assert_eq!(want.turn_cert, conf.turn.cert_file, "{name}: turn_cert");
    assert_eq!(want.turn_key, conf.turn.key_file, "{name}: turn_key");
    assert_eq!(
        want.log_level, conf.logging.config.level,
        "{name}: log_level"
    );
}

const BODY: &str = "port: 39500\nbind_addresses: [127.0.0.1]\nrtc:\n  node_ip: 127.0.0.1\n  udp_port: 39502\nkeys:\n  a: b\nregion: from-body\n";

#[test]
fn flags_are_laid_over_the_configuration() {
    // --node-ip does not replace the file's address: the original's NodeIP refuses a second
    // IPv4 address, and its error is not looked at
    check(
        "flags",
        &[
            "--config-body",
            BODY,
            "--keys",
            "k1: s1",
            "--region",
            "r1",
            "--node-ip",
            "192.0.2.4",
            "--udp-port",
            "9000-9010",
            "--turn-cert",
            "c.pem",
            "--turn-key",
            "k.pem",
            "--bind",
            "127.0.0.2",
            "--bind",
            "::1",
            "--key-file",
            "keys.yaml",
        ],
        &[],
        Got {
            port: 39500,
            bind_addresses: vec!["127.0.0.2", "::1"],
            keys: vec!["k1=s1"],
            key_file: "keys.yaml",
            region: "r1",
            udp_port: "9000-9010",
            turn_cert: "c.pem",
            turn_key: "k.pem",
            ..Default::default()
        },
    );
    check(
        "bind comma",
        &["--bind", "127.0.0.1,::1", "--node-ip", "127.0.0.1"],
        &[],
        Got {
            bind_addresses: vec!["127.0.0.1", "::1"],
            ..Default::default()
        },
    );
    // the flags of the server go after a subcommand too
    let cli =
        Cli::try_parse_from(["musivum-server", "ports", "--keys", "k: s", "--dev=false"]).unwrap();
    assert_eq!(Some("k: s".to_string()), cli.flags.keys);
    assert_eq!(Some(false), cli.flags.dev);
}

#[test]
fn the_environment_stands_in_for_flags_not_given() {
    check(
        "env",
        &[],
        &[
            ("LIVEKIT_CONFIG", BODY),
            ("LIVEKIT_KEYS", "k2: s2"),
            ("LIVEKIT_REGION", "r2"),
            ("NODE_IP", "192.0.2.5"),
            ("UDP_PORT", "9100"),
            ("LIVEKIT_TURN_CERT", "c2.pem"),
            ("LIVEKIT_TURN_KEY", "k2.pem"),
        ],
        Got {
            port: 39500,
            bind_addresses: vec!["127.0.0.1"],
            keys: vec!["k2=s2"],
            region: "r2",
            udp_port: "9100-0",
            turn_cert: "c2.pem",
            turn_key: "k2.pem",
            ..Default::default()
        },
    );
    check(
        "flag over env",
        &[
            "--keys",
            "k1: s1",
            "--config-body",
            "port: 39501\nrtc:\n  node_ip: 127.0.0.1\n",
        ],
        &[("LIVEKIT_KEYS", "k2: s2"), ("LIVEKIT_CONFIG", BODY)],
        Got {
            port: 39501,
            keys: vec!["k1=s1"],
            ..Default::default()
        },
    );
    // set and empty is set
    check(
        "empty env",
        &["--config-body", BODY],
        &[("LIVEKIT_KEYS", ""), ("LIVEKIT_REGION", "")],
        Got {
            port: 39500,
            bind_addresses: vec!["127.0.0.1"],
            udp_port: "39502-0",
            ..Default::default()
        },
    );
}

#[test]
fn development_mode_has_placeholder_keys_on_loopback() {
    let dev = Got {
        development: true,
        keys: vec!["devkey=secret"],
        udp_port: "7882-0",
        log_level: "debug",
        ..Default::default()
    };
    check(
        "dev",
        &["--dev", "--node-ip", "127.0.0.1"],
        &[],
        Got {
            bind_addresses: vec!["127.0.0.1", "::1"],
            ..dev
        },
    );
    let dev = Got {
        development: true,
        keys: vec!["devkey=secret"],
        udp_port: "7882-0",
        log_level: "debug",
        ..Default::default()
    };
    check(
        "dev bind non-loopback",
        &[
            "--dev",
            "--node-ip",
            "127.0.0.1",
            "--bind",
            "192.0.2.4",
            "--bind",
            "127.0.0.1",
        ],
        &[],
        Got {
            bind_addresses: vec!["192.0.2.4", "127.0.0.1"],
            ips_includes: vec!["192.0.2.4/24", "127.0.0.1/24"],
            ..dev
        },
    );
    let dev = Got {
        development: true,
        keys: vec!["devkey=secret"],
        udp_port: "7882-0",
        log_level: "debug",
        ..Default::default()
    };
    check(
        "dev bind loopback",
        &["--dev", "--node-ip", "127.0.0.1", "--bind", "127.0.0.1"],
        &[],
        Got {
            bind_addresses: vec!["127.0.0.1"],
            ..dev
        },
    );
    check(
        "dev with keys",
        &["--dev", "--node-ip", "127.0.0.1", "--keys", "k: s"],
        &[],
        Got {
            development: true,
            keys: vec!["k=s"],
            udp_port: "7882-0",
            log_level: "debug",
            ..Default::default()
        },
    );
    check(
        "dev false",
        &[
            "--dev=false",
            "--config-body",
            "development: true\nrtc:\n  node_ip: 127.0.0.1\n",
        ],
        &[],
        Got::default(),
    );
}

#[test]
fn the_configuration_comes_from_the_body_before_the_file() {
    let dir = TempDir::new();
    let file = dir.join("conf.yaml");
    std::fs::write(&file, "port: 39600\nregion: from-file\n").unwrap();
    check(
        "file",
        &["--config", &file, "--node-ip", "127.0.0.1"],
        &[],
        Got {
            port: 39600,
            region: "from-file",
            ..Default::default()
        },
    );
    check(
        "file and body",
        &["--config", &file, "--config-body", BODY],
        &[],
        Got {
            port: 39500,
            bind_addresses: vec!["127.0.0.1"],
            keys: vec!["a=b"],
            region: "from-body",
            udp_port: "39502-0",
            ..Default::default()
        },
    );

    // strict unless told otherwise
    let unknown = "nope: 1\nrtc:\n  node_ip: 127.0.0.1\n";
    let err = run(&["--config-body", unknown], &[]).unwrap_err();
    assert!(
        err.to_string().starts_with("could not parse config"),
        "{err}"
    );
    check(
        "unknown key not strict",
        &["--disable-strict-config", "--config-body", unknown],
        &[],
        Got::default(),
    );
}
