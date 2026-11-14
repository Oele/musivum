// Ported from LiveKit, pkg/config/config_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Not ported, each with a comment at its place: the two
// tests of the generated command line flags, the two of the `psrpc` section, which is taken
// from a file and not read, and the test of the `yaml` tags, which the port does not have.
// A temporary directory (`t.TempDir()`) is one under the build's `target`. The tests of
// files with permissions are for Unix. The tests after `TestTURNCredentialsFile` are new
// for musivum: they hold the port against what the original holds for the same input.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::config::yaml::Dump;
use crate::sfu::pacer;
use crate::testutils::capture_log;

#[test]
fn test_config_unmarshal_keys() {
    let mut conf = Config::new("", true).unwrap();

    conf.unmarshal_keys("key1: secret1").unwrap();
    assert_eq!("secret1", conf.keys["key1"]);
}

#[test]
fn test_config_defaults_kept() {
    let content = "room:\n  empty_timeout: 10";
    let conf = Config::new(content, true).unwrap();
    assert!(conf.room.auto_create);
    assert_eq!(10u32, conf.room.empty_timeout);
}

#[test]
fn test_config_signal_message_size_limit_defaults() {
    let conf = Config::new("", true).unwrap();
    // both default to 2 MiB when not specified
    assert_eq!(2i64 << 20, conf.limit.signal_message_size_limit);
    assert_eq!(2i64 << 20, conf.limit.agent_signal_message_size_limit);
}

#[test]
fn test_config_signal_message_size_limit_override() {
    let content = "limit:\n  signal_message_size_limit: 1024\n  agent_signal_message_size_limit: 0";
    let conf = Config::new(content, true).unwrap();
    assert_eq!(1024i64, conf.limit.signal_message_size_limit);
    // 0 explicitly disables the limit
    assert_eq!(0i64, conf.limit.agent_signal_message_size_limit);
}

// TestConfig_PSRPCCompressionDefaults and TestConfig_PSRPCCompressionOverride are not
// ported: the `psrpc` section is taken from a file and not read, so there are no values of
// it to look at.

#[test]
fn test_config_unknown_keys() {
    let content = "unknown: 10\nroom:\n  empty_timeout: 10";
    assert!(Config::new(content, true).is_err());
}

// TestGeneratedFlags and TestGeneratedDurationFlags are not ported: the flags generated for
// every key of the configuration, and the environment variables that come with them, are
// not ported.

// TestYAMLTag is not ported: it looks, by reflection, for `yaml` tags without `omitempty`.

#[test]
fn test_clamp_turn_ttl_seconds() {
    let cases: [(&str, i64, i64, bool); 7] = [
        (
            "negative falls back to default",
            -1,
            DEFAULT_TURN_TTL_SECONDS,
            true,
        ),
        (
            "large negative falls back to default",
            -1 << 40,
            DEFAULT_TURN_TTL_SECONDS,
            true,
        ),
        (
            "zero falls back to default",
            0,
            DEFAULT_TURN_TTL_SECONDS,
            true,
        ),
        ("in range preserved", 600, 600, false),
        (
            "max preserved",
            TURN_MAX_TTL_SECONDS,
            TURN_MAX_TTL_SECONDS,
            false,
        ),
        (
            "over max clamps to max",
            TURN_MAX_TTL_SECONDS + 1,
            TURN_MAX_TTL_SECONDS,
            true,
        ),
        (
            "overflowing value clamps to max",
            (1 << 62) + 1,
            TURN_MAX_TTL_SECONDS,
            true,
        ),
    ];
    for (name, r#in, want, want_changed) in cases {
        let (got, changed) = clamp_turn_ttl_seconds(r#in);
        assert_eq!(want, got, "{name}");
        assert_eq!(want_changed, changed, "{name}");
    }
}

#[test]
fn test_normalize_turn_ttls() {
    let mut conf = Config::new("", true).unwrap();

    conf.turn.ttl_seconds = -5;
    conf.rtc.turn_servers = vec![
        TurnServer {
            host: "over".to_string(),
            ttl: TURN_MAX_TTL_SECONDS + 100,
            ..Default::default()
        },
        TurnServer {
            host: "negative".to_string(),
            ttl: -1,
            ..Default::default()
        },
        TurnServer {
            host: "default".to_string(),
            ttl: 0,
            ..Default::default()
        },
        TurnServer {
            host: "ok".to_string(),
            ttl: 600,
            ..Default::default()
        },
    ];

    conf.normalize_turn_ttls();

    // embedded TTL <= 0 falls back to the 5m default
    assert_eq!(DEFAULT_TURN_TTL_SECONDS, conf.turn.ttl_seconds);
    // external TTLs: only the upper bound is capped at load; 0/negative keep their
    // "use the default" meaning and are resolved when credentials are generated
    assert_eq!(TURN_MAX_TTL_SECONDS, conf.rtc.turn_servers[0].ttl);
    assert_eq!(-1, conf.rtc.turn_servers[1].ttl);
    assert_eq!(0, conf.rtc.turn_servers[2].ttl);
    assert_eq!(600, conf.rtc.turn_servers[3].ttl);
}

#[test]
fn test_new_config_normalizes_turn_ttl() {
    let content = "turn:\n  ttl_seconds: -10";
    let conf = Config::new(content, true).unwrap();
    assert_eq!(DEFAULT_TURN_TTL_SECONDS, conf.turn.ttl_seconds);
}

#[test]
fn test_new_config_validates_send_side_bwe_pacer() {
    let cases: [(&str, &str, bool); 6] = [
        ("unset keeps the default", "", false),
        (
            "pass-through",
            pacer::PacerBehavior::PassThrough.as_str(),
            false,
        ),
        ("no-queue", pacer::PacerBehavior::NoQueue.as_str(), false),
        (
            "leaky-bucket",
            pacer::PacerBehavior::Leakybucket.as_str(),
            false,
        ),
        ("typo is rejected", "leakybucket", true),
        ("unknown value is rejected", "not-a-pacer", true),
    ];

    for (name, pacer, want_err) in cases {
        let content = format!("rtc:\n  congestion_control:\n    send_side_bwe_pacer: {pacer}");
        let conf = Config::new(&content, true);
        if want_err {
            let err = conf.expect_err(name);
            assert!(err.to_string().contains(pacer), "{name}: {err}");
            continue;
        }

        let conf = conf.expect(name);
        if pacer.is_empty() {
            assert_eq!(
                pacer::PacerBehavior::NoQueue.as_str(),
                conf.rtc.congestion_control.send_side_bwe_pacer,
                "{name}"
            );
            continue;
        }
        assert_eq!(
            pacer, conf.rtc.congestion_control.send_side_bwe_pacer,
            "{name}"
        );
    }
}

// `t.TempDir()`: a directory of the test's own, under the build's `target`, taken away
// when this is dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> TempDir {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir =
            PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/tmp")).join(format!(
                "config_test-{}-{}",
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

#[cfg(unix)]
fn write_file(path: &str, content: &str, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, content).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

#[cfg(unix)]
fn write_secret_file(dir: &TempDir, content: &str) -> String {
    let path = dir.join("secret");
    write_file(&path, content, 0o600);
    path
}

#[cfg(unix)]
#[test]
fn test_load_turn_secrets() {
    let server = |secret: &str, secret_file: &str, username: &str, credential: &str| {
        let mut conf = Config::default();
        conf.rtc.turn_servers = vec![TurnServer {
            host: "h".to_string(),
            secret: secret.to_string(),
            secret_file: secret_file.to_string(),
            username: username.to_string(),
            credential: credential.to_string(),
            ..Default::default()
        }];
        conf
    };

    // empty secret file is rejected
    {
        let dir = TempDir::new();
        let mut conf = server("", &write_secret_file(&dir, ""), "", "");
        let err = conf.load_turn_secrets().unwrap_err();
        assert!(err.is(&Error::TurnSecretEmpty), "{err}");
    }

    // whitespace-only secret file is rejected
    {
        let dir = TempDir::new();
        let mut conf = server("", &write_secret_file(&dir, "  \n\t "), "", "");
        let err = conf.load_turn_secrets().unwrap_err();
        assert!(err.is(&Error::TurnSecretEmpty), "{err}");
    }

    // valid secret file is loaded and trimmed
    {
        let dir = TempDir::new();
        let mut conf = server("", &write_secret_file(&dir, "  topsecret\n"), "", "");
        conf.load_turn_secrets().unwrap();
        assert_eq!("topsecret", conf.rtc.turn_servers[0].secret);
    }

    // inline whitespace secret without static creds is rejected
    {
        let mut conf = server("   ", "", "", "");
        let err = conf.load_turn_secrets().unwrap_err();
        assert!(err.is(&Error::TurnServerNoCredentials), "{err}");
    }

    // no credentials at all is rejected
    {
        let mut conf = server("", "", "", "");
        let err = conf.load_turn_secrets().unwrap_err();
        assert!(err.is(&Error::TurnServerNoCredentials), "{err}");
    }

    // static credentials are accepted
    {
        let mut conf = server("", "", "u", "c");
        conf.load_turn_secrets().unwrap();
    }

    // partial static credentials are rejected
    {
        let mut conf = server("", "", "u", "");
        let err = conf.load_turn_secrets().unwrap_err();
        assert!(err.is(&Error::TurnServerNoCredentials), "{err}");
    }

    // inline secret takes precedence and is trimmed
    {
        let dir = TempDir::new();
        let mut conf = server(" inline ", &write_secret_file(&dir, "fromfile"), "", "");
        conf.load_turn_secrets().unwrap();
        assert_eq!("inline", conf.rtc.turn_servers[0].secret);
    }

    // blank inline secret falls back to secret file
    {
        let dir = TempDir::new();
        let mut conf = server("   ", &write_secret_file(&dir, "fromfile"), "", "");
        conf.load_turn_secrets().unwrap();
        assert_eq!("fromfile", conf.rtc.turn_servers[0].secret);
    }
}

// Exercise the operator-facing YAML path so this test also runs on the upstream
// branch, where credentials_file is not recognized yet.
#[cfg(unix)]
#[test]
fn test_turn_credentials_file() {
    const FILE_NAME: &str = "turn-credentials.yaml";
    const USERNAME: &str = "turn-user";
    const CREDENTIAL: &str = " turn-password ";
    const VALID_CREDENTIALS: &str = "username: turn-user\ncredential: ' turn-password '\n";
    const PRIVATE_VALUE: &str = "do-not-log-my-password";
    const REDACTED_PREFIX: &str = "do-not-";
    const INLINE_CREDENTIALS: &str =
        "      username: inline-user\n      credential: inline-password\n";

    #[derive(Default)]
    struct Case {
        name: &'static str,
        data: &'static str,
        mode: u32,
        inline: &'static str,
        missing: bool,
        directory: bool,
        want_error: bool,
        want_user: &'static str,
        want_credential: &'static str,
    }
    let cases = [
        Case {
            name: "pair loaded from one file",
            data: VALID_CREDENTIALS,
            mode: 0o600,
            want_user: USERNAME,
            want_credential: CREDENTIAL,
            ..Default::default()
        },
        Case {
            name: "group-readable file accepted",
            data: VALID_CREDENTIALS,
            mode: 0o640,
            want_user: USERNAME,
            want_credential: CREDENTIAL,
            ..Default::default()
        },
        Case {
            name: "inline pair wins without reading missing file",
            missing: true,
            inline: INLINE_CREDENTIALS,
            want_user: "inline-user",
            want_credential: "inline-password",
            ..Default::default()
        },
        Case {
            name: "inline whitespace preserved",
            missing: true,
            inline: "      username: ' u '\n      credential: ' c '\n",
            want_user: " u ",
            want_credential: " c ",
            ..Default::default()
        },
        Case {
            name: "partial inline username is not mixed with file",
            data: VALID_CREDENTIALS,
            mode: 0o600,
            inline: "      username: inline-user\n",
            want_error: true,
            ..Default::default()
        },
        Case {
            name: "partial inline password is not mixed with file",
            data: VALID_CREDENTIALS,
            mode: 0o600,
            inline: "      credential: inline-password\n",
            want_error: true,
            ..Default::default()
        },
        Case {
            name: "missing file",
            missing: true,
            want_error: true,
            ..Default::default()
        },
        Case {
            name: "directory cannot be read",
            directory: true,
            want_error: true,
            ..Default::default()
        },
        Case {
            name: "others permissions rejected",
            data: VALID_CREDENTIALS,
            mode: 0o604,
            want_error: true,
            ..Default::default()
        },
        Case {
            name: "scalar credentials do not leak in errors",
            data: PRIVATE_VALUE,
            mode: 0o600,
            want_error: true,
            ..Default::default()
        },
        Case {
            name: "invalid YAML",
            data: "username: [",
            mode: 0o600,
            want_error: true,
            ..Default::default()
        },
        Case {
            name: "empty file",
            mode: 0o600,
            want_error: true,
            ..Default::default()
        },
        Case {
            name: "whitespace file",
            data: " \n\t",
            mode: 0o600,
            want_error: true,
            ..Default::default()
        },
        Case {
            name: "missing username",
            data: "credential: password\n",
            mode: 0o600,
            want_error: true,
            ..Default::default()
        },
        Case {
            name: "missing password",
            data: "username: user\n",
            mode: 0o600,
            want_error: true,
            ..Default::default()
        },
    ];
    for tc in cases {
        let dir = TempDir::new();
        let path = dir.join(FILE_NAME);
        if tc.directory {
            std::fs::create_dir(&path).unwrap();
        } else if !tc.missing {
            write_file(&path, tc.data, tc.mode);
        }
        let content = format!(
            "rtc:\n  turn_servers:\n    - host: turn.example.com\n      credentials_file: {:?}\n{}",
            path, tc.inline
        );
        let mut conf = Config::new(&content, true).expect(tc.name);
        let err = conf.load_turn_secrets();
        if tc.want_error {
            let err = err.expect_err(tc.name);
            assert!(
                !err.to_string().contains(REDACTED_PREFIX),
                "{}: {err}",
                tc.name
            );
            continue;
        }
        err.expect(tc.name);
        assert_eq!(
            tc.want_user, conf.rtc.turn_servers[0].username,
            "{}",
            tc.name
        );
        assert_eq!(
            tc.want_credential, conf.rtc.turn_servers[0].credential,
            "{}",
            tc.name
        );
    }
}

// ---------------------------------------------------------------------------
// New for musivum.
//
// What the port holds for a configuration is held against what the original holds for the
// same: every value as a line (`Dump`), next to the lines a Go test laid over the original
// at the pin wrote (testdata/dump_test.go has the test and how it is run). The parts that
// are taken from a file and not read are in neither, nor are the addresses of the node
// where the server finds them itself, which are the machine's.

fn dump(mut conf: Config) -> Vec<String> {
    let rtc = &mut conf.rtc.rtc_config;
    let mut lines = vec![format!(
        "rtc.node_ip_auto_generated={}",
        rtc.node_ip_auto_generated
    )];
    if rtc.node_ip_auto_generated {
        rtc.node_ip = Default::default();
    }
    conf.dump("", &mut lines);
    lines.sort();
    lines
}

// Fails with the lines that only one of the two has. Says how many values were compared.
fn assert_equals_the_original(conf: Config, original: &str) -> usize {
    let port = dump(conf);
    let original: Vec<String> = original.lines().map(str::to_string).collect();
    let only_original: Vec<&String> = original.iter().filter(|l| !port.contains(l)).collect();
    let only_port: Vec<&String> = port.iter().filter(|l| !original.contains(l)).collect();
    assert!(
        only_original.is_empty() && only_port.is_empty(),
        "only the original has:\n{only_original:#?}\nonly the port has:\n{only_port:#?}"
    );
    assert_eq!(original.len(), port.len());
    assert!(original.len() > 300, "{} values", original.len());
    original.len()
}

// New for musivum: the configuration without a file is the original's, which is its
// `DefaultConfig` with what `NewConfig` derives.
#[test]
fn test_default_config_equals_the_original() {
    let conf = Config::new("", true).unwrap();
    let compared = assert_equals_the_original(conf, include_str!("testdata/go-default.txt"));
    assert_eq!(319, compared);
}

// New for musivum: LiveKit's sample file with the options it has as comments set
// (testdata/make-sample.py made it) loads, strictly, and gives what the original holds for
// it. What the file sets for parts musivum leaves out is logged, a line each.
#[test]
fn test_sample_config_equals_the_original() {
    let mut loaded = None;
    let log = capture_log(tracing::Level::WARN, || {
        loaded = Some(Config::new(
            include_str!("testdata/config-sample-uncommented.yaml"),
            true,
        ));
    });
    let conf = loaded.unwrap().unwrap();

    let present: Vec<&str> = conf
        .ignored_sections()
        .iter()
        .filter(|(_, ignored)| ignored.is_present())
        .map(|(section, _)| *section)
        .collect();
    assert_eq!(
        vec!["redis", "ingress", "node_selector", "signal_relay", "psrpc"],
        present
    );
    for section in present {
        assert_eq!(
            1,
            log.lines()
                .filter(
                    |line| line.contains("configuration section has no effect in musivum")
                        && line.contains(&format!("{section:?}"))
                )
                .count(),
            "{section}:\n{log}"
        );
    }
    assert_eq!(5, log.lines().count(), "{log}");

    let compared = assert_equals_the_original(conf, include_str!("testdata/go-sample.txt"));
    assert_eq!(324, compared);
}

// New for musivum: a configuration that writes its values in the less common ways YAML has
// (numbers in other bases and with underscores, what YAML 1.1 takes for true and false,
// numbers for strings, nulls, tags, anchors and merge keys, maps laid over the defaults)
// gives what the original holds for it.
#[test]
fn test_edge_config_equals_the_original() {
    let conf = Config::new(include_str!("testdata/config-edge.yaml"), true).unwrap();
    let compared = assert_equals_the_original(conf, include_str!("testdata/go-edge.txt"));
    assert_eq!(357, compared);
}

// New for musivum: what is wrong in a configuration is said in the original's words, all
// of it at once.
#[test]
fn test_errors_of_a_config_are_the_originals() {
    let err = Config::new(include_str!("testdata/config-errors.yaml"), true).unwrap_err();
    assert_eq!(
        include_str!("testdata/go-errors.txt"),
        format!("error={err}\n")
    );
}

// New for musivum: without strict mode a key that is not known is passed over.
#[test]
fn test_config_unknown_keys_without_strict_mode() {
    let content = "unknown: 10\nroom:\n  empty_timeout: 10\n  nor_this: {a: [1, 2]}";
    let conf = Config::new(content, false).unwrap();
    assert_eq!(10u32, conf.room.empty_timeout);
}

// New for musivum: the flags of the server's command line are laid over the file before
// what follows from them is worked out, as in the original.
#[test]
fn test_new_config_with_flags() {
    let flags = BaseFlags {
        dev: Some(true),
        key_file: Some("$MUSIVUM_TEST_NOT_SET/keys.yaml".to_string()),
        keys: Some("key1: secret1".to_string()),
        region: Some("here".to_string()),
        turn_cert: Some("cert.pem".to_string()),
        turn_key: Some("key.pem".to_string()),
        node_ip: Some("192.0.2.4".to_string()),
        udp_port: Some("9000-9010".to_string()),
        bind: Some(vec!["127.0.0.1".to_string()]),
    };
    let conf = Config::new_with_flags("region: there\nkeys:\n  a: b", true, Some(&flags)).unwrap();
    assert!(conf.development);
    assert_eq!("/keys.yaml", conf.key_file);
    assert_eq!(
        HashMap::from([("key1".to_string(), "secret1".to_string())]),
        conf.keys
    );
    assert_eq!("here", conf.region);
    assert_eq!("cert.pem", conf.turn.cert_file);
    assert_eq!("key.pem", conf.turn.key_file);
    assert_eq!("192.0.2.4", conf.rtc.rtc_config.node_ip.v4);
    assert!(!conf.rtc.rtc_config.node_ip_auto_generated);
    assert_eq!(9000, conf.rtc.rtc_config.udp_port.start);
    assert_eq!(9010, conf.rtc.rtc_config.udp_port.end);
    assert_eq!(vec!["127.0.0.1".to_string()], conf.bind_addresses);
    // what follows from development mode
    assert_eq!("debug", conf.logging.config.level);
    assert_eq!(30002, conf.turn.relay_port_range_end);

    let flags = BaseFlags {
        keys: Some("key1 secret1".to_string()),
        ..Default::default()
    };
    let err = Config::new_with_flags("", true, Some(&flags)).unwrap_err();
    assert_eq!(
        "Could not parse keys, it needs to be exactly, \"key: secret\", including the space",
        err.to_string()
    );
}

// New for musivum: `unmarshalKeys` of the original keeps the values that YAML has as
// strings and drops the rest.
#[test]
fn test_unmarshal_keys_keeps_strings_only() {
    let mut conf = Config::default();
    conf.unmarshal_keys("a: secret\nb: 12345\nc: true\nd:\ne: [x]\nf: \"678\"")
        .unwrap();
    assert_eq!(
        HashMap::from([
            ("a".to_string(), "secret".to_string()),
            ("f".to_string(), "678".to_string()),
        ]),
        conf.keys
    );

    conf.unmarshal_keys("").unwrap();
    assert!(conf.keys.is_empty());
}

// New for musivum: the key file.
#[cfg(unix)]
#[test]
fn test_validate_keys() {
    let dir = TempDir::new();
    let path = dir.join("keys.yaml");

    let mut conf = Config::default();
    assert!(conf.validate_keys().unwrap_err().is(&Error::KeysNotSet));

    conf.key_file = path.clone();
    assert!(matches!(conf.validate_keys().unwrap_err(), Error::Io(_)));

    write_file(&path, "key1: secret1\nkey2: 2222\n", 0o644);
    assert!(
        conf.validate_keys()
            .unwrap_err()
            .is(&Error::KeyFileIncorrectPermission)
    );

    write_file(&path, "key1: secret1\nkey2: 2222\n", 0o640);
    conf.keys = HashMap::from([("old".to_string(), "secret".to_string())]);
    let log = capture_log(tracing::Level::ERROR, || conf.validate_keys().unwrap());
    assert_eq!(
        HashMap::from([
            ("key1".to_string(), "secret1".to_string()),
            ("key2".to_string(), "2222".to_string()),
        ]),
        conf.keys
    );
    assert_eq!(
        2,
        log.matches("secret is too short, should be at least 32 characters for security")
            .count(),
        "{log}"
    );

    conf.development = true;
    let log = capture_log(tracing::Level::ERROR, || conf.validate_keys().unwrap());
    assert_eq!("", log);

    write_file(&path, "", 0o600);
    assert_eq!("EOF", conf.validate_keys().unwrap_err().to_string());
}

// New for musivum: the checks of the limits.
#[test]
fn test_limit_config_checks() {
    use crate::protocol::livekit;
    use std::sync::Arc;

    let limit = LimitConfig {
        max_room_name_length: 4,
        max_participant_identity_length: 4,
        max_participant_name_length: 4,
        max_metadata_size: 4,
        max_attributes_size: 4,
        max_data_blob_key_length: 6,
        max_data_blob_size: 10,
        max_data_track_custom_encoding_length: 3,
        ..Default::default()
    };
    let no_limit = LimitConfig::default();

    assert!(limit.check_room_name_length("room"));
    assert!(!limit.check_room_name_length("rooms"));
    assert!(no_limit.check_room_name_length("rooms"));
    assert!(limit.check_participant_identity_length("anna"));
    assert!(!limit.check_participant_identity_length("annas"));
    assert!(limit.check_participant_name_length("anna"));
    assert!(!limit.check_participant_name_length("annas"));
    assert!(limit.check_metadata_size("meta"));
    assert!(!limit.check_metadata_size("metad"));
    let attributes = |v: &str| HashMap::from([("k".to_string(), v.to_string())]);
    assert!(limit.check_attributes_size(&attributes("val")));
    assert!(!limit.check_attributes_size(&attributes("valu")));
    assert!(no_limit.check_attributes_size(&attributes("value")));

    let generic = |key: &str| livekit::DataBlobKey {
        key: Some(livekit::data_blob_key::Key::Generic(key.to_string())),
    };
    let custom = |custom: &str| livekit::DataTrackSchemaEncoding {
        value: Some(livekit::data_track_schema_encoding::Value::Custom(
            custom.to_string(),
        )),
    };
    let schema = |name: &str, encoding: Option<livekit::DataTrackSchemaEncoding>| {
        livekit::DataTrackSchemaId {
            name: name.to_string(),
            encoding,
        }
    };
    let schema_key = livekit::DataBlobKey {
        key: Some(livekit::data_blob_key::Key::SchemaId(schema(
            "name",
            Some(custom("abc")),
        ))),
    };
    assert_eq!(0, data_blob_key_length(None));
    assert_eq!(
        0,
        data_blob_key_length(Some(&livekit::DataBlobKey::default()))
    );
    assert_eq!(3, data_blob_key_length(Some(&generic("key"))));
    assert_eq!(7, data_blob_key_length(Some(&schema_key)));
    assert!(limit.check_data_blob_key_length(Some(&generic("sixsix"))));
    assert!(!limit.check_data_blob_key_length(Some(&schema_key)));
    assert!(no_limit.check_data_blob_key_length(Some(&schema_key)));

    assert!(limit.check_data_track_custom_encoding_length("abc"));
    assert!(!limit.check_data_track_custom_encoding_length("abcd"));
    let frame = |custom: &str| livekit::DataTrackFrameEncoding {
        value: Some(livekit::data_track_frame_encoding::Value::Custom(
            custom.to_string(),
        )),
    };
    assert!(limit.check_data_track_frame_encoding(None));
    assert!(
        limit.check_data_track_frame_encoding(Some(&livekit::DataTrackFrameEncoding {
            value: Some(livekit::data_track_frame_encoding::Value::WellKnown(1)),
        }))
    );
    assert!(limit.check_data_track_frame_encoding(Some(&frame("abc"))));
    assert!(!limit.check_data_track_frame_encoding(Some(&frame("abcd"))));
    assert!(!no_limit.check_data_track_frame_encoding(Some(&frame(""))));
    assert!(limit.check_data_track_schema_id(None));
    assert!(limit.check_data_track_schema_id(Some(&schema("name", None))));
    assert!(limit.check_data_track_schema_id(Some(&schema("name", Some(custom("abc"))))));
    assert!(!limit.check_data_track_schema_id(Some(&schema("name", Some(custom("abcd"))))));
    assert!(!limit.check_data_track_schema_id(Some(&schema("name", Some(custom(""))))));

    let blob = |key: &str, contents: &[u8]| livekit::DataBlob {
        key: Some(generic(key)),
        contents: contents.to_vec(),
    };
    let blobs = vec![Arc::new(blob("ab", b"cde")), Arc::new(blob("f", b"gh"))];
    assert!(limit.check_data_blobs_size(&blobs));
    assert!(limit.can_add_data_blob(&blobs, &blob("i", b"j")));
    assert!(!limit.can_add_data_blob(&blobs, &blob("i", b"jk")));
    assert!(no_limit.can_add_data_blob(&blobs, &blob("i", b"jk")));
    let blobs = vec![Arc::new(blob("ab", b"cde")), Arc::new(blob("f", b"ghijkl"))];
    assert!(!limit.check_data_blobs_size(&blobs));
    assert!(no_limit.check_data_blobs_size(&blobs));
}

// New for musivum: TURN over TLS, of the server's own or of one that is named.
#[test]
fn test_is_turns_enabled() {
    let mut conf = Config::default();
    assert!(!conf.is_turns_enabled());
    conf.turn.tls_port = 5349;
    assert!(!conf.is_turns_enabled());
    conf.turn.enabled = true;
    assert!(conf.is_turns_enabled());

    let mut conf = Config::default();
    conf.rtc.turn_servers = vec![TurnServer {
        protocol: "udp".to_string(),
        ..Default::default()
    }];
    assert!(!conf.is_turns_enabled());
    conf.rtc.turn_servers.push(TurnServer {
        protocol: "tls".to_string(),
        ..Default::default()
    });
    assert!(conf.is_turns_enabled());
}

// New for musivum: `os.ExpandEnv` and `homedir.Expand` for the path of the key file.
#[test]
fn test_expand_key_file() {
    // set by cargo for every test
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    assert_eq!(
        format!("{manifest_dir}/keys-{manifest_dir}.yaml$"),
        expand_env("$CARGO_MANIFEST_DIR/keys-${CARGO_MANIFEST_DIR}.yaml$")
    );
    assert_eq!("a/b", expand_env("a$MUSIVUM_TEST_NOT_SET/b"));
    assert_eq!("a$/b$ c", expand_env("a$/b$ c"));
    assert_eq!("a${}b$1", expand_env("a${}b$1"));
    assert_eq!("a${MUSIVUM", expand_env("a${MUSIVUM"));

    assert_eq!("", homedir_expand("").unwrap());
    assert_eq!("/etc/keys.yaml", homedir_expand("/etc/keys.yaml").unwrap());
    assert_eq!(
        "cannot expand user-specific home dir",
        homedir_expand("~other/keys.yaml").unwrap_err().to_string()
    );
    if let Ok(home) = std::env::var("HOME")
        && !home.is_empty()
    {
        let home = home.trim_end_matches('/');
        assert_eq!(home, homedir_expand("~").unwrap().trim_end_matches('/'));
        assert_eq!(
            format!("{home}/keys.yaml"),
            homedir_expand("~/keys.yaml").unwrap()
        );
    }
}
