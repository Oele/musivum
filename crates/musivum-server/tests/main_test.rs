// Ported from LiveKit, cmd/server/main_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `TestMainExitCode` only (the other tests of the file
// are in src/main_test.rs). The program is the one cargo built, run with an empty
// environment, where the original runs its own test binary. Two texts are the command line
// parser's: clap says "unexpected argument '--unknown-flag'" where urfave says "flag
// provided but not defined", and the version is musivum-server's. The tests after it are
// new: the environment of the program, through commands that print what it gave.

use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_musivum-server");

fn run(args: &[&str], env: &[(&str, &str)]) -> (i32, String) {
    let output = Command::new(BIN)
        .args(args)
        // Run the real entry point without inheriting server configuration.
        .env_clear()
        .envs(env.iter().copied())
        .output()
        .unwrap();
    let mut text = String::from_utf8_lossy(&output.stdout).to_string();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    (output.status.code().unwrap(), text)
}

#[test]
fn test_main_exit_code() {
    let missing_config = concat!(env!("CARGO_TARGET_TMPDIR"), "/missing.yaml");

    let cases: [(&str, &[&str], i32, &str); 5] = [
        (
            "missing config",
            &["--config", missing_config, "ports"],
            1,
            missing_config,
        ),
        (
            "unknown flag",
            &["--unknown-flag"],
            1,
            "unexpected argument '--unknown-flag'",
        ),
        ("invalid flag value", &["--dev=invalid"], 1, "invalid value"),
        ("help", &["--help"], 0, "High performance WebRTC server"),
        ("version", &["--version"], 0, env!("CARGO_PKG_VERSION")),
    ];
    for (name, args, exit_code, output) in cases {
        let (code, text) = run(args, &[]);
        assert_eq!(exit_code, code, "{name}: {text}");
        assert!(text.contains(output), "{name}: {text}");
    }
}

#[test]
fn the_configuration_and_the_keys_come_from_the_environment() {
    let (code, text) = run(
        &["ports"],
        &[(
            "LIVEKIT_CONFIG",
            "port: 39598\nrtc: {node_ip: 127.0.0.1, tcp_port: 0, udp_port: 39597}",
        )],
    );
    assert_eq!(0, code, "{text}");
    assert!(text.contains("39598 - HTTP service\n"), "{text}");
    assert!(text.contains("%!s(int=39597) - ICE/UDP\n"), "{text}");

    let (code, text) = run(
        &["create-join-token", "--room", "r", "--identity", "i"],
        &[
            ("LIVEKIT_CONFIG", "rtc: {node_ip: 127.0.0.1}"),
            ("LIVEKIT_KEYS", "fromenv: secret"),
        ],
    );
    assert_eq!(0, code, "{text}");
    let token = text
        .lines()
        .find_map(|l| l.strip_prefix("Token: "))
        .unwrap();
    let v = musivum::protocol::auth::parse_api_token(token).unwrap();
    assert_eq!("fromenv", v.api_key());
    assert!(v.verify("secret").is_ok());
}
