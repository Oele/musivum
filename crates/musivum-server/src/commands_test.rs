// New for musivum: upstream has no test of these commands. The texts printed and the
// claims of the token are those of LiveKit's server at the pin for the same command lines.

use clap::Parser;
use musivum::protocol::auth::parse_api_token;

use super::*;
use crate::Cli;

fn flags(args: &[&str]) -> Flags {
    Cli::try_parse_from(std::iter::once("musivum-server").chain(args.iter().copied()))
        .unwrap()
        .flags
}

#[test]
fn generate_keys_prints_a_key_and_a_secret() {
    let mut out = Vec::new();
    generate_keys(&mut out).unwrap();
    let out = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(2, lines.len(), "{out}");
    let key = lines[0].strip_prefix("API Key:  ").unwrap();
    let secret = lines[1].strip_prefix("API Secret:  ").unwrap();
    // guid.New(utils.APIKeyPrefix): the prefix and twelve characters
    assert!(key.starts_with("API"), "{key}");
    assert_eq!(15, key.len(), "{key}");
    // utils.RandomSecret: 32 random bytes in base62
    assert!((43..=52).contains(&secret.len()), "{secret}");
    assert!(
        secret.bytes().all(|c| c.is_ascii_alphanumeric()),
        "{secret}"
    );
}

#[test]
fn create_join_token_makes_a_token_the_server_takes() {
    let secret = "a-secret-of-at-least-thirty-two-characters";
    let keys = format!("key1: {secret}");
    let c = flags(&[
        "--keys",
        &keys,
        "--config-body",
        "rtc: {node_ip: 127.0.0.1}",
    ]);

    let mut out = Vec::new();
    create_token(&c, "room1", "alice", false, &mut out).unwrap();
    let out = String::from_utf8(out).unwrap();
    let token = out.trim_end().strip_prefix("Token: ").unwrap();

    let v = parse_api_token(token).unwrap();
    assert_eq!("key1", v.api_key());
    assert_eq!("alice", v.identity());
    let (claims, grants) = v.verify(secret).unwrap();
    let video = grants.video.unwrap();
    assert!(video.room_join);
    assert_eq!("room1", video.room);
    assert!(!video.hidden);
    assert!(!video.recorder);
    assert_eq!(None, video.can_publish);
    // valid for 30 days
    let exp = claims.expires_at.unwrap().unix();
    let nbf = claims.not_before.unwrap().unix();
    assert_eq!(30 * 24 * 60 * 60, exp - nbf);
    // another secret does not verify it
    assert!(v.verify("another-secret-of-thirty-two-characters").is_err());

    // a recorder: hidden, subscribing only
    let mut out = Vec::new();
    create_token(&c, "room1", "rec", true, &mut out).unwrap();
    let out = String::from_utf8(out).unwrap();
    let token = out.trim_end().strip_prefix("Token: ").unwrap();
    let (_, grants) = parse_api_token(token).unwrap().verify(secret).unwrap();
    let video = grants.video.unwrap();
    assert!(video.room_join);
    assert!(video.hidden);
    assert!(video.recorder);
    assert_eq!(Some(false), video.can_publish);
    assert_eq!(Some(false), video.can_publish_data);
}

#[test]
fn create_join_token_needs_keys() {
    // no keys and no key file: Go's "stat : no such file or directory"
    let c = flags(&["--config-body", "rtc: {node_ip: 127.0.0.1}"]);
    let err = create_token(&c, "room1", "alice", false, &mut Vec::new()).unwrap_err();
    assert!(err.to_string().starts_with("stat : "), "{err}");
}

#[test]
fn ports_prints_what_the_configuration_uses() {
    // the configuration of the call test; the single UDP port is printed as the original
    // prints it
    let c = flags(&[
        "--config-body",
        "port: 39500\nbind_addresses: [127.0.0.1]\nrtc: {tcp_port: 39501, udp_port: 39502, node_ip: 127.0.0.1}",
    ]);
    let mut out = Vec::new();
    print_ports(&c, &mut out).unwrap();
    assert_eq!(
        "TCP Ports\n39500 - HTTP service\n39501 - ICE/TCP\nUDP Ports\n%!s(int=39502) - ICE/UDP\n",
        String::from_utf8(out).unwrap()
    );

    let c = flags(&[
        "--config-body",
        "rtc: {node_ip: 127.0.0.1, force_tcp: true}\nturn: {enabled: true, tls_port: 5349, udp_port: 3478, domain: x}",
    ]);
    let mut out = Vec::new();
    print_ports(&c, &mut out).unwrap();
    assert_eq!(
        "TCP Ports\n7880 - HTTP service\n7881 - ICE/TCP\n5349 - TURN/TLS\nUDP Ports\n50000-60000 - ICE/UDP range\n3478 - TURN/UDP\n",
        String::from_utf8(out).unwrap()
    );
}
