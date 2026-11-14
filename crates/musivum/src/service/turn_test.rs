// Ported from LiveKit, pkg/service/turn_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. A connection the listener closed reads as the end of
// the stream (`Ok(0)`), which is Go's `io.EOF` error. TestNewTurnServer_InvalidPeerCIDRFailsStartup
// has no bind addresses, as there: nothing is bound.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::protocol::auth::SimpleKeyProvider;
use crate::webrtc::stun::message::{
    METHOD_CHANNEL_BIND, METHOD_CREATE_PERMISSION, METHOD_REFRESH, METHOD_SEND, Method,
};

pub(crate) const TURN_TEST_API_KEY: &str = "APITestKey";
const TURN_TEST_API_SECRET: &str = "TestSecret";

pub(crate) fn new_test_turn_auth_handler() -> Arc<TurnAuthHandler> {
    new_turn_auth_handler(Arc::new(SimpleKeyProvider::new(
        TURN_TEST_API_KEY,
        TURN_TEST_API_SECRET,
    )))
}

fn must_auth_creds(
    h: &TurnAuthHandler,
    p_id: &livekit::ParticipantId,
    ttl_seconds: i64,
) -> (String, Vec<u8>) {
    let (username, expiry) = h.create_username(TURN_TEST_API_KEY, p_id, ttl_seconds);
    let password = h.create_password(TURN_TEST_API_KEY, p_id, expiry).unwrap();
    let key = turn::generate_auth_key(&username, LIVEKIT_REALM, &password);
    (username, key)
}

fn request(username: &str, method: Method) -> RequestAttributes {
    RequestAttributes {
        username: username.to_string(),
        realm: LIVEKIT_REALM.to_string(),
        src_addr: Addr::Udp("127.0.0.1:1234".parse().unwrap()),
        tls: None,
        method,
    }
}

const ALL_METHODS: [Method; 5] = [
    METHOD_ALLOCATE,
    METHOD_REFRESH,
    METHOD_CREATE_PERMISSION,
    METHOD_CHANNEL_BIND,
    METHOD_SEND,
];

#[test]
fn test_turn_auth_handler_handle_auth_valid_credentials() {
    let h = new_test_turn_auth_handler();
    let p_id = livekit::ParticipantId::new("PA_valid");
    let (username, expected_key) = must_auth_creds(&h, &p_id, 300);

    for method in ALL_METHODS {
        let (user_id, key, ok) = h.handle_auth(&request(&username, method));
        assert!(ok, "{method}");
        assert_eq!(p_id.as_str(), user_id, "{method}");
        assert_eq!(expected_key, key, "{method}");
    }
}

#[test]
fn test_turn_auth_handler_handle_auth_expired_allocate_rejected() {
    let h = new_test_turn_auth_handler();
    let p_id = "PA_expired_alloc";

    let expiry = (time::now() + -Duration::from_secs(60)).unix_secs();
    let username =
        base62::encode_to_string(format!("{TURN_TEST_API_KEY}|{p_id}|{expiry}").as_bytes());
    let (_, _, ok) = h.handle_auth(&request(&username, METHOD_ALLOCATE));
    assert!(
        !ok,
        "Allocate request with expired credentials must be rejected"
    );
}

#[test]
fn test_turn_auth_handler_handle_auth_expired_non_allocate_allowed() {
    let h = new_test_turn_auth_handler();
    let p_id = livekit::ParticipantId::new("PA_expired_refresh");

    let expiry = (time::now() + -Duration::from_secs(60)).unix_secs();
    let username = base62::encode_to_string(
        format!("{TURN_TEST_API_KEY}|{}|{expiry}", p_id.as_str()).as_bytes(),
    );

    // CreatePassword still enforces ErrExpired on its own, but the server hands
    // the same key it generated at allocation time — reproduce that by directly
    // hashing without going through CreatePassword's expiry guard.
    let password = h
        .compute_password(TURN_TEST_API_KEY, &p_id, expiry)
        .unwrap();
    let expected_key = turn::generate_auth_key(&username, LIVEKIT_REALM, &password);

    for method in [
        METHOD_REFRESH,
        METHOD_CREATE_PERMISSION,
        METHOD_CHANNEL_BIND,
        METHOD_SEND,
    ] {
        let (user_id, key, ok) = h.handle_auth(&request(&username, method));
        assert!(
            ok,
            "{method}: Non-allocate request with expired credentials must succeed"
        );
        assert_eq!(p_id.as_str(), user_id, "{method}");
        assert_eq!(expected_key, key, "{method}");
    }
}

#[test]
fn test_turn_auth_handler_handle_auth_wrong_username_rejected() {
    let h = new_test_turn_auth_handler();
    let (_, _, ok) = h.handle_auth(&request("not-base62!!!", METHOD_REFRESH));
    assert!(!ok);
}

#[test]
fn test_turn_auth_handler_handle_auth_two_part_username_rejected() {
    let h = new_test_turn_auth_handler();
    let p_id = "PA_two_part";

    let username = base62::encode_to_string(format!("{TURN_TEST_API_KEY}|{p_id}").as_bytes());

    for method in ALL_METHODS {
        let (_, _, ok) = h.handle_auth(&request(&username, method));
        assert!(!ok, "{method}: Two-part username must be rejected");
    }
}

#[test]
fn test_turn_auth_handler_handle_auth_zero_expiry_rejected() {
    let h = new_test_turn_auth_handler();
    let p_id = "PA_zero_expiry";

    let username = base62::encode_to_string(format!("{TURN_TEST_API_KEY}|{p_id}|{}", 0).as_bytes());

    for method in ALL_METHODS {
        let (_, _, ok) = h.handle_auth(&request(&username, method));
        assert!(!ok, "{method}: Username with expiry=0 must be rejected");
    }
}

#[test]
fn test_turn_auth_handler_parse_username_two_part_rejected() {
    let h = new_test_turn_auth_handler();
    let p_id = "PA_parse_two_part";

    let username = base62::encode_to_string(format!("{TURN_TEST_API_KEY}|{p_id}").as_bytes());

    assert!(h.parse_username(&username).is_err());
}

#[test]
fn test_turn_auth_handler_parse_username_zero_expiry_rejected() {
    let h = new_test_turn_auth_handler();
    let p_id = "PA_parse_zero_expiry";

    let username = base62::encode_to_string(format!("{TURN_TEST_API_KEY}|{p_id}|{}", 0).as_bytes());

    assert_eq!(Err(Error::Expired), h.parse_username(&username));
}

#[test]
fn test_turn_auth_handler_create_password_zero_expiry_rejected() {
    let h = new_test_turn_auth_handler();
    let p_id = livekit::ParticipantId::new("PA_password_zero_expiry");

    assert_eq!(
        Err(Error::Expired),
        h.create_password(TURN_TEST_API_KEY, &p_id, 0)
    );
}

#[test]
fn test_parse_peer_cidrs() {
    // valid entries are compiled
    {
        let nets = parse_peer_cidrs(
            "turn.deny_peer_cidrs",
            &["203.0.113.0/24".to_string(), "10.0.0.0/8".to_string()],
        )
        .unwrap();
        assert_eq!(2, nets.len());
        assert!(nets[0].contains("203.0.113.5".parse().unwrap()));
        assert!(!nets[0].contains("203.0.114.5".parse().unwrap()));
    }

    // empty list is fine
    {
        let nets = parse_peer_cidrs("turn.deny_peer_cidrs", &[]).unwrap();
        assert!(nets.is_empty());
    }

    // invalid entry is rejected with field context
    {
        let err =
            parse_peer_cidrs("turn.deny_peer_cidrs", &["203.0.113.0/33".to_string()]).unwrap_err();
        assert!(err.to_string().contains("turn.deny_peer_cidrs"));
        assert!(err.to_string().contains("203.0.113.0/33"));
    }
}

#[test]
fn test_new_turn_server_invalid_peer_cidr_fails_startup() {
    type Mut = fn(&mut Config);
    for (name, mut_id, field) in [
        (
            "invalid deny cidr",
            (|c: &mut Config| c.turn.deny_peer_cidrs = vec!["203.0.113.0/33".to_string()]) as Mut,
            "turn.deny_peer_cidrs",
        ),
        (
            "invalid allow cidr",
            (|c: &mut Config| c.turn.allow_restricted_peer_cidrs = vec!["not-a-cidr".to_string()])
                as Mut,
            "turn.allow_restricted_peer_cidrs",
        ),
    ] {
        let mut conf = Config::default();
        conf.turn.enabled = true;
        conf.turn.udp_port = 3478;
        mut_id(&mut conf);

        let err = new_turn_server(&conf, None, false).err();
        assert!(err.is_some(), "{name}");
        assert!(err.unwrap().to_string().contains(field), "{name}");
    }
}

#[test]
fn test_turn_auth_handler_create_username_ttl_clamped() {
    let h = new_test_turn_auth_handler();
    let p_id = livekit::ParticipantId::new("PA_ttl_clamp");

    // An overflowing TTL must not wrap into a past expiry; it clamps to the max.
    let (_, overflow_expiry) = h.create_username(TURN_TEST_API_KEY, &p_id, (1 << 62) + 1);
    assert!(overflow_expiry > time::now().unix_secs());
    assert!(overflow_expiry <= time::now().unix_secs() + config::TURN_MAX_TTL_SECONDS + 1);

    // A non-positive TTL falls back to the default rather than producing a past/wrapped expiry.
    let (_, negative_expiry) = h.create_username(TURN_TEST_API_KEY, &p_id, -1 << 40);
    crate::testutils::assert_in_delta(
        (time::now().unix_secs() + config::DEFAULT_TURN_TTL_SECONDS) as f64,
        negative_expiry as f64,
        2.0,
    );
}

fn proxy_protocol_turn_config(trusted_cidrs: &[&str]) -> TurnConfig {
    TurnConfig {
        external_tls: true,
        proxy_protocol: true,
        proxy_protocol_trusted_cidrs: trusted_cidrs.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_turn_tcp_listener_proxy_protocol() {
    let listener: Arc<dyn Listener> = Arc::from(
        new_turn_tcp_listener(&proxy_protocol_turn_config(&["127.0.0.0/8"]), "127.0.0.1:0")
            .unwrap(),
    );

    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    {
        let listener = listener.clone();
        tokio::spawn(async move {
            let Ok(mut conn) = listener.accept().await else {
                let _ = accepted_tx.send(None);
                return;
            };
            // the PROXY header is consumed lazily, on the first read
            let mut buf = [0u8; 1];
            let _ = conn.read(&mut buf).await;
            let _ = accepted_tx.send(Some(conn.remote_addr()));
        });
    }

    let mut conn = tokio::net::TcpStream::connect(listener.addr().socket_addr())
        .await
        .unwrap();
    conn.write_all(b"PROXY TCP4 203.0.113.9 127.0.0.1 40123 443\r\nx")
        .await
        .unwrap();

    let addr = tokio::time::timeout(std::time::Duration::from_secs(5), accepted_rx)
        .await
        .expect("timed out waiting for the accepted connection")
        .unwrap();
    assert!(addr.is_some());
    assert_eq!("203.0.113.9:40123", addr.unwrap().to_string());
    let _ = listener.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_turn_tcp_listener_proxy_protocol_rejects_bare_connection() {
    let listener: Arc<dyn Listener> = Arc::from(
        new_turn_tcp_listener(&proxy_protocol_turn_config(&["127.0.0.0/8"]), "127.0.0.1:0")
            .unwrap(),
    );

    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    {
        let listener = listener.clone();
        tokio::spawn(async move {
            let mut conn = match listener.accept().await {
                Ok(conn) => conn,
                Err(err) => {
                    let _ = result_tx.send(Err(err));
                    return;
                }
            };
            let mut buf = [0u8; 1];
            let _ = result_tx.send(conn.read(&mut buf).await);
        });
    }

    let mut conn = tokio::net::TcpStream::connect(listener.addr().socket_addr())
        .await
        .unwrap();
    conn.write_all(b"not a proxy header\r\n").await.unwrap();

    let r = tokio::time::timeout(std::time::Duration::from_secs(5), result_rx)
        .await
        .expect("timed out waiting for the rejected connection")
        .unwrap();
    assert!(r.is_err());
    let _ = listener.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_turn_tcp_listener_without_proxy_protocol() {
    let listener: Arc<dyn Listener> = Arc::from(
        new_turn_tcp_listener(
            &TurnConfig {
                external_tls: true,
                ..Default::default()
            },
            "127.0.0.1:0",
        )
        .unwrap(),
    );

    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    {
        let listener = listener.clone();
        tokio::spawn(async move {
            let Ok(conn) = listener.accept().await else {
                let _ = accepted_tx.send(None);
                return;
            };
            let _ = accepted_tx.send(Some(conn.remote_addr()));
            drop(conn);
        });
    }

    let conn = tokio::net::TcpStream::connect(listener.addr().socket_addr())
        .await
        .unwrap();

    let addr = tokio::time::timeout(std::time::Duration::from_secs(5), accepted_rx)
        .await
        .expect("timed out waiting for the accepted connection")
        .unwrap();
    assert!(addr.is_some());
    assert_eq!(
        conn.local_addr().unwrap().to_string(),
        addr.unwrap().to_string()
    );
    let _ = listener.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_turn_tcp_listener_proxy_protocol_closes_untrusted_proxy() {
    let listener: Arc<dyn Listener> = Arc::from(
        new_turn_tcp_listener(
            &proxy_protocol_turn_config(&["203.0.113.0/24"]),
            "127.0.0.1:0",
        )
        .unwrap(),
    );

    let (accepted_tx, mut accepted_rx) = tokio::sync::mpsc::unbounded_channel();
    {
        let listener = listener.clone();
        tokio::spawn(async move {
            if let Ok(conn) = listener.accept().await {
                drop(conn);
                let _ = accepted_tx.send(());
            }
        });
    }

    let mut conn = tokio::net::TcpStream::connect(listener.addr().socket_addr())
        .await
        .unwrap();
    let _ = conn
        .write_all(b"PROXY TCP4 203.0.113.9 127.0.0.1 40123 443\r\nx")
        .await;

    let r = tokio::time::timeout(std::time::Duration::from_secs(5), conn.read(&mut [0u8; 1]))
        .await
        .expect("the listener should have closed the connection");
    assert!(
        !matches!(r, Ok(n) if n > 0),
        "the listener should have closed the connection"
    );

    let accepted =
        tokio::time::timeout(std::time::Duration::from_millis(200), accepted_rx.recv()).await;
    assert!(
        accepted.is_err(),
        "connection from an untrusted proxy must not be accepted"
    );
    let _ = listener.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_turn_tcp_listener_proxy_protocol_requires_trusted_cidrs() {
    assert!(new_turn_tcp_listener(&proxy_protocol_turn_config(&[]), "127.0.0.1:0").is_err());

    assert!(
        new_turn_tcp_listener(&proxy_protocol_turn_config(&["not-a-cidr"]), "127.0.0.1:0").is_err()
    );
}
