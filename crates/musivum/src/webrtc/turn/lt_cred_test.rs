// Ported from pion/turn, lt_cred_test.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The server listens on an ephemeral port of the loopback
// interface, not on 127.0.0.1:3478 (a TURN server may already run on the machine); the client is
// the tests' (`testclient.rs`).

use std::net::IpAddr;
use std::sync::Arc;

use super::*;
use crate::webrtc::turn::net::{self, PacketConn};
use crate::webrtc::turn::testclient::TestClient;
use crate::webrtc::turn::{
    PacketConnConfig, RelayAddressGenerator, RelayAddressGeneratorStatic, RequestAttributes,
    Server, ServerConfig,
};

#[test]
fn test_lt_cred_mech() {
    let username = "1599491771";
    let shared_secret = "foobar";

    let expected_password = "Tpz/nKkyvX/vMSLKvL4sbtBt8Vs=";
    let actual_password = long_term_credentials(username, shared_secret);
    assert_eq!(expected_password, actual_password);
}

fn new_test_server(auth_handler: AuthHandler) -> (Arc<Server>, std::net::SocketAddr) {
    let server_listener = net::listen_packet("udp4", "127.0.0.1:0").unwrap();
    let addr = server_listener.local_addr().socket_addr();

    let server = Server::new(ServerConfig {
        auth_handler: Some(auth_handler),
        packet_conn_configs: vec![PacketConnConfig {
            packet_conn: Some(server_listener),
            relay_address_generator: Some(Arc::new(RelayAddressGeneratorStatic {
                relay_address: Some("127.0.0.1".parse::<IpAddr>().unwrap()),
                address: "127.0.0.1".to_string(),
            }) as Arc<dyn RelayAddressGenerator>),
            ..Default::default()
        }],
        realm: "pion.ly".to_string(),
        logger_factory: Some(Logger::default()),
        ..Default::default()
    })
    .unwrap();

    (server, addr)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_long_term_auth_handler() {
    const SHARED_SECRET: &str = "HELLO_WORLD";

    let (server, addr) = new_test_server(new_long_term_auth_handler(SHARED_SECRET, None));

    let (username, password) =
        generate_long_term_credentials(SHARED_SECRET, Duration::from_secs(60));

    let mut client = TestClient::udp(addr, &username, &password).await;

    let relay_conn = client.allocate().await;
    assert!(relay_conn.is_ok(), "{relay_conn:?}");

    assert!(server.close().is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_long_term_turn_rest_auth_handler() {
    const SHARED_SECRET: &str = "HELLO_WORLD";

    let (server, addr) = new_test_server(long_term_turn_rest_auth_handler(SHARED_SECRET, None));

    let (username, password) = generate_long_term_turn_rest_credentials(
        SHARED_SECRET,
        "testuser",
        Duration::from_secs(60),
    );

    let mut client = TestClient::udp(addr, &username, &password).await;

    let relay_conn = client.allocate().await;
    assert!(relay_conn.is_ok(), "{relay_conn:?}");

    assert!(server.close().is_ok());
}

// TestLongTermTURNRESTAuthHandler_NoColon verifies that a username without a colon
// is rejected cleanly (authentication failure, no panic) — regression test for
// the out-of-bounds panic fixed in LongTermTURNRESTAuthHandler.
// The username has no colon so userID falls back to the full username; the
// timestamp (1700000000) is in the past so authentication fails as expired.
#[test]
fn test_long_term_turn_rest_auth_handler_no_colon() {
    let handler = long_term_turn_rest_auth_handler("secret", None);
    let (_, _, ok) = handler(&RequestAttributes {
        username: "1700000000".to_string(), // no colon — previously caused an index out-of-range panic
        realm: "pion.ly".to_string(),
        src_addr: net::Addr::Udp("0.0.0.0:0".parse().unwrap()),
        tls: None,
        method: crate::webrtc::stun::message::METHOD_ALLOCATE,
    });
    assert!(!ok, "username without colon must not authenticate");
}
