// Ported from LiveKit, pkg/service/turnrelay_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The client is the TURN tests' (`webrtc::turn::testclient`,
// in place of pion's). The relay range of the servers the tests make is 39545..=39559 (ports
// this machine leaves to the tests) instead of 40000..=40100. The fake generator has no
// `AllocateListener` and `AllocateConn` (see `turnrelay.rs`); TestRejectTCPRelayAddressGenerator
// asks the wrapper's own refusals. TestNewTurnServer_RejectsTCPRelay sends the Allocate of a TCP
// relay itself (pion's client has `AllocateTCP`). The tests that make servers on the fixed
// relay range take turns (`testutils::lock_fixed_turn_ports`), as do pion's tests of it.

use std::net::SocketAddr;
use std::time::Duration;

use super::*;
use crate::config::Config;
use crate::mediatransportutil::rtcconfig::{self, NodeIp};
use crate::protocol::livekit;
use crate::service::turn::turn_test::{TURN_TEST_API_KEY, new_test_turn_auth_handler};
use crate::service::{get_turn_auth_handler_func, new_turn_server};
use crate::webrtc::stun::message::CLASS_ERROR_RESPONSE;
use crate::webrtc::turn::internal::proto::{PROTO_TCP, RequestedTransport};
use crate::webrtc::turn::net::{self, UdpConn};
use crate::webrtc::turn::testclient::TestClient;

fn rtc_config(f: impl FnOnce(&mut rtcconfig::RtcConfig)) -> RtcConfig {
    let mut c = RtcConfig::default();
    f(&mut c.rtc_config);
    c
}

#[test]
fn test_get_turn_media_ports() {
    let ports = get_turn_media_ports(&rtc_config(|c| {
        c.udp_port = rtcconfig::PortRange {
            start: 7882,
            end: 7885,
        };
        c.use_stun_port_as_ice = true;
    }));
    assert_eq!(
        vec![
            PortRange {
                start: 7882,
                end: 7885
            },
            PortRange {
                start: 3478,
                end: 3478
            }
        ],
        ports
    );

    let ports = get_turn_media_ports(&rtc_config(|c| {
        c.udp_port = rtcconfig::PortRange {
            start: 7882,
            end: 0,
        };
        c.ice_port_range_start = 50000;
        c.ice_port_range_end = 60000;
    }));
    assert_eq!(
        vec![PortRange {
            start: 50000,
            end: 60000
        }],
        ports
    );

    assert!(
        get_turn_media_ports(&rtc_config(|c| {
            c.udp_port = rtcconfig::PortRange {
                start: 7882,
                end: 0,
            };
            c.force_tcp = true;
        }))
        .is_empty()
    );
}

#[test]
fn test_get_turn_local_ips() {
    let ips = get_turn_local_ips(&rtc_config(|c| {
        c.node_ip = NodeIp {
            v4: "203.0.113.1".to_string(),
            v6: "2001:db8::1".to_string(),
        };
    }))
    .unwrap();
    assert!(ips.contains(&"127.0.0.1".parse::<IpAddr>().unwrap()));
    assert!(ips.contains(&"203.0.113.1".parse::<IpAddr>().unwrap()));
    assert!(ips.contains(&"2001:db8::1".parse::<IpAddr>().unwrap()));
}

#[test]
fn test_turn_relay_policy_allows() {
    let policy = new_turn_relay_policy(
        None,
        HashSet::from(["203.0.113.1".parse::<IpAddr>().unwrap()]),
        vec![
            PortRange {
                start: 7882,
                end: 7882,
            },
            PortRange {
                start: 50000,
                end: 60000,
            },
        ],
    );

    for (addr, allowed) in [
        (Addr::Udp("203.0.113.1:7882".parse().unwrap()), true),
        (
            Addr::Udp("[::ffff:203.0.113.1]:55000".parse().unwrap()),
            true,
        ),
        (Addr::Udp("203.0.113.1:22".parse().unwrap()), false),
        (Addr::Udp("203.0.113.1:443".parse().unwrap()), false),
        (Addr::Udp("203.0.113.1:60001".parse().unwrap()), false),
        (Addr::Udp("198.51.100.7:7882".parse().unwrap()), true),
        (Addr::Udp("198.51.100.7:443".parse().unwrap()), true),
        (Addr::Tcp("198.51.100.7:443".parse().unwrap()), false),
    ] {
        assert_eq!(allowed, policy.allows(addr), "{addr}");
    }
}

struct FakeRelayAddressGenerator {
    conn: Arc<dyn PacketConn>,
}

impl RelayAddressGenerator for FakeRelayAddressGenerator {
    fn validate(&self) -> Result<(), turn::Error> {
        Ok(())
    }

    fn allocate_packet_conn(
        &self,
        _: AllocateListenerConfig,
    ) -> Result<(Arc<dyn PacketConn>, Addr), turn::Error> {
        Ok((self.conn.clone(), self.conn.local_addr()))
    }
}

fn listen_udp() -> Arc<UdpConn> {
    net::listen_packet("udp4", "127.0.0.1:0").unwrap()
}

async fn read_from(conn: &UdpConn, timeout: Duration) -> Result<(Vec<u8>, Addr), String> {
    let mut buf = vec![0u8; 64];
    match tokio::time::timeout(timeout, conn.read_from(&mut buf)).await {
        Ok(Ok((n, from))) => Ok((buf[..n].to_vec(), from)),
        Ok(Err(err)) => Err(err.to_string()),
        Err(_) => Err("i/o timeout".to_string()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_turn_relay_policy_packet_conn() {
    let relay = listen_udp();
    let media_peer = listen_udp();
    let other_peer = listen_udp();

    let media_port = media_peer.local_addr().port();
    let policy = new_turn_relay_policy(
        Some(Arc::new(FakeRelayAddressGenerator {
            conn: relay.clone(),
        })),
        HashSet::from(["127.0.0.1".parse::<IpAddr>().unwrap()]),
        vec![PortRange {
            start: media_port,
            end: media_port,
        }],
    );

    let (conn, _) = policy
        .allocate_packet_conn(AllocateListenerConfig {
            user_id: "PA_x".to_string(),
            ..Default::default()
        })
        .unwrap();

    // writes to a non-media port on this host are dropped
    let n = conn
        .write_to(b"denied", other_peer.local_addr())
        .await
        .unwrap();
    assert_eq!("denied".len(), n);
    assert!(
        read_from(&other_peer, Duration::from_millis(100))
            .await
            .is_err()
    );

    conn.write_to(b"allowed", media_peer.local_addr())
        .await
        .unwrap();
    let (got, _) = read_from(&media_peer, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(b"allowed", &got[..]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reject_tcp_relay_address_generator() {
    let relay = listen_udp();
    let g = new_reject_tcp_relay_address_generator(Arc::new(FakeRelayAddressGenerator {
        conn: relay.clone(),
    }));

    assert_eq!(
        Err(Error::TurnTcpRelayDisabled),
        g.allocate_listener(AllocateListenerConfig::default())
    );
    assert_eq!(
        Err(Error::TurnTcpRelayDisabled),
        g.allocate_conn(AllocateListenerConfig::default())
    );

    let (conn, _) = g
        .allocate_packet_conn(AllocateListenerConfig::default())
        .unwrap();
    let relay: Arc<dyn PacketConn> = relay;
    assert!(Arc::ptr_eq(&relay, &conn));
}

// allocateTestTurnRelay starts a TURN server on loopback, with the RTC media
// port set to mediaPort, and returns a relay allocated through it.
async fn allocate_test_turn_relay(
    standalone: bool,
    media_port: u16,
) -> (Arc<turn::Server>, TestClient, SocketAddr) {
    let turn_port = listen_udp();
    let turn_addr = turn_port.local_addr().socket_addr();
    turn_port.close().unwrap();
    drop(turn_port);

    let mut conf = Config::default();
    conf.rtc.rtc_config.node_ip = NodeIp {
        v4: "127.0.0.1".to_string(),
        ..Default::default()
    };
    conf.rtc.rtc_config.udp_port = rtcconfig::PortRange {
        start: media_port as i64,
        end: 0,
    };
    conf.turn.enabled = true;
    conf.turn.udp_port = turn_addr.port() as i64;
    conf.turn.bind_addresses = vec!["127.0.0.1".to_string()];
    conf.turn.relay_port_range_start = 39545;
    conf.turn.relay_port_range_end = 39559;
    conf.turn.allow_restricted_peer_cidrs = vec!["127.0.0.0/8".to_string()];
    let h = new_test_turn_auth_handler();
    let server = new_turn_server(&conf, Some(get_turn_auth_handler_func(&h)), standalone)
        .unwrap()
        .unwrap();

    let p_id = livekit::ParticipantId::new("PA_relay");
    let (username, expiry) = h.create_username(TURN_TEST_API_KEY, &p_id, 300);
    let password = h.create_password(TURN_TEST_API_KEY, &p_id, expiry).unwrap();

    let mut client = TestClient::udp(turn_addr, &username, &password).await;
    let relay_conn = client.allocate().await.unwrap();
    (server, client, relay_conn)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_turn_server_relay_policy() {
    let _ports = crate::testutils::lock_fixed_turn_ports().await;
    let media_peer = listen_udp();
    let other_peer = listen_udp();
    let (server, mut relay_conn, _) =
        allocate_test_turn_relay(false, media_peer.local_addr().port()).await;

    relay_conn
        .write_to(b"denied", other_peer.local_addr().socket_addr())
        .await
        .unwrap();
    relay_conn
        .write_to(b"allowed", media_peer.local_addr().socket_addr())
        .await
        .unwrap();

    let (got, relay_addr) = read_from(&media_peer, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(b"allowed", &got[..]);

    assert!(
        read_from(&other_peer, Duration::from_millis(200))
            .await
            .is_err(),
        "relay to a non-media port on this host must be dropped"
    );

    // the media peer can reply through the relay
    media_peer.write_to(b"reply", relay_addr).await.unwrap();
    let (got, from) = relay_conn.read_from(Duration::from_secs(2)).await.unwrap();
    assert_eq!(b"reply", &got[..]);
    assert_eq!(media_peer.local_addr().socket_addr(), from);

    let _ = server.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_turn_server_standalone_relay_policy() {
    let _ports = crate::testutils::lock_fixed_turn_ports().await;
    let media_peer = listen_udp();
    let other_peer = listen_udp();
    let (server, mut relay_conn, _) =
        allocate_test_turn_relay(true, media_peer.local_addr().port()).await;

    // standalone TURN has no SFU on the host, so ports on this host are not limited
    relay_conn
        .write_to(b"allowed", other_peer.local_addr().socket_addr())
        .await
        .unwrap();

    let (got, _) = read_from(&other_peer, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(b"allowed", &got[..]);

    let _ = server.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_turn_server_rejects_tcp_relay() {
    let _ports = crate::testutils::lock_fixed_turn_ports().await;
    for standalone in [false, true] {
        let ln = net::listen("tcp4", "127.0.0.1:0").unwrap();
        let turn_addr = net::Listener::addr(&ln).socket_addr();
        net::Listener::close(&ln).unwrap();
        drop(ln);

        let mut conf = Config::default();
        conf.rtc.rtc_config.node_ip = NodeIp {
            v4: "127.0.0.1".to_string(),
            ..Default::default()
        };
        conf.rtc.rtc_config.udp_port = rtcconfig::PortRange {
            start: 7882,
            end: 0,
        };
        conf.turn.enabled = true;
        conf.turn.domain = "turn.example.com".to_string();
        conf.turn.tls_port = turn_addr.port() as i64;
        conf.turn.external_tls = true;
        conf.turn.bind_addresses = vec!["127.0.0.1".to_string()];
        conf.turn.relay_port_range_start = 39545;
        conf.turn.relay_port_range_end = 39559;
        conf.turn.allow_restricted_peer_cidrs = vec!["127.0.0.0/8".to_string()];
        let h = new_test_turn_auth_handler();
        let server = new_turn_server(&conf, Some(get_turn_auth_handler_func(&h)), standalone)
            .unwrap()
            .unwrap();

        let p_id = livekit::ParticipantId::new("PA_tcp");
        let (username, expiry) = h.create_username(TURN_TEST_API_KEY, &p_id, 300);
        let password = h.create_password(TURN_TEST_API_KEY, &p_id, expiry).unwrap();

        let tcp_conn = tokio::net::TcpStream::connect(turn_addr).await.unwrap();
        let local = tcp_conn.local_addr().unwrap();
        let mut client =
            TestClient::stream(Box::new(tcp_conn), local, turn_addr, &username, &password);

        let resp = client
            .allocate_with(&[&RequestedTransport {
                protocol: PROTO_TCP,
            }])
            .await
            .unwrap();
        assert_eq!(
            CLASS_ERROR_RESPONSE, resp.typ.class,
            "standalone={standalone}: TCP relay allocation must be refused"
        );

        // a UDP relay over the same TCP connection still works
        let relay_conn = client.allocate().await;
        assert!(
            relay_conn.is_ok(),
            "standalone={standalone}: {relay_conn:?}"
        );

        let _ = server.close();
    }
}
