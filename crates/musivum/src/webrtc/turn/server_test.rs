// Ported from pion/turn, server_test.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust.
// - The client is the tests' (`testclient.rs`, in place of pion's): what is asserted on pion's
//   error texts ("Allocate error response (error 400: )") is asserted on the error codes.
// - The servers listen on ephemeral ports of the loopback interface, never on 3478 (a TURN
//   server of this machine may have it); the second client of "Filter on client address and
//   peer IP" has an ephemeral port as well. TestRelayAddressGeneratorPortRange allocates in
//   39545..=39559 (ports this machine leaves to the tests, held while it runs:
//   `testutils::lock_fixed_turn_ports`), its one port is 39545, and "Range"
//   makes 5 relays where the original makes 25 in a range of a thousand ports.
// - pion's `test.TimeOut` and `test.CheckRoutines` have no counterpart.
// - Not ported: what tests RFC 6062's TCP relays (`AllocateListener` in TestServerConfig,
//   TestRelayAddressGeneratorPortRange and TestNilAddressGenerator), and TestServerVNet, which
//   runs on pion's virtual network (vnet), with its helpers (`VNet`, `buildVNet`, the event
//   handler checks). The certificate of TestTLSHandshakeConnectionState is identified by its DER
//   instead of its common name (the port has no parser of X.509); the CA verifies it in the
//   handshake, as there.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use parking_lot::Mutex;

use super::*;
use crate::webrtc::stun::message::{BINDING_REQUEST, CLASS_ERROR_RESPONSE, Message};
use crate::webrtc::turn::internal::allocation::{AllocateListenerConfig, EventHandler};
use crate::webrtc::turn::internal::proto::{
    DEFAULT_LIFETIME_GO, EvenPort, PROTO_UDP, RelayedAddress, RequestedTransport, ReservationToken,
};
use crate::webrtc::turn::net::{self, BoxFuture, UdpConn};
use crate::webrtc::turn::testclient::{TIMEOUT, TestClient, error_code_of};
use crate::webrtc::turn::{
    AuthHandler, RelayAddressGeneratorNone, RelayAddressGeneratorPortRange,
    RelayAddressGeneratorStatic, RequestAttributes, generate_auth_key,
};
use rtc::stun::attributes::{
    ATTR_CHANNEL_NUMBER, ATTR_DONT_FRAGMENT, ATTR_ERROR_CODE, ATTR_UNKNOWN_ATTRIBUTES, AttrType,
    RawAttribute,
};

const TEST_USER: &str = "testUser";

// mtuConn enforces a maxMTU limit and returns an error on overflow.
struct MtuConn {
    conn: Arc<dyn PacketConn>,
    mtu: usize,
}

impl PacketConn for MtuConn {
    fn read_from<'a>(&'a self, p: &'a mut [u8]) -> BoxFuture<'a, std::io::Result<(usize, Addr)>> {
        self.conn.read_from(p)
    }

    fn write_to<'a>(&'a self, data: &'a [u8], addr: Addr) -> BoxFuture<'a, std::io::Result<usize>> {
        if data.len() > self.mtu {
            return Box::pin(async { Err(std::io::Error::other("MTU violation")) });
        }

        self.conn.write_to(data, addr)
    }

    fn close(&self) -> std::io::Result<()> {
        self.conn.close()
    }

    fn local_addr(&self) -> Addr {
        self.conn.local_addr()
    }
}

// mtuConnGenerator is a relay address generator that wraps the PacketConn returned by a base generator in an mtuConn.
struct MtuConnGenerator {
    relay_address_generator: Arc<dyn RelayAddressGenerator>,
    mtu: usize,
}

impl RelayAddressGenerator for MtuConnGenerator {
    fn validate(&self) -> Result<(), Error> {
        self.relay_address_generator.validate()
    }

    fn allocate_packet_conn(
        &self,
        conf: AllocateListenerConfig,
    ) -> Result<(Arc<dyn PacketConn>, Addr), Error> {
        let (conn, addr) = self.relay_address_generator.allocate_packet_conn(conf)?;

        Ok((
            Arc::new(MtuConn {
                conn,
                mtu: self.mtu,
            }),
            addr,
        ))
    }
}

// truncConn truncates send payloads to a given max length.
struct TruncConn {
    conn: Arc<dyn PacketConn>,
    mtu: usize,
}

impl PacketConn for TruncConn {
    fn read_from<'a>(&'a self, p: &'a mut [u8]) -> BoxFuture<'a, std::io::Result<(usize, Addr)>> {
        self.conn.read_from(p)
    }

    fn write_to<'a>(&'a self, data: &'a [u8], addr: Addr) -> BoxFuture<'a, std::io::Result<usize>> {
        if data.len() > self.mtu {
            return self.conn.write_to(&data[..self.mtu], addr);
        }

        self.conn.write_to(data, addr)
    }

    fn close(&self) -> std::io::Result<()> {
        self.conn.close()
    }

    fn local_addr(&self) -> Addr {
        self.conn.local_addr()
    }
}

// truncConnGenerator is a relay address generator that wraps the PacketConn returned by a base
// generator in a truncConn.
struct TruncConnGenerator {
    relay_address_generator: Arc<dyn RelayAddressGenerator>,
    mtu: usize,
}

impl RelayAddressGenerator for TruncConnGenerator {
    fn validate(&self) -> Result<(), Error> {
        self.relay_address_generator.validate()
    }

    fn allocate_packet_conn(
        &self,
        conf: AllocateListenerConfig,
    ) -> Result<(Arc<dyn PacketConn>, Addr), Error> {
        let (conn, addr) = self.relay_address_generator.allocate_packet_conn(conf)?;

        Ok((
            Arc::new(TruncConn {
                conn,
                mtu: self.mtu,
            }),
            addr,
        ))
    }
}

fn static_generator(address: &str) -> Arc<dyn RelayAddressGenerator> {
    Arc::new(RelayAddressGeneratorStatic {
        relay_address: Some("127.0.0.1".parse().unwrap()),
        address: address.to_string(),
    })
}

fn cred_map_handler(user_id_empty: bool) -> AuthHandler {
    let key = generate_auth_key(TEST_USER, "pion.ly", "pass");
    Arc::new(move |ra: &RequestAttributes| {
        if ra.username == TEST_USER {
            let user_id = if user_id_empty {
                String::new()
            } else {
                ra.username.clone()
            };
            return (user_id, key.clone(), true);
        }

        (String::new(), Vec::new(), false)
    })
}

fn udp_listener() -> (Arc<UdpConn>, SocketAddr) {
    let l = net::listen_packet("udp4", "127.0.0.1:0").unwrap();
    let addr = l.local_addr().socket_addr();
    (l, addr)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_server_config() {
    let (udp_listener, _) = udp_listener();

    let tcp_listener: Arc<dyn Listener> = Arc::new(net::listen("tcp4", "127.0.0.1:0").unwrap());

    // Empty
    assert!(
        matches!(
            ServerConfig::default().validate(),
            Err(Error::NoAvailableConns)
        ),
        "Empty"
    );

    // No PacketConn
    assert!(
        matches!(
            ServerConfig {
                packet_conn_configs: vec![PacketConnConfig::default()],
                ..Default::default()
            }
            .validate(),
            Err(Error::ConnUnset)
        ),
        "No PacketConn"
    );

    // RelayAddressGenerator Error
    assert!(
        matches!(
            ServerConfig {
                packet_conn_configs: vec![PacketConnConfig {
                    packet_conn: Some(udp_listener.clone()),
                    relay_address_generator: Some(Arc::new(RelayAddressGeneratorNone::default())),
                    ..Default::default()
                }],
                ..Default::default()
            }
            .validate(),
            Err(Error::ListeningAddressInvalid)
        ),
        "RelayAddressGenerator Error"
    );

    // No Listener
    assert!(
        matches!(
            ServerConfig {
                listener_configs: vec![ListenerConfig::default()],
                ..Default::default()
            }
            .validate(),
            Err(Error::ListenerUnset)
        ),
        "No Listener"
    );

    // No RelayAddressGenerator
    assert!(
        matches!(
            ServerConfig {
                listener_configs: vec![ListenerConfig {
                    listener: Some(tcp_listener.clone()),
                    ..Default::default()
                }],
                ..Default::default()
            }
            .validate(),
            Err(Error::RelayAddressGeneratorUnset)
        ),
        "No RelayAddressGenerator"
    );

    // Not here: the AllocateListener checks, see the header.

    assert!(tcp_listener.close().is_ok());
    assert!(udp_listener.close().is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_server() {
    let logger_factory = Logger::default();

    // simple
    {
        let (udp_listener, addr) = udp_listener();

        let server = Server::new(ServerConfig {
            auth_handler: Some(cred_map_handler(false)),
            packet_conn_configs: vec![PacketConnConfig {
                packet_conn: Some(udp_listener),
                relay_address_generator: Some(static_generator("127.0.0.1")),
                ..Default::default()
            }],
            realm: "pion.ly".to_string(),
            logger_factory: Some(logger_factory.clone()),
            ..Default::default()
        })
        .unwrap();

        assert_eq!(
            DEFAULT_LIFETIME_GO, server.channel_bind_timeout,
            "should match"
        );

        let mut client = TestClient::udp(addr, "", "").await;

        let r = client.send_binding_request().await;
        assert!(r.is_ok(), "simple: should succeed: {r:?}");

        assert!(server.close().is_ok());
    }

    // default inboundMTU
    {
        let (udp_listener, _) = udp_listener();
        let server = Server::new(ServerConfig {
            logger_factory: Some(logger_factory.clone()),
            packet_conn_configs: vec![PacketConnConfig {
                packet_conn: Some(udp_listener),
                relay_address_generator: Some(static_generator("127.0.0.1")),
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            server.inbound_mtu, DEFAULT_INBOUND_MTU,
            "default inboundMTU"
        );
        assert!(server.close().is_ok());
    }

    // Set inboundMTU
    {
        let (udp_listener, _) = udp_listener();
        let server = Server::new(ServerConfig {
            inbound_mtu: 2000,
            logger_factory: Some(logger_factory.clone()),
            packet_conn_configs: vec![PacketConnConfig {
                packet_conn: Some(udp_listener),
                relay_address_generator: Some(static_generator("127.0.0.1")),
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(server.inbound_mtu, 2000, "Set inboundMTU");
        assert!(server.close().is_ok());
    }

    // Delete allocation on spontaneous TCP close
    {
        // Test whether allocation is properly deleted when client spontaneously closes the
        // TCP connection underlying it
        let tcp_listener = net::listen("tcp4", "127.0.0.1:0").unwrap();
        let server_addr = tcp_listener.addr().socket_addr();

        let server = Server::new(ServerConfig {
            auth_handler: Some(cred_map_handler(false)),
            listener_configs: vec![ListenerConfig {
                listener: Some(Arc::new(tcp_listener)),
                relay_address_generator: Some(static_generator("127.0.0.1")),
                ..Default::default()
            }],
            realm: "pion.ly".to_string(),
            logger_factory: Some(logger_factory.clone()),
            ..Default::default()
        })
        .unwrap();

        let conn = tokio::net::TcpStream::connect(server_addr).await.unwrap();

        let client_addr = conn.local_addr().unwrap();

        let mut client =
            TestClient::stream(Box::new(conn), client_addr, server_addr, TEST_USER, "pass");

        let r = client.send_binding_request().await;
        assert!(r.is_ok(), "should succeed: {r:?}");

        let relay_conn = client.allocate().await;
        assert!(relay_conn.is_ok(), "{relay_conn:?}");

        let five_tuple = FiveTuple {
            protocol: Protocol::Udp, // Fixed UDP
            src_addr: Some(Addr::Tcp(client_addr)),
            dst_addr: Some(Addr::Tcp(server_addr)),
        };
        // Allocation exists
        assert_eq!(1, server.allocation_managers.lock().len());
        assert!(
            server.allocation_managers.lock()[0]
                .get_allocation(&five_tuple)
                .is_some()
        );

        // This should properly close the client and delete the allocation on the server
        drop(client);

        // Let connection to properly close
        tokio::time::sleep(StdDuration::from_millis(100)).await;
        // to we still have the allocation on the server?
        assert!(
            server.allocation_managers.lock()[0]
                .get_allocation(&five_tuple)
                .is_none()
        );

        assert!(server.close().is_ok());
    }

    // Filter on client address and peer IP
    {
        let (udp_listener, addr) = udp_listener();

        // Enforce correct client IP and port
        let mut client = TestClient::udp(addr, TEST_USER, "pass").await;
        let client_local = client.local_addr.to_string();

        let server = Server::new(ServerConfig {
            auth_handler: Some(cred_map_handler(false)),
            packet_conn_configs: vec![PacketConnConfig {
                packet_conn: Some(udp_listener),
                relay_address_generator: Some(static_generator("127.0.0.1")),
                permission_handler: Some(Arc::new(move |src: Addr, peer: IpAddr| {
                    src.to_string() == client_local
                        && peer == "127.0.0.4".parse::<IpAddr>().unwrap()
                })),
            }],
            realm: "pion.ly".to_string(),
            logger_factory: Some(logger_factory.clone()),
            ..Default::default()
        })
        .unwrap();

        assert!(client.allocate().await.is_ok());

        let white_addr: SocketAddr = "127.0.0.4:12345".parse().unwrap();
        let black_addr: SocketAddr = "127.0.0.5:12345".parse().unwrap();

        // Explicit CreatePermission
        assert!(
            client.create_permission(&[white_addr]).await.is_ok(),
            "grant permission for whitelisted peer"
        );

        assert!(
            client.create_permission(&[black_addr]).await.is_err(),
            "deny permission for blacklisted peer address"
        );

        assert!(
            client
                .create_permission(&[white_addr, white_addr])
                .await
                .is_ok(),
            "grant permission for repeated whitelisted peer addresses"
        );

        assert!(
            client.create_permission(&[black_addr]).await.is_err(),
            "deny permission for repeated blacklisted peer address"
        );

        // Isn't this a corner case in the spec?
        assert!(
            client
                .create_permission(&[white_addr, black_addr])
                .await
                .is_err(),
            "deny permission for mixed whitelisted and blacklisted peers"
        );

        // Implicit CreatePermission for ChannelBindRequests: WriteTo always tries to bind a channel
        for i in 1..=3 {
            assert!(
                client.write_to(b"Hello", white_addr).await.is_ok(),
                "write to whitelisted peer address succeeds - {i}"
            );
            assert!(
                client.write_to(b"Hello", black_addr).await.is_err(),
                "write to blacklisted peer address fails - {i}"
            );
        }

        drop(client);

        // Enforce filtered source address
        let mut client2 = TestClient::udp(addr, TEST_USER, "pass").await;

        assert!(client2.allocate().await.is_ok());

        // Explicit CreatePermission
        assert!(
            client2.create_permission(&[white_addr]).await.is_err(),
            "deny permission from filtered source to whitelisted peer"
        );

        assert!(
            client2.create_permission(&[black_addr]).await.is_err(),
            "deny permission from filtered source to blacklisted peer"
        );

        // Implicit CreatePermission for ChannelBindRequests: WriteTo always tries to bind a channel
        for i in 1..=3 {
            assert!(
                client2.write_to(b"Hello", white_addr).await.is_err(),
                "write from filtered source to whitelisted peer fails - {i}"
            );
            assert!(
                client2.write_to(b"Hello", black_addr).await.is_err(),
                "write from filtered source to blacklisted peer fails - {i}"
            );
        }

        assert!(server.close().is_ok());
    }

    // Return error if payload exceeds peer connection MTU
    // Return error if peer payload is truncated
    for (name, truncate, expected) in [
        (
            "Return error if payload exceeds peer connection MTU",
            false,
            "MTU violation",
        ),
        (
            "Return error if peer payload is truncated",
            true,
            "packet write smaller than packet 1 != 5 (expected)",
        ),
    ] {
        let (udp_listener, addr) = udp_listener();

        let err_message = Arc::new(Mutex::new(String::new()));

        let relay_address_generator: Arc<dyn RelayAddressGenerator> = if truncate {
            Arc::new(TruncConnGenerator {
                relay_address_generator: static_generator("127.0.0.1"),
                mtu: 1,
            })
        } else {
            Arc::new(MtuConnGenerator {
                relay_address_generator: static_generator("127.0.0.1"),
                mtu: 1, // allow single-byte payload
            })
        };
        let server = {
            let err_message = err_message.clone();
            Server::new(ServerConfig {
                auth_handler: Some(cred_map_handler(false)),
                event_handler: EventHandler {
                    // used to report write errors from peer
                    on_allocation_error: Some(Arc::new(move |_, _, _, message: &str| {
                        *err_message.lock() = message.to_string();
                    })),
                    ..Default::default()
                },
                packet_conn_configs: vec![PacketConnConfig {
                    packet_conn: Some(udp_listener),
                    relay_address_generator: Some(relay_address_generator),
                    ..Default::default()
                }],
                realm: "pion.ly".to_string(),
                logger_factory: Some(logger_factory.clone()),
                ..Default::default()
            })
            .unwrap()
        };

        let mut client = TestClient::udp(addr, TEST_USER, "pass").await;

        assert!(client.allocate().await.is_ok(), "{name}");

        let peer_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        let r = client.write_to(b"Hello", peer_addr).await;
        assert!(r.is_ok(), "{name}: mtu not enforced in client side: {r:?}");
        let mut ok = false;
        for _ in 0..4 {
            if !err_message.lock().is_empty() {
                ok = true;
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(50)).await;
        }
        assert!(ok, "{name}");
        assert!(
            err_message.lock().contains(expected),
            "{name}: allocation error: {}",
            err_message.lock()
        );

        assert!(server.close().is_ok());
    }

    // Allow empty user-id
    {
        let (udp_listener, addr) = udp_listener();

        let server = Server::new(ServerConfig {
            // Return empty user-id
            auth_handler: Some(cred_map_handler(true)),
            packet_conn_configs: vec![PacketConnConfig {
                packet_conn: Some(udp_listener),
                relay_address_generator: Some(Arc::new(MtuConnGenerator {
                    relay_address_generator: static_generator("127.0.0.1"),
                    mtu: 0,
                })),
                ..Default::default()
            }],
            realm: "pion.ly".to_string(),
            logger_factory: Some(logger_factory.clone()),
            ..Default::default()
        })
        .unwrap();

        let mut client = TestClient::udp(addr, TEST_USER, "pass").await;

        let relay_conn = client.allocate().await;
        assert!(relay_conn.is_ok(), "Allow empty user-id: {relay_conn:?}");

        let _ = server.close();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_server_handle_comprehension_required_attribute() {
    let (udp_listener, addr) = udp_listener();

    let server = Server::new(ServerConfig {
        auth_handler: Some(Arc::new(|_: &RequestAttributes| {
            (String::new(), Vec::new(), false)
        })),
        packet_conn_configs: vec![PacketConnConfig {
            packet_conn: Some(udp_listener),
            relay_address_generator: Some(Arc::new(TruncConnGenerator {
                relay_address_generator: static_generator("127.0.0.1"),
                mtu: 1,
            })),
            ..Default::default()
        }],
        realm: "pion.ly".to_string(),
        logger_factory: Some(Logger::default()),
        ..Default::default()
    })
    .unwrap();

    let conn = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let raw = |typ: u16| RawAttribute {
        typ: AttrType(typ),
        length: 0,
        value: vec![0xBA, 0xAD],
    };
    let invalid = crate::webrtc::turn::internal::proto::build(&[
        &crate::webrtc::stun::message::TransactionId::new()
            as &dyn crate::webrtc::stun::message::Setter,
        &BINDING_REQUEST,
        &raw(0x7AD),
        &raw(ATTR_CHANNEL_NUMBER.0),
        &raw(0x7FFF),
        &raw(0x8000),
    ])
    .unwrap();

    conn.send_to(&invalid.raw, addr).await.unwrap();

    let mut buf = vec![0u8; 1500];
    let (i, _) = tokio::time::timeout(TIMEOUT, conn.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();

    let mut pkt = Message::new();
    pkt.raw = buf[..i].to_vec();
    assert!(pkt.decode().is_ok());

    assert_eq!(
        pkt.attributes.0[0],
        RawAttribute {
            typ: ATTR_ERROR_CODE,
            length: 0x15,
            value: vec![
                0x0, 0x0, 0x4, 0x14, 0x55, 0x6e, 0x6b, 0x6e, 0x6f, 0x77, 0x6e, 0x20, 0x41, 0x74,
                0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65,
            ],
        }
    );
    assert_eq!(
        pkt.attributes.0[1],
        RawAttribute {
            typ: ATTR_UNKNOWN_ATTRIBUTES,
            length: 0x4,
            value: vec![0x7, 0xad, 0x7f, 0xff],
        }
    );

    assert!(server.close().is_ok());
}

// Not here: TestServerVNet and its helpers, see the header.

#[tokio::test(flavor = "multi_thread")]
async fn test_stun_only() {
    let (server_conn, addr) = udp_listener();

    let server = Server::new(ServerConfig {
        packet_conn_configs: vec![PacketConnConfig {
            packet_conn: Some(server_conn),
            ..Default::default()
        }],
        realm: "pion.ly".to_string(),
        logger_factory: Some(Logger::default()),
        ..Default::default()
    })
    .unwrap();

    let mut client = TestClient::udp(addr, TEST_USER, "pass").await;

    let refl_addr = client.send_binding_request().await;
    assert!(refl_addr.is_ok());

    // Allocate error response (error 400: )
    let resp = client
        .allocate_with(&[&RequestedTransport {
            protocol: PROTO_UDP,
        }])
        .await
        .unwrap();
    assert_eq!(CLASS_ERROR_RESPONSE, resp.typ.class);
    assert_eq!(Some(400), error_code_of(&resp));

    let _ = server.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_quota_reached() {
    let (server_conn, addr) = udp_listener();

    let cred_map = [
        ("test:1", generate_auth_key("test:1", "pion.ly", "pass")),
        ("test:2", generate_auth_key("test:2", "pion.ly", "pass")),
    ];
    let alloc_map = Arc::new(Mutex::new(std::collections::HashMap::<String, i64>::new()));
    let server = Server::new(ServerConfig {
        auth_handler: Some(Arc::new(move |ra: &RequestAttributes| {
            if let Some((_, pw)) = cred_map.iter().find(|(u, _)| *u == ra.username) {
                return ("test".to_string(), pw.clone(), true);
            }

            (String::new(), Vec::new(), false)
        })),
        quota_handler: Some(Arc::new(move |user_id: &str, _: &str, _: Addr| {
            let mut alloc_map = alloc_map.lock();
            if alloc_map.get(user_id).copied().unwrap_or(0) > 0 {
                return false;
            }
            *alloc_map.entry(user_id.to_string()).or_default() += 1;

            true
        })),
        realm: "pion.ly".to_string(),
        packet_conn_configs: vec![PacketConnConfig {
            packet_conn: Some(server_conn),
            relay_address_generator: Some(static_generator("0.0.0.0")),
            ..Default::default()
        }],
        logger_factory: Some(Logger::default()),
        ..Default::default()
    })
    .unwrap();

    // First client succeeds
    let mut client1 = TestClient::udp(addr, "test:1", "pass").await;

    let relay_conn = client1.allocate().await;
    assert!(relay_conn.is_ok(), "{relay_conn:?}");

    let mut client2 = TestClient::udp(addr, "test:2", "pass").await;

    // Allocate error response (error 486: )
    let resp = client2
        .allocate_with(&[&RequestedTransport {
            protocol: PROTO_UDP,
        }])
        .await
        .unwrap();
    assert_eq!(CLASS_ERROR_RESPONSE, resp.typ.class);
    assert_eq!(Some(486), error_code_of(&resp));

    let _ = server.close();
}

fn reservation_server() -> (Arc<Server>, SocketAddr) {
    let (server_conn, addr) = udp_listener();

    let server = Server::new(ServerConfig {
        auth_handler: Some(Arc::new(|_: &RequestAttributes| {
            (
                TEST_USER.to_string(),
                generate_auth_key(TEST_USER, "pion.ly", "pass"),
                true,
            )
        })),
        realm: "pion.ly".to_string(),
        packet_conn_configs: vec![PacketConnConfig {
            packet_conn: Some(server_conn),
            relay_address_generator: Some(static_generator("0.0.0.0")),
            ..Default::default()
        }],
        logger_factory: Some(Logger::default()),
        ..Default::default()
    })
    .unwrap();

    (server, addr)
}

fn relayed_port(resp: &Message) -> u16 {
    let mut relayed = RelayedAddress::default();
    crate::webrtc::stun::message::Getter::get_from(&mut relayed, resp).unwrap();
    relayed.port
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reservation() {
    let (server, addr) = reservation_server();

    let mut first_client = TestClient::udp(addr, TEST_USER, "pass").await;

    let first_allocation = first_client
        .allocate_with(&[
            &RequestedTransport {
                protocol: PROTO_UDP,
            },
            &EvenPort::default(),
        ])
        .await
        .unwrap();
    assert_ne!(CLASS_ERROR_RESPONSE, first_allocation.typ.class);

    let first_allocation_port = relayed_port(&first_allocation);
    assert_eq!(first_allocation_port % 2, 0);

    let mut reservation_token = ReservationToken::default();
    crate::webrtc::stun::message::Getter::get_from(&mut reservation_token, &first_allocation)
        .unwrap();
    assert!(!reservation_token.0.is_empty());

    let mut second_client = TestClient::udp(addr, TEST_USER, "pass").await;

    let second_allocation = second_client
        .allocate_with(&[
            &RequestedTransport {
                protocol: PROTO_UDP,
            },
            &reservation_token,
        ])
        .await
        .unwrap();
    assert_ne!(CLASS_ERROR_RESPONSE, second_allocation.typ.class);

    assert_eq!(first_allocation_port + 1, relayed_port(&second_allocation));

    assert!(server.close().is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reservation_invalid_token() {
    let (server, addr) = reservation_server();

    let mut client = TestClient::udp(addr, TEST_USER, "pass").await;

    // Allocate error response (error 508: )
    let resp = client
        .allocate_with(&[
            &RequestedTransport {
                protocol: PROTO_UDP,
            },
            &ReservationToken(vec![105, 99, 86, 84, 103, 97, 108, 112]),
        ])
        .await
        .unwrap();
    assert_eq!(Some(508), error_code_of(&resp));

    assert!(server.close().is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reservation_token_and_even_port() {
    let (server, addr) = reservation_server();

    let mut client = TestClient::udp(addr, TEST_USER, "pass").await;

    // Allocate error response (error 400: )
    let resp = client
        .allocate_with(&[
            &RequestedTransport {
                protocol: PROTO_UDP,
            },
            &EvenPort::default(),
            &ReservationToken(vec![105, 99, 86, 84, 103, 97, 108, 112]),
        ])
        .await
        .unwrap();
    assert_eq!(Some(400), error_code_of(&resp));

    assert!(server.close().is_ok());
}

struct DontFragmentConn {
    conn: Arc<dyn PacketConn>,
}

impl PacketConn for DontFragmentConn {
    fn read_from<'a>(
        &'a self,
        buff: &'a mut [u8],
    ) -> BoxFuture<'a, std::io::Result<(usize, Addr)>> {
        Box::pin(async move {
            let (n, addr) = self.conn.read_from(buff).await?;

            let mut stun_msg = Message::new();
            stun_msg.raw = buff[..n].to_vec();
            if let Err(err) = stun_msg.decode() {
                return Err(std::io::Error::other(err.to_string()));
            }

            stun_msg.attributes.0.push(RawAttribute {
                typ: ATTR_DONT_FRAGMENT,
                length: 0,
                value: Vec::new(),
            });
            stun_msg.encode();

            buff[..stun_msg.raw.len()].copy_from_slice(&stun_msg.raw);
            let n = stun_msg.raw.len();

            Ok((n, addr))
        })
    }

    fn write_to<'a>(&'a self, data: &'a [u8], addr: Addr) -> BoxFuture<'a, std::io::Result<usize>> {
        self.conn.write_to(data, addr)
    }

    fn close(&self) -> std::io::Result<()> {
        self.conn.close()
    }

    fn local_addr(&self) -> Addr {
        self.conn.local_addr()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dont_fragment() {
    let (server_conn, addr) = udp_listener();

    let server = Server::new(ServerConfig {
        auth_handler: Some(Arc::new(|_: &RequestAttributes| {
            (
                TEST_USER.to_string(),
                generate_auth_key(TEST_USER, "pion.ly", "pass"),
                true,
            )
        })),
        realm: "pion.ly".to_string(),
        packet_conn_configs: vec![PacketConnConfig {
            packet_conn: Some(Arc::new(DontFragmentConn { conn: server_conn })),
            relay_address_generator: Some(static_generator("0.0.0.0")),
            ..Default::default()
        }],
        logger_factory: Some(Logger::default()),
        ..Default::default()
    })
    .unwrap();

    let mut client = TestClient::udp(addr, TEST_USER, "pass").await;

    // Allocate error response (error 420: )
    let resp = client
        .allocate_with(&[&RequestedTransport {
            protocol: PROTO_UDP,
        }])
        .await
        .unwrap();
    assert_eq!(Some(420), error_code_of(&resp));

    assert!(server.close().is_ok());
}

#[test]
fn test_relay_address_generator_static() {
    let mut relay_address_generator_static = RelayAddressGeneratorStatic::default();

    assert!(matches!(
        relay_address_generator_static.validate(),
        Err(Error::RelayAddressInvalid)
    ));

    relay_address_generator_static.relay_address = Some("127.0.0.1".parse().unwrap());
    assert!(matches!(
        relay_address_generator_static.validate(),
        Err(Error::ListeningAddressInvalid)
    ));

    relay_address_generator_static.address = "127.0.0.1".to_string();
    assert!(relay_address_generator_static.validate().is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tls_handshake_connection_state() {
    let logger_factory = Logger::default();

    let username = "testuser";

    // generateCACert
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Test CA");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_issuer = rcgen::Issuer::new(ca_params, ca_key);

    // generateSignedCert
    let signed = |common_name: &str| {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params =
            rcgen::CertificateParams::new(vec![common_name.to_string(), "127.0.0.1".to_string()])
                .unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let cert = params.signed_by(&key, &ca_issuer).unwrap();
        (
            cert.der().clone(),
            rustls::pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
        )
    };
    let (server_cert, server_key) = signed("server");
    let (client_cert, client_key) = signed(username);

    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca_cert.der().clone()).unwrap();
    let roots = Arc::new(roots);
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let tls_state_received = Arc::new(Mutex::new(None::<Vec<Vec<u8>>>));

    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        roots.clone(),
        provider.clone(),
    )
    .build()
    .unwrap();
    let server_config = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![server_cert], server_key)
        .unwrap();
    let tls_listener = net::new_tls_listener(
        Box::new(net::listen("tcp4", "127.0.0.1:0").unwrap()),
        Arc::new(server_config),
    );
    let server_addr = tls_listener.addr().socket_addr();

    let server = {
        let tls_state_received = tls_state_received.clone();
        let client_der = client_cert.to_vec();
        let username = username.to_string();
        Server::new(ServerConfig {
            auth_handler: Some(Arc::new(move |ra: &RequestAttributes| {
                let Some(tls) = &ra.tls else {
                    return (String::new(), Vec::new(), false);
                };
                if tls.peer_certificates.is_empty() {
                    return (String::new(), Vec::new(), false);
                }

                *tls_state_received.lock() =
                    Some(tls.peer_certificates.iter().map(|c| c.to_vec()).collect());

                for cert in &tls.peer_certificates {
                    if cert.as_ref() != client_der.as_slice() || ra.username != username {
                        continue;
                    }

                    return (
                        username.clone(),
                        generate_auth_key(&username, "pion.ly", ""),
                        true,
                    );
                }

                (String::new(), Vec::new(), false)
            })),
            listener_configs: vec![ListenerConfig {
                listener: Some(Arc::new(tls_listener)),
                relay_address_generator: Some(static_generator("127.0.0.1")),
                ..Default::default()
            }],
            realm: "pion.ly".to_string(),
            logger_factory: Some(logger_factory),
            ..Default::default()
        })
        .unwrap()
    };

    let client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![client_cert.clone()], client_key)
        .unwrap();
    let tcp = tokio::net::TcpStream::connect(server_addr).await.unwrap();
    let local_addr = tcp.local_addr().unwrap();
    let conn = tokio_rustls::TlsConnector::from(Arc::new(client_config))
        .connect(
            rustls::pki_types::ServerName::try_from("server").unwrap(),
            tcp,
        )
        .await
        .unwrap();

    // Our test Authhandler requires it matches cert's CN; empty password, auth based on certificate
    let mut client = TestClient::stream(Box::new(conn), local_addr, server_addr, username, "");

    let relay_conn = client.allocate().await;
    assert!(relay_conn.is_ok(), "{relay_conn:?}");

    let received_certs = tls_state_received.lock().clone();
    assert!(
        received_certs.is_some(),
        "TLS connection state should have been received by auth handler"
    );

    let received_certs = received_certs.unwrap();
    assert!(
        !received_certs.is_empty(),
        "Should have at least one peer certificate"
    );

    assert_eq!(
        client_cert.to_vec(),
        received_certs[0],
        "Certificate should match"
    );

    let _ = server.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_relay_address_generator_port_range() {
    let _ports = crate::testutils::lock_fixed_turn_ports().await;
    let mut relay_address_generator_port_range = RelayAddressGeneratorPortRange::default();

    // Validate
    assert!(matches!(
        relay_address_generator_port_range.validate(),
        Err(Error::MinPortNotZero)
    ));

    relay_address_generator_port_range.min_port = 39545;
    assert!(matches!(
        relay_address_generator_port_range.validate(),
        Err(Error::MaxPortNotZero)
    ));

    relay_address_generator_port_range.max_port = 39559;
    assert!(matches!(
        relay_address_generator_port_range.validate(),
        Err(Error::RelayAddressInvalid)
    ));

    relay_address_generator_port_range.relay_address = Some("127.0.0.1".parse().unwrap());
    assert!(matches!(
        relay_address_generator_port_range.validate(),
        Err(Error::ListeningAddressInvalid)
    ));

    relay_address_generator_port_range.address = "127.0.0.1".to_string();
    assert!(relay_address_generator_port_range.validate().is_ok());

    // One Port
    {
        let (conn, addr) = relay_address_generator_port_range
            .allocate_packet_conn(AllocateListenerConfig {
                network: "udp".to_string(),
                requested_port: 39545,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(39545, addr.port(), "One Port");

        let Addr::Udp(udp_addr) = conn.local_addr() else {
            panic!("One Port: a UDP address");
        };
        assert_eq!(udp_addr.port(), 39545, "One Port");

        assert!(conn.close().is_ok());

        // Not here: AllocateListener, see the header.
    }

    // Range
    {
        let mut conns = Vec::new();

        for _ in 0..5 {
            let (conn, _addr) = relay_address_generator_port_range
                .allocate_packet_conn(AllocateListenerConfig {
                    network: "udp".to_string(),
                    ..Default::default()
                })
                .unwrap();
            conns.push(conn);
        }

        for conn in &conns {
            let Addr::Udp(udp_addr) = conn.local_addr() else {
                panic!("Range: a UDP address");
            };
            assert!((39545..=39559).contains(&udp_addr.port()), "Range");

            assert!(conn.close().is_ok());
        }
    }
}

#[test]
fn test_nil_address_generator() {
    let generator = NilAddressGenerator;

    assert!(matches!(
        generator.validate(),
        Err(Error::RelayAddressGeneratorNil)
    ));

    let r = generator.allocate_packet_conn(AllocateListenerConfig::default());
    assert!(matches!(r, Err(Error::RelayAddressGeneratorNil)));

    // Not here: AllocateListener, see the header.
}
