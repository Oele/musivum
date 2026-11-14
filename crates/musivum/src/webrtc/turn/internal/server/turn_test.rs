// Ported from pion/turn, internal/server/turn_test.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The managers, requests and messages the tests make one
// by one are made by helpers here (`new_manager`, `new_request`, `new_msg`); a STUN message
// the original builds from an empty `stun.Message{}` starts from `Message::new()`. pion's
// `test.CheckRoutines` (a check for goroutines left running) has no counterpart. The
// captured warnings are LiveKit's log as `testutils::LogCapture` sees it (TestHandleChannelBindRequest
// runs on one thread for that). Where a test relays a packet to a peer for real ("Success" of
// TestHandleSendIndication and TestHandleChannelData), the peer is 192.0.2.1 (TEST-NET-1)
// instead of 192.168.1.1, so that nothing is sent to a host of a local network.
// Not ported, with what they test (RFC 6062, not ported): TestConnectRequest,
// TestConnectionBindRequest and TestConnectRequestPermissionDenied.

use std::net::IpAddr;
use std::sync::Arc;

use parking_lot::Mutex;

use super::*;
use crate::go::time::{Duration, HOUR, MINUTE};
use crate::protocol::logger::Logger;
use crate::testutils::LogCapture;
use crate::webrtc::stun::message::{
    CLASS_ERROR_RESPONSE, CLASS_INDICATION, CLASS_REQUEST, CLASS_SUCCESS_RESPONSE, METHOD_ALLOCATE,
    METHOD_CHANNEL_BIND, METHOD_CREATE_PERMISSION, METHOD_REFRESH, METHOD_SEND, MessageClass,
    Setter, TransactionId,
};
use crate::webrtc::turn::internal::allocation::{
    AllocateListenerConfig, BoxError, ChannelBind, Manager, ManagerConfig, Permission,
};
use crate::webrtc::turn::internal::auth::AuthHandler;
use crate::webrtc::turn::internal::proto::{DEFAULT_LIFETIME_GO, PROTO_UDP};
use crate::webrtc::turn::internal::server::{
    MAXIMUM_ALLOCATION_LIFETIME, NonceManager, allocation_life_time, crypto, new_short_nonce_hash,
};
use crate::webrtc::turn::net::{self, BoxFuture, PacketConn, err_closed};
use rtc::stun::attributes::ATTR_REQUESTED_ADDRESS_FAMILY;
use rtc::stun::error_code::{
    CODE_BAD_REQUEST, CODE_FORBIDDEN, CODE_PEER_ADDR_FAMILY_MISMATCH, CODE_UNAUTHORIZED, ErrorCode,
    ErrorCodeAttribute,
};
use rtc::stun::integrity::MessageIntegrity;

const TEST_USER: &str = "test";

fn udp(s: &str) -> Addr {
    Addr::Udp(s.parse().unwrap())
}

// The manager the tests make, with relays on `relay_addr` of the network asked for.
fn new_manager(relay_addr: &'static str, permission: Option<bool>) -> Arc<Manager> {
    Manager::new(ManagerConfig {
        allocate_packet_conn: Some(Arc::new(move |conf: AllocateListenerConfig| {
            let con = net::listen_packet(&conf.network, relay_addr)
                .map_err(|e| Box::new(e) as BoxError)?;
            let addr = con.local_addr();

            Ok((con as Arc<dyn PacketConn>, addr))
        })),
        permission_handler: permission.map(|ok| {
            Arc::new(move |_: Addr, _: IpAddr| ok)
                as crate::webrtc::turn::internal::allocation::PermissionHandlerFn
        }),
        leveled_logger: Some(Logger::default().with_name("turn")),
        ..Default::default()
    })
    .unwrap()
}

fn static_auth(user_id: &str, static_key: &str) -> AuthHandler {
    let (user_id, static_key) = (user_id.to_string(), static_key.to_string());
    Arc::new(move |_| (user_id.clone(), static_key.as_bytes().to_vec(), true))
}

fn new_request<'a>(
    allocation_manager: Arc<Manager>,
    nonce_hash: Arc<dyn NonceManager>,
    conn: Arc<dyn PacketConn>,
    src_addr: Addr,
    auth_handler: Option<AuthHandler>,
) -> Request<'a> {
    Request {
        conn,
        src_addr,
        buff: &[],
        tls: None,
        allocation_manager,
        nonce_hash,
        auth_handler,
        quota_handler: None,
        log: Logger::default().with_name("turn"),
        realm: String::new(),
        channel_bind_timeout: Duration::ZERO,
        permission_timeout: Duration::ZERO,
        allocation_lifetime: Duration::ZERO,
        strict_address_family: false,
    }
}

fn nonce_and_key() -> (Arc<dyn NonceManager>, String) {
    let nonce_hash: Arc<dyn NonceManager> = new_short_nonce_hash(0).unwrap().into();
    let static_key = nonce_hash.generate().unwrap();
    (nonce_hash, static_key)
}

// m := &stun.Message{}; m.TransactionID = stun.NewTransactionID(); m.Build(stun.NewType(...))
fn new_msg(typ: MessageType) -> Message {
    let mut m = Message::new();
    m.transaction_id = TransactionId::new();
    m.build(&[Box::new(typ)]).unwrap();
    m
}

fn add(m: &mut Message, s: &dyn Setter) {
    s.add_to(m).unwrap();
}

fn integrity(static_key: &str) -> MessageIntegrity<'static> {
    MessageIntegrity::new_raw_integrity_with_provider(static_key.as_bytes().to_vec(), crypto())
}

fn text(attr: rtc::stun::attributes::AttrType, s: &str) -> rtc::stun::textattrs::TextAttribute {
    rtc::stun::textattrs::TextAttribute::new(attr, s.to_string())
}

// MessageIntegrity, Nonce, Realm and Username, in the order the tests add them
fn add_auth(m: &mut Message, static_key: &str, username: &str) {
    add(m, &integrity(static_key));
    add(m, &text(rtc::stun::attributes::ATTR_NONCE, static_key));
    add(m, &text(rtc::stun::attributes::ATTR_REALM, static_key));
    add(m, &text(rtc::stun::attributes::ATTR_USERNAME, username));
}

fn five_tuple(req: &Request<'_>) -> FiveTuple {
    FiveTuple {
        src_addr: Some(req.src_addr),
        dst_addr: Some(req.conn.local_addr()),
        protocol: Protocol::Udp,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_allocation_life_time() {
    // Parsing
    {
        let lifetime = Lifetime(std::time::Duration::from_secs(5));

        let (nonce_hash, _) = nonce_and_key();
        let conn = net::listen_packet("udp4", "127.0.0.1:0").unwrap();
        let mut req = new_request(
            new_manager("0.0.0.0:0", None),
            nonce_hash,
            conn,
            udp("127.0.0.1:5000"),
            None,
        );
        req.allocation_lifetime = DEFAULT_LIFETIME_GO;

        let mut m = Message::new();
        let lifetime_duration = allocation_life_time(&req, &m);
        assert_eq!(
            DEFAULT_LIFETIME_GO, lifetime_duration,
            "Allocation lifetime should be default time duration"
        );
        add(&mut m, &lifetime);

        let lifetime_duration = allocation_life_time(&req, &m);
        assert_eq!(
            Duration::from_secs(5),
            lifetime_duration,
            "Allocation lifetime should be equal to the one set in the message"
        );
    }

    // If lifetime is bigger than maximumLifetime
    // Overflow
    {
        let lifetime = Lifetime((MAXIMUM_ALLOCATION_LIFETIME * 2).to_std());

        let mut m2 = Message::new();
        add(&mut m2, &lifetime);

        let (nonce_hash, _) = nonce_and_key();
        let conn = net::listen_packet("udp4", "127.0.0.1:0").unwrap();
        let mut req = new_request(
            new_manager("0.0.0.0:0", None),
            nonce_hash,
            conn,
            udp("127.0.0.1:5000"),
            None,
        );
        req.allocation_lifetime = DEFAULT_LIFETIME_GO;
        let lifetime_duration = allocation_life_time(&req, &m2);
        assert_eq!(DEFAULT_LIFETIME_GO, lifetime_duration, "Overflow");
    }

    // DeletionZeroLifetime
    {
        let conn = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

        let allocation_manager = new_manager("0.0.0.0:0", None);

        let (nonce_hash, static_key) = nonce_and_key();

        let req = new_request(
            allocation_manager,
            nonce_hash,
            conn.clone(),
            udp("127.0.0.1:5000"),
            Some(static_auth(TEST_USER, &static_key)),
        );

        let five_tuple = five_tuple(&req);

        let r = req.allocation_manager.create_allocation(
            Some(&five_tuple),
            Some(req.conn.clone()),
            PROTO_UDP,
            0,
            HOUR,
            TEST_USER,
            "",
            REQUESTED_FAMILY_IPV4,
        );
        assert!(r.is_ok());

        assert!(req.allocation_manager.get_allocation(&five_tuple).is_some());

        let mut m = Message::new();
        add(&mut m, &Lifetime::default());
        add(&mut m, &integrity(&static_key));
        add(
            &mut m,
            &text(rtc::stun::attributes::ATTR_NONCE, &static_key),
        );
        add(
            &mut m,
            &text(rtc::stun::attributes::ATTR_REALM, &static_key),
        );
        add(
            &mut m,
            &text(rtc::stun::attributes::ATTR_USERNAME, TEST_USER),
        );

        let r = handle_refresh_request(&req, &m).await;
        assert!(r.is_ok(), "DeletionZeroLifetime: {r:?}");
        assert!(req.allocation_manager.get_allocation(&five_tuple).is_none());

        assert!(conn.close().is_ok());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_requested_transport() {
    let conn = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let allocation_manager = new_manager("0.0.0.0:0", None);

    let (nonce_hash, static_key) = nonce_and_key();

    let req = new_request(
        allocation_manager,
        nonce_hash,
        conn.clone(),
        udp("127.0.0.1:5000"),
        Some(static_auth(TEST_USER, &static_key)),
    );

    let mut m = Message::new();
    add(&mut m, &Lifetime::default());
    add(&mut m, &integrity(&static_key));
    add(
        &mut m,
        &text(rtc::stun::attributes::ATTR_NONCE, &static_key),
    );
    add(
        &mut m,
        &text(rtc::stun::attributes::ATTR_REALM, &static_key),
    );
    add(
        &mut m,
        &text(rtc::stun::attributes::ATTR_USERNAME, &static_key),
    );
    assert!(matches!(
        handle_allocate_request(&req, &m).await,
        Err(Error::Stun(
            crate::webrtc::stun::Error::ErrAttributeNotFound
        ))
    ));

    add(
        &mut m,
        &RequestedTransport {
            protocol: proto::Protocol(1),
        },
    );
    assert!(matches!(
        handle_allocate_request(&req, &m).await,
        Err(Error::UnsupportedTransportProtocol)
    ));

    assert!(conn.close().is_ok());
}

// Not here: TestConnectRequest and TestConnectionBindRequest, see the header.

#[tokio::test(flavor = "multi_thread")]
async fn test_requested_address_family_ipv6() {
    let conn = net::listen_packet("udp6", "[::]:0").unwrap();

    let allocation_manager = new_manager("[::]:0", None);

    let (nonce_hash, static_key) = nonce_and_key();

    let req = new_request(
        allocation_manager,
        nonce_hash,
        conn.clone(),
        udp("[::1]:5000"),
        Some(static_auth(TEST_USER, &static_key)),
    );

    let five_tuple = five_tuple(&req);

    // Create IPv6 allocation
    let alloc = req
        .allocation_manager
        .create_allocation(
            Some(&five_tuple),
            Some(req.conn.clone()),
            PROTO_UDP,
            0,
            HOUR,
            TEST_USER,
            "",
            REQUESTED_FAMILY_IPV6,
        )
        .unwrap();
    assert_eq!(REQUESTED_FAMILY_IPV6, alloc.address_family());

    // Test that the allocation is present
    let found_alloc = req.allocation_manager.get_allocation(&five_tuple).unwrap();
    assert_eq!(REQUESTED_FAMILY_IPV6, found_alloc.address_family());

    let _ = conn.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_allocate_default_address_family_from_listener() {
    // IPv6 listener
    {
        let (family, network) = allocate_without_requested_address_family(false).await;

        assert_eq!(REQUESTED_FAMILY_IPV6, family, "IPv6 listener");
        assert_eq!("udp6", network, "IPv6 listener");
    }

    // strict mode
    {
        let (family, network) = allocate_without_requested_address_family(true).await;

        assert_eq!(REQUESTED_FAMILY_IPV4, family, "strict mode");
        assert_eq!("udp4", network, "strict mode");
    }
}

async fn allocate_without_requested_address_family(
    strict_address_family: bool,
) -> (RequestedAddressFamily, String) {
    let conn = net::listen_packet("udp6", "[::]:0").unwrap();

    let relay_network = Arc::new(Mutex::new(String::new()));

    let allocation_manager = {
        let relay_network = relay_network.clone();
        Manager::new(ManagerConfig {
            allocate_packet_conn: Some(Arc::new(move |conf: AllocateListenerConfig| {
                *relay_network.lock() = conf.network.clone();
                let mut addr = "0.0.0.0:0";
                if conf.network == "udp6" {
                    addr = "[::]:0";
                }

                let con =
                    net::listen_packet(&conf.network, addr).map_err(|e| Box::new(e) as BoxError)?;
                let addr = con.local_addr();

                Ok((con as Arc<dyn PacketConn>, addr))
            })),
            leveled_logger: Some(Logger::default()),
            ..Default::default()
        })
        .unwrap()
    };

    let (nonce_hash, static_key) = nonce_and_key();

    let mut req = new_request(
        allocation_manager.clone(),
        nonce_hash,
        conn.clone(),
        udp("[::1]:5000"),
        Some(static_auth(TEST_USER, &static_key)),
    );
    req.allocation_lifetime = DEFAULT_LIFETIME_GO;
    req.strict_address_family = strict_address_family;
    req.realm = "pion.ly".to_string();

    let mut msg = new_msg(MessageType::new(METHOD_ALLOCATE, CLASS_REQUEST));
    add(
        &mut msg,
        &RequestedTransport {
            protocol: PROTO_UDP,
        },
    );
    add(
        &mut msg,
        &text(rtc::stun::attributes::ATTR_NONCE, &static_key),
    );
    add(
        &mut msg,
        &text(rtc::stun::attributes::ATTR_REALM, "pion.ly"),
    );
    add(
        &mut msg,
        &text(rtc::stun::attributes::ATTR_USERNAME, TEST_USER),
    );
    add(&mut msg, &integrity(&static_key));

    let r = handle_allocate_request(&req, &msg).await;
    assert!(r.is_ok(), "{r:?}");

    let alloc = allocation_manager
        .get_allocation(&five_tuple(&req))
        .unwrap();

    let _ = allocation_manager.close();
    let _ = conn.close();

    let network = relay_network.lock().clone();
    (alloc.address_family(), network)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_requested_address_family_unsupported() {
    let conn = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let allocation_manager = new_manager("0.0.0.0:0", None);

    let (nonce_hash, static_key) = nonce_and_key();

    let req = new_request(
        allocation_manager,
        nonce_hash,
        conn.clone(),
        udp("127.0.0.1:5000"),
        Some(static_auth(TEST_USER, &static_key)),
    );

    // Create message with unsupported address family (0x03)
    let mut m = new_msg(MessageType::new(METHOD_ALLOCATE, CLASS_REQUEST));
    add(
        &mut m,
        &RequestedTransport {
            protocol: PROTO_UDP,
        },
    );
    add_auth(&mut m, &static_key, &static_key);
    // Manually add invalid address family
    m.add(ATTR_REQUESTED_ADDRESS_FAMILY, &[0x03, 0x00, 0x00, 0x00]);

    // Should return error with code 440 (Address Family not Supported)
    let r = handle_allocate_request(&req, &m).await;
    assert!(r.is_err());

    let _ = conn.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_requested_address_family_mutual_exclusivity() {
    let conn = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let allocation_manager = new_manager("0.0.0.0:0", None);

    let (nonce_hash, static_key) = nonce_and_key();

    let req = new_request(
        allocation_manager,
        nonce_hash,
        conn.clone(),
        udp("127.0.0.1:5000"),
        Some(static_auth(TEST_USER, &static_key)),
    );

    // Create message with both REQUESTED-ADDRESS-FAMILY and RESERVATION-TOKEN
    let mut m = new_msg(MessageType::new(METHOD_ALLOCATE, CLASS_REQUEST));
    add(
        &mut m,
        &RequestedTransport {
            protocol: PROTO_UDP,
        },
    );
    add_auth(&mut m, &static_key, &static_key);
    add(&mut m, &REQUESTED_FAMILY_IPV4);
    // ReservationToken must be exactly 8 bytes
    add(
        &mut m,
        &ReservationToken(vec![0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]),
    );

    // Should return error with code 400 (Bad Request) for mutual exclusivity violation
    let r = handle_allocate_request(&req, &m).await;
    assert!(r.is_err());

    let _ = conn.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_handle_refresh_request_requested_address_family_mismatch() {
    let conn = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let allocation_manager = new_manager("0.0.0.0:0", None);

    let (nonce_hash, static_key) = nonce_and_key();

    let req = new_request(
        allocation_manager.clone(),
        nonce_hash,
        conn.clone(),
        udp("127.0.0.1:5000"),
        Some(static_auth(TEST_USER, &static_key)),
    );

    let r = req.allocation_manager.create_allocation(
        Some(&five_tuple(&req)),
        Some(req.conn.clone()),
        PROTO_UDP,
        0,
        HOUR,
        "test",
        "",
        REQUESTED_FAMILY_IPV4,
    );
    assert!(r.is_ok());

    let mut m = new_msg(MessageType::new(METHOD_REFRESH, CLASS_REQUEST));
    add(
        &mut m,
        &text(rtc::stun::attributes::ATTR_NONCE, &static_key),
    );
    add(
        &mut m,
        &text(rtc::stun::attributes::ATTR_REALM, &static_key),
    );
    add(
        &mut m,
        &text(rtc::stun::attributes::ATTR_USERNAME, TEST_USER),
    );
    m.add(ATTR_REQUESTED_ADDRESS_FAMILY, &[0x03, 0x00, 0x00, 0x00]);
    add(&mut m, &integrity(&static_key));

    let r = handle_refresh_request(&req, &m).await;
    assert!(matches!(r, Err(Error::PeerAddressFamilyMismatch)), "{r:?}");

    let _ = allocation_manager.close();
    let _ = conn.close();
}

#[test]
fn test_ip_matches_family() {
    // IPv4
    let ipv4: IpAddr = "192.168.1.1".parse().unwrap();
    assert!(ip_matches_family(ipv4, &REQUESTED_FAMILY_IPV4), "IPv4");
    assert!(!ip_matches_family(ipv4, &REQUESTED_FAMILY_IPV6), "IPv4");

    // IPv6
    let ipv6: IpAddr = "2001:db8::1".parse().unwrap();
    assert!(!ip_matches_family(ipv6, &REQUESTED_FAMILY_IPV4), "IPv6");
    assert!(ip_matches_family(ipv6, &REQUESTED_FAMILY_IPV6), "IPv6");

    // IPv6Loopback
    let ipv6_loopback: IpAddr = "::1".parse().unwrap();
    assert!(
        !ip_matches_family(ipv6_loopback, &REQUESTED_FAMILY_IPV4),
        "IPv6Loopback"
    );
    assert!(
        ip_matches_family(ipv6_loopback, &REQUESTED_FAMILY_IPV6),
        "IPv6Loopback"
    );

    // IPv4Loopback
    let ipv4_loopback: IpAddr = "127.0.0.1".parse().unwrap();
    assert!(
        ip_matches_family(ipv4_loopback, &REQUESTED_FAMILY_IPV4),
        "IPv4Loopback"
    );
    assert!(
        !ip_matches_family(ipv4_loopback, &REQUESTED_FAMILY_IPV6),
        "IPv4Loopback"
    );
}

fn assert_last_response(
    conn: &CapturePacketConn,
    method: crate::webrtc::stun::message::Method,
    expected_class: MessageClass,
    expected_code: ErrorCode,
    name: &str,
) {
    let raw = conn.last_write();
    assert!(!raw.is_empty(), "{name}: server should write a response");

    let mut res = Message::new();
    res.raw = raw;
    assert!(res.decode().is_ok(), "{name}");
    assert_eq!(method, res.typ.method, "{name}");
    assert_eq!(expected_class, res.typ.class, "{name}");

    if expected_class == CLASS_ERROR_RESPONSE {
        let mut code = ErrorCodeAttribute::default();
        assert!(code.get_from(&res).is_ok(), "{name}");
        assert_eq!(expected_code.0, code.code.0, "{name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_handle_create_permission_request() {
    let conn = Arc::new(CapturePacketConn::new(udp("127.0.0.1:3478")));

    let allocation_manager = new_manager("0.0.0.0:0", None);

    let (nonce_hash, static_key) = nonce_and_key();

    let mut req = new_request(
        allocation_manager.clone(),
        nonce_hash,
        conn.clone(),
        udp("127.0.0.1:5000"),
        Some(static_auth(TEST_USER, &static_key)),
    );
    req.permission_timeout = 5 * MINUTE;

    let request = |static_key: &str| {
        let mut m = new_msg(MessageType::new(METHOD_CREATE_PERMISSION, CLASS_REQUEST));
        add_auth(&mut m, static_key, TEST_USER);
        m
    };

    // NoAllocationFound
    {
        conn.reset();
        let m = request(&static_key);

        let r = handle_create_permission_request(&req, &m).await;
        assert!(
            matches!(r, Err(Error::NoAllocationFoundFor(_))),
            "NoAllocationFound: {r:?}"
        );
    }

    // Create an allocation for the remaining tests
    let r = req.allocation_manager.create_allocation(
        Some(&five_tuple(&req)),
        Some(req.conn.clone()),
        PROTO_UDP,
        0,
        HOUR,
        TEST_USER,
        "",
        REQUESTED_FAMILY_IPV4,
    );
    assert!(r.is_ok());

    // SuccessIPv4
    {
        conn.reset();
        let mut m = request(&static_key);
        add(
            &mut m,
            &PeerAddress {
                ip: "192.168.1.1".parse().unwrap(),
                port: 8080,
            },
        );

        let r = handle_create_permission_request(&req, &m).await;
        assert!(r.is_ok(), "SuccessIPv4: {r:?}");
        assert_last_response(
            &conn,
            METHOD_CREATE_PERMISSION,
            CLASS_SUCCESS_RESPONSE,
            ErrorCode(0),
            "SuccessIPv4",
        );
    }

    // PeerAddressFamilyMismatch
    {
        conn.reset();
        let mut m = request(&static_key);
        // Try to add IPv6 peer address to IPv4 allocation
        add(
            &mut m,
            &PeerAddress {
                ip: "2001:db8::1".parse().unwrap(),
                port: 8080,
            },
        );

        let r = handle_create_permission_request(&req, &m).await;
        // Should succeed but log a warning and return error response
        assert!(r.is_ok(), "PeerAddressFamilyMismatch: {r:?}");
        assert_last_response(
            &conn,
            METHOD_CREATE_PERMISSION,
            CLASS_ERROR_RESPONSE,
            CODE_PEER_ADDR_FAMILY_MISMATCH,
            "PeerAddressFamilyMismatch",
        );
    }

    // PermissionDenied
    {
        conn.reset();

        let deny_am = new_manager("0.0.0.0:0", Some(false));

        let mut deny_req = req.clone();
        deny_req.allocation_manager = deny_am.clone();
        let r = deny_req.allocation_manager.create_allocation(
            Some(&five_tuple(&deny_req)),
            Some(deny_req.conn.clone()),
            PROTO_UDP,
            0,
            HOUR,
            TEST_USER,
            "",
            REQUESTED_FAMILY_IPV4,
        );
        assert!(r.is_ok());

        let mut m = request(&static_key);
        add(
            &mut m,
            &PeerAddress {
                ip: "192.168.1.2".parse().unwrap(),
                port: 8080,
            },
        );

        let r = handle_create_permission_request(&deny_req, &m).await;
        assert!(r.is_ok(), "PermissionDenied: {r:?}");
        assert_last_response(
            &conn,
            METHOD_CREATE_PERMISSION,
            CLASS_ERROR_RESPONSE,
            CODE_FORBIDDEN,
            "PermissionDenied",
        );
        let _ = deny_am.close();
    }

    // NoPeerAddress
    {
        conn.reset();
        let m = request(&static_key);
        // No peer address added

        let r = handle_create_permission_request(&req, &m).await;
        // Should return error response (addCount == 0)
        assert!(r.is_ok(), "NoPeerAddress: {r:?}");
        assert_last_response(
            &conn,
            METHOD_CREATE_PERMISSION,
            CLASS_ERROR_RESPONSE,
            CODE_BAD_REQUEST,
            "NoPeerAddress",
        );
    }

    let _ = allocation_manager.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_handle_send_indication() {
    let conn = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let allocation_manager = new_manager("0.0.0.0:0", None);

    let (nonce_hash, _) = nonce_and_key();
    let req = new_request(
        allocation_manager.clone(),
        nonce_hash,
        conn.clone(),
        udp("127.0.0.1:5000"),
        None,
    );

    // NoAllocationFound
    {
        let m = new_msg(MessageType::new(METHOD_SEND, CLASS_INDICATION));

        let r = handle_send_indication(&req, &m).await;
        assert!(
            matches!(r, Err(Error::NoAllocationFoundFor(_))),
            "NoAllocationFound: {r:?}"
        );
    }

    // Create an allocation for the remaining tests
    let alloc = req
        .allocation_manager
        .create_allocation(
            Some(&five_tuple(&req)),
            Some(req.conn.clone()),
            PROTO_UDP,
            0,
            HOUR,
            "",
            "",
            REQUESTED_FAMILY_IPV4,
        )
        .unwrap();

    // MissingDataAttribute
    {
        let m = new_msg(MessageType::new(METHOD_SEND, CLASS_INDICATION));

        assert!(
            handle_send_indication(&req, &m).await.is_err(),
            "MissingDataAttribute"
        );
    }

    // MissingPeerAddress
    {
        let mut m = new_msg(MessageType::new(METHOD_SEND, CLASS_INDICATION));
        add(&mut m, &Data(b"test data".to_vec()));

        assert!(
            handle_send_indication(&req, &m).await.is_err(),
            "MissingPeerAddress"
        );
    }

    // NoPermission
    {
        let mut m = new_msg(MessageType::new(METHOD_SEND, CLASS_INDICATION));
        add(&mut m, &Data(b"test data".to_vec()));
        add(
            &mut m,
            &PeerAddress {
                ip: "192.168.1.1".parse().unwrap(),
                port: 8080,
            },
        );

        let r = handle_send_indication(&req, &m).await;
        assert!(
            matches!(r, Err(Error::NoPermission(_))),
            "NoPermission: {r:?}"
        );
    }

    // Success
    {
        // Add permission for the peer
        let peer_addr = udp("192.0.2.1:8080");
        alloc.add_permission(Permission::new(peer_addr, Logger::default(), 5 * MINUTE));

        let mut m = new_msg(MessageType::new(METHOD_SEND, CLASS_INDICATION));
        add(&mut m, &Data(TEST_USER.as_bytes().to_vec()));
        add(
            &mut m,
            &PeerAddress {
                ip: "192.0.2.1".parse().unwrap(),
                port: 8080,
            },
        );

        // May fail due to network issues, but shouldn't panic
        let _ = handle_send_indication(&req, &m).await;
    }

    let _ = allocation_manager.close();
    let _ = conn.close();
}

// On one thread, so that the warnings it logs are captured.
#[tokio::test]
async fn test_handle_channel_bind_request() {
    let conn = Arc::new(CapturePacketConn::new(udp("127.0.0.1:3478")));

    let allocation_manager = new_manager("0.0.0.0:0", None);

    let (nonce_hash, static_key) = nonce_and_key();

    let mut req = new_request(
        allocation_manager.clone(),
        nonce_hash,
        conn.clone(),
        udp("127.0.0.1:5000"),
        Some(static_auth(TEST_USER, &static_key)),
    );
    req.channel_bind_timeout = 10 * MINUTE;
    req.permission_timeout = 5 * MINUTE;

    let base = |static_key: &str| {
        let mut m = new_msg(MessageType::new(METHOD_CHANNEL_BIND, CLASS_REQUEST));
        add_auth(&mut m, static_key, TEST_USER);
        m
    };
    let build_auth_msg = |ch: u16, peer_ip: &str, peer_port: u16| {
        let mut m = base(&static_key);
        add(&mut m, &ChannelNumber(ch));
        add(
            &mut m,
            &PeerAddress {
                ip: peer_ip.parse().unwrap(),
                port: peer_port,
            },
        );
        m
    };

    // NoAllocationFound
    {
        conn.reset();
        let m = base(&static_key);

        let r = handle_channel_bind_request(&req, &m).await;
        assert!(
            matches!(r, Err(Error::NoAllocationFoundFor(_))),
            "NoAllocationFound: {r:?}"
        );
    }

    // Create an allocation for the remaining tests
    let r = req.allocation_manager.create_allocation(
        Some(&five_tuple(&req)),
        Some(req.conn.clone()),
        PROTO_UDP,
        0,
        HOUR,
        TEST_USER,
        "",
        REQUESTED_FAMILY_IPV4,
    );
    assert!(r.is_ok());

    // MissingChannelNumber
    {
        conn.reset();
        let m = base(&static_key);

        assert!(
            handle_channel_bind_request(&req, &m).await.is_err(),
            "MissingChannelNumber"
        );
    }

    // MissingPeerAddress
    {
        conn.reset();
        let mut m = base(&static_key);
        add(&mut m, &ChannelNumber(0x4000));

        assert!(
            handle_channel_bind_request(&req, &m).await.is_err(),
            "MissingPeerAddress"
        );
    }

    // Peer address family mismatch (IPv6 peer with IPv4 allocation)
    // PeerAddressFamilyMismatch
    {
        conn.reset();
        let mut m = base(&static_key);
        add(&mut m, &ChannelNumber(0x4000));
        // Try to bind IPv6 peer to IPv4 allocation
        add(
            &mut m,
            &PeerAddress {
                ip: "2001:db8::1".parse().unwrap(),
                port: 8080,
            },
        );

        let r = handle_channel_bind_request(&req, &m).await;
        assert!(
            matches!(r, Err(Error::PeerAddressFamilyMismatch)),
            "PeerAddressFamilyMismatch: {r:?}"
        );
        assert_last_response(
            &conn,
            METHOD_CHANNEL_BIND,
            CLASS_ERROR_RESPONSE,
            CODE_PEER_ADDR_FAMILY_MISMATCH,
            "PeerAddressFamilyMismatch",
        );
    }

    // PermissionDenied
    {
        conn.reset();

        let deny_am = new_manager("0.0.0.0:0", Some(false));

        let mut deny_req = req.clone();
        deny_req.allocation_manager = deny_am.clone();
        let r = deny_req.allocation_manager.create_allocation(
            Some(&five_tuple(&deny_req)),
            Some(deny_req.conn.clone()),
            PROTO_UDP,
            0,
            HOUR,
            TEST_USER,
            "",
            REQUESTED_FAMILY_IPV4,
        );
        assert!(r.is_ok());

        let m = build_auth_msg(0x4000, "192.168.1.1", 8080);
        let r = handle_channel_bind_request(&deny_req, &m).await;
        assert!(r.is_err(), "PermissionDenied");
        assert_last_response(
            &conn,
            METHOD_CHANNEL_BIND,
            CLASS_ERROR_RESPONSE,
            CODE_UNAUTHORIZED,
            "PermissionDenied",
        );
        let _ = deny_am.close();
    }

    // RejectSamePeerDifferentChannel
    {
        conn.reset();
        let logger = LogCapture::start(tracing::Level::WARN);

        let m1 = build_auth_msg(0x4010, "192.168.1.1", 8080);
        let r = handle_channel_bind_request(&req, &m1).await;
        assert!(r.is_ok(), "RejectSamePeerDifferentChannel: {r:?}");

        conn.reset();
        let m2 = build_auth_msg(0x4011, "192.168.1.1", 8080);
        let r = handle_channel_bind_request(&req, &m2).await;
        assert!(
            matches!(
                r,
                Err(Error::Allocation(
                    allocation::Error::SamePeerDifferentChannel
                ))
            ),
            "RejectSamePeerDifferentChannel: {r:?}"
        );
        assert_last_response(
            &conn,
            METHOD_CHANNEL_BIND,
            CLASS_ERROR_RESPONSE,
            CODE_BAD_REQUEST,
            "RejectSamePeerDifferentChannel",
        );
        assert!(
            logger
                .text()
                .contains("peer 192.168.1.1:8080 already bound"),
            "warn logs: {}",
            logger.text()
        );
    }

    // RejectSameChannelDifferentPeer
    {
        conn.reset();
        let logger = LogCapture::start(tracing::Level::WARN);

        let m1 = build_auth_msg(0x4020, "192.168.2.1", 8080);
        let r = handle_channel_bind_request(&req, &m1).await;
        assert!(r.is_ok(), "RejectSameChannelDifferentPeer: {r:?}");

        conn.reset();
        let m2 = build_auth_msg(0x4020, "192.168.2.2", 8080);
        let r = handle_channel_bind_request(&req, &m2).await;
        assert!(
            matches!(
                r,
                Err(Error::Allocation(
                    allocation::Error::SameChannelDifferentPeer
                ))
            ),
            "RejectSameChannelDifferentPeer: {r:?}"
        );
        assert_last_response(
            &conn,
            METHOD_CHANNEL_BIND,
            CLASS_ERROR_RESPONSE,
            CODE_BAD_REQUEST,
            "RejectSameChannelDifferentPeer",
        );
        assert!(
            logger.text().contains("channel 16416 already bound"),
            "warn logs: {}",
            logger.text()
        );
    }

    // Success
    {
        conn.reset();
        let m = build_auth_msg(0x4030, "192.168.3.1", 8080);

        let r = handle_channel_bind_request(&req, &m).await;
        assert!(r.is_ok(), "Success: {r:?}");
    }

    let _ = allocation_manager.close();
}

struct CapturePacketConn {
    local_addr: Addr,
    // lastWrite and closed, under `mu`
    mu: Mutex<(Vec<u8>, bool)>,
}

impl CapturePacketConn {
    fn new(local_addr: Addr) -> CapturePacketConn {
        CapturePacketConn {
            local_addr,
            mu: Mutex::new((Vec::new(), false)),
        }
    }

    fn reset(&self) {
        self.mu.lock().0.clear();
    }

    fn last_write(&self) -> Vec<u8> {
        self.mu.lock().0.clone()
    }
}

impl PacketConn for CapturePacketConn {
    fn read_from<'a>(&'a self, _: &'a mut [u8]) -> BoxFuture<'a, std::io::Result<(usize, Addr)>> {
        Box::pin(async { Err(err_closed()) })
    }

    fn write_to<'a>(&'a self, p: &'a [u8], _: Addr) -> BoxFuture<'a, std::io::Result<usize>> {
        Box::pin(async move {
            let mut mu = self.mu.lock();
            if mu.1 {
                return Err(err_closed());
            }
            mu.0 = p.to_vec();

            Ok(p.len())
        })
    }

    fn close(&self) -> std::io::Result<()> {
        self.mu.lock().1 = true;

        Ok(())
    }

    fn local_addr(&self) -> Addr {
        self.local_addr
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_handle_allocation_request() {
    let conn = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let allocation_manager = new_manager("0.0.0.0:0", None);

    let (nonce_hash, static_key) = nonce_and_key();

    let mut req = new_request(
        allocation_manager.clone(),
        nonce_hash,
        conn.clone(),
        udp("127.0.0.1:5000"),
        Some(static_auth("test-id", &static_key)),
    );
    req.allocation_lifetime = DEFAULT_LIFETIME_GO;

    // First allocation request with transaction ID 1
    let mut msg1 = new_msg(MessageType::new(METHOD_ALLOCATE, CLASS_REQUEST));
    add(
        &mut msg1,
        &RequestedTransport {
            protocol: PROTO_UDP,
        },
    );
    add_auth(&mut msg1, &static_key, "test-name");

    // First request should succeed
    let r = handle_allocate_request(&req, &msg1).await;
    assert!(r.is_ok(), "{r:?}");

    // Verify allocation exists
    let five_tuple = five_tuple(&req);
    assert!(req.allocation_manager.get_allocation(&five_tuple).is_some());

    // Test if allocation was created for the user-id instead of the username
    assert!(
        req.allocation_manager
            .get_allocation_for_user_id(&five_tuple, "test-id")
            .is_some()
    );
    assert!(
        req.allocation_manager
            .get_allocation_for_user_id(&five_tuple, "test-name")
            .is_none()
    );

    let _ = allocation_manager.close();
    let _ = conn.close();
}

// TestDuplicateAllocationRequest tests the scenario from issue #229
// where a client makes multiple allocation requests from the same 5-tuple
// but with different transaction IDs.
//
// Per RFC 5766 Section 6.2, the server checks if the 5-tuple is currently in use by an
// existing allocation. If yes, the server rejects the request with a 437 (Allocation Mismatch)
// error if the transaction ID differs from the cached transaction ID.
#[tokio::test(flavor = "multi_thread")]
async fn test_handle_duplicate_allocation_request() {
    let conn = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let allocation_manager = new_manager("0.0.0.0:0", None);

    let (nonce_hash, static_key) = nonce_and_key();

    let mut req = new_request(
        allocation_manager.clone(),
        nonce_hash,
        conn.clone(),
        udp("127.0.0.1:5000"),
        Some(static_auth(TEST_USER, &static_key)),
    );
    req.allocation_lifetime = DEFAULT_LIFETIME_GO;

    // First allocation request with transaction ID 1
    let mut msg1 = new_msg(MessageType::new(METHOD_ALLOCATE, CLASS_REQUEST));
    add(
        &mut msg1,
        &RequestedTransport {
            protocol: PROTO_UDP,
        },
    );
    add_auth(&mut msg1, &static_key, TEST_USER);

    // First request should succeed
    let r = handle_allocate_request(&req, &msg1).await;
    assert!(r.is_ok(), "{r:?}");

    // Verify allocation exists
    let five_tuple = five_tuple(&req);
    let alloc = req.allocation_manager.get_allocation(&five_tuple).unwrap();

    // Second allocation request from the same 5-tuple but with a different transaction ID
    let mut msg2 = new_msg(MessageType::new(METHOD_ALLOCATE, CLASS_REQUEST)); // Different transaction ID
    add(
        &mut msg2,
        &RequestedTransport {
            protocol: PROTO_UDP,
        },
    );
    add_auth(&mut msg2, &static_key, TEST_USER);

    // Second request should fail with errRelayAlreadyAllocatedForFiveTuple
    // This is the error reported in issue #229
    let r = handle_allocate_request(&req, &msg2).await;
    assert!(
        matches!(r, Err(Error::RelayAlreadyAllocatedForFiveTuple)),
        "{r:?}"
    );

    // Test retry with same transaction ID (should succeed and return cached response)
    // Per RFC 5766 Section 6.2, when the transaction ID matches the cached transaction ID,
    // the server returns the cached response (proper retry mechanism).
    // RetryWithSameTransactionID
    {
        // Retry the first request with the same transaction ID
        // This should succeed and return the cached response
        let r = handle_allocate_request(&req, &msg1).await;
        assert!(
            r.is_ok(),
            "Retry with same transaction ID should succeed: {r:?}"
        );

        // Verify the allocation still exists and hasn't changed
        let alloc_retry = req.allocation_manager.get_allocation(&five_tuple).unwrap();
        assert_eq!(
            alloc.relay_addr, alloc_retry.relay_addr,
            "Relay address should be the same"
        );
    }

    let _ = allocation_manager.close();
    let _ = conn.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_handle_channel_data() {
    let conn = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let allocation_manager = new_manager("0.0.0.0:0", None);

    let (nonce_hash, _) = nonce_and_key();
    let mut req = new_request(
        allocation_manager.clone(),
        nonce_hash,
        conn.clone(),
        udp("127.0.0.1:5000"),
        None,
    );
    req.channel_bind_timeout = 10 * MINUTE;

    let channel_data = || ChannelData {
        number: ChannelNumber(0x4000),
        data: b"test".to_vec(),
        ..Default::default()
    };

    // NoAllocationFound
    {
        let r = handle_channel_data(&req, &channel_data()).await;
        assert!(
            matches!(r, Err(Error::NoAllocationFoundFor(_))),
            "NoAllocationFound: {r:?}"
        );
    }

    // Create an allocation for remaining tests
    let alloc = req
        .allocation_manager
        .create_allocation(
            Some(&five_tuple(&req)),
            Some(req.conn.clone()),
            PROTO_UDP,
            0,
            HOUR,
            "",
            "",
            REQUESTED_FAMILY_IPV4,
        )
        .unwrap();

    // NoChannelBinding
    {
        let r = handle_channel_data(&req, &channel_data()).await;
        assert!(
            matches!(r, Err(Error::NoSuchChannelBind(_))),
            "NoChannelBinding: {r:?}"
        );
    }

    // Success
    {
        // Add a channel binding
        let peer_addr = udp("192.0.2.1:8080");
        let r = alloc.add_channel_bind(
            ChannelBind::new(ChannelNumber(0x4000), peer_addr, Logger::default()),
            10 * MINUTE,
            5 * MINUTE,
        );
        assert!(r.is_ok());

        // May fail due to network issues, but shouldn't panic
        let _ = handle_channel_data(&req, &channel_data()).await;
    }

    let _ = allocation_manager.close();
    let _ = conn.close();
}

// Not here: TestConnectRequestPermissionDenied, see the header.
