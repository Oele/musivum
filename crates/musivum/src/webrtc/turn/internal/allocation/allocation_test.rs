// Ported from pion/turn, internal/allocation/allocation_test.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. Not ported, with what they test (RFC 6062's TCP
// relays, which are not ported): TestTCPRelay_E2E and the six TestAllocationConnHandler_*
// tests, with their mocks. Go's comparison of permissions and channel bindings by value is a
// comparison of the same `Arc`.

use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;

use super::*;
use crate::go::time::{self, Duration};
use crate::protocol::logger;
use crate::webrtc::stun::message::{Getter, is_stun_message};
use crate::webrtc::turn::internal::allocation::{
    AllocateListenerConfig, BoxError, ChannelBind, DEFAULT_PERMISSION_TIMEOUT, Manager,
    ManagerConfig, Permission,
};
use crate::webrtc::turn::internal::proto::{
    DEFAULT_LIFETIME_GO, MIN_CHANNEL_NUMBER, PROTO_UDP, REQUESTED_FAMILY_IPV4,
    REQUESTED_FAMILY_IPV6,
};
use crate::webrtc::turn::net;

fn new_alloc() -> Allocation {
    Allocation::new(None, None, EventHandler::default(), logger::get_logger())
}

fn udp(s: &str) -> Addr {
    Addr::Udp(s.parse().unwrap())
}

#[test]
fn test_address_family() {
    // IPv4
    {
        let mut alloc = new_alloc();
        alloc.address_family = REQUESTED_FAMILY_IPV4;
        assert_eq!(REQUESTED_FAMILY_IPV4, alloc.address_family(), "IPv4");
    }

    // IPv6
    {
        let mut alloc = new_alloc();
        alloc.address_family = REQUESTED_FAMILY_IPV6;
        assert_eq!(REQUESTED_FAMILY_IPV6, alloc.address_family(), "IPv6");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_permission() {
    let alloc = Arc::new(new_alloc());

    let addr = udp("127.0.0.1:3478");
    let addr2 = udp("127.0.0.1:3479");
    let addr3 = udp("127.0.0.2:3478");

    let perms = Permission::new(addr, logger::get_logger(), DEFAULT_PERMISSION_TIMEOUT);
    let perms2 = Permission::new(addr2, logger::get_logger(), DEFAULT_PERMISSION_TIMEOUT);
    let perms3 = Permission::new(addr3, logger::get_logger(), DEFAULT_PERMISSION_TIMEOUT);

    alloc.add_permission(perms.clone());
    alloc.add_permission(perms2.clone());
    alloc.add_permission(perms3.clone());

    let found_p1 = alloc.get_permission(addr).unwrap();
    assert!(Arc::ptr_eq(&perms, &found_p1), "Should keep the first one.");

    let found_p2 = alloc.get_permission(addr2).unwrap();
    assert!(
        Arc::ptr_eq(&perms, &found_p2),
        "Second one should be ignored."
    );

    let found_p3 = alloc.get_permission(addr3).unwrap();
    assert!(
        Arc::ptr_eq(&perms3, &found_p3),
        "Permission with another IP should be found"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_permission() {
    let alloc = Arc::new(new_alloc());

    let addr = udp("127.0.0.1:3478");

    let p = Permission::new(addr, logger::get_logger(), DEFAULT_PERMISSION_TIMEOUT);

    alloc.add_permission(p.clone());
    assert!(
        p.allocation
            .lock()
            .upgrade()
            .is_some_and(|a| Arc::ptr_eq(&a, &alloc)),
        "Permission's allocation should be the adder."
    );

    let found_permission = alloc.get_permission(p.addr).unwrap();
    assert!(Arc::ptr_eq(&p, &found_permission));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_permission() {
    let alloc = Arc::new(new_alloc());

    let addr = udp("127.0.0.1:3478");

    let p = Permission::new(addr, logger::get_logger(), DEFAULT_PERMISSION_TIMEOUT);

    alloc.add_permission(p.clone());

    let found_permission = alloc.get_permission(p.addr);
    assert!(
        found_permission.is_some_and(|f| Arc::ptr_eq(&p, &f)),
        "Got permission is not same as the the added."
    );

    alloc.remove_permission(p.addr);

    let found_permission = alloc.get_permission(p.addr);
    assert!(
        found_permission.is_none(),
        "Got permission should be nil after removed."
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_channel_bind() {
    let alloc = Arc::new(new_alloc());

    let addr = udp("127.0.0.1:3478");

    let c = ChannelBind::new(
        ChannelNumber(MIN_CHANNEL_NUMBER),
        addr,
        logger::get_logger(),
    );

    let err = alloc.add_channel_bind(c.clone(), DEFAULT_LIFETIME_GO, DEFAULT_PERMISSION_TIMEOUT);
    assert!(err.is_ok(), "should succeed");
    assert!(
        c.allocation
            .lock()
            .upgrade()
            .is_some_and(|a| Arc::ptr_eq(&a, &alloc)),
        "allocation should be the caller."
    );

    let c2 = ChannelBind::new(
        ChannelNumber(MIN_CHANNEL_NUMBER + 1),
        addr,
        logger::get_logger(),
    );
    let err = alloc.add_channel_bind(c2, DEFAULT_LIFETIME_GO, DEFAULT_PERMISSION_TIMEOUT);
    assert!(
        matches!(err, Err(Error::SamePeerDifferentChannel)),
        "should fail with conflicted peer address"
    );

    let addr2 = udp("127.0.0.1:3479");
    let c3 = ChannelBind::new(
        ChannelNumber(MIN_CHANNEL_NUMBER),
        addr2,
        logger::get_logger(),
    );
    let err = alloc.add_channel_bind(c3, DEFAULT_LIFETIME_GO, DEFAULT_PERMISSION_TIMEOUT);
    assert!(
        matches!(err, Err(Error::SameChannelDifferentPeer)),
        "should fail with conflicted number"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_channel_by_number() {
    let alloc = Arc::new(new_alloc());

    let addr = udp("127.0.0.1:3478");

    let c = ChannelBind::new(
        ChannelNumber(MIN_CHANNEL_NUMBER),
        addr,
        logger::get_logger(),
    );

    let _ = alloc.add_channel_bind(c.clone(), DEFAULT_LIFETIME_GO, DEFAULT_PERMISSION_TIMEOUT);

    let exist_channel = alloc.get_channel_by_number(c.number).unwrap();
    assert!(Arc::ptr_eq(&c, &exist_channel));

    let not_exist_channel = alloc.get_channel_by_number(ChannelNumber(MIN_CHANNEL_NUMBER + 1));
    assert!(
        not_exist_channel.is_none(),
        "should be nil for not existed channel."
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_channel_by_addr() {
    let alloc = Arc::new(new_alloc());

    let addr = udp("127.0.0.1:3478");

    let c = ChannelBind::new(
        ChannelNumber(MIN_CHANNEL_NUMBER),
        addr,
        logger::get_logger(),
    );

    let _ = alloc.add_channel_bind(c.clone(), DEFAULT_LIFETIME_GO, DEFAULT_PERMISSION_TIMEOUT);

    let exist_channel = alloc.get_channel_by_addr(c.peer).unwrap();
    assert!(Arc::ptr_eq(&c, &exist_channel));

    let addr2 = udp("127.0.0.1:3479");
    let not_exist_channel = alloc.get_channel_by_addr(addr2);
    assert!(
        not_exist_channel.is_none(),
        "should be nil for not existed channel."
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_channel_bind_same_ip_port_different_zone() {
    let alloc = Arc::new(new_alloc());

    let ip: Ipv6Addr = "fe80::1".parse().unwrap();
    let peer1 = Addr::Udp(SocketAddr::V6(SocketAddrV6::new(ip, 3478, 0, 1)));
    let peer2 = Addr::Udp(SocketAddr::V6(SocketAddrV6::new(ip, 3478, 0, 2)));

    let c1 = ChannelBind::new(
        ChannelNumber(MIN_CHANNEL_NUMBER),
        peer1,
        logger::get_logger(),
    );
    let err = alloc.add_channel_bind(c1, DEFAULT_LIFETIME_GO, DEFAULT_PERMISSION_TIMEOUT);
    assert!(err.is_ok());

    // Rebinding the same channel number to the same IP:port should succeed even
    // if the IPv6 Zone differs.
    let c2 = ChannelBind::new(
        ChannelNumber(MIN_CHANNEL_NUMBER),
        peer2,
        logger::get_logger(),
    );
    let err = alloc.add_channel_bind(c2, DEFAULT_LIFETIME_GO, DEFAULT_PERMISSION_TIMEOUT);
    assert!(err.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_channel_bind() {
    let alloc = Arc::new(new_alloc());

    let addr = udp("127.0.0.1:3478");

    let c = ChannelBind::new(
        ChannelNumber(MIN_CHANNEL_NUMBER),
        addr,
        logger::get_logger(),
    );

    let _ = alloc.add_channel_bind(c.clone(), DEFAULT_LIFETIME_GO, DEFAULT_PERMISSION_TIMEOUT);

    alloc.remove_channel_bind(c.number);

    let channel_by_number = alloc.get_channel_by_number(c.number);
    assert!(channel_by_number.is_none());

    let channel_by_addr = alloc.get_channel_by_addr(c.peer);
    assert!(channel_by_addr.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_allocation_refresh() {
    let mut alloc = new_alloc();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    alloc.lifetime_timer = Some(time::after_func(DEFAULT_LIFETIME_GO, move || {
        let _ = tx.send(());
    }));
    alloc.refresh(Duration::ZERO);
    rx.recv().await.unwrap();

    // LifetimeTimer has expired
    assert!(!alloc.lifetime_timer.as_ref().unwrap().stop());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_allocation_close() {
    let network = "udp";

    let l = net::listen_packet(network, "0.0.0.0:0").unwrap();

    let mut alloc = new_alloc();
    alloc.relay_packet_conn = Some(l.clone());
    // Add mock lifetimeTimer
    alloc.lifetime_timer = Some(time::after_func(DEFAULT_LIFETIME_GO, || {}));
    let alloc = Arc::new(alloc);

    // Add channel
    let addr = udp("127.0.0.1:3478");

    let c = ChannelBind::new(
        ChannelNumber(MIN_CHANNEL_NUMBER),
        addr,
        logger::get_logger(),
    );
    let _ = alloc.add_channel_bind(c, DEFAULT_LIFETIME_GO, DEFAULT_PERMISSION_TIMEOUT);

    // Add permission
    alloc.add_permission(Permission::new(
        addr,
        logger::get_logger(),
        DEFAULT_PERMISSION_TIMEOUT,
    ));

    assert!(alloc.close().is_ok(), "should succeed");
    assert!(is_close(l.as_ref()), "should be closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_packet_handler() {
    let network = "udp";

    let manager = new_test_manager().unwrap();

    // TURN server initialization
    let turn_socket = net::listen_packet(network, "127.0.0.1:0").unwrap();

    // Client listener initialization
    let client_listener = net::listen_packet(network, "127.0.0.1:0").unwrap();

    let (data_tx, mut data_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    // Client listener read data
    {
        let client_listener = client_listener.clone();
        tokio::spawn(async move {
            let mut buffer = vec![0u8; 1600];
            loop {
                let Ok((n, _)) = client_listener.read_from(&mut buffer).await else {
                    return;
                };

                let _ = data_tx.send(buffer[..n].to_vec());
            }
        });
    }

    let alloc = manager.create_allocation(
        Some(&FiveTuple {
            src_addr: Some(client_listener.local_addr()),
            dst_addr: Some(turn_socket.local_addr()),
            ..Default::default()
        }),
        Some(turn_socket.clone()),
        PROTO_UDP,
        0,
        DEFAULT_LIFETIME_GO,
        "",
        "",
        REQUESTED_FAMILY_IPV4,
    );

    assert!(alloc.is_ok(), "should succeed");
    let alloc = alloc.unwrap();

    let peer_listener1 = net::listen_packet(network, "127.0.0.1:0").unwrap();
    let peer_listener2 = net::listen_packet(network, "127.0.0.1:0").unwrap();

    // Add permission with peer1 address
    alloc.add_permission(Permission::new(
        peer_listener1.local_addr(),
        manager_log(),
        DEFAULT_PERMISSION_TIMEOUT,
    ));
    // Add channel with min channel number and peer2 address
    let channel_bind = ChannelBind::new(
        ChannelNumber(MIN_CHANNEL_NUMBER),
        peer_listener2.local_addr(),
        manager_log(),
    );
    let _ = alloc.add_channel_bind(
        channel_bind.clone(),
        DEFAULT_LIFETIME_GO,
        DEFAULT_PERMISSION_TIMEOUT,
    );

    let port = alloc
        .relay_packet_conn
        .as_ref()
        .unwrap()
        .local_addr()
        .port();
    let relay_addr_with_host = udp(&format!("127.0.0.1:{port}"));

    // Test for permission and data message
    let target_text = "permission";
    let _ = peer_listener1
        .write_to(target_text.as_bytes(), relay_addr_with_host)
        .await;
    let data = data_rx.recv().await.unwrap();

    // Resolve stun data message
    assert!(is_stun_message(&data), "should be stun message");

    let mut msg = crate::webrtc::stun::message::Message::new();
    msg.raw = data;
    assert!(msg.decode().is_ok(), "decode data to stun message failed");

    let mut msg_data = Data::default();
    assert!(
        msg_data.get_from(&msg).is_ok(),
        "get data from stun message failed"
    );
    assert_eq!(
        target_text.as_bytes(),
        &msg_data.0[..],
        "get message doesn't equal the target text"
    );

    // Test for channel bind and channel data
    let target_text2 = "channel bind";
    let _ = peer_listener2
        .write_to(target_text2.as_bytes(), relay_addr_with_host)
        .await;
    let data = data_rx.recv().await.unwrap();

    // Resolve channel data
    assert!(proto::is_channel_data(&data), "should be channel data");

    let mut channel_data = ChannelData {
        raw: data,
        ..Default::default()
    };
    let err = channel_data.decode();
    assert!(err.is_ok(), "channel data decode with error: {err:?}");
    assert_eq!(
        channel_bind.number, channel_data.number,
        "get channel data's number is invalid"
    );
    assert_eq!(
        target_text2.as_bytes(),
        &channel_data.data[..],
        "get data doesn't equal the target text."
    );

    // Listeners close
    let _ = manager.close();
    let _ = client_listener.close();
    let _ = peer_listener1.close();
    let _ = peer_listener2.close();
}

// Not here: TestTCPRelay_E2E, see the header.

#[test]
fn test_response_cache() {
    let alloc = new_alloc();
    let transaction_id = [1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let response_attrs: Vec<ResponseAttr> =
        vec![Arc::new(proto::Lifetime(proto::DEFAULT_LIFETIME))];
    alloc.set_response_cache(transaction_id, response_attrs.clone());

    let (cache_id, cache_attr) = alloc.get_response_cache();
    assert_eq!(transaction_id, cache_id);
    assert_eq!(response_attrs.len(), cache_attr.len());
    assert!(
        response_attrs
            .iter()
            .zip(&cache_attr)
            .all(|(a, b)| Arc::ptr_eq(a, b))
    );
}

// Not here: the six TestAllocationConnHandler_* tests and their mocks, see the header.

fn manager_log() -> crate::protocol::logger::Logger {
    logger::get_logger()
}

// newTestManager and isClose of allocation_manager_test.go, here for both test files
pub(crate) fn new_test_manager() -> Result<Arc<Manager>, Error> {
    let config = ManagerConfig {
        leveled_logger: Some(logger::get_logger()),
        allocate_packet_conn: Some(Arc::new(|_: AllocateListenerConfig| {
            let conn =
                net::listen_packet("udp4", "0.0.0.0:0").map_err(|e| Box::new(e) as BoxError)?;
            let addr = conn.local_addr();

            Ok((conn as Arc<dyn PacketConn>, addr))
        })),
        ..Default::default()
    };

    Manager::new(config)
}

pub(crate) fn is_close(conn: &dyn PacketConn) -> bool {
    match conn.close() {
        Err(err) => err.to_string().contains("use of closed network connection"),
        Ok(()) => false,
    }
}
