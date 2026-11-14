// Ported from pion/turn, internal/allocation/allocation_manager_test.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. `newTestManager` and `isClose` are in
// `allocation_test.rs`, for both files. TestNewManagerValidation checks what the configuration
// still has (no `AllocateListener` and `AllocateConn`, see `allocation_manager.rs`). Not
// ported, with what they test (RFC 6062's TCP relays, which are not ported):
// TestCreateTCPConnection, TestCreateTCPConnectionUsesAddressFamily,
// TestCreateTCPConnectionDuplicateDoesNotBlockManager, TestCreateTCPConnectionInvalidPeerAddress,
// TestCreateTCPConnectionInvalid and TestCreateTCPConnectionTimeout.

use std::net::{Ipv6Addr, SocketAddr};

use rand::RngExt;

use super::*;
use crate::webrtc::turn::internal::allocation::allocation::allocation_test::{
    is_close, new_test_manager,
};
use crate::webrtc::turn::internal::proto::{DEFAULT_LIFETIME_GO, PROTO_UDP, REQUESTED_FAMILY_IPV4};
use crate::webrtc::turn::net;

#[test]
fn test_new_manager_validation() {
    let mut cfg = ManagerConfig::default();

    let manager = Manager::new(cfg.clone());
    assert!(matches!(manager, Err(Error::AllocatePacketConnMustBeSet)));

    cfg.allocate_packet_conn = Some(Arc::new(|_| Err("unused".into())));
    let manager = Manager::new(cfg.clone());
    assert!(matches!(manager, Err(Error::LeveledLoggerMustBeSet)));

    cfg.leveled_logger = Some(Logger::default());
    let manager = Manager::new(cfg);
    assert!(manager.is_ok());
    assert!(manager.unwrap().close().is_ok());
}

// Test invalid Allocation creations.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_invalid_allocation() {
    let turn_socket = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let manager = new_test_manager().unwrap();

    let alloc = manager.create_allocation(
        None,
        Some(turn_socket.clone()),
        PROTO_UDP,
        0,
        DEFAULT_LIFETIME_GO,
        "",
        "",
        REQUESTED_FAMILY_IPV4,
    );
    assert!(
        alloc.is_err(),
        "Illegally created allocation with nil FiveTuple"
    );

    let alloc = manager.create_allocation(
        Some(&random_five_tuple()),
        None,
        PROTO_UDP,
        0,
        DEFAULT_LIFETIME_GO,
        "",
        "",
        REQUESTED_FAMILY_IPV4,
    );
    assert!(
        alloc.is_err(),
        "Illegally created allocation with nil turnSocket"
    );

    let alloc = manager.create_allocation(
        Some(&random_five_tuple()),
        Some(turn_socket.clone()),
        PROTO_UDP,
        0,
        Duration::ZERO,
        "",
        "",
        REQUESTED_FAMILY_IPV4,
    );
    assert!(
        alloc.is_err(),
        "Illegally created allocation with 0 lifetime"
    );

    assert!(manager.close().is_ok());
    assert!(turn_socket.close().is_ok());
}

// Test valid Allocation creations.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_allocation() {
    let turn_socket = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let manager = new_test_manager().unwrap();

    let five_tuple = random_five_tuple();
    let alloc = manager.create_allocation(
        Some(&five_tuple),
        Some(turn_socket.clone()),
        PROTO_UDP,
        0,
        DEFAULT_LIFETIME_GO,
        "",
        "",
        REQUESTED_FAMILY_IPV4,
    );
    assert!(alloc.is_ok(), "Failed to create allocation");

    let alloc = manager.get_allocation(&five_tuple);
    assert!(
        alloc.is_some(),
        "Failed to get allocation right after creation"
    );

    assert!(manager.close().is_ok());
    assert!(turn_socket.close().is_ok());
}

// Test that two allocations can't be created with the same FiveTuple.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_allocation_duplicate_five_tuple() {
    let turn_socket = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let manager = new_test_manager().unwrap();

    let five_tuple = random_five_tuple();
    let alloc = manager.create_allocation(
        Some(&five_tuple),
        Some(turn_socket.clone()),
        PROTO_UDP,
        0,
        DEFAULT_LIFETIME_GO,
        "",
        "",
        REQUESTED_FAMILY_IPV4,
    );
    assert!(alloc.is_ok(), "Failed to create allocation");

    let alloc = manager.create_allocation(
        Some(&five_tuple),
        Some(turn_socket.clone()),
        PROTO_UDP,
        0,
        DEFAULT_LIFETIME_GO,
        "",
        "",
        REQUESTED_FAMILY_IPV4,
    );
    assert!(
        alloc.is_err(),
        "Was able to create allocation with same FiveTuple twice"
    );

    assert!(manager.close().is_ok());
    assert!(turn_socket.close().is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_allocation() {
    let turn_socket = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let manager = new_test_manager().unwrap();

    let five_tuple = random_five_tuple();
    let alloc = manager.create_allocation(
        Some(&five_tuple),
        Some(turn_socket.clone()),
        PROTO_UDP,
        0,
        DEFAULT_LIFETIME_GO,
        "",
        "",
        REQUESTED_FAMILY_IPV4,
    );
    assert!(alloc.is_ok(), "Failed to create allocation");

    let alloc = manager.get_allocation(&five_tuple);
    assert!(
        alloc.is_some(),
        "Failed to get allocation right after creation"
    );

    manager.delete_allocation(&five_tuple);
    let alloc = manager.get_allocation(&five_tuple);
    assert!(
        alloc.is_none(),
        "Failed to delete allocation {five_tuple:?}"
    );

    assert!(manager.close().is_ok());
    assert!(turn_socket.close().is_ok());
}

// Test that allocation should be closed if timeout.
#[tokio::test(flavor = "multi_thread")]
async fn test_allocation_timeout() {
    let turn_socket = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let manager = new_test_manager().unwrap();

    let mut allocations = Vec::new();
    let lifetime = Duration::from_secs(1);

    for _ in 0..5 {
        let five_tuple = random_five_tuple();

        let alloc = manager
            .create_allocation(
                Some(&five_tuple),
                Some(turn_socket.clone()),
                PROTO_UDP,
                0,
                lifetime,
                "",
                "",
                REQUESTED_FAMILY_IPV4,
            )
            .unwrap_or_else(|e| panic!("Failed to create allocation with {five_tuple:?}: {e}"));

        allocations.push(alloc);
    }

    // Make sure all allocations timeout
    tokio::time::sleep((lifetime + Duration::from_secs(1)).to_std()).await;
    for alloc in &allocations {
        assert!(
            is_close(alloc.relay_packet_conn.as_ref().unwrap().as_ref()),
            "Allocation relay socket should be closed if lifetime timeout"
        );
    }

    assert!(manager.close().is_ok());
    assert!(turn_socket.close().is_ok());
}

// Test for manager close.
#[tokio::test(flavor = "multi_thread")]
async fn test_manager_close() {
    let turn_socket = net::listen_packet("udp4", "0.0.0.0:0").unwrap();

    let manager = new_test_manager().unwrap();

    let a1 = manager
        .create_allocation(
            Some(&random_five_tuple()),
            Some(turn_socket.clone()),
            PROTO_UDP,
            0,
            Duration::from_secs(1),
            "",
            "",
            REQUESTED_FAMILY_IPV4,
        )
        .unwrap();
    let a2 = manager
        .create_allocation(
            Some(&random_five_tuple()),
            Some(turn_socket.clone()),
            PROTO_UDP,
            0,
            Duration::from_secs(60),
            "",
            "",
            REQUESTED_FAMILY_IPV4,
        )
        .unwrap();
    let allocations = [a1, a2];

    // Make a1 timeout
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(manager.close().is_ok());

    for alloc in &allocations {
        assert!(
            is_close(alloc.relay_packet_conn.as_ref().unwrap().as_ref()),
            "Manager's allocations should be closed"
        );
    }

    assert!(turn_socket.close().is_ok());
}

fn random_five_tuple() -> FiveTuple {
    let mut rng = rand::rng();
    FiveTuple {
        src_addr: Some(Addr::Udp(SocketAddr::new(
            Ipv6Addr::UNSPECIFIED.into(),
            rng.random(),
        ))),
        dst_addr: Some(Addr::Udp(SocketAddr::new(
            Ipv6Addr::UNSPECIFIED.into(),
            rng.random(),
        ))),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_random_even_port() {
    let manager = new_test_manager().unwrap();

    let port = manager.get_random_even_port().unwrap();
    assert!(port > 0);
    assert!(port % 2 == 0);

    assert!(manager.close().is_ok());
}

// Not here: the TCP connection tests, see the header.

#[tokio::test(flavor = "multi_thread")]
async fn test_create_allocation_ipv6() {
    let manager = new_test_manager().unwrap();

    // Create a UDP6 socket for TURN
    let turn_socket = net::listen_packet("udp6", "[::]:0").unwrap();

    let mut five_tuple = random_five_tuple();
    // Change the source address to IPv6
    five_tuple.src_addr = Some(Addr::Udp("[::1]:5000".parse().unwrap()));

    // Create an IPv6 allocation
    let allocation = manager.create_allocation(
        Some(&five_tuple),
        Some(turn_socket.clone()),
        PROTO_UDP,
        0,
        DEFAULT_LIFETIME_GO,
        "",
        "",
        proto::REQUESTED_FAMILY_IPV6,
    );
    assert!(allocation.is_ok());
    assert_eq!(
        proto::REQUESTED_FAMILY_IPV6,
        allocation.unwrap().address_family()
    );

    // Verify allocation is retrievable
    let found_allocation = manager.get_allocation(&five_tuple);
    assert!(found_allocation.is_some());
    assert_eq!(
        proto::REQUESTED_FAMILY_IPV6,
        found_allocation.unwrap().address_family()
    );

    // Delete allocation
    manager.delete_allocation(&five_tuple);
    assert!(manager.get_allocation(&five_tuple).is_none());

    assert!(turn_socket.close().is_ok());
}
