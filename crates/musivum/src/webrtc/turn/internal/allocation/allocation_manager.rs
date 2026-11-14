// Ported from pion/turn, internal/allocation/allocation_manager.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust, on tokio.
// - `NewManager` gives an `Arc`; the manager keeps a `Weak` of itself for the timers and the
//   relay loops it starts, which hold it weakly. `lock` guards the allocations and the
//   reservations; `Close` and `DeleteAllocation` close allocations with it held, as there.
// - The functions of the configuration are `Arc`s; the logger is an `Option`, as the
//   original checks it is set.
// - RFC 6062's TCP relays are not ported: no `AllocateListener`, `AllocateConn`,
//   `AllocateConnConfig`, `tcpConnectionBindTimeout`, `CreateTCPConnection`,
//   `addTCPConnection`, `isDupeTCPConnection`, `GetTCPConnection` or `RemoveTCPConnection`.
//   `CreateAllocation` of a TCP relay fails with `Error::TcpRelayNotSupported` (which the
//   server answers with 508, as it does when the relay cannot be made).
// - A requested port is an `i64` (Go's `int`).

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Weak};

use parking_lot::RwLock;

use super::{Allocation, BoxError, Error, EventHandler, FiveTuple, FiveTupleFingerprint};
use crate::go::time::{self, Duration};
use crate::protocol::logger::Logger;
use crate::webrtc::turn::internal::proto::{self, REQUESTED_FAMILY_IPV6, RequestedAddressFamily};
use crate::webrtc::turn::net::{Addr, PacketConn};

// AllocateListenerConfig contains the parameters passed to the relay address allocator
// when creating a new UDP or TCP allocation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AllocateListenerConfig {
    // Network specifies the network type for the allocation: "udp4", "udp6", "tcp4", or "tcp6".
    pub network: String,
    // UserID is the authenticated user's identifier as returned by the AuthHandler.
    //
    // Note: The UserID is typcally the same as the TURN username, except for authentication
    // schemes that overload the username field with additional info (e.g., the lifetime of the
    // credential, as in the time-windowed credential mechanism in
    // https://datatracker.ietf.org/doc/html/draft-uberti-behave-turn-rest-00.
    pub user_id: String,
    // Realm is the TURN realm for this allocation.
    pub realm: String,
    // RequestedPort is the port requested by the client in the TURN Allocate request.
    // A value of 0 indicates that the client did not request a specific port and any
    // available port may be used.
    pub requested_port: i64,
}

/// `func(info AllocateListenerConfig) (net.PacketConn, net.Addr, error)`.
pub type AllocatePacketConnFn = Arc<
    dyn Fn(AllocateListenerConfig) -> Result<(Arc<dyn PacketConn>, Addr), BoxError> + Send + Sync,
>;

/// `func(sourceAddr net.Addr, peerIP net.IP) bool`.
pub type PermissionHandlerFn = Arc<dyn Fn(Addr, IpAddr) -> bool + Send + Sync>;

// ManagerConfig a bag of config params for Manager.
#[derive(Clone, Default)]
pub struct ManagerConfig {
    pub leveled_logger: Option<Logger>,
    pub allocate_packet_conn: Option<AllocatePacketConnFn>,
    pub permission_handler: Option<PermissionHandlerFn>,
    pub event_handler: EventHandler,
}

struct Reservation {
    token: String,
    port: i64,
}

// what `lock` guards
struct ManagerInner {
    allocations: HashMap<FiveTupleFingerprint, Arc<Allocation>>,
    reservations: Vec<Reservation>,
}

// Manager is used to hold active allocations.
pub struct Manager {
    this: Weak<Manager>,
    lock: RwLock<ManagerInner>,
    log: Logger,

    allocate_packet_conn: AllocatePacketConnFn,
    permission_handler: Option<PermissionHandlerFn>,
    pub event_handler: EventHandler,
}

impl Manager {
    // NewManager creates a new instance of Manager.
    pub fn new(config: ManagerConfig) -> Result<Arc<Manager>, Error> {
        let Some(allocate_packet_conn) = config.allocate_packet_conn else {
            return Err(Error::AllocatePacketConnMustBeSet);
        };
        let Some(log) = config.leveled_logger else {
            return Err(Error::LeveledLoggerMustBeSet);
        };

        Ok(Arc::new_cyclic(|this| Manager {
            this: this.clone(),
            log,
            lock: RwLock::new(ManagerInner {
                allocations: HashMap::with_capacity(64),
                reservations: Vec::new(),
            }),
            allocate_packet_conn,
            permission_handler: config.permission_handler,
            event_handler: config.event_handler,
        }))
    }

    // GetAllocation fetches the allocation matching the passed FiveTuple.
    pub fn get_allocation(&self, five_tuple: &FiveTuple) -> Option<Arc<Allocation>> {
        self.lock
            .read()
            .allocations
            .get(&five_tuple.fingerprint())
            .cloned()
    }

    // GetAllocationForUserID fetches the allocation matching the passed FiveTuple and Username.
    pub fn get_allocation_for_user_id(
        &self,
        five_tuple: &FiveTuple,
        user_id: &str,
    ) -> Option<Arc<Allocation>> {
        let allocation = self.get_allocation(five_tuple);
        allocation.filter(|a| a.user_id == user_id)
    }

    // AllocationCount returns the number of existing allocations.
    pub fn allocation_count(&self) -> usize {
        self.lock.read().allocations.len()
    }

    // Close closes the manager and closes all allocations it manages.
    pub fn close(&self) -> Result<(), Error> {
        let inner = self.lock.write();

        for a in inner.allocations.values() {
            a.close()?;
        }

        Ok(())
    }

    // CreateAllocation creates a new allocation and starts relaying.
    #[allow(clippy::too_many_arguments)]
    pub fn create_allocation(
        &self,
        five_tuple: Option<&FiveTuple>,
        turn_socket: Option<Arc<dyn PacketConn>>,
        protocol: proto::Protocol,
        requested_port: i64,
        lifetime: Duration,
        user_id: &str,
        realm: &str,
        address_family: RequestedAddressFamily,
    ) -> Result<Arc<Allocation>, Error> {
        let Some(five_tuple) = five_tuple else {
            return Err(Error::NilFiveTuple);
        };
        if five_tuple.src_addr.is_none() {
            return Err(Error::NilFiveTupleSrcAddr);
        }
        if five_tuple.dst_addr.is_none() {
            return Err(Error::NilFiveTupleDstAddr);
        }
        let Some(turn_socket) = turn_socket else {
            return Err(Error::NilTurnSocket);
        };
        if lifetime == Duration::ZERO {
            return Err(Error::LifetimeZero);
        }

        if self.get_allocation(five_tuple).is_some() {
            return Err(Error::DupeFiveTuple(format!("{five_tuple:?}")));
        }
        let mut alloc = Allocation::new(
            Some(turn_socket),
            Some(*five_tuple),
            self.event_handler.clone(),
            self.log.clone(),
        );
        alloc.user_id = user_id.to_string();
        alloc.realm = realm.to_string();
        alloc.address_family = RequestedAddressFamily(address_family.0);

        match protocol {
            proto::PROTO_UDP => {
                let mut network = "udp4";
                if address_family == REQUESTED_FAMILY_IPV6 {
                    network = "udp6";
                }
                let (conn, relay_addr) = (self.allocate_packet_conn)(AllocateListenerConfig {
                    network: network.to_string(),
                    user_id: user_id.to_string(),
                    realm: realm.to_string(),
                    requested_port,
                })
                .map_err(Error::Allocate)?;
                alloc.relay_packet_conn = Some(conn);
                alloc.relay_addr = Some(relay_addr);
            }
            proto::PROTO_TCP => {
                // RFC 6062's TCP relays: not ported (see the header)
                return Err(Error::TcpRelayNotSupported);
            }
            _ => {}
        }

        debugf!(
            self.log,
            "Listening on relay address: {}",
            alloc
                .relay_addr
                .map_or("<nil>".to_string(), |a| a.to_string())
        );

        {
            let manager = self.this.clone();
            let five_tuple = *five_tuple;
            alloc.lifetime_timer = Some(time::after_func(lifetime, move || {
                if let Some(manager) = manager.upgrade() {
                    manager.delete_allocation(&five_tuple);
                }
            }));
        }

        let alloc = Arc::new(alloc);
        self.lock
            .write()
            .allocations
            .insert(five_tuple.fingerprint(), alloc.clone());

        if let Some(on_allocation_created) = &self.event_handler.on_allocation_created {
            on_allocation_created(
                five_tuple.src_addr,
                five_tuple.dst_addr,
                &five_tuple.protocol.to_string(),
                user_id,
                realm,
                alloc.relay_addr,
                requested_port,
            );
        }

        // Only start the UDP relay loop for UDP allocations.
        if alloc.relay_packet_conn.is_some() {
            time::spawn(alloc.clone().packet_conn_handler(self.this.clone()));
        }

        Ok(alloc)
    }

    // DeleteAllocation removes an allocation.
    pub fn delete_allocation(&self, five_tuple: &FiveTuple) {
        let fingerprint = five_tuple.fingerprint();

        let allocation = self.lock.write().allocations.remove(&fingerprint);

        let Some(allocation) = allocation else {
            return;
        };

        {
            let _inner = self.lock.write();
            if let Err(err) = allocation.close() {
                errorf!(self.log, "Failed to close allocation: {}", err);
            }
        }

        if let Some(on_allocation_deleted) = &self.event_handler.on_allocation_deleted {
            on_allocation_deleted(
                five_tuple.src_addr,
                five_tuple.dst_addr,
                &five_tuple.protocol.to_string(),
                &allocation.user_id,
                &allocation.realm,
            );
        }
    }

    // CreateReservation stores the reservation for the token+port.
    pub fn create_reservation(&self, reservation_token: &str, port: i64) {
        {
            let manager = self.this.clone();
            let reservation_token = reservation_token.to_string();
            time::after_func(Duration::from_secs(30), move || {
                let Some(manager) = manager.upgrade() else {
                    return;
                };
                let mut inner = manager.lock.write();
                for i in (0..inner.reservations.len()).rev() {
                    if inner.reservations[i].token == reservation_token {
                        inner.reservations.remove(i);

                        return;
                    }
                }
            });
        }

        self.lock.write().reservations.push(Reservation {
            token: reservation_token.to_string(),
            port,
        });
    }

    // GetReservation returns the port for a given reservation if it exists.
    pub fn get_reservation(&self, reservation_token: &str) -> (i64, bool) {
        let inner = self.lock.read();

        for r in &inner.reservations {
            if r.token == reservation_token {
                return (r.port, true);
            }
        }

        (0, false)
    }

    // GetRandomEvenPort returns a random un-allocated udp4 port.
    pub fn get_random_even_port(&self) -> Result<i64, Error> {
        for _ in 0..128 {
            let (conn, addr) = (self.allocate_packet_conn)(AllocateListenerConfig {
                network: "udp4".to_string(),
                ..Default::default()
            })
            .map_err(Error::Allocate)?;
            conn.close()?;

            let Addr::Udp(udp_addr) = addr else {
                return Err(Error::FailedToCastUdpAddr);
            };
            if udp_addr.port() % 2 == 0 {
                return Ok(udp_addr.port() as i64);
            }
        }

        Err(Error::FailedToAllocateEvenPort)
    }

    // GrantPermission handles permission requests by calling the permission handler callback
    // associated with the TURN server listener socket.
    pub fn grant_permission(&self, source_addr: Addr, peer_ip: IpAddr) -> Result<(), Error> {
        // No permission handler: open
        let Some(permission_handler) = &self.permission_handler else {
            return Ok(());
        };

        if permission_handler(source_addr, peer_ip) {
            return Ok(());
        }

        Err(Error::AdminProhibited)
    }

    // Not here: CreateTCPConnection, addTCPConnection, isDupeTCPConnection, GetTCPConnection
    // and RemoveTCPConnection, see the header.
}

#[cfg(test)]
#[path = "allocation_manager_test.rs"]
mod allocation_manager_test;
