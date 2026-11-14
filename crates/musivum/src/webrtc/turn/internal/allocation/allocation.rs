// Ported from pion/turn, internal/allocation/allocation.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust, on tokio.
// - `NewAllocation` gives the allocation by value; whoever made it sets what the original sets
//   afterwards (the relay socket and address, the timer, the user) and then shares it as an
//   `Arc`. The methods that hand the allocation to a permission or a channel binding take
//   `self: &Arc<Self>`; those hold it weakly. `closed` is a flag.
// - RFC 6062's TCP relays are not ported: no `relayListener`, `tcpConnections`,
//   `removeTCPConnection`, `RemoveTCPConnection` or `connHandler`.
// - The response cache keeps the attributes as `Arc`s of `stun::Setter`s.
// - `packetConnHandler` is a task (`packet_conn_handler`) that holds the allocation and the
//   manager weakly. The 5-tuple and the TURN socket of an allocation that relays are always
//   set (Go dereferences them).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use super::{ChannelBind, Error, EventHandler, FiveTuple, Manager, Permission, Protocol};
use crate::go::time::{Duration, Timer};
use crate::protocol::logger::Logger;
use crate::webrtc::stun::message::{
    CLASS_INDICATION, METHOD_DATA, MessageType, Setter, TRANSACTION_ID_SIZE, TransactionId,
};
use crate::webrtc::turn::internal::ipnet;
use crate::webrtc::turn::internal::proto::{
    self, ChannelData, ChannelNumber, Data, PeerAddress, RequestedAddressFamily,
};
use crate::webrtc::turn::net::{Addr, PacketConn};

/// A response attribute kept for a retransmitted Allocate request.
pub type ResponseAttr = Arc<dyn Setter + Send + Sync>;

#[derive(Clone)]
struct AllocationResponse {
    transaction_id: [u8; TRANSACTION_ID_SIZE],
    response_attrs: Vec<ResponseAttr>,
}

// Allocation is tied to a FiveTuple and relays traffic
// use CreateAllocation and GetAllocation to operate.
pub struct Allocation {
    pub relay_addr: Option<Addr>,
    pub protocol: Protocol,
    pub turn_socket: Option<Arc<dyn PacketConn>>,
    pub(crate) five_tuple: Option<FiveTuple>,
    pub(crate) permissions: RwLock<HashMap<String, Arc<Permission>>>,
    pub(crate) channel_bindings: RwLock<Vec<Arc<ChannelBind>>>,
    pub(crate) lifetime_timer: Option<Timer>,
    closed: AtomicBool,
    pub(crate) user_id: String,
    pub(crate) realm: String,
    event_handler: EventHandler,
    pub(crate) log: Logger,
    pub(crate) address_family: RequestedAddressFamily, // RFC 6156

    // Relay Transport for UDP
    pub(crate) relay_packet_conn: Option<Arc<dyn PacketConn>>,

    // Some clients (Firefox or others using resiprocate's nICE lib) may retry allocation
    // with same 5 tuple when received 413, for compatible with these clients,
    // cache for response lost and client retry to implement 'stateless stack approach'
    // See: https://datatracker.ietf.org/doc/html/rfc5766#section-6.2
    response_cache: Mutex<Option<AllocationResponse>>, // *allocationResponse
}

impl Allocation {
    // NewAllocation creates a new instance of NewAllocation.
    pub fn new(
        turn_socket: Option<Arc<dyn PacketConn>>,
        five_tuple: Option<FiveTuple>,
        event_handler: EventHandler,
        log: Logger,
    ) -> Allocation {
        Allocation {
            relay_addr: None,
            protocol: Protocol::default(),
            turn_socket,
            five_tuple,
            permissions: RwLock::new(HashMap::with_capacity(64)),
            channel_bindings: RwLock::new(Vec::new()),
            lifetime_timer: None,
            closed: AtomicBool::new(false),
            user_id: String::new(),
            realm: String::new(),
            event_handler,
            log,
            address_family: RequestedAddressFamily::default(),
            relay_packet_conn: None,
            response_cache: Mutex::new(None),
        }
    }

    // GetPermission gets the Permission from the allocation.
    pub fn get_permission(&self, addr: Addr) -> Option<Arc<Permission>> {
        self.permissions
            .read()
            .get(&ipnet::fingerprint_addr(addr))
            .cloned()
    }

    // AddPermission adds a new permission to the allocation.
    pub fn add_permission(self: &Arc<Self>, perms: Arc<Permission>) {
        let fingerprint = ipnet::fingerprint_addr(perms.addr);

        let existed_permission = self.permissions.read().get(&fingerprint).cloned();

        if let Some(existed_permission) = existed_permission {
            existed_permission.refresh(perms.timeout);

            return;
        }

        *perms.allocation.lock() = Arc::downgrade(self);
        self.permissions.write().insert(fingerprint, perms.clone());

        if let Some(on_permission_created) = &self.event_handler.on_permission_created
            && let Addr::Udp(u) = perms.addr
        {
            let ft = self.five_tuple.unwrap_or_default();
            on_permission_created(
                ft.src_addr,
                ft.dst_addr,
                &ft.protocol.to_string(),
                &self.user_id,
                &self.realm,
                self.relay_addr,
                u.ip(),
            );
        }

        perms.start(perms.timeout);
    }

    // RemovePermission removes the net.Addr's fingerprint from the allocation's permissions.
    pub fn remove_permission(&self, addr: Addr) {
        let mut permissions = self.permissions.write();
        permissions.remove(&ipnet::fingerprint_addr(addr));

        if let Some(on_permission_deleted) = &self.event_handler.on_permission_deleted
            && let Addr::Udp(u) = addr
        {
            let ft = self.five_tuple.unwrap_or_default();
            on_permission_deleted(
                ft.src_addr,
                ft.dst_addr,
                &ft.protocol.to_string(),
                &self.user_id,
                &self.realm,
                self.relay_addr,
                u.ip(),
            );
        }
    }

    // ListPermissions returns the permissions associated with an allocation.
    pub fn list_permissions(&self) -> Vec<Arc<Permission>> {
        self.permissions.read().values().cloned().collect()
    }

    // AddChannelBind adds a new ChannelBind to the allocation, it also updates the
    // permissions needed for this ChannelBind.
    pub fn add_channel_bind(
        self: &Arc<Self>,
        chan_bind: Arc<ChannelBind>,
        channel_lifetime: Duration,
        permission_lifetime: Duration,
    ) -> Result<(), Error> {
        // Check that this channel id isn't bound to another transport address, and
        // that this transport address isn't bound to another channel number.
        let channel_by_number = self.get_channel_by_number(chan_bind.number);
        let channel_by_addr = self.get_channel_by_addr(chan_bind.peer);

        // Peer already bound to a different channel number.
        if let Some(channel_by_addr) = &channel_by_addr
            && channel_by_addr.number != chan_bind.number
        {
            return Err(Error::SamePeerDifferentChannel);
        }

        // Channel number already bound to a different peer.
        if let Some(channel_by_number) = &channel_by_number
            && !ipnet::addr_equal(channel_by_number.peer, chan_bind.peer)
        {
            return Err(Error::SameChannelDifferentPeer);
        }

        // Add or refresh this channel.
        match channel_by_number {
            None => {
                let mut channel_bindings = self.channel_bindings.write();

                *chan_bind.allocation.lock() = Arc::downgrade(self);
                channel_bindings.push(chan_bind.clone());
                chan_bind.start(channel_lifetime);

                // Channel binds also refresh permissions.
                self.add_permission(Permission::new(
                    chan_bind.peer,
                    self.log.clone(),
                    permission_lifetime,
                ));

                if let Some(on_channel_created) = &self.event_handler.on_channel_created {
                    let ft = self.five_tuple.unwrap_or_default();
                    on_channel_created(
                        ft.src_addr,
                        ft.dst_addr,
                        &ft.protocol.to_string(),
                        &self.user_id,
                        &self.realm,
                        self.relay_addr,
                        chan_bind.peer,
                        chan_bind.number.0,
                    );
                }
            }
            Some(channel_by_number) => {
                channel_by_number.refresh(channel_lifetime);

                // Channel binds also refresh permissions.
                self.add_permission(Permission::new(
                    channel_by_number.peer,
                    self.log.clone(),
                    permission_lifetime,
                ));
            }
        }

        Ok(())
    }

    // RemoveChannelBind removes the ChannelBind from this allocation by id.
    pub fn remove_channel_bind(&self, number: ChannelNumber) -> bool {
        let mut channel_bindings = self.channel_bindings.write();

        for i in (0..channel_bindings.len()).rev() {
            if channel_bindings[i].number == number {
                if let Some(on_channel_deleted) = &self.event_handler.on_channel_deleted {
                    let ft = self.five_tuple.unwrap_or_default();
                    on_channel_deleted(
                        ft.src_addr,
                        ft.dst_addr,
                        &ft.protocol.to_string(),
                        &self.user_id,
                        &self.realm,
                        self.relay_addr,
                        channel_bindings[i].peer,
                        channel_bindings[i].number.0,
                    );
                }

                channel_bindings.remove(i);

                return true;
            }
        }

        false
    }

    // GetChannelByNumber gets the ChannelBind from this allocation by id.
    pub fn get_channel_by_number(&self, number: ChannelNumber) -> Option<Arc<ChannelBind>> {
        self.channel_bindings
            .read()
            .iter()
            .find(|cb| cb.number == number)
            .cloned()
    }

    // GetChannelByAddr gets the ChannelBind from this allocation by net.Addr.
    pub fn get_channel_by_addr(&self, addr: Addr) -> Option<Arc<ChannelBind>> {
        self.channel_bindings
            .read()
            .iter()
            .find(|cb| ipnet::addr_equal(cb.peer, addr))
            .cloned()
    }

    // ListChannelBindings returns the channel bindings associated with an allocation.
    pub fn list_channel_bindings(&self) -> Vec<Arc<ChannelBind>> {
        self.channel_bindings.read().clone()
    }

    // Refresh updates the allocations lifetime.
    pub fn refresh(&self, lifetime: Duration) {
        if !self
            .lifetime_timer
            .as_ref()
            .is_some_and(|t| t.reset(lifetime))
        {
            errorf!(
                self.log,
                "Failed to reset allocation timer for {:?}",
                self.five_tuple
            );
        }
    }

    // AddressFamily returns the address family of the allocation (RFC 6156).
    pub fn address_family(&self) -> RequestedAddressFamily {
        RequestedAddressFamily(self.address_family.0)
    }

    // SetResponseCache cache allocation response for retransmit allocation request.
    pub fn set_response_cache(
        &self,
        transaction_id: [u8; TRANSACTION_ID_SIZE],
        attrs: Vec<ResponseAttr>,
    ) {
        *self.response_cache.lock() = Some(AllocationResponse {
            transaction_id,
            response_attrs: attrs,
        });
    }

    // GetResponseCache return response cache for retransmit allocation request.
    pub fn get_response_cache(&self) -> ([u8; TRANSACTION_ID_SIZE], Vec<ResponseAttr>) {
        match &*self.response_cache.lock() {
            Some(res) => (res.transaction_id, res.response_attrs.clone()),
            None => ([0; TRANSACTION_ID_SIZE], Vec::new()),
        }
    }

    // Close closes the allocation.
    pub fn close(&self) -> Result<(), Error> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        if let Some(t) = &self.lifetime_timer {
            t.stop();
        }

        for p in self.list_permissions() {
            self.remove_permission(p.addr);
            if let Some(t) = &*p.lifetime_timer.lock() {
                t.stop();
            }
        }

        for c in self.list_channel_bindings() {
            self.remove_channel_bind(c.number);
            if let Some(t) = &*c.lifetime_timer.lock() {
                t.stop();
            }
        }

        if let Some(relay_packet_conn) = &self.relay_packet_conn {
            return Ok(relay_packet_conn.close()?);
        }

        Ok(())
    }

    // WriteTo writes a packet with payload p to addr via the Relay socket.
    pub async fn write_to(&self, p: &[u8], addr: Addr) -> Result<usize, Error> {
        let Some(relay_packet_conn) = &self.relay_packet_conn else {
            return Err(Error::NilRelaySocket);
        };

        Ok(relay_packet_conn.write_to(p, addr).await?)
    }

    fn turn_socket_and_client(&self) -> (&Arc<dyn PacketConn>, Addr) {
        (
            self.turn_socket
                .as_ref()
                .expect("an allocation that relays has its TURN socket"),
            self.five_tuple
                .and_then(|ft| ft.src_addr)
                .expect("an allocation that relays has its 5-tuple"),
        )
    }

    //  https://tools.ietf.org/html/rfc5766#section-10.3
    //  When the server receives a UDP datagram at a currently allocated
    //  relayed transport address, the server looks up the allocation
    //  associated with the relayed transport address.  The server then
    //  checks to see whether the set of permissions for the allocation allow
    //  the relaying of the UDP datagram as described in Section 8.
    //
    //  If relaying is permitted, then the server checks if there is a
    //  channel bound to the peer that sent the UDP datagram (see
    //  Section 11).  If a channel is bound, then processing proceeds as
    //  described in Section 11.7.
    //
    //  If relaying is permitted but no channel is bound to the peer, then
    //  the server forms and sends a Data indication.  The Data indication
    //  MUST contain both an XOR-PEER-ADDRESS and a DATA attribute.  The DATA
    //  attribute is set to the value of the 'data octets' field from the
    //  datagram, and the XOR-PEER-ADDRESS attribute is set to the source
    //  transport address of the received UDP datagram.  The Data indication
    //  is then sent on the 5-tuple associated with the allocation.

    pub(crate) async fn packet_conn_handler(self: Arc<Self>, manager: Weak<Manager>) {
        let relay_packet_conn = self
            .relay_packet_conn
            .clone()
            .expect("the relay loop runs for an allocation with a relay socket");
        let mut buffer = vec![0u8; RTP_MTU];

        loop {
            let (n, src_addr) = match relay_packet_conn.read_from(&mut buffer).await {
                Ok(r) => r,
                Err(_) => {
                    if let (Some(manager), Some(five_tuple)) = (manager.upgrade(), self.five_tuple)
                    {
                        manager.delete_allocation(&five_tuple);
                    }

                    return;
                }
            };

            debugf!(
                self.log,
                "Relay socket {} received {} bytes from {}",
                relay_packet_conn.local_addr(),
                n,
                src_addr
            );

            let (turn_socket, client_addr) = self.turn_socket_and_client();
            if let Some(channel) = self.get_channel_by_addr(src_addr) {
                let mut channel_data = ChannelData {
                    data: buffer[..n].to_vec(),
                    number: channel.number,
                    ..Default::default()
                };
                channel_data.encode();

                if let Err(err) = turn_socket.write_to(&channel_data.raw, client_addr).await {
                    errorf!(
                        self.log,
                        "Failed to send ChannelData from allocation {} {}",
                        src_addr,
                        err
                    );
                }
            } else if self.get_permission(src_addr).is_some() {
                let Addr::Udp(udp_addr) = src_addr else {
                    errorf!(
                        self.log,
                        "Failed to send DataIndication from allocation {} {}",
                        src_addr,
                        "<nil>"
                    );

                    return;
                };

                let peer_address_attr = PeerAddress {
                    ip: udp_addr.ip().to_canonical(),
                    port: udp_addr.port(),
                };
                let data_attr = Data(buffer[..n].to_vec());

                let msg = match proto::build(&[
                    &TransactionId::new(),
                    &MessageType::new(METHOD_DATA, CLASS_INDICATION),
                    &peer_address_attr,
                    &data_attr,
                ]) {
                    Ok(msg) => msg,
                    Err(err) => {
                        errorf!(
                            self.log,
                            "Failed to send DataIndication from allocation {} {}",
                            src_addr,
                            err
                        );

                        return;
                    }
                };
                debugf!(
                    self.log,
                    "Relaying message from {} to client at {}",
                    src_addr,
                    client_addr
                );
                if let Err(err) = turn_socket.write_to(&msg.raw, client_addr).await {
                    errorf!(
                        self.log,
                        "Failed to send DataIndication from allocation {} {}",
                        src_addr,
                        err
                    );
                }
            } else {
                infof!(
                    self.log,
                    "No Permission or Channel exists for {} on allocation {:?}",
                    src_addr,
                    self.relay_addr
                );
            }
        }
    }
}

const RTP_MTU: usize = 1600;

#[cfg(test)]
#[path = "allocation_test.rs"]
pub(crate) mod allocation_test;
