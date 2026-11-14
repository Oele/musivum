// Ported from pion/turn, relay_address_generator_none.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The network is the host's (pion's `transport.Net` is not
// here). `AllocateListener` and `AllocateConn` are not here (RFC 6062).

use std::sync::Arc;

use super::internal::allocation::AllocateListenerConfig;
use super::net::{self, Addr, PacketConn};
use super::relay_address_generator_range::join_host_port;
use super::{Error, RelayAddressGenerator};

// RelayAddressGeneratorNone returns the listener with no modifications.
#[derive(Clone, Debug, Default)]
pub struct RelayAddressGeneratorNone {
    // Address is passed to Listen/ListenPacket when creating the Relay
    pub address: String,
}

impl RelayAddressGenerator for RelayAddressGeneratorNone {
    // Validate is called on server startup and confirms the RelayAddressGenerator is properly configured.
    fn validate(&self) -> Result<(), Error> {
        if self.address.is_empty() {
            return Err(Error::ListeningAddressInvalid);
        }

        Ok(())
    }

    // AllocatePacketConn generates a new PacketConn to receive traffic on and the IP/Port
    // to populate the allocation response with.
    fn allocate_packet_conn(
        &self,
        conf: AllocateListenerConfig,
    ) -> Result<(Arc<dyn PacketConn>, Addr), Error> {
        let conn = net::listen_packet(
            &conf.network,
            &join_host_port(&self.address, conf.requested_port),
        )?;

        let addr = conn.local_addr();
        Ok((conn, addr))
    }
}
