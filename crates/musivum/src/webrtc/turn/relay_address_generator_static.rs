// Ported from pion/turn, relay_address_generator_static.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The network is the host's (pion's `transport.Net` is not
// here). `AllocateListener` and `AllocateConn` are not here (RFC 6062). The relay address is a
// copy of the socket's with the IP replaced (in Go the socket's own is changed with it).

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use super::internal::allocation::AllocateListenerConfig;
use super::net::{self, Addr, PacketConn};
use super::relay_address_generator_range::join_host_port;
use super::{Error, RelayAddressGenerator};

// RelayAddressGeneratorStatic can be used to return static IP address each time a relay is created.
// This can be used when you have a single static IP address that you want to use.
#[derive(Clone, Debug, Default)]
pub struct RelayAddressGeneratorStatic {
    // RelayAddress is the IP returned to the user when the relay is created
    pub relay_address: Option<IpAddr>,

    // Address is passed to Listen/ListenPacket when creating the Relay
    pub address: String,
}

impl RelayAddressGenerator for RelayAddressGeneratorStatic {
    // Validate is called on server startup and confirms the RelayAddressGenerator is properly configured.
    fn validate(&self) -> Result<(), Error> {
        if self.relay_address.is_none() {
            Err(Error::RelayAddressInvalid)
        } else if self.address.is_empty() {
            Err(Error::ListeningAddressInvalid)
        } else {
            Ok(())
        }
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

        // Replace actual listening IP with the user requested one of RelayAddressGeneratorStatic
        let Addr::Udp(local) = conn.local_addr() else {
            return Err(Error::NilConn);
        };
        let relay_ip = self.relay_address.ok_or(Error::RelayAddressInvalid)?;

        Ok((conn, Addr::Udp(SocketAddr::new(relay_ip, local.port()))))
    }
}
