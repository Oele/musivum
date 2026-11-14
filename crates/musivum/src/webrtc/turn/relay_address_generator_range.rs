// Ported from pion/turn, relay_address_generator_range.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The network is the host's (pion's `transport.Net`, which
// can be a virtual one, is not here); the random source is `rand`'s. `Validate` takes `&self`
// (see `server_config.rs`): its default of ten tries is applied where the tries are counted.
// `AllocateListener` and `AllocateConn` are not here (RFC 6062). In Go, setting the IP of the
// address `LocalAddr` returns sets the socket's own local address as well (it is the same
// pointer); here the relay address is a copy and the socket keeps its own.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use rand::RngExt;

use super::internal::allocation::AllocateListenerConfig;
use super::net::{self, Addr, PacketConn};
use super::{Error, RelayAddressGenerator};

// RelayAddressGeneratorPortRange can be used to only allocate connections inside a defined port range.
// Similar to the RelayAddressGeneratorStatic a static ip address can be set.
#[derive(Clone, Debug, Default)]
pub struct RelayAddressGeneratorPortRange {
    // RelayAddress is the IP returned to the user when the relay is created
    pub relay_address: Option<IpAddr>,

    // MinPort the minimum port to allocate
    pub min_port: u16,
    // MaxPort the maximum (inclusive) port to allocate
    pub max_port: u16,

    // MaxRetries the amount of tries to allocate a random port in the defined range
    pub max_retries: i64,

    // Address is passed to Listen/ListenPacket when creating the Relay
    pub address: String,
}

/// Go's `net.JoinHostPort(host, strconv.Itoa(port))`.
pub(crate) fn join_host_port(host: &str, port: i64) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

impl RelayAddressGenerator for RelayAddressGeneratorPortRange {
    // Validate is called on server startup and confirms the RelayAddressGenerator is properly configured.
    fn validate(&self) -> Result<(), Error> {
        if self.min_port == 0 {
            Err(Error::MinPortNotZero)
        } else if self.max_port == 0 {
            Err(Error::MaxPortNotZero)
        } else if self.relay_address.is_none() {
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
        let relay_ip = self.relay_address.ok_or(Error::RelayAddressInvalid)?;

        if conf.requested_port != 0 {
            let listen_addr = join_host_port(&self.address, conf.requested_port);
            let conn = net::listen_packet(&conf.network, &listen_addr)?;

            let Addr::Udp(local) = conn.local_addr() else {
                return Err(Error::NilConn);
            };
            let relay_addr = Addr::Udp(SocketAddr::new(relay_ip, local.port()));

            return Ok((conn, relay_addr));
        }

        let max_retries = if self.max_retries == 0 {
            10
        } else {
            self.max_retries
        };
        let mut rng = rand::rng();
        for _ in 0..max_retries {
            let n = self.max_port.wrapping_add(1).wrapping_sub(self.min_port) as i64;
            let port = self.min_port.wrapping_add(rng.random_range(0..n) as u16);
            let listen_addr = join_host_port(&self.address, port as i64);
            let Ok(conn) = net::listen_packet(&conf.network, &listen_addr) else {
                continue;
            };

            let Addr::Udp(local) = conn.local_addr() else {
                return Err(Error::NilConn);
            };
            let relay_addr = Addr::Udp(SocketAddr::new(relay_ip, local.port()));

            return Ok((conn, relay_addr));
        }

        Err(Error::MaxRetriesExceeded)
    }
}
