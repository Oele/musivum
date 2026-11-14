// Ported from pion/turn, server_config.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust.
// - `RelayAddressGenerator` is a trait without `AllocateListener` and `AllocateConn`: RFC 6062's
//   TCP relays are not ported. `Validate` takes `&self`: a generator is shared (LiveKit hands the
//   same one to every listener) and the defaults it sets in Go are applied where they are used.
// - The handlers are `Arc`s of functions; what Go may leave nil is an `Option`. The logger
//   factory is a logger, from which the server's is named "turn".
// - `GenerateAuthKey` computes its MD5 with the WebRTC library's cryptography.

use std::net::IpAddr;
use std::sync::Arc;

use super::Error;
use super::internal::allocation::{self, AllocateListenerConfig, EventHandler};
use super::internal::auth::AuthHandler;
use super::internal::server::{QuotaHandlerFn, crypto};
use super::net::{Addr, Listener, PacketConn};
use crate::go::time::Duration;
use crate::protocol::logger::Logger;
use rtc::crypto::HashAlgorithm;

// RelayAddressGenerator is used to generate a RelayAddress when creating an allocation.
// You can use one of the provided ones or provide your own.
pub trait RelayAddressGenerator: Send + Sync {
    // Validate confirms that the RelayAddressGenerator is properly initialized
    fn validate(&self) -> Result<(), Error>;

    // Allocate a PacketConn (UDP) RelayAddress
    fn allocate_packet_conn(
        &self,
        conf: AllocateListenerConfig,
    ) -> Result<(Arc<dyn PacketConn>, Addr), Error>;

    // Not here: AllocateListener and AllocateConn, see the header.
}

// PermissionHandler is a callback to filter incoming CreatePermission and ChannelBindRequest
// requests based on the client IP address and port and the peer IP address the client intends to
// connect to. If the client is behind a NAT then the filter acts on the server reflexive
// ("mapped") address instead of the real client IP address and port. Note that TURN permissions
// are per-allocation and per-peer-IP-address, to mimic the address-restricted filtering mechanism
// of NATs that comply with [RFC4787], see https://tools.ietf.org/html/rfc5766#section-2.3.
pub type PermissionHandler = allocation::PermissionHandlerFn;

// DefaultPermissionHandler is convince function that grants permission to all peers.
pub fn default_permission_handler(_: Addr, _: IpAddr) -> bool {
    true
}

// PacketConnConfig is a single net.PacketConn to listen/write on.
// This will be used for UDP listeners.
#[derive(Clone, Default)]
pub struct PacketConnConfig {
    pub packet_conn: Option<Arc<dyn PacketConn>>,

    // When an allocation is generated the RelayAddressGenerator
    // creates the net.PacketConn and returns the IP/Port it is available at
    pub relay_address_generator: Option<Arc<dyn RelayAddressGenerator>>,

    // PermissionHandler is a callback to filter peer addresses. Can be set as nil, in which
    // case the DefaultPermissionHandler is automatically instantiated to admit all peer
    // connections
    pub permission_handler: Option<PermissionHandler>,
}

impl PacketConnConfig {
    fn validate(&self) -> Result<(), Error> {
        if self.packet_conn.is_none() {
            return Err(Error::ConnUnset);
        }

        if let Some(relay_address_generator) = &self.relay_address_generator {
            relay_address_generator.validate()?;
        }

        Ok(())
    }
}

// ListenerConfig is a single net.Listener to accept connections on.
// This will be used for TCP, TLS and DTLS listeners.
#[derive(Clone, Default)]
pub struct ListenerConfig {
    pub listener: Option<Arc<dyn Listener>>,

    // When an allocation is generated the RelayAddressGenerator
    // creates the net.Listener and returns the IP/Port it is available at
    pub relay_address_generator: Option<Arc<dyn RelayAddressGenerator>>,

    // PermissionHandler is a callback to filter peer addresses. Can be set as nil, in which
    // case the DefaultPermissionHandler is automatically instantiated to admit all peer
    // connections
    pub permission_handler: Option<PermissionHandler>,
}

impl ListenerConfig {
    fn validate(&self) -> Result<(), Error> {
        if self.listener.is_none() {
            return Err(Error::ListenerUnset);
        }

        let Some(relay_address_generator) = &self.relay_address_generator else {
            return Err(Error::RelayAddressGeneratorUnset);
        };

        relay_address_generator.validate()
    }
}

// RequestAttributes and AuthHandler: the internal package's, re-exported by the module.

// GenerateAuthKey is a convenience function to easily generate keys in the format used by AuthHandler.
pub fn generate_auth_key(username: &str, realm: &str, password: &str) -> Vec<u8> {
    crypto()
        .hash(
            HashAlgorithm::Md5,
            [username, realm, password].join(":").as_bytes(),
        )
        .expect("the WebRTC library's cryptography has MD5")
}

// EventHandler: the internal package's, re-exported by the module.

// QuotaHandler is a callback allows allocations to be rejected when a per-user quota is
// exceeded. If the callback returns true the allocation request is accepted, otherwise it is
// rejected and a 486 (Allocation Quota Reached) error is returned to the user.
pub type QuotaHandler = QuotaHandlerFn;

// ServerConfig configures the Pion TURN Server.
#[derive(Clone, Default)]
pub struct ServerConfig {
    // PacketConnConfigs and ListenerConfigs are a list of all the turn listeners
    // Each listener can have custom behavior around the creation of Relays
    pub packet_conn_configs: Vec<PacketConnConfig>,
    pub listener_configs: Vec<ListenerConfig>,

    // LoggerFactory must be set for logging from this server.
    pub logger_factory: Option<Logger>,

    // Realm sets the realm for this server
    pub realm: String,

    // AuthHandler is a callback used to handle incoming auth requests,
    // allowing users to customize Pion TURN with custom behavior
    pub auth_handler: Option<AuthHandler>,

    // QuotaHandler is a callback used to reject new allocations when a
    // per-user quota is exceeded.
    pub quota_handler: Option<QuotaHandler>,

    // EventHandlers is a set of callbacks for tracking allocation lifecycle.
    pub event_handler: EventHandler,

    // ChannelBindTimeout sets the lifetime of channel binding. Defaults to 10 minutes.
    pub channel_bind_timeout: Duration,

    // PermissionTimeout sets the lifetime of permission. Defaults to 10 minutes.
    pub permission_timeout: Duration,

    // AllocationLife sets the lifetime of allocation. Defaults to 10 minutes.
    pub allocation_lifetime: Duration,

    // StrictAddressFamily keeps the RFC 6156 default of IPv4 when
    // REQUESTED-ADDRESS-FAMILY is omitted. By default, omitted address family
    // follows the listener address family for browser compatibility.
    pub strict_address_family: bool,

    // Sets the server inbound MTU(Maximum transmition unit). Defaults to 1600 bytes.
    pub inbound_mtu: usize,
}

impl ServerConfig {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.packet_conn_configs.is_empty() && self.listener_configs.is_empty() {
            return Err(Error::NoAvailableConns);
        }

        for s in &self.packet_conn_configs {
            s.validate()?;
        }

        for s in &self.listener_configs {
            s.validate()?;
        }

        Ok(())
    }
}
