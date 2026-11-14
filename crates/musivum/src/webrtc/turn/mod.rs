//! pion/turn's server: Package turn contains the public API for pion/turn, a toolkit for
//! building TURN clients and servers. What LiveKit's embedded TURN server
//! (`service/turn.rs`) is made of.
//!
//! Ported from pion/turn v5.0.13 (the version LiveKit's `go.mod` names): `server.go`,
//! `server_config.go`, `lt_cred.go`, the three relay address generators, `errors.go`,
//! `addr_family.go`, `stun_conn.go` and the internal packages the server uses (`allocation`,
//! `auth`, `ipnet`, `server`, and of `proto` what the WebRTC library's TURN crate does not
//! have). Not ported: the client (`client.go`, `internal/client`), the examples, and RFC 6062's
//! TCP relays (an Allocate of a TCP relay is answered with 508, as LiveKit's generator refuses
//! them; Connect and ConnectionBind are methods the server does not know).
//!
//! What pion/turn takes from Go's `net` and `crypto/tls` is in `net.rs`, on tokio.

// pion's leveled logger (`log.Debugf(format, args...)`): the message is formatted only when the
// level is on. pion's `Tracef` and `Debugf` are debug lines.
macro_rules! pion_log {
    ($level:ident, $logger:expr, $($arg:tt)*) => {{
        let logger = &$logger;
        if logger.enabled($crate::protocol::logger::Level::$level) {
            logger.log($crate::protocol::logger::Level::$level, &format!($($arg)*), None, &[]);
        }
    }};
}

macro_rules! debugf {
    ($logger:expr, $($arg:tt)*) => { pion_log!(Debug, $logger, $($arg)*) };
}

macro_rules! infof {
    ($logger:expr, $($arg:tt)*) => { pion_log!(Info, $logger, $($arg)*) };
}

macro_rules! warnf {
    ($logger:expr, $($arg:tt)*) => { pion_log!(Warn, $logger, $($arg)*) };
}

macro_rules! errorf {
    ($logger:expr, $($arg:tt)*) => { pion_log!(Error, $logger, $($arg)*) };
}

pub mod internal;
pub mod net;

mod addr_family;
mod errors;
mod lt_cred;
mod relay_address_generator_none;
mod relay_address_generator_range;
mod relay_address_generator_static;
mod server;
mod server_config;
mod stun_conn;

#[cfg(test)]
pub(crate) mod testclient;

pub use addr_family::*;
pub use errors::*;
pub use internal::allocation::{AllocateListenerConfig, EventHandler};
pub use internal::auth::{AuthHandler, RequestAttributes};
pub use lt_cred::*;
pub use relay_address_generator_none::*;
pub use relay_address_generator_range::RelayAddressGeneratorPortRange;
pub use relay_address_generator_static::*;
pub use server::*;
pub use server_config::*;
pub use stun_conn::*;
