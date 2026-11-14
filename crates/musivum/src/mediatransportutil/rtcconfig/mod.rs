//! The Go package `github.com/livekit/mediatransportutil/pkg/rtcconfig`: the `rtc` section of
//! LiveKit's configuration file ([`RtcConfig`]), the addresses of the node ([`get_local_ip_addresses`],
//! [`get_external_ip`]), and what the connections of a node are made from ([`new_webrtc_config`]).
//!
//! The options and what they mean are the original's. The code that applies them is written
//! against musivum's connection layer (`crate::webrtc::peerconnection`) where the original
//! is written against pion: `webrtc_config.rs` says for every option where it goes, and logs
//! the ones the layer cannot honour yet.
//!
//! Finding the node's addresses blocks, as in the original: STUN requests with their
//! timeouts, and pauses between attempts. [`RtcConfig::validate`] and [`new_webrtc_config`]
//! are for the start of a server, on a plain thread or in `spawn_blocking`.

mod config;
mod ip;
#[cfg(unix)]
mod rtc_unix;
mod webrtc_config;

pub use config::*;
pub use ip::*;
pub use webrtc_config::*;

/// The errors of this package. The original makes them with `fmt.Errorf` and `errors.New`
/// where they arise; the texts are the same. An error of the system (a socket that cannot be
/// bound, a name that cannot be resolved) is the standard library's here, with its text.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid port range {0}, should be <start>-<end>")]
    InvalidPortRange(String),
    #[error("invalid start port {0}: {1}")]
    InvalidStartPort(String, String),
    #[error("invalid end port {0}: {1}")]
    InvalidEndPort(String, String),
    #[error("end port {end} must be greater than start port {start}")]
    EndPortNotGreater { end: i64, start: i64 },
    #[error("invalid port {0}: {1}")]
    InvalidPort(String, String),
    #[error("invalid node ip {0}, should be <ipv4>,<ipv6>")]
    InvalidNodeIp(String),
    #[error("invalid ip {0}")]
    InvalidIp(String),
    #[error("multiple ipv4 addresses provided: {0} and {1}")]
    MultipleIpv4(String, String),
    #[error("multiple ipv6 addresses provided: {0} and {1}")]
    MultipleIpv6(String, String),

    #[error("could not resolve external IP: {0}")]
    CouldNotResolveExternalIp(#[source] Box<Error>),
    #[error("no IPv4 address available")]
    NoIpv4AddressAvailable,
    #[error("could not find local IP address")]
    NoLocalIpAddress,
    #[error("STUN servers are required but not defined")]
    StunServersRequired,

    #[error("TCP is forced but not configured")]
    TcpForcedButNotConfigured,

    /// Go's `context.DeadlineExceeded`.
    #[error("context deadline exceeded")]
    DeadlineExceeded,
    /// Go's `context.Canceled`.
    #[error("context canceled")]
    Canceled,
    /// What Go's `net.ParseCIDR` returns.
    #[error("invalid CIDR address: {0}")]
    InvalidCidr(String),
    /// What Go's dialer returns when a name has no address of the family asked for.
    #[error("no suitable address found")]
    NoSuitableAddress,
    /// pion's `ice.ErrPort`.
    #[error("invalid port")]
    Port,
    /// pion's `ice.ErrInvalidNAT1To1IPMapping`.
    #[error("invalid address rewrite mapping")]
    InvalidNat1To1IpMapping,
    /// A STUN message that cannot be made or read.
    #[error("{0}")]
    Stun(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
