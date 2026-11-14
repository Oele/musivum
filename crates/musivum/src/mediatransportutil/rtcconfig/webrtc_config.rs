// Ported from LiveKit's mediatransportutil, pkg/rtcconfig/webrtc_config.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, and written against musivum's connection layer
// (`crate::webrtc::peerconnection`) where the original is written against pion. What that
// changes:
// - `WebRTCConfig.SettingEngine` is a `SettingEngineConfig`: the settings the original makes
//   on pion's engine, as data, which LiveKit copies and changes for every connection.
//   `build` makes the layer's engine from it, with the settings the layer has, and
//   `host_candidate_ips` works out the addresses a connection made from a changed copy
//   advertises (the layer gathers nothing, so the filters and the rewrite rules are applied
//   here).
// - `WebRTCConfig.UDPMux` is `node_config`: what the node's one socket is made from. No
//   socket is bound here; `Node::new` does that. The addresses the node advertises are
//   worked out here once (`node_config_for`), as pion's agent works them out for every
//   connection from the mux's addresses and the rewrite rules. `local_interfaces` is the
//   function of that name in pkg/transport/ip.go of the same module (which addresses the
//   mux listens on); `host_candidate_ips` does what `gatherCandidatesLocalUDPMux` and
//   `evaluateRewriteRules` of pion/ice do (gather.go and external_ip_mapper.go at
//   livekit/ice v4.4.0-warp.3, MIT) for the rules this package makes, so an address in a
//   rule that is none is refused here and not when the first connection gathers.
// - `WebRTCConfig.Configuration` is `ice_servers`, the one thing the original sets in it
//   besides the SDP semantics, which are always unified plan in `rtc`. They go into the
//   layer's `Configuration.ice_servers`.
// - `WebRTCConfig.TCPMuxListener` is not there: no listener is opened.
// - What the layer cannot do yet is said in the log when the option is set (`not_applied`):
//   see the table in the description of `new_webrtc_config`.
// - `ICEAddressRewriteRule` and its mode are pion's types, with the fields this package
//   sets; `net.IPNet` is `IpNet`.
// - In `get_nat_1to1_ips_for_conf` the goroutines are threads, the mapping keeps the order
//   in which it was filled (Go's is random), and the wait for answers ends when every
//   lookup has ended, where the original always waits for its timer.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Instant;

use rand::RngExt;

use crate::go::time::Duration;
use crate::protocol::logger;
use crate::webrtc::peerconnection::{IceServer, Node, NodeConfig, SettingEngine};
use crate::{infow, warnw};

use super::config::{DEFAULT_UDP_BUFFER_SIZE, parse_ip};
use super::ip::{Context, Interface, Network, get_external_ip_on, interfaces, is_link_local};
use super::{
    DEFAULT_STUN_SERVERS, Error, InterfacesConfig, IpsConfig, RtcConfig, get_local_ip_addresses,
};

/// pion's `MulticastDNSMode`, as the connection layer has it.
pub use crate::webrtc::ice::MulticastDnsMode;
/// pion's `ICECandidateType`, as the connection layer has it.
pub use crate::webrtc::peerconnection::IceCandidateType;
/// pion's `NetworkType`, as the connection layer has it.
pub use crate::webrtc::peerconnection::NetworkType;

/// Go's `func(string) bool` on the name of an interface: whether it is to be used.
pub type InterfaceFilter = Arc<dyn Fn(&str) -> bool + Send + Sync>;
/// Go's `func(net.IP) bool`: whether an address is to be used.
pub type IpFilter = Arc<dyn Fn(IpAddr) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct WebRtcConfig {
    /// `Configuration.ICEServers` of the original. A connection of the layer is given them
    /// (`Configuration.ice_servers`) and gathers nothing from them.
    pub ice_servers: Vec<IceServer>,
    pub setting_engine: SettingEngineConfig,
    /// What the node's socket is made from (`Node::new`): it stands where `UDPMux` did.
    pub node_config: NodeConfig,
    /// The port of the node's TCP listener for ICE over TCP (`rtc.tcp_port`), where the
    /// original opens its `TCPMux`; 0 for none. [`WebRtcConfig::new_node`] passes it on.
    pub tcp_port: u16,
    pub nat_1to1_ips: Vec<String>,
    pub use_mdns: bool,
}

impl WebRtcConfig {
    /// The node the connections of this configuration run on: its UDP socket, and its TCP
    /// listener when `tcp_port` is set. Upstream `NewWebRTCConfig` opens both itself; here
    /// whoever owns the connections makes the node once, from this.
    pub fn new_node(&self) -> std::io::Result<Node> {
        if self.tcp_port != 0 {
            Node::new_with_tcp(self.node_config.clone(), self.tcp_port)
        } else {
            Node::new(self.node_config.clone())
        }
    }
}

/// What `new_webrtc_config` sets on pion's `SettingEngine`, each field under the name of its
/// setter there. A copy can be changed for one connection, as LiveKit does with pion's.
#[derive(Clone, Default)]
pub struct SettingEngineConfig {
    /// `SetSCTPMaxMessageSize`. 0: not set.
    pub sctp_max_message_size: u32,
    /// `SetSCTPMinCwnd`. 0: not set. `rtc` has no such setting.
    pub sctp_min_cwnd: u32,
    /// `SetSCTPFastRtxWnd`. 0: not set. `rtc` has no such setting.
    pub sctp_fast_rtx_wnd: u32,
    /// `SetSCTPCwndCAStep`. 0: not set. `rtc` has no such setting.
    pub sctp_cwnd_ca_step: u32,
    /// `SetInterfaceFilter`. It is used where the node's addresses are chosen.
    pub interface_filter: Option<InterfaceFilter>,
    /// `SetIPFilter`. It is used where the node's addresses are chosen.
    pub ip_filter: Option<IpFilter>,
    /// `SetICEMulticastDNSMode`. Not set, it is what it is in pion: queries only.
    pub ice_multicast_dns_mode: MulticastDnsMode,
    /// `SetICEAddressRewriteRules`. They are used where the node's addresses are chosen.
    pub ice_address_rewrite_rules: Vec<IceAddressRewriteRule>,
    /// `SetEphemeralUDPPortRange`. The layer has one socket for all connections.
    pub ephemeral_udp_port_range: Option<(u16, u16)>,
    /// `SetNetworkTypes`.
    pub network_types: Vec<NetworkType>,
    /// `SetHostAcceptanceMinWait`. 0: not set.
    pub host_acceptance_min_wait: Duration,
    /// `SetSrflxAcceptanceMinWait`. 0: not set.
    pub srflx_acceptance_min_wait: Duration,
    /// `SetPrflxAcceptanceMinWait`. 0: not set.
    pub prflx_acceptance_min_wait: Duration,
    /// `SetRelayAcceptanceMinWait`. 0: not set.
    pub relay_acceptance_min_wait: Duration,
    /// `SetIncludeLoopbackCandidate`.
    pub include_loopback_candidate: bool,
    /// `SetLite`.
    pub lite: bool,
}

impl SettingEngineConfig {
    /// pion's `SettingEngine.SetEphemeralUDPPortRange`.
    pub fn set_ephemeral_udp_port_range(
        &mut self,
        port_min: u16,
        port_max: u16,
    ) -> Result<(), Error> {
        if port_max < port_min {
            return Err(Error::Port);
        }

        self.ephemeral_udp_port_range = Some((port_min, port_max));
        Ok(())
    }

    /// The layer's `SettingEngine` with what the layer has of these settings: ICE lite, the
    /// mDNS mode, the waits before a candidate pair is accepted, the largest SCTP message,
    /// and the network types (a connection with the TCP ones has passive TCP candidates on
    /// the node's TCP listener, when the node has one). The caller goes on from there with the
    /// settings of its own (the layer's engine is data too).
    ///
    /// A connection made with it advertises the node's addresses. For a copy of these
    /// settings whose filters or rewrite rules were changed for one connection, the addresses
    /// are those of `host_candidate_ips`, to be put into the engine's field of that name.
    pub fn build(&self) -> SettingEngine {
        let wait = |d: Duration| (d > Duration::ZERO).then_some(d);
        SettingEngine {
            lite: self.lite,
            host_acceptance_min_wait: wait(self.host_acceptance_min_wait),
            srflx_acceptance_min_wait: wait(self.srflx_acceptance_min_wait),
            prflx_acceptance_min_wait: wait(self.prflx_acceptance_min_wait),
            relay_acceptance_min_wait: wait(self.relay_acceptance_min_wait),
            ice_multicast_dns_mode: self.ice_multicast_dns_mode,
            network_types: self.network_types.clone(),
            sctp_max_message_size: self.sctp_max_message_size,
            ..Default::default()
        }
    }

    /// The addresses a connection made from these settings gives as host candidates: what
    /// pion's agent makes of the addresses of the UDP mux with this engine's filters and
    /// rewrite rules. For the settings `new_webrtc_config` made they are the node's
    /// (`node_config.nat_1to1_ips`, which has the IPv4 ones). It asks the system for its
    /// interfaces.
    pub fn host_candidate_ips(&self) -> Result<Vec<IpAddr>, Error> {
        host_candidate_ips_of(self, self.include_loopback_candidate)
    }
}

/// pion's `ICEAddressRewriteMode`: whether a rule replaces or appends candidates.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IceAddressRewriteMode {
    #[default]
    Unspecified,
    Replace,
    Append,
}

/// pion's `ICEAddressRewriteRule`, with the fields this package sets (it leaves `Iface`,
/// `CIDR` and `Networks` empty).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IceAddressRewriteRule {
    pub external: Vec<String>,
    pub local: String,
    pub as_candidate_type: IceCandidateType,
    pub mode: IceAddressRewriteMode,
}

/// Says in the log that an option is set and does nothing here yet.
fn not_applied(option: &str, value: &dyn fmt::Debug, reason: &str) {
    warnw!(logger::get_logger(), "rtc option is accepted but not applied", nil,
        "option" => option,
        "value" => value,
        "reason" => reason);
}

/// Makes of the `rtc` section of the configuration what the connections of this node are
/// made from. It finds the external addresses when `use_external_ip` is set, which takes
/// seconds and blocks.
///
/// Where each option goes:
///
/// | option | here |
/// |---|---|
/// | `udp_port` | the port of the node's socket (`node_config.bind_address`); of a range the first port only (logged) |
/// | `port_range_start`, `port_range_end` | kept (`setting_engine.ephemeral_udp_port_range`) and logged: the node has one socket for all connections, bound to the first port of the range |
/// | `tcp_port` | the port of the node's TCP listener (`tcp_port`, made by `new_node`); the TCP network types |
/// | `force_tcp` | the UDP network types are left out, as in the original; without `tcp_port` it is the original's error |
/// | `use_stun_port_as_ice` | logged: no second socket on port 3478 |
/// | `node_ip` | set by hand: the address every local address is advertised as (a rewrite rule for all); found by `validate`: as below |
/// | `use_external_ip`, `stun_servers`, `skip_external_ip_validation` | the external address of every local address is asked from the STUN servers and advertised in its place (`nat_1to1_ips`, a rewrite rule each) |
/// | `advertise_internal_ip` | the rewrite rules append: the local address is advertised too |
/// | `external_ip_only` | the IP filter keeps only local IPv4 addresses that have an external one |
/// | `require_ipv4` | in `validate` only, as in the original |
/// | `interfaces`, `ips` | which local addresses are advertised and asked about; logged: the socket itself listens on all addresses |
/// | `enable_loopback_candidate` | loopback addresses are advertised too |
/// | `use_ice_lite` | `setting_engine.lite`, to the layer |
/// | `use_mdns` | off: `rtc` is told to drop mDNS candidates, and `use_mdns` is there for the server to drop them from what it signals; on: logged, the node gives the answer to an mDNS query to no connection |
/// | `stun_servers` as the ICE servers of a server that is not ICE lite and has no address set | kept (`ice_servers`) and logged: the connections gather no server reflexive candidates |
/// | `batch_io` | logged: the node writes every datagram at once |
/// | `sctp_min_cwnd`, `sctp_fast_rtx_wnd`, `sctp_cwnd_ca_step` | kept and logged: `rtc` has no such settings |
/// | `host_acceptance_min_wait` and the three like it | to the layer |
/// | (no option) the buffers of 16 MiB on the socket | logged: the node's socket has the system's |
/// | (no option) IPv6 | logged when there are such addresses: the node's socket is IPv4 |
#[cfg_attr(not(unix), allow(unused_variables))]
pub fn new_webrtc_config(rtc_conf: &RtcConfig, development: bool) -> Result<WebRtcConfig, Error> {
    let logger = logger::get_logger();
    let mut ice_servers = Vec::new();
    let mut s = SettingEngineConfig {
        sctp_max_message_size: 65535,
        ..Default::default()
    };

    if rtc_conf.sctp_min_cwnd > 0 {
        s.sctp_min_cwnd = rtc_conf.sctp_min_cwnd as u32;
        not_applied("sctp_min_cwnd", &rtc_conf.sctp_min_cwnd, NO_SCTP_SETTING);
    }

    if rtc_conf.sctp_fast_rtx_wnd > 0 {
        s.sctp_fast_rtx_wnd = rtc_conf.sctp_fast_rtx_wnd as u32;
        not_applied(
            "sctp_fast_rtx_wnd",
            &rtc_conf.sctp_fast_rtx_wnd,
            NO_SCTP_SETTING,
        );
    }

    if rtc_conf.sctp_cwnd_ca_step > 0 {
        s.sctp_cwnd_ca_step = rtc_conf.sctp_cwnd_ca_step as u32;
        not_applied(
            "sctp_cwnd_ca_step",
            &rtc_conf.sctp_cwnd_ca_step,
            NO_SCTP_SETTING,
        );
    }

    let mut if_filter: Option<InterfaceFilter> = None;
    if !rtc_conf.interfaces.includes.is_empty() || !rtc_conf.interfaces.excludes.is_empty() {
        if_filter = Some(interface_filter_from_conf(&rtc_conf.interfaces));
        s.interface_filter = if_filter.clone();
        not_applied("interfaces", &rtc_conf.interfaces, FILTER_NOT_ON_SOCKET);
    }

    let mut ip_filter: Option<IpFilter> = None;
    if !rtc_conf.ips.includes.is_empty() || !rtc_conf.ips.excludes.is_empty() {
        let filter = ip_filter_from_conf(&rtc_conf.ips)?;
        ip_filter = Some(filter.clone());
        s.ip_filter = Some(filter);
        not_applied("ips", &rtc_conf.ips, FILTER_NOT_ON_SOCKET);
    }

    if !rtc_conf.use_mdns {
        s.ice_multicast_dns_mode = MulticastDnsMode::Disabled;
    } else {
        not_applied(
            "use_mdns",
            &rtc_conf.use_mdns,
            "the node gives the answer to an mDNS query to no connection: a candidate with an mDNS name is not resolved",
        );
    }

    let mut nat_1to1_ips: Vec<String> = Vec::new();
    // force it to the node IPs that the user has set
    if !rtc_conf.node_ip.is_empty()
        && (rtc_conf.use_external_ip || !rtc_conf.node_ip_auto_generated)
    {
        if rtc_conf.use_external_ip {
            let (ips, new_filter) =
                get_nat_1to1_ips_for_conf(rtc_conf, if_filter.as_ref(), ip_filter)?;
            ip_filter = new_filter;
            s.ip_filter = ip_filter.clone();
            if ips.is_empty() {
                infow!(logger, "no external IPs found, using node IP for NAT1To1Ips", "ip" => rtc_conf.node_ip, "advertiseInternalIP" => rtc_conf.advertise_internal_ip);
                set_nat_1to1_address_rewrite_rules(
                    &mut s,
                    &rtc_conf.node_ip.to_string_slice(),
                    rtc_conf.advertise_internal_ip,
                )?;
            } else {
                infow!(logger, "using external IPs", "ips" => ips, "advertiseInternalIP" => rtc_conf.advertise_internal_ip);
                set_nat_1to1_address_rewrite_rules(&mut s, &ips, rtc_conf.advertise_internal_ip)?;
            }
            nat_1to1_ips = ips;
        } else {
            infow!(logger, "using explicit node IP for NAT1To1Ips", "ip" => rtc_conf.node_ip, "advertiseInternalIP" => rtc_conf.advertise_internal_ip);
            set_nat_1to1_address_rewrite_rules(
                &mut s,
                &rtc_conf.node_ip.to_string_slice(),
                rtc_conf.advertise_internal_ip,
            )?;
        }
    }

    let mut network_types = Vec::with_capacity(4);

    // The port of the node's socket. With neither a port nor a range (a configuration that
    // was not validated) pion takes any port, and so does the node. With TCP forced the
    // node still has its UDP socket, on any port, and no connection gathers on it.
    let mut port: u16 = 0;
    if !rtc_conf.force_tcp {
        network_types.extend([NetworkType::Udp4, NetworkType::Udp6]);
        if rtc_conf.ice_port_range_start != 0 && rtc_conf.ice_port_range_end != 0 {
            s.set_ephemeral_udp_port_range(
                rtc_conf.ice_port_range_start as u16,
                rtc_conf.ice_port_range_end as u16,
            )?;
            port = rtc_conf.ice_port_range_start as u16;
            not_applied(
                "port_range_start, port_range_end",
                &(rtc_conf.ice_port_range_start, rtc_conf.ice_port_range_end),
                "the node has one socket for all connections: it is bound to the first port of the range",
            );
        } else if rtc_conf.udp_port.valid() {
            port = u16::try_from(rtc_conf.udp_port.start).map_err(|_| Error::Port)?;
            not_applied(
                "the socket buffers of the UDP mux",
                &DEFAULT_UDP_BUFFER_SIZE,
                "the node's socket keeps the sizes the system gives it",
            );
            if rtc_conf.batch_io.batch_size > 0 {
                not_applied(
                    "batch_io",
                    &rtc_conf.batch_io,
                    "the node writes every datagram at once",
                );
            }
            if rtc_conf.udp_port.to_slice().len() > 1 {
                not_applied(
                    "udp_port",
                    &rtc_conf.udp_port,
                    "the node has one socket: of a range of ports the first is used",
                );
            }

            if rtc_conf.use_stun_port_as_ice {
                not_applied(
                    "use_stun_port_as_ice",
                    &rtc_conf.use_stun_port_as_ice,
                    "the node has one socket: none is bound to port 3478",
                );
            }

            #[cfg(unix)]
            if !development {
                super::rtc_unix::check_udp_read_buffer();
            }
        }
    }
    let node_config = node_config_for(&s, port, rtc_conf.enable_loopback_candidate)?;

    // use TCP mux when it's set
    let mut tcp_port: u16 = 0;
    if rtc_conf.tcp_port != 0 {
        network_types.extend([NetworkType::Tcp4, NetworkType::Tcp6]);
        // `net.ListenTCP` refuses a port that is not one; the node is made later, so the
        // check is here.
        tcp_port = u16::try_from(rtc_conf.tcp_port).map_err(|_| Error::Port)?;
    }

    if rtc_conf.force_tcp && rtc_conf.tcp_port == 0 {
        return Err(Error::TcpForcedButNotConfigured);
    }
    s.network_types = network_types;

    if rtc_conf.host_acceptance_min_wait > Duration::ZERO {
        s.host_acceptance_min_wait = rtc_conf.host_acceptance_min_wait;
    }
    if rtc_conf.srflx_acceptance_min_wait > Duration::ZERO {
        s.srflx_acceptance_min_wait = rtc_conf.srflx_acceptance_min_wait;
    }
    if rtc_conf.prflx_acceptance_min_wait > Duration::ZERO {
        s.prflx_acceptance_min_wait = rtc_conf.prflx_acceptance_min_wait;
    }
    if rtc_conf.relay_acceptance_min_wait > Duration::ZERO {
        s.relay_acceptance_min_wait = rtc_conf.relay_acceptance_min_wait;
    }

    if rtc_conf.enable_loopback_candidate {
        s.include_loopback_candidate = true;
    }

    if rtc_conf.use_ice_lite {
        s.lite = true;
    } else if (rtc_conf.node_ip.is_empty() || rtc_conf.node_ip_auto_generated)
        && !rtc_conf.use_external_ip
    {
        // use STUN servers for server to support NAT
        // when deployed in production, we expect UseExternalIP to be used, and ports accessible
        // this is not compatible with ICE Lite
        // Do not automatically add STUN servers if nodeIP is set
        if !rtc_conf.stun_servers.is_empty() {
            ice_servers = vec![ice_server_for_stun_servers(&rtc_conf.stun_servers)];
        } else {
            ice_servers = vec![ice_server_for_stun_servers(&DEFAULT_STUN_SERVERS)];
        }
        not_applied(
            "stun_servers, as the ICE servers of the node's connections",
            &ice_servers,
            "the connections gather no server reflexive candidates: the node's addresses are all they have",
        );
    }

    Ok(WebRtcConfig {
        ice_servers,
        setting_engine: s,
        node_config,
        tcp_port,
        nat_1to1_ips,
        use_mdns: rtc_conf.use_mdns,
    })
}

const NO_SCTP_SETTING: &str = "the WebRTC library has no such setting";
const FILTER_NOT_ON_SOCKET: &str =
    "only in part: it chooses the addresses that are advertised, the node's socket listens on all";

/// What the node's socket is made from: bound to all IPv4 addresses on `port`, advertising
/// the addresses pion's agent would give as host candidates on a UDP mux made with these
/// settings.
fn node_config_for(
    s: &SettingEngineConfig,
    port: u16,
    include_loopback: bool,
) -> Result<NodeConfig, Error> {
    let candidates = host_candidate_ips_of(s, include_loopback)?;
    let (v4, v6): (Vec<IpAddr>, Vec<IpAddr>) = candidates.into_iter().partition(IpAddr::is_ipv4);
    if !v6.is_empty() {
        not_applied(
            "IPv6 addresses",
            &v6,
            "the node has one socket, for IPv4: these addresses are not advertised",
        );
    }
    if v4.is_empty() {
        warnw!(
            logger::get_logger(),
            "the node has no IPv4 address to advertise",
            nil
        );
    }
    Ok(NodeConfig {
        bind_address: SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port),
        nat_1to1_ips: v4,
        logger: logger::get_logger(),
    })
}

/// The host candidates of a UDP mux made with these settings, of both families.
fn host_candidate_ips_of(
    s: &SettingEngineConfig,
    include_loopback: bool,
) -> Result<Vec<IpAddr>, Error> {
    let local_ips = local_interfaces(
        &interfaces()?,
        s.interface_filter.as_ref(),
        s.ip_filter.as_ref(),
        &[NetworkType::Udp4, NetworkType::Udp6],
        include_loopback,
    );
    host_candidate_ips(&local_ips, &s.ice_address_rewrite_rules)
}

// localInterfaces of pkg/transport/ip.go: the addresses `CreateUDPMuxesFromPorts` listens on.
fn local_interfaces(
    ifaces: &[Interface],
    interface_filter: Option<&InterfaceFilter>,
    ip_filter: Option<&IpFilter>,
    network_types: &[NetworkType],
    include_loopback: bool,
) -> Vec<IpAddr> {
    let mut ips = Vec::new();

    let ipv4_requested = network_types
        .iter()
        .any(|typ| matches!(typ, NetworkType::Udp4 | NetworkType::Tcp4));
    let ipv6_requested = network_types
        .iter()
        .any(|typ| matches!(typ, NetworkType::Udp6 | NetworkType::Tcp6));

    for iface in ifaces {
        if !iface.up {
            continue; // Interface down
        }
        if iface.loopback && !include_loopback {
            continue; // Loopback interface
        }

        if interface_filter.is_some_and(|interface_filter| !interface_filter(&iface.name)) {
            continue;
        }

        for ip in iface.addrs.iter().copied() {
            if ip.is_loopback() && !include_loopback {
                continue;
            }

            match ip {
                IpAddr::V6(ip) => {
                    if !ipv6_requested || !is_supported_ipv6(ip) {
                        continue;
                    }
                }
                IpAddr::V4(_) => {
                    if !ipv4_requested {
                        continue;
                    }
                }
            }

            if ip_filter.is_some_and(|ip_filter| !ip_filter(ip)) {
                continue;
            }

            ips.push(ip);
        }
    }
    ips
}

// The conditions of invalidation written below are defined in
// https://tools.ietf.org/html/rfc8445#section-5.1.1.1
fn is_supported_ipv6(ip: Ipv6Addr) -> bool {
    let octets = ip.octets();
    !(octets[..12] == [0; 12] // !(IPv4-compatible IPv6)
        || (octets[0] == 0xfe && octets[1] & 0xc0 == 0xc0) // !(IPv6 site-local unicast)
        || is_link_local(IpAddr::V6(ip)))
}

/// A rewrite rule for host candidates, its addresses read.
struct RuleMapping {
    mode: IceAddressRewriteMode,
    /// The local address the rule is for; none: all of the family of an external address.
    local: Option<IpAddr>,
    external: Vec<IpAddr>,
}

/// The addresses given as host candidates for these local addresses: what pion's ICE agent
/// makes of the addresses of a UDP mux and the rewrite rules. A local address a rule names
/// is replaced by the rule's external addresses, or kept with them after it when the rule
/// appends; one that no rule names falls to the first rule without a local address that has
/// an external address of its family; one that no rule is for stays as it is.
fn host_candidate_ips(
    local_ips: &[IpAddr],
    rules: &[IceAddressRewriteRule],
) -> Result<Vec<IpAddr>, Error> {
    let mut mappings = Vec::new();
    for rule in rules {
        if !matches!(
            rule.as_candidate_type,
            IceCandidateType::Unspecified | IceCandidateType::Host
        ) {
            continue;
        }
        // For a host candidate a rule replaces, unless it says otherwise.
        let mode = match rule.mode {
            IceAddressRewriteMode::Unspecified => IceAddressRewriteMode::Replace,
            mode => mode,
        };
        let read = |ip: &str| parse_ip(ip.trim()).ok_or(Error::InvalidNat1To1IpMapping);
        let local = match rule.local.trim() {
            "" => None,
            local => Some(read(local)?),
        };
        let external = rule
            .external
            .iter()
            .map(|ip| read(ip))
            .collect::<Result<Vec<IpAddr>, Error>>()?;
        mappings.push(RuleMapping {
            mode,
            local,
            external,
        });
    }

    let find_external_ips = |local: IpAddr| -> Option<(Vec<IpAddr>, IceAddressRewriteMode)> {
        let mut catch_all = None;
        for rule in &mappings {
            match rule.local {
                Some(rule_local) => {
                    if rule_local == local {
                        return Some((rule.external.clone(), rule.mode));
                    }
                }
                None => {
                    let ips: Vec<IpAddr> = rule
                        .external
                        .iter()
                        .copied()
                        .filter(|ip| ip.is_ipv4() == local.is_ipv4())
                        .collect();
                    if catch_all.is_none() && (!ips.is_empty() || rule.external.is_empty()) {
                        catch_all = Some((ips, rule.mode));
                    }
                }
            }
        }
        catch_all
    };

    let mut candidates = Vec::new();
    for local in local_ips.iter().map(IpAddr::to_canonical) {
        let candidate_ips = match find_external_ips(local) {
            None => vec![local],
            Some((mapped, mode)) if mapped.is_empty() => {
                if mode == IceAddressRewriteMode::Replace {
                    continue;
                }
                vec![local]
            }
            Some((mapped, IceAddressRewriteMode::Replace)) => mapped,
            Some((mapped, _)) => [vec![local], mapped].concat(),
        };
        for ip in candidate_ips {
            if !candidates.contains(&ip) {
                candidates.push(ip);
            }
        }
    }
    Ok(candidates)
}

pub fn set_nat_1to1_address_rewrite_rules(
    s: &mut SettingEngineConfig,
    ips: &[String],
    include_internal: bool,
) -> Result<(), Error> {
    s.ice_address_rewrite_rules = nat_1to1_address_rewrite_rules(ips, include_internal);
    Ok(())
}

fn nat_1to1_address_rewrite_rules(
    ips: &[String],
    include_internal: bool,
) -> Vec<IceAddressRewriteRule> {
    let mut rules = Vec::with_capacity(ips.len() + 1);
    let mut catch_all = Vec::with_capacity(ips.len());

    let mut mode = IceAddressRewriteMode::Unspecified;
    if include_internal {
        mode = IceAddressRewriteMode::Append;
    }
    for ip in ips {
        let parts: Vec<&str> = ip.split('/').collect();
        if parts.len() == 2 {
            rules.push(IceAddressRewriteRule {
                external: vec![parts[0].to_string()],
                local: parts[1].to_string(),
                as_candidate_type: IceCandidateType::Host,
                mode,
            });
        } else {
            catch_all.push(ip.clone());
        }
    }
    if !catch_all.is_empty() {
        rules.push(IceAddressRewriteRule {
            external: catch_all,
            as_candidate_type: IceCandidateType::Host,
            mode,
            ..Default::default()
        });
    }

    rules
}

fn ice_server_for_stun_servers<S: AsRef<str>>(servers: &[S]) -> IceServer {
    let mut ice_server = IceServer::default();
    for stun_server in servers {
        ice_server
            .urls
            .push(format!("stun:{}", stun_server.as_ref()));
    }
    ice_server
}

fn get_nat_1to1_ips_for_conf(
    rtc_conf: &RtcConfig,
    if_filter: Option<&InterfaceFilter>,
    mut ip_filter: Option<IpFilter>,
) -> Result<(Vec<String>, Option<IpFilter>), Error> {
    let logger = logger::get_logger();
    let mut stun_servers = rtc_conf.stun_servers.clone();
    if stun_servers.is_empty() {
        stun_servers = DEFAULT_STUN_SERVERS.map(str::to_string).to_vec();
    }
    let local_ips = get_local_ip_addresses(
        rtc_conf.enable_loopback_candidate,
        true,
        if_filter,
        ip_filter.as_ref(),
    )?;
    struct IpMapping {
        external_ip: String,
        local_ip: String,
    }
    let (addr_tx, addr_rx) = mpsc::channel::<IpMapping>();

    let mut udp_ports: Vec<i64> = Vec::new();
    if rtc_conf.ice_port_range_start != 0 && rtc_conf.ice_port_range_end != 0 {
        let (port_range_start, port_range_end) = (
            rtc_conf.ice_port_range_start as u16,
            rtc_conf.ice_port_range_end as u16,
        );
        for _ in 0..5 {
            // As in the original, a range of one port (or of 65536) is a panic here: there
            // is nothing to choose from.
            udp_ports.push(
                rand::rng().random_range(0..port_range_end.wrapping_sub(port_range_start) as i64)
                    + port_range_start as i64,
            );
        }
    } else if rtc_conf.udp_port.valid() {
        udp_ports.push(rtc_conf.udp_port.start);
    } else {
        udp_ports.push(0);
    }

    let mut wg = Vec::new();
    let (ctx, cancel) = Context::background().with_cancel();
    for ip in &local_ips {
        let local_ip = ip.clone();
        let addr_tx = addr_tx.clone();
        let ctx = ctx.clone();
        let stun_servers = stun_servers.clone();
        let udp_ports = udp_ports.clone();
        let logger = logger.clone();
        let validate = !rtc_conf.skip_external_ip_validation;
        wg.push(std::thread::spawn(move || {
            for port in &udp_ports {
                let addr = parse_ip(&local_ip)
                    .ok_or_else(|| Error::InvalidIp(local_ip.clone()))
                    .and_then(|ip| {
                        let port = u16::try_from(*port).map_err(|_| Error::Port)?;
                        get_external_ip_on(
                            &ctx,
                            &stun_servers,
                            Some(SocketAddr::new(ip, port)),
                            validate,
                            Network::Udp,
                        )
                    });
                match addr {
                    Err(err) => {
                        if is_address_in_use(&err) {
                            infow!(logger, "failed to get external ip, address already in use", "local" => local_ip, "port" => port);
                            continue;
                        }
                        infow!(logger, "failed to get external ip", "local" => local_ip, "err" => err.to_string());
                        return;
                    }
                    Ok(addr) => {
                        let _ = addr_tx.send(IpMapping {
                            external_ip: addr,
                            local_ip,
                        });
                        return;
                    }
                }
            }
            infow!(logger, "failed to get external ip after all ports tried", "local" => local_ip, "ports" => udp_ports);
        }));
    }
    drop(addr_tx);

    let mut first_resolved = false;
    let mut mapped_ips: Vec<String> = Vec::new();
    // Go's `map[string]string`, external to local, in the order it was filled.
    let mut nat_mapping: Vec<(String, String)> = Vec::new();
    let mut timeout = Instant::now() + Duration::from_secs(5).to_std();

    // Until the timer, or until every lookup has ended.
    while let Ok(mapping) = addr_rx.recv_timeout(timeout.saturating_duration_since(Instant::now()))
    {
        if !first_resolved {
            first_resolved = true;
            timeout = Instant::now() + Duration::from_secs(1).to_std();
        }
        if let Some((_, local)) = nat_mapping
            .iter()
            .find(|(external, _)| *external == mapping.external_ip)
        {
            infow!(
                logger,
                "external ip already resolved, ignore duplicate",
                "external" => mapping.external_ip,
                "local" => local,
                "ignore" => mapping.local_ip);
        } else {
            nat_mapping.push((mapping.external_ip, mapping.local_ip.clone()));
            mapped_ips.push(mapping.local_ip);
        }
    }
    cancel.cancel();
    for lookup in wg {
        let _ = lookup.join();
    }

    if nat_mapping.is_empty() {
        // no external ip resolved
        return Ok((Vec::new(), ip_filter));
    }

    // mapping unresolved local ip to itself
    for local in &local_ips {
        let found = nat_mapping
            .iter()
            .any(|(_, local_ip_mapping)| local == local_ip_mapping);
        if !found {
            match nat_mapping
                .iter_mut()
                .find(|(external, _)| external == local)
            {
                Some(mapping) => mapping.1 = local.clone(),
                None => nat_mapping.push((local.clone(), local.clone())),
            }
        }
    }

    let nat_1to1_ips: Vec<String> = nat_mapping
        .iter()
        .map(|(external, local)| format!("{external}/{local}"))
        .collect();

    if rtc_conf.external_ip_only {
        let origin_filter = ip_filter.clone();
        let mapped: Vec<IpAddr> = mapped_ips.iter().filter_map(|ip| parse_ip(ip)).collect();
        ip_filter = Some(Arc::new(move |ip: IpAddr| {
            // don't filter out ipv6 address
            let ip = ip.to_canonical();
            if !ip.is_ipv4() {
                return origin_filter
                    .as_ref()
                    .is_none_or(|origin_filter| origin_filter(ip));
            }

            mapped.contains(&ip)
        }));
        infow!(logger, "use ips(v4) mapped to external only", "ips" => mapped_ips);
    }
    Ok((nat_1to1_ips, ip_filter))
}

/// The original looks for "address already in use" in the text of the error.
fn is_address_in_use(err: &Error) -> bool {
    matches!(err, Error::Io(err) if err.kind() == std::io::ErrorKind::AddrInUse)
}

pub fn interface_filter_from_conf(ifs: &InterfacesConfig) -> InterfaceFilter {
    let includes = ifs.includes.clone();
    let excludes = ifs.excludes.clone();
    Arc::new(move |s: &str| {
        // filter by include interfaces
        if !includes.is_empty() {
            return includes.iter().any(|iface| iface == s);
        }

        // filter by exclude interfaces
        if !excludes.is_empty() && excludes.iter().any(|iface| iface == s) {
            return false;
        }
        true
    })
}

pub fn ip_filter_from_conf(ips: &IpsConfig) -> Result<IpFilter, Error> {
    let mut ipnets: [Vec<IpNet>; 2] = [Vec::new(), Vec::new()];
    for (i, ips) in [&ips.includes, &ips.excludes].into_iter().enumerate() {
        for ip in ips {
            ipnets[i].push(IpNet::parse_cidr(ip)?);
        }
    }

    let [includes, excludes] = ipnets;

    Ok(Arc::new(move |ip: IpAddr| {
        if !includes.is_empty() {
            return includes.iter().any(|ipn| ipn.contains(ip));
        }

        if !excludes.is_empty() && excludes.iter().any(|ipn| ipn.contains(ip)) {
            return false;
        }
        true
    }))
}

/// Go's `net.IPNet`: a network, as `net.ParseCIDR` reads one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct IpNet {
    /// The address of the network: nothing set beyond the prefix.
    ip: IpAddr,
    prefix: u32,
}

impl IpNet {
    /// Go's `net.ParseCIDR`, of which only the network is used here.
    fn parse_cidr(s: &str) -> Result<IpNet, Error> {
        let err = || Error::InvalidCidr(s.to_string());
        let (addr, mask) = s.split_once('/').ok_or_else(err)?;
        let ip: IpAddr = addr.parse().map_err(|_| err())?;
        if mask.is_empty() || !mask.bytes().all(|b| b.is_ascii_digit()) {
            return Err(err());
        }
        let prefix: u32 = mask.parse().map_err(|_| err())?;
        match ip {
            IpAddr::V4(ip) if prefix <= 32 => Ok(IpNet {
                ip: mask_v4(ip, prefix).into(),
                prefix,
            }),
            IpAddr::V6(ip) if prefix <= 128 => {
                let ip = mask_v6(ip, prefix);
                // A network of IPv4 addresses written as IPv6 addresses is one of IPv4
                // addresses, as `Contains` has it there.
                match ip.to_ipv4_mapped() {
                    Some(ip) if prefix >= 96 => Ok(IpNet {
                        ip: ip.into(),
                        prefix: prefix - 96,
                    }),
                    _ => Ok(IpNet {
                        ip: ip.into(),
                        prefix,
                    }),
                }
            }
            _ => Err(err()),
        }
    }

    fn contains(&self, ip: IpAddr) -> bool {
        match (self.ip, ip.to_canonical()) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => mask_v4(ip, self.prefix) == net,
            (IpAddr::V6(net), IpAddr::V6(ip)) => mask_v6(ip, self.prefix) == net,
            _ => false,
        }
    }
}

fn mask_v4(ip: Ipv4Addr, prefix: u32) -> Ipv4Addr {
    Ipv4Addr::from(u32::from(ip) & u32::MAX.checked_shl(32 - prefix).unwrap_or(0))
}

fn mask_v6(ip: Ipv6Addr, prefix: u32) -> Ipv6Addr {
    Ipv6Addr::from(u128::from(ip) & u128::MAX.checked_shl(128 - prefix).unwrap_or(0))
}

#[cfg(test)]
#[path = "webrtc_config_test.rs"]
mod webrtc_config_test;
