// Ported from LiveKit, pkg/service/turnrelay.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - pion/turn's `RelayAddressGenerator` has no `AllocateListener` and `AllocateConn` in the
//   port (RFC 6062's TCP relays are not ported, and the server refuses them itself), so
//   `rejectTCPRelayAddressGenerator` passes `Validate` and `AllocatePacketConn` on and has its
//   two refusals as methods of its own, which nothing calls.
// - Go's `net.IP` predicates are written out for `IpAddr` (`is_restricted_peer_ip`), with an
//   IPv4 address in an IPv6 one taken as the IPv4 address, as Go's are.
// - `net.InterfaceAddrs` is `getifaddrs` of the crate `nix`.

use std::collections::HashSet;
use std::io;
use std::net::IpAddr;
use std::sync::Arc;

use super::Error;
use crate::config::RtcConfig;
use crate::webrtc::turn::net::{Addr, BoxFuture, IpNet, PacketConn};
use crate::webrtc::turn::{self, AllocateListenerConfig, RelayAddressGenerator};

// restricted ranges not covered by the net.IP predicates
fn extra_restricted_peer_prefixes() -> [IpNet; 6] {
    [
        IpNet::parse_cidr("0.0.0.0/8").unwrap(),     // "this" network
        IpNet::parse_cidr("100.64.0.0/10").unwrap(), // carrier-grade NAT
        IpNet::parse_cidr("198.18.0.0/15").unwrap(), // benchmarking
        IpNet::parse_cidr("240.0.0.0/4").unwrap(),   // reserved, includes broadcast
        IpNet::parse_cidr("64:ff9b::/96").unwrap(),  // NAT64, can map to restricted IPv4
        IpNet::parse_cidr("64:ff9b:1::/48").unwrap(), // local-use NAT64
    ]
}

pub(crate) fn is_restricted_peer_ip(ip: IpAddr) -> bool {
    // ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
    // ip.IsMulticast() || ip.IsPrivate() || ip.IsUnspecified()
    let restricted = match ip.to_canonical() {
        IpAddr::V4(ip) => {
            ip.is_loopback()
                || ip.is_link_local()
                || ip.octets()[..3] == [224, 0, 0]
                || ip.is_multicast()
                || ip.is_private()
                || ip.is_unspecified()
        }
        IpAddr::V6(ip) => {
            let s0 = ip.segments()[0];
            ip.is_loopback()
                || s0 & 0xffc0 == 0xfe80
                || s0 & 0xff0f == 0xff02
                || ip.is_multicast()
                || s0 & 0xfe00 == 0xfc00
                || ip.is_unspecified()
        }
    };
    if restricted {
        return true;
    }

    let addr = ip.to_canonical();
    for prefix in extra_restricted_peer_prefixes() {
        if prefix.contains(addr) {
            return true;
        }
    }
    false
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PortRange {
    pub(crate) start: u16,
    pub(crate) end: u16,
}

// rejectTCPRelayAddressGenerator refuses RFC 6062 TCP relays, which would let
// a client open TCP connections to any permitted address.
pub(crate) struct RejectTcpRelayAddressGenerator {
    relay_address_generator: Arc<dyn RelayAddressGenerator>,
}

pub(crate) fn new_reject_tcp_relay_address_generator(
    g: Arc<dyn RelayAddressGenerator>,
) -> RejectTcpRelayAddressGenerator {
    RejectTcpRelayAddressGenerator {
        relay_address_generator: g,
    }
}

// Nothing calls these but their test: the port's TURN server has no TCP relays to ask for.
#[cfg_attr(not(test), allow(dead_code))]
impl RejectTcpRelayAddressGenerator {
    pub(crate) fn allocate_listener(&self, _: AllocateListenerConfig) -> Result<(), Error> {
        Err(Error::TurnTcpRelayDisabled)
    }

    pub(crate) fn allocate_conn(&self, _: AllocateListenerConfig) -> Result<(), Error> {
        Err(Error::TurnTcpRelayDisabled)
    }
}

impl RelayAddressGenerator for RejectTcpRelayAddressGenerator {
    fn validate(&self) -> Result<(), turn::Error> {
        self.relay_address_generator.validate()
    }

    fn allocate_packet_conn(
        &self,
        conf: AllocateListenerConfig,
    ) -> Result<(Arc<dyn PacketConn>, Addr), turn::Error> {
        self.relay_address_generator.allocate_packet_conn(conf)
    }
}

// turnRelayPolicy enforces what TURN permissions cannot, since they are per IP
// only: relays to this host's own addresses may reach only the RTC media
// ports, because such packets are delivered locally and bypass any firewall
// in front of the host. Used for embedded TURN, which shares the host with
// the SFU.
pub(crate) struct TurnRelayPolicy {
    relay_address_generator: Option<Arc<dyn RelayAddressGenerator>>,

    local_ips: HashSet<IpAddr>,
    media_ports: Vec<PortRange>,
}

pub(crate) fn new_turn_relay_policy(
    g: Option<Arc<dyn RelayAddressGenerator>>,
    local_ips: HashSet<IpAddr>,
    media_ports: Vec<PortRange>,
) -> Arc<TurnRelayPolicy> {
    Arc::new(TurnRelayPolicy {
        relay_address_generator: g,
        local_ips,
        media_ports,
    })
}

impl TurnRelayPolicy {
    pub(crate) fn allows(&self, addr: Addr) -> bool {
        let Addr::Udp(udp_addr) = addr else {
            return false;
        };
        if !self.local_ips.contains(&udp_addr.ip().to_canonical()) {
            return true;
        }
        for r in &self.media_ports {
            if udp_addr.port() >= r.start && udp_addr.port() <= r.end {
                return true;
            }
        }
        false
    }
}

impl RelayAddressGenerator for Arc<TurnRelayPolicy> {
    fn validate(&self) -> Result<(), turn::Error> {
        self.relay_address_generator
            .as_ref()
            .expect("a relay policy that is used wraps a generator")
            .validate()
    }

    fn allocate_packet_conn(
        &self,
        c: AllocateListenerConfig,
    ) -> Result<(Arc<dyn PacketConn>, Addr), turn::Error> {
        let (conn, addr) = self
            .relay_address_generator
            .as_ref()
            .expect("a relay policy that is used wraps a generator")
            .allocate_packet_conn(c)?;
        Ok((
            Arc::new(TurnRelayPacketConn {
                packet_conn: conn,
                policy: self.clone(),
            }),
            addr,
        ))
    }
}

// turnRelayPacketConn silently drops relayed packets to peers the policy does
// not allow.
pub(crate) struct TurnRelayPacketConn {
    packet_conn: Arc<dyn PacketConn>,
    policy: Arc<TurnRelayPolicy>,
}

impl PacketConn for TurnRelayPacketConn {
    fn read_from<'a>(&'a self, p: &'a mut [u8]) -> BoxFuture<'a, io::Result<(usize, Addr)>> {
        self.packet_conn.read_from(p)
    }

    fn write_to<'a>(&'a self, p: &'a [u8], addr: Addr) -> BoxFuture<'a, io::Result<usize>> {
        if !self.policy.allows(addr) {
            return Box::pin(async move { Ok(p.len()) });
        }
        self.packet_conn.write_to(p, addr)
    }

    fn close(&self) -> io::Result<()> {
        self.packet_conn.close()
    }

    fn local_addr(&self) -> Addr {
        self.packet_conn.local_addr()
    }
}

// getTURNLocalIPs returns the addresses that are delivered to this host: its
// interface addresses and the node IPs, which may be 1:1 NAT addresses.
pub(crate) fn get_turn_local_ips(rtc_conf: &RtcConfig) -> Result<HashSet<IpAddr>, Error> {
    let mut ips = HashSet::new();

    for ip in interface_addrs().map_err(|err| Error::TurnLocalIps(err.to_string()))? {
        ips.insert(ip.to_canonical());
    }

    for ip in rtc_conf.rtc_config.node_ip.to_string_slice() {
        let addr: IpAddr = ip
            .parse()
            .map_err(|_| Error::TurnLocalIps(format!("ParseAddr({ip:?}): unable to parse IP")))?;
        ips.insert(addr.to_canonical());
    }
    Ok(ips)
}

// Go's net.InterfaceAddrs()
#[cfg(unix)]
fn interface_addrs() -> io::Result<Vec<IpAddr>> {
    let mut addrs = Vec::new();
    for ifa in nix::ifaddrs::getifaddrs()? {
        let Some(address) = ifa.address else {
            continue;
        };
        if let Some(sin) = address.as_sockaddr_in() {
            addrs.push(IpAddr::V4(sin.ip()));
        } else if let Some(sin6) = address.as_sockaddr_in6() {
            addrs.push(IpAddr::V6(sin6.ip()));
        }
    }
    Ok(addrs)
}

#[cfg(not(unix))]
fn interface_addrs() -> io::Result<Vec<IpAddr>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "the interfaces of this system cannot be listed",
    ))
}

pub(crate) fn get_turn_media_ports(rtc_conf: &RtcConfig) -> Vec<PortRange> {
    let rtc = &rtc_conf.rtc_config;
    if rtc.force_tcp {
        return Vec::new();
    }

    let mut ports = Vec::new();
    if rtc.ice_port_range_start != 0 && rtc.ice_port_range_end != 0 {
        ports.push(PortRange {
            start: rtc.ice_port_range_start as u16,
            end: rtc.ice_port_range_end as u16,
        });
    } else if rtc.udp_port.valid() {
        let end = rtc.udp_port.end.max(rtc.udp_port.start);
        ports.push(PortRange {
            start: rtc.udp_port.start as u16,
            end: end as u16,
        });
        if rtc.use_stun_port_as_ice {
            ports.push(PortRange {
                start: 3478,
                end: 3478,
            });
        }
    }
    ports
}

#[cfg(test)]
#[path = "turnrelay_test.rs"]
mod turnrelay_test;
