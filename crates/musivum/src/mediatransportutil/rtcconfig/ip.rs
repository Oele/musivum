// Ported from LiveKit's mediatransportutil, pkg/rtcconfig/ip.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the functions block, as in the original, on sockets
// of the standard library; `context.Context` is `Context` here, which has what the package
// uses of one (a deadline and a cancellation, both looked at every 50 ms as the original
// does); a local address is an `Option<SocketAddr>`, so the original's "not UDP address"
// cannot arise; the interfaces of the system (Go's `net.Interfaces` and `Addrs`) come from
// `interfaces`, on `getifaddrs` of the crate `nix`, and `get_local_ip_addresses` hands them
// to `local_ip_addresses_from`, which has the original's loop, so that it can be given a
// made-up list; pion's STUN client is the exchange written out in `find_external_ip` with
// the message types of `rtc`'s STUN crate: the request is sent again after 300 ms, then
// 600 ms, 900 ms and so on as that client does, answers to another transaction are passed
// over, and the deadline of 5 s comes before the client's own limit of 7 repeats; the
// goroutine that reads in `validate_external_ip` is a loop on the calling thread; errors of
// the system have Rust's texts.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs, UdpSocket};
use std::time::Instant;

use tokio_util::sync::CancellationToken;

use crate::go::time::Duration;
use crate::protocol::logger;
use crate::webrtc::stun;
use crate::webrtc::stun::message::{BINDING_REQUEST, Getter, Message, TransactionId};
use crate::webrtc::stun::xoraddr::XorMappedAddress;
use crate::{debugw, infow, warnw};

use super::config::parse_ip;
use super::{DEFAULT_STUN_SERVERS, Error, InterfaceFilter, IpFilter, RtcConfig};

const STUN_PING_TIMEOUT: Duration = Duration::from_secs(5);
const VALIDATION_TIMEOUT: Duration = Duration::from_secs(5);

/// How often a wait looks at its context.
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// pion's STUN client: the time after which a request is sent again, which grows by as much
/// with every repeat.
const STUN_RTO: Duration = Duration::from_millis(300);

/// Go's `context.Context`, as far as this package uses one.
#[derive(Clone, Debug, Default)]
pub struct Context {
    deadline: Option<Instant>,
    cancel: CancellationToken,
}

impl Context {
    /// `context.Background()`.
    pub fn background() -> Context {
        Context::default()
    }

    /// `context.WithTimeout(ctx, d)`.
    pub fn with_timeout(&self, d: Duration) -> Context {
        let deadline = Instant::now() + d.to_std();
        Context {
            deadline: Some(
                self.deadline
                    .map_or(deadline, |parent| parent.min(deadline)),
            ),
            cancel: self.cancel.clone(),
        }
    }

    /// `context.WithCancel(ctx)`: the context, and what cancels it.
    pub fn with_cancel(&self) -> (Context, CancellationToken) {
        let cancel = self.cancel.child_token();
        (
            Context {
                deadline: self.deadline,
                cancel: cancel.clone(),
            },
            cancel,
        )
    }

    /// `ctx.Err()`.
    pub fn err(&self) -> Option<Error> {
        if self.cancel.is_cancelled() {
            Some(Error::Canceled)
        } else if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            Some(Error::DeadlineExceeded)
        } else {
            None
        }
    }
}

/// The networks the original dials a STUN server on: Go's "udp" and "udp4".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Network {
    Udp,
    Udp4,
}

/// Go's `net.Interface` with its `Addrs()`, as far as this package looks at one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Interface {
    pub(super) name: String,
    /// `net.FlagUp`.
    pub(super) up: bool,
    /// `net.FlagLoopback`.
    pub(super) loopback: bool,
    pub(super) addrs: Vec<IpAddr>,
}

/// Go's `net.Interfaces()`: the interfaces of the system in its order, each with its
/// addresses.
#[cfg(unix)]
pub(super) fn interfaces() -> io::Result<Vec<Interface>> {
    use nix::net::if_::InterfaceFlags;

    let mut ifaces: Vec<Interface> = Vec::new();
    for ifa in nix::ifaddrs::getifaddrs()? {
        // An address with a label of its own ("eth0:1") is its interface's.
        let name = ifa.interface_name.split(':').next().unwrap_or_default();
        let at = match ifaces.iter().position(|iface| iface.name == name) {
            Some(at) => at,
            None => {
                ifaces.push(Interface {
                    name: name.to_string(),
                    up: ifa.flags.contains(InterfaceFlags::IFF_UP),
                    loopback: ifa.flags.contains(InterfaceFlags::IFF_LOOPBACK),
                    addrs: Vec::new(),
                });
                ifaces.len() - 1
            }
        };
        let Some(address) = ifa.address else {
            continue;
        };
        if let Some(sin) = address.as_sockaddr_in() {
            ifaces[at].addrs.push(IpAddr::V4(sin.ip()));
        } else if let Some(sin6) = address.as_sockaddr_in6() {
            ifaces[at].addrs.push(IpAddr::V6(sin6.ip()));
        }
    }
    Ok(ifaces)
}

#[cfg(not(unix))]
pub(super) fn interfaces() -> io::Result<Vec<Interface>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "the interfaces of this system cannot be listed",
    ))
}

/// Go's `ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()`.
pub(super) fn is_link_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_link_local() || ip.octets()[..3] == [224, 0, 0],
        IpAddr::V6(ip) => {
            let first = ip.segments()[0];
            first & 0xffc0 == 0xfe80 || first & 0xff0f == 0xff02
        }
    }
}

impl RtcConfig {
    pub(super) fn determine_ip(&self) -> Result<String, Error> {
        if self.use_external_ip {
            let mut stun_servers = self.stun_servers.clone();
            if stun_servers.is_empty() {
                stun_servers = DEFAULT_STUN_SERVERS.map(str::to_string).to_vec();
            }
            // When an IPv4 is required, dial STUN over udp4 so a not-yet-attached IPv4
            // surfaces as an error (and is retried) instead of the request silently
            // resolving the node's IPv6.
            let get_ip = if self.require_ipv4 {
                get_external_ipv4
            } else {
                get_external_ip
            };
            let mut err = Error::StunServersRequired;
            for _ in 0..3 {
                match get_ip(&Context::background(), &stun_servers, None) {
                    Ok(ip) => return Ok(ip),
                    Err(e) => {
                        err = e;
                        std::thread::sleep(Duration::from_millis(500).to_std());
                    }
                }
            }
            warnw!(logger::get_logger(), "could not resolve external IP", err);
            return Err(Error::CouldNotResolveExternalIp(Box::new(err)));
        }

        // use local ip instead
        let addresses = get_local_ip_addresses(false, true, None, None);
        // prefer IPv4 address if available
        for addr in addresses.iter().flatten() {
            if parse_ip(addr).is_some_and(|ip| ip.is_ipv4()) {
                return Ok(addr.clone());
            }
        }
        if self.require_ipv4 {
            return Err(Error::NoIpv4AddressAvailable);
        }
        addresses?.into_iter().next().ok_or(Error::NoLocalIpAddress)
    }
}

pub fn get_local_ip_addresses(
    include_loopback: bool,
    include_v6: bool,
    if_filter: Option<&InterfaceFilter>,
    ip_filter: Option<&IpFilter>,
) -> Result<Vec<String>, Error> {
    let ifaces = interfaces()?;
    local_ip_addresses_from(&ifaces, include_loopback, include_v6, if_filter, ip_filter)
}

pub(super) fn local_ip_addresses_from(
    ifaces: &[Interface],
    include_loopback: bool,
    include_v6: bool,
    if_filter: Option<&InterfaceFilter>,
    ip_filter: Option<&IpFilter>,
) -> Result<Vec<String>, Error> {
    let mut loop_backs = Vec::new();
    let mut addresses = Vec::new();
    for iface in ifaces {
        if if_filter.is_some_and(|if_filter| !if_filter(&iface.name)) {
            continue;
        }

        for ip in iface.addrs.iter().copied() {
            if !include_v6 && !ip.is_ipv4() {
                continue;
            }
            if is_link_local(ip) {
                continue;
            }
            if ip_filter.is_some_and(|ip_filter| !ip_filter(ip)) {
                continue;
            }
            if ip.is_loopback() {
                loop_backs.push(ip.to_string());
            } else {
                addresses.push(ip.to_string());
            }
        }
    }

    if include_loopback {
        addresses.extend(loop_backs.iter().cloned());
    }

    if !addresses.is_empty() {
        return Ok(addresses);
    }
    if !loop_backs.is_empty() {
        return Ok(loop_backs);
    }
    Err(Error::NoLocalIpAddress)
}

/// Go's `dialer.Dial(network, stun_server)` with a local address: a UDP socket bound to the
/// local address, if there is one, and connected to the first address of the server that is
/// of the family asked for.
fn dial(
    network: Network,
    stun_server: &str,
    local_addr: Option<SocketAddr>,
) -> Result<UdpSocket, Error> {
    let mut err = None;
    for remote in stun_server.to_socket_addrs()? {
        if network == Network::Udp4 && !remote.is_ipv4() {
            continue;
        }
        let local = match local_addr {
            Some(local) if local.ip().is_unspecified() => local,
            Some(local) if local.is_ipv4() != remote.is_ipv4() => continue,
            Some(local) => local,
            None if remote.is_ipv4() => SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0),
            None => SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0),
        };
        match UdpSocket::bind(local).and_then(|conn| conn.connect(remote).map(|()| conn)) {
            Ok(conn) => return Ok(conn),
            Err(e) => err = Some(e),
        }
    }
    Err(err.map_or(Error::NoSuitableAddress, Error::Io))
}

fn find_external_ip(
    ctx: &Context,
    stun_server: &str,
    local_addr: Option<SocketAddr>,
    validate: bool,
    network: Network,
) -> Result<String, Error> {
    let ctx1 = ctx.with_timeout(STUN_PING_TIMEOUT);

    let conn = dial(network, stun_server, local_addr)?;
    conn.set_read_timeout(Some(POLL_INTERVAL.to_std()))?;

    let stun_err = |err: stun::Error| Error::Stun(err.to_string());
    let mut message = Message::new();
    message
        .build(&[Box::new(TransactionId::new()), Box::new(BINDING_REQUEST)])
        .map_err(stun_err)?;
    conn.send(&message.raw)?;

    let mut attempt = 0;
    let mut next_timeout = Instant::now() + STUN_RTO.to_std();
    let mut buf = [0u8; 2048];
    let ip_addr = loop {
        if let Some(err) = ctx1.err() {
            return Err(err);
        }

        match conn.recv(&mut buf) {
            Ok(n) => {
                let mut res = Message::new();
                if res.unmarshal_binary(&buf[..n]).is_err()
                    || res.transaction_id != message.transaction_id
                {
                    continue;
                }
                let mut xor_addr = XorMappedAddress::default();
                xor_addr.get_from(&res).map_err(stun_err)?;
                break xor_addr.ip.to_canonical().to_string();
            }
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            // What the system says about an earlier datagram (nobody listens there): the
            // client reads on, and so does this.
            Err(_) => std::thread::sleep(Duration::from_millis(5).to_std()),
        }

        if Instant::now() >= next_timeout {
            attempt += 1;
            conn.send(&message.raw)?;
            next_timeout = Instant::now() + (STUN_RTO * (attempt + 1)).to_std();
        }
    };

    drop(conn);
    infow!(
        logger::get_logger(),
        "found external IP via STUN",
        "localAddr" => local_addr,
        "stunServer" => stun_server,
        "externalIP" => ip_addr,
        "validateExternalIP" => validate,
    );
    if !validate {
        return Ok(ip_addr);
    }
    validate_external_ip(ctx, &ip_addr, local_addr)?;
    Ok(ip_addr)
}

// GetExternalIP return external IP for localAddr from stun server. If localAddr is nil, a local address is chosen automatically,
// else the address will be used to validate the external IP is accessible from the outside.
pub fn get_external_ip(
    ctx: &Context,
    stun_servers: &[String],
    local_addr: Option<SocketAddr>,
) -> Result<String, Error> {
    get_external_ip_on(ctx, stun_servers, local_addr, true, Network::Udp)
}

// GetExternalIPv4 is like GetExternalIP but forces the STUN request over IPv4, so
// it returns an IPv4 reflexive address (or an error if no IPv4 route exists).
pub fn get_external_ipv4(
    ctx: &Context,
    stun_servers: &[String],
    local_addr: Option<SocketAddr>,
) -> Result<String, Error> {
    get_external_ip_on(ctx, stun_servers, local_addr, true, Network::Udp4)
}

// getExternalIP of the original, which Go tells from GetExternalIP by its first letter.
pub(super) fn get_external_ip_on(
    ctx: &Context,
    stun_servers: &[String],
    local_addr: Option<SocketAddr>,
    validate: bool,
    network: Network,
) -> Result<String, Error> {
    if stun_servers.is_empty() {
        return Err(Error::StunServersRequired);
    }

    let ctx1 =
        ctx.with_timeout((STUN_PING_TIMEOUT + VALIDATION_TIMEOUT) * stun_servers.len() as i64);

    let mut err = Error::StunServersRequired;
    for ss in stun_servers {
        match find_external_ip(&ctx1, ss, local_addr, validate, network) {
            Ok(ip_addr) => return Ok(ip_addr),
            Err(e) => err = e,
        }
    }

    Err(err)
}

// validateExternalIP validates that the external IP is accessible from the outside by listen the local address,
// it will send a magic string to the external IP and check the string is received by the local address.
fn validate_external_ip(
    ctx: &Context,
    node_ip: &str,
    addr: Option<SocketAddr>,
) -> Result<(), Error> {
    let Some(udp_addr) = addr else {
        return Ok(());
    };

    let srv = UdpSocket::bind(udp_addr)?;
    srv.set_read_timeout(Some(POLL_INTERVAL.to_std()))?;

    let magic_string = "9#B8D2Nvg2xg5P$ZRwJ+f)*^Nne6*W3WamGY";

    let to = parse_ip(node_ip).ok_or_else(|| Error::InvalidIp(node_ip.to_string()))?;
    let local: IpAddr = if to.is_ipv4() {
        Ipv4Addr::UNSPECIFIED.into()
    } else {
        Ipv6Addr::UNSPECIFIED.into()
    };
    let cli = UdpSocket::bind(SocketAddr::new(local, 0))?;
    cli.connect(SocketAddr::new(to, srv.local_addr()?.port()))?;

    cli.send(magic_string.as_bytes())?;

    let ctx1 = ctx.with_timeout(VALIDATION_TIMEOUT);
    let mut buf = [0u8; 1024];
    let mut reading = true;
    loop {
        if let Some(err) = ctx1.err() {
            warnw!(logger::get_logger(), "could not validate external IP", err, "ip" => node_ip, "from" => addr);
            return Err(err);
        }
        if !reading {
            std::thread::sleep(POLL_INTERVAL.to_std());
            continue;
        }
        match srv.recv(&mut buf) {
            Ok(n) if &buf[..n] == magic_string.as_bytes() => return Ok(()),
            Ok(_) => {}
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(err) => {
                debugw!(logger::get_logger(), "error reading from UDP socket", "err" => err);
                reading = false;
            }
        }
    }
}

#[cfg(test)]
#[path = "ip_test.rs"]
pub(super) mod ip_test;
