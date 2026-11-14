// Ported from pion/turn, internal/ipnet/util.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. A `net.Addr` is `net::Addr`, which is always a UDP or a
// TCP address, so `AddrIPPort` cannot fail and `errFailedToCastAddr` is gone; `AddrEqual` of
// a UDP and a TCP address is false, as there. IP addresses are compared and written as Go's
// `net.IP` does: an IPv4 address in an IPv6 one is the IPv4 address.

// Package ipnet contains helper functions around net and IP

use std::net::IpAddr;

use crate::webrtc::turn::net::Addr;

// AddrIPPort extracts the IP and Port from a net.Addr.
pub fn addr_ip_port(a: Addr) -> (IpAddr, u16) {
    (a.ip(), a.port())
}

// AddrEqual asserts that two net.Addrs are equal.
// Compares IP and Port and intentionally ignores IPv6 Zone.
pub fn addr_equal(addr_a: Addr, addr_b: Addr) -> bool {
    match (addr_a, addr_b) {
        (Addr::Udp(a), Addr::Udp(b)) | (Addr::Tcp(a), Addr::Tcp(b)) => {
            a.ip().to_canonical() == b.ip().to_canonical() && a.port() == b.port()
        }
        _ => false,
    }
}

// FingerprintAddr generates a fingerprint from net.UDPAddr or net.TCPAddr's
// which can be used for indexing maps.
pub fn fingerprint_addr(addr: Addr) -> String {
    addr.ip().to_canonical().to_string()
}
