// Ported from pion/turn, internal/allocation/five_tuple.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The addresses may be nil as in Go (`Option`); a nil
// address has the zero fingerprint, as there.

use std::fmt;
use std::net::IpAddr;

use crate::webrtc::turn::net::Addr;

// Protocol is an enum for relay protocol.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub enum Protocol {
    // Network protocols for relay.
    #[default]
    Udp = 0,
    Tcp = 1,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Protocol::Udp => f.write_str("UDP"),
            Protocol::Tcp => f.write_str("TCP"),
        }
    }
}

// FiveTuple is the combination (client IP address and port, server IP
// address and port, and transport protocol (currently one of UDP,
// TCP, or TLS)) used to communicate between the client and the
// server.  The 5-tuple uniquely identifies this communication
// stream.  The 5-tuple also uniquely identifies the Allocation on
// the server.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct FiveTuple {
    pub protocol: Protocol,
    pub src_addr: Option<Addr>,
    pub dst_addr: Option<Addr>,
}

impl fmt::Debug for FiveTuple {
    // Go's `%v` of the struct
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let addr = |a: Option<Addr>| a.map_or("<nil>".to_string(), |a| a.to_string());
        write!(
            f,
            "&{{{} {} {}}}",
            self.protocol as u8,
            addr(self.src_addr),
            addr(self.dst_addr)
        )
    }
}

impl fmt::Display for FiveTuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

// FiveTupleFingerprint is a comparable representation of a FiveTuple.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct FiveTupleFingerprint {
    src_ip: [u8; 16],
    dst_ip: [u8; 16],
    src_port: u16,
    dst_port: u16,
    protocol: Protocol,
}

impl FiveTuple {
    // Equal asserts if two FiveTuples are equal.
    pub fn equal(&self, b: &FiveTuple) -> bool {
        self.fingerprint() == b.fingerprint()
    }

    // Fingerprint is the identity of a FiveTuple.
    pub fn fingerprint(&self) -> FiveTupleFingerprint {
        let mut fp = FiveTupleFingerprint::default();
        let (src_ip, src_port) = net_addr_ip_and_port(self.src_addr);
        fp.src_ip = src_ip;
        fp.src_port = src_port;
        let (dst_ip, dst_port) = net_addr_ip_and_port(self.dst_addr);
        fp.dst_ip = dst_ip;
        fp.dst_port = dst_port;
        fp.protocol = self.protocol;

        fp
    }
}

// Go's `IP.To16()` and the port; nothing for a nil address.
fn net_addr_ip_and_port(addr: Option<Addr>) -> ([u8; 16], u16) {
    match addr {
        Some(a) => {
            let ip = match a.ip() {
                IpAddr::V4(ip) => ip.to_ipv6_mapped(),
                IpAddr::V6(ip) => ip,
            };
            (ip.octets(), a.port())
        }
        None => ([0; 16], 0),
    }
}

#[cfg(test)]
#[path = "five_tuple_test.rs"]
mod five_tuple_test;
