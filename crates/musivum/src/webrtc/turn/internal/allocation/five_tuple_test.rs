// Ported from pion/turn, internal/allocation/five_tuple_test.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust.

use super::*;

#[test]
fn test_five_tuple_protocol() {
    let udp_expect = 0u8;
    let tcp_expect = 1u8;
    assert_eq!(Protocol::Udp as u8, udp_expect);
    assert_eq!(Protocol::Tcp as u8, tcp_expect);
}

#[test]
fn test_five_tuple_equal() {
    let src_addr1 = Some(Addr::Udp("0.0.0.0:3478".parse().unwrap()));
    let src_addr2 = Some(Addr::Udp("0.0.0.0:3479".parse().unwrap()));

    let dst_addr1 = Some(Addr::Udp("0.0.0.0:3480".parse().unwrap()));
    let dst_addr2 = Some(Addr::Udp("0.0.0.0:3481".parse().unwrap()));

    let ft = |protocol, src_addr, dst_addr| FiveTuple {
        protocol,
        src_addr,
        dst_addr,
    };
    let tt = [
        (
            "Equal",
            true,
            ft(Protocol::Udp, src_addr1, dst_addr1),
            ft(Protocol::Udp, src_addr1, dst_addr1),
        ),
        (
            "DifferentProtocol",
            false,
            ft(Protocol::Udp, src_addr1, dst_addr1),
            ft(Protocol::Tcp, src_addr1, dst_addr1),
        ),
        (
            "DifferentSrcAddr",
            false,
            ft(Protocol::Udp, src_addr1, dst_addr1),
            ft(Protocol::Udp, src_addr2, dst_addr1),
        ),
        (
            "DifferentDstAddr",
            false,
            ft(Protocol::Udp, src_addr1, dst_addr1),
            ft(Protocol::Udp, src_addr1, dst_addr2),
        ),
    ];

    for (name, expect, a, b) in tt {
        let fact = a.equal(&b);
        assert_eq!(
            expect, fact,
            "{name}: {a:?}, {b:?} equal check should be {expect}, but {fact}"
        );
    }
}
