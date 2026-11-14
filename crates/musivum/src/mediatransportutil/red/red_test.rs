// Ported from LiveKit's mediatransportutil, pkg/red/red_test.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks and loops that carry their
// names in the assertion messages; `require.ErrorIs` is a comparison with the error.

use bytes::Bytes;

use super::*;

const TS_STEP: u32 = 48000 / 1000 * 10;
const RED_PT: u8 = 63;

// one redundant block (PT 111, ts offset 960, length 1) then primary PT 111
const GOLDEN_PAYLOAD: [u8; 8] = [0xEF, 0x0F, 0x00, 0x01, 0x6F, 0xCC, 0xAA, 0xBB];

#[test]
fn test_extract_packets() {
    {
        let name = "golden";
        let pkt = rtp::Packet {
            header: rtp::Header {
                sequence_number: 100,
                timestamp: 10000,
                payload_type: RED_PT,
                ..Default::default()
            },
            payload: Bytes::from_static(&GOLDEN_PAYLOAD),
            ..Default::default()
        };
        let pkts = extract_packets(&pkt, 0xFF);
        assert!(pkts.is_ok(), "{name}: {pkts:?}");
        require_pkts_equal(
            name,
            &[
                rtp::Packet {
                    header: rtp::Header {
                        sequence_number: 99,
                        timestamp: 9040,
                        payload_type: 111,
                        ..Default::default()
                    },
                    payload: Bytes::from_static(&[0xCC]),
                    ..Default::default()
                },
                rtp::Packet {
                    header: rtp::Header {
                        sequence_number: 100,
                        timestamp: 10000,
                        payload_type: 111,
                        ..Default::default()
                    },
                    payload: Bytes::from_static(&[0xAA, 0xBB]),
                    ..Default::default()
                },
            ],
            &pkts.unwrap(),
        );
    }

    let header = rtp::Header {
        sequence_number: 65534,
        timestamp: (1u32 << 31) - 2 * TS_STEP,
        payload_type: 111,
        ..Default::default()
    };
    let primaries = generate_pkts(header, 3, TS_STEP);
    let red_pkt = generate_red_pkts(&primaries, 2)[2].clone();

    struct TestCase {
        name: &'static str,
        recover_bits: u8,
        want: Vec<rtp::Packet>,
    }
    for tc in [
        TestCase {
            name: "all",
            recover_bits: 0xFF,
            want: primaries.clone(),
        },
        TestCase {
            name: "none",
            recover_bits: 0,
            want: primaries[2..].to_vec(),
        },
        TestCase {
            name: "newest redundant",
            recover_bits: 0b01,
            want: primaries[1..].to_vec(),
        },
        TestCase {
            name: "oldest redundant",
            recover_bits: 0b10,
            want: vec![primaries[0].clone(), primaries[2].clone()],
        },
    ] {
        let pkts = extract_packets(&red_pkt, tc.recover_bits);
        assert!(pkts.is_ok(), "{}: {pkts:?}", tc.name);
        require_pkts_equal(tc.name, &tc.want, &pkts.unwrap());
    }
}

#[test]
fn test_extract_primary_encoding() {
    let header = rtp::Header {
        sequence_number: 65530,
        timestamp: (1u32 << 31) - 2 * TS_STEP,
        payload_type: 111,
        ..Default::default()
    };
    let pkts = generate_pkts(header, 10, TS_STEP);

    let mut primary_pkts = Vec::with_capacity(pkts.len());
    for red_pkt in generate_red_pkts(&pkts, 2) {
        let payload = extract_primary_encoding(&red_pkt.payload);
        assert!(payload.is_ok(), "{payload:?}");
        let mut primary_header = red_pkt.header.clone();
        primary_header.payload_type = 111;
        primary_pkts.push(rtp::Packet {
            header: primary_header,
            payload: red_pkt.payload.slice_ref(payload.unwrap()),
            ..Default::default()
        });
    }
    require_pkts_equal("", &pkts, &primary_pkts);

    let payload = extract_primary_encoding(&GOLDEN_PAYLOAD);
    assert_eq!(Ok(&[0xAA, 0xBB][..]), payload);
}

#[test]
fn test_depacketizer() {
    let (mut in_order, mut duplicates, mut every_third) = (Vec::new(), Vec::new(), Vec::new());
    for i in 0..19usize {
        in_order.push(i);
        duplicates.push(i);
        if i > 0 {
            duplicates.push(i - 1);
        }
        duplicates.push(i);
        if i % 3 == 0 {
            every_third.push(i);
        }
    }

    // indices into a stream starting at seq 65530, so index 6 is seq 0
    struct TestCase {
        name: &'static str,
        red_count: usize,
        send: Vec<usize>,
        want: Vec<usize>,
    }
    for tc in [
        TestCase {
            name: "in order",
            red_count: 2,
            send: in_order.clone(),
            want: in_order.clone(),
        },
        TestCase {
            name: "duplicate and out of order",
            red_count: 2,
            send: duplicates.clone(),
            want: duplicates.clone(),
        },
        TestCase {
            name: "full recover",
            red_count: 2,
            send: every_third.clone(),
            want: in_order.clone(),
        },
        TestCase {
            name: "lost 2, redundancy 1",
            red_count: 1,
            send: vec![0, 3, 6, 9, 12],
            want: vec![0, 2, 3, 5, 6, 8, 9, 11, 12],
        },
        TestCase {
            name: "partial recover and long jump",
            red_count: 2,
            send: vec![0, 5, 12, 21, 24, 27],
            want: vec![0, 3, 4, 5, 10, 11, 12, 19, 20, 21, 22, 23, 24, 25, 26, 27],
        },
        TestCase {
            name: "out of order",
            red_count: 2,
            send: vec![20, 10, 25, 23, 34],
            want: vec![20, 10, 23, 24, 25, 21, 22, 23, 32, 33, 34],
        },
        TestCase {
            name: "gap of 1",
            red_count: 2,
            send: vec![0, 1, 3],
            want: vec![0, 1, 2, 3],
        },
        TestCase {
            name: "gap of 3",
            red_count: 2,
            send: vec![0, 4],
            want: vec![0, 2, 3, 4],
        },
        TestCase {
            name: "jump over 8",
            red_count: 2,
            send: vec![0, 1, 11],
            want: vec![0, 1, 9, 10, 11],
        },
        // 7 behind recovers one block, 8 behind is in history but has no room, 9 behind is outside it
        TestCase {
            name: "history window edge",
            red_count: 2,
            send: vec![20, 13, 12, 11],
            want: vec![20, 12, 13, 12, 11],
        },
        TestCase {
            name: "wraparound",
            red_count: 2,
            send: vec![0, 1, 2, 3, 4, 5, 7],
            want: vec![0, 1, 2, 3, 4, 5, 6, 7],
        },
    ] {
        let header = rtp::Header {
            sequence_number: 65530,
            timestamp: (1u32 << 31) - 2 * TS_STEP,
            payload_type: 111,
            ..Default::default()
        };
        let primaries = generate_pkts(header, 50, TS_STEP);
        let red_pkts = generate_red_pkts(&primaries, tc.red_count);

        let mut d = Depacketizer::default();
        let mut got = Vec::new();
        for &i in &tc.send {
            let pkts = d.depacketize(&red_pkts[i]);
            assert!(pkts.is_ok(), "{}: {pkts:?}", tc.name);
            got.extend(pkts.unwrap());
        }

        let mut want = Vec::with_capacity(tc.want.len());
        for &i in &tc.want {
            want.push(primaries[i].clone());
        }
        require_pkts_equal(tc.name, &want, &got);
    }
}

#[test]
fn test_truncated() {
    struct TestCase {
        name: &'static str,
        payload: &'static [u8],
        err: Error,
    }
    for tc in [
        TestCase {
            name: "empty",
            payload: &[],
            err: Error::IncompleteHeader,
        },
        TestCase {
            name: "short redundant header",
            payload: &[0x80, 0, 0],
            err: Error::IncompleteHeader,
        },
        TestCase {
            name: "no primary header",
            payload: &[0xEF, 0x0F, 0x00, 0x01],
            err: Error::IncompleteHeader,
        },
        TestCase {
            name: "short block",
            payload: &[0xEF, 0x0F, 0x00, 0x05, 0x6F, 0xCC],
            err: Error::IncompleteBlock,
        },
    ] {
        let err = extract_primary_encoding(tc.payload).err();
        assert_eq!(Some(tc.err), err, "{}", tc.name);

        let pkt = rtp::Packet {
            payload: Bytes::from_static(tc.payload),
            ..Default::default()
        };
        let err = extract_packets(&pkt, 0xFF).err();
        assert_eq!(Some(tc.err), err, "{}", tc.name);

        let err = Depacketizer::default().depacketize(&pkt).err();
        assert_eq!(Some(tc.err), err, "{}", tc.name);
    }
}

fn generate_pkts(mut header: rtp::Header, count: usize, ts_step: u32) -> Vec<rtp::Packet> {
    let mut pkts = Vec::with_capacity(count);
    for _ in 0..count {
        let hbuf = header.marshal().unwrap_or_default();
        pkts.push(rtp::Packet {
            header: header.clone(),
            payload: Bytes::from(hbuf),
            ..Default::default()
        });
        header.sequence_number = header.sequence_number.wrapping_add(1);
        header.timestamp = header.timestamp.wrapping_add(ts_step);
    }
    pkts
}

fn generate_red_pkts(pkts: &[rtp::Packet], red_count: usize) -> Vec<rtp::Packet> {
    let mut red_pkts = Vec::with_capacity(pkts.len());
    for (i, pkt) in pkts.iter().enumerate() {
        let mut red_pkt = pkt.clone();
        red_pkt.payload_type = RED_PT;
        // max(0, i-redCount)
        red_pkt.payload = Bytes::from(encode_red(&pkts[i.saturating_sub(red_count)..i], pkt));
        red_pkts.push(red_pkt);
    }
    red_pkts
}

fn encode_red(redundant: &[rtp::Packet], primary: &rtp::Packet) -> Vec<u8> {
    let mut out = Vec::new();
    for p in redundant {
        out.extend_from_slice(
            &(((0x80 | p.payload_type as u32) << 24)
                | ((primary.timestamp.wrapping_sub(p.timestamp) & 0x3FFF) << 10)
                | (p.payload.len() as u32 & 0x3FF))
                .to_be_bytes(),
        );
    }
    out.push(primary.payload_type);
    for p in redundant {
        out.extend_from_slice(&p.payload);
    }
    out.extend_from_slice(&primary.payload);
    out
}

#[track_caller]
fn require_pkts_equal(name: &str, want: &[rtp::Packet], got: &[rtp::Packet]) {
    assert_eq!(want.len(), got.len(), "{name}");
    for i in 0..want.len() {
        assert_eq!(
            want[i].sequence_number, got[i].sequence_number,
            "{name}: packet {i}"
        );
        assert_eq!(want[i].timestamp, got[i].timestamp, "{name}: packet {i}");
        assert_eq!(
            want[i].payload_type, got[i].payload_type,
            "{name}: packet {i}"
        );
        assert_eq!(want[i].payload, got[i].payload, "{name}: packet {i}");
    }
}
