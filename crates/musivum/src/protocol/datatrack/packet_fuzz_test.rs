// Ported from LiveKit's protocol, datatrack/packet_fuzz_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The original is a Go fuzz test. What is here is what a
// plain `go test` runs of it: the property, over the seed corpus. The fuzzing itself, the
// mutation of the seeds by `go test -fuzz`, is not carried over.

use bytes::Bytes;

use crate::go::time::MILLISECOND;
use crate::protocol::datatrack;
use crate::protocol::datatrack::datatracktest;

// Mutation-based fuzzing for packet deserialization.
//
// A plain `go test` only exercises the seed corpus below.
#[test]
fn fuzz_packet_unmarshal() {
    let mut seeds: Vec<Vec<u8>> = Vec::new();

    // Fixed vectors from packet_test.go: no extensions, participant SID
    // extension, and extension with padding.
    seeds.push(vec![
        0x18, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0xff, 0xfe, 0xfd,
        0xfc, 0xfb, 0xfa,
    ]);
    seeds.push(vec![
        0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x04, 0x01,
        0x10, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61,
        0x6e, 0x74, 0xff, 0xfe, 0xfd, 0xfc,
    ]);
    seeds.push(vec![
        0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x03, 0x01,
        0x0b, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61, 0x6e, 0x74, 0x00, 0xff, 0xfe,
        0xfd, 0xfc,
    ]);

    // Generated multi-packet frames with the participant SID extension.
    for raw in datatracktest::generate_raw_data_packets(1, 1, 1, 4, 600, 10 * MILLISECOND) {
        seeds.push(raw);
    }
    // New for musivum: the corpus is the 3 fixed vectors and 4 frames of 600 bytes in packets
    // of at most 256 (the generator leaves out a packet that does not marshal, silently).
    assert_eq!(3 + 4 * 3, seeds.len());

    for data in seeds {
        let data = Bytes::from(data);
        let mut p = datatrack::Packet::default();
        if p.unmarshal(&data).is_err() {
            // Malformed input must be rejected with an error, never a panic.
            continue;
        }

        // Round-trip stability: a packet that parsed must marshal, and the
        // re-parsed result must be structurally identical.
        let buf = p.marshal();
        assert!(buf.is_ok(), "{data:?}: {buf:?}");

        let mut p2 = datatrack::Packet::default();
        assert_eq!(Ok(()), p2.unmarshal(&Bytes::from(buf.unwrap())), "{data:?}");
        assert_eq!(p, p2, "{data:?}");
    }
}
