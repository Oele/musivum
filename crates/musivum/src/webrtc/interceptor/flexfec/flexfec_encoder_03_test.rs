// Ported from pion/interceptor, pkg/flexfec/flexfec_encoder_03_test.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; `assert.Nil` and `assert.NotNil` on the list of FEC
// packets are "empty" and "not empty", since the port has an empty list where the original
// has nil.

use bytes::Bytes;

use super::*;

#[test]
fn test_flex_encoder03_encode_fec_empty_media_packets() {
    let mut encoder = FlexEncoder03Factory {}.new_encoder(96, 1234);
    let media_packets: Vec<rtp::Packet> = Vec::new();

    let result = encoder.encode_fec(&media_packets, 1);

    assert!(
        result.is_empty(),
        "EncodeFec should return nil when mediaPackets is empty"
    );
}

#[test]
fn test_flex_encoder03_encode_fec_out_of_order_packets() {
    let mut encoder = FlexEncoder03Factory {}.new_encoder(96, 1234);
    let media_packets = vec![
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 2,
                ssrc: 1234,
                ..Default::default()
            },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 1, // Out of order (should be 3)
                ssrc: 1234,
                ..Default::default()
            },
            ..Default::default()
        },
    ];

    let result = encoder.encode_fec(&media_packets, 1);

    assert!(
        result.is_empty(),
        "EncodeFec should return nil when packets are out of order"
    );
}

#[test]
fn test_flex_encoder03_encode_fec_missing_packets() {
    let mut encoder = FlexEncoder03Factory {}.new_encoder(96, 1234);
    let media_packets = vec![
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 1,
                ssrc: 1234,
                ..Default::default()
            },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 3, // Missing packet with sequence number 2
                ssrc: 1234,
                ..Default::default()
            },
            ..Default::default()
        },
    ];

    let result = encoder.encode_fec(&media_packets, 1);
    assert!(
        result.is_empty(),
        "EncodeFec should return nil when there are missing packets"
    );
}

#[test]
fn test_flex_encoder03_encode_fec_different_payload_sizes() {
    let mut encoder = FlexEncoder03Factory {}.new_encoder(96, 1234);

    let small_payload = Bytes::from_static(&[1, 2, 3]);
    let large_payload = Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8]);

    let media_packets = vec![
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 1,
                ssrc: 1234,
                ..Default::default()
            },
            payload: small_payload,
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 2,
                ssrc: 1234,
                ..Default::default()
            },
            payload: large_payload.clone(),
            ..Default::default()
        },
    ];

    let fec_packets = encoder.encode_fec(&media_packets, 1);

    assert!(
        !fec_packets.is_empty(),
        "EncodeFec should return FEC packets"
    );
    assert_eq!(1, fec_packets.len(), "EncodeFec should return 1 FEC packet");

    let expected_payload_size = large_payload.len();
    let actual_payload_size = fec_packets[0].payload.len() - BASE_FEC03_HEADER_SIZE;

    assert_eq!(
        expected_payload_size, actual_payload_size,
        "FEC payload size should match the size of the largest media packet payload"
    );
}
