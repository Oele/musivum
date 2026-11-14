// Ported from pion/rtp, packet_test.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The two tests of `Clone` change the original packet by
// replacing its bytes, since `Bytes` cannot be written into as the original's slices are. The
// four benchmarks are left out. The tests after `test_marshal_to_panic` are new and say so.

use super::*;

#[test]
fn test_basic() {
    let mut packet = Packet::default();

    assert!(
        packet.unmarshal(&Bytes::new()).is_err(),
        "Unmarshal did not error on zero length packet"
    );
    assert!(matches!(
        packet.unmarshal(&Bytes::new()),
        Err(Error::HeaderSizeInsufficient { .. })
    ));

    let raw_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x00, 0x01, 0x00,
        0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let parsed_packet = Packet {
        header: Header {
            padding: false,
            marker: true,
            extension: true,
            extension_profile: 1,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            padding_size: 0,
            ..Default::default()
        },
        payload: raw_pkt.slice(20..),
        ..Default::default()
    };

    // Unmarshal to the used Packet should work as well.
    for i in 0..2 {
        let run = format!("Run{}", i + 1);
        assert_eq!(Ok(()), packet.unmarshal(&raw_pkt), "{run}");
        assert_eq!(packet, parsed_packet, "{run}");

        assert_eq!(
            packet.header.marshal_size(),
            20,
            "{run}: wrong computed header marshal size"
        );
        assert_eq!(
            packet.marshal_size(),
            raw_pkt.len(),
            "{run}: wrong computed marshal size"
        );

        let raw = packet.marshal();
        assert!(raw.is_ok(), "{run}");
        assert_eq!(raw_pkt, raw.unwrap(), "{run}");
    }

    // packet with padding
    let raw_pkt = Bytes::from_static(&[
        0xb0, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x00, 0x01, 0x00,
        0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x98, 0x36, 0xbe, 0x88, 0x04,
    ]);
    let parsed_packet = Packet {
        header: Header {
            padding: true,
            marker: true,
            extension: true,
            extension_profile: 1,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            padding_size: 4,
            ..Default::default()
        },
        payload: raw_pkt.slice(20..21),
        padding_size: 4,
        ..Default::default()
    };
    assert_eq!(Ok(()), packet.unmarshal(&raw_pkt));
    assert_eq!(packet, parsed_packet);

    // packet with zero padding following packet with non-zero padding
    let raw_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x00, 0x01, 0x00,
        0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let parsed_packet = Packet {
        header: Header {
            padding: false,
            marker: true,
            extension: true,
            extension_profile: 1,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            padding_size: 0,
            ..Default::default()
        },
        payload: raw_pkt.slice(20..),
        ..Default::default()
    };
    assert_eq!(Ok(()), packet.unmarshal(&raw_pkt));
    assert_eq!(packet, parsed_packet);

    // packet with only padding
    let raw_pkt = Bytes::from_static(&[
        0xb0, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x00, 0x01, 0x00,
        0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x98, 0x36, 0xbe, 0x88, 0x05,
    ]);
    let parsed_packet = Packet {
        header: Header {
            padding: true,
            marker: true,
            extension: true,
            extension_profile: 1,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            padding_size: 5,
            ..Default::default()
        },
        payload: Bytes::new(),
        padding_size: 5,
        ..Default::default()
    };
    assert_eq!(Ok(()), packet.unmarshal(&raw_pkt));
    assert_eq!(packet, parsed_packet);
    assert_eq!(
        0,
        packet.payload.len(),
        "Unmarshal of padding only packet has payload of non-zero length"
    );

    // packet with padding bit set but zero padding byte
    let raw_pkt = Bytes::from_static(&[
        0xb0, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x00, 0x01, 0x00,
        0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x98, 0x36, 0xbe, 0x88, 0x00,
    ]);
    let err = packet.unmarshal(&raw_pkt);
    assert_eq!(Err(Error::InvalidRtpPadding), err);

    // packet with excessive padding
    let raw_pkt = Bytes::from_static(&[
        0xb0, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x00, 0x01, 0x00,
        0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x98, 0x36, 0xbe, 0x88, 0x06,
    ]);
    // The original builds the packet it would expect here and does not compare with it.
    let _parsed_packet = Packet {
        header: Header {
            padding: true,
            marker: true,
            extension: true,
            extension_profile: 1,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            padding_size: 0,
            ..Default::default()
        },
        payload: Bytes::new(),
        ..Default::default()
    };
    let err = packet.unmarshal(&raw_pkt);
    assert!(
        err.is_err(),
        "Unmarshal did not error on packet with excessive padding"
    );
    assert_eq!(Err(Error::TooSmall), err);

    // marshal packet with padding
    let raw_pkt = Bytes::from_static(&[
        0xb0, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x00, 0x01, 0x00,
        0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x98, 0x00, 0x00, 0x00, 0x04,
    ]);
    let parsed_packet = Packet {
        header: Header {
            padding: true,
            marker: true,
            extension: true,
            extension_profile: 1,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            padding_size: 4,
            ..Default::default()
        },
        payload: raw_pkt.slice(20..21),
        ..Default::default()
    };
    let buf = parsed_packet.marshal();
    assert!(buf.is_ok());
    assert_eq!(raw_pkt, buf.unwrap());

    // marshal packet with padding only
    let raw_pkt = Bytes::from_static(&[
        0xb0, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x00, 0x01, 0x00,
        0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x05,
    ]);
    let parsed_packet = Packet {
        header: Header {
            padding: true,
            marker: true,
            extension: true,
            extension_profile: 1,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            padding_size: 5,
            ..Default::default()
        },
        payload: Bytes::new(),
        ..Default::default()
    };
    let buf = parsed_packet.marshal();
    assert!(buf.is_ok());
    assert_eq!(raw_pkt, buf.unwrap());

    // marshal packet with padding only without setting Padding explicitly in Header
    let raw_pkt = Bytes::from_static(&[
        0xb0, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x00, 0x01, 0x00,
        0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x05,
    ]);
    let parsed_packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 1,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
            }],
            version: 2,
            padding: true,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            padding_size: 5,
            ..Default::default()
        },
        payload: Bytes::new(),
        ..Default::default()
    };
    let buf = parsed_packet.marshal();
    assert!(buf.is_ok());
    assert_eq!(raw_pkt, buf.unwrap());

    // marshal packet after deleting extensions
    let raw_pkt = Bytes::from_static(&[
        0xb0, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0xBE, 0xDE, 0x00,
        0x01, 0x10, 0xAA, 0x00, 0x00, 0x98, 0x00, 0x00, 0x00, 0x04,
    ]);
    let mut parsed_packet = Packet {
        header: Header {
            padding: true,
            marker: true,
            extension: true,
            extension_profile: 0xBEDE,
            extensions: vec![Extension {
                id: 1,
                payload: Bytes::from_static(&[0xAA]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            padding_size: 4,
            ..Default::default()
        },
        payload: raw_pkt.slice(20..21),
        ..Default::default()
    };
    let buf = parsed_packet.marshal();
    assert!(buf.is_ok());
    assert_eq!(raw_pkt, buf.unwrap());
    assert_eq!(
        Ok(()),
        parsed_packet.del_extension(1),
        "Should successfully delete extension"
    );
    // should not include extensions and X bit should be unset after deleting all extensions
    let raw_pkt = Bytes::from_static(&[
        0xa0, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x98, 0x00, 0x00,
        0x00, 0x04,
    ]);
    let buf = parsed_packet.marshal();
    assert!(buf.is_ok());
    assert_eq!(raw_pkt, buf.unwrap());
}

#[test]
fn test_extension() {
    let mut packet = Packet::default();

    let missing_extension_pkt = Bytes::from_static(&[
        0x90, 0x60, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82,
    ]);
    assert!(
        packet.unmarshal(&missing_extension_pkt).is_err(),
        "Unmarshal did not error on packet with missing extension data"
    );

    let invalid_extension_length_pkt = Bytes::from_static(&[
        0x90, 0x60, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x99, 0x99, 0x99,
        0x99,
    ]);
    assert!(
        packet.unmarshal(&invalid_extension_length_pkt).is_err(),
        "Unmarshal did not error on packet with invalid extension length"
    );

    let packet = Packet {
        header: Header {
            extension: true,
            extension_profile: 3,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0]),
            }],
            ..Default::default()
        },
        payload: Bytes::new(),
        ..Default::default()
    };
    let err = packet.marshal();
    assert!(
        err.is_err(),
        "Marshal did not error on packet with invalid extension length"
    );
}

#[test]
fn test_rfc8285_one_byte_extension() {
    let mut packet = Packet::default();

    let raw_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0xBE, 0xDE, 0x00,
        0x01, 0x50, 0xAA, 0x00, 0x00, 0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    assert_eq!(Ok(()), packet.unmarshal(&raw_pkt));

    let packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0xBEDE,
            extensions: vec![Extension {
                id: 5,
                payload: Bytes::from_static(&[0xAA]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload: raw_pkt.slice(20..),
        ..Default::default()
    };

    let dst_data = packet.marshal().unwrap_or_default();
    assert_eq!(raw_pkt, dst_data);
}

#[test]
fn test_rfc8285_one_byte_two_extension_of_two_bytes() {
    let mut packet = Packet::default();

    //  0                   1                   2                   3
    //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |       0xBE    |    0xDE       |           length=1            |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |  ID   | L=0   |     data      |  ID   |  L=0  |   data...
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    let raw_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0xBE, 0xDE, 0x00,
        0x01, 0x10, 0xAA, 0x20, 0xBB, // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    assert_eq!(Ok(()), packet.unmarshal(&raw_pkt));

    let ext1 = packet.get_extension(1);
    let ext1_expect = Bytes::from_static(&[0xAA]);
    assert_eq!(Some(ext1_expect), ext1, "Extension has incorrect data");

    let ext2 = packet.get_extension(2);
    let ext2_expect = Bytes::from_static(&[0xBB]);
    assert_eq!(Some(ext2_expect), ext2, "Extension has incorrect data");

    // Test Marshal
    let packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0xBEDE,
            extensions: vec![
                Extension {
                    id: 1,
                    payload: Bytes::from_static(&[0xAA]),
                },
                Extension {
                    id: 2,
                    payload: Bytes::from_static(&[0xBB]),
                },
            ],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload: raw_pkt.slice(20..),
        ..Default::default()
    };

    let dst_data = packet.marshal().unwrap_or_default();
    assert_eq!(raw_pkt, dst_data);
}

#[test]
fn test_rfc8285_one_byte_multiple_extensions_with_padding() {
    let mut packet = Packet::default();

    //  0                   1                   2                   3
    //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |       0xBE    |    0xDE       |           length=3            |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |  ID   | L=0   |     data      |  ID   |  L=1  |   data...
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //       ...data   |    0 (pad)    |    0 (pad)    |  ID   | L=3   |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                          data                                 |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    let raw_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0xBE, 0xDE, 0x00,
        0x03, 0x10, 0xAA, 0x21, 0xBB, 0xBB, 0x00, 0x00, 0x33, 0xCC, 0xCC, 0xCC, 0xCC,
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    assert_eq!(Ok(()), packet.unmarshal(&raw_pkt));

    let ext1 = packet.get_extension(1);
    let ext1_expect = Bytes::from_static(&[0xAA]);
    assert_eq!(Some(ext1_expect), ext1, "Extension has incorrect data");

    let ext2 = packet.get_extension(2);
    let ext2_expect = Bytes::from_static(&[0xBB, 0xBB]);
    assert_eq!(Some(ext2_expect), ext2, "Extension has incorrect data");

    let ext3 = packet.get_extension(3);
    let ext3_expect = Bytes::from_static(&[0xCC, 0xCC, 0xCC, 0xCC]);
    assert_eq!(Some(ext3_expect), ext3, "Extension has incorrect data");

    let raw_pkt_re_marshal: &[u8] = &[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0xBE, 0xDE, 0x00,
        0x03, 0x10, 0xAA, 0x21, 0xBB, 0xBB, 0x33, 0xCC, 0xCC, 0xCC, 0xCC, 0x00,
        0x00, // padding is moved to the end by re-marshaling
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ];
    let dst_buf = [
        ("CleanBuffer", vec![0u8; 1000]),
        ("DirtyBuffer", vec![0xFFu8; 1000]),
    ];
    for (name, mut buf) in dst_buf {
        let n = packet.marshal_to(&mut buf);
        assert!(n.is_ok(), "{name}");
        assert_eq!(raw_pkt_re_marshal, &buf[..n.unwrap()], "{name}");
    }
}

#[test]
fn test_rfc8285_one_byte_multiple_extensions() {
    //  0                   1                   2                   3
    //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |       0xBE    |    0xDE       |           length=3            |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |  ID=1 | L=0   |     data      |  ID=2 |  L=1  |   data...
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //       ...data   |  ID=3 | L=3   |           data...
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //             ...data             |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    let raw_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0xBE, 0xDE, 0x00,
        0x03, 0x10, 0xAA, 0x21, 0xBB, 0xBB, 0x33, 0xCC, 0xCC, 0xCC, 0xCC, 0x00, 0x00,
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);

    let packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0xBEDE,
            extensions: vec![
                Extension {
                    id: 1,
                    payload: Bytes::from_static(&[0xAA]),
                },
                Extension {
                    id: 2,
                    payload: Bytes::from_static(&[0xBB, 0xBB]),
                },
                Extension {
                    id: 3,
                    payload: Bytes::from_static(&[0xCC, 0xCC, 0xCC, 0xCC]),
                },
            ],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload: raw_pkt.slice(28..),
        ..Default::default()
    };

    let dst_data = packet.marshal().unwrap_or_default();
    assert_eq!(raw_pkt, dst_data);
}

#[test]
fn test_rfc8285_two_byte_extension() {
    let mut packet = Packet::default();

    let raw_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x10, 0x00, 0x00,
        0x07, 0x05, 0x18, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
        0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0x00, 0x00, 0x98,
        0x36, 0xbe, 0x88, 0x9e,
    ]);
    assert_eq!(Ok(()), packet.unmarshal(&raw_pkt));

    let packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0x1000,
            extensions: vec![Extension {
                id: 5,
                payload: Bytes::from_static(&[
                    0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
                    0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
                ]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload: raw_pkt.slice(44..),
        ..Default::default()
    };

    let dst_data = packet.marshal().unwrap_or_default();
    assert_eq!(raw_pkt, dst_data);
}

#[test]
fn test_rfc8285_two_byte_multiple_extensions_with_padding() {
    let mut packet = Packet::default();

    // 0                   1                   2                   3
    // 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |       0x10    |    0x00       |           length=3            |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |      ID=1     |     L=0       |     ID=2      |     L=1       |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |       data    |    0 (pad)    |       ID=3    |      L=4      |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                          data                                 |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    let raw_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x10, 0x00, 0x00,
        0x03, 0x01, 0x00, 0x02, 0x01, 0xBB, 0x00, 0x03, 0x04, 0xCC, 0xCC, 0xCC, 0xCC, 0x98, 0x36,
        0xbe, 0x88, 0x9e,
    ]);
    assert_eq!(Ok(()), packet.unmarshal(&raw_pkt));

    let ext1 = packet.get_extension(1);
    let ext1_expect = Bytes::from_static(&[]);
    assert_eq!(Some(ext1_expect), ext1, "Extension has incorrect data");

    let ext2 = packet.get_extension(2);
    let ext2_expect = Bytes::from_static(&[0xBB]);
    assert_eq!(Some(ext2_expect), ext2, "Extension has incorrect data");

    let ext3 = packet.get_extension(3);
    let ext3_expect = Bytes::from_static(&[0xCC, 0xCC, 0xCC, 0xCC]);
    assert_eq!(Some(ext3_expect), ext3, "Extension has incorrect data");

    let raw_pkt_re_marshal: &[u8] = &[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x10, 0x00, 0x00,
        0x03, 0x01, 0x00, 0x02, 0x01, 0xBB, 0x03, 0x04, 0xCC, 0xCC, 0xCC, 0xCC,
        0x00, // padding is moved to the end by re-marshaling
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ];
    let dst_buf = [
        ("CleanBuffer", vec![0u8; 1000]),
        ("DirtyBuffer", vec![0xFFu8; 1000]),
    ];
    for (name, mut buf) in dst_buf {
        let n = packet.marshal_to(&mut buf);
        assert!(n.is_ok(), "{name}");
        assert_eq!(raw_pkt_re_marshal, &buf[..n.unwrap()], "{name}");
    }
}

#[test]
fn test_rfc8285_two_byte_multiple_extensions_with_large_extension() {
    // 0                   1                   2                   3
    // 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |       0x10    |    0x00       |           length=3            |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |      ID=1     |     L=0       |     ID=2      |     L=1       |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |       data    |       ID=3    |      L=17      |    data...
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //                            ...data...
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //                            ...data...
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //                            ...data...
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //                            ...data...                           |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    let raw_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x10, 0x00, 0x00,
        0x06, 0x01, 0x00, 0x02, 0x01, 0xBB, 0x03, 0x11, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC,
        0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);

    let packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0x1000,
            extensions: vec![
                Extension {
                    id: 1,
                    payload: Bytes::from_static(&[]),
                },
                Extension {
                    id: 2,
                    payload: Bytes::from_static(&[0xBB]),
                },
                Extension {
                    id: 3,
                    payload: Bytes::from_static(&[
                        0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC,
                        0xCC, 0xCC, 0xCC, 0xCC, 0xCC,
                    ]),
                },
            ],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload: raw_pkt.slice(40..),
        ..Default::default()
    };
    let dst_data = packet.marshal().unwrap_or_default();
    assert_eq!(raw_pkt, dst_data);
}

#[test]
fn test_rfc8285_get_extension_returns_nil_when_extensions_disabled() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let packet = Packet {
        header: Header {
            marker: true,
            extension: false,
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };
    assert_eq!(
        None,
        packet.get_extension(1),
        "Should return nil on GetExtension when h.Extension: false"
    );
}

#[test]
fn test_rfc8285_del_extension() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0xBEDE,
            extensions: vec![
                Extension {
                    id: 1,
                    payload: Bytes::from_static(&[0xAA]),
                },
                Extension {
                    id: 2,
                    payload: Bytes::from_static(&[0xBB]),
                },
            ],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };
    assert!(packet.get_extension(1).is_some(), "Extension should exist");
    assert_eq!(
        Ok(()),
        packet.del_extension(1),
        "Should successfully delete extension"
    );
    assert_eq!(None, packet.get_extension(1), "Extension should not exist");
    assert!(
        packet.del_extension(1).is_err(),
        "Should return error when deleting extension that doesnt exist"
    );
    assert!(
        packet.extension,
        "Extension flag should not be cleared if extensions are still present"
    );
    assert_ne!(
        0, packet.extension_profile,
        "ExtensionProfile should have a valid extension profile type"
    );

    assert!(packet.get_extension(2).is_some(), "Extension should exist");
    assert_eq!(
        Ok(()),
        packet.del_extension(2),
        "Should successfully delete extension"
    );
    assert_eq!(None, packet.get_extension(2), "Extension should not exist");
    assert!(
        packet.del_extension(2).is_err(),
        "Should return error when deleting extension that doesnt exist"
    );
    assert!(
        !packet.extension,
        "Extension flag should be cleared if all extensions are deleted"
    );
    assert_eq!(
        0, packet.extension_profile,
        "ExtensionProfile should be cleared"
    );
}

#[test]
fn test_rfc8285_clear_extensions() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0xBEDE,
            extensions: vec![
                Extension {
                    id: 1,
                    payload: Bytes::from_static(&[0xAA]),
                },
                Extension {
                    id: 2,
                    payload: Bytes::from_static(&[0xBB]),
                },
            ],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };
    assert!(packet.get_extension(1).is_some(), "Extension should exist");
    assert!(packet.get_extension(2).is_some(), "Extension should exist");
    packet.clear_extensions();
    assert_eq!(None, packet.get_extension(1), "Extension should not exist");
    assert_eq!(None, packet.get_extension(2), "Extension should not exist");
    assert!(
        !packet.extension,
        "Extension flag should be cleared after clearing extensions"
    );
    assert_eq!(0, packet.extensions.len(), "Should not have any extensions");
    assert_eq!(
        0, packet.extension_profile,
        "ExtensionProfile should be reset"
    );
}

#[test]
fn test_rfc8285_get_extension_ids() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0xBEDE,
            extensions: vec![
                Extension {
                    id: 1,
                    payload: Bytes::from_static(&[0xAA]),
                },
                Extension {
                    id: 2,
                    payload: Bytes::from_static(&[0xBB]),
                },
            ],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };
    let ids = packet.get_extension_ids();
    // Not nil upstream: not empty here.
    assert!(!ids.is_empty(), "Extension should exist");
    assert_eq!(
        packet.extensions.len(),
        ids.len(),
        "The number of IDs should be equal to the number of extensions"
    );

    for id in ids {
        let ext = packet.get_extension(id);
        assert!(ext.is_some(), "Extension should exist");
    }
}

#[test]
fn test_rfc8285_get_extension_ids_returns_error_when_extensions_disabled() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let packet = Packet {
        header: Header {
            marker: true,
            extension: false,
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };
    // Nil upstream: empty here.
    assert!(
        packet.get_extension_ids().is_empty(),
        "Should return nil on GetExtensionIDs when h.Extensions is nil"
    );
}

#[test]
fn test_rfc8285_del_extension_returns_error_when_extensions_disabled() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: false,
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };
    assert!(
        packet.del_extension(1).is_err(),
        "DelExtension did not error on h.Extension: false"
    );
}

#[test]
fn test_rfc8285_one_byte_set_extension_should_enable_exensions_when_adding() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: false,
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };
    let extension = Bytes::from_static(&[0xAA, 0xAA]);
    assert_eq!(Ok(()), packet.set_extension(1, extension.clone()));
    assert!(packet.extension);
    assert_eq!(0xBEDE_u16, packet.extension_profile);
    assert_eq!(1, packet.extensions.len());
    assert_eq!(Some(extension), packet.get_extension(1));
}

#[test]
fn test_rfc8285_one_byte_set_extension_should_set_correct_extension_profile_for_16_byte_extension()
{
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: false,
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };

    let extension = Bytes::from_static(&[
        0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
        0xAA,
    ]);
    let err = packet.set_extension(1, extension);
    assert_eq!(Ok(()), err, "Error setting extension");
    assert_eq!(0xBEDE_u16, packet.extension_profile);
}

#[test]
fn test_rfc8285_one_byte_set_extension_should_update_existing_exension() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0xBEDE,
            extensions: vec![Extension {
                id: 1,
                payload: Bytes::from_static(&[0xAA]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };
    assert_eq!(Some(Bytes::from_static(&[0xAA])), packet.get_extension(1));

    let extension = Bytes::from_static(&[0xBB]);
    let err = packet.set_extension(1, extension.clone());
    assert_eq!(Ok(()), err, "Error setting extension");
    assert_eq!(Some(extension), packet.get_extension(1));
}

#[test]
fn test_rfc8285_one_byte_set_extension_should_error_when_invalid_id_provided() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0xBEDE,
            extensions: vec![Extension {
                id: 1,
                payload: Bytes::from_static(&[0xAA]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };
    assert!(
        packet
            .set_extension(0, Bytes::from_static(&[0xBB]))
            .is_err(),
        "SetExtension did not error on invalid id"
    );
    assert!(
        packet
            .set_extension(15, Bytes::from_static(&[0xBB]))
            .is_err(),
        "SetExtension did not error on invalid id"
    );
}

#[test]
fn test_rfc8285_one_byte_extension_terminate_processing_when_reserved_id_encountered() {
    let mut packet = Packet::default();

    let reserved_id_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0xBE, 0xDE, 0x00,
        0x01, 0xF0, 0xAA, 0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    assert_eq!(
        Ok(()),
        packet.unmarshal(&reserved_id_pkt),
        "Unmarshal error on packet with reserved extension id"
    );
    assert_eq!(
        0,
        packet.extensions.len(),
        "Extensions should be empty for invalid id"
    );

    let payload = reserved_id_pkt.slice(20..);
    assert_eq!(payload, packet.payload);
}

#[test]
fn test_rfc8285_one_byte_extension_terminate_processing_when_padding_with_size_encountered() {
    let mut packet = Packet::default();

    let reserved_id_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0xBE, 0xDE, 0x00,
        0x01, 0x01, 0xAA, 0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    assert_eq!(
        Ok(()),
        packet.unmarshal(&reserved_id_pkt),
        "Unmarshal error on packet with non-zero padding size"
    );
    assert_eq!(
        0,
        packet.extensions.len(),
        "Extensions should be empty for non-zero padding size"
    );

    let payload = reserved_id_pkt.slice(20..);
    assert_eq!(payload, packet.payload);
}

#[test]
fn test_rfc8285_one_byte_set_extension_should_error_when_payload_too_large() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0xBEDE,
            extensions: vec![Extension {
                id: 1,
                payload: Bytes::from_static(&[0xAA]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };
    assert!(
        packet
            .set_extension(
                1,
                Bytes::from_static(&[
                    0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB,
                    0xBB, 0xBB, 0xBB, 0xBB,
                ])
            )
            .is_err(),
        "SetExtension did not error on too large payload"
    );
}

#[test]
fn test_rfc8285_two_byte_set_extension_should_enable_exensions_when_adding() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: false,
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };

    let extension = Bytes::from_static(&[
        0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
        0xAA, 0xAA,
    ]);
    assert_eq!(Ok(()), packet.set_extension(1, extension.clone()));
    assert!(packet.extension);
    assert_eq!(0x1000_u16, packet.extension_profile);
    assert_eq!(1, packet.extensions.len());
    assert_eq!(Some(extension), packet.get_extension(1));
}

#[test]
fn test_rfc8285_two_byte_set_extension_should_update_existing_exension() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0x1000,
            extensions: vec![Extension {
                id: 1,
                payload: Bytes::from_static(&[0xAA]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };
    assert_eq!(
        Some(Bytes::from_static(&[0xAA])),
        packet.get_extension(1),
        "Extension value not initialize properly"
    );

    let extension = Bytes::from_static(&[
        0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB,
        0xBB, 0xBB,
    ]);
    let err = packet.set_extension(1, extension.clone());
    assert_eq!(Ok(()), err);
    assert_eq!(packet.get_extension(1), Some(extension));
}

#[test]
fn test_rfc8285_two_byte_set_extension_should_error_when_payload_too_large() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0xBEDE,
            extensions: vec![Extension {
                id: 1,
                payload: Bytes::from_static(&[0xAA]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };

    // 256 bytes, written out upstream.
    let err = packet.set_extension(1, Bytes::from_static(&[0xBB; 256]));
    assert!(
        err.is_err(),
        "SetExtension did not error on too large payload"
    );
}

#[test]
fn test_rfc8285_padding() {
    let mut header = Header::default();

    for (n, payload) in [
        Bytes::from_static(&[
            0b00010000, // header.Extension = true
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // SequenceNumber, Timestamp, SSRC
            0xBE, 0xDE, // header.ExtensionProfile = extensionProfileOneByte
            0, 1, // extensionLength
            0, 0, 0,    // padding
            0x10, // extid and length
        ]),
        Bytes::from_static(&[
            0b00010000, // header.Extension = true
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // SequenceNumber, Timestamp, SSRC
            0x10, 0x00, // header.ExtensionProfile = extensionProfileTwoByte
            0, 1, // extensionLength
            0, 0, // padding
            0x01, 0x01, // extid and length
        ]),
    ]
    .iter()
    .enumerate()
    {
        let err = header.unmarshal(payload);
        assert!(
            matches!(err, Err(Error::HeaderSizeInsufficientForExtension(..))),
            "case {n}"
        );
    }
}

#[test]
fn test_rfc3550_set_extension_should_error_when_non_zero() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0x1111,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0xAA]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };

    let expect = Bytes::from_static(&[0xBB]);
    assert_eq!(
        Ok(()),
        packet.set_extension(0, expect.clone()),
        "SetExtension should not error on valid id"
    );

    let actual = packet.get_extension(0);
    assert_eq!(Some(expect), actual);
}

#[test]
fn test_rfc3550_set_extension_should_raise_error_when_setting_nonzero_id() {
    let payload = Bytes::from_static(&[
        // Payload
        0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        header: Header {
            marker: true,
            extension: true,
            extension_profile: 0x1111,
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload,
        ..Default::default()
    };

    assert!(
        packet
            .set_extension(1, Bytes::from_static(&[0xBB]))
            .is_err(),
        "SetExtension should error on invalid id"
    );
}

#[test]
fn test_unmarshal_error_handling() {
    struct Case {
        name: &'static str,
        input: Bytes,
        // Whether the error is the one the case expects: `errors.Is` upstream.
        err: fn(&Error) -> bool,
    }
    let cases = [
        Case {
            name: "ShortHeader",
            input: Bytes::from_static(&[
                0x80, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, // timestamp
                0x1c, 0x64, 0x27, // SSRC (one byte missing)
            ]),
            err: |err| matches!(err, Error::HeaderSizeInsufficient { .. }),
        },
        Case {
            name: "MissingCSRC",
            input: Bytes::from_static(&[
                0x81, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, // timestamp
                0x1c, 0x64, 0x27, 0x82, // SSRC
            ]),
            err: |err| matches!(err, Error::HeaderSizeInsufficient { .. }),
        },
        Case {
            name: "MissingExtension",
            input: Bytes::from_static(&[
                0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, // timestamp
                0x1c, 0x64, 0x27, 0x82, // SSRC
            ]),
            err: |err| matches!(err, Error::HeaderSizeInsufficientForExtension(..)),
        },
        Case {
            name: "MissingExtensionData",
            input: Bytes::from_static(&[
                0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, // timestamp
                0x1c, 0x64, 0x27, 0x82, // SSRC
                0xBE, 0xDE, 0x00, 0x03, // specified to have 3 extensions, but actually not
            ]),
            err: |err| matches!(err, Error::HeaderSizeInsufficientForExtension(..)),
        },
        Case {
            name: "MissingExtensionDataPayload",
            input: Bytes::from_static(&[
                0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, // timestamp
                0x1c, 0x64, 0x27, 0x82, // SSRC
                0xBE, 0xDE, 0x00, 0x01, // have 1 extension
                0x12,
                0x00, // length of the payload is expected to be 3, but actually have only 1
            ]),
            err: |err| matches!(err, Error::HeaderSizeInsufficientForExtension(..)),
        },
    ];

    for test_case in cases {
        let mut h = Header::default();
        let err = h.unmarshal(&test_case.input);
        assert!(
            err.as_ref().is_err_and(test_case.err),
            "{}: {err:?}",
            test_case.name
        );
    }
}

// https://github.com/pion/rtp/issues/275
#[test]
fn test_unmarshal_one_byte_extension_without_rtp_payload() {
    let raw_pkt = Bytes::from_static(&[
        0x10, 0x64, 0x57, 0x49, 0x00, 0x00, 0x01, 0x90, 0x12, 0x34, 0xAB, 0xCD, 0xBE, 0xDE, 0x00,
        0x01, // One-Byte extension header, 4 bytes
        0x02, // ID=0, Len=2 (3 bytes data)
        0x01, 0x02, 0x03, // Extension data
    ]);

    let mut p = Packet::default();
    assert_eq!(Ok(()), p.unmarshal(&raw_pkt));
}

#[test]
fn test_unmarshal_two_byte_extension_without_rtp_payload() {
    let raw_pkt = Bytes::from_static(&[
        0x10, 0x64, 0x57, 0x49, 0x00, 0x00, 0x01, 0x90, 0x12, 0x34, 0xAB, 0xCD, 0x10, 0x00, 0x00,
        0x01, // Two-Byte extension header, 4 bytes
        0x02, 0x02, // ID=0, Len=2 (2 bytes data)
        0x02, 0x03, // Extension data
    ]);

    let mut p = Packet::default();
    assert_eq!(Ok(()), p.unmarshal(&raw_pkt));
}

#[test]
fn test_unmarshal_non_standard_extension_without_rtp_payload() {
    let raw_pkt = Bytes::from_static(&[
        0x10, 0x64, 0x57, 0x49, 0x00, 0x00, 0x01, 0x90, 0x12, 0x34, 0xAB, 0xCD, 0xAA, 0xAA, 0x00,
        0x01, // Non-standard header extension 0xAAAA, 4 bytes
        0x01, 0x02, 0x03, 0x04, // Extension data
    ]);

    let mut p = Packet::default();
    assert_eq!(Ok(()), p.unmarshal(&raw_pkt));
}

#[test]
fn test_unmarshal_empty_one_byte_extension_without_rtp_payload() {
    let raw_pkt = Bytes::from_static(&[
        0x10, 0x64, 0x57, 0x49, 0x00, 0x00, 0x01, 0x90, 0x12, 0x34, 0xAB, 0xCD, 0xBE, 0xDE, 0x00,
        0x00, // One-Byte extension header, 0 bytes
    ]);

    let mut p = Packet::default();
    assert_eq!(Ok(()), p.unmarshal(&raw_pkt));
}

#[test]
fn test_unmarshal_empty_two_byte_extension_without_rtp_payload() {
    let raw_pkt = Bytes::from_static(&[
        0x10, 0x64, 0x57, 0x49, 0x00, 0x00, 0x01, 0x90, 0x12, 0x34, 0xAB, 0xCD, 0x10, 0x00, 0x00,
        0x00, // Two-Byte extension header, 0 bytes
    ]);

    let mut p = Packet::default();
    assert_eq!(Ok(()), p.unmarshal(&raw_pkt));
}

#[test]
fn test_unmarshal_empty_non_standard_extension_without_rtp_payload() {
    let raw_pkt = Bytes::from_static(&[
        0x10, 0x64, 0x57, 0x49, 0x00, 0x00, 0x01, 0x90, 0x12, 0x34, 0xAB, 0xCD, 0xAA, 0xAA, 0x00,
        0x00, // Non-standard header extension 0xAAAA, 0 bytes
    ]);

    let mut p = Packet::default();
    assert_eq!(Ok(()), p.unmarshal(&raw_pkt));
}

#[test]
fn test_roundtrip() {
    let raw_pkt = Bytes::from_static(&[
        0x00, 0x10, 0x23, 0x45, 0x12, 0x34, 0x45, 0x67, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11, 0x22,
        0x33, 0x44, 0x55, 0x66, 0x77,
    ]);
    let payload = raw_pkt.slice(12..);

    let mut packet = Packet::default();
    assert_eq!(Ok(()), packet.unmarshal(&raw_pkt));
    assert_eq!(payload, packet.payload);

    let buf = packet.marshal();
    assert!(buf.is_ok());
    assert_eq!(raw_pkt, buf.unwrap());
    assert_eq!(payload, packet.payload);
}

#[test]
fn test_clone_header() {
    let mut header = Header {
        marker: true,
        extension: true,
        extension_profile: 1,
        extensions: vec![Extension {
            id: 0,
            payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
        }],
        version: 2,
        payload_type: 96,
        sequence_number: 27023,
        timestamp: 3653407706,
        ssrc: 476325762,
        ..Default::default()
    };
    let clone = header.clone();
    assert_eq!(header, clone);

    header.csrc.push(1);
    assert_ne!(
        clone.csrc.len(),
        header.csrc.len(),
        "Expected CSRC to be unchanged"
    );
    // The original writes into the payload of the extension (`payload[0] = 0x1F`). `Bytes`
    // cannot be written into: the payload is replaced by one with that byte changed.
    header.extensions[0].payload = Bytes::from_static(&[0x1F, 0xFF, 0xFF, 0xFF]);
    assert_ne!(
        clone.extensions[0].payload[0], 0x1F_u8,
        "Expected extension to be unchanged"
    );
}

#[test]
fn test_clone_packet() {
    let raw_pkt = Bytes::from_static(&[
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0xBE, 0xDE, 0x00,
        0x01, 0x50, 0xAA, 0x00, 0x00, 0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet {
        payload: raw_pkt.slice(20..),
        ..Default::default()
    };

    let clone = packet.clone();
    assert_eq!(packet, clone);

    // The original writes into the payload (`Payload[0] = 0x1F`). `Bytes` cannot be written
    // into: the payload is replaced by one with that byte changed.
    let mut payload = packet.payload.to_vec();
    payload[0] = 0x1F;
    packet.payload = Bytes::from(payload);
    // The original compares the byte with an untyped constant here, which testify never finds
    // equal to a byte; this compares two bytes.
    assert_ne!(clone.payload[0], 0x1F, "Expected payload to be unchanged");
}

#[test]
fn test_marshal_rtp_packet_funcs() {
    // packet with only padding
    let raw_pkt = Bytes::from_static(&[
        0xb0, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x00, 0x01, 0x00,
        0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x05,
    ]);
    let parsed_packet = Packet {
        header: Header {
            padding: true,
            marker: true,
            extension: true,
            extension_profile: 1,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            padding_size: 5,
            ..Default::default()
        },
        payload: Bytes::new(),
        ..Default::default()
    };

    let mut buf = vec![0u8; 100];
    let n = marshal_packet_to(&mut buf, &parsed_packet.header, &parsed_packet.payload);
    assert!(n.is_ok());
    let n = n.unwrap();
    assert_eq!(raw_pkt.len(), n);
    assert_eq!(raw_pkt, &buf[..n]);

    assert_eq!(
        n,
        packet_marshal_size(&parsed_packet.header, &parsed_packet.payload)
    );

    let (hdr_len, packet_len) =
        header_and_packet_marshal_size(&parsed_packet.header, &parsed_packet.payload);
    assert_eq!(parsed_packet.header.marshal_size(), hdr_len);
    assert_eq!(n, packet_len);
}

#[test]
fn test_deprecated_padding_size_field() {
    // packet with only padding
    let raw_pkt = Bytes::from_static(&[
        0xb0, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0x00, 0x01, 0x00,
        0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x05,
    ]);
    let parsed_packet = Packet {
        header: Header {
            padding: true,
            marker: true,
            extension: true,
            extension_profile: 1,
            extensions: vec![Extension {
                id: 0,
                payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
            }],
            version: 2,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload: Bytes::new(),
        padding_size: 5,
        ..Default::default()
    };

    let buf = parsed_packet.marshal();
    assert!(buf.is_ok());
    assert_eq!(raw_pkt, buf.unwrap());
    assert_eq!(0, parsed_packet.header.padding_size);

    assert_eq!(raw_pkt.len(), parsed_packet.marshal_size());
    assert_eq!(0, parsed_packet.header.padding_size);

    let parsed_packet2 = parsed_packet.clone();
    assert_eq!(5, parsed_packet2.padding_size);
    assert_eq!(0, parsed_packet2.header.padding_size);
}

#[test]
fn test_set_extension_with_profile() {
    {
        let name = "add two-byte extension due to the size > 16";
        let mut h = Header::default();
        assert_eq!(
            Ok(()),
            h.set_extension(1, Bytes::from_static(&[0; 2])),
            "{name}"
        );
        assert_eq!(
            Ok(()),
            h.set_extension(2, Bytes::from_static(&[0; 3])),
            "{name}"
        );

        // Adding another extension that requires two-byte header extension
        assert_eq!(
            Ok(()),
            h.set_extension_with_profile(
                3,
                Bytes::from_static(&[0; 20]),
                EXTENSION_PROFILE_TWO_BYTE
            ),
            "{name}"
        );
        assert_eq!(h.extension_profile, EXTENSION_PROFILE_TWO_BYTE, "{name}");
    }

    {
        let name = "add two-byte extension due to id > 14";
        let mut h = Header::default();
        assert_eq!(
            Ok(()),
            h.set_extension(1, Bytes::from_static(&[0; 2])),
            "{name}"
        );
        assert_eq!(
            Ok(()),
            h.set_extension(2, Bytes::from_static(&[0; 3])),
            "{name}"
        );

        // Adding another extension that requires two-byte header extension
        // because the extmap ID is greater than 14.
        assert_eq!(
            Ok(()),
            h.set_extension_with_profile(
                16,
                Bytes::from_static(&[0; 4]),
                EXTENSION_PROFILE_TWO_BYTE
            ),
            "{name}"
        );
        assert_eq!(h.extension_profile, EXTENSION_PROFILE_TWO_BYTE, "{name}");
    }

    {
        let name = "Downgrade 2 byte header Extension";
        let pkt = Bytes::from_static(&[
            0x90, 0x60, 0x00, 0x01, // V=2, P=0, X=1, CC=0; M=0, PT=96; sequence=1
            0x00, 0x00, 0x00, 0x01, // timestamp=1
            0x12, 0x34, 0x56, 0x78, // SSRC=0x12345678
            0x10, 0x00, 0x00, 0x01, // profile=0x1000 (two-byte), length=1 (4 bytes)
            0x01, 0x02, 0x00, 0x01, // id=1, len=2, data=0x00,0x01 (padded to 32-bit)
        ]);
        let mut h = Header::default();

        let err = h.unmarshal(&pkt);
        assert!(err.is_ok(), "{name}");
        assert_eq!(h.extension_profile, EXTENSION_PROFILE_TWO_BYTE, "{name}");

        assert_eq!(
            Ok(()),
            h.set_extension_with_profile(
                1,
                Bytes::from_static(&[0x02, 0x03]),
                EXTENSION_PROFILE_ONE_BYTE
            ),
            "{name}"
        );
        assert_eq!(h.extension_profile, EXTENSION_PROFILE_ONE_BYTE, "{name}");

        let pkt = h.marshal();
        assert!(pkt.is_ok(), "{name}");

        assert_eq!(
            pkt.unwrap(),
            [
                0x90, 0x60, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x12, 0x34, 0x56, 0x78, 0xbe, 0xde,
                0x00, 0x01, 0x11, 0x02, 0x03, 0x00,
            ],
            "{name}"
        );
    }

    {
        let name = "Do not mutate packet for invalid extension";
        let mut h = Header::default();
        assert_eq!(
            Ok(()),
            h.set_extension(1, Bytes::from_static(&[0; 2])),
            "{name}"
        );

        assert!(
            h.set_extension_with_profile(
                16,
                Bytes::from_static(&[0; 4096]),
                EXTENSION_PROFILE_TWO_BYTE
            )
            .is_err(),
            "{name}"
        );

        assert_eq!(h.extension_profile, EXTENSION_PROFILE_ONE_BYTE, "{name}");
        assert_eq!(1, h.extensions.len(), "{name}");
    }
}

// https://github.com/pion/rtp/issues/315
#[test]
fn test_marshal_size_panic() {
    let hdr = Header {
        extension: true,
        ..Default::default()
    };

    assert_eq!(16, hdr.marshal_size());
}

// https://github.com/pion/rtp/issues/315
#[test]
fn test_marshal_to_panic() {
    let hdr = Header {
        extension: true,
        ..Default::default()
    };

    let mut buf = vec![0u8; 16];
    let n = hdr.marshal_to(&mut buf);
    assert_eq!(Ok(16), n);
}

// New for musivum: the texts of the errors, which the original does not test. They are what
// pion v1.10.5 prints for the same input.
#[test]
fn error_texts_are_pions() {
    let text = |err: Result<usize, Error>| err.unwrap_err().to_string();
    let mut h = Header::default();
    assert_eq!(
        "RTP header size insufficient: 0 < 4",
        text(h.unmarshal(&Bytes::new()))
    );
    assert_eq!(
        "size 11 < 12: RTP header size insufficient",
        text(h.unmarshal(&Bytes::from_static(&[
            0x80, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27
        ])))
    );
    assert_eq!(
        "size 12 < 16: RTP header size insufficient",
        text(h.unmarshal(&Bytes::from_static(&[
            0x81, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82
        ])))
    );
    assert_eq!(
        "size 12 < 16: RTP header size insufficient for extension",
        text(h.unmarshal(&Bytes::from_static(&[
            0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82
        ])))
    );
    assert_eq!(
        "size 16 < 28: RTP header size insufficient for extension",
        text(h.unmarshal(&Bytes::from_static(&[
            0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0xBE, 0xDE,
            0x00, 0x03
        ])))
    );
    assert_eq!(
        "size 20 < 20: RTP header size insufficient for extension",
        text(h.unmarshal(&Bytes::from_static(&[
            0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10, 0x00, 0, 1, 0, 0, 0, 0x01
        ])))
    );

    let text = |err: Result<(), Error>| err.unwrap_err().to_string();
    let mut p = Packet::default();
    assert_eq!(
        "buffer too small",
        text(p.unmarshal(&Bytes::from_static(&[
            0xa0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        ])))
    );
    assert_eq!(
        "invalid RTP padding",
        text(p.unmarshal(&Bytes::from_static(&[
            0xa0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        ])))
    );
    assert_eq!(
        "buffer too small",
        text(p.unmarshal(&Bytes::from_static(&[
            0xa0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9
        ])))
    );

    let mut h = Header {
        extension: true,
        extension_profile: EXTENSION_PROFILE_ONE_BYTE,
        ..Default::default()
    };
    assert_eq!(
        "header extension id must be between 1 and 14 for RFC 5285 one byte extensions actual(0)",
        text(h.set_extension(0, Bytes::from_static(&[1])))
    );
    assert_eq!(
        "header extension id must be between 1 and 14 for RFC 5285 one byte extensions actual(15)",
        text(h.set_extension(15, Bytes::from_static(&[1])))
    );
    assert_eq!(
        "header extension payload must be 16bytes or less for RFC 5285 one byte extensions actual(17)",
        text(h.set_extension(1, Bytes::from_static(&[0; 17])))
    );
    let mut h = Header {
        extension: true,
        extension_profile: EXTENSION_PROFILE_TWO_BYTE,
        ..Default::default()
    };
    assert_eq!(
        "header extension id must be between 1 and 255 for RFC 5285 two byte extensions actual(0)",
        text(h.set_extension(0, Bytes::from_static(&[1])))
    );
    assert_eq!(
        "header extension payload must be 255bytes or less for RFC 5285 two byte extensions actual(256)",
        text(h.set_extension(1, Bytes::from_static(&[0; 256])))
    );
    let mut h = Header {
        extension: true,
        extension_profile: 0x1111,
        ..Default::default()
    };
    assert_eq!(
        "header extension id must be 0 for non-RFC 5285 extensions actual(3)",
        text(h.set_extension(3, Bytes::from_static(&[1])))
    );
    assert_eq!("extension not found", text(h.del_extension(3)));
    let mut h = Header::default();
    assert_eq!("h.Extension not enabled", text(h.del_extension(3)));
    assert_eq!(
        "short buffer",
        h.marshal_to(&mut [0; 3]).unwrap_err().to_string()
    );
    let p = Packet {
        header: Header {
            padding: true,
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!("invalid RTP padding", p.marshal().unwrap_err().to_string());
}

// New for musivum: what `unmarshal` keeps is the buffer it was given, not a copy of it.
#[test]
fn unmarshal_keeps_slices_of_the_buffer() {
    let raw_pkt = Bytes::from(vec![
        0x90, 0xe0, 0x69, 0x8f, 0xd9, 0xc2, 0x93, 0xda, 0x1c, 0x64, 0x27, 0x82, 0xBE, 0xDE, 0x00,
        0x01, 0x10, 0xAA, 0x20, 0xBB, 0x98, 0x36, 0xbe, 0x88, 0x9e,
    ]);
    let mut packet = Packet::default();
    assert_eq!(Ok(()), packet.unmarshal(&raw_pkt));
    assert_eq!(raw_pkt[20..].as_ptr(), packet.payload.as_ptr());
    assert_eq!(5, packet.payload.len());
    assert_eq!(
        raw_pkt[17..].as_ptr(),
        packet.get_extension(1).unwrap().as_ptr()
    );
    assert_eq!(
        raw_pkt[19..].as_ptr(),
        packet.get_extension(2).unwrap().as_ptr()
    );

    let mut header = Header::default();
    assert_eq!(Ok(20), header.unmarshal(&raw_pkt));
    assert_eq!(header, packet.header);
}

// New for musivum: which of the two sizes of the padding counts when both are set, which the
// original does not test. The bytes are what pion v1.10.5 gives for the same packets.
#[test]
fn padding_size_of_the_header_goes_before_the_deprecated_one() {
    let mut packet = Packet {
        header: Header {
            padding: true,
            padding_size: 3,
            ..Default::default()
        },
        payload: Bytes::from_static(&[1]),
        padding_size: 7,
        ..Default::default()
    };
    assert_eq!(16, packet.marshal_size());
    assert_eq!(
        Ok(vec![0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 3]),
        packet.marshal()
    );

    // The deprecated one counts when the header has none.
    packet.header.padding_size = 0;
    assert_eq!(20, packet.marshal_size());
    assert_eq!(
        Ok(vec![
            0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 7
        ]),
        packet.marshal()
    );

    // A size without the padding bit: the bytes are there, the bit and the count are not.
    let packet = Packet {
        header: Header {
            padding_size: 3,
            ..Default::default()
        },
        payload: Bytes::from_static(&[1]),
        ..Default::default()
    };
    assert_eq!(16, packet.marshal_size());
    assert_eq!(
        Ok(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0]),
        packet.marshal()
    );
}

// New for musivum: `String()` of the original, which it does not test. The text is what pion
// v1.10.5 prints for this packet.
#[test]
fn display_is_pions_string() {
    let packet = Packet {
        header: Header {
            version: 2,
            marker: true,
            payload_type: 96,
            sequence_number: 27023,
            timestamp: 3653407706,
            ssrc: 476325762,
            ..Default::default()
        },
        payload: Bytes::from_static(&[1, 2, 3]),
        ..Default::default()
    };
    assert_eq!(
        "RTP PACKET:\n\tVersion: 2\n\tMarker: true\n\tPayload Type: 96\n\tSequence Number: 27023\n\tTimestamp: 3653407706\n\tSSRC: 476325762 (1c642782)\n\tPayload Length: 3\n",
        packet.to_string()
    );
}
