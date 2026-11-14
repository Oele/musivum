// Ported from LiveKit's mediatransportutil, pkg/bucket/bucket_test.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the packets are built by a stand-in for the little of
// pion's `rtp.Packet` that the tests use (`rtp` below), until `crate::webrtc::rtp` is there;
// the helpers `testQueue`, `testQueueEdges` and `testQueueWrap` have `_inner` at their names,
// which the tests that call them would otherwise share; the packet that `AddPacket` returned
// is taken again from the bucket's slots where the original keeps the slice
// (`stored_packet`).

use super::*;

// The original builds its packets with pion's `rtp.Packet`, which the port does not have yet
// (`crate::webrtc::rtp`). This stands in for what the tests use of it: a packet that has a
// sequence number and a payload, written and read as pion writes and reads a header without
// extensions and CSRCs. With the zero values everywhere else, as in the original, that is
// twelve bytes that are zero but for the sequence number, and then the payload.
mod rtp {
    const HEADER_LENGTH: usize = 12;

    #[derive(Clone, Debug, Default)]
    pub struct Header {
        pub sequence_number: u16,
    }

    #[derive(Clone, Debug, Default)]
    pub struct Packet {
        pub header: Header,
        pub payload: Vec<u8>,
    }

    impl Packet {
        pub fn marshal(&self) -> Result<Vec<u8>, String> {
            let mut buf = vec![0u8; HEADER_LENGTH + self.payload.len()];
            buf[2..4].copy_from_slice(&self.header.sequence_number.to_be_bytes());
            buf[HEADER_LENGTH..].copy_from_slice(&self.payload);
            Ok(buf)
        }

        pub fn unmarshal(&mut self, buf: &[u8]) -> Result<(), String> {
            if buf.len() < HEADER_LENGTH {
                return Err(format!(
                    "RTP header size insufficient: {} < {}",
                    buf.len(),
                    HEADER_LENGTH
                ));
            }
            if buf[0] != 0 {
                return Err(format!(
                    "a first byte of {:#04x}: more than this stand-in reads",
                    buf[0]
                ));
            }
            self.header.sequence_number = u16::from_be_bytes([buf[2], buf[3]]);
            self.payload = buf[HEADER_LENGTH..].to_vec();
            Ok(())
        }
    }
}

// testify's `require.ErrorIs(t, err, target)` for a result of the bucket: the error is the
// given one of the package's, with whatever details.
macro_rules! require_error_is {
    ($result:expr, $target:pat) => {
        match $result {
            Err($target) => {}
            other => panic!("expected {}, got {:?}", stringify!($target), other),
        }
    };
}

// The original keeps the slice that `AddPacket` returned and writes to it later, to change the
// packet inside the bucket. In Rust that slice borrows the bucket, which the test goes on to
// use. So the test notes where the slice is (`as_ptr`, `len`) and takes it again from the
// bucket's slots here: the same bytes, by another way.
fn stored_packet<ET: Number, T: Number>(
    q: &mut Bucket<ET, T>,
    stored_pkt: (*const u8, usize),
) -> &mut [u8] {
    let (ptr, len) = stored_pkt;
    let pkt_size_header = q.pkt_size_header;
    let slot = q
        .slots
        .iter_mut()
        .find(|slot| slot[pkt_size_header..].as_ptr() == ptr)
        .expect("the packet that was returned is in a slot of the bucket");
    &mut slot[pkt_size_header..pkt_size_header + len]
}

// binary.BigEndian.Uint16
fn big_endian_uint16(b: &[u8]) -> u16 {
    u16::from_be_bytes([b[0], b[1]])
}

// binary.BigEndian.PutUint16
fn big_endian_put_uint16(b: &mut [u8], v: u16) {
    b[..2].copy_from_slice(&v.to_be_bytes());
}

fn test_queue_inner<ET: Number, T: Number>(mut q: Bucket<ET, T>) {
    let test_packets = [
        rtp::Packet {
            header: rtp::Header { sequence_number: 1 },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 3 },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 4 },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 6 },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 7 },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 10,
            },
            ..Default::default()
        },
    ];

    for p in &test_packets {
        let buf = p.marshal().unwrap();
        // require.NotPanics: a panic fails the test
        let _ = q.add_packet(&buf);
        assert_eq!(
            ET::from_u64(p.header.sequence_number as u64),
            q.head_sequence_number()
        );
    }

    let pkt_too_large = vec![0u8; RTP_MAX_PKT_SIZE + 1];
    let err = q.add_packet(&pkt_too_large);
    require_error_is!(err, Error::PacketTooLarge);

    let mut expected_sn = ET::from_u64(6);
    let mut np = rtp::Packet::default();
    let mut buff = vec![0u8; RTP_MAX_PKT_SIZE];
    let mut i = q.get_packet(&mut buff, expected_sn).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(expected_sn.to_u64() as u16, np.header.sequence_number);

    // add an out-of-order packet and ensure it can be retrieved
    let np2 = rtp::Packet {
        header: rtp::Header { sequence_number: 8 },
        ..Default::default()
    };
    let mut buf = np2.marshal().unwrap();
    let stored_pkt = q.add_packet(&buf).unwrap();
    let stored_pkt = (stored_pkt.as_ptr(), stored_pkt.len());
    expected_sn = ET::from_u64(8);
    i = q.get_packet(&mut buff, expected_sn).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(expected_sn.to_u64() as u16, np.header.sequence_number);

    // adding a duplicate should not add and return retransmitted packet error
    let err = q.add_packet(&buf);
    require_error_is!(err, Error::RtxPacket);

    // adding a duplicate with incorrect size should return rtx size error
    let err = q.add_packet(&buf[..buf.len() - 1]);
    require_error_is!(err, Error::RtxPacketSize(_));

    // mangle the sequence number of a duplicate inside the bucket, should return mismatch error
    let stored_sn = big_endian_uint16(&stored_packet(&mut q, stored_pkt)[RTP_SEQ_NUM_OFFSET..]);
    big_endian_put_uint16(
        &mut stored_packet(&mut q, stored_pkt)[RTP_SEQ_NUM_OFFSET..],
        stored_sn.wrapping_sub(1),
    );
    let err = q.add_packet(&buf);
    require_error_is!(err, Error::PacketMismatch(_));
    // restore and ensure that RTX error is returned
    big_endian_put_uint16(
        &mut stored_packet(&mut q, stored_pkt)[RTP_SEQ_NUM_OFFSET..],
        stored_sn,
    );
    let err = q.add_packet(&buf);
    require_error_is!(err, Error::RtxPacket);

    // try to get old packets
    let err = q.get_packet(&mut buff, ET::from_u64(0));
    require_error_is!(err, Error::PacketTooOld(_));

    // ask for something ahead of headSN
    let err = q.get_packet(&mut buff, ET::from_u64(11));
    require_error_is!(err, Error::PacketTooNew(_));

    q.resync_on_next_packet();

    // should be able to get packets before adding a packet which will resync
    expected_sn = ET::from_u64(8);
    i = q.get_packet(&mut buff, expected_sn).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(expected_sn.to_u64() as u16, np.header.sequence_number);

    // adding a packet will resync and invalidate all existing
    buf = test_packets[1].marshal().unwrap();
    q.add_packet(&buf).unwrap();

    // try to get a packet that was valid before resync, should not be found
    let err = q.get_packet(&mut buff, ET::from_u64(8));
    require_error_is!(err, Error::PacketTooNew(_));

    // getting a packet added after resync should succeed
    expected_sn = ET::from_u64(test_packets[1].header.sequence_number as u64);
    i = q.get_packet(&mut buff, expected_sn).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(expected_sn.to_u64() as u16, np.header.sequence_number);

    // adding a packet with sequence number override
    buf = test_packets[2].marshal().unwrap();
    // sequence number in packet is 4, add with 5
    q.add_packet_with_sequence_number(&buf, ET::from_u64(5))
        .unwrap();

    // should not be able to get sequence number 4 that was in the packet that was given to the bucket
    // it should have been overwritten
    expected_sn = ET::from_u64(test_packets[2].header.sequence_number as u64);
    let err = q.get_packet(&mut buff, expected_sn);
    require_error_is!(err, Error::PacketSizeInvalid(_));

    // should be able to get overridden sequence number
    expected_sn = ET::from_u64(5);
    i = q.get_packet(&mut buff, expected_sn).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(expected_sn.to_u64() as u16, np.header.sequence_number);
}

#[test]
fn test_queue() {
    assert!(Bucket::<u64, u16>::new(10, 1024 * 1024, RTP_SEQ_NUM_OFFSET).is_none());

    test_queue_inner(Bucket::<u16, u16>::new(10, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap());
    test_queue_inner(Bucket::<u32, u16>::new(10, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap());
    test_queue_inner(Bucket::<u64, u16>::new(10, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap());
}

fn test_queue_edges_inner<ET: Number, T: Number>(mut q: Bucket<ET, T>) {
    let test_packets = [
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 65533,
            },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 65534,
            },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 2 },
            ..Default::default()
        },
    ];

    for p in &test_packets {
        // require.NotPanics: a panic fails the test
        let buf = p.marshal().unwrap();
        let _ = q.add_packet(&buf);
    }

    let expected_sn = ET::from_u64(65534);
    let mut np = rtp::Packet::default();
    let mut buff = vec![0u8; RTP_MAX_PKT_SIZE];
    let mut i = q.get_packet(&mut buff, expected_sn).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(expected_sn.to_u64() as u16, np.header.sequence_number);

    // add an out-of-order packet where the head sequence has wrapped and ensure it can be retrieved
    let np2 = rtp::Packet {
        header: rtp::Header {
            sequence_number: 65535,
        },
        ..Default::default()
    };
    let buf = np2.marshal().unwrap();
    let _ = q.add_packet(&buf);
    // expectedSN+1
    i = q
        .get_packet(&mut buff, ET::from_u64(expected_sn.to_u64() + 1))
        .unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(
        (expected_sn.to_u64() as u16).wrapping_add(1),
        np.header.sequence_number
    );
}

#[test]
fn test_queue_edges() {
    test_queue_edges_inner(
        Bucket::<u16, u16>::new(10, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap(),
    );
    test_queue_edges_inner(
        Bucket::<u32, u16>::new(10, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap(),
    );
    test_queue_edges_inner(
        Bucket::<u64, u16>::new(10, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap(),
    );
}

#[test]
fn test_queue_edges_extended() {
    let test_packets = [
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 65533,
            },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 65534,
            },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 2 },
            ..Default::default()
        },
    ];

    let mut q = Bucket::<u64, u16>::new(100, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap();

    for p in &test_packets {
        // require.NotPanics: a panic fails the test
        let buf = p.marshal().unwrap();
        if p.header.sequence_number < 32768 {
            let _ =
                q.add_packet_with_sequence_number(&buf, p.header.sequence_number as u64 + 65536);
        } else {
            let _ = q.add_packet_with_sequence_number(&buf, p.header.sequence_number as u64);
        }
    }

    let expected_sn = 65534u64;
    let mut np = rtp::Packet::default();
    let mut buff = vec![0u8; RTP_MAX_PKT_SIZE];
    let mut i = q.get_packet(&mut buff, expected_sn).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(expected_sn as u16, np.header.sequence_number);

    // add an out-of-order packet where the head sequence has wrapped and ensure it can be retrieved
    let np2 = rtp::Packet {
        header: rtp::Header {
            sequence_number: 65535,
        },
        ..Default::default()
    };
    let buf = np2.marshal().unwrap();
    let _ = q.add_packet_with_sequence_number(&buf, np2.header.sequence_number as u64);

    i = q.get_packet(&mut buff, expected_sn + 1).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(
        (expected_sn as u16).wrapping_add(1),
        np.header.sequence_number
    );

    // wrap around should be linear in extended range (0/65536 and 1/65537 were not added)
    // (The original has `require.Error(t, err, ErrPacketSizeInvalid)` in the next two checks:
    // any error passes, and the error it names is only the message of a failure.)
    let err = q.get_packet(&mut buff, expected_sn + 2);
    assert!(err.is_err(), "{}", Error::PacketSizeInvalid(String::new()));
    let err = q.get_packet(&mut buff, expected_sn + 3);
    assert!(err.is_err(), "{}", Error::PacketSizeInvalid(String::new()));

    i = q.get_packet(&mut buff, expected_sn + 4).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(
        (expected_sn as u16).wrapping_add(4),
        np.header.sequence_number
    );
}

fn test_queue_wrap_inner<ET: Number, T: Number>(mut q: Bucket<ET, T>) {
    let test_packets = [
        rtp::Packet {
            header: rtp::Header { sequence_number: 1 },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 3 },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 4 },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 6 },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 7 },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 10,
            },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 13,
            },
            ..Default::default()
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 15,
            },
            ..Default::default()
        },
    ];

    for p in &test_packets {
        let buf = p.marshal().unwrap();
        // require.NotPanics: a panic fails the test
        let _ = q.add_packet(&buf);
    }

    let mut buff = vec![0u8; RTP_MAX_PKT_SIZE];

    // try to get old packets, but were valid before the bucket wrapped
    let err = q.get_packet(&mut buff, ET::from_u64(1));
    require_error_is!(err, Error::PacketTooOld(_));
    let err = q.get_packet(&mut buff, ET::from_u64(3));
    require_error_is!(err, Error::PacketTooOld(_));
    let err = q.get_packet(&mut buff, ET::from_u64(4));
    require_error_is!(err, Error::PacketTooOld(_));

    let mut expected_sn = ET::from_u64(6);
    let mut np = rtp::Packet::default();
    let mut i = q.get_packet(&mut buff, expected_sn).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(expected_sn.to_u64() as u16, np.header.sequence_number);

    // add an out-of-order packet and ensure it can be retrieved
    let np2 = rtp::Packet {
        header: rtp::Header { sequence_number: 8 },
        ..Default::default()
    };
    let mut buf = np2.marshal().unwrap();
    q.add_packet(&buf).unwrap();
    expected_sn = ET::from_u64(8);
    i = q.get_packet(&mut buff, expected_sn).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(expected_sn.to_u64() as u16, np.header.sequence_number);

    // add a packet with a large gap in sequence number which will invalidate all the slots
    let np3 = rtp::Packet {
        header: rtp::Header {
            sequence_number: 56,
        },
        ..Default::default()
    };
    buf = np3.marshal().unwrap();
    q.add_packet(&buf).unwrap();
    expected_sn = ET::from_u64(56);
    i = q.get_packet(&mut buff, expected_sn).unwrap();
    np.unmarshal(&buff[..i]).unwrap();
    assert_eq!(expected_sn.to_u64() as u16, np.header.sequence_number);

    // after the large jump invalidating all slots, retrieving previously added packets should fail
    let err = q.get_packet(&mut buff, ET::from_u64(6));
    require_error_is!(err, Error::PacketTooOld(_));
    let err = q.get_packet(&mut buff, ET::from_u64(7));
    require_error_is!(err, Error::PacketTooOld(_));
    let err = q.get_packet(&mut buff, ET::from_u64(8));
    require_error_is!(err, Error::PacketTooOld(_));
    let err = q.get_packet(&mut buff, ET::from_u64(10));
    require_error_is!(err, Error::PacketTooOld(_));
    let err = q.get_packet(&mut buff, ET::from_u64(13));
    require_error_is!(err, Error::PacketTooOld(_));
    let err = q.get_packet(&mut buff, ET::from_u64(15));
    require_error_is!(err, Error::PacketTooOld(_));
}

#[test]
fn test_queue_wrap() {
    test_queue_wrap_inner(
        Bucket::<u16, u16>::new(10, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap(),
    );
    test_queue_wrap_inner(
        Bucket::<u32, u16>::new(10, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap(),
    );
    test_queue_wrap_inner(
        Bucket::<u64, u16>::new(10, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap(),
    );
}

#[test]
fn test_growing_full_bucket() {
    let mut test_packets = Vec::with_capacity(8);
    for i in 0..8 {
        test_packets.push(rtp::Packet {
            header: rtp::Header {
                sequence_number: i as u16,
            },
            payload: vec![i as u8],
        });
    }

    let mut bucket = Bucket::<u16, u16>::new(4, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap();

    for p in &test_packets {
        let buf = p.marshal().unwrap();
        bucket.add_packet(&buf).unwrap();
    }

    for i in (4..=7).rev() {
        bucket
            .get_packet(&mut vec![0u8; RTP_MAX_PKT_SIZE], i as u16)
            .unwrap();
    }

    let err = bucket.get_packet(&mut vec![0u8; RTP_MAX_PKT_SIZE], 3u16);
    require_error_is!(err, Error::PacketTooOld(_));

    assert_eq!(8, bucket.grow());
    assert_eq!(8, bucket.capacity());

    let mut next_test_packets = Vec::with_capacity(8);
    for i in 8..16 {
        next_test_packets.push(rtp::Packet {
            header: rtp::Header {
                sequence_number: i as u16,
            },
            payload: vec![i as u8],
        });
    }

    for p in &next_test_packets {
        let buf = p.marshal().unwrap();
        bucket.add_packet(&buf).unwrap();
    }

    for i in (8..=15).rev() {
        let mut buf = vec![0u8; RTP_MAX_PKT_SIZE];
        bucket.get_packet(&mut buf, i as u16).unwrap();
    }
}

#[test]
fn test_queue_grow() {
    let test_packets = [
        rtp::Packet {
            header: rtp::Header { sequence_number: 1 },
            payload: vec![1],
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 3 },
            payload: vec![3],
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 4 },
            payload: vec![4],
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 6 },
            payload: vec![6],
        },
        rtp::Packet {
            header: rtp::Header { sequence_number: 7 },
            payload: vec![7],
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 10,
            },
            payload: vec![10],
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 13,
            },
            payload: vec![13],
        },
        rtp::Packet {
            header: rtp::Header {
                sequence_number: 15,
            },
            payload: vec![15],
        },
    ];

    let mut q = Bucket::<u16, u16>::new(10, RTP_MAX_PKT_SIZE, RTP_SEQ_NUM_OFFSET).unwrap();

    for p in &test_packets {
        let buf = p.marshal().unwrap();
        // require.NotPanics: a panic fails the test
        let _ = q.add_packet(&buf);
    }

    // seq1 is too old
    let mut pbuf = test_packets[0].marshal().unwrap();
    let err = q.add_packet(&pbuf);
    require_error_is!(err, Error::PacketTooOld(_));

    assert_eq!(q.grow(), 20);
    assert_eq!(q.capacity(), 20);

    // grow the bucket should not impact the existing packets
    let mut buf = vec![0u8; RTP_MAX_PKT_SIZE];
    for sn in [15u16, 13, 10, 7, 6] {
        let i = q.get_packet(&mut buf, sn).unwrap();
        assert_eq!(13, i);
        assert_eq!(sn, buf[i - 1] as u16);
    }

    // too old packets before grow should not be found
    for sn in [4u16, 3, 2, 1] {
        let err = q.get_packet(&mut buf, sn);
        assert!(matches!(
            err,
            Err(Error::PacketSizeInvalid(_)) | Err(Error::PacketMismatch(_))
        ));
    }

    // seq1 can be added and retrieved after grow
    q.add_packet(&pbuf).unwrap();
    let i = q.get_packet(&mut buf, 1).unwrap();
    assert_eq!(13, i);
    assert_eq!(1, buf[i - 1] as u16);

    // add a packet with gap less than the growed capacity
    let np = rtp::Packet {
        header: rtp::Header {
            sequence_number: 34,
        },
        ..Default::default()
    };
    pbuf = np.marshal().unwrap();
    q.add_packet(&pbuf).unwrap();
    for sn in [34u16, 15] {
        q.get_packet(&mut buf, sn).unwrap();
    }

    // add a packet with a large gap in sequence number which will invalidate all the slots
    let np2 = rtp::Packet {
        header: rtp::Header {
            sequence_number: 156,
        },
        ..Default::default()
    };
    pbuf = np2.marshal().unwrap();
    q.add_packet(&pbuf).unwrap();
    q.get_packet(&mut buf, 156).unwrap();

    for sn in [34u16, 15] {
        let err = q.get_packet(&mut buf, sn);
        require_error_is!(err, Error::PacketTooOld(_));
    }

    assert_eq!(q.grow(), 30);
    let np3 = rtp::Packet {
        header: rtp::Header {
            sequence_number: 127,
        },
        ..Default::default()
    };
    pbuf = np3.marshal().unwrap();
    q.add_packet(&pbuf).unwrap();
    q.get_packet(&mut buf, 127).unwrap();
}
