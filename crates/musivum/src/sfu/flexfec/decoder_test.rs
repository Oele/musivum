// Ported from LiveKit, pkg/sfu/flexfec/decoder_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The FEC packets are made by the port of pion's encoder
// (`crate::webrtc::interceptor::flexfec`). The sizes and the bytes of the media payloads come
// from a small generator of the port's own (`Rng`, seeded as the original seeds Go's
// `math/rand`), since Go's generator is not there: they are not the same as upstream's, and no
// expectation depends on them. The test decoder gives the decoder the lookup into its map of
// media packets with every packet, as the port of the decoder takes it (the original hands
// over a method of itself when it makes the decoder). The subtests are blocks and loops that
// carry their names in the assertion messages; `require.ErrorIs` is a match on the variant of
// the error. The two tests of memory reuse cannot write into the bytes that the decoder has
// seen, as the original does: see there. What reference_test.rs uses of this file is visible
// to it. `decoder_benchmark_test.go` is benchmarks and is left out; the tests use nothing of
// it.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::ops::{Deref, DerefMut};

use crate::protocol::logger;
use crate::webrtc::interceptor::flexfec as pionflexfec;

use super::*;

pub(super) const TEST_FEC_SSRC: u32 = 1234;
pub(super) const TEST_MEDIA_SSRC: u32 = 5678;
pub(super) const TEST_FEC_PT: u8 = 115;
pub(super) const TEST_MEDIA_PT: u8 = 96;

const ERR_TEST_MEDIA_PACKET_NOT_FOUND: &str = "test media packet not found";

pub(super) struct TestDecoder {
    pub(super) decoder: Decoder,
    pub(super) media_packets: HashMap<u16, Vec<u8>>,
}

// The original embeds the decoder.
impl Deref for TestDecoder {
    type Target = Decoder;
    fn deref(&self) -> &Decoder {
        &self.decoder
    }
}

impl DerefMut for TestDecoder {
    fn deref_mut(&mut self) -> &mut Decoder {
        &mut self.decoder
    }
}

pub(super) fn new_test_decoder(
    fec_ssrc: u32,
    protected_ssrc: u32,
    lgr: logger::Logger,
) -> TestDecoder {
    TestDecoder {
        media_packets: HashMap::new(),
        // d.getMediaPacket goes with every call of DecodeFEC, see below
        decoder: Decoder::new(fec_ssrc, protected_ssrc, lgr),
    }
}

impl TestDecoder {
    // A method of the test decoder in the original; here it is handed the map, since the
    // decoder that calls it is borrowed from the test decoder at that time.
    pub(super) fn get_media_packet(
        media_packets: &HashMap<u16, Vec<u8>>,
        sequence_number: u16,
        dst: &mut [u8],
    ) -> Result<usize, MediaPacketLookupError> {
        let Some(packet) = media_packets.get(&sequence_number) else {
            return Err(ERR_TEST_MEDIA_PACKET_NOT_FOUND.into());
        };
        if dst.len() < packet.len() {
            return Err("test media packet buffer too small".into());
        }

        Ok(copy(dst, packet))
    }

    pub(super) fn decode_fec(&mut self, packet: &rtp::Packet) -> Vec<rtp::Packet> {
        if packet.ssrc == self.protected_ssrc {
            let raw = packet.marshal().unwrap();
            self.media_packets.insert(packet.sequence_number, raw);
        }

        let media_packets = &self.media_packets;
        let recovered = self.decoder.decode_fec(
            packet,
            Some(&|sequence_number, dst| {
                Self::get_media_packet(media_packets, sequence_number, dst)
            }),
        );
        for recovered_packet in &recovered {
            let raw = recovered_packet.marshal().unwrap();
            self.media_packets
                .insert(recovered_packet.sequence_number, raw);
        }
        recovered
    }
}

/// What the original uses of Go's `rand.New(rand.NewSource(seed))`: numbers that follow from
/// the seed. The generator is SplitMix64, not Go's, so the numbers are not Go's.
pub(super) struct Rng(u64);

impl Rng {
    pub(super) fn new(seed: i64) -> Self {
        Self(seed as u64)
    }

    pub(super) fn uint64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// `Intn`: a number from 0 up to, not including, `n`.
    pub(super) fn intn(&mut self, n: usize) -> usize {
        (self.uint64() % n as u64) as usize
    }

    /// `Read`: fills `p` with random bytes.
    pub(super) fn read(&mut self, p: &mut [u8]) {
        for b in p {
            *b = self.uint64() as u8;
        }
    }
}

// Go's `rand.Intn(n)` of the generator that is not seeded by the test: another number each
// run. Every `RandomState` of the standard library has keys of its own, which start from the
// operating system's randomness; the hash of nothing under them is the number.
fn rand_intn(n: usize) -> usize {
    (std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
        % n as u64) as usize
}

fn make_media_packets(base_sn: u16, count: usize) -> Vec<rtp::Packet> {
    let mut rng = Rng::new(base_sn as i64);
    let mut packets = Vec::with_capacity(count);
    for i in 0..count {
        let mut payload = vec![0u8; 100 + rng.intn(900)];
        rng.read(&mut payload);
        packets.push(rtp::Packet {
            header: rtp::Header {
                version: 2,
                payload_type: TEST_MEDIA_PT,
                sequence_number: base_sn.wrapping_add(i as u16),
                timestamp: 3000u32.wrapping_mul(i as u32),
                ssrc: TEST_MEDIA_SSRC,
                marker: i == count - 1,
                ..Default::default()
            },
            payload: Bytes::from(payload),
            ..Default::default()
        });
    }
    packets
}

#[track_caller]
fn encode_fec(media_packets: &[rtp::Packet], num_fec: u32) -> Vec<rtp::Packet> {
    let mut encoder = pionflexfec::FlexEncoder03::new(TEST_FEC_PT, TEST_FEC_SSRC);
    let fec_packets = encoder.encode_fec(media_packets, num_fec);
    assert!(!fec_packets.is_empty());
    fec_packets
}

#[track_caller]
fn require_packet_equal(expected: &rtp::Packet, actual: &rtp::Packet) {
    assert_eq!(expected.sequence_number, actual.sequence_number);
    assert_eq!(expected.timestamp, actual.timestamp);
    assert_eq!(expected.payload_type, actual.payload_type);
    assert_eq!(expected.ssrc, actual.ssrc);
    assert_eq!(expected.marker, actual.marker);
    assert_eq!(expected.payload, actual.payload);
}

#[test]
fn test_decoder_recovers_single_loss() {
    let media = make_media_packets(100, 5);
    let fec = encode_fec(&media, 1);

    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    // drop media[2], feed the rest
    let mut recovered = Vec::new();
    for (i, packet) in media.iter().enumerate() {
        if i == 2 {
            continue;
        }
        recovered.extend(decoder.decode_fec(packet));
    }
    assert!(recovered.is_empty());

    for packet in &fec {
        recovered.extend(decoder.decode_fec(packet));
    }

    assert_eq!(1, recovered.len());
    require_packet_equal(&media[2], &recovered[0]);
    assert!(decoder.received_fec_packets.is_empty());

    let stats = decoder.stats();
    assert_eq!(fec.len() as u64, stats.fec_packets_received);
    assert_eq!(1u64, stats.packets_recovered);
    assert_eq!(0u64, stats.fec_packets_discarded);
}

#[test]
fn test_decoder_recovers_packet_with_extended_header() {
    let mut media = make_media_packets(150, 5);
    for (i, packet) in media.iter_mut().enumerate() {
        packet.csrc = vec![(1000 + i) as u32];
        assert_eq!(
            Ok(()),
            packet.set_extension(3, Bytes::from(vec![i as u8, (i + 1) as u8]))
        );
    }
    let fec = encode_fec(&media, 1);
    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    for (i, packet) in media.iter().enumerate() {
        if i != 2 {
            assert!(decoder.decode_fec(packet).is_empty());
        }
    }
    let recovered = decoder.decode_fec(&fec[0]);
    assert_eq!(1, recovered.len());

    let expected_raw = media[2].marshal();
    assert!(expected_raw.is_ok());
    let actual_raw = recovered[0].marshal();
    assert!(actual_raw.is_ok());
    assert_eq!(expected_raw, actual_raw);
}

#[test]
fn test_decoder_recovers_with_late_media() {
    // FEC arrives while two packets are missing; recovery happens once one of
    // them shows up late. Exercises retained FEC state and packet lookup.
    let media = make_media_packets(200, 5);
    let fec = encode_fec(&media, 1);

    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    let mut recovered = Vec::new();
    for i in [0, 3, 4] {
        recovered.extend(decoder.decode_fec(&media[i]));
    }
    for packet in &fec {
        recovered.extend(decoder.decode_fec(packet));
    }
    // two packets missing from the protected window, nothing recoverable yet
    assert!(recovered.is_empty());
    assert_eq!(1, decoder.received_fec_packets.len());

    // late arrival of media[1] leaves only media[2] missing
    let recovered = decoder.decode_fec(&media[1]);
    assert_eq!(1, recovered.len());
    require_packet_equal(&media[2], &recovered[0]);
    assert!(decoder.received_fec_packets.is_empty());
}

#[test]
fn test_decoder_recovers_multiple_windows() {
    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());
    let mut encoder = pionflexfec::FlexEncoder03::new(TEST_FEC_PT, TEST_FEC_SSRC);

    let mut all_recovered = Vec::new();
    let mut dropped: HashMap<u16, rtp::Packet> = HashMap::new();
    let mut base_sn: u16 = 1000;
    for window in 0..10 {
        let media = make_media_packets(base_sn, 10);
        let fec_packets = encoder.encode_fec(&media, 1);
        assert!(!fec_packets.is_empty());

        let drop_idx = window % 10;
        for (i, packet) in media.iter().enumerate() {
            if i == drop_idx {
                dropped.insert(packet.sequence_number, packet.clone());
                continue;
            }
            all_recovered.extend(decoder.decode_fec(packet));
        }
        for packet in &fec_packets {
            all_recovered.extend(decoder.decode_fec(packet));
        }
        base_sn += 10;
    }

    assert_eq!(10, all_recovered.len());
    for rec in &all_recovered {
        let expected = dropped.get(&rec.sequence_number);
        assert!(
            expected.is_some(),
            "recovered unexpected sequence number {}",
            rec.sequence_number
        );
        require_packet_equal(expected.unwrap(), rec);
    }
    assert_eq!(10u64, decoder.stats().packets_recovered);
}

#[test]
fn test_decoder_sequence_number_wrap() {
    let media = make_media_packets(65533, 5); // spans 65533..1
    let fec = encode_fec(&media, 1);

    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    let mut recovered = Vec::new();
    for (i, packet) in media.iter().enumerate() {
        if i == 3 {
            // sequence number 0
            continue;
        }
        recovered.extend(decoder.decode_fec(packet));
    }
    for packet in &fec {
        recovered.extend(decoder.decode_fec(packet));
    }

    assert_eq!(1, recovered.len());
    require_packet_equal(&media[3], &recovered[0]);
}

#[test]
fn test_decoder_fec_window_order() {
    let media = make_media_packets(50, 5);
    let mut fec = encode_fec(&media, 1)[0].clone();
    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    for i in [0, 3, 4] {
        assert!(decoder.decode_fec(&media[i]).is_empty());
    }
    for seq in [102u16, 100, 101] {
        fec.sequence_number = seq;
        assert!(decoder.decode_fec(&fec).is_empty());
    }

    assert_eq!(3, decoder.received_fec_packets.len());
    for (i, seq) in [100u16, 101, 102].into_iter().enumerate() {
        assert_eq!(seq, decoder.received_fec_packets[i].packet.sequence_number);
    }
}

#[test]
fn test_decoder_fec_sequence_number_wrap() {
    let media = make_media_packets(75, 5);
    let mut fec = encode_fec(&media, 1)[0].clone();
    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    for i in [0, 3, 4] {
        assert!(decoder.decode_fec(&media[i]).is_empty());
    }
    for sequence_number in [65535u16, 0] {
        fec.sequence_number = sequence_number;
        assert!(decoder.decode_fec(&fec).is_empty());
    }

    assert_eq!(2, decoder.received_fec_packets.len());
    assert_eq!(
        65535u16,
        decoder.received_fec_packets[0].packet.sequence_number
    );
    assert_eq!(0u16, decoder.received_fec_packets[1].packet.sequence_number);

    let recovered = decoder.decode_fec(&media[1]);
    assert_eq!(1, recovered.len());
    require_packet_equal(&media[2], &recovered[0]);
    assert!(decoder.received_fec_packets.is_empty());
}

#[test]
fn test_decoder_bounds_retained_fec_state() {
    let media = make_media_packets(90, 5);
    let mut fec = encode_fec(&media, 1)[0].clone();
    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    for i in [0, 3, 4] {
        assert!(decoder.decode_fec(&media[i]).is_empty());
    }
    for sequence_number in 0..(MAX_FEC_PACKETS + 5) as u16 {
        fec.sequence_number = sequence_number;
        assert!(decoder.decode_fec(&fec).is_empty());
    }

    assert_eq!(MAX_FEC_PACKETS, decoder.received_fec_packets.len());
    assert_eq!(5u16, decoder.received_fec_packets[0].packet.sequence_number);
    assert_eq!(
        (MAX_FEC_PACKETS + 4) as u16,
        decoder.received_fec_packets[MAX_FEC_PACKETS - 1]
            .packet
            .sequence_number
    );
}

#[test]
fn test_decoder_discards_stale_fec_state() {
    let media = make_media_packets(95, 5);
    let mut fec = encode_fec(&media, 1)[0].clone();
    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    for i in [0, 3, 4] {
        assert!(decoder.decode_fec(&media[i]).is_empty());
    }
    fec.sequence_number = 1;
    assert!(decoder.decode_fec(&fec).is_empty());
    fec.sequence_number = 0x4001;
    assert!(decoder.decode_fec(&fec).is_empty());

    assert_eq!(1, decoder.received_fec_packets.len());
    assert_eq!(
        0x4001u16,
        decoder.received_fec_packets[0].packet.sequence_number
    );
}

#[test]
fn test_decoder_discards_foreign_protected_ssrc() {
    let media = make_media_packets(300, 5);
    let fec = encode_fec(&media, 1);

    // decoder bound to a different protected stream
    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC + 1, logger::get_logger());
    let recovered = decoder.decode_fec(&fec[0]);
    assert!(recovered.is_empty());

    let stats = decoder.stats();
    assert_eq!(1u64, stats.fec_packets_received);
    assert_eq!(1u64, stats.fec_packets_discarded);
}

#[test]
fn test_decoder_discards_malformed_fec() {
    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    for payload in [
        vec![],
        vec![0x00],
        vec![0u8; 10],
        {
            // R bit set
            let mut p = vec![0u8; pionflexfec::BASE_FEC03_HEADER_SIZE + 4];
            p[0] = 0x80;
            p[8] = 1;
            p
        },
        {
            // multiple protected ssrcs
            let mut p = vec![0u8; pionflexfec::BASE_FEC03_HEADER_SIZE + 4];
            p[8] = 2;
            p
        },
        {
            // empty mask
            let mut p = vec![0u8; pionflexfec::BASE_FEC03_HEADER_SIZE + 4];
            p[8] = 1;
            p[12..16].copy_from_slice(&TEST_MEDIA_SSRC.to_be_bytes());
            p[18..20].copy_from_slice(&0x8000u16.to_be_bytes());
            p
        },
    ] {
        let pkt = rtp::Packet {
            header: rtp::Header {
                version: 2,
                payload_type: TEST_FEC_PT,
                sequence_number: rand_intn(65536) as u16,
                ssrc: TEST_FEC_SSRC,
                ..Default::default()
            },
            payload: Bytes::from(payload),
            ..Default::default()
        };
        // require.NotPanics: a panic fails the test
        assert!(decoder.decode_fec(&pkt).is_empty());
    }

    let stats = decoder.stats();
    assert_eq!(6u64, stats.fec_packets_received);
    assert_eq!(6u64, stats.fec_packets_discarded);
}

#[test]
fn test_parse_flex_fec03_header_optional_masks() {
    let make_header = |size: usize| -> Vec<u8> {
        let mut data = vec![0u8; size];
        data[8] = 1;
        data[12..16].copy_from_slice(&TEST_MEDIA_SSRC.to_be_bytes());
        data[16..18].copy_from_slice(&500u16.to_be_bytes());
        data
    };

    {
        let name = "first mask";
        let mut data = make_header(21);
        data[18..20].copy_from_slice(&0x8001u16.to_be_bytes());
        data[20] = 0xaa;

        let fec = parse_flex_fec03_header(&Bytes::from(data));
        assert!(fec.is_ok(), "{name}: {fec:?}");
        let fec = fec.unwrap();
        assert_eq!(1u16, fec.mask0, "{name}");
        assert_eq!(0, fec.mask1, "{name}");
        assert_eq!(0, fec.mask2, "{name}");
        assert_eq!(&[0xaa][..], &fec.payload[..], "{name}");
        assert_eq!(vec![514u16], fec.protected_sequences(Vec::new()), "{name}");
    }

    {
        let name = "second mask";
        let mut data = make_header(24);
        data[18..20].copy_from_slice(&1u16.to_be_bytes());
        data[20..24].copy_from_slice(&0x80000001u32.to_be_bytes());

        let fec = parse_flex_fec03_header(&Bytes::from(data));
        assert!(fec.is_ok(), "{name}: {fec:?}");
        let fec = fec.unwrap();
        assert_eq!(1u16, fec.mask0, "{name}");
        assert_eq!(1u32, fec.mask1, "{name}");
        assert_eq!(0, fec.mask2, "{name}");
        assert_eq!(
            vec![514u16, 545],
            fec.protected_sequences(Vec::new()),
            "{name}"
        );
    }

    {
        let name = "third mask";
        let mut data = make_header(32);
        data[18..20].copy_from_slice(&1u16.to_be_bytes());
        data[20..24].copy_from_slice(&1u32.to_be_bytes());
        data[24..32].copy_from_slice(&0x8000000000000001u64.to_be_bytes());

        let fec = parse_flex_fec03_header(&Bytes::from(data));
        assert!(fec.is_ok(), "{name}: {fec:?}");
        let fec = fec.unwrap();
        assert_eq!(1u16, fec.mask0, "{name}");
        assert_eq!(1u32, fec.mask1, "{name}");
        assert_eq!(1u64, fec.mask2, "{name}");
        assert_eq!(
            vec![514u16, 545, 608],
            fec.protected_sequences(Vec::new()),
            "{name}"
        );
    }
}

#[test]
fn test_parse_flex_fec03_header_rejects_invalid_optional_masks() {
    let make_header = |size: usize| -> Vec<u8> {
        let mut data = vec![0u8; size];
        data[8] = 1;
        data
    };

    struct Test {
        name: &'static str,
        data: Vec<u8>,
        // require.ErrorIs(t, err, test.err)
        err: fn(&Error) -> bool,
    }
    let tests = [
        Test {
            name: "inflexible matrix",
            data: {
                let mut data = make_header(20);
                data[0] = FEC_INFLEXIBLE_BIT;
                data
            },
            err: |err| matches!(err, Error::InflexibleGeneratorMatrix),
        },
        Test {
            name: "truncated second mask",
            data: make_header(23),
            err: |err| matches!(err, Error::PacketTruncated(_)),
        },
        Test {
            name: "truncated third mask",
            data: make_header(31),
            err: |err| matches!(err, Error::PacketTruncated(_)),
        },
        Test {
            name: "unterminated third mask",
            data: make_header(32),
            err: |err| matches!(err, Error::LastOptionalMaskKBitSetToFalse),
        },
    ];

    for test in tests {
        let result = parse_flex_fec03_header(&Bytes::from(test.data));
        assert!(
            result.as_ref().is_err_and(test.err),
            "{}: {result:?}",
            test.name
        );
    }
}

#[test]
fn test_decoder_discards_duplicate_fec() {
    let media = make_media_packets(400, 5);
    let fec = encode_fec(&media, 1);

    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());
    for i in [0, 3, 4] {
        decoder.decode_fec(&media[i]);
    }
    assert!(decoder.decode_fec(&fec[0]).is_empty());
    assert!(decoder.decode_fec(&fec[0]).is_empty());

    let stats = decoder.stats();
    assert_eq!(2u64, stats.fec_packets_received);
    assert_eq!(1u64, stats.fec_packets_discarded);
}

#[test]
fn test_decoder_does_not_retain_complete_fec_state() {
    let media = make_media_packets(450, 5);
    let fec = encode_fec(&media, 1);

    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());
    for packet in &media {
        assert!(decoder.decode_fec(packet).is_empty());
    }

    assert!(decoder.decode_fec(&fec[0]).is_empty());
    assert!(decoder.received_fec_packets.is_empty());
}

// The original marshals the packet, unmarshals it into a scratch packet, hands that to the
// decoder and then overwrites the bytes of the scratch packet's payload, to show that the
// decoder kept no reference to memory of its caller. Here the bytes a packet is unmarshalled
// from are `Bytes`: what the decoder keeps of them it shares with the caller, and nobody can
// write to them, so there is nothing to overwrite. What is left of the clobbering is that the
// scratch packet gets another payload after each packet.
fn feed(
    decoder: &mut TestDecoder,
    scratch: &mut rtp::Packet,
    src: &rtp::Packet,
) -> Vec<rtp::Packet> {
    let buf = src.marshal();
    assert!(buf.is_ok());
    assert_eq!(Ok(()), scratch.unmarshal(&Bytes::from(buf.unwrap())));
    let out = decoder.decode_fec(scratch);
    // clobber the scratch memory the decoder saw
    scratch.payload = Bytes::from(vec![0xde; scratch.payload.len()]);
    out
}

#[test]
fn test_decoder_input_memory_reuse() {
    // the decoder must not retain references to caller-owned packet memory
    let media = make_media_packets(500, 5);
    let fec = encode_fec(&media, 1);

    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    let mut scratch = rtp::Packet::default();

    let mut recovered = Vec::new();
    for (i, packet) in media.iter().enumerate() {
        if i == 2 {
            continue;
        }
        recovered.extend(feed(&mut decoder, &mut scratch, packet));
    }
    for packet in &fec {
        recovered.extend(feed(&mut decoder, &mut scratch, packet));
    }

    assert_eq!(1, recovered.len());
    require_packet_equal(&media[2], &recovered[0]);
}

#[test]
fn test_decoder_retained_fec_memory_reuse() {
    let media = make_media_packets(550, 5);
    let fec = encode_fec(&media, 1);

    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());
    let mut scratch = rtp::Packet::default();

    for i in [0, 3, 4] {
        assert!(feed(&mut decoder, &mut scratch, &media[i]).is_empty());
    }
    assert!(feed(&mut decoder, &mut scratch, &fec[0]).is_empty());
    assert_eq!(1, decoder.received_fec_packets.len());

    let recovered = feed(&mut decoder, &mut scratch, &media[1]);
    assert_eq!(1, recovered.len());
    require_packet_equal(&media[2], &recovered[0]);
}

#[test]
fn test_decoder_two_fec_packets_two_losses() {
    // with 2 FEC packets over 10 media packets, the coverage interleaves, so
    // two losses landing in different coverage groups are both recoverable
    let media = make_media_packets(600, 10);
    let fec = encode_fec(&media, 2);
    assert_eq!(2, fec.len());

    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    let mut recovered = Vec::new();
    for (i, packet) in media.iter().enumerate() {
        if i == 2 || i == 3 {
            continue;
        }
        recovered.extend(decoder.decode_fec(packet));
    }
    for packet in &fec {
        recovered.extend(decoder.decode_fec(packet));
    }

    let mut recovered_sns: HashMap<u16, bool> = HashMap::new();
    for r in &recovered {
        recovered_sns.insert(r.sequence_number, true);
    }
    assert_eq!(2, recovered.len());
    for r in &recovered {
        let expected_idx = r.sequence_number.wrapping_sub(600) as usize;
        require_packet_equal(&media[expected_idx], r);
    }
    assert!(recovered_sns.get(&602).copied().unwrap_or(false));
    assert!(recovered_sns.get(&603).copied().unwrap_or(false));
}

#[test]
fn test_decoder_resets_on_big_sequence_gap() {
    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    let media = make_media_packets(100, 110);
    for packet in &media {
        decoder.decode_fec(packet);
    }

    // jump far ahead, decoder should reset its windows rather than misuse
    // stale state
    let far_media = make_media_packets(30000, 5);
    let fec = encode_fec(&far_media, 1);
    let mut recovered = Vec::new();
    for (i, packet) in far_media.iter().enumerate() {
        if i == 1 {
            continue;
        }
        recovered.extend(decoder.decode_fec(packet));
    }
    for packet in &fec {
        recovered.extend(decoder.decode_fec(packet));
    }
    assert_eq!(1, recovered.len());
    require_packet_equal(&far_media[1], &recovered[0]);
}
