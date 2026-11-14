//! New for musivum: the port held against the originals, byte for byte.
//!
//! `testdata/reference_output.txt` is what pion's FlexFEC-03 encoder and LiveKit's decoder at
//! the pins give for a set of inputs: the FEC packets of runs of media packets, the round
//! trips of pion's own `flexfec_03_test.go` (which is not ported: it needs pion's decoder),
//! what the parser makes of FEC headers, and what the decoder recovers from streams of packets
//! with losses, duplicates, reordering, corrupted FEC packets and jumps in the sequence
//! numbers.
//! `testdata/reference_output_test.go` printed it, and says how it is run. The test here makes
//! the same inputs in the same order, with the port of the encoder
//! (`crate::webrtc::interceptor::flexfec`) and the decoder here, and expects the same lines.
//!
//! The tests of the original recover through the first packet mask only (windows of at most
//! ten packets); the streams here also go through the second and the third.

use std::collections::HashMap;
use std::fmt::Write;

use crate::protocol::logger;
use crate::webrtc::interceptor::flexfec as pionflexfec;

use super::decoder_test::{
    Rng, TEST_FEC_PT, TEST_FEC_SSRC, TEST_MEDIA_PT, TEST_MEDIA_SSRC, TestDecoder, new_test_decoder,
};
use super::*;

// The lines of the reference output by their names, each to be taken once.
struct Reference(HashMap<&'static str, &'static str>);

impl Reference {
    fn load() -> Self {
        let mut lines = HashMap::new();
        for line in include_str!("testdata/reference_output.txt").lines() {
            let line = line.strip_prefix("REF ").expect("not a line of the output");
            let (name, value) = line.split_once(": ").expect("a line without a name");
            assert!(lines.insert(name, value).is_none(), "{name} is there twice");
        }
        Reference(lines)
    }

    #[track_caller]
    fn take(&mut self, name: &str) -> &'static str {
        self.0
            .remove(name)
            .unwrap_or_else(|| panic!("no line {name} in the reference output"))
    }
}

fn hex_encode_to_string(buf: &[u8]) -> String {
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

// FNV-1a, to keep the lines short.
const REF_HASH_INIT: u64 = 0xcbf29ce484222325;

fn ref_hash(mut h: u64, p: &[u8]) -> u64 {
    for &b in p {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

// media packets as those of makeMediaPackets in decoder_test.go, some of them with a CSRC,
// a header extension or the version 3, and some of about the largest size that the decoder
// takes or just above it
fn ref_media_packets(rng: &mut Rng, base_sn: u16, count: usize) -> Vec<rtp::Packet> {
    let mut packets = Vec::with_capacity(count);
    for i in 0..count {
        let mut size = 100 + rng.intn(900);
        if rng.intn(40) == 0 {
            size = 1470 + rng.intn(40);
        }
        let mut payload = vec![0u8; size];
        rng.read(&mut payload);
        let mut packet = rtp::Packet {
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
        };
        if rng.intn(4) == 0 {
            packet.csrc = vec![rng.uint64() as u32];
        }
        if rng.intn(4) == 0 {
            let mut ext = vec![0u8; 2];
            rng.read(&mut ext);
            packet.set_extension(3, Bytes::from(ext)).unwrap();
        }
        if rng.intn(16) == 0 {
            packet.version = 3;
        }
        packets.push(packet);
    }
    packets
}

// what the encoder gives for a run of media packets
fn encode_line(
    encoder: &mut pionflexfec::FlexEncoder03,
    rng: &mut Rng,
    base_sn: u16,
    count: usize,
    num_fec: u32,
) -> String {
    let media = ref_media_packets(rng, base_sn, count);
    let fec = encoder.encode_fec(&media, num_fec);
    let mut line = format!("{}", fec.len());
    for packet in &fec {
        let raw = packet.marshal().unwrap();
        let head = &packet.payload[..packet.payload.len().min(32)];
        write!(
            line,
            " | {} {} {} {:016x}",
            packet.sequence_number,
            packet.payload.len(),
            hex_encode_to_string(head),
            ref_hash(REF_HASH_INIT, &raw)
        )
        .unwrap();
    }
    line
}

// what the parser makes of a FEC header
fn parse_line(data: Vec<u8>) -> String {
    let fec = match parse_flex_fec03_header(&Bytes::from(data)) {
        Ok(fec) => fec,
        Err(err) => return format!("error: {err}"),
    };
    let seqs = fec.protected_sequences(Vec::new());
    let mut h = REF_HASH_INIT;
    for seq in &seqs {
        h = ref_hash(h, &seq.to_be_bytes());
    }
    format!(
        "{} {} {:x} {:x} {:x} {} | {} {:016x}",
        fec.protected_ssrc,
        fec.seq_num_base,
        fec.mask0,
        fec.mask1,
        fec.mask2,
        fec.payload.len(),
        seqs.len(),
        h
    )
}

// a FEC payload with something wrong in it
fn ref_corrupt(rng: &mut Rng, payload: &[u8]) -> Bytes {
    let mut p = payload.to_vec();
    match rng.intn(10) {
        0 => {
            // truncated
            let n = rng.intn(33);
            p.truncate(n);
        }
        // R bit
        1 => p[0] |= 0x80,
        // F bit
        2 => p[0] |= 0x40,
        // SSRC count
        3 => p[8] = rng.intn(4) as u8,
        4 => {
            // another SSRC
            let i = 12 + rng.intn(4);
            let bit = rng.intn(8);
            p[i] ^= 1 << bit;
        }
        5 => {
            // any bit of the header
            let i = rng.intn(32);
            let bit = rng.intn(8);
            p[i] ^= 1 << bit;
        }
        6 => {
            // another base
            let base = u16::from_be_bytes([p[16], p[17]])
                .wrapping_add(rng.intn(41) as u16)
                .wrapping_sub(20);
            p[16..18].copy_from_slice(&base.to_be_bytes());
        }
        // the first mask is not the last
        7 => p[18] &= 0x7f,
        8 => {
            // another length
            let i = 2 + rng.intn(2);
            let bit = rng.intn(8);
            p[i] ^= 1 << bit;
        }
        _ => {
            // any bit
            let i = rng.intn(p.len());
            let bit = rng.intn(8);
            p[i] ^= 1 << bit;
        }
    }
    Bytes::from(p)
}

// the round trips of pion's flexfec_03_test.go, with LiveKit's decoder in the place of
// pion's: whichever single packet of a run is lost, the FEC packets bring it back
fn round_trip_line(seq_start: u16, seq_len: usize, fec_count: u32) -> String {
    let mut media = Vec::with_capacity(seq_len);
    for i in 0..seq_len {
        let mut packet = rtp::Packet {
            header: rtp::Header {
                marker: true,
                version: 2,
                payload_type: TEST_MEDIA_PT,
                sequence_number: seq_start.wrapping_add(i as u16),
                timestamp: 3653407706,
                ssrc: TEST_MEDIA_SSRC,
                ..Default::default()
            },
            payload: Bytes::from(vec![1, 2, 3, 4, 5, i as u8]),
            ..Default::default()
        };
        packet
            .set_extension(1, Bytes::from_static(&[0xAA, 0xAA]))
            .unwrap();
        media.push(packet);
    }
    let fec =
        pionflexfec::FlexEncoder03::new(TEST_FEC_PT, TEST_FEC_SSRC).encode_fec(&media, fec_count);

    let mut brought_back = 0;
    let mut h = REF_HASH_INIT;
    for lost in 0..media.len() {
        let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());
        let mut recovered = Vec::new();
        for (i, packet) in media.iter().enumerate() {
            if i != lost {
                recovered.extend(decoder.decode_fec(packet));
            }
        }
        let early = recovered.len();
        for packet in &fec {
            recovered.extend(decoder.decode_fec(packet));
        }

        let expected = media[lost].marshal().unwrap();
        for r in &recovered {
            let actual = r.marshal().unwrap();
            h = ref_hash(h, &actual);
            if early == 0 && recovered.len() == 1 && expected == actual {
                brought_back += 1;
            }
        }
    }
    assert_eq!(media.len(), brought_back);
    format!("{} {} {} {:016x}", media.len(), fec.len(), brought_back, h)
}

#[derive(Default)]
struct RefScenario {
    name: &'static str,
    seed: u64,
    // the first media sequence number, and the first of the FEC stream
    base_sn: u16,
    fec_seq_base: u16,
    // windows of minCount to maxCount media packets, each with 1 to maxFEC FEC packets
    windows: usize,
    min_count: usize,
    max_count: usize,
    max_fec: usize,
    // chances in percent: a media packet lost, a FEC packet lost, a packet sent twice, a FEC
    // packet corrupted, a packet swapped with one up to swapSpan places later
    loss_pct: usize,
    fec_loss_pct: usize,
    dup_pct: usize,
    corrupt_pct: usize,
    swap_pct: usize,
    swap_span: usize,
    // after every gapEvery windows the media sequence number jumps by gapMin and up to
    // gapSpan-1 more, after every fecJumpEvery windows the one of the FEC stream by fecJump
    // (0: never)
    gap_every: usize,
    gap_min: u16,
    gap_span: usize,
    fec_jump_every: usize,
    fec_jump: u16,
}

// what the decoder recovers from a stream of packets, and what it is left with
fn scenario_line(sc: &RefScenario) -> String {
    let mut rng = Rng::new(sc.seed as i64);
    let mut encoder = pionflexfec::FlexEncoder03::new(TEST_FEC_PT, TEST_FEC_SSRC);
    let mut decoder = new_test_decoder(TEST_FEC_SSRC, TEST_MEDIA_SSRC, logger::get_logger());

    let mut events: Vec<rtp::Packet> = Vec::new();
    let mut sn = sc.base_sn;
    let mut fec_shift = sc.fec_seq_base.wrapping_sub(1000);
    for w in 0..sc.windows {
        let count = sc.min_count + rng.intn(sc.max_count - sc.min_count + 1);
        let num_fec = 1 + rng.intn(sc.max_fec);
        let media = ref_media_packets(&mut rng, sn, count);
        let fec = encoder.encode_fec(&media, num_fec as u32);

        for packet in &media {
            if rng.intn(100) < sc.loss_pct {
                continue;
            }
            events.push(packet.clone());
            if rng.intn(100) < sc.dup_pct {
                events.push(packet.clone());
            }
        }
        for packet in &fec {
            let mut packet = packet.clone();
            packet.sequence_number = packet.sequence_number.wrapping_add(fec_shift);
            if rng.intn(100) < sc.fec_loss_pct {
                continue;
            }
            if rng.intn(100) < sc.corrupt_pct {
                packet.payload = ref_corrupt(&mut rng, &packet.payload);
            }
            events.push(packet.clone());
            if rng.intn(100) < sc.dup_pct {
                events.push(packet);
            }
        }

        sn = sn.wrapping_add(count as u16);
        if sc.gap_every > 0 && (w + 1) % sc.gap_every == 0 {
            sn = sn
                .wrapping_add(sc.gap_min)
                .wrapping_add(rng.intn(sc.gap_span) as u16);
        }
        if sc.fec_jump_every > 0 && (w + 1) % sc.fec_jump_every == 0 {
            fec_shift = fec_shift.wrapping_add(sc.fec_jump);
        }
    }

    for i in 0..events.len() {
        if rng.intn(100) < sc.swap_pct {
            let j = i + 1 + rng.intn(sc.swap_span);
            if j < events.len() {
                events.swap(i, j);
            }
        }
    }

    // as DecodeFEC of the test decoder, which keeps what arrives and what is recovered, but a
    // recovered packet that cannot be marshalled is not kept
    let mut seqs: Vec<String> = Vec::new();
    let mut h = REF_HASH_INIT;
    for packet in &events {
        if packet.ssrc == TEST_MEDIA_SSRC {
            let raw = packet.marshal().unwrap();
            decoder.media_packets.insert(packet.sequence_number, raw);
        }
        let media_packets = &decoder.media_packets;
        let recovered = decoder.decoder.decode_fec(
            packet,
            Some(&|sequence_number, dst| {
                TestDecoder::get_media_packet(media_packets, sequence_number, dst)
            }),
        );
        for recovered in recovered {
            seqs.push(recovered.sequence_number.to_string());
            let Ok(raw) = recovered.marshal() else {
                h = ref_hash(h, b"E");
                continue;
            };
            h = ref_hash(h, &raw);
            decoder.media_packets.insert(recovered.sequence_number, raw);
        }
    }

    let retained: Vec<String> = decoder
        .received_fec_packets
        .iter()
        .map(|state| state.packet.sequence_number.to_string())
        .collect();
    let stats = decoder.stats();
    format!(
        "{} | {} {} {} {} | {} [{}] {:016x} | [{}]",
        events.len(),
        stats.fec_packets_received,
        stats.fec_bytes_received,
        stats.fec_packets_discarded,
        stats.packets_recovered,
        seqs.len(),
        seqs.join(" "),
        h,
        retained.join(" ")
    )
}

#[test]
fn test_reference_output() {
    let mut reference = Reference::load();

    // the encoder: one that is used again and again, so that its coverage is kept, updated
    // and left as it was (more media packets than it takes), and new ones
    let mut rng = Rng::new(1);
    let mut encoder = pionflexfec::FlexEncoder03::new(TEST_FEC_PT, TEST_FEC_SSRC);
    for (i, (base_sn, count, num_fec)) in [
        (100, 5, 1),
        (105, 5, 1),
        (110, 10, 2),
        (120, 10, 3),
        (65530, 15, 1),
        (200, 16, 1),
        (300, 46, 2),
        (400, 47, 1),
        (500, 109, 2),
        (700, 110, 3),
        (900, 111, 1),
        (1100, 3, 5),
        (1200, 0, 1),
        (1300, 7, 0),
        (1400, 60, 4),
        (65500, 100, 10),
        (2000, 100, 40),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("encode {i}");
        let line = encode_line(&mut encoder, &mut rng, base_sn, count, num_fec);
        assert_eq!(reference.take(&name), line, "{name}");
    }
    let mut encoder = pionflexfec::FlexEncoder03::new(TEST_FEC_PT, TEST_FEC_SSRC);
    assert_eq!(
        reference.take("encode too many"),
        encode_line(&mut encoder, &mut rng, 100, 111, 1)
    );
    let mut encoder = pionflexfec::FlexEncoder03::new(TEST_FEC_PT, TEST_FEC_SSRC);
    assert_eq!(
        reference.take("encode one"),
        encode_line(&mut encoder, &mut rng, 65535, 1, 1)
    );

    for (name, seq_start, seq_len, fec_count) in [
        ("first", 1, 5, 2),
        ("last", 65531, 5, 2),
        ("wrap", 65533, 5, 2),
        ("whole range", 0, 109, 2),
        ("single FEC short window", 42, 8, 1),
        ("dual FEC mask2 coverage", 5, 40, 2),
        ("triple FEC mask3 coverage", 120, 90, 3),
        ("wrap around sequences", 65510, 60, 4),
        ("high media count", 41000, 100, 10),
    ] {
        let name = format!("round trip {name}");
        let line = round_trip_line(seq_start, seq_len, fec_count);
        assert_eq!(reference.take(&name), line, "{name}");
    }

    // the parser
    let mut rng = Rng::new(2);
    for i in 0..200 {
        let mut data = vec![0u8; rng.intn(48)];
        rng.read(&mut data);
        if data.len() > 8 && rng.intn(4) != 0 {
            data[0] &= 0x3f;
            data[8] = 1;
        }
        if data.len() > 26 && rng.intn(2) == 0 {
            data[18] &= 0x7f;
            if rng.intn(2) == 0 {
                data[20] &= 0x7f;
            }
        }
        let name = format!("parse {i}");
        assert_eq!(reference.take(&name), parse_line(data), "{name}");
    }

    // the decoder
    for sc in [
        RefScenario {
            name: "complete",
            seed: 10,
            base_sn: 100,
            fec_seq_base: 1000,
            windows: 20,
            min_count: 3,
            max_count: 12,
            max_fec: 2,
            swap_span: 8,
            ..Default::default()
        },
        RefScenario {
            name: "small",
            seed: 11,
            base_sn: 100,
            fec_seq_base: 1000,
            windows: 60,
            min_count: 3,
            max_count: 12,
            max_fec: 2,
            loss_pct: 10,
            fec_loss_pct: 5,
            dup_pct: 3,
            swap_pct: 10,
            swap_span: 8,
            ..Default::default()
        },
        RefScenario {
            name: "second mask",
            seed: 12,
            base_sn: 5000,
            fec_seq_base: 1000,
            windows: 40,
            min_count: 16,
            max_count: 46,
            max_fec: 3,
            loss_pct: 5,
            fec_loss_pct: 5,
            dup_pct: 2,
            swap_pct: 10,
            swap_span: 8,
            ..Default::default()
        },
        RefScenario {
            name: "third mask",
            seed: 13,
            base_sn: 20000,
            fec_seq_base: 1000,
            windows: 40,
            min_count: 47,
            max_count: 109,
            max_fec: 4,
            loss_pct: 3,
            fec_loss_pct: 5,
            dup_pct: 2,
            swap_pct: 10,
            swap_span: 8,
            ..Default::default()
        },
        RefScenario {
            name: "wrap",
            seed: 14,
            base_sn: 65000,
            fec_seq_base: 65500,
            windows: 60,
            min_count: 5,
            max_count: 40,
            max_fec: 3,
            loss_pct: 8,
            fec_loss_pct: 5,
            dup_pct: 3,
            swap_pct: 15,
            swap_span: 8,
            ..Default::default()
        },
        RefScenario {
            name: "lossy",
            seed: 15,
            base_sn: 300,
            fec_seq_base: 1000,
            windows: 80,
            min_count: 5,
            max_count: 30,
            max_fec: 5,
            loss_pct: 25,
            fec_loss_pct: 20,
            dup_pct: 5,
            swap_pct: 30,
            swap_span: 40,
            ..Default::default()
        },
        RefScenario {
            name: "retained",
            seed: 16,
            base_sn: 60000,
            fec_seq_base: 65300,
            windows: 300,
            min_count: 4,
            max_count: 8,
            max_fec: 2,
            loss_pct: 45,
            fec_loss_pct: 5,
            dup_pct: 2,
            swap_pct: 20,
            swap_span: 20,
            ..Default::default()
        },
        RefScenario {
            name: "corrupt",
            seed: 17,
            base_sn: 40000,
            fec_seq_base: 1000,
            windows: 150,
            min_count: 5,
            max_count: 60,
            max_fec: 3,
            loss_pct: 10,
            fec_loss_pct: 5,
            dup_pct: 3,
            corrupt_pct: 40,
            swap_pct: 10,
            swap_span: 8,
            ..Default::default()
        },
        RefScenario {
            name: "gaps",
            seed: 18,
            base_sn: 100,
            fec_seq_base: 1000,
            windows: 160,
            min_count: 8,
            max_count: 12,
            max_fec: 2,
            loss_pct: 20,
            fec_loss_pct: 5,
            dup_pct: 1,
            swap_pct: 5,
            swap_span: 8,
            gap_every: 7,
            gap_span: 400,
            ..Default::default()
        },
        RefScenario {
            name: "half way",
            seed: 21,
            base_sn: 100,
            fec_seq_base: 1000,
            windows: 400,
            min_count: 8,
            max_count: 12,
            max_fec: 2,
            loss_pct: 30,
            fec_loss_pct: 5,
            dup_pct: 1,
            swap_pct: 5,
            swap_span: 8,
            gap_every: 20,
            gap_min: 0x7ffd,
            gap_span: 7,
            ..Default::default()
        },
        RefScenario {
            name: "fec jumps",
            seed: 19,
            base_sn: 100,
            fec_seq_base: 30000,
            windows: 80,
            min_count: 4,
            max_count: 10,
            max_fec: 2,
            loss_pct: 35,
            fec_loss_pct: 5,
            dup_pct: 1,
            swap_pct: 5,
            swap_span: 8,
            fec_jump_every: 7,
            fec_jump: 0x4100,
            ..Default::default()
        },
        RefScenario {
            name: "sparse",
            seed: 22,
            base_sn: 50000,
            fec_seq_base: 1000,
            windows: 12,
            min_count: 90,
            max_count: 109,
            max_fec: 45,
            loss_pct: 10,
            fec_loss_pct: 5,
            dup_pct: 1,
            swap_pct: 10,
            swap_span: 8,
            ..Default::default()
        },
        RefScenario {
            name: "everything",
            seed: 20,
            base_sn: 64000,
            fec_seq_base: 65000,
            windows: 150,
            min_count: 1,
            max_count: 109,
            max_fec: 6,
            loss_pct: 12,
            fec_loss_pct: 10,
            dup_pct: 4,
            corrupt_pct: 15,
            swap_pct: 20,
            swap_span: 30,
            gap_every: 37,
            gap_span: 300,
            fec_jump_every: 29,
            fec_jump: 0x4100,
            ..Default::default()
        },
    ] {
        let name = format!("scenario {}", sc.name);
        assert_eq!(reference.take(&name), scenario_line(&sc), "{name}");
    }

    let mut left: Vec<_> = reference.0.keys().collect();
    left.sort();
    assert!(
        left.is_empty(),
        "lines of the reference output that nothing was held against: {left:?}"
    );
}
