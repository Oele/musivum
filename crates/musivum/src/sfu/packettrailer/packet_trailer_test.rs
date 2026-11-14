// Ported from LiveKit, pkg/sfu/packettrailer/packet_trailer_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are a loop over the table.

use super::*;

const TAG_TIMESTAMP_US: u8 = 0x01;
const TAG_FRAME_ID: u8 = 0x02;

// appendTLV appends a single XORed TLV element to dst.
fn append_tlv(mut dst: Vec<u8>, tag: u8, value: &[u8]) -> Vec<u8> {
    dst.extend_from_slice(&[tag ^ XOR_BYTE, (value.len() as u8) ^ XOR_BYTE]);
    for b in value {
        dst.push(b ^ XOR_BYTE);
    }
    dst
}

// appendEnvelope appends the 5-byte envelope (XORed trailer_len + magic).
fn append_envelope(mut dst: Vec<u8>, trailer_len: u8) -> Vec<u8> {
    dst.push(trailer_len ^ XOR_BYTE);
    dst.extend_from_slice(&MAGIC[..]);
    dst
}

// makeTrailer builds a complete LKTS trailer with both timestamp and frame_id TLVs.
fn make_trailer(timestamp_us: i64, frame_id: u32) -> Vec<u8> {
    let mut trailer: Vec<u8> = Vec::new();

    let ts_buf: [u8; 8] = (timestamp_us as u64).to_be_bytes();
    trailer = append_tlv(trailer, TAG_TIMESTAMP_US, &ts_buf[..]);

    let fid_buf: [u8; 4] = frame_id.to_be_bytes();
    trailer = append_tlv(trailer, TAG_FRAME_ID, &fid_buf[..]);

    let trailer_len = (trailer.len() + ENVELOPE_SIZE) as u8;
    trailer = append_envelope(trailer, trailer_len);
    trailer
}

// makePayloadWithTrailer builds a fake video payload followed by a full LKTS trailer.
fn make_payload_with_trailer(video_len: usize, timestamp_us: i64, frame_id: u32) -> Vec<u8> {
    let mut video = vec![0u8; video_len];
    for (i, b) in video.iter_mut().enumerate() {
        *b = i as u8;
    }
    video.extend_from_slice(&make_trailer(timestamp_us, frame_id));
    video
}

// makeTimestampOnlyTrailer builds a trailer with only the timestamp TLV.
fn make_timestamp_only_trailer(timestamp_us: i64) -> Vec<u8> {
    let mut trailer: Vec<u8> = Vec::new();
    let ts_buf: [u8; 8] = (timestamp_us as u64).to_be_bytes();
    trailer = append_tlv(trailer, TAG_TIMESTAMP_US, &ts_buf[..]);
    let trailer_len = (trailer.len() + ENVELOPE_SIZE) as u8;
    trailer = append_envelope(trailer, trailer_len);
    trailer
}

#[test]
fn test_strip_trailer() {
    let full_trailer_size: usize = 21; // (1+1+8) + (1+1+4) + 5
    let ts_only_trailer_size: usize = 15; // (1+1+8) + 5

    struct Test {
        name: &'static str,
        payload: Vec<u8>,
        marker: bool,
        want_strip: usize,
    }
    let tests = [
        Test {
            name: "marker set with full trailer (timestamp + frame_id)",
            payload: make_payload_with_trailer(20, 1700000000000000, 42),
            marker: true,
            want_strip: full_trailer_size,
        },
        Test {
            name: "marker set with timestamp-only trailer",
            payload: {
                let mut video = vec![0u8; 20];
                video.extend_from_slice(&make_timestamp_only_trailer(1700000000000000));
                video
            },
            marker: true,
            want_strip: ts_only_trailer_size,
        },
        Test {
            name: "marker not set with valid trailer",
            payload: make_payload_with_trailer(20, 1700000000000000, 42),
            marker: false,
            want_strip: 0,
        },
        Test {
            name: "marker set without magic",
            payload: vec![0u8; 32],
            marker: true,
            want_strip: 0,
        },
        Test {
            name: "marker set but payload too short for envelope",
            payload: vec![0x4C, 0x4B, 0x54, 0x53],
            marker: true,
            want_strip: 0,
        },
        Test {
            name: "marker set with partial magic mismatch",
            payload: {
                let mut p = make_payload_with_trailer(20, 1700000000000000, 42);
                let last = p.len() - 1;
                p[last] = b'x';
                p
            },
            marker: true,
            want_strip: 0,
        },
        Test {
            name: "trailer_len exceeds payload length",
            payload: {
                let mut trailer: Vec<u8> = Vec::new();
                let ts_buf: [u8; 8] = 42u64.to_be_bytes();
                trailer = append_tlv(trailer, TAG_TIMESTAMP_US, &ts_buf[..]);
                trailer = append_envelope(trailer, 200);
                trailer
            },
            marker: true,
            want_strip: 0,
        },
        Test {
            name: "trailer_len smaller than envelope (invalid)",
            payload: {
                let mut video = vec![0u8; 20];
                let mut trailer: Vec<u8> = Vec::new();
                let ts_buf: [u8; 8] = 42u64.to_be_bytes();
                trailer = append_tlv(trailer, TAG_TIMESTAMP_US, &ts_buf[..]);
                trailer = append_envelope(trailer, 3);
                video.extend_from_slice(&trailer);
                video
            },
            marker: true,
            want_strip: 0,
        },
        Test {
            name: "exactly envelope-only trailer",
            payload: append_envelope(Vec::new(), ENVELOPE_SIZE as u8),
            marker: true,
            want_strip: ENVELOPE_SIZE,
        },
        Test {
            name: "empty payload",
            payload: vec![],
            marker: true,
            want_strip: 0,
        },
    ];

    for tt in tests {
        let got = strip_trailer(&tt.payload, tt.marker);
        assert_eq!(
            tt.want_strip, got,
            "{}: StripTrailer() = {}, want {}",
            tt.name, got, tt.want_strip
        );
    }
}
