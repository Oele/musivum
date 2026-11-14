// Ported from LiveKit's protocol, datatrack/datatracktest/testutils.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; compiled for tests only.

// Package datatracktest provides helpers for generating data track packets in tests.

use bytes::Bytes;
use rand::RngExt;

use crate::go::time::Duration;
use crate::protocol::datatrack;

pub fn generate_raw_data_packets(
    handle: u16,
    mut seq_num: u16,
    mut frame_num: u16,
    num_frames: usize,
    frame_size: usize,
    frame_duration: Duration,
) -> Vec<Vec<u8>> {
    let mut rng = rand::rng();
    if seq_num == 0 {
        seq_num = (rng.random_range(0..256) + 1) as u16;
    }
    if frame_num == 0 {
        frame_num = (rng.random_range(0..256) + 1) as u16;
    }
    let mut timestamp = rng.random_range(0..1024) as u32;

    let packets_per_frame = frame_size.div_ceil(256); // using 256 bytes of payload per packet
    if packets_per_frame == 0 {
        return Vec::new();
    }
    let num_packets = packets_per_frame * num_frames;
    let mut raw_packets = Vec::with_capacity(num_packets);
    for _ in 0..num_frames {
        let mut remaining_size = frame_size;
        for packet_idx in 0..packets_per_frame {
            let payload_size = remaining_size.min(256);
            let mut payload = vec![0u8; payload_size];
            for (i, b) in payload.iter_mut().enumerate() {
                *b = (255 - i) as u8;
            }
            let mut packet = datatrack::Packet {
                header: datatrack::Header {
                    version: 0,
                    is_start_of_frame: packet_idx == 0,
                    is_final_of_frame: packet_idx == packets_per_frame - 1,
                    handle,
                    sequence_number: seq_num,
                    frame_number: frame_num,
                    timestamp,
                    ..Default::default()
                },
                payload: Bytes::from(payload),
            };
            if let Ok(ext_participant_sid) =
                datatrack::ExtensionParticipantSid::new("test_participant".into())
                && let Ok(ext) = ext_participant_sid.marshal()
            {
                packet.add_extension(ext);
            }
            if let Ok(raw_packet) = packet.marshal() {
                raw_packets.push(raw_packet);
            }
            seq_num = seq_num.wrapping_add(1);
            remaining_size -= payload_size;
        }
        frame_num = frame_num.wrapping_add(1);
        timestamp = timestamp.wrapping_add((90000.0 * frame_duration.seconds()) as u32);
    }

    raw_packets
}
