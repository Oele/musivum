// Ported from LiveKit, pkg/sfu/testutils/rtptest/packet.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the sizes and the bytes of the payloads come from a
// small generator of the port's own (`Rng`, seeded as the original seeds Go's `math/rand`),
// since Go's generator is not there: the packets are as deterministic as the original's, but
// their sizes and bytes are not the same as upstream's for the same arguments.

use bytes::Bytes;

use crate::webrtc::rtp;

/// GenerateVP8Packets returns deterministic RTP packets with valid VP8 payload descriptors.
pub fn generate_vp8_packets(
    base_sn: u16,
    count: usize,
    payload_type: u8,
    ssrc: u32,
) -> Vec<rtp::Packet> {
    let mut rng = Rng::new(base_sn as i64);
    let mut packets = Vec::with_capacity(count);
    for i in 0..count {
        let mut payload = vec![0u8; 50 + rng.intn(200)];
        rng.read(&mut payload);
        payload[0] = 0x10;

        let sequence_number = base_sn.wrapping_add(i as u16);
        packets.push(rtp::Packet {
            header: rtp::Header {
                version: 2,
                payload_type,
                sequence_number,
                timestamp: 90000u32.wrapping_add(3000u32.wrapping_mul(sequence_number as u32)),
                ssrc,
                marker: i == count - 1,
                ..Default::default()
            },
            payload: Bytes::from(payload),
            ..Default::default()
        });
    }
    packets
}

/// What the original uses of Go's `rand.New(rand.NewSource(seed))`: numbers that follow from
/// the seed. The generator is SplitMix64, not Go's, so the numbers are not Go's.
struct Rng(u64);

impl Rng {
    fn new(seed: i64) -> Self {
        Self(seed as u64)
    }

    fn uint64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// `Intn`: a number from 0 up to, not including, `n`.
    fn intn(&mut self, n: usize) -> usize {
        (self.uint64() % n as u64) as usize
    }

    /// `Read`: fills `p` with random bytes.
    fn read(&mut self, p: &mut [u8]) {
        for b in p {
            *b = self.uint64() as u8;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no tests. What the tests that use the packets rely
    // on: consecutive sequence numbers from the base (also across the wrap), the marker on
    // the last packet only, a VP8 payload descriptor at the start of every payload, and the
    // same packets for the same base.
    #[test]
    fn the_packets_are_a_deterministic_run_from_the_base() {
        let packets = generate_vp8_packets(65533, 5, 96, 5678);
        assert_eq!(5, packets.len());
        for (i, packet) in packets.iter().enumerate() {
            let sequence_number = 65533u16.wrapping_add(i as u16);
            assert_eq!(2, packet.version, "packet {i}");
            assert_eq!(96, packet.payload_type, "packet {i}");
            assert_eq!(5678, packet.ssrc, "packet {i}");
            assert_eq!(sequence_number, packet.sequence_number, "packet {i}");
            assert_eq!(
                90000 + 3000 * sequence_number as u32,
                packet.timestamp,
                "packet {i}"
            );
            assert_eq!(i == 4, packet.marker, "packet {i}");
            assert!(
                (50..250).contains(&packet.payload.len()),
                "packet {i}: {} bytes",
                packet.payload.len()
            );
            assert_eq!(0x10, packet.payload[0], "packet {i}");
        }
        assert_eq!(
            [65533, 65534, 65535, 0, 1],
            *packets
                .iter()
                .map(|packet| packet.sequence_number)
                .collect::<Vec<_>>()
        );

        assert_eq!(packets, generate_vp8_packets(65533, 5, 96, 5678));
        assert_ne!(
            packets[0].payload,
            generate_vp8_packets(65534, 5, 96, 5678)[0].payload
        );
        assert!(generate_vp8_packets(100, 0, 96, 5678).is_empty());
    }
}
