// Ported from pion/interceptor, pkg/flexfec/flexfec_coverage.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; the coverage keeps a copy of the list of media packets
// (the payloads are shared, not copied), where the original keeps the caller's slice;
// `NewCoverage` returning nil is `ProtectionCoverage::new` returning `None`.

use crate::webrtc::rtp;

use super::util;

// Maximum number of media packets that can be protected by a single FEC packet.
// We are not supporting the possibility of having an FEC packet protect multiple
// SSRC source packets for now.
// https://datatracker.ietf.org/doc/html/rfc8627#section-4.2.2.1
pub const MAX_MEDIA_PACKETS: u32 = 110;
pub const MAX_FEC_PACKETS: u32 = MAX_MEDIA_PACKETS;

/// ProtectionCoverage defines the map of RTP packets that individual Fec packets protect.
#[derive(Debug)]
pub struct ProtectionCoverage {
    // Array of masks, each mask capable of covering up to maxMediaPkts = 110.
    // A mask is represented as a grouping of bytes where each individual bit
    // represents the coverage for the media packet at the corresponding index.
    packet_masks: [util::BitArray; MAX_FEC_PACKETS as usize],
    num_fec_packets: u32,
    num_media_packets: u32,
    media_packets: Vec<rtp::Packet>,
}

impl ProtectionCoverage {
    /// NewCoverage returns a new ProtectionCoverage object. numFecPackets represents the number of
    /// Fec packets that we will be generating to cover the list of mediaPackets. This allows us to know
    /// how big the underlying map should be.
    pub fn new(media_packets: &[rtp::Packet], num_fec_packets: u32) -> Option<Self> {
        let num_media_packets = media_packets.len() as u32;

        // Basic sanity checks
        if num_media_packets == 0 || num_media_packets > MAX_MEDIA_PACKETS {
            return None;
        }

        // We allocate the biggest array of bitmasks that respects the max constraints.
        let packet_masks = [util::BitArray::default(); MAX_FEC_PACKETS as usize];

        let mut coverage = Self {
            packet_masks,
            num_fec_packets: 0,
            num_media_packets: 0,
            media_packets: Vec::new(),
        };

        coverage.update_coverage(media_packets, num_fec_packets);

        Some(coverage)
    }

    /// UpdateCoverage updates the ProtectionCoverage object with new bitmasks accounting for the numFecPackets
    /// we want to use to protect the batch media packets.
    pub fn update_coverage(&mut self, media_packets: &[rtp::Packet], num_fec_packets: u32) {
        let num_media_packets = media_packets.len() as u32;

        // Basic sanity checks
        if num_media_packets == 0 || num_media_packets > MAX_MEDIA_PACKETS {
            return;
        }

        self.media_packets = media_packets.to_vec();

        if num_fec_packets == self.num_fec_packets && num_media_packets == self.num_media_packets {
            // We have the same number of FEC packets covering the same number of media packets, we can simply
            // reuse the previous coverage map with the updated media packets.
            return;
        }

        self.num_fec_packets = num_fec_packets;
        self.num_media_packets = num_media_packets;

        // The number of FEC packets and/or the number of packets has changed, we need to update the coverage map
        // to reflect these new values.
        self.reset_coverage();

        // Generate FEC bit mask where numFecPackets FEC packets are covering numMediaPackets Media packets.
        // In the packetMasks array, each FEC packet is represented by a single BitArray, each bit in a given BitArray
        // corresponds to a specific Media packet.
        // Ex: Row I, Col J is set to 1 -> FEC packet I will protect media packet J.
        for fec_packet_index in 0..num_fec_packets {
            // We use an interleaved method to determine coverage. Given N FEC packets, Media packet X will be
            // covered by FEC packet X % N.
            let mut covered_media_packet_index = fec_packet_index;
            while covered_media_packet_index < num_media_packets {
                self.packet_masks[fec_packet_index as usize].set_bit(covered_media_packet_index);
                covered_media_packet_index =
                    covered_media_packet_index.wrapping_add(num_fec_packets);
            }
        }
    }

    // ResetCoverage clears the underlying map so that we can reuse it for new batches of RTP packets.
    fn reset_coverage(&mut self) {
        for i in 0..MAX_FEC_PACKETS {
            self.packet_masks[i as usize].reset();
        }
    }

    /// GetCoveredBy returns an iterator over RTP packets that are protected by the specified Fec packet index.
    pub fn get_covered_by(&self, fec_packet_index: u32) -> util::MediaPacketIterator<'_> {
        let mut coverage = Vec::with_capacity(self.num_media_packets as usize);
        for media_packet_index in 0..self.num_media_packets {
            if self.packet_masks[fec_packet_index as usize].get_bit(media_packet_index) == 1 {
                coverage.push(media_packet_index);
            }
        }

        util::MediaPacketIterator::new(&self.media_packets, coverage)
    }

    /// ExtractMask1 returns the first section of the bitmask as defined by the FEC header.
    /// https://datatracker.ietf.org/doc/html/rfc8627#section-4.2.2.1
    pub fn extract_mask1(&self, fec_packet_index: u32) -> u16 {
        extract_mask1(self.packet_masks[fec_packet_index as usize])
    }

    /// ExtractMask2 returns the second section of the bitmask as defined by the FEC header.
    /// https://datatracker.ietf.org/doc/html/rfc8627#section-4.2.2.1
    pub fn extract_mask2(&self, fec_packet_index: u32) -> u32 {
        extract_mask2(self.packet_masks[fec_packet_index as usize])
    }

    /// ExtractMask3 returns the third section of the bitmask as defined by the FEC header.
    /// https://datatracker.ietf.org/doc/html/rfc8627#section-4.2.2.1
    pub fn extract_mask3(&self, fec_packet_index: u32) -> u64 {
        extract_mask3(self.packet_masks[fec_packet_index as usize])
    }

    /// ExtractMask3_03 returns the third section of the bitmask as defined by the FEC header.
    /// https://datatracker.ietf.org/doc/html/draft-ietf-payload-flexible-fec-scheme-03#section-4.2
    pub fn extract_mask3_03(&self, fec_packet_index: u32) -> u64 {
        extract_mask3_03(self.packet_masks[fec_packet_index as usize])
    }
}

fn extract_mask1(mask: util::BitArray) -> u16 {
    // We get the first 16 bits (64 - 16 -> shift by 48) and we shift once more for K field
    let mask1 = mask.lo >> 49;

    mask1 as u16
}

fn extract_mask2(mask: util::BitArray) -> u32 {
    // We remove the first 15 bits
    let mut mask2 = mask.lo << 15;
    // We get the first 31 bits (64 - 32 -> shift by 32) and we shift once more for K field
    mask2 >>= 33;

    mask2 as u32
}

fn extract_mask3(mask: util::BitArray) -> u64 {
    // We remove the first 46 bits
    let mask_lo = mask.lo << 46;
    let mask_hi = mask.hi >> 18;

    mask_lo | mask_hi
}

fn extract_mask3_03(mask: util::BitArray) -> u64 {
    // We remove the first 46 bits
    let mask_lo = mask.lo << 46;
    let mask_hi = mask.hi >> 18;
    let mut mask3 = mask_lo | mask_hi;
    // We shift once for the K bit.
    mask3 >>= 1;

    mask3
}

#[cfg(test)]
#[path = "flexfec_coverage_test.rs"]
mod flexfec_coverage_test;
