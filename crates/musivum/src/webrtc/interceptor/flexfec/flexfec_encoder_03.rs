// Ported from pion/interceptor, pkg/flexfec/flexfec_encoder_03.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; where the original returns nil there is an empty
// list; the buffer a media packet is marshalled into is made new (and zero) for each FEC
// packet, where the original takes one from a pool with whatever an earlier use left in it;
// `encode_flex_fec_packet` gives `None` where the original gives an empty packet and false;
// the interface `FlexEncoder` is implemented at the end of the file, by the method of the
// same name.

use crate::webrtc::rtp;

use super::flexfec_coverage::ProtectionCoverage;
use super::flexfec_encoder::{BASE_RTP_HEADER_SIZE, EncoderFactory, FlexEncoder};

/// BaseFec03HeaderSize represents the minium FEC payload's header size including the
/// required first mask.
pub const BASE_FEC03_HEADER_SIZE: usize = 20;
// maxRTPPacketSize represents the maximum size of an RTP packet buffer.
// This is a reasonable upper bound for typical RTP packets.
const MAX_RTP_PACKET_SIZE: usize = 1500;

/// FlexEncoder03 implements the Fec encoding mechanism for the "Flex" variant of FlexFec.
#[derive(Debug)]
pub struct FlexEncoder03 {
    fec_base_sn: u16,
    payload_type: u8,
    ssrc: u32,
    coverage: Option<ProtectionCoverage>,
}

/// FlexEncoder03Factory is a factory for FlexFEC-03 encoders.
#[derive(Clone, Copy, Debug, Default)]
pub struct FlexEncoder03Factory {}

impl EncoderFactory for FlexEncoder03Factory {
    /// NewEncoder creates new FlexFEC-03 encoder.
    fn new_encoder(&self, payload_type: u8, ssrc: u32) -> Box<dyn FlexEncoder> {
        Box::new(FlexEncoder03::new(payload_type, ssrc))
    }
}

impl FlexEncoder03 {
    /// NewFlexEncoder03 creates new FlexFEC-03 encoder.
    pub fn new(payload_type: u8, ssrc: u32) -> Self {
        Self {
            payload_type,
            ssrc,
            fec_base_sn: 1000,
            coverage: None,
        }
    }

    /// EncodeFec returns a list of generated RTP packets with FEC payloads that protect the specified mediaPackets.
    /// This method returns nil in case of missing RTP packets in the mediaPackets array or packets passed out of order.
    pub fn encode_fec(
        &mut self,
        media_packets: &[rtp::Packet],
        num_fec_packets: u32,
    ) -> Vec<rtp::Packet> {
        // Check if mediaPackets is empty
        if media_packets.is_empty() {
            return Vec::new();
        }

        // Check if RTP packets are in order by comparing sequence numbers
        for i in 1..media_packets.len() {
            if media_packets[i].sequence_number
                != media_packets[i - 1].sequence_number.wrapping_add(1)
            {
                // Packets are not in order or there are missing packets
                return Vec::new();
            }
        }

        // Start by defining which FEC packets cover which media packets
        match &mut self.coverage {
            None => self.coverage = ProtectionCoverage::new(media_packets, num_fec_packets),
            Some(coverage) => coverage.update_coverage(media_packets, num_fec_packets),
        }

        if self.coverage.is_none() {
            return Vec::new();
        }

        // Generate FEC payloads
        let mut fec_packets = Vec::with_capacity(num_fec_packets as usize);
        for fec_packet_index in 0..num_fec_packets {
            let fec_packet =
                self.encode_flex_fec_packet(fec_packet_index, media_packets[0].sequence_number);
            if let Some(fec_packet) = fec_packet {
                fec_packets.push(fec_packet);
            }
        }

        fec_packets
    }

    fn encode_flex_fec_packet(
        &mut self,
        fec_packet_index: u32,
        media_base_sn: u16,
    ) -> Option<rtp::Packet> {
        // `EncodeFec` has made sure that there is a coverage.
        let coverage = self.coverage.as_ref()?;
        let mut media_packets = coverage.get_covered_by(fec_packet_index);
        let mask1 = coverage.extract_mask1(fec_packet_index);
        let optional_mask2 = coverage.extract_mask2(fec_packet_index);
        let optional_mask3 = coverage.extract_mask3_03(fec_packet_index);

        /*
            FlexFEC Header Format:
            0                   1                   2                   3
            0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
           |0|0| P|X|  CC  |M| PT recovery |         length recovery       |
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
           |                          TS recovery                          |
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
           |   SSRCCount   |                    reserved                   |
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
           |                             SSRC_i                            |
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
           |           SN base_i           |k|          Mask [0-14]        |
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
           |k|                   Mask [15-45] (optional)                   |
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
           |k|                                                             |
           +-+                   Mask [46-108] (optional)                  |
           |                                                               |
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
           |                     ... next in SSRC_i ...                    |
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        */

        if !media_packets.has_next() {
            return None;
        }

        // Get header size - This depends on the size of the bitmask.
        let mut header_size = BASE_FEC03_HEADER_SIZE;
        if optional_mask2 > 0 || optional_mask3 > 0 {
            header_size += 4;
        }
        if optional_mask3 > 0 {
            header_size += 8;
        }

        // Find the maximum payload size among all media packets
        let mut max_payload_size = 0;
        while let Some(media_packet) = media_packets.next() {
            max_payload_size =
                max_payload_size.max(media_packet.marshal_size() - BASE_RTP_HEADER_SIZE);
        }
        media_packets.reset();

        let mut flex_fec_payload = vec![0u8; header_size + max_payload_size];

        let (flex_fec_header, flex_fec_repair_payload) = flex_fec_payload.split_at_mut(header_size);

        let mut tmp_media_packet_buf = vec![0u8; MAX_RTP_PACKET_SIZE];

        while let Some(media_packet) = media_packets.next() {
            let packet_size = media_packet.marshal_size();
            if packet_size > tmp_media_packet_buf.len() {
                // Packet is too large for our fixed buffer, fallback to dynamic allocation
                tmp_media_packet_buf = vec![0u8; packet_size];
            }

            match media_packet.marshal_to(&mut tmp_media_packet_buf[..packet_size]) {
                Ok(n) if n != 0 => {}
                _ => return None,
            }

            // XOR the first 2 bytes of the header: V, P, X, CC, M, PT fields
            flex_fec_header[0] ^= tmp_media_packet_buf[0];
            flex_fec_header[1] ^= tmp_media_packet_buf[1];

            // Clear the first 2 bits
            flex_fec_header[0] &= 0b00111111;

            // XOR the length recovery field
            let length_recovery_val = (media_packet.marshal_size() - BASE_RTP_HEADER_SIZE) as u16;
            flex_fec_header[2] ^= (length_recovery_val >> 8) as u8;
            flex_fec_header[3] ^= length_recovery_val as u8;

            // XOR the 5th to 8th bytes of the header: the timestamp field
            flex_fec_header[4] ^= tmp_media_packet_buf[4];
            flex_fec_header[5] ^= tmp_media_packet_buf[5];
            flex_fec_header[6] ^= tmp_media_packet_buf[6];
            flex_fec_header[7] ^= tmp_media_packet_buf[7];

            // Process FlexFEC Repair Payload (bytes after RTP header)
            for byte_index in 0..packet_size - BASE_RTP_HEADER_SIZE {
                flex_fec_repair_payload[byte_index] ^=
                    tmp_media_packet_buf[byte_index + BASE_RTP_HEADER_SIZE];
            }
        }

        // Write the SSRC count
        flex_fec_header[8] = 1;

        // Write 0s in reserved
        flex_fec_header[9] = 0;
        flex_fec_header[10] = 0;
        flex_fec_header[11] = 0;

        // Write the SSRC of media packets protected by this FEC packet
        flex_fec_header[12..16].copy_from_slice(&media_packets.first()?.ssrc.to_be_bytes());

        // Write the base SN for the batch of media packets
        flex_fec_header[16..18].copy_from_slice(&media_base_sn.to_be_bytes());

        // Write the bitmasks to the header
        flex_fec_header[18..20].copy_from_slice(&mask1.to_be_bytes());

        if optional_mask2 == 0 && optional_mask3 == 0 {
            flex_fec_header[18] |= 0b10000000;
        } else {
            flex_fec_header[20..24].copy_from_slice(&optional_mask2.to_be_bytes());

            if optional_mask3 == 0 {
                flex_fec_header[20] |= 0b10000000;
            } else {
                flex_fec_header[24..32].copy_from_slice(&optional_mask3.to_be_bytes());
                flex_fec_header[24] |= 0b10000000;
            }
        }

        let packet = rtp::Packet {
            header: rtp::Header {
                version: 2,
                padding: false,
                extension: false,
                marker: false,
                payload_type: self.payload_type,
                sequence_number: self.fec_base_sn,
                timestamp: 54243243,
                ssrc: self.ssrc,
                csrc: vec![],
                ..Default::default()
            },
            payload: flex_fec_payload.into(),
            ..Default::default()
        };
        self.fec_base_sn = self.fec_base_sn.wrapping_add(1);

        Some(packet)
    }
}

// In Go the method above is what makes a `FlexEncoder03` a `FlexEncoder`.
impl FlexEncoder for FlexEncoder03 {
    fn encode_fec(
        &mut self,
        media_packets: &[rtp::Packet],
        num_fec_packets: u32,
    ) -> Vec<rtp::Packet> {
        FlexEncoder03::encode_fec(self, media_packets, num_fec_packets)
    }
}

#[cfg(test)]
#[path = "flexfec_encoder_03_test.rs"]
mod flexfec_encoder_03_test;
