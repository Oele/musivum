// Ported from pion/rtp, codecs/vp9_packet.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; only `VP9Packet` is here: `VP9Payloader` and the
// deprecated `VP9PartitionHeadChecker` are left out. `payload` is a copy of the payload, in a
// buffer that is kept when the packet is used again; the slice of the parsed packet that the
// original keeps there is what `unmarshal` returns. A `&[u8]` is never nil, so
// `Error::NilPacket` is never returned. The parse functions return the new position or the
// error, not both. The lists of a Picture Group's reference indices (`pgp_diff`) are not kept
// beyond the length of the list of lists, as the original does in the capacity of its slice.
// `VP9Packet` embeds `videoDepacketizer` in Go: here it holds one and dereferences to it, so
// that `packet.is_partition_tail(..)` reads the same.

use std::ops::{Deref, DerefMut};

use super::Error;
use super::common::{VideoDepacketizer, resize_uint16_slice};

const MAX_SPATIAL_LAYERS: u8 = 5;
const MAX_VP9_REF_PICS: usize = 3;

// VP9Packet represents the VP9 header that is stored in the payload of an RTP Packet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Vp9Packet {
    // Required header
    pub i: bool, // PictureID is present
    pub p: bool, // Inter-picture predicted frame
    pub l: bool, // Layer indices is present
    pub f: bool, // Flexible mode
    pub b: bool, // Start of a frame
    pub e: bool, // End of a frame
    pub v: bool, // Scalability structure (SS) data present
    pub z: bool, // Not a reference frame for upper spatial layers

    // Recommended headers
    pub picture_id: u16, // 7 or 16 bits, picture ID

    // Conditionally recommended headers
    pub tid: u8, // Temporal layer ID
    pub u: bool, // Switching up point
    pub sid: u8, // Spatial layer ID
    pub d: bool, // Inter-layer dependency used

    // Conditionally required headers
    pub p_diff: Vec<u8>, // Reference index (F=1)
    pub tl0picidx: u8,   // Temporal layer zero index (F=0)

    // Scalability structure headers
    pub ns: u8, // N_S + 1 indicates the number of spatial layers present in the VP9 stream
    pub y: bool, // Each spatial layer's frame resolution present
    pub g: bool, // PG description present flag.
    pub ng: u8, // N_G indicates the number of pictures in a Picture Group (PG)
    pub width: Vec<u16>,
    pub height: Vec<u16>,
    pub pgtid: Vec<u8>,         // Temporal layer ID of pictures in a Picture Group
    pub pgu: Vec<bool>,         // Switching up point of pictures in a Picture Group
    pub pgp_diff: Vec<Vec<u8>>, // Reference indecies of pictures in a Picture Group

    pub payload: Vec<u8>,

    pub video_depacketizer: VideoDepacketizer,
}

impl Deref for Vp9Packet {
    type Target = VideoDepacketizer;
    fn deref(&self) -> &VideoDepacketizer {
        &self.video_depacketizer
    }
}

impl DerefMut for Vp9Packet {
    fn deref_mut(&mut self) -> &mut VideoDepacketizer {
        &mut self.video_depacketizer
    }
}

impl Vp9Packet {
    // Unmarshal parses the passed byte slice and stores the result in the VP9Packet this method is called upon.
    pub fn unmarshal<'a>(&mut self, packet: &'a [u8]) -> Result<&'a [u8], Error> {
        // The original returns errNilPacket for a nil packet here. A `&[u8]` is never nil.
        if packet.is_empty() {
            return Err(Error::ShortPacket);
        }

        self.i = packet[0] & 0x80 != 0;
        self.p = packet[0] & 0x40 != 0;
        self.l = packet[0] & 0x20 != 0;
        self.f = packet[0] & 0x10 != 0;
        self.b = packet[0] & 0x08 != 0;
        self.e = packet[0] & 0x04 != 0;
        self.v = packet[0] & 0x02 != 0;
        self.z = packet[0] & 0x01 != 0;

        let mut pos = 1;

        if self.i {
            pos = self.parse_picture_id(packet, pos)?;
        }

        if self.l {
            pos = self.parse_layer_info(packet, pos)?;
        }

        if self.f && self.p {
            pos = self.parse_ref_indices(packet, pos)?;
        }

        if self.v {
            pos = self.parse_ss_data(packet, pos)?;
        }

        // p.Payload = packet[pos:]
        self.payload.clear();
        self.payload.extend_from_slice(&packet[pos..]);

        Ok(&packet[pos..])
    }

    // Picture ID:
    /*
     *      +-+-+-+-+-+-+-+-+
     * I:   |M| PICTURE ID  |   M:0 => picture id is 7 bits.
     *      +-+-+-+-+-+-+-+-+   M:1 => picture id is 15 bits.
     * M:   | EXTENDED PID  |
     *      +-+-+-+-+-+-+-+-+
     **/
    // .
    fn parse_picture_id(&mut self, packet: &[u8], mut pos: usize) -> Result<usize, Error> {
        if packet.len() <= pos {
            return Err(Error::ShortPacket);
        }

        self.picture_id = (packet[pos] & 0x7F) as u16;
        if packet[pos] & 0x80 != 0 {
            pos += 1;
            if packet.len() <= pos {
                return Err(Error::ShortPacket);
            }
            self.picture_id = (self.picture_id << 8) | packet[pos] as u16;
        }
        pos += 1;

        Ok(pos)
    }

    fn parse_layer_info(&mut self, packet: &[u8], pos: usize) -> Result<usize, Error> {
        let pos = self.parse_layer_info_common(packet, pos)?;

        if self.f {
            return Ok(pos);
        }

        self.parse_layer_info_non_flexible_mode(packet, pos)
    }

    // Layer indices (flexible mode):
    /*
     *      +-+-+-+-+-+-+-+-+
     * L:   |  T  |U|  S  |D|
     *      +-+-+-+-+-+-+-+-+
     **/
    // .
    fn parse_layer_info_common(&mut self, packet: &[u8], mut pos: usize) -> Result<usize, Error> {
        if packet.len() <= pos {
            return Err(Error::ShortPacket);
        }

        self.tid = packet[pos] >> 5;
        self.u = packet[pos] & 0x10 != 0;
        self.sid = (packet[pos] >> 1) & 0x7;
        self.d = packet[pos] & 0x01 != 0;

        if self.sid >= MAX_SPATIAL_LAYERS {
            return Err(Error::TooManySpatialLayers);
        }

        pos += 1;

        Ok(pos)
    }

    // Layer indices (non-flexible mode):
    /*
     *      +-+-+-+-+-+-+-+-+
     * L:   |  T  |U|  S  |D|
     *      +-+-+-+-+-+-+-+-+
     *      |   TL0PICIDX   |
     *      +-+-+-+-+-+-+-+-+
     **/
    // .
    fn parse_layer_info_non_flexible_mode(
        &mut self,
        packet: &[u8],
        mut pos: usize,
    ) -> Result<usize, Error> {
        if packet.len() <= pos {
            return Err(Error::ShortPacket);
        }

        self.tl0picidx = packet[pos];
        pos += 1;

        Ok(pos)
    }

    // Reference indices: .
    /*
     *      +-+-+-+-+-+-+-+-+                P=1,F=1: At least one reference index
     * P,F: | P_DIFF      |N|  up to 3 times          has to be specified.
     *      +-+-+-+-+-+-+-+-+                    N=1: An additional P_DIFF follows
     *                                              current P_DIFF.
     *
     **/
    // .
    fn parse_ref_indices(&mut self, packet: &[u8], mut pos: usize) -> Result<usize, Error> {
        loop {
            if packet.len() <= pos {
                return Err(Error::ShortPacket);
            }
            self.p_diff.push(packet[pos] >> 1);
            if packet[pos] & 0x01 == 0 {
                break;
            }
            if self.p_diff.len() >= MAX_VP9_REF_PICS {
                return Err(Error::TooManyPDiff);
            }
            pos += 1;
        }
        pos += 1;

        Ok(pos)
    }

    // Scalability structure (SS):
    /*
     *      +-+-+-+-+-+-+-+-+
     * V:   | N_S |Y|G|-|-|-|
     *      +-+-+-+-+-+-+-+-+              -|
     * Y:   |     WIDTH     | (OPTIONAL)    .
     *      +               .
     *      |               | (OPTIONAL)    .
     *      +-+-+-+-+-+-+-+-+               . N_S + 1 times
     *      |     HEIGHT    | (OPTIONAL)    .
     *      +               .
     *      |               | (OPTIONAL)    .
     *      +-+-+-+-+-+-+-+-+              -|
     * G:   |      N_G      | (OPTIONAL)
     *      +-+-+-+-+-+-+-+-+                           -|
     * N_G: |  T  |U| R |-|-| (OPTIONAL)                 .
     *      +-+-+-+-+-+-+-+-+              -|            . N_G times
     *      |    P_DIFF     | (OPTIONAL)    . R times    .
     *      +-+-+-+-+-+-+-+-+              -|           -|
     **/
    // .
    fn parse_ss_data(&mut self, packet: &[u8], mut pos: usize) -> Result<usize, Error> {
        if packet.len() <= pos {
            return Err(Error::ShortPacket);
        }

        self.ns = packet[pos] >> 5;
        self.y = packet[pos] & 0x10 != 0;
        self.g = packet[pos] & 0x8 != 0;
        pos += 1;

        let ns = self.ns + 1;
        self.ng = 0;

        if self.y {
            self.width = resize_uint16_slice(std::mem::take(&mut self.width), ns as usize);
            self.height = resize_uint16_slice(std::mem::take(&mut self.height), ns as usize);
            for i in 0..ns as usize {
                if packet.len() <= (pos + 3) {
                    return Err(Error::ShortPacket);
                }

                self.width[i] = ((packet[pos] as u16) << 8) | packet[pos + 1] as u16;
                pos += 2;
                self.height[i] = ((packet[pos] as u16) << 8) | packet[pos + 1] as u16;
                pos += 2;
            }
        }

        if self.g {
            if packet.len() <= pos {
                return Err(Error::ShortPacket);
            }

            self.ng = packet[pos];
            pos += 1;
        }

        for i in 0..self.ng as usize {
            if packet.len() <= pos {
                return Err(Error::ShortPacket);
            }

            self.pgtid.push(packet[pos] >> 5);
            self.pgu.push(packet[pos] & 0x10 != 0);
            let reference = ((packet[pos] >> 2) & 0x3) as usize;
            pos += 1;

            // The original compares with cap(p.PGPDiff) and takes list i back from the
            // capacity of the slice when it is there. What is in the lists comes out the same.
            if i >= self.pgp_diff.len() {
                self.pgp_diff.push(Vec::new());
            } else {
                self.pgp_diff.truncate(i + 1);
                self.pgp_diff[i].clear();
            }

            if packet.len() <= (pos + reference - 1) {
                return Err(Error::ShortPacket);
            }

            for _ in 0..reference {
                self.pgp_diff[i].push(packet[pos]);
                pos += 1;
            }
        }

        Ok(pos)
    }

    // IsPartitionHead checks whether if this is a head of the VP9 partition.
    pub fn is_partition_head(&self, payload: &[u8]) -> bool {
        if payload.is_empty() {
            return false;
        }

        (payload[0] & 0x08) != 0
    }
}

#[cfg(test)]
#[path = "vp9_packet_test.rs"]
mod vp9_packet_test;
