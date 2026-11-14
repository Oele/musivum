// Ported from LiveKit's mediatransportutil, pkg/codec/vp8.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors of the package are one enum, which also has
// the errors that h26xhelper.go makes in place (`errors.New`, `fmt.Errorf`); a `&[u8]` is
// never nil, so `Error::NilPacket` is never returned (an empty payload is a short packet, as
// in the original); `marshal` returns a `Vec<u8>`.

use super::VideoSize;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("packet is not large enough")]
    ShortPacket,
    #[error("invalid nil packet")]
    NilPacket,
    #[error("invalid packet")]
    InvalidPacket,

    // The errors of h26xhelper.go, which have no names there.
    #[error("not enough bits")]
    NotEnoughBits,
    #[error("not an HEVC SPS NAL (type={0})")]
    NotAnHevcSpsNal(u64),
    #[error("empty SPS NAL")]
    EmptySpsNal,
    #[error("empty SPS NAL after stripping start code")]
    EmptySpsNalAfterStrippingStartCode,
    #[error("not an SPS NAL (type={0})")]
    NotAnSpsNal(u8),
    #[error("SPS: num_ref_frames_in_pic_order_cnt_cycle too large")]
    SpsNumRefFramesInPicOrderCntCycleTooLarge,
}

// VP8 is a helper to get temporal data from VP8 packet header
/*
    VP8 Payload Descriptor
            0 1 2 3 4 5 6 7                      0 1 2 3 4 5 6 7
            +-+-+-+-+-+-+-+-+                   +-+-+-+-+-+-+-+-+
            |X|R|N|S|R| PID | (REQUIRED)        |X|R|N|S|R| PID | (REQUIRED)
            +-+-+-+-+-+-+-+-+                   +-+-+-+-+-+-+-+-+
        X:  |I|L|T|K| RSV   | (OPTIONAL)   X:   |I|L|T|K| RSV   | (OPTIONAL)
            +-+-+-+-+-+-+-+-+                   +-+-+-+-+-+-+-+-+
        I:  |M| PictureID   | (OPTIONAL)   I:   |M| PictureID   | (OPTIONAL)
            +-+-+-+-+-+-+-+-+                   +-+-+-+-+-+-+-+-+
        L:  |   TL0PICIDX   | (OPTIONAL)        |   PictureID   |
            +-+-+-+-+-+-+-+-+                   +-+-+-+-+-+-+-+-+
        T/K:|TID|Y| KEYIDX  | (OPTIONAL)   L:   |   TL0PICIDX   | (OPTIONAL)
            +-+-+-+-+-+-+-+-+                   +-+-+-+-+-+-+-+-+
        T/K:|TID|Y| KEYIDX  | (OPTIONAL)
            +-+-+-+-+-+-+-+-+
*/
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Vp8 {
    pub first_byte: u8,
    pub s: bool,

    pub i: bool,
    pub m: bool,
    pub picture_id: u16, /* 7 or 15 bits, picture ID */

    pub l: bool,
    pub tl0picidx: u8, /* 8 bits temporal level zero index */

    // Optional Header If either of the T or K bits are set to 1,
    // the TID/Y/KEYIDX extension field MUST be present.
    pub t: bool,
    pub tid: u8, /* 2 bits temporal layer idx */
    pub y: bool,

    pub k: bool,
    pub keyidx: u8, /* 5 bits of key frame idx */

    pub header_size: usize,

    // IsKeyFrame is a helper to detect if current packet is a keyframe
    pub is_key_frame: bool,
}

impl Vp8 {
    // Unmarshal parses the passed byte slice and stores the result in the VP8 this method is called upon
    pub fn unmarshal(&mut self, payload: &[u8]) -> Result<(), Error> {
        // The original returns errNilPacket for a nil payload here. A `&[u8]` is never nil.

        let payload_len = payload.len();
        if payload_len < 1 {
            return Err(Error::ShortPacket);
        }

        let mut idx = 0;
        self.first_byte = payload[idx];
        self.s = payload[idx] & 0x10 > 0;
        // Check for extended bit control
        if payload[idx] & 0x80 > 0 {
            idx += 1;
            if payload_len < idx + 1 {
                return Err(Error::ShortPacket);
            }
            self.i = payload[idx] & 0x80 > 0;
            self.l = payload[idx] & 0x40 > 0;
            self.t = payload[idx] & 0x20 > 0;
            self.k = payload[idx] & 0x10 > 0;
            if self.l && !self.t {
                return Err(Error::InvalidPacket);
            }

            if self.i {
                idx += 1;
                if payload_len < idx + 1 {
                    return Err(Error::ShortPacket);
                }
                let pid = payload[idx] & 0x7f;
                // if m is 1, then Picture ID is 15 bits
                self.m = payload[idx] & 0x80 > 0;
                if self.m {
                    idx += 1;
                    if payload_len < idx + 1 {
                        return Err(Error::ShortPacket);
                    }
                    self.picture_id = u16::from_be_bytes([pid, payload[idx]]);
                } else {
                    self.picture_id = pid as u16;
                }
            }

            if self.l {
                idx += 1;
                if payload_len < idx + 1 {
                    return Err(Error::ShortPacket);
                }
                self.tl0picidx = payload[idx];
            }

            if self.t || self.k {
                idx += 1;
                if payload_len < idx + 1 {
                    return Err(Error::ShortPacket);
                }

                if self.t {
                    self.tid = (payload[idx] & 0xc0) >> 6;
                    self.y = (payload[idx] & 0x20) > 0;
                }

                if self.k {
                    self.keyidx = payload[idx] & 0x1f;
                }
            }
            idx += 1;
            if payload_len < idx + 1 {
                return Err(Error::ShortPacket);
            }

            // Check is packet is a keyframe by looking at P bit in vp8 payload
            self.is_key_frame = payload[idx] & 0x01 == 0 && self.s;
        } else {
            idx += 1;
            if payload_len < idx + 1 {
                return Err(Error::ShortPacket);
            }
            // Check is packet is a keyframe by looking at P bit in vp8 payload
            self.is_key_frame = payload[idx] & 0x01 == 0 && self.s;
        }
        self.header_size = idx;
        Ok(())
    }

    pub fn marshal(&self) -> Result<Vec<u8>, Error> {
        let mut buf = [0u8; 8];
        let n = self.marshal_to(&mut buf)?;
        Ok(buf[..n].to_vec())
    }

    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, Error> {
        if buf.len() < self.header_size {
            return Err(Error::ShortPacket);
        }

        let mut idx = 0;
        buf[idx] = self.first_byte;
        if self.i || self.l || self.t || self.k {
            buf[idx] |= 0x80; // X bit
            idx += 1;

            let xpos = idx;
            let mut xval = 0u8;

            idx += 1;
            if self.i {
                xval |= 1 << 7;
                if self.m {
                    buf[idx] = 0x80 | ((self.picture_id >> 8) & 0x7f) as u8;
                    buf[idx + 1] = (self.picture_id & 0xff) as u8;
                    idx += 2;
                } else {
                    buf[idx] = self.picture_id as u8;
                    idx += 1;
                }
            }

            if self.l {
                xval |= 1 << 6;
                buf[idx] = self.tl0picidx;
                idx += 1;
            }

            if self.t || self.k {
                buf[idx] = 0;
                if self.t {
                    xval |= 1 << 5;
                    buf[idx] = self.tid << 6;
                    if self.y {
                        buf[idx] |= 1 << 5;
                    }
                }

                if self.k {
                    xval |= 1 << 4;
                    buf[idx] |= self.keyidx & 0x1f;
                }
                idx += 1;
            }

            buf[xpos] = xval;
        } else {
            buf[idx] &= !0x80; // X bit
            idx += 1;
        }

        Ok(idx)
    }
}

// -------------------------------------

// ExtractVP8VideoSize extracts video resolution from VP8 key frame
pub fn extract_vp8_video_size(vp8_packet: &Vp8, payload: &[u8]) -> VideoSize {
    if !vp8_packet.is_key_frame || payload.len() < vp8_packet.header_size + 10 {
        return VideoSize::default();
    }

    let vp8_payload = &payload[vp8_packet.header_size..];

    // Check for VP8 start code
    if vp8_payload.len() < 10
        || vp8_payload[3] != 0x9D
        || vp8_payload[4] != 0x01
        || vp8_payload[5] != 0x2A
    {
        return VideoSize::default();
    }

    // Read width and height from bytes 6-9
    let width = (vp8_payload[6] as u32) | ((vp8_payload[7] as u32) << 8);
    let height = (vp8_payload[8] as u32) | ((vp8_payload[9] as u32) << 8);

    VideoSize {
        width: width & 0x3FFF,
        height: height & 0x3FFF,
    }
}

#[cfg(test)]
#[path = "vp8_test.rs"]
mod vp8_test;
