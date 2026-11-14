// Ported from LiveKit's mediatransportutil, pkg/codec/keyframe.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the VP9 packet that `is_vp9_key_frame` may be given is
// an `Option<&Vp9Packet>`.

use super::Vp8;
use crate::debugw;
use crate::protocol::logger;
use crate::webrtc::rtp::codecs;

pub fn is_key_frame(codec: &str, payload: &[u8]) -> bool {
    match codec.to_lowercase().as_str() {
        "h264" => is_h264_key_frame(payload),
        "h265" => is_h265_key_frame(payload),
        "vp8" => is_vp8_key_frame(payload),
        "vp9" => is_vp9_key_frame(None, payload),
        "av1" => is_av1_key_frame(payload),
        _ => false,
    }
}

// IsH264KeyFrame detects if h264 payload is a keyframe
// this code was taken from https://github.com/jech/galene/blob/codecs/rtpconn/rtpreader.go#L45
// all credits belongs to Juliusz Chroboczek @jech and the awesome Galene SFU
pub fn is_h264_key_frame(payload: &[u8]) -> bool {
    if payload.is_empty() {
        return false;
    }
    let nalu = payload[0] & 0x1F;
    if nalu == 0 {
        // reserved
        return false;
    } else if nalu <= 23 {
        // simple NALU
        return nalu == 7;
    } else if nalu == 24 || nalu == 25 || nalu == 26 || nalu == 27 {
        // STAP-A, STAP-B, MTAP16 or MTAP24
        let mut i = 1;
        if nalu == 25 || nalu == 26 || nalu == 27 {
            // skip DON
            i += 2;
        }
        while i < payload.len() {
            if i + 2 > payload.len() {
                return false;
            }
            let length = ((payload[i] as u16) << 8) | (payload[i + 1] as u16);
            i += 2;
            if i + length as usize > payload.len() {
                return false;
            }
            let mut offset = 0;
            match nalu {
                26 => offset = 3,
                27 => offset = 4,
                _ => {}
            }
            if offset >= length as usize {
                return false;
            }
            let n = payload[i + offset] & 0x1F;
            if n == 7 {
                return true;
            } else if n >= 24 {
                // is this legal?
                debugw!(logger::get_logger(), "Non-simple NALU within a STAP");
            }
            i += length as usize;
        }
        if i == payload.len() {
            return false;
        }
        return false;
    } else if nalu == 28 || nalu == 29 {
        // FU-A or FU-B
        if payload.len() < 2 {
            return false;
        }
        if (payload[1] & 0x80) == 0 {
            // not a starting fragment
            return false;
        }
        return payload[1] & 0x1F == 7;
    }
    false
}

pub fn is_h265_key_frame(payload: &[u8]) -> bool {
    if payload.len() < 2 {
        return false;
    }
    let mut nalu_type = (payload[0] & 0x7E) >> 1;
    match nalu_type {
        33 | 34 => true,
        48 => {
            // AP
            let mut idx = 2;
            while idx < payload.len() - 2 {
                // TODO: check the DONL field (controlled by sprop-max-don-diff)
                let size = u16::from_be_bytes([payload[idx], payload[idx + 1]]);
                idx += 2;
                if idx >= payload.len() {
                    return false;
                }
                nalu_type = (payload[idx] & 0x7E) >> 1;
                if nalu_type == 33 || nalu_type == 34 {
                    return true;
                }
                idx += size as usize;
            }
            false
        }

        49 => {
            // FU
            if payload.len() < 3 {
                return false;
            }
            nalu_type = (payload[2] & 0x7E) >> 1;
            nalu_type == 33 || nalu_type == 34
        }
        _ => false,
    }
}

pub fn is_vp8_key_frame(payload: &[u8]) -> bool {
    let mut vp8 = Vp8::default();
    if vp8.unmarshal(payload).is_err() {
        return false;
    }

    vp8.is_key_frame
}

// IsVP9KeyFrame detects if vp9 payload is a keyframe
// taken from https://github.com/jech/galene/blob/master/codecs/codecs.go
// all credits belongs to Juliusz Chroboczek @jech and the awesome Galene SFU
pub fn is_vp9_key_frame(vp9: Option<&codecs::Vp9Packet>, payload: &[u8]) -> bool {
    let mut parsed;
    let vp9 = match vp9 {
        Some(vp9) => vp9,
        None => {
            parsed = codecs::Vp9Packet::default();
            let res = parsed.unmarshal(payload);
            if res.is_err() || parsed.payload.is_empty() {
                return false;
            }
            &parsed
        }
    };

    if !vp9.b {
        return false;
    }

    // The buffer asks through `sfu/buffer/vp9keyframe.rs`, which answers first for a packet
    // without frame data.
    if (vp9.payload[0] & 0xc0) != 0x80 {
        return false;
    }

    let profile = (vp9.payload[0] >> 4) & 0x3;
    if profile != 3 {
        return (vp9.payload[0] & 0xC) == 0;
    }
    (vp9.payload[0] & 0x6) == 0
}

// IsAV1KeyFrame detects if av1 payload is a keyframe
// taken from https://github.com/jech/galene/blob/master/codecs/codecs.go
// all credits belongs to Juliusz Chroboczek @jech and the awesome Galene SFU
pub fn is_av1_key_frame(payload: &[u8]) -> bool {
    if payload.len() < 2 {
        return false;
    }
    // Z=0, N=1
    if (payload[0] & 0x88) != 0x08 {
        return false;
    }
    let w = (payload[0] & 0x30) >> 4;

    fn get_obu(data: &[u8], last: bool) -> (&[u8], usize, bool) {
        if last {
            return (data, data.len(), false);
        }
        let mut offset = 0;
        let mut length = 0;
        loop {
            if data.len() <= offset {
                return (&[], offset, offset > 0);
            }
            if offset >= 4 {
                return (&[], offset, true);
            }
            let l = data[offset];
            length |= ((l & 0x7f) as usize) << (offset * 7);
            offset += 1;
            if (l & 0x80) == 0 {
                break;
            }
        }
        if data.len() < offset + length {
            return (&data[offset..], data.len(), true);
        }
        (&data[offset..offset + length], offset + length, false)
    }
    let mut offset = 1;
    let mut i = 0;
    loop {
        let (obu, length, truncated) = get_obu(&payload[offset..], w as usize == i + 1);
        if obu.is_empty() {
            return false;
        }
        let tpe = (obu[0] & 0x38) >> 3;
        match i {
            0 => {
                // OBU_SEQUENCE_HEADER
                if tpe != 1 {
                    return false;
                }
            }
            _ => {
                // OBU_FRAME_HEADER or OBU_FRAME
                if tpe == 3 || tpe == 6 {
                    if obu.len() < 2 {
                        return false;
                    }
                    // show_existing_frame == 0
                    if (obu[1] & 0x80) != 0 {
                        return false;
                    }
                    // frame_type == KEY_FRAME
                    return (obu[1] & 0x60) == 0;
                }
            }
        }
        if truncated || i >= w as usize {
            // the first frame header is in a second
            // packet, give up.
            return false;
        }
        offset += length;
        i += 1;
    }
}

#[cfg(test)]
#[path = "keyframe_test.rs"]
mod keyframe_test;
