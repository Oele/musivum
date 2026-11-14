// Ported from LiveKit, pkg/sfu/codecmunger/ddtemporal.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the methods of the interface are in the block that
// implements the trait, `decision` and `record` in the type's own after it; `shl` at the end
// is Go's shift by a count that is not a constant.

use std::any::Any;

use crate::mediatransportutil::codec;
use crate::protocol::codecs::mime;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;

use super::{CodecMunger, Error, MAX_HEADER_SIZE, State};

// frames with a remembered decision, behind the highest frame number
const DD_TEMPORAL_FRAME_WINDOW: u64 = 64;

// DDTemporal filters the temporal layers of a simulcast stream with the dependency
// descriptor, for any codec: it leaves the payload unchanged. Every packet names its frame
// and temporal layer, so the munger decides once per frame and applies that decision to all
// packets of the frame, a late one included. For VP8, libwebrtc sends a one byte payload
// descriptor with the dependency descriptor, so there is no picture ID to munge; the
// forwarder uses the VP8 munger for a stream that keeps picture IDs.
pub struct DdTemporal {
    #[allow(dead_code)] // not read upstream either
    logger: Logger,
    is_vp8: bool, // padding needs a VP8 payload descriptor

    initialized: bool,
    highest_frame: u64,
    decided_mask: u64,   // bit i: frame highestFrame - i has a decision
    forwarded_mask: u64, // bit i: frame highestFrame - i is forwarded
}

impl DdTemporal {
    pub fn new(mime_type: mime::MimeType, logger: Logger) -> Self {
        Self {
            logger,
            is_vp8: mime_type == mime::MimeType::Vp8,
            initialized: false,
            highest_frame: 0,
            decided_mask: 0,
            forwarded_mask: 0,
        }
    }
}

impl CodecMunger for DdTemporal {
    fn get_state(&self) -> Option<State> {
        None
    }

    fn seed_state(&mut self, _state: Option<State>) {}

    fn set_last(&mut self, _ext_pkt: &buffer::ExtPacket) {
        self.initialized = false;
    }

    fn update_offsets(&mut self, _ext_pkt: &buffer::ExtPacket) {
        // a layer switch starts a stream with its own frame numbers
        self.initialized = false;
    }

    fn update_and_get(
        &mut self,
        ext_pkt: &buffer::ExtPacket,
        sn_out_of_order: bool,
        _sn_has_gap: bool,
        max_temporal_layer: i32,
    ) -> Result<(usize, [u8; MAX_HEADER_SIZE], usize), Error> {
        let hdr = [0u8; MAX_HEADER_SIZE];
        let Some(ddwdt) = &ext_pkt.dependency_descriptor else {
            return Ok((0, hdr, 0));
        };

        let frame = ddwdt.ext_frame_num;
        let (mut is_forwarded, ok) = self.decision(frame);
        if !ok {
            is_forwarded = ext_pkt.video_layer.temporal <= max_temporal_layer;
            if !self.record(frame, is_forwarded) {
                // a late packet of a frame that is too old to have a decision
                is_forwarded = false;
            }
        }

        if is_forwarded {
            Ok((0, hdr, 0))
        } else if sn_out_of_order {
            Err(Error::FilteredOutOfOrder)
        } else {
            Err(Error::FilteredTemporalLayer)
        }
    }

    fn update_and_get_padding(&mut self, _new_picture: bool) -> Result<Vec<u8>, Error> {
        if !self.is_vp8 {
            return Ok(Vec::new());
        }

        // the one byte VP8 payload descriptor of a key frame that starts a partition
        let vp8_packet = codec::Vp8 {
            first_byte: 0x10,
            is_key_frame: true,
            header_size: 1,
            ..Default::default()
        };
        Ok(vp8_packet.marshal()?)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl DdTemporal {
    // returns (isForwarded, ok)
    fn decision(&self, frame: u64) -> (bool, bool) {
        if !self.initialized
            || frame > self.highest_frame
            || self.highest_frame.wrapping_sub(frame) >= DD_TEMPORAL_FRAME_WINDOW
        {
            return (false, false);
        }
        let bit = shl(1, self.highest_frame.wrapping_sub(frame));
        (self.forwarded_mask & bit != 0, self.decided_mask & bit != 0)
    }

    fn record(&mut self, frame: u64, is_forwarded: bool) -> bool {
        if !self.initialized || frame >= self.highest_frame.wrapping_add(DD_TEMPORAL_FRAME_WINDOW) {
            self.initialized = true;
            self.highest_frame = frame;
            self.decided_mask = 0;
            self.forwarded_mask = 0;
        } else if frame > self.highest_frame {
            self.decided_mask = shl(self.decided_mask, frame.wrapping_sub(self.highest_frame));
            self.forwarded_mask = shl(self.forwarded_mask, frame.wrapping_sub(self.highest_frame));
            self.highest_frame = frame;
        } else if self.highest_frame.wrapping_sub(frame) >= DD_TEMPORAL_FRAME_WINDOW {
            return false;
        }

        let bit = shl(1, self.highest_frame.wrapping_sub(frame));
        self.decided_mask |= bit;
        if is_forwarded {
            self.forwarded_mask |= bit;
        }
        true
    }
}

// Go's `v << count` of a `uint64` for a count that is not a constant: zero for a count of 64
// or more, where Rust would panic.
fn shl(v: u64, count: u64) -> u64 {
    u32::try_from(count)
        .ok()
        .and_then(|count| v.checked_shl(count))
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "ddtemporal_test.rs"]
mod ddtemporal_test;
