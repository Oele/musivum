// Ported from LiveKit, pkg/sfu/videolayerselector/temporallayerselector/dependencydescriptor.go
// at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the `switch` of `select` is a chain of `if`s, its
// `break` leaves a labelled block; the shifts by a count that is not a constant go through
// `shl`, which gives what Go gives for a count of 64 or more (zero).

use super::TemporalLayerSelector;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::rtpextension::dependencydescriptor as dd;

// frames tracked behind the highest frame number, more than temporal references reach
const DD_FRAME_WINDOW: u64 = 64;

// DependencyDescriptor switches temporal layers with the dependency descriptor alone, for
// a stream with one spatial layer. It only picks the layer, the codec munger drops the
// packets above it. libwebrtc strips the VP8 payload descriptor when it sends the
// dependency descriptor, so for VP8 the temporal ID and the layer sync bit are only here.
pub struct DependencyDescriptor {
    // not used by the original either
    #[allow(dead_code)]
    logger: Logger,

    initialized: bool,
    highest_frame: u64,
    forwarded_mask: u64, // bit i: frame highestFrame - i was forwarded
}

impl DependencyDescriptor {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            initialized: false,
            highest_frame: 0,
            forwarded_mask: 0,
        }
    }
}

impl TemporalLayerSelector for DependencyDescriptor {
    fn select(&mut self, ext_pkt: &buffer::ExtPacket, current: i32, target: i32) -> (i32, i32) {
        let mut this = current;
        let mut next = current;

        let Some(ddwdt) = &ext_pkt.dependency_descriptor else {
            return (this, next);
        };
        let Some(descriptor) = &ddwdt.descriptor else {
            return (this, next);
        };
        let Some(frame_dependencies) = &descriptor.frame_dependencies else {
            return (this, next);
        };

        // A late first packet, a retransmission for example, starts a frame whose other
        // packets were already decided. A key frame is the exception: it references nothing,
        // and it restarts the frame numbers on a stream restart.
        let is_frame_start = descriptor.first_packet_in_frame && !ext_pkt.is_out_of_order;
        if current < target && ext_pkt.is_key_frame {
            this = target;
            next = target;
        } else if current < target && is_frame_start {
            'switch: {
                // A switch point of a decode target makes all later frames of that target
                // decodable if this frame is decodable, so also every frame this one references
                // has to have been forwarded.
                if !self.are_forwarded(ddwdt.ext_frame_num, &frame_dependencies.frame_diffs) {
                    break 'switch;
                }
                let dtis = &frame_dependencies.decode_target_indications;
                for dt in &ddwdt.decode_targets {
                    // sorted from high to low
                    if dt.layer.temporal <= current
                        || dt.layer.temporal > target
                        || dt.target >= dtis.len() as i64
                    {
                        continue;
                    }
                    if dtis[dt.target as usize] == dd::DecodeTargetIndication::Switch {
                        this = dt.layer.temporal;
                        next = dt.layer.temporal;
                        break;
                    }
                }
            }
        } else if current > target && ext_pkt.packet.marker {
            next = target;
        }

        if ext_pkt.is_key_frame || is_frame_start {
            self.on_frame(
                ddwdt.ext_frame_num,
                ext_pkt.is_key_frame,
                ext_pkt.video_layer.temporal <= this,
            );
        }
        (this, next)
    }
}

impl DependencyDescriptor {
    fn on_frame(&mut self, frame: u64, is_key_frame: bool, is_forwarded: bool) {
        if is_key_frame
            || !self.initialized
            || frame >= self.highest_frame.wrapping_add(DD_FRAME_WINDOW)
        {
            self.initialized = true;
            self.highest_frame = frame;
            self.forwarded_mask = 0;
        } else if frame > self.highest_frame {
            self.forwarded_mask = shl(self.forwarded_mask, frame.wrapping_sub(self.highest_frame));
            self.highest_frame = frame;
        } else if self.highest_frame.wrapping_sub(frame) >= DD_FRAME_WINDOW {
            return;
        }

        if is_forwarded {
            self.forwarded_mask |= shl(1, self.highest_frame.wrapping_sub(frame));
        }
    }

    fn are_forwarded(&self, frame: u64, frame_diffs: &[i64]) -> bool {
        if !self.initialized {
            return false;
        }
        for &diff in frame_diffs {
            if diff <= 0 || diff as u64 > frame {
                return false;
            }
            let r#ref = frame.wrapping_sub(diff as u64);
            if r#ref > self.highest_frame
                || self.highest_frame.wrapping_sub(r#ref) >= DD_FRAME_WINDOW
                || self.forwarded_mask & shl(1, self.highest_frame.wrapping_sub(r#ref)) == 0
            {
                return false;
            }
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
#[path = "dependencydescriptor_test.rs"]
mod dependencydescriptor_test;
