// Ported from LiveKit, pkg/sfu/videolayerselector/ddforwarder.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The fields `descriptor` and `activeDecodeTargetsBitmask` are gone: they are the storage
//   of the descriptor that is written, kept in the forwarder so that a packet does not
//   allocate. Here the extension that is marshaled owns its descriptor, a copy that shares
//   what the packet's points at.
// - The `switch` of `forward` is a chain of `if`s. `new_from_other` and `seed_state` take an
//   `Option<&..>` where the original takes a pointer that may be nil, `get_state` gives an
//   `Option`.

use std::sync::Arc;

use super::{VideoLayerSelectorResult, shl};
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::warnw;

// DDForwarder forwards the dependency descriptor of a simulcast stream. Without it, a
// subscriber of a stream without picture IDs, VP8 from libwebrtc for example, finds frame
// references by sequence number alone and stalls on every sequence number it cannot account
// for.
//
// libwebrtc numbers frames with one counter for all simulcast layers of a sender, so the
// frame numbers of a new layer are close to those of the old one. The numbers sent to the
// subscriber must still never go back: a key frame older than the last structure the
// subscriber received is dropped. So whenever the stream changes, a layer switch, a stream
// restart or a new forwarder, the numbers are moved up past the last one sent if needed.
// Otherwise they pass through with the current offset, so frame diffs and chain diffs stay
// valid. A migrated subscriber continues with the last number sent, which is part of the
// migrated forwarder state.
pub struct DdForwarder {
    logger: Logger,

    initialized: bool, // a key frame with a structure set the stream
    ssrc: u32,
    restart_generation: i64,
    structure: Option<Arc<dd::FrameDependencyStructure>>,

    frame_num_offset: u64, // added to the incoming extended frame number
    has_last: bool,
    ext_last_frame_num: u64, // highest outgoing extended frame number
}

impl DdForwarder {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            initialized: false,
            ssrc: 0,
            restart_generation: 0,
            structure: None,
            frame_num_offset: 0,
            has_last: false,
            ext_last_frame_num: 0,
        }
    }

    // NewDDForwarderFromOther continues the frame numbers of a forwarder it replaces on the
    // same downtrack, a restarted upstream receiver for example.
    pub fn new_from_other(other: Option<&DdForwarder>, logger: Logger) -> Self {
        let mut d = Self::new(logger);
        if let Some(other) = other
            && other.has_last
        {
            d.has_last = true;
            d.ext_last_frame_num = other.ext_last_frame_num;
        }
        d
    }

    // GetState returns the last frame number sent, nil before the first one.
    pub fn get_state(&self) -> Option<livekit::DependencyDescriptorForwarderState> {
        if !self.has_last {
            return None;
        }
        Some(livekit::DependencyDescriptorForwarderState {
            ext_last_frame_number: self.ext_last_frame_num,
        })
    }

    // SeedState continues after the last frame number a forwarder on another node sent to the
    // subscriber. Extended numbers there started from a lower or equal base, so the first key
    // frame here moves past it, and the 16 bit numbers continue.
    pub fn seed_state(&mut self, state: Option<&livekit::DependencyDescriptorForwarderState>) {
        let Some(state) = state else {
            return;
        };
        if !self.has_last || state.ext_last_frame_number > self.ext_last_frame_num {
            self.has_last = true;
            self.ext_last_frame_num = state.ext_last_frame_number;
        }
    }

    // Forward marshals the descriptor of a forwarded packet into result. temporal is the
    // temporal layer the packet is forwarded at. It returns false when there is nothing to
    // send, before the first key frame of the stream for example.
    pub fn forward(
        &mut self,
        ext_pkt: &buffer::ExtPacket,
        temporal: i32,
        result: &mut VideoLayerSelectorResult,
    ) -> bool {
        let Some(ddwdt) = &ext_pkt.dependency_descriptor else {
            return false;
        };
        let Some(descriptor) = &ddwdt.descriptor else {
            return false;
        };
        if descriptor.frame_dependencies.is_none() {
            return false;
        }

        let ext_frame_num = ddwdt.ext_frame_num;
        // the parser accepts a structure only on the first packet of a frame
        let has_structure = descriptor.attached_structure.is_some();
        if !self.initialized || ext_pkt.packet.ssrc != self.ssrc {
            // a stream starts on a key frame, a late packet of the old layer is not described
            if !has_structure {
                return false;
            }
            self.rebase(ext_frame_num);
            self.initialized = true;
            self.ssrc = ext_pkt.packet.ssrc;
            self.restart_generation = ddwdt.restart_generation;
        } else if ddwdt.restart_generation != self.restart_generation {
            // the restarted stream numbers its frames from a new start and has its own structure,
            // the first packet can be a delta frame
            self.rebase(ext_frame_num);
            self.restart_generation = ddwdt.restart_generation;
            self.structure = None;
        }
        if has_structure {
            self.structure = descriptor.attached_structure.clone();
        }
        if self.structure.is_none() {
            return false;
        }

        let ext_out_frame_num = ext_frame_num.wrapping_add(self.frame_num_offset);
        if !self.has_last || ext_out_frame_num > self.ext_last_frame_num {
            self.has_last = true;
            self.ext_last_frame_num = ext_out_frame_num;
        }

        // tell the subscriber which decode targets to expect: the publisher's active ones up to
        // the forwarded temporal layer. On a key frame too, where the structure alone would make
        // all of them active.
        let mut active_decode_targets_bitmask = 0u32;
        for dt in &ddwdt.decode_targets {
            if dt.layer.temporal <= temporal {
                active_decode_targets_bitmask |= shl(1, dt.target);
            }
        }
        active_decode_targets_bitmask &= ddwdt.active_decode_targets_mask;
        let mut out_descriptor = descriptor.clone();
        out_descriptor.frame_number = ext_out_frame_num as u16;
        out_descriptor.active_decode_targets_bitmask = Some(active_decode_targets_bitmask);

        if let Err(err) =
            result.marshal_dependency_descriptor_extension(&dd::DependencyDescriptorExtension {
                descriptor: out_descriptor,
                structure: self.structure.clone(),
            })
        {
            warnw!(self.logger, "could not marshal dependency descriptor", err, "extFrameNum" => ext_frame_num);
            return false;
        }
        true
    }

    // rebase moves the frame numbers up when a new stream would go back past the last one sent
    fn rebase(&mut self, ext_frame_num: u64) {
        if self.has_last
            && ext_frame_num.wrapping_add(self.frame_num_offset) <= self.ext_last_frame_num
        {
            self.frame_num_offset = self
                .ext_last_frame_num
                .wrapping_add(1)
                .wrapping_sub(ext_frame_num);
        }
    }
}

#[cfg(test)]
#[path = "ddforwarder_test.rs"]
mod ddforwarder_test;
