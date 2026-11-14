// Ported from LiveKit, pkg/sfu/videolayerselector/dependencydescriptor.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The embedded `*Base` is the field `base`. `new_from_other` takes the selector it
//   replaces and its `Base` with it (the original's two selectors share the `*Base`).
// - The decision cache and the frame chains are pointed at from several places in the
//   original: they are an `Arc<Mutex<..>>` and `Arc`s here, see framechain.rs. The lock of
//   the cache is taken for one call at a time and never held while a chain is called.
// - `decodeTargetsLock` is gone. The decode targets are changed only through `&mut self`,
//   so `check_sync`, which takes `&self`, cannot run meanwhile.
// - `ddClone` is gone: it is the storage of the descriptor that is written when the frame
//   number or the active decode targets are rewritten, kept in the selector so that a packet
//   does not allocate. Here the extension that is marshaled owns its descriptor, a copy that
//   shares what the packet's points at, and is rewritten in place.
// - The two bitmasks, pointers that may be nil, are `Option<u32>`.
// - No panic leaves `select`:
//   - The original's `recover()` around the marshalling is `catch_unwind`: the writer panics
//     where the original dereferences nil (a descriptor without frame dependencies, an
//     extension without structure) or indexes out of range. The panic's stack is not logged
//     (the panic hook has written it by then, if it is asked to).
//   - A packet whose descriptor, frame dependencies, attached structure or active decode
//     targets are absent where the original dereferences them (nil there, which its parser
//     never gives) is not selected. The original would crash.
// - `buffer.GetActiveDecodeTargetBitmask` is at the end of this file until the file it
//   belongs to, buffer/dependencydescriptorparser.go, is ported in full.
// - `check_sync` stands with the other methods of the trait, not between
//   `invalidate_key_frame` and `restart`.
// - The lines of logging that are commented out in the original are kept so, as the macros
//   would be written.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use parking_lot::Mutex;

use super::{
    Base, DecodeTarget, FrameChain, FrameNumberWrapper, SelectorDecision, SelectorDecisionCache,
    VideoLayerSelector, VideoLayerSelectorResult, shl,
};
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::rtpextension::dependencydescriptor as dede;
use crate::{debugw, errorw, warnw};

const DECISION_CACHE_MAX_ELEMENTS: u64 = 256;
const DECISION_CACHE_NACK_ENTRIES: u64 = 80;

pub struct DependencyDescriptor {
    pub(super) base: Base,

    decisions: Arc<Mutex<SelectorDecisionCache>>,

    previous_active_decode_targets_bitmask: Option<u32>,
    active_decode_targets_bitmask: Option<u32>,
    structure: Option<Arc<dede::FrameDependencyStructure>>,
    ext_key_frame_num: u64,
    key_frame_valid: bool,

    chains: Vec<Arc<FrameChain>>,

    decode_targets: Vec<DecodeTarget>,
    fn_wrapper: FrameNumberWrapper,

    restart_generation: i64,
}

impl DependencyDescriptor {
    pub fn new(logger: Logger) -> Self {
        Self {
            base: Base::new(logger.clone()),
            decisions: Arc::new(Mutex::new(SelectorDecisionCache::new(
                DECISION_CACHE_MAX_ELEMENTS,
                DECISION_CACHE_NACK_ENTRIES,
            ))),
            previous_active_decode_targets_bitmask: None,
            active_decode_targets_bitmask: None,
            structure: None,
            ext_key_frame_num: 0,
            key_frame_valid: false,
            chains: Vec::new(),
            decode_targets: Vec::new(),
            fn_wrapper: FrameNumberWrapper {
                logger,
                ..Default::default()
            },
            restart_generation: 0,
        }
    }

    pub fn new_from_other(vls: Box<dyn VideoLayerSelector>) -> Self {
        let logger = vls.get_logger();
        Self {
            base: vls.into_base(),
            decisions: Arc::new(Mutex::new(SelectorDecisionCache::new(256, 80))),
            previous_active_decode_targets_bitmask: None,
            active_decode_targets_bitmask: None,
            structure: None,
            ext_key_frame_num: 0,
            key_frame_valid: false,
            chains: Vec::new(),
            decode_targets: Vec::new(),
            fn_wrapper: FrameNumberWrapper {
                logger,
                ..Default::default()
            },
            restart_generation: 0,
        }
    }
}

impl VideoLayerSelector for DependencyDescriptor {
    fn get_base(&self) -> &Base {
        &self.base
    }

    fn get_base_mut(&mut self) -> &mut Base {
        &mut self.base
    }

    fn into_base(self: Box<Self>) -> Base {
        self.base
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_overshoot_okay(&self) -> bool {
        false
    }

    fn select(&mut self, ext_pkt: &buffer::ExtPacket, _layer: i32) -> VideoLayerSelectorResult {
        let mut result = VideoLayerSelectorResult::default();
        // a packet is always relevant for the svc codec
        if self.base.current_layer.is_valid() {
            result.is_relevant = true;
        }

        let Some(ddwdt) = &ext_pkt.dependency_descriptor else {
            // packet doesn't have dependency descriptor
            // debugw!(self.base.logger, format!("drop packet, no DD, incoming {}, sn: {}, isKeyFrame: {}", ext_pkt.video_layer, ext_pkt.packet.sequence_number, ext_pkt.is_key_frame));
            return result;
        };

        if ddwdt.restart_generation > self.restart_generation {
            debugw!(
                self.base.logger,
                "stream restarted",
                "packet" => ddwdt.restart_generation,
                "current" => self.restart_generation,
                "structureKeyFrame" => self.ext_key_frame_num,
                "efn" => ddwdt.ext_frame_num,
                "lastEfn" => self.fn_wrapper.last_origin(),
            );
            self.restart(ddwdt.restart_generation);
        } else if ddwdt.restart_generation < self.restart_generation {
            // must not happen
            warnw!(self.base.logger, "packet from old generation", nil, "packet" => ddwdt.restart_generation, "current" => self.restart_generation);
        }

        // nil in the original would crash it a few lines on
        let Some(dd) = &ddwdt.descriptor else {
            return result;
        };

        let ext_frame_num = ddwdt.ext_frame_num;

        // nil in the original would crash it on the next line
        let Some(fd) = &dd.frame_dependencies else {
            return result;
        };
        let incoming_layer = buffer::VideoLayer {
            spatial: fd.spatial_id as i32,
            temporal: fd.temporal_id as i32,
        };

        if !self.key_frame_valid && dd.attached_structure.is_none() {
            // debugw!(self.base.logger, format!("drop packet, no attached structure, incoming {}, sn: {}, isKeyFrame: {}", ext_pkt.video_layer, ext_pkt.packet.sequence_number, ext_pkt.is_key_frame));
            return result;
        }

        // early return if this frame is already forwarded or dropped
        let sd = self.decisions.lock().get_decision(ext_frame_num);
        let Ok(sd) = sd else {
            // do not mark as dropped as only error is an old frame
            // debugw!(self.base.logger, format!("drop packet on decision error, incoming {}, fn: {}/{}, sn: {}",
            //	incoming_layer,
            //	dd.frame_number,
            //	ext_frame_num,
            //	ext_pkt.packet.sequence_number,
            // ), "err" => err);
            return result;
        };
        if sd == SelectorDecision::Dropped {
            // a packet of an alreadty dropped frame, maintain decision
            // debugw!(self.base.logger, format!("drop packet already dropped, incoming {}, fn: {}/{}, sn: {}",
            //	incoming_layer,
            //	dd.frame_number,
            //	ext_frame_num,
            //	ext_pkt.packet.sequence_number,
            // ));
            return result;
        }

        if ddwdt.structure_updated {
            // debugw!(self.base.logger, "update dependency structure",
            // 	"structureID" => dd.attached_structure.structure_id,
            // 	"structure" => dd.attached_structure,
            // 	"decodeTargets" => ddwdt.decode_targets,
            // 	"efn" => ext_frame_num,
            // 	"sn" => ext_pkt.packet.sequence_number,
            // 	"isKeyFrame" => ext_pkt.is_key_frame,
            // 	"currentKeyframe" => self.ext_key_frame_num,
            // );

            // nil in the original would crash it in updateDependencyStructure
            let Some(attached_structure) = &dd.attached_structure else {
                return result;
            };
            self.update_dependency_structure(
                attached_structure.clone(),
                &ddwdt.decode_targets,
                ext_frame_num,
            );
        }

        if ddwdt.ext_key_frame_num != self.ext_key_frame_num {
            // keyframe mismatch, drop and reset chains
            debugw!(self.base.logger, "drop packet for keyframe mismatch", "incoming" => incoming_layer, "efn" => ext_frame_num, "sn" => ext_pkt.packet.sequence_number, "requiredKeyFrame" => ddwdt.ext_key_frame_num, "structureKeyFrame" => self.ext_key_frame_num);
            self.decisions.lock().add_dropped(ext_frame_num);
            self.invalidate_key_frame();
            return result;
        }

        if ddwdt.active_decode_targets_updated {
            // nil in the original would crash it here
            let Some(active_decode_targets_bitmask) = dd.active_decode_targets_bitmask else {
                return result;
            };
            self.update_active_decode_targets(active_decode_targets_bitmask);
        }

        if fd.chain_diffs.len() != self.chains.len() {
            // The original gives this `Debugw` a nil for an error it does not take, which
            // moves its keys and values by one.
            debugw!(
                self.base.logger,
                "frame chain diff length mismatch",
                "incoming" => incoming_layer,
                "efn" => ext_frame_num,
                "sn" => ext_pkt.packet.sequence_number,
                "chainDiffs" => fd.chain_diffs,
                "chains" => self.chains.len(),
                "requiredKeyFrame" => ddwdt.ext_key_frame_num,
                "structureKeyFrame" => self.ext_key_frame_num,
            );
            self.decisions.lock().add_dropped(ext_frame_num);
            return result;
        }

        for chain in &self.chains {
            chain.on_frame(ext_frame_num, fd);
        }

        // find decode target closest to targetLayer
        let mut highest_decode_target = buffer::DependencyDescriptorDecodeTarget {
            target: -1,
            layer: buffer::INVALID_LAYER,
        };
        let mut dti = dede::DecodeTargetIndication::default();

        // decodeTargets be sorted from high to low, find the highest decode target that is active and integrity
        for dt in &self.decode_targets {
            if !dt.active()
                || dt.dependency_descriptor_decode_target.layer.spatial
                    > self.base.target_layer.spatial
                || dt.dependency_descriptor_decode_target.layer.temporal
                    > self.base.target_layer.temporal
            {
                continue;
            }

            let frame_result = match dt.on_frame(ext_frame_num, fd) {
                Ok(frame_result) => frame_result,
                Err(err) => {
                    // dtis error, dependency descriptor might lost
                    warnw!(
                        self.base.logger,
                        &format!(
                            "drop packet for frame detection error,  incoming: {incoming_layer}"
                        ),
                        err
                    );
                    self.decisions.lock().add_dropped(ext_frame_num);
                    return result;
                }
            };

            if frame_result.target_valid {
                highest_decode_target = dt.dependency_descriptor_decode_target;
                dti = frame_result.dti;
                break;
            }
        }

        if highest_decode_target.target < 0 {
            // no active decode target, do not select
            // debugw!(
            //	self.base.logger,
            //	"drop packet for no target found",
            //	"highestDecodeTarget" => highest_decode_target,
            //	"decodeTargets" => self.decode_targets,
            //	"tagetLayer" => self.base.target_layer,
            //	"incoming" => incoming_layer,
            //	"fn" => dd.frame_number,
            //	"efn" => ext_frame_num,
            //	"sn" => ext_pkt.packet.sequence_number,
            //	"isKeyFrame" => ext_pkt.is_key_frame,
            // );
            self.decisions.lock().add_dropped(ext_frame_num);
            return result;
        }

        // DD-TODO : if bandwidth in congest, could drop the 'Discardable' frame
        if dti == dede::DecodeTargetIndication::NotPresent {
            // debugw!(
            //	self.base.logger,
            //	"drop packet for decode target not present",
            //	"highestDecodeTarget" => highest_decode_target,
            //	"decodeTargets" => self.decode_targets,
            //	"tagetLayer" => self.base.target_layer,
            //	"incoming" => incoming_layer,
            //	"fn" => dd.frame_number,
            //	"efn" => ext_frame_num,
            //	"sn" => ext_pkt.packet.sequence_number,
            //	"isKeyFrame" => ext_pkt.is_key_frame,
            // );
            self.decisions.lock().add_dropped(ext_frame_num);
            return result;
        }

        // check decodability using reference frames
        let mut is_decodable = true;
        for &fdiff in &fd.frame_diffs {
            if fdiff == 0 {
                continue;
            }

            // use relaxed check for frame diff that we have chain intact detection and don't want
            // to drop packet due to out-of-order packet or recoverable packet loss
            let sd = self
                .decisions
                .lock()
                .get_decision(ext_frame_num.wrapping_sub(fdiff as u64));
            if sd == Ok(SelectorDecision::Dropped) {
                is_decodable = false;
                break;
            }
        }
        if !is_decodable {
            // debugw!(
            //	self.base.logger,
            //	"drop packet for not decodable",
            //	"highestDecodeTarget" => highest_decode_target,
            //	"decodeTargets" => self.decode_targets,
            //	"tagetLayer" => self.base.target_layer,
            //	"incoming" => incoming_layer,
            //	"fn" => dd.frame_number,
            //	"efn" => ext_frame_num,
            //	"sn" => ext_pkt.packet.sequence_number,
            //	"isKeyFrame" => ext_pkt.is_key_frame,
            // );
            self.decisions.lock().add_dropped(ext_frame_num);
            return result;
        }

        if self.base.current_layer != highest_decode_target.layer {
            result.is_switching = true;
            if !self.base.current_layer.is_valid() {
                result.is_resuming = true;
                debugw!(
                    self.base.logger,
                    "resuming at layer",
                    "current" => incoming_layer,
                    "target" => self.base.target_layer,
                    "max" => self.base.max_layer,
                    "layer" => fd.spatial_id,
                    "req" => self.base.request_spatial,
                    "maxSeen" => self.base.max_seen_layer,
                    "feed" => ext_pkt.packet.ssrc,
                    "fn" => dd.frame_number,
                    "efn" => ext_frame_num,
                    "sn" => ext_pkt.packet.sequence_number,
                    "isKeyFrame" => ext_pkt.is_key_frame,
                );
            }

            self.base.previous_layer = self.base.current_layer;
            self.base.current_layer = highest_decode_target.layer;

            self.previous_active_decode_targets_bitmask = self.active_decode_targets_bitmask;
            let active_decode_targets_bitmask =
                get_active_decode_target_bitmask(self.base.current_layer, &ddwdt.decode_targets);
            self.active_decode_targets_bitmask = Some(active_decode_targets_bitmask);
            debugw!(
                self.base.logger,
                "switch to target",
                "highestDecodeTarget" => highest_decode_target,
                "previous" => self.base.previous_layer,
                "bitmask" => active_decode_targets_bitmask,
                "fn" => dd.frame_number,
                "efn" => ext_frame_num,
                "sn" => ext_pkt.packet.sequence_number,
                "isKeyFrame" => ext_pkt.is_key_frame,
            );

            result.is_relevant = true;
        }

        let mut dd_extension = dede::DependencyDescriptorExtension {
            descriptor: dd.clone(),
            structure: self.structure.clone(),
        };

        let un_wrap_fn = self
            .fn_wrapper
            .update_and_get(ext_frame_num, ddwdt.structure_updated) as u16;
        if un_wrap_fn != dd.frame_number {
            dd_extension.descriptor.frame_number = un_wrap_fn;
        }

        if dd.attached_structure.is_none() && self.active_decode_targets_bitmask.is_some() {
            // clone and override activebitmask
            // DD-TODO: if the packet that contains the bitmask is acknowledged by RR, then we don't need it until it changed.
            dd_extension.descriptor.active_decode_targets_bitmask =
                self.active_decode_targets_bitmask;
            // debugw!(self.base.logger, "set active decode targets bitmask", "activeDecodeTargetsBitmask" => self.active_decode_targets_bitmask);
        }

        let mut dd_marshaled = false;
        match catch_unwind(AssertUnwindSafe(|| {
            result.marshal_dependency_descriptor_extension(&dd_extension)
        })) {
            Err(r) => {
                errorw!(
                    self.base.logger,
                    "panic marshalling dependency descriptor extension",
                    nil,
                    "efn" => ext_frame_num,
                    "sn" => ext_pkt.packet.sequence_number,
                    "keyframeRequired" => ddwdt.ext_key_frame_num,
                    "currentKeyframe" => self.ext_key_frame_num,
                    "panic" => panic_message(r.as_ref()),
                );
            }
            Ok(Err(err)) => {
                warnw!(
                    self.base.logger,
                    "error marshalling dependency descriptor extension",
                    err
                );
            }
            Ok(Ok(())) => {
                dd_marshaled = true;
            }
        }

        if !dd_marshaled {
            // drop packet if we can't marshal dependency descriptor
            self.decisions.lock().add_dropped(ext_frame_num);
            return result;
        }

        if ddwdt.integrity {
            self.decisions.lock().add_forwarded(ext_frame_num);
        }
        result.rtp_marker = ext_pkt.packet.header.marker
            || (dd.last_packet_in_frame && self.base.current_layer.spatial == fd.spatial_id as i32);
        result.is_selected = true;
        result
    }

    fn rollback(&mut self) {
        self.active_decode_targets_bitmask = self.previous_active_decode_targets_bitmask;

        self.base.rollback();
    }

    fn check_sync(&self) -> (bool, i32) {
        let layer = self.get_request_spatial();
        if !self.base.current_layer.is_valid() || !self.key_frame_valid {
            // always declare not locked when trying to resume from nothing
            return (false, layer);
        }

        for dt in &self.decode_targets {
            if dt.active()
                && dt.dependency_descriptor_decode_target.layer.spatial == layer
                && dt.valid()
            {
                return (true, layer);
            }
        }

        (false, layer)
    }
}

impl DependencyDescriptor {
    fn update_dependency_structure(
        &mut self,
        structure: Arc<dede::FrameDependencyStructure>,
        decode_targets: &[buffer::DependencyDescriptorDecodeTarget],
        ext_frame_num: u64,
    ) {
        self.structure = Some(structure.clone());
        self.ext_key_frame_num = ext_frame_num;
        self.key_frame_valid = true;

        self.chains.clear();

        for chain_idx in 0..structure.num_chains {
            self.chains.push(FrameChain::new(
                self.decisions.clone(),
                chain_idx,
                self.base.logger.clone(),
            ));
        }

        let mut new_targets = Vec::with_capacity(decode_targets.len());
        for dt in decode_targets {
            let mut chain = None;
            // When chain_cnt > 0, each Decode target MUST be protected by exactly one Chain.
            if structure.num_chains > 0 {
                let chain_idx = structure.decode_target_protected_by_chain[dt.target as usize];
                if chain_idx >= self.chains.len() {
                    // should not happen
                    errorw!(self.base.logger, "DecodeTargetProtectedByChain chainIdx out of range", nil, "chainIdx" => chain_idx, "NumChains" => self.chains.len());
                } else {
                    chain = Some(self.chains[chain_idx].clone());
                }
            }
            new_targets.push(DecodeTarget::new(*dt, chain));
        }
        self.decode_targets = new_targets;
    }

    fn update_active_decode_targets(&mut self, active_decode_targets_bitmask: u32) {
        for chain in &self.chains {
            chain.begin_update_active();
        }

        for dt in &mut self.decode_targets {
            dt.update_active(active_decode_targets_bitmask);
        }

        for chain in &self.chains {
            chain.end_update_active();
        }
    }

    fn invalidate_key_frame(&mut self) {
        self.key_frame_valid = false;
        self.chains.clear();
        self.decode_targets.clear();
    }

    fn restart(&mut self, generation: i64) {
        self.restart_generation = generation;
        self.invalidate_key_frame();
        self.decisions = Arc::new(Mutex::new(SelectorDecisionCache::new(
            DECISION_CACHE_MAX_ELEMENTS,
            DECISION_CACHE_NACK_ENTRIES,
        )));
    }
}

// What a panic was raised with, for the log: the original logs what `recover()` gives.
fn panic_message(r: &(dyn Any + Send)) -> String {
    if let Some(s) = r.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = r.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic".to_string()
    }
}

// `GetActiveDecodeTargetBitmask` of pkg/sfu/buffer/dependencydescriptorparser.go, which is
// not ported yet apart from its two types: to be `buffer::get_active_decode_target_bitmask`
// when it is. It gives the bitmask itself, where the original gives a pointer that is never
// nil.
fn get_active_decode_target_bitmask(
    layer: buffer::VideoLayer,
    decode_targets: &[buffer::DependencyDescriptorDecodeTarget],
) -> u32 {
    let mut active_bit_mask = 0u32;
    for dt in decode_targets {
        if dt.layer.spatial <= layer.spatial && dt.layer.temporal <= layer.temporal {
            active_bit_mask |= shl(1, dt.target);
        }
    }

    active_bit_mask
}

// Open to the package: ddforwarder_test.rs uses its fixture.
#[cfg(test)]
#[path = "dependencydescriptor_test.rs"]
pub(super) mod dependencydescriptor_test;
