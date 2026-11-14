// Ported from LiveKit, pkg/sfu/buffer/dependencydescriptorparser.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `ExtDependencyDescriptor` holds its descriptor by value, absent where the original's
//   pointer is nil. The original points into storage of its own inside the same struct and
//   takes the structs from a pool, both to keep parsing from allocating: the storage fields,
//   `ExtDependencyDescriptorFactory` and `ReleaseExtDependencyDescriptor` have no
//   counterpart, a descriptor is a value that is dropped when its owner is done with it. Its
//   decode targets are a copy of the parser's, where the original's slice shares them.
// - `parse` returns the descriptor with the video layer, or the error. With an error the
//   original also returns the video layer as far as it has read it, which no caller looks
//   at.
// - The three `ErrX` values are variants of the package's `Error`; an error of the reader of
//   the extension, which `parse` passes on, is its variant `DependencyDescriptor`.
// - `onMaxLayerChanged` cannot be nil, and the parser, which has no lock, takes `&mut self`.
// - `get_active_decode_target_bitmask` returns the mask, not a pointer to it, and
//   `extract_dependency_descriptor_video_size` an empty `Vec` where the original returns
//   nil.
// - The decode targets are sorted with a stable sort. Go's `slices.SortFunc` is stable up to
//   12 elements only, so with more decode targets than that, those of the same layer may
//   come in another order than in the original.
// - The shifts by the number of a decode target go through `shl`, which gives what Go gives
//   for a count of 32 or more (zero) and for a negative one (a panic).
// - `String()` is `Display`.

use std::fmt;
use std::sync::Arc;

use super::{Error, FrameIntegrityChecker, VideoLayer};
use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::mediatransportutil::codec;
use crate::mediatransportutil::utils;
use crate::protocol::logger::Logger;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::webrtc::rtp;
use crate::{debugw, infow, warnw};

// --------------------------------------

const DD_RESTART_THRESHOLD: Duration = Duration::from_secs(30);

// frame integrity check 2 seconds for L3T3 30fps video
const INTEGRITY_CHECK_FRAME: usize = 180;
const INTEGRITY_CHECK_PKT: usize = 1024;

pub struct DependencyDescriptorParser {
    structure: Option<Arc<dd::FrameDependencyStructure>>,
    dd_ext_id: u8,
    logger: Logger,
    on_max_layer_changed: Box<dyn Fn(i32, i32) + Send + Sync>,
    decode_targets: Vec<DependencyDescriptorDecodeTarget>,

    seq_wrap_around: utils::WrapAround<u16, u64>,
    frame_wrap_around: utils::WrapAround<u16, u64>,
    structure_ext_frame_num: u64,
    // drop threshold for frames belonging to a previous dependency structure.
    // advanced only when the structure id actually changes: structureExtFrameNum
    // advances on every structure-bearing key frame, so with frequent key frames
    // repeating the same structure (e. g. screen content), valid late/retransmitted
    // frames would be dropped as "earlier than current structure".
    structure_change_ext_frame_num: u64,
    has_active_decode_targets: bool, // the first mask is taken whatever its sequence number
    active_decode_targets_ext_seq: u64,
    active_decode_targets_mask: u32,
    frame_checker: FrameIntegrityChecker,

    dd_not_found_count: atomic::Uint32,

    // restart detection
    restart_generation: i64,
    enable_restart: bool,
    last_packet_at: Time,
}

impl DependencyDescriptorParser {
    pub fn new(
        dd_ext_id: u8,
        logger: Logger,
        on_max_layer_changed: impl Fn(i32, i32) + Send + Sync + 'static,
        enable_restart: bool,
    ) -> Self {
        Self {
            structure: None,
            dd_ext_id,
            logger,
            on_max_layer_changed: Box::new(on_max_layer_changed),
            decode_targets: Vec::new(),
            seq_wrap_around: utils::WrapAround::new(utils::WrapAroundParams {
                is_restart_allowed: false,
            }),
            frame_wrap_around: utils::WrapAround::new(utils::WrapAroundParams {
                is_restart_allowed: false,
            }),
            structure_ext_frame_num: 0,
            structure_change_ext_frame_num: 0,
            has_active_decode_targets: false,
            active_decode_targets_ext_seq: 0,
            active_decode_targets_mask: 0,
            frame_checker: FrameIntegrityChecker::new(INTEGRITY_CHECK_FRAME, INTEGRITY_CHECK_PKT),
            dd_not_found_count: atomic::Uint32::new(0),
            restart_generation: 0,
            enable_restart,
            last_packet_at: Time::ZERO,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtDependencyDescriptor {
    pub descriptor: Option<dd::DependencyDescriptor>,

    pub decode_targets: Vec<DependencyDescriptorDecodeTarget>,
    pub structure_updated: bool,
    pub active_decode_targets_updated: bool,
    // the stream's current active decode targets, on every packet, also when the mask
    // changed on a packet the subscriber does not get
    pub active_decode_targets_mask: u32,
    pub integrity: bool,
    pub ext_frame_num: u64,
    // the frame number of the keyframe which the current frame depends on
    pub ext_key_frame_num: u64,

    // increase when the stream restarts, clear and reinitialize all dd state includes
    // attached structure, frame chain, decode target.
    pub restart_generation: i64,
}

impl DependencyDescriptorParser {
    pub fn parse(
        &mut self,
        pkt: &rtp::Packet,
    ) -> Result<(ExtDependencyDescriptor, VideoLayer), Error> {
        let mut video_layer = VideoLayer::default();
        let Some(dd_buf) = pkt.get_extension(self.dd_ext_id) else {
            let dd_not_found_count = self.dd_not_found_count.inc();
            if dd_not_found_count.is_multiple_of(100) {
                warnw!(self.logger, "dependency descriptor extension is not present", nil, "seq" => pkt.sequence_number, "count" => dd_not_found_count);
            }
            return Err(Error::DdExtentionNotFound);
        };

        let mut ext_dd = ExtDependencyDescriptor::default();
        let mut ext = dd::DependencyDescriptorExtension {
            descriptor: dd::DependencyDescriptor::default(),
            structure: self.structure.clone(),
        };
        if let Err(err) = ext.unmarshal(&dd_buf) {
            if err != dd::Error::DdReaderNoStructure
                && err != dd::Error::DdReaderInvalidTemplateIndex
            {
                // The original passes the error where a key is expected, which its logger
                // writes as the error of the line.
                infow!(self.logger, "failed to parse generic dependency descriptor", "error" => format_args!("{err}"), "payload" => pkt.payload_type, "ddbufLen" => dd_buf.len());
            }
            return Err(err.into());
        }
        let dd_val = ext.descriptor;

        let mut restart = false;
        if self.enable_restart {
            if !self.last_packet_at.is_zero()
                && time::since(self.last_packet_at) > DD_RESTART_THRESHOLD
            {
                self.restart();
                restart = true;
                debugw!(
                    self.logger,
                    "dependency descriptor parser restart stream",
                    "generation" => self.restart_generation,
                    "lastPacketAt" => self.last_packet_at,
                    "sinceLast" => time::since(self.last_packet_at),
                    "frameWrapAround" => self.frame_wrap_around,
                );
            }
            self.last_packet_at = time::now();
        }

        let ext_seq = self
            .seq_wrap_around
            .update(pkt.sequence_number)
            .extended_val;

        if let Some(frame_dependencies) = &dd_val.frame_dependencies {
            (video_layer.spatial, video_layer.temporal) = (
                frame_dependencies.spatial_id as i32,
                frame_dependencies.temporal_id as i32,
            );
        }

        // assume the packet is in-order when stream restarting
        let unwrapped = self
            .frame_wrap_around
            .update_with_order_known(dd_val.frame_number, restart);
        let ext_fn = unwrapped.extended_val;

        if ext_fn < self.structure_change_ext_frame_num {
            debugw!(
                self.logger,
                "drop frame which is earlier than current structure",
                "fn" => dd_val.frame_number,
                "extFN" => ext_fn,
                "structureChangeExtFrameNum" => self.structure_change_ext_frame_num,
                "unwrappedFN" => unwrapped,
                "frameWrapAround" => self.frame_wrap_around,
            );
            return Err(Error::FrameEarlierThanKeyFrame);
        }

        self.frame_checker.add_packet(ext_seq, ext_fn, &dd_val);

        // `extDD.Descriptor = ddVal` is at the end: the descriptor is read until there, and
        // then moved.
        ext_dd.ext_frame_num = ext_fn;
        ext_dd.integrity = self.frame_checker.frame_integrity(ext_fn);
        ext_dd.restart_generation = self.restart_generation;

        if let Some(attached_structure) = &dd_val.attached_structure {
            if !dd_val.first_packet_in_frame {
                warnw!(
                    self.logger,
                    "attached structure is not the first packet in frame", nil,
                    "sn" => pkt.sequence_number,
                    "extSeq" => ext_seq,
                    "fn" => dd_val.frame_number,
                    "extFN" => ext_fn,
                );
                return Err(Error::DdStructureAttachedToNonFirstPacket);
            }

            if ext_fn < self.structure_ext_frame_num {
                // out-of-order key frame repeating the current structure: accepting it
                // would regress structureExtFrameNum (ExtKeyFrameNum) and replay a stale
                // structure update, confusing the downtrack's dependency descriptor
                // selector.
                debugw!(
                    self.logger,
                    "drop out-of-order key frame",
                    "extFN" => ext_fn,
                    "structureExtFrameNum" => self.structure_ext_frame_num,
                );
                return Err(Error::FrameEarlierThanKeyFrame);
            }

            if self
                .structure
                .as_ref()
                .is_none_or(|structure| attached_structure.structure_id != structure.structure_id)
            {
                // structure actually changed (or first structure): advance the drop threshold
                // so that only frames preceding this structure are dropped.
                self.structure_change_ext_frame_num = ext_fn;
                debugw!(
                    self.logger,
                    "structure updated",
                    "structureID" => attached_structure.structure_id,
                    "sn" => pkt.sequence_number,
                    "extSeq" => ext_seq,
                    "fn" => dd_val.frame_number,
                    "extFN" => ext_fn,
                    "descriptor" => dd_val.to_string(),
                    "unwrappedFN" => unwrapped,
                    "frameWrapAround" => self.frame_wrap_around,
                );
            }
            self.structure = Some(attached_structure.clone());
            self.decode_targets = process_frame_dependency_structure(attached_structure);
            if ext_fn > unwrapped.pre_extended_highest
                && ext_fn.wrapping_sub(unwrapped.pre_extended_highest) > 1000
            {
                debugw!(
                    self.logger,
                    "large frame number jump on structure updating",
                    "fn" => dd_val.frame_number,
                    "extFN" => ext_fn,
                    "preExtendedHighest" => unwrapped.pre_extended_highest,
                    "structureExtFrameNum" => self.structure_ext_frame_num,
                    "unwrappedFN" => unwrapped,
                    "frameWrapAround" => self.frame_wrap_around,
                );
            }
            self.structure_ext_frame_num = ext_fn;
            ext_dd.structure_updated = true;
            ext_dd.active_decode_targets_updated = true;
            // The dependency descriptor reader will always set ActiveDecodeTargetsBitmask for TemplateDependencyStructure is present,
            // so don't need to notify max layer change here.
        }

        if let Some(mask) = dd_val.active_decode_targets_bitmask
            && (!self.has_active_decode_targets || ext_seq > self.active_decode_targets_ext_seq)
        {
            self.has_active_decode_targets = true;
            self.active_decode_targets_ext_seq = ext_seq;
            if mask != self.active_decode_targets_mask {
                self.active_decode_targets_mask = mask;
                ext_dd.active_decode_targets_updated = true;
                let (mut max_spatial, mut max_temporal) = (0i32, 0i32);
                for dt in &self.decode_targets {
                    if mask & shl(1, dt.target) != dd::DecodeTargetIndication::NotPresent as u32 {
                        if max_spatial < dt.layer.spatial {
                            max_spatial = dt.layer.spatial;
                        }
                        if max_temporal < dt.layer.temporal {
                            max_temporal = dt.layer.temporal;
                        }
                    }
                }
                debugw!(self.logger, "max layer changed", "maxSpatial" => max_spatial, "maxTemporal" => max_temporal);
                (self.on_max_layer_changed)(max_spatial, max_temporal);
            }
        }

        ext_dd.decode_targets = self.decode_targets.clone();
        ext_dd.ext_key_frame_num = self.structure_ext_frame_num;
        ext_dd.active_decode_targets_mask = self.active_decode_targets_mask;
        ext_dd.descriptor = Some(dd_val);

        Ok((ext_dd, video_layer))
    }

    fn restart(&mut self) {
        self.frame_checker = FrameIntegrityChecker::new(INTEGRITY_CHECK_FRAME, INTEGRITY_CHECK_PKT);
        self.structure = None;
        self.structure_ext_frame_num = 0;
        self.structure_change_ext_frame_num = 0;
        self.has_active_decode_targets = false;
        self.active_decode_targets_ext_seq = 0;
        self.active_decode_targets_mask = 0;
        self.decode_targets.clear();
        self.restart_generation += 1;
    }
}

// ------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DependencyDescriptorDecodeTarget {
    pub target: i64,
    pub layer: VideoLayer,
}

impl fmt::Display for DependencyDescriptorDecodeTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DecodeTarget{{t: {}, l: {}}}", self.target, self.layer)
    }
}

// ------------------------------------------------------------------------------

pub fn process_frame_dependency_structure(
    structure: &dd::FrameDependencyStructure,
) -> Vec<DependencyDescriptorDecodeTarget> {
    let mut decode_targets = Vec::with_capacity(structure.num_decode_targets);
    for target in 0..structure.num_decode_targets {
        let mut layer = VideoLayer {
            spatial: 0,
            temporal: 0,
        };
        for t in &structure.templates {
            if t.decode_target_indications[target] != dd::DecodeTargetIndication::NotPresent {
                if layer.spatial < t.spatial_id as i32 {
                    layer.spatial = t.spatial_id as i32;
                }
                if layer.temporal < t.temporal_id as i32 {
                    layer.temporal = t.temporal_id as i32;
                }
            }
        }
        decode_targets.push(DependencyDescriptorDecodeTarget {
            target: target as i64,
            layer,
        });
    }

    // sort decode target layer by spatial and temporal from high to low
    decode_targets.sort_by(|a, b| {
        if a.layer.greater_than(b.layer) {
            return std::cmp::Ordering::Less;
        }
        if b.layer.greater_than(a.layer) {
            return std::cmp::Ordering::Greater;
        }
        std::cmp::Ordering::Equal
    });

    decode_targets
}

pub fn get_active_decode_target_bitmask(
    layer: VideoLayer,
    decode_targets: &[DependencyDescriptorDecodeTarget],
) -> u32 {
    let mut active_bit_mask = 0u32;
    for dt in decode_targets {
        if dt.layer.spatial <= layer.spatial && dt.layer.temporal <= layer.temporal {
            active_bit_mask |= shl(1, dt.target);
        }
    }

    active_bit_mask
}

// Go's `v << count` of a `uint32` for a count that is not a constant: zero for a count of 32
// or more, where Rust would panic, and a panic for a negative count, as in Go.
fn shl(v: u32, count: i64) -> u32 {
    assert!(count >= 0, "negative shift amount");
    u32::try_from(count)
        .ok()
        .and_then(|count| v.checked_shl(count))
        .unwrap_or(0)
}

// ------------------------------------------------------------------------------

pub fn extract_dependency_descriptor_video_size(
    dd: &dd::DependencyDescriptor,
) -> Vec<codec::VideoSize> {
    let Some(attached_structure) = &dd.attached_structure else {
        return Vec::new();
    };

    let mut video_sizes = Vec::with_capacity(attached_structure.resolutions.len());
    for res in &attached_structure.resolutions {
        video_sizes.push(codec::VideoSize {
            width: res.width as u32,
            height: res.height as u32,
        });
    }

    video_sizes
}

#[cfg(test)]
#[path = "dependencydescriptorparser_test.rs"]
mod dependencydescriptorparser_test;
