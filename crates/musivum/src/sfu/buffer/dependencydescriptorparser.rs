// Ported from LiveKit, pkg/sfu/buffer/dependencydescriptorparser.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the two types that travel with a packet are here
// so far; the parser itself follows. `ExtDependencyDescriptor` holds its descriptor by value
// (the original points into storage of its own inside the same struct, to keep parsing from
// allocating), absent where the original's pointer is nil.

use super::VideoLayer;
use crate::sfu::rtpextension::dependencydescriptor as dd;

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DependencyDescriptorDecodeTarget {
    pub target: i64,
    pub layer: VideoLayer,
}
