// Ported from LiveKit, pkg/sfu/rtpextension/dependencydescriptor/dependencydescriptorextension.go
// at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The errors of the package are one enum, `Error`, which is here: `ErrBufferTooSmall` of
//   this file, the `ErrDDReader...` values of the reader, and the errors the other files make
//   in place.
// - `DependencyDescriptorExtension` owns its descriptor (Go: a pointer to the caller's, which
//   must not be nil). `Unmarshal` leaves the result there.
// - The pointers of `DependencyDescriptor`: `FrameDependencies` and `AttachedStructure` are
//   `Option<Arc<..>>`, like the templates of a structure, because a descriptor may point at a
//   template of its structure and descriptors are copied with what they point at.
//   `Resolution` and `ActiveDecodeTargetsBitmask` are plain `Option`s: the value is in the
//   descriptor, there is no storage of the caller to write into.
// - `DecodeTargetIndication` is an enum of its four values, so `String()`'s "Unknown" is gone.
// - `FrameDependencyTemplate.Clone()` is the derived `Clone`.
// - A `Vec` is never nil, where Go's writer tells a nil slice from an empty one: see
//   `calculate_match` in dependencydescriptorwriter.rs.
// - `String()` is `Display`, with the text Go's fmt gives.
// - Of Go's `int`s the counts and indexes are `usize`, the identifiers, differences and
//   dimensions `i64`.

use std::fmt;
use std::sync::Arc;

use super::{
    DependencyDescriptorReader, DependencyDescriptorWriter, new_dependency_descriptor_writer,
};

// DependencyDescriptorExtension is a extension payload format in
// https://aomediacodec.github.io/av1-rtp-spec/#dependency-descriptor-rtp-header-extension

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    // ErrBufferTooSmall is returned by MarshalTo when the caller's storage cannot
    // hold the marshaled extension.
    #[error("dependency descriptor buffer too small")]
    BufferTooSmall,

    // dependencydescriptorreader.go
    #[error("DependencyDescriptorReader: Structure is nil")]
    DdReaderNoStructure,
    #[error(
        "DependencyDescriptorReader: has templateDependencyStructurePresentFlag but AttachedStructure is nil"
    )]
    DdReaderTemplateWithoutStructure,
    #[error("DependencyDescriptorReader: too many templates")]
    DdReaderTooManyTemplates,
    #[error("DependencyDescriptorReader: too many temporal layers")]
    DdReaderTooManyTemporalLayers,
    #[error("DependencyDescriptorReader: too many spatial layers")]
    DdReaderTooManySpatialLayers,
    #[error("DependencyDescriptorReader: invalid template index")]
    DdReaderInvalidTemplateIndex,
    #[error(
        "DependencyDescriptorReader: invalid spatial layer, should be less than the number of resolutions"
    )]
    DdReaderInvalidSpatialLayer,
    #[error(
        "DependencyDescriptorReader: decode target indications length mismatch with structure num decode targets"
    )]
    DdReaderNumDtiMismatch,
    #[error("DependencyDescriptorReader: chain diffs length mismatch with structure num chains")]
    DdReaderNumChainDiffsMismatch,

    // bitstreamreader.go
    #[error("invalid number of bits, expected 0-64")]
    InvalidNumberOfBits,
    // io.EOF
    #[error("EOF")]
    Eof,
    #[error("invalid number of values, expected 0-2^31")]
    InvalidNumberOfValues,

    // bitstreamwriter.go
    #[error("insufficient space")]
    InsufficientSpace,
    #[error("invalid argument, val {val}, numValues {num_values}")]
    InvalidArgument { val: u32, num_values: u32 },

    // dependencydescriptorwriter.go
    #[error("no template found for spatial layer {spatial_id} and temporal layer {temporal_id}")]
    NoTemplateFound { spatial_id: i64, temporal_id: i64 },
    #[error(
        "invalid arguments, structureId: {structure_id}, numDecodeTargets: {num_decode_targets}"
    )]
    InvalidArguments {
        structure_id: i64,
        num_decode_targets: usize,
    },
    #[error(
        "invalid templates, len {len}, templates[0]: spatialId {spatial_id}, temporalId {temporal_id}"
    )]
    InvalidTemplates {
        len: usize,
        spatial_id: i64,
        temporal_id: i64,
    },
    #[error("invalid next_layer_idc {0}")]
    InvalidNextLayerIdc(i64),
}

fn format_bitmask(b: Option<u32>) -> String {
    match b {
        None => "-".to_string(),
        Some(b) => format!("{:b}", b as i64),
    }
}

// What Go's fmt writes for a slice: the elements between brackets, with a space between them.
fn fmt_slice<T>(
    f: &mut fmt::Formatter<'_>,
    s: &[T],
    fmt_element: impl Fn(&mut fmt::Formatter<'_>, &T) -> fmt::Result,
) -> fmt::Result {
    f.write_str("[")?;
    for (i, v) in s.iter().enumerate() {
        if i > 0 {
            f.write_str(" ")?;
        }
        fmt_element(f, v)?;
    }
    f.write_str("]")
}

// What Go's fmt writes for a RenderResolution with %+v.
fn fmt_resolution(f: &mut fmt::Formatter<'_>, r: &RenderResolution) -> fmt::Result {
    write!(f, "{{Width:{} Height:{}}}", r.width, r.height)
}

// What Go's fmt writes for a FrameDependencyTemplate with %+v.
fn fmt_template(f: &mut fmt::Formatter<'_>, t: &FrameDependencyTemplate) -> fmt::Result {
    write!(
        f,
        "{{SpatialId:{} TemporalId:{} DecodeTargetIndications:",
        t.spatial_id, t.temporal_id
    )?;
    fmt_slice(f, &t.decode_target_indications, |f, v| write!(f, "{v}"))?;
    f.write_str(" FrameDiffs:")?;
    fmt_slice(f, &t.frame_diffs, |f, v| write!(f, "{v}"))?;
    f.write_str(" ChainDiffs:")?;
    fmt_slice(f, &t.chain_diffs, |f, v| write!(f, "{v}"))?;
    f.write_str("}")
}

// ------------------------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DependencyDescriptorExtension {
    pub descriptor: DependencyDescriptor,
    pub structure: Option<Arc<FrameDependencyStructure>>,
}

impl DependencyDescriptorExtension {
    /// Panics, as the original does, if there is no structure or the descriptor has no frame
    /// dependencies; so do the other functions that marshal.
    pub fn marshal(&self) -> Result<Vec<u8>, Error> {
        self.marshal_with_active_chains(!0u32)
    }

    pub fn marshal_with_active_chains(&self, active_chains: u32) -> Result<Vec<u8>, Error> {
        let (writer, size) = self.new_writer(active_chains)?;

        let mut buf = vec![0u8; size];
        let mut writer = writer.with_buf(&mut buf);
        writer.write()?;
        Ok(buf)
    }

    // MarshalTo marshals into caller owned storage and returns the number of bytes
    // written, or ErrBufferTooSmall, leaving buf untouched, if the extension does not fit.
    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, Error> {
        let (writer, size) = self.new_writer(!0u32)?;
        if buf.len() < size {
            // bare sentinel, key frames take this path and formatting an error there would allocate
            return Err(Error::BufferTooSmall);
        }

        let mut writer = writer.with_buf(&mut buf[..size]);
        writer.write()?;
        Ok(size)
    }

    // newWriter returns a writer for the extension, by value so that it stays on the
    // caller's stack, along with the number of bytes the marshaled extension needs.
    fn new_writer(
        &self,
        active_chains: u32,
    ) -> Result<(DependencyDescriptorWriter<'_>, usize), Error> {
        let mut writer = new_dependency_descriptor_writer(
            &mut [],
            self.structure.as_deref(),
            active_chains,
            &self.descriptor,
        );
        writer.find_best_template()?;
        let size = (writer.value_size_bits() as f64 / 8.0).ceil() as usize;
        Ok((writer, size))
    }

    pub fn unmarshal(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let mut reader =
            DependencyDescriptorReader::new(buf, self.structure.clone(), &mut self.descriptor);
        reader.parse()
    }
}

// ------------------------------------------------------------------------------

pub const MAX_SPATIAL_IDS: i64 = 4;
pub const MAX_TEMPORAL_IDS: i64 = 8;
pub const MAX_DECODE_TARGETS: usize = 32;
pub const MAX_TEMPLATES: usize = 64;

// MaxInlineExtensionSize is the inline storage the forwarding path reserves for a marshaled extension, pion's one byte profile cap
pub const MAX_INLINE_EXTENSION_SIZE: usize = 16;

pub const ALL_CHAINS_ARE_ACTIVE: u32 = 0;

pub const EXTENSION_URI: &str =
    "https://aomediacodec.github.io/av1-rtp-spec/#dependency-descriptor-rtp-header-extension";

// ------------------------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DependencyDescriptor {
    pub first_packet_in_frame: bool,
    pub last_packet_in_frame: bool,
    pub frame_number: u16,
    // Unmarshal writes into FrameDependencies and Resolution when they are set, else it allocates them
    //
    // In Rust: into `frame_dependencies` when nothing else holds that `Arc`; a `resolution` is
    // a value of the descriptor.
    pub frame_dependencies: Option<Arc<FrameDependencyTemplate>>,
    pub resolution: Option<RenderResolution>,
    pub active_decode_targets_bitmask: Option<u32>,
    pub attached_structure: Option<Arc<FrameDependencyStructure>>,
}

impl DependencyDescriptor {
    pub fn marshal_size(&self) -> Result<usize, Error> {
        self.marshal_size_with_active_chains(!0u32)
    }

    pub fn marshal_size_with_active_chains(&self, active_chains: u32) -> Result<usize, Error> {
        let writer = DependencyDescriptorWriter::new(
            &mut [],
            self.attached_structure.as_deref(),
            active_chains,
            self,
        )?;
        Ok((writer.value_size_bits() as f64 / 8.0).ceil() as usize)
    }
}

impl fmt::Display for DependencyDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DependencyDescriptor{{FirstPacketInFrame: {}, LastPacketInFrame: {}, FrameNumber: {}, FrameDependencies: ",
            self.first_packet_in_frame, self.last_packet_in_frame, self.frame_number
        )?;
        match &self.frame_dependencies {
            Some(dependencies) => fmt_template(f, dependencies)?,
            None => f.write_str("-")?,
        }
        f.write_str(", Resolution: ")?;
        match &self.resolution {
            Some(resolution) => fmt_resolution(f, resolution)?,
            None => f.write_str("-")?,
        }
        write!(
            f,
            ", ActiveDecodeTargetsBitmask: {}, AttachedStructure: ",
            format_bitmask(self.active_decode_targets_bitmask)
        )?;
        match &self.attached_structure {
            Some(structure) => write!(f, "{structure}")?,
            None => f.write_str("<nil>")?,
        }
        f.write_str("}")
    }
}

// ------------------------------------------------------------------------------

// Relationship of a frame to a Decode target.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum DecodeTargetIndication {
    #[default]
    NotPresent = 0, // DecodeTargetInfo symbol '-'
    Discardable = 1, // DecodeTargetInfo symbol 'D'
    Switch = 2,      // DecodeTargetInfo symbol 'S'
    Required = 3,    // DecodeTargetInfo symbol 'R'
}

impl DecodeTargetIndication {
    // The original's conversion `DecodeTargetIndication(indication)` of the two bits that the
    // reader has read.
    pub(super) fn from_bits(indication: u64) -> Self {
        match indication & 3 {
            0 => DecodeTargetIndication::NotPresent,
            1 => DecodeTargetIndication::Discardable,
            2 => DecodeTargetIndication::Switch,
            _ => DecodeTargetIndication::Required,
        }
    }
}

impl fmt::Display for DecodeTargetIndication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DecodeTargetIndication::NotPresent => "-",
            DecodeTargetIndication::Discardable => "D",
            DecodeTargetIndication::Switch => "S",
            DecodeTargetIndication::Required => "R",
        })
    }
}

// ------------------------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FrameDependencyTemplate {
    pub spatial_id: i64,
    pub temporal_id: i64,
    pub decode_target_indications: Vec<DecodeTargetIndication>,
    pub frame_diffs: Vec<i64>,
    pub chain_diffs: Vec<i64>,
}

impl FrameDependencyTemplate {
    // CopyInto copies t into dst and reuses the slices of dst
    pub fn copy_into(&self, dst: &mut FrameDependencyTemplate) {
        dst.spatial_id = self.spatial_id;
        dst.temporal_id = self.temporal_id;
        dst.decode_target_indications.clear();
        dst.decode_target_indications
            .extend_from_slice(&self.decode_target_indications);
        dst.frame_diffs.clear();
        dst.frame_diffs.extend_from_slice(&self.frame_diffs);
        dst.chain_diffs.clear();
        dst.chain_diffs.extend_from_slice(&self.chain_diffs);
    }
}

// ------------------------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FrameDependencyStructure {
    pub structure_id: i64,
    pub num_decode_targets: usize,
    pub num_chains: usize,
    // If chains are used (num_chains > 0), maps decode target index into index of
    // the chain protecting that target.
    pub decode_target_protected_by_chain: Vec<usize>,
    pub resolutions: Vec<RenderResolution>,
    pub templates: Vec<Arc<FrameDependencyTemplate>>,
}

impl fmt::Display for FrameDependencyStructure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FrameDependencyStructure{{StructureId: {}, NumDecodeTargets: {}, NumChains: {}, DecodeTargetProtectedByChain: ",
            self.structure_id, self.num_decode_targets, self.num_chains
        )?;
        fmt_slice(f, &self.decode_target_protected_by_chain, |f, v| {
            write!(f, "{v}")
        })?;
        f.write_str(", Resolutions: ")?;
        fmt_slice(f, &self.resolutions, fmt_resolution)?;
        f.write_str(", Templates: [")?;

        // templates
        for t in &self.templates {
            f.write_str("&")?;
            fmt_template(f, t)?;
            f.write_str(", ")?;
        }
        f.write_str("]}")
    }
}

// ------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RenderResolution {
    pub width: i64,
    pub height: i64,
}

// ------------------------------------------------------------------------------

#[cfg(test)]
#[path = "dependencydescriptorextension_test.rs"]
mod dependencydescriptorextension_test;
