// Ported from LiveKit, pkg/sfu/rtpextension/dependencydescriptor/dependencydescriptorwriter.go
// at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The writer borrows the descriptor, the structure and the buffer. `with_buf` takes the
//   writer and gives it back with the other buffer. `new` returns the writer or the error,
//   not both.
// - A structure or frame dependencies that are nil are a nil dereference in the original, and
//   a panic here: `structure` and `frame_dependencies` at the end of the file.
// - `calculate_match`: a `Vec` is never nil. Frame diffs are always taken as given; decode
//   target indications that are empty are taken as not given. See there.
// - The errors are variants of the package's `Error`.

use super::{
    BitStreamWriter, DependencyDescriptor, Error, FrameDependencyStructure,
    FrameDependencyTemplate, GoShift, MAX_DECODE_TARGETS, MAX_TEMPLATES, NextLayerIdcType,
    size_non_symmetric_bits,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TemplateMatch {
    pub template_idx: usize,
    pub need_custom_dtis: bool,
    pub need_custom_fdiffs: bool,
    pub need_custom_chains: bool,
    // Size in bits to store frame-specific details, i.e.
    // excluding mandatory fields and template dependency structure.
    pub extra_size_bits: i64,
}

#[derive(Debug)]
pub struct DependencyDescriptorWriter<'a> {
    descriptor: &'a DependencyDescriptor,
    structure: Option<&'a FrameDependencyStructure>,
    active_chains: u32,
    writer: BitStreamWriter<'a>,
    best_template: TemplateMatch,
}

pub(super) fn new_dependency_descriptor_writer<'a>(
    buf: &'a mut [u8],
    structure: Option<&'a FrameDependencyStructure>,
    active_chains: u32,
    descriptor: &'a DependencyDescriptor,
) -> DependencyDescriptorWriter<'a> {
    DependencyDescriptorWriter {
        descriptor,
        structure,
        active_chains,
        writer: BitStreamWriter::new(buf),
        best_template: TemplateMatch::default(),
    }
}

impl<'a> DependencyDescriptorWriter<'a> {
    pub fn new(
        buf: &'a mut [u8],
        structure: Option<&'a FrameDependencyStructure>,
        active_chains: u32,
        descriptor: &'a DependencyDescriptor,
    ) -> Result<Self, Error> {
        let mut w = new_dependency_descriptor_writer(buf, structure, active_chains, descriptor);
        w.find_best_template()?;
        Ok(w)
    }

    // withBuf returns a copy of the writer that writes into buf, value receiver so a caller owned buffer does not escape
    pub(super) fn with_buf(mut self, buf: &'a mut [u8]) -> Self {
        self.writer = BitStreamWriter::new(buf);
        self
    }

    pub fn write(&mut self) -> Result<(), Error> {
        self.find_best_template()?;

        self.write_mandatory_fields()?;

        if self.has_extended_fields() {
            self.write_extended_fields()?;

            self.write_frame_dependency_definition()?;
        }

        let remaining_bits = self.writer.remaining_bits();
        // Zero remaining memory to avoid leaving it uninitialized.
        if remaining_bits % 64 != 0 {
            self.write_bits(0, remaining_bits % 64)?;
        }

        for _ in 0..remaining_bits / 64 {
            self.write_bits(0, 64)?;
        }

        Ok(())
    }

    pub(super) fn find_best_template(&mut self) -> Result<(), Error> {
        let structure = self.structure();
        let frame_dependencies = self.frame_dependencies();
        // Find templates with same spatial and temporal layer of frame dependency.
        let mut first_same_layer: Option<&FrameDependencyTemplate> = None;
        let (mut first_same_layer_idx, mut last_same_layer_idx) = (0usize, 0usize);
        for (i, t) in structure.templates.iter().enumerate() {
            if frame_dependencies.spatial_id == t.spatial_id
                && frame_dependencies.temporal_id == t.temporal_id
            {
                first_same_layer = Some(t);
                first_same_layer_idx = i;
                break;
            }
        }

        let Some(first_same_layer) = first_same_layer else {
            return Err(Error::NoTemplateFound {
                spatial_id: frame_dependencies.spatial_id,
                temporal_id: frame_dependencies.temporal_id,
            });
        };

        for (i, t) in structure.templates[first_same_layer_idx..]
            .iter()
            .enumerate()
        {
            if frame_dependencies.spatial_id != t.spatial_id
                || frame_dependencies.temporal_id != t.temporal_id
            {
                last_same_layer_idx = i + first_same_layer_idx;
            }
        }

        // Search if there any better template that have small extra size.
        self.best_template = self.calculate_match(first_same_layer_idx, first_same_layer);
        for i in first_same_layer_idx + 1..=last_same_layer_idx {
            let t = &structure.templates[i];
            let template_match = self.calculate_match(i, t);
            if template_match.extra_size_bits < self.best_template.extra_size_bits {
                self.best_template = template_match;
            }
        }
        Ok(())
    }

    fn calculate_match(&self, idx: usize, template: &FrameDependencyTemplate) -> TemplateMatch {
        let structure = self.structure();
        let frame_dependencies = self.frame_dependencies();
        let mut result = TemplateMatch {
            template_idx: idx,
            ..Default::default()
        };
        // The original asks for custom frame diffs and custom decode target indications only
        // if the slice of the descriptor is not nil (`FrameDiffs != nil && ...`): one that is
        // nil stands for "as the template has it". A `Vec` cannot be nil, so:
        // - Frame diffs are always taken as given, the empty list as "this frame has none".
        //   That is what the original does for every descriptor it has read into the buffer's
        //   pooled storage, which is never nil. It differs for a descriptor made by hand
        //   without frame diffs, or read without storage from a template that has none: the
        //   original would send that with a template that has frame diffs, unchanged.
        // - Decode target indications that are empty are taken as nil. A descriptor that
        //   states them has one for each decode target, of which there is at least one.
        result.need_custom_fdiffs = frame_dependencies.frame_diffs != template.frame_diffs;
        result.need_custom_dtis = !frame_dependencies.decode_target_indications.is_empty()
            && frame_dependencies.decode_target_indications != template.decode_target_indications;

        for i in 0..structure.num_chains {
            if self.active_chains & 1u32.go_shl(i as i64) != 0
                && (frame_dependencies.chain_diffs.len() <= i
                    || template.chain_diffs.len() <= i
                    || frame_dependencies.chain_diffs[i] != template.chain_diffs[i])
            {
                result.need_custom_chains = true;
                break;
            }
        }

        if result.need_custom_fdiffs {
            result.extra_size_bits = 2 * (1 + frame_dependencies.frame_diffs.len() as i64);

            for &fdiff in &frame_dependencies.frame_diffs {
                if fdiff <= (1 << 4) {
                    result.extra_size_bits += 4;
                } else if fdiff <= (1 << 8) {
                    result.extra_size_bits += 8;
                } else {
                    result.extra_size_bits += 12;
                }
            }
        }

        if result.need_custom_dtis {
            result.extra_size_bits += 2 * frame_dependencies.decode_target_indications.len() as i64;
        }
        if result.need_custom_chains {
            result.extra_size_bits = result
                .extra_size_bits
                .wrapping_add(8i64.wrapping_mul(structure.num_chains as i64));
        }

        result
    }

    fn write_mandatory_fields(&mut self) -> Result<(), Error> {
        self.write_bool(self.descriptor.first_packet_in_frame)?;

        self.write_bool(self.descriptor.last_packet_in_frame)?;

        let template_id = (self.best_template.template_idx as i64)
            .wrapping_add(self.structure().structure_id)
            % MAX_TEMPLATES as i64;

        self.write_bits(template_id as u64, 6)?;

        self.write_bits(self.descriptor.frame_number as u64, 16)
    }

    fn write_bool(&mut self, val: bool) -> Result<(), Error> {
        let mut v = 0u64;
        if val {
            v = 1;
        }
        self.writer.write_bits(v, 1)
    }

    fn write_bits(&mut self, val: u64, bit_count: i64) -> Result<(), Error> {
        self.writer.write_bits(val, bit_count)?;
        Ok(())
    }

    fn has_extended_fields(&self) -> bool {
        self.best_template.extra_size_bits > 0
            || self.descriptor.attached_structure.is_some()
            || self.descriptor.active_decode_targets_bitmask.is_some()
    }

    fn write_extended_fields(&mut self) -> Result<(), Error> {
        // template_dependency_structure_present_flag
        self.write_bool(self.descriptor.attached_structure.is_some())?;

        // active_decode_targets_present_flag
        let active_decode_targets_present_flag = self.should_write_active_decode_targets_bitmask();
        self.write_bool(active_decode_targets_present_flag)?;

        // need_custom_dtis
        self.write_bool(self.best_template.need_custom_dtis)?;

        // need_custom_fdiffs
        self.write_bool(self.best_template.need_custom_fdiffs)?;

        // need_custom_chains
        self.write_bool(self.best_template.need_custom_chains)?;

        // template_dependency_structure
        if self.descriptor.attached_structure.is_some() {
            self.write_template_dependency_structure()?;
        }

        // active_decode_targets_bitmask
        if active_decode_targets_present_flag {
            // The flag is not set without a bitmask.
            let active_decode_targets_bitmask = self
                .descriptor
                .active_decode_targets_bitmask
                .expect("DependencyDescriptorWriter: ActiveDecodeTargetsBitmask is nil");
            self.write_bits(
                active_decode_targets_bitmask as u64,
                self.structure().num_decode_targets as i64,
            )?;
        }

        Ok(())
    }

    fn write_template_dependency_structure(&mut self) -> Result<(), Error> {
        let structure = self.structure();
        // `NumDecodeTargets <= 0` of the original: the count cannot be below zero here.
        if structure.structure_id < 0
            || structure.structure_id >= MAX_TEMPLATES as i64
            || structure.num_decode_targets == 0
            || structure.num_decode_targets > MAX_DECODE_TARGETS
        {
            return Err(Error::InvalidArguments {
                structure_id: structure.structure_id,
                num_decode_targets: structure.num_decode_targets,
            });
        }

        self.write_bits(structure.structure_id as u64, 6)?;

        self.write_bits((structure.num_decode_targets - 1) as u64, 5)?;

        self.write_template_layers()?;

        self.write_template_dtis()?;

        self.write_template_fdiffs()?;

        self.write_template_chains()?;

        let has_resolutions = !structure.resolutions.is_empty();
        self.write_bool(has_resolutions)?;
        self.write_resolutions()
    }

    fn write_template_layers(&mut self) -> Result<(), Error> {
        let templates = &self.structure().templates;
        if templates.is_empty()
            || templates.len() > MAX_TEMPLATES
            || templates[0].spatial_id != 0
            || templates[0].temporal_id != 0
        {
            // As in the original, this would index a structure without templates; but
            // `find_best_template` has failed before on one.
            return Err(Error::InvalidTemplates {
                len: templates.len(),
                spatial_id: templates[0].spatial_id,
                temporal_id: templates[0].temporal_id,
            });
        }
        for i in 1..templates.len() {
            let next_layer_idc = get_next_layer_idc(&templates[i - 1], &templates[i]);
            if next_layer_idc as i64 >= 3 {
                return Err(Error::InvalidNextLayerIdc(next_layer_idc as i64));
            }
            self.write_bits(next_layer_idc as u64, 2)?;
        }
        self.write_bits(NextLayerIdcType::NoMoreLayer as u64, 2)
    }
}

fn get_next_layer_idc(
    prev_template: &FrameDependencyTemplate,
    next_template: &FrameDependencyTemplate,
) -> NextLayerIdcType {
    if next_template.spatial_id == prev_template.spatial_id
        && next_template.temporal_id == prev_template.temporal_id
    {
        return NextLayerIdcType::SameLayer;
    } else if next_template.spatial_id == prev_template.spatial_id
        && next_template.temporal_id == prev_template.temporal_id.wrapping_add(1)
    {
        return NextLayerIdcType::NextTemporalLayer;
    } else if next_template.spatial_id == prev_template.spatial_id.wrapping_add(1)
        && next_template.temporal_id == 0
    {
        return NextLayerIdcType::NextSpatialLayer;
    }

    NextLayerIdcType::InvalidLayer
}

impl DependencyDescriptorWriter<'_> {
    fn write_template_dtis(&mut self) -> Result<(), Error> {
        for t in &self.structure().templates {
            for &dti in &t.decode_target_indications {
                self.write_bits(dti as u64, 2)?;
            }
        }

        Ok(())
    }

    fn write_template_fdiffs(&mut self) -> Result<(), Error> {
        for t in &self.structure().templates {
            for &fdiff in &t.frame_diffs {
                self.write_bits((1 << 4) | fdiff.wrapping_sub(1) as u64, 1 + 4)?;
            }
            // no more fdiffs for this template
            self.write_bits(0, 1)?;
        }

        Ok(())
    }

    fn write_template_chains(&mut self) -> Result<(), Error> {
        let structure = self.structure();
        self.write_non_symmetric(
            structure.num_chains as u32,
            structure.num_decode_targets.wrapping_add(1) as u32,
        )?;

        if structure.num_chains == 0 {
            return Ok(());
        }

        for &protected_by in &structure.decode_target_protected_by_chain {
            self.write_non_symmetric(protected_by as u32, structure.num_chains as u32)?;
        }

        for t in &structure.templates {
            for &chain_diff in &t.chain_diffs {
                self.write_bits(chain_diff as u64, 4)?;
            }
        }
        Ok(())
    }

    fn write_non_symmetric(&mut self, value: u32, num_values: u32) -> Result<(), Error> {
        self.writer.write_non_symmetric(value, num_values)
    }

    fn write_resolutions(&mut self) -> Result<(), Error> {
        for res in &self.structure().resolutions {
            self.write_bits((res.width as u64).wrapping_sub(1), 16)?;
            self.write_bits((res.height as u64).wrapping_sub(1), 16)?;
        }
        Ok(())
    }

    fn write_frame_dependency_definition(&mut self) -> Result<(), Error> {
        if self.best_template.need_custom_dtis {
            self.write_frame_dtis()?;
        }

        if self.best_template.need_custom_fdiffs {
            self.write_frame_fdiffs()?;
        }

        if self.best_template.need_custom_chains {
            self.write_frame_chains()?;
        }

        Ok(())
    }

    fn write_frame_dtis(&mut self) -> Result<(), Error> {
        for &dti in &self.frame_dependencies().decode_target_indications {
            self.write_bits(dti as u64, 2)?;
        }
        Ok(())
    }

    fn write_frame_fdiffs(&mut self) -> Result<(), Error> {
        for &fdiff in &self.frame_dependencies().frame_diffs {
            if fdiff <= (1 << 4) {
                self.write_bits((1 << 4) | fdiff.wrapping_sub(1) as u64, 2 + 4)?;
            } else if fdiff <= (1 << 8) {
                self.write_bits((2 << 8) | fdiff.wrapping_sub(1) as u64, 2 + 8)?;
            } else {
                // fdiff <= (1<<12)
                self.write_bits((3 << 12) | fdiff.wrapping_sub(1) as u64, 2 + 12)?;
            }
        }
        // no more fdiffs
        self.write_bits(0, 2)
    }

    fn write_frame_chains(&mut self) -> Result<(), Error> {
        for i in 0..self.structure().num_chains {
            let mut chain_diff = 0;
            if self.active_chains & 1u32.go_shl(i as i64) != 0 {
                // As in the original, a panic if the descriptor has fewer chain diffs than
                // the structure has chains.
                chain_diff = self.frame_dependencies().chain_diffs[i];
            }
            self.write_bits(chain_diff as u64, 8)?;
        }
        Ok(())
    }
}

const MANDATORY_FIELD_SIZE: i64 = 1 + 1 + 6 + 16;

impl<'a> DependencyDescriptorWriter<'a> {
    pub fn value_size_bits(&self) -> i64 {
        let mut value_size_bits =
            MANDATORY_FIELD_SIZE.wrapping_add(self.best_template.extra_size_bits);
        if self.has_extended_fields() {
            value_size_bits = value_size_bits.wrapping_add(5);
            if self.descriptor.attached_structure.is_some() {
                value_size_bits = value_size_bits.wrapping_add(self.structure_size_bits());
            }
            if self.should_write_active_decode_targets_bitmask() {
                value_size_bits =
                    value_size_bits.wrapping_add(self.structure().num_decode_targets as i64);
            }
        }

        value_size_bits
    }

    fn should_write_active_decode_targets_bitmask(&self) -> bool {
        let Some(active_decode_targets_bitmask) = self.descriptor.active_decode_targets_bitmask
        else {
            return false;
        };

        let all_decode_targets_bitmask = 1u64
            .go_shl(self.structure().num_decode_targets as i64)
            .wrapping_sub(1);
        if self.descriptor.attached_structure.is_some()
            && active_decode_targets_bitmask as u64 == all_decode_targets_bitmask
        {
            return false;
        }

        true
    }

    fn structure_size_bits(&self) -> i64 {
        let structure = self.structure();
        let templates = structure.templates.len() as i64;
        let num_decode_targets = structure.num_decode_targets as i64;
        let num_chains = structure.num_chains as i64;
        // template_id offset (6 bits) and number of decode targets (5 bits)
        let mut bits: i64 = 11;
        // template layers
        bits += 2 * templates;
        // dtis
        bits = bits.wrapping_add((2 * templates).wrapping_mul(num_decode_targets));
        // fdiffs. each templates uses 1 + 5 * sizeof(fdiff) bits.
        bits = bits.wrapping_add(templates);
        for t in &structure.templates {
            bits = bits.wrapping_add(5 * t.frame_diffs.len() as i64);
        }
        bits = bits.wrapping_add(size_non_symmetric_bits(
            structure.num_chains as u32,
            structure.num_decode_targets.wrapping_add(1) as u32,
        ));
        if structure.num_chains > 0 {
            for &protected_by in &structure.decode_target_protected_by_chain {
                bits = bits.wrapping_add(size_non_symmetric_bits(
                    protected_by as u32,
                    structure.num_chains as u32,
                ));
            }
            bits = bits.wrapping_add((4 * templates).wrapping_mul(num_chains));
        }

        // resolutions
        bits = bits.wrapping_add(1 + 32 * structure.resolutions.len() as i64);

        bits
    }

    // Not in the original, which dereferences its pointers.

    // `w.structure` where the original dereferences it.
    fn structure(&self) -> &'a FrameDependencyStructure {
        self.structure
            .expect("DependencyDescriptorWriter: structure is nil")
    }

    // `w.descriptor.FrameDependencies` where the original dereferences them.
    fn frame_dependencies(&self) -> &'a FrameDependencyTemplate {
        self.descriptor
            .frame_dependencies
            .as_deref()
            .expect("DependencyDescriptorWriter: FrameDependencies is nil")
    }
}
