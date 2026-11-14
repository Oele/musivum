// Ported from LiveKit, pkg/sfu/rtpextension/dependencydescriptor/dependencydescriptorreader.go
// at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The `ErrDDReader...` values are variants of the package's `Error`, which is in
//   dependencydescriptorextension.rs.
// - The reader borrows the buffer and the descriptor it fills, and holds the structure as an
//   `Arc`.
// - The structure and the templates that are read are behind `Arc`s which nothing else holds
//   yet; `attached_structure` and `frame_dependencies` at the end of the file give the
//   mutable access the original has through its pointers.
// - Frame dependencies of the caller are written into only when no one else holds them: an
//   `Arc` that is shared is replaced by a new one, as one that is a template of the structure
//   is. A resolution is a value of the descriptor, so there is nothing to allocate for it.

use std::sync::Arc;

use super::{
    BitStreamReader, DecodeTargetIndication, DependencyDescriptor, Error, FrameDependencyStructure,
    FrameDependencyTemplate, GoShift, MAX_SPATIAL_IDS, MAX_TEMPLATES, MAX_TEMPORAL_IDS,
    RenderResolution,
};

pub struct DependencyDescriptorReader<'a> {
    // Output.
    descriptor: &'a mut DependencyDescriptor,

    // Values that are needed while reading the descriptor, but can be discarded
    // when reading is complete.
    buffer: BitStreamReader<'a>,
    frame_dependency_template_id: i64,
    active_decode_targets_present_flag: bool,
    custom_dtis_flag: bool,
    custom_fdiffs_flag: bool,
    custom_chains_flag: bool,
    structure: Option<Arc<FrameDependencyStructure>>,
}

impl<'a> DependencyDescriptorReader<'a> {
    pub fn new(
        buf: &'a [u8],
        structure: Option<Arc<FrameDependencyStructure>>,
        descriptor: &'a mut DependencyDescriptor,
    ) -> Self {
        let buffer = BitStreamReader::new(buf);
        Self {
            buffer,
            descriptor,
            structure,
            frame_dependency_template_id: 0,
            active_decode_targets_present_flag: false,
            custom_dtis_flag: false,
            custom_fdiffs_flag: false,
            custom_chains_flag: false,
        }
    }

    pub fn parse(&mut self) -> Result<usize, Error> {
        self.read_mandatory_fields()?;
        if self.buffer.buf.len() > 3 {
            self.read_extended_fields()?;
        }

        if self.descriptor.attached_structure.is_some() {
            self.structure = self.descriptor.attached_structure.clone();
        }

        let Some(structure) = &self.structure else {
            self.buffer.invalidate();
            return Err(Error::DdReaderNoStructure);
        };

        if self.active_decode_targets_present_flag {
            let bitmask = self.buffer.read_bits(structure.num_decode_targets as i64)?;
            let mask = bitmask as u32;
            self.descriptor.active_decode_targets_bitmask = Some(mask);
        }

        self.read_frame_dependency_definition()?;
        Ok(self.buffer.bytes_read())
    }

    fn read_mandatory_fields(&mut self) -> Result<(), Error> {
        // The original assigns the two flags before it looks at the error: false if the read
        // has failed.
        let flag = self.buffer.read_bool();
        self.descriptor.first_packet_in_frame = matches!(flag, Ok(true));
        flag?;

        let flag = self.buffer.read_bool();
        self.descriptor.last_packet_in_frame = matches!(flag, Ok(true));
        flag?;

        let template_id = self.buffer.read_bits(6)?;
        self.frame_dependency_template_id = template_id as i64;

        let frame_number = self.buffer.read_bits(16)?;
        self.descriptor.frame_number = frame_number as u16;
        Ok(())
    }

    fn read_extended_fields(&mut self) -> Result<(), Error> {
        let template_dependency_structure_present_flag = self.buffer.read_bool()?;

        let flag = self.buffer.read_bool()?;
        self.active_decode_targets_present_flag = flag;

        let flag = self.buffer.read_bool()?;
        self.custom_dtis_flag = flag;

        let flag = self.buffer.read_bool()?;
        self.custom_fdiffs_flag = flag;

        let flag = self.buffer.read_bool()?;
        self.custom_chains_flag = flag;

        if template_dependency_structure_present_flag {
            self.read_template_dependency_structure()?;
            let Some(attached_structure) = &self.descriptor.attached_structure else {
                return Err(Error::DdReaderTemplateWithoutStructure);
            };
            let bitmask = 1u64
                .go_shl(attached_structure.num_decode_targets as i64)
                .wrapping_sub(1) as u32;
            self.descriptor.active_decode_targets_bitmask = Some(bitmask);
        }
        Ok(())
    }

    fn read_template_dependency_structure(&mut self) -> Result<(), Error> {
        self.descriptor.attached_structure = Some(Arc::new(FrameDependencyStructure::default()));
        let structure_id = self.buffer.read_bits(6)?;
        attached_structure(self.descriptor).structure_id = structure_id as i64;

        let num_decode_targets = self.buffer.read_bits(5)?;
        attached_structure(self.descriptor).num_decode_targets = num_decode_targets as usize + 1;

        self.read_template_layers()?;
        self.read_template_dtis()?;
        self.read_template_fdiffs()?;
        self.read_template_chains()?;

        let flag = self.buffer.read_bool()?;
        if flag {
            return self.read_resolutions();
        }
        Ok(())
    }
}

// The names are the original's.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum NextLayerIdcType {
    SameLayer = 0,
    NextTemporalLayer = 1,
    NextSpatialLayer = 2,
    NoMoreLayer = 3,
    InvalidLayer = 4,
}

impl NextLayerIdcType {
    // The original's conversion `nextLayerIdcType(idc)` of the two bits that have been read.
    fn from_bits(idc: u64) -> Self {
        match idc & 3 {
            0 => NextLayerIdcType::SameLayer,
            1 => NextLayerIdcType::NextTemporalLayer,
            2 => NextLayerIdcType::NextSpatialLayer,
            _ => NextLayerIdcType::NoMoreLayer,
        }
    }
}

impl DependencyDescriptorReader<'_> {
    fn read_template_layers(&mut self) -> Result<(), Error> {
        let mut templates: Vec<Arc<FrameDependencyTemplate>> = Vec::new();
        let (mut temporal_id, mut spatial_id) = (0i64, 0i64);
        let mut next_layer_idc;
        loop {
            if templates.len() == MAX_TEMPLATES {
                return Err(Error::DdReaderTooManyTemplates);
            }

            templates.push(Arc::new(FrameDependencyTemplate {
                temporal_id,
                spatial_id,
                ..Default::default()
            }));

            let idc = self.buffer.read_bits(2)?;
            next_layer_idc = NextLayerIdcType::from_bits(idc);

            match next_layer_idc {
                NextLayerIdcType::NextTemporalLayer => {
                    temporal_id += 1;
                    if temporal_id >= MAX_TEMPORAL_IDS {
                        return Err(Error::DdReaderTooManyTemporalLayers);
                    }
                }
                NextLayerIdcType::NextSpatialLayer => {
                    spatial_id += 1;
                    temporal_id = 0;
                    if spatial_id >= MAX_SPATIAL_IDS {
                        return Err(Error::DdReaderTooManySpatialLayers);
                    }
                }
                _ => {}
            }

            if next_layer_idc == NextLayerIdcType::NoMoreLayer || !self.buffer.ok() {
                break;
            }
        }

        attached_structure(self.descriptor).templates = templates;
        Ok(())
    }

    fn read_template_dtis(&mut self) -> Result<(), Error> {
        let structure = attached_structure(self.descriptor);
        let num_decode_targets = structure.num_decode_targets;
        for template in structure.templates.iter_mut() {
            let template = Arc::make_mut(template);
            if template.decode_target_indications.len() < num_decode_targets {
                template
                    .decode_target_indications
                    .resize(num_decode_targets, DecodeTargetIndication::default());
            } else {
                template
                    .decode_target_indications
                    .truncate(num_decode_targets);
            }

            for dti in template.decode_target_indications.iter_mut() {
                let indication = self.buffer.read_bits(2)?;
                *dti = DecodeTargetIndication::from_bits(indication);
            }
        }
        Ok(())
    }

    fn read_template_fdiffs(&mut self) -> Result<(), Error> {
        for template in attached_structure(self.descriptor).templates.iter_mut() {
            let template = Arc::make_mut(template);
            loop {
                let fdiff_follow = self.buffer.read_bool()?;
                if !fdiff_follow {
                    break;
                }
                let f_diff_minus_one = self.buffer.read_bits(4)?;
                template
                    .frame_diffs
                    .push(f_diff_minus_one.wrapping_add(1) as i64);
            }
        }

        Ok(())
    }

    fn read_template_chains(&mut self) -> Result<(), Error> {
        let structure = attached_structure(self.descriptor);

        let num_chains = self
            .buffer
            .read_non_symmetric((structure.num_decode_targets as u32).wrapping_add(1))?;
        structure.num_chains = num_chains as usize;
        if structure.num_chains == 0 {
            return Ok(());
        }

        for _ in 0..structure.num_decode_targets {
            let protected_by_chain = self
                .buffer
                .read_non_symmetric(structure.num_chains as u32)?;
            structure
                .decode_target_protected_by_chain
                .push(protected_by_chain as usize);
        }

        for frame_template in structure.templates.iter_mut() {
            let frame_template = Arc::make_mut(frame_template);
            for _chain_id in 0..structure.num_chains {
                let chain_diff = self.buffer.read_bits(4)?;
                frame_template.chain_diffs.push(chain_diff as i64);
            }
        }

        Ok(())
    }

    fn read_resolutions(&mut self) -> Result<(), Error> {
        let structure = attached_structure(self.descriptor);
        // The way templates are bitpacked, they are always ordered by spatial_id.
        let spatial_layers = structure.templates[structure.templates.len() - 1]
            .spatial_id
            .wrapping_add(1);
        for _sid in 0..spatial_layers {
            let width_minus_1 = self.buffer.read_bits(16)?;
            let height_minus_1 = self.buffer.read_bits(16)?;
            structure.resolutions.push(RenderResolution {
                width: width_minus_1.wrapping_add(1) as i64,
                height: height_minus_1.wrapping_add(1) as i64,
            });
        }

        Ok(())
    }

    fn read_frame_dependency_definition(&mut self) -> Result<(), Error> {
        // `r.structure`: `parse` has made sure that there is one.
        let structure = self
            .structure
            .as_deref()
            .expect("DependencyDescriptorReader: Structure is nil");
        let template_index = self
            .frame_dependency_template_id
            .wrapping_add(MAX_TEMPLATES as i64)
            .wrapping_sub(structure.structure_id)
            % MAX_TEMPLATES as i64;

        if template_index >= structure.templates.len() as i64 {
            self.buffer.invalidate();
            return Err(Error::DdReaderInvalidTemplateIndex);
        }

        // Copy all the fields from the matching template into caller storage,
        // never into one of the structure's templates, custom fields would change the template
        //
        // In Rust neither into storage that the caller still shares with somebody else.
        let in_caller_storage = match self.descriptor.frame_dependencies.as_mut() {
            None => false,
            Some(dst) => {
                !structure.templates.iter().any(|t| Arc::ptr_eq(t, dst))
                    && Arc::get_mut(dst).is_some()
            }
        };
        if !in_caller_storage {
            self.descriptor.frame_dependencies = Some(Arc::new(FrameDependencyTemplate::default()));
        }
        // As in Go, a template index below zero (a structure id above 63 in a structure that
        // was not read from a packet) is a panic here.
        structure.templates[template_index as usize].copy_into(frame_dependencies(self.descriptor));

        if self.custom_dtis_flag {
            self.read_frame_dtis()?;
        }

        if self.custom_fdiffs_flag {
            self.read_frame_fdiffs()?;
        }

        if self.custom_chains_flag {
            self.read_frame_chains()?;
        }

        let structure = self
            .structure
            .as_deref()
            .expect("DependencyDescriptorReader: Structure is nil");
        if structure.resolutions.is_empty() {
            self.descriptor.resolution = None;
        } else {
            // Format guarantees that if there were resolutions in the last structure,
            // then each spatial layer got one.
            let spatial_id = frame_dependencies(self.descriptor).spatial_id;
            if spatial_id >= structure.resolutions.len() as i64 {
                self.buffer.invalidate();
                return Err(Error::DdReaderInvalidSpatialLayer);
            }
            self.descriptor.resolution = Some(structure.resolutions[spatial_id as usize]);
        }

        Ok(())
    }

    fn read_frame_dtis(&mut self) -> Result<(), Error> {
        let num_decode_targets = self
            .structure
            .as_deref()
            .expect("DependencyDescriptorReader: Structure is nil")
            .num_decode_targets;
        let frame_dependencies = frame_dependencies(self.descriptor);
        if frame_dependencies.decode_target_indications.len() != num_decode_targets {
            return Err(Error::DdReaderNumDtiMismatch);
        }

        for dti in frame_dependencies.decode_target_indications.iter_mut() {
            let indication = self.buffer.read_bits(2)?;
            *dti = DecodeTargetIndication::from_bits(indication);
        }
        Ok(())
    }

    fn read_frame_fdiffs(&mut self) -> Result<(), Error> {
        let frame_dependencies = frame_dependencies(self.descriptor);
        frame_dependencies.frame_diffs.clear();
        loop {
            let nex_fdiff_size = self.buffer.read_bits(2)?;
            if nex_fdiff_size == 0 {
                break;
            }
            let f_diff_minus_one = self.buffer.read_bits((nex_fdiff_size * 4) as i64)?;
            frame_dependencies
                .frame_diffs
                .push(f_diff_minus_one.wrapping_add(1) as i64);
        }

        Ok(())
    }

    fn read_frame_chains(&mut self) -> Result<(), Error> {
        let num_chains = self
            .structure
            .as_deref()
            .expect("DependencyDescriptorReader: Structure is nil")
            .num_chains;
        let frame_dependencies = frame_dependencies(self.descriptor);
        if frame_dependencies.chain_diffs.len() != num_chains {
            return Err(Error::DdReaderNumChainDiffsMismatch);
        }

        for chain_diff in frame_dependencies.chain_diffs.iter_mut() {
            *chain_diff = self.buffer.read_bits(8)? as i64;
        }
        Ok(())
    }
}

// Not in the original, which writes through its pointers.

// `r.descriptor.AttachedStructure` while it is read. The reader has made it and nothing else
// holds it yet, so this does not copy.
fn attached_structure(descriptor: &mut DependencyDescriptor) -> &mut FrameDependencyStructure {
    Arc::make_mut(
        descriptor
            .attached_structure
            .as_mut()
            .expect("DependencyDescriptorReader: AttachedStructure is nil"),
    )
}

// `r.descriptor.FrameDependencies` once `read_frame_dependency_definition` has set them, to
// storage that nothing else holds: this does not copy either.
fn frame_dependencies(descriptor: &mut DependencyDescriptor) -> &mut FrameDependencyTemplate {
    Arc::make_mut(
        descriptor
            .frame_dependencies
            .as_mut()
            .expect("DependencyDescriptorReader: FrameDependencies is nil"),
    )
}
