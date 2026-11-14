// Ported from LiveKit, pkg/sfu/videolayerselector/decodetarget.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the embedded `buffer.DependencyDescriptorDecodeTarget`
// is the field `dependency_descriptor_decode_target`; the chain, a pointer that may be nil,
// is an `Option<Arc<FrameChain>>`; `on_frame` gives the result or the error, not both.

use std::sync::Arc;

use super::{Error, FrameChain, shl};
use crate::sfu::buffer;
use crate::sfu::rtpextension::dependencydescriptor as dd;

pub struct DecodeTarget {
    pub dependency_descriptor_decode_target: buffer::DependencyDescriptorDecodeTarget,
    chain: Option<Arc<FrameChain>>,
    active: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FrameDetectionResult {
    pub target_valid: bool,
    pub dti: dd::DecodeTargetIndication,
}

impl DecodeTarget {
    pub fn new(
        target: buffer::DependencyDescriptorDecodeTarget,
        chain: Option<Arc<FrameChain>>,
    ) -> Self {
        Self {
            dependency_descriptor_decode_target: target,
            chain,
            active: false,
        }
    }

    pub fn valid(&self) -> bool {
        match &self.chain {
            None => true,
            Some(chain) => !chain.broken(),
        }
    }

    pub fn active(&self) -> bool {
        self.active
    }

    pub fn update_active(&mut self, active_bitmask: u32) {
        let active =
            (active_bitmask & shl(1, self.dependency_descriptor_decode_target.target)) != 0;
        self.active = active;
        if let Some(chain) = &self.chain {
            chain.update_active(active);
        }
    }

    pub fn on_frame(
        &self,
        _ext_frame_num: u64,
        fd: &dd::FrameDependencyTemplate,
    ) -> Result<FrameDetectionResult, Error> {
        let mut result = FrameDetectionResult::default();
        let target = self.dependency_descriptor_decode_target.target;
        if fd.decode_target_indications.len() as i64 <= target {
            return Err(Error::MismatchTarget {
                target,
                len: fd.decode_target_indications.len(),
            });
        }

        result.dti = fd.decode_target_indications[target as usize];
        // The encoder can choose not to use frame chain in theory, and we need to trace every required frame is decodable in this case.
        // But we don't observe this in browser and it makes no sense to not use the chain with svc, so only use chain to detect decode target broken now,
        // and always return decodable if it is not protect by chain.
        result.target_valid = self.valid();
        Ok(result)
    }
}
