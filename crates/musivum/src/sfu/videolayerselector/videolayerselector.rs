// Ported from LiveKit, pkg/sfu/videolayerselector/videolayerselector.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The interface is a trait, held as `Box<dyn VideoLayerSelector>`. The original's
//   implementations embed `*Base`, whose methods are theirs unless they have their own: here
//   an implementation holds a `Base` and gives it out through `get_base` and `get_base_mut`,
//   and the trait's methods forward to `Base`'s by default. An implementation writes out only
//   what it has of its own in the original.
// - A selector has no lock, its owner guards it: what changes it takes `&mut self`, the
//   getters and `check_sync` take `&self`.
// - `getBase()` gives the original's `New...FromOther` constructors the `*Base` of the
//   selector they replace, which the two then share. Here the new selector takes the `Base`
//   over: `into_base` gives it up with the selector. `as_any` is for what the original does
//   with a type switch or a type assertion on the interface.
// - `DDBytesSpill` is a `Vec`, which is never nil: empty where the original's is nil. A
//   marshaled descriptor has three bytes at least.

use std::any::Any;

use super::Base;
use super::temporallayerselector::TemporalLayerSelector;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::rtpextension::dependencydescriptor as dd;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VideoLayerSelectorResult {
    pub is_selected: bool,
    pub is_relevant: bool,
    pub is_switching: bool,
    pub is_resuming: bool,
    pub rtp_marker: bool,

    // marshaled dependency descriptor extension, held inline when it fits and
    // spilled to the heap when it does not, the way the sequencer keeps its copy
    pub dd_bytes: [u8; dd::MAX_INLINE_EXTENSION_SIZE],
    pub dd_bytes_len: usize,
    pub dd_bytes_spill: Vec<u8>,
}

impl VideoLayerSelectorResult {
    // marshalDependencyDescriptorExtension marshals ddExtension into the result,
    // inline if it fits and on the heap otherwise.
    pub(super) fn marshal_dependency_descriptor_extension(
        &mut self,
        dd_extension: &dd::DependencyDescriptorExtension,
    ) -> Result<(), dd::Error> {
        match dd_extension.marshal_to(&mut self.dd_bytes[..]) {
            Ok(dd_bytes_len) => {
                self.dd_bytes_len = dd_bytes_len;
                return Ok(());
            }
            Err(dd::Error::BufferTooSmall) => {}
            Err(err) => return Err(err),
        }

        // descriptors carrying the full dependency structure do not fit inline
        self.dd_bytes_spill = dd_extension.marshal()?;
        Ok(())
    }
}

pub trait VideoLayerSelector: Send + Sync {
    fn get_base(&self) -> &Base;
    // Not in the original, whose `getBase()` gives a pointer.
    fn get_base_mut(&mut self) -> &mut Base;
    // Not in the original: see at the top.
    fn into_base(self: Box<Self>) -> Base;
    // Not in the original: see at the top.
    fn as_any(&self) -> &dyn Any;

    fn get_logger(&self) -> Logger {
        self.get_base().get_logger()
    }

    fn is_overshoot_okay(&self) -> bool {
        self.get_base().is_overshoot_okay()
    }

    fn set_temporal_layer_selector(&mut self, tls: Option<Box<dyn TemporalLayerSelector>>) {
        self.get_base_mut().set_temporal_layer_selector(tls)
    }
    fn set_enable_start_at_desired_quality(&mut self, enable: bool) {
        self.get_base_mut()
            .set_enable_start_at_desired_quality(enable)
    }

    fn set_max(&mut self, max_layer: buffer::VideoLayer) {
        self.get_base_mut().set_max(max_layer)
    }
    fn set_max_spatial(&mut self, layer: i32) {
        self.get_base_mut().set_max_spatial(layer)
    }
    fn set_max_temporal(&mut self, layer: i32) {
        self.get_base_mut().set_max_temporal(layer)
    }
    fn get_max(&self) -> buffer::VideoLayer {
        self.get_base().get_max()
    }

    fn set_target(&mut self, target_layer: buffer::VideoLayer) {
        self.get_base_mut().set_target(target_layer)
    }
    fn get_target(&self) -> buffer::VideoLayer {
        self.get_base().get_target()
    }

    fn set_request_spatial(&mut self, layer: i32) {
        self.get_base_mut().set_request_spatial(layer)
    }
    fn get_request_spatial(&self) -> i32 {
        self.get_base().get_request_spatial()
    }

    // returns (locked, layer)
    fn check_sync(&self) -> (bool, i32) {
        self.get_base().check_sync()
    }

    fn set_max_seen(&mut self, max_seen_layer: buffer::VideoLayer) {
        self.get_base_mut().set_max_seen(max_seen_layer)
    }
    fn set_max_seen_spatial(&mut self, layer: i32) {
        self.get_base_mut().set_max_seen_spatial(layer)
    }
    fn set_max_seen_temporal(&mut self, layer: i32) {
        self.get_base_mut().set_max_seen_temporal(layer)
    }
    fn get_max_seen(&self) -> buffer::VideoLayer {
        self.get_base().get_max_seen()
    }

    fn set_current(&mut self, current_layer: buffer::VideoLayer) {
        self.get_base_mut().set_current(current_layer)
    }
    fn get_current(&self) -> buffer::VideoLayer {
        self.get_base().get_current()
    }

    fn select(&mut self, ext_pkt: &buffer::ExtPacket, layer: i32) -> VideoLayerSelectorResult {
        self.get_base_mut().select(ext_pkt, layer)
    }
    fn select_temporal(&mut self, ext_pkt: &buffer::ExtPacket) -> i32 {
        self.get_base_mut().select_temporal(ext_pkt)
    }
    fn rollback(&mut self) {
        self.get_base_mut().rollback()
    }
}
