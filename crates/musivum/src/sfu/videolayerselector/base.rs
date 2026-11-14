// Ported from LiveKit, pkg/sfu/videolayerselector/base.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `Base` is held by the selectors as a field, not
// through a pointer they share: `get_base` is on the trait, see videolayerselector.rs. The
// temporal layer selector is an `Option<Box<..>>`. `check_sync` gives `(locked, layer)`.

use super::VideoLayerSelectorResult;
use super::temporallayerselector::TemporalLayerSelector;
use crate::debugw;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;

pub struct Base {
    pub(super) logger: Logger,

    pub(super) tls: Option<Box<dyn TemporalLayerSelector>>,

    pub(super) max_layer: buffer::VideoLayer,
    pub(super) max_seen_layer: buffer::VideoLayer,

    pub(super) target_layer: buffer::VideoLayer,
    pub(super) previous_target_layer: buffer::VideoLayer,

    pub(super) request_spatial: i32,

    pub(super) current_layer: buffer::VideoLayer,
    pub(super) previous_layer: buffer::VideoLayer,

    // when set, on initial acquisition latch directly onto the target (requested) layer instead of
    // opportunistically latching onto the first lower-layer key frame that arrives (see Simulcast.Select)
    pub(super) enable_start_at_desired_quality: bool,
}

impl Base {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            tls: None,
            max_layer: buffer::INVALID_LAYER,
            max_seen_layer: buffer::INVALID_LAYER,
            target_layer: buffer::INVALID_LAYER, // start off with nothing, let streamallocator/opportunistic forwarder set the target
            previous_target_layer: buffer::INVALID_LAYER,
            request_spatial: buffer::INVALID_LAYER_SPATIAL,
            current_layer: buffer::INVALID_LAYER,
            previous_layer: buffer::INVALID_LAYER,
            enable_start_at_desired_quality: false,
        }
    }

    pub fn get_logger(&self) -> Logger {
        self.logger.clone()
    }

    pub fn is_overshoot_okay(&self) -> bool {
        false
    }

    pub fn set_temporal_layer_selector(&mut self, tls: Option<Box<dyn TemporalLayerSelector>>) {
        self.tls = tls;
    }

    pub fn set_enable_start_at_desired_quality(&mut self, enable: bool) {
        self.enable_start_at_desired_quality = enable;
    }

    pub fn set_max(&mut self, max_layer: buffer::VideoLayer) {
        self.max_layer = max_layer;
    }

    pub fn set_max_spatial(&mut self, layer: i32) {
        self.max_layer.spatial = layer;
    }

    pub fn set_max_temporal(&mut self, layer: i32) {
        self.max_layer.temporal = layer;
    }

    pub fn get_max(&self) -> buffer::VideoLayer {
        self.max_layer
    }

    pub fn set_target(&mut self, target_layer: buffer::VideoLayer) {
        self.previous_target_layer = target_layer;
        self.target_layer = target_layer;
    }

    pub fn get_target(&self) -> buffer::VideoLayer {
        self.target_layer
    }

    pub fn set_request_spatial(&mut self, layer: i32) {
        self.request_spatial = layer;
    }

    pub fn get_request_spatial(&self) -> i32 {
        self.request_spatial
    }

    pub fn check_sync(&self) -> (bool, i32) {
        let layer = self.get_request_spatial();
        let locked = layer == self.get_current().spatial;
        (locked, layer)
    }

    pub fn set_max_seen(&mut self, max_seen_layer: buffer::VideoLayer) {
        self.max_seen_layer = max_seen_layer;
    }

    pub fn set_max_seen_spatial(&mut self, layer: i32) {
        self.max_seen_layer.spatial = layer;
    }

    pub fn set_max_seen_temporal(&mut self, layer: i32) {
        self.max_seen_layer.temporal = layer;
    }

    pub fn get_max_seen(&self) -> buffer::VideoLayer {
        self.max_seen_layer
    }

    pub fn set_current(&mut self, current_layer: buffer::VideoLayer) {
        self.current_layer = current_layer;
    }

    pub fn get_current(&self) -> buffer::VideoLayer {
        self.current_layer
    }

    pub fn select(
        &mut self,
        _ext_pkt: &buffer::ExtPacket,
        _layer: i32,
    ) -> VideoLayerSelectorResult {
        VideoLayerSelectorResult::default()
    }

    pub fn rollback(&mut self) {
        debugw!(
            self.logger,
            "rolling back",
            "previous" => self.previous_layer,
            "current" => self.current_layer,
            "previousTarget" => self.previous_target_layer,
            "target" => self.target_layer,
            "max" => self.max_layer,
            "req" => self.request_spatial,
            "maxSeen" => self.max_seen_layer,
        );
        self.current_layer = self.previous_layer;
        self.target_layer = self.previous_target_layer;
    }

    pub fn select_temporal(&mut self, ext_pkt: &buffer::ExtPacket) -> i32 {
        if let Some(tls) = &mut self.tls {
            let (this, next) = tls.select(
                ext_pkt,
                self.current_layer.temporal,
                self.target_layer.temporal,
            );
            if next != self.current_layer.temporal {
                let previous_layer = self.current_layer;
                self.current_layer.temporal = next;

                debugw!(
                    self.logger,
                    "updating temporal layer",
                    "previous" => previous_layer,
                    "current" => self.current_layer,
                    "target" => self.target_layer,
                    "max" => self.max_layer,
                    "req" => self.request_spatial,
                    "maxSeen" => self.max_seen_layer,
                );
            }
            return this;
        }

        self.current_layer.temporal
    }
}
