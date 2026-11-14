// Ported from LiveKit, pkg/sfu/videolayerselector/vp9.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The embedded `*Base` is the field `base`.
// - `new_from_other` takes the selector it replaces and its `Base` with it (the original's
//   two selectors share the `*Base`).
// - In `select`, `v` is the selector's `Base`, where the fields are that the original reaches
//   through the selector.

use std::any::Any;

use super::{Base, VideoLayerSelector, VideoLayerSelectorResult};
use crate::protocol::logger::Logger;
use crate::sfu::buffer;

pub struct Vp9 {
    pub(super) base: Base,
}

impl Vp9 {
    pub fn new(logger: Logger) -> Self {
        Self {
            base: Base::new(logger),
        }
    }

    pub fn new_from_other(vls: Box<dyn VideoLayerSelector>) -> Self {
        Self {
            base: vls.into_base(),
        }
    }
}

impl VideoLayerSelector for Vp9 {
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
        let Some(vp9) = &ext_pkt.vp9 else {
            return result;
        };

        let v = &mut self.base;
        let mut current_layer = v.current_layer;
        if v.current_layer != v.target_layer {
            let mut updated_layer = v.current_layer;

            if !v.current_layer.is_valid() {
                if !ext_pkt.is_key_frame {
                    return result;
                }

                updated_layer = ext_pkt.video_layer;
            } else {
                if v.current_layer.temporal != v.target_layer.temporal {
                    if v.current_layer.temporal < v.target_layer.temporal {
                        // temporal scale up
                        if ext_pkt.video_layer.temporal > v.current_layer.temporal
                            && ext_pkt.video_layer.temporal <= v.target_layer.temporal
                            && vp9.u
                            && vp9.b
                        {
                            current_layer.temporal = ext_pkt.video_layer.temporal;
                            updated_layer.temporal = ext_pkt.video_layer.temporal;
                        }
                    } else {
                        // temporal scale down
                        if vp9.e {
                            updated_layer.temporal = v.target_layer.temporal;
                        }
                    }
                }

                if v.current_layer.spatial != v.target_layer.spatial {
                    if v.current_layer.spatial < v.target_layer.spatial {
                        // spatial scale up
                        if ext_pkt.video_layer.spatial > v.current_layer.spatial
                            && ext_pkt.video_layer.spatial <= v.target_layer.spatial
                            && !vp9.p
                            && vp9.b
                        {
                            current_layer.spatial = ext_pkt.video_layer.spatial;
                            updated_layer.spatial = ext_pkt.video_layer.spatial;
                        }
                    } else {
                        // spatial scale down
                        if vp9.e {
                            updated_layer.spatial = v.target_layer.spatial;
                        }
                    }
                }
            }

            if updated_layer != v.current_layer {
                result.is_switching = true;
                if !v.current_layer.is_valid() && updated_layer.is_valid() {
                    result.is_resuming = true;
                }

                v.previous_layer = v.current_layer;
                v.current_layer = updated_layer;
            }
        }

        result.rtp_marker = ext_pkt.packet.marker;
        if vp9.e
            && ext_pkt.video_layer.spatial == current_layer.spatial
            && (vp9.p || v.target_layer.spatial <= v.current_layer.spatial)
        {
            result.rtp_marker = true;
        }
        result.is_selected = !ext_pkt.video_layer.greater_than(current_layer);
        result.is_relevant = true;
        result
    }
}
