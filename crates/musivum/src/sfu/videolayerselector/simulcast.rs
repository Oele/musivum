// Ported from LiveKit, pkg/sfu/videolayerselector/simulcast.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The embedded `*Base` is the field `base`.
// - `new_from_other` takes the selector it replaces and its `Base` with it (the original's
//   two selectors share the `*Base`). Where the original gives nil, for a selector of a type
//   it does not know, it gives the selector back as the error.
// - In `select`, `s` is the selector's `Base`, where the fields are that the original reaches
//   through the selector. The closure `populateSwitches` is given it and the result, which it
//   cannot hold on to while `select` changes them.

use std::any::Any;

use super::{Base, DependencyDescriptor, Null, VideoLayerSelector, VideoLayerSelectorResult, Vp9};
use crate::debugw;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;

pub struct Simulcast {
    pub(super) base: Base,
}

impl Simulcast {
    pub fn new(logger: Logger) -> Self {
        Self {
            base: Base::new(logger),
        }
    }

    pub fn new_from_other(
        vls: Box<dyn VideoLayerSelector>,
    ) -> Result<Self, Box<dyn VideoLayerSelector>> {
        let any = vls.as_any();
        if any.is::<Null>()
            || any.is::<Simulcast>()
            || any.is::<DependencyDescriptor>()
            || any.is::<Vp9>()
        {
            Ok(Self {
                base: vls.into_base(),
            })
        } else {
            Err(vls)
        }
    }
}

impl VideoLayerSelector for Simulcast {
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
        true
    }

    fn select(&mut self, ext_pkt: &buffer::ExtPacket, layer: i32) -> VideoLayerSelectorResult {
        let mut result = VideoLayerSelectorResult::default();
        let populate_switches =
            |s: &Base, result: &mut VideoLayerSelectorResult, is_active: bool, reason: &str| {
                result.is_switching = true;

                if !is_active {
                    result.is_resuming = true;
                }

                if !reason.is_empty() {
                    debugw!(
                        s.logger,
                        reason,
                        "previous" => s.previous_layer,
                        "current" => s.current_layer,
                        "previousTarget" => s.previous_target_layer,
                        "target" => s.target_layer,
                        "max" => s.max_layer,
                        "layer" => layer,
                        "req" => s.request_spatial,
                        "maxSeen" => s.max_seen_layer,
                        "feed" => ext_pkt.packet.ssrc,
                    );
                }
            };

        let s = &mut self.base;
        if s.current_layer.spatial != s.target_layer.spatial {
            let mut current_layer = s.current_layer;

            // Two things to check when not locked to target
            //   1. Opportunistic layer upgrade - needs a key frame
            //   2. Need to downgrade - needs a key frame
            let is_active = s.current_layer.is_valid();
            let mut found = false;
            let mut reason = "";
            if ext_pkt.is_key_frame {
                if s.enable_start_at_desired_quality && !is_active {
                    // Initial acquisition: latch directly onto the target layer instead of
                    // opportunistically latching onto the first key frame of any lower layer that
                    // happens to arrive first. This avoids a visible low-quality -> high-quality ramp
                    // (e.g. briefly decoding layer 0 before settling on a requested layer 2) for a
                    // subscriber that requested the higher layer.
                    //
                    // The target is chosen by the allocator: during the initial-acquisition grace it
                    // points at the requested layer (so we wait for it); if that layer never shows
                    // up the grace expires and the allocator drops the target to the highest layer
                    // actually seen, so we always end up latching onto a layer that is flowing.
                    if layer == s.target_layer.spatial {
                        reason = "acquiring target layer";
                        found = true;
                    }
                } else {
                    // default: opportunistically latch on to / step towards the target layer
                    if layer > s.current_layer.spatial && layer <= s.target_layer.spatial {
                        reason = "upgrading layer";
                        found = true;
                    }

                    if layer < s.current_layer.spatial && layer >= s.target_layer.spatial {
                        reason = "downgrading layer";
                        found = true;
                    }
                }

                if found {
                    current_layer.spatial = layer;
                    current_layer.temporal = ext_pkt.video_layer.temporal;
                }
            }

            if found {
                s.previous_layer = s.current_layer;
                s.current_layer = current_layer;

                s.previous_target_layer = s.target_layer;
                if s.current_layer.spatial >= s.max_layer.spatial
                    || s.current_layer.spatial == s.max_seen_layer.spatial
                {
                    s.target_layer.spatial = s.current_layer.spatial;
                }

                populate_switches(s, &mut result, is_active, reason);
            }
        }

        // if locked to higher than max layer due to overshoot, check if it can be dialed back
        if s.current_layer.spatial > s.max_layer.spatial
            && layer <= s.max_layer.spatial
            && ext_pkt.is_key_frame
        {
            s.previous_layer = s.current_layer;
            s.current_layer.spatial = layer;

            s.previous_target_layer = s.target_layer;
            if s.current_layer.spatial >= s.max_layer.spatial
                || s.current_layer.spatial == s.max_seen_layer.spatial
            {
                s.target_layer.spatial = layer;
            }

            populate_switches(s, &mut result, true, "adjusting overshoot");
        }

        result.rtp_marker = ext_pkt.packet.marker;
        result.is_selected = layer == s.current_layer.spatial;
        result.is_relevant = false;
        result
    }
}

#[cfg(test)]
#[path = "simulcast_test.rs"]
mod simulcast_test;
