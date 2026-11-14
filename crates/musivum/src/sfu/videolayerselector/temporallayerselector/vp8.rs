// Ported from LiveKit, pkg/sfu/videolayerselector/temporallayerselector/vp8.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `extPkt.Temporal` of the embedded video layer is
// `ext_pkt.video_layer.temporal`.

use super::TemporalLayerSelector;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;

pub struct Vp8 {
    // not used by the original either
    #[allow(dead_code)]
    logger: Logger,
}

impl Vp8 {
    pub fn new(logger: Logger) -> Self {
        Self { logger }
    }
}

impl TemporalLayerSelector for Vp8 {
    fn select(&mut self, ext_pkt: &buffer::ExtPacket, current: i32, target: i32) -> (i32, i32) {
        let mut this = current;
        let mut next = current;
        if current == target {
            return (this, next);
        }

        let Some(vp8) = &ext_pkt.vp8 else {
            return (this, next);
        };

        let tid = ext_pkt.video_layer.temporal;
        if current < target {
            // Up-switch only where nothing dropped can be referenced: a layer sync
            // (Y) frame depends only on TL0 (RFC 7741), a key frame refreshes all
            // reference buffers. Key frames also cover encoders that never set Y.
            // A sync frame raises one layer at a time, the layers in between would
            // otherwise forward frames that reference their dropped frames.
            if vp8.is_key_frame && tid <= target {
                this = target;
                next = target;
            } else if tid == current.wrapping_add(1) && tid <= target && vp8.s && vp8.y {
                this = tid;
                next = tid;
            }
        } else if ext_pkt.packet.marker {
            next = target;
        }
        (this, next)
    }
}
