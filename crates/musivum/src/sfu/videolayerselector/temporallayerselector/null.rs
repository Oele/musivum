// Ported from LiveKit, pkg/sfu/videolayerselector/temporallayerselector/null.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

use super::TemporalLayerSelector;
use crate::sfu::buffer;

// Null is for a stream whose temporal layers cannot be filtered, simulcast without the
// dependency descriptor for example: every temporal layer is forwarded. It reports the
// target as the current layer, so a target below the highest layer does not stay pending
// and block further allocation. The forwarder pairs it with the null codec munger, which
// ignores the temporal layer it returns for the packet.
#[derive(Clone, Copy, Debug, Default)]
pub struct Null {}

impl Null {
    pub fn new() -> Self {
        Self {}
    }
}

impl TemporalLayerSelector for Null {
    fn select(&mut self, _ext_pkt: &buffer::ExtPacket, _current: i32, target: i32) -> (i32, i32) {
        (target, target)
    }
}

#[cfg(test)]
#[path = "null_test.rs"]
mod null_test;
