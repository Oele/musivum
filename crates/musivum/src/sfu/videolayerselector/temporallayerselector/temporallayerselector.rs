// Ported from LiveKit, pkg/sfu/videolayerselector/temporallayerselector/temporallayerselector.go
// at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the interface is a trait, held as
// `Box<dyn TemporalLayerSelector>`. `select` takes `&mut self` (a selector has no lock, its
// owner guards it) and a packet that is never nil, and gives `(this, next)`.

use crate::sfu::buffer;

pub trait TemporalLayerSelector: Send + Sync {
    // returns (this, next)
    fn select(&mut self, ext_pkt: &buffer::ExtPacket, current: i32, target: i32) -> (i32, i32);
}
