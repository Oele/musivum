// Ported from LiveKit, pkg/sfu/receiver_base.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the types `Bitrates` and `ReceiverCodecState` are
// here so far, so that what reports bitrates per layer and the interfaces of the package can
// be ported before the receiver; the receiver itself follows.

use crate::sfu::buffer;

// --------------------------------------

pub type Bitrates = [[i64; (buffer::DEFAULT_MAX_LAYER_TEMPORAL + 1) as usize];
    (buffer::DEFAULT_MAX_LAYER_SPATIAL + 1) as usize];

// --------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ReceiverCodecState {
    #[default]
    Normal,
    Suspended,
    Invalid,
}
