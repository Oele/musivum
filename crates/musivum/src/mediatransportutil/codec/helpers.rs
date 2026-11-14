// Ported from LiveKit's mediatransportutil, pkg/codec/helpers.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

// VideoSize represents video resolution
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct VideoSize {
    pub width: u32,
    pub height: u32,
}

pub fn vpx_picture_id_size_diff(m_bit1: bool, m_bit2: bool) -> i64 {
    if m_bit1 == m_bit2 {
        return 0;
    }

    if m_bit1 {
        return 1;
    }

    -1
}

#[cfg(test)]
#[path = "helpers_test.rs"]
mod helpers_test;
