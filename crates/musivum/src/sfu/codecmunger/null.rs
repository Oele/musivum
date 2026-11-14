// Ported from LiveKit, pkg/sfu/codecmunger/null.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the methods of the interface are in the block that
// implements the trait, so `get_seeded_state` stands before them and not among them.

use std::any::Any;

use crate::protocol::logger::Logger;
use crate::sfu::buffer;

use super::{CodecMunger, Error, MAX_HEADER_SIZE, State};

#[derive(Clone, Debug, Default)]
pub struct Null {
    seeded_state: Option<State>,
}

impl Null {
    pub fn new(_logger: Logger) -> Self {
        Self::default()
    }

    pub fn get_seeded_state(&self) -> Option<State> {
        self.seeded_state
    }
}

impl CodecMunger for Null {
    fn get_state(&self) -> Option<State> {
        None
    }

    fn seed_state(&mut self, state: Option<State>) {
        self.seeded_state = state;
    }

    fn set_last(&mut self, _ext_pkt: &buffer::ExtPacket) {}

    fn update_offsets(&mut self, _ext_pkt: &buffer::ExtPacket) {}

    fn update_and_get(
        &mut self,
        _ext_pkt: &buffer::ExtPacket,
        _sn_out_of_order: bool,
        _sn_has_gap: bool,
        _max_temporal: i32,
    ) -> Result<(usize, [u8; MAX_HEADER_SIZE], usize), Error> {
        Ok((0, [0u8; MAX_HEADER_SIZE], 0))
    }

    fn update_and_get_padding(&mut self, _new_picture: bool) -> Result<Vec<u8>, Error> {
        Ok(Vec::new())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
