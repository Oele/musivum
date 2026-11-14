// Ported from LiveKit, pkg/sfu/videolayerselector/null.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the embedded `*Base` is the field `base`, and the
// selector says where it is: every method of the trait is `Base`'s.

use std::any::Any;

use super::{Base, VideoLayerSelector};
use crate::protocol::logger::Logger;

pub struct Null {
    pub(super) base: Base,
}

impl Null {
    pub fn new(logger: Logger) -> Self {
        Self {
            base: Base::new(logger),
        }
    }
}

impl VideoLayerSelector for Null {
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
}
