// Ported from LiveKit, pkg/sfu/videolayerselector/framenumberwrapper.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the original has no constructor and is made as a
// struct literal with its logger, so the fields are open to the package and the rest is
// `Default`.

use crate::debugw;
use crate::protocol::logger::Logger;

#[derive(Default)]
pub struct FrameNumberWrapper {
    pub(super) offset: u64,
    pub(super) last: u64,
    pub(super) inited: bool,
    pub(super) logger: Logger,
}

impl FrameNumberWrapper {
    // UpdateAndGet returns the wrapped frame number from the given frame number, and updates the offset to
    // make sure the returned frame number is always inorder. Should only updateOffset if the new frame is a keyframe
    // because frame dependencies uses on the frame number diff so frames inside a GOP should have the same offset.
    pub fn update_and_get(&mut self, new: u64, update_offset: bool) -> u64 {
        if !self.inited {
            self.last = new;
            self.inited = true;
            return new;
        }

        if new <= self.last {
            return new.wrapping_add(self.offset);
        }

        if update_offset {
            let new16 = new.wrapping_add(self.offset) as u16;
            let last16 = self.last.wrapping_add(self.offset) as u16;
            // if new frame number wraps around and is considered as earlier by client, increase offset to make it later
            let diff = new16.wrapping_sub(last16);
            if diff > 0x8000 || (diff == 0x8000 && new16 < last16) {
                // increase offset by 6000, nearly 10 seconds for 30fps video with 3 spatial layers
                let prev_offset = self.offset;
                self.offset = self
                    .offset
                    .wrapping_add(65535u16.wrapping_sub(diff).wrapping_add(6000) as u64);

                debugw!(
                    self.logger,
                    "wrap around frame number seen, update offset",
                    "new" => new,
                    "last" => self.last,
                    "offset" => self.offset,
                    "prevOffset" => prev_offset,
                    "lastWrapFn" => last16,
                    "newWrapFn" => new16,
                );
            }
        }
        self.last = new;
        new.wrapping_add(self.offset)
    }

    pub fn last_origin(&self) -> u64 {
        self.last
    }
}

#[cfg(test)]
#[path = "framenumberwrapper_test.rs"]
mod framenumberwrapper_test;
