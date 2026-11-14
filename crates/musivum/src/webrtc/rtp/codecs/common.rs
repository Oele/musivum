// Ported from pion/rtp, codecs/common.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; only what `VP9Packet` uses is here: `minInt` and
// `audioDepacketizer` are left out. `videoDepacketizer` is public, as a struct that holds one
// could not be written out elsewhere otherwise.

// videoDepacketizer is a mixin for video codec depacketizers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VideoDepacketizer {
    zero_allocation: bool,
}

impl VideoDepacketizer {
    pub fn is_partition_tail(&self, marker: bool, _: &[u8]) -> bool {
        marker
    }

    // SetZeroAllocation enables Zero Allocation mode for the depacketizer
    // By default the Depacketizers will allocate as they parse. These allocations
    // are needed for Metadata and other optional values. If you don't need this information
    // enabling SetZeroAllocation gives you higher performance at a reduced feature set.
    pub fn set_zero_allocation(&mut self, zero_allocation: bool) {
        self.zero_allocation = zero_allocation;
    }
}

// resizeUint16Slice resizes the provided slice to the desired size, or
// allocates a new slice if needed. The contents are unspecified; the
// caller must overwrite every element.
pub(super) fn resize_uint16_slice(mut s: Vec<u16>, desired_size: usize) -> Vec<u16> {
    if s.capacity() >= desired_size {
        s.resize(desired_size, 0);
        return s;
    }

    vec![0; desired_size]
}

#[cfg(test)]
#[path = "common_test.rs"]
mod common_test;
