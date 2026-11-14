// Ported from pion/interceptor, pkg/flexfec/util/bitarray.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust.

/// BitArray provides support for bitmask manipulations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BitArray {
    /// leftmost 64 bits
    pub lo: u64,
    /// rightmost 64 bits
    pub hi: u64,
}

impl BitArray {
    /// SetBit sets a bit to the specified bit value on the bitmask.
    pub fn set_bit(&mut self, bit_index: u32) {
        if bit_index < 64 {
            self.lo |= 0b1u64 << (63 - bit_index);
        } else {
            let hi_bit_index = bit_index - 64;
            self.hi |= shl(0b1, 63u32.wrapping_sub(hi_bit_index));
        }
    }

    /// Reset clears the bitmask.
    pub fn reset(&mut self) {
        self.lo = 0;
        self.hi = 0;
    }

    /// GetBit returns the bit value at a specified index of the bitmask.
    pub fn get_bit(&self, bit_index: u32) -> u8 {
        if bit_index < 64 {
            let result = self.lo & (0b1u64 << (63 - bit_index));
            if result > 0 {
                return 1;
            }

            return 0;
        }

        let hi_bit_index = bit_index - 64;
        let result = self.hi & shl(0b1, 63u32.wrapping_sub(hi_bit_index));
        if result > 0 {
            return 1;
        }

        0
    }
}

/// Go's `<<` on a `uint64`: a shift by 64 or more gives zero. That is what an index of 128
/// or more comes to in the original, whose subtraction wraps.
fn shl(value: u64, count: u32) -> u64 {
    value.checked_shl(count).unwrap_or(0)
}
