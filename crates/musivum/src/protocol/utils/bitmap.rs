// Ported from LiveKit's protocol, utils/bitmap.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the type constraint is a trait; a shift by 64, which
// gives zero in Go, is written out as that.

use std::marker::PhantomData;

/// `uint8 | uint16 | uint32 | uint64` upstream.
pub trait BitmapNumber: Copy + PartialOrd {
    fn to_u64(self) -> u64;
}

impl BitmapNumber for u8 {
    fn to_u64(self) -> u64 {
        self as u64
    }
}

impl BitmapNumber for u16 {
    fn to_u64(self) -> u64 {
        self as u64
    }
}

impl BitmapNumber for u32 {
    fn to_u64(self) -> u64 {
        self as u64
    }
}

impl BitmapNumber for u64 {
    fn to_u64(self) -> u64 {
        self
    }
}

#[derive(Clone, Debug)]
pub struct Bitmap<T: BitmapNumber> {
    bits: Vec<u64>,
    _number: PhantomData<T>,
}

// Go's `1 << n` of a `uint64`, which is zero for n = 64.
fn shl(v: u64, n: usize) -> u64 {
    v.checked_shl(n as u32).unwrap_or(0)
}

// Go's `v >> n` of a `uint64`, which is zero for n = 64.
fn shr(v: u64, n: usize) -> u64 {
    v.checked_shr(n as u32).unwrap_or(0)
}

impl<T: BitmapNumber> Bitmap<T> {
    pub fn new(size: usize) -> Self {
        // 1<<bits.Len64(uint64(size+63)/64)
        let len64 = u64::BITS - (size as u64).div_ceil(64).leading_zeros();
        Self {
            bits: vec![0; 1 << len64],
            _number: PhantomData,
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.bits.len() << 6
    }

    pub fn set(&mut self, val: T) {
        let (sm, s, o) = self.get_slot_and_offset(val);
        self.bits[s & sm] |= 1 << o;
    }

    pub fn get_and_set(&mut self, val: T) -> bool {
        let (sm, s, o) = self.get_slot_and_offset(val);
        let prev = self.bits[s & sm] & (1 << o) != 0;
        self.bits[s & sm] |= 1 << o;
        prev
    }

    pub fn set_range(&mut self, min: T, max: T) {
        if max < min {
            return;
        }

        let (sm, ls, rs, lo, ro) = self.get_slots_and_offsets(min, max);
        if ls == rs {
            self.bits[ls & sm] |= shl(1, ro - lo + 1).wrapping_sub(1) << lo;
        } else {
            self.bits[ls & sm] |= !((1 << lo) - 1);
            for i in ls + 1..rs {
                self.bits[i & sm] = !0u64;
            }
            self.bits[rs & sm] |= shl(1, ro + 1).wrapping_sub(1);
        }
    }

    pub fn clear(&mut self, val: T) {
        let (sm, s, o) = self.get_slot_and_offset(val);
        self.bits[s & sm] &= !(1 << o);
    }

    pub fn clear_range(&mut self, min: T, max: T) {
        if max < min {
            return;
        }

        let (sm, ls, rs, lo, ro) = self.get_slots_and_offsets(min, max);
        if ls == rs {
            self.bits[ls & sm] &= !(shl(1, ro - lo + 1).wrapping_sub(1) << lo);
        } else {
            self.bits[ls & sm] &= shr(!0u64, 64 - lo);
            for i in ls + 1..rs {
                self.bits[i & sm] = 0;
            }
            self.bits[rs & sm] &= shl(!0u64, ro + 1);
        }
    }

    pub fn is_set(&self, val: T) -> bool {
        let (sm, s, o) = self.get_slot_and_offset(val);
        self.bits[s & sm] & (1 << o) != 0
    }

    fn get_slot_and_offset(&self, val: T) -> (usize, usize, usize) {
        let val = val.to_u64();
        let sm = self.bits.len() - 1; // slot mask
        let s = (val >> 6) as usize; // slot
        let o = (val & 0x3f) as usize; // offset
        (sm, s, o)
    }

    fn get_slots_and_offsets(&self, min: T, max: T) -> (usize, usize, usize, usize, usize) {
        let (min, max) = (min.to_u64(), max.to_u64());
        let sm = self.bits.len() - 1; // slot mask

        let ls = (min >> 6) as usize; // left slot
        let mut rs = (max >> 6) as usize; // right slot

        if rs - ls > self.bits.len() {
            rs = ls + self.bits.len();
            return (sm, ls, rs, 0, 0);
        }

        let lo = (min & 0x3f) as usize; // left offset
        let ro = (max & 0x3f) as usize; // right offset
        (sm, ls, rs, lo, ro)
    }
}

#[cfg(test)]
#[path = "bitmap_test.rs"]
mod bitmap_test;
