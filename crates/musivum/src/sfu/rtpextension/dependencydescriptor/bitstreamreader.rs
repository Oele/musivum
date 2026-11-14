// Ported from LiveKit, pkg/sfu/rtpextension/dependencydescriptor/bitstreamreader.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the reader borrows the buffer it reads; numbers of bits
// are `i64` (Go's `int`: they are checked for being negative, and the bits that remain go
// below zero when reading has failed); the errors are variants of the package's `Error`,
// `io.EOF` among them.

use super::{Error, GoShift};

#[derive(Debug)]
pub struct BitStreamReader<'a> {
    pub(super) buf: &'a [u8],
    pos: usize,
    remaining_bits: i64,
}

impl<'a> BitStreamReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            remaining_bits: buf.len() as i64 * 8,
        }
    }

    pub fn remaining_bits(&self) -> i64 {
        self.remaining_bits
    }

    // Reads `bits` from the bitstream. `bits` must be in range [0, 64].
    // Returns an unsigned integer in range [0, 2^bits - 1].
    // On failure sets `BitstreamReader` into the failure state and returns 0.
    pub fn read_bits(&mut self, mut bits: i64) -> Result<u64, Error> {
        if !(0..=64).contains(&bits) {
            return Err(Error::InvalidNumberOfBits);
        }

        if self.remaining_bits < bits {
            self.remaining_bits -= bits;
            return Err(Error::Eof);
        }

        let remaining_bits_in_first_byte = self.remaining_bits % 8;
        self.remaining_bits -= bits;
        if bits < remaining_bits_in_first_byte {
            // Reading fewer bits than what's left in the current byte, just
            // return the portion of this byte that is needed.
            let offset = remaining_bits_in_first_byte - bits;
            return Ok(
                (self.buf[self.pos].go_shr(offset) & 1u8.go_shl(bits).wrapping_sub(1)) as u64,
            );
        }
        let mut result: u64 = 0;
        if remaining_bits_in_first_byte > 0 {
            // Read all bits that were left in the current byte and consume that byte.
            bits -= remaining_bits_in_first_byte;
            let mask = 1u8.go_shl(remaining_bits_in_first_byte).wrapping_sub(1);
            result = ((self.buf[self.pos] & mask) as u64).go_shl(bits);
            self.pos += 1;
        }
        // Read as many full bytes as we can.
        while bits >= 8 {
            bits -= 8;
            result |= (self.buf[self.pos] as u64).go_shl(bits);
            self.pos += 1;
        }

        // Whatever is left to read is smaller than a byte, so grab just the needed
        // bits and shift them into the lowest bits.
        if bits > 0 {
            result |= self.buf[self.pos].go_shr(8 - bits) as u64;
        }
        Ok(result)
    }

    pub fn read_bool(&mut self) -> Result<bool, Error> {
        let val = self.read_bits(1)?;
        Ok(val != 0)
    }

    pub fn ok(&self) -> bool {
        self.remaining_bits >= 0
    }

    pub fn invalidate(&mut self) {
        self.remaining_bits = -1;
    }

    // Reads value in range [0, `num_values` - 1].
    // This encoding is similar to ReadBits(val, Ceil(Log2(num_values)),
    // but reduces wastage incurred when encoding non-power of two value ranges
    // Non symmetric values are encoded as:
    // 1) n = bit_width(num_values)
    // 2) k = (1 << n) - num_values
    // Value v in range [0, k - 1] is encoded in (n-1) bits.
    // Value v in range [k, num_values - 1] is encoded as (v+k) in n bits.
    // https://aomediacodec.github.io/av1-spec/#nsn
    pub fn read_non_symmetric(&mut self, num_values: u32) -> Result<u32, Error> {
        if num_values >= (1u32 << 31) {
            return Err(Error::InvalidNumberOfValues);
        }

        let width = bitwidth(num_values);
        let num_min_bits_values = 1u32.go_shl(width).wrapping_sub(num_values);

        let val = self.read_bits(width - 1)?;
        if val < num_min_bits_values as u64 {
            return Ok(val as u32);
        }
        let bit = self.read_bits(1)?;
        Ok((val << 1)
            .wrapping_add(bit)
            .wrapping_sub(num_min_bits_values as u64) as u32)
    }

    pub fn bytes_read(&self) -> usize {
        if self.remaining_bits % 8 > 0 {
            return self.pos + 1;
        }
        self.pos
    }
}

pub(super) fn bitwidth(mut n: u32) -> i64 {
    let mut w: i64 = 0;
    while n != 0 {
        n >>= 1;
        w += 1;
    }
    w
}
