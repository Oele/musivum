// Ported from LiveKit, pkg/sfu/rtpextension/dependencydescriptor/bitstreamwriter.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the writer borrows the buffer it writes to; numbers of
// bits are `i64` (Go's `int`), as in the reader; where the original moves its slice on by a
// byte (`buf = buf[1:]`) an index moves; `write_partial_byte` has no receiver, which it does
// not use; the errors are variants of the package's `Error`.

use super::{Error, GoShift, bitwidth};

#[derive(Debug)]
pub struct BitStreamWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
    bit_offset: i64, // bit offset in the current byte
}

impl<'a> BitStreamWriter<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            bit_offset: 0,
        }
    }

    pub fn remaining_bits(&self) -> i64 {
        (self.buf.len() as i64 - self.pos as i64) * 8 - self.bit_offset
    }

    pub fn write_bits(&mut self, mut val: u64, mut bit_count: i64) -> Result<(), Error> {
        if bit_count > self.remaining_bits() {
            return Err(Error::InsufficientSpace);
        }

        let total_bits = bit_count;

        // push bits to the highest bits of uint64
        val = val.go_shl(64 - bit_count);

        // `buf[i]` is the original's `buf[0]`, `i += 1` its `buf = buf[1:]`.
        let buf = &mut self.buf[self.pos..];
        let mut i = 0;

        // The first byte is relatively special; the bit offset to write to may put us
        // in the middle of the byte, and the total bit count to write may require we
        // save the bits at the end of the byte.
        let remaining_bits_in_current_byte = 8 - self.bit_offset;
        let mut bits_in_first_byte = bit_count;
        if bits_in_first_byte > remaining_bits_in_current_byte {
            bits_in_first_byte = remaining_bits_in_current_byte;
        }

        buf[i] = Self::write_partial_byte(
            (val >> 56) as u8,
            bits_in_first_byte,
            buf[i],
            self.bit_offset,
        );

        if bit_count <= remaining_bits_in_current_byte {
            // no bit left to write
            return self.consume_bits(total_bits);
        }

        // write the rest of the bits
        val = val.go_shl(bits_in_first_byte);
        i += 1;
        bit_count -= bits_in_first_byte;
        while bit_count >= 8 {
            buf[i] = (val >> 56) as u8;
            i += 1;
            val <<= 8;
            bit_count -= 8;
        }

        // write the last bits
        if bit_count > 0 {
            buf[i] = Self::write_partial_byte((val >> 56) as u8, bit_count, buf[i], 0);
        }
        self.consume_bits(total_bits)
    }

    fn consume_bits(&mut self, bit_count: i64) -> Result<(), Error> {
        if bit_count > self.remaining_bits() {
            return Err(Error::InsufficientSpace);
        }

        // The sums are made as Go makes them, in its `int`.
        self.pos =
            (self.pos as i64).wrapping_add(self.bit_offset.wrapping_add(bit_count) / 8) as usize;
        self.bit_offset = self.bit_offset.wrapping_add(bit_count) % 8;

        Ok(())
    }

    fn write_partial_byte(
        source: u8,
        source_bit_count: i64,
        target: u8,
        target_bit_offset: i64,
    ) -> u8 {
        // if !(targetBitOffset < 8 &&  sourceBitCount <= (8-targetBitOffset)) {
        // 	return fmt.Errorf("invalid argument, source %d, sourceBitCount %d, target %d, targetBitOffset %d", source, sourceBitCount, target, targetBitOffset)
        // }

        // generate mask for bits to overwrite, shift source bits to highest bits, then position to target bit offset
        let mask = 0xffu8
            .go_shl(8 - source_bit_count)
            .go_shr(target_bit_offset as u8 as i64);

        // clear target bits and write source bits
        (target & !mask) | source.go_shr(target_bit_offset)
    }

    pub fn write_non_symmetric(&mut self, val: u32, num_values: u32) -> Result<(), Error> {
        if val >= num_values || num_values > 1 << 31 {
            return Err(Error::InvalidArgument { val, num_values });
        }
        if num_values == 1 {
            // When there is only one possible value, it requires zero bits to store it.
            // But WriteBits doesn't support writing zero bits.
            return Ok(());
        }

        let count_bits = bitwidth(num_values);
        let num_min_bits_values = 1u32.go_shl(count_bits).wrapping_sub(num_values);
        if val < num_min_bits_values {
            self.write_bits(val as u64, count_bits - 1)
        } else {
            self.write_bits(val.wrapping_add(num_min_bits_values) as u64, count_bits)
        }
    }
}

pub fn size_non_symmetric_bits(val: u32, num_values: u32) -> i64 {
    let count_bits = bitwidth(num_values);
    let num_min_bits_values = 1u32.go_shl(count_bits).wrapping_sub(num_values);
    if val < num_min_bits_values {
        count_bits - 1
    } else {
        count_bits
    }
}
