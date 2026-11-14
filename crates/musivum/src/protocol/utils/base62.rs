// Ported from jxskiss/base62, base62.go and default.go at v1.1.0.
// Copyright (c) 2019 Shawn Wang <jxskiss@126.com>. Licensed under the MIT License.
// Changes: translated from Go to Rust, as far as LiveKit's protocol utilities (hashed ids and
// secrets) and its TURN credentials use it:
// `EncodeToString` and `DecodeString` of the standard alphabet (`_encodeV2` and `decode`).
// Not here: the obsolete first encoder, `EncodeToBuf`, `DecodeToBuf`, other alphabets and the
// integers of `integer.go`.

use std::fmt;

const BASE: usize = 62;
const COMPACT_MASK: u8 = 0x1E; // 00011110
const MASK5BITS: u8 = 0x1F; // 00011111
const MASK6BITS: u8 = 0x3F; // 00111111

const ENCODE_STD: &[u8; BASE] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

fn decode_map() -> [u8; 256] {
    let mut m = [0xFFu8; 256];
    for (i, c) in ENCODE_STD.iter().enumerate() {
        m[*c as usize] = i as u8;
    }
    m
}

// EncodeToString returns a base62 string representation of src.
pub fn encode_to_string(src: &[u8]) -> String {
    if src.is_empty() {
        return String::new();
    }
    let mut dst = Vec::with_capacity(src.len() * 9 / 5);
    let mut enc = Encoder {
        src,
        pos: src.len() * 8,
    };
    enc.encode_v2(&mut dst, ENCODE_STD);
    String::from_utf8(dst).expect("the alphabet is ASCII")
}

struct Encoder<'a> {
    src: &'a [u8],
    pos: usize,
}

impl Encoder<'_> {
    fn encode_v2(&mut self, dst: &mut Vec<u8>, enc_table: &[u8]) {
        while self.pos > 0 {
            let mut size = 6;
            let mut b = self.get6bits();
            if b & COMPACT_MASK == COMPACT_MASK {
                if self.pos > 6 || b > MASK5BITS {
                    size = 5;
                }
                b &= MASK5BITS;
            }
            dst.push(enc_table[b as usize]);
            self.pos = self.pos.saturating_sub(size);
        }
    }

    fn get6bits(&self) -> u8 {
        let mut r = self.pos & 0x7;
        let mut i = self.pos >> 3;
        if r == 0 {
            i -= 1;
            r = 8;
        }
        let mut b = self.src[i] >> (8 - r);
        if r < 6 && i > 0 {
            b |= self.src[i - 1] << r;
        }
        b & MASK6BITS
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorruptInputError(pub i64);

impl fmt::Display for CorruptInputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "illegal base62 data at input byte {}", self.0)
    }
}

impl std::error::Error for CorruptInputError {}

// DecodeString returns the bytes represented by the base62 string src.
//
// If src contains invalid base62 data, it will return nil and CorruptInputError.
pub fn decode_string(src: &str) -> Result<Vec<u8>, CorruptInputError> {
    let src = src.as_bytes();
    if src.is_empty() {
        return Ok(Vec::new());
    }
    let mut dst = vec![0u8; src.len() * 6 / 8 + 1];
    let idx = decode(src, &mut dst, &decode_map())?;
    Ok(dst[idx..].to_vec())
}

fn decode(dec: &[u8], dst: &mut [u8], dec_table: &[u8; 256]) -> Result<usize, CorruptInputError> {
    let mut idx = dst.len();
    let mut pos: u32 = 0;
    let mut b: u64 = 0;
    for (i, c) in dec.iter().enumerate() {
        let x = dec_table[*c as usize];
        if x == 0xFF {
            return Err(CorruptInputError(i as i64));
        }
        if i == dec.len() - 1 {
            b |= (x as u64) << pos;
            pos += 8 - x.leading_zeros(); // bits.Len8(x)
        } else if x & COMPACT_MASK == COMPACT_MASK {
            b |= (x as u64) << pos;
            pos += 5;
        } else {
            b |= (x as u64) << pos;
            pos += 6;
        }
        if pos >= 8 {
            idx -= 1;
            dst[idx] = b as u8;
            pos %= 8;
            b >>= 8;
        }
    }
    if pos > 0 {
        idx -= 1;
        dst[idx] = b as u8;
    }
    Ok(idx)
}

#[cfg(test)]
#[path = "base62_test.rs"]
mod base62_test;
