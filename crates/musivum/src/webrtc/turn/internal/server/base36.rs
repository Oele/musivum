// Ported from pion/turn, internal/server/base36.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. Go's `big.Int` is a big-endian number in bytes,
// divided and multiplied by hand; `Bytes()` of it has no leading zeros, as there.

// Base36 alphabet for encoding.
const BASE36_ALPHABET: &[u8; 36] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

// EncodeBase36 converts bytes to base36 string using big.Int for arbitrary length.
pub(crate) fn encode_base36(data: &[u8]) -> String {
    if data.is_empty() {
        return String::new();
    }

    // num := new(big.Int).SetBytes(data)
    let mut num: Vec<u8> = data.iter().copied().skip_while(|b| *b == 0).collect();
    if num.is_empty() {
        return "0".to_string();
    }

    let mut buf: Vec<u8> = Vec::with_capacity(data.len() * 2);
    while !num.is_empty() {
        // num.DivMod(num, base, remainder)
        let mut remainder: u32 = 0;
        for b in num.iter_mut() {
            let cur = (remainder << 8) | *b as u32;
            *b = (cur / 36) as u8;
            remainder = cur % 36;
        }
        let leading = num.iter().take_while(|b| **b == 0).count();
        num.drain(..leading);
        buf.push(BASE36_ALPHABET[remainder as usize]);
    }

    buf.reverse();

    String::from_utf8(buf).expect("the alphabet is ASCII")
}

// DecodeBase36 converts base36 string back to bytes using big.Int for arbitrary length.
pub(crate) fn decode_base36(encoded: &str) -> Option<Vec<u8>> {
    if encoded.is_empty() {
        return Some(Vec::new());
    }

    if encoded == "0" {
        return Some(vec![0]);
    }

    // num as big-endian bytes, without leading zeros
    let mut num: Vec<u8> = Vec::new();

    for c in encoded.to_uppercase().chars() {
        let digit = BASE36_ALPHABET.iter().position(|a| *a as char == c)?;
        // num.Mul(num, base); num.Add(num, digit)
        let mut carry = digit as u32;
        for b in num.iter_mut().rev() {
            let cur = *b as u32 * 36 + carry;
            *b = (cur & 0xff) as u8;
            carry = cur >> 8;
        }
        while carry > 0 {
            num.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
        let leading = num.iter().take_while(|b| **b == 0).count();
        num.drain(..leading);
    }

    Some(num)
}

#[cfg(test)]
#[path = "base36_test.rs"]
mod base36_test;
