// Ported from jxskiss/base62, base62.go and default.go at v1.1.0.
// Copyright (c) 2019 Shawn Wang <jxskiss@126.com>. Licensed under the MIT License.
// Changes: translated from Go to Rust; only what LiveKit's protocol utilities use is here:
// `EncodeToString` with the standard alphabet. Decoding, the other alphabets, the integer
// formats and the older encoder that the original keeps to compare itself with are left out.

const COMPACT_MASK: u8 = 0x1E; // 00011110
const MASK_5_BITS: u8 = 0x1F; // 00011111
const MASK_6_BITS: u8 = 0x3F; // 00111111

const ENCODE_STD: &[u8; 62] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

// EncodeToString returns a base62 string representation of src
// using StdEncoding.
pub(crate) fn encode_to_string(src: &[u8]) -> String {
    if src.is_empty() {
        return String::new();
    }
    let dst = Vec::with_capacity(src.len() * 9 / 5);
    let encoder = Encoder::new(src);
    let ret = encoder.encode_v2(dst, ENCODE_STD);
    String::from_utf8(ret).expect("the alphabet is ASCII")
}

struct Encoder<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Encoder<'a> {
    fn new(src: &'a [u8]) -> Self {
        Self {
            src,
            pos: src.len() * 8,
        }
    }

    fn encode_v2(mut self, mut dst: Vec<u8>, enc_table: &[u8; 62]) -> Vec<u8> {
        while self.pos > 0 {
            let mut size = 6;
            let mut b = self.get_6_bits();
            if b & COMPACT_MASK == COMPACT_MASK {
                if self.pos > 6 || b > MASK_5_BITS {
                    size = 5;
                }
                b &= MASK_5_BITS;
            }
            dst.push(enc_table[b as usize]);
            // Go's `pos` is a signed integer that ends below zero here; it is only asked
            // whether it is above zero.
            self.pos = self.pos.saturating_sub(size);
        }
        dst
    }

    fn get_6_bits(&self) -> u8 {
        let mut r = self.pos & 0x7;
        let mut i = self.pos >> 3;
        if r == 0 {
            (i, r) = (i - 1, 8);
        }
        let mut b = self.src[i] >> (8 - r);
        if r < 6 && i > 0 {
            b |= self.src[i - 1] << r;
        }
        b & MASK_6_BITS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum. The tests of the original (base62_test.go) encode and decode again, and
    // compare the encoder with its older one; neither the decoder nor the older encoder is
    // ported. Instead: what the original's `EncodeToString` gives for these inputs (run at
    // v1.1.0).
    #[test]
    fn encodes_as_the_original_does() {
        let mut counting = [0u8; 32];
        for (i, b) in counting.iter_mut().enumerate() {
            *b = (i * 7 + 3) as u8;
        }
        let cases: [(&[u8], &str); 18] = [
            (&[], ""),
            (&[0], "AA"),
            (&[0xff], "fH"),
            (&[0, 0], "AAA"),
            (&[0xff, 0xff, 0xff], "ffffP"),
            // the last six bits are 62 or 63, which takes one character more
            (&[0xff, 0xff], "fffB"),
            (&[0xfb, 0xff], "ffeB"),
            (&[0xf8, 0x00], "AgP"),
            (&[0x7c], "8B"),
            (&[0xfc], "8D"),
            (&[0x3e], "eB"),
            (&[0x3f], "fB"),
            (&[0xff; 5], "ffffffff"),
            ("Hello, 世界！".as_bytes(), "By77MW55WiL5gwybsxWZIB"),
            (
                &[0x1e, 0x3e, 0x1f, 0x3f, 0x00, 0x7b, 0xde, 0xf7],
                "379eA4nP8xD",
            ),
            (&counting, "cXtzHDcuyuKpda5jIGoemjty86KoSSodopFTeBGRowA"),
            (&[0; 20], "AAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            (&[0xff; 20], "ffffffffffffffffffffffffffffffff"),
        ];
        for (src, want) in cases {
            assert_eq!(want, encode_to_string(src), "{src:02x?}");
        }
    }
}
