// Ported from jxskiss/base62, base62_test.go at v1.1.0.
// Copyright (c) 2019 Shawn Wang <jxskiss@126.com>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The tests of what is ported: the round trips of
// encoding and decoding (without the comparison with the obsolete first encoder, which is not
// ported) and the error on corrupt input. Test_EncodeDecode_RandomBytes makes ten thousand
// inputs where the original makes a million. Not here: Test_EncodeToBuf, TestDecodeToBuf,
// Test_encoder_next and Test_NewEncoding_panic (what they test is not ported), and the
// tests of integer.go.

use rand::RngExt;

use super::*;

#[test]
fn test_encode_decode() {
    let src = "Hello, 世界！".as_bytes();
    let dst = encode_to_string(src);
    let got = decode_string(&dst).unwrap_or_else(|err| panic!("failed decode, err = {err}"));
    assert_eq!(src, &got[..], "failed decode");
}

#[test]
fn test_encode_decode_zeros() {
    for i in 0..1000 {
        let src = vec![0u8; i];
        let dst = encode_to_string(&src);
        let got = decode_string(&dst).unwrap_or_else(|err| panic!("failed decode: err = {err}"));
        assert_eq!(src, got, "failed decode");
    }
}

#[test]
fn test_encode_decode_0x_ff() {
    for i in 0..1000 {
        let src = vec![0xffu8; i];
        let dst = encode_to_string(&src);
        let got = decode_string(&dst).unwrap_or_else(|err| panic!("failed decode: err = {err}"));
        assert_eq!(src, got, "failed decode");
    }
}

#[test]
fn test_encode_decode_random_bytes() {
    let mut rng = rand::rng();
    for _ in 0..10_000 {
        let mut src = vec![0u8; 32 + rng.random_range(0..32)];
        rand::fill(&mut src[..]);
        let dst = encode_to_string(&src);
        let got = decode_string(&dst).unwrap_or_else(|err| panic!("failed decode, err = {err}"));
        assert_eq!(src, got, "failed decode");
    }
}

#[test]
fn test_decode_corrupt_input_error() {
    let src: Vec<u8> = (0..=255u8).collect();
    let mut dst = vec![0u8; src.len() * 6 / 8 + 1];
    let err = decode(&src, &mut dst, &decode_map());
    assert!(
        err.is_err_and(|e| e.to_string().contains("illegal base62 data at input byte")),
        "decoding invalid data did not return CorruptInputError"
    );
}

// New for musivum: what the original's `EncodeToString` gives for these inputs (run at
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
