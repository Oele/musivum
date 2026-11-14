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
