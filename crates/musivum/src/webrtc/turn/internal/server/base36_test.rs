// Ported from pion/turn, internal/server/base36_test.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust.

use super::*;

#[test]
fn test_base36() {
    for (name, input, expected) in [
        ("empty input", &b""[..], ""),
        ("zero byte", &[0u8][..], "0"),
        ("single byte - 1", &[1u8][..], "1"),
        ("single byte - 35", &[35u8][..], "Z"),
        ("single byte - 36", &[36u8][..], "10"),
        ("single byte - 255", &[255u8][..], "73"),
        ("multiple bytes - hello", &b"hello"[..], "5PZCSZU7"),
        ("multiple bytes - test", &b"test"[..], "WANEK4"),
        (
            "complex text",
            &b"long_complexTeXT wITH_space"[..],
            "6XMY2Y5EIZEF867E5LXYHH2OVVURC1A852VPOAZP0L",
        ),
    ] {
        let encoded = encode_base36(input);
        assert_eq!(expected, encoded, "{name}");
        let decoded = decode_base36(&encoded);
        assert_eq!(Some(input.to_vec()), decoded, "{name}");
        let decoded = decode_base36(&encoded.to_lowercase());
        assert_eq!(Some(input.to_vec()), decoded, "{name}");
    }
}
