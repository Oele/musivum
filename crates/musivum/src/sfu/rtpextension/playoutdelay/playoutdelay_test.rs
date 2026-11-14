// Ported from LiveKit, pkg/sfu/rtpextension/playoutdelay/playoutdelay_test.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

use super::*;

#[test]
fn test_playout_delay() {
    let p1 = PlayOutDelay { min: 100, max: 200 };
    let b = p1.marshal();
    assert!(b.is_ok(), "{b:?}");
    let b = b.unwrap();
    assert_eq!(PLAYOUT_DELAY_EXTENSION_SIZE, b.len());
    let mut p2 = PlayOutDelay::default();
    let err = p2.unmarshal(&b);
    assert_eq!(Ok(()), err);
    assert_eq!(p1, p2);

    // overflow
    let p3 = PlayOutDelay {
        min: 100,
        max: (1 << 12) * 10,
    };
    let err = p3.marshal();
    assert_eq!(Err(Error::PlayoutDelayOverflow), err);

    // too small
    let mut p4 = PlayOutDelay::default();
    let err = p4.unmarshal(&[0x00, 0x00]);
    assert_eq!(Err(Error::TooSmall), err);

    // from value
    let p5 = playout_delay_from_value((1 << 12) * 10, (1 << 12) * 10 + 10);
    let err = p5.marshal();
    assert!(err.is_ok(), "{err:?}");
    assert_eq!(((1u16 << 12) - 1) * 10, p5.min);
    assert_eq!(((1u16 << 12) - 1) * 10, p5.max);

    let p6 = PlayOutDelay {
        min: 100,
        max: PLAYOUT_DELAY_MAX_VALUE,
    };
    let bytes = p6.marshal();
    assert!(bytes.is_ok(), "{bytes:?}");
    let bytes = bytes.unwrap();
    let mut p6_unmarshal = PlayOutDelay::default();
    let err = p6_unmarshal.unmarshal(&bytes);
    assert_eq!(Ok(()), err);
    assert_eq!(p6, p6_unmarshal);
}
