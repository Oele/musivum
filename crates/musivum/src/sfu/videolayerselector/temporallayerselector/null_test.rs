// Ported from LiveKit, pkg/sfu/videolayerselector/temporallayerselector/null_test.go at
// v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the original passes a nil packet, here it is an empty
// one.

use super::*;

#[test]
fn test_null_follows_target() {
    let mut n = Null::new();

    // a switch key frame on temporal layer 0 reaches a higher target right away
    let (this, next) = n.select(&buffer::ExtPacket::default(), 0, 2);
    assert_eq!(2i32, this);
    assert_eq!(2i32, next);

    // and a lower one
    let (this, next) = n.select(&buffer::ExtPacket::default(), 2, 1);
    assert_eq!(1i32, this);
    assert_eq!(1i32, next);
}
