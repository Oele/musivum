// Ported from LiveKit's protocol, utils/rtputil/rtpconverter_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright LiveKit, Inc. (the original file has no notice of its own). Licensed under the
// Apache License, Version 2.0.
// Changes: translated from Go to Rust.

use super::*;

#[test]
fn test_rtp_converter() {
    let r = RtpConverter::new(90000);
    assert_eq!(time::SECOND, r.to_duration(90000));
    assert_eq!(500 * time::MILLISECOND, r.to_duration(45000));
    assert_eq!(time::MILLISECOND, r.to_duration(90));

    let r = RtpConverter::new(48000);
    assert_eq!(time::SECOND, r.to_duration_ext(48000));
    assert_eq!(48 * time::HOUR, r.to_duration_ext(24 * 60 * 60 * 2 * 48000));

    let r = RtpConverter::new(44100);
    assert_eq!(44100u32, r.to_rtp(time::SECOND));
    assert_eq!(44u32, r.to_rtp(time::MILLISECOND)); // 44.1 gets truncated to 44

    let r = RtpConverter::new(8000);
    assert_eq!(8000u64, r.to_rtp_ext(time::SECOND));
    assert_eq!(24 * 60 * 60 * 10 * 8000u64, r.to_rtp_ext(240 * time::HOUR));
}
