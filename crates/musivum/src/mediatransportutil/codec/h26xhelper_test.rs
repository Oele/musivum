// Ported from LiveKit's mediatransportutil, pkg/codec/h26xhelper_test.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the payloads are decoded by a small base64 decoder
// written here, where the original uses Go's `encoding/base64`; a panic fails a Rust test by
// itself, so the three tests that only look for one have no `recover`. The original has five
// tests; the four at the end are new and say so.

use std::sync::mpsc;
use std::time::Duration;

use super::*;

// New for musivum: Go's `base64.StdEncoding.DecodeString`, for input that is valid.
fn base64_std_decode(s: &str) -> Vec<u8> {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let mut acc = 0u32;
    let mut bits = 0;
    for c in s.bytes() {
        if c == b'=' {
            break;
        }
        let v = ALPHABET
            .iter()
            .position(|&a| a == c)
            .expect("a character of the base64 alphabet");
        acc = (acc << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    out
}

#[test]
fn test_extract_h26x_video_size() {
    struct Testcase {
        payload: &'static str,
        width: u32,
        height: u32,
        is_h264: bool,
    }
    let tc = |payload, width, height, is_h264| Testcase {
        payload,
        width,
        height,
        is_h264,
    };

    #[rustfmt::skip]
    let testcases = [
        tc("eAAOZ0LAH4xoBQBboB4RCNQABGjOPIA=", 1280, 720, true),
        tc("eAAPZ0LAFoxoCgL3lgHhEI1AAARozjyA", 640, 360, true),
        tc("eAAOZ0LADIxoFBl54B4RCNQABGjOPIA=", 320, 180, true),
        tc("YAEAGkABDAP//wFgAAADALAAAAMAAAMAXQAAGwJAAC9CAQMBYAAAAwCwAAADAAADAF0AAKACgIAtFiBu5FIy5+E9C+ob1SmoCAgIH8IBBAAHRAHAcvBbJA==", 1280, 720, false),
        tc("YAEAGkABDAP//wFgAAADALAAAAMAAAMAPwAAGwJAADBCAQMBYAAAAwCwAAADAAADAD8AAKAFAgFx8uIG7kUjLn4T0L6hvVKagICAgfwgEEAAB0QBwHLwWyQ=", 640, 360, false),
        tc("QgEDAWAAAAMAsAAAAwAAAwA8AACgCggMHz4gM7kUhi5+E9C+ob1Q/qoI9VQT6qoK9VVBfqqqDPVVVKagICAgfwgEEA==", 320, 180, false),
    ];

    for tc in testcases {
        let payload = base64_std_decode(tc.payload);

        let sz = if tc.is_h264 {
            extract_h264_video_size(&payload)
        } else {
            extract_h265_video_size(&payload)
        };
        assert_eq!(tc.width, sz.width, "{}", tc.payload);
        assert_eq!(tc.height, sz.height, "{}", tc.payload);
    }
}

#[test]
fn test_extract_h264_video_size_zero_size_stapa() {
    let payload = [0x38, 0x00, 0x00];
    let _ = extract_h264_video_size(&payload);
}

#[test]
fn test_extract_h265_video_size_zero_size_ap() {
    let payload = [0x61, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    let _ = extract_h265_video_size(&payload);
}

#[test]
fn test_parse_h264_sps_empty_after_strip_start_code() {
    let payload = [0x1c, 0xc0, 0x00, 0x01];
    let _ = extract_h264_video_size(&payload);
}

#[test]
fn test_parse_h264_sps_unbounded_poc_type_loop() {
    #[rustfmt::skip]
    let payload = [0x27, 0x08, 0x30, 0x30, 0x30, 0x41, 0x30,
        0x00, 0x00, 0x00, 0x7f, 0x27, 0x08, 0xff, 0x7f, 0xa8];
    let (done, wait) = mpsc::channel::<()>();
    std::thread::spawn(move || {
        let _ = extract_h264_video_size(&payload);
        let _ = done.send(());
    });
    match wait.recv_timeout(Duration::from_secs(3)) {
        Ok(()) => {}
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("ExtractH264VideoSize hung — CPU exhaustion")
        }
        // The thread went away without saying that it was done.
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("ExtractH264VideoSize panicked"),
    }
}

// ------------------------------------------
// New for musivum from here on: payloads that the original's tests do not have. Every one was
// given to the original, and what it answered is what is expected, also where the answer
// looks wrong (those places say so).

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
        .collect()
}

fn size(sz: VideoSize) -> (u32, u32) {
    (sz.width, sz.height)
}

// 1280x720: H.264 baseline and H.265.
const SPS_H264: &str = "6742c01f8c6805005ba01e1108d4";
const SPS_H265: &str = "420103016000000300b0000003000003005d0000a00280802d16206ee45232e7e13d0bea1bd529a80808081fc20104";

#[test]
fn video_size_of_more_payloads_as_in_the_original() {
    // A SPS on its own: H.264 baseline, H.264 high profile with cropping, H.265.
    let sps264 = unhex(SPS_H264);
    let sps264_high = unhex("67640028acd940780227e5c044000003000400000300f03c60c658");
    let sps265 = unhex(SPS_H265);
    assert_eq!((1280, 720), size(extract_h264_video_size(&sps264)));
    assert_eq!((1920, 1080), size(extract_h264_video_size(&sps264_high)));
    assert_eq!((1280, 720), size(extract_h265_video_size(&sps265)));
    // Neither is a SPS of the other codec.
    assert_eq!((0, 0), size(extract_h265_video_size(&sps264)));
    assert_eq!((0, 0), size(extract_h264_video_size(&sps265)));

    // The original does not look at the errors of reading a H.264 SPS: one that ends early
    // has a size all the same, here the first fragment (FU-A) of one and a STAP-B with one
    // of two bytes.
    let h264 = |payload: &str| size(extract_h264_video_size(&unhex(payload)));
    assert_eq!((16, 32), h264("7c8742c0"));
    assert_eq!((16, 32), h264("79000100026742"));
    // A fragment that is not the first has none.
    assert_eq!((0, 0), h264("7c0742c0"));
    // MTAP16 and MTAP24: the original does not find the SPS that `is_h264_key_frame` finds
    // in these.
    assert_eq!((0, 0), h264("7a000100050000016742"));
    assert_eq!((0, 0), h264("7b00010006000001026742"));
    // What it does find there is a unit whose length is 3 (MTAP16) or 4 (MTAP24) bytes
    // behind the first byte and which starts with the SPS. That is not where RFC 6184 has
    // them: 2 bytes (DONB) behind the first byte the length, and in the unit its DOND and
    // timestamp offset in front of the NAL unit.
    assert_eq!((1280, 720), h264(&format!("7affffff000e{SPS_H264}")));
    assert_eq!((1280, 720), h264(&format!("7bffffffff000e{SPS_H264}")));
    assert_eq!((0, 0), h264(&format!("7affff000e{SPS_H264}")));
    // The first fragment of a NAL unit of type 0 with a start code and a SPS behind it: the
    // original takes the start code off what it put together and finds the SPS. With a NRI
    // that is not zero there is no start code.
    let rest = &SPS_H264[2..];
    assert_eq!((1280, 720), h264(&format!("1c80000167{rest}")));
    assert_eq!((1280, 720), h264(&format!("1c8000000167{rest}")));
    assert_eq!((0, 0), h264(&format!("7c80000167{rest}")));
}

#[test]
fn video_size_behind_a_large_unit() {
    // H.264 STAP-A: a slice of 260 bytes (two bytes count in its length), then the SPS.
    let mut h264 = vec![0x78, 0x01, 0x04, 0x41];
    h264.extend_from_slice(&[0xff; 259]);
    assert_eq!((0, 0), size(extract_h264_video_size(&h264)));
    h264.extend_from_slice(&[0x00, 0x0e]);
    h264.extend_from_slice(&unhex(SPS_H264));
    assert_eq!((1280, 720), size(extract_h264_video_size(&h264)));

    // H.265 AP: a VPS of 260 bytes, then the SPS.
    let mut h265 = vec![0x60, 0x01, 0x01, 0x04, 0x40, 0x01];
    h265.extend_from_slice(&[0xff; 258]);
    assert_eq!((0, 0), size(extract_h265_video_size(&h265)));
    h265.extend_from_slice(&[0x00, 0x2f]);
    h265.extend_from_slice(&unhex(SPS_H265));
    assert_eq!((1280, 720), size(extract_h265_video_size(&h265)));
}

#[test]
fn start_codes_and_emulation_prevention_as_in_the_original() {
    for (b, want) in [
        // a start code and nothing else
        ("00000001", ""),
        ("000001", ""),
        // no start code
        ("000000", "000000"),
        ("0005000167", "0005000167"),
        ("0500000167", "0500000167"),
        ("0000050167", "0000050167"),
        ("0000000267", "0000000267"),
        // three and four bytes of start code
        ("00000167", "67"),
        ("0000000167", "67"),
    ] {
        assert_eq!(unhex(want), strip_start_code(&unhex(b)), "{b}");
    }

    for (data, want) in [
        ("0003", "0003"),
        ("000301", "000301"),
        ("000003", "0000"),
        ("00000003", "000000"),
        ("00000300000301", "0000000001"),
        ("0000030000030003", "000000000003"),
        ("0300000300", "03000000"),
    ] {
        assert_eq!(
            unhex(want),
            remove_emulation_prevention_bytes(&unhex(data)),
            "{data}"
        );
    }

    // A SPS behind a start code.
    for start_code in ["000001", "00000001"] {
        let info = parse_h264_sps(&unhex(&format!("{start_code}{SPS_H264}"))).unwrap();
        assert_eq!((1280, 720), (info.display_width, info.display_height));
        let info = parse_h265_sps(&unhex(&format!("{start_code}{SPS_H265}"))).unwrap();
        assert_eq!((1280, 720), (info.display_width, info.display_height));
    }
    assert_eq!(
        Err(Error::EmptySpsNalAfterStrippingStartCode),
        parse_h264_sps(&unhex("00000001"))
    );
    assert_eq!(
        Err(Error::NotEnoughBits),
        parse_h265_sps(&unhex("00000001"))
    );
}

// The number of offsets in the picture order count cycle of a H.264 SPS
// (num_ref_frames_in_pic_order_cnt_cycle), which the original limits to 255.
#[test]
fn h264_sps_with_a_picture_order_count_cycle_as_in_the_original() {
    let read = |nal: &str| parse_h264_sps(&unhex(nal)).map(|i| (i.display_width, i.display_height));

    // The number cannot be read (16 zero bits, then fewer than 16 bits): it is zero, and
    // reading goes on behind its zero bits. 32x48 is what the bits that are left say.
    assert_eq!(Ok((32, 48)), read("6742001ed30000e9c0"));
    // 255 offsets, each of one bit, and the same 32x48 behind them.
    let nal = format!("6742001ed300807f{}d3c0", "ff".repeat(31));
    assert_eq!(Ok((32, 48)), read(&nal));
    // 256 offsets: an error, and no size.
    let nal = format!("6742001ed30080{}e9e0", "ff".repeat(32));
    assert_eq!(
        Err(Error::SpsNumRefFramesInPicOrderCntCycleTooLarge),
        read(&nal)
    );
    assert_eq!((0, 0), size(extract_h264_video_size(&unhex(&nal))));

    // The type of the picture order count cannot be read (16 zero bits, a one, then fewer
    // than 16 bits): it is zero, and reading goes on behind the one. 32x48 is what the bits
    // that are left say when they are read as the fields that follow type 0.
    assert_eq!(Ok((32, 48)), read("6742001ec000393c"));
}
