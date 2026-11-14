//! New for musivum: the original has no tests for keyframe.go. Every payload here was given to
//! the original, at the version the header of keyframe.rs names, and what it answered is what
//! is expected, also where the answer looks wrong (those places say so).

use crate::mediatransportutil::codec::*;
use crate::webrtc::rtp::codecs::Vp9Packet;

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
        .collect()
}

fn bit(b: bool) -> char {
    if b { '1' } else { '0' }
}

#[test]
fn key_frames_as_the_original_finds_them() {
    // The payload, and whether it is a key frame of H.264, H.265, VP8, VP9, AV1, in this
    // order. Every payload is given to all five: a payload of one codec is just bytes to the
    // others.
    let cases = [
        // H.264: a SPS is a key frame, an IDR slice is not
        ("6742c01f", "10000"),
        ("6588", "00000"),
        ("419a", "00000"),
        // reserved
        ("00", "00000"),
        ("", "00000"),
        // STAP-A with SPS and PPS, with a PPS only
        ("78000e6742c01f8c6805005ba01e1108d4000468ce3c80", "10100"),
        ("78000268ce", "00100"),
        // STAP-B, MTAP16, MTAP24 with a SPS
        ("79000100026742", "10100"),
        ("7a000100050000016742", "10100"),
        ("7b00010006000001026742", "10100"),
        // FU-A: the start of a SPS, the start of an IDR slice, not a start, too short
        ("7c8742c0", "10000"),
        ("7c8542c0", "00000"),
        ("7c0742", "00000"),
        ("7c", "00000"),
        // STAP-A: a unit that is longer than the packet, a STAP in a STAP before a SPS
        ("78000e6742", "00100"),
        ("780002780000026742", "10100"),
        // H.265: SPS and PPS are key frames, VPS and an IDR slice are not
        ("420101", "01000"),
        ("4401", "01000"),
        ("40010c", "00000"),
        ("2601", "00000"),
        // AP with VPS, SPS, PPS; with VPS and SPS; with VPS and SEI
        ("600100024001000342010100024401", "01000"),
        ("600100024001000242014e01", "01000"),
        ("60010002400100024e01", "00000"),
        // FU: the original reads the FU header (S, E, 6 bits of type) as if it were the
        // first byte of a NAL unit header (the type in bits 1 to 6). The start of a SPS (a1)
        // is not found, and the last fragments of the types 2 to 5 are taken for one.
        ("6201a101", "00000"),
        ("62014201", "01000"),
        ("620144", "01000"),
        ("6201", "00000"),
        // VP8: key frames with and without the extension, with all of the extension
        ("9080121002009d012a800268010000", "00100"),
        ("101002009d012a4001b4000000", "00100"),
        ("90f09267b49f1002009d012a8082684100", "00100"),
        // the P bit is set; not the start of a partition; too short; L without T
        ("9080121102009d012a800268010000", "00000"),
        ("8080121002009d012a800268010000", "00000"),
        ("9080", "00000"),
        ("90401210", "00000"),
        // VP9: the start of a key frame, of an inter frame, not a start
        ("88058249834200", "00010"),
        ("880586004092", "00000"),
        ("80058249", "00000"),
        // profile 3: a key frame, an inter frame; no frame marker; nothing after the header
        ("08b00102", "00010"),
        ("08b40102", "00000"),
        ("084201", "00000"),
        ("08", "00000"),
        // a key frame behind a scalability structure
        ("8a05180280016800824983", "00010"),
        // AV1: sequence header and a key frame, W=2; W=3 with an OBU between them
        ("280308000030100000", "00001"),
        ("38020800022800180000", "00101"),
        ("28030800003010", "00001"),
        // W=0, every OBU with its length: never a key frame in the original
        ("08020800023010", "00000"),
        // an inter frame; show_existing_frame
        ("2803080000303000", "00000"),
        ("2803080000309000", "00000"),
        // Y is set: still a key frame. Not the start of a coded video sequence (N=0), the
        // continuation of an OBU (Z=1): not one.
        ("6803080000301000", "00001"),
        ("2003080000301000", "00000"),
        ("a803080000301000", "00000"),
        // the first OBU is not a sequence header; W=1; a length beyond the packet; no second OBU
        ("28033010000800", "00000"),
        ("18080000", "00100"),
        ("280a0800", "00000"),
        ("2803080000", "00000"),
        // the reserved bits of the first byte are not looked at
        ("2903080000301000", "00001"),
        ("2f03080000301000", "00001"),
        // the length of the first OBU (3) written in three and in four bytes: read. In five:
        // not read. (These two are also the start of a VP9 key frame.)
        ("2883800008000030100000", "00001"),
        ("2883808000080000301000", "00011"),
        ("288380808000080000301000", "00010"),
    ];
    for (payload, want) in cases {
        let p = unhex(payload);
        let got: String = [
            is_h264_key_frame(&p),
            is_h265_key_frame(&p),
            is_vp8_key_frame(&p),
            is_vp9_key_frame(None, &p),
            is_av1_key_frame(&p),
        ]
        .into_iter()
        .map(bit)
        .collect();
        assert_eq!(want, got, "{payload}");
    }
}

// The lengths in the payloads above fit one byte. Here the unit in front of the key frame is
// larger: two bytes count in the length of a unit of H.264 and H.265, and the length of an
// AV1 OBU has a second byte.
#[test]
fn key_frames_behind_large_units() {
    let key_frames = |p: &[u8]| -> String {
        [
            is_h264_key_frame(p),
            is_h265_key_frame(p),
            is_vp8_key_frame(p),
            is_vp9_key_frame(None, p),
            is_av1_key_frame(p),
        ]
        .into_iter()
        .map(bit)
        .collect()
    };

    // AV1, W=2: a sequence header of 130 bytes, then a key frame, or an inter frame
    let mut av1 = vec![0x28, 0x82, 0x01, 0x08];
    av1.extend_from_slice(&[0; 129]);
    let mut av1_inter = av1.clone();
    av1.extend_from_slice(&[0x30, 0x10, 0x00]);
    av1_inter.extend_from_slice(&[0x30, 0x30, 0x00]);
    assert_eq!("00001", key_frames(&av1));
    assert_eq!("00000", key_frames(&av1_inter));

    // H.264 STAP-A: a slice of 260 bytes, with and without a SPS after it
    let mut h264 = vec![0x78, 0x01, 0x04, 0x41];
    h264.extend_from_slice(&[0xff; 259]);
    assert_eq!("00000", key_frames(&h264));
    h264.extend_from_slice(&[0x00, 0x0e]);
    h264.extend_from_slice(&unhex("6742c01f8c6805005ba01e1108d4"));
    assert_eq!("10000", key_frames(&h264));

    // H.265 AP: a VPS of 260 bytes, with and without a SPS after it
    let mut h265 = vec![0x60, 0x01, 0x01, 0x04, 0x40, 0x01];
    h265.extend_from_slice(&[0xff; 258]);
    assert_eq!("00000", key_frames(&h265));
    h265.extend_from_slice(&[0x00, 0x2f]);
    h265.extend_from_slice(&unhex(
        "420103016000000300b0000003000003005d0000a00280802d16206ee45232e7e13d0bea1bd529a80808081fc20104",
    ));
    assert_eq!("01000", key_frames(&h265));
}

#[test]
fn key_frame_by_the_name_of_the_codec() {
    let h264 = unhex("6742c01f");
    let h265 = unhex("420101");
    let vp8 = unhex("9080121002009d012a800268010000");
    let vp9 = unhex("88058249834200");
    let av1 = unhex("280308000030100000");
    for (codec, payload) in [
        ("h264", &h264),
        ("H264", &h264),
        ("h265", &h265),
        ("H265", &h265),
        ("vp8", &vp8),
        ("VP8", &vp8),
        ("vp9", &vp9),
        ("VP9", &vp9),
        ("av1", &av1),
        ("Av1", &av1),
    ] {
        assert!(is_key_frame(codec, payload), "{codec}");
        // No payload here is a key frame of another codec, and no name is that of another.
        for other in [&h264, &h265, &vp8, &vp9, &av1] {
            if other != payload {
                assert!(!is_key_frame(codec, other), "{codec}");
            }
        }
        assert!(!is_key_frame("opus", payload));
        assert!(!is_key_frame("", payload));
    }
}

#[test]
fn vp9_key_frame_of_a_packet_that_is_parsed_already() {
    for (payload, want) in [
        ("88058249834200", true),
        ("880586004092", false),
        ("80058249", false),
        ("08b00102", true),
        ("08b40102", false),
        ("084201", false),
        ("8a05180280016800824983", true),
    ] {
        let p = unhex(payload);
        let mut vp9 = Vp9Packet::default();
        vp9.unmarshal(&p).unwrap();
        assert_eq!(want, is_vp9_key_frame(Some(&vp9), &p), "{payload}");
        assert_eq!(want, is_vp9_key_frame(None, &p), "{payload}");
    }
}
