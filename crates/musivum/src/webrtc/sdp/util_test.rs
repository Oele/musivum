// Ported from pion/sdp, util_test.go at v3.0.20.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; only the tests of what util.rs has are here. Left
// out, with what they test: TestGetPayloadTypeForVP8, TestGetCodecsForPayloadTypes,
// TestNewSessionID, TestConnectionRole_String, TestCodec_String,
// TestEquivalentFmtp_MismatchAfterSortAndTrim, TestCodecsMatch_MiddleFalse_ClockRateMismatch,
// TestCodecsMatch_MiddleFalse_EncodingParamsMismatch, TestGetPayloadTypeForCodec_Error_NotFound,
// TestLexer_HandleType_ElseIfErrorFromReadType, TestLexer_HandleType_SyntaxErrorWhenFnReturnsNil.
// `parse_rtcp_fb` returns a codec or an error, so the codec and the flag that the original
// returns next to an error are not there to look at.

use super::*;
use crate::webrtc::sdp::{MediaName, RangedPort, SessionDescription};

fn new_attribute(key: &str, value: &str) -> Attribute {
    Attribute::new(key.to_string(), Some(value.to_string()))
}

fn strings(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

fn get_test_session_description() -> SessionDescription {
    SessionDescription {
        media_descriptions: vec![MediaDescription {
            media_name: MediaName {
                media: "video".to_string(),
                port: RangedPort {
                    value: 51372,
                    range: None,
                },
                protos: strings(&["RTP", "AVP"]),
                formats: strings(&["120", "121", "126", "97", "98"]),
            },
            attributes: vec![
                new_attribute(
                    "fmtp:126 profile-level-id=42e01f;level-asymmetry-allowed=1;packetization-mode=1",
                    "",
                ),
                new_attribute(
                    "fmtp:97 profile-level-id=42e01f;level-asymmetry-allowed=1",
                    "",
                ),
                new_attribute("fmtp:98 profile-level-id=42e01e; packetization-mode=1", ""),
                new_attribute("fmtp:120 max-fs=12288;max-fr=60", ""),
                new_attribute("fmtp:121 max-fs=12288;max-fr=60", ""),
                new_attribute("rtpmap:120 VP8/90000", ""),
                new_attribute("rtpmap:121 VP9/90000", ""),
                new_attribute("rtpmap:126 H264/90000", ""),
                new_attribute("rtpmap:97 H264/90000", ""),
                new_attribute("rtpmap:98 H264/90000", ""),
                new_attribute("rtcp-fb:97 ccm fir", ""),
                new_attribute("rtcp-fb:97 nack", ""),
                new_attribute("rtcp-fb:97 nack pli", ""),
                new_attribute("rtcp-fb:* transport-cc", ""),
                new_attribute("rtcp-fb:* nack", ""),
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn test_get_codec_for_payload_type() {
    let audio_without_rtpmap = || SessionDescription {
        media_descriptions: vec![MediaDescription {
            media_name: MediaName {
                media: "audio".to_string(),
                protos: strings(&["RTP", "AVP"]),
                formats: strings(&["0", "8", "9"]),
                ..Default::default()
            },
            ..Default::default()
        }],
        ..Default::default()
    };

    struct Test {
        name: &'static str,
        sd: SessionDescription,
        payload_type: u8,
        expected: Codec,
    }
    let tests = [
        Test {
            name: "vp8",
            sd: get_test_session_description(),
            payload_type: 120,
            expected: Codec {
                payload_type: 120,
                name: "VP8".to_string(),
                clock_rate: 90000,
                fmtp: "max-fs=12288;max-fr=60".to_string(),
                rtcp_feedback: strings(&["transport-cc", "nack"]),
                ..Default::default()
            },
        },
        Test {
            name: "vp9",
            sd: get_test_session_description(),
            payload_type: 121,
            expected: Codec {
                payload_type: 121,
                name: "VP9".to_string(),
                clock_rate: 90000,
                fmtp: "max-fs=12288;max-fr=60".to_string(),
                rtcp_feedback: strings(&["transport-cc", "nack"]),
                ..Default::default()
            },
        },
        Test {
            name: "h264 126",
            sd: get_test_session_description(),
            payload_type: 126,
            expected: Codec {
                payload_type: 126,
                name: "H264".to_string(),
                clock_rate: 90000,
                fmtp: "profile-level-id=42e01f;level-asymmetry-allowed=1;packetization-mode=1"
                    .to_string(),
                rtcp_feedback: strings(&["transport-cc", "nack"]),
                ..Default::default()
            },
        },
        Test {
            name: "h264 97",
            sd: get_test_session_description(),
            payload_type: 97,
            expected: Codec {
                payload_type: 97,
                name: "H264".to_string(),
                clock_rate: 90000,
                fmtp: "profile-level-id=42e01f;level-asymmetry-allowed=1".to_string(),
                rtcp_feedback: strings(&["ccm fir", "nack", "nack pli", "transport-cc"]),
                ..Default::default()
            },
        },
        Test {
            name: "h264 98",
            sd: get_test_session_description(),
            payload_type: 98,
            expected: Codec {
                payload_type: 98,
                name: "H264".to_string(),
                clock_rate: 90000,
                fmtp: "profile-level-id=42e01e; packetization-mode=1".to_string(),
                rtcp_feedback: strings(&["transport-cc", "nack"]),
                ..Default::default()
            },
        },
        Test {
            name: "pcmu without rtpmap",
            sd: audio_without_rtpmap(),
            payload_type: 0,
            expected: Codec {
                payload_type: 0,
                name: "PCMU".to_string(),
                clock_rate: 8000,
                ..Default::default()
            },
        },
        Test {
            name: "pcma without rtpmap",
            sd: audio_without_rtpmap(),
            payload_type: 8,
            expected: Codec {
                payload_type: 8,
                name: "PCMA".to_string(),
                clock_rate: 8000,
                ..Default::default()
            },
        },
        Test {
            name: "g722 without rtpmap",
            sd: audio_without_rtpmap(),
            payload_type: 9,
            expected: Codec {
                payload_type: 9,
                name: "G722".to_string(),
                clock_rate: 8000,
                ..Default::default()
            },
        },
    ];
    for test in tests {
        let actual = get_codec_for_payload_type(&test.sd.media_descriptions, test.payload_type);
        assert_eq!(Ok(test.expected), actual, "{}", test.name);
    }
}

#[test]
fn test_parse_rtpmap_no_encoding_params() {
    let codec = parse_rtpmap("rtpmap:111 opus/48000").unwrap();

    assert_eq!(111u8, codec.payload_type);
    assert_eq!("opus", codec.name);
    assert_eq!(48000u32, codec.clock_rate);
    assert_eq!("", codec.encoding_parameters);
}

#[test]
fn test_parse_rtpmap_with_encoding_params() {
    let codec = parse_rtpmap("rtpmap:96 MP4A-LATM/44100/2").unwrap();

    assert_eq!(96u8, codec.payload_type);
    assert_eq!("MP4A-LATM", codec.name);
    assert_eq!(44100u32, codec.clock_rate);
    assert_eq!("2", codec.encoding_parameters);
}

#[test]
fn test_parse_rtpmap_error_missing_space() {
    // missing space
    assert_eq!(Err(Error::ExtractCodecRtpmap), parse_rtpmap("rtpmap:111"));
}

#[test]
fn test_parse_rtpmap_error_missing_colon() {
    // missing colon
    assert_eq!(
        Err(Error::ExtractCodecRtpmap),
        parse_rtpmap("rtpmap111 opus/48000")
    );
}

#[test]
fn test_parse_rtpmap_error_non_numeric_payload() {
    // non-numeric
    assert_eq!(
        Err(Error::ExtractCodecRtpmap),
        parse_rtpmap("rtpmap:xx opus/48000")
    );
}

#[test]
fn test_parse_rtpmap_error_non_numeric_clock_rate() {
    assert_eq!(
        Err(Error::ExtractCodecRtpmap),
        parse_rtpmap("rtpmap:111 opus/notanumber")
    );
}

#[test]
fn test_parse_fmtp_error_missing_space() {
    assert_eq!(Err(Error::ExtractCodecFmtp), parse_fmtp("fmtp:111"));
}

#[test]
fn test_parse_fmtp_error_missing_colon() {
    assert_eq!(Err(Error::ExtractCodecFmtp), parse_fmtp("fmtp111 a=b;c=d"));
}

#[test]
fn test_parse_fmtp_error_non_numeric_payload() {
    assert_eq!(
        Err(Error::ExtractCodecFmtp),
        parse_fmtp("fmtp:xx profile-level-id=42e01f;packetization-mode=1")
    );
}

#[test]
fn test_parse_rtcp_fb_missing_space() {
    assert_eq!(Err(Error::ExtractCodecRtcpFb), parse_rtcp_fb("rtcp-fb:97"));
}

#[test]
fn test_parse_rtcp_fb_missing_colon() {
    assert_eq!(
        Err(Error::ExtractCodecRtcpFb),
        parse_rtcp_fb("rtcp-fb97 nack")
    );
}

#[test]
fn test_parse_rtcp_fb_non_numeric() {
    assert!(parse_rtcp_fb("rtcp-fb:xx nack").is_err());
}

#[test]
fn test_build_codec_map_rtcp_fb_error() {
    let sd = SessionDescription {
        media_descriptions: vec![MediaDescription {
            attributes: vec![
                // non-numeric should return an error
                new_attribute("rtcp-fb:xx nack", ""),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };

    let codecs = get_codec_map(&sd.media_descriptions);

    // the three static codecs should be present, unchanged.
    assert_eq!(3, codecs.len());
    let c = &codecs[&0];
    assert_eq!(0u8, c.payload_type);
    assert_eq!("PCMU", c.name);
    assert_eq!(8000u32, c.clock_rate);
    assert!(c.rtcp_feedback.is_empty());
    let c = &codecs[&8];
    assert_eq!(8u8, c.payload_type);
    assert_eq!("PCMA", c.name);
    assert_eq!(8000u32, c.clock_rate);
    assert!(c.rtcp_feedback.is_empty());
    let c = &codecs[&9];
    assert_eq!(9u8, c.payload_type);
    assert_eq!("G722", c.name);
    assert_eq!(8000u32, c.clock_rate);
    assert!(c.rtcp_feedback.is_empty());
}

#[test]
fn test_get_codec_for_payload_type_error_not_found() {
    let sd = SessionDescription::default();
    assert_eq!(
        Err(Error::PayloadTypeNotFound),
        get_codec_for_payload_type(&sd.media_descriptions, 42)
    );
}
