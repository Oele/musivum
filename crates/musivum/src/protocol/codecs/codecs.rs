// Ported from LiveKit's protocol, codecs/codecs.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the codec parameters, variables of the package in the
// original, are statics that are made when first used (they hold strings), and are cloned
// where Go copies one; the codec that `to_webrtc_codec_parameters` takes may be `None`, as
// the pointer of the original may be nil. The tests at the end are new: the original has none.

use std::collections::HashMap;
use std::sync::LazyLock;

use crate::protocol::codecs::mime::{self, MimeType};
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::warnw;
use crate::webrtc::{self, RtcpFeedback, RtpCodecCapability, RtpCodecParameters};

pub static OPUS_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::Opus.to_string(),
            clock_rate: 48000,
            channels: 2,
            sdp_fmtp_line: "minptime=10;useinbandfec=1".to_string(),
            ..Default::default()
        },
        payload_type: 111,
    });

pub static RED_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::Red.to_string(),
            clock_rate: 48000,
            channels: 2,
            sdp_fmtp_line: "111/111".to_string(),
            ..Default::default()
        },
        payload_type: 63,
    });

pub static PCMU_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::Pcmu.to_string(),
            clock_rate: 8000,
            ..Default::default()
        },
        payload_type: 0,
    });

pub static PCMA_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::Pcma.to_string(),
            clock_rate: 8000,
            ..Default::default()
        },
        payload_type: 8,
    });

pub static VIDEO_RTX_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::Rtx.to_string(),
            clock_rate: 90000,
            ..Default::default()
        },
        ..Default::default()
    });

pub static VP8_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::Vp8.to_string(),
            clock_rate: 90000,
            ..Default::default()
        },
        payload_type: 96,
    });

pub static VP9_PROFILE_ID0_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::Vp9.to_string(),
            clock_rate: 90000,
            sdp_fmtp_line: "profile-id=0".to_string(),
            ..Default::default()
        },
        payload_type: 98,
    });

pub static VP9_PROFILE_ID1_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::Vp9.to_string(),
            clock_rate: 90000,
            sdp_fmtp_line: "profile-id=1".to_string(),
            ..Default::default()
        },
        payload_type: 100,
    });

pub static H264_PROFILE_LEVEL_ID42001F_PACKETIZATION_MODE0_CODEC_PARAMETERS: LazyLock<
    RtpCodecParameters,
> = LazyLock::new(|| RtpCodecParameters {
    rtp_codec_capability: RtpCodecCapability {
        mime_type: MimeType::H264.to_string(),
        clock_rate: 90000,
        sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42001f"
            .to_string(),
        ..Default::default()
    },
    payload_type: 104,
});

pub static H264_PROFILE_LEVEL_ID42001F_PACKETIZATION_MODE1_CODEC_PARAMETERS: LazyLock<
    RtpCodecParameters,
> = LazyLock::new(|| RtpCodecParameters {
    rtp_codec_capability: RtpCodecCapability {
        mime_type: MimeType::H264.to_string(),
        clock_rate: 90000,
        sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f"
            .to_string(),
        ..Default::default()
    },
    payload_type: 102,
});

pub static H264_PROFILE_LEVEL_ID42E01F_PACKETIZATION_MODE0_CODEC_PARAMETERS: LazyLock<
    RtpCodecParameters,
> = LazyLock::new(|| RtpCodecParameters {
    rtp_codec_capability: RtpCodecCapability {
        mime_type: MimeType::H264.to_string(),
        clock_rate: 90000,
        sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f"
            .to_string(),
        ..Default::default()
    },
    payload_type: 125,
});

pub static H264_PROFILE_LEVEL_ID42E01F_PACKETIZATION_MODE1_CODEC_PARAMETERS: LazyLock<
    RtpCodecParameters,
> = LazyLock::new(|| RtpCodecParameters {
    rtp_codec_capability: RtpCodecCapability {
        mime_type: MimeType::H264.to_string(),
        clock_rate: 90000,
        sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
            .to_string(),
        ..Default::default()
    },
    payload_type: 108,
});

pub static H264_PROFILE_LEVEL_ID4D001F_PACKETIZATION_MODE0_CODEC_PARAMETERS: LazyLock<
    RtpCodecParameters,
> = LazyLock::new(|| RtpCodecParameters {
    rtp_codec_capability: RtpCodecCapability {
        mime_type: MimeType::H264.to_string(),
        clock_rate: 90000,
        sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=4d001f"
            .to_string(),
        ..Default::default()
    },
    payload_type: 39,
});

pub static H264_PROFILE_LEVEL_ID4D001F_PACKETIZATION_MODE1_CODEC_PARAMETERS: LazyLock<
    RtpCodecParameters,
> = LazyLock::new(|| RtpCodecParameters {
    rtp_codec_capability: RtpCodecCapability {
        mime_type: MimeType::H264.to_string(),
        clock_rate: 90000,
        sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=4d001f"
            .to_string(),
        ..Default::default()
    },
    payload_type: 106,
});

pub const H264_HIGH_PROFILE_FMTP: &str =
    "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640032";
pub static H264_HIGH_PROFILE_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::H264.to_string(),
            clock_rate: 90000,
            sdp_fmtp_line: H264_HIGH_PROFILE_FMTP.to_string(),
            ..Default::default()
        },
        payload_type: 123,
    });

pub static AV1_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::Av1.to_string(),
            clock_rate: 90000,
            ..Default::default()
        },
        payload_type: 35,
    });

pub static H265_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::H265.to_string(),
            clock_rate: 90000,
            ..Default::default()
        },
        payload_type: 116,
    });

pub static FLEX_FEC03_CODEC_PARAMETERS: LazyLock<RtpCodecParameters> =
    LazyLock::new(|| RtpCodecParameters {
        rtp_codec_capability: RtpCodecCapability {
            mime_type: MimeType::FlexFec03.to_string(),
            clock_rate: 90000,
            sdp_fmtp_line: "repair-window=10000000".to_string(),
            rtcp_feedback: vec![RtcpFeedback {
                r#type: webrtc::TYPE_RTCP_FB_TRANSPORT_CC.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        },
        payload_type: 115,
    });

pub static VIDEO_CODECS_PARAMETERS: LazyLock<Vec<RtpCodecParameters>> = LazyLock::new(|| {
    vec![
        VP8_CODEC_PARAMETERS.clone(),
        VP9_PROFILE_ID0_CODEC_PARAMETERS.clone(),
        VP9_PROFILE_ID1_CODEC_PARAMETERS.clone(),
        H264_PROFILE_LEVEL_ID42E01F_PACKETIZATION_MODE0_CODEC_PARAMETERS.clone(),
        H264_PROFILE_LEVEL_ID42E01F_PACKETIZATION_MODE1_CODEC_PARAMETERS.clone(),
        H264_HIGH_PROFILE_CODEC_PARAMETERS.clone(),
        AV1_CODEC_PARAMETERS.clone(),
        H265_CODEC_PARAMETERS.clone(),
    ]
});

fn are_fmtp_lines_equal(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }

    let mut params_a: HashMap<&str, &str> = HashMap::new();
    for param in a.split(';') {
        if let Some((key, value)) = param.split_once('=') {
            params_a.insert(key, value);
        }
    }

    let mut count = 0;
    for param in b.split(';') {
        if let Some((key, value)) = param.split_once('=') {
            // a key that is not there reads as the empty string, as from Go's map
            if params_a.get(key).copied().unwrap_or("") != value {
                return false;
            }
            count += 1;
        }
    }
    count == params_a.len()
}

pub fn to_webrtc_codec_parameters(codec: Option<&livekit::Codec>) -> RtpCodecParameters {
    // codec.GetFmtpLine() and codec.GetMime(), which give the empty string for a nil codec
    let fmtp = codec.map_or("", |codec| codec.fmtp_line.as_str());
    let codec_mime = codec.map_or("", |codec| codec.mime.as_str());

    let mut params = match mime::normalize_mime_type(codec_mime) {
        MimeType::Opus => OPUS_CODEC_PARAMETERS.clone(),
        MimeType::Red => RED_CODEC_PARAMETERS.clone(),
        MimeType::Pcmu => PCMU_CODEC_PARAMETERS.clone(),
        MimeType::Pcma => PCMA_CODEC_PARAMETERS.clone(),
        MimeType::Rtx => VIDEO_RTX_CODEC_PARAMETERS.clone(),
        MimeType::Vp8 => VP8_CODEC_PARAMETERS.clone(),
        MimeType::Vp9 => {
            if fmtp.contains("profile-id=1") {
                VP9_PROFILE_ID1_CODEC_PARAMETERS.clone()
            } else {
                VP9_PROFILE_ID0_CODEC_PARAMETERS.clone()
            }
        }
        MimeType::Av1 => AV1_CODEC_PARAMETERS.clone(),
        MimeType::H264 => {
            if fmtp.contains("profile-level-id=640032") {
                H264_HIGH_PROFILE_CODEC_PARAMETERS.clone()
            } else if fmtp.contains("packetization-mode=0") {
                H264_PROFILE_LEVEL_ID42E01F_PACKETIZATION_MODE0_CODEC_PARAMETERS.clone()
            } else {
                H264_PROFILE_LEVEL_ID42E01F_PACKETIZATION_MODE1_CODEC_PARAMETERS.clone()
            }
        }
        MimeType::H265 => H265_CODEC_PARAMETERS.clone(),
        MimeType::FlexFec03 => FLEX_FEC03_CODEC_PARAMETERS.clone(),
        _ => return RtpCodecParameters::default(),
    };

    if !fmtp.is_empty() && !are_fmtp_lines_equal(fmtp, &params.sdp_fmtp_line) {
        warnw!(
            logger::get_logger(),
            "non-standard fmtp, may not be supported",
            nil,
            "mime" => codec_mime,
            "fmtp" => fmtp,
            "supportedFmtp" => params.sdp_fmtp_line,
        );
        params.sdp_fmtp_line = fmtp.to_string();
    }

    params
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codec(mime: &str, fmtp_line: &str) -> livekit::Codec {
        livekit::Codec {
            mime: mime.to_string(),
            fmtp_line: fmtp_line.to_string(),
        }
    }

    // New for musivum: the original has no tests. The expectations of the tests here are
    // what the original gives for the same lines and codecs (checked by running it).
    #[test]
    fn fmtp_lines_are_equal_when_their_parameters_are() {
        for (a, b, expected) in [
            ("", "", true),
            ("profile-id=0", "profile-id=0", true),
            ("profile-id=0", "profile-id=1", false),
            // the order does not matter
            ("a=1;b=2", "b=2;a=1", true),
            ("a=1;b=2", "a=1", false),
            ("a=1", "a=1;b=2", false),
            // only parameters with a value are compared
            ("111/111", "63/63", true),
            ("a=1;x", "y;a=1", true),
            ("a=1; b=2", "b=2;a=1", false),
            // a parameter that is there twice counts once on the left and twice on the right
            ("a=1;a=1", "a=1", true),
            ("a=1", "a=1;a=1", false),
            // a parameter without a value on the right is equal to one that is not there on
            // the left, and then only the numbers of parameters have to agree
            ("a=1", "b=", true),
            ("a=1", "a=1;b=", false),
            ("a=1;c=3", "a=1;b=", true),
            ("b=", "a=1", false),
        ] {
            assert_eq!(are_fmtp_lines_equal(a, b), expected, "{a:?} against {b:?}");
        }
    }

    #[track_caller]
    fn assert_parameters(
        params: &RtpCodecParameters,
        mime_type: &str,
        clock_rate: u32,
        channels: u16,
        sdp_fmtp_line: &str,
        payload_type: u8,
    ) {
        assert_eq!(params.mime_type, mime_type);
        assert_eq!(params.clock_rate, clock_rate);
        assert_eq!(params.channels, channels);
        assert_eq!(params.sdp_fmtp_line, sdp_fmtp_line);
        assert_eq!(params.payload_type, payload_type);
    }

    // New for musivum: every set of parameters, as the original prints them.
    #[test]
    fn the_parameters_of_the_codecs_are_those_of_the_original() {
        let h264 = |mode: u8, profile_level_id: &str| {
            format!(
                "level-asymmetry-allowed=1;packetization-mode={mode};profile-level-id={profile_level_id}"
            )
        };
        let p = &*OPUS_CODEC_PARAMETERS;
        assert_parameters(p, "audio/opus", 48000, 2, "minptime=10;useinbandfec=1", 111);
        let p = &*RED_CODEC_PARAMETERS;
        assert_parameters(p, "audio/red", 48000, 2, "111/111", 63);
        let p = &*PCMU_CODEC_PARAMETERS;
        assert_parameters(p, "audio/PCMU", 8000, 0, "", 0);
        let p = &*PCMA_CODEC_PARAMETERS;
        assert_parameters(p, "audio/PCMA", 8000, 0, "", 8);
        let p = &*VIDEO_RTX_CODEC_PARAMETERS;
        assert_parameters(p, "video/rtx", 90000, 0, "", 0);
        let p = &*VP8_CODEC_PARAMETERS;
        assert_parameters(p, "video/VP8", 90000, 0, "", 96);
        let p = &*VP9_PROFILE_ID0_CODEC_PARAMETERS;
        assert_parameters(p, "video/VP9", 90000, 0, "profile-id=0", 98);
        let p = &*VP9_PROFILE_ID1_CODEC_PARAMETERS;
        assert_parameters(p, "video/VP9", 90000, 0, "profile-id=1", 100);
        let p = &*H264_PROFILE_LEVEL_ID42001F_PACKETIZATION_MODE0_CODEC_PARAMETERS;
        assert_parameters(p, "video/H264", 90000, 0, &h264(0, "42001f"), 104);
        let p = &*H264_PROFILE_LEVEL_ID42001F_PACKETIZATION_MODE1_CODEC_PARAMETERS;
        assert_parameters(p, "video/H264", 90000, 0, &h264(1, "42001f"), 102);
        let p = &*H264_PROFILE_LEVEL_ID42E01F_PACKETIZATION_MODE0_CODEC_PARAMETERS;
        assert_parameters(p, "video/H264", 90000, 0, &h264(0, "42e01f"), 125);
        let p = &*H264_PROFILE_LEVEL_ID42E01F_PACKETIZATION_MODE1_CODEC_PARAMETERS;
        assert_parameters(p, "video/H264", 90000, 0, &h264(1, "42e01f"), 108);
        let p = &*H264_PROFILE_LEVEL_ID4D001F_PACKETIZATION_MODE0_CODEC_PARAMETERS;
        assert_parameters(p, "video/H264", 90000, 0, &h264(0, "4d001f"), 39);
        let p = &*H264_PROFILE_LEVEL_ID4D001F_PACKETIZATION_MODE1_CODEC_PARAMETERS;
        assert_parameters(p, "video/H264", 90000, 0, &h264(1, "4d001f"), 106);
        let p = &*H264_HIGH_PROFILE_CODEC_PARAMETERS;
        assert_parameters(p, "video/H264", 90000, 0, &h264(1, "640032"), 123);
        assert_eq!(H264_HIGH_PROFILE_FMTP, h264(1, "640032"));
        let p = &*AV1_CODEC_PARAMETERS;
        assert_parameters(p, "video/AV1", 90000, 0, "", 35);
        let p = &*H265_CODEC_PARAMETERS;
        assert_parameters(p, "video/H265", 90000, 0, "", 116);
        let p = &*FLEX_FEC03_CODEC_PARAMETERS;
        assert_parameters(
            p,
            "video/flexfec-03",
            90000,
            0,
            "repair-window=10000000",
            115,
        );

        // only the last has feedback of its own
        assert_eq!(
            FLEX_FEC03_CODEC_PARAMETERS.rtcp_feedback,
            vec![RtcpFeedback {
                r#type: "transport-cc".to_string(),
                parameter: String::new(),
            }]
        );
        assert!(H265_CODEC_PARAMETERS.rtcp_feedback.is_empty());

        let payload_types: Vec<u8> = VIDEO_CODECS_PARAMETERS
            .iter()
            .map(|params| params.payload_type)
            .collect();
        assert_eq!(payload_types, vec![96, 98, 100, 125, 108, 123, 35, 116]);
    }

    // New for musivum.
    #[test]
    fn a_codec_of_the_protocol_becomes_the_parameters_of_its_mime_type() {
        for (mime, fmtp, expected) in [
            ("audio/OPUS", "", &*OPUS_CODEC_PARAMETERS),
            ("audio/red", "", &*RED_CODEC_PARAMETERS),
            ("audio/pcmu", "", &*PCMU_CODEC_PARAMETERS),
            ("audio/pcma", "", &*PCMA_CODEC_PARAMETERS),
            ("video/rtx", "", &*VIDEO_RTX_CODEC_PARAMETERS),
            ("video/vp8", "", &*VP8_CODEC_PARAMETERS),
            ("video/vp9", "", &*VP9_PROFILE_ID0_CODEC_PARAMETERS),
            (
                "video/vp9",
                "profile-id=0",
                &*VP9_PROFILE_ID0_CODEC_PARAMETERS,
            ),
            (
                "video/vp9",
                "profile-id=1",
                &*VP9_PROFILE_ID1_CODEC_PARAMETERS,
            ),
            ("video/av1", "", &*AV1_CODEC_PARAMETERS),
            (
                "video/h264",
                "",
                &*H264_PROFILE_LEVEL_ID42E01F_PACKETIZATION_MODE1_CODEC_PARAMETERS,
            ),
            (
                "video/h264",
                "profile-level-id=42e01f;level-asymmetry-allowed=1;packetization-mode=0",
                &*H264_PROFILE_LEVEL_ID42E01F_PACKETIZATION_MODE0_CODEC_PARAMETERS,
            ),
            (
                "video/h264",
                H264_HIGH_PROFILE_FMTP,
                &*H264_HIGH_PROFILE_CODEC_PARAMETERS,
            ),
            ("video/h265", "", &*H265_CODEC_PARAMETERS),
            ("video/flexfec-03", "", &*FLEX_FEC03_CODEC_PARAMETERS),
        ] {
            assert_eq!(
                &to_webrtc_codec_parameters(Some(&codec(mime, fmtp))),
                expected,
                "{mime} {fmtp}"
            );
        }

        // a line that is not the one of the parameters is kept
        let h264 = to_webrtc_codec_parameters(Some(&codec(
            "video/h264",
            "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42001f",
        )));
        assert_eq!(h264.payload_type, 125);
        assert_eq!(
            h264.sdp_fmtp_line,
            "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42001f"
        );
        let opus = to_webrtc_codec_parameters(Some(&codec("audio/opus", "minptime=20")));
        assert_eq!(opus.payload_type, 111);
        assert_eq!(opus.sdp_fmtp_line, "minptime=20");

        // what the server has no parameters for
        for mime in [
            "audio/G722",
            "video/flexfec",
            "video/ulpfec",
            "audio/aac",
            "",
        ] {
            assert_eq!(
                to_webrtc_codec_parameters(Some(&codec(mime, "a=1"))),
                RtpCodecParameters::default(),
                "{mime}"
            );
        }
        assert_eq!(
            to_webrtc_codec_parameters(None),
            RtpCodecParameters::default()
        );
    }
}
