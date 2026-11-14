// Ported from LiveKit's protocol, codecs/mime/mimetype.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `MimeTypeCodec` and `MimeType` are enums, and their
// `String()` is `as_str()`, which like the original does not allocate, and `Display`;
// `MimeType.ReporterType()`, the conversion to the type of `observability/roomobs`, is left
// out: musivum does not have that reporting; `strings.EqualFold` is the function `equal_fold`
// at the end. The tests are new: the original has none.

use std::fmt;

use crate::webrtc;

pub const MIME_TYPE_PREFIX_AUDIO: &str = "audio/";
pub const MIME_TYPE_PREFIX_VIDEO: &str = "video/";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum MimeTypeCodec {
    #[default]
    Unknown,
    H264,
    H265,
    Opus,
    Red,
    Vp8,
    Vp9,
    Av1,
    G722,
    Pcmu,
    Pcma,
    Rtx,
    FlexFec,
    UlpFec,
    FlexFec03,
}

impl MimeTypeCodec {
    pub fn as_str(self) -> &'static str {
        match self {
            MimeTypeCodec::Unknown => "MimeTypeCodecUnknown",
            MimeTypeCodec::H264 => "H264",
            MimeTypeCodec::H265 => "H265",
            MimeTypeCodec::Opus => "opus",
            MimeTypeCodec::Red => "red",
            MimeTypeCodec::Vp8 => "VP8",
            MimeTypeCodec::Vp9 => "VP9",
            MimeTypeCodec::Av1 => "AV1",
            MimeTypeCodec::G722 => "G722",
            MimeTypeCodec::Pcmu => "PCMU",
            MimeTypeCodec::Pcma => "PCMA",
            MimeTypeCodec::Rtx => "rtx",
            MimeTypeCodec::FlexFec => "flexfec",
            MimeTypeCodec::UlpFec => "ulpfec",
            MimeTypeCodec::FlexFec03 => "flexfec-03",
        }
    }

    pub fn to_mime_type(self) -> MimeType {
        match self {
            MimeTypeCodec::Unknown => MimeType::Unknown,
            MimeTypeCodec::H264 => MimeType::H264,
            MimeTypeCodec::H265 => MimeType::H265,
            MimeTypeCodec::Opus => MimeType::Opus,
            MimeTypeCodec::Red => MimeType::Red,
            MimeTypeCodec::Vp8 => MimeType::Vp8,
            MimeTypeCodec::Vp9 => MimeType::Vp9,
            MimeTypeCodec::Av1 => MimeType::Av1,
            MimeTypeCodec::G722 => MimeType::G722,
            MimeTypeCodec::Pcmu => MimeType::Pcmu,
            MimeTypeCodec::Pcma => MimeType::Pcma,
            MimeTypeCodec::Rtx => MimeType::Rtx,
            MimeTypeCodec::FlexFec => MimeType::FlexFec,
            MimeTypeCodec::UlpFec => MimeType::UlpFec,
            MimeTypeCodec::FlexFec03 => MimeType::FlexFec03,
        }
    }
}

impl fmt::Display for MimeTypeCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn normalize_mime_type_codec(codec: &str) -> MimeTypeCodec {
    if equal_fold(codec, "h264") {
        MimeTypeCodec::H264
    } else if equal_fold(codec, "h265") {
        MimeTypeCodec::H265
    } else if equal_fold(codec, "opus") {
        MimeTypeCodec::Opus
    } else if equal_fold(codec, "red") {
        MimeTypeCodec::Red
    } else if equal_fold(codec, "vp8") {
        MimeTypeCodec::Vp8
    } else if equal_fold(codec, "vp9") {
        MimeTypeCodec::Vp9
    } else if equal_fold(codec, "av1") {
        MimeTypeCodec::Av1
    } else if equal_fold(codec, "g722") {
        MimeTypeCodec::G722
    } else if equal_fold(codec, "pcmu") {
        MimeTypeCodec::Pcmu
    } else if equal_fold(codec, "pcma") {
        MimeTypeCodec::Pcma
    } else if equal_fold(codec, "rtx") {
        MimeTypeCodec::Rtx
    } else if equal_fold(codec, "flexfec") {
        MimeTypeCodec::FlexFec
    } else if equal_fold(codec, "ulpfec") {
        MimeTypeCodec::UlpFec
    } else if equal_fold(codec, "flexfec-03") {
        MimeTypeCodec::FlexFec03
    } else {
        MimeTypeCodec::Unknown
    }
}

pub fn get_mime_type_codec(mime: &str) -> MimeTypeCodec {
    let Some(i) = mime.find('/') else {
        return MimeTypeCodec::Unknown;
    };

    normalize_mime_type_codec(&mime[i + 1..])
}

pub fn is_mime_type_codec_string_opus(codec: &str) -> bool {
    normalize_mime_type_codec(codec) == MimeTypeCodec::Opus
}

pub fn is_mime_type_codec_string_red(codec: &str) -> bool {
    normalize_mime_type_codec(codec) == MimeTypeCodec::Red
}

pub fn is_mime_type_codec_string_pcma(codec: &str) -> bool {
    normalize_mime_type_codec(codec) == MimeTypeCodec::Pcma
}

pub fn is_mime_type_codec_string_pcmu(codec: &str) -> bool {
    normalize_mime_type_codec(codec) == MimeTypeCodec::Pcmu
}

pub fn is_mime_type_codec_string_h264(codec: &str) -> bool {
    normalize_mime_type_codec(codec) == MimeTypeCodec::H264
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum MimeType {
    #[default]
    Unknown,
    H264,
    H265,
    Opus,
    Red,
    Vp8,
    Vp9,
    Av1,
    G722,
    Pcmu,
    Pcma,
    Rtx,
    FlexFec,
    UlpFec,
    FlexFec03,
}

impl MimeType {
    pub fn as_str(self) -> &'static str {
        match self {
            MimeType::Unknown => "MimeTypeUnknown",
            MimeType::H264 => webrtc::MIME_TYPE_H264,
            MimeType::H265 => webrtc::MIME_TYPE_H265,
            MimeType::Opus => webrtc::MIME_TYPE_OPUS,
            MimeType::Red => "audio/red",
            MimeType::Vp8 => webrtc::MIME_TYPE_VP8,
            MimeType::Vp9 => webrtc::MIME_TYPE_VP9,
            MimeType::Av1 => webrtc::MIME_TYPE_AV1,
            MimeType::G722 => webrtc::MIME_TYPE_G722,
            MimeType::Pcmu => webrtc::MIME_TYPE_PCMU,
            MimeType::Pcma => webrtc::MIME_TYPE_PCMA,
            MimeType::Rtx => webrtc::MIME_TYPE_RTX,
            MimeType::FlexFec => webrtc::MIME_TYPE_FLEX_FEC,
            MimeType::UlpFec => "video/ulpfec",
            MimeType::FlexFec03 => webrtc::MIME_TYPE_FLEX_FEC03,
        }
    }
}

impl fmt::Display for MimeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn normalize_mime_type(mime: &str) -> MimeType {
    if equal_fold(mime, webrtc::MIME_TYPE_H264) {
        MimeType::H264
    } else if equal_fold(mime, webrtc::MIME_TYPE_H265) {
        MimeType::H265
    } else if equal_fold(mime, webrtc::MIME_TYPE_OPUS) {
        MimeType::Opus
    } else if equal_fold(mime, "audio/red") {
        MimeType::Red
    } else if equal_fold(mime, webrtc::MIME_TYPE_VP8) {
        MimeType::Vp8
    } else if equal_fold(mime, webrtc::MIME_TYPE_VP9) {
        MimeType::Vp9
    } else if equal_fold(mime, webrtc::MIME_TYPE_AV1) {
        MimeType::Av1
    } else if equal_fold(mime, webrtc::MIME_TYPE_G722) {
        MimeType::G722
    } else if equal_fold(mime, webrtc::MIME_TYPE_PCMU) {
        MimeType::Pcmu
    } else if equal_fold(mime, webrtc::MIME_TYPE_PCMA) {
        MimeType::Pcma
    } else if equal_fold(mime, webrtc::MIME_TYPE_RTX) {
        MimeType::Rtx
    } else if equal_fold(mime, webrtc::MIME_TYPE_FLEX_FEC) {
        MimeType::FlexFec
    } else if equal_fold(mime, "video/ulpfec") {
        MimeType::UlpFec
    } else if equal_fold(mime, webrtc::MIME_TYPE_FLEX_FEC03) {
        MimeType::FlexFec03
    } else {
        MimeType::Unknown
    }
}

pub fn is_mime_type_string_equal(mime1: &str, mime2: &str) -> bool {
    normalize_mime_type(mime1) == normalize_mime_type(mime2)
}

pub fn is_mime_type_string_audio(mime: &str) -> bool {
    mime.starts_with(MIME_TYPE_PREFIX_AUDIO)
}

pub fn is_mime_type_audio(mime_type: MimeType) -> bool {
    mime_type.as_str().starts_with(MIME_TYPE_PREFIX_AUDIO)
}

pub fn is_mime_type_string_video(mime: &str) -> bool {
    mime.starts_with(MIME_TYPE_PREFIX_VIDEO)
}

pub fn is_mime_type_video(mime_type: MimeType) -> bool {
    mime_type.as_str().starts_with(MIME_TYPE_PREFIX_VIDEO)
}

pub fn is_mime_type_svc_capable(mime_type: MimeType) -> bool {
    matches!(mime_type, MimeType::Av1 | MimeType::Vp9)
}

pub fn is_mime_type_string_svc_capable(mime: &str) -> bool {
    is_mime_type_svc_capable(normalize_mime_type(mime))
}

pub fn is_mime_type_string_red(mime: &str) -> bool {
    normalize_mime_type(mime) == MimeType::Red
}

pub fn is_mime_type_string_opus(mime: &str) -> bool {
    normalize_mime_type(mime) == MimeType::Opus
}

pub fn is_mime_type_string_pcma(mime: &str) -> bool {
    normalize_mime_type(mime) == MimeType::Pcma
}

pub fn is_mime_type_string_pcmu(mime: &str) -> bool {
    normalize_mime_type(mime) == MimeType::Pcmu
}

pub fn is_mime_type_string_rtx(mime: &str) -> bool {
    normalize_mime_type(mime) == MimeType::Rtx
}

pub fn is_mime_type_string_vp8(mime: &str) -> bool {
    normalize_mime_type(mime) == MimeType::Vp8
}

pub fn is_mime_type_string_vp9(mime: &str) -> bool {
    normalize_mime_type(mime) == MimeType::Vp9
}

pub fn is_mime_type_string_h264(mime: &str) -> bool {
    normalize_mime_type(mime) == MimeType::H264
}

pub fn is_mime_type_string_flex_fec03(mime: &str) -> bool {
    normalize_mime_type(mime) == MimeType::FlexFec03
}

// Go's strings.EqualFold(s, t) for a `t` that is ASCII, as every name is that this file
// compares with: whether the two are equal under Unicode's simple case folding. That folding
// takes, next to the ASCII letters in their other case, two more characters to an ASCII
// letter: 'ſ' (U+017F) to 's' and the Kelvin sign (U+212A) to 'k'.
fn equal_fold(s: &str, t: &str) -> bool {
    let mut t = t.chars();
    for sr in s.chars() {
        let Some(tr) = t.next() else {
            return false;
        };
        if sr.eq_ignore_ascii_case(&tr) {
            continue;
        }
        match (sr, tr) {
            ('\u{17f}', 's' | 'S') | ('\u{212a}', 'k' | 'K') => continue,
            _ => return false,
        }
    }
    t.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIME_TYPES: [MimeType; 15] = [
        MimeType::Unknown,
        MimeType::H264,
        MimeType::H265,
        MimeType::Opus,
        MimeType::Red,
        MimeType::Vp8,
        MimeType::Vp9,
        MimeType::Av1,
        MimeType::G722,
        MimeType::Pcmu,
        MimeType::Pcma,
        MimeType::Rtx,
        MimeType::FlexFec,
        MimeType::UlpFec,
        MimeType::FlexFec03,
    ];

    const MIME_TYPE_CODECS: [MimeTypeCodec; 15] = [
        MimeTypeCodec::Unknown,
        MimeTypeCodec::H264,
        MimeTypeCodec::H265,
        MimeTypeCodec::Opus,
        MimeTypeCodec::Red,
        MimeTypeCodec::Vp8,
        MimeTypeCodec::Vp9,
        MimeTypeCodec::Av1,
        MimeTypeCodec::G722,
        MimeTypeCodec::Pcmu,
        MimeTypeCodec::Pcma,
        MimeTypeCodec::Rtx,
        MimeTypeCodec::FlexFec,
        MimeTypeCodec::UlpFec,
        MimeTypeCodec::FlexFec03,
    ];

    // New for musivum: the original has no tests. The texts are the original's, with the
    // constants of pion written out.
    #[test]
    fn mime_types_have_the_texts_of_the_original() {
        let texts = [
            "MimeTypeUnknown",
            "video/H264",
            "video/H265",
            "audio/opus",
            "audio/red",
            "video/VP8",
            "video/VP9",
            "video/AV1",
            "audio/G722",
            "audio/PCMU",
            "audio/PCMA",
            "video/rtx",
            "video/flexfec",
            "video/ulpfec",
            "video/flexfec-03",
        ];
        for (mime_type, text) in MIME_TYPES.into_iter().zip(texts) {
            assert_eq!(mime_type.as_str(), text);
            assert_eq!(mime_type.to_string(), text);
        }

        let texts = [
            "MimeTypeCodecUnknown",
            "H264",
            "H265",
            "opus",
            "red",
            "VP8",
            "VP9",
            "AV1",
            "G722",
            "PCMU",
            "PCMA",
            "rtx",
            "flexfec",
            "ulpfec",
            "flexfec-03",
        ];
        for (codec, text) in MIME_TYPE_CODECS.into_iter().zip(texts) {
            assert_eq!(codec.as_str(), text);
            assert_eq!(codec.to_string(), text);
        }

        assert_eq!(MimeType::default(), MimeType::Unknown);
        assert_eq!(MimeTypeCodec::default(), MimeTypeCodec::Unknown);
    }

    // New for musivum.
    #[test]
    fn every_mime_type_comes_back_from_its_text_in_any_case() {
        for mime_type in MIME_TYPES {
            let text = mime_type.as_str();
            // the text of the unknown type is not a mime type, and stays unknown
            assert_eq!(normalize_mime_type(text), mime_type, "{text}");
            assert_eq!(
                normalize_mime_type(&text.to_lowercase()),
                mime_type,
                "{text}"
            );
            assert_eq!(
                normalize_mime_type(&text.to_uppercase()),
                mime_type,
                "{text}"
            );
            assert_eq!(normalize_mime_type(text).to_string(), text);
        }

        for text in [
            "",
            "video",
            "video/",
            "VP8",
            "video/VP8 ",
            " video/VP8",
            "video/VP80",
            "video/VP",
            "audio/VP8",
            "video/opus",
            "video/red",
            "video/flexfec-0",
            "audio/aac",
        ] {
            assert_eq!(normalize_mime_type(text), MimeType::Unknown, "{text:?}");
        }
    }

    // New for musivum.
    #[test]
    fn every_codec_comes_back_from_its_text_in_any_case() {
        for (codec, mime_type) in MIME_TYPE_CODECS.into_iter().zip(MIME_TYPES) {
            let text = codec.as_str();
            assert_eq!(normalize_mime_type_codec(text), codec, "{text}");
            assert_eq!(
                normalize_mime_type_codec(&text.to_lowercase()),
                codec,
                "{text}"
            );
            assert_eq!(
                normalize_mime_type_codec(&text.to_uppercase()),
                codec,
                "{text}"
            );

            // the codec of a mime type is what follows the first slash, whatever is before
            assert_eq!(codec.to_mime_type(), mime_type);
            assert_eq!(get_mime_type_codec(mime_type.as_str()), codec);
            assert_eq!(
                get_mime_type_codec(&mime_type.as_str().to_uppercase()),
                codec
            );
        }

        assert_eq!(get_mime_type_codec("x/vp8"), MimeTypeCodec::Vp8);
        assert_eq!(get_mime_type_codec("/vp8"), MimeTypeCodec::Vp8);
        assert_eq!(get_mime_type_codec("vp8"), MimeTypeCodec::Unknown);
        assert_eq!(get_mime_type_codec("video/"), MimeTypeCodec::Unknown);
        assert_eq!(get_mime_type_codec("video/x/vp8"), MimeTypeCodec::Unknown);
        assert_eq!(normalize_mime_type_codec("vp8 "), MimeTypeCodec::Unknown);
        assert_eq!(
            normalize_mime_type_codec("flexfec-3"),
            MimeTypeCodec::Unknown
        );

        assert!(is_mime_type_codec_string_opus("OPUS"));
        assert!(is_mime_type_codec_string_red("Red"));
        assert!(is_mime_type_codec_string_pcma("pcma"));
        assert!(is_mime_type_codec_string_pcmu("pcmu"));
        assert!(is_mime_type_codec_string_h264("h264"));
        assert!(!is_mime_type_codec_string_h264("h265"));
    }

    // New for musivum.
    #[test]
    fn folding_is_go_s() {
        // strings.EqualFold folds these two to ASCII letters as well
        assert_eq!(normalize_mime_type("audio/opu\u{17f}"), MimeType::Opus);
        assert_eq!(normalize_mime_type_codec("OPU\u{17f}"), MimeTypeCodec::Opus);
        assert!(equal_fold("\u{212a}", "k"));
        assert!(equal_fold("\u{212a}", "K"));
        assert!(!equal_fold("\u{212a}", "s"));
        // and nothing else
        assert_eq!(normalize_mime_type("audio/\u{f6}pus"), MimeType::Unknown);
        assert_eq!(normalize_mime_type("vide\u{3bf}/vp8"), MimeType::Unknown);
        assert!(equal_fold("", ""));
        assert!(!equal_fold("a", ""));
        assert!(!equal_fold("", "a"));
        assert!(!equal_fold("a-", "a_"));
        assert!(!equal_fold("@", "`"));
        assert!(!equal_fold("[", "{"));
    }

    // New for musivum.
    #[test]
    fn what_kind_a_mime_type_is() {
        for mime_type in MIME_TYPES {
            let audio = matches!(
                mime_type,
                MimeType::Opus | MimeType::Red | MimeType::G722 | MimeType::Pcmu | MimeType::Pcma
            );
            let video = !audio && mime_type != MimeType::Unknown;
            assert_eq!(is_mime_type_audio(mime_type), audio, "{mime_type}");
            assert_eq!(is_mime_type_video(mime_type), video, "{mime_type}");
            assert_eq!(is_mime_type_string_audio(mime_type.as_str()), audio);
            assert_eq!(is_mime_type_string_video(mime_type.as_str()), video);

            let svc = matches!(mime_type, MimeType::Av1 | MimeType::Vp9);
            assert_eq!(is_mime_type_svc_capable(mime_type), svc, "{mime_type}");
            assert_eq!(
                is_mime_type_string_svc_capable(&mime_type.as_str().to_lowercase()),
                svc
            );
        }

        // the prefixes are compared as they are, unlike the names
        assert!(is_mime_type_string_audio("audio/anything"));
        assert!(!is_mime_type_string_audio("Audio/opus"));
        assert!(is_mime_type_string_video("video/anything"));
        assert!(!is_mime_type_string_video("VIDEO/VP8"));

        assert!(is_mime_type_string_equal("video/vp8", "VIDEO/VP8"));
        assert!(!is_mime_type_string_equal("video/vp8", "video/vp9"));
        // two that are not known are equal
        assert!(is_mime_type_string_equal("audio/aac", "video/theora"));

        assert!(is_mime_type_string_red("audio/RED"));
        assert!(is_mime_type_string_opus("audio/OPUS"));
        assert!(is_mime_type_string_pcma("audio/pcma"));
        assert!(is_mime_type_string_pcmu("audio/pcmu"));
        assert!(is_mime_type_string_rtx("video/RTX"));
        assert!(is_mime_type_string_vp8("video/vp8"));
        assert!(is_mime_type_string_vp9("video/vp9"));
        assert!(is_mime_type_string_h264("video/h264"));
        assert!(is_mime_type_string_flex_fec03("video/FLEXFEC-03"));
        assert!(!is_mime_type_string_flex_fec03("video/flexfec"));
    }
}
