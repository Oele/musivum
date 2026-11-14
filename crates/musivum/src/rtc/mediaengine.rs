// Ported from LiveKit, pkg/rtc/mediaengine.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, against musivum's connection layer: the media engine
// is the layer's (`webrtc::peerconnection::MediaEngine`, `rtc`'s, which has pion's shape),
// filled with the layer's `register_codec` and `register_header_extension` where the
// original calls pion's methods of those names; a header extension is registered for all
// directions, as pion's call without directions does.
// - `createMediaEngine` returns the engine by value, where the original returns a pointer.
// - A list of codecs of the protocol (`[]*livekit.Codec`) is a slice of codecs; a nil codec
//   in it, on which the original would panic, cannot be.
// - `codecToRegister` embeds the codec parameters: they are its field
//   `rtp_codec_parameters`.
// - `strings.EqualFold` is the function `equal_fold` at the end, for the one place where
//   the original calls it.
// - The functions the rest of the package calls are `pub`, where they are unexported in Go.
// - `mergeCodecsByMime` returns copies of the codecs, where the original returns the
//   pointers it was given.

use crate::protocol::codecs::mime;
use crate::protocol::codecs::{
    FLEX_FEC03_CODEC_PARAMETERS, H264_HIGH_PROFILE_FMTP,
    H264_PROFILE_LEVEL_ID4D001F_PACKETIZATION_MODE0_CODEC_PARAMETERS,
    H264_PROFILE_LEVEL_ID4D001F_PACKETIZATION_MODE1_CODEC_PARAMETERS,
    H264_PROFILE_LEVEL_ID42001F_PACKETIZATION_MODE0_CODEC_PARAMETERS,
    H264_PROFILE_LEVEL_ID42001F_PACKETIZATION_MODE1_CODEC_PARAMETERS, OPUS_CODEC_PARAMETERS,
    PCMA_CODEC_PARAMETERS, PCMU_CODEC_PARAMETERS, RED_CODEC_PARAMETERS, VIDEO_CODECS_PARAMETERS,
    VIDEO_RTX_CODEC_PARAMETERS,
};
use crate::protocol::livekit;
use crate::rtc::Error;
use crate::rtc::{DirectionConfig, RtcpFeedbackConfig, RtpHeaderExtensionConfig};
use crate::webrtc::peerconnection::{MediaEngine, register_codec, register_header_extension};
use crate::webrtc::{RtpCodecCapability, RtpCodecParameters, RtpCodecType};

#[derive(Clone)]
struct CodecToRegister {
    rtp_codec_parameters: RtpCodecParameters,
    strict_fmtp: bool,
}

fn register_codecs(
    me: &mut MediaEngine,
    codecs: &[livekit::Codec],
    rtcp_feedback: &RtcpFeedbackConfig,
    filter_out_h264_high_profile: bool,
) -> Result<(), Error> {
    // audio codecs
    if is_codec_enabled(codecs, &OPUS_CODEC_PARAMETERS.rtp_codec_capability) {
        let mut cp = OPUS_CODEC_PARAMETERS.clone();
        cp.rtp_codec_capability.rtcp_feedback = rtcp_feedback.audio.clone();
        register_codec(me, &cp, RtpCodecType::Audio)?;

        if is_codec_enabled(codecs, &RED_CODEC_PARAMETERS.rtp_codec_capability) {
            register_codec(me, &RED_CODEC_PARAMETERS, RtpCodecType::Audio)?;
        }
    }

    for codec in [&*PCMU_CODEC_PARAMETERS, &*PCMA_CODEC_PARAMETERS] {
        if !is_codec_enabled(codecs, &codec.rtp_codec_capability) {
            continue;
        }

        let mut cp = codec.clone();
        cp.rtp_codec_capability.rtcp_feedback = rtcp_feedback.audio.clone();
        register_codec(me, &cp, RtpCodecType::Audio)?;
    }

    // video codecs
    let rtx_enabled = is_codec_enabled(codecs, &VIDEO_RTX_CODEC_PARAMETERS.rtp_codec_capability);

    let mut codecs_to_register = Vec::new();
    for codec in VIDEO_CODECS_PARAMETERS.iter() {
        codecs_to_register.push(CodecToRegister {
            rtp_codec_parameters: codec.clone(),
            strict_fmtp: false,
        });
    }
    codecs_to_register.extend([
        CodecToRegister {
            rtp_codec_parameters: H264_PROFILE_LEVEL_ID42001F_PACKETIZATION_MODE0_CODEC_PARAMETERS
                .clone(),
            strict_fmtp: true,
        },
        CodecToRegister {
            rtp_codec_parameters: H264_PROFILE_LEVEL_ID42001F_PACKETIZATION_MODE1_CODEC_PARAMETERS
                .clone(),
            strict_fmtp: true,
        },
        CodecToRegister {
            rtp_codec_parameters: H264_PROFILE_LEVEL_ID4D001F_PACKETIZATION_MODE0_CODEC_PARAMETERS
                .clone(),
            strict_fmtp: true,
        },
        CodecToRegister {
            rtp_codec_parameters: H264_PROFILE_LEVEL_ID4D001F_PACKETIZATION_MODE1_CODEC_PARAMETERS
                .clone(),
            strict_fmtp: true,
        },
    ]);

    for codec in &codecs_to_register {
        if filter_out_h264_high_profile
            && codec.rtp_codec_parameters.sdp_fmtp_line == H264_HIGH_PROFILE_FMTP
        {
            continue;
        }
        if mime::is_mime_type_string_rtx(&codec.rtp_codec_parameters.mime_type) {
            continue;
        }
        if !is_codec_enabled_with_fmtp(
            codecs,
            &codec.rtp_codec_parameters.rtp_codec_capability,
            codec.strict_fmtp,
        ) {
            continue;
        }

        let mut cp = codec.clone();
        cp.rtp_codec_parameters.rtp_codec_capability.rtcp_feedback = rtcp_feedback.video.clone();
        register_codec(me, &cp.rtp_codec_parameters, RtpCodecType::Video)?;

        if !rtx_enabled {
            continue;
        }

        let mut cp_rtx = VIDEO_RTX_CODEC_PARAMETERS.clone();
        cp_rtx.rtp_codec_capability.sdp_fmtp_line =
            format!("apt={}", codec.rtp_codec_parameters.payload_type);
        cp_rtx.payload_type = codec.rtp_codec_parameters.payload_type.wrapping_add(1);
        register_codec(me, &cp_rtx, RtpCodecType::Video)?;
    }
    Ok(())
}

fn register_header_extensions(
    me: &mut MediaEngine,
    rtp_header_extension: &RtpHeaderExtensionConfig,
) -> Result<(), Error> {
    for extension in &rtp_header_extension.video {
        register_header_extension(me, extension, RtpCodecType::Video, None)?;
    }

    for extension in &rtp_header_extension.audio {
        register_header_extension(me, extension, RtpCodecType::Audio, None)?;
    }

    Ok(())
}

pub fn create_media_engine(
    codecs: &[livekit::Codec],
    config: &DirectionConfig,
    filter_out_h264_high_profile: bool,
) -> Result<MediaEngine, Error> {
    let mut me = MediaEngine::default();
    register_codecs(
        &mut me,
        codecs,
        &config.rtcp_feedback,
        filter_out_h264_high_profile,
    )?;

    if config.flex_fec.enabled {
        // registering a flexfec codec makes pion allocate FEC SSRCs for video
        // senders and emit a=ssrc-group:FEC-FR in offers, and lets answers
        // accept flexfec offered by publishers
        register_codec(&mut me, &FLEX_FEC03_CODEC_PARAMETERS, RtpCodecType::Video)?;
    }

    register_header_extensions(&mut me, &config.rtp_header_extension)?;

    Ok(me)
}

pub fn is_codec_enabled(codecs: &[livekit::Codec], cap: &RtpCodecCapability) -> bool {
    is_codec_enabled_with_fmtp(codecs, cap, false)
}

fn is_codec_enabled_with_fmtp(
    codecs: &[livekit::Codec],
    cap: &RtpCodecCapability,
    strict_fmtp: bool,
) -> bool {
    for codec in codecs {
        if !mime::is_mime_type_string_equal(&codec.mime, &cap.mime_type) {
            continue;
        }
        if (!strict_fmtp && codec.fmtp_line.is_empty())
            || equal_fold(&codec.fmtp_line, &cap.sdp_fmtp_line)
        {
            return true;
        }
    }
    false
}

pub fn select_alternative_video_codec(enabled_codecs: &[livekit::Codec]) -> String {
    for c in enabled_codecs {
        // RTX and FEC are not real codecs, skip them
        if mime::is_mime_type_string_rtx(&c.mime) || mime::is_mime_type_string_flex_fec03(&c.mime) {
            continue;
        }
        if mime::is_mime_type_string_video(&c.mime) {
            return c.mime.clone();
        }
    }
    // no viable codec in the list of enabled codecs, fall back to the most widely supported codec
    mime::MimeType::Vp8.to_string()
}

pub fn select_alternative_audio_codec(enabled_codecs: &[livekit::Codec]) -> String {
    for c in enabled_codecs {
        if mime::is_mime_type_string_audio(&c.mime) {
            return c.mime.clone();
        }
    }
    // no viable codec in the list of enabled codecs, fall back to the most widely supported codec
    mime::MimeType::Opus.to_string()
}

// mergeCodecsByMime returns a union b, deduplicated by mime type.
pub fn merge_codecs_by_mime(a: &[livekit::Codec], b: &[livekit::Codec]) -> Vec<livekit::Codec> {
    let mut merged = Vec::with_capacity(a.len() + b.len());
    merged.extend_from_slice(a);
    for c in b {
        let mut seen = false;
        for existing in a {
            if mime::is_mime_type_string_equal(&c.mime, &existing.mime) {
                seen = true;
                break;
            }
        }
        if !seen {
            merged.push(c.clone());
        }
    }
    merged
}

pub fn filter_codecs(
    codecs: &[RtpCodecParameters],
    enabled_codecs: &[livekit::Codec],
    rtcp_feedback_config: &RtcpFeedbackConfig,
    filter_out_h264_high_profile: bool,
    enable_flex_fec: bool,
) -> Vec<RtpCodecParameters> {
    let mut filtered_codecs = Vec::with_capacity(codecs.len());
    for c in codecs {
        let mut c = c.clone();
        if filter_out_h264_high_profile
            && is_h264_high_profile(&c.rtp_codec_capability.sdp_fmtp_line)
        {
            continue;
        }

        // flexfec-03 is not part of the enabled codec lists, retain it when
        // the transport direction has FlexFEC enabled
        if mime::is_mime_type_string_flex_fec03(&c.rtp_codec_capability.mime_type) {
            if enable_flex_fec {
                filtered_codecs.push(c);
            }
            continue;
        }

        for enabled_codec in enabled_codecs {
            if mime::normalize_mime_type(&enabled_codec.mime)
                == mime::normalize_mime_type(&c.rtp_codec_capability.mime_type)
            {
                if !mime::is_mime_type_string_equal(
                    &c.rtp_codec_capability.mime_type,
                    mime::MimeType::Rtx.as_str(),
                ) {
                    if mime::is_mime_type_string_video(&c.rtp_codec_capability.mime_type) {
                        c.rtp_codec_capability.rtcp_feedback = rtcp_feedback_config.video.clone();
                    } else {
                        c.rtp_codec_capability.rtcp_feedback = rtcp_feedback_config.audio.clone();
                    }
                }
                filtered_codecs.push(c);
                break;
            }
        }
    }
    filtered_codecs
}

fn is_h264_high_profile(fmtp: &str) -> bool {
    let params = fmtp.split(';');
    for param in params {
        let parts: Vec<&str> = param.split('=').collect();
        if parts.len() == 2 && parts[0] == "profile-level-id" {
            // https://datatracker.ietf.org/doc/html/rfc6184#section-8.1
            // hex value 0x64 for profile_idc is high profile
            return parts[1].starts_with("64");
        }
    }

    false
}

// Go's strings.EqualFold(s, t) for a `t` that is ASCII, as the fmtp lines of the server's
// codecs are, which are what a client's line is compared with: whether the two are equal
// under Unicode's simple case folding. That folding takes, next to the ASCII letters in
// their other case, two more characters to an ASCII letter: 'ſ' (U+017F) to 's' and the
// Kelvin sign (U+212A) to 'k'.
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
#[path = "mediaengine_test.rs"]
mod mediaengine_test;
