// Ported from LiveKit, pkg/rtc/mediaengine_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The offer is made by a connection of musivum's layer
// on a node of its own (on the loopback interface, a port of the system's choosing), where
// the original makes it with pion's API and no node. The subtests of a test are blocks that
// carry their names in the messages of their assertions.

use std::collections::HashMap;
use std::fmt;

use super::*;
use crate::config;
use crate::webrtc::peerconnection::{Configuration, Node, NodeConfig, PeerConnection};
use crate::webrtc::{PayloadType, RtpCodecCapability};

const H264_MAIN_PROFILE_PACKETIZATION_MODE0_FMTP: &str =
    "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=4d001f";
const H264_MAIN_PROFILE_PACKETIZATION_MODE1_FMTP: &str =
    "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=4d001f";
const H264_BASELINE_PROFILE_PACKETIZATION_MODE0_FMTP: &str =
    "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42001f";
const H264_BASELINE_PROFILE_PACKETIZATION_MODE1_FMTP: &str =
    "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f";

fn codec(mime: &str, fmtp_line: &str) -> livekit::Codec {
    livekit::Codec {
        mime: mime.to_string(),
        fmtp_line: fmtp_line.to_string(),
    }
}

fn cap(mime_type: mime::MimeType, sdp_fmtp_line: &str) -> RtpCodecCapability {
    RtpCodecCapability {
        mime_type: mime_type.to_string(),
        sdp_fmtp_line: sdp_fmtp_line.to_string(),
        ..Default::default()
    }
}

#[test]
fn test_is_codec_enabled() {
    {
        // empty fmtp requirement should match all
        let enabled_codecs = [codec("video/h264", "")];
        assert!(is_codec_enabled(
            &enabled_codecs,
            &cap(mime::MimeType::H264, "special")
        ));
        assert!(is_codec_enabled(
            &enabled_codecs,
            &cap(mime::MimeType::H264, "")
        ));
        assert!(!is_codec_enabled(
            &enabled_codecs,
            &cap(mime::MimeType::Vp8, "")
        ));
    }

    {
        // when fmtp is provided, require match
        let enabled_codecs = [codec("video/h264", "special")];
        assert!(is_codec_enabled(
            &enabled_codecs,
            &cap(mime::MimeType::H264, "special")
        ));
        assert!(!is_codec_enabled(
            &enabled_codecs,
            &cap(mime::MimeType::H264, "")
        ));
        assert!(!is_codec_enabled(
            &enabled_codecs,
            &cap(mime::MimeType::Vp8, "")
        ));
    }

    {
        // strict fmtp requires an exact fmtp, mime alone is not enough
        let cap = cap(
            mime::MimeType::H264,
            H264_MAIN_PROFILE_PACKETIZATION_MODE1_FMTP,
        );

        // mime-only config opts into every non-strict codec, but never a strict one
        let mime_only = [codec("video/h264", "")];
        assert!(is_codec_enabled_with_fmtp(&mime_only, &cap, false));
        assert!(!is_codec_enabled_with_fmtp(&mime_only, &cap, true));

        // spelling the fmtp out enables it
        let explicit = [codec(
            "video/h264",
            H264_MAIN_PROFILE_PACKETIZATION_MODE1_FMTP,
        )];
        assert!(is_codec_enabled_with_fmtp(&explicit, &cap, true));

        // ...but only for the profile that was asked for
        assert!(!is_codec_enabled_with_fmtp(
            &explicit,
            &RtpCodecCapability {
                mime_type: mime::MimeType::H264.to_string(),
                sdp_fmtp_line: H264_MAIN_PROFILE_PACKETIZATION_MODE0_FMTP.to_string(),
                ..Default::default()
            },
            true
        ));
    }
}

#[test]
fn test_select_alternative_video_codec() {
    // RTX and FEC have the video/ prefix but are not real codecs. The fallback must skip them.
    {
        // skips rtx and fec, picks real codec
        let enabled = [
            codec(mime::MimeType::Rtx.as_str(), ""),
            codec(mime::MimeType::FlexFec03.as_str(), ""),
            codec(mime::MimeType::H264.as_str(), ""),
        ];
        assert_eq!(
            mime::MimeType::H264.to_string(),
            select_alternative_video_codec(&enabled),
            "skips rtx and fec, picks real codec"
        );
    }

    {
        // falls back to vp8 when only rtx is enabled
        let enabled = [codec(mime::MimeType::Rtx.as_str(), "")];
        assert_eq!(
            mime::MimeType::Vp8.to_string(),
            select_alternative_video_codec(&enabled),
            "falls back to vp8 when only rtx is enabled"
        );
    }
}

#[derive(Clone, PartialEq, Eq)]
struct OfferedCodec {
    payload_type: PayloadType,
    name: String,
    fmtp: String,
}

impl fmt::Debug for OfferedCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.payload_type, self.name, self.fmtp)
    }
}

// offeredVideoCodecs returns the video codecs an offer built from enabledCodecs carries.
// It goes through a real offer rather than inspecting the MediaEngine so that payload
// type assignment, which is what collides, is covered too.
#[track_caller]
fn offered_video_codecs(
    enabled_codecs: &[livekit::Codec],
    filter_out_h264_high_profile: bool,
) -> Vec<OfferedCodec> {
    let me = create_media_engine(
        enabled_codecs,
        &DirectionConfig::default(),
        filter_out_h264_high_profile,
    )
    .unwrap();

    let node = Node::new(NodeConfig::default()).unwrap();
    let pc = PeerConnection::new(
        &node,
        Configuration {
            media_engine: me,
            ..Default::default()
        },
    )
    .unwrap();

    pc.add_transceiver_from_kind(RtpCodecType::Video, None)
        .unwrap();

    let offer = pc.create_offer(None).unwrap();
    // `defer pc.Close()`
    pc.close().unwrap();

    let mut codecs: Vec<OfferedCodec> = Vec::new();
    let mut index_by_payload_type: HashMap<PayloadType, usize> = HashMap::new();
    for line in offer.sdp.split("\r\n") {
        let Some((attr, value)) = line.split_once(':') else {
            continue;
        };
        if attr != "a=rtpmap" && attr != "a=fmtp" {
            continue;
        }

        let (raw_payload_type, rest) = value.split_once(' ').unwrap_or((value, ""));
        let payload_type: i64 = raw_payload_type.parse().unwrap();

        if attr == "a=rtpmap" {
            index_by_payload_type.insert(payload_type as PayloadType, codecs.len());
            codecs.push(OfferedCodec {
                payload_type: payload_type as PayloadType,
                name: rest.to_string(),
                fmtp: String::new(),
            });
            continue;
        }

        let i = index_by_payload_type.get(&(payload_type as PayloadType));
        let Some(&i) = i else {
            panic!("fmtp for payload type {payload_type} without an rtpmap");
        };
        codecs[i].fmtp = rest.to_string();
    }

    assert!(!codecs.is_empty());
    codecs
}

fn has_offered_fmtp(codecs: &[OfferedCodec], mime_type: mime::MimeType, fmtp: &str) -> bool {
    for c in codecs {
        let name = c
            .name
            .split_once('/')
            .map_or(c.name.as_str(), |(name, _)| name);
        if mime::normalize_mime_type(&format!("video/{name}")) == mime_type && c.fmtp == fmtp {
            return true;
        }
    }
    false
}

// defaultEnabledCodecs mirrors what roomallocator derives from config, so these tests
// track the shipped default codec set instead of a hand-maintained copy of it.
fn default_enabled_codecs() -> Vec<livekit::Codec> {
    let mut codecs = Vec::with_capacity(config::DEFAULT_CONFIG.room.enabled_codecs.len());
    for c in &config::DEFAULT_CONFIG.room.enabled_codecs {
        codecs.push(codec(&c.mime, &c.fmtp_line));
    }
    codecs
}

#[tokio::test(flavor = "multi_thread")]
async fn test_register_codecs_h264_profiles() {
    {
        let name = "constrained baseline are offered by default";
        let codecs = offered_video_codecs(&default_enabled_codecs(), false);

        for fmtp in [
            "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f",
            "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
            "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640032",
        ] {
            assert!(
                has_offered_fmtp(&codecs, mime::MimeType::H264, fmtp),
                "{name}: missing {fmtp:?} in {codecs:?}"
            );
        }
    }

    {
        let name = "main/baseline profile is not offered unless explicitly enabled";
        let codecs = offered_video_codecs(&default_enabled_codecs(), false);

        for fmtp in [
            H264_MAIN_PROFILE_PACKETIZATION_MODE0_FMTP,
            H264_MAIN_PROFILE_PACKETIZATION_MODE1_FMTP,
            H264_BASELINE_PROFILE_PACKETIZATION_MODE0_FMTP,
            H264_BASELINE_PROFILE_PACKETIZATION_MODE1_FMTP,
        ] {
            assert!(
                !has_offered_fmtp(&codecs, mime::MimeType::H264, fmtp),
                "{name}: {codecs:?}"
            );
        }
    }

    {
        let name = "main/baseline profile is offered when explicitly enabled";
        for fmtp in [
            H264_MAIN_PROFILE_PACKETIZATION_MODE0_FMTP,
            H264_MAIN_PROFILE_PACKETIZATION_MODE1_FMTP,
            H264_BASELINE_PROFILE_PACKETIZATION_MODE0_FMTP,
            H264_BASELINE_PROFILE_PACKETIZATION_MODE1_FMTP,
        ] {
            let mut enabled = default_enabled_codecs();
            enabled.push(codec(mime::MimeType::H264.as_str(), fmtp));

            let codecs = offered_video_codecs(&enabled, false);
            assert!(
                has_offered_fmtp(&codecs, mime::MimeType::H264, fmtp),
                "{name}/{fmtp}: missing {fmtp:?} in {codecs:?}"
            );
        }
    }

    {
        let name = "high profile is filtered out for the answerer";
        let high_profile_fmtp =
            "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640032";

        let codecs = offered_video_codecs(&default_enabled_codecs(), true);
        assert!(
            !has_offered_fmtp(&codecs, mime::MimeType::H264, high_profile_fmtp),
            "{name}: {codecs:?}"
        );

        // the other profiles survive the filter
        assert!(
            has_offered_fmtp(
                &codecs,
                mime::MimeType::H264,
                "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
            ),
            "{name}: {codecs:?}"
        );
    }
}

// Every video codec claims its own payload type plus the next one for its RTX stream,
// so a codec whose payload type lands on another's RTX slot makes pion refuse to build
// the MediaEngine at all.
#[tokio::test(flavor = "multi_thread")]
async fn test_register_codecs_payload_types_are_unique() {
    let mut enabled = default_enabled_codecs();
    enabled.extend([
        codec(
            mime::MimeType::H264.as_str(),
            H264_MAIN_PROFILE_PACKETIZATION_MODE0_FMTP,
        ),
        codec(
            mime::MimeType::H264.as_str(),
            H264_MAIN_PROFILE_PACKETIZATION_MODE1_FMTP,
        ),
    ]);

    for filter_out_h264_high_profile in [false, true] {
        let codecs = offered_video_codecs(&enabled, filter_out_h264_high_profile);

        let mut seen: HashMap<PayloadType, OfferedCodec> = HashMap::with_capacity(codecs.len());
        for c in codecs {
            if let Some(previous) = seen.get(&c.payload_type) {
                panic!(
                    "payload type {} claimed by both {previous:?} and {c:?}",
                    c.payload_type
                );
            }
            seen.insert(c.payload_type, c);
        }
    }
}
