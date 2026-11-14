// Ported from LiveKit, pkg/rtc/transport_fec_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The offer of `TestMediaEngineRegistersFlexFEC` is made
// by a connection of musivum's layer on a node of its own, where the original uses pion's API.
// The subtests are blocks that carry their names in the messages of their assertions.

use std::collections::HashMap;
use std::io::Cursor;

use super::transport_test::{codec, new_peer_connection_with, sendrecv};
use super::*;
use crate::protocol::codecs::{
    FLEX_FEC03_CODEC_PARAMETERS, OPUS_CODEC_PARAMETERS, PCMA_CODEC_PARAMETERS,
    PCMU_CODEC_PARAMETERS, RED_CODEC_PARAMETERS, VIDEO_CODECS_PARAMETERS,
};
use crate::protocol::logger;
use crate::rtc::FlexFecDirectionConfig;

fn parse(offer: &str) -> sdp::SessionDescription {
    sdp::SessionDescription::unmarshal(&mut Cursor::new(offer.as_bytes())).unwrap()
}

fn ssrc_group(value: Option<&str>) -> sdp::Attribute {
    sdp::Attribute {
        key: sdp::ATTR_KEY_SSRC_GROUP.to_string(),
        value: value.map(str::to_string),
    }
}

#[test]
fn test_fec_pairs_from_sdp() {
    let offer = "v=0
o=- 8541913762120318441 2 IN IP4 127.0.0.1
s=-
t=0 0
m=video 9 UDP/TLS/RTP/SAVPF 96 97 115
c=IN IP4 0.0.0.0
a=mid:0
a=sendonly
a=rtpmap:96 VP8/90000
a=rtpmap:97 rtx/90000
a=fmtp:97 apt=96
a=rtpmap:115 flexfec-03/90000
a=fmtp:115 repair-window=10000000
a=ssrc-group:FID 1111 2222
a=ssrc-group:FEC-FR 1111 3333
a=ssrc:1111 cname:test
a=ssrc:2222 cname:test
a=ssrc:3333 cname:test
";
    let parsed = parse(offer);

    let fec_pairs = fec_pairs_from_sdp(&parsed, &logger::get_logger());
    assert_eq!(1, fec_pairs.len());
    assert_eq!(Some(&1111), fec_pairs.get(&3333));

    // FID pairs are not picked up as FEC
    let rtx_pairs = non_simulcast_rtx_repairs_from_sdp(&parsed, &logger::get_logger());
    assert_eq!(1, rtx_pairs.len());
    assert_eq!(Some(&1111), rtx_pairs.get(&2222));
}

#[test]
fn test_fec_pairs_from_sdp_no_groups() {
    let offer = "v=0
o=- 8541913762120318441 2 IN IP4 127.0.0.1
s=-
t=0 0
m=video 9 UDP/TLS/RTP/SAVPF 96
c=IN IP4 0.0.0.0
a=mid:0
a=sendonly
a=rtpmap:96 VP8/90000
a=ssrc:1111 cname:test
";
    let parsed = parse(offer);
    assert!(fec_pairs_from_sdp(&parsed, &logger::get_logger()).is_empty());
}

#[test]
fn test_fec_pairs_from_sdp_ignores_malformed_groups() {
    let description = sdp::SessionDescription {
        media_descriptions: vec![sdp::MediaDescription {
            attributes: vec![
                ssrc_group(None),
                ssrc_group(Some("FEC-FR 1111")),
                ssrc_group(Some("FEC-FR invalid 3333")),
                ssrc_group(Some("FEC-FR 1111 invalid")),
                ssrc_group(Some("FEC-FR 1111 3333 4444")),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };

    // `require.NotPanics`: a panic fails the test
    assert!(fec_pairs_from_sdp(&description, &logger::get_logger()).is_empty());
}

#[test]
fn test_fec_pairs_from_sdp_handles_whitespace() {
    let description = sdp::SessionDescription {
        media_descriptions: vec![sdp::MediaDescription {
            attributes: vec![ssrc_group(Some("  FEC-FR   1111\t3333  "))],
            ..Default::default()
        }],
        ..Default::default()
    };

    assert_eq!(
        HashMap::from([(3333, 1111)]),
        fec_pairs_from_sdp(&description, &logger::get_logger())
    );
}

#[test]
fn test_flex_fec_payload_type_does_not_collide() {
    let flex_fec_payload_type = FLEX_FEC03_CODEC_PARAMETERS.payload_type;
    assert!(flex_fec_payload_type <= 127);
    for codec in VIDEO_CODECS_PARAMETERS.iter() {
        assert_ne!(
            codec.payload_type, flex_fec_payload_type,
            "{}",
            codec.rtp_codec_capability.mime_type
        );
        assert_ne!(
            codec.payload_type.wrapping_add(1),
            flex_fec_payload_type,
            "{} RTX",
            codec.rtp_codec_capability.mime_type
        );
    }
    for codec in [
        &*OPUS_CODEC_PARAMETERS,
        &*RED_CODEC_PARAMETERS,
        &*PCMU_CODEC_PARAMETERS,
        &*PCMA_CODEC_PARAMETERS,
    ] {
        assert_ne!(
            codec.payload_type, flex_fec_payload_type,
            "{}",
            codec.rtp_codec_capability.mime_type
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_media_engine_registers_flex_fec() {
    let enabled_codecs = vec![codec("video/VP8"), codec("video/rtx")];

    for (name, enabled) in [("disabled", false), ("enabled", true)] {
        let me = create_media_engine(
            &enabled_codecs,
            &DirectionConfig {
                flex_fec: FlexFecDirectionConfig { enabled },
                rtcp_feedback: RtcpFeedbackConfig {
                    video: vec![webrtc::RtcpFeedback {
                        r#type: webrtc::TYPE_RTCP_FB_TRANSPORT_CC.to_string(),
                        parameter: String::new(),
                    }],
                    ..Default::default()
                },
                ..Default::default()
            },
            false,
        )
        .unwrap();

        let pc = new_peer_connection_with(me);
        pc.add_transceiver_from_kind(RtpCodecType::Video, Some(sendrecv()))
            .unwrap();
        let offer = pc.create_offer(None).unwrap();

        let flex_fec_params = FLEX_FEC03_CODEC_PARAMETERS.clone();
        assert_eq!(
            "repair-window=10000000", flex_fec_params.rtp_codec_capability.sdp_fmtp_line,
            "{name}"
        );
        let filtered = filter_codecs(
            &[flex_fec_params],
            &enabled_codecs,
            &RtcpFeedbackConfig::default(),
            false,
            enabled,
        );
        if enabled {
            assert_eq!(1, filtered.len(), "{name}");
            assert_eq!(
                webrtc::MIME_TYPE_FLEX_FEC03,
                filtered[0].rtp_codec_capability.mime_type,
                "{name}"
            );
            assert!(
                offer.sdp.contains("a=rtpmap:115 flexfec-03/90000"),
                "{name}: {}",
                offer.sdp
            );
            assert!(
                offer.sdp.contains("a=fmtp:115 repair-window=10000000"),
                "{name}: {}",
                offer.sdp
            );
        } else {
            assert!(filtered.is_empty(), "{name}");
            assert!(!offer.sdp.contains("flexfec-03"), "{name}: {}", offer.sdp);
        }
        let _ = pc.close();
    }
}

#[test]
fn test_web_rtc_config_flex_fec() {
    let new_config = || {
        let mut conf = config::Config::new("", true).unwrap();
        conf.rtc.rtc_config.tcp_port = 0;
        conf
    };

    // "defaults and publisher updates"
    {
        let mut conf = new_config();
        conf.rtc.flex_fec = config::FlexFecConfig {
            upstream_enabled: true,
        };

        let mut web_rtc_config = WebRtcConfig::new(&conf).unwrap();
        assert_eq!(
            FlexFecDirectionConfig { enabled: true },
            web_rtc_config.publisher.flex_fec
        );
        assert!(!web_rtc_config.subscriber.flex_fec.enabled);

        web_rtc_config.update_publisher_config(true);
        assert!(web_rtc_config.publisher.flex_fec.enabled);
    }
}
