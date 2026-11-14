// Ported from LiveKit, pkg/sfu/utils/helpers.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are in the enum of the package (`Error`, in
// rangemap.rs), with pion's `webrtc.ErrCodecNotFound` among them; the codec that is looked
// for and the header extension are taken by reference; the id of a header extension is an
// `i32`, as it is in `webrtc::RtpHeaderExtensionParameter`.

use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::webrtc;
use crate::webrtc::interceptor;
use crate::webrtc::rtp;

use super::rangemap::Error;

// Do a fuzzy find for a codec in the list of codecs
// Used for lookup up a codec in an existing list to find a match
pub fn codec_parameters_fuzzy_search(
    needle: &webrtc::RtpCodecParameters,
    haystack: &[webrtc::RtpCodecParameters],
) -> Result<webrtc::RtpCodecParameters, Error> {
    // First attempt to match on MimeType + SDPFmtpLine
    for c in haystack {
        if mime::is_mime_type_string_equal(
            &c.rtp_codec_capability.mime_type,
            &needle.rtp_codec_capability.mime_type,
        ) && c.rtp_codec_capability.sdp_fmtp_line == needle.rtp_codec_capability.sdp_fmtp_line
        {
            return Ok(c.clone());
        }
    }

    // Fallback to just MimeType
    for c in haystack {
        if mime::is_mime_type_string_equal(
            &c.rtp_codec_capability.mime_type,
            &needle.rtp_codec_capability.mime_type,
        ) {
            return Ok(c.clone());
        }
    }

    Err(Error::CodecNotFound)
}

// Given a CodecParameters find the RTX CodecParameters if one exists
pub fn find_rtx_payload_type(
    needle: webrtc::PayloadType,
    haystack: &[webrtc::RtpCodecParameters],
) -> webrtc::PayloadType {
    let apt_str = format!("apt={needle}");
    for c in haystack {
        if apt_str == c.sdp_fmtp_line {
            return c.payload_type;
        }
    }

    0
}

// GetHeaderExtensionID returns the ID of a header extension, or 0 if not found
pub fn get_header_extension_id(
    extensions: &[interceptor::RtpHeaderExtension],
    extension: &webrtc::RtpHeaderExtensionCapability,
) -> i32 {
    for h in extensions {
        if extension.uri == h.uri {
            return h.id;
        }
    }
    0
}

// ValidateRTPPacket checks for a valid RTP packet and returns an error if fields are incorrect
pub fn validate_rtp_packet(
    pkt: &rtp::Packet,
    expected_payload_type: u8,
    expected_ssrc: u32,
) -> Result<(), Error> {
    if pkt.version != 2 {
        return Err(Error::InvalidRtpVersion(format!(
            "expected: 2, actual: {}",
            pkt.version
        )));
    }

    if expected_payload_type != 0 && pkt.payload_type != expected_payload_type {
        return Err(Error::RtpPayloadTypeMismatch(format!(
            "expected: {}, actual: {}",
            expected_payload_type, pkt.payload_type
        )));
    }

    if expected_ssrc != 0 && pkt.ssrc != expected_ssrc {
        return Err(Error::RtpSsrcMismatch(format!(
            "expected: {}, actual: {}",
            expected_ssrc, pkt.ssrc
        )));
    }

    Ok(())
}

pub fn is_simulcast_mode(m: livekit::video_layer::Mode) -> bool {
    m == livekit::video_layer::Mode::OneSpatialLayerPerStream
        || m == livekit::video_layer::Mode::OneSpatialLayerPerStreamIncompleteRtcpSr
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codec(mime_type: &str, sdp_fmtp_line: &str, payload_type: u8) -> webrtc::RtpCodecParameters {
        webrtc::RtpCodecParameters {
            rtp_codec_capability: webrtc::RtpCodecCapability {
                mime_type: mime_type.to_string(),
                sdp_fmtp_line: sdp_fmtp_line.to_string(),
                ..Default::default()
            },
            payload_type,
        }
    }

    // New for musivum: the original has no tests. A codec is found by mime type and fmtp line
    // before it is found by mime type alone, whatever the case of the mime type.
    #[test]
    fn the_fuzzy_search_prefers_the_same_fmtp_line() {
        let haystack = [
            codec("video/VP8", "", 96),
            codec("video/H264", "profile-level-id=42e01f", 102),
            codec("video/H264", "profile-level-id=640032", 104),
            codec("video/rtx", "apt=102", 103),
        ];

        // mime type and fmtp line: the second H264, not the first
        let found = codec_parameters_fuzzy_search(
            &codec("video/h264", "profile-level-id=640032", 0),
            &haystack,
        );
        assert_eq!(Ok(haystack[2].clone()), found);

        // the fmtp line is compared as it is written
        let found = codec_parameters_fuzzy_search(
            &codec("video/H264", "PROFILE-LEVEL-ID=640032", 0),
            &haystack,
        );
        assert_eq!(Ok(haystack[1].clone()), found);

        // no such fmtp line: the first of that mime type
        let found = codec_parameters_fuzzy_search(&codec("VIDEO/H264", "", 0), &haystack);
        assert_eq!(Ok(haystack[1].clone()), found);
        let found = codec_parameters_fuzzy_search(&codec("video/vp8", "x=1", 0), &haystack);
        assert_eq!(Ok(haystack[0].clone()), found);

        let found = codec_parameters_fuzzy_search(&codec("video/VP9", "", 0), &haystack);
        assert_eq!(Err(Error::CodecNotFound), found);
        assert_eq!("codec not found", found.unwrap_err().to_string());
        let found = codec_parameters_fuzzy_search(&codec("video/VP8", "", 0), &[]);
        assert_eq!(Err(Error::CodecNotFound), found);
    }

    // New for musivum.
    #[test]
    fn the_rtx_payload_type_is_found_by_its_apt() {
        let haystack = [
            codec("video/VP8", "", 96),
            codec("video/rtx", "apt=96", 97),
            codec("video/H264", "profile-level-id=42e01f", 102),
            codec("video/rtx", "apt=102", 103),
            // not the whole line: no match
            codec("video/rtx", "apt=104;rtx-time=200", 105),
        ];

        assert_eq!(97, find_rtx_payload_type(96, &haystack));
        assert_eq!(103, find_rtx_payload_type(102, &haystack));
        assert_eq!(0, find_rtx_payload_type(104, &haystack));
        assert_eq!(0, find_rtx_payload_type(98, &haystack));
        assert_eq!(0, find_rtx_payload_type(96, &[]));
    }

    // New for musivum.
    #[test]
    fn the_id_of_a_header_extension_is_found_by_its_uri() {
        let extensions = [
            interceptor::RtpHeaderExtension {
                uri: webrtc::sdp::SDES_MID_URI.to_string(),
                id: 1,
            },
            interceptor::RtpHeaderExtension {
                uri: webrtc::sdp::TRANSPORT_CC_URI.to_string(),
                id: 5,
            },
        ];
        let capability = |uri: &str| webrtc::RtpHeaderExtensionCapability {
            uri: uri.to_string(),
        };

        assert_eq!(
            5,
            get_header_extension_id(&extensions, &capability(webrtc::sdp::TRANSPORT_CC_URI))
        );
        assert_eq!(
            1,
            get_header_extension_id(&extensions, &capability(webrtc::sdp::SDES_MID_URI))
        );
        assert_eq!(
            0,
            get_header_extension_id(&extensions, &capability(webrtc::sdp::AUDIO_LEVEL_URI))
        );
        assert_eq!(
            0,
            get_header_extension_id(&[], &capability(webrtc::sdp::SDES_MID_URI))
        );
    }

    // New for musivum. An expected payload type or SSRC of 0 is not checked, and the version
    // is checked first, then the payload type, then the SSRC.
    #[test]
    fn a_packet_is_validated_by_version_payload_type_and_ssrc() {
        let pkt = rtp::Packet {
            header: rtp::Header {
                version: 2,
                payload_type: 96,
                ssrc: 1234,
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(Ok(()), validate_rtp_packet(&pkt, 96, 1234));
        assert_eq!(Ok(()), validate_rtp_packet(&pkt, 0, 1234));
        assert_eq!(Ok(()), validate_rtp_packet(&pkt, 96, 0));
        assert_eq!(Ok(()), validate_rtp_packet(&pkt, 0, 0));

        let err = validate_rtp_packet(&pkt, 97, 1234).unwrap_err();
        assert!(matches!(err, Error::RtpPayloadTypeMismatch(_)), "{err:?}");
        assert_eq!(
            "RTP payload type mismatch, expected: 97, actual: 96",
            err.to_string()
        );

        let err = validate_rtp_packet(&pkt, 96, 5678).unwrap_err();
        assert!(matches!(err, Error::RtpSsrcMismatch(_)), "{err:?}");
        assert_eq!(
            "RTP SSRC mismatch, expected: 5678, actual: 1234",
            err.to_string()
        );

        // both wrong: the payload type is what is reported
        let err = validate_rtp_packet(&pkt, 97, 5678).unwrap_err();
        assert!(matches!(err, Error::RtpPayloadTypeMismatch(_)), "{err:?}");

        // a packet that was not parsed has version 0, and nothing else is looked at
        let mut bad = pkt.clone();
        bad.version = 0;
        let err = validate_rtp_packet(&bad, 97, 5678).unwrap_err();
        assert!(matches!(err, Error::InvalidRtpVersion(_)), "{err:?}");
        assert_eq!(
            "invalid RTP version, expected: 2, actual: 0",
            err.to_string()
        );
    }

    // New for musivum.
    #[test]
    fn simulcast_is_one_spatial_layer_per_stream() {
        use livekit::video_layer::Mode;

        assert!(is_simulcast_mode(Mode::OneSpatialLayerPerStream));
        assert!(is_simulcast_mode(
            Mode::OneSpatialLayerPerStreamIncompleteRtcpSr
        ));
        assert!(!is_simulcast_mode(Mode::MultipleSpatialLayersPerStream));
        assert!(!is_simulcast_mode(Mode::Unused));
    }
}
