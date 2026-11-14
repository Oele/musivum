// Ported from pion/webrtc (as LiveKit builds it: livekit/webrtc-pion v4.2.18-warp.7),
// rtpcodec.go, rtcpfeedback.go, rtpcodingparameters.go, mimetype.go and webrtc.go.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; only the types and constants that LiveKit's code names
// are here, not pion's codec matching. `RTPCodecParameters` embeds `RTPCodecCapability` in Go:
// here it holds one and dereferences to it, so that `codec.mime_type` reads the same.

use std::fmt;
use std::ops::{Deref, DerefMut};

/// SSRC represents a synchronization source
/// A synchronization source is a randomly chosen
/// value meant to be globally unique within a particular
/// RTP session. Used to identify a single stream of media.
///
/// https://tools.ietf.org/html/rfc3550#section-3
pub type Ssrc = u32;

/// PayloadType identifies the format of the RTP payload and determines
/// its interpretation by the application. Each codec in a RTP Session
/// will have a different PayloadType
///
/// https://tools.ietf.org/html/rfc3550#section-3
pub type PayloadType = u8;

/// MimeTypeH264 H264 MIME type.
pub const MIME_TYPE_H264: &str = "video/H264";
/// MimeTypeH265 H265 MIME type.
pub const MIME_TYPE_H265: &str = "video/H265";
/// MimeTypeOpus Opus MIME type.
pub const MIME_TYPE_OPUS: &str = "audio/opus";
/// MimeTypeVP8 VP8 MIME type.
pub const MIME_TYPE_VP8: &str = "video/VP8";
/// MimeTypeVP9 VP9 MIME type.
pub const MIME_TYPE_VP9: &str = "video/VP9";
/// MimeTypeAV1 AV1 MIME type.
pub const MIME_TYPE_AV1: &str = "video/AV1";
/// MimeTypeG722 G722 MIME type.
pub const MIME_TYPE_G722: &str = "audio/G722";
/// MimeTypePCMU PCMU MIME type.
pub const MIME_TYPE_PCMU: &str = "audio/PCMU";
/// MimeTypePCMA PCMA MIME type.
pub const MIME_TYPE_PCMA: &str = "audio/PCMA";
/// MimeTypeRTX RTX MIME type.
pub const MIME_TYPE_RTX: &str = "video/rtx";
/// MimeTypeFlexFEC FEC MIME Type.
pub const MIME_TYPE_FLEX_FEC: &str = "video/flexfec";
/// MimeTypeFlexFEC03 FlexFEC03 MIME Type.
pub const MIME_TYPE_FLEX_FEC03: &str = "video/flexfec-03";
/// MimeTypeUlpFEC UlpFEC MIME Type.
pub const MIME_TYPE_ULP_FEC: &str = "video/ulpfec";

pub const TYPE_RTCP_FB_TRANSPORT_CC: &str = "transport-cc";
pub const TYPE_RTCP_FB_GOOG_REMB: &str = "goog-remb";
pub const TYPE_RTCP_FB_ACK: &str = "ack";
pub const TYPE_RTCP_FB_CCM: &str = "ccm";
pub const TYPE_RTCP_FB_NACK: &str = "nack";

/// RTCPFeedback signals the connection to use additional RTCP packet types.
/// https://draft.ortc.org/#dom-rtcrtcpfeedback
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RtcpFeedback {
    /// Type is the type of feedback.
    /// see: https://draft.ortc.org/#dom-rtcrtcpfeedback
    /// valid: ack, ccm, nack, goog-remb, transport-cc
    pub r#type: String,

    /// The parameter value depends on the type.
    /// For example, type="nack" parameter="pli" will send Picture Loss Indicator packets.
    pub parameter: String,
}

/// RTPCodecType determines the type of a codec.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum RtpCodecType {
    /// RTPCodecTypeUnknown is the enum's zero-value.
    #[default]
    Unknown,
    /// RTPCodecTypeAudio indicates this is an audio codec.
    Audio,
    /// RTPCodecTypeVideo indicates this is a video codec.
    Video,
}

impl fmt::Display for RtpCodecType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RtpCodecType::Audio => "audio",
            RtpCodecType::Video => "video",
            RtpCodecType::Unknown => "unknown",
        })
    }
}

impl RtpCodecType {
    /// NewRTPCodecType creates a RTPCodecType from a string.
    pub fn new(r: &str) -> RtpCodecType {
        if r.eq_ignore_ascii_case("audio") {
            RtpCodecType::Audio
        } else if r.eq_ignore_ascii_case("video") {
            RtpCodecType::Video
        } else {
            RtpCodecType::Unknown
        }
    }
}

/// RTPCodecCapability provides information about codec capabilities.
///
/// https://w3c.github.io/webrtc-pc/#dictionary-rtcrtpcodeccapability-members
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RtpCodecCapability {
    pub mime_type: String,
    pub clock_rate: u32,
    pub channels: u16,
    pub sdp_fmtp_line: String,
    pub rtcp_feedback: Vec<RtcpFeedback>,
}

/// RTPHeaderExtensionCapability is used to define a RFC5285 RTP header extension supported by the codec.
///
/// https://w3c.github.io/webrtc-pc/#dom-rtcrtpcapabilities-headerextensions
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RtpHeaderExtensionCapability {
    pub uri: String,
}

/// RTPHeaderExtensionParameter represents a negotiated RFC5285 RTP header extension.
///
/// https://w3c.github.io/webrtc-pc/#dictionary-rtcrtpheaderextensionparameters-members
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RtpHeaderExtensionParameter {
    pub uri: String,
    pub id: i32,
}

/// RTPCodecParameters is a sequence containing the media codecs that an RtpSender
/// will choose from, as well as entries for RTX, RED and FEC mechanisms. This also
/// includes the PayloadType that has been negotiated
///
/// https://w3c.github.io/webrtc-pc/#rtcrtpcodecparameters
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RtpCodecParameters {
    pub rtp_codec_capability: RtpCodecCapability,
    pub payload_type: PayloadType,
}

impl Deref for RtpCodecParameters {
    type Target = RtpCodecCapability;
    fn deref(&self) -> &RtpCodecCapability {
        &self.rtp_codec_capability
    }
}

impl DerefMut for RtpCodecParameters {
    fn deref_mut(&mut self) -> &mut RtpCodecCapability {
        &mut self.rtp_codec_capability
    }
}

/// RTPParameters is a list of negotiated codecs and header extensions
///
/// https://w3c.github.io/webrtc-pc/#dictionary-rtcrtpparameters-members
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RtpParameters {
    pub header_extensions: Vec<RtpHeaderExtensionParameter>,
    pub codecs: Vec<RtpCodecParameters>,
}

/// RTPRtxParameters dictionary contains information relating to retransmission (RTX) settings.
/// https://draft.ortc.org/#dom-rtcrtprtxparameters
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RtpRtxParameters {
    pub ssrc: Ssrc,
}

/// RTPFecParameters dictionary contains information relating to forward error correction (FEC) settings.
/// https://draft.ortc.org/#dom-rtcrtpfecparameters
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RtpFecParameters {
    pub ssrc: Ssrc,
}

/// RTPCodingParameters provides information relating to both encoding and decoding.
/// This is a subset of the RFC since Pion WebRTC doesn't implement encoding/decoding itself
/// http://draft.ortc.org/#dom-rtcrtpcodingparameters
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RtpCodingParameters {
    pub rid: String,
    pub ssrc: Ssrc,
    pub payload_type: PayloadType,
    pub rtx: RtpRtxParameters,
    pub fec: RtpFecParameters,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_parameters_read_like_the_embedded_struct() {
        let mut codec = RtpCodecParameters {
            rtp_codec_capability: RtpCodecCapability {
                mime_type: MIME_TYPE_VP8.to_string(),
                clock_rate: 90000,
                ..Default::default()
            },
            payload_type: 96,
        };
        assert_eq!(codec.mime_type, "video/VP8");
        codec.channels = 2;
        assert_eq!(codec.rtp_codec_capability.channels, 2);
        assert_eq!(RtpCodecType::new("VIDEO"), RtpCodecType::Video);
        assert_eq!(RtpCodecType::Audio.to_string(), "audio");
        assert_eq!(RtpCodecType::new("text"), RtpCodecType::Unknown);
    }
}
