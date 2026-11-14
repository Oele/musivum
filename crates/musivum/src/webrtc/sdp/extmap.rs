// Ported from pion/sdp, extmap.go at v3.0.20.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; only the names of the header extensions are here.

//! The constants of `pion/sdp` that `pkg/sfu` uses.

pub const ABS_SEND_TIME_URI: &str = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time";
pub const TRANSPORT_CC_URI: &str =
    "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01";
pub const SDES_MID_URI: &str = "urn:ietf:params:rtp-hdrext:sdes:mid";
pub const SDES_RTP_STREAM_ID_URI: &str = "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id";
pub const SDES_REPAIR_RTP_STREAM_ID_URI: &str =
    "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id";
pub const AUDIO_LEVEL_URI: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
