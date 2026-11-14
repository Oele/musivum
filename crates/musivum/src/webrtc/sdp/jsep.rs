// Ported from pion/sdp, jsep.go at v3.0.20.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; only the names of the attributes and of the semantic
// tokens are here.

// Constants for SDP attributes used in JSEP.
pub const ATTR_KEY_CANDIDATE: &str = "candidate";
pub const ATTR_KEY_END_OF_CANDIDATES: &str = "end-of-candidates";
pub const ATTR_KEY_IDENTITY: &str = "identity";
pub const ATTR_KEY_GROUP: &str = "group";
pub const ATTR_KEY_SSRC: &str = "ssrc";
pub const ATTR_KEY_SSRC_GROUP: &str = "ssrc-group";
pub const ATTR_KEY_MSID: &str = "msid";
pub const ATTR_KEY_MSID_SEMANTIC: &str = "msid-semantic";
pub const ATTR_KEY_CONNECTION_SETUP: &str = "setup";
pub const ATTR_KEY_MID: &str = "mid";
pub const ATTR_KEY_ICE_LITE: &str = "ice-lite";
pub const ATTR_KEY_ICE_OPTIONS: &str = "ice-options";
pub const ATTR_KEY_RTCP_MUX: &str = "rtcp-mux";
pub const ATTR_KEY_RTCP_RSIZE: &str = "rtcp-rsize";
pub const ATTR_KEY_INACTIVE: &str = "inactive";
pub const ATTR_KEY_RECV_ONLY: &str = "recvonly";
pub const ATTR_KEY_SEND_ONLY: &str = "sendonly";
pub const ATTR_KEY_SEND_RECV: &str = "sendrecv";
pub const ATTR_KEY_EXT_MAP: &str = "extmap";
pub const ATTR_KEY_EXT_MAP_ALLOW_MIXED: &str = "extmap-allow-mixed";
pub const ATTR_KEY_CRYPTEX: &str = "cryptex";

// Constants for semantic tokens used in JSEP.
pub const SEMANTIC_TOKEN_LIP_SYNCHRONIZATION: &str = "LS";
pub const SEMANTIC_TOKEN_FLOW_IDENTIFICATION: &str = "FID";
pub const SEMANTIC_TOKEN_FORWARD_ERROR_CORRECTION: &str = "FEC";
// https://datatracker.ietf.org/doc/html/rfc5956#section-4.1
pub const SEMANTIC_TOKEN_FORWARD_ERROR_CORRECTION_FRAMEWORK: &str = "FEC-FR";
pub const SEMANTIC_TOKEN_WEBRTC_MEDIA_STREAMS: &str = "WMS";
