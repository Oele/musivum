//! The Go package `github.com/livekit/protocol/sdp`: what LiveKit's server reads out of a
//! session description (the mid of the BUNDLE group, the simulcast rids, the DTLS
//! fingerprint and role, the ICE credentials, the codecs of a media section, the ids of
//! streams and tracks) and the SDP fragments of WHIP.
//!
//! The descriptions are parsed ones, [`crate::webrtc::sdp::SessionDescription`]; the one
//! that is signalled gives it with `unmarshal()`.

// The Go file of the package's name.
#[allow(clippy::module_inception)]
mod sdp;

pub use sdp::*;
