//! What LiveKit's code takes from pion, as far as it is data: packets ([`rtp`], [`rtcp`]) and
//! the small descriptions of codecs and header extensions that pass through `pkg/sfu`.
//!
//! The WebRTC library that does the transport is not here: it meets the ported code at the
//! traits the port has where pion's objects were.

pub mod rtcp;
pub mod rtp;
pub mod sdp;

mod rtpcodec;

pub use rtpcodec::*;
