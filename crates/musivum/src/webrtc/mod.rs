//! What LiveKit's code takes from pion, as far as it is data: packets ([`rtp`], [`rtcp`]) and
//! the small descriptions of codecs and header extensions that pass through `pkg/sfu`.
//!
//! The WebRTC library that does the transport meets the ported code at the traits the port
//! has where pion's objects were ([`track`]). [`peerconnection`] is the only place that knows
//! that library: musivum's peer connection, which stands where pion's did. What else of the
//! library the ported code names (as it names pion's packages `rtcp`, `sdp`, `ice` and `stun`)
//! it names through the modules of those names here, never from the library's crate.

pub mod ice;
pub mod interceptor;
pub mod peerconnection;
pub mod rtcp;
pub mod rtp;
pub mod sdp;
pub mod stun;
pub mod track;

mod rtpcodec;

pub use rtpcodec::*;
pub use track::*;
