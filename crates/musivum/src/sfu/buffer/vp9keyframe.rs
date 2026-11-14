//! Whether a VP9 packet starts a key frame, as the buffer asks it (`processVideoPacket` in
//! `buffer_base.go` asks `codec.IsVP9KeyFrame(vp9Packet, payload)`). Written new: a packet
//! whose descriptor starts a frame but that carries no frame data starts no key frame, and
//! the codec's check is asked only for the others.

use super::Error;
use crate::mediatransportutil::codec;
use crate::webrtc::rtp::codecs;

/// Whether `vp9`, parsed from `payload`, starts a key frame. An error makes the buffer drop
/// the packet, as it does with a VP9 packet it cannot parse.
pub fn vp9_key_frame(vp9: &codecs::Vp9Packet, payload: &[u8]) -> Result<bool, Error> {
    // TODO(human): decide what a packet gets whose descriptor says "beginning of a frame"
    // (`vp9.b`) and that has nothing behind the descriptor (`vp9.payload.is_empty()`).
    // Until then this asks as upstream does, and panics on that packet as upstream does.
    Ok(codec::is_vp9_key_frame(Some(vp9), payload))
}
