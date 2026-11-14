//! Whether a VP9 packet starts a key frame, as the buffer asks it (`processVideoPacket` in
//! `buffer_base.go` asks `codec.IsVP9KeyFrame(vp9Packet, payload)`). Written new: a packet
//! whose descriptor starts a frame but that carries no frame data starts no key frame, and
//! the codec's check is asked only for the others.

use super::Error;
use crate::mediatransportutil::codec;
use crate::webrtc::rtp::codecs;

/// Whether `vp9`, parsed from `payload`, starts a key frame. An error would make the buffer
/// drop the packet, as it does with a VP9 packet it cannot parse; none is returned today.
pub fn vp9_key_frame(vp9: &codecs::Vp9Packet, payload: &[u8]) -> Result<bool, Error> {
    // The descriptor says a frame begins and there is no frame data.
    if vp9.b && vp9.payload.is_empty() {
        return Ok(false);
    }
    Ok(codec::is_vp9_key_frame(Some(vp9), payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(payload: &[u8]) -> codecs::Vp9Packet {
        let mut vp9 = codecs::Vp9Packet::default();
        vp9.unmarshal(payload).unwrap();
        vp9
    }

    #[test]
    fn a_descriptor_that_begins_a_frame_with_no_frame_behind_it_is_no_key_frame() {
        // the one byte is a payload descriptor with only the B bit
        let payload = [0x08];
        let vp9 = parsed(&payload);
        assert!(vp9.b && vp9.payload.is_empty());
        assert_eq!(Ok(false), vp9_key_frame(&vp9, &payload));
    }

    #[test]
    fn every_other_packet_gets_the_answer_of_the_original() {
        for (payload, want) in [
            // a key frame (the packet of the codec package's tests)
            (&[0x88, 0x05, 0x82, 0x49, 0x83, 0x42, 0x00][..], true),
            // the same frame, not its beginning
            (&[0x80, 0x05, 0x82, 0x49, 0x83, 0x42, 0x00][..], false),
            // a descriptor without the B bit and nothing behind it
            (&[0x00][..], false),
        ] {
            let vp9 = parsed(payload);
            assert_eq!(
                codec::is_vp9_key_frame(Some(&vp9), payload),
                want,
                "{payload:02x?}"
            );
            assert_eq!(Ok(want), vp9_key_frame(&vp9, payload), "{payload:02x?}");
        }
    }
}
