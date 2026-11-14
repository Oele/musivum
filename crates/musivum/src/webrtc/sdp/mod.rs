//! pion's `sdp` package as LiveKit's code uses it: a session description as parsed text, the
//! names of its attributes and of the header extensions.
//!
//! The parsed description is the one of the WebRTC library: [`SessionDescription`] and what
//! it is made of are the types of `rtc`'s SDP crate, itself a port of `pion/sdp`, with pion's
//! fields in Rust's spelling (`media_descriptions`, `media_name.formats`, `attributes`).
//! [`unmarshal`](crate::webrtc::peerconnection::SessionDescription::unmarshal) of the
//! description that is signalled gives one, `marshal()` writes it back as text.
//!
//! Where that crate differs from pion in what the ported code sees, pion's way is here:
//!
//! - The value of an attribute is an `Option<String>`, `None` for a property such as
//!   `a=recvonly`; pion's is a string, empty for a property. The library's
//!   `SessionDescription::attribute` returns the value alone, so it cannot tell a property
//!   from an attribute that is not there, and its `MediaDescription::attribute` returns an
//!   `Option` in an `Option`. [`attribute`] is pion's `Attribute(key) (string, bool)` for
//!   both.
//! - The library's `get_codec_for_payload_type` knows no static payload types (0, 8 and 9
//!   need no `a=rtpmap`), does not apply `a=rtcp-fb:*` to the codecs, and drops an `a=fmtp`
//!   whose parameters have a space in them (`a=fmtp:98 profile-level-id=42e01e;
//!   packetization-mode=1`). [`get_codec_map`] and [`get_codec_for_payload_type`] are pion's.
//! - The names of the attributes ([`ATTR_KEY_MID`], ...) are pion's in this port's spelling;
//!   the library spells some of them differently and lacks `ice-options` and `cryptex`.
//!
//! Not made the same, all of it in the parser: the library trims white space at both ends of
//! every line, pion takes the line ending off and nothing else; for `a=:x` the library has
//! an empty key with the value `x`, pion the key `:x`; and a media line without formats
//! (`m=application 9 UDP/DTLS/SCTP`) is refused by the library and taken by pion. What the
//! library marshals is, for the descriptions of the tests in `crate::protocol::sdp`, what
//! pion marshals.

mod extmap;
mod jsep;
mod util;

pub use extmap::*;
pub use jsep::*;
pub use util::*;

pub use rtc::sdp::description::common::{Address, Attribute, ConnectionInformation};
pub use rtc::sdp::description::media::{MediaDescription, MediaName, RangedPort};
pub use rtc::sdp::description::session::SessionDescription;
pub use rtc::sdp::util::{Codec, ConnectionRole};

use crate::webrtc;

/// pion's `SessionDescription.Attribute` and `MediaDescription.Attribute`, on the attributes
/// of either: the value of the first attribute with this key, empty for one that has none
/// (`a=ice-lite`), and `None` where pion's second result is false.
pub fn attribute<'a>(attributes: &'a [Attribute], key: &str) -> Option<&'a str> {
    for a in attributes {
        if a.key == key {
            return Some(a.value.as_deref().unwrap_or(""));
        }
    }

    None
}

impl webrtc::peerconnection::SessionDescription {
    /// Unmarshal is a helper to deserialize the sdp.
    ///
    /// pion's `webrtc.SessionDescription.Unmarshal`. It is here, with the type it returns, so
    /// that what is parsed is named in one place.
    pub fn unmarshal(&self) -> Result<SessionDescription, webrtc::Error> {
        let mut reader = std::io::Cursor::new(self.sdp.as_bytes());
        Ok(SessionDescription::unmarshal(&mut reader)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webrtc::peerconnection::{SdpType, SessionDescription as SignalledDescription};

    #[test]
    fn attribute_tells_a_property_from_an_attribute_that_is_not_there() {
        let sd = SignalledDescription {
            r#type: SdpType::Offer,
            sdp: "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=ice-lite\r\na=group:BUNDLE 0\r\n\
                  m=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=mid:0\r\na=recvonly\r\na=mid:1\r\n"
                .to_string(),
        };
        let parsed = sd.unmarshal().unwrap();

        assert_eq!(Some(""), attribute(&parsed.attributes, ATTR_KEY_ICE_LITE));
        assert_eq!(
            Some("BUNDLE 0"),
            attribute(&parsed.attributes, ATTR_KEY_GROUP)
        );
        assert_eq!(None, attribute(&parsed.attributes, ATTR_KEY_MID));

        let media = &parsed.media_descriptions[0];
        assert_eq!(Some(""), attribute(&media.attributes, ATTR_KEY_RECV_ONLY));
        // the first one of two
        assert_eq!(Some("0"), attribute(&media.attributes, ATTR_KEY_MID));
        assert_eq!(None, attribute(&media.attributes, ATTR_KEY_SEND_ONLY));

        assert_eq!(sd.sdp, parsed.marshal());
    }

    #[test]
    fn unmarshal_refuses_what_is_not_a_description() {
        let sd = SignalledDescription {
            r#type: SdpType::Offer,
            sdp: "not a description".to_string(),
        };
        assert!(sd.unmarshal().is_err());
    }
}
