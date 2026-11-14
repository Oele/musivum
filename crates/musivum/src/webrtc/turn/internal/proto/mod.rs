//! pion/turn's `internal/proto` as far as the server uses it. The attributes and the
//! ChannelData message are the WebRTC library's TURN crate (`rtc-turn`, a port of pion/turn's
//! `internal/proto`); what that crate does not have is ported here (`stun_conn.rs`).
//!
//! Where `rtc-turn` differs from pion: `RequestedAddressFamily` is not `Copy` (it is rebuilt
//! from its byte where a copy is needed) and its error for a family that is neither IPv4 nor
//! IPv6 is `Error::Other("ErrInvalidRequestedFamilyValue")`; `Lifetime` holds a
//! `std::time::Duration`; `ChannelData::decode` checks the channel number before the length
//! (both fail either way) and copies the data. `ConnectionID` (RFC 6062) is not here: the TCP
//! relays it belongs to are not ported.

pub use rtc::turn::proto::chandata::ChannelData;
pub use rtc::turn::proto::channum::{ChannelNumber, MAX_CHANNEL_NUMBER, MIN_CHANNEL_NUMBER};
pub use rtc::turn::proto::data::Data;
pub use rtc::turn::proto::evenport::EvenPort;
pub use rtc::turn::proto::lifetime::{DEFAULT_LIFETIME, Lifetime};
pub use rtc::turn::proto::peeraddr::PeerAddress;
pub use rtc::turn::proto::relayaddr::RelayedAddress;
pub use rtc::turn::proto::reqfamily::{
    REQUESTED_FAMILY_IPV4, REQUESTED_FAMILY_IPV6, RequestedAddressFamily,
};
pub use rtc::turn::proto::reqtrans::RequestedTransport;
pub use rtc::turn::proto::rsrvtoken::ReservationToken;
pub use rtc::turn::proto::{PROTO_TCP, PROTO_UDP, Protocol};

mod stun_conn;

pub use stun_conn::*;

use crate::webrtc::stun::Error as StunError;
use crate::webrtc::stun::message::{Message, Setter};

/// pion's `proto.DefaultLifetime` as a Go duration.
pub const DEFAULT_LIFETIME_GO: crate::go::time::Duration =
    crate::go::time::Duration::from_secs(DEFAULT_LIFETIME.as_secs() as i64);

/// pion's `proto.IsChannelData`.
pub fn is_channel_data(buf: &[u8]) -> bool {
    ChannelData::is_channel_data(buf)
}

/// pion/stun's `Build`: a message made by the setters, in order.
pub fn build(setters: &[&dyn Setter]) -> Result<Message, StunError> {
    let mut m = Message::new();
    m.reset();
    m.write_header();
    for s in setters {
        s.add_to(&mut m)?;
    }
    Ok(m)
}
