//! pion's `stun` package as far as LiveKit's code names it: the message and the attribute a
//! binding response carries, for the STUN exchange `mediatransportutil/rtcconfig` does itself
//! to learn the node's external address.
//!
//! These are the modules of the WebRTC library's STUN crate (`rtc-stun`, itself a port of
//! `pion/stun`).

/// What the functions of these modules fail with: the one error type of the WebRTC library.
pub use rtc::shared::error::Error;
/// pion's `stun.Message`, `stun.TransactionID`, the message types and the `Setter` and
/// `Getter` interfaces.
pub use rtc::stun::message;
/// pion's `stun.XORMappedAddress`.
pub use rtc::stun::xoraddr;
