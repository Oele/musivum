//! pion's `ice` package as far as LiveKit's code names it: a candidate as the ICE agent has
//! it, the function that reads one from a line of a description, and two of the agent's
//! settings.
//!
//! These are the types of the WebRTC library's ICE crate (`rtc-ice`, itself a port of
//! `pion/ice`), under pion's names in Rust's spelling. What a peer connection hands out and
//! takes (`ICECandidate`, `ICECandidatePair`, `ICECandidateInit` and the types of their
//! fields) is in [`peerconnection`](crate::webrtc::peerconnection), as it is in pion's
//! `webrtc` package.

/// pion's `ice.Candidate`: an interface there, a struct here, with the methods LiveKit
/// calls (`address()`, `port()`, `candidate_type()`, `network_type()`, `tcp_type()`,
/// `related_address()`, `marshal()`).
pub use rtc::ice::candidate::Candidate;
/// pion's `ice.CandidateType`.
pub use rtc::ice::candidate::CandidateType;
/// pion's `ice.UnmarshalCandidate`: reads the value of an `a=candidate` attribute (without
/// the `candidate:` in front of it).
pub use rtc::ice::candidate::unmarshal_candidate;
/// pion's `ice.MulticastDNSMode`.
pub use rtc::ice::mdns::MulticastDnsMode;
/// pion's `ice.NetworkType`.
pub use rtc::ice::network_type::NetworkType;
/// pion's `ice.TCPType`.
pub use rtc::ice::tcp_type::TcpType;
/// What the functions of this module fail with: the one error type of the WebRTC library.
pub use rtc::shared::error::Error;
