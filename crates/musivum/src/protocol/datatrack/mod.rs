//! The Go package `github.com/livekit/protocol/datatrack`: the packet format of data tracks,
//! a small header (handle, sequence number, frame number, flags, extensions) in front of the
//! payload, and the extensions the protocol knows.

mod extension_e2ee;
mod extension_participant_sid;
mod extension_user_timestamp;
mod packet;

/// The Go package `datatrack/datatracktest`, compiled for tests only.
#[cfg(test)]
pub mod datatracktest;

pub use extension_e2ee::*;
pub use extension_participant_sid::*;
pub use extension_user_timestamp::*;
pub use packet::*;

#[cfg(test)]
#[path = "packet_fuzz_test.rs"]
mod packet_fuzz_test;
