//! LiveKit's `pkg/sfu/rtpstats`: the bookkeeping of every stream. Packets and bytes, loss,
//! duplicates, out of order, jitter, RTT, NACKs, key frames, the gap histogram, what goes into
//! RTCP receiver and sender reports, the drift between a publisher's clock and the server's,
//! and the snapshots that statistics and connection quality are computed from.
//!
//! How the types that embed each other upstream are laid out here: there is one lock per
//! statistics object and it is the outer type's (`RtpStatsReceiver { lock: RwLock<...Inner> }`).
//! What the Go types embed is a field of that inner struct, named after its type
//! (`rtp_stats_base`, and in it `rtp_stats_base_lite`), and is plain state whose methods are
//! called with the lock held. The methods the outer types inherit upstream (`stop`,
//! `is_active`, `update_nack`, `update_rtt`, ...) are forwarded by each outer type; the two
//! macros that write those forwards are at the end of `rtpstats_base_lite.rs` and
//! `rtpstats_base.rs`.

mod rtpstats_base;
mod rtpstats_base_lite;
mod rtpstats_receiver;
mod rtpstats_receiver_lite;
mod rtpstats_sender;
mod rtpstats_sender_lite;

pub use rtpstats_base::*;
pub use rtpstats_base_lite::*;
pub use rtpstats_receiver::*;
pub use rtpstats_receiver_lite::*;
pub use rtpstats_sender::*;
pub use rtpstats_sender_lite::*;

/// The errors of the package. The original makes each with `errors.New` at the place it
/// returns it; the texts are the original's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("too many packets expected in delta lite")]
    TooManyPacketsExpectedInDeltaLite,
    #[error("unexpected number of packets lost in delta lite")]
    UnexpectedNumberOfPacketsLostInDeltaLite,
    #[error("not initialized")]
    NotInitialized,
    #[error("no time elapsed")]
    NoTimeElapsed,
    #[error("adjusting first packet time, too big, ignoring")]
    AdjustingFirstPacketTimeTooBig,
    #[error("too many packets expected in delta")]
    TooManyPacketsExpectedInDelta,
    #[error("padding packets more than expected")]
    PaddingPacketsMoreThanExpected,
    #[error("uninitialized")]
    Uninitialized,
}

#[cfg(test)]
#[path = "rtpstats_test.rs"]
mod rtpstats_test;

#[cfg(test)]
#[path = "rtpstats_reference_test.rs"]
mod rtpstats_reference_test;
