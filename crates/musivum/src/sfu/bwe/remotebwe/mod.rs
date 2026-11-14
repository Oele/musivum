//! LiveKit's `pkg/sfu/bwe/remotebwe`: estimating a subscriber's bandwidth from REMB and NACKs.
//! The estimate the subscriber reports and the share of its NACKs that are repeated ones are
//! watched for a trend (`channel_observer`, `nack_tracker`); a falling estimate or lasting
//! loss makes the channel congested, at a capacity that is then committed, and a probe for
//! more is followed through its stages (`probe_controller`).

mod channel_observer;
mod nack_tracker;
mod probe_controller;
mod remote_bwe;

pub use channel_observer::*;
pub use nack_tracker::*;
pub use probe_controller::*;
pub use remote_bwe::*;
