//! LiveKit's `pkg/rtc/transport`: what a transport tells its owner, and the states of a
//! negotiation. `handler.go`, what a transport tells its owner, is not ported yet.

mod negotiationstate;

pub use negotiationstate::*;
