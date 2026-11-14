//! LiveKit's `pkg/rtc/types`: the interfaces the parts of `pkg/rtc` meet through. The
//! interfaces are not ported yet; the protocol versions and the traffic statistics are.

mod protocol_version;
mod trafficstats;

pub use protocol_version::*;
pub use trafficstats::*;
