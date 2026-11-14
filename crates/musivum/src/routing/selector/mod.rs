//! LiveKit's `pkg/routing/selector`: whether a node is available and within its limits, and
//! the choice of a node by its statistics.
//!
//! One node has no choice to make, but it still asks whether it is over the limits of its
//! configuration ([`limits_reached`], which the connect validation of `service` calls) and
//! whether its statistics are recent ([`is_available`]). The selectors (`any`, `cpuload`,
//! `sysload` and what they share in `utils.rs` and `nodesfilter.rs`) are ported with their
//! tests as they are. Not ported: `regionaware.go`, which prefers nodes of a nearby region,
//! and its test; one node has no other region.

mod any;
mod cpuload;
mod errors;
mod interfaces;
mod nodesfilter;
mod sysload;
mod utils;

pub use any::*;
pub use cpuload::*;
pub use errors::*;
pub use interfaces::*;
pub use nodesfilter::*;
pub use sysload::*;
pub use utils::*;

#[cfg(test)]
#[path = "sortby_test.rs"]
mod sortby_test;
