//! LiveKit's `pkg/sfu/codecmunger`: rewriting codec headers for one subscriber, so that what
//! it gets stays continuous across layer switches and dropped packets. `Vp8` rewrites the
//! VP8 payload descriptor, `DdTemporal` filters temporal layers by the dependency descriptor
//! and `Null` does nothing.

// The Go file of the package's name.
#[allow(clippy::module_inception)]
mod codecmunger;
mod ddtemporal;
mod null;
mod orderedmap;
mod vp8;

pub use codecmunger::*;
pub use ddtemporal::*;
pub use null::*;
pub use vp8::*;
