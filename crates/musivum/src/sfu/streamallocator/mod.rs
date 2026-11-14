//! LiveKit's `pkg/sfu/streamallocator`: one per subscriber connection, it divides the
//! bandwidth that the estimator finds for the connection over the subscriber's video tracks:
//! which track is forwarded at which layer, which tracks are paused when there is not enough,
//! and when to probe for more.

// The Go file of the package's name.
#[allow(clippy::module_inception)]
mod streamallocator;
mod streamstateupdate;
mod track;

pub use streamallocator::*;
pub use streamstateupdate::*;
pub use track::*;
