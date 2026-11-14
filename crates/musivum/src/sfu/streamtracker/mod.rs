//! LiveKit's `pkg/sfu/streamtracker`: whether a layer of a published stream is live, decided
//! by packets, by frames or from dependency descriptors, and at what bitrate it comes in.

mod interfaces;
// The Go file of the package's name.
#[allow(clippy::module_inception)]
mod streamtracker;
mod streamtracker_dd;
mod streamtracker_frame;
mod streamtracker_packet;

pub use interfaces::*;
pub use streamtracker::*;
pub use streamtracker_dd::*;
pub use streamtracker_frame::*;
pub use streamtracker_packet::*;
