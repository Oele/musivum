//! LiveKit's `pkg/sfu/pacer`: what every outgoing media packet of a subscriber goes through.
//! A pacer stamps the abs-send-time and transport-wide sequence number extensions at the
//! moment of sending, tells the bandwidth estimator what was sent and writes the packet to
//! the connection: at once (`PassThrough`), from a worker (`NoQueue`) or spaced out at a
//! bitrate (`LeakyBucket`). It also counts what is sent during a probe cluster.

mod base;
mod leaky_bucket;
mod no_queue;
// The Go file of the package's name.
#[allow(clippy::module_inception)]
mod pacer;
mod pass_through;
mod probe_observer;

pub use base::*;
pub use leaky_bucket::*;
pub use no_queue::*;
pub use pacer::*;
pub use pass_through::*;
pub use probe_observer::*;
