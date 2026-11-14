//! `github.com/frostbyte73/core`, small helpers that LiveKit's code takes from there, as far as
//! the ported code uses them: [`Throttle`] and the [`QueuePool`]. Its `Fuse` is a
//! `CancellationToken` where the ported code uses one, and in the pool it is written out
//! with what the pool needs of it.

mod pool;
mod throttle;

pub use pool::*;
pub use throttle::*;
