//! The Go package `github.com/pion/interceptor/pkg/twcc`, as far as LiveKit uses it: the
//! [`Recorder`], which records the arrival of packets and builds transport wide congestion
//! control feedback from it. The interceptors of the package (the sender interceptor and the
//! header extension interceptor) are not ported.

mod arrival_time_map;
// The file is named after its original, which has the name of its package.
#[allow(clippy::module_inception)]
mod twcc;

pub use twcc::*;
