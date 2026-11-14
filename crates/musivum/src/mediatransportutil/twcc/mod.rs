//! The Go package `github.com/livekit/mediatransportutil/pkg/twcc`: the responder that builds
//! transport-wide congestion control feedback for a publisher.

// The file is named after its original, which has the name of its package.
#[allow(clippy::module_inception)]
mod twcc;

pub use twcc::*;
