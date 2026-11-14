//! The Go package `github.com/livekit/protocol/codecs`: the parameters of the codecs the
//! server knows, and in [`mime`] their mime types.

// The Go file of the package's name.
#[allow(clippy::module_inception)]
mod codecs;

pub mod mime;

pub use codecs::*;
