//! LiveKit's `pkg/sfu/bwe`: what an estimator of the available bandwidth is to the rest of
//! the server, and an estimator that does nothing. The estimators themselves are in
//! `remotebwe` and `sendsidebwe`.

// The Go file of the package's name.
#[allow(clippy::module_inception)]
mod bwe;
mod null_bwe;
pub mod remotebwe;
pub mod sendsidebwe;

pub use bwe::*;
pub use null_bwe::*;
