//! LiveKit's `pkg/sfu/videolayerselector`: for every packet of a video stream, whether a
//! subscriber gets it. One selector for simulcast (which switches layers at key frames), one
//! for VP9 SVC and one for streams with dependency descriptors, which also rewrites the
//! descriptor; `DdForwarder` passes the descriptor of a simulcast stream on. The selectors of
//! temporal layers that the simulcast selector uses are in `temporallayerselector`.

mod base;
mod ddforwarder;
mod decodetarget;
mod dependencydescriptor;
mod framechain;
mod framenumberwrapper;
mod null;
mod selectordecisioncache;
mod simulcast;
pub mod temporallayerselector;
// The Go file of the package's name.
#[allow(clippy::module_inception)]
mod videolayerselector;
mod vp9;

pub use base::*;
pub use ddforwarder::*;
pub use decodetarget::*;
pub use dependencydescriptor::*;
pub use framechain::*;
pub use framenumberwrapper::*;
pub use null::*;
pub use selectordecisioncache::*;
pub use simulcast::*;
pub use videolayerselector::*;
pub use vp9::*;

/// The errors of the package. The original has no `ErrX` values: these are the errors it
/// makes in place with `fmt.Errorf`, with the same messages.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    // selectordecisioncache.go
    #[error("too old, oldest: {oldest}, asking: {asking}")]
    TooOld { oldest: u64, asking: u64 },

    // decodetarget.go
    #[error("mismatch target {target} and len(DecodeTargetIndications) {len}")]
    MismatchTarget { target: i64, len: usize },
}

/// Go's `v << count` of a `uint32` for a count that is not a constant, where the files of
/// this package shift by the number of a decode target: zero for a count of 32 or more, where
/// Rust would panic, and a panic for a negative count, as in Go.
fn shl(v: u32, count: i64) -> u32 {
    assert!(count >= 0, "negative shift amount");
    u32::try_from(count)
        .ok()
        .and_then(|count| v.checked_shl(count))
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "reference_test.rs"]
mod reference_test;
