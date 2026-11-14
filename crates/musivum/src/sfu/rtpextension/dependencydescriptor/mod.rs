//! LiveKit's `pkg/sfu/rtpextension/dependencydescriptor`: the AV1 dependency descriptor RTP
//! header extension, read and written bit by bit.

mod bitstreamreader;
mod bitstreamwriter;
mod dependencydescriptorextension;
mod dependencydescriptorreader;
mod dependencydescriptorwriter;

pub use bitstreamreader::*;
pub use bitstreamwriter::*;
pub use dependencydescriptorextension::*;
pub use dependencydescriptorreader::*;
pub use dependencydescriptorwriter::*;

/// Go's `v << count` and `v >> count` for a count that is not a constant, which the files of
/// this package use where the original shifts by one: a count of the width of the type or
/// more gives zero (Rust would panic), and a negative count is a panic, as it is in Go.
trait GoShift: Sized {
    fn go_shl(self, count: i64) -> Self;
    fn go_shr(self, count: i64) -> Self;
}

macro_rules! impl_go_shift {
    ($($t:ty),*) => {$(
        impl GoShift for $t {
            fn go_shl(self, count: i64) -> Self {
                assert!(count >= 0, "negative shift amount");
                u32::try_from(count)
                    .ok()
                    .and_then(|count| self.checked_shl(count))
                    .unwrap_or(0)
            }

            fn go_shr(self, count: i64) -> Self {
                assert!(count >= 0, "negative shift amount");
                u32::try_from(count)
                    .ok()
                    .and_then(|count| self.checked_shr(count))
                    .unwrap_or(0)
            }
        }
    )*};
}

impl_go_shift!(u8, u32, u64);

#[cfg(test)]
#[path = "dependencydescriptorextension_benchmark_test.rs"]
mod dependencydescriptorextension_benchmark_test;

#[cfg(test)]
#[path = "reference_test.rs"]
mod reference_test;
