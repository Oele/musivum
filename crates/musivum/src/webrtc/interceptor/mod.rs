//! What musivum uses of `github.com/pion/interceptor`, ported: the recorder that builds
//! transport-wide congestion control feedback ([`twcc`]). The interceptors themselves are not
//! here; LiveKit does its feedback outside of pion's interceptor chain. Also here: the
//! description of a negotiated header extension ([`RtpHeaderExtension`]) and, for tests only,
//! the FlexFEC-03 encoder (`flexfec`).

mod internal;
mod streaminfo;

// The FlexFEC-03 encoder, which the tests of `sfu::flexfec` make their FEC packets with.
#[cfg(test)]
pub mod flexfec;
pub mod twcc;

pub use streaminfo::*;
