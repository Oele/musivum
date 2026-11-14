//! The Go package `github.com/pion/interceptor/pkg/flexfec`, as far as the tests of LiveKit's
//! FlexFEC decoder (`sfu::flexfec`) and of its buffer use it: the FlexFEC-03 encoder and what
//! it stands on. It is compiled for tests only. Not ported: the interceptor around the
//! encoder (`encoder_interceptor.go`, `option.go`), the FlexFEC-20 encoder, and pion's own
//! decoder (`flexfec_decoder_03.go`), in whose place LiveKit has `pkg/sfu/flexfec`.
//!
//! Of pion's tests, those of the coverage, of the encoder and of the iterator are ported.
//! `flexfec_03_test.go` is not: it sends what the encoder makes through pion's decoder. What
//! it is there for (that every single packet of a window can be recovered, also through the
//! second and third packet mask and across the wrap of the sequence numbers) is held against
//! the originals in `sfu/flexfec/reference_test.rs`, with LiveKit's decoder. Not ported
//! either: the tests of pion's decoder and of the interceptor, and the benchmarks.

mod flexfec_coverage;
mod flexfec_encoder;
mod flexfec_encoder_03;

pub mod util;

pub use flexfec_coverage::*;
pub use flexfec_encoder::*;
pub use flexfec_encoder_03::*;
