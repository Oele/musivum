//! LiveKit's `pkg/sfu/flexfec`: a FlexFEC-03 decoder used to recover RTP packets lost on the
//! publisher leg before they are forwarded downstream.

mod decoder;

pub use decoder::*;
