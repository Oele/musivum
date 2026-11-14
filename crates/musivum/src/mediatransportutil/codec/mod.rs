//! The Go package `github.com/livekit/mediatransportutil/pkg/codec`: reading the payload
//! headers of the video codecs (the VP8 payload descriptor, H.264 and H.265 NAL units), key
//! frame detection and the size of the video.

mod h26xhelper;
mod helpers;
mod keyframe;
mod vp8;

pub use h26xhelper::*;
pub use helpers::*;
pub use keyframe::*;
pub use vp8::*;

#[cfg(test)]
#[path = "reference_test.rs"]
mod reference_test;
