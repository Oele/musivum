//! The Go package `github.com/livekit/mediatransportutil/pkg/latency`: the estimator of the
//! one-way delay between a sender and a receiver whose clocks are not the same.

mod owd_estimator;

pub use owd_estimator::*;
