//! LiveKit's `pkg/sfu/connectionquality`: the quality a client is shown for a track
//! (excellent, good, poor, lost). A scorer turns the loss, jitter, round trip time and
//! bitrate of each window into a score that falls fast and recovers slowly, and
//! `ConnectionStats` runs it on a timer over the delta statistics of its owner, a receiver or
//! a down track, and hands out what is reported as an `AnalyticsStat`.

mod connectionstats;
mod scorer;

pub use connectionstats::*;
pub use scorer::*;

#[cfg(test)]
#[path = "scorer_reference_test.rs"]
mod scorer_reference_test;
