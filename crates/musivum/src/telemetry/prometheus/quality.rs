// Ported from LiveKit, pkg/telemetry/prometheus/quality.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the functions that ported code calls are here,
// more are added as they are needed. The collectors are musivum's own (see the module's
// description) and are statics, so there is no `initQualityStats` to call first; the constant
// labels `node_id` and `node_type` are left to whoever serves the metrics.

use super::{Histogram, MetricFamily};
use crate::protocol::livekit;

pub static QUALITY_RATING: Histogram<3> = Histogram::new([0.0, 1.0, 2.0]);
pub static QUALITY_SCORE: Histogram<10> =
    Histogram::new([1.0, 2.0, 2.5, 3.0, 3.25, 3.5, 3.75, 4.0, 4.25, 4.5]);

// The collectors of this file under the names upstream registers them with.
pub(super) fn collect(families: &mut Vec<MetricFamily>) {
    families.push(MetricFamily::of(
        "livekit_quality_rating",
        "",
        &QUALITY_RATING,
    ));
    families.push(MetricFamily::of(
        "livekit_quality_score",
        "",
        &QUALITY_SCORE,
    ));
}

pub fn record_quality(rating: livekit::ConnectionQuality, score: f32) {
    QUALITY_RATING.observe(rating as i32 as f64);
    QUALITY_SCORE.observe(score as f64);
}

// New: the original has no test of this file. The statics are shared with every test of the
// crate that reports to them while this one runs, so it expects "at least".
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_quality_counts_its_rating_and_score() {
        let (rating, score) = (QUALITY_RATING.snapshot(), QUALITY_SCORE.snapshot());
        record_quality(livekit::ConnectionQuality::Poor, 2.25);
        let (rating_after, score_after) = (QUALITY_RATING.snapshot(), QUALITY_SCORE.snapshot());
        assert!(rating_after.count > rating.count);
        assert!(rating_after.sum >= rating.sum);
        assert!(score_after.count > score.count);
        assert!(score_after.sum >= score.sum + 2.25);
        // POOR is 0, which is in the first bucket
        assert!(rating_after.le(0.0) > rating.le(0.0));
    }
}
