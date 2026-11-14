// Ported from LiveKit, pkg/telemetry/prometheus/rooms.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the functions that ported code calls are here,
// more are added as they are needed. The collectors are musivum's own (see the module's
// description) and are statics, so there is no `initRoomStats` to call first; the constant
// labels `node_id` and `node_type` are left to whoever serves the metrics. The labels of
// `promPubSubTime` are written out, where the original appends to `promStreamLabels` of
// `packets.go`. A count of cancels that is negative panics as the original's counter does.

use std::collections::HashMap;

use super::{Counter, Histogram, MetricFamily, MetricVec};
use crate::go::atomic;
use crate::go::time::{Duration, MILLISECOND};
use crate::protocol::livekit;

// read by the node's statistics upstream (`node.go`)
pub(super) static TRACK_SUBSCRIBE_CANCELS: atomic::Int32 = atomic::Int32::new(0);

pub static PROM_TRACK_SUBSCRIBE_COUNTER: MetricVec<Counter, 2> =
    MetricVec::new(["state", "error"], Counter::new);
pub static PROM_PUB_SUB_TIME: MetricVec<Histogram<7>, 7> = MetricVec::new(
    // append(promStreamLabels, "sdk", "kind", "count")
    [
        "direction",
        "source",
        "type",
        "country",
        "sdk",
        "kind",
        "count",
    ],
    || Histogram::new([100.0, 200.0, 500.0, 700.0, 1000.0, 5000.0, 10000.0]),
);
pub static PROM_SUBSCRIBE_PACE_SHADOW: MetricVec<Histogram<14>, 1> =
    MetricVec::new(["rate"], || {
        Histogram::new([
            0.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 30000.0,
            60000.0, 120000.0,
        ])
    });

// The collectors of this file under the names upstream registers them with.
pub(super) fn collect(families: &mut Vec<MetricFamily>) {
    families.push(MetricFamily::of_vec(
        "livekit_track_subscribe_counter",
        "",
        &PROM_TRACK_SUBSCRIBE_COUNTER,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_pubsubtime_ms",
        "",
        &PROM_PUB_SUB_TIME,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_subscribe_pace_shadow_delay_ms",
        "Wait that pacing video subscribes at each rate would add, per video subscribe.",
        &PROM_SUBSCRIBE_PACE_SHADOW,
    ));
}

pub fn record_subscribe_time(
    country: &str,
    source: livekit::TrackSource,
    track_type: livekit::TrackType,
    d: Duration,
    sdk: livekit::client_info::Sdk,
    kind: livekit::participant_info::Kind,
    count: i64,
) {
    record_pub_sub_stream_start_time(
        "subscribe",
        country,
        source,
        track_type,
        d,
        sdk,
        kind,
        count,
    );
}

pub fn record_subscribe_stream_start_time(
    country: &str,
    source: livekit::TrackSource,
    track_type: livekit::TrackType,
    d: Duration,
    sdk: livekit::client_info::Sdk,
    kind: livekit::participant_info::Kind,
) {
    record_pub_sub_stream_start_time(
        "subscribe-stream-start",
        country,
        source,
        track_type,
        d,
        sdk,
        kind,
        1,
    );
}

#[allow(clippy::too_many_arguments)]
fn record_pub_sub_stream_start_time(
    direction: &str,
    country: &str,
    source: livekit::TrackSource,
    track_type: livekit::TrackType,
    d: Duration,
    sdk: livekit::client_info::Sdk,
    kind: livekit::participant_info::Kind,
    count: i64,
) {
    PROM_PUB_SUB_TIME
        .with_label_values([
            direction,
            source.as_str_name(),
            track_type.as_str_name(),
            country,
            sdk.as_str_name(),
            kind.as_str_name(),
            &count.to_string(),
        ])
        .observe(d.milliseconds() as f64);
}

// RecordSubscribePaceShadow records, per shadow pacing rate, the wait one video subscribe would have had.
pub fn record_subscribe_pace_shadow(delays: &HashMap<String, Duration>) {
    for (rate, delay) in delays {
        PROM_SUBSCRIBE_PACE_SHADOW
            .with_label_values([rate])
            .observe(delay.0 as f64 / MILLISECOND.0 as f64);
    }
}

pub fn record_track_subscribe_cancels(num_cancels: i32) {
    TRACK_SUBSCRIBE_CANCELS.add(num_cancels);
    PROM_TRACK_SUBSCRIBE_COUNTER
        .with_label_values(["cancel", ""])
        .add(u64::try_from(num_cancels).expect("counter cannot decrease in value"));
}

// The original has tests of its own (`rooms_test.go`) for functions that are not here yet.
// These are new. The statics are shared with every test of the crate that reports to them
// while this one runs, so it looks at what only it changes, or expects "at least".
#[cfg(test)]
mod tests {
    use super::super::HistogramSnapshot;
    use super::*;
    use crate::go::time::SECOND;

    #[test]
    fn a_subscribe_time_counts_under_its_labels() {
        let subscribe = |count: &str| {
            PROM_PUB_SUB_TIME
                .with_label_values([
                    "subscribe",
                    "CAMERA",
                    "VIDEO",
                    "a country of this test",
                    "JS",
                    "STANDARD",
                    count,
                ])
                .snapshot()
        };
        let stream_start = || {
            PROM_PUB_SUB_TIME
                .with_label_values([
                    "subscribe-stream-start",
                    "MICROPHONE",
                    "AUDIO",
                    "a country of this test",
                    "UNKNOWN",
                    "AGENT",
                    "1",
                ])
                .snapshot()
        };
        let (before, before_start) = (subscribe("3"), stream_start());

        // 150 ms: above 100, not above 200
        record_subscribe_time(
            "a country of this test",
            livekit::TrackSource::Camera,
            livekit::TrackType::Video,
            Duration::from_millis(150),
            livekit::client_info::Sdk::Js,
            livekit::participant_info::Kind::Standard,
            3,
        );
        // 2.5 s as whole milliseconds: above 1000, not above 5000
        record_subscribe_stream_start_time(
            "a country of this test",
            livekit::TrackSource::Microphone,
            livekit::TrackType::Audio,
            Duration::from_micros(2_500_900),
            livekit::client_info::Sdk::Unknown,
            livekit::participant_info::Kind::Agent,
        );

        let (after, after_start) = (subscribe("3"), stream_start());
        let in_bucket = |snapshot: &HistogramSnapshot, lower: f64, upper: f64| {
            snapshot.le(upper) - snapshot.le(lower)
        };
        assert_eq!(
            in_bucket(&before, 100.0, 200.0) + 1,
            in_bucket(&after, 100.0, 200.0)
        );
        assert_eq!(before.sum + 150.0, after.sum);
        assert_eq!(
            in_bucket(&before_start, 1000.0, 5000.0) + 1,
            in_bucket(&after_start, 1000.0, 5000.0)
        );
        assert_eq!(before_start.sum + 2500.0, after_start.sum);
        // another count is another collector
        assert_eq!(0, subscribe("a count nobody records").count);
    }

    #[test]
    fn a_shadow_delay_counts_under_its_rate_in_milliseconds() {
        let of = |rate: &str| {
            PROM_SUBSCRIBE_PACE_SHADOW
                .with_label_values([rate])
                .snapshot()
        };
        let (before_zero, before_late) = (
            of("a rate of this test"),
            of("another rate of this test"),
        );

        record_subscribe_pace_shadow(&HashMap::from([
            ("a rate of this test".to_string(), Duration::ZERO),
            ("another rate of this test".to_string(), SECOND + SECOND / 2),
        ]));

        let (after_zero, after_late) = (
            of("a rate of this test"),
            of("another rate of this test"),
        );
        // no wait counts in the bucket of zero
        assert_eq!(before_zero.le(0.0) + 1, after_zero.le(0.0));
        // 1500 ms: above 1000, not above 2000
        assert_eq!(
            before_late.le(2000.0) - before_late.le(1000.0) + 1,
            after_late.le(2000.0) - after_late.le(1000.0)
        );
        assert_eq!(before_late.sum + 1500.0, after_late.sum);
    }

    #[test]
    fn cancelled_subscribes_count() {
        let count = || {
            PROM_TRACK_SUBSCRIBE_COUNTER
                .with_label_values(["cancel", ""])
                .get()
        };
        let (before, before_total) = (count(), TRACK_SUBSCRIBE_CANCELS.load());
        record_track_subscribe_cancels(3);
        record_track_subscribe_cancels(0);
        // the tests of the subscription manager cancel subscribes of their own meanwhile
        assert!(count() >= before + 3);
        assert!(TRACK_SUBSCRIBE_CANCELS.load() >= before_total + 3);
    }
}
