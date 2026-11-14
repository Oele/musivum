// Ported from LiveKit, pkg/telemetry/prometheus/rooms.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the functions that ported code calls are here,
// more are added as they are needed. The collectors are musivum's own (see the module's
// description) and are statics, so there is no `initRoomStats` to call first; the constant
// labels `node_id` and `node_type` are left to whoever serves the metrics. The labels of
// `promPubSubTime` are written out, where the original appends to `promStreamLabels` of
// `packets.go`. A count of cancels that is negative panics as the original's counter does.

use std::collections::HashMap;

use super::{Counter, Gauge, Histogram, MetricFamily, MetricVec};
use crate::go::atomic;
use crate::go::time::{self, Duration, MILLISECOND, Time};
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

pub static PROM_PEER_CONNECTION: MetricVec<Counter, 2> =
    MetricVec::new(["transport", "state"], Counter::new);

// Added for the telemetry service (`events.go`), from here on.
pub(super) static ROOM_CURRENT: atomic::Int32 = atomic::Int32::new(0);
pub(super) static PARTICIPANT_CURRENT: atomic::Int32 = atomic::Int32::new(0);
pub(super) static TRACK_PUBLISHED_CURRENT: atomic::Int32 = atomic::Int32::new(0);
pub(super) static TRACK_SUBSCRIBED_CURRENT: atomic::Int32 = atomic::Int32::new(0);
pub(super) static TRACK_PUBLISH_ATTEMPTS: atomic::Int32 = atomic::Int32::new(0);
pub(super) static TRACK_PUBLISH_SUCCESS: atomic::Int32 = atomic::Int32::new(0);
pub(super) static TRACK_SUBSCRIBE_ATTEMPTS: atomic::Int32 = atomic::Int32::new(0);
pub(super) static TRACK_SUBSCRIBE_SUCCESS: atomic::Int32 = atomic::Int32::new(0);
// count the number of failures that are due to user error (permissions, track doesn't exist), so we could compute
// success rate by subtracting this from total attempts
pub(super) static TRACK_SUBSCRIBE_USER_ERROR: atomic::Int32 = atomic::Int32::new(0);

pub static PROM_ROOM_CURRENT: Gauge = Gauge::new();
pub static PROM_ROOM_DURATION: Histogram<10> = Histogram::new([
    5.0,
    10.0,
    60.0,
    5.0 * 60.0,
    10.0 * 60.0,
    30.0 * 60.0,
    60.0 * 60.0,
    2.0 * 60.0 * 60.0,
    5.0 * 60.0 * 60.0,
    10.0 * 60.0 * 60.0,
]);
pub static PROM_PARTICIPANT_CURRENT: Gauge = Gauge::new();
pub static PROM_TRACK_PUBLISHED_CURRENT: MetricVec<Gauge, 1> = MetricVec::new(["kind"], Gauge::new);
pub static PROM_TRACK_SUBSCRIBED_CURRENT: MetricVec<Gauge, 1> =
    MetricVec::new(["kind"], Gauge::new);
pub static PROM_TRACK_PACKET_TRAILER_CURRENT: Gauge = Gauge::new();
// Upstream's `initRoomStats` sets the gauge of every known feature to zero, so that each is
// there before a track has it; here a feature's gauge is there once a track had it.
pub static PROM_TRACK_PACKET_TRAILER_BY_FEATURE_CURRENT: MetricVec<Gauge, 1> =
    MetricVec::new(["feature"], Gauge::new);
pub static PROM_TRACK_PUBLISH_COUNTER: MetricVec<Counter, 2> =
    MetricVec::new(["kind", "state"], Counter::new);

// Added for the participant (participant.go).
pub(super) static TRACK_PUBLISH_CANCELS: atomic::Int32 = atomic::Int32::new(0);
// prometheus.ExponentialBucketsRange(100, 10000, 15), as Go computes it
pub static PROM_SESSION_START_TIME: MetricVec<Histogram<15>, 3> =
    MetricVec::new(["protocol_version", "warp", "sdk"], || {
        Histogram::new([
            100.0,
            138.94954943731375,
            193.06977288832496,
            268.26957952797244,
            372.7593720314938,
            517.9474679231207,
            719.6856730011514,
            999.999999999999,
            1389.495494373136,
            1930.6977288832477,
            2682.695795279722,
            3727.593720314934,
            5179.474679231202,
            7196.856730011506,
            9999.999999999978,
        ])
    });
// prometheus.ExponentialBucketsRange(100, 4*60*60*1000, 15), as Go computes it
pub static PROM_SESSION_DURATION: MetricVec<Histogram<15>, 1> =
    MetricVec::new(["protocol_version"], || {
        Histogram::new([
            100.0,
            233.59011674382418,
            545.6434264039341,
            1274.5691167419518,
            2977.267487778254,
            6954.602600477145,
            16245.264333523593,
            37947.33192202059,
            88641.21693781434,
            207057.12212818698,
            483664.9733056345,
            1.1297935757936179e6,
            2.639086132660537e6,
            6.164644378251824e6,
            1.440000000000003e7,
        ])
    });

// The collectors of this file under the names upstream registers them with.
pub(super) fn collect(families: &mut Vec<MetricFamily>) {
    families.push(MetricFamily::of(
        "livekit_room_total",
        "",
        &PROM_ROOM_CURRENT,
    ));
    families.push(MetricFamily::of(
        "livekit_room_duration_seconds",
        "",
        &PROM_ROOM_DURATION,
    ));
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
    families.push(MetricFamily::of_vec(
        "livekit_peer_connection_state",
        "",
        &PROM_PEER_CONNECTION,
    ));
    families.push(MetricFamily::of(
        "livekit_participant_total",
        "",
        &PROM_PARTICIPANT_CURRENT,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_track_published_total",
        "",
        &PROM_TRACK_PUBLISHED_CURRENT,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_track_subscribed_total",
        "",
        &PROM_TRACK_SUBSCRIBED_CURRENT,
    ));
    families.push(MetricFamily::of(
        "livekit_track_packet_trailer_total",
        "Current number of published video tracks with packet trailers enabled.",
        &PROM_TRACK_PACKET_TRAILER_CURRENT,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_track_packet_trailer_feature_total",
        "Current number of published video tracks by enabled packet trailer feature.",
        &PROM_TRACK_PACKET_TRAILER_BY_FEATURE_CURRENT,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_track_publish_counter",
        "",
        &PROM_TRACK_PUBLISH_COUNTER,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_session_start_time_ms",
        "",
        &PROM_SESSION_START_TIME,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_session_duration_ms",
        "",
        &PROM_SESSION_DURATION,
    ));
}

pub fn record_peer_connection_state(transport: livekit::SignalTarget, state: &str) {
    // transport.String()
    PROM_PEER_CONNECTION
        .with_label_values([transport.as_str_name(), state])
        .inc();
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

// Added for the telemetry service (`events.go`), from here on, in upstream's order.

pub fn room_started() {
    PROM_ROOM_CURRENT.add(1.0);
    ROOM_CURRENT.inc();
}

pub fn room_ended(started_at: Time) {
    if !started_at.is_zero() {
        PROM_ROOM_DURATION.observe(time::since(started_at).0 as f64 / time::SECOND.0 as f64);
    }
    PROM_ROOM_CURRENT.sub(1.0);
    ROOM_CURRENT.dec();
}

pub fn add_participant() {
    PROM_PARTICIPANT_CURRENT.add(1.0);
    PARTICIPANT_CURRENT.inc();
}

pub fn sub_participant() {
    PROM_PARTICIPANT_CURRENT.sub(1.0);
    PARTICIPANT_CURRENT.dec();
}

pub fn add_published_track(kind: &str) {
    PROM_TRACK_PUBLISHED_CURRENT
        .with_label_values([kind])
        .add(1.0);
    TRACK_PUBLISHED_CURRENT.inc();
}

pub fn sub_published_track(kind: &str) {
    PROM_TRACK_PUBLISHED_CURRENT
        .with_label_values([kind])
        .sub(1.0);
    TRACK_PUBLISHED_CURRENT.dec();
}

pub fn add_packet_trailer_track(track: &livekit::TrackInfo) {
    update_packet_trailer_tracks(track, 1.0);
}

pub fn sub_packet_trailer_track(track: &livekit::TrackInfo) {
    update_packet_trailer_tracks(track, -1.0);
}

fn update_packet_trailer_tracks(track: &livekit::TrackInfo, delta: f64) {
    if track.r#type() != livekit::TrackType::Video {
        return;
    }

    let features = packet_trailer_feature_labels(&track.packet_trailer_features);
    if features.is_empty() {
        return;
    }

    PROM_TRACK_PACKET_TRAILER_CURRENT.add(delta);
    for feature in &features {
        PROM_TRACK_PACKET_TRAILER_BY_FEATURE_CURRENT
            .with_label_values([feature])
            .add(delta);
    }
}

fn packet_trailer_feature_labels(features: &[i32]) -> Vec<&'static str> {
    let mut labels = Vec::with_capacity(features.len());
    let mut seen = std::collections::HashSet::with_capacity(features.len());
    for feature in features {
        let mut label = "UNKNOWN";
        if let Ok(feature) = livekit::PacketTrailerFeature::try_from(*feature) {
            label = feature.as_str_name();
        }

        if !seen.insert(label) {
            continue;
        }
        labels.push(label);
    }
    labels
}

pub fn record_track_publish_attempt(kind: &str) {
    TRACK_PUBLISH_ATTEMPTS.inc();
    PROM_TRACK_PUBLISH_COUNTER
        .with_label_values([kind, "attempt"])
        .inc();
}

pub fn record_track_publish_success(kind: &str) {
    TRACK_PUBLISH_SUCCESS.inc();
    PROM_TRACK_PUBLISH_COUNTER
        .with_label_values([kind, "success"])
        .inc();
}

pub fn record_track_publish_cancels(kind: &str, num_cancels: i32) {
    TRACK_PUBLISH_CANCELS.add(num_cancels);
    PROM_TRACK_PUBLISH_COUNTER
        .with_label_values([kind, "cancel"])
        .add(u64::try_from(num_cancels).expect("counter cannot decrease in value"));
}

pub fn record_publish_time(
    country: &str,
    source: livekit::TrackSource,
    track_type: livekit::TrackType,
    d: Duration,
    sdk: livekit::client_info::Sdk,
    kind: livekit::participant_info::Kind,
) {
    record_pub_sub_stream_start_time("publish", country, source, track_type, d, sdk, kind, 1);
}

pub fn record_session_start_time(
    protocol_version: i64,
    warp: bool,
    sdk: livekit::client_info::Sdk,
    d: Duration,
) {
    // strconv.Itoa(protocolVersion), strconv.FormatBool(warp), sdk.String()
    PROM_SESSION_START_TIME
        .with_label_values([
            &protocol_version.to_string(),
            &warp.to_string(),
            sdk.as_str_name(),
        ])
        .observe(d.milliseconds() as f64);
}

pub fn record_session_duration(protocol_version: i64, d: Duration) {
    PROM_SESSION_DURATION
        .with_label_values([&protocol_version.to_string()])
        .observe(d.milliseconds() as f64);
}

pub fn record_track_subscribe_success(kind: &str) {
    // modify both current and total counters
    PROM_TRACK_SUBSCRIBED_CURRENT
        .with_label_values([kind])
        .add(1.0);
    TRACK_SUBSCRIBED_CURRENT.inc();

    PROM_TRACK_SUBSCRIBE_COUNTER
        .with_label_values(["success", ""])
        .inc();
    TRACK_SUBSCRIBE_SUCCESS.inc();
}

pub fn record_track_unsubscribed(kind: &str) {
    // unsubscribed modifies current counter, but we leave the total values alone since they
    // are used to compute rate
    PROM_TRACK_SUBSCRIBED_CURRENT
        .with_label_values([kind])
        .sub(1.0);
    TRACK_SUBSCRIBED_CURRENT.dec();
}

pub fn record_track_subscribe_attempt() {
    TRACK_SUBSCRIBE_ATTEMPTS.inc();
    PROM_TRACK_SUBSCRIBE_COUNTER
        .with_label_values(["attempt", ""])
        .inc();
}

/// `RecordTrackSubscribeFailure(err error, isUserError bool)`, with the error's text.
pub fn record_track_subscribe_failure(err: &str, is_user_error: bool) {
    PROM_TRACK_SUBSCRIBE_COUNTER
        .with_label_values(["failure", err])
        .inc();

    if is_user_error {
        TRACK_SUBSCRIBE_USER_ERROR.inc();
        TRACK_SUBSCRIBE_CANCELS.inc();
    }
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
    fn a_peer_connection_state_counts_under_its_transport() {
        let count = |transport: livekit::SignalTarget| {
            PROM_PEER_CONNECTION
                .with_label_values([transport.as_str_name(), "a state of this test"])
                .get()
        };
        let before = count(livekit::SignalTarget::Subscriber);
        record_peer_connection_state(livekit::SignalTarget::Subscriber, "a state of this test");
        record_peer_connection_state(livekit::SignalTarget::Subscriber, "a state of this test");
        assert_eq!(before + 2, count(livekit::SignalTarget::Subscriber));
        assert_eq!(
            "SUBSCRIBER",
            livekit::SignalTarget::Subscriber.as_str_name()
        );
    }

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
        let (before_zero, before_late) =
            (of("a rate of this test"), of("another rate of this test"));

        record_subscribe_pace_shadow(&HashMap::from([
            ("a rate of this test".to_string(), Duration::ZERO),
            ("another rate of this test".to_string(), SECOND + SECOND / 2),
        ]));

        let (after_zero, after_late) = (of("a rate of this test"), of("another rate of this test"));
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
    fn publish_times_cancels_and_sessions_count_under_their_labels() {
        let publish = || {
            PROM_PUB_SUB_TIME
                .with_label_values([
                    "publish",
                    "SCREEN_SHARE",
                    "VIDEO",
                    "a country of this test",
                    "GO",
                    "INGRESS",
                    "1",
                ])
                .snapshot()
        };
        let cancels = || {
            PROM_TRACK_PUBLISH_COUNTER
                .with_label_values(["a kind of this test", "cancel"])
                .get()
        };
        let start = || {
            PROM_SESSION_START_TIME
                .with_label_values(["9999", "false", "GO"])
                .snapshot()
        };
        let duration = || PROM_SESSION_DURATION.with_label_values(["9999"]).snapshot();
        let (before_publish, before_cancels, before_total) =
            (publish(), cancels(), TRACK_PUBLISH_CANCELS.load());
        let (before_start, before_duration) = (start(), duration());

        // 600 ms: above 500, not above 700
        record_publish_time(
            "a country of this test",
            livekit::TrackSource::ScreenShare,
            livekit::TrackType::Video,
            Duration::from_millis(600),
            livekit::client_info::Sdk::Go,
            livekit::participant_info::Kind::Ingress,
        );
        record_track_publish_cancels("a kind of this test", 2);
        // 150 ms: above 138.9..., not above 193.0...
        record_session_start_time(
            9999,
            false,
            livekit::client_info::Sdk::Go,
            Duration::from_millis(150),
        );
        // an hour: above 2639086.1..., not above 6164644.3...
        record_session_duration(9999, Duration::from_secs(3600));

        let after_publish = publish();
        assert_eq!(
            before_publish.le(700.0) - before_publish.le(500.0) + 1,
            after_publish.le(700.0) - after_publish.le(500.0)
        );
        assert_eq!(before_cancels + 2, cancels());
        assert!(TRACK_PUBLISH_CANCELS.load() >= before_total + 2);
        let after_start = start();
        assert_eq!(before_start.count + 1, after_start.count);
        assert_eq!(
            before_start.le(193.06977288832496) - before_start.le(138.94954943731375) + 1,
            after_start.le(193.06977288832496) - after_start.le(138.94954943731375)
        );
        let after_duration = duration();
        assert_eq!(before_duration.sum + 3_600_000.0, after_duration.sum);
        assert_eq!(
            before_duration.le(6.164644378251824e6) - before_duration.le(2.639086132660537e6) + 1,
            after_duration.le(6.164644378251824e6) - after_duration.le(2.639086132660537e6)
        );
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

    // Ported from LiveKit, pkg/telemetry/prometheus/rooms_test.go at v1.13.11: the original's
    // two tests, of the functions added for the telemetry service. They run one after the
    // other, as upstream's do, since both look at the same gauge. There is no `Init`.
    static PACKET_TRAILER_TESTS: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    fn gauge_value(gauge: &Gauge) -> f64 {
        gauge.get()
    }

    #[test]
    fn test_packet_trailer_track_metrics() {
        let _serial = PACKET_TRAILER_TESTS.lock();

        let track = livekit::TrackInfo {
            r#type: livekit::TrackType::Video as i32,
            packet_trailer_features: vec![
                livekit::PacketTrailerFeature::PtfUserTimestamp as i32,
                livekit::PacketTrailerFeature::PtfUserTimestamp as i32,
                livekit::PacketTrailerFeature::PtfFrameId as i32,
                livekit::PacketTrailerFeature::PtfUserData as i32,
                99,
                100,
            ],
            ..Default::default()
        };
        let by_feature = |feature: &str| {
            gauge_value(&PROM_TRACK_PACKET_TRAILER_BY_FEATURE_CURRENT.with_label_values([feature]))
        };

        let trailer_before = gauge_value(&PROM_TRACK_PACKET_TRAILER_CURRENT);
        let timestamp_before = by_feature("PTF_USER_TIMESTAMP");
        let frame_id_before = by_feature("PTF_FRAME_ID");
        let user_data_before = by_feature("PTF_USER_DATA");
        let unknown_before = by_feature("UNKNOWN");

        add_packet_trailer_track(&track);
        assert_eq!(
            trailer_before + 1.0,
            gauge_value(&PROM_TRACK_PACKET_TRAILER_CURRENT)
        );
        assert_eq!(timestamp_before + 1.0, by_feature("PTF_USER_TIMESTAMP"));
        assert_eq!(frame_id_before + 1.0, by_feature("PTF_FRAME_ID"));
        assert_eq!(user_data_before + 1.0, by_feature("PTF_USER_DATA"));
        assert_eq!(unknown_before + 1.0, by_feature("UNKNOWN"));

        sub_packet_trailer_track(&track);
        assert_eq!(
            trailer_before,
            gauge_value(&PROM_TRACK_PACKET_TRAILER_CURRENT)
        );
        assert_eq!(timestamp_before, by_feature("PTF_USER_TIMESTAMP"));
        assert_eq!(frame_id_before, by_feature("PTF_FRAME_ID"));
        assert_eq!(user_data_before, by_feature("PTF_USER_DATA"));
        assert_eq!(unknown_before, by_feature("UNKNOWN"));
    }

    #[test]
    fn test_packet_trailer_track_metrics_ignore_audio() {
        let _serial = PACKET_TRAILER_TESTS.lock();

        let before = gauge_value(&PROM_TRACK_PACKET_TRAILER_CURRENT);
        add_packet_trailer_track(&livekit::TrackInfo {
            r#type: livekit::TrackType::Audio as i32,
            packet_trailer_features: vec![livekit::PacketTrailerFeature::PtfUserData as i32],
            ..Default::default()
        });
        assert_eq!(before, gauge_value(&PROM_TRACK_PACKET_TRAILER_CURRENT));
    }

    // New for musivum: the counters of participants, tracks and subscriptions.
    #[test]
    fn participants_tracks_and_subscriptions_count_under_their_labels() {
        let gauge =
            |vec: &MetricVec<Gauge, 1>| vec.with_label_values(["a kind of this test"]).get();
        let counter = |kind: &str, state: &str| {
            PROM_TRACK_PUBLISH_COUNTER
                .with_label_values([kind, state])
                .get()
        };

        let published = gauge(&PROM_TRACK_PUBLISHED_CURRENT);
        add_published_track("a kind of this test");
        add_published_track("a kind of this test");
        sub_published_track("a kind of this test");
        assert_eq!(published + 1.0, gauge(&PROM_TRACK_PUBLISHED_CURRENT));

        let (attempts, successes) = (
            counter("a kind of this test", "attempt"),
            counter("a kind of this test", "success"),
        );
        record_track_publish_attempt("a kind of this test");
        record_track_publish_attempt("a kind of this test");
        record_track_publish_success("a kind of this test");
        assert_eq!(attempts + 2, counter("a kind of this test", "attempt"));
        assert_eq!(successes + 1, counter("a kind of this test", "success"));

        let subscribed = gauge(&PROM_TRACK_SUBSCRIBED_CURRENT);
        record_track_subscribe_success("a kind of this test");
        record_track_subscribe_success("a kind of this test");
        record_track_unsubscribed("a kind of this test");
        assert_eq!(subscribed + 1.0, gauge(&PROM_TRACK_SUBSCRIBED_CURRENT));

        let failures = || {
            PROM_TRACK_SUBSCRIBE_COUNTER
                .with_label_values(["failure", "an error of this test"])
                .get()
        };
        let (before, user_errors) = (failures(), TRACK_SUBSCRIBE_USER_ERROR.load());
        record_track_subscribe_failure("an error of this test", true);
        record_track_subscribe_failure("an error of this test", false);
        assert_eq!(before + 2, failures());
        assert!(TRACK_SUBSCRIBE_USER_ERROR.load() > user_errors);
    }
}
