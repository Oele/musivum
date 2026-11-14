// Ported from LiveKit, pkg/telemetry/prometheus/packets.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the functions that ported code calls are here,
// more are added as they are needed. The collectors are musivum's own (see the module's
// description) and are statics, so there is no `initPacketStats` to call first; the constant
// labels `node_id` and `node_type` are left to whoever serves the metrics.

use super::{Counter, Gauge, Histogram, MetricFamily, MetricVec};
use crate::go::atomic;
use crate::protocol::livekit;

/// `type Direction string` of the original, with its two values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Direction {
    Incoming,
    Outgoing,
}

impl Direction {
    /// `string(direction)`
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Incoming => "incoming",
            Direction::Outgoing => "outgoing",
        }
    }
}

// read by the node's statistics upstream (`node.go`)
pub(super) static FORWARD_LATENCY: atomic::Uint32 = atomic::Uint32::new(0);

pub static PROM_KEY_FRAME_TOTAL: MetricVec<Counter, 1> =
    MetricVec::new(["direction"], Counter::new);
pub static PROM_FORWARD_LATENCY: Gauge = Gauge::new();
pub static PROM_FORWARD_LATENCY_HIST: Histogram<10> = Histogram::new(
    // 50us, 100us, 250us, 500us, 1ms, 2ms, 3ms, 5ms, 10ms, 20ms
    [
        50.0 * 1000.0,
        100.0 * 1000.0,
        250.0 * 1000.0,
        500.0 * 1000.0,
        1.0 * 1000.0 * 1000.0,
        2.0 * 1000.0 * 1000.0,
        3.0 * 1000.0 * 1000.0,
        5.0 * 1000.0 * 1000.0,
        10.0 * 1000.0 * 1000.0,
        20.0 * 1000.0 * 1000.0,
    ],
);
pub static PROM_RTX_CAPPED_TOTAL: MetricVec<Counter, 1> = MetricVec::new(["kind"], Counter::new);
// promFECLabels
pub static PROM_FEC_PACKET_TOTAL: MetricVec<Counter, 2> =
    MetricVec::new(["direction", "type"], Counter::new);
pub static PROM_FEC_PACKET_BYTES: MetricVec<Counter, 2> =
    MetricVec::new(["direction", "type"], Counter::new);

// Added for the telemetry service (`stats.go`, `events.go`, `statsconn.go`), from here on.
pub(super) static BYTES_IN: atomic::Uint64 = atomic::Uint64::new(0);
pub(super) static BYTES_OUT: atomic::Uint64 = atomic::Uint64::new(0);
pub(super) static PACKETS_IN: atomic::Uint64 = atomic::Uint64::new(0);
pub(super) static PACKETS_OUT: atomic::Uint64 = atomic::Uint64::new(0);
pub(super) static NACK_TOTAL: atomic::Uint64 = atomic::Uint64::new(0);
pub(super) static RETRANSMIT_BYTES: atomic::Uint64 = atomic::Uint64::new(0);
pub(super) static RETRANSMIT_PACKETS: atomic::Uint64 = atomic::Uint64::new(0);
pub(super) static PARTICIPANT_SIGNAL_CONNECTED: atomic::Uint64 = atomic::Uint64::new(0);
pub(super) static PARTICIPANT_RTC_INIT: atomic::Uint64 = atomic::Uint64::new(0);
pub(super) static PARTICIPANT_RTC_CONNECTED: atomic::Uint64 = atomic::Uint64::new(0);
pub(super) static PARTICIPANT_RTC_ACTIVE: atomic::Uint64 = atomic::Uint64::new(0);
// Added for the participant (participant.go).
pub(super) static PARTICIPANT_RTC_CANCELED: atomic::Uint64 = atomic::Uint64::new(0);

// promPacketLabels
pub static PROM_PACKET_TOTAL: MetricVec<Counter, 3> =
    MetricVec::new(["direction", "transmission", "country"], Counter::new);
pub static PROM_PACKET_BYTES: MetricVec<Counter, 3> =
    MetricVec::new(["direction", "transmission", "country"], Counter::new);
// promRTCPLabels
pub static PROM_NACK_TOTAL: MetricVec<Counter, 2> =
    MetricVec::new(["direction", "country"], Counter::new);
pub static PROM_PLI_TOTAL: MetricVec<Counter, 2> =
    MetricVec::new(["direction", "country"], Counter::new);
pub static PROM_FIR_TOTAL: MetricVec<Counter, 2> =
    MetricVec::new(["direction", "country"], Counter::new);
// promStreamLabels
pub static PROM_PACKET_LOSS_TOTAL: MetricVec<Counter, 4> =
    MetricVec::new(["direction", "source", "type", "country"], Counter::new);
pub static PROM_PACKET_LOSS: MetricVec<Histogram<10>, 4> =
    MetricVec::new(["direction", "source", "type", "country"], || {
        Histogram::new([0.0, 0.1, 0.3, 0.5, 0.7, 1.0, 5.0, 10.0, 40.0, 100.0])
    });
pub static PROM_PACKET_OUT_OF_ORDER_TOTAL: MetricVec<Counter, 4> =
    MetricVec::new(["direction", "source", "type", "country"], Counter::new);
pub static PROM_PACKET_OUT_OF_ORDER: MetricVec<Histogram<10>, 4> =
    MetricVec::new(["direction", "source", "type", "country"], || {
        Histogram::new([0.0, 0.1, 0.3, 0.5, 0.7, 1.0, 5.0, 10.0, 40.0, 100.0])
    });
pub static PROM_JITTER: MetricVec<Histogram<9>, 4> =
    MetricVec::new(["direction", "source", "type", "country"], || {
        // 1ms, 10ms, 30ms, 50ms, 70ms, 100ms, 300ms, 600ms, 1s
        Histogram::new([
            1000.0, 10000.0, 30000.0, 50000.0, 70000.0, 100000.0, 300000.0, 600000.0, 1000000.0,
        ])
    });
pub static PROM_RTT: MetricVec<Histogram<10>, 4> =
    MetricVec::new(["direction", "source", "type", "country"], || {
        Histogram::new([
            50.0, 100.0, 150.0, 200.0, 250.0, 500.0, 750.0, 1000.0, 5000.0, 10000.0,
        ])
    });
pub static PROM_PARTICIPANT_JOIN: MetricVec<Counter, 3> =
    MetricVec::new(["state", "warp", "sdk"], Counter::new);
pub static PROM_CONNECTIONS: MetricVec<Gauge, 1> = MetricVec::new(["kind"], Gauge::new);

// The collectors of this file under the names upstream registers them with.
pub(super) fn collect(families: &mut Vec<MetricFamily>) {
    families.push(MetricFamily::of_vec(
        "livekit_keyframe_total",
        "Key frames received from publishers (incoming) and forwarded to subscribers (outgoing, once per subscriber).",
        &PROM_KEY_FRAME_TOTAL,
    ));
    families.push(MetricFamily::of(
        "livekit_forward_latency",
        "",
        &PROM_FORWARD_LATENCY,
    ));
    families.push(MetricFamily::of(
        "livekit_forward_latency_ns",
        "",
        &PROM_FORWARD_LATENCY_HIST,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_rtx_capped_total",
        "NACKed packets not retransmitted because the down track was over its retransmission budget.",
        &PROM_RTX_CAPPED_TOTAL,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_fec_total",
        "",
        &PROM_FEC_PACKET_TOTAL,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_fec_bytes",
        "",
        &PROM_FEC_PACKET_BYTES,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_packet_total",
        "",
        &PROM_PACKET_TOTAL,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_packet_bytes",
        "",
        &PROM_PACKET_BYTES,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_nack_total",
        "",
        &PROM_NACK_TOTAL,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_pli_total",
        "",
        &PROM_PLI_TOTAL,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_fir_total",
        "",
        &PROM_FIR_TOTAL,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_packet_loss_total",
        "",
        &PROM_PACKET_LOSS_TOTAL,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_packet_loss_percent",
        "",
        &PROM_PACKET_LOSS,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_packet_out_of_order_total",
        "",
        &PROM_PACKET_OUT_OF_ORDER_TOTAL,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_packet_out_of_order_percent",
        "",
        &PROM_PACKET_OUT_OF_ORDER,
    ));
    families.push(MetricFamily::of_vec("livekit_jitter_us", "", &PROM_JITTER));
    families.push(MetricFamily::of_vec("livekit_rtt_ms", "", &PROM_RTT));
    families.push(MetricFamily::of_vec(
        "livekit_participant_join_total",
        "",
        &PROM_PARTICIPANT_JOIN,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_connection_total",
        "",
        &PROM_CONNECTIONS,
    ));
}

pub fn record_fec_upstream(
    received: usize,
    recovered: usize,
    discarded: usize,
    bytes_received: u64,
) {
    if received > 0 {
        PROM_FEC_PACKET_TOTAL
            .with_label_values([Direction::Incoming.as_str(), "received"])
            .add(received as u64);
    }
    if recovered > 0 {
        PROM_FEC_PACKET_TOTAL
            .with_label_values([Direction::Incoming.as_str(), "recovered"])
            .add(recovered as u64);
    }
    if discarded > 0 {
        PROM_FEC_PACKET_TOTAL
            .with_label_values([Direction::Incoming.as_str(), "discarded"])
            .add(discarded as u64);
    }
    if bytes_received > 0 {
        PROM_FEC_PACKET_BYTES
            .with_label_values([Direction::Incoming.as_str(), "received"])
            .add(bytes_received);
    }
}

pub fn record_rtx_capped(kind: &str, packets: u32) {
    PROM_RTX_CAPPED_TOTAL
        .with_label_values([kind])
        .add(packets as u64);
}

pub fn record_forward_latency_sample(forward_latency: i64) {
    PROM_FORWARD_LATENCY_HIST.observe(forward_latency as f64);
}

pub fn record_forward_latency(p90: u32) {
    FORWARD_LATENCY.store(p90);
    PROM_FORWARD_LATENCY.set(p90 as f64);
}

pub fn increment_key_frames(direction: Direction) {
    PROM_KEY_FRAME_TOTAL
        .with_label_values([direction.as_str()])
        .inc();
}

// Added for the telemetry service (`stats.go`, `events.go`, `statsconn.go`), from here on,
// in upstream's order.

/// `type TransmissionType string` of the original, with its two values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TransmissionType {
    TransmissionInitial,
    TransmissionRetransmit,
}

impl TransmissionType {
    /// `string(transmission)`
    pub fn as_str(self) -> &'static str {
        match self {
            TransmissionType::TransmissionInitial => "initial",
            TransmissionType::TransmissionRetransmit => "retransmit",
        }
    }
}

pub fn increment_packets(country: &str, direction: Direction, count: u64, retransmit: bool) {
    let transmission = if retransmit {
        TransmissionType::TransmissionRetransmit
    } else {
        TransmissionType::TransmissionInitial
    };
    PROM_PACKET_TOTAL
        .with_label_values([direction.as_str(), transmission.as_str(), country])
        .add(count);

    if direction == Direction::Incoming {
        PACKETS_IN.add(count);
    } else {
        PACKETS_OUT.add(count);
        if retransmit {
            RETRANSMIT_PACKETS.add(count);
        }
    }
}

pub fn increment_bytes(country: &str, direction: Direction, count: u64, retransmit: bool) {
    let transmission = if retransmit {
        TransmissionType::TransmissionRetransmit
    } else {
        TransmissionType::TransmissionInitial
    };
    PROM_PACKET_BYTES
        .with_label_values([direction.as_str(), transmission.as_str(), country])
        .add(count);

    if direction == Direction::Incoming {
        BYTES_IN.add(count);
    } else {
        BYTES_OUT.add(count);
        if retransmit {
            RETRANSMIT_BYTES.add(count);
        }
    }
}

pub fn increment_rtcp(country: &str, direction: Direction, nack: u32, pli: u32, fir: u32) {
    if nack > 0 {
        PROM_NACK_TOTAL
            .with_label_values([direction.as_str(), country])
            .add(nack as u64);
        NACK_TOTAL.add(nack as u64);
    }
    if pli > 0 {
        PROM_PLI_TOTAL
            .with_label_values([direction.as_str(), country])
            .add(pli as u64);
    }
    if fir > 0 {
        PROM_FIR_TOTAL
            .with_label_values([direction.as_str(), country])
            .add(fir as u64);
    }
}

pub fn record_packet_loss(
    country: &str,
    direction: Direction,
    track_source: livekit::TrackSource,
    track_type: livekit::TrackType,
    lost: u32,
    total: u32,
) {
    let labels = [
        direction.as_str(),
        track_source.as_str_name(),
        track_type.as_str_name(),
        country,
    ];
    if total > 0 {
        PROM_PACKET_LOSS
            .with_label_values(labels)
            .observe(lost as f64 / total as f64 * 100.0);
    }
    if lost > 0 {
        PROM_PACKET_LOSS_TOTAL
            .with_label_values(labels)
            .add(lost as u64);
    }
}

pub fn record_packet_out_of_order(
    country: &str,
    direction: Direction,
    track_source: livekit::TrackSource,
    track_type: livekit::TrackType,
    ooo: u32,
    total: u32,
) {
    let labels = [
        direction.as_str(),
        track_source.as_str_name(),
        track_type.as_str_name(),
        country,
    ];
    if total > 0 {
        PROM_PACKET_OUT_OF_ORDER
            .with_label_values(labels)
            .observe(ooo as f64 / total as f64 * 100.0);
    }
    if ooo > 0 {
        PROM_PACKET_OUT_OF_ORDER_TOTAL
            .with_label_values(labels)
            .add(ooo as u64);
    }
}

pub fn record_jitter(
    country: &str,
    direction: Direction,
    track_source: livekit::TrackSource,
    track_type: livekit::TrackType,
    jitter: u32,
) {
    if jitter > 0 {
        PROM_JITTER
            .with_label_values([
                direction.as_str(),
                track_source.as_str_name(),
                track_type.as_str_name(),
                country,
            ])
            .observe(jitter as f64);
    }
}

pub fn record_rtt(
    country: &str,
    direction: Direction,
    track_source: livekit::TrackSource,
    track_type: livekit::TrackType,
    rtt: u32,
) {
    if rtt > 0 {
        PROM_RTT
            .with_label_values([
                direction.as_str(),
                track_source.as_str_name(),
                track_type.as_str_name(),
                country,
            ])
            .observe(rtt as f64);
    }
}

pub fn increment_participant_join(join: u32) {
    if join > 0 {
        PARTICIPANT_SIGNAL_CONNECTED.add(u64::from(join));
        PROM_PARTICIPANT_JOIN
            .with_label_values(["signal_connected", "", ""])
            .add(u64::from(join));
    }
}

pub fn increment_participant_join_fail(fail: u32) {
    if fail > 0 {
        PROM_PARTICIPANT_JOIN
            .with_label_values(["signal_failed", "", ""])
            .add(u64::from(fail));
    }
}

pub fn increment_participant_join_validation_fail(validation_fail: u32) {
    if validation_fail > 0 {
        PROM_PARTICIPANT_JOIN
            .with_label_values(["signal_validation_failed", "", ""])
            .add(u64::from(validation_fail));
    }
}

pub fn increment_participant_join_upgrade_fail(upgrade_fail: u32) {
    if upgrade_fail > 0 {
        PROM_PARTICIPANT_JOIN
            .with_label_values(["signal_upgrade_failed", "", ""])
            .add(u64::from(upgrade_fail));
    }
}

pub fn increment_participant_join_write_initial_response_fail(write_initial_response_fail: u32) {
    if write_initial_response_fail > 0 {
        PROM_PARTICIPANT_JOIN
            .with_label_values(["signal_write_initial_response_failed", "", ""])
            .add(u64::from(write_initial_response_fail));
    }
}

pub fn increment_participant_rtc_init(init: u32) {
    if init > 0 {
        PARTICIPANT_RTC_INIT.add(u64::from(init));
        PROM_PARTICIPANT_JOIN
            .with_label_values(["rtc_init", "", ""])
            .add(u64::from(init));
    }
}

pub fn increment_participant_rtc_connected(connected: u32, sdk: livekit::client_info::Sdk) {
    if connected > 0 {
        PARTICIPANT_RTC_CONNECTED.add(connected as u64);
        PROM_PARTICIPANT_JOIN
            .with_label_values(["rtc_connected", "", sdk.as_str_name()])
            .add(connected as u64);
    }
}

pub fn increment_participant_rtc_active(active: u32, warp: bool) {
    if active > 0 {
        PARTICIPANT_RTC_ACTIVE.add(active as u64);
        PROM_PARTICIPANT_JOIN
            .with_label_values(["rtc_active", if warp { "true" } else { "false" }, ""])
            .add(active as u64);
    }
}

pub fn increment_participant_rtc_canceled(
    canceled: u64,
    warp: bool,
    sdk: livekit::client_info::Sdk,
) {
    if canceled > 0 {
        PARTICIPANT_RTC_CANCELED.add(canceled);
        // strconv.FormatBool(warp), sdk.String()
        PROM_PARTICIPANT_JOIN
            .with_label_values(["rtc_canceled", &warp.to_string(), sdk.as_str_name()])
            .add(canceled);
    }
}

// todo: check if need to record warp to rtcSucc/Failure
pub fn increment_participant_rtc_success(success: u64, warp: bool, sdk: livekit::client_info::Sdk) {
    if success > 0 {
        PROM_PARTICIPANT_JOIN
            .with_label_values(["rtc_success", &warp.to_string(), sdk.as_str_name()])
            .add(success);
    }
}

pub fn increment_participant_rtc_failure(failure: u64, warp: bool, sdk: livekit::client_info::Sdk) {
    if failure > 0 {
        PROM_PARTICIPANT_JOIN
            .with_label_values(["rtc_failure", &warp.to_string(), sdk.as_str_name()])
            .add(failure);
    }
}

pub fn add_connection(direction: Direction) {
    PROM_CONNECTIONS
        .with_label_values([direction.as_str()])
        .add(1.0);
}

pub fn sub_connection(direction: Direction) {
    PROM_CONNECTIONS
        .with_label_values([direction.as_str()])
        .sub(1.0);
}

// The original has tests of its own (`packets_test.go`) for functions that are not here yet.
// These are new. The statics are shared with every test of the crate that reports to them
// while this one runs, so it looks at what only it changes.
#[cfg(test)]
mod tests {
    use super::super::HistogramSnapshot;
    use super::*;

    #[test]
    fn fec_from_publishers_counts_by_type_and_skips_zeros() {
        let count = |t: &str| {
            PROM_FEC_PACKET_TOTAL
                .with_label_values([Direction::Incoming.as_str(), t])
                .get()
        };
        let bytes = || {
            PROM_FEC_PACKET_BYTES
                .with_label_values([Direction::Incoming.as_str(), "received"])
                .get()
        };
        let before = (
            count("received"),
            count("recovered"),
            count("discarded"),
            bytes(),
        );
        record_fec_upstream(5, 2, 1, 1200);
        record_fec_upstream(3, 0, 0, 0);
        let after = (
            count("received"),
            count("recovered"),
            count("discarded"),
            bytes(),
        );
        assert!(after.0 >= before.0 + 8);
        assert!(after.1 >= before.1 + 2);
        assert!(after.2 > before.2);
        assert!(after.3 >= before.3 + 1200);
    }

    #[test]
    fn capped_retransmissions_count_under_their_kind() {
        let count = || {
            PROM_RTX_CAPPED_TOTAL
                .with_label_values(["a kind of this test"])
                .get()
        };
        let before = count();
        record_rtx_capped("a kind of this test", 3);
        record_rtx_capped("a kind of this test", 4);
        assert_eq!(before + 7, count());
    }

    #[test]
    fn a_forward_latency_sample_counts_in_its_bucket() {
        let before = PROM_FORWARD_LATENCY_HIST.snapshot();
        // 15 ms: above 10 ms, not above 20 ms
        record_forward_latency_sample(15_000_000);
        // 1 s: above the last bound
        record_forward_latency_sample(1_000_000_000);
        let after = PROM_FORWARD_LATENCY_HIST.snapshot();

        let in_bucket =
            |snapshot: &HistogramSnapshot| snapshot.le(20_000_000.0) - snapshot.le(10_000_000.0);
        let above = |snapshot: &HistogramSnapshot| snapshot.count - snapshot.le(20_000_000.0);
        assert!(in_bucket(&after) > in_bucket(&before));
        assert!(above(&after) > above(&before));
        assert!(after.sum >= before.sum + 1_015_000_000.0);
    }

    #[test]
    fn the_forward_latency_is_what_was_recorded_last() {
        // Other tests record their own (a few milliseconds at most), so try until this one
        // was the last to.
        let p90 = u32::MAX - 7;
        for _ in 0..1000 {
            record_forward_latency(p90);
            if FORWARD_LATENCY.load() == p90 && PROM_FORWARD_LATENCY.get() == p90 as f64 {
                return;
            }
            std::thread::yield_now();
        }
        panic!("the recorded forward latency was never read back");
    }

    #[test]
    fn a_key_frame_counts_under_its_direction() {
        let count = |direction: Direction| {
            PROM_KEY_FRAME_TOTAL
                .with_label_values([direction.as_str()])
                .get()
        };
        let (incoming, outgoing) = (count(Direction::Incoming), count(Direction::Outgoing));
        increment_key_frames(Direction::Outgoing);
        increment_key_frames(Direction::Outgoing);
        increment_key_frames(Direction::Incoming);
        assert!(count(Direction::Outgoing) >= outgoing + 2);
        assert!(count(Direction::Incoming) > incoming);
    }

    // New for musivum: the counters added for the telemetry service. Countries and kinds of
    // their own keep them apart from what other tests count.
    #[test]
    fn packets_bytes_and_rtcp_count_under_their_labels() {
        let packets = |d: Direction, t: &str| {
            PROM_PACKET_TOTAL
                .with_label_values([d.as_str(), t, "a country of this test"])
                .get()
        };
        let bytes = |d: Direction, t: &str| {
            PROM_PACKET_BYTES
                .with_label_values([d.as_str(), t, "a country of this test"])
                .get()
        };
        let retransmitted = RETRANSMIT_PACKETS.load();
        increment_packets("a country of this test", Direction::Outgoing, 5, true);
        increment_packets("a country of this test", Direction::Incoming, 7, false);
        increment_bytes("a country of this test", Direction::Outgoing, 1200, false);
        assert_eq!(5, packets(Direction::Outgoing, "retransmit"));
        assert_eq!(7, packets(Direction::Incoming, "initial"));
        assert_eq!(1200, bytes(Direction::Outgoing, "initial"));
        assert!(RETRANSMIT_PACKETS.load() >= retransmitted + 5);

        increment_rtcp("a country of this test", Direction::Incoming, 2, 0, 1);
        let rtcp = |vec: &MetricVec<Counter, 2>| {
            vec.with_label_values([Direction::Incoming.as_str(), "a country of this test"])
                .get()
        };
        assert_eq!(
            (2, 0, 1),
            (
                rtcp(&PROM_NACK_TOTAL),
                rtcp(&PROM_PLI_TOTAL),
                rtcp(&PROM_FIR_TOTAL)
            )
        );
    }

    #[test]
    fn loss_jitter_and_rtt_are_observed_in_percent_and_as_they_are() {
        let labels = [
            Direction::Outgoing.as_str(),
            "CAMERA",
            "VIDEO",
            "a country of this test",
        ];
        let record = || {
            record_packet_loss(
                "a country of this test",
                Direction::Outgoing,
                livekit::TrackSource::Camera,
                livekit::TrackType::Video,
                1,
                4,
            );
            record_packet_out_of_order(
                "a country of this test",
                Direction::Outgoing,
                livekit::TrackSource::Camera,
                livekit::TrackType::Video,
                0,
                0,
            );
            record_jitter(
                "a country of this test",
                Direction::Outgoing,
                livekit::TrackSource::Camera,
                livekit::TrackType::Video,
                20000,
            );
            record_rtt(
                "a country of this test",
                Direction::Outgoing,
                livekit::TrackSource::Camera,
                livekit::TrackType::Video,
                0,
            );
        };
        record();
        let loss = PROM_PACKET_LOSS.with_label_values(labels).snapshot();
        assert_eq!((1, 25.0), (loss.count, loss.sum));
        assert_eq!(1, PROM_PACKET_LOSS_TOTAL.with_label_values(labels).get());
        // nothing out of order of nothing, and no RTT: not observed
        assert_eq!(
            0,
            PROM_PACKET_OUT_OF_ORDER
                .with_label_values(labels)
                .snapshot()
                .count
        );
        assert_eq!(
            0,
            PROM_PACKET_OUT_OF_ORDER_TOTAL
                .with_label_values(labels)
                .get()
        );
        assert_eq!(0, PROM_RTT.with_label_values(labels).snapshot().count);
        let jitter = PROM_JITTER.with_label_values(labels).snapshot();
        assert_eq!((1, 20000.0), (jitter.count, jitter.sum));
    }

    #[test]
    fn rtc_outcomes_count_under_their_state_and_skip_zeros() {
        let count = |state: &str| {
            PROM_PARTICIPANT_JOIN
                .with_label_values([state, "true", "UNITY"])
                .get()
        };
        let before = (
            count("rtc_canceled"),
            count("rtc_success"),
            count("rtc_failure"),
            PARTICIPANT_RTC_CANCELED.load(),
        );
        increment_participant_rtc_canceled(2, true, livekit::client_info::Sdk::Unity);
        increment_participant_rtc_canceled(0, true, livekit::client_info::Sdk::Unity);
        increment_participant_rtc_success(3, true, livekit::client_info::Sdk::Unity);
        increment_participant_rtc_failure(4, true, livekit::client_info::Sdk::Unity);
        increment_participant_rtc_failure(0, true, livekit::client_info::Sdk::Unity);
        // the participants of other tests may count under these labels too
        assert!(count("rtc_canceled") >= before.0 + 2);
        assert!(count("rtc_success") >= before.1 + 3);
        assert!(count("rtc_failure") >= before.2 + 4);
        assert!(PARTICIPANT_RTC_CANCELED.load() >= before.3 + 2);
        assert_eq!("UNITY", livekit::client_info::Sdk::Unity.as_str_name());
    }

    #[test]
    fn participants_joining_and_connections_count() {
        let join = |state: &str, warp: &str, sdk: &str| {
            PROM_PARTICIPANT_JOIN
                .with_label_values([state, warp, sdk])
                .get()
        };
        let (connected, active) = (
            join("rtc_connected", "", "GO"),
            join("rtc_active", "true", ""),
        );
        increment_participant_rtc_connected(1, livekit::client_info::Sdk::Go);
        increment_participant_rtc_connected(0, livekit::client_info::Sdk::Go);
        increment_participant_rtc_active(2, true);
        assert!(join("rtc_connected", "", "GO") > connected);
        assert!(join("rtc_active", "true", "") >= active + 2);

        // the connections of the other tests come and go, these two do not stay
        add_connection(Direction::Incoming);
        sub_connection(Direction::Incoming);
    }
}
