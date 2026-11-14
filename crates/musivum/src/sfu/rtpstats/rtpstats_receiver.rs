// Ported from LiveKit, pkg/sfu/rtpstats/rtpstats_receiver.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the lock is the outer type's and everything else,
// with the `rtpStatsBase` the original embeds, is in `RtpStatsReceiverInner` (see
// `rtpstats_base_lite.rs`): the public methods take the lock and what the original calls with
// the lock held are methods of the inner struct; the methods inherited from `rtpStatsBase`
// and `rtpStatsBaseLite` are forwarded; `lockedRTPStatsReceiverLogEncoder` is `Debug` of the
// inner struct and `MarshalLogObject` is `Debug` of the outer one, which takes the lock;
// `receiverUpdateLoggingFields` does not hold the statistics object, it is given it when a
// line is written, and `zap.Inline(rulf)` is `rulf.log(level, message, r, more)`;
// `RTPFlowUnhandledReason` is an enum whose `Debug` is its `String()`; the sender report
// state is taken and kept by value (see `rtpstats_base.rs`); `LastSenderReportTime` gives the
// zero `Time` where the original gives a zero `time.Time`.

use std::fmt;

use parking_lot::RwLock;

use crate::go::time::{self, Duration, Time};
use crate::mediatransportutil::NtpTime;
use crate::mediatransportutil::latency::{OWD_ESTIMATOR_PARAMS_DEFAULT, OwdEstimator};
use crate::mediatransportutil::utils::{WrapAround, WrapAroundParams, WrapAroundUpdateResult};
use crate::protocol::livekit;
use crate::protocol::logger::Level;
use crate::protocol::utils::Bitmap;
use crate::protocol::utils::mono;
use crate::webrtc::rtcp::reception_report::ReceptionReport;
use crate::{debugw, infow, warnw};

use super::rtpstats_base::{
    RtpDeltaInfo, RtpStatsBase, WrappedRtcpSenderReportStateLogger, forward_rtp_stats_base,
    init_snapshot,
};
use super::rtpstats_base_lite::{
    C_FIRST_SNAPSHOT_ID, C_NUM_SEQUENCE_NUMBERS, RtpStatsParams, forward_rtp_stats_base_lite,
    get_packets_expected, infow_logging_fields, should_log,
};

const C_HISTORY_SIZE: usize = 8192;

// number of seconds the current report RTP timestamp can be off from expected RTP timestamp
const C_REPORT_SLACK: f64 = 60.0;

const C_TS_JUMP_TOO_HIGH_FACTOR: f64 = 1.5;

pub(super) const RESTART_THRESHOLD: usize = 5;

// ---------------------------------------------------------------------

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum RtpFlowUnhandledReason {
    #[default]
    None,
    Ended,
    Unconfigured,
    PaddingOnly,
    PreStartTimestamp,
    OldTimestamp,
    PreStartSequenceNumber,
    OldSequenceNumber,
    Restart,
}

impl fmt::Display for RtpFlowUnhandledReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RtpFlowUnhandledReason::None => "NONE",
            RtpFlowUnhandledReason::Ended => "ENDED",
            RtpFlowUnhandledReason::Unconfigured => "UNCONFIGURED",
            RtpFlowUnhandledReason::PaddingOnly => "PADDING_ONLY",
            RtpFlowUnhandledReason::PreStartTimestamp => "PRE_START_TIMESTAMP",
            RtpFlowUnhandledReason::OldTimestamp => "OLD_TIMESTAMP",
            RtpFlowUnhandledReason::PreStartSequenceNumber => "PRE_START_SEQUENCE_NUMBER",
            RtpFlowUnhandledReason::OldSequenceNumber => "OLD_SEQUENCE_NUMBER",
            RtpFlowUnhandledReason::Restart => "RESTART",
        })
    }
}

impl fmt::Debug for RtpFlowUnhandledReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RtpFlowState {
    pub unhandled_reason: RtpFlowUnhandledReason,

    pub loss_start_inclusive: u64,
    pub loss_end_exclusive: u64,

    pub is_duplicate: bool,
    pub is_out_of_order: bool,

    pub ext_sequence_number: u64,
    pub ext_timestamp: u64,
}

// ---------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Packet {
    pub(super) sequence_number: u16,
    pub(super) timestamp: u32,
}

// ---------------------------------------------------------------------

struct ReceiverUpdateLoggingFields {
    packet_time: i64,
    sequence_number: u16,
    timestamp: u32,
    marker: bool,
    hdr_size: usize,
    payload_size: usize,
    padding_size: usize,
    res_sn: WrapAroundUpdateResult<u64>,
    gap_sn: i64,
    res_ts: WrapAroundUpdateResult<u64>,
    gap_ts: i64,
    sn_rollover_count: i32,
    expected_ts_jump: i64,
    ts_rollover_count: i32,
    time_since_highest: i64,
}

impl ReceiverUpdateLoggingFields {
    // A line with what `MarshalLogObject` of the original adds, which `zap.Inline(rulf)`
    // writes into the line itself, and then `more`.
    fn log(
        &self,
        level: Level,
        msg: &str,
        rtp_stats: &RtpStatsReceiverInner,
        more: &[(&str, &dyn fmt::Debug)],
    ) {
        let logger = &rtp_stats.rtp_stats_base.rtp_stats_base_lite.logger;
        if !logger.enabled(level) {
            return;
        }

        let packet_time = Time::unix(0, self.packet_time);
        let time_since_highest = Duration(self.time_since_highest);
        let mut kvs: Vec<(&str, &dyn fmt::Debug)> = vec![
            ("resSN", &self.res_sn),
            ("gapSN", &self.gap_sn),
            ("resTS", &self.res_ts),
            ("gapTS", &self.gap_ts),
            ("snRolloverCount", &self.sn_rollover_count),
            ("expectedTSJump", &self.expected_ts_jump),
            ("tsRolloverCount", &self.ts_rollover_count),
            ("packetTime", &packet_time),
            ("timeSinceHighest", &time_since_highest),
            ("sequenceNumber", &self.sequence_number),
            ("timestamp", &self.timestamp),
            ("marker", &self.marker),
            ("hdrSize", &self.hdr_size),
            ("payloadSize", &self.payload_size),
            ("paddingSize", &self.padding_size),
            ("rtpStats", rtp_stats),
        ];
        kvs.extend_from_slice(more);
        logger.log(level, msg, None, &kvs);
    }
}

// ---------------------------------------------------------------------

pub struct RtpStatsReceiver {
    pub(super) lock: RwLock<RtpStatsReceiverInner>,
}

// What the lock guards: all of `RTPStatsReceiver` upstream.
pub(super) struct RtpStatsReceiverInner {
    pub(super) rtp_stats_base: RtpStatsBase,

    pub(super) sequence_number: WrapAround<u16, u64>,

    pub(super) ts_rollover_threshold: i64,
    pub(super) timestamp: WrapAround<u32, u64>,

    pub(super) history: Bitmap<u64>,

    pub(super) propagation_delay_estimator: OwdEstimator,

    pub(super) clock_skew_count: i64,
    pub(super) clock_skew_media_path_count: i64,
    pub(super) out_of_order_sender_report_count: i64,
    pub(super) time_reversed_count: i64,

    pub(super) packets_dropped_pre_start_timestamp: i64,
    pub(super) packets_dropped_old_timestamp: i64,
    pub(super) packets_dropped_pre_start_sequence_number: i64,
    pub(super) packets_dropped_old_sequence_number: i64,

    pub(super) restart_packets_buf: [Packet; RESTART_THRESHOLD],
    pub(super) restart_packets_n: usize,
}

forward_rtp_stats_base_lite!(RtpStatsReceiver, rtp_stats_base.rtp_stats_base_lite);
forward_rtp_stats_base!(RtpStatsReceiver, rtp_stats_base);

impl RtpStatsReceiver {
    pub fn new(params: RtpStatsParams) -> Self {
        Self {
            lock: RwLock::new(RtpStatsReceiverInner {
                rtp_stats_base: RtpStatsBase::new(params),
                sequence_number: WrapAround::new(WrapAroundParams {
                    is_restart_allowed: false,
                }),
                ts_rollover_threshold: 0,
                timestamp: WrapAround::new(WrapAroundParams {
                    is_restart_allowed: false,
                }),
                history: Bitmap::new(C_HISTORY_SIZE),
                propagation_delay_estimator: OwdEstimator::new(OWD_ESTIMATOR_PARAMS_DEFAULT),
                clock_skew_count: 0,
                clock_skew_media_path_count: 0,
                out_of_order_sender_report_count: 0,
                time_reversed_count: 0,
                packets_dropped_pre_start_timestamp: 0,
                packets_dropped_old_timestamp: 0,
                packets_dropped_pre_start_sequence_number: 0,
                packets_dropped_old_sequence_number: 0,
                restart_packets_buf: [Packet::default(); RESTART_THRESHOLD],
                restart_packets_n: 0,
            }),
        }
    }

    pub fn set_clock_rate(&self, clock_rate: u32) {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        r.ts_rollover_threshold = (1i64 << 31) * 1_000_000_000 / clock_rate as i64;
        r.rtp_stats_base.set_clock_rate_locked(clock_rate);
    }

    pub fn new_snapshot_id(&self) -> u32 {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        r.rtp_stats_base
            .new_snapshot_id(r.sequence_number.get_extended_highest())
    }
}

impl RtpStatsReceiverInner {
    fn get_ts_rollover_count(&self, diff_nano: i64, ts: u32) -> i32 {
        if diff_nano < self.ts_rollover_threshold {
            // time not more than rollover threshold
            return -1;
        }

        let excess = diff_nano
            .wrapping_sub(self.ts_rollover_threshold.wrapping_mul(2))
            .wrapping_mul(self.rtp_stats_base.rtp_stats_base_lite.clock_rate as i64)
            / 1_000_000_000;
        let mut roc = (excess / (1i64 << 32)).max(0);
        if self.timestamp.get_highest() > ts {
            roc += 1;
        }
        roc as i32
    }

    fn undo_updates_locked(
        &mut self,
        res_sn: WrapAroundUpdateResult<u64>,
        res_ts: WrapAroundUpdateResult<u64>,
    ) {
        self.sequence_number.undo_update(res_sn);
        self.timestamp.undo_update(res_ts);
    }
}

impl RtpStatsReceiver {
    #[allow(clippy::too_many_arguments)]
    pub fn update(
        &self,
        packet_time: i64,
        sequence_number: u16,
        timestamp: u32,
        marker: bool,
        hdr_size: usize,
        payload_size: usize,
        padding_size: usize,
    ) -> RtpFlowState {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        let mut flow_state = RtpFlowState::default();
        if r.rtp_stats_base.rtp_stats_base_lite.end_time != 0 {
            flow_state.unhandled_reason = RtpFlowUnhandledReason::Ended;
            return flow_state;
        }
        if r.rtp_stats_base.rtp_stats_base_lite.clock_rate == 0 {
            flow_state.unhandled_reason = RtpFlowUnhandledReason::Unconfigured;
            return flow_state;
        }

        let mut res_sn: WrapAroundUpdateResult<u64>;
        let mut gap_sn: i64;
        let res_ts: WrapAroundUpdateResult<u64>;
        let mut gap_ts: i64 = 0;
        let mut expected_ts_jump: i64 = 0;
        let mut time_since_highest: i64 = 0;
        let mut ts_rollover_count: i32 = 0;
        let mut sn_rollover_count: i32 = 0;

        if !r.rtp_stats_base.rtp_stats_base_lite.initialized {
            if payload_size == 0 {
                // do not start on a padding only packet
                flow_state.unhandled_reason = RtpFlowUnhandledReason::PaddingOnly;
                return flow_state;
            }

            r.rtp_stats_base.rtp_stats_base_lite.initialized = true;

            r.rtp_stats_base.rtp_stats_base_lite.start_time = mono::unix_nano();

            r.rtp_stats_base.first_time = packet_time;
            r.rtp_stats_base.highest_time = packet_time;

            res_sn = r.sequence_number.update(sequence_number);
            res_ts = r.timestamp.update(timestamp);

            // initialize snapshots if any
            for i in 0..r
                .rtp_stats_base
                .next_snapshot_id
                .wrapping_sub(C_FIRST_SNAPSHOT_ID)
            {
                r.rtp_stats_base.snapshots[i as usize] = init_snapshot(
                    r.rtp_stats_base.rtp_stats_base_lite.start_time,
                    r.sequence_number.get_extended_start(),
                );
            }

            debugw!(
                r.rtp_stats_base.rtp_stats_base_lite.logger,
                "rtp receiver stream start",
                "rtpStats" => r,
            );
        } else {
            res_sn = r.sequence_number.update(sequence_number);
            gap_sn = res_sn
                .extended_val
                .wrapping_sub(res_sn.pre_extended_highest) as i64;

            time_since_highest = packet_time.wrapping_sub(r.rtp_stats_base.highest_time);
            expected_ts_jump = r
                .rtp_stats_base
                .rtp_converter()
                .to_rtp_ext(Duration(time_since_highest)) as i64;
            ts_rollover_count = r.get_ts_rollover_count(time_since_highest, timestamp);
            res_ts = r.timestamp.rollover(timestamp, ts_rollover_count);
            gap_ts = res_ts
                .extended_val
                .wrapping_sub(res_ts.pre_extended_highest) as i64;
            if res_ts.is_unhandled {
                r.undo_updates_locked(res_sn, res_ts);

                r.packets_dropped_pre_start_timestamp += 1;
                let rulf = ReceiverUpdateLoggingFields {
                    packet_time,
                    sequence_number,
                    timestamp,
                    marker,
                    hdr_size,
                    payload_size,
                    padding_size,
                    res_sn,
                    gap_sn,
                    res_ts,
                    gap_ts,
                    sn_rollover_count,
                    expected_ts_jump,
                    ts_rollover_count,
                    time_since_highest,
                };
                if should_log(r.packets_dropped_pre_start_timestamp) {
                    rulf.log(Level::Warn, "dropping packet, pre-start timestamp", r, &[]);
                }

                if r.maybe_restart(sequence_number, timestamp, payload_size) {
                    rulf.log(Level::Info, "potential restart", r, &[]);
                    r.reset_restart();
                    flow_state.unhandled_reason = RtpFlowUnhandledReason::Restart;
                } else {
                    flow_state.unhandled_reason = RtpFlowUnhandledReason::PreStartTimestamp;
                }
                return flow_state;
            }

            if ts_rollover_count >= 0 && payload_size > 0 {
                let rulf = ReceiverUpdateLoggingFields {
                    packet_time,
                    sequence_number,
                    timestamp,
                    marker,
                    hdr_size,
                    payload_size,
                    padding_size,
                    res_sn,
                    gap_sn,
                    res_ts,
                    gap_ts,
                    sn_rollover_count,
                    expected_ts_jump,
                    ts_rollover_count,
                    time_since_highest,
                };
                rulf.log(Level::Warn, "potential time stamp roll over", r, &[]);
            }

            if !res_sn.is_unhandled {
                // it is possible to receive old packets in two different scenarios
                // as it is not possible to detect how far to roll back, ignore old packets
                //
                // Case 1:
                //  Very old time stamp, happens under the following conditions
                //  - resume after long mute, this causes big time stamp jump ahead for the packets
                //    after unmute
                //  - an out of order packet from before the mute arrives (unsure what causes this
                //    very old packet to be transmitted from remote), causing time stamp to jump back
                //    to before mute, but it appears like it has rolled over.
                //  Use a threshold against expected to ignore these.
                if gap_sn < 0
                    && gap_ts > 0
                    && gap_ts > (expected_ts_jump as f64 * C_TS_JUMP_TOO_HIGH_FACTOR) as i64
                {
                    r.undo_updates_locked(res_sn, res_ts);

                    r.packets_dropped_old_timestamp += 1;
                    let rulf = ReceiverUpdateLoggingFields {
                        packet_time,
                        sequence_number,
                        timestamp,
                        marker,
                        hdr_size,
                        payload_size,
                        padding_size,
                        res_sn,
                        gap_sn,
                        res_ts,
                        gap_ts,
                        sn_rollover_count,
                        expected_ts_jump,
                        ts_rollover_count,
                        time_since_highest,
                    };
                    if should_log(r.packets_dropped_old_timestamp) {
                        rulf.log(Level::Warn, "dropping packet, old timestamp", r, &[]);
                    }

                    if r.maybe_restart(sequence_number, timestamp, payload_size) {
                        rulf.log(Level::Info, "potential restart", r, &[]);
                        r.reset_restart();
                        flow_state.unhandled_reason = RtpFlowUnhandledReason::Restart;
                    } else {
                        flow_state.unhandled_reason = RtpFlowUnhandledReason::OldTimestamp;
                    }
                    return flow_state;
                }

                // Case 2:
                //  Sequence number looks like it is moving forward, but it is actually a very old packet.
                if gap_ts < 0 && gap_sn > 0 {
                    r.undo_updates_locked(res_sn, res_ts);

                    expected_ts_jump = r
                        .rtp_stats_base
                        .rtp_converter()
                        .to_rtp_ext(Duration(time_since_highest))
                        as i64;

                    r.packets_dropped_old_sequence_number += 1;
                    let rulf = ReceiverUpdateLoggingFields {
                        packet_time,
                        sequence_number,
                        timestamp,
                        marker,
                        hdr_size,
                        payload_size,
                        padding_size,
                        res_sn,
                        gap_sn,
                        res_ts,
                        gap_ts,
                        sn_rollover_count,
                        expected_ts_jump,
                        ts_rollover_count,
                        time_since_highest,
                    };
                    if should_log(r.packets_dropped_old_sequence_number) {
                        rulf.log(Level::Warn, "dropping packet, old sequence number", r, &[]);
                    }

                    if r.maybe_restart(sequence_number, timestamp, payload_size) {
                        rulf.log(Level::Info, "potential restart", r, &[]);
                        r.reset_restart();
                        flow_state.unhandled_reason = RtpFlowUnhandledReason::Restart;
                    } else {
                        flow_state.unhandled_reason = RtpFlowUnhandledReason::OldSequenceNumber;
                    }
                    return flow_state;
                }
            }

            // it is possible that sequence number has rolled over too
            #[allow(clippy::manual_range_contains)]
            if (gap_sn < 0 || gap_sn > (1 << 15)) && gap_ts > 0 && payload_size > 0 {
                // not possible to know how many cycles of sequence number roll over could have happened,
                // ensure that it at least does not go backwards
                sn_rollover_count = 0;
                if sequence_number < r.sequence_number.get_highest() {
                    sn_rollover_count = 1;
                }
                res_sn = r
                    .sequence_number
                    .rollover(sequence_number, sn_rollover_count);
                if !res_sn.is_unhandled {
                    let rulf = ReceiverUpdateLoggingFields {
                        packet_time,
                        sequence_number,
                        timestamp,
                        marker,
                        hdr_size,
                        payload_size,
                        padding_size,
                        res_sn,
                        gap_sn,
                        res_ts,
                        gap_ts,
                        sn_rollover_count,
                        expected_ts_jump,
                        ts_rollover_count,
                        time_since_highest,
                    };
                    rulf.log(Level::Warn, "forcing sequence number rollover", r, &[]);
                }
            }

            if res_sn.is_unhandled {
                r.undo_updates_locked(res_sn, res_ts);

                r.packets_dropped_pre_start_sequence_number += 1;
                let rulf = ReceiverUpdateLoggingFields {
                    packet_time,
                    sequence_number,
                    timestamp,
                    marker,
                    hdr_size,
                    payload_size,
                    padding_size,
                    res_sn,
                    gap_sn,
                    res_ts,
                    gap_ts,
                    sn_rollover_count,
                    expected_ts_jump,
                    ts_rollover_count,
                    time_since_highest,
                };
                if should_log(r.packets_dropped_pre_start_sequence_number) {
                    rulf.log(
                        Level::Warn,
                        "dropping packet, pre-start sequence number",
                        r,
                        &[],
                    );
                }

                if r.maybe_restart(sequence_number, timestamp, payload_size) {
                    rulf.log(Level::Info, "potential restart", r, &[]);
                    r.reset_restart();
                    flow_state.unhandled_reason = RtpFlowUnhandledReason::Restart;
                } else {
                    flow_state.unhandled_reason = RtpFlowUnhandledReason::PreStartSequenceNumber;
                }
                return flow_state;
            }
        }
        gap_sn = res_sn
            .extended_val
            .wrapping_sub(res_sn.pre_extended_highest) as i64;

        let pkt_size = hdr_size
            .wrapping_add(payload_size)
            .wrapping_add(padding_size) as u64;
        if gap_sn <= 0 {
            // duplicate OR out-of-order
            if gap_sn != 0 {
                r.rtp_stats_base.rtp_stats_base_lite.packets_out_of_order += 1;
            }

            if r.is_in_range(res_sn.extended_val, res_sn.pre_extended_highest) {
                if r.history.get_and_set(res_sn.extended_val) {
                    r.rtp_stats_base.bytes_duplicate =
                        r.rtp_stats_base.bytes_duplicate.wrapping_add(pkt_size);
                    r.rtp_stats_base.header_bytes_duplicate = r
                        .rtp_stats_base
                        .header_bytes_duplicate
                        .wrapping_add(hdr_size as u64);
                    r.rtp_stats_base.packets_duplicate += 1;
                    flow_state.is_duplicate = true;
                } else {
                    r.rtp_stats_base.rtp_stats_base_lite.packets_lost = r
                        .rtp_stats_base
                        .rtp_stats_base_lite
                        .packets_lost
                        .wrapping_sub(1);
                }
            }

            flow_state.is_out_of_order = true;
        } else {
            // in-order
            if res_ts.extended_val < res_ts.pre_extended_highest
                && r.rtp_stats_base.rtp_stats_base_lite.bytes > 0
            {
                r.time_reversed_count += 1;
                if should_log(r.time_reversed_count) {
                    let rulf = ReceiverUpdateLoggingFields {
                        packet_time,
                        sequence_number,
                        timestamp,
                        marker,
                        hdr_size,
                        payload_size,
                        padding_size,
                        res_sn,
                        gap_sn,
                        res_ts,
                        gap_ts,
                        sn_rollover_count,
                        expected_ts_jump,
                        ts_rollover_count,
                        time_since_highest,
                    };
                    rulf.log(
                        Level::Warn,
                        "time reversed",
                        r,
                        &[("count", &r.time_reversed_count)],
                    );
                }
            }

            // update gap histogram
            r.rtp_stats_base.update_gap_histogram(gap_sn);

            // update missing sequence numbers
            r.history.clear_range(
                res_sn.pre_extended_highest.wrapping_add(1),
                res_sn.extended_val.wrapping_sub(1),
            );
            r.rtp_stats_base.rtp_stats_base_lite.packets_lost = r
                .rtp_stats_base
                .rtp_stats_base_lite
                .packets_lost
                .wrapping_add((gap_sn - 1) as u64);

            r.history.set(res_sn.extended_val);

            if timestamp != res_ts.pre_extended_highest as u32 {
                // update only on first packet as same timestamp could be in multiple packets.
                // NOTE: this may not be the first packet with this time stamp if there is packet loss.
                r.rtp_stats_base.highest_time = packet_time;
            }

            flow_state.loss_start_inclusive = res_sn.pre_extended_highest.wrapping_add(1);
            flow_state.loss_end_exclusive = res_sn.extended_val;
        }
        flow_state.ext_sequence_number = res_sn.extended_val;
        flow_state.ext_timestamp = res_ts.extended_val;

        if !flow_state.is_duplicate {
            if payload_size == 0 {
                r.rtp_stats_base.packets_padding += 1;
                r.rtp_stats_base.bytes_padding =
                    r.rtp_stats_base.bytes_padding.wrapping_add(pkt_size);
                r.rtp_stats_base.header_bytes_padding = r
                    .rtp_stats_base
                    .header_bytes_padding
                    .wrapping_add(hdr_size as u64);
            } else {
                r.rtp_stats_base.rtp_stats_base_lite.bytes = r
                    .rtp_stats_base
                    .rtp_stats_base_lite
                    .bytes
                    .wrapping_add(pkt_size);
                r.rtp_stats_base.header_bytes =
                    r.rtp_stats_base.header_bytes.wrapping_add(hdr_size as u64);

                if marker {
                    r.rtp_stats_base.frames = r.rtp_stats_base.frames.wrapping_add(1);
                }

                r.rtp_stats_base
                    .update_jitter(res_ts.extended_val, packet_time);
            }
        }
        r.reset_restart();
        flow_state
    }
}

impl RtpStatsReceiverInner {
    fn get_extended_sender_report(
        &self,
        sr_data: &livekit::RtcpSenderReportState,
    ) -> livekit::RtcpSenderReportState {
        let mut ts_cycles = 0u64;
        if let Some(sr_newest) = &self.rtp_stats_base.sr_newest {
            // use time since last sender report to ensure long gaps where the time stamp might
            // jump more than half the range
            let sr_time = NtpTime(sr_data.ntp_timestamp).time();
            let sr_newest_time = NtpTime(sr_newest.ntp_timestamp).time();
            let time_since_last_report = sr_time.sub(sr_newest_time);
            let expected_rtp_timestamp_ext = sr_newest.rtp_timestamp_ext.wrapping_add(
                self.rtp_stats_base
                    .rtp_converter()
                    .to_rtp_ext(time_since_last_report),
            );
            let clock_rate = self.rtp_stats_base.rtp_stats_base_lite.clock_rate;
            let lbound = expected_rtp_timestamp_ext
                .wrapping_sub((C_REPORT_SLACK * clock_rate as f64) as u64);
            let ubound = expected_rtp_timestamp_ext
                .wrapping_add((C_REPORT_SLACK * clock_rate as f64) as u64);
            let is_in_range = sr_data.rtp_timestamp.wrapping_sub(lbound as u32) < (1 << 31)
                && (ubound as u32).wrapping_sub(sr_data.rtp_timestamp) < (1 << 31);
            if is_in_range {
                let lb_ts_cycles = lbound & 0xFFFF_FFFF_0000_0000;
                let ub_ts_cycles = ubound & 0xFFFF_FFFF_0000_0000;
                if lb_ts_cycles == ub_ts_cycles {
                    ts_cycles = lb_ts_cycles;
                } else if sr_data.rtp_timestamp < (1 << 31) {
                    // rolled over
                    ts_cycles = ub_ts_cycles;
                } else {
                    ts_cycles = lb_ts_cycles;
                }
            } else {
                // ideally this method should not be required, but there are clients
                // negotiating one clock rate, but actually send media at a different rate.
                ts_cycles = sr_newest.rtp_timestamp_ext & 0xFFFF_FFFF_0000_0000;
                if sr_data.rtp_timestamp.wrapping_sub(sr_newest.rtp_timestamp) < (1 << 31)
                    && sr_data.rtp_timestamp < sr_newest.rtp_timestamp
                {
                    ts_cycles = ts_cycles.wrapping_add(1 << 32);
                }

                if ts_cycles >= (1 << 32)
                    && sr_data.rtp_timestamp.wrapping_sub(sr_newest.rtp_timestamp) >= (1 << 31)
                    && sr_data.rtp_timestamp > sr_newest.rtp_timestamp
                {
                    ts_cycles -= 1 << 32;
                }
            }
        }

        let mut sr_data_ext = *sr_data;
        sr_data_ext.rtp_timestamp_ext = (sr_data_ext.rtp_timestamp as u64).wrapping_add(ts_cycles);
        sr_data_ext
    }

    fn check_out_of_order_sender_report(
        &mut self,
        sr_data: &livekit::RtcpSenderReportState,
    ) -> bool {
        if let Some(sr_newest) = &self.rtp_stats_base.sr_newest
            && sr_data.rtp_timestamp_ext < sr_newest.rtp_timestamp_ext
        {
            // This can happen when a track is replaced with a null and then restored -
            // i. e. muting replacing with null and unmute restoring the original track.
            // Or it could be due bad report generation.
            // In any case, ignore out-of-order reports.
            self.out_of_order_sender_report_count += 1;
            if should_log(self.out_of_order_sender_report_count) {
                infow!(
                    self.rtp_stats_base.rtp_stats_base_lite.logger,
                    "received sender report, out-of-order, skipping",
                    "current" => WrappedRtcpSenderReportStateLogger {
                        rtcp_sender_report_state: Some(*sr_data),
                    },
                    "count" => self.out_of_order_sender_report_count,
                    "rtpStats" => self,
                );
            }
            return true;
        }

        false
    }

    fn check_rtp_clock_skew_for_sender_report(&mut self, sr_data: &livekit::RtcpSenderReportState) {
        // srFirst is set whenever srNewest is
        let (Some(sr_newest), Some(sr_first)) =
            (self.rtp_stats_base.sr_newest, self.rtp_stats_base.sr_first)
        else {
            return;
        };

        let sr_time = NtpTime(sr_data.ntp_timestamp).time();
        let sr_newest_time = NtpTime(sr_newest.ntp_timestamp).time();
        let sr_first_time = NtpTime(sr_first.ntp_timestamp).time();

        let time_since_last = sr_time.sub(sr_newest_time).seconds();
        let rtp_diff_since_last = sr_data
            .rtp_timestamp_ext
            .wrapping_sub(sr_newest.rtp_timestamp_ext);
        let calculated_clock_rate_from_last = rtp_diff_since_last as f64 / time_since_last;

        let time_since_first = sr_time.sub(sr_first_time).seconds();
        let rtp_diff_since_first = sr_data
            .rtp_timestamp_ext
            .wrapping_sub(sr_first.rtp_timestamp_ext);
        let calculated_clock_rate_from_first = rtp_diff_since_first as f64 / time_since_first;

        let clock_rate = self.rtp_stats_base.rtp_stats_base_lite.clock_rate as f64;
        if (time_since_last > 0.2
            && (clock_rate - calculated_clock_rate_from_last).abs() > 0.2 * clock_rate)
            || (time_since_first > 0.2
                && (clock_rate - calculated_clock_rate_from_first).abs() > 0.2 * clock_rate)
        {
            self.clock_skew_count += 1;
            if should_log(self.clock_skew_count) {
                infow!(
                    self.rtp_stats_base.rtp_stats_base_lite.logger,
                    "received sender report, clock skew",
                    "current" => WrappedRtcpSenderReportStateLogger {
                        rtcp_sender_report_state: Some(*sr_data),
                    },
                    "timeSinceFirst" => time_since_first,
                    "rtpDiffSinceFirst" => rtp_diff_since_first,
                    "calculatedFirst" => calculated_clock_rate_from_first,
                    "timeSinceLast" => time_since_last,
                    "rtpDiffSinceLast" => rtp_diff_since_last,
                    "calculatedLast" => calculated_clock_rate_from_last,
                    "count" => self.clock_skew_count,
                    "rtpStats" => self,
                );
            }
        }
    }

    fn check_rtp_clock_skew_against_media_path_for_sender_report(
        &mut self,
        sr_data: &livekit::RtcpSenderReportState,
    ) {
        if self.rtp_stats_base.highest_time == 0 {
            return;
        }

        let now_nano = mono::unix_nano();
        let time_since_sr = Duration(now_nano.wrapping_sub(sr_data.at_adjusted));
        let ext_now_ts_sr = sr_data.rtp_timestamp_ext.wrapping_add(
            self.rtp_stats_base
                .rtp_converter()
                .to_rtp_ext(time_since_sr),
        );

        let time_since_highest = Duration(now_nano.wrapping_sub(self.rtp_stats_base.highest_time));
        let ext_now_ts_highest = self.timestamp.get_extended_highest().wrapping_add(
            self.rtp_stats_base
                .rtp_converter()
                .to_rtp_ext(time_since_highest),
        );
        let diff_highest = ext_now_ts_sr.wrapping_sub(ext_now_ts_highest);

        let time_since_first = Duration(now_nano.wrapping_sub(self.rtp_stats_base.first_time));
        let ext_now_ts_first = self.timestamp.get_extended_start().wrapping_add(
            self.rtp_stats_base
                .rtp_converter()
                .to_rtp_ext(time_since_first),
        );
        let diff_first = ext_now_ts_sr.wrapping_sub(ext_now_ts_first);

        // is it more than 5 seconds off?
        let clock_rate = self.rtp_stats_base.rtp_stats_base_lite.clock_rate;
        if (diff_highest as i64 as f64).abs() as u32 > 5u32.wrapping_mul(clock_rate)
            || (diff_first as i64 as f64).abs() as u32 > 5u32.wrapping_mul(clock_rate)
        {
            self.clock_skew_media_path_count += 1;
            if should_log(self.clock_skew_media_path_count) {
                infow!(
                    self.rtp_stats_base.rtp_stats_base_lite.logger,
                    "received sender report, clock skew against media path",
                    "current" => WrappedRtcpSenderReportStateLogger {
                        rtcp_sender_report_state: Some(*sr_data),
                    },
                    "timeSinceSR" => time_since_sr,
                    "extNowTSSR" => ext_now_ts_sr,
                    "timeSinceHighest" => time_since_highest,
                    "extNowTSHighest" => ext_now_ts_highest,
                    "diffHighest" => diff_highest as i64,
                    "timeSinceFirst" => time_since_first,
                    "extNowTSFirst" => ext_now_ts_first,
                    "diffFirst" => diff_first as i64,
                    "count" => self.clock_skew_media_path_count,
                    "rtpStats" => self,
                );
            }
        }
    }

    fn update_propagation_delay_and_record_sender_report(
        &mut self,
        sr_data: &mut livekit::RtcpSenderReportState,
    ) {
        let sr_time = NtpTime(sr_data.ntp_timestamp).time();
        let sender_clock_time = sr_time.unix_nano();
        let (estimated_propagation_delay, step_change) = self
            .propagation_delay_estimator
            .update(sender_clock_time, sr_data.at);
        if step_change {
            debugw!(
                self.rtp_stats_base.rtp_stats_base_lite.logger,
                "propagation delay step change",
                "currentSenderReport" => WrappedRtcpSenderReportStateLogger {
                    rtcp_sender_report_state: Some(*sr_data),
                },
                "rtpStats" => self,
            );
        }

        // adjust receive time to estimated propagation delay
        //
        // The original makes `srFirst` point to this report before it adjusts the time, and
        // the first report is then adjusted through both pointers. Kept by value, the time
        // is adjusted first.
        sr_data.at_adjusted = sender_clock_time.wrapping_add(estimated_propagation_delay);
        if self.rtp_stats_base.sr_first.is_none() {
            self.rtp_stats_base.sr_first = Some(*sr_data);
        }
        self.rtp_stats_base.sr_newest = Some(*sr_data);
    }
}

impl RtpStatsReceiver {
    pub fn set_rtcp_sender_report_data(
        &self,
        sr_data: Option<&livekit::RtcpSenderReportState>,
    ) -> bool {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        let Some(sr_data) = sr_data else {
            return false;
        };
        if !r.rtp_stats_base.rtp_stats_base_lite.initialized
            || r.rtp_stats_base.rtp_stats_base_lite.clock_rate == 0
        {
            return false;
        }

        // prevent against extreme case of anachronous sender reports
        if let Some(sr_newest) = &r.rtp_stats_base.sr_newest
            && sr_newest.ntp_timestamp > sr_data.ntp_timestamp
        {
            infow!(
                r.rtp_stats_base.rtp_stats_base_lite.logger,
                "received sender report, anachronous, dropping",
                "current" => WrappedRtcpSenderReportStateLogger {
                    rtcp_sender_report_state: Some(*sr_data),
                },
                "rtpStats" => r,
            );
            return false;
        }

        let mut sr_data_ext = r.get_extended_sender_report(sr_data);

        if r.check_out_of_order_sender_report(&sr_data_ext) {
            return false;
        }

        r.check_rtp_clock_skew_for_sender_report(&sr_data_ext);
        r.update_propagation_delay_and_record_sender_report(&mut sr_data_ext);
        r.check_rtp_clock_skew_against_media_path_for_sender_report(&sr_data_ext);

        // `r.srNewest` of the original is the report just recorded
        let (adjustment, logging_fields, err) = r.rtp_stats_base.maybe_adjust_first_packet_time(
            &sr_data_ext,
            0,
            r.timestamp.get_extended_start(),
        );
        if let Some(err) = err {
            infow_logging_fields(
                &r.rtp_stats_base.rtp_stats_base_lite.logger,
                err,
                &logging_fields,
                r,
            );
        }
        r.propagation_delay_estimator.initial_adjustment(adjustment);
        true
    }

    pub fn get_rtcp_sender_report_data(&self) -> Option<livekit::RtcpSenderReportState> {
        let r = self.lock.read();

        r.rtp_stats_base.sr_newest
    }

    pub fn last_sender_report_time(&self) -> Time {
        let r = self.lock.read();

        if let Some(sr_newest) = &r.rtp_stats_base.sr_newest
            && sr_newest.at != 0
        {
            // srNewest.At is a mono timestamp, convert to wall time for comparison with time.Now()
            return time::now().add(-Duration(mono::unix_nano().wrapping_sub(sr_newest.at)));
        }

        Time::ZERO
    }

    pub fn get_rtcp_reception_report(
        &self,
        ssrc: u32,
        proxy_frac_lost: u8,
        snapshot_id: u32,
    ) -> Option<ReceptionReport> {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        let ext_highest_sn = r.sequence_number.get_extended_highest();
        let (then, now, ok) = r.rtp_stats_base.get_and_reset_snapshot(
            snapshot_id,
            r.sequence_number.get_extended_start(),
            ext_highest_sn,
        );
        if !ok {
            return None;
        }

        let packets_expected = now
            .snapshot_lite
            .ext_start_sn
            .wrapping_sub(then.snapshot_lite.ext_start_sn);
        if packets_expected > C_NUM_SEQUENCE_NUMBERS {
            warnw!(
                r.rtp_stats_base.rtp_stats_base_lite.logger,
                "too many packets expected in receiver report",
                format!(
                    "start: {}, end: {}, expected: {}",
                    then.snapshot_lite.ext_start_sn, now.snapshot_lite.ext_start_sn, packets_expected
                ),
                "rtpStats" => r,
            );
            return None;
        }
        if packets_expected == 0 {
            return None;
        }

        let mut packets_lost = now
            .snapshot_lite
            .packets_lost
            .wrapping_sub(then.snapshot_lite.packets_lost) as u32;
        if (packets_lost as i32) < 0 {
            packets_lost = 0;
        }
        let loss_rate = packets_lost as f32 / packets_expected as f32;
        let frac_lost = proxy_frac_lost.max((loss_rate * 256.0) as u8);

        let total_lost = r
            .rtp_stats_base
            .rtp_stats_base_lite
            .packets_lost
            .min(0xffffff); // 24-bits max

        let mut last_sr = 0u32;
        let mut dlsr = 0u32;
        if let Some(sr_newest) = &r.rtp_stats_base.sr_newest {
            last_sr = (sr_newest.ntp_timestamp >> 16) as u32;
            if sr_newest.at != 0 {
                let delay_us = mono::unix_nano().wrapping_sub(sr_newest.at) / 1000;
                dlsr = (delay_us.wrapping_mul(65536) / 1_000_000) as u32;
            }
        }

        Some(ReceptionReport {
            ssrc,
            fraction_lost: frac_lost,
            total_lost: total_lost as u32,
            last_sequence_number: now.snapshot_lite.ext_start_sn as u32,
            jitter: r.rtp_stats_base.jitter as u32,
            last_sender_report: last_sr,
            delay: dlsr,
        })
    }

    pub fn delta_info(&self, snapshot_id: u32) -> Option<RtpDeltaInfo> {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        let (delta_info, logging_fields, err) = r.rtp_stats_base.delta_info(
            snapshot_id,
            r.sequence_number.get_extended_start(),
            r.sequence_number.get_extended_highest(),
        );
        if let Some(err) = err {
            infow_logging_fields(
                &r.rtp_stats_base.rtp_stats_base_lite.logger,
                err,
                &logging_fields,
                r,
            );
        }

        delta_info
    }
}

impl fmt::Debug for RtpStatsReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let r = self.lock.read();

        fmt::Debug::fmt(&*r, f)
    }
}

impl RtpStatsReceiver {
    pub fn to_proto(&self) -> Option<livekit::RtpStats> {
        let r = self.lock.read();

        let (ext_start_sn, ext_highest_sn) = (
            r.sequence_number.get_extended_start(),
            r.sequence_number.get_extended_highest(),
        );
        r.rtp_stats_base.to_proto(
            get_packets_expected(ext_start_sn, ext_highest_sn),
            r.rtp_stats_base
                .get_packets_seen_minus_padding(ext_start_sn, ext_highest_sn),
            r.rtp_stats_base.rtp_stats_base_lite.packets_lost,
            r.timestamp.get_extended_start(),
            r.timestamp.get_extended_highest(),
            r.rtp_stats_base.jitter,
            r.rtp_stats_base.max_jitter,
        )
    }
}

impl RtpStatsReceiverInner {
    fn is_in_range(&self, esn: u64, ehsn: u64) -> bool {
        let diff = ehsn.wrapping_sub(esn) as i64;
        diff >= 0 && diff < C_HISTORY_SIZE as i64
    }
}

impl RtpStatsReceiver {
    pub fn highest_timestamp(&self) -> u32 {
        let r = self.lock.read();

        r.timestamp.get_highest()
    }

    // for testing only
    pub fn highest_sequence_number(&self) -> u16 {
        let r = self.lock.read();

        r.sequence_number.get_highest()
    }

    // for testing only
    pub fn extended_highest_sequence_number(&self) -> u64 {
        let r = self.lock.read();

        r.sequence_number.get_extended_highest()
    }
}

impl RtpStatsReceiverInner {
    pub(super) fn maybe_restart(&mut self, sn: u16, ts: u32, payload_size: usize) -> bool {
        if payload_size > 0 {
            if self.restart_packets_n < RESTART_THRESHOLD {
                self.restart_packets_buf[self.restart_packets_n] = Packet {
                    sequence_number: sn,
                    timestamp: ts,
                };
                self.restart_packets_n += 1;
            } else {
                // keep last restartThreshold entries: shift left and append
                self.restart_packets_buf.copy_within(1.., 0);
                self.restart_packets_buf[RESTART_THRESHOLD - 1] = Packet {
                    sequence_number: sn,
                    timestamp: ts,
                };
            }
        }
        if self.restart_packets_n < RESTART_THRESHOLD {
            return false;
        }

        // check for contiguous sequence numbers and equal or increasing timestamps
        for i in 1..self.restart_packets_n {
            let p = &self.restart_packets_buf[i];
            let prev = &self.restart_packets_buf[i - 1];
            if p.sequence_number != prev.sequence_number.wrapping_add(1)
                || p.timestamp.wrapping_sub(prev.timestamp) > (1 << 31)
            {
                return false;
            }
        }

        true
    }

    pub(super) fn reset_restart(&mut self) {
        self.restart_packets_n = 0;
    }
}

// ----------------------------------

// lockedRTPStatsReceiverLogEncoder
impl fmt::Debug for RtpStatsReceiverInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("RtpStatsReceiver");

        let (ext_start_sn, ext_highest_sn) = (
            self.sequence_number.get_extended_start(),
            self.sequence_number.get_extended_highest(),
        );
        let (ext_start_ts, ext_highest_ts) = (
            self.timestamp.get_extended_start(),
            self.timestamp.get_extended_highest(),
        );
        if let Err(err) = self.rtp_stats_base.marshal_log_object(
            &mut e,
            get_packets_expected(ext_start_sn, ext_highest_sn),
            self.rtp_stats_base
                .get_packets_seen_minus_padding(ext_start_sn, ext_highest_sn),
            ext_start_ts,
            ext_highest_ts,
        ) {
            return e.field("error", &format_args!("{err}")).finish();
        }

        e.field("extStartSN", &ext_start_sn);
        e.field("extHighestSN", &ext_highest_sn);
        e.field("extStartTS", &ext_start_ts);
        e.field("extHighestTS", &ext_highest_ts);

        e.field(
            "propagationDelayEstimator",
            &self.propagation_delay_estimator,
        );

        e.field("clockSkewCount", &self.clock_skew_count);
        e.field("clockSkewMediaPathCount", &self.clock_skew_media_path_count);
        e.field(
            "outOfOrderSenderReportCount",
            &self.out_of_order_sender_report_count,
        );
        e.field("timeReversedCount", &self.time_reversed_count);

        e.field(
            "packetsDroppedPreStartTimestamp",
            &self.packets_dropped_pre_start_timestamp,
        );
        e.field(
            "packetsDroppedOldTimestamp",
            &self.packets_dropped_old_timestamp,
        );
        e.field(
            "packetsDroppedPreStartSequenceNumber",
            &self.packets_dropped_pre_start_sequence_number,
        );
        e.field(
            "packetsDroppedOldSequenceNumber",
            &self.packets_dropped_old_sequence_number,
        );

        e.field(
            "restartPackets",
            &&self.restart_packets_buf[..self.restart_packets_n],
        );
        e.finish()
    }
}

// ----------------------------------
