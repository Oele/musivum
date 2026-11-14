// Ported from LiveKit, pkg/sfu/rtpstats/rtpstats_base.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `RtpStatsBase` is the state inside the lock of the
// outer type (see `rtpstats_base_lite.rs`), with the `rtpStatsBaseLite` it embeds as the field
// `rtp_stats_base_lite`; the methods that take the lock upstream (`UpdateFEC`, `UpdateRtt`,
// `KeyFrame`, ...) are forwarded by the outer types through `forward_rtp_stats_base!` at the
// end of this file; the two sender report states are held by value, where the original has
// pointers (which, for the first report of a receiver, are one and the same message);
// `WrappedRTCPSenderReportStateLogger` holds a copy of the state it wraps; a pointer that may
// be nil is an `Option`, also in what the functions at the end take and give; an unset
// `lastFir` or `lastKeyFrame` is the zero `Time`, which a protocol message carries as 1970
// where the original's zero `time.Time` gives the year 1.

use std::fmt;

use crate::go::time::{self, Duration, Time, timestamppb_as_time, timestamppb_new};
use crate::mediatransportutil::NtpTime;
use crate::protocol::livekit;
use crate::protocol::logger::Level;
use crate::protocol::utils;
use crate::protocol::utils::mono;
use crate::protocol::utils::rtputil::RtpConverter;

use super::Error;
use super::rtpstats_base_lite::{
    C_FIRST_SNAPSHOT_ID, C_GAP_HISTOGRAM_NUM_BINS, C_NUM_SEQUENCE_NUMBERS, LoggingFields,
    RtpStatsBaseLite, RtpStatsParams, SnapshotLite, init_snapshot_lite, logging_fields_as_kvs,
};

const C_FIRST_PACKET_TIME_ADJUST_WINDOW: Duration = Duration(2 * time::MINUTE.0);
const C_FIRST_PACKET_TIME_ADJUST_THRESHOLD: i64 = 15 * 1_000_000_000;

// -------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RtpDeltaInfo {
    pub start_time: Time,
    pub end_time: Time,
    pub packets: u32,
    pub bytes: u64,
    pub header_bytes: u64,
    pub packets_duplicate: u32,
    pub bytes_duplicate: u64,
    pub header_bytes_duplicate: u64,
    pub packets_padding: u32,
    pub bytes_padding: u64,
    pub header_bytes_padding: u64,
    pub fec_packets: u32,
    pub fec_bytes: u64,
    pub fec_packets_discarded: u32,
    pub fec_packets_recovered: u32,
    pub packets_lost: u32,
    pub packets_missing: u32,
    pub packets_out_of_order: u32,
    pub frames: u32,
    pub rtt_max: u32,
    pub jitter_max: f64,
    pub nacks: u32,
    pub nack_repeated: u32,
    pub plis: u32,
    pub firs: u32,
}

// -------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(super) struct Snapshot {
    pub(super) snapshot_lite: SnapshotLite,

    pub(super) header_bytes: u64,

    pub(super) packets_duplicate: u64,
    pub(super) bytes_duplicate: u64,
    pub(super) header_bytes_duplicate: u64,

    pub(super) packets_padding: u64,
    pub(super) bytes_padding: u64,
    pub(super) header_bytes_padding: u64,

    pub(super) fec_packets: u64,
    pub(super) fec_bytes: u64,
    pub(super) fec_packets_discarded: u64,
    pub(super) fec_packets_recovered: u64,

    pub(super) frames: u32,

    pub(super) plis: u32,
    pub(super) firs: u32,

    pub(super) max_rtt: u32,
    pub(super) max_jitter: f64,
}

impl Snapshot {
    pub(super) fn maybe_update_max_rtt(&mut self, rtt: u32) {
        if rtt > self.max_rtt {
            self.max_rtt = rtt;
        }
    }

    pub(super) fn maybe_update_max_jitter(&mut self, jitter: f64) {
        if jitter > self.max_jitter {
            self.max_jitter = jitter;
        }
    }
}

// ------------------------------------------------------------------

struct WrappedRtpDriftLogger<'a> {
    rtp_drift: Option<&'a livekit::RtpDrift>,
}

impl fmt::Debug for WrappedRtpDriftLogger<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("RtpDrift");
        let Some(rd) = self.rtp_drift else {
            return e.finish();
        };

        // a timestamp that is missing reads as 1970-01-01, as `AsTime` of the original has it
        let as_time = |ts: &Option<musivum_protocol::Timestamp>| {
            ts.as_ref().map(timestamppb_as_time).unwrap_or(Time::ZERO)
        };
        e.field("StartTime", &as_time(&rd.start_time))
            .field("EndTime", &as_time(&rd.end_time))
            .field("Duration", &rd.duration)
            .field("StartTimestamp", &rd.start_timestamp)
            .field("EndTimestamp", &rd.end_timestamp)
            .field("RtpClockTicks", &rd.rtp_clock_ticks)
            .field("DriftSamples", &rd.drift_samples)
            .field("DriftMs", &rd.drift_ms)
            .field("ClockRate", &rd.clock_rate)
            .finish()
    }
}

// ------------------------------------------------------------------

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct WrappedRtcpSenderReportStateLogger {
    pub rtcp_sender_report_state: Option<livekit::RtcpSenderReportState>,
}

impl fmt::Debug for WrappedRtcpSenderReportStateLogger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("RtcpSenderReportState");
        let Some(rsrs) = &self.rtcp_sender_report_state else {
            return e.finish();
        };

        e.field("RtpTimestamp", &rsrs.rtp_timestamp)
            .field("RtpTimestampExt", &rsrs.rtp_timestamp_ext)
            .field("NtpTimestamp", &NtpTime(rsrs.ntp_timestamp).time())
            .field("At", &Time::unix(0, rsrs.at))
            .field("AtAdjusted", &Time::unix(0, rsrs.at_adjusted))
            .field("Packets", &rsrs.packets)
            .field("Octets", &rsrs.octets)
            .finish()
    }
}

// ------------------------------------------------------------------

pub fn rtcp_sender_report_propagation_delay(rsrs: &livekit::RtcpSenderReportState) -> Duration {
    Time::unix(0, rsrs.at_adjusted).sub(NtpTime(rsrs.ntp_timestamp).time())
}

// ------------------------------------------------------------------

pub(super) struct RtpStatsBase {
    pub(super) rtp_stats_base_lite: RtpStatsBaseLite,

    pub(super) rtp_converter: Option<RtpConverter>,

    pub(super) first_time: i64,
    pub(super) first_time_adjustment: Duration,
    pub(super) highest_time: i64,

    pub(super) last_transit: u64,
    pub(super) last_jitter_ext_timestamp: u64,

    pub(super) header_bytes: u64,

    pub(super) packets_duplicate: u64,
    pub(super) bytes_duplicate: u64,
    pub(super) header_bytes_duplicate: u64,

    pub(super) packets_padding: u64,
    pub(super) bytes_padding: u64,
    pub(super) header_bytes_padding: u64,

    pub(super) fec_packets: u64,
    pub(super) fec_bytes: u64,
    pub(super) fec_packets_discarded: u64,
    pub(super) fec_packets_recovered: u64,

    pub(super) frames: u32,

    pub(super) jitter: f64,
    pub(super) max_jitter: f64,

    pub(super) firs: u32,
    pub(super) last_fir: Time,

    pub(super) key_frames: u32,
    pub(super) last_key_frame: Time,

    pub(super) rtt: u32,
    pub(super) max_rtt: u32,

    pub(super) sr_first: Option<livekit::RtcpSenderReportState>,
    pub(super) sr_newest: Option<livekit::RtcpSenderReportState>,

    pub(super) next_snapshot_id: u32,
    pub(super) snapshots: Vec<Snapshot>,
}

impl RtpStatsBase {
    pub(super) fn new(params: RtpStatsParams) -> Self {
        Self {
            rtp_stats_base_lite: RtpStatsBaseLite::new(params),
            rtp_converter: None,
            first_time: 0,
            first_time_adjustment: Duration(0),
            highest_time: 0,
            last_transit: 0,
            last_jitter_ext_timestamp: 0,
            header_bytes: 0,
            packets_duplicate: 0,
            bytes_duplicate: 0,
            header_bytes_duplicate: 0,
            packets_padding: 0,
            bytes_padding: 0,
            header_bytes_padding: 0,
            fec_packets: 0,
            fec_bytes: 0,
            fec_packets_discarded: 0,
            fec_packets_recovered: 0,
            frames: 0,
            jitter: 0.0,
            max_jitter: 0.0,
            firs: 0,
            last_fir: Time::ZERO,
            key_frames: 0,
            last_key_frame: Time::ZERO,
            rtt: 0,
            max_rtt: 0,
            sr_first: None,
            sr_newest: None,
            next_snapshot_id: C_FIRST_SNAPSHOT_ID,
            snapshots: vec![Snapshot::default(); 2],
        }
    }

    pub(super) fn set_clock_rate(&mut self, clock_rate: u32) {
        self.set_clock_rate_locked(clock_rate);
    }

    pub(super) fn set_clock_rate_locked(&mut self, clock_rate: u32) {
        self.rtp_converter = Some(RtpConverter::new(clock_rate as i64));
        self.rtp_stats_base_lite.set_clock_rate_locked(clock_rate);
    }

    // `r.rtpConverter` where the original uses it without looking whether it is nil: it is
    // set with the clock rate, and what is called without one panics as the original does.
    pub(super) fn rtp_converter(&self) -> RtpConverter {
        self.rtp_converter
            .expect("rtpConverter is nil: the clock rate is not set")
    }

    pub(super) fn seed(&mut self, from: &RtpStatsBase) -> bool {
        if !self
            .rtp_stats_base_lite
            .seed(Some(&from.rtp_stats_base_lite))
        {
            return false;
        }

        self.first_time = from.first_time;
        self.first_time_adjustment = from.first_time_adjustment;
        self.highest_time = from.highest_time;

        self.last_transit = from.last_transit;
        self.last_jitter_ext_timestamp = from.last_jitter_ext_timestamp;

        self.header_bytes = from.header_bytes;

        self.packets_duplicate = from.packets_duplicate;
        self.bytes_duplicate = from.bytes_duplicate;
        self.header_bytes_duplicate = from.header_bytes_duplicate;

        self.packets_padding = from.packets_padding;
        self.bytes_padding = from.bytes_padding;
        self.header_bytes_padding = from.header_bytes_padding;

        self.fec_packets = from.fec_packets;
        self.fec_bytes = from.fec_bytes;
        self.fec_packets_discarded = from.fec_packets_discarded;
        self.fec_packets_recovered = from.fec_packets_recovered;

        self.frames = from.frames;

        self.jitter = from.jitter;
        self.max_jitter = from.max_jitter;

        self.firs = from.firs;
        self.last_fir = from.last_fir;

        self.key_frames = from.key_frames;
        self.last_key_frame = from.last_key_frame;

        self.rtt = from.rtt;
        self.max_rtt = from.max_rtt;

        self.sr_first = from.sr_first;
        self.sr_newest = from.sr_newest;

        self.next_snapshot_id = from.next_snapshot_id;
        self.snapshots = from.snapshots.clone();
        true
    }

    pub(super) fn new_snapshot_id(&mut self, ext_start_sn: u64) -> u32 {
        let id = self.next_snapshot_id;
        self.next_snapshot_id = self.next_snapshot_id.wrapping_add(1);

        let num = self.next_snapshot_id.wrapping_sub(C_FIRST_SNAPSHOT_ID) as usize;
        if self.snapshots.len() < num {
            self.snapshots.resize(num, Snapshot::default());
        }

        if self.rtp_stats_base_lite.initialized {
            self.snapshots[id.wrapping_sub(C_FIRST_SNAPSHOT_ID) as usize] =
                init_snapshot(mono::unix_nano(), ext_start_sn);
        }
        id
    }

    pub(super) fn update_fec(
        &mut self,
        fec_packets: u64,
        fec_bytes: u64,
        fec_packets_discarded: u64,
        fec_packets_recovered: u64,
    ) {
        if self.rtp_stats_base_lite.end_time != 0 {
            return;
        }

        self.fec_packets = self.fec_packets.wrapping_add(fec_packets);
        self.fec_bytes = self.fec_bytes.wrapping_add(fec_bytes);
        self.fec_packets_discarded = self
            .fec_packets_discarded
            .wrapping_add(fec_packets_discarded);
        self.fec_packets_recovered = self
            .fec_packets_recovered
            .wrapping_add(fec_packets_recovered);
    }

    pub(super) fn update_fir(&mut self, fir_count: u32) {
        if self.rtp_stats_base_lite.end_time != 0 {
            return;
        }

        self.firs = self.firs.wrapping_add(fir_count);
    }

    pub(super) fn update_fir_time(&mut self) {
        if self.rtp_stats_base_lite.end_time != 0 {
            return;
        }

        self.last_fir = time::now();
    }

    pub(super) fn update_key_frame(&mut self, kf_count: u32) {
        if self.rtp_stats_base_lite.end_time != 0 {
            return;
        }

        self.key_frames = self.key_frames.wrapping_add(kf_count);
        self.last_key_frame = time::now();
    }

    pub(super) fn key_frame(&self) -> (u32, Time) {
        (self.key_frames, self.last_key_frame)
    }

    pub(super) fn update_rtt(&mut self, rtt: u32) {
        if self.rtp_stats_base_lite.end_time != 0 {
            return;
        }

        self.rtt = rtt;
        if rtt > self.max_rtt {
            self.max_rtt = rtt;
        }

        for i in 0..self.next_snapshot_id.wrapping_sub(C_FIRST_SNAPSHOT_ID) {
            let s = &mut self.snapshots[i as usize];
            if rtt > s.max_rtt {
                s.max_rtt = rtt;
            }
        }
    }

    pub(super) fn get_rtt(&self) -> u32 {
        self.rtt
    }

    pub(super) fn maybe_adjust_first_packet_time(
        &mut self,
        sr_data: &livekit::RtcpSenderReportState,
        ts_offset: u64,
        ext_start_ts: u64,
    ) -> (i64, LoggingFields, Option<Error>) {
        let now_nano = mono::unix_nano();
        if Duration(now_nano.wrapping_sub(self.rtp_stats_base_lite.start_time))
            > C_FIRST_PACKET_TIME_ADJUST_WINDOW
        {
            return (0, Vec::new(), None);
        }

        // for some time after the start, adjust time of first packet.
        // Helps improve accuracy of expected timestamp calculation.
        // Adjusting only one way, i. e. if the first sample experienced
        // abnormal delay (maybe due to pacing or maybe due to queuing
        // in some network element along the way), push back first time
        // to an earlier instance.
        let time_since_receive = Duration(now_nano.wrapping_sub(sr_data.at_adjusted));
        let ext_now_ts = sr_data
            .rtp_timestamp_ext
            .wrapping_sub(ts_offset)
            .wrapping_add(self.rtp_converter().to_rtp_ext(time_since_receive));
        let samples_diff = ext_now_ts.wrapping_sub(ext_start_ts) as i64;
        if samples_diff < 0 {
            // out-of-order, skip
            return (0, Vec::new(), None);
        }

        let samples_duration = self.rtp_converter().to_duration_ext(samples_diff as u64);
        let time_since_first = Duration(now_nano.wrapping_sub(self.first_time));
        let now = self.first_time.wrapping_add(time_since_first.nanoseconds());
        let first_time = now.wrapping_sub(samples_duration.nanoseconds());
        let adjustment = self.first_time.wrapping_sub(first_time);

        let mut logging_fields: LoggingFields = Vec::new();
        let mut err = None;
        if first_time < self.first_time {
            if adjustment > C_FIRST_PACKET_TIME_ADJUST_THRESHOLD {
                err = Some(Error::AdjustingFirstPacketTimeTooBig);
                logging_fields = self.first_packet_time_adjust_log_fields(
                    now,
                    first_time,
                    adjustment,
                    ext_now_ts,
                    ext_start_ts,
                    sr_data,
                    ts_offset,
                    time_since_receive,
                    time_since_first,
                    samples_duration,
                    samples_diff,
                );
            } else {
                self.first_time_adjustment += Duration(adjustment);
                if self.rtp_stats_base_lite.logger.enabled(Level::Debug) {
                    self.rtp_stats_base_lite.logger.debugw(
                        "adjusting first packet time",
                        &logging_fields_as_kvs(&self.first_packet_time_adjust_log_fields(
                            now,
                            first_time,
                            adjustment,
                            ext_now_ts,
                            ext_start_ts,
                            sr_data,
                            ts_offset,
                            time_since_receive,
                            time_since_first,
                            samples_duration,
                            samples_diff,
                        )),
                    );
                }
                self.first_time = first_time;
            }
        }

        (adjustment, logging_fields, err)
    }

    #[allow(clippy::too_many_arguments)]
    fn first_packet_time_adjust_log_fields(
        &self,
        now: i64,
        first_time: i64,
        adjustment: i64,
        ext_now_ts: u64,
        ext_start_ts: u64,
        sr_data: &livekit::RtcpSenderReportState,
        ts_offset: u64,
        time_since_receive: Duration,
        time_since_first: Duration,
        samples_duration: Duration,
        samples_diff: i64,
    ) -> LoggingFields {
        vec![
            (
                "startTime",
                Box::new(Time::unix(0, self.rtp_stats_base_lite.start_time)),
            ),
            ("nowTime", Box::new(Time::unix(0, now))),
            ("before", Box::new(Time::unix(0, self.first_time))),
            ("after", Box::new(Time::unix(0, first_time))),
            ("adjustment", Box::new(Duration(adjustment))),
            ("firstTimeAdjustment", Box::new(self.first_time_adjustment)),
            ("extNowTS", Box::new(ext_now_ts)),
            ("extStartTS", Box::new(ext_start_ts)),
            (
                "srData",
                Box::new(WrappedRtcpSenderReportStateLogger {
                    rtcp_sender_report_state: Some(*sr_data),
                }),
            ),
            ("tsOffset", Box::new(ts_offset)),
            ("timeSinceReceive", Box::new(time_since_receive)),
            ("timeSinceFirst", Box::new(time_since_first)),
            ("samplesDiff", Box::new(samples_diff)),
            ("samplesDuration", Box::new(samples_duration)),
        ]
    }

    pub(super) fn get_packets_seen_minus_padding(
        &self,
        ext_start_sn: u64,
        ext_highest_sn: u64,
    ) -> u64 {
        let packets_seen = self
            .rtp_stats_base_lite
            .get_packets_seen(ext_start_sn, ext_highest_sn);
        if self.packets_padding > packets_seen {
            return 0;
        }

        packets_seen - self.packets_padding
    }

    pub(super) fn get_packets_seen_plus_duplicates(
        &self,
        ext_start_sn: u64,
        ext_highest_sn: u64,
    ) -> u64 {
        self.rtp_stats_base_lite
            .get_packets_seen(ext_start_sn, ext_highest_sn)
            .wrapping_add(self.packets_duplicate)
    }

    pub(super) fn delta_info(
        &mut self,
        snapshot_id: u32,
        ext_start_sn: u64,
        ext_highest_sn: u64,
    ) -> (Option<RtpDeltaInfo>, LoggingFields, Option<Error>) {
        if self.rtp_stats_base_lite.clock_rate == 0 {
            return (None, Vec::new(), None);
        }

        let (then, now, ok) =
            self.get_and_reset_snapshot(snapshot_id, ext_start_sn, ext_highest_sn);
        if !ok {
            return (None, Vec::new(), None);
        }

        let start_time = then.snapshot_lite.start_time;
        let end_time = now.snapshot_lite.start_time;

        let mut packets_expected = now
            .snapshot_lite
            .ext_start_sn
            .wrapping_sub(then.snapshot_lite.ext_start_sn);
        if then.snapshot_lite.ext_start_sn > ext_highest_sn {
            packets_expected = 0;
        }
        if packets_expected > C_NUM_SEQUENCE_NUMBERS {
            let logging_fields: LoggingFields = vec![
                ("snapshotID", Box::new(snapshot_id)),
                ("snapshotNow", Box::new(now)),
                ("snapshotThen", Box::new(then)),
                (
                    "duration",
                    Box::new(Duration(end_time.wrapping_sub(start_time))),
                ),
                ("packetsExpected", Box::new(packets_expected)),
            ];
            return (
                None,
                logging_fields,
                Some(Error::TooManyPacketsExpectedInDelta),
            );
        }
        if packets_expected == 0 {
            return (
                Some(RtpDeltaInfo {
                    start_time: Time::unix(0, start_time),
                    end_time: Time::unix(0, end_time),
                    fec_packets: now.fec_packets.wrapping_sub(then.fec_packets) as u32,
                    fec_bytes: now.fec_bytes.wrapping_sub(then.fec_bytes),
                    fec_packets_discarded: now
                        .fec_packets_discarded
                        .wrapping_sub(then.fec_packets_discarded)
                        as u32,
                    fec_packets_recovered: now
                        .fec_packets_recovered
                        .wrapping_sub(then.fec_packets_recovered)
                        as u32,
                    ..Default::default()
                }),
                Vec::new(),
                None,
            );
        }

        let mut packets_lost = now
            .snapshot_lite
            .packets_lost
            .wrapping_sub(then.snapshot_lite.packets_lost) as u32;
        if (packets_lost as i32) < 0 {
            packets_lost = 0;
        }

        // padding packets delta could be higher than expected due to out-of-order padding packets
        let packets_padding = now.packets_padding.wrapping_sub(then.packets_padding);
        let mut logging_fields: LoggingFields = Vec::new();
        let mut err = None;
        if packets_expected < packets_padding {
            logging_fields = vec![
                ("snapshotID", Box::new(snapshot_id)),
                ("snapshotNow", Box::new(now)),
                ("snapshotThen", Box::new(then)),
                (
                    "duration",
                    Box::new(Duration(end_time.wrapping_sub(start_time))),
                ),
                ("packetsExpected", Box::new(packets_expected)),
                ("packetsPadding", Box::new(packets_padding)),
                ("packetsLost", Box::new(packets_lost)),
            ];
            err = Some(Error::PaddingPacketsMoreThanExpected);
            packets_expected = 0;
        } else {
            packets_expected -= packets_padding;
        }

        let delta_info = RtpDeltaInfo {
            start_time: Time::unix(0, start_time),
            end_time: Time::unix(0, end_time),
            packets: packets_expected as u32,
            bytes: now
                .snapshot_lite
                .bytes
                .wrapping_sub(then.snapshot_lite.bytes),
            header_bytes: now.header_bytes.wrapping_sub(then.header_bytes),
            packets_duplicate: now.packets_duplicate.wrapping_sub(then.packets_duplicate) as u32,
            bytes_duplicate: now.bytes_duplicate.wrapping_sub(then.bytes_duplicate),
            header_bytes_duplicate: now
                .header_bytes_duplicate
                .wrapping_sub(then.header_bytes_duplicate),
            packets_padding: packets_padding as u32,
            bytes_padding: now.bytes_padding.wrapping_sub(then.bytes_padding),
            header_bytes_padding: now
                .header_bytes_padding
                .wrapping_sub(then.header_bytes_padding),
            fec_packets: now.fec_packets.wrapping_sub(then.fec_packets) as u32,
            fec_bytes: now.fec_bytes.wrapping_sub(then.fec_bytes),
            fec_packets_discarded: now
                .fec_packets_discarded
                .wrapping_sub(then.fec_packets_discarded) as u32,
            fec_packets_recovered: now
                .fec_packets_recovered
                .wrapping_sub(then.fec_packets_recovered) as u32,
            packets_lost,
            packets_missing: 0,
            packets_out_of_order: now
                .snapshot_lite
                .packets_out_of_order
                .wrapping_sub(then.snapshot_lite.packets_out_of_order)
                as u32,
            frames: now.frames.wrapping_sub(then.frames),
            rtt_max: then.max_rtt,
            jitter_max: then.max_jitter / self.rtp_stats_base_lite.clock_rate as f64 * 1e6,
            nacks: now
                .snapshot_lite
                .nacks
                .wrapping_sub(then.snapshot_lite.nacks),
            nack_repeated: 0,
            plis: now.plis.wrapping_sub(then.plis),
            firs: now.firs.wrapping_sub(then.firs),
        };
        (Some(delta_info), logging_fields, err)
    }

    pub(super) fn marshal_log_object(
        &self,
        e: &mut fmt::DebugStruct<'_, '_>,
        packets_expected: u64,
        packets_seen_minus_padding: u64,
        ext_start_ts: u64,
        ext_highest_ts: u64,
    ) -> Result<f64, Error> {
        let elapsed_seconds = self.rtp_stats_base_lite.marshal_log_object(
            e,
            packets_expected,
            packets_seen_minus_padding,
        )?;

        e.field("firstTime", &Time::unix(0, self.first_time));
        e.field("firstTimeAdjustment", &self.first_time_adjustment);
        e.field("highestTime", &Time::unix(0, self.highest_time));

        e.field("headerBytes", &self.header_bytes);

        e.field("packetsDuplicate", &self.packets_duplicate);
        e.field(
            "packetsDuplicateRate",
            &(self.packets_duplicate as f64 / elapsed_seconds),
        );
        e.field("bytesDuplicate", &self.bytes_duplicate);
        e.field(
            "bitrateDuplicate",
            &(self.bytes_duplicate as f64 * 8.0 / elapsed_seconds),
        );
        e.field("headerBytesDuplicate", &self.header_bytes_duplicate);

        e.field("packetsPadding", &self.packets_padding);
        e.field(
            "packetsPaddingRate",
            &(self.packets_padding as f64 / elapsed_seconds),
        );
        e.field("bytesPadding", &self.bytes_padding);
        e.field(
            "bitratePadding",
            &(self.bytes_padding as f64 * 8.0 / elapsed_seconds),
        );
        e.field("headerBytesPadding", &self.header_bytes_padding);

        e.field("fecPackets", &self.fec_packets);
        e.field("fecBytes", &self.fec_bytes);
        e.field("fecPacketsDiscarded", &self.fec_packets_discarded);
        e.field("fecPacketsRecovered", &self.fec_packets_recovered);

        e.field("frames", &self.frames);
        e.field("frameRate", &(self.frames as f64 / elapsed_seconds));

        e.field("jitter", &self.jitter);
        e.field("maxJitter", &self.max_jitter);

        e.field("firs", &self.firs);
        e.field("lastFir", &self.last_fir);

        e.field("keyFrames", &self.key_frames);
        e.field("lastKeyFrame", &self.last_key_frame);

        e.field("rtt", &self.rtt);
        e.field("maxRtt", &self.max_rtt);

        e.field(
            "srFirst",
            &WrappedRtcpSenderReportStateLogger {
                rtcp_sender_report_state: self.sr_first,
            },
        );
        e.field(
            "srNewest",
            &WrappedRtcpSenderReportStateLogger {
                rtcp_sender_report_state: self.sr_newest,
            },
        );

        let (packet_drift, ntp_report_drift, received_report_drift, rebased_report_drift) =
            self.get_drift(ext_start_ts, ext_highest_ts);
        e.field(
            "packetDrift",
            &WrappedRtpDriftLogger {
                rtp_drift: packet_drift.as_ref(),
            },
        );
        e.field(
            "ntpReportDrift",
            &WrappedRtpDriftLogger {
                rtp_drift: ntp_report_drift.as_ref(),
            },
        );
        e.field(
            "receivedReportDrift",
            &WrappedRtpDriftLogger {
                rtp_drift: received_report_drift.as_ref(),
            },
        );
        e.field(
            "rebasedReportDrift",
            &WrappedRtpDriftLogger {
                rtp_drift: rebased_report_drift.as_ref(),
            },
        );
        Ok(elapsed_seconds)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn to_proto(
        &self,
        packets_expected: u64,
        packets_seen_minus_padding: u64,
        packets_lost: u64,
        ext_start_ts: u64,
        ext_highest_ts: u64,
        jitter: f64,
        max_jitter: f64,
    ) -> Option<livekit::RtpStats> {
        let mut p = self.rtp_stats_base_lite.to_proto(
            packets_expected,
            packets_seen_minus_padding,
            packets_lost,
        )?;

        p.header_bytes = self.header_bytes;

        p.packets_duplicate = self.packets_duplicate as u32;
        p.packet_duplicate_rate = self.packets_duplicate as f64 / p.duration;
        p.bytes_duplicate = self.bytes_duplicate;
        p.bitrate_duplicate = self.bytes_duplicate as f64 * 8.0 / p.duration;
        p.header_bytes_duplicate = self.header_bytes_duplicate;

        p.packets_padding = self.packets_padding as u32;
        p.packet_padding_rate = self.packets_padding as f64 / p.duration;
        p.bytes_padding = self.bytes_padding;
        p.bitrate_padding = self.bytes_padding as f64 * 8.0 / p.duration;
        p.header_bytes_padding = self.header_bytes_padding;

        p.fec_packets = self.fec_packets as u32;
        p.fec_bytes = self.fec_bytes;
        p.fec_packets_discarded = self.fec_packets_discarded as u32;
        p.fec_packets_recovered = self.fec_packets_recovered as u32;

        p.frames = self.frames;
        p.frame_rate = self.frames as f64 / p.duration;

        p.key_frames = self.key_frames;
        p.last_key_frame = Some(timestamppb_new(self.last_key_frame));

        p.jitter_current = jitter / self.rtp_stats_base_lite.clock_rate as f64 * 1e6;
        p.jitter_max = max_jitter / self.rtp_stats_base_lite.clock_rate as f64 * 1e6;

        p.firs = self.firs;
        p.last_fir = Some(timestamppb_new(self.last_fir));

        p.rtt_current = self.rtt;
        p.rtt_max = self.max_rtt;

        (
            p.packet_drift,
            p.ntp_report_drift,
            p.received_report_drift,
            p.rebased_report_drift,
        ) = self.get_drift(ext_start_ts, ext_highest_ts);
        Some(p)
    }

    pub(super) fn update_jitter(&mut self, ets: u64, packet_time: i64) -> f64 {
        // Do not update jitter on multiple packets of same frame.
        // All packets of a frame have the same time stamp.
        // NOTE: This does not protect against using more than one packet of the same frame
        //       if packets arrive out-of-order. For example,
        //          p1f1 -> p1f2 -> p2f1
        //       In this case, p2f1 (packet 2, frame 1) will still be used in jitter calculation
        //       although it is the second packet of a frame because of out-of-order receival.
        if self.last_jitter_ext_timestamp != ets
            && let Some(rtp_converter) = self.rtp_converter
        {
            let time_since_first = packet_time.wrapping_sub(self.first_time);
            let packet_time_rtp = rtp_converter.to_rtp_ext(Duration(time_since_first));
            let transit = packet_time_rtp.wrapping_sub(ets);

            if self.last_transit != 0 {
                let mut d = transit.wrapping_sub(self.last_transit) as i64;
                if d < 0 {
                    d = d.wrapping_neg();
                }
                self.jitter += (d as f64 - self.jitter) / 16.0;
                if self.jitter > self.max_jitter {
                    self.max_jitter = self.jitter;
                }

                for i in 0..self.next_snapshot_id.wrapping_sub(C_FIRST_SNAPSHOT_ID) {
                    self.snapshots[i as usize].maybe_update_max_jitter(self.jitter);
                }
            }

            self.last_transit = transit;
            self.last_jitter_ext_timestamp = ets;
        }
        self.jitter
    }

    pub(super) fn get_and_reset_snapshot(
        &mut self,
        snapshot_id: u32,
        ext_start_sn: u64,
        ext_highest_sn: u64,
    ) -> (Snapshot, Snapshot, bool) {
        if !self.rtp_stats_base_lite.initialized || snapshot_id < C_FIRST_SNAPSHOT_ID {
            return (Snapshot::default(), Snapshot::default(), false);
        }

        let idx = (snapshot_id - C_FIRST_SNAPSHOT_ID) as usize;
        let mut then = self.snapshots[idx];
        if !then.snapshot_lite.is_valid {
            then = init_snapshot(self.rtp_stats_base_lite.start_time, ext_start_sn);
            self.snapshots[idx] = then;
        }

        // snapshot now
        let now = self.get_snapshot(mono::unix_nano(), ext_highest_sn.wrapping_add(1));
        self.snapshots[idx] = now;
        (then, now, true)
    }

    pub(super) fn get_drift(
        &self,
        ext_start_ts: u64,
        ext_highest_ts: u64,
    ) -> (
        Option<livekit::RtpDrift>,
        Option<livekit::RtpDrift>,
        Option<livekit::RtpDrift>,
        Option<livekit::RtpDrift>,
    ) {
        let mut packet_drift = None;
        let mut ntp_report_drift = None;
        let mut received_report_drift = None;
        let mut rebased_report_drift = None;
        let clock_rate = self.rtp_stats_base_lite.clock_rate;

        if self.first_time != 0 {
            let elapsed = Duration(self.highest_time.wrapping_sub(self.first_time));
            let rtp_clock_ticks = ext_highest_ts.wrapping_sub(ext_start_ts);
            let drift_samples =
                rtp_clock_ticks.wrapping_sub(self.rtp_converter().to_rtp_ext(elapsed)) as i64;
            if elapsed > Duration(0) {
                packet_drift = Some(livekit::RtpDrift {
                    start_time: Some(timestamppb_new(Time::unix(0, self.first_time))),
                    end_time: Some(timestamppb_new(Time::unix(0, self.highest_time))),
                    duration: elapsed.seconds(),
                    start_timestamp: ext_start_ts,
                    end_timestamp: ext_highest_ts,
                    rtp_clock_ticks,
                    drift_samples,
                    drift_ms: (drift_samples as f64 * 1000.0) / clock_rate as f64,
                    clock_rate: rtp_clock_ticks as f64 / elapsed.seconds(),
                });
            }
        }

        if let (Some(sr_first), Some(sr_newest)) = (&self.sr_first, &self.sr_newest)
            && sr_first.rtp_timestamp != sr_newest.rtp_timestamp
        {
            let rtp_clock_ticks = sr_newest
                .rtp_timestamp_ext
                .wrapping_sub(sr_first.rtp_timestamp_ext);

            let sr_first_time = NtpTime(sr_first.ntp_timestamp).time();
            let sr_newest_time = NtpTime(sr_newest.ntp_timestamp).time();
            let mut elapsed = sr_newest_time.sub(sr_first_time);
            if elapsed.seconds() > 0.0 {
                let drift_samples =
                    rtp_clock_ticks.wrapping_sub(self.rtp_converter().to_rtp_ext(elapsed)) as i64;
                ntp_report_drift = Some(livekit::RtpDrift {
                    start_time: Some(timestamppb_new(sr_first_time)),
                    end_time: Some(timestamppb_new(sr_newest_time)),
                    duration: elapsed.seconds(),
                    start_timestamp: sr_first.rtp_timestamp_ext,
                    end_timestamp: sr_newest.rtp_timestamp_ext,
                    rtp_clock_ticks,
                    drift_samples,
                    drift_ms: (drift_samples as f64 * 1000.0) / clock_rate as f64,
                    clock_rate: rtp_clock_ticks as f64 / elapsed.seconds(),
                });
            }

            elapsed = Duration(sr_newest.at.wrapping_sub(sr_first.at));
            if elapsed.seconds() > 0.0 {
                let drift_samples =
                    rtp_clock_ticks.wrapping_sub(self.rtp_converter().to_rtp_ext(elapsed)) as i64;
                received_report_drift = Some(livekit::RtpDrift {
                    start_time: Some(timestamppb_new(Time::unix(0, sr_first.at))),
                    end_time: Some(timestamppb_new(Time::unix(0, sr_newest.at))),
                    duration: elapsed.seconds(),
                    start_timestamp: sr_first.rtp_timestamp_ext,
                    end_timestamp: sr_newest.rtp_timestamp_ext,
                    rtp_clock_ticks,
                    drift_samples,
                    drift_ms: (drift_samples as f64 * 1000.0) / clock_rate as f64,
                    clock_rate: rtp_clock_ticks as f64 / elapsed.seconds(),
                });
            }

            elapsed = Duration(sr_newest.at_adjusted.wrapping_sub(sr_first.at_adjusted));
            if elapsed.seconds() > 0.0 {
                let drift_samples =
                    rtp_clock_ticks.wrapping_sub(self.rtp_converter().to_rtp_ext(elapsed)) as i64;
                rebased_report_drift = Some(livekit::RtpDrift {
                    start_time: Some(timestamppb_new(Time::unix(0, sr_first.at_adjusted))),
                    end_time: Some(timestamppb_new(Time::unix(0, sr_newest.at_adjusted))),
                    duration: elapsed.seconds(),
                    start_timestamp: sr_first.rtp_timestamp_ext,
                    end_timestamp: sr_newest.rtp_timestamp_ext,
                    rtp_clock_ticks,
                    drift_samples,
                    drift_ms: (drift_samples as f64 * 1000.0) / clock_rate as f64,
                    clock_rate: rtp_clock_ticks as f64 / elapsed.seconds(),
                });
            }
        }
        (
            packet_drift,
            ntp_report_drift,
            received_report_drift,
            rebased_report_drift,
        )
    }

    pub(super) fn update_gap_histogram(&mut self, gap: i64) {
        if gap < 2 {
            return;
        }

        let gap_histogram = &mut self.rtp_stats_base_lite.gap_histogram;
        let missing = gap - 1;
        if missing > gap_histogram.len() as i64 {
            let last = gap_histogram.len() - 1;
            gap_histogram[last] = gap_histogram[last].wrapping_add(1);
        } else {
            let bin = (missing - 1) as usize;
            gap_histogram[bin] = gap_histogram[bin].wrapping_add(1);
        }
    }

    pub(super) fn get_snapshot(&self, start_time: i64, ext_start_sn: u64) -> Snapshot {
        Snapshot {
            snapshot_lite: self
                .rtp_stats_base_lite
                .get_snapshot_lite(start_time, ext_start_sn),
            header_bytes: self.header_bytes,
            packets_duplicate: self.packets_duplicate,
            bytes_duplicate: self.bytes_duplicate,
            header_bytes_duplicate: self.header_bytes_duplicate,
            packets_padding: self.packets_padding,
            bytes_padding: self.bytes_padding,
            header_bytes_padding: self.header_bytes_padding,
            fec_packets: self.fec_packets,
            fec_bytes: self.fec_bytes,
            fec_packets_discarded: self.fec_packets_discarded,
            fec_packets_recovered: self.fec_packets_recovered,
            frames: self.frames,
            plis: self.rtp_stats_base_lite.plis,
            firs: self.firs,
            max_rtt: self.rtt,
            max_jitter: self.jitter,
        }
    }
}

// ----------------------------------

pub(super) fn init_snapshot(start_time: i64, ext_start_sn: u64) -> Snapshot {
    Snapshot {
        snapshot_lite: init_snapshot_lite(start_time, ext_start_sn),
        ..Default::default()
    }
}

pub fn aggregate_rtp_stats(stats_list: &[livekit::RtpStats]) -> Option<livekit::RtpStats> {
    utils::aggregate_rtp_stats(stats_list, C_GAP_HISTOGRAM_NUM_BINS)
}

pub fn aggregate_rtp_delta_info(delta_info_list: &[Option<&RtpDeltaInfo>]) -> Option<RtpDeltaInfo> {
    if delta_info_list.is_empty() {
        return None;
    }

    let mut start_time = 0i64;
    let mut end_time = 0i64;

    let mut packets = 0u32;
    let mut bytes = 0u64;
    let mut header_bytes = 0u64;

    let mut packets_duplicate = 0u32;
    let mut bytes_duplicate = 0u64;
    let mut header_bytes_duplicate = 0u64;

    let mut packets_padding = 0u32;
    let mut bytes_padding = 0u64;
    let mut header_bytes_padding = 0u64;

    let mut fec_packets = 0u32;
    let mut fec_bytes = 0u64;
    let mut fec_packets_discarded = 0u32;
    let mut fec_packets_recovered = 0u32;

    let mut packets_lost = 0u32;
    let mut packets_missing = 0u32;
    let mut packets_out_of_order = 0u32;

    let mut frames = 0u32;

    let mut max_rtt = 0u32;
    let mut max_jitter = 0f64;

    let mut nacks = 0u32;
    let mut plis = 0u32;
    let mut firs = 0u32;

    for delta_info in delta_info_list {
        let Some(delta_info) = delta_info else {
            continue;
        };

        if start_time == 0 || start_time > delta_info.start_time.unix_nano() {
            start_time = delta_info.start_time.unix_nano();
        }

        if end_time == 0 || end_time < delta_info.end_time.unix_nano() {
            end_time = delta_info.end_time.unix_nano();
        }

        packets = packets.wrapping_add(delta_info.packets);
        bytes = bytes.wrapping_add(delta_info.bytes);
        header_bytes = header_bytes.wrapping_add(delta_info.header_bytes);

        packets_duplicate = packets_duplicate.wrapping_add(delta_info.packets_duplicate);
        bytes_duplicate = bytes_duplicate.wrapping_add(delta_info.bytes_duplicate);
        header_bytes_duplicate =
            header_bytes_duplicate.wrapping_add(delta_info.header_bytes_duplicate);

        packets_padding = packets_padding.wrapping_add(delta_info.packets_padding);
        bytes_padding = bytes_padding.wrapping_add(delta_info.bytes_padding);
        header_bytes_padding = header_bytes_padding.wrapping_add(delta_info.header_bytes_padding);

        fec_packets = fec_packets.wrapping_add(delta_info.fec_packets);
        fec_bytes = fec_bytes.wrapping_add(delta_info.fec_bytes);
        fec_packets_discarded =
            fec_packets_discarded.wrapping_add(delta_info.fec_packets_discarded);
        fec_packets_recovered =
            fec_packets_recovered.wrapping_add(delta_info.fec_packets_recovered);

        packets_lost = packets_lost.wrapping_add(delta_info.packets_lost);
        packets_missing = packets_missing.wrapping_add(delta_info.packets_missing);
        packets_out_of_order = packets_out_of_order.wrapping_add(delta_info.packets_out_of_order);

        frames = frames.wrapping_add(delta_info.frames);

        if delta_info.rtt_max > max_rtt {
            max_rtt = delta_info.rtt_max;
        }

        if delta_info.jitter_max > max_jitter {
            max_jitter = delta_info.jitter_max;
        }

        nacks = nacks.wrapping_add(delta_info.nacks);
        plis = plis.wrapping_add(delta_info.plis);
        firs = firs.wrapping_add(delta_info.firs);
    }
    if start_time == 0 || end_time == 0 {
        return None;
    }

    Some(RtpDeltaInfo {
        start_time: Time::unix(0, start_time),
        end_time: Time::unix(0, end_time),
        packets,
        bytes,
        header_bytes,
        packets_duplicate,
        bytes_duplicate,
        header_bytes_duplicate,
        packets_padding,
        bytes_padding,
        header_bytes_padding,
        fec_packets,
        fec_bytes,
        fec_packets_discarded,
        fec_packets_recovered,
        packets_lost,
        packets_missing,
        packets_out_of_order,
        frames,
        rtt_max: max_rtt,
        jitter_max: max_jitter,
        nacks,
        nack_repeated: 0,
        plis,
        firs,
    })
}

pub fn reconcile_rtp_stats_with_rtx(
    primary_stats: Option<livekit::RtpStats>,
    rtx_stats: Option<&livekit::RtpStats>,
) -> Option<livekit::RtpStats> {
    let mut primary_stats = primary_stats?;
    let Some(rtx_stats) = rtx_stats else {
        return Some(primary_stats);
    };

    primary_stats.packets_duplicate = primary_stats
        .packets_duplicate
        .wrapping_add(rtx_stats.packets);
    primary_stats.packet_duplicate_rate =
        primary_stats.packets_duplicate as f64 / primary_stats.duration;

    primary_stats.bytes_duplicate = primary_stats.bytes_duplicate.wrapping_add(rtx_stats.bytes);
    primary_stats.header_bytes_duplicate = primary_stats
        .header_bytes_duplicate
        .wrapping_add(rtx_stats.header_bytes);
    primary_stats.bitrate_duplicate =
        primary_stats.bytes_duplicate as f64 * 8.0 / primary_stats.duration;

    primary_stats.packets_padding = primary_stats
        .packets_padding
        .wrapping_add(rtx_stats.packets_padding);
    primary_stats.packet_padding_rate =
        primary_stats.packets_padding as f64 / primary_stats.duration;

    primary_stats.bytes_padding = primary_stats
        .bytes_padding
        .wrapping_add(rtx_stats.bytes_padding);
    primary_stats.header_bytes_padding = primary_stats
        .header_bytes_padding
        .wrapping_add(rtx_stats.header_bytes_padding);
    primary_stats.bitrate_padding =
        primary_stats.bytes_padding as f64 * 8.0 / primary_stats.duration;

    // RTX non-padding packets are responses to NACKs, that should discount packets lost,
    let mut loss_adjustment = rtx_stats
        .packets
        .wrapping_sub(rtx_stats.packets_lost)
        .wrapping_sub(primary_stats.nack_repeated);
    if (loss_adjustment as i32) < 0 {
        loss_adjustment = 0;
    }
    if loss_adjustment >= primary_stats.packets_lost {
        primary_stats.packets_lost = 0;
    } else {
        primary_stats.packets_lost -= loss_adjustment;
    }
    primary_stats.packet_loss_rate = primary_stats.packets_lost as f64 / primary_stats.duration;
    primary_stats.packet_loss_percentage = primary_stats.packets_lost as f32
        / primary_stats
            .packets
            .wrapping_add(primary_stats.packets_padding)
            .wrapping_add(primary_stats.packets_lost) as f32
        * 100.0;
    Some(primary_stats)
}

pub fn reconcile_rtp_delta_info_with_rtx(
    primary_delta_info: Option<RtpDeltaInfo>,
    rtx_delta_info: Option<&RtpDeltaInfo>,
) -> Option<RtpDeltaInfo> {
    let (Some(mut primary_delta_info), Some(rtx_delta_info)) = (primary_delta_info, rtx_delta_info)
    else {
        return primary_delta_info;
    };

    primary_delta_info.packets_duplicate = primary_delta_info
        .packets_duplicate
        .wrapping_add(rtx_delta_info.packets);

    primary_delta_info.bytes_duplicate = primary_delta_info
        .bytes_duplicate
        .wrapping_add(rtx_delta_info.bytes);
    primary_delta_info.header_bytes_duplicate = primary_delta_info
        .header_bytes_duplicate
        .wrapping_add(rtx_delta_info.header_bytes);

    primary_delta_info.packets_padding = primary_delta_info
        .packets_padding
        .wrapping_add(rtx_delta_info.packets_padding);

    primary_delta_info.bytes_padding = primary_delta_info
        .bytes_padding
        .wrapping_add(rtx_delta_info.bytes_padding);
    primary_delta_info.header_bytes_padding = primary_delta_info
        .header_bytes_padding
        .wrapping_add(rtx_delta_info.header_bytes_padding);

    // RTX non-padding packets are responses to NACKs, that should discount packets lost
    let mut loss_adjustment = rtx_delta_info
        .packets
        .wrapping_sub(rtx_delta_info.packets_lost)
        .wrapping_sub(primary_delta_info.nack_repeated);
    if (loss_adjustment as i32) < 0 {
        loss_adjustment = 0;
    }
    if loss_adjustment >= primary_delta_info.packets_lost {
        primary_delta_info.packets_lost = 0;
    } else {
        primary_delta_info.packets_lost -= loss_adjustment;
    }
    Some(primary_delta_info)
}

// -------------------------------------------------------------------

/// The methods of `rtpStatsBase` that take the lock, for an outer type that embeds it: each
/// takes the outer type's lock and calls the method of that name of the state. `SetClockRate`
/// is not among them, see `forward_rtp_stats_base_lite!`.
macro_rules! forward_rtp_stats_base {
    ($outer:ident, $($rtp_stats_base:ident).+) => {
        impl $outer {
            pub fn update_fec(
                &self,
                fec_packets: u64,
                fec_bytes: u64,
                fec_packets_discarded: u64,
                fec_packets_recovered: u64,
            ) {
                self.lock.write().$($rtp_stats_base).+.update_fec(
                    fec_packets,
                    fec_bytes,
                    fec_packets_discarded,
                    fec_packets_recovered,
                )
            }

            pub fn update_fir(&self, fir_count: u32) {
                self.lock.write().$($rtp_stats_base).+.update_fir(fir_count)
            }

            pub fn update_fir_time(&self) {
                self.lock.write().$($rtp_stats_base).+.update_fir_time()
            }

            pub fn update_key_frame(&self, kf_count: u32) {
                self.lock.write().$($rtp_stats_base).+.update_key_frame(kf_count)
            }

            pub fn key_frame(&self) -> (u32, $crate::go::time::Time) {
                self.lock.read().$($rtp_stats_base).+.key_frame()
            }

            pub fn update_rtt(&self, rtt: u32) {
                self.lock.write().$($rtp_stats_base).+.update_rtt(rtt)
            }

            pub fn get_rtt(&self) -> u32 {
                self.lock.read().$($rtp_stats_base).+.get_rtt()
            }
        }
    };
}
pub(super) use forward_rtp_stats_base;
