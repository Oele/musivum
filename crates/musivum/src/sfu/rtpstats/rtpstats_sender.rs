// Ported from LiveKit, pkg/sfu/rtpstats/rtpstats_sender.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the lock is the outer type's and everything else,
// with the `rtpStatsBase` the original embeds, is in `RtpStatsSenderInner` (see
// `rtpstats_base_lite.rs`): the public methods take the lock and what the original calls with
// the lock held are methods of the inner struct; the methods inherited from `rtpStatsBase`
// and `rtpStatsBaseLite` are forwarded; `lockedRTPStatsSenderLogEncoder` is `Debug` of the
// inner struct and `MarshalLogObject` is `Debug` of the outer one, which takes the lock; the
// two structs of logging fields do not hold the statistics object, they are given it when a
// line is written, and `zap.Inline(sulf)` is `sulf.log(level, message, r, more)`; `Seed`
// reads the object it seeds from under that object's lock, which the original does not take;
// what `UpdateFromReceiverReport` defers is written after a block that holds the rest of the
// function; a reception report is taken by reference; `getAndResetSenderSnapshotWindow` and
// its sibling copy the view they need where the original copies the whole snapshot; an unset
// `lastLayerLockPli` is the zero `Time` (see `rtpstats_base.rs`).

use std::fmt;

use parking_lot::RwLock;

use crate::go::time::{self, Duration, Time, timestamppb_new};
use crate::mediatransportutil::{NtpTime, get_rtt_ms, to_ntp_time};
use crate::protocol::livekit;
use crate::protocol::logger::Level;
use crate::protocol::utils::mono;
use crate::webrtc::rtcp::reception_report::ReceptionReport;
use crate::webrtc::rtcp::sender_report::SenderReport;
use crate::{debugw, infow, warnw};

use super::Error;
use super::rtpstats_base::{
    RtpDeltaInfo, RtpStatsBase, WrappedRtcpSenderReportStateLogger, forward_rtp_stats_base,
    init_snapshot,
};
use super::rtpstats_base_lite::{
    C_FIRST_SNAPSHOT_ID, C_NUM_SEQUENCE_NUMBERS, RtpStatsParams, forward_rtp_stats_base_lite,
    get_packets_expected, infow_logging_fields, should_log,
};

// -------------------------------------------------------------------

type SnInfoFlag = u8;

const SN_INFO_FLAG_MARKER: SnInfoFlag = 1 << 0;
const SN_INFO_FLAG_PADDING: SnInfoFlag = 1 << 1;
const SN_INFO_FLAG_OUT_OF_ORDER: SnInfoFlag = 1 << 2;

// max number of receiver reports kept per sender snapshot for logging
const C_MAX_PROCESSED_RECEPTION_REPORTS: usize = 64;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct SnInfo {
    pkt_size: u16,
    hdr_size: u8,
    flags: SnInfoFlag,
}

// -------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct IntervalStats {
    pub(super) packets: u64,
    pub(super) bytes: u64,
    pub(super) header_bytes: u64,
    pub(super) packets_padding: u64,
    pub(super) bytes_padding: u64,
    pub(super) header_bytes_padding: u64,
    pub(super) packets_lost_feed: u64,
    pub(super) packets_out_of_order_feed: u64,
    pub(super) frames: u32,
    pub(super) packets_not_found_metadata: u64,
}

impl IntervalStats {
    fn aggregate(&mut self, other: &IntervalStats) {
        self.packets = self.packets.wrapping_add(other.packets);
        self.bytes = self.bytes.wrapping_add(other.bytes);
        self.header_bytes = self.header_bytes.wrapping_add(other.header_bytes);
        self.packets_padding = self.packets_padding.wrapping_add(other.packets_padding);
        self.bytes_padding = self.bytes_padding.wrapping_add(other.bytes_padding);
        self.header_bytes_padding = self
            .header_bytes_padding
            .wrapping_add(other.header_bytes_padding);
        self.packets_lost_feed = self.packets_lost_feed.wrapping_add(other.packets_lost_feed);
        self.packets_out_of_order_feed = self
            .packets_out_of_order_feed
            .wrapping_add(other.packets_out_of_order_feed);
        self.frames = self.frames.wrapping_add(other.frames);
        self.packets_not_found_metadata = self
            .packets_not_found_metadata
            .wrapping_add(other.packets_not_found_metadata);
    }
}

// -------------------------------------------------------------------

struct WrappedReceptionReportsLogger<'a> {
    sender_snapshot_receiver_view: &'a SenderSnapshotReceiverView,
}

impl fmt::Debug for WrappedReceptionReportsLogger<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let w = self.sender_snapshot_receiver_view;
        let mut e = f.debug_map();
        for i in 0..w.processed_reception_reports_size {
            let rr = &w.processed_reception_reports
                [(w.processed_reception_reports_head + i) % C_MAX_PROCESSED_RECEPTION_REPORTS];
            e.entry(&i, rr);
        }

        e.finish()
    }
}

// -------------------------------------------------------------------

#[derive(Clone, Copy, Default, PartialEq)]
pub(super) struct SenderSnapshotWindow {
    is_valid: bool,

    start_time: i64,

    ext_start_sn: u64,
    bytes: u64,
    header_bytes: u64,

    packets_padding: u64,
    bytes_padding: u64,
    header_bytes_padding: u64,

    packets_duplicate: u64,
    bytes_duplicate: u64,
    header_bytes_duplicate: u64,

    packets_out_of_order_feed: u64,

    packets_lost_feed: u64,

    frames: u32,

    nacks: u32,
    nack_repeated: u32,
    plis: u32,
    firs: u32,

    max_jitter_feed: f64,
}

impl SenderSnapshotWindow {
    fn marshal_log_object(&self, e: &mut fmt::DebugStruct<'_, '_>) {
        e.field("isValid", &self.is_valid);
        e.field("startTime", &Time::unix(0, self.start_time));
        e.field("extStartSN", &self.ext_start_sn);
        e.field("bytes", &self.bytes);
        e.field("headerBytes", &self.header_bytes);
        e.field("packetsPadding", &self.packets_padding);
        e.field("bytesPadding", &self.bytes_padding);
        e.field("headerBytesPadding", &self.header_bytes_padding);
        e.field("packetsDuplicate", &self.packets_duplicate);
        e.field("bytesDuplicate", &self.bytes_duplicate);
        e.field("headerBytesDuplicate", &self.header_bytes_duplicate);
        e.field("packetsOutOfOrderFeed", &self.packets_out_of_order_feed);
        e.field("packetsLostFeed", &self.packets_lost_feed);
        e.field("frames", &self.frames);
        e.field("nacks", &self.nacks);
        e.field("nackRepeated", &self.nack_repeated);
        e.field("plis", &self.plis);
        e.field("firs", &self.firs);
        e.field("maxJitterFeed", &self.max_jitter_feed);
    }

    fn maybe_reinit(&mut self, old_esn: u64, new_esn: u64) {
        if self.ext_start_sn == old_esn {
            self.ext_start_sn = new_esn;
        }
    }

    fn maybe_update_max_jitter_feed(&mut self, jitter: f64) {
        if jitter > self.max_jitter_feed {
            self.max_jitter_feed = jitter;
        }
    }
}

impl fmt::Debug for SenderSnapshotWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("SenderSnapshotWindow");
        self.marshal_log_object(&mut e);
        e.finish()
    }
}

// ---------

#[derive(Clone, PartialEq)]
pub(super) struct SenderSnapshotReceiverView {
    sender_snapshot_window: SenderSnapshotWindow,

    packets_lost: u64,

    max_rtt: u32,
    max_jitter: f64,

    ext_last_rr_sn: u64,
    interval_stats: IntervalStats,

    processed_reception_reports: [ReceptionReport; C_MAX_PROCESSED_RECEPTION_REPORTS],
    processed_reception_reports_head: usize,
    processed_reception_reports_size: usize,

    metadata_cache_overflow_count: i64,
}

impl Default for SenderSnapshotReceiverView {
    fn default() -> Self {
        Self {
            sender_snapshot_window: SenderSnapshotWindow::default(),
            packets_lost: 0,
            max_rtt: 0,
            max_jitter: 0.0,
            ext_last_rr_sn: 0,
            interval_stats: IntervalStats::default(),
            processed_reception_reports: std::array::from_fn(|_| ReceptionReport::default()),
            processed_reception_reports_head: 0,
            processed_reception_reports_size: 0,
            metadata_cache_overflow_count: 0,
        }
    }
}

impl fmt::Debug for SenderSnapshotReceiverView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("SenderSnapshotReceiverView");
        self.sender_snapshot_window.marshal_log_object(&mut e);
        e.field("packetsLost", &self.packets_lost);
        e.field("maxRtt", &self.max_rtt);
        e.field("maxJitter", &self.max_jitter);
        e.field("extLastRRSN", &self.ext_last_rr_sn);
        e.field("intervalStats", &self.interval_stats);
        e.field(
            "processedReceptionReports",
            &WrappedReceptionReportsLogger {
                sender_snapshot_receiver_view: self,
            },
        );
        e.field(
            "metadataCacheOverflowCount",
            &self.metadata_cache_overflow_count,
        );
        e.finish()
    }
}

impl SenderSnapshotReceiverView {
    fn maybe_reinit(&mut self, old_esn: u64, new_esn: u64) {
        if self.sender_snapshot_window.ext_start_sn == old_esn {
            self.sender_snapshot_window.ext_start_sn = new_esn;
            if self.ext_last_rr_sn == old_esn.wrapping_sub(1) {
                self.ext_last_rr_sn = new_esn.wrapping_sub(1);
            }
        }
    }

    fn maybe_update_max_rtt(&mut self, rtt: u32) {
        if rtt > self.max_rtt {
            self.max_rtt = rtt;
        }
    }

    fn maybe_update_max_jitter(&mut self, jitter: f64) {
        if jitter > self.max_jitter {
            self.max_jitter = jitter;
        }
    }
}

// ---------

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct SenderSnapshot {
    sender_view: SenderSnapshotWindow,
    receiver_view: SenderSnapshotReceiverView,
}

impl SenderSnapshot {
    fn maybe_reinit(&mut self, old_esn: u64, new_esn: u64) {
        self.sender_view.maybe_reinit(old_esn, new_esn);
        self.receiver_view.maybe_reinit(old_esn, new_esn);
    }

    fn maybe_update_max_jitter_feed(&mut self, jitter: f64) {
        self.sender_view.maybe_update_max_jitter_feed(jitter);
        self.receiver_view
            .sender_snapshot_window
            .maybe_update_max_jitter_feed(jitter);
    }

    fn maybe_update_max_rtt(&mut self, rtt: u32) {
        self.receiver_view.maybe_update_max_rtt(rtt);
    }

    fn maybe_update_max_jitter(&mut self, jitter: f64) {
        self.receiver_view.maybe_update_max_jitter(jitter);
    }
}

// -------------------------------------------------------------------

struct SenderUpdateLoggingFields {
    packet_time: i64,
    ext_sequence_number: u64,
    ext_timestamp: u64,
    marker: bool,
    hdr_size: usize,
    payload_size: usize,
    padding_size: usize,
    gap_sn: i64,
    gap_ts: i64,
    time_since_highest: i64,
}

impl SenderUpdateLoggingFields {
    // A line with what `MarshalLogObject` of the original adds, which `zap.Inline(sulf)`
    // writes into the line itself, and then `more`.
    fn log(
        &self,
        level: Level,
        msg: &str,
        rtp_stats: &RtpStatsSenderInner,
        more: &[(&str, &dyn fmt::Debug)],
    ) {
        let logger = &rtp_stats.rtp_stats_base.rtp_stats_base_lite.logger;
        if !logger.enabled(level) {
            return;
        }

        let packet_time = Time::unix(0, self.packet_time);
        let time_since_highest = Duration(self.time_since_highest);
        let mut kvs: Vec<(&str, &dyn fmt::Debug)> = vec![
            ("currSN", &self.ext_sequence_number),
            ("gapSN", &self.gap_sn),
            ("currTS", &self.ext_timestamp),
            ("gapTS", &self.gap_ts),
            ("packetTime", &packet_time),
            ("timeSinceHighest", &time_since_highest),
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

// -------------------------------------------------------------------

struct SenderReportLoggingFields {
    sr_data: livekit::RtcpSenderReportState,
    publisher_sr_data: livekit::RtcpSenderReportState,
    ts_offset: u64,
    report_time: i64,
    report_time_adjusted: i64,
    now_nano: i64,
    now_rtp_ext: u64,
}

impl SenderReportLoggingFields {
    // As `SenderUpdateLoggingFields::log`, for `zap.Inline(srlf)`.
    fn log(
        &self,
        level: Level,
        msg: &str,
        rtp_stats: &RtpStatsSenderInner,
        more: &[(&str, &dyn fmt::Debug)],
    ) {
        let logger = &rtp_stats.rtp_stats_base.rtp_stats_base_lite.logger;
        if !logger.enabled(level) {
            return;
        }

        let curr = WrappedRtcpSenderReportStateLogger {
            rtcp_sender_report_state: Some(self.sr_data),
        };
        let feed = WrappedRtcpSenderReportStateLogger {
            rtcp_sender_report_state: Some(self.publisher_sr_data),
        };
        let time_now = time::now();
        let time_since_highest = Duration(
            self.now_nano
                .wrapping_sub(rtp_stats.rtp_stats_base.highest_time),
        );
        let time_since_first = Duration(
            self.now_nano
                .wrapping_sub(rtp_stats.rtp_stats_base.first_time),
        );
        let time_since_publisher_sr_adjusted = Duration(
            self.now_nano
                .wrapping_sub(self.publisher_sr_data.at_adjusted),
        );
        let time_since_publisher_sr =
            Duration(self.now_nano.wrapping_sub(self.publisher_sr_data.at));
        let mut kvs: Vec<(&str, &dyn fmt::Debug)> = vec![
            ("curr", &curr),
            ("feed", &feed),
            ("tsOffset", &self.ts_offset),
            ("timeNow", &time_now),
            ("reportTime", &self.report_time),
            ("reportTimeAdjusted", &self.report_time_adjusted),
            ("timeSinceHighest", &time_since_highest),
            ("timeSinceFirst", &time_since_first),
            (
                "timeSincePublisherSRAdjusted",
                &time_since_publisher_sr_adjusted,
            ),
            ("timeSincePublisherSR", &time_since_publisher_sr),
            ("nowRTPExt", &self.now_rtp_ext),
            ("rtpStats", rtp_stats),
        ];
        kvs.extend_from_slice(more);
        logger.log(level, msg, None, &kvs);
    }
}

// -------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct RttMarker {
    ntp_time: NtpTime,
    sent_at: Time,
}

// -------------------------------------------------------------------

pub struct RtpStatsSender {
    pub(super) lock: RwLock<RtpStatsSenderInner>,
}

// What the lock guards: all of `RTPStatsSender` upstream.
pub(super) struct RtpStatsSenderInner {
    pub(super) rtp_stats_base: RtpStatsBase,

    pub(super) ext_start_sn: u64,
    pub(super) ext_highest_sn: u64,
    pub(super) ext_highest_sn_from_rr: u64,
    pub(super) ext_highest_sn_from_rr_misalignment: u64,

    pub(super) rtt_marker: RttMarker,

    pub(super) last_rr_time: i64,
    pub(super) last_rr: ReceptionReport,

    pub(super) ext_start_ts: u64,
    pub(super) ext_highest_ts: u64,

    pub(super) packets_lost_from_rr: u64,

    pub(super) jitter_from_rr: f64,
    pub(super) max_jitter_from_rr: f64,

    pub(super) sn_infos: Vec<SnInfo>,

    pub(super) layer_lock_plis: u32,
    pub(super) last_layer_lock_pli: Time,

    pub(super) next_sender_snapshot_id: u32,
    pub(super) sender_snapshots: Vec<SenderSnapshot>,

    pub(super) clock_skew_count: i64,
    pub(super) time_reversed_count: i64,
}

forward_rtp_stats_base_lite!(RtpStatsSender, rtp_stats_base.rtp_stats_base_lite);
forward_rtp_stats_base!(RtpStatsSender, rtp_stats_base);

impl RtpStatsSender {
    pub fn new(params: RtpStatsParams, cache_size: usize) -> Self {
        Self {
            lock: RwLock::new(RtpStatsSenderInner {
                rtp_stats_base: RtpStatsBase::new(params),
                ext_start_sn: 0,
                ext_highest_sn: 0,
                ext_highest_sn_from_rr: 0,
                ext_highest_sn_from_rr_misalignment: 0,
                rtt_marker: RttMarker::default(),
                last_rr_time: 0,
                last_rr: ReceptionReport::default(),
                ext_start_ts: 0,
                ext_highest_ts: 0,
                packets_lost_from_rr: 0,
                jitter_from_rr: 0.0,
                max_jitter_from_rr: 0.0,
                sn_infos: vec![SnInfo::default(); cache_size],
                layer_lock_plis: 0,
                last_layer_lock_pli: Time::ZERO,
                next_sender_snapshot_id: C_FIRST_SNAPSHOT_ID,
                sender_snapshots: vec![SenderSnapshot::default(); 2],
                clock_skew_count: 0,
                time_reversed_count: 0,
            }),
        }
    }

    // rtpStatsBase.SetClockRate, inherited upstream
    pub fn set_clock_rate(&self, clock_rate: u32) {
        self.lock.write().rtp_stats_base.set_clock_rate(clock_rate)
    }

    pub fn seed(&self, from: &RtpStatsSender) {
        // An object seeded from itself stays as it is upstream, where that needs one lock;
        // here it would need the same lock twice.
        if std::ptr::eq(self, from) {
            return;
        }
        let from = from.lock.read();
        let mut lock = self.lock.write();
        let r = &mut *lock;

        if !r.rtp_stats_base.seed(&from.rtp_stats_base) {
            return;
        }

        r.ext_start_sn = from.ext_start_sn;
        r.ext_highest_sn = from.ext_highest_sn;
        r.ext_highest_sn_from_rr = from.ext_highest_sn_from_rr;
        r.ext_highest_sn_from_rr_misalignment = from.ext_highest_sn_from_rr_misalignment;

        r.rtt_marker = from.rtt_marker;

        r.last_rr_time = from.last_rr_time;
        r.last_rr = from.last_rr.clone();

        r.ext_start_ts = from.ext_start_ts;
        r.ext_highest_ts = from.ext_highest_ts;

        r.packets_lost_from_rr = from.packets_lost_from_rr;

        r.jitter_from_rr = from.jitter_from_rr;
        r.max_jitter_from_rr = from.max_jitter_from_rr;

        r.sn_infos = from.sn_infos.clone();

        r.layer_lock_plis = from.layer_lock_plis;
        r.last_layer_lock_pli = from.last_layer_lock_pli;

        r.next_sender_snapshot_id = from.next_sender_snapshot_id;
        r.sender_snapshots = from.sender_snapshots.clone();
    }

    pub fn new_snapshot_id(&self) -> u32 {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        r.rtp_stats_base.new_snapshot_id(r.ext_highest_sn)
    }

    pub fn new_sender_snapshot_id(&self) -> u32 {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        let id = r.next_sender_snapshot_id;
        r.next_sender_snapshot_id = r.next_sender_snapshot_id.wrapping_add(1);

        let num = r.next_sender_snapshot_id.wrapping_sub(C_FIRST_SNAPSHOT_ID) as usize;
        if r.sender_snapshots.len() < num {
            r.sender_snapshots.resize(num, SenderSnapshot::default());
        }

        if r.rtp_stats_base.rtp_stats_base_lite.initialized {
            r.sender_snapshots[id.wrapping_sub(C_FIRST_SNAPSHOT_ID) as usize] =
                init_sender_snapshot(mono::unix_nano(), r.ext_highest_sn);
        }
        id
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update(
        &self,
        packet_time: i64,
        ext_sequence_number: u64,
        ext_timestamp: u64,
        marker: bool,
        hdr_size: usize,
        payload_size: usize,
        padding_size: usize,
        is_out_of_order: bool,
    ) {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        if r.rtp_stats_base.rtp_stats_base_lite.end_time != 0
            || r.rtp_stats_base.rtp_stats_base_lite.clock_rate == 0
        {
            return;
        }

        if !r.rtp_stats_base.rtp_stats_base_lite.initialized {
            if payload_size == 0 && !r.rtp_stats_base.rtp_stats_base_lite.params.is_rtx {
                // do not start on a padding only packet
                return;
            }

            r.rtp_stats_base.rtp_stats_base_lite.initialized = true;

            r.rtp_stats_base.rtp_stats_base_lite.start_time = mono::unix_nano();

            r.rtp_stats_base.highest_time = packet_time;

            r.ext_start_sn = ext_sequence_number;
            r.ext_highest_sn = ext_sequence_number.wrapping_sub(1);

            r.ext_start_ts = ext_timestamp;
            r.ext_highest_ts = ext_timestamp;

            // initialize snapshots if any
            for i in 0..r
                .rtp_stats_base
                .next_snapshot_id
                .wrapping_sub(C_FIRST_SNAPSHOT_ID)
            {
                r.rtp_stats_base.snapshots[i as usize] = init_snapshot(
                    r.rtp_stats_base.rtp_stats_base_lite.start_time,
                    r.ext_start_sn,
                );
            }
            for i in 0..r.next_sender_snapshot_id.wrapping_sub(C_FIRST_SNAPSHOT_ID) {
                r.sender_snapshots[i as usize] = init_sender_snapshot(
                    r.rtp_stats_base.rtp_stats_base_lite.start_time,
                    r.ext_start_sn,
                );
            }

            debugw!(
                r.rtp_stats_base.rtp_stats_base_lite.logger,
                "rtp sender stream start",
                "rtpStats" => r,
            );
        }
        if !is_out_of_order && r.rtp_stats_base.first_time == 0 {
            // do not set first packet time if packet is out-of-order,
            // as first packet time is used to calculate expected time stamp,
            // using an out-of-order packet would skew that.
            r.rtp_stats_base.first_time = packet_time;
        }

        let pkt_size = hdr_size
            .wrapping_add(payload_size)
            .wrapping_add(padding_size) as u64;
        let mut is_duplicate = false;
        let gap_sn = ext_sequence_number.wrapping_sub(r.ext_highest_sn) as i64;
        if gap_sn <= 0 {
            // duplicate OR out-of-order
            if payload_size == 0
                && ext_sequence_number < r.ext_start_sn
                && !r.rtp_stats_base.rtp_stats_base_lite.params.is_rtx
            {
                // do not start on a padding only packet
                return;
            }

            if ext_sequence_number < r.ext_start_sn {
                r.rtp_stats_base.rtp_stats_base_lite.packets_lost = r
                    .rtp_stats_base
                    .rtp_stats_base_lite
                    .packets_lost
                    .wrapping_add(
                        r.ext_start_sn
                            .wrapping_sub(ext_sequence_number)
                            .wrapping_sub(1),
                    );

                // adjust start of snapshots
                for i in 0..r
                    .rtp_stats_base
                    .next_snapshot_id
                    .wrapping_sub(C_FIRST_SNAPSHOT_ID)
                {
                    let s = &mut r.rtp_stats_base.snapshots[i as usize];
                    if s.snapshot_lite.ext_start_sn == r.ext_start_sn {
                        s.snapshot_lite.ext_start_sn = ext_sequence_number;
                    }
                }
                for i in 0..r.next_sender_snapshot_id.wrapping_sub(C_FIRST_SNAPSHOT_ID) {
                    r.sender_snapshots[i as usize]
                        .maybe_reinit(r.ext_start_sn, ext_sequence_number);
                }

                let sulf = SenderUpdateLoggingFields {
                    packet_time,
                    ext_sequence_number,
                    ext_timestamp,
                    marker,
                    hdr_size,
                    payload_size,
                    padding_size,
                    gap_sn,
                    gap_ts: ext_timestamp.wrapping_sub(r.ext_highest_ts) as i64,
                    time_since_highest: packet_time.wrapping_sub(r.rtp_stats_base.highest_time),
                };
                sulf.log(
                    Level::Info,
                    "adjusting start sequence number",
                    r,
                    &[
                        ("snAfter", &ext_sequence_number),
                        ("tsAfter", &ext_timestamp),
                    ],
                );
                r.ext_start_sn = ext_sequence_number;
            }

            if gap_sn != 0 {
                r.rtp_stats_base.rtp_stats_base_lite.packets_out_of_order += 1;
            }

            if !r.is_sn_info_lost(ext_sequence_number, r.ext_highest_sn) {
                r.rtp_stats_base.bytes_duplicate =
                    r.rtp_stats_base.bytes_duplicate.wrapping_add(pkt_size);
                r.rtp_stats_base.header_bytes_duplicate = r
                    .rtp_stats_base
                    .header_bytes_duplicate
                    .wrapping_add(hdr_size as u64);
                r.rtp_stats_base.packets_duplicate += 1;
                is_duplicate = true;
            } else {
                r.rtp_stats_base.rtp_stats_base_lite.packets_lost = r
                    .rtp_stats_base
                    .rtp_stats_base_lite
                    .packets_lost
                    .wrapping_sub(1);
                r.set_sn_info(
                    ext_sequence_number,
                    r.ext_highest_sn,
                    pkt_size as u16,
                    hdr_size as u8,
                    payload_size as u16,
                    marker,
                    true,
                );
            }
        } else {
            // in-order
            if ext_timestamp < r.ext_highest_ts {
                r.time_reversed_count += 1;
                if should_log(r.time_reversed_count) {
                    let sulf = SenderUpdateLoggingFields {
                        packet_time,
                        ext_sequence_number,
                        ext_timestamp,
                        marker,
                        hdr_size,
                        payload_size,
                        padding_size,
                        gap_sn,
                        gap_ts: ext_timestamp.wrapping_sub(r.ext_highest_ts) as i64,
                        time_since_highest: packet_time.wrapping_sub(r.rtp_stats_base.highest_time),
                    };
                    sulf.log(
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
            r.clear_sn_infos(r.ext_highest_sn.wrapping_add(1), ext_sequence_number);
            r.rtp_stats_base.rtp_stats_base_lite.packets_lost = r
                .rtp_stats_base
                .rtp_stats_base_lite
                .packets_lost
                .wrapping_add((gap_sn - 1) as u64);

            r.set_sn_info(
                ext_sequence_number,
                r.ext_highest_sn,
                pkt_size as u16,
                hdr_size as u8,
                payload_size as u16,
                marker,
                false,
            );

            r.ext_highest_sn = ext_sequence_number;
        }

        if ext_timestamp < r.ext_start_ts {
            let sulf = SenderUpdateLoggingFields {
                packet_time,
                ext_sequence_number,
                ext_timestamp,
                marker,
                hdr_size,
                payload_size,
                padding_size,
                gap_sn,
                gap_ts: ext_timestamp.wrapping_sub(r.ext_highest_ts) as i64,
                time_since_highest: packet_time.wrapping_sub(r.rtp_stats_base.highest_time),
            };
            sulf.log(
                Level::Info,
                "adjusting start timestamp",
                r,
                &[
                    ("snAfter", &ext_sequence_number),
                    ("tsAfter", &ext_timestamp),
                ],
            );
            r.ext_start_ts = ext_timestamp;
        }

        if ext_timestamp > r.ext_highest_ts {
            // update only on first packet as same timestamp could be in multiple packets.
            // NOTE: this may not be the first packet with this time stamp if there is packet loss.
            if payload_size > 0 {
                // skip updating on padding only packets as they could re-use an old timestamp
                r.rtp_stats_base.highest_time = packet_time;
            }
            r.ext_highest_ts = ext_timestamp;
        }

        if !is_duplicate {
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

                let jitter = r.rtp_stats_base.update_jitter(ext_timestamp, packet_time);
                for i in 0..r.next_sender_snapshot_id.wrapping_sub(C_FIRST_SNAPSHOT_ID) {
                    r.sender_snapshots[i as usize].maybe_update_max_jitter_feed(jitter);
                }
            }
        }
    }

    pub fn update_layer_lock_pli_and_time(&self, pli_count: u32) {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        if r.rtp_stats_base.rtp_stats_base_lite.end_time != 0 {
            return;
        }

        r.layer_lock_plis = r.layer_lock_plis.wrapping_add(pli_count);
        r.last_layer_lock_pli = time::now();
    }

    pub fn get_packets_seen_minus_padding(&self) -> u64 {
        let r = self.lock.read();

        r.rtp_stats_base
            .get_packets_seen_minus_padding(r.ext_start_sn, r.ext_highest_sn)
    }

    pub fn update_from_receiver_report(&self, rr: &ReceptionReport) -> (u32, bool) {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        if !r.rtp_stats_base.rtp_stats_base_lite.initialized
            || r.rtp_stats_base.rtp_stats_base_lite.end_time != 0
        {
            return (0, false);
        }

        let mut ext_highest_sn_from_rr = (r.ext_highest_sn_from_rr & 0xFFFF_FFFF_0000_0000)
            .wrapping_add(rr.last_sequence_number as u64)
            .wrapping_add(r.ext_highest_sn_from_rr_misalignment);
        if r.last_rr_time != 0
            && rr
                .last_sequence_number
                .wrapping_sub(r.last_rr.last_sequence_number)
                < (1 << 31)
            && rr.last_sequence_number < r.last_rr.last_sequence_number
        {
            ext_highest_sn_from_rr = ext_highest_sn_from_rr.wrapping_add(1 << 32);
        }
        if ext_highest_sn_from_rr.wrapping_add(r.ext_start_sn & 0xFFFF_FFFF_FFFF_0000)
            < r.ext_start_sn
        {
            // it is possible that the `LastSequenceNumber` in the receiver report is before the starting
            // sequence number when dummy packets are used to trigger Pion's OnTrack path.
            return (0, false);
        }

        let now_nano = mono::unix_nano();

        // The rest of the function is this block; what the original defers follows it.
        let (rtt, is_rtt_changed) = 'deferred: {
            let mut time_since_last_rr =
                Duration(now_nano.wrapping_sub(r.rtp_stats_base.rtp_stats_base_lite.start_time));
            if r.last_rr_time != 0 {
                time_since_last_rr = Duration(now_nano.wrapping_sub(r.last_rr_time));
            }

            let mut ext_received_rr_sn =
                ext_highest_sn_from_rr.wrapping_add(r.ext_start_sn & 0xFFFF_FFFF_FFFF_0000);
            if (r.ext_highest_sn.wrapping_sub(ext_received_rr_sn) as i64) < 0
                || (r.ext_highest_sn.wrapping_sub(ext_received_rr_sn) as i64) > (1 << 32)
            {
                infow!(
                    r.rtp_stats_base.rtp_stats_base_lite.logger,
                    "receiver report runaway, dropping",
                    "timeSinceLastRR" => time_since_last_rr,
                    "receivedRR" => rr,
                    "extHighestSNFromRR" => ext_highest_sn_from_rr,
                    "extReceivedRRSN" => ext_received_rr_sn,
                    "rtpStats" => r,
                );
                break 'deferred (0, false);
            }

            if r.ext_highest_sn_from_rr != ext_highest_sn_from_rr
                && (r.ext_highest_sn.wrapping_sub(ext_received_rr_sn) as i64) >= (1 << 16)
            {
                // there are cases where remote does not send RTCP Receiver Report for extended periods of time,
                // some times several minutes, in that interval the sequence number rolls over,
                //
                // NOTE: even if there is a large gap in time, the sequence number should be higher
                // than previous report (extended sequence number in receiver report is 32-bit wide and
                // should not roll over for long time, for e. g. it will approximately take 100 days at 500 pps).
                // So, there seems to be a remote reporter issue where the sequence number rollover is missed.
                //
                // catch up till difference between highest sent and highest received via receiver report is
                // less than full 16-bit range.
                //
                // in a different flavor, there are clients that do not report properly,
                // i. e. never update the last received sequence number,
                // so skip any catch up if the last receeved sequence number reported in
                // RTCP RR does not change.
                infow!(
                    r.rtp_stats_base.rtp_stats_base_lite.logger,
                    "receiver report missed rollover, adjusting",
                    "timeSinceLastRR" => time_since_last_rr,
                    "receivedRR" => rr,
                    "extHighestSNFromRR" => ext_highest_sn_from_rr,
                    "extReceivedRRSN" => ext_received_rr_sn,
                    "rtpStats" => r,
                );
                while (r.ext_highest_sn.wrapping_sub(ext_received_rr_sn) as i64) >= (1 << 16) {
                    ext_highest_sn_from_rr = ext_highest_sn_from_rr.wrapping_add(1 << 16);
                    r.ext_highest_sn_from_rr_misalignment =
                        r.ext_highest_sn_from_rr_misalignment.wrapping_add(1 << 16);
                    ext_received_rr_sn =
                        ext_highest_sn_from_rr.wrapping_add(r.ext_start_sn & 0xFFFF_FFFF_FFFF_0000);
                }
                infow!(
                    r.rtp_stats_base.rtp_stats_base_lite.logger,
                    "receiver report missed rollover, adjusted",
                    "timeSinceLastRR" => time_since_last_rr,
                    "receivedRR" => rr,
                    "extHighestSNFromRR" => ext_highest_sn_from_rr,
                    "extReceivedRRSN" => ext_received_rr_sn,
                    "rtpStats" => r,
                );
            }

            if r.ext_highest_sn < ext_received_rr_sn {
                // if remote adjusts somehow, roll back alignment
                infow!(
                    r.rtp_stats_base.rtp_stats_base_lite.logger,
                    "receiver report caught up rollover, adjusting",
                    "timeSinceLastRR" => time_since_last_rr,
                    "receivedRR" => rr,
                    "extHighestSNFromRR" => ext_highest_sn_from_rr,
                    "extReceivedRRSN" => ext_received_rr_sn,
                    "rtpStats" => r,
                );
                while r.ext_highest_sn < ext_received_rr_sn {
                    ext_highest_sn_from_rr = ext_highest_sn_from_rr.wrapping_sub(1 << 16);
                    r.ext_highest_sn_from_rr_misalignment =
                        r.ext_highest_sn_from_rr_misalignment.wrapping_sub(1 << 16);
                    ext_received_rr_sn =
                        ext_highest_sn_from_rr.wrapping_add(r.ext_start_sn & 0xFFFF_FFFF_FFFF_0000);
                }
                infow!(
                    r.rtp_stats_base.rtp_stats_base_lite.logger,
                    "receiver report caught up rollover, adjusted",
                    "timeSinceLastRR" => time_since_last_rr,
                    "receivedRR" => rr,
                    "extHighestSNFromRR" => ext_highest_sn_from_rr,
                    "extReceivedRRSN" => ext_received_rr_sn,
                    "rtpStats" => r,
                );
            }

            if r.ext_highest_sn_from_rr > ext_highest_sn_from_rr {
                debugw!(
                    r.rtp_stats_base.rtp_stats_base_lite.logger,
                    "receiver report out-of-order, dropping",
                    "timeSinceLastRR" => time_since_last_rr,
                    "receivedRR" => rr,
                    "extHighestSNFromRR" => ext_highest_sn_from_rr,
                    "extReceivedRRSN" => ext_received_rr_sn,
                    "rtpStats" => r,
                );
                break 'deferred (0, false);
            }
            r.ext_highest_sn_from_rr = ext_highest_sn_from_rr;

            let mut rtt = 0u32;
            let mut is_rtt_changed = false;
            if r.rtp_stats_base.sr_newest.is_some() {
                match get_rtt_ms(rr, r.rtt_marker.ntp_time, r.rtt_marker.sent_at) {
                    Ok(v) => {
                        rtt = v;
                        is_rtt_changed = rtt != r.rtp_stats_base.rtt;
                    }
                    Err(err) => {
                        debugw!(
                            r.rtp_stats_base.rtp_stats_base_lite.logger,
                            "error getting rtt",
                            "error" => err,
                        );
                    }
                }
            }

            r.packets_lost_from_rr = rr.total_lost as u64;
            let loss_delta = rr.total_lost.wrapping_sub(r.last_rr.total_lost) & ((1 << 24) - 1);
            if loss_delta < (1 << 23) && rr.total_lost < r.last_rr.total_lost {
                r.packets_lost_from_rr = r.packets_lost_from_rr.wrapping_add(1 << 24);
            }

            if is_rtt_changed {
                r.rtp_stats_base.rtt = rtt;
                if rtt > r.rtp_stats_base.max_rtt {
                    r.rtp_stats_base.max_rtt = rtt;
                }
            }

            r.jitter_from_rr = rr.jitter as f64;
            if r.jitter_from_rr > r.max_jitter_from_rr {
                r.max_jitter_from_rr = r.jitter_from_rr;
            }

            // update snapshots
            for i in 0..r
                .rtp_stats_base
                .next_snapshot_id
                .wrapping_sub(C_FIRST_SNAPSHOT_ID)
            {
                let s = &mut r.rtp_stats_base.snapshots[i as usize];
                if is_rtt_changed {
                    s.maybe_update_max_rtt(rtt);
                }
            }

            for i in 0..r.next_sender_snapshot_id.wrapping_sub(C_FIRST_SNAPSHOT_ID) {
                let idx = i as usize;
                let s = &mut r.sender_snapshots[idx];
                if is_rtt_changed {
                    s.maybe_update_max_rtt(rtt);
                }

                s.maybe_update_max_jitter(r.jitter_from_rr);

                // on every RR, calculate delta since last RR using packet metadata cache
                let is = r.get_interval_stats(
                    r.sender_snapshots[idx]
                        .receiver_view
                        .ext_last_rr_sn
                        .wrapping_add(1),
                    ext_received_rr_sn.wrapping_add(1),
                    r.ext_highest_sn,
                );
                let s = &mut r.sender_snapshots[idx];
                s.receiver_view.interval_stats.aggregate(&is);
                if is.packets_not_found_metadata != 0 {
                    s.receiver_view.metadata_cache_overflow_count += 1;
                    if (s.receiver_view.metadata_cache_overflow_count - 1) % 10 == 0 {
                        let s = &r.sender_snapshots[idx];
                        infow!(
                            r.rtp_stats_base.rtp_stats_base_lite.logger,
                            "metadata cache overflow",
                            "senderSnapshotID" => i.wrapping_add(C_FIRST_SNAPSHOT_ID),
                            "senderSnapshot" => s,
                            "timeSinceLastRR" => time_since_last_rr,
                            "receivedRR" => rr,
                            "extReceivedRRSN" => ext_received_rr_sn,
                            "packetsInInterval" => ext_received_rr_sn
                                .wrapping_sub(s.receiver_view.ext_last_rr_sn),
                            "intervalStats" => is,
                            "aggregateIntervalStats" => s.receiver_view.interval_stats,
                            "rtpStats" => r,
                        );
                    }
                }
                let s = &mut r.sender_snapshots[idx];
                s.receiver_view.ext_last_rr_sn = ext_received_rr_sn;

                s.receiver_view.processed_reception_reports
                    [s.receiver_view.processed_reception_reports_head] = rr.clone();
                s.receiver_view.processed_reception_reports_head += 1;
                if s.receiver_view.processed_reception_reports_head
                    >= C_MAX_PROCESSED_RECEPTION_REPORTS
                {
                    s.receiver_view.processed_reception_reports_head = 0;
                }
                s.receiver_view.processed_reception_reports_size += 1;
                if s.receiver_view.processed_reception_reports_size
                    > C_MAX_PROCESSED_RECEPTION_REPORTS
                {
                    s.receiver_view.processed_reception_reports_size =
                        C_MAX_PROCESSED_RECEPTION_REPORTS;
                }
            }

            (rtt, is_rtt_changed)
        };

        r.last_rr_time = now_nano;
        r.last_rr = rr.clone();

        (rtt, is_rtt_changed)
    }

    pub fn last_receiver_report_time(&self) -> i64 {
        let r = self.lock.read();

        r.last_rr_time
    }

    pub fn maybe_adjust_first_packet_time(
        &self,
        publisher_sr_data: Option<&livekit::RtcpSenderReportState>,
        ts_offset: u64,
    ) {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        let Some(publisher_sr_data) = publisher_sr_data else {
            return;
        };
        if !r.rtp_stats_base.rtp_stats_base_lite.initialized
            || r.rtp_stats_base.rtp_stats_base_lite.clock_rate == 0
        {
            return;
        }

        let (_, logging_fields, err) = r.rtp_stats_base.maybe_adjust_first_packet_time(
            publisher_sr_data,
            ts_offset,
            r.ext_start_ts,
        );
        if let Some(err) = err {
            infow_logging_fields(
                &r.rtp_stats_base.rtp_stats_base_lite.logger,
                err,
                &logging_fields,
                r,
            );
        }
    }

    pub fn get_expected_rtp_timestamp(&self, at_nano: i64) -> Result<u64, Error> {
        let r = self.lock.read();

        if r.rtp_stats_base.first_time == 0 || r.rtp_stats_base.rtp_stats_base_lite.clock_rate == 0
        {
            return Err(Error::Uninitialized);
        }

        let time_diff = Duration(at_nano.wrapping_sub(r.rtp_stats_base.first_time));
        let expected_ts_ext = r
            .ext_start_ts
            .wrapping_add(r.rtp_stats_base.rtp_converter().to_rtp_ext(time_diff));
        Ok(expected_ts_ext)
    }

    pub fn get_rtcp_sender_report(
        &self,
        ssrc: u32,
        publisher_sr_data: Option<&livekit::RtcpSenderReportState>,
        ts_offset: u64,
        pass_through: bool,
    ) -> Option<SenderReport> {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        let publisher_sr_data = publisher_sr_data?;
        if !r.rtp_stats_base.rtp_stats_base_lite.initialized
            || r.rtp_stats_base.rtp_stats_base_lite.clock_rate == 0
        {
            return None;
        }

        let report_time: i64;
        let report_time_adjusted: i64;
        let now_ntp: NtpTime;
        let now_rtp_ext: u64;
        let now_nano = mono::unix_nano();
        if pass_through {
            let time_since_publisher_sr = Duration(now_nano.wrapping_sub(publisher_sr_data.at));
            report_time = publisher_sr_data
                .at
                .wrapping_add(time_since_publisher_sr.nanoseconds());
            report_time_adjusted = publisher_sr_data
                .at_adjusted
                .wrapping_add(time_since_publisher_sr.nanoseconds());

            now_ntp = to_ntp_time(
                NtpTime(publisher_sr_data.ntp_timestamp)
                    .time()
                    .add(time_since_publisher_sr),
            );
            now_rtp_ext = publisher_sr_data
                .rtp_timestamp_ext
                .wrapping_sub(ts_offset)
                .wrapping_add(
                    r.rtp_stats_base
                        .rtp_converter()
                        .to_rtp_ext(time_since_publisher_sr),
                );
        } else {
            let time_since_publisher_sr_adjusted =
                Duration(now_nano.wrapping_sub(publisher_sr_data.at_adjusted));
            report_time_adjusted = publisher_sr_data
                .at_adjusted
                .wrapping_add(time_since_publisher_sr_adjusted.nanoseconds());
            report_time = report_time_adjusted;

            now_ntp = to_ntp_time(Time::unix(0, report_time));
            now_rtp_ext = publisher_sr_data
                .rtp_timestamp_ext
                .wrapping_sub(ts_offset)
                .wrapping_add(
                    r.rtp_stats_base
                        .rtp_converter()
                        .to_rtp_ext(time_since_publisher_sr_adjusted),
                );
        }

        let packet_count = r
            .rtp_stats_base
            .get_packets_seen_plus_duplicates(r.ext_start_sn, r.ext_highest_sn)
            as u32;
        let octet_count = r
            .rtp_stats_base
            .rtp_stats_base_lite
            .bytes
            .wrapping_add(r.rtp_stats_base.bytes_duplicate)
            .wrapping_add(r.rtp_stats_base.bytes_padding);
        let sr_data = livekit::RtcpSenderReportState {
            ntp_timestamp: now_ntp.0,
            rtp_timestamp: now_rtp_ext as u32,
            rtp_timestamp_ext: now_rtp_ext,
            at: report_time,
            at_adjusted: report_time_adjusted,
            packets: packet_count,
            octets: octet_count,
        };

        let clock_rate = r.rtp_stats_base.rtp_stats_base_lite.clock_rate;
        if let Some(sr_newest) = r.rtp_stats_base.sr_newest
            && now_rtp_ext >= sr_newest.rtp_timestamp_ext
            && clock_rate != 0
        {
            let now_time = now_ntp.time();
            let sr_newest_time = NtpTime(sr_newest.ntp_timestamp).time();
            let time_since_last_report = now_time.sub(sr_newest_time);
            let rtp_diff_since_last_report = now_rtp_ext.wrapping_sub(sr_newest.rtp_timestamp_ext);
            let window_clock_rate =
                rtp_diff_since_last_report as f64 / time_since_last_report.seconds();
            if time_since_last_report.seconds() > 0.2
                && (clock_rate as f64 - window_clock_rate).abs() > 0.2 * clock_rate as f64
            {
                r.clock_skew_count += 1;
                if should_log(r.clock_skew_count) {
                    let srlf = SenderReportLoggingFields {
                        sr_data,
                        publisher_sr_data: *publisher_sr_data,
                        ts_offset,
                        report_time,
                        report_time_adjusted,
                        now_nano,
                        now_rtp_ext,
                    };
                    srlf.log(
                        Level::Info,
                        "sending sender report, clock skew",
                        r,
                        &[
                            ("timeSinceLastReport", &time_since_last_report),
                            ("rtpDiffSinceLastReport", &rtp_diff_since_last_report),
                            ("windowClockRate", &window_clock_rate),
                            ("count", &r.clock_skew_count),
                        ],
                    );
                }
            }
        }

        if let Some(sr_newest) = &r.rtp_stats_base.sr_newest
            && now_rtp_ext < sr_newest.rtp_timestamp_ext
        {
            // If report being generated is behind the last report, skip it.
            // Should not happen.
            let srlf = SenderReportLoggingFields {
                sr_data,
                publisher_sr_data: *publisher_sr_data,
                ts_offset,
                report_time,
                report_time_adjusted,
                now_nano,
                now_rtp_ext,
            };
            srlf.log(
                Level::Info,
                "sending sender report, out-of-order, skipping",
                r,
                &[],
            );
            return None;
        }

        r.rtp_stats_base.sr_newest = Some(sr_data);
        if r.rtp_stats_base.sr_first.is_none() {
            r.rtp_stats_base.sr_first = r.rtp_stats_base.sr_newest;
        }

        r.rtt_marker = RttMarker {
            ntp_time: now_ntp,
            sent_at: time::now(),
        };

        Some(SenderReport {
            ssrc,
            ntp_time: now_ntp.0,
            rtp_time: now_rtp_ext as u32,
            packet_count,
            octet_count: octet_count as u32,
            ..Default::default()
        })
    }

    pub fn delta_info(&self, snapshot_id: u32) -> Option<RtpDeltaInfo> {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        let (delta_info, logging_fields, err) =
            r.rtp_stats_base
                .delta_info(snapshot_id, r.ext_start_sn, r.ext_highest_sn);
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

    pub fn delta_info_sender(
        &self,
        sender_snapshot_id: u32,
    ) -> (Option<RtpDeltaInfo>, Option<RtpDeltaInfo>) {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        let clock_rate = r.rtp_stats_base.rtp_stats_base_lite.clock_rate;
        if clock_rate == 0 {
            return (None, None);
        }

        let mut delta_stats_sender_view = None;
        let (then_sender_view, now_sender_view, ok) =
            r.get_and_reset_sender_snapshot_window(sender_snapshot_id);
        if ok {
            let start_time = then_sender_view.start_time;
            let end_time = now_sender_view.start_time;

            let packets_expected = now_sender_view
                .ext_start_sn
                .wrapping_sub(then_sender_view.ext_start_sn)
                as u32;
            if packets_expected as u64 > C_NUM_SEQUENCE_NUMBERS {
                warnw!(
                    r.rtp_stats_base.rtp_stats_base_lite.logger,
                    "too many packets expected in delta (sender)", nil,
                    "senderSnapshotID" => sender_snapshot_id,
                    "senderSnapshotNow" => now_sender_view,
                    "senderSnapshotThen" => then_sender_view,
                    "packetsExpected" => packets_expected,
                    "duration" => Duration(end_time.wrapping_sub(start_time)),
                    "rtpStats" => r,
                );
            } else if packets_expected != 0 {
                let mut packets_lost_feed = now_sender_view
                    .packets_lost_feed
                    .wrapping_sub(then_sender_view.packets_lost_feed)
                    as u32;
                if (packets_lost_feed as i32) < 0 {
                    packets_lost_feed = 0;
                }
                if packets_lost_feed > packets_expected {
                    warnw!(
                        r.rtp_stats_base.rtp_stats_base_lite.logger,
                        "unexpected number of packets lost", nil,
                        "senderSnapshotID" => sender_snapshot_id,
                        "senderSnapshotNow" => now_sender_view,
                        "senderSnapshotThen" => then_sender_view,
                        "packetsExpected" => packets_expected,
                        "packetsLostFeed" => packets_lost_feed,
                        "duration" => Duration(end_time.wrapping_sub(start_time)),
                        "rtpStats" => r,
                    );
                    packets_lost_feed = packets_expected;
                }

                let max_jitter_time = then_sender_view.max_jitter_feed / clock_rate as f64 * 1e6;

                delta_stats_sender_view = Some(RtpDeltaInfo {
                    start_time: Time::unix(0, start_time),
                    end_time: Time::unix(0, end_time),
                    packets: packets_expected.wrapping_sub(
                        now_sender_view
                            .packets_padding
                            .wrapping_sub(then_sender_view.packets_padding)
                            as u32,
                    ),
                    bytes: now_sender_view.bytes.wrapping_sub(then_sender_view.bytes),
                    header_bytes: now_sender_view
                        .header_bytes
                        .wrapping_sub(then_sender_view.header_bytes),
                    packets_duplicate: now_sender_view
                        .packets_duplicate
                        .wrapping_sub(then_sender_view.packets_duplicate)
                        as u32,
                    bytes_duplicate: now_sender_view
                        .bytes_duplicate
                        .wrapping_sub(then_sender_view.bytes_duplicate),
                    header_bytes_duplicate: now_sender_view
                        .header_bytes_duplicate
                        .wrapping_sub(then_sender_view.header_bytes_duplicate),
                    packets_padding: now_sender_view
                        .packets_padding
                        .wrapping_sub(then_sender_view.packets_padding)
                        as u32,
                    bytes_padding: now_sender_view
                        .bytes_padding
                        .wrapping_sub(then_sender_view.bytes_padding),
                    header_bytes_padding: now_sender_view
                        .header_bytes_padding
                        .wrapping_sub(then_sender_view.header_bytes_padding),
                    packets_missing: packets_lost_feed,
                    packets_out_of_order: now_sender_view
                        .packets_out_of_order_feed
                        .wrapping_sub(then_sender_view.packets_out_of_order_feed)
                        as u32,
                    frames: now_sender_view.frames.wrapping_sub(then_sender_view.frames),
                    jitter_max: max_jitter_time,
                    nacks: now_sender_view.nacks.wrapping_sub(then_sender_view.nacks),
                    nack_repeated: now_sender_view
                        .nack_repeated
                        .wrapping_sub(then_sender_view.nack_repeated),
                    plis: now_sender_view.plis.wrapping_sub(then_sender_view.plis),
                    firs: now_sender_view.firs.wrapping_sub(then_sender_view.firs),
                    ..Default::default()
                });
            }
        }

        let mut delta_stats_receiver_view = None;
        if r.last_rr_time != 0 {
            let (then_receiver_view, now_receiver_view, ok) =
                r.get_and_reset_sender_snapshot_receiver_view(sender_snapshot_id);
            if ok {
                let then_window = &then_receiver_view.sender_snapshot_window;
                let now_window = &now_receiver_view.sender_snapshot_window;
                let start_time = then_window.start_time;
                let end_time = now_window.start_time;

                let packets_expected = now_window
                    .ext_start_sn
                    .wrapping_sub(then_window.ext_start_sn)
                    as u32;
                if packets_expected as u64 > C_NUM_SEQUENCE_NUMBERS {
                    warnw!(
                        r.rtp_stats_base.rtp_stats_base_lite.logger,
                        "too many packets expected in delta (sender - receiver view)", nil,
                        "senderSnapshotID" => sender_snapshot_id,
                        "senderSnapshotNow" => now_receiver_view,
                        "senderSnapshotThen" => then_receiver_view,
                        "packetsExpected" => packets_expected,
                        "duration" => Duration(end_time.wrapping_sub(start_time)),
                        "rtpStats" => r,
                    );
                } else if packets_expected != 0 {
                    // do not process if no RTCP RR (OR) publisher is not producing any data
                    let mut packets_lost = now_receiver_view
                        .packets_lost
                        .wrapping_sub(then_receiver_view.packets_lost)
                        as u32;
                    if (packets_lost as i32) < 0 {
                        packets_lost = 0;
                    }
                    let mut packets_lost_feed = now_window
                        .packets_lost_feed
                        .wrapping_sub(then_window.packets_lost_feed)
                        as u32;
                    if (packets_lost_feed as i32) < 0 {
                        packets_lost_feed = 0;
                    }
                    if packets_lost > packets_expected {
                        warnw!(
                            r.rtp_stats_base.rtp_stats_base_lite.logger,
                            "unexpected number of packets lost (sender - receiver view)", nil,
                            "senderSnapshotID" => sender_snapshot_id,
                            "senderSnapshotNow" => now_receiver_view,
                            "senderSnapshotThen" => then_receiver_view,
                            "packetsExpected" => packets_expected,
                            "packetsLost" => packets_lost,
                            "packetsLostFeed" => packets_lost_feed,
                            "duration" => Duration(end_time.wrapping_sub(start_time)),
                            "rtpStats" => r,
                        );
                        packets_lost = packets_expected;
                    }

                    let max_jitter_time = then_receiver_view.max_jitter / clock_rate as f64 * 1e6;

                    delta_stats_receiver_view = Some(RtpDeltaInfo {
                        start_time: Time::unix(0, start_time),
                        end_time: Time::unix(0, end_time),
                        packets: packets_expected.wrapping_sub(
                            now_window
                                .packets_padding
                                .wrapping_sub(then_window.packets_padding)
                                as u32,
                        ),
                        bytes: now_window.bytes.wrapping_sub(then_window.bytes),
                        header_bytes: now_window
                            .header_bytes
                            .wrapping_sub(then_window.header_bytes),
                        packets_duplicate: now_window
                            .packets_duplicate
                            .wrapping_sub(then_window.packets_duplicate)
                            as u32,
                        bytes_duplicate: now_window
                            .bytes_duplicate
                            .wrapping_sub(then_window.bytes_duplicate),
                        header_bytes_duplicate: now_window
                            .header_bytes_duplicate
                            .wrapping_sub(then_window.header_bytes_duplicate),
                        packets_padding: now_window
                            .packets_padding
                            .wrapping_sub(then_window.packets_padding)
                            as u32,
                        bytes_padding: now_window
                            .bytes_padding
                            .wrapping_sub(then_window.bytes_padding),
                        header_bytes_padding: now_window
                            .header_bytes_padding
                            .wrapping_sub(then_window.header_bytes_padding),
                        packets_lost,
                        packets_missing: packets_lost_feed,
                        packets_out_of_order: now_window
                            .packets_out_of_order_feed
                            .wrapping_sub(then_window.packets_out_of_order_feed)
                            as u32,
                        frames: now_window.frames.wrapping_sub(then_window.frames),
                        rtt_max: then_receiver_view.max_rtt,
                        jitter_max: max_jitter_time,
                        nacks: now_window.nacks.wrapping_sub(then_window.nacks),
                        nack_repeated: now_window
                            .nack_repeated
                            .wrapping_sub(then_window.nack_repeated),
                        plis: now_window.plis.wrapping_sub(then_window.plis),
                        firs: now_window.firs.wrapping_sub(then_window.firs),
                        ..Default::default()
                    });
                }
            }
        }

        (delta_stats_sender_view, delta_stats_receiver_view)
    }
}

impl fmt::Debug for RtpStatsSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let r = self.lock.read();

        fmt::Debug::fmt(&*r, f)
    }
}

impl RtpStatsSender {
    pub fn to_proto(&self) -> Option<livekit::RtpStats> {
        let r = self.lock.read();

        let mut p = r.rtp_stats_base.to_proto(
            get_packets_expected(r.ext_start_sn, r.ext_highest_sn),
            r.rtp_stats_base
                .get_packets_seen_minus_padding(r.ext_start_sn, r.ext_highest_sn),
            r.packets_lost_from_rr,
            r.ext_start_ts,
            r.ext_highest_ts,
            r.jitter_from_rr,
            r.max_jitter_from_rr,
        );

        if let Some(p) = &mut p {
            p.layer_lock_plis = r.layer_lock_plis;
            p.last_layer_lock_pli = Some(timestamppb_new(r.last_layer_lock_pli));
        }
        p
    }
}

impl RtpStatsSenderInner {
    fn get_and_reset_sender_snapshot_window(
        &mut self,
        sender_snapshot_id: u32,
    ) -> (SenderSnapshotWindow, SenderSnapshotWindow, bool) {
        if !self.rtp_stats_base.rtp_stats_base_lite.initialized
            || sender_snapshot_id < C_FIRST_SNAPSHOT_ID
        {
            return (
                SenderSnapshotWindow::default(),
                SenderSnapshotWindow::default(),
                false,
            );
        }

        let idx = (sender_snapshot_id - C_FIRST_SNAPSHOT_ID) as usize;
        let mut then = self.sender_snapshots[idx].sender_view;
        if !then.is_valid {
            then = init_sender_snapshot_window(
                self.rtp_stats_base.rtp_stats_base_lite.start_time,
                self.ext_start_sn,
            );
            self.sender_snapshots[idx].sender_view = then;
        }

        // snapshot now
        self.sender_snapshots[idx].sender_view = self.get_sender_snapshot_window(mono::unix_nano());
        (then, self.sender_snapshots[idx].sender_view, true)
    }

    fn get_sender_snapshot_window(&self, start_time: i64) -> SenderSnapshotWindow {
        SenderSnapshotWindow {
            is_valid: true,
            start_time,
            ext_start_sn: self.ext_highest_sn.wrapping_add(1),
            bytes: self.rtp_stats_base.rtp_stats_base_lite.bytes,
            header_bytes: self.rtp_stats_base.header_bytes,
            packets_padding: self.rtp_stats_base.packets_padding,
            bytes_padding: self.rtp_stats_base.bytes_padding,
            header_bytes_padding: self.rtp_stats_base.header_bytes_padding,
            packets_duplicate: self.rtp_stats_base.packets_duplicate,
            bytes_duplicate: self.rtp_stats_base.bytes_duplicate,
            header_bytes_duplicate: self.rtp_stats_base.header_bytes_duplicate,
            packets_out_of_order_feed: self.rtp_stats_base.rtp_stats_base_lite.packets_out_of_order,
            packets_lost_feed: self.rtp_stats_base.rtp_stats_base_lite.packets_lost,
            frames: self.rtp_stats_base.frames,
            nacks: self.rtp_stats_base.rtp_stats_base_lite.nacks,
            nack_repeated: self.rtp_stats_base.rtp_stats_base_lite.nack_repeated,
            plis: self.rtp_stats_base.rtp_stats_base_lite.plis,
            firs: self.rtp_stats_base.firs,
            max_jitter_feed: self.rtp_stats_base.jitter,
        }
    }

    fn get_and_reset_sender_snapshot_receiver_view(
        &mut self,
        sender_snapshot_id: u32,
    ) -> (SenderSnapshotReceiverView, SenderSnapshotReceiverView, bool) {
        if !self.rtp_stats_base.rtp_stats_base_lite.initialized
            || self.last_rr_time == 0
            || sender_snapshot_id < C_FIRST_SNAPSHOT_ID
        {
            return (
                SenderSnapshotReceiverView::default(),
                SenderSnapshotReceiverView::default(),
                false,
            );
        }

        let idx = (sender_snapshot_id - C_FIRST_SNAPSHOT_ID) as usize;
        let mut then = self.sender_snapshots[idx].receiver_view.clone();
        if !then.sender_snapshot_window.is_valid {
            then = init_sender_snapshot_receiver_view(
                self.rtp_stats_base.rtp_stats_base_lite.start_time,
                self.ext_start_sn,
            );
            self.sender_snapshots[idx].receiver_view = then.clone();
        }

        // snapshot now
        self.sender_snapshots[idx].receiver_view =
            self.get_sender_snapshot_receiver_view(self.last_rr_time, Some(&then));
        (then, self.sender_snapshots[idx].receiver_view.clone(), true)
    }

    fn get_sender_snapshot_receiver_view(
        &self,
        start_time: i64,
        s: Option<&SenderSnapshotReceiverView>,
    ) -> SenderSnapshotReceiverView {
        let Some(s) = s else {
            return SenderSnapshotReceiverView::default();
        };
        let w = &s.sender_snapshot_window;

        SenderSnapshotReceiverView {
            sender_snapshot_window: SenderSnapshotWindow {
                is_valid: true,
                start_time,
                ext_start_sn: s.ext_last_rr_sn.wrapping_add(1),
                bytes: w.bytes.wrapping_add(s.interval_stats.bytes),
                header_bytes: w.header_bytes.wrapping_add(s.interval_stats.header_bytes),
                packets_padding: w
                    .packets_padding
                    .wrapping_add(s.interval_stats.packets_padding),
                bytes_padding: w.bytes_padding.wrapping_add(s.interval_stats.bytes_padding),
                header_bytes_padding: w
                    .header_bytes_padding
                    .wrapping_add(s.interval_stats.header_bytes_padding),
                packets_duplicate: self.rtp_stats_base.packets_duplicate,
                bytes_duplicate: self.rtp_stats_base.bytes_duplicate,
                header_bytes_duplicate: self.rtp_stats_base.header_bytes_duplicate,
                packets_out_of_order_feed: w
                    .packets_out_of_order_feed
                    .wrapping_add(s.interval_stats.packets_out_of_order_feed),
                packets_lost_feed: w
                    .packets_lost_feed
                    .wrapping_add(s.interval_stats.packets_lost_feed),
                frames: w.frames.wrapping_add(s.interval_stats.frames),
                nacks: self.rtp_stats_base.rtp_stats_base_lite.nacks,
                nack_repeated: self.rtp_stats_base.rtp_stats_base_lite.nack_repeated,
                plis: self.rtp_stats_base.rtp_stats_base_lite.plis,
                firs: self.rtp_stats_base.firs,
                max_jitter_feed: self.rtp_stats_base.jitter,
            },
            packets_lost: self.packets_lost_from_rr,
            max_rtt: self.rtp_stats_base.rtt,
            max_jitter: self.jitter_from_rr,
            ext_last_rr_sn: s.ext_last_rr_sn,
            metadata_cache_overflow_count: s.metadata_cache_overflow_count,
            ..Default::default()
        }
    }

    fn get_sn_info_out_of_order_slot(&self, esn: u64, ehsn: u64) -> i64 {
        let offset = ehsn.wrapping_sub(esn) as i64;
        if offset >= self.sn_infos.len() as i64 || offset < 0 {
            // too old OR too new (i. e. ahead of highest)
            return -1;
        }

        esn as i64 % self.sn_infos.len() as i64
    }

    #[allow(clippy::too_many_arguments)]
    fn set_sn_info(
        &mut self,
        esn: u64,
        ehsn: u64,
        pkt_size: u16,
        hdr_size: u8,
        payload_size: u16,
        marker: bool,
        is_out_of_order: bool,
    ) {
        let slot: i64;
        if (esn.wrapping_sub(ehsn) as i64) < 0 {
            slot = self.get_sn_info_out_of_order_slot(esn, ehsn);
            if slot < 0 {
                return;
            }
        } else {
            slot = esn as i64 % self.sn_infos.len() as i64;
        }

        let sn_info = &mut self.sn_infos[slot as usize];
        sn_info.pkt_size = pkt_size;
        sn_info.hdr_size = hdr_size;
        sn_info.flags = 0;
        if marker {
            sn_info.flags |= SN_INFO_FLAG_MARKER;
        }
        if payload_size == 0 {
            sn_info.flags |= SN_INFO_FLAG_PADDING;
        }
        if is_out_of_order {
            sn_info.flags |= SN_INFO_FLAG_OUT_OF_ORDER;
        }
    }

    fn clear_sn_infos(&mut self, ext_start_inclusive: u64, ext_end_exclusive: u64) {
        if ext_end_exclusive <= ext_start_inclusive {
            return;
        }

        if ext_end_exclusive - ext_start_inclusive > self.sn_infos.len() as u64 {
            self.sn_infos.fill(SnInfo::default());
            return;
        }

        let mut esn = ext_start_inclusive;
        while esn != ext_end_exclusive {
            let slot = (esn as i64 % self.sn_infos.len() as i64) as usize;
            self.sn_infos[slot] = SnInfo::default();
            esn = esn.wrapping_add(1);
        }
    }

    fn is_sn_info_lost(&self, esn: u64, ehsn: u64) -> bool {
        let slot = self.get_sn_info_out_of_order_slot(esn, ehsn);
        if slot < 0 {
            return false;
        }

        self.sn_infos[slot as usize].pkt_size == 0
    }

    pub(super) fn get_interval_stats(
        &self,
        ext_start_inclusive: u64,
        ext_end_exclusive: u64,
        ehsn: u64,
    ) -> IntervalStats {
        let mut interval_stats = IntervalStats::default();
        let upper_bound = ehsn.wrapping_add(1);
        let mut lower_bound = 0u64;
        let n = self.sn_infos.len() as u64;
        if n != 0 && ehsn >= n - 1 {
            lower_bound = ehsn - n + 1;
        }
        let ext_start_inclusive_clamped = ext_start_inclusive.min(upper_bound).max(lower_bound);
        let ext_end_exclusive_clamped = ext_end_exclusive
            .min(upper_bound)
            .max(ext_start_inclusive_clamped);

        interval_stats.packets_not_found_metadata = ext_end_exclusive
            .wrapping_sub(ext_start_inclusive)
            .wrapping_sub(ext_end_exclusive_clamped.wrapping_sub(ext_start_inclusive_clamped));

        let mut esn = ext_start_inclusive_clamped;
        while esn != ext_end_exclusive_clamped {
            let sn_info = &self.sn_infos[(esn as i64 % self.sn_infos.len() as i64) as usize];
            if sn_info.pkt_size == 0 {
                interval_stats.packets_lost_feed += 1;
            } else if sn_info.flags & SN_INFO_FLAG_PADDING != 0 {
                interval_stats.packets_padding += 1;
                interval_stats.bytes_padding = interval_stats
                    .bytes_padding
                    .wrapping_add(sn_info.pkt_size as u64);
                interval_stats.header_bytes_padding = interval_stats
                    .header_bytes_padding
                    .wrapping_add(sn_info.hdr_size as u64);
            } else {
                interval_stats.packets += 1;
                interval_stats.bytes = interval_stats.bytes.wrapping_add(sn_info.pkt_size as u64);
                interval_stats.header_bytes = interval_stats
                    .header_bytes
                    .wrapping_add(sn_info.hdr_size as u64);
                if (sn_info.flags & SN_INFO_FLAG_OUT_OF_ORDER) != 0 {
                    interval_stats.packets_out_of_order_feed += 1;
                }
            }

            if (sn_info.flags & SN_INFO_FLAG_MARKER) != 0 {
                interval_stats.frames = interval_stats.frames.wrapping_add(1);
            }
            esn = esn.wrapping_add(1);
        }
        interval_stats
    }
}

impl RtpStatsSender {
    pub fn ext_highest_sequence_number(&self) -> u64 {
        let r = self.lock.read();

        r.ext_highest_sn
    }
}

// -------------------------------------------------------------------

// lockedRTPStatsSenderLogEncoder
impl fmt::Debug for RtpStatsSenderInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("RtpStatsSender");

        let packets_expected = get_packets_expected(self.ext_start_sn, self.ext_highest_sn);
        let elapsed_seconds = match self.rtp_stats_base.marshal_log_object(
            &mut e,
            packets_expected,
            self.rtp_stats_base
                .get_packets_seen_minus_padding(self.ext_start_sn, self.ext_highest_sn),
            self.ext_start_ts,
            self.ext_highest_ts,
        ) {
            Ok(elapsed_seconds) => elapsed_seconds,
            Err(err) => return e.field("error", &format_args!("{err}")).finish(),
        };

        e.field("extStartSN", &self.ext_start_sn);
        e.field("extHighestSN", &self.ext_highest_sn);

        e.field("extStartTS", &self.ext_start_ts);
        e.field("extHighestTS", &self.ext_highest_ts);

        e.field("lastRRTime", &Time::unix(0, self.last_rr_time));
        e.field("lastRR", &self.last_rr);
        e.field("extHighestSNFromRR", &self.ext_highest_sn_from_rr);
        e.field(
            "extHighestSNFromRRMisalignment",
            &self.ext_highest_sn_from_rr_misalignment,
        );
        e.field("packetsLostFromRR", &self.packets_lost_from_rr);
        e.field(
            "packetsLostFromRRRate",
            &(self.packets_lost_from_rr as f64 / elapsed_seconds),
        );
        if packets_expected != 0 {
            e.field(
                "packetLostFromRRPercentage",
                &(self.packets_lost_from_rr as f32 / packets_expected as f32 * 100.0),
            );
        }
        e.field("jitterFromRR", &self.jitter_from_rr);
        e.field("maxJitterFromRR", &self.max_jitter_from_rr);

        e.field("layerLockPlis", &self.layer_lock_plis);
        e.field("lastLayerLockPli", &self.last_layer_lock_pli);
        e.finish()
    }
}

// -------------------------------------------------------------------

fn init_sender_snapshot(start_time: i64, ext_start_sn: u64) -> SenderSnapshot {
    SenderSnapshot {
        sender_view: init_sender_snapshot_window(start_time, ext_start_sn),
        receiver_view: init_sender_snapshot_receiver_view(start_time, ext_start_sn),
    }
}

fn init_sender_snapshot_window(start_time: i64, ext_start_sn: u64) -> SenderSnapshotWindow {
    SenderSnapshotWindow {
        is_valid: true,
        start_time,
        ext_start_sn,
        ..Default::default()
    }
}

fn init_sender_snapshot_receiver_view(
    start_time: i64,
    ext_start_sn: u64,
) -> SenderSnapshotReceiverView {
    SenderSnapshotReceiverView {
        sender_snapshot_window: SenderSnapshotWindow {
            is_valid: true,
            start_time,
            ext_start_sn,
            ..Default::default()
        },
        ext_last_rr_sn: ext_start_sn.wrapping_sub(1),
        ..Default::default()
    }
}
