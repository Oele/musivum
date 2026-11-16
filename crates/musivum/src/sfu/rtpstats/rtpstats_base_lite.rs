// Ported from LiveKit, pkg/sfu/rtpstats/rtpstats_base_lite.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the lock of `rtpStatsBaseLite`, which the types that
// embed it take upstream, is the lock of the outer type here (one per statistics object):
// `RtpStatsBaseLite` is the state inside that lock, all its methods are called with the lock
// held, and those that take the lock upstream (`SetLogger`, `Stop`, `IsActive`, ...) are
// forwarded by each outer type through `forward_rtp_stats_base_lite!` at the end of this
// file; `loggingFields []any` is a list of keys and boxed values; the errors are the
// package's `Error`; `marshalLogObject` writes into a `fmt::DebugStruct`, and
// `MarshalLogObject` is `Debug`, with the keys of the original where it is written by hand.

use std::fmt;

use crate::go::time::{Duration, Time, timestamppb_new};
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;

use super::Error;

pub(super) const C_GAP_HISTOGRAM_NUM_BINS: usize = 101;
pub(super) const C_NUM_SEQUENCE_NUMBERS: u64 = 65536;
pub(super) const C_FIRST_SNAPSHOT_ID: u32 = 1;

/// `loggingFields []any` of the original: the keys and values of a line that the caller
/// writes, with what it adds.
pub(super) type LoggingFields = Vec<(&'static str, Box<dyn fmt::Debug>)>;

/// `r.logger.Infow(err.Error(), append(loggingFields, "rtpStats", rtpStats)...)`
pub(super) fn infow_logging_fields(
    logger: &Logger,
    err: Error,
    logging_fields: &LoggingFields,
    rtp_stats: &dyn fmt::Debug,
) {
    let mut kvs = logging_fields_as_kvs(logging_fields);
    kvs.push(("rtpStats", rtp_stats));
    logger.infow(&err.to_string(), &kvs);
}

pub(super) fn logging_fields_as_kvs(
    logging_fields: &LoggingFields,
) -> Vec<(&'static str, &dyn fmt::Debug)> {
    logging_fields
        .iter()
        .map(|(k, v)| (*k, v.as_ref() as &dyn fmt::Debug))
        .collect()
}

// -------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RtpDeltaInfoLite {
    pub start_time: Time,
    pub end_time: Time,
    pub packets: u32,
    pub bytes: u64,
    pub packets_lost: u32,
    pub packets_out_of_order: u32,
    pub nacks: u32,
}

// -------------------------------------------------------

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct SnapshotLite {
    pub(super) is_valid: bool,

    pub(super) start_time: i64,

    pub(super) ext_start_sn: u64,
    pub(super) bytes: u64,

    pub(super) packets_out_of_order: u64,

    pub(super) packets_lost: u64,

    pub(super) nacks: u32,
}

impl fmt::Debug for SnapshotLite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotLite")
            .field("isValid", &self.is_valid)
            .field("startTime", &Time::unix(0, self.start_time))
            .field("extStartSN", &self.ext_start_sn)
            .field("bytes", &self.bytes)
            .field("packetsOutOfOrder", &self.packets_out_of_order)
            .field("packetsLost", &self.packets_lost)
            .field("nacks", &self.nacks)
            .finish()
    }
}

// ------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RtpStatsParams {
    pub is_rtx: bool,
}

pub(super) struct RtpStatsBaseLite {
    pub(super) params: RtpStatsParams,
    pub(super) clock_rate: u32,
    pub(super) logger: Logger,

    // `lock sync.RWMutex` of the original is the lock of the outer type, see the header
    pub(super) initialized: bool,

    pub(super) start_time: i64,
    pub(super) end_time: i64,

    pub(super) bytes: u64,

    pub(super) packets_out_of_order: u64,

    pub(super) packets_lost: u64,

    pub(super) gap_histogram: [u32; C_GAP_HISTOGRAM_NUM_BINS],

    pub(super) nacks: u32,
    pub(super) nack_acks: u32,
    pub(super) nack_misses: u32,
    pub(super) nack_repeated: u32,

    pub(super) plis: u32,
    pub(super) last_pli: i64,

    pub(super) next_snapshot_lite_id: u32,
    pub(super) snapshot_lites: Vec<SnapshotLite>,
}

impl RtpStatsBaseLite {
    pub(super) fn new(params: RtpStatsParams) -> Self {
        Self {
            params,
            clock_rate: 0,
            logger: crate::protocol::logger::get_logger(),
            initialized: false,
            start_time: 0,
            end_time: 0,
            bytes: 0,
            packets_out_of_order: 0,
            packets_lost: 0,
            gap_histogram: [0; C_GAP_HISTOGRAM_NUM_BINS],
            nacks: 0,
            nack_acks: 0,
            nack_misses: 0,
            nack_repeated: 0,
            plis: 0,
            last_pli: 0,
            next_snapshot_lite_id: C_FIRST_SNAPSHOT_ID,
            snapshot_lites: vec![SnapshotLite::default(); 2],
        }
    }

    pub(super) fn seed(&mut self, from: Option<&RtpStatsBaseLite>) -> bool {
        let Some(from) = from else {
            return false;
        };
        if !from.initialized || self.initialized {
            return false;
        }

        self.initialized = from.initialized;

        self.start_time = from.start_time;
        // do not clone endTime as a non-zero endTime indicates an ended object

        self.bytes = from.bytes;

        self.packets_out_of_order = from.packets_out_of_order;

        self.packets_lost = from.packets_lost;

        self.gap_histogram = from.gap_histogram;

        self.nacks = from.nacks;
        self.nack_acks = from.nack_acks;
        self.nack_misses = from.nack_misses;
        self.nack_repeated = from.nack_repeated;

        self.plis = from.plis;
        self.last_pli = from.last_pli;

        self.next_snapshot_lite_id = from.next_snapshot_lite_id;
        self.snapshot_lites = from.snapshot_lites.clone();
        true
    }

    pub(super) fn set_clock_rate(&mut self, clock_rate: u32) {
        self.set_clock_rate_locked(clock_rate);
    }

    pub(super) fn set_clock_rate_locked(&mut self, clock_rate: u32) {
        self.clock_rate = clock_rate;
    }

    pub(super) fn set_logger(&mut self, logger: Logger) {
        self.logger = logger;
    }

    pub(super) fn stop(&mut self) {
        self.end_time = mono::unix_nano();
    }

    pub(super) fn new_snapshot_lite_id(&mut self, ext_start_sn: u64) -> u32 {
        let id = self.next_snapshot_lite_id;
        self.next_snapshot_lite_id = self.next_snapshot_lite_id.wrapping_add(1);

        let num = self.next_snapshot_lite_id.wrapping_sub(C_FIRST_SNAPSHOT_ID) as usize;
        if self.snapshot_lites.len() < num {
            self.snapshot_lites.resize(num, SnapshotLite::default());
        }

        if self.initialized {
            self.snapshot_lites[id.wrapping_sub(C_FIRST_SNAPSHOT_ID) as usize] =
                init_snapshot_lite(mono::unix_nano(), ext_start_sn);
        }
        id
    }

    pub(super) fn is_active(&self) -> bool {
        self.initialized && self.end_time == 0
    }

    pub(super) fn update_nack(&mut self, nack_count: u32) {
        if self.end_time != 0 {
            return;
        }

        self.nacks = self.nacks.wrapping_add(nack_count);
    }

    pub(super) fn update_nack_processed(
        &mut self,
        nack_ack_count: u32,
        nack_miss_count: u32,
        nack_repeated_count: u32,
    ) {
        if self.end_time != 0 {
            return;
        }

        self.nack_acks = self.nack_acks.wrapping_add(nack_ack_count);
        self.nack_misses = self.nack_misses.wrapping_add(nack_miss_count);
        self.nack_repeated = self.nack_repeated.wrapping_add(nack_repeated_count);
    }

    pub(super) fn check_and_update_pli(&mut self, throttle: i64, force: bool) -> bool {
        if self.end_time != 0
            || (!force && mono::unix_nano().wrapping_sub(self.last_pli) < throttle)
        {
            return false;
        }
        self.update_pli_locked(1);
        self.update_pli_time_locked();
        true
    }

    pub(super) fn update_pli_and_time(&mut self, pli_count: u32) {
        if self.end_time != 0 {
            return;
        }

        self.update_pli_locked(pli_count);
        self.update_pli_time_locked();
    }

    pub(super) fn update_pli(&mut self, pli_count: u32) {
        if self.end_time != 0 {
            return;
        }

        self.update_pli_locked(pli_count);
    }

    pub(super) fn update_pli_locked(&mut self, pli_count: u32) {
        self.plis = self.plis.wrapping_add(pli_count);
    }

    pub(super) fn update_pli_time(&mut self) {
        if self.end_time != 0 {
            return;
        }

        self.update_pli_time_locked();
    }

    pub(super) fn update_pli_time_locked(&mut self) {
        self.last_pli = mono::unix_nano();
    }

    pub(super) fn last_pli(&self) -> i64 {
        self.last_pli
    }

    pub(super) fn get_packets_seen(&self, ext_start_sn: u64, ext_highest_sn: u64) -> u64 {
        let packets_expected = get_packets_expected(ext_start_sn, ext_highest_sn);
        if self.packets_lost > packets_expected {
            // should not happen
            return 0;
        }

        packets_expected - self.packets_lost
    }

    pub(super) fn delta_info_lite(
        &mut self,
        snapshot_lite_id: u32,
        ext_start_sn: u64,
        ext_highest_sn: u64,
    ) -> (Option<RtpDeltaInfoLite>, LoggingFields, Option<Error>) {
        let (then, now, ok) =
            self.get_and_reset_snapshot_lite(snapshot_lite_id, ext_start_sn, ext_highest_sn);
        if !ok {
            return (None, Vec::new(), None);
        }

        let start_time = then.start_time;
        let end_time = now.start_time;

        let mut packets_expected = now.ext_start_sn.wrapping_sub(then.ext_start_sn) as u32;
        if then.ext_start_sn > ext_highest_sn {
            packets_expected = 0;
        }
        if packets_expected as u64 > C_NUM_SEQUENCE_NUMBERS {
            let logging_fields: LoggingFields = vec![
                ("snapshotLiteID", Box::new(snapshot_lite_id)),
                ("snapshotLiteNow", Box::new(now)),
                ("snapshotLiteThen", Box::new(then)),
                ("packetsExpected", Box::new(packets_expected)),
                (
                    "duration",
                    Box::new(Duration(end_time.wrapping_sub(start_time))),
                ),
            ];
            return (
                None,
                logging_fields,
                Some(Error::TooManyPacketsExpectedInDeltaLite),
            );
        }
        if packets_expected == 0 {
            return (
                Some(RtpDeltaInfoLite {
                    start_time: Time::unix(0, start_time),
                    end_time: Time::unix(0, end_time),
                    ..Default::default()
                }),
                Vec::new(),
                None,
            );
        }

        let mut packets_lost = now.packets_lost.wrapping_sub(then.packets_lost) as u32;
        if (packets_lost as i32) < 0 {
            packets_lost = 0;
        }
        let mut logging_fields: LoggingFields = Vec::new();
        let mut err = None;
        if packets_lost > packets_expected {
            logging_fields = vec![
                ("snapshotLiteID", Box::new(snapshot_lite_id)),
                ("snapshotLiteNow", Box::new(now)),
                ("snapshotLiteThen", Box::new(then)),
                ("packetsExpected", Box::new(packets_expected)),
                ("packetsLost", Box::new(packets_lost)),
                (
                    "duration",
                    Box::new(Duration(end_time.wrapping_sub(start_time))),
                ),
            ];
            err = Some(Error::UnexpectedNumberOfPacketsLostInDeltaLite);
        }

        let delta_info_lite = RtpDeltaInfoLite {
            start_time: Time::unix(0, start_time),
            end_time: Time::unix(0, end_time),
            packets: packets_expected,
            bytes: now.bytes.wrapping_sub(then.bytes),
            packets_lost,
            packets_out_of_order: now
                .packets_out_of_order
                .wrapping_sub(then.packets_out_of_order) as u32,
            nacks: now.nacks.wrapping_sub(then.nacks),
        };
        (Some(delta_info_lite), logging_fields, err)
    }

    pub(super) fn marshal_log_object(
        &self,
        e: &mut fmt::DebugStruct<'_, '_>,
        packets_expected: u64,
        packets_seen_minus_padding: u64,
    ) -> Result<f64, Error> {
        if !self.initialized || self.clock_rate == 0 {
            return Err(Error::NotInitialized);
        }

        let mut end_time = self.end_time;
        if end_time == 0 {
            end_time = mono::unix_nano();
        }
        let elapsed = Duration(end_time.wrapping_sub(self.start_time));
        if elapsed.is_zero() {
            return Err(Error::NoTimeElapsed);
        }
        let elapsed_seconds = elapsed.seconds();

        e.field("startTime", &Time::unix(0, self.start_time));
        e.field("endTime", &Time::unix(0, self.end_time));
        e.field("elapsed", &elapsed);

        e.field("packetsExpected", &packets_expected);
        e.field(
            "packetsExpectedRate",
            &(packets_expected as f64 / elapsed_seconds),
        );
        e.field("packetsSeenPrimary", &packets_seen_minus_padding);
        e.field(
            "packetsSeenPrimaryRate",
            &(packets_seen_minus_padding as f64 / elapsed_seconds),
        );
        e.field("bytes", &self.bytes);
        e.field("bitrate", &(self.bytes as f64 * 8.0 / elapsed_seconds));

        e.field("packetsOutOfOrder", &self.packets_out_of_order);

        e.field("packetsLost", &self.packets_lost);
        e.field(
            "packetsLostRate",
            &(self.packets_lost as f64 / elapsed_seconds),
        );
        if packets_expected != 0 {
            e.field(
                "packetLostPercentage",
                &(self.packets_lost as f32 / packets_expected as f32 * 100.0),
            );
        }

        let mut sb = String::new();
        for (burst, count) in self.gap_histogram.iter().enumerate() {
            if *count == 0 {
                continue;
            }
            if !sb.is_empty() {
                sb.push_str(", ");
            }
            sb.push_str(&format!("{}:{}", burst + 1, count));
        }
        if !sb.is_empty() {
            e.field("gapHistogram", &format_args!("[{sb}]"));
        }

        e.field("nacks", &self.nacks);
        e.field("nackAcks", &self.nack_acks);
        e.field("nackMisses", &self.nack_misses);
        e.field("nackRepeated", &self.nack_repeated);

        e.field("plis", &self.plis);
        e.field("lastPli", &Time::unix(0, self.last_pli));
        Ok(elapsed_seconds)
    }

    #[allow(clippy::needless_range_loop)] // the loops as the original has them
    pub(super) fn to_proto(
        &self,
        packets_expected: u64,
        packets_seen_minus_padding: u64,
        packets_lost: u64,
    ) -> Option<livekit::RtpStats> {
        if self.start_time == 0 || self.clock_rate == 0 {
            return None;
        }

        let mut end_time = self.end_time;
        if end_time == 0 {
            end_time = mono::unix_nano();
        }
        let elapsed = Duration(end_time.wrapping_sub(self.start_time)).seconds();
        if elapsed == 0.0 {
            return None;
        }

        let packet_rate = packets_seen_minus_padding as f64 / elapsed;
        let bitrate = self.bytes as f64 * 8.0 / elapsed;

        let packet_lost_rate = packets_lost as f64 / elapsed;
        let mut packet_lost_percentage = 0f32;
        if packets_expected != 0 {
            packet_lost_percentage = packets_lost as f32 / packets_expected as f32 * 100.0;
        }

        let mut p = livekit::RtpStats {
            start_time: Some(timestamppb_new(Time::unix(0, self.start_time))),
            end_time: Some(timestamppb_new(Time::unix(0, end_time))),
            duration: elapsed,
            packets: packets_seen_minus_padding as u32,
            packet_rate,
            bytes: self.bytes,
            bitrate,
            packets_lost: packets_lost as u32,
            packet_loss_rate: packet_lost_rate,
            packet_loss_percentage: packet_lost_percentage,
            packets_out_of_order: self.packets_out_of_order as u32,
            nacks: self.nacks,
            nack_acks: self.nack_acks,
            nack_misses: self.nack_misses,
            nack_repeated: self.nack_repeated,
            plis: self.plis,
            last_pli: Some(timestamppb_new(Time::unix(0, self.last_pli))),
            ..Default::default()
        };

        for i in 0..self.gap_histogram.len() {
            if self.gap_histogram[i] == 0 {
                continue;
            }
            p.gap_histogram
                .insert((i + 1) as i32, self.gap_histogram[i]);
        }

        Some(p)
    }

    pub(super) fn get_and_reset_snapshot_lite(
        &mut self,
        snapshot_lite_id: u32,
        ext_start_sn: u64,
        ext_highest_sn: u64,
    ) -> (SnapshotLite, SnapshotLite, bool) {
        if !self.initialized {
            return (SnapshotLite::default(), SnapshotLite::default(), false);
        }

        let idx = snapshot_lite_id.wrapping_sub(C_FIRST_SNAPSHOT_ID) as usize;
        let mut then = self.snapshot_lites[idx];
        if !then.is_valid {
            then = init_snapshot_lite(self.start_time, ext_start_sn);
            self.snapshot_lites[idx] = then;
        }

        // snapshot now
        let now = self.get_snapshot_lite(mono::unix_nano(), ext_highest_sn.wrapping_add(1));
        self.snapshot_lites[idx] = now;
        (then, now, true)
    }

    pub(super) fn update_gap_histogram(&mut self, gap: i64) {
        if gap < 2 {
            return;
        }

        let missing = gap - 1;
        if missing > self.gap_histogram.len() as i64 {
            let last = self.gap_histogram.len() - 1;
            self.gap_histogram[last] = self.gap_histogram[last].wrapping_add(1);
        } else {
            let bin = (missing - 1) as usize;
            self.gap_histogram[bin] = self.gap_histogram[bin].wrapping_add(1);
        }
    }

    pub(super) fn get_snapshot_lite(&self, start_time: i64, ext_start_sn: u64) -> SnapshotLite {
        SnapshotLite {
            is_valid: true,
            start_time,
            ext_start_sn,
            bytes: self.bytes,
            packets_out_of_order: self.packets_out_of_order,
            packets_lost: self.packets_lost,
            nacks: self.nacks,
        }
    }
}

// ----------------------------------

pub(super) fn init_snapshot_lite(start_time: i64, ext_start_sn: u64) -> SnapshotLite {
    SnapshotLite {
        is_valid: true,
        start_time,
        ext_start_sn,
        ..Default::default()
    }
}

pub(super) fn get_packets_expected(ext_start_sn: u64, ext_highest_sn: u64) -> u64 {
    ext_highest_sn.wrapping_sub(ext_start_sn).wrapping_add(1)
}

pub(super) fn should_log(count: i64) -> bool {
    count < 20 || count % 200 == 0
}

// ----------------------------------

/// The methods of `rtpStatsBaseLite` that take its lock, for an outer type that embeds it:
/// each takes the outer type's lock, as the original takes the one it shares, and calls the
/// method of that name of the state. `SetClockRate` is not among them: every outer type has
/// its own, as upstream where the types that embed `rtpStatsBase` do not get this one.
macro_rules! forward_rtp_stats_base_lite {
    ($outer:ident, $($rtp_stats_base_lite:ident).+) => {
        impl $outer {
            pub fn set_logger(&self, logger: $crate::protocol::logger::Logger) {
                self.lock.write().$($rtp_stats_base_lite).+.set_logger(logger)
            }

            pub fn stop(&self) {
                self.lock.write().$($rtp_stats_base_lite).+.stop()
            }

            pub fn is_active(&self) -> bool {
                self.lock.read().$($rtp_stats_base_lite).+.is_active()
            }

            pub fn update_nack(&self, nack_count: u32) {
                self.lock.write().$($rtp_stats_base_lite).+.update_nack(nack_count)
            }

            pub fn update_nack_processed(
                &self,
                nack_ack_count: u32,
                nack_miss_count: u32,
                nack_repeated_count: u32,
            ) {
                self.lock.write().$($rtp_stats_base_lite).+.update_nack_processed(
                    nack_ack_count,
                    nack_miss_count,
                    nack_repeated_count,
                )
            }

            pub fn check_and_update_pli(&self, throttle: i64, force: bool) -> bool {
                self.lock
                    .write()
                    .$($rtp_stats_base_lite).+
                    .check_and_update_pli(throttle, force)
            }

            pub fn update_pli_and_time(&self, pli_count: u32) {
                self.lock.write().$($rtp_stats_base_lite).+.update_pli_and_time(pli_count)
            }

            pub fn update_pli(&self, pli_count: u32) {
                self.lock.write().$($rtp_stats_base_lite).+.update_pli(pli_count)
            }

            pub fn update_pli_time(&self) {
                self.lock.write().$($rtp_stats_base_lite).+.update_pli_time()
            }

            pub fn last_pli(&self) -> i64 {
                self.lock.read().$($rtp_stats_base_lite).+.last_pli()
            }
        }
    };
}
pub(super) use forward_rtp_stats_base_lite;
