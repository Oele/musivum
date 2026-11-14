// Ported from LiveKit, pkg/sfu/rtpstats/rtpstats_receiver_lite.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the lock is the outer type's and everything else,
// with the `rtpStatsBaseLite` the original embeds, is in `RtpStatsReceiverLiteInner` (see
// `rtpstats_base_lite.rs`); the methods inherited from `rtpStatsBaseLite` are forwarded;
// `lockedRTPStatsReceiverLiteLogEncoder` is `Debug` of the inner struct and
// `MarshalLogObject` is `Debug` of the outer one, which takes the lock; an error of the
// marshaller, which zap writes next to the object, is the field `error`.

use std::fmt;

use parking_lot::RwLock;

use crate::debugw;
use crate::mediatransportutil::utils::{WrapAround, WrapAroundParams, WrapAroundUpdateResult};
use crate::protocol::livekit;
use crate::protocol::utils::mono;

use super::rtpstats_base_lite::{
    C_FIRST_SNAPSHOT_ID, RtpDeltaInfoLite, RtpStatsBaseLite, RtpStatsParams,
    forward_rtp_stats_base_lite, get_packets_expected, infow_logging_fields, init_snapshot_lite,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RtpFlowStateLite {
    pub is_not_handled: bool,

    pub loss_start_inclusive: u64,
    pub loss_end_exclusive: u64,

    pub ext_sequence_number: u64,
}

// ---------------------------------------------------------------------

pub struct RtpStatsReceiverLite {
    pub(super) lock: RwLock<RtpStatsReceiverLiteInner>,
}

// What the lock guards: all of `RTPStatsReceiverLite` upstream.
pub(super) struct RtpStatsReceiverLiteInner {
    pub(super) rtp_stats_base_lite: RtpStatsBaseLite,

    pub(super) sequence_number: WrapAround<u16, u64>,
}

forward_rtp_stats_base_lite!(RtpStatsReceiverLite, rtp_stats_base_lite);

impl RtpStatsReceiverLite {
    pub fn new(params: RtpStatsParams) -> Self {
        Self {
            lock: RwLock::new(RtpStatsReceiverLiteInner {
                rtp_stats_base_lite: RtpStatsBaseLite::new(params),
                sequence_number: WrapAround::new(WrapAroundParams {
                    is_restart_allowed: false,
                }),
            }),
        }
    }

    // rtpStatsBaseLite.SetClockRate, inherited upstream
    pub fn set_clock_rate(&self, clock_rate: u32) {
        self.lock
            .write()
            .rtp_stats_base_lite
            .set_clock_rate(clock_rate)
    }

    pub fn new_snapshot_lite_id(&self) -> u32 {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        r.rtp_stats_base_lite
            .new_snapshot_lite_id(r.sequence_number.get_extended_highest())
    }

    pub fn update(
        &self,
        _packet_time: i64,
        packet_size: usize,
        sequence_number: u16,
    ) -> RtpFlowStateLite {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        let mut flow_state_lite = RtpFlowStateLite::default();
        if r.rtp_stats_base_lite.end_time != 0 {
            flow_state_lite.is_not_handled = true;
            return flow_state_lite;
        }

        let res_sn: WrapAroundUpdateResult<u64>;
        if !r.rtp_stats_base_lite.initialized {
            r.rtp_stats_base_lite.initialized = true;

            r.rtp_stats_base_lite.start_time = mono::unix_nano();

            res_sn = r.sequence_number.update(sequence_number);

            // initialize lite snapshots if any
            for i in 0..r
                .rtp_stats_base_lite
                .next_snapshot_lite_id
                .wrapping_sub(C_FIRST_SNAPSHOT_ID)
            {
                r.rtp_stats_base_lite.snapshot_lites[i as usize] = init_snapshot_lite(
                    r.rtp_stats_base_lite.start_time,
                    r.sequence_number.get_extended_start(),
                );
            }

            debugw!(
                r.rtp_stats_base_lite.logger,
                "rtp receiver lite stream start",
                "rtpStats" => r,
            );
        } else {
            res_sn = r.sequence_number.update(sequence_number);
            if res_sn.is_unhandled {
                flow_state_lite.is_not_handled = true;
                return flow_state_lite;
            }
        }

        let gap_sn = res_sn
            .extended_val
            .wrapping_sub(res_sn.pre_extended_highest) as i64;
        if gap_sn <= 0 {
            // duplicate OR out-of-order
            r.rtp_stats_base_lite.packets_out_of_order += 1; // counting duplicate as out-of-order
            if gap_sn != 0 && r.rtp_stats_base_lite.packets_lost > 0 {
                r.rtp_stats_base_lite.packets_lost -= 1;
            }
        } else {
            // in-order
            r.rtp_stats_base_lite.update_gap_histogram(gap_sn);
            r.rtp_stats_base_lite.packets_lost = r
                .rtp_stats_base_lite
                .packets_lost
                .wrapping_add((gap_sn - 1) as u64);

            flow_state_lite.loss_start_inclusive = res_sn.pre_extended_highest.wrapping_add(1);
            flow_state_lite.loss_end_exclusive = res_sn.extended_val;
        }
        flow_state_lite.ext_sequence_number = res_sn.extended_val;
        r.rtp_stats_base_lite.bytes = r.rtp_stats_base_lite.bytes.wrapping_add(packet_size as u64);
        flow_state_lite
    }

    pub fn delta_info_lite(&self, snapshot_lite_id: u32) -> Option<RtpDeltaInfoLite> {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        let (delta_info_lite, logging_fields, err) = r.rtp_stats_base_lite.delta_info_lite(
            snapshot_lite_id,
            r.sequence_number.get_extended_start(),
            r.sequence_number.get_extended_highest(),
        );
        if let Some(err) = err {
            infow_logging_fields(&r.rtp_stats_base_lite.logger, err, &logging_fields, r);
        }

        delta_info_lite
    }

    pub fn to_proto(&self) -> Option<livekit::RtpStats> {
        let r = self.lock.read();

        r.rtp_stats_base_lite.to_proto(
            r.sequence_number.get_extended_start(),
            r.sequence_number.get_extended_highest(),
            r.rtp_stats_base_lite.packets_lost,
        )
    }
}

impl fmt::Debug for RtpStatsReceiverLite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let r = self.lock.read();

        fmt::Debug::fmt(&*r, f)
    }
}

// ----------------------------------

// lockedRTPStatsReceiverLiteLogEncoder
impl fmt::Debug for RtpStatsReceiverLiteInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("RtpStatsReceiverLite");

        let (ext_start_sn, ext_highest_sn) = (
            self.sequence_number.get_extended_start(),
            self.sequence_number.get_extended_highest(),
        );
        if let Err(err) = self.rtp_stats_base_lite.marshal_log_object(
            &mut e,
            get_packets_expected(ext_start_sn, ext_highest_sn),
            get_packets_expected(ext_start_sn, ext_highest_sn),
        ) {
            return e.field("error", &format_args!("{err}")).finish();
        }

        e.field("extStartSN", &self.sequence_number.get_extended_start());
        e.field("extHighestSN", &self.sequence_number.get_extended_highest());
        e.finish()
    }
}

// ----------------------------------
