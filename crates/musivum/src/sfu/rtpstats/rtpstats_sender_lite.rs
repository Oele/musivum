// Ported from LiveKit, pkg/sfu/rtpstats/rtpstats_sender_lite.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the lock is the outer type's and everything else,
// with the `rtpStatsBaseLite` the original embeds, is in `RtpStatsSenderLiteInner` (see
// `rtpstats_base_lite.rs`); the methods inherited from `rtpStatsBaseLite` are forwarded;
// `lockedRTPStatsSenderLiteLogEncoder` is `Debug` of the inner struct and `MarshalLogObject`
// is `Debug` of the outer one, which takes the lock.

use std::fmt;

use parking_lot::RwLock;

use crate::debugw;
use crate::protocol::livekit;
use crate::protocol::utils::mono;

use super::rtpstats_base_lite::{
    RtpStatsBaseLite, RtpStatsParams, forward_rtp_stats_base_lite, get_packets_expected,
};

pub struct RtpStatsSenderLite {
    pub(super) lock: RwLock<RtpStatsSenderLiteInner>,
}

// What the lock guards: all of `RTPStatsSenderLite` upstream.
pub(super) struct RtpStatsSenderLiteInner {
    pub(super) rtp_stats_base_lite: RtpStatsBaseLite,

    pub(super) ext_start_sn: u64,
    pub(super) ext_highest_sn: u64,
}

forward_rtp_stats_base_lite!(RtpStatsSenderLite, rtp_stats_base_lite);

impl RtpStatsSenderLite {
    pub fn new(params: RtpStatsParams) -> Self {
        Self {
            lock: RwLock::new(RtpStatsSenderLiteInner {
                rtp_stats_base_lite: RtpStatsBaseLite::new(params),
                ext_start_sn: 0,
                ext_highest_sn: 0,
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

    pub fn update(&self, _packet_time: i64, packet_size: usize, ext_sequence_number: u64) {
        let mut lock = self.lock.write();
        let r = &mut *lock;

        if r.rtp_stats_base_lite.end_time != 0 {
            return;
        }

        if !r.rtp_stats_base_lite.initialized {
            r.rtp_stats_base_lite.initialized = true;

            r.rtp_stats_base_lite.start_time = mono::unix_nano();

            r.ext_start_sn = ext_sequence_number;
            r.ext_highest_sn = ext_sequence_number.wrapping_sub(1);

            debugw!(
                r.rtp_stats_base_lite.logger,
                "rtp sender lite stream start",
                "rtpStats" => r,
            );
        }

        let gap_sn = ext_sequence_number.wrapping_sub(r.ext_highest_sn) as i64;
        if gap_sn <= 0 {
            // duplicate OR out-of-order
            r.rtp_stats_base_lite.packets_out_of_order += 1; // counting duplicate as out-of-order
            r.rtp_stats_base_lite.packets_lost = r.rtp_stats_base_lite.packets_lost.wrapping_sub(1);
        } else {
            // in-order
            r.rtp_stats_base_lite.update_gap_histogram(gap_sn);
            r.rtp_stats_base_lite.packets_lost = r
                .rtp_stats_base_lite
                .packets_lost
                .wrapping_add((gap_sn - 1) as u64);

            r.ext_highest_sn = ext_sequence_number;
        }

        r.rtp_stats_base_lite.bytes = r.rtp_stats_base_lite.bytes.wrapping_add(packet_size as u64);
    }

    pub fn to_proto(&self) -> Option<livekit::RtpStats> {
        let r = self.lock.read();

        r.rtp_stats_base_lite.to_proto(
            r.ext_start_sn,
            r.ext_highest_sn,
            r.rtp_stats_base_lite.packets_lost,
        )
    }

    pub fn ext_highest_sequence_number(&self) -> u64 {
        let r = self.lock.read();

        r.ext_highest_sn
    }
}

impl fmt::Debug for RtpStatsSenderLite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let r = self.lock.read();

        fmt::Debug::fmt(&*r, f)
    }
}

// -------------------------------------------------------------------

// lockedRTPStatsSenderLiteLogEncoder
impl fmt::Debug for RtpStatsSenderLiteInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("RtpStatsSenderLite");

        if let Err(err) = self.rtp_stats_base_lite.marshal_log_object(
            &mut e,
            get_packets_expected(self.ext_start_sn, self.ext_highest_sn),
            get_packets_expected(self.ext_start_sn, self.ext_highest_sn),
        ) {
            return e.field("error", &format_args!("{err}")).finish();
        }

        e.field("extStartSN", &self.ext_start_sn);
        e.field("extHighestSN", &self.ext_highest_sn);
        e.finish()
    }
}

// -------------------------------------------------------------------
