// Ported from LiveKit, pkg/sfu/rtpmunger.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `SequenceNumberOrdering` is an enum;
// `UpdateAndGetSnTs` returns its parameters also when it returns an error (the ordering is
// set then), so it gives the pair of the parameters and a `Result`; `DebugInfo`'s map from
// names to values of any type is the struct `RtpMungerDebugInfo`; `GetState` returns the
// message by value; the number of padding packets is unsigned; `FilterRTX` takes a slice and
// returns a vector also when it filters nothing.

use super::Error;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::utils;
use crate::{errorw, warnw};

// RTPMunger
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SequenceNumberOrdering {
    #[default]
    Contiguous,
    OutOfOrder,
    Gap,
    Duplicate,
}

pub const RTX_GATE_WINDOW: u64 = 2000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TranslationParamsRtp {
    pub(super) sn_ordering: SequenceNumberOrdering,
    pub(super) ext_sequence_number: u64,
    pub(super) ext_timestamp: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnTs {
    pub(super) ext_sequence_number: u64,
    pub(super) ext_timestamp: u64,
}

// ----------------------------------------------------------------------

pub struct RtpMunger {
    logger: Logger,

    ext_highest_incoming_sn: u64,
    // seen by the package, as in the original: the forwarder's test reaches into it
    pub(super) sn_range_map: utils::RangeMap<u64, u64>,

    ext_last_sn: u64,
    ext_second_last_sn: u64,
    sn_offset: u64,

    ext_last_ts: u64,
    ext_second_last_ts: u64,
    ts_offset: u64,

    last_marker: bool,
    second_last_marker: bool,

    ext_rtx_gate_sn: u64,
    is_in_rtx_gate_region: bool,
}

/// What `DebugInfo` returns: the original's `map[string]any`, its keys as fields.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RtpMungerDebugInfo {
    pub ext_highest_incoming_sn: u64,
    pub ext_last_sn: u64,
    pub ext_second_last_sn: u64,
    pub sn_offset: u64,
    pub ext_last_ts: u64,
    pub ext_second_last_ts: u64,
    pub ts_offset: u64,
    pub last_marker: bool,
    pub second_last_marker: bool,
}

impl RtpMunger {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            ext_highest_incoming_sn: 0,
            sn_range_map: utils::RangeMap::new(100),
            ext_last_sn: 0,
            ext_second_last_sn: 0,
            sn_offset: 0,
            ext_last_ts: 0,
            ext_second_last_ts: 0,
            ts_offset: 0,
            last_marker: false,
            second_last_marker: false,
            ext_rtx_gate_sn: 0,
            is_in_rtx_gate_region: false,
        }
    }

    pub fn debug_info(&self) -> RtpMungerDebugInfo {
        RtpMungerDebugInfo {
            ext_highest_incoming_sn: self.ext_highest_incoming_sn,
            ext_last_sn: self.ext_last_sn,
            ext_second_last_sn: self.ext_second_last_sn,
            sn_offset: self.sn_offset,
            ext_last_ts: self.ext_last_ts,
            ext_second_last_ts: self.ext_second_last_ts,
            ts_offset: self.ts_offset,
            last_marker: self.last_marker,
            second_last_marker: self.second_last_marker,
        }
    }

    pub fn get_state(&self) -> livekit::RtpMungerState {
        livekit::RtpMungerState {
            ext_last_sequence_number: self.ext_last_sn,
            ext_second_last_sequence_number: self.ext_second_last_sn,
            ext_last_timestamp: self.ext_last_ts,
            ext_second_last_timestamp: self.ext_second_last_ts,
            last_marker: self.last_marker,
            second_last_marker: self.second_last_marker,
        }
    }

    pub fn get_ext_last_timestamp(&self) -> u64 {
        self.ext_last_ts
    }

    pub fn get_ts_offset(&self) -> u64 {
        self.ts_offset
    }

    pub fn seed_state(&mut self, state: &livekit::RtpMungerState) {
        self.ext_last_sn = state.ext_last_sequence_number;
        self.ext_second_last_sn = state.ext_second_last_sequence_number;
        self.ext_last_ts = state.ext_last_timestamp;
        self.ext_second_last_ts = state.ext_second_last_timestamp;
        self.last_marker = state.last_marker;
        self.second_last_marker = state.second_last_marker;
    }

    pub fn set_last_sn_ts(&mut self, ext_pkt: &buffer::ExtPacket) {
        self.ext_highest_incoming_sn = ext_pkt.ext_sequence_number.wrapping_sub(1);

        self.ext_last_sn = ext_pkt.ext_sequence_number;
        self.ext_second_last_sn = self.ext_last_sn.wrapping_sub(1);
        self.sn_range_map
            .clear_and_reset_value(ext_pkt.ext_sequence_number, 0);
        self.update_sn_offset();

        self.ext_last_ts = ext_pkt.ext_timestamp;
        self.ext_second_last_ts = ext_pkt.ext_timestamp;
        self.ts_offset = 0;
    }

    pub fn update_sn_ts_offsets(
        &mut self,
        ext_pkt: &buffer::ExtPacket,
        sn_adjust: u64,
        ts_adjust: u64,
    ) {
        self.ext_highest_incoming_sn = ext_pkt.ext_sequence_number.wrapping_sub(1);

        self.sn_range_map.clear_and_reset_value(
            ext_pkt.ext_sequence_number,
            ext_pkt
                .ext_sequence_number
                .wrapping_sub(self.ext_last_sn)
                .wrapping_sub(sn_adjust),
        );
        self.update_sn_offset();

        self.ts_offset = ext_pkt
            .ext_timestamp
            .wrapping_sub(self.ext_last_ts)
            .wrapping_sub(ts_adjust);
    }

    pub fn packet_dropped(&mut self, ext_pkt: &buffer::ExtPacket) {
        if self.ext_highest_incoming_sn != ext_pkt.ext_sequence_number {
            return;
        }

        if let Ok(sn_offset) = self.sn_range_map.get_value(ext_pkt.ext_sequence_number) {
            let out_sn = ext_pkt.ext_sequence_number.wrapping_sub(sn_offset);
            if out_sn != self.ext_last_sn {
                warnw!(
                    self.logger,
                    "last outgoing sequence number mismatch",
                    nil,
                    "expected" => self.ext_last_sn,
                    "got" => out_sn,
                );
            }
        }
        if self.ext_last_sn == self.ext_second_last_sn {
            warnw!(
                self.logger,
                "cannot roll back on drop",
                nil,
                "extLastSN" => self.ext_last_sn,
                "secondLastSN" => self.ext_second_last_sn,
            );
        }

        if let Err(err) = self.sn_range_map.exclude_range(
            self.ext_highest_incoming_sn,
            self.ext_highest_incoming_sn.wrapping_add(1),
        ) {
            errorw!(
                self.logger,
                "could not exclude range",
                err,
                "sn" => self.ext_highest_incoming_sn,
            );
        }

        self.ext_last_sn = self.ext_second_last_sn;
        self.update_sn_offset();

        self.ext_last_ts = self.ext_second_last_ts;
        self.last_marker = self.second_last_marker;
    }

    pub fn update_and_get_sn_ts(
        &mut self,
        ext_pkt: &buffer::ExtPacket,
        marker: bool,
    ) -> (TranslationParamsRtp, Result<(), Error>) {
        let diff = ext_pkt
            .ext_sequence_number
            .wrapping_sub(self.ext_highest_incoming_sn) as i64;
        if (diff == 1 && !ext_pkt.packet.payload.is_empty()) || diff > 1 {
            // in-order - either contiguous packet with payload OR packet following a gap, may or may not have payload
            self.ext_highest_incoming_sn = ext_pkt.ext_sequence_number;

            let mut ordering = SequenceNumberOrdering::Contiguous;
            if diff > 1 {
                ordering = SequenceNumberOrdering::Gap;
            }

            let ext_munged_sn = ext_pkt.ext_sequence_number.wrapping_sub(self.sn_offset);
            let ext_munged_ts = ext_pkt.ext_timestamp.wrapping_sub(self.ts_offset);

            self.ext_second_last_sn = self.ext_last_sn;
            self.ext_last_sn = ext_munged_sn;
            self.ext_second_last_ts = self.ext_last_ts;
            self.ext_last_ts = ext_munged_ts;
            self.second_last_marker = self.last_marker;
            self.last_marker = marker;

            if ext_pkt.is_key_frame {
                self.ext_rtx_gate_sn = ext_munged_sn;
                self.is_in_rtx_gate_region = true;
            }

            if self.is_in_rtx_gate_region
                && ext_munged_sn.wrapping_sub(self.ext_rtx_gate_sn) > RTX_GATE_WINDOW
            {
                self.is_in_rtx_gate_region = false;
            }

            return (
                TranslationParamsRtp {
                    sn_ordering: ordering,
                    ext_sequence_number: ext_munged_sn,
                    ext_timestamp: ext_munged_ts,
                },
                Ok(()),
            );
        }

        if diff < 0 {
            // out-of-order, look up sequence number offset cache
            let Ok(sn_offset) = self.sn_range_map.get_value(ext_pkt.ext_sequence_number) else {
                return (
                    TranslationParamsRtp {
                        sn_ordering: SequenceNumberOrdering::OutOfOrder,
                        ..Default::default()
                    },
                    Err(Error::OutOfOrderSequenceNumberCacheMiss),
                );
            };

            let ext_sequence_number = ext_pkt.ext_sequence_number.wrapping_sub(sn_offset);
            if ext_sequence_number >= self.ext_last_sn {
                // should not happen, just being paranoid
                errorw!(
                    self.logger,
                    "unexpected packet ordering",
                    nil,
                    "extIncomingSN" => ext_pkt.ext_sequence_number,
                    "extHighestIncomingSN" => self.ext_highest_incoming_sn,
                    "extLastSN" => self.ext_last_sn,
                    "snOffsetIncoming" => sn_offset,
                    "snOffsetHighest" => self.sn_offset,
                );
                return (
                    TranslationParamsRtp {
                        sn_ordering: SequenceNumberOrdering::OutOfOrder,
                        ..Default::default()
                    },
                    Err(Error::OutOfOrderSequenceNumberCacheMiss),
                );
            }

            return (
                TranslationParamsRtp {
                    sn_ordering: SequenceNumberOrdering::OutOfOrder,
                    ext_sequence_number,
                    ext_timestamp: ext_pkt.ext_timestamp.wrapping_sub(self.ts_offset),
                },
                Ok(()),
            );
        }

        // if padding only packet, can be dropped and sequence number adjusted, if contiguous
        if diff == 1 {
            self.ext_highest_incoming_sn = ext_pkt.ext_sequence_number;

            if let Err(err) = self.sn_range_map.exclude_range(
                self.ext_highest_incoming_sn,
                self.ext_highest_incoming_sn.wrapping_add(1),
            ) {
                errorw!(
                    self.logger,
                    "could not exclude range",
                    err,
                    "sn" => self.ext_highest_incoming_sn,
                );
            }

            self.update_sn_offset();

            return (
                TranslationParamsRtp {
                    sn_ordering: SequenceNumberOrdering::Contiguous,
                    ..Default::default()
                },
                Err(Error::PaddingOnlyPacket),
            );
        }

        // can get duplicate packet due to FEC
        (
            TranslationParamsRtp {
                sn_ordering: SequenceNumberOrdering::Duplicate,
                ..Default::default()
            },
            Err(Error::DuplicatePacket),
        )
    }

    pub fn filter_rtx(&self, nacks: &[u16]) -> Vec<u16> {
        if !self.is_in_rtx_gate_region {
            return nacks.to_vec();
        }

        let mut filtered = Vec::with_capacity(nacks.len());
        for &sn in nacks {
            if sn.wrapping_sub(self.ext_rtx_gate_sn as u16) < (1 << 15) {
                filtered.push(sn);
            }
        }

        filtered
    }

    #[allow(clippy::manual_checked_ops)] // the check and the division as the original has them
    pub fn update_and_get_padding_sn_ts(
        &mut self,
        num: usize,
        clock_rate: u32,
        frame_rate: u32,
        force_marker: bool,
        ext_rtp_timestamp: u64,
    ) -> Result<Vec<SnTs>, Error> {
        if num == 0 {
            return Ok(Vec::new());
        }

        let mut use_last_ts_for_first = false;
        let mut ts_offset = 0;
        if !self.last_marker {
            if !force_marker {
                return Err(Error::PaddingNotOnFrameBoundary);
            }

            // if forcing frame end, use timestamp of latest received frame for the first one
            use_last_ts_for_first = true;
            ts_offset = 1;
        }

        let mut ext_last_sn = self.ext_last_sn;
        let mut ext_last_ts = self.ext_last_ts;
        let mut vals = vec![SnTs::default(); num];
        for (i, val) in vals.iter_mut().enumerate() {
            ext_last_sn = ext_last_sn.wrapping_add(1);
            val.ext_sequence_number = ext_last_sn;

            if frame_rate != 0 {
                if use_last_ts_for_first && i == 0 {
                    val.ext_timestamp = self.ext_last_ts;
                } else {
                    let mut ets = ext_rtp_timestamp.wrapping_add(
                        (((i + 1 - ts_offset) as u32)
                            .wrapping_mul(clock_rate)
                            .wrapping_add(frame_rate)
                            .wrapping_sub(1)
                            / frame_rate) as u64,
                    );
                    if (ets.wrapping_sub(ext_last_ts) as i64) <= 0 {
                        ets = ext_last_ts.wrapping_add(1);
                    }
                    ext_last_ts = ets;
                    val.ext_timestamp = ets;
                }
            } else {
                val.ext_timestamp = self.ext_last_ts;
            }
        }

        self.ext_second_last_sn = ext_last_sn.wrapping_sub(1);
        self.ext_last_sn = ext_last_sn;
        self.sn_range_map
            .dec_value(self.ext_highest_incoming_sn, num as u64);
        self.update_sn_offset();

        if vals.len() == 1 {
            self.ext_second_last_ts = self.ext_last_ts;
        } else {
            self.ext_second_last_ts = vals[vals.len() - 2].ext_timestamp;
        }
        self.ts_offset = self
            .ts_offset
            .wrapping_sub(ext_last_ts.wrapping_sub(self.ext_last_ts));
        self.ext_last_ts = ext_last_ts;

        self.second_last_marker = self.last_marker;
        if force_marker {
            self.last_marker = true;
        }

        Ok(vals)
    }

    pub fn is_on_frame_boundary(&self) -> bool {
        self.last_marker
    }

    fn update_sn_offset(&mut self) {
        let sn_offset = match self
            .sn_range_map
            .get_value(self.ext_highest_incoming_sn.wrapping_add(1))
        {
            Ok(sn_offset) => sn_offset,
            Err(err) => {
                errorw!(self.logger, "could not get sequence number offset", err);
                // what the original's `GetValue` returns next to an error
                0
            }
        };
        self.sn_offset = sn_offset;
    }
}

#[cfg(test)]
#[path = "rtpmunger_test.rs"]
mod rtpmunger_test;
