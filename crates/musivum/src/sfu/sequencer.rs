// Ported from LiveKit, pkg/sfu/sequencer.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; what the embedded `sync.Mutex` guards is in an inner
// struct (`size`, `startTime` and the logger do not change after construction and stay
// outside it); of the functions the original calls with the lock held, `updateSNOffset` takes
// the inner struct, and `invalidateSlot` and `isInvalidSlot` take the slots, which is the
// part of it they work on; `newSequencer` gives `None` where the original returns nil, and
// `lookupExtPacketMeta` likewise; `MarshalLogObject` is `Debug`; a nil slice is an empty one;
// the embedded `packetMeta` is the field `packet_meta`; sizes and slots are unsigned.

use std::fmt;

use parking_lot::Mutex;

use crate::go::time;
use crate::protocol::logger::Logger;
use crate::sfu::rtpextension::abscapturetime as act;
use crate::sfu::utils;
use crate::{errorw, warnw};

const DEFAULT_RTT: u32 = 70;
pub(super) const IGNORE_RETRANSMISSION: i64 = 100; // Ignore packet retransmission after ignoreRetransmission milliseconds
const MAX_ACK: u8 = 3;

#[derive(Clone, Default, PartialEq, Eq)]
pub(super) struct PacketMeta {
    // Original extended sequence number from stream.
    // The original extended sequence number is used to find the original
    // packet from publisher
    pub(super) source_seq_no: u64,
    // Modified sequence number after offset.
    // This sequence number is used for the associated
    // down track, is modified according the offsets, and
    // must not be shared
    pub(super) target_seq_no: u16,
    // Modified timestamp for current associated
    // down track.
    pub(super) timestamp: u32,
    // Modified marker
    pub(super) marker: bool,
    // The last time this packet was nack requested.
    // Sometimes clients request the same packet more than once, so keep
    // track of the requested packets helps to avoid writing multiple times
    // the same packet.
    // The resolution is 1 ms counting after the sequencer start time.
    pub(super) last_nack: u32,
    // number of NACKs this packet has received
    pub(super) nacked: u8,
    // Spatial layer of packet
    pub(super) layer: i8,
    // Information that differs depending on the codec
    pub(super) codec_bytes: [u8; 8],
    pub(super) num_codec_bytes_in: u8,
    pub(super) num_codec_bytes_out: u8,
    pub(super) codec_bytes_slice: Vec<u8>,
    // Dependency Descriptor of packet
    pub(super) dd_bytes: [u8; 8],
    pub(super) dd_bytes_size: u8,
    pub(super) dd_bytes_slice: Vec<u8>,
    // abs-capture-time of packet
    pub(super) act_bytes: [u8; act::MAX_SIZE],
    pub(super) act_bytes_size: u8,
    // number of packet trailer bytes stripped when the packet was forwarded
    pub(super) trailer_stripped: u8,
}

impl fmt::Debug for PacketMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("PacketMeta");
        e.field("sourceSeqNo", &self.source_seq_no);
        e.field("targetSeqNo", &self.target_seq_no);
        e.field("layer", &self.layer);
        e.field("nacked", &self.nacked);
        e.field("numCodecBytesIn", &self.num_codec_bytes_in);
        if !self.codec_bytes_slice.is_empty() {
            e.field("codecBytesSlice", &self.codec_bytes_slice.len());
        } else {
            e.field("numCodecBytesOut", &self.num_codec_bytes_out);
        }
        if !self.dd_bytes_slice.is_empty() {
            e.field("ddBytesSlice", &self.dd_bytes_slice.len());
        } else {
            e.field("ddBytesSize", &self.dd_bytes_size);
        }
        if self.act_bytes_size != 0 {
            e.field("actBytes", &self.act_bytes_size);
        }
        if self.trailer_stripped != 0 {
            e.field("trailerStripped", &self.trailer_stripped);
        }
        e.finish()
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub(super) struct ExtPacketMeta {
    pub(super) packet_meta: PacketMeta,
    pub(super) ext_sequence_number: u64,
    pub(super) ext_timestamp: u64,
    pub(super) slot: usize,
}

impl fmt::Debug for ExtPacketMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("ExtPacketMeta");
        e.field("packetMeta", &self.packet_meta);
        e.field("extSequenceNumber", &self.ext_sequence_number);
        e.field("extTimestamp", &self.ext_timestamp);
        e.finish()
    }
}

// Sequencer stores the packet sequence received by the down track
pub(super) struct Sequencer {
    size: usize,
    start_time: i64,
    logger: Logger,

    // what the embedded `sync.Mutex` guards upstream
    inner: Mutex<SequencerInner>,
}

struct SequencerInner {
    initialized: bool,
    ext_start_sn: u64,
    ext_highest_sn: u64,
    sn_offset: u64,
    ext_highest_ts: u64,
    meta: Vec<PacketMeta>,
    sn_range_map: Option<utils::RangeMap<u64, u64>>,
    rtt: u32,
}

impl Sequencer {
    #[allow(clippy::manual_div_ceil)] // the rounding as the original has it
    pub(super) fn new(size: usize, maybe_sparse: bool, logger: Logger) -> Option<Sequencer> {
        if size == 0 {
            return None;
        }

        let mut s = SequencerInner {
            initialized: false,
            ext_start_sn: 0,
            ext_highest_sn: 0,
            sn_offset: 0,
            ext_highest_ts: 0,
            meta: vec![PacketMeta::default(); size],
            sn_range_map: None,
            rtt: DEFAULT_RTT,
        };

        if maybe_sparse {
            s.sn_range_map = Some(utils::RangeMap::new((size + 1) / 2)); // assume run lengths of at least 2 in between padding bursts
        }
        Some(Sequencer {
            size,
            start_time: time::now().unix_nano(),
            logger,
            inner: Mutex::new(s),
        })
    }

    pub(super) fn set_rtt(&self, rtt: u32) {
        let mut inner = self.inner.lock();

        if rtt == 0 {
            inner.rtt = DEFAULT_RTT;
        } else {
            inner.rtt = rtt;
        }
    }

    // flush discards all recorded packet metadata. It must be called on a stream restart: the
    // metadata maps outgoing sequence numbers to source packets in the receiver's bucket, and a
    // restart resyncs that bucket, so retransmitting against stale metadata would send the wrong
    // packet (or read past the re-read payload). After flush, NACKs for pre-restart packets are
    // ignored until the sequencer is re-initialized by the next push.
    #[allow(clippy::manual_div_ceil)] // the rounding as the original has it
    pub(super) fn flush(&self) {
        let mut inner = self.inner.lock();

        inner.initialized = false;
        inner.ext_start_sn = 0;
        inner.ext_highest_sn = 0;
        inner.ext_highest_ts = 0;
        inner.sn_offset = 0;
        inner.meta.fill(PacketMeta::default());
        if inner.sn_range_map.is_some() {
            inner.sn_range_map = Some(utils::RangeMap::new((self.size + 1) / 2));
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn push(
        &self,
        packet_time: i64,
        ext_incoming_sn: u64,
        ext_modified_sn: u64,
        ext_modified_ts: u64,
        marker: bool,
        layer: i8,
        codec_bytes: &[u8],
        num_codec_bytes_in: usize,
        dd_bytes: &[u8],
        act_bytes: &[u8],
        trailer_stripped: usize,
    ) {
        let mut guard = self.inner.lock();
        let inner = &mut *guard;

        if !inner.initialized {
            inner.initialized = true;
            inner.ext_start_sn = ext_modified_sn;
            inner.ext_highest_sn = ext_modified_sn;
            inner.ext_highest_ts = ext_modified_ts;
            self.update_sn_offset(inner);
        }

        if ext_modified_sn < inner.ext_start_sn {
            // old packet, should not happen
            return;
        }

        let ext_highest_sn_adjusted = inner.ext_highest_sn.wrapping_sub(inner.sn_offset);
        let mut ext_modified_sn_adjusted = ext_modified_sn.wrapping_sub(inner.sn_offset);
        if ext_modified_sn < inner.ext_highest_sn
            && let Some(sn_range_map) = &inner.sn_range_map
        {
            let sn_offset = match sn_range_map.get_value(ext_modified_sn) {
                Ok(sn_offset) => sn_offset,
                Err(err) => {
                    errorw!(
                        self.logger,
                        "could not get sequence number offset",
                        err,
                        "extStartSN" => inner.ext_start_sn,
                        "extHighestSN" => inner.ext_highest_sn,
                        "extIncomingSN" => ext_incoming_sn,
                        "extModifiedSN" => ext_modified_sn,
                        "snOffset" => inner.sn_offset,
                    );
                    return;
                }
            };

            ext_modified_sn_adjusted = ext_modified_sn.wrapping_sub(sn_offset);
        }

        if (ext_modified_sn_adjusted.wrapping_sub(ext_highest_sn_adjusted) as i64)
            <= -(self.size as i64)
        {
            warnw!(
                self.logger,
                "old packet, cannot be sequenced",
                nil,
                "extHighestSN" => inner.ext_highest_sn,
                "extIncomingSN" => ext_incoming_sn,
                "extModifiedSN" => ext_modified_sn,
            );
            return;
        }

        // invalidate missing sequence numbers
        if ext_modified_sn_adjusted > ext_highest_sn_adjusted {
            let mut num_invalidated = 0;
            // for esn := extHighestSNAdjusted + 1; esn != extModifiedSNAdjusted; esn++
            for esn in std::iter::successors(Some(ext_highest_sn_adjusted.wrapping_add(1)), |esn| {
                Some(esn.wrapping_add(1))
            })
            .take_while(|&esn| esn != ext_modified_sn_adjusted)
            {
                invalidate_slot(&mut inner.meta, (esn % self.size as u64) as usize);
                num_invalidated += 1;
                if num_invalidated >= self.size {
                    break;
                }
            }
        }

        let slot = (ext_modified_sn_adjusted % self.size as u64) as usize;
        inner.meta[slot] = PacketMeta {
            source_seq_no: ext_incoming_sn,
            target_seq_no: ext_modified_sn as u16,
            timestamp: ext_modified_ts as u32,
            marker,
            layer,
            num_codec_bytes_in: num_codec_bytes_in as u8,
            trailer_stripped: trailer_stripped as u8,
            last_nack: self.get_ref_time(packet_time), // delay retransmissions after the original transmission
            ..Default::default()
        };
        let pm = &mut inner.meta[slot];

        pm.num_codec_bytes_out = codec_bytes.len() as u8;
        if codec_bytes.len() > pm.codec_bytes.len() {
            pm.codec_bytes_slice = codec_bytes.to_vec();
        } else {
            pm.codec_bytes[..pm.num_codec_bytes_out as usize].copy_from_slice(codec_bytes);
        }

        pm.dd_bytes_size = dd_bytes.len() as u8;
        if dd_bytes.len() > pm.dd_bytes.len() {
            pm.dd_bytes_slice = dd_bytes.to_vec();
        } else {
            pm.dd_bytes[..pm.dd_bytes_size as usize].copy_from_slice(dd_bytes);
        }

        // copy(pm.actBytes[:], actBytes): as much as fits
        let num_act_bytes = act_bytes.len().min(pm.act_bytes.len());
        pm.act_bytes[..num_act_bytes].copy_from_slice(&act_bytes[..num_act_bytes]);
        pm.act_bytes_size = num_act_bytes as u8;

        if ext_modified_sn > inner.ext_highest_sn {
            inner.ext_highest_sn = ext_modified_sn;
        }
        if ext_modified_ts > inner.ext_highest_ts {
            inner.ext_highest_ts = ext_modified_ts;
        }
    }

    pub(super) fn push_padding(&self, ext_start_sn_inclusive: u64, ext_end_sn_inclusive: u64) {
        let mut guard = self.inner.lock();
        let inner = &mut *guard;

        let (Some(sn_range_map), true) = (inner.sn_range_map.as_mut(), inner.initialized) else {
            return;
        };

        if ext_start_sn_inclusive <= inner.ext_highest_sn {
            // a higher sequence number has already been recorded with an offset,
            // adding an exclusion range before the highest means the offset of sequence numbers
            // after the exclusion range will be affected and all those higher sequence numbers
            // need to be patched.
            //
            // Not recording exclusion range means a few slots (of the size of exclusion range)
            // are wasted in this cycle. That should be fine as the exclusion ranges should be
            // a few packets at a time.
            if ext_end_sn_inclusive >= inner.ext_highest_sn {
                errorw!(
                    self.logger,
                    "cannot exclude overlapping range",
                    nil,
                    "extHighestSN" => inner.ext_highest_sn,
                    "startSN" => ext_start_sn_inclusive,
                    "endSN" => ext_end_sn_inclusive,
                );
            } else {
                warnw!(
                    self.logger,
                    "cannot exclude old range",
                    nil,
                    "extHighestSN" => inner.ext_highest_sn,
                    "startSN" => ext_start_sn_inclusive,
                    "endSN" => ext_end_sn_inclusive,
                );
            }

            // if exclusion range is before what has already been sequenced, invalidate exclusion range slots
            // for esn := extStartSNInclusive; esn != extEndSNInclusive+1; esn++
            let ext_end_sn_exclusive = ext_end_sn_inclusive.wrapping_add(1);
            for esn in std::iter::successors(Some(ext_start_sn_inclusive), |esn| {
                Some(esn.wrapping_add(1))
            })
            .take_while(|&esn| esn != ext_end_sn_exclusive)
            {
                let diff = esn.wrapping_sub(inner.ext_highest_sn) as i64;
                if diff >= 0 || diff < -(self.size as i64) {
                    // too old OR too new (too new should not happen, just be safe)
                    continue;
                }

                let sn_offset = match sn_range_map.get_value(esn) {
                    Ok(sn_offset) => sn_offset,
                    Err(err) => {
                        errorw!(
                            self.logger,
                            "could not get sequence number offset",
                            err,
                            "sn" => esn,
                        );
                        continue;
                    }
                };

                let slot = esn.wrapping_sub(sn_offset) % self.size as u64;
                invalidate_slot(&mut inner.meta, slot as usize);
            }
            return;
        }

        if let Err(err) =
            sn_range_map.exclude_range(ext_start_sn_inclusive, ext_end_sn_inclusive.wrapping_add(1))
        {
            errorw!(
                self.logger,
                "could not exclude range",
                err,
                "startSN" => ext_start_sn_inclusive,
                "endSN" => ext_end_sn_inclusive,
            );
            return;
        }

        inner.ext_highest_sn = ext_end_sn_inclusive;
        self.update_sn_offset(inner);
    }

    pub(super) fn get_ext_packet_metas(&self, seq_no: &[u16]) -> Vec<ExtPacketMeta> {
        let mut guard = self.inner.lock();
        let inner = &mut *guard;

        if !inner.initialized {
            return Vec::new();
        }

        let mut sn_offset = 0u64;
        let mut ext_packet_metas = Vec::with_capacity(seq_no.len());
        let ref_time = self.get_ref_time(time::now().unix_nano());
        let highest_sn = inner.ext_highest_sn as u16;
        let highest_ts = inner.ext_highest_ts as u32;
        for &sn in seq_no {
            let diff = highest_sn.wrapping_sub(sn);
            if diff > (1 << 15) {
                // out-of-order from head (should not happen, just be safe)
                continue;
            }

            // find slot by adjusting for padding only packets that were not recorded in sequencer
            let mut ext_sn = (sn as u64).wrapping_add(inner.ext_highest_sn & 0xFFFF_FFFF_FFFF_0000);
            if sn > highest_sn {
                ext_sn = ext_sn.wrapping_sub(1 << 16);
            }

            if let Some(sn_range_map) = &inner.sn_range_map {
                sn_offset = match sn_range_map.get_value(ext_sn) {
                    Ok(sn_offset) => sn_offset,
                    Err(_) => {
                        // could be padding packet which is excluded and will not have value
                        continue;
                    }
                };
            }

            let ext_sn_adjusted = ext_sn.wrapping_sub(sn_offset);
            let ext_highest_sn_adjusted = inner.ext_highest_sn.wrapping_sub(inner.sn_offset);
            if ext_highest_sn_adjusted.wrapping_sub(ext_sn_adjusted) >= self.size as u64 {
                // too old
                continue;
            }

            let slot = (ext_sn_adjusted % self.size as u64) as usize;
            if inner.meta[slot].target_seq_no != sn || is_invalid_slot(&inner.meta, slot) {
                // invalid slot access could happen if padding packets exclusion range could not be recorded
                continue;
            }
            let meta = &mut inner.meta[slot];

            if meta.nacked < MAX_ACK
                && ref_time.wrapping_sub(meta.last_nack)
                    > (IGNORE_RETRANSMISSION as f64).min(2u32.wrapping_mul(inner.rtt) as f64) as u32
            {
                meta.nacked += 1;
                meta.last_nack = ref_time;

                let mut ext_ts = (meta.timestamp as u64)
                    .wrapping_add(inner.ext_highest_ts & 0xFFFF_FFFF_0000_0000);
                if meta.timestamp > highest_ts {
                    ext_ts = ext_ts.wrapping_sub(1 << 32);
                }
                // the clone copies `codecBytesSlice` and `ddBytesSlice`, as the original does
                // by hand
                let epm = ExtPacketMeta {
                    packet_meta: meta.clone(),
                    ext_sequence_number: ext_sn,
                    ext_timestamp: ext_ts,
                    slot,
                };
                ext_packet_metas.push(epm);
            }
        }

        ext_packet_metas
    }

    // unmarkNacked reverts the NACK count getExtPacketMetas added for packets that were not
    // retransmitted, so they do not run into maxAck. lastNack stays, so a repeat NACK inside the
    // duplicate window is still ignored.
    pub(super) fn unmark_nacked(&self, epms: &[ExtPacketMeta]) {
        let mut inner = self.inner.lock();

        for epm in epms {
            let meta = &mut inner.meta[epm.slot];
            // skip if the slot holds a newer packet, or a newer NACK marked it again
            if meta.target_seq_no != epm.packet_meta.target_seq_no
                || meta.last_nack != epm.packet_meta.last_nack
                || meta.nacked == 0
            {
                continue;
            }
            meta.nacked -= 1;
        }
    }

    pub(super) fn lookup_ext_packet_meta(&self, ext_sn: u64) -> Option<ExtPacketMeta> {
        let inner = self.inner.lock();

        if !inner.initialized {
            return None;
        }

        let mut sn_offset = 0u64;
        if let Some(sn_range_map) = &inner.sn_range_map {
            sn_offset = match sn_range_map.get_value(ext_sn) {
                Ok(sn_offset) => sn_offset,
                Err(_) => return None,
            };
        }

        let ext_sn_adjusted = ext_sn.wrapping_sub(sn_offset);
        let ext_highest_sn_adjusted = inner.ext_highest_sn.wrapping_sub(inner.sn_offset);
        if ext_highest_sn_adjusted.wrapping_sub(ext_sn_adjusted) >= self.size as u64 {
            // too old
            return None;
        }

        let slot = (ext_sn_adjusted % self.size as u64) as usize;
        let meta = &inner.meta[slot];
        if is_invalid_slot(&inner.meta, slot) {
            // invalid slot access could happen if padding packets exclusion range could not be recorded
            return None;
        }

        let mut ext_ts =
            (meta.timestamp as u64).wrapping_add(inner.ext_highest_ts & 0xFFFF_FFFF_0000_0000);
        if meta.timestamp > inner.ext_highest_ts as u32 {
            ext_ts = ext_ts.wrapping_sub(1 << 32);
        }
        // the clone copies `codecBytesSlice` and `ddBytesSlice`, as the original does by hand;
        // the slot is left at zero there too
        let epm = ExtPacketMeta {
            packet_meta: meta.clone(),
            ext_sequence_number: ext_sn,
            ext_timestamp: ext_ts,
            ..Default::default()
        };
        Some(epm)
    }

    fn get_ref_time(&self, at: i64) -> u32 {
        (at.wrapping_sub(self.start_time) / 1_000_000) as u32
    }

    fn update_sn_offset(&self, inner: &mut SequencerInner) {
        let Some(sn_range_map) = &inner.sn_range_map else {
            return;
        };

        let sn_offset = match sn_range_map.get_value(inner.ext_highest_sn.wrapping_add(1)) {
            Ok(sn_offset) => sn_offset,
            Err(err) => {
                errorw!(
                    self.logger,
                    "could not update sequence number offset",
                    err,
                    "extHighestSN" => inner.ext_highest_sn,
                );
                return;
            }
        };
        inner.sn_offset = sn_offset;
    }
}

fn invalidate_slot(meta: &mut [PacketMeta], slot: usize) {
    if slot >= meta.len() {
        return;
    }

    meta[slot] = PacketMeta {
        source_seq_no: 0,
        target_seq_no: 0,
        last_nack: 0,
        ..Default::default()
    };
}

fn is_invalid_slot(meta: &[PacketMeta], slot: usize) -> bool {
    if slot >= meta.len() {
        return true;
    }

    let meta = &meta[slot];
    meta.source_seq_no == 0 && meta.target_seq_no == 0 && meta.last_nack == 0
}

#[cfg(test)]
#[path = "sequencer_test.rs"]
mod sequencer_test;
