// Ported from pion/interceptor, pkg/twcc/twcc.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; the feedback is built with the types of the crate
// `rtc-rtcp`, where the packet status symbols and the symbol size are enums and where
// `TransportLayerCc` has no header field (the crate computes the header from the contents
// when the packet is marshalled): `get_rtcp` still computes the header as the original does
// and asserts, in debug builds, that the crate's is the same; `get_rtcp` takes the feedback
// by value, as it must be called at most once; a nil slice of packets is an empty vector; the
// start sequence number, a pointer upstream, is an `Option`.

use crate::webrtc::interceptor::internal::sequencenumber;
use crate::webrtc::rtcp;
use crate::webrtc::rtcp::Packet as _;
use crate::webrtc::rtcp::header::{FORMAT_TCC, PacketType};
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::{
    PacketStatusChunk, RecvDelta, RunLengthChunk, StatusVectorChunk, SymbolSizeTypeTcc,
    SymbolTypeTcc, TYPE_TCC_DELTA_SCALE_FACTOR, TransportLayerCc,
};

use super::arrival_time_map::PacketArrivalTimeMap;

const PACKET_WINDOW_MICROSECONDS: i64 = 500_000;
const MAX_MISSING_SEQUENCE_NUMBERS: i64 = 0x7FFE;

/// Recorder records incoming RTP packets and their delays and creates
/// transport wide congestion control feedback reports as specified in
/// <https://datatracker.ietf.org/doc/html/draft-holmer-rmcat-transport-wide-cc-extensions-01>
#[derive(Default)]
pub struct Recorder {
    arrival_time_map: PacketArrivalTimeMap,

    sequence_unwrapper: sequencenumber::Unwrapper,

    // startSequenceNumber is the first sequence number that will be included in the the
    // next feedback packet.
    start_sequence_number: Option<i64>,

    sender_ssrc: u32,
    media_ssrc: u32,
    fb_pkt_cnt: u8,

    packets_held: usize,
}

impl Recorder {
    /// NewRecorder creates a new Recorder which uses the given senderSSRC in the created
    /// feedback packets.
    pub fn new(sender_ssrc: u32) -> Self {
        Self {
            sender_ssrc,
            ..Default::default()
        }
    }

    /// Record marks a packet with mediaSSRC and a transport wide sequence number sequenceNumber as received at arrivalTime.
    pub fn record(&mut self, media_ssrc: u32, sequence_number: u16, arrival_time: i64) {
        self.media_ssrc = media_ssrc;

        // "Unwrap" the sequence number to get a monotonically increasing sequence number that
        // won't wrap around after math.MaxUint16.
        let unwrapped_sn = self.sequence_unwrapper.unwrap(sequence_number);
        self.maybe_cull_old_packets(unwrapped_sn, arrival_time);
        if self
            .start_sequence_number
            .is_none_or(|start_sequence_number| unwrapped_sn < start_sequence_number)
        {
            self.set_start_sequence_number(unwrapped_sn);
        }

        // We are only interested in the first time a packet is received.
        if self.arrival_time_map.has_received(unwrapped_sn) {
            return;
        }

        self.arrival_time_map.add_packet(unwrapped_sn, arrival_time);
        self.packets_held += 1;

        // Limit the range of sequence numbers to send feedback for.
        let begin_sequence_number = self.arrival_time_map.begin_sequence_number();
        if self
            .start_sequence_number
            .is_some_and(|start_sequence_number| start_sequence_number < begin_sequence_number)
        {
            self.set_start_sequence_number(begin_sequence_number);
        }
    }

    // setStartSequenceNumber stores sequenceNumber. (Upstream it does so in a single reused
    // allocation, which a pointer needs and an `Option` does not.)
    fn set_start_sequence_number(&mut self, sequence_number: i64) {
        self.start_sequence_number = Some(sequence_number);
    }

    fn maybe_cull_old_packets(&mut self, sequence_number: i64, arrival_time: i64) {
        if self
            .start_sequence_number
            .is_some_and(|start_sequence_number| {
                start_sequence_number >= self.arrival_time_map.end_sequence_number()
            })
            && arrival_time >= PACKET_WINDOW_MICROSECONDS
        {
            self.arrival_time_map
                .remove_old_packets(sequence_number, arrival_time - PACKET_WINDOW_MICROSECONDS);
        }
    }

    /// PacketsHeld returns the number of received packets currently held by the recorder.
    pub fn packets_held(&self) -> usize {
        self.packets_held
    }

    /// BuildFeedbackPacket creates a new RTCP packet containing a TWCC feedback report.
    pub fn build_feedback_packet(&mut self) -> Vec<Box<dyn rtcp::Packet>> {
        if self.start_sequence_number.is_none() {
            return Vec::new();
        }

        let end_sn = self.arrival_time_map.end_sequence_number();
        let mut feedbacks: Vec<Box<dyn rtcp::Packet>> = Vec::new();
        while let Some(start_sequence_number) = self.start_sequence_number {
            if start_sequence_number >= end_sn {
                break;
            }
            let Some(feedback) = self.maybe_build_feedback_packet(start_sequence_number, end_sn)
            else {
                break;
            };
            feedbacks.push(Box::new(feedback.get_rtcp()));

            // NOTE: we don't erase packets from the history in case they need to be resent
            // after a reordering. They will be removed instead in Record when they get too
            // old.
        }
        self.packets_held = 0;

        feedbacks
    }

    // maybeBuildFeedbackPacket builds a feedback packet starting from startSN (inclusive) until
    // endSN (exclusive).
    fn maybe_build_feedback_packet(
        &mut self,
        begin_seq_num_inclusive: i64,
        end_seq_num_exclusive: i64,
    ) -> Option<Feedback> {
        // NOTE: The logic of this method is inspired by the implementation in Chrome.
        // See https://source.chromium.org/chromium/chromium/src/+/refs/heads/main:third_party/webrtc/modules/remote_bitrate_estimator/remote_estimator_proxy.cc;l=276;drc=b5cd13bb6d5d157a5fbe3628b2dd1c1e106203c6
        let (start_sn_inclusive, end_sn_exclusive) = (
            self.arrival_time_map.clamp(begin_seq_num_inclusive),
            self.arrival_time_map.clamp(end_seq_num_exclusive),
        );

        // Create feedback on demand, as we don't yet know if there are packets in the range that have been
        // received.
        let mut fb: Option<Feedback> = None;

        let mut next_sequence_number = begin_seq_num_inclusive;

        let mut seq = start_sn_inclusive;
        while seq < end_sn_exclusive {
            let (found_seq, arrival_time, ok) = self.arrival_time_map.find_next_at_or_after(seq);
            seq = found_seq;
            if !ok || seq >= end_sn_exclusive {
                break;
            }

            match fb.as_mut() {
                None => {
                    let mut new_fb =
                        Feedback::new(self.sender_ssrc, self.media_ssrc, self.fb_pkt_cnt);
                    self.fb_pkt_cnt = self.fb_pkt_cnt.wrapping_add(1);

                    // It should be possible to add seq to this new packet.
                    // If the difference between seq and beginSeqNumInclusive is too large, discard
                    // reporting too old missing packets.
                    let base_sequence_number =
                        begin_seq_num_inclusive.max(seq - MAX_MISSING_SEQUENCE_NUMBERS);

                    // baseSequenceNumber is the expected first sequence number. This is known,
                    // but we may not have actually received it, so the base time should be the time
                    // of the first received packet in the feedback.
                    new_fb.set_base(base_sequence_number as u16, arrival_time);

                    if !new_fb.add_received(seq as u16, arrival_time) {
                        // Could not add a single received packet to the feedback.
                        // This is unexpected to actually occur, but if it does, we'll
                        // try again after skipping any missing packets.
                        // NOTE: It's fine that we already incremented fbPktCnt, as in essence
                        // we did actually "skip" a feedback (and this matches Chrome's behavior).
                        self.set_start_sequence_number(seq);

                        return None;
                    }
                    fb = Some(new_fb);
                }
                Some(fb) => {
                    if !fb.add_received(seq as u16, arrival_time) {
                        // Could not add timestamp. Packet may be full. Return
                        // and try again with a fresh packet.
                        break;
                    }
                }
            }

            next_sequence_number = seq + 1;
            seq += 1;
        }

        self.set_start_sequence_number(next_sequence_number);

        fb
    }
}

#[derive(Debug, Default)]
struct Feedback {
    rtcp: TransportLayerCc,
    base_sequence_number: u16,
    ref_timestamp_64ms: i64,
    last_timestamp_us: i64,
    next_sequence_number: u16,
    sequence_number_count: u16,
    len: usize,
    last_chunk: Chunk,
    chunks: Vec<PacketStatusChunk>,
    deltas: Vec<RecvDelta>,
}

impl Feedback {
    fn new(sender_ssrc: u32, media_ssrc: u32, count: u8) -> Self {
        Self {
            rtcp: TransportLayerCc {
                sender_ssrc,
                media_ssrc,
                fb_pkt_count: count,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn set_base(&mut self, sequence_number: u16, time_us: i64) {
        self.base_sequence_number = sequence_number;
        self.next_sequence_number = self.base_sequence_number;
        self.ref_timestamp_64ms = time_us / 64_000;
        self.last_timestamp_us = self.ref_timestamp_64ms * 64_000;
    }

    // getRTCP finalizes the feedback and returns it as an RTCP packet. It consumes
    // the accumulated state and must be called at most once.
    fn get_rtcp(mut self) -> TransportLayerCc {
        self.rtcp.packet_status_count = self.sequence_number_count;
        self.rtcp.reference_time = self.ref_timestamp_64ms as u32;
        self.rtcp.base_sequence_number = self.base_sequence_number;
        while !self.last_chunk.deltas.is_empty() {
            self.chunks.push(self.last_chunk.encode());
        }
        self.rtcp.packet_chunks.append(&mut self.chunks);
        self.rtcp.recv_deltas = std::mem::take(&mut self.deltas);

        // 4 bytes header + 16 bytes twcc header + 2 bytes for each chunk + length of deltas
        let mut pad_len = 20 + self.rtcp.packet_chunks.len() * 2 + self.len;
        let padding = !pad_len.is_multiple_of(4);
        while !pad_len.is_multiple_of(4) {
            pad_len += 1;
        }
        // The original stores this header in the packet. The packet of `rtc-rtcp` has no place
        // for it and computes its own: the two have to be the same.
        let header = rtcp::Header {
            count: FORMAT_TCC,
            packet_type: PacketType::TransportSpecificFeedback,
            padding,
            length: ((pad_len / 4) - 1) as u16,
        };
        debug_assert_eq!(header, self.rtcp.header());

        self.rtcp
    }

    #[allow(clippy::manual_range_contains)] // the comparisons as the original has them
    fn add_received(&mut self, sequence_number: u16, timestamp_us: i64) -> bool {
        let delta_us = timestamp_us.wrapping_sub(self.last_timestamp_us);
        let delta_250us = if delta_us >= 0 {
            delta_us.wrapping_add(TYPE_TCC_DELTA_SCALE_FACTOR / 2) / TYPE_TCC_DELTA_SCALE_FACTOR
        } else {
            delta_us.wrapping_sub(TYPE_TCC_DELTA_SCALE_FACTOR / 2) / TYPE_TCC_DELTA_SCALE_FACTOR
        };
        // delta doesn't fit into 16 bit, need to create new packet
        if delta_250us < i16::MIN as i64 || delta_250us > i16::MAX as i64 {
            return false;
        }
        let delta_us_rounded = delta_250us * TYPE_TCC_DELTA_SCALE_FACTOR;

        while self.next_sequence_number != sequence_number {
            if !self.last_chunk.can_add(SymbolTypeTcc::PacketNotReceived) {
                self.chunks.push(self.last_chunk.encode());
            }
            self.last_chunk.add(SymbolTypeTcc::PacketNotReceived);
            self.sequence_number_count = self.sequence_number_count.wrapping_add(1);

            // The post statement of the original's loop.
            self.next_sequence_number = self.next_sequence_number.wrapping_add(1);
        }

        let recv_delta = if delta_250us >= 0 && delta_250us <= 0xff {
            self.len += 1;
            SymbolTypeTcc::PacketReceivedSmallDelta
        } else {
            self.len += 2;
            SymbolTypeTcc::PacketReceivedLargeDelta
        };

        if !self.last_chunk.can_add(recv_delta) {
            self.chunks.push(self.last_chunk.encode());
        }
        self.last_chunk.add(recv_delta);
        self.deltas.push(RecvDelta {
            type_tcc_packet: recv_delta,
            delta: delta_us_rounded,
        });
        self.last_timestamp_us = self.last_timestamp_us.wrapping_add(delta_us_rounded);
        self.sequence_number_count = self.sequence_number_count.wrapping_add(1);
        self.next_sequence_number = self.next_sequence_number.wrapping_add(1);

        true
    }
}

const MAX_RUN_LENGTH_CAP: usize = 0x1fff; // 13 bits
const MAX_ONE_BIT_CAP: usize = 14; // bits
const MAX_TWO_BIT_CAP: usize = 7; // bits

#[derive(Debug, Default)]
struct Chunk {
    has_large_delta: bool,
    has_different_types: bool,
    deltas: Vec<SymbolTypeTcc>,
}

impl Chunk {
    fn can_add(&self, delta: SymbolTypeTcc) -> bool {
        if self.deltas.len() < MAX_TWO_BIT_CAP {
            return true;
        }
        if self.deltas.len() < MAX_ONE_BIT_CAP
            && !self.has_large_delta
            && delta != SymbolTypeTcc::PacketReceivedLargeDelta
        {
            return true;
        }
        if self.deltas.len() < MAX_RUN_LENGTH_CAP
            && !self.has_different_types
            && delta == self.deltas[0]
        {
            return true;
        }

        false
    }

    fn add(&mut self, delta: SymbolTypeTcc) {
        self.deltas.push(delta);
        self.has_large_delta =
            self.has_large_delta || delta == SymbolTypeTcc::PacketReceivedLargeDelta;
        self.has_different_types = self.has_different_types || delta != self.deltas[0];
    }

    // The original leaves the `Type` of both kinds of chunk at its zero value, which is the
    // run length chunk's also in a status vector chunk. Marshalling does not look at it.
    fn encode(&mut self) -> PacketStatusChunk {
        if !self.has_different_types {
            let chunk = PacketStatusChunk::RunLengthChunk(RunLengthChunk {
                packet_status_symbol: self.deltas[0],
                run_length: self.deltas.len() as u16,
                ..Default::default()
            });
            self.reset();

            return chunk;
        }
        if self.deltas.len() == MAX_ONE_BIT_CAP {
            let chunk = PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                symbol_size: SymbolSizeTypeTcc::OneBit,
                symbol_list: std::mem::take(&mut self.deltas),
                ..Default::default()
            });
            self.reset();

            return chunk;
        }

        let min_cap = MAX_TWO_BIT_CAP.min(self.deltas.len());
        let svc = PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
            symbol_size: SymbolSizeTypeTcc::TwoBit,
            symbol_list: self.deltas.drain(..min_cap).collect(),
            ..Default::default()
        });
        self.has_different_types = false;
        self.has_large_delta = false;

        if !self.deltas.is_empty() {
            let tmp = self.deltas[0];
            for d in &self.deltas {
                if tmp != *d {
                    self.has_different_types = true;
                }
                if *d == SymbolTypeTcc::PacketReceivedLargeDelta {
                    self.has_large_delta = true;
                }
            }
        }

        svc
    }

    fn reset(&mut self) {
        self.deltas = Vec::new();
        self.has_large_delta = false;
        self.has_different_types = false;
    }
}

#[cfg(test)]
#[path = "twcc_test.rs"]
mod twcc_test;
