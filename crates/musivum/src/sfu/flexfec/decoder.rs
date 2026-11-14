// Ported from LiveKit, pkg/sfu/flexfec/decoder.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The errors are one enum; those that the original wraps
// with details (`fmt.Errorf("%w: ...")`) carry as text what follows the `%w` there, and the
// two errors that the original makes on the spot are variants as well. The values of the key
// "error" in the log are the texts of the errors.
//
// The lookup of media packets is handed to `decode_fec` with every packet, and from there to
// the functions that use it, where the original is given it once, by `NewDecoder`, and keeps
// it. In LiveKit the lookup is a method of the buffer that owns the decoder, called while the
// buffer's lock is held; a closure kept inside the decoder could not borrow that buffer's
// state while the buffer calls into the decoder, one that comes with the call can. It gives
// whatever error its owner has, boxed, as it may in Go; no lookup (nil) is `None`.
//
// Payloads are `Bytes`: a FEC packet that is retained is cloned, which shares its bytes where
// the original copies them (nobody can write to them here), and the payload of the parsed
// header is a slice of the same bytes. The packets that come out are values, not pointers.
// `count_missing_packets` takes the parsed header by value, as the original does (a clone:
// the masks and a reference to the payload), `recover_packet` takes the index of the FEC
// packet in the list where the original takes a pointer into it, and `get_media_packet` has no
// `self`: all three because a method cannot borrow the decoder while the decoder's own buffer
// is written to. The sequence numbers that a FEC packet protects are appended to a list made
// with room for all of them, where the original appends to an array on its stack.
// `sort_search` is Go's `sort.Search`.

// Package flexfec implements a FlexFEC-03 decoder used to recover RTP packets
// lost on the publisher leg before they are forwarded downstream.
// https://datatracker.ietf.org/doc/html/draft-ietf-payload-flexible-fec-scheme-03
//
// The recovery logic is ported from pion/interceptor pkg/flexfec
// (https://github.com/pion/interceptor, MIT License, Copyright The Pion
// community), which is itself modeled on libwebrtc's ForwardErrorCorrection
// receiver. Deviations from the pion implementation:
//   - FEC packets are deep-copied only when retained (callers reuse packet
//     memory), while protected media is read from the owning packet store
//   - packet masks are expanded on demand instead of retaining per-packet
//     protection entries
//   - recovery XORs the stored RTP wire representation directly
//   - failed recoveries are not emitted as empty packets
//   - usage counters for metrics

use bytes::Bytes;

use crate::protocol::logger::Logger;
use crate::webrtc::rtp;
use crate::{debugw, infow};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
enum Error {
    #[error("packet truncated{0}")]
    PacketTruncated(String),
    #[error("packet with retransmission bit set not supported")]
    RetransmissionBitSet,
    #[error("packet with inflexible generator matrix not supported")]
    InflexibleGeneratorMatrix,
    #[error("multiple ssrc protection not supported{0}")]
    MultipleSsrcProtection(String),
    #[error("k-bit of last optional mask is set to false")]
    LastOptionalMaskKBitSetToFalse,
    #[error("empty fec packet mask")]
    EmptyMask,
    #[error("fec is protecting unknown ssrc")]
    UnknownProtectedSsrc,
    #[error("protected media packet not found")]
    MediaPacketNotFound,
    #[error("invalid recovered packet size{0}")]
    InvalidRecoveredPacketSize(String),

    // fmt.Errorf("cannot recover with %d missing packets", missing)
    #[error("cannot recover with {0} missing packets")]
    CannotRecover(usize),
    // fmt.Errorf("unmarshal recovered: %w", err)
    #[error("unmarshal recovered: {0}")]
    UnmarshalRecovered(rtp::Error),
}

// number of media arrivals before sequence gaps trigger a state reset
const MEDIA_PACKETS_BEFORE_GAP_CHECK: usize = 100;
// maximum number of FEC packets retained
const MAX_FEC_PACKETS: usize = 100;
// maximum number of sequence numbers represented by the three packet masks
const MAX_PROTECTED_PACKETS: usize = (FEC_MASK0_BITS + FEC_MASK1_BITS + FEC_MASK2_BITS) as usize;
// matches the maximum packet size of the primary RTP packet bucket
const MAX_MEDIA_PACKET_SIZE: usize = 1500;

// FlexFEC-03 header bit fields.
// https://datatracker.ietf.org/doc/html/draft-ietf-payload-flexible-fec-scheme-03#section-6.1
const FEC_RETRANSMISSION_BIT: u8 = 0x80; // R bit, first FEC header byte
const FEC_INFLEXIBLE_BIT: u8 = 0x40; // F bit, first FEC header byte
const FEC_MASK_K_BIT: u8 = 0x80; // K bit, terminates the run of packet-mask chunks

// Data-bit width of each packet-mask chunk (the chunk minus its K bit).
const FEC_MASK0_BITS: u16 = 15;
const FEC_MASK1_BITS: u16 = 31;
const FEC_MASK2_BITS: u16 = 63;

// Value masks that clear the K bit from each packet-mask chunk.
const FEC_MASK0_VALUE: u16 = 0x7FFF;
const FEC_MASK1_VALUE: u32 = 0x7FFFFFFF;
const FEC_MASK2_VALUE: u64 = 0x7FFFFFFFFFFFFFFF;

/// DecoderStats accumulates FEC usage counters. Snapshot via Decoder.Stats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DecoderStats {
    /// FEC packets fed to the decoder
    pub fec_packets_received: u64,
    /// FEC bytes fed to the decoder (RTP payload sizes)
    pub fec_bytes_received: u64,
    /// FEC packets that could not be used: parse failures, foreign protected
    /// SSRC, empty masks and duplicates
    pub fec_packets_discarded: u64,
    /// media packets reconstructed from FEC
    pub packets_recovered: u64,
}

/// The error of a [`MediaPacketLookup`]: Go's `error`. The decoder only asks whether there is
/// one.
pub type MediaPacketLookupError = Box<dyn std::error::Error + Send + Sync>;

/// MediaPacketLookup copies a protected media packet into dst. The caller
/// serializes access with writes to the underlying packet store.
pub type MediaPacketLookup<'a> =
    dyn Fn(u16, &mut [u8]) -> Result<usize, MediaPacketLookupError> + 'a;

/// Decoder recovers lost media packets of a single protected SSRC from a
/// FlexFEC-03 repair stream. It is not safe for concurrent use; the owning
/// buffer serializes access.
pub struct Decoder {
    logger: Logger,
    fec_ssrc: u32,
    protected_ssrc: u32,
    media_packet_buf: [u8; MAX_MEDIA_PACKET_SIZE],
    newest_media_seq: u16,
    media_packets_seen: usize,
    has_newest_media_seq: bool,
    received_fec_packets: Vec<FecPacketState>,
    stats: DecoderStats,
}

impl Decoder {
    pub fn new(fec_ssrc: u32, protected_ssrc: u32, logger: Logger) -> Self {
        Self {
            logger,
            fec_ssrc,
            protected_ssrc,
            media_packet_buf: [0; MAX_MEDIA_PACKET_SIZE],
            newest_media_seq: 0,
            media_packets_seen: 0,
            has_newest_media_seq: false,
            received_fec_packets: Vec::new(),
            stats: DecoderStats::default(),
        }
    }

    pub fn stats(&self) -> DecoderStats {
        self.stats
    }

    /// DecodeFEC ingests a packet of either the FEC stream (fecSSRC) or the
    /// protected media stream (protectedSSRC) and returns any media packets that
    /// became recoverable. Ownership of returned packets transfers to the caller.
    ///
    /// `media_packet_lookup` is what the original is given when it is made (`NewDecoder`):
    /// see the header of this file.
    pub fn decode_fec(
        &mut self,
        received_packet: &rtp::Packet,
        media_packet_lookup: Option<&MediaPacketLookup<'_>>,
    ) -> Vec<rtp::Packet> {
        if received_packet.ssrc == self.fec_ssrc {
            self.stats.fec_packets_received += 1;
            self.stats.fec_bytes_received += received_packet.payload.len() as u64;
            self.discard_old_fec_packets(received_packet.sequence_number);
            self.insert_fec_packet(received_packet, media_packet_lookup);
        } else if received_packet.ssrc == self.protected_ssrc {
            self.observe_media_packet(received_packet.sequence_number);
        } else {
            return Vec::new();
        }

        let recovered = self.attempt_recovery(media_packet_lookup);
        self.stats.packets_recovered += recovered.len() as u64;
        recovered
    }

    fn observe_media_packet(&mut self, sequence_number: u16) {
        if self.has_newest_media_seq
            && self.media_packets_seen >= MEDIA_PACKETS_BEFORE_GAP_CHECK
            && is_newer_seq(self.newest_media_seq, sequence_number)
            && seq_diff(sequence_number, self.newest_media_seq)
                > MEDIA_PACKETS_BEFORE_GAP_CHECK as u16
        {
            infow!(
                self.logger,
                "flexfec: big gap in media sequence numbers - resetting buffers"
            );
            self.received_fec_packets = Vec::new();
            self.media_packets_seen = 0;
            self.newest_media_seq = sequence_number;
        }

        if !self.has_newest_media_seq || is_newer_seq(self.newest_media_seq, sequence_number) {
            self.newest_media_seq = sequence_number;
            self.has_newest_media_seq = true;
        }
        if self.media_packets_seen < MEDIA_PACKETS_BEFORE_GAP_CHECK {
            self.media_packets_seen += 1;
        }
    }

    fn discard_old_fec_packets(&mut self, sequence_number: u16) {
        // Keep the retained sequence-number span well below half of the sequence
        // space. This keeps ordering unambiguous across wrap-around and reduces the
        // possibility of decoding against stale state.
        if !self.received_fec_packets.is_empty() {
            let mut to_remove = 0;
            for fec_pkt in &self.received_fec_packets {
                if seq_diff(sequence_number, fec_pkt.packet.sequence_number) > 0x3fff {
                    to_remove += 1;
                } else {
                    // no need to keep iterating, since receivedFECPackets is sorted
                    break;
                }
            }
            if to_remove > 0 {
                self.received_fec_packets.drain(..to_remove);
            }
        }
    }

    fn insert_fec_packet(
        &mut self,
        fec_pkt: &rtp::Packet,
        media_packet_lookup: Option<&MediaPacketLookup<'_>>,
    ) {
        for i in 0..self.received_fec_packets.len() {
            if self.received_fec_packets[i].packet.sequence_number == fec_pkt.sequence_number {
                self.stats.fec_packets_discarded += 1;
                return;
            }
        }

        let fec = match parse_flex_fec03_header(&fec_pkt.payload) {
            Ok(fec) => fec,
            Err(err) => {
                self.stats.fec_packets_discarded += 1;
                debugw!(self.logger, "flexfec: failed to parse header", "error" => err.to_string());
                return;
            }
        };

        if fec.protected_ssrc != self.protected_ssrc {
            self.stats.fec_packets_discarded += 1;
            debugw!(
                self.logger,
                "flexfec: discarding packet protecting foreign ssrc",
                "error" => Error::UnknownProtectedSsrc.to_string(),
                "expectedSSRC" => self.protected_ssrc,
                "protectedSSRC" => fec.protected_ssrc,
            );
            return;
        }

        if fec.mask0 == 0 && fec.mask1 == 0 && fec.mask2 == 0 {
            self.stats.fec_packets_discarded += 1;
            debugw!(self.logger, "flexfec: discarding packet", "error" => Error::EmptyMask.to_string());
            return;
        }

        if self.count_missing_packets(fec.clone(), &[], media_packet_lookup) == 0 {
            return;
        }

        // The caller may reuse packet memory after DecodeFEC returns. Take
        // ownership only now that this FEC state needs to be retained.
        let owned_fec_pkt = fec_pkt.clone();
        let mut owned_fec = fec;
        owned_fec.payload = owned_fec_pkt
            .payload
            .slice(fec_pkt.payload.len() - owned_fec.payload.len()..);

        let state = FecPacketState {
            packet: owned_fec_pkt,
            flex_fec: owned_fec,
        };
        let sequence_number = state.packet.sequence_number;
        self.received_fec_packets.push(state);
        let len = self.received_fec_packets.len();
        if len > 1
            && !is_newer_seq(
                self.received_fec_packets[len - 2].packet.sequence_number,
                sequence_number,
            )
        {
            let insert_at = sort_search(len - 1, |i| {
                is_newer_seq(
                    sequence_number,
                    self.received_fec_packets[i].packet.sequence_number,
                )
            });
            // copy(d.receivedFECPackets[insertAt+1:], d.receivedFECPackets[insertAt:len-1])
            // d.receivedFECPackets[insertAt] = state
            self.received_fec_packets[insert_at..].rotate_right(1);
        }

        if self.received_fec_packets.len() > MAX_FEC_PACKETS {
            self.remove_fec_packet_at(0);
        }
    }

    fn attempt_recovery(
        &mut self,
        media_packet_lookup: Option<&MediaPacketLookup<'_>>,
    ) -> Vec<rtp::Packet> {
        let mut recovered_packets: Vec<rtp::Packet> = Vec::new();
        loop {
            let mut packets_recovered = 0;
            let mut i = 0;
            while i < self.received_fec_packets.len() {
                let flex_fec = self.received_fec_packets[i].flex_fec.clone();
                let packets_missing =
                    self.count_missing_packets(flex_fec, &recovered_packets, media_packet_lookup);
                if packets_missing == 0 {
                    self.remove_fec_packet_at(i);
                    continue;
                }

                if packets_missing != 1 {
                    i += 1;
                    continue;
                }

                let recovered = match self.recover_packet(
                    i,
                    &recovered_packets,
                    media_packet_lookup,
                ) {
                    Ok(recovered) => recovered,
                    Err(err) => {
                        debugw!(self.logger, "flexfec: failed to recover packet", "error" => err.to_string());
                        self.stats.fec_packets_discarded += 1;
                        self.remove_fec_packet_at(i);
                        continue;
                    }
                };

                self.remove_fec_packet_at(i);
                recovered_packets.push(recovered);
                packets_recovered += 1;
            }

            if packets_recovered == 0 {
                break;
            }
        }

        recovered_packets
    }

    fn count_missing_packets(
        &mut self,
        fec: FlexFec,
        recovered_packets: &[rtp::Packet],
        media_packet_lookup: Option<&MediaPacketLookup<'_>>,
    ) -> usize {
        let protected_seqs = fec.protected_sequences(Vec::with_capacity(MAX_PROTECTED_PACKETS));
        let mut missing = 0;
        for &sequence_number in &protected_seqs {
            if Self::get_media_packet(
                sequence_number,
                recovered_packets,
                &mut self.media_packet_buf,
                media_packet_lookup,
            )
            .is_err()
            {
                missing += 1;
                if missing > 1 {
                    break;
                }
            }
        }

        missing
    }

    fn get_media_packet(
        sequence_number: u16,
        recovered_packets: &[rtp::Packet],
        dst: &mut [u8],
        media_packet_lookup: Option<&MediaPacketLookup<'_>>,
    ) -> Result<usize, MediaPacketLookupError> {
        for recovered_packet in recovered_packets {
            if recovered_packet.sequence_number == sequence_number {
                return Ok(recovered_packet.marshal_to(dst)?);
            }
        }
        let Some(media_packet_lookup) = media_packet_lookup else {
            return Err(Error::MediaPacketNotFound.into());
        };

        media_packet_lookup(sequence_number, dst)
    }

    fn remove_fec_packet_at(&mut self, index: usize) {
        self.received_fec_packets.remove(index);
    }

    fn recover_packet(
        &mut self,
        index: usize,
        recovered_packets: &[rtp::Packet],
        media_packet_lookup: Option<&MediaPacketLookup<'_>>,
    ) -> Result<rtp::Packet, Error> {
        let fec = &self.received_fec_packets[index];

        // https://datatracker.ietf.org/doc/html/draft-ietf-payload-flexible-fec-scheme-03#section-6.3.2

        // 2. For the repair packet in T, extract the FEC bit string as the
        //    first 80 bits of the FEC header.
        let mut header_recovery = [0u8; 12];
        header_recovery[..10].copy_from_slice(&fec.packet.payload[..10]);
        let protected_seqs = fec
            .flex_fec
            .protected_sequences(Vec::with_capacity(MAX_PROTECTED_PACKETS));

        let mut missing = 0;
        let mut sequence_number: u16 = 0;
        for &protected_seq in &protected_seqs {
            let n = match Self::get_media_packet(
                protected_seq,
                recovered_packets,
                &mut self.media_packet_buf,
                media_packet_lookup,
            ) {
                Ok(n) => n,
                Err(_) => {
                    missing += 1;
                    sequence_number = protected_seq;
                    continue;
                }
            };
            if n < 12 {
                return Err(Error::InvalidRecoveredPacketSize(format!(
                    ": protected packet length {n}"
                )));
            }

            // 1. For each source packet received in T, XOR the first 64 header
            // bits with the sequence-number field replaced by the packet length
            // after the fixed 12-byte RTP header.
            let packet = &self.media_packet_buf[..n];
            header_recovery[0] ^= packet[0];
            header_recovery[1] ^= packet[1];
            let packet_length = (n - 12) as u16; // #nosec G115 -- RTP packet size is bounded above
            header_recovery[2] ^= (packet_length >> 8) as u8;
            header_recovery[3] ^= packet_length as u8;
            for i in 4..8 {
                header_recovery[i] ^= packet[i];
            }
        }
        if missing != 1 {
            return Err(Error::CannotRecover(missing));
        }

        // set version to 2
        header_recovery[0] |= 0x80;
        header_recovery[0] &= 0xbf;
        let payload_length = u16::from_be_bytes([header_recovery[2], header_recovery[3]]);
        if payload_length as usize > fec.flex_fec.payload.len()
            || payload_length as usize + 12 > MAX_MEDIA_PACKET_SIZE
        {
            return Err(Error::InvalidRecoveredPacketSize(format!(
                ": recovered packet length {}",
                payload_length as usize + 12
            )));
        }
        header_recovery[2..4].copy_from_slice(&sequence_number.to_be_bytes());
        header_recovery[8..12].copy_from_slice(&self.protected_ssrc.to_be_bytes());

        let mut recovered_raw = vec![0u8; 12 + payload_length as usize];
        recovered_raw[..12].copy_from_slice(&header_recovery);
        copy(&mut recovered_raw[12..], &fec.flex_fec.payload);
        for &protected_seq in &protected_seqs {
            let n = match Self::get_media_packet(
                protected_seq,
                recovered_packets,
                &mut self.media_packet_buf,
                media_packet_lookup,
            ) {
                Ok(n) => n,
                Err(_) => continue,
            };
            if n < 12 {
                return Err(Error::InvalidRecoveredPacketSize(format!(
                    ": protected packet length {n}"
                )));
            }
            for i in 0..(payload_length as usize).min(n - 12) {
                recovered_raw[12 + i] ^= self.media_packet_buf[12 + i];
            }
        }

        let mut packet = rtp::Packet::default();
        if let Err(err) = packet.unmarshal(&Bytes::from(recovered_raw)) {
            return Err(Error::UnmarshalRecovered(err));
        }

        Ok(packet)
    }
}

fn append_mask_sequences(
    mut dst: Vec<u16>,
    mask: u64,
    bit_count: u16,
    seq_num_base: u16,
) -> Vec<u16> {
    for i in 0..bit_count {
        if (mask >> (bit_count - 1 - i)) & 1 == 1 {
            dst.push(seq_num_base.wrapping_add(i));
        }
    }

    dst
}

#[derive(Clone, Debug)]
struct FecPacketState {
    packet: rtp::Packet,
    flex_fec: FlexFec,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FlexFec {
    protected_ssrc: u32,
    seq_num_base: u16,
    mask0: u16,
    mask1: u32,
    mask2: u64,
    payload: Bytes,
}

impl FlexFec {
    fn protected_sequences(&self, mut dst: Vec<u16>) -> Vec<u16> {
        dst = append_mask_sequences(dst, self.mask0 as u64, FEC_MASK0_BITS, self.seq_num_base);
        if self.mask1 != 0 {
            dst = append_mask_sequences(
                dst,
                self.mask1 as u64,
                FEC_MASK1_BITS,
                self.seq_num_base.wrapping_add(FEC_MASK0_BITS),
            );
        }
        if self.mask2 != 0 {
            dst = append_mask_sequences(
                dst,
                self.mask2,
                FEC_MASK2_BITS,
                self.seq_num_base
                    .wrapping_add(FEC_MASK0_BITS)
                    .wrapping_add(FEC_MASK1_BITS),
            );
        }

        dst
    }
}

fn parse_flex_fec03_header(data: &Bytes) -> Result<FlexFec, Error> {
    if data.len() < 20 {
        return Err(Error::PacketTruncated(format!(": length {}", data.len())));
    }

    let r_bit = (data[0] & FEC_RETRANSMISSION_BIT) != 0;
    if r_bit {
        return Err(Error::RetransmissionBitSet);
    }

    let f_bit = (data[0] & FEC_INFLEXIBLE_BIT) != 0;
    if f_bit {
        return Err(Error::InflexibleGeneratorMatrix);
    }

    let ssrc_count = data[8];
    if ssrc_count != 1 {
        return Err(Error::MultipleSsrcProtection(format!(
            ": count {ssrc_count}"
        )));
    }

    let protected_ssrc = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);
    let seq_num_base = u16::from_be_bytes([data[16], data[17]]);
    let raw_packet_mask = data.slice(18..);
    let payload: Bytes;

    let k_bit0 = (raw_packet_mask[0] & FEC_MASK_K_BIT) != 0;
    let mask_part0 = u16::from_be_bytes([raw_packet_mask[0], raw_packet_mask[1]]) & FEC_MASK0_VALUE;
    let mut mask_part1: u32 = 0;
    let mut mask_part2: u64 = 0;

    if k_bit0 {
        payload = raw_packet_mask.slice(2..);
    } else {
        if data.len() < 24 {
            return Err(Error::PacketTruncated(format!(": length {}", data.len())));
        }

        let k_bit1 = (raw_packet_mask[2] & FEC_MASK_K_BIT) != 0;
        mask_part1 = u32::from_be_bytes([
            raw_packet_mask[2],
            raw_packet_mask[3],
            raw_packet_mask[4],
            raw_packet_mask[5],
        ]) & FEC_MASK1_VALUE;

        if k_bit1 {
            payload = raw_packet_mask.slice(6..);
        } else {
            if data.len() < 32 {
                return Err(Error::PacketTruncated(format!(": length {}", data.len())));
            }

            let k_bit2 = (raw_packet_mask[6] & FEC_MASK_K_BIT) != 0;
            mask_part2 = u64::from_be_bytes([
                raw_packet_mask[6],
                raw_packet_mask[7],
                raw_packet_mask[8],
                raw_packet_mask[9],
                raw_packet_mask[10],
                raw_packet_mask[11],
                raw_packet_mask[12],
                raw_packet_mask[13],
            ]) & FEC_MASK2_VALUE;

            if k_bit2 {
                payload = raw_packet_mask.slice(14..);
            } else {
                return Err(Error::LastOptionalMaskKBitSetToFalse);
            }
        }
    }

    Ok(FlexFec {
        protected_ssrc,
        seq_num_base,
        mask0: mask_part0,
        mask1: mask_part1,
        mask2: mask_part2,
        payload,
    })
}

fn seq_diff(a: u16, b: u16) -> u16 {
    a.wrapping_sub(b).min(b.wrapping_sub(a))
}

fn is_newer_seq(prev_value: u16, value: u16) -> bool {
    // half-way mark
    let breakpoint: u16 = 0x8000;
    if value.wrapping_sub(prev_value) == breakpoint {
        return value > prev_value;
    }

    value != prev_value && value.wrapping_sub(prev_value) < breakpoint
}

/// Go's builtin `copy`: copies as many bytes as both slices have and returns how many.
fn copy(dst: &mut [u8], src: &[u8]) -> usize {
    let n = dst.len().min(src.len());
    dst[..n].copy_from_slice(&src[..n]);
    n
}

/// Go's `sort.Search`: the smallest index in `[0, n)` at which `f` is true, or `n`. It is a
/// binary search that halves the range the way Go's does (the middle rounded down, the lower
/// half kept when `f` holds there), so that it also lands where the original lands when `f` is
/// not ordered, as it may be here when the sequence numbers that are kept span more than half
/// of their range.
fn sort_search(n: usize, f: impl Fn(usize) -> bool) -> usize {
    // What is known at every step: `f` is false below `i` and true from `j` on.
    let (mut i, mut j) = (0, n);
    while i < j {
        let h = (i + j) >> 1;
        if !f(h) {
            i = h + 1;
        } else {
            j = h;
        }
    }
    i
}

#[cfg(test)]
#[path = "decoder_test.rs"]
mod decoder_test;

#[cfg(test)]
#[path = "reference_test.rs"]
mod reference_test;
