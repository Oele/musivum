// Ported from LiveKit's mediatransportutil, pkg/red/red.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the two errors are one enum; the packets that come out
// are values, not pointers, and their payloads are slices of the `Bytes` of the packet that
// went in (as the original's alias `pkt.Payload`); `extract_primary_encoding` takes and gives
// plain slices, the one that comes out borrowed from the one that went in (`Bytes::slice_ref`
// makes a `Bytes` of it again).

use bytes::Buf;

use crate::webrtc::rtp;

const MAX_RECOVER_COUNT: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("incomplete red block header")]
    IncompleteHeader,
    #[error("incomplete red block payload")]
    IncompleteBlock,
}

/// Depacketizer extracts primary packets from a RED stream, recovering lost ones from redundant blocks.
/// The zero value is ready to use; it is not safe for concurrent use.
#[derive(Clone, Debug, Default)]
pub struct Depacketizer {
    started: bool,
    last_seq: u16,

    // bit k set means lastSeq-1-k was received
    pkt_history: u8,
}

impl Depacketizer {
    /// Depacketize returns the packets to emit for a RED packet: recovered ones oldest first, then the primary.
    pub fn depacketize(&mut self, pkt: &rtp::Packet) -> Result<Vec<rtp::Packet>, Error> {
        let mut need_recover = false;
        if !self.started {
            self.last_seq = pkt.sequence_number;
            self.pkt_history = 0;
            self.started = true;
        } else {
            let diff = pkt.sequence_number.wrapping_sub(self.last_seq);
            if diff == 0 {
                // duplicate
            } else if diff > 0x8000 {
                // unorder
                // in history
                if 65535 - diff < 8 {
                    self.pkt_history |= 1 << (65535 - diff);
                    need_recover = true;
                }
            } else if diff > 8 {
                // long jump
                self.last_seq = pkt.sequence_number;
                self.pkt_history = 0;
                need_recover = true;
            } else {
                self.last_seq = pkt.sequence_number;
                // A shift of the byte by 8 gives zero in Go.
                self.pkt_history =
                    self.pkt_history.checked_shl(diff as u32).unwrap_or(0) | 1 << (diff - 1);
                need_recover = true;
            }
        }

        let mut recover_bits: u8 = 0;
        if need_recover {
            let mut bit_index = self.last_seq.wrapping_sub(pkt.sequence_number);
            #[allow(clippy::explicit_counter_loop)] // the loop as the original has it
            for i in 0..MAX_RECOVER_COUNT {
                if bit_index > 7 {
                    break;
                }
                if self.pkt_history & (1 << bit_index) == 0 {
                    recover_bits |= 1 << i;
                }
                bit_index += 1;
            }
        }

        extract_packets(pkt, recover_bits)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Block {
    ts_offset: u32,
    length: usize,
    pt: u8,
    primary: bool,
}

/// ExtractPackets splits a RED packet into the redundant blocks flagged in recoverBits, oldest first, then the primary.
/// Bit i of recoverBits selects the block i+1 packets before the primary; payloads alias pkt.Payload.
pub fn extract_packets(pkt: &rtp::Packet, recover_bits: u8) -> Result<Vec<rtp::Packet>, Error> {
    let mut payload = pkt.payload.clone();
    let mut blocks: Vec<Block> = Vec::new();
    let mut block_length: usize = 0;
    loop {
        if payload.is_empty() {
            // need at least one byte for primary encoding
            return Err(Error::IncompleteHeader);
        }

        if payload[0] & 0x80 == 0 {
            // last block is primary encoding data
            let pt = payload[0] & 0x7F;

            blocks.push(Block {
                pt,
                primary: true,
                ..Default::default()
            });

            payload.advance(1);
            break;
        } else {
            if payload.len() < 4 {
                return Err(Error::IncompleteHeader);
            }

            // RFC 2198 §3 block header: F(1) | PT(7) | ts offset(14) | length(10)
            let mut block_head =
                u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let length = (block_head & 0x03FF) as usize;
            block_head >>= 10;
            let ts_offset = block_head & 0x3FFF;
            block_head >>= 14;
            let pt = (block_head & 0x7F) as u8;

            blocks.push(Block {
                pt,
                length,
                ts_offset,
                ..Default::default()
            });

            block_length += length;
            payload.advance(4);
        }
    }

    if payload.len() < block_length {
        return Err(Error::IncompleteBlock);
    }

    let mut pkts = Vec::with_capacity(blocks.len());
    for (i, b) in blocks.iter().enumerate() {
        if b.primary {
            let mut header = pkt.header.clone();
            header.payload_type = b.pt;
            pkts.push(rtp::Packet {
                header,
                payload,
                ..Default::default()
            });
            break;
        }

        let recover_index = blocks.len() - i - 1;
        // A shift of the byte by 8 or more gives zero in Go.
        if recover_index < 1
            || recover_bits & 1u8.checked_shl((recover_index - 1) as u32).unwrap_or(0) == 0
        {
            // skip past packet/block that does not need recovery
            payload.advance(b.length);
            continue;
        }

        let mut header = pkt.header.clone();
        header.sequence_number = header.sequence_number.wrapping_sub(recover_index as u16);
        header.timestamp = header.timestamp.wrapping_sub(b.ts_offset);
        header.payload_type = b.pt;
        pkts.push(rtp::Packet {
            header,
            payload: payload.split_to(b.length),
            ..Default::default()
        });
    }

    Ok(pkts)
}

/// ExtractPrimaryEncoding returns the primary block of a RED payload, aliasing payload.
pub fn extract_primary_encoding(mut payload: &[u8]) -> Result<&[u8], Error> {
    let mut block_length: usize = 0;
    loop {
        if payload.is_empty() {
            // need at least one byte for primary encoding
            return Err(Error::IncompleteHeader);
        }

        if payload[0] & 0x80 == 0 {
            // last block is primary encoding data
            payload = &payload[1..];
            break;
        } else {
            if payload.len() < 4 {
                return Err(Error::IncompleteHeader);
            }

            block_length += (u16::from_be_bytes([payload[2], payload[3]]) & 0x03FF) as usize;
            payload = &payload[4..];
        }
    }

    if payload.len() < block_length {
        return Err(Error::IncompleteBlock);
    }

    Ok(&payload[block_length..])
}

#[cfg(test)]
#[path = "red_test.rs"]
mod red_test;
