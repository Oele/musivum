// Ported from LiveKit, pkg/sfu/buffer/frameintegrity.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; a `FrameEntity` does not point to the packet history
// of its checker, which the checker owns: `FrameEntity::add_packet` is given it; the counts
// are unsigned; the shifts by a count that is not a constant go through `shl`, which gives
// what Go gives for a count of 64 (zero) and for a negative one (a panic).

use crate::sfu::rtpextension::dependencydescriptor as dd;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FrameEntity {
    start_seq: u64,
    end_seq: u64,
    has_start_seq: bool,
    has_end_seq: bool,
    integrity: bool,
}

impl FrameEntity {
    pub fn add_packet(
        &mut self,
        ext_seq: u64,
        dd_val: &dd::DependencyDescriptor,
        pkt_history: &PacketHistory,
    ) {
        // duplicate packet
        if self.integrity {
            return;
        }

        if !self.has_start_seq && dd_val.first_packet_in_frame {
            (self.start_seq, self.has_start_seq) = (ext_seq, true);
        }
        if !self.has_end_seq && dd_val.last_packet_in_frame {
            (self.end_seq, self.has_end_seq) = (ext_seq, true);
        }

        if self.has_start_seq
            && self.has_end_seq
            && pkt_history.packets_consecutive(self.start_seq, self.end_seq)
        {
            self.integrity = true;
        }
    }

    pub fn reset(&mut self) {
        self.integrity = false;
        (self.has_start_seq, self.has_end_seq) = (false, false);
    }

    pub fn integrity(&self) -> bool {
        self.integrity
    }
}

// ------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PacketHistory {
    base: u64,
    last: u64,
    bits: Vec<u64>,
    packet_count: usize,
    inited: bool,
}

impl PacketHistory {
    #[allow(clippy::manual_div_ceil)] // the rounding as the original has it
    pub fn new(packet_count: usize) -> Self {
        let packet_count = (packet_count + 63) / 64 * 64;
        Self {
            base: 0,
            last: 0,
            bits: vec![0; packet_count / 64],
            packet_count,
            inited: false,
        }
    }

    pub fn add_packet(&mut self, ext_seq: u64) {
        if !self.inited {
            self.inited = true;
            self.base = ext_seq;
            // set base to extSeq-100 to avoid out-of-order packets belongs to first frame to be dropped
            if self.base > 100 {
                self.base -= 100;
            } else {
                self.base = 0;
            }
            self.last = ext_seq;
            self.set(ext_seq, true);
            return;
        }

        if ext_seq <= self.base {
            // too old
            return;
        }

        if ext_seq <= self.last {
            if self.last.wrapping_sub(ext_seq) < self.packet_count as u64 {
                self.set(ext_seq, true);
            }
            return;
        }

        // A forward jump only needs at most packetCount slots cleared; anything older aliases
        // into the same ring and would be overwritten anyway. Cap the loop so a single crafted
        // sequence number jump (up to ~32k by the extension wrap-around heuristic) cannot force
        // a large per-packet iteration count.
        let mut start = self.last.wrapping_add(1);
        if ext_seq.wrapping_sub(start) > self.packet_count as u64 {
            start = ext_seq.wrapping_sub(self.packet_count as u64);
        }
        for i in start..ext_seq {
            self.set(i, false);
        }

        self.set(ext_seq, true);
        self.last = ext_seq;
    }

    fn get_pos(&self, seq: u64) -> (usize, i64) {
        let idx = seq.wrapping_sub(self.base) % self.packet_count as u64;
        ((idx >> 6) as usize, (idx % 64) as i64)
    }

    fn set(&mut self, seq: u64, received: bool) {
        let (idx, offset) = self.get_pos(seq);
        if !received {
            self.bits[idx] &= !shl(1, offset);
        } else {
            self.bits[idx] |= shl(1, offset);
        }
    }

    pub fn packets_consecutive(&self, start: u64, end: u64) -> bool {
        if start > end {
            return false;
        }

        if end.wrapping_sub(start) >= self.packet_count as u64 {
            return false;
        }

        let (start_index, start_offset) = self.get_pos(start);
        let (end_index, end_offset) = self.get_pos(end);

        if start_index == end_index && end.wrapping_sub(start) <= 64 {
            let test_bits = shl(
                shl(1, end_offset - start_offset + 1).wrapping_sub(1),
                start_offset,
            );
            return self.bits[start_index] & test_bits == test_bits;
        }

        if (self.bits[start_index] >> start_offset).wrapping_add(1) != shl(1, 64 - start_offset) {
            return false;
        }

        let mut i = start_index + 1;
        while i != end_index {
            if i == self.bits.len() {
                i = 0;
                if i == end_index {
                    break;
                }
            }
            if self.bits[i].wrapping_add(1) != 0 {
                return false;
            }
            i += 1;
        }

        let test_bits = shl(1, end_offset + 1).wrapping_sub(1);
        self.bits[end_index] & test_bits == test_bits
    }
}

// Go's `v << count` of a `uint64` for a count that is not a constant: zero for a count of 64
// or more, where Rust would panic, and a panic for a negative count, as in Go.
fn shl(v: u64, count: i64) -> u64 {
    assert!(count >= 0, "negative shift amount");
    u32::try_from(count)
        .ok()
        .and_then(|count| v.checked_shl(count))
        .unwrap_or(0)
}

// ------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameIntegrityChecker {
    frame_count: usize,
    frames: Vec<FrameEntity>,
    base: u64,
    last: u64,

    pkt_history: PacketHistory,
    inited: bool,
}

impl FrameIntegrityChecker {
    pub fn new(frame_count: usize, packet_count: usize) -> Self {
        let mut fc = Self {
            frame_count,
            frames: vec![FrameEntity::default(); frame_count],
            base: 0,
            last: 0,
            pkt_history: PacketHistory::new(packet_count),
            inited: false,
        };

        for frame in &mut fc.frames {
            frame.reset();
        }
        fc
    }

    pub fn add_packet(
        &mut self,
        ext_seq: u64,
        ext_frame_num: u64,
        dd_val: &dd::DependencyDescriptor,
    ) {
        self.pkt_history.add_packet(ext_seq);

        if !self.inited {
            self.inited = true;
            self.base = ext_frame_num;
            self.last = ext_frame_num;
        }

        if ext_frame_num < self.base {
            // frame too old
            return;
        }

        if ext_frame_num <= self.last {
            if self.last.wrapping_sub(ext_frame_num) >= self.frame_count as u64 {
                // frame too old
                return;
            }
            self.frames
                [(ext_frame_num.wrapping_sub(self.base) as i64 % self.frame_count as i64) as usize]
                .add_packet(ext_seq, dd_val, &self.pkt_history);
            return;
        }

        // reset missing frames; cap to frameCount so a crafted frame-number jump cannot force a
        // large loop (older frames alias into the same ring and get overwritten anyway).
        let mut start = self.last.wrapping_add(1);
        if ext_frame_num.wrapping_sub(self.last) > self.frame_count as u64 {
            start = ext_frame_num
                .wrapping_sub(self.frame_count as u64)
                .wrapping_add(1);
        }
        for i in start..=ext_frame_num {
            self.frames[(i.wrapping_sub(self.base) as i64 % self.frame_count as i64) as usize]
                .reset();
        }
        self.frames
            [(ext_frame_num.wrapping_sub(self.base) as i64 % self.frame_count as i64) as usize]
            .add_packet(ext_seq, dd_val, &self.pkt_history);
        self.last = ext_frame_num;
    }

    pub fn frame_integrity(&self, ext_frame_num: u64) -> bool {
        if ext_frame_num < self.base
            || ext_frame_num > self.last
            || self.last.wrapping_sub(ext_frame_num) >= self.frame_count as u64
        {
            return false;
        }

        self.frames
            [(ext_frame_num.wrapping_sub(self.base) as i64 % self.frame_count as i64) as usize]
            .integrity()
    }
}

#[cfg(test)]
#[path = "frameintegrity_test.rs"]
mod frameintegrity_test;
