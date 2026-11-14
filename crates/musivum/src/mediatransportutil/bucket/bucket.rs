// Ported from LiveKit's mediatransportutil, pkg/bucket/bucket.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the type constraint is a trait that carries the size of
// the type, where the original asks `unsafe.Sizeof`; `NewBucket` returning nil is `Bucket::new`
// returning `None`, and `GetPacket` on a nil bucket (`ErrNoBucket`) is for the holder of an
// `Option<Bucket>` to answer; the slots are parts split off one `BytesMut`, where the original
// slices one buffer; `get_packet` copies into a `&mut [u8]`, which has no capacity beyond its
// length; the stored packet that the adding functions return borrows the bucket; `addPacket` is
// `add_packet_inner`; in `push` the step moves before the packet is stored, because of that
// borrow.

use std::fmt::{Debug, Display};
use std::marker::PhantomData;

use bytes::BytesMut;

use super::errors::Error;

const PKT_SIZE_LIMIT: usize = 256 * 1024;

pub const RTP_MAX_PKT_SIZE: usize = 1500;
pub const RTP_SEQ_NUM_OFFSET: usize = 2;

/// The sequence numbers a bucket takes: `uint16 | uint32 | uint64` upstream.
pub trait Number: Copy + PartialOrd + Eq + Default + Debug + Display {
    /// The size of the type in bits.
    const BITS: u32;
    fn wrapping_sub(self, rhs: Self) -> Self;
    fn to_u64(self) -> u64;
    /// Go's conversion `T(v)`: the low bits of `v`.
    fn from_u64(v: u64) -> Self;
}

macro_rules! impl_number {
    ($($t:ty),*) => {$(
        impl Number for $t {
            const BITS: u32 = <$t>::BITS;
            fn wrapping_sub(self, rhs: Self) -> Self {
                <$t>::wrapping_sub(self, rhs)
            }
            fn to_u64(self) -> u64 {
                self as u64
            }
            fn from_u64(v: u64) -> Self {
                v as $t
            }
        }
    )*};
}

impl_number!(u16, u32, u64);

type PutFn = fn(buf: &mut [u8], value: u64);
type GetFn = fn(buf: &[u8]) -> u64;

pub struct Bucket<ET: Number, T: Number> {
    max_pkt_size: usize,
    seq_num_offset: usize,

    pkt_size_header: usize,
    invalid_pkt_size: u64,
    put_size: PutFn,
    get_size: GetFn,
    put_seq_num: PutFn,
    get_seq_num: GetFn,

    half_range: ET,
    slots: Vec<BytesMut>,
    init: bool,
    resync_on_next_packet: bool,
    step: usize,
    head_sn: ET,
    init_capacity: usize,
    max_steps: usize,

    // T is the width of the sequence number inside a packet: it is in the methods only.
    _t: PhantomData<T>,
}

impl<ET: Number, T: Number> Bucket<ET, T> {
    pub fn new(capacity: usize, max_pkt_size: usize, seq_num_offset: usize) -> Option<Self> {
        if max_pkt_size > PKT_SIZE_LIMIT {
            return None;
        }

        let (pkt_size_header, invalid_pkt_size, put_size, get_size): (usize, u64, PutFn, GetFn) =
            if max_pkt_size < 256 {
                (1, 0xff, put8, get8)
            } else if max_pkt_size < 65536 {
                (2, 0xffff, put16, get16)
            } else {
                (4, 0xffffffff, put32, get32)
            };

        // unsafe.Sizeof(t)
        let (put_seq_num, get_seq_num): (PutFn, GetFn) = match T::BITS / 8 {
            1 => (put8, get8),

            2 => (put16, get16),

            4 => (put32, get32),

            _ => (put64, get64),
        };

        let mut b = Self {
            max_pkt_size,
            seq_num_offset,

            pkt_size_header,
            invalid_pkt_size,
            put_size,
            get_size,
            put_seq_num,
            get_seq_num,

            // 1 << ((unsafe.Sizeof(et) * 8) - 1)
            half_range: ET::from_u64(1 << (ET::BITS - 1)),
            slots: Vec::new(),
            init: false,
            resync_on_next_packet: false,
            step: 0,
            head_sn: ET::default(),
            init_capacity: capacity,
            max_steps: capacity,

            _t: PhantomData,
        };

        b.slots = b.create_slots();
        Some(b)
    }

    // Grow increases the capacity of the bucket by adding initial capacity to the buffer
    pub fn grow(&mut self) -> usize {
        let new_slots = self.create_slots();
        // The original appends to a slice of its own (`growedSlots`) and reads the old one in
        // the loop. No slot is read after it was moved, so the slots can grow in place.
        self.slots.extend(new_slots);
        // move wrapped slots to new slots
        for i in (self.step..self.max_steps).rev() {
            if (self.get_size)(&self.slots[i]) != self.invalid_pkt_size {
                self.slots.swap(i + self.init_capacity, i);
            }
        }
        self.max_steps += self.init_capacity;
        self.max_steps
    }

    pub fn resync_on_next_packet(&mut self) {
        self.resync_on_next_packet = true;
    }

    pub fn head_sequence_number(&self) -> ET {
        self.head_sn
    }

    pub fn capacity(&self) -> usize {
        self.max_steps
    }

    fn add_packet_inner(&mut self, pkt: &[u8], sn: ET) -> Result<&mut [u8], Error> {
        // len(pkt) > b.maxPktSize-b.pktSizeHeader
        if pkt.len() as i64 > self.max_pkt_size as i64 - self.pkt_size_header as i64 {
            return Err(Error::PacketTooLarge);
        }

        if !self.init {
            self.head_sn = sn.wrapping_sub(ET::from_u64(1));
            self.init = true;
        }

        if self.resync_on_next_packet {
            self.resync_on_next_packet = false;

            self.head_sn = sn.wrapping_sub(ET::from_u64(1));
            self.invalidate(0, self.max_steps as i64);
        }

        let diff = sn.wrapping_sub(self.head_sn);
        if diff == ET::from_u64(0) || diff > self.half_range {
            // duplicate of last packet or out-of-order
            return self.set(sn, self.head_sn.wrapping_sub(sn).to_u64() as i64, pkt);
        }

        self.push(sn, (diff.to_u64() as i64).wrapping_sub(1), pkt)
    }

    pub fn add_packet(&mut self, pkt: &[u8]) -> Result<&mut [u8], Error> {
        let sn = ET::from_u64((self.get_seq_num)(&pkt[self.seq_num_offset..]));
        self.add_packet_inner(pkt, sn)
    }

    pub fn add_packet_with_sequence_number(
        &mut self,
        pkt: &[u8],
        sn: ET,
    ) -> Result<&mut [u8], Error> {
        let put_seq_num = self.put_seq_num;
        let seq_num_offset = self.seq_num_offset;
        let stored_pkt = self.add_packet_inner(pkt, sn)?;

        // overwrite sequence number in packet
        put_seq_num(&mut stored_pkt[seq_num_offset..], sn.to_u64());
        Ok(stored_pkt)
    }

    pub fn get_packet(&self, buf: &mut [u8], sn: ET) -> Result<usize, Error> {
        let mut diff = self.head_sn.wrapping_sub(sn);
        let p = if diff > self.half_range {
            diff = sn.wrapping_sub(self.head_sn);
            self.get(sn, (diff.to_u64() as i64).wrapping_neg())?
        } else {
            self.get(sn, diff.to_u64() as i64)?
        };
        let n = p.len();
        // cap(buf) < n
        if buf.len() < n {
            return Err(Error::BufferTooSmall);
        }
        buf[..n].copy_from_slice(p);
        Ok(n)
    }

    fn push(&mut self, sn: ET, diff: i64, pkt: &[u8]) -> Result<&mut [u8], Error> {
        self.head_sn = sn;

        // invalidate slots if there is a gap in the sequence number
        self.invalidate(self.step, diff);

        // store headSN packet
        let idx = self.wrap((self.step as i64).wrapping_add(diff));

        // for next packet
        self.step = self.wrap((self.step as i64).wrapping_add(diff).wrapping_add(1));

        Ok(self.store(idx, pkt))
    }

    fn get(&self, sn: ET, diff: i64) -> Result<&[u8], Error> {
        if diff < 0 {
            // asking for something ahead of headSN
            return Err(Error::PacketTooNew(format!(
                ", headSN {}, sn {}",
                self.head_sn, sn
            )));
        }
        if diff >= self.max_steps as i64 {
            // too old
            return Err(Error::PacketTooOld(format!(
                ", headSN {}, sn {}",
                self.head_sn, sn
            )));
        }

        let idx = self.wrap((self.step as i64).wrapping_sub(diff).wrapping_sub(1));
        let slot = &self.slots[idx];
        let sz = (self.get_size)(slot);
        if sz == self.invalid_pkt_size {
            return Err(Error::PacketSizeInvalid(format!(
                ", headSN {}, sn {}, size {}",
                self.head_sn, sn, sz
            )));
        }

        let cache_sn = (self.get_seq_num)(&slot[self.pkt_size_header + self.seq_num_offset..]);
        if T::from_u64(cache_sn) != T::from_u64(sn.to_u64()) {
            return Err(Error::PacketMismatch(format!(
                ", headSN {}, sn {}, cacheSN {}",
                self.head_sn, sn, cache_sn
            )));
        }

        Ok(&slot[self.pkt_size_header..self.pkt_size_header + sz as usize])
    }

    fn set(&mut self, sn: ET, diff: i64, pkt: &[u8]) -> Result<&mut [u8], Error> {
        if diff >= self.max_steps as i64 {
            return Err(Error::PacketTooOld(format!(
                ", headSN {}, sn {}",
                self.head_sn, sn
            )));
        }

        let idx = self.wrap((self.step as i64).wrapping_sub(diff).wrapping_sub(1));
        let slot = &self.slots[idx];
        let size = (self.get_size)(slot);
        if size != self.invalid_pkt_size {
            // Do not overwrite if duplicate
            if size as i64 != pkt.len() as i64 {
                return Err(Error::RtxPacketSize(format!(
                    ", incorrect RTX size, expected {}, actual {}",
                    size,
                    pkt.len()
                )));
            }

            let stored_sn = (self.get_seq_num)(&slot[self.pkt_size_header + self.seq_num_offset..]);
            if T::from_u64(stored_sn) == T::from_u64(sn.to_u64()) {
                return Err(Error::RtxPacket);
            }

            // packet is valid, but has a different sequence number
            return Err(Error::PacketMismatch(format!(
                ", expected {}, actual: {}",
                stored_sn, sn
            )));
        }

        Ok(self.store(idx, pkt))
    }

    fn store(&mut self, idx: usize, pkt: &[u8]) -> &mut [u8] {
        // store packet size
        let slot = &mut self.slots[idx];
        (self.put_size)(slot, pkt.len() as u64);

        // store packet
        slot[self.pkt_size_header..self.pkt_size_header + pkt.len()].copy_from_slice(pkt);

        &mut slot[self.pkt_size_header..self.pkt_size_header + pkt.len()]
    }

    fn wrap(&self, mut slot: i64) -> usize {
        let max_steps = self.max_steps as i64;
        while slot < 0 {
            slot += max_steps;
        }

        while slot >= max_steps {
            slot -= max_steps;
        }

        slot as usize
    }

    fn invalidate(&mut self, start_slot: usize, mut num_slots: i64) {
        if num_slots > self.max_steps as i64 {
            num_slots = self.max_steps as i64;
        }

        for i in 0..num_slots {
            let idx = self.wrap(start_slot as i64 + i);
            (self.put_size)(&mut self.slots[idx], self.invalid_pkt_size);
        }
    }

    fn create_slots(&self) -> Vec<BytesMut> {
        let pkt_size = self.max_pkt_size + self.pkt_size_header;
        let mut buf = BytesMut::zeroed(pkt_size * self.init_capacity);
        let mut slots = Vec::with_capacity(self.init_capacity);
        for _ in 0..self.init_capacity {
            // slots[i] = buf[i*pktSize : (i+1)*pktSize]
            let mut slot = buf.split_to(pkt_size);
            (self.put_size)(&mut slot, self.invalid_pkt_size);
            slots.push(slot);
        }
        slots
    }
}

// -------------------------------------------------------------

fn put8(buf: &mut [u8], value: u64) {
    buf[0] = value as u8;
}

fn get8(buf: &[u8]) -> u64 {
    buf[0] as u64
}

fn put16(buf: &mut [u8], value: u64) {
    buf[..2].copy_from_slice(&(value as u16).to_be_bytes());
}

fn get16(buf: &[u8]) -> u64 {
    u16::from_be_bytes([buf[0], buf[1]]) as u64
}

fn put32(buf: &mut [u8], value: u64) {
    buf[..4].copy_from_slice(&(value as u32).to_be_bytes());
}

fn get32(buf: &[u8]) -> u64 {
    u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64
}

fn put64(buf: &mut [u8], value: u64) {
    buf[..8].copy_from_slice(&value.to_be_bytes());
}

fn get64(buf: &[u8]) -> u64 {
    u64::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ])
}

#[cfg(test)]
#[path = "bucket_test.rs"]
mod bucket_test;
