// Ported from LiveKit, pkg/sfu/utils/rangemap.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the two type constraints are traits with the wrapping
// arithmetic Go's unsigned integers have; the errors are one enum, and the one that the
// original wraps with details (`fmt.Errorf("%w, ...")`) carries those details as text;
// `MarshalLogObject` is `Debug`; the size is unsigned, so there is no negative size to raise
// to `minRanges`. The enum of the errors is the one of the package: it also has the errors of
// helpers.go.

use std::fmt;

const MIN_RANGES: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("end <= start, {0}")]
    ReversedOrder(String),
    #[error("key not found")]
    KeyNotFound,
    #[error("key too old")]
    KeyTooOld,
    #[error("key excluded")]
    KeyExcluded,

    // The errors of helpers.go. The original wraps each of the first three with what was
    // expected and what there was (`fmt.Errorf("%w, expected: ..., actual: ...")`): that is
    // the text they carry.
    #[error("invalid RTP version, {0}")]
    InvalidRtpVersion(String),
    #[error("RTP payload type mismatch, {0}")]
    RtpPayloadTypeMismatch(String),
    #[error("RTP SSRC mismatch, {0}")]
    RtpSsrcMismatch(String),
    /// pion's `webrtc.ErrCodecNotFound`, which `CodecParametersFuzzySearch` returns as it is.
    #[error("codec not found")]
    CodecNotFound,
}

/// The keys of the map: `uint32 | uint64` upstream.
pub trait RangeType: Copy + PartialOrd + Eq + Default + fmt::Debug + fmt::Display {
    /// The size of the type in bits.
    const BITS: u32;
    fn wrapping_add(self, rhs: Self) -> Self;
    fn wrapping_sub(self, rhs: Self) -> Self;
    fn to_u64(self) -> u64;
    /// Go's conversion `RT(v)`: the low bits of `v`.
    fn from_u64(v: u64) -> Self;
}

/// The values of the map: `uint32 | uint64` upstream.
pub trait ValueType: Copy + Eq + Default + fmt::Debug {
    fn wrapping_add(self, rhs: Self) -> Self;
    fn wrapping_sub(self, rhs: Self) -> Self;
    /// Go's conversion `VT(v)`: the low bits of `v`.
    fn from_u64(v: u64) -> Self;
}

macro_rules! impl_range_type {
    ($($t:ty),*) => {$(
        impl RangeType for $t {
            const BITS: u32 = <$t>::BITS;
            fn wrapping_add(self, rhs: Self) -> Self {
                <$t>::wrapping_add(self, rhs)
            }
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

macro_rules! impl_value_type {
    ($($t:ty),*) => {$(
        impl ValueType for $t {
            fn wrapping_add(self, rhs: Self) -> Self {
                <$t>::wrapping_add(self, rhs)
            }
            fn wrapping_sub(self, rhs: Self) -> Self {
                <$t>::wrapping_sub(self, rhs)
            }
            fn from_u64(v: u64) -> Self {
                v as $t
            }
        }
    )*};
}

impl_range_type!(u32, u64);
impl_value_type!(u32, u64);

// ---------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RangeVal<RT: RangeType, VT: ValueType> {
    start: RT,
    end: RT,
    value: VT,
}

// ---------------------------------------------------

#[derive(Clone)]
pub struct RangeMap<RT: RangeType, VT: ValueType> {
    half_range: RT,

    size: usize,
    ranges: Vec<RangeVal<RT, VT>>,
}

impl<RT: RangeType, VT: ValueType> fmt::Debug for RangeMap<RT, VT> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("RangeMap");
        e.field("numRanges", &self.ranges.len());

        // just the last 10 ranges max
        let start_idx = self.ranges.len().saturating_sub(10);
        for i in start_idx..self.ranges.len() {
            e.field(&format!("range[{i}]"), &self.ranges[i]);
        }

        e.finish()
    }
}

impl<RT: RangeType, VT: ValueType> RangeMap<RT, VT> {
    pub fn new(size: usize) -> Self {
        let mut r = Self {
            half_range: RT::from_u64(1u64 << (RT::BITS - 1)),
            size: size.max(MIN_RANGES),
            ranges: Vec::new(),
        };
        r.init_ranges(RT::default(), VT::default());
        r
    }

    pub fn clear_and_reset_value(&mut self, start: RT, val: VT) {
        self.init_ranges(start, val);
    }

    pub fn dec_value(&mut self, end: RT, dec: VT) {
        let last = self.ranges.len() - 1;
        let lr = &mut self.ranges[last];
        if lr.start > end {
            // modify existing value if end is in open range
            lr.value = lr.value.wrapping_sub(dec);
            return;
        }

        // close open range
        lr.end = end;

        // start a new open one with decremented value
        let value = lr.value.wrapping_sub(dec);
        self.ranges.push(RangeVal {
            start: end.wrapping_add(RT::from_u64(1)),
            end: RT::default(),
            value,
        });
        self.prune();
    }

    fn init_ranges(&mut self, start: RT, val: VT) {
        self.ranges = vec![RangeVal {
            start,
            end: RT::default(),
            value: val,
        }];
    }

    pub fn exclude_range(&mut self, start_inclusive: RT, end_exclusive: RT) -> Result<(), Error> {
        if end_exclusive == start_inclusive
            || end_exclusive.wrapping_sub(start_inclusive) > self.half_range
        {
            return Err(Error::ReversedOrder(format!(
                "start {start_inclusive}, end {end_exclusive}"
            )));
        }

        let last = self.ranges.len() - 1;
        let lr = &mut self.ranges[last];
        if lr.start > start_inclusive {
            // start of open range is after start of exclusion range, cannot close the open range
            return Err(Error::ReversedOrder(format!(
                "existingStart {}, newStart {}",
                lr.start, start_inclusive
            )));
        }

        let new_value = lr.value.wrapping_add(VT::from_u64(
            end_exclusive.wrapping_sub(start_inclusive).to_u64(),
        ));

        // if start of exclusion range matches start of open range, move the open range
        if lr.start == start_inclusive {
            lr.start = end_exclusive;
            lr.value = new_value;
            return Ok(());
        }

        // close previous range
        lr.end = start_inclusive.wrapping_sub(RT::from_u64(1));

        // start new open one after given exclusion range
        self.ranges.push(RangeVal {
            start: end_exclusive,
            end: RT::default(),
            value: new_value,
        });

        self.prune();
        Ok(())
    }

    pub fn get_value(&self, key: RT) -> Result<VT, Error> {
        let zero = RT::default();
        let num_ranges = self.ranges.len();
        if num_ranges != 0 {
            if key >= self.ranges[num_ranges - 1].start {
                // in the open range
                return Ok(self.ranges[num_ranges - 1].value);
            }

            if key < self.ranges[0].start {
                // too old
                return Err(Error::KeyTooOld);
            }
        }

        for idx in (0..num_ranges).rev() {
            let rv = &self.ranges[idx];
            if idx != num_ranges - 1 {
                // open range checked above
                if key.wrapping_sub(rv.start) < self.half_range
                    && rv.end.wrapping_sub(key) < self.half_range
                {
                    return Ok(rv.value);
                }
            }

            if idx > 0 {
                let rv_prev = &self.ranges[idx - 1];
                let before_diff = key.wrapping_sub(rv_prev.end);
                let after_diff = rv.start.wrapping_sub(key);
                if before_diff > zero
                    && before_diff < self.half_range
                    && after_diff > zero
                    && after_diff < self.half_range
                {
                    // in excluded range
                    return Err(Error::KeyExcluded);
                }
            }
        }

        Err(Error::KeyNotFound)
    }

    fn prune(&mut self) {
        if self.ranges.len() > self.size + 1 {
            // +1 to accommodate the open range
            self.ranges.drain(..self.ranges.len() - self.size - 1);
        }
    }
}

#[cfg(test)]
#[path = "rangemap_test.rs"]
mod rangemap_test;
