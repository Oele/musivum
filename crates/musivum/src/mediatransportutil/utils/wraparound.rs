// Ported from LiveKit's mediatransportutil, pkg/utils/wraparound.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the two type constraints are traits with the wrapping
// arithmetic Go's unsigned integers have; `MarshalLogObject` is `Debug`.

use std::fmt::Debug;

/// The counters that wrap: `uint16 | uint32` upstream.
pub trait Number: Copy + PartialOrd + Eq + Default + Debug {
    /// The size of the type in bits.
    const BITS: u32;
    fn wrapping_sub(self, rhs: Self) -> Self;
    fn to_u64(self) -> u64;
    /// Go's conversion `T(v)`: the low bits of `v`.
    fn from_u64(v: u64) -> Self;
}

/// What they are extended to: `uint32 | uint64` upstream.
pub trait ExtendedNumber: Copy + PartialOrd + Eq + Default + Debug {
    fn wrapping_add(self, rhs: Self) -> Self;
    fn wrapping_sub(self, rhs: Self) -> Self;
    fn wrapping_mul(self, rhs: Self) -> Self;
    fn to_u64(self) -> u64;
    /// Go's conversion `ET(v)`: the low bits of `v`.
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

macro_rules! impl_extended_number {
    ($($t:ty),*) => {$(
        impl ExtendedNumber for $t {
            fn wrapping_add(self, rhs: Self) -> Self {
                <$t>::wrapping_add(self, rhs)
            }
            fn wrapping_sub(self, rhs: Self) -> Self {
                <$t>::wrapping_sub(self, rhs)
            }
            fn wrapping_mul(self, rhs: Self) -> Self {
                <$t>::wrapping_mul(self, rhs)
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

impl_number!(u16, u32);
impl_extended_number!(u32, u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WrapAroundParams {
    pub is_restart_allowed: bool,
}

#[derive(Clone, Debug)]
pub struct WrapAround<T: Number, ET: ExtendedNumber> {
    params: WrapAroundParams,
    full_range: ET,

    initialized: bool,
    start: T,
    highest: T,
    cycles: ET,
    extended_highest: ET,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WrapAroundUpdateResult<ET: ExtendedNumber> {
    pub is_unhandled: bool, // when set, other fields are invalid
    pub is_restart: bool,
    pub pre_extended_start: ET, // valid only if IsRestart = true
    pub pre_extended_highest: ET,
    pub extended_val: ET,
}

impl<T: Number, ET: ExtendedNumber> WrapAround<T, ET> {
    pub fn new(params: WrapAroundParams) -> Self {
        Self {
            params,
            full_range: ET::from_u64(1u64 << T::BITS),

            initialized: false,
            start: T::default(),
            highest: T::default(),
            cycles: ET::default(),
            extended_highest: ET::default(),
        }
    }

    pub fn seed(&mut self, from: &WrapAround<T, ET>) {
        self.initialized = from.initialized;
        self.start = from.start;
        self.highest = from.highest;
        self.cycles = from.cycles;
        self.update_extended_highest();
    }

    pub fn update_with_order_known(
        &mut self,
        val: T,
        order_known: bool,
    ) -> WrapAroundUpdateResult<ET> {
        let mut result = WrapAroundUpdateResult::default();
        if !self.initialized {
            result.pre_extended_highest = et::<T, ET>(val).wrapping_sub(ET::from_u64(1));
            result.extended_val = et::<T, ET>(val);

            self.start = val;
            self.highest = val;
            self.update_extended_highest();
            self.initialized = true;
            return result;
        }

        if !order_known {
            let gap = val.wrapping_sub(self.highest);
            if gap > self.half_range_t() {
                // out-of-order
                return self.maybe_adjust_start(val);
            }
        }

        // in-order
        result.pre_extended_highest = self.extended_highest;

        if val < self.highest {
            self.cycles = self.cycles.wrapping_add(self.full_range);
        }
        self.highest = val;

        self.update_extended_highest();
        result.extended_val = self.extended_highest;
        result
    }

    pub fn update(&mut self, val: T) -> WrapAroundUpdateResult<ET> {
        self.update_with_order_known(val, false)
    }

    pub fn undo_update(&mut self, result: WrapAroundUpdateResult<ET>) {
        if !self.initialized
            || result.is_unhandled
            || result.pre_extended_highest >= result.extended_val
        {
            return;
        }

        self.reset_highest(result.pre_extended_highest);
    }

    pub fn rollover(&mut self, val: T, num_cycles: i32) -> WrapAroundUpdateResult<ET> {
        if num_cycles < 0 || !self.initialized {
            return self.update(val);
        }

        let mut result = WrapAroundUpdateResult {
            pre_extended_highest: self.extended_highest,
            ..Default::default()
        };

        self.cycles = self
            .cycles
            .wrapping_add(ET::from_u64(num_cycles as u64).wrapping_mul(self.full_range));
        self.highest = val;

        self.update_extended_highest();
        result.extended_val = self.extended_highest;
        result
    }

    pub fn rollback_restart(&mut self, ev: ET) {
        if self.is_wrap_back(self.start, t::<T, ET>(ev)) {
            self.cycles = self.cycles.wrapping_sub(self.full_range);
            self.update_extended_highest();
        }
        self.start = t::<T, ET>(ev);
    }

    pub fn reset_highest(&mut self, ev: ET) {
        self.highest = t::<T, ET>(ev);
        // ev & ^(w.fullRange - 1)
        self.cycles = ET::from_u64(ev.to_u64() & !(self.full_range.to_u64().wrapping_sub(1)));
        self.update_extended_highest();
    }

    pub fn get_start(&self) -> T {
        self.start
    }

    pub fn get_extended_start(&self) -> ET {
        et::<T, ET>(self.start)
    }

    pub fn get_highest(&self) -> T {
        self.highest
    }

    pub fn get_extended_highest(&self) -> ET {
        self.extended_highest
    }

    fn update_extended_highest(&mut self) {
        self.extended_highest = get_extended(self.cycles, self.highest);
    }

    fn maybe_adjust_start(&mut self, val: T) -> WrapAroundUpdateResult<ET> {
        let mut result = WrapAroundUpdateResult::default();
        // re-adjust start if necessary. The conditions are
        // 1. Not seen more than half the range yet
        // 1. wrap back compared to start and not completed a half cycle, sequences like (10, 65530) in uint16 space
        // 2. no wrap around, but out-of-order compared to start and not completed a half cycle , sequences like (10, 9), (65530, 65528) in uint16 space
        let mut cycles = self.cycles;
        let total_num = self
            .get_extended_highest()
            .wrapping_sub(self.get_extended_start())
            .wrapping_add(ET::from_u64(1));
        if total_num > self.half_range() {
            if self.is_wrap_back(val, self.highest) {
                cycles = cycles.wrapping_sub(self.full_range);
            }
            result.pre_extended_highest = self.extended_highest;
            result.extended_val = get_extended(cycles, val);
            return result;
        }

        if val.wrapping_sub(self.start) > self.half_range_t() {
            if self.params.is_restart_allowed {
                // out-of-order with existing start => a new start
                result.is_restart = true;
                if val > self.start {
                    result.pre_extended_start =
                        self.full_range.wrapping_add(et::<T, ET>(self.start));
                } else {
                    result.pre_extended_start = et::<T, ET>(self.start);
                }

                if self.is_wrap_back(val, self.highest) {
                    self.cycles = self.full_range;
                    self.update_extended_highest();
                    cycles = ET::default();
                }
                self.start = val;
            } else {
                result.is_unhandled = true;
            }
        } else if self.is_wrap_back(val, self.highest) {
            cycles = cycles.wrapping_sub(self.full_range);
        }
        result.pre_extended_highest = self.extended_highest;
        result.extended_val = get_extended(cycles, val);
        result
    }

    fn is_wrap_back(&self, earlier: T, later: T) -> bool {
        et::<T, ET>(later) < self.half_range() && et::<T, ET>(earlier) >= self.half_range()
    }

    // w.fullRange >> 1
    fn half_range(&self) -> ET {
        ET::from_u64(self.full_range.to_u64() >> 1)
    }

    // T(w.fullRange >> 1)
    fn half_range_t(&self) -> T {
        T::from_u64(self.full_range.to_u64() >> 1)
    }
}

// ------------------------------------

// Go's conversion ET(val).
fn et<T: Number, ET: ExtendedNumber>(val: T) -> ET {
    ET::from_u64(val.to_u64())
}

// Go's conversion T(ev).
fn t<T: Number, ET: ExtendedNumber>(ev: ET) -> T {
    T::from_u64(ev.to_u64())
}

fn get_extended<T: Number, ET: ExtendedNumber>(cycles: ET, val: T) -> ET {
    cycles.wrapping_add(et::<T, ET>(val))
}

#[cfg(test)]
#[path = "wraparound_test.rs"]
mod wraparound_test;
