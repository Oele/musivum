// Ported from LiveKit, pkg/utils/math.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the two type constraints are traits. `OrderedNumber`
// carries the order Go's `slices.Sort` has (a NaN before every other number) and the
// arithmetic of the median, which wraps for integers as Go's does.

use std::cmp::Ordering;

// OrderedNumber defines a constraint for numeric types that can be ordered and divided.
pub trait OrderedNumber: Copy + Default {
    /// Go's `cmp.Compare`.
    fn compare(&self, other: &Self) -> Ordering;
    /// `left + (right-left)/T(2)`
    fn midpoint(left: Self, right: Self) -> Self;
}

macro_rules! impl_ordered_number_integer {
    ($($t:ty),*) => {$(
        impl OrderedNumber for $t {
            fn compare(&self, other: &Self) -> Ordering {
                self.cmp(other)
            }
            fn midpoint(left: Self, right: Self) -> Self {
                left.wrapping_add(right.wrapping_sub(left) / 2)
            }
        }
    )*};
}

macro_rules! impl_ordered_number_float {
    ($($t:ty),*) => {$(
        impl OrderedNumber for $t {
            fn compare(&self, other: &Self) -> Ordering {
                match (self.is_nan(), other.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    (false, false) => self.partial_cmp(other).unwrap_or(Ordering::Equal),
                }
            }
            fn midpoint(left: Self, right: Self) -> Self {
                left + (right - left) / 2.0
            }
        }
    )*};
}

impl_ordered_number_integer!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);
impl_ordered_number_float!(f32, f64);

// Median gets the median value for a slice without modifying the original slice.
//
// Note:
//  1. For integer types, if the slice has an even length, the division (/ 2)
//     is performed using integer division, which truncates the result towards zero.
//  2. Uses an overflow-safe formula left + (right-left)/2 to support narrow integer types.
pub fn median<T: OrderedNumber>(input: &[T]) -> T {
    let num = input.len();
    match num {
        0 => {
            return T::default();
        }
        1 => {
            return input[0];
        }
        _ => {}
    }

    // Clone the slice to avoid mutating the caller's slice
    let mut sorted_input = input.to_vec();
    sorted_input.sort_unstable_by(T::compare);

    if !num.is_multiple_of(2) {
        return sorted_input[num / 2];
    }
    let left = sorted_input[num / 2 - 1];
    let right = sorted_input[num / 2];
    T::midpoint(left, right)
}

/// The numbers `Signum` takes: `int | int8 | int16 | int32 | int64 | float32 | float64`
/// upstream.
pub trait SignedNumber: Copy + Default + PartialOrd {}

impl SignedNumber for i8 {}
impl SignedNumber for i16 {}
impl SignedNumber for i32 {}
impl SignedNumber for i64 {}
impl SignedNumber for isize {}
impl SignedNumber for f32 {}
impl SignedNumber for f64 {}

pub fn signum<T: SignedNumber>(val: T) -> i64 {
    if val < T::default() {
        -1
    } else if val > T::default() {
        1
    } else {
        0
    }
}

#[cfg(test)]
#[path = "math_test.rs"]
mod math_test;
