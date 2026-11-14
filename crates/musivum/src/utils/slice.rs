// Ported from LiveKit, pkg/utils/slice.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0. (The original
// has no copyright line of its own; this is the notice of its repository.)
// Changes: translated from Go to Rust; the slice is taken and given back, where Go sorts the
// caller's slice in place and returns the part of it that is left; `cmp.Ordered` is `Ord`
// (no floating point numbers, which are not deduplicated anywhere).

pub fn dedupe_slice<T: Ord>(mut s: Vec<T>) -> Vec<T> {
    s.sort_unstable();
    s.dedup();
    s
}

#[cfg(test)]
#[path = "slice_test.rs"]
mod slice_test;
