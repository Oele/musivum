// Ported from LiveKit's protocol, utils/dedupedslice.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `Get` gives the values as a slice, which is empty where
// Go's is nil.

#[derive(Clone, Debug)]
pub struct DedupedSlice<T: PartialEq> {
    max_len: usize,

    values: Vec<T>,
}

impl<T: PartialEq> DedupedSlice<T> {
    pub fn new(max_len: usize) -> Self {
        Self {
            max_len,
            values: Vec::new(),
        }
    }

    pub fn add(&mut self, val: T) -> bool {
        for v in &self.values {
            if val == *v {
                return false;
            }
        }

        self.values.push(val);
        if self.values.len() > self.max_len {
            self.values.drain(..self.values.len() - self.max_len);
        }
        true
    }

    pub fn get(&self) -> &[T] {
        &self.values
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn has(&self, val: &T) -> bool {
        for v in &self.values {
            if v == val {
                return true;
            }
        }

        false
    }

    pub fn clear(&mut self) {
        self.values = Vec::new();
    }
}

#[cfg(test)]
#[path = "dedupedslice_test.rs"]
mod dedupedslice_test;
