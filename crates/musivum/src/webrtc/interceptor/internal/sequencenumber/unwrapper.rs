// Ported from pion/interceptor, internal/sequencenumber/unwrapper.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust.

const MAX_SEQUENCE_NUMBER_PLUS_ONE: i64 = 65536;
const BREAKPOINT: u16 = 32768; // half of max uint16

/// Unwrapper stores an unwrapped sequence number.
#[derive(Clone, Debug, Default)]
pub struct Unwrapper {
    init: bool,
    last_unwrapped: i64,
}

fn is_newer(value: u16, previous: u16) -> bool {
    if value.wrapping_sub(previous) == BREAKPOINT {
        return value > previous;
    }

    value != previous && value.wrapping_sub(previous) < BREAKPOINT
}

impl Unwrapper {
    /// Unwrap unwraps the next sequencenumber.
    pub fn unwrap(&mut self, i: u16) -> i64 {
        if !self.init {
            self.init = true;
            self.last_unwrapped = i as i64;

            return self.last_unwrapped;
        }

        let last_wrapped = self.last_unwrapped as u16;
        let mut delta = i.wrapping_sub(last_wrapped) as i64;
        if is_newer(i, last_wrapped) {
            if delta < 0 {
                delta += MAX_SEQUENCE_NUMBER_PLUS_ONE;
            }
        } else if delta > 0 && self.last_unwrapped + delta - MAX_SEQUENCE_NUMBER_PLUS_ONE >= 0 {
            delta -= MAX_SEQUENCE_NUMBER_PLUS_ONE;
        }

        self.last_unwrapped += delta;

        self.last_unwrapped
    }
}

#[cfg(test)]
#[path = "unwrapper_test.rs"]
mod unwrapper_test;
