// Ported from LiveKit, pkg/sfu/rtpextension/playoutdelay/playoutdelay.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the two constants of delays, which are untyped in the
// original, have the type of the delays (`u16`); the errors are one enum.

pub const PLAYOUT_DELAY_URI: &str = "http://www.webrtc.org/experiments/rtp-hdrext/playout-delay";
pub const MAX_PLAYOUT_DELAY_DEFAULT: u16 = 10000; // 10s, equal to chrome's default max playout delay
pub const PLAYOUT_DELAY_MAX_VALUE: u16 = 10 * ((1 << 12) - 1); // max value for playout delay can be represented

const PLAYOUT_DELAY_EXTENSION_SIZE: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("playout delay overflow")]
    PlayoutDelayOverflow,
    #[error("buffer too small")]
    TooSmall,
}

//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |  ID   | len=2 |       MIN delay       |       MAX delay       |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// The wired MIN/MAX delay is in 10ms unit

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlayOutDelay {
    // delay in ms
    pub min: u16,
    pub max: u16,
}

pub fn playout_delay_from_value(mut min: u16, mut max: u16) -> PlayOutDelay {
    if min > PLAYOUT_DELAY_MAX_VALUE {
        min = PLAYOUT_DELAY_MAX_VALUE;
    }
    if max > PLAYOUT_DELAY_MAX_VALUE {
        max = PLAYOUT_DELAY_MAX_VALUE;
    }
    PlayOutDelay { min, max }
}

impl PlayOutDelay {
    pub fn marshal(&self) -> Result<Vec<u8>, Error> {
        let (min, max) = (self.min / 10, self.max / 10);
        if min >= 1 << 12 || max >= 1 << 12 {
            return Err(Error::PlayoutDelayOverflow);
        }

        Ok(vec![
            (min >> 4) as u8,
            ((min << 4) as u8) | ((max >> 8) as u8),
            max as u8,
        ])
    }

    pub fn unmarshal(&mut self, raw_data: &[u8]) -> Result<(), Error> {
        if raw_data.len() < PLAYOUT_DELAY_EXTENSION_SIZE {
            return Err(Error::TooSmall);
        }

        self.min = (u16::from_be_bytes([raw_data[0], raw_data[1]]) >> 4) * 10;
        self.max = (u16::from_be_bytes([raw_data[1], raw_data[2]]) & 0x0FFF) * 10;
        Ok(())
    }
}

#[cfg(test)]
#[path = "playoutdelay_test.rs"]
mod playoutdelay_test;
