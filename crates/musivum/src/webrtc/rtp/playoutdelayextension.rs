// Ported from pion/rtp, playoutdelayextension.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; `errPlayoutDelayInvalidValue` is
// `Error::PlayoutDelayInvalidValue`, in the package's one enum of errors (error.rs).

use bytes::Buf;

use super::error::Error;

const PLAYOUT_DELAY_EXTENSION_SIZE: usize = 3;
const PLAYOUT_DELAY_MAX_VALUE: u16 = (1 << 12) - 1;

/// PlayoutDelayExtension is a extension payload format in
/// http://www.webrtc.org/experiments/rtp-hdrext/playout-delay
/// ```text
/// 0                   1                   2                   3
/// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |  ID   | len=2 |       MIN delay       |       MAX delay       |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlayoutDelayExtension {
    pub min_delay: u16,
    pub max_delay: u16,
}

impl PlayoutDelayExtension {
    /// MarshalSize returns the size of the PlayoutDelayExtension once marshaled.
    pub fn marshal_size(&self) -> usize {
        PLAYOUT_DELAY_EXTENSION_SIZE
    }

    /// MarshalTo marshals the extension to the given buffer.
    /// Returns io.ErrShortBuffer if buf is too small.
    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, Error> {
        if self.min_delay > PLAYOUT_DELAY_MAX_VALUE || self.max_delay > PLAYOUT_DELAY_MAX_VALUE {
            return Err(Error::PlayoutDelayInvalidValue);
        }
        if buf.len() < PLAYOUT_DELAY_EXTENSION_SIZE {
            return Err(Error::ShortBuffer);
        }
        buf[0] = (self.min_delay >> 4) as u8;
        buf[1] = ((self.min_delay << 4) as u8) | ((self.max_delay >> 8) as u8);
        buf[2] = self.max_delay as u8;

        Ok(PLAYOUT_DELAY_EXTENSION_SIZE)
    }

    /// Marshal serializes the members to buffer.
    pub fn marshal(&self) -> Result<Vec<u8>, Error> {
        if self.min_delay > PLAYOUT_DELAY_MAX_VALUE || self.max_delay > PLAYOUT_DELAY_MAX_VALUE {
            return Err(Error::PlayoutDelayInvalidValue);
        }

        Ok(vec![
            (self.min_delay >> 4) as u8,
            ((self.min_delay << 4) as u8) | ((self.max_delay >> 8) as u8),
            self.max_delay as u8,
        ])
    }

    /// Unmarshal parses the passed byte slice and stores the result in the members.
    pub fn unmarshal(&mut self, raw_data: &[u8]) -> Result<(), Error> {
        if raw_data.len() < PLAYOUT_DELAY_EXTENSION_SIZE {
            return Err(Error::TooSmall);
        }
        self.min_delay = (&raw_data[0..2]).get_u16() >> 4;
        self.max_delay = (&raw_data[1..3]).get_u16() & 0x0FFF;

        Ok(())
    }
}

#[cfg(test)]
#[path = "playoutdelayextension_test.rs"]
mod playoutdelayextension_test;
