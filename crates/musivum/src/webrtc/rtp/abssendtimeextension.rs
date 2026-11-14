// Ported from pion/rtp, abssendtimeextension.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; `NewAbsSendTimeExtension` is
// `AbsSendTimeExtension::new` and gives the value, not a pointer to it.

use crate::go::time::Time;

use super::error::Error;

const ABS_SEND_TIME_EXTENSION_SIZE: usize = 3;

/// AbsSendTimeExtension is a extension payload format in
/// http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AbsSendTimeExtension {
    pub timestamp: u64,
}

impl AbsSendTimeExtension {
    /// MarshalSize returns the size of the AbsSendTimeExtension once marshaled.
    pub fn marshal_size(&self) -> usize {
        ABS_SEND_TIME_EXTENSION_SIZE
    }

    /// MarshalTo marshals the extension to the given buffer.
    /// Returns io.ErrShortBuffer if buf is too small.
    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, Error> {
        if buf.len() < ABS_SEND_TIME_EXTENSION_SIZE {
            return Err(Error::ShortBuffer);
        }
        buf[0] = ((self.timestamp & 0xFF0000) >> 16) as u8;
        buf[1] = ((self.timestamp & 0xFF00) >> 8) as u8;
        buf[2] = (self.timestamp & 0xFF) as u8;

        Ok(ABS_SEND_TIME_EXTENSION_SIZE)
    }

    /// Marshal serializes the members to buffer.
    pub fn marshal(&self) -> Result<Vec<u8>, Error> {
        Ok(vec![
            ((self.timestamp & 0xFF0000) >> 16) as u8,
            ((self.timestamp & 0xFF00) >> 8) as u8,
            (self.timestamp & 0xFF) as u8,
        ])
    }

    /// Unmarshal parses the passed byte slice and stores the result in the members.
    pub fn unmarshal(&mut self, raw_data: &[u8]) -> Result<(), Error> {
        if raw_data.len() < ABS_SEND_TIME_EXTENSION_SIZE {
            return Err(Error::TooSmall);
        }
        self.timestamp =
            ((raw_data[0] as u64) << 16) | ((raw_data[1] as u64) << 8) | (raw_data[2] as u64);

        Ok(())
    }

    /// Estimate absolute send time according to the receive time.
    /// Note that if the transmission delay is larger than 64 seconds, estimated time will be wrong.
    pub fn estimate(&self, receive: Time) -> Time {
        let receive_ntp = to_ntp_time(receive);
        let mut ntp = (receive_ntp & 0xFFFFFFC000000000) | ((self.timestamp & 0xFFFFFF) << 14);
        if receive_ntp < ntp {
            // Receive time must be always later than send time
            ntp = ntp.wrapping_sub(0x1000000 << 14);
        }

        to_time(ntp)
    }

    /// NewAbsSendTimeExtension makes new AbsSendTimeExtension from time.Time.
    pub fn new(send_time: Time) -> AbsSendTimeExtension {
        AbsSendTimeExtension {
            timestamp: to_ntp_time(send_time) >> 14,
        }
    }
}

pub(super) fn to_ntp_time(t: Time) -> u64 {
    let u = t.unix_nano() as u64;
    let mut s = u / 1_000_000_000;
    s = s.wrapping_add(0x83AA7E80); // offset in seconds between unix epoch and ntp epoch
    let mut f = u % 1_000_000_000;
    f <<= 32;
    f /= 1_000_000_000;
    s <<= 32;

    s | f
}

pub(super) fn to_time(t: u64) -> Time {
    let mut s = t >> 32;
    let mut f = t & 0xFFFFFFFF;
    f = f.wrapping_mul(1_000_000_000);
    f >>= 32;
    s = s.wrapping_sub(0x83AA7E80);
    let u = s.wrapping_mul(1_000_000_000).wrapping_add(f);

    Time::from_unix_nano(u as i64)
}

#[cfg(test)]
#[path = "abssendtimeextension_test.rs"]
mod abssendtimeextension_test;
