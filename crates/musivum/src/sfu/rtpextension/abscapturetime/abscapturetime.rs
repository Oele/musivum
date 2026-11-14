// Ported from LiveKit, pkg/sfu/rtpextension/abscapturetime/abscapturetime.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are one enum; `AbsCaptureTimeFromValue`
// gives the value, not a pointer to one.

use crate::go::time::Duration;
use crate::mediatransportutil::{self, NtpTime};

pub const ABS_CAPTURE_TIME_URI: &str =
    "http://www.webrtc.org/experiments/rtp-hdrext/abs-capture-time";

// MaxSize is the marshaled size with the estimated capture clock offset
pub const MAX_SIZE: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid data")]
    InvalidData,
    #[error("buffer too small")]
    TooSmall,
}

// Reference: https://webrtc.googlesource.com/src/+/refs/heads/main/docs/native-code/rtp-hdrext/abs-capture-time/
//
// Data layout of the shortened version of abs-capture-time with a 1-byte header + 8 bytes of data:
//
//  0                   1                   2                   3
//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |  ID   | len=7 |     absolute capture timestamp (bit 0-23)     |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |             absolute capture timestamp (bit 24-55)            |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |  ... (56-63)  |
// +-+-+-+-+-+-+-+-+
//
//Data layout of the extended version of abs-capture-time with a 1-byte header + 16 bytes of data:
//
//  0                   1                   2                   3
//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |  ID   | len=15|     absolute capture timestamp (bit 0-23)     |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |             absolute capture timestamp (bit 24-55)            |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |  ... (56-63)  |   estimated capture clock offset (bit 0-23)   |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |           estimated capture clock offset (bit 24-55)          |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |  ... (56-63)  |
// +-+-+-+-+-+-+-+-+

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AbsCaptureTime {
    absolute_capture_timestamp: NtpTime,
    estimated_capture_clock_offset: i64,
}

pub fn abs_capture_time_from_value(
    absolute_capture_timestamp: u64,
    estimated_capture_clock_offset: i64,
) -> AbsCaptureTime {
    AbsCaptureTime {
        absolute_capture_timestamp: NtpTime(absolute_capture_timestamp),
        estimated_capture_clock_offset,
    }
}

impl AbsCaptureTime {
    pub fn rewrite(&mut self, offset: Duration) -> Result<(), Error> {
        if self.absolute_capture_timestamp.0 == 0 {
            return Err(Error::InvalidData);
        }

        let captured_at = self.absolute_capture_timestamp.time().add(offset);
        self.absolute_capture_timestamp = mediatransportutil::to_ntp_time(captured_at);
        self.estimated_capture_clock_offset = 0;
        Ok(())
    }

    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, Error> {
        if self.absolute_capture_timestamp.0 == 0 {
            return Err(Error::InvalidData);
        }

        let mut size = 8;
        if self.estimated_capture_clock_offset != 0 {
            size += 8;
        }
        if buf.len() < size {
            return Err(Error::TooSmall);
        }
        buf[..8].copy_from_slice(&self.absolute_capture_timestamp.0.to_be_bytes());
        if self.estimated_capture_clock_offset != 0 {
            buf[8..16].copy_from_slice(&(self.estimated_capture_clock_offset as u64).to_be_bytes());
        }
        Ok(size)
    }

    pub fn unmarshal(&mut self, marshalled: &[u8]) -> Result<(), Error> {
        if marshalled.len() < 8 {
            return Err(Error::TooSmall);
        }

        self.absolute_capture_timestamp = NtpTime(be_uint64(marshalled));
        if marshalled.len() >= 16 {
            self.estimated_capture_clock_offset = be_uint64(&marshalled[8..]) as i64;
        }
        Ok(())
    }
}

// binary.BigEndian.Uint64
fn be_uint64(b: &[u8]) -> u64 {
    u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

// New for musivum: the original has no tests.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::go::time;

    #[test]
    fn marshals_unmarshals_and_rewrites() {
        // The short form: the timestamp alone.
        let a = abs_capture_time_from_value(0x0102030405060708, 0);
        let mut buf = [0xAAu8; MAX_SIZE];
        assert_eq!(Ok(8), a.marshal_to(&mut buf));
        assert_eq!([1, 2, 3, 4, 5, 6, 7, 8], buf[..8]);
        assert_eq!([0xAA; 8], buf[8..], "nothing is written past the size");
        let mut back = AbsCaptureTime::default();
        assert_eq!(Ok(()), back.unmarshal(&buf[..8]));
        assert_eq!(a, back);

        // The extended form: with the offset of the capture clock, which may be negative.
        let a = abs_capture_time_from_value(0x0102030405060708, -2);
        let mut buf = [0u8; MAX_SIZE];
        assert_eq!(Ok(16), a.marshal_to(&mut buf));
        assert_eq!(
            [
                1, 2, 3, 4, 5, 6, 7, 8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE
            ],
            buf
        );
        let mut back = AbsCaptureTime::default();
        assert_eq!(Ok(()), back.unmarshal(&buf));
        assert_eq!(a, back);

        // What cannot be written or read.
        assert_eq!(
            Err(Error::InvalidData),
            abs_capture_time_from_value(0, 9).marshal_to(&mut buf)
        );
        assert_eq!(Err(Error::TooSmall), a.marshal_to(&mut buf[..15]));
        assert_eq!(
            Err(Error::TooSmall),
            abs_capture_time_from_value(1, 0).marshal_to(&mut buf[..7])
        );
        assert_eq!(Err(Error::TooSmall), back.unmarshal(&buf[..7]));

        // Rewriting moves the timestamp and drops the offset. The second of 2026-01-01
        // 00:00:00 UTC as an NTP time, and a half: a second and a half later is two whole
        // seconds on.
        let sec = 1_767_225_600u64 + 2_208_988_800;
        let mut a = abs_capture_time_from_value((sec << 32) | (1 << 31), 1234);
        assert_eq!(Ok(()), a.rewrite(1500 * time::MILLISECOND));
        assert_eq!(abs_capture_time_from_value((sec + 2) << 32, 0), a);
        let mut unset = AbsCaptureTime::default();
        assert_eq!(Err(Error::InvalidData), unset.rewrite(time::SECOND));
    }
}
