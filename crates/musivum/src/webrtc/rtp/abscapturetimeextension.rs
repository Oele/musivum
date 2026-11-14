// Ported from pion/rtp, abscapturetimeextension.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; `NewAbsCaptureTimeExtension` is
// `AbsCaptureTimeExtension::new` and `NewAbsCaptureTimeExtensionWithCaptureClockOffset` is
// `AbsCaptureTimeExtension::new_with_capture_clock_offset`, and they give the value, not a
// pointer to it; the two pointers that may be nil (`*int64`, `*time.Duration`) are `Option`s.

use bytes::{Buf, BufMut};

use crate::go::time::{self, Duration, Time};

use super::abssendtimeextension::{to_ntp_time, to_time};
use super::error::Error;

const ABS_CAPTURE_TIME_EXTENSION_SIZE: usize = 8;
const ABS_CAPTURE_TIME_EXTENDED_EXTENSION_SIZE: usize = 16;

/// AbsCaptureTimeExtension is a extension payload format in.
/// http://www.webrtc.org/experiments/rtp-hdrext/abs-capture-time
/// ```text
/// 0                   1                   2                   3
/// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |  ID   | len=7 |     absolute capture timestamp (bit 0-23)     |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |             absolute capture timestamp (bit 24-55)            |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |  ... (56-63)  |
/// +-+-+-+-+-+-+-+-+
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AbsCaptureTimeExtension {
    pub timestamp: u64,
    pub estimated_capture_clock_offset: Option<i64>,
}

impl AbsCaptureTimeExtension {
    /// MarshalSize returns the size of the AbsCaptureTimeExtension once marshaled.
    pub fn marshal_size(&self) -> usize {
        if self.estimated_capture_clock_offset.is_some() {
            return ABS_CAPTURE_TIME_EXTENDED_EXTENSION_SIZE;
        }

        ABS_CAPTURE_TIME_EXTENSION_SIZE
    }

    /// MarshalTo marshals the extension to the given buffer.
    /// Returns io.ErrShortBuffer if buf is too small.
    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, Error> {
        if let Some(estimated_capture_clock_offset) = self.estimated_capture_clock_offset {
            if buf.len() < ABS_CAPTURE_TIME_EXTENDED_EXTENSION_SIZE {
                return Err(Error::ShortBuffer);
            }
            (&mut buf[0..8]).put_u64(self.timestamp);
            (&mut buf[8..16]).put_u64(estimated_capture_clock_offset as u64);

            return Ok(ABS_CAPTURE_TIME_EXTENDED_EXTENSION_SIZE);
        }
        if buf.len() < ABS_CAPTURE_TIME_EXTENSION_SIZE {
            return Err(Error::ShortBuffer);
        }
        (&mut buf[0..8]).put_u64(self.timestamp);

        Ok(ABS_CAPTURE_TIME_EXTENSION_SIZE)
    }

    /// Marshal serializes the members to buffer.
    pub fn marshal(&self) -> Result<Vec<u8>, Error> {
        if let Some(estimated_capture_clock_offset) = self.estimated_capture_clock_offset {
            let mut buf = vec![0; ABS_CAPTURE_TIME_EXTENDED_EXTENSION_SIZE];
            (&mut buf[0..8]).put_u64(self.timestamp);
            (&mut buf[8..16]).put_u64(estimated_capture_clock_offset as u64);

            return Ok(buf);
        }
        let mut buf = vec![0; ABS_CAPTURE_TIME_EXTENSION_SIZE];
        (&mut buf[0..8]).put_u64(self.timestamp);

        Ok(buf)
    }

    /// Unmarshal parses the passed byte slice and stores the result in the members.
    pub fn unmarshal(&mut self, raw_data: &[u8]) -> Result<(), Error> {
        if raw_data.len() < ABS_CAPTURE_TIME_EXTENSION_SIZE {
            return Err(Error::TooSmall);
        }
        self.timestamp = (&raw_data[0..8]).get_u64();
        if raw_data.len() >= ABS_CAPTURE_TIME_EXTENDED_EXTENSION_SIZE {
            let offset = (&raw_data[8..16]).get_u64() as i64;
            self.estimated_capture_clock_offset = Some(offset);
        }

        Ok(())
    }

    /// CaptureTime produces the estimated time.Time represented by this extension.
    pub fn capture_time(&self) -> Time {
        to_time(self.timestamp)
    }

    /// EstimatedCaptureClockOffsetDuration produces the estimated time.Duration represented by this extension.
    pub fn estimated_capture_clock_offset_duration(&self) -> Option<Duration> {
        let mut offset = self.estimated_capture_clock_offset?;
        let mut negative = false;
        if offset < 0 {
            offset = offset.wrapping_neg();
            negative = true;
        }
        let mut duration = (offset / (1 << 32)) * time::SECOND
            + ((offset & 0xFFFFFFFF).wrapping_mul(1_000_000_000) / (1 << 32)) * time::NANOSECOND;
        if negative {
            duration = -duration;
        }

        Some(duration)
    }

    /// NewAbsCaptureTimeExtension makes new AbsCaptureTimeExtension from time.Time.
    pub fn new(capture_time: Time) -> AbsCaptureTimeExtension {
        AbsCaptureTimeExtension {
            timestamp: to_ntp_time(capture_time),
            ..Default::default()
        }
    }

    /// NewAbsCaptureTimeExtensionWithCaptureClockOffset makes new AbsCaptureTimeExtension from time.Time and a clock offset.
    pub fn new_with_capture_clock_offset(
        capture_time: Time,
        capture_clock_offset: Duration,
    ) -> AbsCaptureTimeExtension {
        let mut ns = capture_clock_offset.nanoseconds();
        let mut negative = false;
        if ns < 0 {
            ns = ns.wrapping_neg();
            negative = true;
        }
        let lsb = (ns / 1_000_000_000) & 0xFFFFFFFF;
        let msb = (((ns % 1_000_000_000).wrapping_mul(1 << 32)) / 1_000_000_000) & 0xFFFFFFFF;
        let mut offset = (lsb << 32) | msb;
        if negative {
            offset = offset.wrapping_neg();
        }

        AbsCaptureTimeExtension {
            timestamp: to_ntp_time(capture_time),
            estimated_capture_clock_offset: Some(offset),
        }
    }
}

#[cfg(test)]
#[path = "abscapturetimeextension_test.rs"]
mod abscapturetimeextension_test;
