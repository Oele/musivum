// Ported from pion/rtp, audiolevelextension.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; `errAudioLevelOverflow` is `Error::AudioLevelOverflow`,
// in the package's one enum of errors (error.rs).

use super::error::Error;

// audioLevelExtensionSize One byte header size.
const AUDIO_LEVEL_EXTENSION_SIZE: usize = 1;

/// AudioLevelExtension is a extension payload format described in
/// https://tools.ietf.org/html/rfc6464
///
/// Implementation based on:
/// https://chromium.googlesource.com/external/webrtc/+/e2a017725570ead5946a4ca8235af27470ca0df9/webrtc/modules/rtp_rtcp/source/rtp_header_extensions.cc#49
///
/// One byte format:
/// ```text
/// 0                   1
/// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |  ID   | len=0 |V| level       |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// ```
///
/// Two byte format:
/// ```text
/// 0                   1                   2                   3
/// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |      ID       |     len=1     |V|    level    |    0 (pad)    |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AudioLevelExtension {
    pub level: u8,
    pub voice: bool,
}

impl AudioLevelExtension {
    /// MarshalSize returns the size of the AudioLevelExtension once marshaled.
    pub fn marshal_size(&self) -> usize {
        AUDIO_LEVEL_EXTENSION_SIZE
    }

    /// MarshalTo marshals the extension to the given buffer.
    /// Returns io.ErrShortBuffer if buf is too small.
    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, Error> {
        if self.level > 127 {
            return Err(Error::AudioLevelOverflow);
        }
        if buf.len() < AUDIO_LEVEL_EXTENSION_SIZE {
            return Err(Error::ShortBuffer);
        }
        let mut voice = 0x00u8;
        if self.voice {
            voice = 0x80;
        }
        buf[0] = voice | self.level;

        Ok(AUDIO_LEVEL_EXTENSION_SIZE)
    }

    /// Marshal serializes the members to buffer.
    pub fn marshal(&self) -> Result<Vec<u8>, Error> {
        if self.level > 127 {
            return Err(Error::AudioLevelOverflow);
        }
        let mut voice = 0x00u8;
        if self.voice {
            voice = 0x80;
        }
        let mut buf = vec![0; AUDIO_LEVEL_EXTENSION_SIZE];
        buf[0] = voice | self.level;

        Ok(buf)
    }

    /// Unmarshal parses the passed byte slice and stores the result in the members.
    pub fn unmarshal(&mut self, raw_data: &[u8]) -> Result<(), Error> {
        if raw_data.len() < AUDIO_LEVEL_EXTENSION_SIZE {
            return Err(Error::TooSmall);
        }
        self.level = raw_data[0] & 0x7F;
        self.voice = raw_data[0] & 0x80 != 0;

        Ok(())
    }
}

#[cfg(test)]
#[path = "audiolevelextension_test.rs"]
mod audiolevelextension_test;
