// Ported from pion/rtp, transportccextension.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust.

use bytes::{Buf, BufMut};

use super::error::Error;

// transport-wide sequence.
const TRANSPORT_CC_EXTENSION_SIZE: usize = 2;

/// TransportCCExtension is a extension payload format in
/// https://tools.ietf.org/html/draft-holmer-rmcat-transport-wide-cc-extensions-01
/// ```text
/// 0                   1                   2                   3
/// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |       0xBE    |    0xDE       |           length=1            |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |  ID   | L=1   |transport-wide sequence number | zero padding  |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportCcExtension {
    pub transport_sequence: u16,
}

impl TransportCcExtension {
    /// MarshalSize returns the size of the TransportCCExtension once marshaled.
    pub fn marshal_size(&self) -> usize {
        TRANSPORT_CC_EXTENSION_SIZE
    }

    /// MarshalTo marshals the extension to the given buffer.
    /// Returns io.ErrShortBuffer if buf is too small.
    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, Error> {
        if buf.len() < TRANSPORT_CC_EXTENSION_SIZE {
            return Err(Error::ShortBuffer);
        }
        (&mut buf[0..2]).put_u16(self.transport_sequence);

        Ok(TRANSPORT_CC_EXTENSION_SIZE)
    }

    /// Marshal serializes the members to buffer.
    pub fn marshal(&self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0; TRANSPORT_CC_EXTENSION_SIZE];
        (&mut buf[0..2]).put_u16(self.transport_sequence);

        Ok(buf)
    }

    /// Unmarshal parses the passed byte slice and stores the result in the members.
    pub fn unmarshal(&mut self, raw_data: &[u8]) -> Result<(), Error> {
        if raw_data.len() < TRANSPORT_CC_EXTENSION_SIZE {
            return Err(Error::TooSmall);
        }
        self.transport_sequence = (&raw_data[0..2]).get_u16();

        Ok(())
    }
}

#[cfg(test)]
#[path = "transportccextension_test.rs"]
mod transportccextension_test;
