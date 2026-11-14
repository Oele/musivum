// Ported from pion/rtp, packet.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The payload and the payloads of the extensions are
// `Bytes`: `unmarshal` takes the buffer as `&Bytes` and keeps slices of it, as the original
// keeps slices of the buffer it is given, and what is kept cannot be written to afterwards.
// `Packet` embeds `Header` in Go: here it holds one and dereferences to it, so that
// `packet.sequence_number` and `packet.get_extension(id)` read the same. `String()` is
// `Display`. `Clone()` of `Packet` and of `Header` is the derived `Clone` (the bytes are shared,
// not copied; unlike the original's, a packet's clone keeps `raw`). An `unmarshal` that fails
// gives the error only, not also the number of bytes read up to there. `get_extension` gives
// `None` where the original gives nil, and `get_extension_ids` an empty list.

use std::fmt;
use std::ops::{Deref, DerefMut};

use bytes::{Buf, BufMut, Bytes};

use super::error::Error;
use super::header_extension::header_extension_check;

/// Extension RTP Header extension.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Extension {
    id: u8,
    payload: Bytes,
}

/// Header represents an RTP packet header.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Header {
    pub version: u8,
    pub padding: bool,
    pub extension: bool,
    pub marker: bool,
    pub payload_type: u8,
    pub sequence_number: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    pub csrc: Vec<u32>,
    pub extension_profile: u16,
    pub extensions: Vec<Extension>,

    /// PaddingLength is the length of the padding in bytes. It is not part of the RTP header
    /// (it is sent in the last byte of RTP packet padding), but logically it belongs here.
    pub padding_size: u8,

    /// Deprecated: will be removed in a future version.
    pub payload_offset: usize,
}

/// Packet represents an RTP Packet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Packet {
    pub header: Header,
    pub payload: Bytes,

    /// Deprecated: will be removed in a future version. Use Header.PaddingSize instead.
    pub padding_size: u8,

    /// Deprecated: will be removed in a future version.
    pub raw: Bytes,
    // Please do not add any new field directly to Packet struct unless you know that it is safe.
    // pion internally passes Header and Payload separately, what causes bugs like
    // https://github.com/pion/webrtc/issues/2403 .
}

impl Deref for Packet {
    type Target = Header;
    fn deref(&self) -> &Header {
        &self.header
    }
}

impl DerefMut for Packet {
    fn deref_mut(&mut self) -> &mut Header {
        &mut self.header
    }
}

/// ExtensionProfileOneByte is the RTP One Byte Header Extension Profile, defined in RFC 8285.
pub const EXTENSION_PROFILE_ONE_BYTE: u16 = 0xBEDE;
/// ExtensionProfileTwoByte is the RTP Two Byte Header Extension Profile, defined in RFC 8285.
pub const EXTENSION_PROFILE_TWO_BYTE: u16 = 0x1000;
/// CryptexProfileOneByte is the Cryptex One Byte Header Extension Profile, defined in RFC 9335.
pub const CRYPTEX_PROFILE_ONE_BYTE: u16 = 0xC0DE;
/// CryptexProfileTwoByte is the Cryptex Two Byte Header Extension Profile, defined in RFC 9335.
pub const CRYPTEX_PROFILE_TWO_BYTE: u16 = 0xC2DE;

const HEADER_LENGTH: usize = 4;
const VERSION_SHIFT: u8 = 6;
const VERSION_MASK: u8 = 0x3;
const PADDING_SHIFT: u8 = 5;
const PADDING_MASK: u8 = 0x1;
const EXTENSION_SHIFT: u8 = 4;
const EXTENSION_MASK: u8 = 0x1;
const EXTENSION_ID_RESERVED: u8 = 0xF;
const EXTENSION_ID_PADDING: u8 = 0x0;
const CC_MASK: u8 = 0xF;
const MARKER_SHIFT: u8 = 7;
const MARKER_MASK: u8 = 0x1;
const PT_MASK: u8 = 0x7F;
const SEQ_NUM_OFFSET: usize = 2;
const SEQ_NUM_LENGTH: usize = 2;
const TIMESTAMP_OFFSET: usize = 4;
const TIMESTAMP_LENGTH: usize = 4;
const SSRC_OFFSET: usize = 8;
const SSRC_LENGTH: usize = 4;
const CSRC_OFFSET: usize = 12;
const CSRC_LENGTH: usize = 4;

/// String helps with debugging by printing packet information in a readable way.
impl fmt::Display for Packet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = String::from("RTP PACKET:\n");

        out += &format!("\tVersion: {}\n", self.version);
        out += &format!("\tMarker: {}\n", self.marker);
        out += &format!("\tPayload Type: {}\n", self.payload_type);
        out += &format!("\tSequence Number: {}\n", self.sequence_number);
        out += &format!("\tTimestamp: {}\n", self.timestamp);
        out += &format!("\tSSRC: {} ({:x})\n", self.ssrc, self.ssrc);
        out += &format!("\tPayload Length: {}\n", self.payload.len());

        f.write_str(&out)
    }
}

impl Header {
    /// Unmarshal parses the passed byte slice and stores the result in the Header.
    /// It returns the number of bytes read n and any error.
    pub fn unmarshal(&mut self, buf: &Bytes) -> Result<usize, Error> {
        if buf.len() < HEADER_LENGTH {
            return Err(Error::HeaderSizeInsufficient {
                before: String::new(),
                after: format!(": {} < {}", buf.len(), HEADER_LENGTH),
            });
        }

        /*
         *  0                   1                   2                   3
         *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * |V=2|P|X|  CC   |M|     PT      |       sequence number         |
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * |                           timestamp                           |
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * |           synchronization source (SSRC) identifier            |
         * +=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
         * |            contributing source (CSRC) identifiers             |
         * |                             ....                              |
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         */

        self.version = (buf[0] >> VERSION_SHIFT) & VERSION_MASK;
        self.padding = ((buf[0] >> PADDING_SHIFT) & PADDING_MASK) > 0;
        self.extension = ((buf[0] >> EXTENSION_SHIFT) & EXTENSION_MASK) > 0;
        let n_csrc = (buf[0] & CC_MASK) as usize;
        // The original makes a new slice when the one it has is too small and cuts it to
        // length otherwise: `resize` does both.
        self.csrc.resize(n_csrc, 0);

        let mut n = CSRC_OFFSET + (n_csrc * CSRC_LENGTH);
        if buf.len() < n {
            return Err(Error::HeaderSizeInsufficient {
                before: format!("size {} < {}: ", buf.len(), n),
                after: String::new(),
            });
        }
        let mut header_length = n;

        self.marker = ((buf[1] >> MARKER_SHIFT) & MARKER_MASK) > 0;
        self.payload_type = buf[1] & PT_MASK;

        self.sequence_number = (&buf[SEQ_NUM_OFFSET..SEQ_NUM_OFFSET + SEQ_NUM_LENGTH]).get_u16();
        self.timestamp = (&buf[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + TIMESTAMP_LENGTH]).get_u32();
        self.ssrc = (&buf[SSRC_OFFSET..SSRC_OFFSET + SSRC_LENGTH]).get_u32();

        for (i, csrc) in self.csrc.iter_mut().enumerate() {
            let offset = CSRC_OFFSET + (i * CSRC_LENGTH);
            *csrc = (&buf[offset..]).get_u32();
        }

        self.extensions.clear();

        if self.extension {
            let expected = n + 4;
            if buf.len() < expected {
                return Err(Error::HeaderSizeInsufficientForExtension(
                    buf.len(),
                    expected,
                ));
            }

            self.extension_profile = (&buf[n..]).get_u16();
            n += 2;
            let extension_length = (&buf[n..]).get_u16() as usize * 4;
            n += 2;
            let extension_end = n + extension_length;
            header_length = extension_end;

            if buf.len() < extension_end {
                return Err(Error::HeaderSizeInsufficientForExtension(
                    buf.len(),
                    extension_end,
                ));
            }

            if self.extension_profile == EXTENSION_PROFILE_ONE_BYTE
                || self.extension_profile == EXTENSION_PROFILE_TWO_BYTE
            {
                while n < extension_end {
                    if buf[n] == EXTENSION_ID_PADDING {
                        // padding
                        n += 1;

                        continue;
                    }

                    let extid: u8;
                    let payload_len: usize;
                    if self.extension_profile == EXTENSION_PROFILE_ONE_BYTE {
                        extid = buf[n] >> 4;
                        payload_len = ((buf[n] & !0xF0) + 1) as usize;
                        n += 1;

                        // Stop parsing extensions if we reach the reserved ID or padding with non-zero length
                        if extid == EXTENSION_ID_RESERVED || extid == EXTENSION_ID_PADDING {
                            break;
                        }
                    } else {
                        extid = buf[n];
                        n += 1;

                        if extension_end <= n {
                            return Err(Error::HeaderSizeInsufficientForExtension(
                                extension_end,
                                n,
                            ));
                        }

                        payload_len = buf[n] as usize;
                        n += 1;
                    }

                    let extension_payload_end = n + payload_len;
                    if extension_end < extension_payload_end {
                        return Err(Error::HeaderSizeInsufficientForExtension(
                            extension_end,
                            extension_payload_end,
                        ));
                    }

                    let extension = Extension {
                        id: extid,
                        payload: buf.slice(n..n + payload_len),
                    };
                    self.extensions.push(extension);
                    n += payload_len;
                }
            } else {
                // RFC3550 Extension
                let extension = Extension {
                    id: 0,
                    payload: buf.slice(n..extension_end),
                };
                self.extensions.push(extension);
            }
        }

        Ok(header_length)
    }
}

impl Packet {
    /// Unmarshal parses the passed byte slice and stores the result in the Packet.
    pub fn unmarshal(&mut self, buf: &Bytes) -> Result<(), Error> {
        let n = self.header.unmarshal(buf)?;

        // `end` can go below zero, as the original's `int` can.
        let mut end = buf.len() as i64;
        if self.header.padding {
            if end <= n as i64 {
                return Err(Error::TooSmall);
            }
            self.header.padding_size = buf[(end - 1) as usize];
            if self.header.padding_size == 0 {
                return Err(Error::InvalidRtpPadding);
            }
            end -= self.header.padding_size as i64;
        } else {
            self.header.padding_size = 0;
        }
        self.padding_size = self.header.padding_size;
        if end < n as i64 {
            return Err(Error::TooSmall);
        }

        self.payload = buf.slice(n..end as usize);

        Ok(())
    }
}

impl Header {
    /// Marshal serializes the header into bytes.
    pub fn marshal(&self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0; self.marshal_size()];

        let n = self.marshal_to(&mut buf)?;

        buf.truncate(n);
        Ok(buf)
    }

    /// MarshalTo serializes the header and writes to the buffer.
    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, Error> {
        /*
         *  0                   1                   2                   3
         *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * |V=2|P|X|  CC   |M|     PT      |       sequence number         |
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * |                           timestamp                           |
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * |           synchronization source (SSRC) identifier            |
         * +=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
         * |            contributing source (CSRC) identifiers             |
         * |                             ....                              |
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         */

        let size = self.marshal_size();
        if size > buf.len() {
            return Err(Error::ShortBuffer);
        }

        // The first byte contains the version, padding bit, extension bit,
        // and csrc size.
        buf[0] = (self.version << VERSION_SHIFT) | self.csrc.len() as u8;
        if self.padding {
            buf[0] |= 1 << PADDING_SHIFT;
        }

        if self.extension {
            buf[0] |= 1 << EXTENSION_SHIFT;
        }

        // The second byte contains the marker bit and payload type.
        buf[1] = self.payload_type;
        if self.marker {
            buf[1] |= 1 << MARKER_SHIFT;
        }

        (&mut buf[2..4]).put_u16(self.sequence_number);
        (&mut buf[4..8]).put_u32(self.timestamp);
        (&mut buf[8..12]).put_u32(self.ssrc);

        let mut n = 12;
        for csrc in &self.csrc {
            (&mut buf[n..n + 4]).put_u32(*csrc);
            n += 4;
        }

        if self.extension {
            let ext_header_pos = n;
            (&mut buf[n..n + 2]).put_u16(self.extension_profile);
            n += 4;
            let start_extensions_pos = n;

            match self.extension_profile {
                // RFC 8285 RTP One Byte Header Extension
                EXTENSION_PROFILE_ONE_BYTE => {
                    for extension in &self.extensions {
                        buf[n] =
                            (extension.id << 4) | (extension.payload.len() as u8).wrapping_sub(1);
                        n += 1;
                        n += copy(&mut buf[n..], &extension.payload);
                    }
                }
                // RFC 8285 RTP Two Byte Header Extension
                EXTENSION_PROFILE_TWO_BYTE => {
                    for extension in &self.extensions {
                        buf[n] = extension.id;
                        n += 1;
                        buf[n] = extension.payload.len() as u8;
                        n += 1;
                        n += copy(&mut buf[n..], &extension.payload);
                    }
                }
                // RFC3550 Extension
                _ => {
                    // Zero length extension is valid per the RFC3550 spec
                    // https://www.rfc-editor.org/rfc/rfc3550#section-5.3.1
                    if !self.extensions.is_empty() {
                        let extlen = self.extensions[0].payload.len();
                        if !extlen.is_multiple_of(4) {
                            // the payload must be in 32-bit words.
                            return Err(Error::ShortBuffer);
                        }
                        n += copy(&mut buf[n..], &self.extensions[0].payload);
                    }
                }
            }

            // calculate extensions size and round to 4 bytes boundaries
            let ext_size = n - start_extensions_pos;
            let rounded_ext_size = ext_size.div_ceil(4) * 4;

            (&mut buf[ext_header_pos + 2..ext_header_pos + 4])
                .put_u16((rounded_ext_size / 4) as u16);

            // add padding to reach 4 bytes boundaries
            for _ in 0..rounded_ext_size - ext_size {
                buf[n] = 0;
                n += 1;
            }
        }

        Ok(n)
    }

    /// MarshalSize returns the size of the header once marshaled.
    pub fn marshal_size(&self) -> usize {
        // NOTE: Be careful to match the MarshalTo() method.
        let mut size = 12 + (self.csrc.len() * CSRC_LENGTH);

        if self.extension {
            let mut ext_size = 4;

            match self.extension_profile {
                // RFC 8285 RTP One Byte Header Extension
                EXTENSION_PROFILE_ONE_BYTE => {
                    for extension in &self.extensions {
                        ext_size += 1 + extension.payload.len();
                    }
                }
                // RFC 8285 RTP Two Byte Header Extension
                EXTENSION_PROFILE_TWO_BYTE => {
                    for extension in &self.extensions {
                        ext_size += 2 + extension.payload.len();
                    }
                }
                _ => {
                    if !self.extensions.is_empty() {
                        ext_size += self.extensions[0].payload.len();
                    }
                }
            }

            // extensions size must have 4 bytes boundaries
            size += ext_size.div_ceil(4) * 4;
        }

        size
    }

    /// SetExtension sets an RTP header extension.
    pub fn set_extension(&mut self, id: u8, payload: Bytes) -> Result<(), Error> {
        if self.extension {
            header_extension_check(self.extension_profile, id, &payload)?;

            // Update existing if it exists else add new extension
            for extension in self.extensions.iter_mut() {
                if extension.id == id {
                    extension.payload = payload;

                    return Ok(());
                }
            }

            self.extensions.push(Extension { id, payload });

            return Ok(());
        }

        // No existing header extensions
        self.extension = true;

        match payload.len() {
            0..=16 => self.extension_profile = EXTENSION_PROFILE_ONE_BYTE,
            17..=255 => self.extension_profile = EXTENSION_PROFILE_TWO_BYTE,
            // The original's switch has no case for 256 and more: the profile stays as it is.
            _ => {}
        }

        self.extensions.push(Extension { id, payload });

        Ok(())
    }

    /// SetExtensionWithProfile sets an RTP header extension and converts Header Extension Profile if needed.
    pub fn set_extension_with_profile(
        &mut self,
        id: u8,
        payload: Bytes,
        intended_profile: u16,
    ) -> Result<(), Error> {
        if !self.extension || self.extension_profile == intended_profile {
            return self.set_extension(id, payload);
        }

        // Don't mutate the packet if Set is going to fail anyway
        header_extension_check(intended_profile, id, &payload)?;

        // If downgrading assert that existing Extensions will work
        if intended_profile == EXTENSION_PROFILE_ONE_BYTE {
            for extension in &self.extensions {
                header_extension_check(intended_profile, extension.id, &extension.payload)?;
            }
        }

        self.extension_profile = intended_profile;

        self.set_extension(id, payload)
    }

    /// GetExtensionIDs returns an extension id array.
    pub fn get_extension_ids(&self) -> Vec<u8> {
        if !self.extension {
            return Vec::new();
        }

        if self.extensions.is_empty() {
            return Vec::new();
        }

        let mut ids = Vec::with_capacity(self.extensions.len());
        for extension in &self.extensions {
            ids.push(extension.id);
        }

        ids
    }

    /// GetExtension returns an RTP header extension.
    pub fn get_extension(&self, id: u8) -> Option<Bytes> {
        if !self.extension {
            return None;
        }
        for extension in &self.extensions {
            if extension.id == id {
                return Some(extension.payload.clone());
            }
        }

        None
    }

    /// DelExtension Removes an RTP Header extension.
    pub fn del_extension(&mut self, id: u8) -> Result<(), Error> {
        if !self.extension {
            return Err(Error::HeaderExtensionsNotEnabled);
        }
        if let Some(i) = self
            .extensions
            .iter()
            .position(|extension| extension.id == id)
        {
            self.extensions.remove(i);
            if self.extensions.is_empty() {
                self.extension = false;
                self.extension_profile = 0;
            }

            return Ok(());
        }

        Err(Error::HeaderExtensionNotFound)
    }

    /// ClearExtensions Removes all RTP Header extension.
    pub fn clear_extensions(&mut self) {
        self.extensions.clear();
        self.extension = false;
        self.extension_profile = 0;
    }
}

impl Packet {
    /// Marshal serializes the packet into bytes.
    pub fn marshal(&self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0; self.marshal_size()];

        let n = self.marshal_to(&mut buf)?;

        buf.truncate(n);
        Ok(buf)
    }

    /// MarshalTo serializes the packet and writes to the buffer.
    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, Error> {
        if self.header.padding && self.padding_size() == 0 {
            return Err(Error::InvalidRtpPadding);
        }

        let n = self.header.marshal_to(buf)?;

        marshal_payload_and_padding_to(buf, n, &self.header, &self.payload, self.padding_size())
    }
}

fn marshal_payload_and_padding_to(
    buf: &mut [u8],
    offset: usize,
    header: &Header,
    payload: &[u8],
    padding_size: u8,
) -> Result<usize, Error> {
    // Make sure the buffer is large enough to hold the packet.
    if offset + payload.len() + padding_size as usize > buf.len() {
        return Err(Error::ShortBuffer);
    }

    let m = copy(&mut buf[offset..], payload);

    if header.padding {
        buf[offset + m + padding_size.wrapping_sub(1) as usize] = padding_size;
    }

    Ok(offset + m + padding_size as usize)
}

impl Packet {
    /// MarshalSize returns the size of the packet once marshaled.
    pub fn marshal_size(&self) -> usize {
        self.header.marshal_size() + self.payload.len() + self.padding_size() as usize
    }

    // Clone returns a deep copy of p, and Clone of Header a deep copy h: the derived `Clone`.

    fn padding_size(&self) -> u8 {
        if self.header.padding_size > 0 {
            return self.header.padding_size;
        }

        self.padding_size
    }
}

/// MarshalPacketTo serializes the header and payload into bytes.
/// Parts of pion code passes RTP header and payload separately, so this function
/// is provided to help with that.
///
/// Deprecated: this function is a temporary workaround and will be removed in pion/webrtc v5.
pub fn marshal_packet_to(buf: &mut [u8], header: &Header, payload: &[u8]) -> Result<usize, Error> {
    let n = header.marshal_to(buf)?;

    marshal_payload_and_padding_to(buf, n, header, payload, header.padding_size)
}

/// PacketMarshalSize returns the size of the header and payload once marshaled.
/// Parts of pion code passes RTP header and payload separately, so this function
/// is provided to help with that.
///
/// Deprecated: this function is a temporary workaround and will be removed in pion/webrtc v5.
pub fn packet_marshal_size(header: &Header, payload: &[u8]) -> usize {
    header.marshal_size() + payload.len() + header.padding_size as usize
}

/// HeaderAndPacketMarshalSize returns the size of the header and full packet once marshaled.
/// Parts of pion code passes RTP header and payload separately, so this function
/// is provided to help with that.
///
/// Deprecated: this function is a temporary workaround and will be removed in pion/webrtc v5.
pub fn header_and_packet_marshal_size(header: &Header, payload: &[u8]) -> (usize, usize) {
    let header_size = header.marshal_size();

    (
        header_size,
        header_size + payload.len() + header.padding_size as usize,
    )
}

/// Go's builtin `copy`: copies as many bytes as both slices have and returns how many.
fn copy(dst: &mut [u8], src: &[u8]) -> usize {
    let n = dst.len().min(src.len());
    dst[..n].copy_from_slice(&src[..n]);
    n
}

#[cfg(test)]
#[path = "packet_test.rs"]
mod packet_test;
