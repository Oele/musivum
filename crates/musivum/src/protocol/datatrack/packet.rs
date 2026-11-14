// Ported from LiveKit's protocol, datatrack/packet.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The errors are one enum: the ones the original wraps
// with numbers (`fmt.Errorf("%w: ...")`) carry those numbers, and their texts are the wrapped
// ones; it also has the error that `NewExtensionParticipantSid` makes on the spot. The payload
// and the data of an extension are `Bytes`: `unmarshal` takes the buffer as `&Bytes` and keeps
// slices of it, as the original keeps slices of the buffer it is given. `Packet` embeds
// `Header` in Go: here it holds one and dereferences to it, so that `packet.handle` and
// `packet.add_extension(ext)` read the same. An `unmarshal` or `marshal_to` that fails gives
// the error only, not also a size of zero.

use std::ops::{Deref, DerefMut};

use bytes::{Buf, BufMut, Bytes};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// ErrHeaderSizeInsufficient, with the size there is and the size needed.
    #[error("data track packet header size insufficient: {0} < {1}")]
    HeaderSizeInsufficient(usize, usize),
    /// ErrBufferSizeInsufficient, with the size there is and the size needed.
    #[error("data track packet buffer size insufficient: {0} < {1}")]
    BufferSizeInsufficient(usize, usize),
    /// ErrExtensionSizeInsufficient, with what is left of the extensions' size (it can be
    /// below zero, as in the original), what is left of the buffer, and the size needed.
    #[error("data track packet extension size insufficient: {0}/{1} < {2}")]
    ExtensionSizeInsufficient(i64, usize, i64),
    /// ErrExtensionNotFound, with the ID asked for.
    #[error("data track packet extension not found, id: {0}")]
    ExtensionNotFound(u8),
    /// ErrExtensionSizeTooBig, with the size of the extension's data.
    #[error("extension size is too big: {0} > 255")]
    ExtensionSizeTooBig(usize),
    #[error("invalid extension ID")]
    ExtensionInvalidId,
    #[error("extension data too short")]
    ExtensionDataTooShort,
    /// What `NewExtensionParticipantSid` makes with `errors.New` on the spot.
    #[error("participantID too long")]
    ParticipantIdTooLong,
}

const HEADER_LENGTH: usize = 12;

const VERSION_SHIFT: u8 = 5;
const VERSION_MASK: u8 = (1 << 3) - 1;

const START_OF_FRAME_SHIFT: u8 = 4;
const START_OF_FRAME_MASK: u8 = (1 << 1) - 1;

const FINAL_OF_FRAME_SHIFT: u8 = 3;
const FINAL_OF_FRAME_MASK: u8 = (1 << 1) - 1;

const EXTENSIONS_SHIFT: u8 = 2;
const EXTENSIONS_MASK: u8 = (1 << 1) - 1;

const HANDLE_OFFSET: usize = 2;
const HANDLE_LENGTH: usize = 2;

const SEQ_NUM_OFFSET: usize = 4;
const SEQ_NUM_LENGTH: usize = 2;

const FRAME_NUM_OFFSET: usize = 6;
const FRAME_NUM_LENGTH: usize = 2;

const TIMESTAMP_OFFSET: usize = 8;
const TIMESTAMP_LENGTH: usize = 4;

const EXTENSIONS_SIZE_OFFSET: usize = HEADER_LENGTH;
const EXTENSIONS_SIZE_LENGTH: usize = 2;

const EXTENSION_ID_LENGTH: usize = 1;
const EXTENSION_SIZE_LENGTH: usize = 1;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Extension {
    pub(super) id: u8,
    pub(super) data: Bytes,
}

impl Extension {
    pub fn new(id: u8, data: Bytes) -> Self {
        Self { id, data }
    }

    pub fn id(&self) -> u8 {
        self.id
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Header {
    pub version: u8,
    pub is_start_of_frame: bool,
    pub is_final_of_frame: bool,
    pub has_extensions: bool,
    pub handle: u16,
    pub sequence_number: u16,
    pub frame_number: u16,
    pub timestamp: u32,
    pub extensions_size: u16,
    pub extensions: Vec<Extension>,
}

/*
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |V    |S|F|X| reserved          | handle                        |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    | sequence number               | frame number                  |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                           timestamp                           |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    | Extensions Size if X=1          | Extensions...                 |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    Each extension
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    | Extension ID  | Extension size| Extension payload             |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    End of all extensions
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    | padded to 4 byte boundary if aggregate of `Extensions Size`     |
    | field and all extensions do not end on a 4 byte boundary        |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*/

impl Header {
    pub fn unmarshal(&mut self, buf: &Bytes) -> Result<usize, Error> {
        if buf.len() < HEADER_LENGTH {
            return Err(Error::HeaderSizeInsufficient(buf.len(), HEADER_LENGTH));
        }

        let mut hdr_size = HEADER_LENGTH;
        self.version = (buf[0] >> VERSION_SHIFT) & VERSION_MASK;
        self.is_start_of_frame = ((buf[0] >> START_OF_FRAME_SHIFT) & START_OF_FRAME_MASK) > 0;
        self.is_final_of_frame = ((buf[0] >> FINAL_OF_FRAME_SHIFT) & FINAL_OF_FRAME_MASK) > 0;
        self.has_extensions = ((buf[0] >> EXTENSIONS_SHIFT) & EXTENSIONS_MASK) > 0;

        self.handle = (&buf[HANDLE_OFFSET..HANDLE_OFFSET + HANDLE_LENGTH]).get_u16();
        self.sequence_number = (&buf[SEQ_NUM_OFFSET..SEQ_NUM_OFFSET + SEQ_NUM_LENGTH]).get_u16();
        self.frame_number = (&buf[FRAME_NUM_OFFSET..FRAME_NUM_OFFSET + FRAME_NUM_LENGTH]).get_u16();
        self.timestamp = (&buf[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + TIMESTAMP_LENGTH]).get_u32();

        if self.has_extensions {
            if buf.len() < EXTENSIONS_SIZE_OFFSET + EXTENSIONS_SIZE_LENGTH {
                return Err(Error::HeaderSizeInsufficient(
                    buf.len(),
                    EXTENSIONS_SIZE_OFFSET + EXTENSIONS_SIZE_LENGTH,
                ));
            }
            // An `int` in the original, as `remaining_size` is, which can go below zero.
            let extensions_size: i64 = ((&buf
                [EXTENSIONS_SIZE_OFFSET..EXTENSIONS_SIZE_OFFSET + EXTENSIONS_SIZE_LENGTH])
                .get_u16() as i64
                + 1)
                * 4
                - EXTENSIONS_SIZE_LENGTH as i64;
            hdr_size += EXTENSIONS_SIZE_LENGTH;

            let extension_header_size = EXTENSION_ID_LENGTH + EXTENSION_SIZE_LENGTH;
            let mut remaining_size = extensions_size;
            let mut idx = EXTENSIONS_SIZE_OFFSET + EXTENSIONS_SIZE_LENGTH;
            while remaining_size != 0 {
                // read extension header
                if buf[idx..].len() < EXTENSION_ID_LENGTH
                    || remaining_size < EXTENSION_ID_LENGTH as i64
                {
                    return Err(Error::ExtensionSizeInsufficient(
                        remaining_size,
                        buf[idx..].len(),
                        EXTENSION_ID_LENGTH as i64,
                    ));
                }
                let id = buf[idx];
                if id == 0 {
                    // end of extensions, padding has started
                    if (buf[idx..].len() as i64) < remaining_size {
                        return Err(Error::ExtensionSizeInsufficient(
                            remaining_size,
                            buf[idx..].len(),
                            remaining_size,
                        ));
                    }
                    hdr_size += remaining_size as usize;
                    break;
                }

                if buf[idx + 1..].len() < EXTENSION_SIZE_LENGTH
                    || remaining_size < EXTENSION_SIZE_LENGTH as i64
                {
                    return Err(Error::ExtensionSizeInsufficient(
                        remaining_size,
                        buf[idx..].len(),
                        EXTENSION_SIZE_LENGTH as i64,
                    ));
                }
                let size = buf[idx + 1] as usize;

                remaining_size -= extension_header_size as i64;
                idx += extension_header_size;
                hdr_size += extension_header_size;

                // read extension data
                if buf[idx..].len() < size || remaining_size < size as i64 {
                    return Err(Error::ExtensionSizeInsufficient(
                        remaining_size,
                        buf[idx..].len(),
                        size as i64,
                    ));
                }
                self.extensions.push(Extension {
                    id,
                    data: buf.slice(idx..idx + size),
                });

                remaining_size -= size as i64;
                idx += size;
                hdr_size += size;
            }
            self.extensions_size = (extensions_size - remaining_size) as u16;
        }

        Ok(hdr_size)
    }

    pub fn marshal_size(&self) -> usize {
        let mut extensions_size = 0;
        if self.has_extensions {
            extensions_size += EXTENSIONS_SIZE_LENGTH;
            for ext in &self.extensions {
                extensions_size += ext.data.len() + EXTENSION_ID_LENGTH + EXTENSION_SIZE_LENGTH;
            }
        }

        HEADER_LENGTH + extensions_size.div_ceil(4) * 4
    }

    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, Error> {
        if buf.len() < HEADER_LENGTH {
            return Err(Error::HeaderSizeInsufficient(buf.len(), HEADER_LENGTH));
        }

        let mut hdr_size = HEADER_LENGTH;
        buf[0] = self.version << VERSION_SHIFT;
        if self.is_start_of_frame {
            buf[0] |= 1 << START_OF_FRAME_SHIFT;
        }
        if self.is_final_of_frame {
            buf[0] |= 1 << FINAL_OF_FRAME_SHIFT;
        }
        if self.has_extensions {
            buf[0] |= 1 << EXTENSIONS_SHIFT;
        }

        (&mut buf[HANDLE_OFFSET..HANDLE_OFFSET + HANDLE_LENGTH]).put_u16(self.handle);
        (&mut buf[SEQ_NUM_OFFSET..SEQ_NUM_OFFSET + SEQ_NUM_LENGTH]).put_u16(self.sequence_number);
        (&mut buf[FRAME_NUM_OFFSET..FRAME_NUM_OFFSET + FRAME_NUM_LENGTH])
            .put_u16(self.frame_number);
        (&mut buf[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + TIMESTAMP_LENGTH]).put_u32(self.timestamp);

        if self.has_extensions {
            // `uint16` arithmetic in the original, here and for the padding below: it wraps.
            let extensions_size = (EXTENSIONS_SIZE_LENGTH as u16)
                .wrapping_add(self.extensions_size)
                .wrapping_add(3)
                / 4
                * 4;
            (&mut buf[EXTENSIONS_SIZE_OFFSET..EXTENSIONS_SIZE_OFFSET + EXTENSIONS_SIZE_LENGTH])
                .put_u16((extensions_size / 4).wrapping_sub(1));
            hdr_size += EXTENSIONS_SIZE_LENGTH;

            let mut added_size = 0;
            let mut idx = EXTENSIONS_SIZE_OFFSET + EXTENSIONS_SIZE_LENGTH;
            for ext in &self.extensions {
                buf[idx] = ext.id;
                if ext.data.len() > 255 {
                    return Err(Error::ExtensionSizeTooBig(ext.data.len()));
                }
                buf[idx + EXTENSION_ID_LENGTH] = ext.data.len() as u8;
                copy(
                    &mut buf[idx + EXTENSION_ID_LENGTH + EXTENSION_SIZE_LENGTH..],
                    &ext.data,
                );

                let ext_size = ext.data.len() + EXTENSION_ID_LENGTH + EXTENSION_SIZE_LENGTH;
                idx += ext_size;
                hdr_size += ext_size;
                added_size += ext_size;
            }

            let padding_size = extensions_size
                .wrapping_sub(EXTENSIONS_SIZE_LENGTH as u16)
                .wrapping_sub(added_size as u16);
            for i in 0..padding_size {
                buf[idx + i as usize] = 0;
            }
            // The original also moves `idx` past the padding, and does not read it again.
            hdr_size += padding_size as usize;
        }

        Ok(hdr_size)
    }

    pub fn add_extension(&mut self, ext: Extension) {
        for existing_ext in self.extensions.iter_mut() {
            if existing_ext.id == ext.id {
                self.extensions_size = self.extensions_size.wrapping_sub(
                    (existing_ext.data.len() + EXTENSION_ID_LENGTH + EXTENSION_SIZE_LENGTH) as u16,
                );
                existing_ext.data = ext.data;
                self.extensions_size = self.extensions_size.wrapping_add(
                    (existing_ext.data.len() + EXTENSION_ID_LENGTH + EXTENSION_SIZE_LENGTH) as u16,
                );
                return;
            }
        }

        let data_len = ext.data.len();
        self.extensions.push(ext);
        self.extensions_size = self
            .extensions_size
            .wrapping_add((data_len + EXTENSION_ID_LENGTH + EXTENSION_SIZE_LENGTH) as u16);
        self.has_extensions = true;
    }

    pub fn get_extension(&self, id: u8) -> Result<Extension, Error> {
        for ext in &self.extensions {
            if ext.id == id {
                return Ok(ext.clone());
            }
        }
        Err(Error::ExtensionNotFound(id))
    }
}

// ----------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Packet {
    pub header: Header,
    pub payload: Bytes,
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

impl Packet {
    pub fn unmarshal(&mut self, buf: &Bytes) -> Result<(), Error> {
        let hdr_size = self.header.unmarshal(buf)?;
        if hdr_size > buf.len() {
            return Err(Error::BufferSizeInsufficient(buf.len(), hdr_size));
        }

        self.payload = buf.slice(hdr_size..);
        Ok(())
    }

    pub fn marshal(&self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0; self.header.marshal_size() + self.payload.len()];
        self.marshal_to(&mut buf)?;

        Ok(buf)
    }

    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<(), Error> {
        let size = self.header.marshal_size() + self.payload.len();
        if buf.len() < size {
            return Err(Error::BufferSizeInsufficient(buf.len(), size));
        }

        let hdr_size = self.header.marshal_to(buf)?;

        copy(&mut buf[hdr_size..], &self.payload);
        Ok(())
    }
}

/// Go's builtin `copy`: copies as many bytes as both slices have and returns how many.
pub(super) fn copy(dst: &mut [u8], src: &[u8]) -> usize {
    let n = dst.len().min(src.len());
    dst[..n].copy_from_slice(&src[..n]);
    n
}

#[cfg(test)]
#[path = "packet_test.rs"]
mod packet_test;
