// Ported from LiveKit's protocol, datatrack/extension_user_timestamp.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `new` gives the value, not a pointer to it, and
// `unmarshal` borrows the extension; `binary.Size` of the timestamp is `size_of`.

use bytes::{Buf, BufMut, Bytes};

use super::packet::{Error, Extension};
use crate::protocol::livekit;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtensionUserTimestamp {
    timestamp: u64,
}

impl ExtensionUserTimestamp {
    pub fn new(timestamp: u64) -> Self {
        Self { timestamp }
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn marshal(&self) -> Result<Extension, Error> {
        let mut data = vec![0; size_of::<u64>()];
        (&mut data[..]).put_u64(self.timestamp);
        Ok(Extension {
            id: livekit::DataTrackExtensionId::DteiUserTimestamp as u8,
            data: Bytes::from(data),
        })
    }

    pub fn unmarshal(&mut self, ext: &Extension) -> Result<(), Error> {
        if ext.id != livekit::DataTrackExtensionId::DteiUserTimestamp as u8 {
            return Err(Error::ExtensionInvalidId);
        }

        if ext.data.len() < size_of::<u64>() {
            return Err(Error::ExtensionDataTooShort);
        }

        self.timestamp = (&ext.data[..]).get_u64();
        Ok(())
    }
}

#[cfg(test)]
#[path = "extension_user_timestamp_test.rs"]
mod extension_user_timestamp_test;
