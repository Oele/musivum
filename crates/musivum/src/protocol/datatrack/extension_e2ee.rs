// Ported from LiveKit's protocol, datatrack/extension_e2ee.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `new` gives the value, not a pointer to it, and
// `unmarshal` borrows the extension.

use bytes::Bytes;

use super::packet::{Error, Extension, copy};
use crate::protocol::livekit;

pub const EXTENSION_E2EE_IV_LENGTH: usize = 12;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtensionE2ee {
    key_index: u8,
    iv: [u8; EXTENSION_E2EE_IV_LENGTH],
}

impl ExtensionE2ee {
    pub fn new(key_index: u8, iv: [u8; EXTENSION_E2EE_IV_LENGTH]) -> Self {
        Self { key_index, iv }
    }

    pub fn key_index(&self) -> u8 {
        self.key_index
    }

    pub fn iv(&self) -> [u8; EXTENSION_E2EE_IV_LENGTH] {
        self.iv
    }

    pub fn marshal(&self) -> Result<Extension, Error> {
        let mut data = vec![0; 1 + self.iv.len()];
        data[0] = self.key_index;
        copy(&mut data[1..], &self.iv);
        Ok(Extension {
            id: livekit::DataTrackExtensionId::DteiE2ee as u8,
            data: Bytes::from(data),
        })
    }

    pub fn unmarshal(&mut self, ext: &Extension) -> Result<(), Error> {
        if ext.id != livekit::DataTrackExtensionId::DteiE2ee as u8 {
            return Err(Error::ExtensionInvalidId);
        }

        if ext.data.len() < 1 + self.iv.len() {
            return Err(Error::ExtensionDataTooShort);
        }

        self.key_index = ext.data[0];
        copy(&mut self.iv, &ext.data[1..]);
        Ok(())
    }
}

#[cfg(test)]
#[path = "extension_e2ee_test.rs"]
mod extension_e2ee_test;
