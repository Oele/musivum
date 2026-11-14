// Ported from LiveKit's protocol, datatrack/extension_participant_sid.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `new` gives the value, not a pointer to it, and its
// error is one of the package's enum; `unmarshal` borrows the extension. A Go string holds any
// bytes and `ParticipantId` holds text: `unmarshal` replaces what is not UTF-8 in the
// extension's data with U+FFFD, where the original keeps the bytes as they are.

use bytes::Bytes;

use super::packet::{Error, Extension};
use crate::protocol::livekit;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtensionParticipantSid {
    participant_id: livekit::ParticipantId,
}

impl ExtensionParticipantSid {
    pub fn new(participant_id: livekit::ParticipantId) -> Result<Self, Error> {
        if participant_id.as_str().len() >= 256 {
            return Err(Error::ParticipantIdTooLong);
        }

        Ok(Self { participant_id })
    }

    pub fn participant_id(&self) -> livekit::ParticipantId {
        self.participant_id.clone()
    }

    pub fn marshal(&self) -> Result<Extension, Error> {
        let data = Bytes::copy_from_slice(self.participant_id.as_str().as_bytes());
        Ok(Extension {
            id: livekit::DataTrackExtensionId::DteiParticipantSid as u8,
            data,
        })
    }

    pub fn unmarshal(&mut self, ext: &Extension) -> Result<(), Error> {
        if ext.id != livekit::DataTrackExtensionId::DteiParticipantSid as u8 {
            return Err(Error::ExtensionInvalidId);
        }

        if ext.data.is_empty() {
            return Err(Error::ExtensionDataTooShort);
        }

        self.participant_id =
            livekit::ParticipantId::from(String::from_utf8_lossy(&ext.data).as_ref());
        Ok(())
    }
}

#[cfg(test)]
#[path = "extension_participant_sid_test.rs"]
mod extension_participant_sid_test;
