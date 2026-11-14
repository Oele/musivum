// Ported from pion/rtp, header_extension.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; only `headerExtensionCheck` is here, which packet.go
// needs. The types of the file (`HeaderExtension`, `OneByteHeaderExtension`,
// `TwoByteHeaderExtension`, `RawExtension`) are not ported: LiveKit does not use them.

use super::error::Error;
use super::packet::{EXTENSION_PROFILE_ONE_BYTE, EXTENSION_PROFILE_TWO_BYTE};

// Assert that id + value is valid for give Header Extension Profile.
pub(super) fn header_extension_check(
    extension_profile: u16,
    id: u8,
    payload: &[u8],
) -> Result<(), Error> {
    match extension_profile {
        // RFC 8285 RTP One Byte Header Extension
        EXTENSION_PROFILE_ONE_BYTE => {
            if !(1..=14).contains(&id) {
                return Err(Error::Rfc8285OneByteHeaderIdRange(id));
            }
            if payload.len() > 16 {
                return Err(Error::Rfc8285OneByteHeaderSize(payload.len()));
            }
        }
        // RFC 8285 RTP Two Byte Header Extension
        EXTENSION_PROFILE_TWO_BYTE => {
            if id < 1 {
                return Err(Error::Rfc8285TwoByteHeaderIdRange(id));
            }
            if payload.len() > 255 {
                return Err(Error::Rfc8285TwoByteHeaderSize(payload.len()));
            }
        }
        // RFC3550 Extension
        _ => {
            if id != 0 {
                return Err(Error::Rfc3550HeaderIdRange(id));
            }
        }
    }

    Ok(())
}
