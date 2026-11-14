// Ported from pion/turn, internal/server/short_nonce.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. `NonceManager` is a trait. The key comes from `rand`'s
// generator for keys (the thread's, cryptographically secure), where Go's `crypto/rand` cannot
// fail either.

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use super::Error;
use super::base36::{decode_base36, encode_base36};
use crate::go::time;

// NonceManager interface that both implementations satisfy.
pub trait NonceManager: Send + Sync {
    fn generate(&self) -> Result<String, Error>;
    fn validate(&self, nonce: &str) -> Result<(), Error>;
}

const SHORT_NONCE_KEY_LENGTH: usize = 64; // Same as original
const SHORT_NONCE_TIMESTAMP_LEN: usize = 4; // 6 bytes for timestamp (minutes) - optimal size
const SHORT_NONCE_MIN_HMAC_LEN: usize = 2; // Minimum HMAC length for security
const SHORT_NONCE_MAX_HMAC_LEN: usize = 32; // Maximum HMAC length (full SHA256)
const DEFAULT_NONCE_HMAC_LEN: usize = 12; // Default HMAC length

// NewShortNonceHash creates a ShortNonceHash. The hmacLen argument specifies the number of HMAC
// bytes to include (2-32 bytes).  The total nonce size will be 4 + hmacLen bytes, default hmaclen
// is 12 bytes. The 4 bytes timestamp gives about ~8000 years before nonces would start to repeat
// (safe until year 10,135).
pub fn new_short_nonce_hash(mut hmac_len: usize) -> Result<Box<dyn NonceManager>, Error> {
    if hmac_len == 0 {
        hmac_len = DEFAULT_NONCE_HMAC_LEN;
    }

    if !(SHORT_NONCE_MIN_HMAC_LEN..=SHORT_NONCE_MAX_HMAC_LEN).contains(&hmac_len) {
        return Err(Error::FailedToGenerateNonce);
    }

    let mut key = vec![0u8; SHORT_NONCE_KEY_LENGTH];
    rand::fill(&mut key[..]);

    Ok(Box::new(ShortNonceHash { key, hmac_len }))
}

// ShortNonceHash is used to create and verify short nonces.
pub struct ShortNonceHash {
    key: Vec<u8>,
    hmac_len: usize,
}

impl ShortNonceHash {
    fn hmac(&self, timestamp_bytes: &[u8]) -> Result<Vec<u8>, Error> {
        let mut hash =
            Hmac::<Sha256>::new_from_slice(&self.key).map_err(|_| Error::FailedToGenerateNonce)?;
        hash.update(timestamp_bytes);
        Ok(hash.finalize().into_bytes().to_vec())
    }
}

impl NonceManager for ShortNonceHash {
    // Generate a short nonce (4 + hmacLen bytes encoded as base36).
    fn generate(&self) -> Result<String, Error> {
        let timestamp_minutes = time::now().unix_secs() / 60;

        // Convert to bytes and trim to 4 bytes.  This safely handles the conversion since we know
        // current values fit in 4 bytes until year 10,135.
        let timestamp_bytes8 = (timestamp_minutes as u64).to_be_bytes();
        let timestamp_bytes = &timestamp_bytes8[4..];

        let full_hmac = self.hmac(timestamp_bytes)?;
        let truncated_hmac = &full_hmac[..self.hmac_len];

        let total_len = SHORT_NONCE_TIMESTAMP_LEN + self.hmac_len;
        let mut nonce = vec![0u8; total_len];
        nonce[..SHORT_NONCE_TIMESTAMP_LEN].copy_from_slice(timestamp_bytes);
        nonce[SHORT_NONCE_TIMESTAMP_LEN..].copy_from_slice(truncated_hmac);

        Ok(encode_base36(&nonce))
    }

    // Validate checks that nonce is signed and is not expired.
    fn validate(&self, nonce: &str) -> Result<(), Error> {
        let Some(mut nonce_bytes) = decode_base36(nonce) else {
            return Err(Error::InvalidNonce);
        };

        let expected_len = SHORT_NONCE_TIMESTAMP_LEN + self.hmac_len;
        if nonce_bytes.len() != expected_len {
            // Pad with leadnign zeros if leading zeros were stripped during encoding/decoding.
            if nonce_bytes.len() < expected_len {
                let mut padded = vec![0u8; expected_len];
                padded[expected_len - nonce_bytes.len()..].copy_from_slice(&nonce_bytes);
                nonce_bytes = padded;
            } else {
                return Err(Error::InvalidNonce);
            }
        }

        let timestamp_bytes = &nonce_bytes[..SHORT_NONCE_TIMESTAMP_LEN];
        let received_hmac = &nonce_bytes[SHORT_NONCE_TIMESTAMP_LEN..];
        let timestamp_minutes =
            u32::from_be_bytes(timestamp_bytes.try_into().expect("four bytes")) as i64;

        // Check if nonce is expired (older than 1 hour).
        let current_minutes = time::now().unix_secs() / 60;
        if current_minutes < timestamp_minutes {
            return Err(Error::InvalidNonce);
        }

        let age_minutes = current_minutes - timestamp_minutes;
        if age_minutes > 60 {
            return Err(Error::InvalidNonce);
        }

        // Recompute HMAC and compare.
        let full_hmac = self
            .hmac(timestamp_bytes)
            .map_err(|_| Error::InvalidNonce)?;
        let expected_hmac = &full_hmac[..self.hmac_len];

        if !constant_time_eq(received_hmac, expected_hmac) {
            return Err(Error::InvalidNonce);
        }

        Ok(())
    }
}

// hmac.Equal
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
