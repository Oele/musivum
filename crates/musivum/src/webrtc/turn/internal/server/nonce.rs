// Ported from pion/turn, internal/server/nonce.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The key comes from `rand`'s generator for keys (the
// thread's, cryptographically secure), where Go's `crypto/rand` cannot fail either.

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use super::{Error, NonceManager};
use crate::go::time::{self, Duration, Time};

const NONCE_LIFETIME: Duration = Duration::from_secs(60 * 60); // See: https://tools.ietf.org/html/rfc5766#section-4
const NONCE_LENGTH: usize = 40;
const NONCE_KEY_LENGTH: usize = 64;

// NewNonceHash creates a NonceHash.
pub fn new_nonce_hash() -> Result<Box<dyn NonceManager>, Error> {
    let mut key = vec![0u8; NONCE_KEY_LENGTH];
    rand::fill(&mut key[..]);

    Ok(Box::new(NonceHash { key }))
}

// NonceHash is used to create and verify nonces.
pub struct NonceHash {
    key: Vec<u8>,
}

impl NonceManager for NonceHash {
    // Generate a nonce.
    fn generate(&self) -> Result<String, Error> {
        let mut nonce = Vec::with_capacity(NONCE_LENGTH);
        nonce.extend_from_slice(&(time::now().unix_milli() as u64).to_be_bytes());

        let mut hash = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|err| Error::InvalidNonceWith(err.to_string()))?;
        hash.update(&nonce[..8]);
        nonce.extend_from_slice(&hash.finalize().into_bytes());

        Ok(hex_encode(&nonce))
    }

    // Validate checks that nonce is signed and is not expired.
    fn validate(&self, nonce: &str) -> Result<(), Error> {
        let b = match hex_decode(nonce) {
            Some(b) if b.len() == NONCE_LENGTH => b,
            Some(_) => return Err(Error::InvalidNonceWith("<nil>".to_string())),
            None => {
                return Err(Error::InvalidNonceWith(
                    "encoding/hex: invalid byte".to_string(),
                ));
            }
        };

        let ts = Time::unix(0, 0).add(Duration::from_millis(i64::from_be_bytes(
            b[..8].try_into().expect("eight bytes"),
        )));
        if time::since(ts) > NONCE_LIFETIME {
            return Err(Error::InvalidNonce);
        }

        let mut hash = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|err| Error::InvalidNonceWith(err.to_string()))?;
        hash.update(&b[..8]);
        if hash.verify_slice(&b[8..]).is_err() {
            return Err(Error::InvalidNonce);
        }

        Ok(())
    }
}

fn hex_encode(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for v in b {
        s.push(HEX[(v >> 4) as usize] as char);
        s.push(HEX[(v & 0x0f) as usize] as char);
    }
    s
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    let s = s.as_bytes();
    if !s.len().is_multiple_of(2) {
        return None;
    }
    s.chunks(2)
        .map(|p| Some(digit(p[0])? << 4 | digit(p[1])?))
        .collect()
}

#[cfg(test)]
#[path = "nonce_test.rs"]
mod nonce_test;
