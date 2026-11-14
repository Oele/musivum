// Ported from LiveKit's protocol, utils/secret.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the random bytes are the operating system's
// (`rand::rngs::SysRng`), as Go's `crypto/rand` are.

use rand::TryRng;
use rand::rngs::SysRng;

use super::base62;

pub fn random_secret() -> String {
    // 256 bit secret
    let mut buf = [0u8; 32];
    // cannot error
    if SysRng.try_fill_bytes(&mut buf).is_err() {
        panic!("could not read random");
    }
    base62::encode_to_string(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: upstream has no test for this file.
    #[test]
    fn a_secret_is_32_random_bytes_in_base62() {
        let a = random_secret();
        let b = random_secret();
        assert_ne!(a, b);
        for s in [a, b] {
            // 256 bits at five or six bits a character
            assert!((43..=52).contains(&s.len()), "{s}");
            assert!(s.bytes().all(|c| c.is_ascii_alphanumeric()), "{s}");
        }
    }
}
