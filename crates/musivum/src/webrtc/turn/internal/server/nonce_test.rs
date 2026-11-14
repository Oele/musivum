// Ported from pion/turn, internal/server/nonce_test.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust.

use super::*;
use crate::webrtc::turn::internal::server::new_short_nonce_hash;

#[test]
fn test_nonce_hash() {
    // generated hashes validate
    {
        let h = new_nonce_hash().unwrap();
        let nonce = h.generate().unwrap();
        assert!(h.validate(&nonce).is_ok(), "generated hashes validate");
    }

    // generated short hashes validate
    for i in 1..=8 {
        let h = new_short_nonce_hash(i * 4)
            .unwrap_or_else(|e| panic!("nonce init at size {}: {e}", i * 4));
        let nonce = h
            .generate()
            .unwrap_or_else(|e| panic!("generate at size {}: {e}", i * 4));
        assert!(h.validate(&nonce).is_ok(), "decode at size {}", i * 4);
    }
}
