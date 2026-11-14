// Ported from LiveKit, pkg/utils/protocol.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the pointer that may be nil is an `Option`.

use crate::protocol::livekit;

pub fn client_info_without_address(c: Option<&livekit::ClientInfo>) -> Option<livekit::ClientInfo> {
    let c = c?;
    let mut clone = c.clone();
    clone.address = String::new();
    Some(clone)
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: upstream has no test for this file.
    #[test]
    fn the_address_is_left_out_and_nothing_else() {
        assert_eq!(None, client_info_without_address(None));

        let c = livekit::ClientInfo {
            version: "2.1.0".to_string(),
            browser: "firefox".to_string(),
            address: "192.0.2.7".to_string(),
            ..Default::default()
        };
        let clone = client_info_without_address(Some(&c)).unwrap();
        assert_eq!("", clone.address);
        assert_eq!(
            livekit::ClientInfo {
                address: String::new(),
                ..c.clone()
            },
            clone
        );
        assert_eq!("192.0.2.7", c.address);
    }
}
