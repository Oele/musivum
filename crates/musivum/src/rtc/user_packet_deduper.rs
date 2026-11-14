// Ported from LiveKit, pkg/rtc/user_packet_deduper.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `uuid.UUID` is the 16 bytes it is, and
// `uuid.FromBytes`, which only checks that there are 16 of them, the conversion of the nonce
// to such an array; what `lock` guards, which is everything, is in an inner struct. The tests
// at the end are new: the original has none.

use std::collections::HashMap;

use parking_lot::Mutex;

use crate::protocol::livekit;

const MAX_SIZE: usize = 100;

// uuid.UUID
type Uuid = [u8; 16];

pub struct UserPacketDeduper {
    // what `lock sync.Mutex` guards upstream
    inner: Mutex<UserPacketDeduperInner>,
}

struct UserPacketDeduperInner {
    seen: HashMap<Uuid, Uuid>,
    head: Uuid,
    tail: Uuid,
}

impl UserPacketDeduper {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(UserPacketDeduperInner {
                seen: HashMap::new(),
                head: Uuid::default(),
                tail: Uuid::default(),
            }),
        }
    }

    pub fn is_duplicate(&self, up: &livekit::UserPacket) -> bool {
        let Ok(id) = Uuid::try_from(up.nonce.as_slice()) else {
            return false;
        };

        let mut u = self.inner.lock();

        if u.head == id {
            return true;
        }
        if u.seen.contains_key(&id) {
            return true;
        }

        let head = u.head;
        u.seen.insert(head, id);
        u.head = id;

        if u.seen.len() == MAX_SIZE {
            let tail = u.tail;
            // a key that is not in a Go map reads as the zero value
            u.tail = u.seen.get(&tail).copied().unwrap_or_default();
            u.seen.remove(&tail);
        }
        false
    }
}

impl Default for UserPacketDeduper {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no tests. The expectations of these tests are what
    // the original gives for the same packets (printed by a Go test at the pin).

    fn packet(i: usize) -> livekit::UserPacket {
        let mut nonce = vec![0u8; 16];
        nonce[0] = (i >> 8) as u8;
        nonce[1] = i as u8;
        nonce[15] = 1;
        livekit::UserPacket {
            nonce,
            ..Default::default()
        }
    }

    fn packet_with_nonce(nonce: Vec<u8>) -> livekit::UserPacket {
        livekit::UserPacket {
            nonce,
            ..Default::default()
        }
    }

    #[test]
    fn a_packet_seen_before_is_a_duplicate() {
        let d = UserPacketDeduper::new();

        assert!(!d.is_duplicate(&packet(1)));
        assert!(d.is_duplicate(&packet(1)));
        assert!(d.is_duplicate(&packet(1)));

        assert!(!d.is_duplicate(&packet(2)));
        // the one before the last, and the last
        assert!(d.is_duplicate(&packet(1)));
        assert!(d.is_duplicate(&packet(2)));

        // the payload does not count, only the nonce
        let mut other = packet(3);
        other.payload = b"one".to_vec();
        assert!(!d.is_duplicate(&other));
        other.payload = b"another".to_vec();
        assert!(d.is_duplicate(&other));
    }

    #[test]
    fn a_nonce_that_is_not_16_bytes_is_never_a_duplicate() {
        let d = UserPacketDeduper::new();

        for len in [0, 1, 15, 17, 32] {
            assert!(
                !d.is_duplicate(&packet_with_nonce(vec![0; len])),
                "{len} bytes"
            );
            assert!(
                !d.is_duplicate(&packet_with_nonce(vec![0; len])),
                "{len} bytes again"
            );
            assert!(
                !d.is_duplicate(&packet_with_nonce(vec![7; len])),
                "{len} bytes"
            );
            assert!(
                !d.is_duplicate(&packet_with_nonce(vec![7; len])),
                "{len} bytes again"
            );
        }
        assert_eq!(0, d.inner.lock().seen.len());
    }

    #[test]
    fn the_last_hundred_packets_are_remembered() {
        let d = UserPacketDeduper::new();

        for i in 1..=100 {
            assert!(!d.is_duplicate(&packet(i)), "packet {i}");
        }
        for i in 1..=100 {
            assert!(d.is_duplicate(&packet(i)), "packet {i}");
        }
        assert_eq!(99, d.inner.lock().seen.len());

        // one more, and the oldest is forgotten
        assert!(!d.is_duplicate(&packet(101)));
        assert_eq!(99, d.inner.lock().seen.len());
        assert!(d.is_duplicate(&packet(2)));
        assert!(!d.is_duplicate(&packet(1)));
        assert_eq!(99, d.inner.lock().seen.len());

        // which made it the newest, at the cost of the next oldest
        assert!(d.is_duplicate(&packet(1)));
        assert!(!d.is_duplicate(&packet(2)));
        assert!(!d.is_duplicate(&packet(3)));
    }

    // As upstream: the list starts at the nonce of 16 zero bytes, so a packet with that nonce
    // is a duplicate from the start, until a hundred others have been seen.
    #[test]
    fn a_nonce_of_zeroes_is_a_duplicate_from_the_start() {
        let d = UserPacketDeduper::new();
        let zero = packet_with_nonce(vec![0; 16]);

        assert!(d.is_duplicate(&zero));

        for i in 1..=99 {
            assert!(!d.is_duplicate(&packet(i)), "packet {i}");
        }
        assert_eq!(99, d.inner.lock().seen.len());
        assert!(d.is_duplicate(&zero));

        assert!(!d.is_duplicate(&packet(100)));
        assert_eq!(99, d.inner.lock().seen.len());
        assert!(!d.is_duplicate(&zero));
        assert_eq!(99, d.inner.lock().seen.len());

        assert!(!d.is_duplicate(&packet(1)));
        assert!(!d.is_duplicate(&packet(2)));
        assert!(!d.is_duplicate(&packet(3)));
        assert_eq!(99, d.inner.lock().seen.len());
    }
}
