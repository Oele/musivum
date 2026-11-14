// Ported from LiveKit's protocol, utils/guid/id_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `TestHash` is not ported, because `Hash` is not (see
// `id.rs`). `BenchmarkNew` is left out. The tests after `test_is_valid_id` are new and say so.

use std::collections::VecDeque;
use std::convert::Infallible;

use rand::TryRng;

use super::*;

#[test]
fn test_marshal_unmarshal() {
    let id0 = livekit::TrackId::from(new(TRACK_PREFIX));
    let b0 = marshal(&id0);
    let id1 = unmarshal::<livekit::TrackId>(b0);
    let b1 = marshal(&id1);
    assert_eq!(id0, id1);
    assert_eq!(b0, b1);
}

#[test]
fn test_marshal_append() {
    let id0 = livekit::RoomId::from(new(ROOM_PREFIX));
    let id1 = livekit::RoomId::from(new(ROOM_PREFIX));
    let mut b = Vec::new();
    b = marshal_append(b, &id0);
    b = marshal_append(b, &id1);
    assert_eq!(
        id0,
        unmarshal::<livekit::RoomId>(b[0..9].try_into().unwrap())
    );
    assert_eq!(
        id1,
        unmarshal::<livekit::RoomId>(b[9..18].try_into().unwrap())
    );
}

// `TestHash` is not ported: it expects the id that Go's `math/rand/v2.ChaCha8` gives when it
// is seeded with the SHA-256 of "test", and `Hash` is left out together with that generator.

#[test]
fn test_is_valid_id() {
    assert!(is_valid_id("A_SFo4igEG5Dg5"));
    assert!(is_valid_id("NM_OJOHANNESBURG1A_K6SMQw2ZCZyB"));
    assert!(!is_valid_id("A_A_A_SFo4igEG5Dg5"));
    assert!(!is_valid_id("_A_SFo4igEG5Dg5"));
    assert!(!is_valid_id("_SFo4igEG5Dg5"));
    assert!(!is_valid_id("SFo4igEG5Dg5"));
}

// New for musivum: the edges of the pattern, which the original leaves to the regular
// expression.
#[test]
fn the_pattern_of_an_id_is_matched_to_its_ends() {
    // a group has 1 to 16 characters, the last part 0 to 12
    assert!(is_valid_id("A_"));
    assert!(is_valid_id("1234567890123456_123456789012"));
    assert!(!is_valid_id("12345678901234567_123456789012"));
    assert!(!is_valid_id("1234567890123456_1234567890123"));
    assert!(!is_valid_id(""));
    assert!(!is_valid_id("_"));
    assert!(!is_valid_id("A_B_C_"));
    assert!(!is_valid_id("A-B_abc"));
    assert!(!is_valid_id("A_abc\n"));
    assert!(!is_valid_id("A_äbc"));
}

// New for musivum: an id is its prefix and twelve characters of the alphabet, and no two are
// the same.
#[test]
fn a_new_id_is_a_prefix_and_twelve_characters() {
    let mut seen = std::collections::HashSet::new();
    for prefix in [TRACK_PREFIX, PARTICIPANT_PREFIX, DATA_TRACK_PREFIX, ""] {
        for _ in 0..1000 {
            let id = new(prefix);
            assert_eq!(prefix.len() + SIZE, id.len());
            assert!(id.starts_with(prefix));
            assert!(
                id[prefix.len()..].bytes().all(|c| B57_CHARS.contains(&c)),
                "{id}"
            );
            assert!(seen.insert(id));
        }
    }
    // another thread has a generator of its own
    let other = std::thread::spawn(|| new(TRACK_PREFIX)).join().unwrap();
    assert!(seen.insert(other));
}

// New for musivum: a generator that gives the numbers the test chooses.
struct Numbers(VecDeque<u64>);

impl TryRng for Numbers {
    type Error = Infallible;

    fn try_next_u32(&mut self) -> Result<u32, Infallible> {
        unreachable!("an id is made of 64 bit numbers")
    }

    fn try_next_u64(&mut self) -> Result<u64, Infallible> {
        Ok(self.0.pop_front().expect("a number is left"))
    }

    fn try_fill_bytes(&mut self, _dst: &mut [u8]) -> Result<(), Infallible> {
        unreachable!("an id is made of 64 bit numbers")
    }
}

// Ten groups of six bits, the first in the lowest bits, with the four bits that are left set.
fn number(groups: [u64; 10]) -> u64 {
    let mut r = 0xf << 60;
    for (i, group) in groups.into_iter().enumerate() {
        assert!(group < 64);
        r |= group << (6 * i);
    }
    r
}

// New for musivum: how the characters of an id are taken from the random numbers: six bits
// at a time from the low end, ten times for each number, and a value that the alphabet has no
// character for is passed over.
#[test]
fn the_characters_of_an_id_are_six_bits_each_of_the_random_numbers() {
    let mut g = GuidGenerator {
        rng: Numbers(VecDeque::from([
            number([0, 1, 56, 57, 63, 2, 58, 3, 4, 5]),
            number([60, 6, 7, 8, 9, 10, 11, 12, 13, 14]),
            number([15; 10]),
            number([16; 10]),
        ])),
    };
    assert_eq!("TR_23z456789ABC", g.new(TRACK_PREFIX));
    // what is left of the second number is not used for the next id
    assert_eq!(2, g.rng.0.len());
    assert_eq!("HHHHHHHHHHJJ", g.new(""));
    assert_eq!(0, g.rng.0.len());
}

// New for musivum: values the original gives (run at the pin).
#[test]
fn hashed_ids_and_blocks_are_the_ones_the_original_gives() {
    assert_eq!("JcA2vCJGgV57ftKZaYp18y9RzRb", hashed_id(""));
    assert_eq!("Tv7LYew0jc65QgJOMdzYZu8HVKV", hashed_id("test"));
    assert_eq!("lVdzxQBzHYHofEd5YyJiyXEbd3C", hashed_id("livekit"));
    assert_eq!(
        "OAIYW2mAr97Gt1yYPILkPZmc8HD",
        hashed_id("a-somewhat-longer-identifier@example.org")
    );

    let id = livekit::TrackId::from("TR_SFo4igEG5Dg5");
    let block = marshal(&id);
    assert_eq!(
        [0x60, 0xdb, 0x42, 0xa2, 0x63, 0x0e, 0x0c, 0xb9, 0x83],
        block
    );
    assert_eq!(id, unmarshal::<livekit::TrackId>(block));
    assert_eq!(
        livekit::ParticipantId::from("PA_SFo4igEG5Dg5"),
        unmarshal::<livekit::ParticipantId>(block)
    );
}
