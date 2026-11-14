// Ported from LiveKit, pkg/sfu/buffer/frameintegrity_test.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the shuffle of the packets of a frame is seeded with
// the frame number as in the original, but from a small generator of this file instead of
// `math/rand`, so the orders are other ones than the original's (what is asserted holds for
// every order).

use super::*;

// Stands in for `rand.New(rand.NewSource(seed))` and its `Shuffle`.
struct Rng(u64);

impl Rng {
    fn new(seed: i64) -> Self {
        Self(seed as u64)
    }

    // splitmix64
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn shuffle(&mut self, n: usize, mut swap: impl FnMut(usize, usize)) {
        for i in (1..n).rev() {
            let j = (self.next() % (i as u64 + 1)) as usize;
            swap(i, j);
        }
    }
}

#[test]
fn test_frame_integrity_checker() {
    let mut fc = FrameIntegrityChecker::new(100, 1000);

    // first frame out of order
    fc.add_packet(10, 10, &dd::DependencyDescriptor::default());
    assert!(!fc.frame_integrity(10));
    fc.add_packet(
        9,
        10,
        &dd::DependencyDescriptor {
            first_packet_in_frame: true,
            ..Default::default()
        },
    );
    assert!(!fc.frame_integrity(10));
    fc.add_packet(
        11,
        10,
        &dd::DependencyDescriptor {
            last_packet_in_frame: true,
            ..Default::default()
        },
    );
    assert!(fc.frame_integrity(10));

    // single packet frame
    fc.add_packet(
        100,
        100,
        &dd::DependencyDescriptor {
            first_packet_in_frame: true,
            last_packet_in_frame: true,
            ..Default::default()
        },
    );
    assert!(fc.frame_integrity(100));
    assert!(!fc.frame_integrity(101));
    assert!(!fc.frame_integrity(99));

    // frame too old than first frame
    fc.add_packet(
        99,
        99,
        &dd::DependencyDescriptor {
            first_packet_in_frame: true,
            last_packet_in_frame: true,
            ..Default::default()
        },
    );

    // multiple packet frame, out of order
    fc.add_packet(2001, 2001, &dd::DependencyDescriptor::default());
    assert!(!fc.frame_integrity(2001));
    assert!(!fc.frame_integrity(1999));
    // out of frame count(100)
    assert!(!fc.frame_integrity(100));
    assert!(!fc.frame_integrity(1900));

    fc.add_packet(
        2000,
        2001,
        &dd::DependencyDescriptor {
            first_packet_in_frame: true,
            ..Default::default()
        },
    );
    assert!(!fc.frame_integrity(2001));
    fc.add_packet(
        2002,
        2001,
        &dd::DependencyDescriptor {
            last_packet_in_frame: true,
            ..Default::default()
        },
    );
    assert!(fc.frame_integrity(2001));
    // duplicate packet
    fc.add_packet(2001, 2001, &dd::DependencyDescriptor::default());
    assert!(fc.frame_integrity(2001));

    // frame too old
    fc.add_packet(
        900,
        1900,
        &dd::DependencyDescriptor {
            first_packet_in_frame: true,
            last_packet_in_frame: true,
            ..Default::default()
        },
    );
    assert!(!fc.frame_integrity(1900));

    for frame in 2002u64..2102 {
        // large frame (1000 packets) out of order / retransmitted
        let first_frame = 3000 + (frame - 2002) * 1000;
        let last_frame = 3999 + (frame - 2002) * 1000;
        let mut frames = Vec::with_capacity((last_frame - first_frame + 1) as usize);
        for i in first_frame..=last_frame {
            frames.push(i);
        }
        assert!(!fc.frame_integrity(frame));
        let mut rng = Rng::new(frame as i64);
        rng.shuffle(frames.len(), |i, j| frames.swap(i, j));
        for (i, &f) in frames.iter().enumerate() {
            fc.add_packet(
                f,
                frame,
                &dd::DependencyDescriptor {
                    first_packet_in_frame: f == first_frame,
                    last_packet_in_frame: f == last_frame,
                    ..Default::default()
                },
            );
            assert_eq!(i == frames.len() - 1, fc.frame_integrity(frame), "{i}");
        }
        assert!(fc.frame_integrity(frame));
    }
}

fn count_set_bits(ph: &PacketHistory) -> usize {
    let mut n = 0;
    for w in &ph.bits {
        n += w.count_ones() as usize;
    }
    n
}

// A forward sequence-number jump much larger than the ring must clear the whole ring,
// leaving only the newly received sequence number set.
#[test]
fn test_packet_history_large_forward_jump() {
    let mut ph = PacketHistory::new(1000); // rounds up to a multiple of 64
    assert_eq!(1024, ph.packet_count);

    // Fill the entire ring so every slot holds a "received" bit.
    let base = 100000u64;
    ph.add_packet(base);
    for i in base + 1..=base + 2000 {
        ph.add_packet(i);
    }
    assert_eq!(ph.packet_count, count_set_bits(&ph));
    let last = base + 2000;

    // Forward jump well beyond both the ring and the ~32k extension wrap-around cap. The ring
    // must end up fully cleared, with only newSeq marked received.
    let new_seq = last + 40000;
    ph.add_packet(new_seq);

    // If the cap under-cleared, stale bits from the pre-jump fill would survive here.
    assert_eq!(1, count_set_bits(&ph));
    assert!(ph.packets_consecutive(new_seq, new_seq));
    assert!(!ph.packets_consecutive(new_seq - 5, new_seq));

    // The window just below newSeq was cleared and can be refilled normally.
    for i in new_seq - 5..new_seq {
        ph.add_packet(i);
    }
    assert!(ph.packets_consecutive(new_seq - 5, new_seq));
}

// A forward frame-number jump much larger than frameCount must reset the whole frame ring,
// so no frame that aliases an old slot inherits stale integrity.
#[test]
fn test_frame_integrity_checker_large_frame_jump() {
    let mut fc = FrameIntegrityChecker::new(100, 1000);

    // Populate every ring slot with an integral single-packet frame.
    for f in 200u64..=399 {
        fc.add_packet(
            f,
            f,
            &dd::DependencyDescriptor {
                first_packet_in_frame: true,
                last_packet_in_frame: true,
                ..Default::default()
            },
        );
    }
    assert!(fc.frame_integrity(399));

    // Jump far beyond frameCount. The capped reset loop must clear the entire frame ring; if it
    // under-cleared, some aliased slot would still report a stale frame's integrity.
    let new_frame = 399u64 + 5000;
    fc.add_packet(50000, new_frame, &dd::DependencyDescriptor::default()); // incomplete frame, no first/last
    for f in new_frame - fc.frame_count as u64 + 1..=new_frame {
        assert!(
            !fc.frame_integrity(f),
            "frame {f} should not be integral after jump"
        );
    }
}
