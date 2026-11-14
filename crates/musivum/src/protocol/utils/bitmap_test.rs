// Ported from LiveKit's protocol, utils/bitmap_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `BenchmarkBitmap` is left out. The tests after
// `test_bitmap` are new and say so.

use super::*;

#[test]
fn test_bitmap() {
    let mut b = Bitmap::<u32>::new(153);
    assert_eq!(4, b.bits.capacity()); // rounded up to next power of 2, i. e. 256
    assert_eq!(4, b.bits.len());

    let mut e = vec![0u64; 4];
    assert_eq!(e, b.bits);

    b.set(666666);
    assert!(b.is_set(666666));
    assert!(b.is_set(666666 + 256)); // aliased
    assert!(b.is_set(666666 - 256)); // aliased
    assert!(!b.is_set(666666 - 1));
    assert!(!b.is_set(666666 + 1));

    e.copy_from_slice(&b.bits);
    b.set(42); // same as above - aliased
    assert_eq!(e, b.bits);

    // same slot range
    b.set_range(24, 63);
    // different slot range
    b.set_range(64, 240);
    assert!(!b.is_set(241));
    assert!(!b.is_set(23));
    assert!(b.is_set(24));
    assert!(b.is_set(240));
    e[0] = 0xFFFF_FFFF_FF00_0000;
    e[1] = !0u64;
    e[2] = !0u64;
    e[3] = 0x0001_FFFF_FFFF_FFFF;
    assert_eq!(e, b.bits);

    b.clear(6700); // aliases to 44
    e[0] = 0xFFFF_EFFF_FF00_0000;
    assert_eq!(e, b.bits);
    assert!(!b.is_set(44));

    // same slot range
    b.clear_range(24, 34);
    e[0] = 0xFFFF_EFF8_0000_0000;
    assert_eq!(e, b.bits);
    assert!(!b.is_set(24));
    assert!(b.is_set(35));

    // different slot range
    b.clear_range(95, 234);
    e[1] = 0x0000_0000_7FFF_FFFF;
    e[2] = 0x0;
    e[3] = 0x0001_F800_0000_0000;
    assert_eq!(e, b.bits);
    assert!(b.is_set(94));
    assert!(!b.is_set(95));
    assert!(!b.is_set(234));
    assert!(b.is_set(235));

    // set large range
    b.set_range(0, 1000);
    e[0] = 0xFFFF_FFFF_FFFF_FFFF;
    e[1] = 0xFFFF_FFFF_FFFF_FFFF;
    e[2] = 0xFFFF_FFFF_FFFF_FFFF;
    e[3] = 0xFFFF_FFFF_FFFF_FFFF;
    assert_eq!(e, b.bits);

    // clear large range
    b.clear_range(0, 1000);
    e[0] = 0x0000_0000_0000_0000;
    e[1] = 0x0000_0000_0000_0000;
    e[2] = 0x0000_0000_0000_0000;
    e[3] = 0x0000_0000_0000_0000;
    assert_eq!(e, b.bits);

    // large range changes touch each word once
    let (sm, ls, rs, lo, ro) = b.get_slots_and_offsets(0, u32::MAX);
    assert_eq!(vec![3, 0, 4, 0, 0], vec![sm, ls, rs, lo, ro]);
}

// New for musivum: the size is rounded up as the original rounds it, which for most sizes is
// to more than the next power of two (values run at the pin).
#[test]
fn the_size_is_rounded_up_as_the_original_rounds_it() {
    let sizes = [
        (0, 64),
        (1, 128),
        (63, 128),
        (64, 128),
        (65, 256),
        (127, 256),
        (128, 256),
        (129, 256),
        (153, 256),
        (192, 256),
        (193, 512),
        (256, 512),
        (257, 512),
        (1000, 2048),
        (4096, 8192),
        (4097, 8192),
        (65536, 131072),
    ];
    for (size, len) in sizes {
        assert_eq!(len, Bitmap::<u16>::new(size).len(), "size {size}");
    }
}

// New for musivum: ranges that begin or end at the edge of a word, where the original shifts
// by 64 and Go gives zero for that; ranges that go around the end of the bitmap; the largest
// values (values run at the pin).
#[test]
fn ranges_at_the_edges_of_words_and_of_the_bitmap() {
    const ALL: u64 = 0xffff_ffff_ffff_ffff;

    let mut b = Bitmap::<u64>::new(128);
    b.set_range(0, 63);
    assert_eq!(vec![ALL, 0, 0, 0], b.bits);
    b.set_range(100, 127);
    assert_eq!(vec![ALL, 0xffff_fff0_0000_0000, 0, 0], b.bits);
    b.set_range(190, 255);
    assert_eq!(
        vec![ALL, 0xffff_fff0_0000_0000, 0xc000_0000_0000_0000, ALL],
        b.bits
    );
    b.clear_range(64, 127);
    assert_eq!(vec![ALL, 0, 0xc000_0000_0000_0000, ALL], b.bits);
    b.clear_range(0, 63);
    assert_eq!(vec![0, 0, 0xc000_0000_0000_0000, ALL], b.bits);
    b.clear_range(192, 255);
    assert_eq!(vec![0, 0, 0xc000_0000_0000_0000, 0], b.bits);
    b.clear_range(128, 191);
    assert_eq!(vec![0, 0, 0, 0], b.bits);

    // around the end of the bitmap, and the largest values
    b.set_range(250, 260);
    assert_eq!(vec![0x1f, 0, 0, 0xfc00_0000_0000_0000], b.bits);
    b.set_range(u64::MAX - 3, u64::MAX);
    assert_eq!(vec![0x1f, 0, 0, 0xfc00_0000_0000_0000], b.bits);
    assert!(b.is_set(u64::MAX));
    assert!(!b.get_and_set(7));
    assert!(b.get_and_set(7));
    // a range that ends before it begins is no range
    b.clear_range(5, 4);
    b.set_range(5, 4);
    assert_eq!(vec![0x9f, 0, 0, 0xfc00_0000_0000_0000], b.bits);

    b.clear_range(0, u64::MAX);
    assert_eq!(vec![0, 0, 0, 0], b.bits);
    // exactly as many words apart as the bitmap has: the first word is written to twice
    b.set_range(10, 10 + 256);
    assert_eq!(vec![ALL; 4], b.bits);
    b.clear_range(70, 70 + 256);
    assert_eq!(vec![0, 0, 0, 0], b.bits);
    // and then the bits between the end and the beginning are left as they are
    b.set_range(10, 261);
    assert_eq!(vec![0xffff_ffff_ffff_fc3f, ALL, ALL, ALL], b.bits);
    b.set_range(0, 1000);
    b.clear_range(70, 321);
    assert_eq!(vec![0, 0x3c, 0, 0], b.bits);
    assert_eq!((3, 0, 4, 10, 5), b.get_slots_and_offsets(10, 261));
    // one word more and every word is written once
    assert_eq!((3, 0, 4, 0, 0), b.get_slots_and_offsets(10, 330));

    let mut b8 = Bitmap::<u8>::new(64);
    b8.set_range(0, 255);
    assert_eq!(vec![ALL; 2], b8.bits);
    assert_eq!(128, b8.len());
    b8.clear(200);
    assert_eq!(vec![ALL, 0xffff_ffff_ffff_feff], b8.bits);
}
