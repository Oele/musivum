// Ported from pion/interceptor, pkg/flexfec/flexfec_coverage_test.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; the subtests are a loop that carries their names in
// the assertion messages.

use super::*;

#[test]
fn test_mask_extractors() {
    struct Test {
        name: &'static str,
        set_bits: &'static [u32],
        mask1: u16,
        mask2: u32,
        mask3: u64,
        mask3_03: u64,
    }
    let tests = [
        Test {
            name: "Empty mask",
            set_bits: &[],
            mask1: 0,
            mask2: 0,
            mask3: 0,
            mask3_03: 0,
        },
        Test {
            name: "Single bit in each mask",
            set_bits: &[5, 20, 50],
            mask1: 0x200,                // bit 5
            mask2: 0x2000000,            // bit 20
            mask3: 0x800000000000000,    // bit 50
            mask3_03: 0x400000000000000, // bit 50
        },
        Test {
            name: "Multiple bits in each mask",
            set_bits: &[0, 7, 14, 15, 30, 45, 46, 80, 108, 109],
            mask1: 0x4081,                // bits 0, 7, 14
            mask2: 0x40008001,            // bits 15, 30, 45
            mask3: 0x8000000020000003,    // bits 46, 80, 108, 109
            mask3_03: 0x4000000010000001, // bits 46, 80, 108
        },
        Test {
            name: "Boundary values",
            set_bits: &[0, 14, 15, 45, 46, 108, 109],
            mask1: 0x4001,                // bits 0, 14
            mask2: 0x40000001,            // bits 15, 45
            mask3: 0x8000000000000003,    // bits 46, 108, 109
            mask3_03: 0x4000000000000001, // bits 46, 108
        },
    ];

    for tt in tests {
        let mut mask = util::BitArray::default();

        for &bit in tt.set_bits {
            mask.set_bit(bit);
        }

        let actual_mask1 = extract_mask1(mask);
        let actual_mask2 = extract_mask2(mask);
        let actual_mask3 = extract_mask3(mask);
        let actual_mask3_03 = extract_mask3_03(mask);

        assert_eq!(tt.mask1, actual_mask1, "{}: Mask1 mismatch", tt.name);
        assert_eq!(tt.mask2, actual_mask2, "{}: Mask2 mismatch", tt.name);
        assert_eq!(tt.mask3, actual_mask3, "{}: Mask3 mismatch", tt.name);
        assert_eq!(
            tt.mask3_03, actual_mask3_03,
            "{}: Mask3_03 mismatch",
            tt.name
        );
    }
}
