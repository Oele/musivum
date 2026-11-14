// Ported from LiveKit, pkg/utils/math_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; Go's `int` is `i64` here.

use super::*;

#[test]
fn test_median() {
    // Empty slice
    {
        assert_eq!(0f32, median::<f32>(&[]));
        assert_eq!(0i64, median::<i64>(&[]));
    }

    // Single element
    {
        assert_eq!(42f32, median(&[42f32]));
        assert_eq!(42i64, median(&[42i64]));
    }

    // Odd length float32
    {
        let input = [3.0f32, 1.0, 2.0];
        assert_eq!(2.0f32, median(&input));
    }

    // Even length float32 - exact average
    {
        let input = [1.0f32, 2.0, 3.0, 4.0];
        assert_eq!(2.5f32, median(&input));
    }

    // Even length int - integer truncation
    {
        let input = [1i64, 2];
        // (1 + 2) / 2 = 1.5 -> truncates to 1
        assert_eq!(1i64, median(&input));

        let input_odd_average = [1i64, 3];
        // (1 + 3) / 2 = 2
        assert_eq!(2i64, median(&input_odd_average));
    }

    // Int8 overflow prevention
    {
        // Without overflow protection: 120 + 126 = 246 (overflows int8 to -10) -> -10 / 2 = -5
        // With overflow protection: 120 + (126-120)/2 = 123
        let input = [120i8, 126];
        assert_eq!(123i8, median(&input));
    }

    // Uint8 overflow prevention
    {
        let input = [250u8, 254];
        assert_eq!(252u8, median(&input));
    }

    // Immutability test - caller slice is not sorted/mutated
    {
        let original = vec![3i64, 1, 4, 2];
        let input = original.clone();

        let median = median(&input);
        assert_eq!(2i64, median);
        assert_eq!(
            original, input,
            "Input slice must not be modified by Median"
        );
    }
}

#[test]
fn test_signum() {
    // Integer values
    {
        assert_eq!(-1, signum(-42i64));
        assert_eq!(0, signum(0i64));
        assert_eq!(1, signum(42i64));
    }

    // Floating point values
    {
        assert_eq!(-1, signum(-0.01f32));
        assert_eq!(0, signum(0.0f32));
        assert_eq!(1, signum(0.01f32));
    }
}
