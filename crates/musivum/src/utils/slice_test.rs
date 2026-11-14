// Ported from LiveKit, pkg/utils/slice_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; Go's `int` is `i64` here.

use super::*;

#[test]
fn test_dedupe_slice() {
    // Empty slice
    {
        let input: Vec<i64> = Vec::new();
        let result = dedupe_slice(input);
        assert!(result.is_empty());
    }

    // Single element
    {
        let input = vec!["hello"];
        let result = dedupe_slice(input);
        assert_eq!(vec!["hello"], result);
    }

    // Unsorted slice with duplicates
    {
        let input = vec![4i64, 2, 4, 1, 3, 2];
        let result = dedupe_slice(input);
        assert_eq!(vec![1i64, 2, 3, 4], result);
    }

    // Already sorted and unique
    {
        let input = vec!["apple", "banana", "cherry"];
        let result = dedupe_slice(input);
        assert_eq!(vec!["apple", "banana", "cherry"], result);
    }
}
