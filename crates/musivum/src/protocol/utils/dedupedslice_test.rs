// Ported from LiveKit's protocol, utils/dedupedslice_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; Go's `int` is `i64` here; where the original expects a
// nil slice after `Clear`, the slice is empty.

use super::*;

#[test]
fn test_deduped_slice() {
    // no_overflow
    {
        let mut ds_int = DedupedSlice::<i64>::new(10);
        for (idx, v) in [1, 2, 3, 4, 6, 4, 7].into_iter().enumerate() {
            if idx == 5 {
                assert!(!ds_int.add(v));
            } else {
                assert!(ds_int.add(v));
            }
        }
        assert_eq!([1, 2, 3, 4, 6, 7], ds_int.get());
        assert_eq!(6, ds_int.len());
        assert!(ds_int.has(&2));
        assert!(!ds_int.has(&8));

        let mut ds_string = DedupedSlice::<String>::new(10);
        for (idx, v) in ["one", "two", "ten", "six", "six", "four"]
            .into_iter()
            .enumerate()
        {
            if idx == 4 {
                assert!(!ds_string.add(v.to_string()));
            } else {
                assert!(ds_string.add(v.to_string()));
            }
        }
        assert_eq!(["one", "two", "ten", "six", "four"], ds_string.get());
        assert_ne!(
            ["one", "two", "ten", "six", "six", "four"].as_slice(),
            ds_string.get()
        );
        assert_eq!(5, ds_string.len());
        assert!(ds_string.has(&"two".to_string()));
        assert!(!ds_string.has(&"eight".to_string()));

        ds_string.clear();
        assert!(ds_string.get().is_empty());
        assert_eq!(0, ds_string.len());
    }

    // max_len
    {
        let mut ds_int = DedupedSlice::<i64>::new(5);
        for v in [1, 2, 3, 4, 6, 4, 7] {
            ds_int.add(v);
        }
        assert_eq!([2, 3, 4, 6, 7], ds_int.get());
        assert_eq!(5, ds_int.len());
        assert!(ds_int.has(&2));
        assert!(!ds_int.has(&1));
    }
}
