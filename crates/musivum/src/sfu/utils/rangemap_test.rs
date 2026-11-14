// Ported from LiveKit, pkg/sfu/utils/rangemap_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `require.ErrorIs` for the error that carries details
// is a match on its variant.

use super::*;

#[test]
fn test_range_map_uint32() {
    let mut r = RangeMap::<u32, u32>::new(2);

    // getting value for any key should be 0 default
    let value = r.get_value(33333);
    assert_eq!(Ok(0u32), value);

    let expected_ranges: Vec<RangeVal<u32, u32>> = vec![RangeVal {
        start: 0,
        end: 0,
        value: 0,
    }];
    assert_eq!(expected_ranges, r.ranges);

    // add an exclusion, should create a new range
    let err = r.exclude_range(10, 11);
    assert_eq!(Ok(()), err);

    let expected_ranges: Vec<RangeVal<u32, u32>> = vec![
        RangeVal {
            start: 0,
            end: 9,
            value: 0,
        },
        RangeVal {
            start: 11,
            end: 0,
            value: 1,
        },
    ];
    assert_eq!(expected_ranges, r.ranges);

    // getting value in old range should return 0
    let value = r.get_value(6);
    assert_eq!(Ok(0u32), value);

    // newer should return 1
    let value = r.get_value(11);
    assert_eq!(Ok(1u32), value);

    // excluded range should return error
    let err = r.get_value(10);
    assert_eq!(Err(Error::KeyExcluded), err);

    // out-of-order exclusion should return error
    let err = r.exclude_range(9, 10);
    assert!(matches!(err, Err(Error::ReversedOrder(_))), "{err:?}");

    // flipped exclusion should return error
    let err = r.exclude_range(12, 11);
    assert!(matches!(err, Err(Error::ReversedOrder(_))), "{err:?}");
    let err = r.exclude_range(11, 11);
    assert!(matches!(err, Err(Error::ReversedOrder(_))), "{err:?}");

    // add adjacent exclusion range of length = 1
    let err = r.exclude_range(11, 12);
    assert_eq!(Ok(()), err);

    let expected_ranges: Vec<RangeVal<u32, u32>> = vec![
        RangeVal {
            start: 0,
            end: 9,
            value: 0,
        },
        RangeVal {
            start: 12,
            end: 0,
            value: 2,
        },
    ];
    assert_eq!(expected_ranges, r.ranges);

    // excluded range should return error, now is excluded because exclusion range could be extended
    let err = r.get_value(11);
    assert_eq!(Err(Error::KeyExcluded), err);

    // getting value in old range should return 0
    let err = r.get_value(6);
    assert!(err.is_ok(), "{err:?}");

    // newer should return 2
    let value = r.get_value(12);
    assert_eq!(Ok(2u32), value);

    // add adjacent exclusion range of length = 10
    let err = r.exclude_range(12, 22);
    assert_eq!(Ok(()), err);

    let expected_ranges: Vec<RangeVal<u32, u32>> = vec![
        RangeVal {
            start: 0,
            end: 9,
            value: 0,
        },
        RangeVal {
            start: 22,
            end: 0,
            value: 12,
        },
    ];
    assert_eq!(expected_ranges, r.ranges);

    // excluded range should return error, now is excluded because exclusion range could be extended
    let err = r.get_value(15);
    assert_eq!(Err(Error::KeyExcluded), err);

    // newer should return 12
    let value = r.get_value(25);
    assert_eq!(Ok(12u32), value);

    // add a disjoint exclusion of length = 4
    let err = r.exclude_range(26, 30);
    assert_eq!(Ok(()), err);

    let expected_ranges: Vec<RangeVal<u32, u32>> = vec![
        RangeVal {
            start: 0,
            end: 9,
            value: 0,
        },
        RangeVal {
            start: 22,
            end: 25,
            value: 12,
        },
        RangeVal {
            start: 30,
            end: 0,
            value: 16,
        },
    ];
    assert_eq!(expected_ranges, r.ranges);

    // get a value from newly closed range [22, 25]
    let value = r.get_value(23);
    assert_eq!(Ok(12u32), value);

    // add a disjoint exclusion of length = 1
    let err = r.exclude_range(50, 51);
    assert_eq!(Ok(()), err);

    // previously first range would have been pruned due to size limitations
    let expected_ranges: Vec<RangeVal<u32, u32>> = vec![
        RangeVal {
            start: 22,
            end: 25,
            value: 12,
        },
        RangeVal {
            start: 30,
            end: 49,
            value: 16,
        },
        RangeVal {
            start: 51,
            end: 0,
            value: 17,
        },
    ];
    assert_eq!(expected_ranges, r.ranges);

    // excluded range should return error
    let err = r.get_value(50);
    assert_eq!(Err(Error::KeyExcluded), err);
    let err = r.get_value(28);
    assert_eq!(Err(Error::KeyExcluded), err);
    let err = r.get_value(17);
    assert_eq!(Err(Error::KeyTooOld), err);

    // previously valid, but aged out key should return error
    let err = r.get_value(5);
    assert_eq!(Err(Error::KeyTooOld), err);

    // valid range access should return values
    let value = r.get_value(24);
    assert_eq!(Ok(12u32), value);

    let value = r.get_value(34);
    assert_eq!(Ok(16u32), value);

    let value = r.get_value(49);
    assert_eq!(Ok(16u32), value);

    let value = r.get_value(55555555);
    assert_eq!(Ok(17u32), value);

    // reset
    r.clear_and_reset_value(24, 23);
    let expected_ranges: Vec<RangeVal<u32, u32>> = vec![RangeVal {
        start: 24,
        end: 0,
        value: 23,
    }];
    assert_eq!(expected_ranges, r.ranges);

    let value = r.get_value(55555555);
    assert_eq!(Ok(23u32), value);

    // decrement value and ensure that any key after start in ClearAndResetValue above returns that value
    // (as given end is higher than open range start, open range should be closed and a new range added)
    r.dec_value(34, 12);

    let expected_ranges: Vec<RangeVal<u32, u32>> = vec![
        RangeVal {
            start: 24,
            end: 34,
            value: 23,
        },
        RangeVal {
            start: 35,
            end: 0,
            value: 11,
        },
    ];
    assert_eq!(expected_ranges, r.ranges);

    let value = r.get_value(55555555);
    assert_eq!(Ok(11u32), value);

    // add an exclusion and then decrement value
    let err = r.exclude_range(40, 45);
    assert_eq!(Ok(()), err);

    let expected_ranges: Vec<RangeVal<u32, u32>> = vec![
        RangeVal {
            start: 24,
            end: 34,
            value: 23,
        },
        RangeVal {
            start: 35,
            end: 39,
            value: 11,
        },
        RangeVal {
            start: 45,
            end: 0,
            value: 16,
        },
    ];
    assert_eq!(expected_ranges, r.ranges);

    // before first range access
    let err = r.get_value(5);
    assert_eq!(Err(Error::KeyTooOld), err);

    // first range access
    let value = r.get_value(25);
    assert_eq!(Ok(23u32), value);

    // second range access
    let value = r.get_value(35);
    assert_eq!(Ok(11u32), value);

    // open range access
    let value = r.get_value(55555555);
    assert_eq!(Ok(16u32), value);

    r.dec_value(66, 6);

    let expected_ranges: Vec<RangeVal<u32, u32>> = vec![
        RangeVal {
            start: 35,
            end: 39,
            value: 11,
        },
        RangeVal {
            start: 45,
            end: 66,
            value: 16,
        },
        RangeVal {
            start: 67,
            end: 0,
            value: 10,
        },
    ];
    assert_eq!(expected_ranges, r.ranges);

    // aged out range access
    let err = r.get_value(25);
    assert_eq!(Err(Error::KeyTooOld), err);

    // access closed range before decrementing value
    let value = r.get_value(66);
    assert_eq!(Ok(16u32), value);

    // open range access
    let value = r.get_value(67);
    assert_eq!(Ok(10u32), value);

    // decrement with old end and check that open range gets decremented
    r.dec_value(66, 6);

    let expected_ranges: Vec<RangeVal<u32, u32>> = vec![
        RangeVal {
            start: 35,
            end: 39,
            value: 11,
        },
        RangeVal {
            start: 45,
            end: 66,
            value: 16,
        },
        RangeVal {
            start: 67,
            end: 0,
            value: 4,
        },
    ];
    assert_eq!(expected_ranges, r.ranges);

    // open range access should get decremented value
    let value = r.get_value(67);
    assert_eq!(Ok(4u32), value);
}
