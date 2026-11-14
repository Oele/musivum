// Ported from LiveKit's mediatransportutil, pkg/utils/wraparound_test.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

use super::*;

#[test]
fn test_wrap_around_uint16() {
    let mut w = WrapAround::<u16, u32>::new(WrapAroundParams {
        is_restart_allowed: true,
    });
    struct TestCase {
        name: &'static str,
        input: u16,
        updated: WrapAroundUpdateResult<u32>,
        start: u16,
        extended_start: u32,
        highest: u16,
        extended_highest: u32,
    }
    let test_cases = [
        // initialize
        TestCase {
            name: "initialize",
            input: 10,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: 9,
                extended_val: 10,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: 10,
            extended_highest: 10,
        },
        // an older number without wrap around should reset start point
        TestCase {
            name: "reset start no wrap around",
            input: 8,
            updated: WrapAroundUpdateResult {
                is_restart: true,
                pre_extended_start: 10,
                pre_extended_highest: 10,
                extended_val: 8,
                ..Default::default()
            },
            start: 8,
            extended_start: 8,
            highest: 10,
            extended_highest: 10,
        },
        // an older number with wrap around should reset start point
        TestCase {
            name: "reset start wrap around",
            input: ((1u32 << 16) - 6) as u16,
            updated: WrapAroundUpdateResult {
                is_restart: true,
                pre_extended_start: (1 << 16) + 8,
                pre_extended_highest: (1 << 16) + 10,
                extended_val: (1 << 16) - 6,
                ..Default::default()
            },
            start: ((1u32 << 16) - 6) as u16,
            extended_start: (1 << 16) - 6,
            highest: 10,
            extended_highest: (1 << 16) + 10,
        },
        // an older number with wrap around should reset start point again
        TestCase {
            name: "reset start again",
            input: ((1u32 << 16) - 12) as u16,
            updated: WrapAroundUpdateResult {
                is_restart: true,
                pre_extended_start: (1 << 16) - 6,
                pre_extended_highest: (1 << 16) + 10,
                extended_val: (1 << 16) - 12,
                ..Default::default()
            },
            start: ((1u32 << 16) - 12) as u16,
            extended_start: (1 << 16) - 12,
            highest: 10,
            extended_highest: (1 << 16) + 10,
        },
        // out of order with highest, wrap back, but no restart
        TestCase {
            name: "out of order - no restart",
            input: ((1u32 << 16) - 3) as u16,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: (1 << 16) + 10,
                extended_val: (1 << 16) - 3,
                ..Default::default()
            },
            start: ((1u32 << 16) - 12) as u16,
            extended_start: (1 << 16) - 12,
            highest: 10,
            extended_highest: (1 << 16) + 10,
        },
        // duplicate should return same as highest
        TestCase {
            name: "duplicate",
            input: 10,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: (1 << 16) + 10,
                extended_val: (1 << 16) + 10,
                ..Default::default()
            },
            start: ((1u32 << 16) - 12) as u16,
            extended_start: (1 << 16) - 12,
            highest: 10,
            extended_highest: (1 << 16) + 10,
        },
        // a significant jump in order should not reset start
        TestCase {
            name: "big in-order jump",
            input: (1 << 15) - 10,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: (1 << 16) + 10,
                extended_val: (1 << 16) + (1 << 15) - 10,
                ..Default::default()
            },
            start: ((1u32 << 16) - 12) as u16,
            extended_start: (1 << 16) - 12,
            highest: (1 << 15) - 10,
            extended_highest: (1 << 16) + (1 << 15) - 10,
        },
        // now out-of-order should not reset start as half the range has been seen
        TestCase {
            name: "out-of-order after half range",
            input: (1 << 15) - 11,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: (1 << 16) + (1 << 15) - 10,
                extended_val: (1 << 16) + (1 << 15) - 11,
                ..Default::default()
            },
            start: ((1u32 << 16) - 12) as u16,
            extended_start: (1 << 16) - 12,
            highest: (1 << 15) - 10,
            extended_highest: (1 << 16) + (1 << 15) - 10,
        },
        // wrap back out-of-order
        TestCase {
            name: "wrap back out-of-order after half range",
            input: ((1u32 << 16) - 1) as u16,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: (1 << 16) + (1 << 15) - 10,
                extended_val: (1 << 16) - 1,
                ..Default::default()
            },
            start: ((1u32 << 16) - 12) as u16,
            extended_start: (1 << 16) - 12,
            highest: (1 << 15) - 10,
            extended_highest: (1 << 16) + (1 << 15) - 10,
        },
        // in-order, should update highest
        TestCase {
            name: "in-order",
            input: (1 << 15) + 3,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: (1 << 16) + (1 << 15) - 10,
                extended_val: (1 << 16) + (1 << 15) + 3,
                ..Default::default()
            },
            start: ((1u32 << 16) - 12) as u16,
            extended_start: (1 << 16) - 12,
            highest: (1 << 15) + 3,
            extended_highest: (1 << 16) + (1 << 15) + 3,
        },
    ];

    for tc in test_cases {
        assert_eq!(tc.updated, w.update(tc.input), "{}", tc.name);
        assert_eq!(tc.start, w.get_start(), "{}", tc.name);
        assert_eq!(tc.extended_start, w.get_extended_start(), "{}", tc.name);
        assert_eq!(tc.highest, w.get_highest(), "{}", tc.name);
        assert_eq!(tc.extended_highest, w.get_extended_highest(), "{}", tc.name);
    }
}

#[test]
fn test_wrap_around_uint16_no_restart() {
    let mut w = WrapAround::<u16, u32>::new(WrapAroundParams {
        is_restart_allowed: false,
    });
    struct TestCase {
        name: &'static str,
        input: u16,
        updated: WrapAroundUpdateResult<u32>,
        start: u16,
        extended_start: u32,
        highest: u16,
        extended_highest: u32,
    }
    let test_cases = [
        // initialize
        TestCase {
            name: "initialize",
            input: 10,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: 9,
                extended_val: 10,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: 10,
            extended_highest: 10,
        },
        // an older number without wrap around should not reset start point
        TestCase {
            name: "no reset start no wrap around",
            input: 8,
            updated: WrapAroundUpdateResult {
                is_unhandled: true,
                // the following fields are not valid when `IsUnhandled = true`, but code fills it in
                // and they are filled in here for testing purposes
                pre_extended_highest: 10,
                extended_val: 8,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: 10,
            extended_highest: 10,
        },
        // an older number with wrap around should not reset start point
        TestCase {
            name: "no reset start wrap around",
            input: ((1u32 << 16) - 6) as u16,
            updated: WrapAroundUpdateResult {
                is_unhandled: true,
                pre_extended_highest: 10,
                extended_val: (1 << 16) - 6,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: 10,
            extended_highest: 10,
        },
        // yet another older number with wrap around should not reset start point
        TestCase {
            name: "no reset start again",
            input: ((1u32 << 16) - 12) as u16,
            updated: WrapAroundUpdateResult {
                is_unhandled: true,
                pre_extended_highest: 10,
                extended_val: (1 << 16) - 12,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: 10,
            extended_highest: 10,
        },
        // duplicate should return same as highest
        TestCase {
            name: "duplicate",
            input: 10,
            updated: WrapAroundUpdateResult {
                pre_extended_highest: 10,
                extended_val: 10,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: 10,
            extended_highest: 10,
        },
        // a significant jump in order should move highest to that
        TestCase {
            name: "big in-order jump",
            input: (1 << 15) - 10,
            updated: WrapAroundUpdateResult {
                pre_extended_highest: 10,
                extended_val: (1 << 15) - 10,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: (1 << 15) - 10,
            extended_highest: (1 << 15) - 10,
        },
        // in-order, should update highest
        TestCase {
            name: "in-order",
            input: (1 << 15) + 13,
            updated: WrapAroundUpdateResult {
                pre_extended_highest: (1 << 15) - 10,
                extended_val: (1 << 15) + 13,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: (1 << 15) + 13,
            extended_highest: (1 << 15) + 13,
        },
        // now out-of-order should not reset start as half the range has been seen
        TestCase {
            name: "out-of-order after half range",
            input: (1 << 15) - 11,
            updated: WrapAroundUpdateResult {
                pre_extended_highest: (1 << 15) + 13,
                extended_val: (1 << 15) - 11,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: (1 << 15) + 13,
            extended_highest: (1 << 15) + 13,
        },
    ];

    for tc in test_cases {
        assert_eq!(tc.updated, w.update(tc.input), "{}", tc.name);
        assert_eq!(tc.start, w.get_start(), "{}", tc.name);
        assert_eq!(tc.extended_start, w.get_extended_start(), "{}", tc.name);
        assert_eq!(tc.highest, w.get_highest(), "{}", tc.name);
        assert_eq!(tc.extended_highest, w.get_extended_highest(), "{}", tc.name);
    }
}

#[test]
fn test_wrap_around_uint16_rollback_restart_and_reset_highest() {
    let mut w = WrapAround::<u16, u64>::new(WrapAroundParams {
        is_restart_allowed: true,
    });

    // initialize
    w.update(23);
    assert_eq!(23u16, w.get_start());
    assert_eq!(23u64, w.get_extended_start());
    assert_eq!(23u16, w.get_highest());
    assert_eq!(23u64, w.get_extended_highest());

    // an in-order update
    w.update(25);
    assert_eq!(23u16, w.get_start());
    assert_eq!(23u64, w.get_extended_start());
    assert_eq!(25u16, w.get_highest());
    assert_eq!(25u64, w.get_extended_highest());

    // force restart without wrap
    let mut res = w.update(12);
    let mut expected_result = WrapAroundUpdateResult::<u64> {
        is_restart: true,
        pre_extended_start: 23,
        pre_extended_highest: 25,
        extended_val: 12,
        ..Default::default()
    };
    assert_eq!(expected_result, res);
    assert_eq!(12u16, w.get_start());
    assert_eq!(12u64, w.get_extended_start());
    assert_eq!(25u16, w.get_highest());
    assert_eq!(25u64, w.get_extended_highest());

    // roll back restart
    w.rollback_restart(res.pre_extended_start);
    assert_eq!(23u16, w.get_start());
    assert_eq!(23u64, w.get_extended_start());
    assert_eq!(25u16, w.get_highest());
    assert_eq!(25u64, w.get_extended_highest());

    // force restart with wrap
    res = w.update(65533);
    expected_result = WrapAroundUpdateResult::<u64> {
        is_restart: true,
        pre_extended_start: (1 << 16) + 23,
        pre_extended_highest: (1 << 16) + 25,
        extended_val: 65533,
        ..Default::default()
    };
    assert_eq!(expected_result, res);
    assert_eq!(65533u16, w.get_start());
    assert_eq!(65533u64, w.get_extended_start());
    assert_eq!(25u16, w.get_highest());
    assert_eq!(65536u64 + 25, w.get_extended_highest());

    // roll back restart
    w.rollback_restart(res.pre_extended_start);
    assert_eq!(23u16, w.get_start());
    assert_eq!(23u64, w.get_extended_start());
    assert_eq!(25u16, w.get_highest());
    assert_eq!(25u64, w.get_extended_highest());

    // reset highest
    w.reset_highest(0x1234);
    assert_eq!(23u16, w.get_start());
    assert_eq!(23u64, w.get_extended_start());
    assert_eq!(0x1234u16, w.get_highest());
    assert_eq!(0x1234u64, w.get_extended_highest());

    w.reset_highest(0x7f1234);
    assert_eq!(23u16, w.get_start());
    assert_eq!(23u64, w.get_extended_start());
    assert_eq!(0x1234u16, w.get_highest());
    assert_eq!(0x7f1234u64, w.get_extended_highest());
}

#[test]
fn test_wrap_around_uint16_wrap_around_restart_duplicate() {
    let mut w = WrapAround::<u16, u64>::new(WrapAroundParams {
        is_restart_allowed: true,
    });

    // initialize
    w.update(65534);
    assert_eq!(65534u16, w.get_start());
    assert_eq!(65534u64, w.get_extended_start());
    assert_eq!(65534u16, w.get_highest());
    assert_eq!(65534u64, w.get_extended_highest());

    // an in-order update with a roll over
    w.update(32);
    assert_eq!(65534u16, w.get_start());
    assert_eq!(65534u64, w.get_extended_start());
    assert_eq!(32u16, w.get_highest());
    assert_eq!(65568u64, w.get_extended_highest());

    // duplicate of start
    let mut res = w.update(65534);
    let mut expected_result = WrapAroundUpdateResult::<u64> {
        is_restart: false,
        pre_extended_start: 0,
        pre_extended_highest: 65568,
        extended_val: 65534,
        ..Default::default()
    };
    assert_eq!(expected_result, res);
    assert_eq!(65534u16, w.get_start());
    assert_eq!(65534u64, w.get_extended_start());
    assert_eq!(32u16, w.get_highest());
    assert_eq!(65568u64, w.get_extended_highest());

    // duplicate of start - again
    res = w.update(65534);
    expected_result = WrapAroundUpdateResult::<u64> {
        is_restart: false,
        pre_extended_start: 0,
        pre_extended_highest: 65568,
        extended_val: 65534,
        ..Default::default()
    };
    assert_eq!(expected_result, res);
    assert_eq!(65534u16, w.get_start());
    assert_eq!(65534u64, w.get_extended_start());
    assert_eq!(32u16, w.get_highest());
    assert_eq!(65568u64, w.get_extended_highest());
}

#[test]
fn test_wrap_around_uint16_rollover() {
    let mut w = WrapAround::<u16, u32>::new(WrapAroundParams {
        is_restart_allowed: false,
    });
    struct TestCase {
        name: &'static str,
        input: u16,
        num_cycles: i32,
        updated: WrapAroundUpdateResult<u32>,
        start: u16,
        extended_start: u32,
        highest: u16,
        extended_highest: u32,
    }
    let test_cases = [
        // initialize - should initialize irrespective of numCycles
        TestCase {
            name: "initialize",
            input: 10,
            num_cycles: 10,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: 9,
                extended_val: 10,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: 10,
            extended_highest: 10,
        },
        // negative cycles - should just do an update
        TestCase {
            name: "zero",
            input: 8,
            num_cycles: -1,
            updated: WrapAroundUpdateResult {
                is_unhandled: true,
                // the following fields are not valid when `IsUnhandled = true`, but code fills it in
                // and they are filled in here for testing purposes
                pre_extended_highest: 10,
                extended_val: 8,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: 10,
            extended_highest: 10,
        },
        // one cycle
        TestCase {
            name: "one cycle",
            input: ((1u32 << 16) - 6) as u16,
            num_cycles: 1,
            updated: WrapAroundUpdateResult {
                pre_extended_highest: 10,
                extended_val: (1 << 16) - 6 + (1 << 16),
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: ((1u32 << 16) - 6) as u16,
            extended_highest: (1 << 16) - 6 + (1 << 16),
        },
        // two cycles
        TestCase {
            name: "two cycles",
            input: ((1u32 << 16) - 7) as u16,
            num_cycles: 2,
            updated: WrapAroundUpdateResult {
                pre_extended_highest: (1 << 16) - 6 + (1 << 16),
                extended_val: (1 << 16) - 7 + 3 * (1 << 16),
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: ((1u32 << 16) - 7) as u16,
            extended_highest: (1 << 16) - 7 + 3 * (1 << 16),
        },
    ];

    for tc in test_cases {
        assert_eq!(
            tc.updated,
            w.rollover(tc.input, tc.num_cycles),
            "{}",
            tc.name
        );
        assert_eq!(tc.start, w.get_start(), "{}", tc.name);
        assert_eq!(tc.extended_start, w.get_extended_start(), "{}", tc.name);
        assert_eq!(tc.highest, w.get_highest(), "{}", tc.name);
        assert_eq!(tc.extended_highest, w.get_extended_highest(), "{}", tc.name);
    }
}

#[test]
fn test_wrap_around_uint32() {
    let mut w = WrapAround::<u32, u64>::new(WrapAroundParams {
        is_restart_allowed: true,
    });
    struct TestCase {
        name: &'static str,
        input: u32,
        updated: WrapAroundUpdateResult<u64>,
        start: u32,
        extended_start: u64,
        highest: u32,
        extended_highest: u64,
    }
    let test_cases = [
        // initialize
        TestCase {
            name: "initialize",
            input: 10,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: 9,
                extended_val: 10,
                ..Default::default()
            },
            start: 10,
            extended_start: 10,
            highest: 10,
            extended_highest: 10,
        },
        // an older number without wrap around should reset start point
        TestCase {
            name: "reset start no wrap around",
            input: 8,
            updated: WrapAroundUpdateResult {
                is_restart: true,
                pre_extended_start: 10,
                pre_extended_highest: 10,
                extended_val: 8,
                ..Default::default()
            },
            start: 8,
            extended_start: 8,
            highest: 10,
            extended_highest: 10,
        },
        // an older number with wrap around should reset start point
        TestCase {
            name: "reset start wrap around",
            input: ((1u64 << 32) - 6) as u32,
            updated: WrapAroundUpdateResult {
                is_restart: true,
                pre_extended_start: (1 << 32) + 8,
                pre_extended_highest: (1 << 32) + 10,
                extended_val: (1 << 32) - 6,
                ..Default::default()
            },
            start: ((1u64 << 32) - 6) as u32,
            extended_start: (1 << 32) - 6,
            highest: 10,
            extended_highest: (1 << 32) + 10,
        },
        // an older number with wrap around should reset start point again
        TestCase {
            name: "reset start again",
            input: ((1u64 << 32) - 12) as u32,
            updated: WrapAroundUpdateResult {
                is_restart: true,
                pre_extended_start: (1 << 32) - 6,
                pre_extended_highest: (1 << 32) + 10,
                extended_val: (1 << 32) - 12,
                ..Default::default()
            },
            start: ((1u64 << 32) - 12) as u32,
            extended_start: (1 << 32) - 12,
            highest: 10,
            extended_highest: (1 << 32) + 10,
        },
        // duplicate should return same as highest
        TestCase {
            name: "duplicate",
            input: 10,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: (1 << 32) + 10,
                extended_val: (1 << 32) + 10,
                ..Default::default()
            },
            start: ((1u64 << 32) - 12) as u32,
            extended_start: (1 << 32) - 12,
            highest: 10,
            extended_highest: (1 << 32) + 10,
        },
        // a significant jump in order should not reset start
        TestCase {
            name: "big in-order jump",
            input: 1 << 31,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: (1 << 32) + 10,
                extended_val: (1 << 32) + (1 << 31),
                ..Default::default()
            },
            start: ((1u64 << 32) - 12) as u32,
            extended_start: (1 << 32) - 12,
            highest: 1 << 31,
            extended_highest: (1 << 32) + (1 << 31),
        },
        // now out-of-order should not reset start as half the range has been seen
        TestCase {
            name: "out-of-order after half range",
            input: (1 << 31) - 1,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: (1 << 32) + (1 << 31),
                extended_val: (1 << 32) + (1 << 31) - 1,
                ..Default::default()
            },
            start: ((1u64 << 32) - 12) as u32,
            extended_start: (1 << 32) - 12,
            highest: 1 << 31,
            extended_highest: (1 << 32) + (1 << 31),
        },
        // in-order, should update highest
        TestCase {
            name: "in-order",
            input: (1 << 31) + 3,
            updated: WrapAroundUpdateResult {
                is_restart: false,
                pre_extended_start: 0,
                pre_extended_highest: (1 << 32) + (1 << 31),
                extended_val: (1 << 32) + (1 << 31) + 3,
                ..Default::default()
            },
            start: ((1u64 << 32) - 12) as u32,
            extended_start: (1 << 32) - 12,
            highest: (1 << 31) + 3,
            extended_highest: (1 << 32) + (1 << 31) + 3,
        },
    ];

    for tc in test_cases {
        assert_eq!(tc.updated, w.update(tc.input), "{}", tc.name);
        assert_eq!(tc.start, w.get_start(), "{}", tc.name);
        assert_eq!(tc.extended_start, w.get_extended_start(), "{}", tc.name);
        assert_eq!(tc.highest, w.get_highest(), "{}", tc.name);
        assert_eq!(tc.extended_highest, w.get_extended_highest(), "{}", tc.name);
    }
}
