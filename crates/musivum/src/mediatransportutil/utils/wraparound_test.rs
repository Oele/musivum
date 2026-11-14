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
