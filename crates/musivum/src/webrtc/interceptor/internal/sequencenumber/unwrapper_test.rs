// Ported from pion/interceptor, internal/sequencenumber/unwrapper_test.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust.

use super::*;

#[test]
fn test_is_newer() {
    struct Case {
        a: u16,
        b: u16,
        expected: bool,
    }
    let cases = [
        Case {
            a: 1,
            b: 0,
            expected: true,
        },
        Case {
            a: 65534,
            b: 65535,
            expected: false,
        },
        Case {
            a: 65535,
            b: 65535,
            expected: false,
        },
        Case {
            a: 0,
            b: 65535,
            expected: true,
        },
        Case {
            a: 0,
            b: 32767,
            expected: false,
        },
        Case {
            a: 32770,
            b: 2,
            expected: true,
        },
        Case {
            a: 3,
            b: 32770,
            expected: false,
        },
    ];
    for (i, tc) in cases.iter().enumerate() {
        assert_eq!(
            tc.expected,
            is_newer(tc.a, tc.b),
            "{}: expected is_newer({}, {}) to be {}",
            i,
            tc.a,
            tc.b,
            tc.expected
        );
    }
}

#[test]
fn test_unwrapper() {
    struct Case {
        input: Vec<u16>,
        expected: Vec<i64>,
    }
    let cases = [
        Case {
            input: vec![],
            expected: vec![],
        },
        Case {
            input: vec![0, 1, 2, 3, 4],
            expected: vec![0, 1, 2, 3, 4],
        },
        Case {
            input: vec![65534, 65535, 0, 1, 2],
            expected: vec![65534, 65535, 65536, 65537, 65538],
        },
        Case {
            input: vec![32769, 0],
            expected: vec![32769, 65536],
        },
        Case {
            input: vec![32767, 0],
            expected: vec![32767, 0],
        },
        Case {
            input: vec![0, 1, 4, 3, 2, 5],
            expected: vec![0, 1, 4, 3, 2, 5],
        },
        Case {
            input: vec![65534, 0, 1, 65535, 4, 3, 2, 5],
            expected: vec![65534, 65536, 65537, 65535, 65540, 65539, 65538, 65541],
        },
        Case {
            input: vec![0, 32767, 32768, 32769, 32770, 1, 2, 32765, 32770, 65535],
            expected: vec![
                0, 32767, 32768, 32769, 32770, 65537, 65538, 98301, 98306, 131071,
            ],
        },
    ];
    for (i, tc) in cases.iter().enumerate() {
        let mut u = Unwrapper::default();
        let mut result: Vec<i64> = vec![];
        for i in &tc.input {
            result.push(u.unwrap(*i));
        }
        assert_eq!(tc.expected, result, "{i}");
    }
}
