// Ported from pion/rtp, abssendtimeextension_test.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; the dates of `TestNtpConversion` are written as the
// Unix times that Go's `time.Date` gives for them. The two benchmarks are left out.

use super::*;
use crate::go::time::{self, Duration};

const ABS_SEND_TIME_RESOLUTION: Duration = Duration(3800 * time::NANOSECOND.0);

#[test]
fn test_ntp_conversion() {
    // loc := time.FixedZone("UTC-5", -5*60*60)

    struct Test {
        t: Time,
        n: u64,
    }
    let tests = [
        Test {
            // time.Date(1985, time.June, 23, 4, 0, 0, 0, loc)
            t: Time::unix(488365200, 0),
            n: 0xa0c65b1000000000,
        },
        Test {
            // time.Date(1999, time.December, 31, 23, 59, 59, 500000, loc)
            t: Time::unix(946702799, 500000),
            n: 0xbc18084f0020c49b,
        },
        Test {
            // time.Date(2019, time.March, 27, 13, 39, 30, 8675309, loc)
            t: Time::unix(1553711970, 8675309),
            n: 0xe04641e202388b88,
        },
    ];

    for (i, r#in) in tests.iter().enumerate() {
        let out = to_ntp_time(r#in.t);
        assert_eq!(
            r#in.n, out,
            "[{i}] Converted NTP time from time.Time differs"
        );
    }
    for (i, r#in) in tests.iter().enumerate() {
        let out = to_time(r#in.n);
        let diff = r#in.t.sub(out);
        assert!(
            diff >= -ABS_SEND_TIME_RESOLUTION,
            "[{i}] Converted time.Time from NTP time differs"
        );
        assert!(
            diff <= ABS_SEND_TIME_RESOLUTION,
            "[{i}] Converted time.Time from NTP time differs"
        );
    }
}

#[test]
fn test_abs_send_time_extension_roundtrip() {
    let tests = [
        AbsSendTimeExtension { timestamp: 123456 },
        AbsSendTimeExtension { timestamp: 654321 },
    ];
    for (i, r#in) in tests.iter().enumerate() {
        let b = r#in.marshal();
        assert!(b.is_ok());

        let mut out = AbsSendTimeExtension::default();
        assert_eq!(Ok(()), out.unmarshal(&b.unwrap()));
        assert_eq!(r#in.timestamp, out.timestamp, "[{i}] Timestamp differs");
    }
}

#[test]
fn test_abs_send_time_extension_estimate() {
    struct Test {
        send_ntp: u64,
        receive_ntp: u64,
    }
    let tests = [
        // FFFFFFC000000000 mask of second
        Test {
            send_ntp: 0xa0c65b1000100000,
            receive_ntp: 0xa0c65b1001000000,
        }, // not carried
        Test {
            send_ntp: 0xa0c65b3f00000000,
            receive_ntp: 0xa0c65b4001000000,
        }, // carried during transmission
    ];
    for (i, r#in) in tests.iter().enumerate() {
        let in_time = to_time(r#in.send_ntp);
        let send = AbsSendTimeExtension {
            timestamp: r#in.send_ntp >> 14,
        };
        let b = send.marshal();
        assert!(b.is_ok());
        let mut received = AbsSendTimeExtension::default();
        assert_eq!(Ok(()), received.unmarshal(&b.unwrap()));

        let estimated = received.estimate(to_time(r#in.receive_ntp));
        let diff = estimated.sub(in_time);
        assert!(
            diff >= -ABS_SEND_TIME_RESOLUTION,
            "[{i}] Estimated time differs, expected: {in_time}, estimated: {estimated} (receive time: {})",
            to_time(r#in.receive_ntp),
        );
        assert!(
            diff <= ABS_SEND_TIME_RESOLUTION,
            "[{i}] Estimated time differs, expected: {in_time}, estimated: {estimated} (receive time: {})",
            to_time(r#in.receive_ntp),
        );
    }
}

#[test]
fn test_abs_send_time_extension_marshal_to() {
    let ext = AbsSendTimeExtension { timestamp: 123456 };

    let mut buf = vec![0u8; ext.marshal_size()];
    let n = ext.marshal_to(&mut buf);
    assert_eq!(Ok(ext.marshal_size()), n);

    let expected = ext.marshal().unwrap_or_default();
    assert_eq!(expected, buf);

    let err = ext.marshal_to(&mut []);
    assert_eq!(Err(Error::ShortBuffer), err);
}
