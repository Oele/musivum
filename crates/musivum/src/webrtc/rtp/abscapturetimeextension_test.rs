// Ported from pion/rtp, abscapturetimeextension_test.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The four benchmarks are left out.

use super::*;

#[test]
fn test_abs_capture_time_extension_roundtrip() {
    {
        let name = "positive captureClockOffset";
        let t0 = time::now();
        let e1 = AbsCaptureTimeExtension::new(t0);
        let b1 = e1.marshal();
        assert!(b1.is_ok(), "{name}");
        let mut o1 = AbsCaptureTimeExtension::default();
        assert_eq!(Ok(()), o1.unmarshal(&b1.unwrap()), "{name}");
        let dt1 = o1.capture_time().sub(t0).seconds();
        assert!(dt1 >= -0.001, "{name}");
        assert!(dt1 <= 0.001, "{name}");
        assert_eq!(None, o1.estimated_capture_clock_offset_duration(), "{name}");

        let e2 =
            AbsCaptureTimeExtension::new_with_capture_clock_offset(t0, 1250 * time::MILLISECOND);
        let b2 = e2.marshal();
        assert!(b2.is_ok(), "{name}");
        let mut o2 = AbsCaptureTimeExtension::default();
        assert_eq!(Ok(()), o2.unmarshal(&b2.unwrap()), "{name}");
        let dt2 = o1.capture_time().sub(t0).seconds();
        assert!(dt2 >= -0.001, "{name}");
        assert!(dt2 <= 0.001, "{name}");
        assert_eq!(
            Some(1250 * time::MILLISECOND),
            o2.estimated_capture_clock_offset_duration(),
            "{name}"
        );
    }

    // This test can verify the for for the issue 247
    {
        let name = "negative captureClockOffset";
        let t0 = time::now();
        let e1 = AbsCaptureTimeExtension::new(t0);
        let b1 = e1.marshal();
        assert!(b1.is_ok(), "{name}");
        let mut o1 = AbsCaptureTimeExtension::default();
        assert_eq!(Ok(()), o1.unmarshal(&b1.unwrap()), "{name}");
        let dt1 = o1.capture_time().sub(t0).seconds();
        assert!(dt1 >= -0.001, "{name}");
        assert!(dt1 <= 0.001, "{name}");
        assert_eq!(None, o1.estimated_capture_clock_offset_duration(), "{name}");

        let e2 =
            AbsCaptureTimeExtension::new_with_capture_clock_offset(t0, -250 * time::MILLISECOND);
        let b2 = e2.marshal();
        assert!(b2.is_ok(), "{name}");

        let mut o2 = AbsCaptureTimeExtension::default();
        assert_eq!(Ok(()), o2.unmarshal(&b2.unwrap()), "{name}");
        let dt2 = o1.capture_time().sub(t0).seconds();
        assert!(dt2 >= -0.001, "{name}");
        assert!(dt2 <= 0.001, "{name}");
        assert_eq!(
            Some(-250 * time::MILLISECOND),
            o2.estimated_capture_clock_offset_duration(),
            "{name}"
        );
    }
}

#[test]
fn test_abs_capture_time_extension_marshal_to() {
    {
        let name = "without offset";
        let ext = AbsCaptureTimeExtension::new(time::now());

        let mut buf = vec![0u8; ext.marshal_size()];
        let n = ext.marshal_to(&mut buf);
        assert_eq!(Ok(ext.marshal_size()), n, "{name}");

        let expected = ext.marshal().unwrap_or_default();
        assert_eq!(expected, buf, "{name}");

        let err = ext.marshal_to(&mut []);
        assert_eq!(Err(Error::ShortBuffer), err, "{name}");
    }

    {
        let name = "with offset";
        let ext = AbsCaptureTimeExtension::new_with_capture_clock_offset(
            time::now(),
            100 * time::MILLISECOND,
        );

        let mut buf = vec![0u8; ext.marshal_size()];
        let n = ext.marshal_to(&mut buf);
        assert_eq!(Ok(ext.marshal_size()), n, "{name}");

        let expected = ext.marshal().unwrap_or_default();
        assert_eq!(expected, buf, "{name}");

        let err = ext.marshal_to(&mut []);
        assert_eq!(Err(Error::ShortBuffer), err, "{name}");
    }
}
