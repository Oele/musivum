// Ported from pion/rtp, transportccextension_test.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The two benchmarks are left out.

use super::*;

#[test]
fn test_transport_cc_extension_too_small() {
    let mut t1 = TransportCcExtension::default();

    let raw_data: &[u8] = &[];

    let err = t1.unmarshal(raw_data);
    assert_eq!(Err(Error::TooSmall), err);
}

#[test]
fn test_transport_cc_extension() {
    let mut t1 = TransportCcExtension::default();

    let raw_data: &[u8] = &[0x00, 0x02];

    let err = t1.unmarshal(raw_data);
    assert_eq!(Ok(()), err);

    let t2 = TransportCcExtension {
        transport_sequence: 2,
    };

    assert_eq!(t1, t2);

    let dst_data = t2.marshal().unwrap_or_default();
    assert_eq!(dst_data, raw_data);
}

#[test]
fn test_transport_cc_extension_extra_bytes() {
    let mut t1 = TransportCcExtension::default();

    let raw_data: &[u8] = &[0x00, 0x02, 0x00, 0xff, 0xff];

    let err = t1.unmarshal(raw_data);
    assert_eq!(Ok(()), err);

    let t2 = TransportCcExtension {
        transport_sequence: 2,
    };

    assert_eq!(t1, t2);
}

#[test]
fn test_transport_cc_extension_marshal_to() {
    let ext = TransportCcExtension {
        transport_sequence: 1234,
    };

    let mut buf = vec![0u8; ext.marshal_size()];
    let n = ext.marshal_to(&mut buf);
    assert_eq!(Ok(ext.marshal_size()), n);

    let expected = ext.marshal().unwrap_or_default();
    assert_eq!(expected, buf);

    let err = ext.marshal_to(&mut []);
    assert_eq!(Err(Error::ShortBuffer), err);
}
