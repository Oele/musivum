// Ported from LiveKit's protocol, datatrack/extension_e2ee_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

use bytes::Bytes;

use super::*;

#[test]
fn test_extension_e2ee() {
    let iv: [u8; EXTENSION_E2EE_IV_LENGTH] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
    let ext_e2ee = ExtensionE2ee::new(0xfa, iv);

    let expected_ext = Extension {
        id: livekit::DataTrackExtensionId::DteiE2ee as u8,
        data: Bytes::from_static(&[0xfa, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
    };
    let ext = ext_e2ee.marshal();
    assert!(ext.is_ok(), "{ext:?}");
    let ext = ext.unwrap();
    assert_eq!(expected_ext, ext);

    let mut unmarshaled = ExtensionE2ee::default();
    let err = unmarshaled.unmarshal(&ext);
    assert_eq!(Ok(()), err);
    assert_eq!(0xfa_u8, unmarshaled.key_index());
    assert_eq!(iv, unmarshaled.iv());
}
