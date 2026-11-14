// Ported from LiveKit's protocol, datatrack/extension_user_timestamp_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

use bytes::Bytes;

use super::*;

#[test]
fn test_extension_user_timestamp() {
    let ext_user_timestamp = ExtensionUserTimestamp::new(0x4411221111118811);

    let expected_ext = Extension {
        id: livekit::DataTrackExtensionId::DteiUserTimestamp as u8,
        data: Bytes::from_static(&[0x44, 0x11, 0x22, 0x11, 0x11, 0x11, 0x88, 0x11]),
    };
    let ext = ext_user_timestamp.marshal();
    assert!(ext.is_ok(), "{ext:?}");
    let ext = ext.unwrap();
    assert_eq!(expected_ext, ext);

    let mut unmarshaled = ExtensionUserTimestamp::default();
    let err = unmarshaled.unmarshal(&ext);
    assert_eq!(Ok(()), err);
    assert_eq!(0x4411221111118811_u64, unmarshaled.timestamp());
}
