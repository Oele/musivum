// Ported from LiveKit's protocol, datatrack/extension_participant_sid_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The test after `test_extension_participant_sid` is new
// and says so.

use bytes::Bytes;

use super::*;

#[test]
fn test_extension_participant_sid() {
    let long_test_participant_id = livekit::ParticipantId::from("\0".repeat(256));
    let err = ExtensionParticipantSid::new(long_test_participant_id);
    assert!(err.is_err());

    let test_participant_id = livekit::ParticipantId::from("test");
    let ext_participant_sid = ExtensionParticipantSid::new(test_participant_id.clone());
    assert!(ext_participant_sid.is_ok(), "{ext_participant_sid:?}");
    let ext_participant_sid = ext_participant_sid.unwrap();

    let expected_ext = Extension {
        id: livekit::DataTrackExtensionId::DteiParticipantSid as u8,
        data: Bytes::from_static(b"test"),
    };
    let ext = ext_participant_sid.marshal();
    assert!(ext.is_ok(), "{ext:?}");
    let ext = ext.unwrap();
    assert_eq!(expected_ext, ext);

    let mut unmarshaled = ExtensionParticipantSid::default();
    let err = unmarshaled.unmarshal(&ext);
    assert_eq!(Ok(()), err);
    assert_eq!(test_participant_id, unmarshaled.participant_id());
}

// New for musivum: where the port differs from the original. Data that is not UTF-8 is taken,
// as the original takes it, and what is not text in it is replaced.
#[test]
fn test_extension_participant_sid_not_utf8() {
    let ext = Extension {
        id: livekit::DataTrackExtensionId::DteiParticipantSid as u8,
        data: Bytes::from_static(&[b'P', b'A', 0xc3, 0x28, 0xa0]),
    };
    let mut unmarshaled = ExtensionParticipantSid::default();
    assert_eq!(Ok(()), unmarshaled.unmarshal(&ext));
    assert_eq!(
        livekit::ParticipantId::from("PA\u{fffd}(\u{fffd}"),
        unmarshaled.participant_id()
    );
}
