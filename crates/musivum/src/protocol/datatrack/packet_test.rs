// Ported from LiveKit's protocol, datatrack/packet_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks that carry their names in the
// assertion messages. The tests after `test_packet` are new and say so.

use bytes::Bytes;

use super::*;
use crate::protocol::datatrack::{
    EXTENSION_E2EE_IV_LENGTH, ExtensionE2ee, ExtensionParticipantSid, ExtensionUserTimestamp,
};
use crate::protocol::livekit;

#[test]
fn test_packet() {
    {
        let name = "without extension";
        let mut payload = vec![0u8; 6];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (255 - i) as u8;
        }
        let packet = Packet {
            header: Header {
                version: 0,
                is_start_of_frame: true,
                is_final_of_frame: true,
                handle: 3333,
                sequence_number: 6666,
                frame_number: 9999,
                timestamp: 0xdeadbeef,
                ..Default::default()
            },
            payload: Bytes::from(payload),
        };
        let raw_packet = packet.marshal();
        assert!(raw_packet.is_ok(), "{name}: {raw_packet:?}");
        let raw_packet = raw_packet.unwrap();

        let expected_raw_packet: Vec<u8> = vec![
            0x18, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0xff, 0xfe,
            0xfd, 0xfc, 0xfb, 0xfa,
        ];
        assert_eq!(expected_raw_packet, raw_packet, "{name}");

        let mut unmarshaled = Packet::default();
        let err = unmarshaled.unmarshal(&Bytes::from(raw_packet));
        assert_eq!(Ok(()), err, "{name}");
        assert_eq!(packet, unmarshaled, "{name}");
    }

    {
        let name = "with extension";
        let mut payload = vec![0u8; 4];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (255 - i) as u8;
        }
        let mut packet = Packet {
            header: Header {
                version: 0,
                is_start_of_frame: true,
                is_final_of_frame: false,
                handle: 3333,
                sequence_number: 6666,
                frame_number: 9999,
                timestamp: 0xdeadbeef,
                ..Default::default()
            },
            payload: Bytes::from(payload),
        };
        if let Ok(ext_participant_sid) = ExtensionParticipantSid::new("test_participant".into())
            && let Ok(ext) = ext_participant_sid.marshal()
        {
            packet.add_extension(ext);
        }
        let raw_packet = packet.marshal();
        assert!(raw_packet.is_ok(), "{name}: {raw_packet:?}");
        let raw_packet = raw_packet.unwrap();

        let expected_raw_packet: Vec<u8> = vec![
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x04,
            0xff, 0x10, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69,
            0x70, 0x61, 0x6e, 0x74, 0xff, 0xfe, 0xfd, 0xfc,
        ];
        assert_eq!(expected_raw_packet, raw_packet, "{name}");

        let mut unmarshaled = Packet::default();
        let err = unmarshaled.unmarshal(&Bytes::from(raw_packet));
        assert_eq!(Ok(()), err, "{name}");
        assert_eq!(packet, unmarshaled, "{name}");

        let ext =
            unmarshaled.get_extension(livekit::DataTrackExtensionId::DteiParticipantSid as u8);
        assert!(ext.is_ok(), "{name}: {ext:?}");
        let ext = ext.unwrap();

        let mut ext_participant_sid = ExtensionParticipantSid::default();
        assert_eq!(Ok(()), ext_participant_sid.unmarshal(&ext), "{name}");
        assert_eq!(
            livekit::ParticipantId::from("test_participant"),
            ext_participant_sid.participant_id(),
            "{name}"
        );
    }

    {
        let name = "with extension padding";
        let mut payload = vec![0u8; 4];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (255 - i) as u8;
        }
        let mut packet = Packet {
            header: Header {
                version: 0,
                is_start_of_frame: true,
                is_final_of_frame: false,
                handle: 3333,
                sequence_number: 6666,
                frame_number: 9999,
                timestamp: 0xdeadbeef,
                ..Default::default()
            },
            payload: Bytes::from(payload),
        };
        if let Ok(ext_participant_sid) = ExtensionParticipantSid::new("participant".into())
            && let Ok(ext) = ext_participant_sid.marshal()
        {
            packet.add_extension(ext);
        }
        let raw_packet = packet.marshal();
        assert!(raw_packet.is_ok(), "{name}: {raw_packet:?}");
        let raw_packet = raw_packet.unwrap();

        let expected_raw_packet: Vec<u8> = vec![
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x03,
            0xff, 0x0b, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61, 0x6e, 0x74, 0x00,
            0xff, 0xfe, 0xfd, 0xfc,
        ];
        assert_eq!(expected_raw_packet, raw_packet, "{name}");

        let mut unmarshaled = Packet::default();
        let err = unmarshaled.unmarshal(&Bytes::from(raw_packet));
        assert_eq!(Ok(()), err, "{name}");
        assert_eq!(packet, unmarshaled, "{name}");

        let ext =
            unmarshaled.get_extension(livekit::DataTrackExtensionId::DteiParticipantSid as u8);
        assert!(ext.is_ok(), "{name}: {ext:?}");
        let ext = ext.unwrap();

        let mut ext_participant_sid = ExtensionParticipantSid::default();
        assert_eq!(Ok(()), ext_participant_sid.unmarshal(&ext), "{name}");
        assert_eq!(
            livekit::ParticipantId::from("participant"),
            ext_participant_sid.participant_id(),
            "{name}"
        );
    }

    {
        let name = "replace extension";
        let mut payload = vec![0u8; 4];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (255 - i) as u8;
        }
        let mut packet = Packet {
            header: Header {
                version: 0,
                is_start_of_frame: true,
                is_final_of_frame: false,
                handle: 3333,
                sequence_number: 6666,
                frame_number: 9999,
                timestamp: 0xdeadbeef,
                ..Default::default()
            },
            payload: Bytes::from(payload),
        };
        if let Ok(ext_participant_sid) = ExtensionParticipantSid::new("participant".into())
            && let Ok(ext) = ext_participant_sid.marshal()
        {
            packet.add_extension(ext);
        }
        let raw_packet = packet.marshal();
        assert!(raw_packet.is_ok(), "{name}: {raw_packet:?}");
        let raw_packet = raw_packet.unwrap();

        let expected_raw_packet: Vec<u8> = vec![
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x03,
            0xff, 0x0b, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61, 0x6e, 0x74, 0x00,
            0xff, 0xfe, 0xfd, 0xfc,
        ];
        assert_eq!(expected_raw_packet, raw_packet, "{name}");

        // replace existing extension ID and ensure that marshalled packet is updated
        if let Ok(ext_participant_sid) = ExtensionParticipantSid::new("test_participant".into())
            && let Ok(ext) = ext_participant_sid.marshal()
        {
            packet.add_extension(ext);
        }
        let raw_packet = packet.marshal();
        assert!(raw_packet.is_ok(), "{name}: {raw_packet:?}");
        let raw_packet = raw_packet.unwrap();

        let expected_raw_packet: Vec<u8> = vec![
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x04,
            0xff, 0x10, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69,
            0x70, 0x61, 0x6e, 0x74, 0xff, 0xfe, 0xfd, 0xfc,
        ];
        assert_eq!(expected_raw_packet, raw_packet, "{name}");

        let mut unmarshaled = Packet::default();
        let err = unmarshaled.unmarshal(&Bytes::from(raw_packet));
        assert_eq!(Ok(()), err, "{name}");
        assert_eq!(packet, unmarshaled, "{name}");

        let ext =
            unmarshaled.get_extension(livekit::DataTrackExtensionId::DteiParticipantSid as u8);
        assert!(ext.is_ok(), "{name}: {ext:?}");
        let ext = ext.unwrap();

        let mut ext_participant_sid = ExtensionParticipantSid::default();
        assert_eq!(Ok(()), ext_participant_sid.unmarshal(&ext), "{name}");
        assert_eq!(
            livekit::ParticipantId::from("test_participant"),
            ext_participant_sid.participant_id(),
            "{name}"
        );
    }

    {
        let name = "with client extensions";
        // mirrors the serialization test vector of the Rust client (livekit-datatrack)
        let payload = vec![0xfa_u8; 1024];
        let mut packet = Packet {
            header: Header {
                version: 0,
                is_start_of_frame: false,
                is_final_of_frame: true,
                handle: 0x8811,
                sequence_number: 0x4422,
                frame_number: 0x4411,
                timestamp: 0x44221188,
                ..Default::default()
            },
            payload: Bytes::from(payload.clone()),
        };
        let mut iv = [0u8; EXTENSION_E2EE_IV_LENGTH];
        for b in iv.iter_mut() {
            *b = 0x3c;
        }
        let ext = ExtensionE2ee::new(0xfa, iv).marshal();
        assert!(ext.is_ok(), "{name}: {ext:?}");
        packet.add_extension(ext.unwrap());
        let ext = ExtensionUserTimestamp::new(0x4411221111118811).marshal();
        assert!(ext.is_ok(), "{name}: {ext:?}");
        packet.add_extension(ext.unwrap());

        let raw_packet = packet.marshal();
        assert!(raw_packet.is_ok(), "{name}: {raw_packet:?}");
        let raw_packet = raw_packet.unwrap();
        assert_eq!(1064, raw_packet.len(), "{name}");

        let expected_header: Vec<u8> = vec![
            0x0c, 0x00, 0x88, 0x11, 0x44, 0x22, 0x44,
            0x11, // version 0, final, extensions; handle; sequence; frame
            0x44, 0x22, 0x11, 0x88, 0x00, 0x06, // timestamp; extension words
            0x01, 0x0d, 0xfa, 0x3c, 0x3c, 0x3c, 0x3c, 0x3c, 0x3c, 0x3c, 0x3c, 0x3c, 0x3c, 0x3c,
            0x3c, // E2EE
            0x02, 0x08, 0x44, 0x11, 0x22, 0x11, 0x11, 0x11, 0x88, 0x11, // user timestamp
            0x00, // padding
        ];
        assert_eq!(
            expected_header,
            raw_packet[..expected_header.len()],
            "{name}"
        );
        assert_eq!(payload, raw_packet[expected_header.len()..], "{name}");

        let mut unmarshaled = Packet::default();
        assert_eq!(
            Ok(()),
            unmarshaled.unmarshal(&Bytes::from(raw_packet)),
            "{name}"
        );
        assert_eq!(packet, unmarshaled, "{name}");

        let ext = unmarshaled.get_extension(livekit::DataTrackExtensionId::DteiE2ee as u8);
        assert!(ext.is_ok(), "{name}: {ext:?}");
        let mut ext_e2ee = ExtensionE2ee::default();
        assert_eq!(Ok(()), ext_e2ee.unmarshal(&ext.unwrap()), "{name}");
        assert_eq!(0xfa_u8, ext_e2ee.key_index(), "{name}");
        assert_eq!(iv, ext_e2ee.iv(), "{name}");

        let ext = unmarshaled.get_extension(livekit::DataTrackExtensionId::DteiUserTimestamp as u8);
        assert!(ext.is_ok(), "{name}: {ext:?}");
        let mut ext_user_timestamp = ExtensionUserTimestamp::default();
        assert_eq!(
            Ok(()),
            ext_user_timestamp.unmarshal(&ext.unwrap()),
            "{name}"
        );
        assert_eq!(
            0x4411221111118811_u64,
            ext_user_timestamp.timestamp(),
            "{name}"
        );
    }

    {
        let name = "bad packet";
        let mut unmarshaled = Packet::default();
        // extensions size too small
        let bad_packet = Bytes::from_static(&[
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x02,
            0x01, 0x0b, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61, 0x6e, 0x74, 0x00,
            0xff, 0xfe, 0xfd, 0xfc,
        ]);
        let err = unmarshaled.unmarshal(&bad_packet);
        assert!(err.is_err(), "{name}");

        // get an invalid extension id
        let bad_packet = Bytes::from_static(&[
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x03,
            0x02, 0x0b, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61, 0x6e, 0x74, 0x00,
            0xff, 0xfe, 0xfd, 0xfc,
        ]);
        let err = unmarshaled.unmarshal(&bad_packet);
        assert_eq!(Ok(()), err, "{name}");
        let err =
            unmarshaled.get_extension(livekit::DataTrackExtensionId::DteiParticipantSid as u8);
        assert!(err.is_err(), "{name}");

        // extension payload size bigger than payload
        let bad_packet = Bytes::from_static(&[
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x03,
            0x01, 0x0d, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61, 0x6e, 0x74, 0x00,
            0xff, 0xfe, 0xfd, 0xfc,
        ]);
        let err = unmarshaled.unmarshal(&bad_packet);
        assert!(err.is_err(), "{name}");

        // extension payload size smaller than payload
        let bad_packet = Bytes::from_static(&[
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x03,
            0x01, 0x07, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61, 0x6e, 0x74, 0x00,
            0xff, 0xfe, 0xfd, 0xfc,
        ]);
        let err = unmarshaled.unmarshal(&bad_packet);
        assert!(err.is_err(), "{name}");
    }

    {
        let name = "oversized extension padding does not panic";
        let mut unmarshaled = Packet::default();
        // HasExtensions set, extensionsSize describes more bytes than present,
        // terminated by a 0x00 padding id -> hdrSize would exceed len(buf)
        let bad_packet = Bytes::from_static(&[
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
        ]);
        let err = unmarshaled.unmarshal(&bad_packet);
        assert!(err.is_err(), "{name}");
    }

    {
        let name = "extensions size wraparound does not panic";
        let mut unmarshaled = Packet::default();
        // 0xFFFF extensions-size field wraps (raw+1)*4 uint16 arithmetic to a huge
        // remainingSize; the 0x00 padding id must not push hdrSize past len(buf)
        let bad_packet = Bytes::from_static(&[
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff,
            0x00,
        ]);
        let err = unmarshaled.unmarshal(&bad_packet);
        assert!(err.is_err(), "{name}");
    }

    {
        let name = "truncated extensions size field does not panic";
        let mut unmarshaled = Packet::default();
        // HasExtensions set but buffer too short to hold the extensionsSize field
        let bad_packet = Bytes::from_static(&[
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]);
        let err = unmarshaled.unmarshal(&bad_packet);
        assert!(err.is_err(), "{name}");
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
        .collect()
}

// What the original and the port are compared by: all that `unmarshal` reads out of `data` or
// the text of its error, the three kinds of extension read from it, and what `marshal` then
// makes of it.
fn describe(data: &[u8]) -> String {
    let data = Bytes::copy_from_slice(data);
    let mut p = Packet::default();
    if let Err(err) = p.unmarshal(&data) {
        return format!("ERR {err}");
    }
    let mut s = format!(
        "OK v={} s={} f={} x={} h={} sn={} fn={} ts={} es={} ms={} exts=[",
        p.version,
        p.is_start_of_frame,
        p.is_final_of_frame,
        p.has_extensions,
        p.handle,
        p.sequence_number,
        p.frame_number,
        p.timestamp,
        p.extensions_size,
        p.header.marshal_size()
    );
    for (i, ext) in p.extensions.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s += &format!("{}:{}", ext.id(), hex(ext.data()));
    }
    s += &format!("] payload={}", hex(&p.payload));

    match p.get_extension(livekit::DataTrackExtensionId::DteiE2ee as u8) {
        Err(err) => s += &format!(" e2ee=ERR {err}"),
        Ok(ext) => {
            let mut e = ExtensionE2ee::default();
            match e.unmarshal(&ext) {
                Err(err) => s += &format!(" e2ee=ERR {err}"),
                Ok(()) => s += &format!(" e2ee={}:{}", e.key_index(), hex(&e.iv())),
            }
        }
    }
    match p.get_extension(livekit::DataTrackExtensionId::DteiUserTimestamp as u8) {
        Err(err) => s += &format!(" uts=ERR {err}"),
        Ok(ext) => {
            let mut e = ExtensionUserTimestamp::default();
            match e.unmarshal(&ext) {
                Err(err) => s += &format!(" uts=ERR {err}"),
                Ok(()) => s += &format!(" uts={}", e.timestamp()),
            }
        }
    }
    match p.get_extension(livekit::DataTrackExtensionId::DteiParticipantSid as u8) {
        Err(err) => s += &format!(" psid=ERR {err}"),
        Ok(ext) => {
            let mut e = ExtensionParticipantSid::default();
            match e.unmarshal(&ext) {
                Err(err) => s += &format!(" psid=ERR {err}"),
                // the port holds the ID as text: what is not UTF-8 is not compared
                Ok(()) if std::str::from_utf8(ext.data()).is_err() => s += " psid=nonutf8",
                Ok(()) => s += &format!(" psid={}", hex(e.participant_id().as_str().as_bytes())),
            }
        }
    }

    // the first extension read as each of the three kinds
    if let Some(first) = p.extensions.first() {
        fn res(r: Result<(), Error>) -> String {
            match r {
                Err(err) => err.to_string(),
                Ok(()) => "ok".to_string(),
            }
        }
        s += &format!(
            " first={}/{}/{}",
            res(ExtensionE2ee::default().unmarshal(first)),
            res(ExtensionUserTimestamp::default().unmarshal(first)),
            res(ExtensionParticipantSid::default().unmarshal(first))
        );
    }

    match p.marshal() {
        Err(err) => s += &format!(" marshal=ERR {err}"),
        Ok(buf) => {
            s += &format!(" marshal={}", hex(&buf));

            // the property of the fuzz test
            let mut p2 = Packet::default();
            match p2.unmarshal(&Bytes::copy_from_slice(&buf)) {
                Err(err) => s += &format!(" rt=ERR {err}"),
                Ok(()) => s += &format!(" rt={}", p == p2),
            }

            if !buf.is_empty() {
                match p.marshal_to(&mut vec![0; buf.len() - 1]) {
                    Err(err) => s += &format!(" short=ERR {err}"),
                    Ok(()) => s += " short=ok",
                }
            }
        }
    }

    // replace or add the participant SID, as a server would
    let mut added = p.clone();
    if let Ok(e) = ExtensionParticipantSid::new("PA_diff".into())
        && let Ok(ext) = e.marshal()
    {
        added.add_extension(ext);
    }
    match added.marshal() {
        Err(err) => s += &format!(" add=ERR {err}"),
        Ok(buf) => s += &format!(" es2={} add={}", added.extensions_size, hex(&buf)),
    }

    // an extension that is too big to marshal
    let mut big = p.clone();
    big.add_extension(Extension::new(77, Bytes::from(vec![0; 256])));
    match big.marshal() {
        Err(err) => s += &format!(" big=ERR {err}"),
        Ok(_) => s += " big=ok",
    }

    s
}

// New for musivum: inputs with what the original gives for them, taken from a run of 5173
// pseudo-random inputs through the original and through this port (valid packets with each
// extension, truncated at every length, with bits flipped; random bytes), in which all agreed.
// One of each kind of outcome is kept here.
#[test]
fn test_unmarshal_and_marshal_as_the_original() {
    const PINNED: &[(&str, &str)] = &[
        ("", "ERR data track packet header size insufficient: 0 < 12"),
        (
            "6c0093444b6f13f6776ac07a",
            "ERR data track packet header size insufficient: 12 < 14",
        ),
        (
            "0c00b8af6b525efedefe50fe0004",
            "ERR data track packet extension size insufficient: 18/0 < 1",
        ),
        (
            "0c00b8af6b525efedefe50fe000401",
            "ERR data track packet extension size insufficient: 18/1 < 1",
        ),
        (
            "bc00f9be57c9c288c80a49fd000100",
            "ERR data track packet extension size insufficient: 6/1 < 6",
        ),
        (
            "0c00b8af6b525efedefe50fe0004010d",
            "ERR data track packet extension size insufficient: 16/0 < 13",
        ),
        (
            "740014d3e7f58264ec98970800020208",
            "ERR data track packet extension size insufficient: 8/0 < 8",
        ),
        (
            "1c003a6148676aa48de4e3c7f87a640ef6",
            "ERR data track packet extension size insufficient: 254440/1 < 14",
        ),
        (
            "1c00754a1165a57be7d206b400000702047e",
            "ERR data track packet extension size insufficient: 0/2 < 2",
        ),
        (
            "0400123456789abc0102030400010503aabbcc0700",
            "ERR data track packet extension size insufficient: -1/0 < 0",
        ),
        (
            "0400123456789abc0102030400010503aabbcc070001",
            "ERR data track packet extension size insufficient: -1/1 < 0",
        ),
        (
            "34007bef86b79572ec0c34090001c8030102030197ef",
            "ERR data track packet extension size insufficient: -1/1 < 151",
        ),
        (
            "680093444b6f13f6776ac07a",
            "OK v=3 s=false f=true x=false h=37700 sn=19311 fn=5110 ts=2003484794 es=0 ms=12 exts=[] payload= e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 marshal=680093444b6f13f6776ac07a rt=true short=ERR data track packet buffer size insufficient: 11 < 12 es2=9 add=6c0093444b6f13f6776ac07a0002ff0750415f6469666600 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "682093444b6f13f6776ac07a",
            "OK v=3 s=false f=true x=false h=37700 sn=19311 fn=5110 ts=2003484794 es=0 ms=12 exts=[] payload= e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 marshal=680093444b6f13f6776ac07a rt=true short=ERR data track packet buffer size insufficient: 11 < 12 es2=9 add=6c0093444b6f13f6776ac07a0002ff0750415f6469666600 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "04000007000000000000000000000000",
            "OK v=0 s=false f=false x=true h=7 sn=0 fn=0 ts=0 es=0 ms=16 exts=[] payload= e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 marshal=04000007000000000000000000000000 rt=true short=ERR data track packet buffer size insufficient: 15 < 16 es2=9 add=0400000700000000000000000002ff0750415f6469666600 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "1c00756a1165a57be7d206b400000700",
            "OK v=0 s=true f=true x=true h=30058 sn=4453 fn=42363 ts=3889301172 es=2 ms=16 exts=[7:] payload= e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/invalid extension ID/invalid extension ID marshal=1c00756a1165a57be7d206b400000700 rt=true short=ERR data track packet buffer size insufficient: 15 < 16 es2=11 add=1c00756a1165a57be7d206b400030700ff0750415f64696666000000 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "3400c23f7f21b9836a3fb0780000ff00",
            "OK v=1 s=true f=false x=true h=49727 sn=32545 fn=47491 ts=1782558840 es=2 ms=16 exts=[255:] payload= e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=ERR extension data too short first=invalid extension ID/invalid extension ID/extension data too short marshal=3400c23f7f21b9836a3fb0780000ff00 rt=true short=ERR data track packet buffer size insufficient: 15 < 16 es2=9 add=3400c23f7f21b9836a3fb0780002ff0750415f6469666600 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0400000700000000000000000000001086642c",
            "OK v=0 s=false f=false x=true h=7 sn=0 fn=0 ts=0 es=0 ms=16 exts=[] payload=86642c e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 marshal=0400000700000000000000000000000086642c rt=true short=ERR data track packet buffer size insufficient: 18 < 19 es2=9 add=0400000700000000000000000002ff0750415f646966660086642c big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0400000700000000000000000000020086642c",
            "OK v=0 s=false f=false x=true h=7 sn=0 fn=0 ts=0 es=2 ms=16 exts=[2:] payload=86642c e2ee=ERR data track packet extension not found, id: 1 uts=ERR extension data too short psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/extension data too short/invalid extension ID marshal=0400000700000000000000000000020086642c rt=true short=ERR data track packet buffer size insufficient: 18 < 19 es2=11 add=04000007000000000000000000030200ff0750415f6469666600000086642c big=ERR extension size is too big: 256 > 255",
        ),
        (
            "640041b509e78182fa2432780001ff0150000000",
            "OK v=3 s=false f=false x=true h=16821 sn=2535 fn=33154 ts=4196676216 es=3 ms=20 exts=[255:50] payload= e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=50 first=invalid extension ID/invalid extension ID/ok marshal=640041b509e78182fa2432780001ff0150000000 rt=true short=ERR data track packet buffer size insufficient: 19 < 20 es2=9 add=640041b509e78182fa2432780002ff0750415f6469666600 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "6c00359642766411f77ee1ac0001ff04c328a0a1",
            "OK v=3 s=false f=true x=true h=13718 sn=17014 fn=25617 ts=4152287660 es=6 ms=20 exts=[255:c328a0a1] payload= e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=nonutf8 first=invalid extension ID/invalid extension ID/ok marshal=6c00359642766411f77ee1ac0001ff04c328a0a1 rt=true short=ERR data track packet buffer size insufficient: 19 < 20 es2=9 add=6c00359642766411f77ee1ac0002ff0750415f6469666600 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "cc006c639f655d513f64ab4000010200010203043d3a",
            "OK v=6 s=false f=true x=true h=27747 sn=40805 fn=23889 ts=1063562048 es=6 ms=20 exts=[2:,1:0304] payload=3d3a e2ee=ERR extension data too short uts=ERR extension data too short psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/extension data too short/invalid extension ID marshal=cc006c639f655d513f64ab4000010200010203043d3a rt=true short=ERR data track packet buffer size insufficient: 21 < 22 es2=15 add=cc006c639f655d513f64ab400004020001020304ff0750415f646966660000003d3a big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0400123656789abc0102030400010103aabbcc00b0fb71",
            "OK v=0 s=false f=false x=true h=4662 sn=22136 fn=39612 ts=16909060 es=5 ms=20 exts=[1:aabbcc] payload=b0fb71 e2ee=ERR extension data too short uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 first=extension data too short/invalid extension ID/invalid extension ID marshal=0400123656789abc0102030400010103aabbcc00b0fb71 rt=true short=ERR data track packet buffer size insufficient: 22 < 23 es2=14 add=0400123656789abc0102030400030103aabbccff0750415f64696666b0fb71 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "640041b509e78182fa2432780001ff01500200000e8066",
            "OK v=3 s=false f=false x=true h=16821 sn=2535 fn=33154 ts=4196676216 es=5 ms=20 exts=[255:50,2:] payload=0e8066 e2ee=ERR data track packet extension not found, id: 1 uts=ERR extension data too short psid=50 first=invalid extension ID/invalid extension ID/ok marshal=640041b509e78182fa2432780001ff01500200000e8066 rt=true short=ERR data track packet buffer size insufficient: 22 < 23 es2=11 add=640041b509e78182fa2432780003ff0750415f6469666602000000000e8066 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "640041b509e78182fa2432780001ff01501000000e8466",
            "OK v=3 s=false f=false x=true h=16821 sn=2535 fn=33154 ts=4196676216 es=5 ms=20 exts=[255:50,16:] payload=0e8466 e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=50 first=invalid extension ID/invalid extension ID/ok marshal=640041b509e78182fa2432780001ff01501000000e8466 rt=true short=ERR data track packet buffer size insufficient: 22 < 23 es2=11 add=640041b509e78182fa2432780003ff0750415f6469666610000000000e8466 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "f400b0488f14d70fbbd8e9710001ef0250410400668cd8",
            "OK v=7 s=true f=false x=true h=45128 sn=36628 fn=55055 ts=3151554929 es=6 ms=20 exts=[239:5041,4:] payload=668cd8 e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/invalid extension ID/invalid extension ID marshal=f400b0488f14d70fbbd8e9710001ef0250410400668cd8 rt=true short=ERR data track packet buffer size insufficient: 22 < 23 es2=15 add=f400b0488f14d70fbbd8e9710004ef0250410400ff0750415f64696666000000668cd8 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "740014d3e7f58264ec989708000202080102030405060708",
            "OK v=3 s=true f=false x=true h=5331 sn=59381 fn=33380 ts=3969423112 es=10 ms=24 exts=[2:0102030405060708] payload= e2ee=ERR data track packet extension not found, id: 1 uts=72623859790382856 psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/ok/invalid extension ID marshal=740014d3e7f58264ec989708000202080102030405060708 rt=true short=ERR data track packet buffer size insufficient: 23 < 24 es2=19 add=740014d3e7f58264ec989708000502080102030405060708ff0750415f64696666000000 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "e400789f53d2c745f4fa324f000207000801090902090900",
            "OK v=7 s=false f=false x=true h=30879 sn=21458 fn=51013 ts=4110037583 es=9 ms=24 exts=[7:,8:09,9:0909] payload= e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/invalid extension ID/invalid extension ID marshal=e400789f53d2c745f4fa324f000207000801090902090900 rt=true short=ERR data track packet buffer size insufficient: 23 < 24 es2=18 add=e400789f53d2c745f4fa324f0004070008010909020909ff0750415f64696666 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "14003d33805c98bd9ee4d688000201050102030405100000f845",
            "OK v=0 s=true f=false x=true h=15667 sn=32860 fn=39101 ts=2665797256 es=9 ms=24 exts=[1:0102030405,16:] payload=f845 e2ee=ERR extension data too short uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 first=extension data too short/invalid extension ID/invalid extension ID marshal=14003d33805c98bd9ee4d688000201050102030405100000f845 rt=true short=ERR data track packet buffer size insufficient: 25 < 26 es2=18 add=14003d33805c98bd9ee4d6880004010501020304051000ff0750415f64696666f845 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0400123456789abc0102030400020101aa0502bbcc05000084b6e2",
            "OK v=0 s=false f=false x=true h=4660 sn=22136 fn=39612 ts=16909060 es=9 ms=24 exts=[1:aa,5:bbcc,5:] payload=84b6e2 e2ee=ERR extension data too short uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 first=extension data too short/invalid extension ID/invalid extension ID marshal=0400123456789abc0102030400020101aa0502bbcc05000084b6e2 rt=true short=ERR data track packet buffer size insufficient: 26 < 27 es2=18 add=0400123456789abc0102030400040101aa0502bbcc0500ff0750415f6469666684b6e2 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0400123456789abc010203040002bf024142ff034344450050ae28",
            "OK v=0 s=false f=false x=true h=4660 sn=22136 fn=39612 ts=16909060 es=9 ms=24 exts=[191:4142,255:434445] payload=50ae28 e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=434445 first=invalid extension ID/invalid extension ID/invalid extension ID marshal=0400123456789abc010203040002bf024142ff034344450050ae28 rt=true short=ERR data track packet buffer size insufficient: 26 < 27 es2=13 add=0400123456789abc010203040003bf024142ff0750415f646966660050ae28 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0400123456789abc010203040002ff0241c2ff034344450050ae28",
            "OK v=0 s=false f=false x=true h=4660 sn=22136 fn=39612 ts=16909060 es=9 ms=24 exts=[255:41c2,255:434445] payload=50ae28 e2ee=ERR data track packet extension not found, id: 1 uts=ERR data track packet extension not found, id: 2 psid=nonutf8 first=invalid extension ID/invalid extension ID/ok marshal=0400123456789abc010203040002ff0241c2ff034344450050ae28 rt=true short=ERR data track packet buffer size insufficient: 26 < 27 es2=14 add=0400123456789abc010203040003ff0750415f64696666ff0343444550ae28 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0400123456789abc0102030400030202112220000000000000000000861def",
            "OK v=0 s=false f=false x=true h=4660 sn=22136 fn=39612 ts=16909060 es=6 ms=20 exts=[2:1122,32:] payload=861def e2ee=ERR data track packet extension not found, id: 1 uts=ERR extension data too short psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/extension data too short/invalid extension ID marshal=0400123456789abc010203040001020211222000861def rt=true short=ERR data track packet buffer size insufficient: 22 < 23 es2=15 add=0400123456789abc010203040004020211222000ff0750415f64696666000000861def big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0400123456789abc010203040004010100020101030102040103050104060105",
            "OK v=0 s=false f=false x=true h=4660 sn=22136 fn=39612 ts=16909060 es=18 ms=32 exts=[1:00,2:01,3:02,4:03,5:04,6:05] payload= e2ee=ERR extension data too short uts=ERR extension data too short psid=ERR data track packet extension not found, id: 255 first=extension data too short/invalid extension ID/invalid extension ID marshal=0400123456789abc010203040004010100020101030102040103050104060105 rt=true short=ERR data track packet buffer size insufficient: 31 < 32 es2=27 add=0400123456789abc010203040007010100020101030102040103050104060105ff0750415f64696666000000 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0c00b8af6b525efedefe50fe0004010d03101112131415161718191a1b000000",
            "OK v=0 s=false f=true x=true h=47279 sn=27474 fn=24318 ts=3741208830 es=15 ms=32 exts=[1:03101112131415161718191a1b] payload= e2ee=3:101112131415161718191a1b uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 first=ok/invalid extension ID/invalid extension ID marshal=0c00b8af6b525efedefe50fe0004010d03101112131415161718191a1b000000 rt=true short=ERR data track packet buffer size insufficient: 31 < 32 es2=24 add=0c00b8af6b525efedefe50fe0006010d03101112131415161718191a1bff0750415f646966660000 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "540084a87e8e21d16ce168ef0004010dfff8f9fafbfcfdfeff00010203200000",
            "OK v=2 s=true f=false x=true h=33960 sn=32398 fn=8657 ts=1826711791 es=17 ms=32 exts=[1:fff8f9fafbfcfdfeff00010203,32:] payload= e2ee=255:f8f9fafbfcfdfeff00010203 uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 first=ok/invalid extension ID/invalid extension ID marshal=540084a87e8e21d16ce168ef0004010dfff8f9fafbfcfdfeff00010203200000 rt=true short=ERR data track packet buffer size insufficient: 31 < 32 es2=26 add=540084a87e8e21d16ce168ef0006010dfff8f9fafbfcfdfeff000102032000ff0750415f64696666 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0400123456789abc010203040004810100020101030102040103050104060105bb18d5",
            "OK v=0 s=false f=false x=true h=4660 sn=22136 fn=39612 ts=16909060 es=18 ms=32 exts=[129:00,2:01,3:02,4:03,5:04,6:05] payload=bb18d5 e2ee=ERR data track packet extension not found, id: 1 uts=ERR extension data too short psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/invalid extension ID/invalid extension ID marshal=0400123456789abc010203040004810100020101030102040103050104060105bb18d5 rt=true short=ERR data track packet buffer size insufficient: 34 < 35 es2=27 add=0400123456789abc010203040007810100020101030102040103050104060105ff0750415f64696666000000bb18d5 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0400123456789abc01020304000501090001020304050607080209010203040506070809",
            "OK v=0 s=false f=false x=true h=4660 sn=22136 fn=39612 ts=16909060 es=22 ms=36 exts=[1:000102030405060708,2:010203040506070809] payload= e2ee=ERR extension data too short uts=72623859790382856 psid=ERR data track packet extension not found, id: 255 first=extension data too short/invalid extension ID/invalid extension ID marshal=0400123456789abc01020304000501090001020304050607080209010203040506070809 rt=true short=ERR data track packet buffer size insufficient: 35 < 36 es2=31 add=0400123456789abc01020304000801090001020304050607080209010203040506070809ff0750415f64696666000000 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0c00b8af6b525efedefe50fe0004010d03101112131415161718191a1b020000814c2f9d",
            "OK v=0 s=false f=true x=true h=47279 sn=27474 fn=24318 ts=3741208830 es=17 ms=32 exts=[1:03101112131415161718191a1b,2:] payload=814c2f9d e2ee=3:101112131415161718191a1b uts=ERR extension data too short psid=ERR data track packet extension not found, id: 255 first=ok/invalid extension ID/invalid extension ID marshal=0c00b8af6b525efedefe50fe0004010d03101112131415161718191a1b020000814c2f9d rt=true short=ERR data track packet buffer size insufficient: 35 < 36 es2=26 add=0c00b8af6b525efedefe50fe0006010d03101112131415161718191a1b0200ff0750415f64696666814c2f9d big=ERR extension size is too big: 256 > 255",
        ),
        (
            "0400123456789abc010203040005090900010203040506070802090102030405060708090341e7",
            "OK v=0 s=false f=false x=true h=4660 sn=22136 fn=39612 ts=16909060 es=22 ms=36 exts=[9:000102030405060708,2:010203040506070809] payload=0341e7 e2ee=ERR data track packet extension not found, id: 1 uts=72623859790382856 psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/invalid extension ID/invalid extension ID marshal=0400123456789abc010203040005090900010203040506070802090102030405060708090341e7 rt=true short=ERR data track packet buffer size insufficient: 38 < 39 es2=31 add=0400123456789abc01020304000809090001020304050607080209010203040506070809ff0750415f646966660000000341e7 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "8c00a4ed7831e912364fc44900060208000000000000004d010d02303132333435363738393a3b00",
            "OK v=4 s=false f=true x=true h=42221 sn=30769 fn=59666 ts=911197257 es=25 ms=40 exts=[2:000000000000004d,1:02303132333435363738393a3b] payload= e2ee=2:303132333435363738393a3b uts=77 psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/ok/invalid extension ID marshal=8c00a4ed7831e912364fc44900060208000000000000004d010d02303132333435363738393a3b00 rt=true short=ERR data track packet buffer size insufficient: 39 < 40 es2=34 add=8c00a4ed7831e912364fc44900080208000000000000004d010d02303132333435363738393a3bff0750415f64696666 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "94007720c56d958fa24de07f0006010d01202122232425262728292a2b0208ffffffffffffffff00",
            "OK v=4 s=true f=false x=true h=30496 sn=50541 fn=38287 ts=2723012735 es=25 ms=40 exts=[1:01202122232425262728292a2b,2:ffffffffffffffff] payload= e2ee=1:202122232425262728292a2b uts=18446744073709551615 psid=ERR data track packet extension not found, id: 255 first=ok/invalid extension ID/invalid extension ID marshal=94007720c56d958fa24de07f0006010d01202122232425262728292a2b0208ffffffffffffffff00 rt=true short=ERR data track packet buffer size insufficient: 39 < 40 es2=34 add=94007720c56d958fa24de07f0008010d01202122232425262728292a2b0208ffffffffffffffffff0750415f64696666 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "8c00a4ed7831e912364fc44900060208000000000000004d110d0230313a333435363738393a3b00b8cb3267",
            "OK v=4 s=false f=true x=true h=42221 sn=30769 fn=59666 ts=911197257 es=25 ms=40 exts=[2:000000000000004d,17:0230313a333435363738393a3b] payload=b8cb3267 e2ee=ERR data track packet extension not found, id: 1 uts=77 psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/ok/invalid extension ID marshal=8c00a4ed7831e912364fc44900060208000000000000004d110d0230313a333435363738393a3b00b8cb3267 rt=true short=ERR data track packet buffer size insufficient: 43 < 44 es2=34 add=8c00a4ed7831e912364fc44900080208000000000000004d110d0230313a333435363738393a3bff0750415f64696666b8cb3267 big=ERR extension size is too big: 256 > 255",
        ),
        (
            "8c00a4ed7831e912364fc44900068208000000000000004d010d02303132333435363738393a3b00b8cb3267",
            "OK v=4 s=false f=true x=true h=42221 sn=30769 fn=59666 ts=911197257 es=25 ms=40 exts=[130:000000000000004d,1:02303132333435363738393a3b] payload=b8cb3267 e2ee=2:303132333435363738393a3b uts=ERR data track packet extension not found, id: 2 psid=ERR data track packet extension not found, id: 255 first=invalid extension ID/invalid extension ID/invalid extension ID marshal=8c00a4ed7831e912364fc44900068208000000000000004d010d02303132333435363738393a3b00b8cb3267 rt=true short=ERR data track packet buffer size insufficient: 43 < 44 es2=34 add=8c00a4ed7831e912364fc44900088208000000000000004d010d02303132333435363738393a3bff0750415f64696666b8cb3267 big=ERR extension size is too big: 256 > 255",
        ),
    ];
    for (input, want) in PINNED {
        assert_eq!(*want, describe(&unhex(input)), "input {input:?}");
    }
}

// New for musivum: extensions of 64 KiB and more, from the same run. The original keeps the
// size of the extensions in 16 bits and writes the size field with 16 bit arithmetic: from
// 65531 bytes of extensions on, what `marshal` makes of a packet that `unmarshal` took does
// not read back, or reads back as another packet. Ported as it is. What the original gives
// for these inputs is too long to keep here: its SHA-256 is.
#[test]
fn test_extensions_of_64_kib_as_the_original() {
    use sha2::{Digest, Sha256};

    fn repeat_ext(n: usize, size: usize) -> Vec<u8> {
        let mut out = Vec::new();
        for i in 0..n {
            out.push((1 + i % 250) as u8);
            out.push(size as u8);
            for j in 0..size {
                out.push((i + j) as u8);
            }
        }
        out
    }
    fn raw(size_field: u16, exts: &[Vec<u8>], payload: &[u8]) -> Vec<u8> {
        let mut buf = vec![
            0x04, 0x00, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0x01, 0x02, 0x03, 0x04,
        ];
        buf.extend_from_slice(&size_field.to_be_bytes());
        for ext in exts {
            buf.extend_from_slice(ext);
        }
        buf.extend_from_slice(payload);
        buf
    }

    // the bytes of extensions, the input, how what is marshalled reads back, the SHA-256
    let cases: [(usize, Vec<u8>, &str, &str); 4] = [
        (
            65534,
            raw(
                16383,
                &[repeat_ext(254, 255), repeat_ext(1, 254)],
                &[0x75, 0x8e, 0x43, 0x21, 0x53],
            ),
            " rt=ERR data track packet extension size insufficient: 196606/3 < 142 ",
            "8793ae7d412b5828abde9c6523b118b1cfdb30fd8beec4f3521c7bba6abebb07",
        ),
        (
            65530,
            raw(
                16382,
                &[repeat_ext(254, 255), repeat_ext(1, 250)],
                &[0x91, 0xea, 0xf8, 0x56, 0x22],
            ),
            " rt=true ",
            "153161c00b0f10b14d7fd550321cdd092bdd1d70f0d1cf8459a448d964e90c83",
        ),
        (
            66562,
            raw(
                0x4100,
                &[repeat_ext(258, 255), repeat_ext(1, 254)],
                &[0x6d, 0xe0, 0x50, 0xd9, 0x5f],
            ),
            " rt=ERR data track packet extension size insufficient: 253/65794 < 255 ",
            "4b31817f0cfa73a6f798bf941236ac19ca20d4486c86cd26a66ab11d7847ba6e",
        ),
        (
            262142,
            raw(
                0xffff,
                &[repeat_ext(1020, 255), repeat_ext(1, 0)],
                &[0x9c, 0xc4, 0x53, 0x26, 0xb4],
            ),
            " rt=true ",
            "985253447d1e001793b303996d4c7bea3d7c61708c27f2b033045d84265e9ac8",
        ),
    ];
    for (size, input, read_back, sha256) in cases {
        assert_eq!(12 + 2 + size + 5, input.len(), "{size}");
        let got = describe(&input);
        assert!(got.contains(read_back), "{size}");
        assert_eq!(sha256, hex(&Sha256::digest(got.as_bytes())), "{size}");
    }
}
