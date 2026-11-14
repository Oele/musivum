// New for musivum: the original has no test of this file.

use std::collections::HashMap;

use bytes::Bytes;

use super::*;
use crate::webrtc::interceptor::RtpHeaderExtension;

const MID: u8 = 4;
const RID: u8 = 5;
const RSID: u8 = 6;

fn stream_info(ssrc: u32, uris: &[&str]) -> webrtc::StreamInfo {
    webrtc::StreamInfo {
        ssrc,
        rtp_header_extensions: uris
            .iter()
            .enumerate()
            .map(|(i, uri)| RtpHeaderExtension {
                uri: uri.to_string(),
                id: i as i32 + MID as i32,
            })
            .collect(),
        ..Default::default()
    }
}

const ALL: [&str; 3] = [
    sdp::SDES_MID_URI,
    sdp::SDES_RTP_STREAM_ID_URI,
    sdp::SDES_REPAIR_RTP_STREAM_ID_URI,
];

fn interceptor() -> UnhandleSimulcastInterceptor {
    let tracks = HashMap::from([
        (
            1001,
            SimulcastTrackInfo {
                mid: "0".to_string(),
                stream_id: "q".to_string(),
                repair_ssrc: 2001,
                is_repair_stream: false,
            },
        ),
        (
            2001,
            SimulcastTrackInfo {
                mid: "0".to_string(),
                stream_id: "q".to_string(),
                repair_ssrc: 0,
                is_repair_stream: true,
            },
        ),
    ]);
    UnhandleSimulcastInterceptorFactory::new(vec![unhandle_simulcast_tracks(
        Logger::default(),
        tracks,
    )])
    .new_interceptor("")
}

fn packet(ssrc: u32, sequence_number: u16, extensions: &[(u8, &str)], payload: &[u8]) -> Vec<u8> {
    let mut header = rtp::Header {
        version: 2,
        payload_type: 96,
        sequence_number,
        timestamp: 3000 * sequence_number as u32,
        ssrc,
        ..Default::default()
    };
    for (id, value) in extensions {
        header
            .set_extension(*id, Bytes::copy_from_slice(value.as_bytes()))
            .unwrap();
    }
    rtp::Packet {
        header,
        payload: Bytes::copy_from_slice(payload),
        ..Default::default()
    }
    .marshal()
    .unwrap()
}

/// Passes a packet through the reader in a buffer of `capacity` bytes, and gives what the
/// packet is afterwards.
fn read(reader: &mut UnhandleSimulcastRtpReader, packet: &[u8], capacity: usize) -> rtp::Packet {
    let mut b = vec![0u8; capacity];
    b[..packet.len()].copy_from_slice(packet);
    let n = reader.read(&mut b, packet.len());
    let mut out = rtp::Packet::default();
    out.unmarshal(&Bytes::copy_from_slice(&b[..n])).unwrap();
    out
}

fn extension(packet: &rtp::Packet, id: u8) -> Option<String> {
    packet
        .get_extension(id)
        .map(|payload| String::from_utf8(payload.to_vec()).unwrap())
}

#[test]
fn test_bind_remote_stream() {
    let u = interceptor();

    // a stream of the tracks, with the three header extensions negotiated
    assert!(u.bind_remote_stream(&stream_info(1001, &ALL)).is_some());
    assert!(u.bind_remote_stream(&stream_info(2001, &ALL)).is_some());
    // a stream that is not one of the tracks
    assert!(u.bind_remote_stream(&stream_info(3001, &ALL)).is_none());
    // one of the three is missing
    for missing in 0..3 {
        let mut uris = ALL.to_vec();
        uris.remove(missing);
        assert!(u.bind_remote_stream(&stream_info(1001, &uris)).is_none());
    }

    // without the option there are no tracks
    let u = UnhandleSimulcastInterceptorFactory::new(Vec::new()).new_interceptor("");
    assert!(u.bind_remote_stream(&stream_info(1001, &ALL)).is_none());
}

#[test]
fn test_read_stamps_the_first_packets() {
    let u = interceptor();
    let payload = [1u8, 2, 3, 4, 5, 6, 7];

    // The stream of a layer: mid and rid are put on its packets, and nothing else changes.
    // Eleven packets are counted (the count runs from ten down to zero), then the reader lets
    // everything through.
    let mut reader = u.bind_remote_stream(&stream_info(1001, &ALL)).unwrap();
    for sn in 0..14u16 {
        let original = packet(1001, sn, &[], &payload);
        let out = read(&mut reader, &original, 1500);
        assert_eq!(
            (1001, sn, 96),
            (out.ssrc, out.sequence_number, out.payload_type)
        );
        assert_eq!(&payload[..], &out.payload[..], "packet {sn}");
        if sn < 11 {
            assert_eq!(Some("0".to_string()), extension(&out, MID), "packet {sn}");
            assert_eq!(Some("q".to_string()), extension(&out, RID), "packet {sn}");
            assert_eq!(None, extension(&out, RSID), "packet {sn}");
        } else {
            assert_eq!(original, out.marshal().unwrap(), "packet {sn}");
        }
    }

    // The repair stream: mid and the repaired stream's id. An extension the packet has
    // already is kept, a padding-only packet is stamped but not counted.
    let mut reader = u.bind_remote_stream(&stream_info(2001, &ALL)).unwrap();
    let mut padding = packet(2001, 100, &[], &[]);
    padding[0] |= 0x20;
    padding.extend_from_slice(&[0, 0, 0, 4]);
    for _ in 0..20 {
        let out = read(&mut reader, &padding, 1500);
        assert!(out.padding);
        assert_eq!(4, out.padding_size);
        assert!(out.payload.is_empty());
        assert_eq!(Some("0".to_string()), extension(&out, MID));
        assert_eq!(Some("q".to_string()), extension(&out, RSID));
    }
    for sn in 0..14u16 {
        let out = read(
            &mut reader,
            &packet(2001, sn, &[(MID, "7")], &payload),
            1500,
        );
        assert_eq!(&payload[..], &out.payload[..], "packet {sn}");
        assert_eq!(Some("7".to_string()), extension(&out, MID), "packet {sn}");
        assert_eq!(None, extension(&out, RID), "packet {sn}");
        assert_eq!(
            (sn < 11).then(|| "q".to_string()),
            extension(&out, RSID),
            "packet {sn}"
        );
    }
}

#[test]
fn test_read_stops_when_the_stream_says_what_it_is() {
    let u = interceptor();
    let payload = [9u8; 20];

    // A packet that has its mid and its rid ends the stamping at once.
    let mut reader = u.bind_remote_stream(&stream_info(1001, &ALL)).unwrap();
    let out = read(&mut reader, &packet(1001, 0, &[], &payload), 1500);
    assert_eq!(Some("q".to_string()), extension(&out, RID));
    let own = packet(1001, 1, &[(MID, "0"), (RID, "q")], &payload);
    assert_eq!(own, read(&mut reader, &own, 1500).marshal().unwrap());
    let bare = packet(1001, 2, &[], &payload);
    assert_eq!(bare, read(&mut reader, &bare, 1500).marshal().unwrap());

    // So does one of a repair stream that has its mid and its repaired stream's id.
    let mut reader = u.bind_remote_stream(&stream_info(2001, &ALL)).unwrap();
    let own = packet(2001, 0, &[(MID, "0"), (RSID, "q")], &payload);
    assert_eq!(own, read(&mut reader, &own, 1500).marshal().unwrap());
    let bare = packet(2001, 1, &[], &payload);
    assert_eq!(bare, read(&mut reader, &bare, 1500).marshal().unwrap());

    // A buffer that has no room for the extensions gives the packet back as it was, and
    // what is not RTP too.
    let mut reader = u.bind_remote_stream(&stream_info(1001, &ALL)).unwrap();
    let bare = packet(1001, 0, &[], &payload);
    assert_eq!(
        bare,
        read(&mut reader, &bare, bare.len() + 7).marshal().unwrap()
    );
    let out = read(&mut reader, &bare, bare.len() + 8);
    assert_eq!(bare.len() + 8, out.marshal().unwrap().len());
    assert_eq!(Some("q".to_string()), extension(&out, RID));
    let mut b = [1u8, 2, 3, 0, 0, 0, 0, 0];
    assert_eq!(3, reader.read(&mut b, 3));
    assert_eq!([1u8, 2, 3, 0, 0, 0, 0, 0], b);
}
