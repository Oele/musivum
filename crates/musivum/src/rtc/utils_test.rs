// Ported from LiveKit, pkg/rtc/utils_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; in `test_chunk_proto_batch` the lengths of the
// metadata, which the original draws from `rand.New(rand.NewPCG(1, 2))`, are written out,
// and the identifiers, random there, are made up with the same lengths. The tests after the
// three of the original are new.

use super::*;

use std::sync::Arc;

use parking_lot::Mutex;

#[test]
fn test_pack_stream_id() {
    let packed = "PA_123abc|uuid-id";
    let (p_id, track_id) = unpack_stream_id(packed);
    assert_eq!(livekit::ParticipantId::from("PA_123abc"), p_id);
    assert_eq!(livekit::TrackId::from("uuid-id"), track_id);

    assert_eq!(packed, pack_stream_id(&p_id, &track_id));
}

#[test]
fn test_pack_data_track_label() {
    let p_id = livekit::ParticipantId::from("PA_123abc");
    let track_id = livekit::TrackId::from("TR_b3da25");
    let label = "trackLabel";
    let packed = "PA_123abc|TR_b3da25|trackLabel";
    assert_eq!(packed, pack_data_track_label(&p_id, &track_id, label));

    let (p, tr, l) = unpack_data_track_label(packed);
    assert_eq!(p_id, p);
    assert_eq!(track_id, tr);
    assert_eq!(label, l);
}

// What `rng.IntN(128*1024)` gives 32 times for `rng := rand.New(rand.NewPCG(1, 2))`.
const METADATA_LENGTHS: [usize; 32] = [
    62736, 126060, 98440, 49562, 96592, 23228, 53881, 12174, 44441, 38064, 93423, 51274, 21435,
    7837, 4446, 5499, 10471, 126242, 50489, 29550, 111769, 78967, 118317, 75334, 90310, 55920,
    31699, 36065, 82012, 11242, 8371, 33550,
];

// The updates of the original's test: its participant IDs (`guid.New(guid.ParticipantPrefix)`)
// are 15 characters and its identities (`uuid.NewString()`) 36, both random.
fn updates_of_the_original() -> Vec<livekit::ParticipantInfo> {
    let mut updates: Vec<livekit::ParticipantInfo> = Vec::new();
    for (i, n) in METADATA_LENGTHS.into_iter().enumerate() {
        updates.push(livekit::ParticipantInfo {
            sid: format!("PA_{i:012}"),
            identity: format!("00000000-0000-4000-8000-{i:012}"),
            metadata: "x".repeat(n),
            ..Default::default()
        });
    }
    updates
}

#[test]
fn test_chunk_proto_batch() {
    let updates = updates_of_the_original();

    let target = 64 * 1024;
    let batches = chunk_proto_batch(&updates, target);
    let mut count = 0;
    for b in &batches {
        let mut sum = 0;
        for m in b.iter() {
            sum += m.encoded_len();
            count += 1;
        }
        assert!(sum < target || b.len() == 1, "batch size exceeds target");
    }
    assert_eq!(updates.len(), count);
}

// New for musivum, as all tests from here on. Where a test says that its expected values are
// the original's, they were printed by a Go test that ran the original at the pin with the
// same inputs.

// The chunks themselves, which the original's test does not look at: how many messages each
// has, for the batch of that test and for small ones. The expected values are the original's.
#[test]
fn chunks_are_those_of_the_original() {
    fn lens(chunks: Vec<&[livekit::ParticipantInfo]>) -> Vec<usize> {
        chunks.iter().map(|c| c.len()).collect()
    }

    let updates = updates_of_the_original();
    assert_eq!(
        vec![
            1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3
        ],
        lens(chunk_proto_batch(&updates, 64 * 1024))
    );
    assert_eq!(
        vec![2, 2, 4, 3, 6, 2, 2, 2, 2, 3, 4],
        lens(chunk_proto_batch(&updates, 200 * 1000))
    );
    // the chunks are the batch, in its order
    let chunks = chunk_proto_batch(&updates, 200 * 1000);
    assert_eq!(updates, chunks.concat());

    // messages with a metadata of these lengths: 2 bytes more each on the wire, but none for
    // an empty one
    let table: [(&[usize], usize, &[usize]); 11] = [
        (&[], 100, &[]),
        (&[10], 100, &[1]),
        (&[10, 10, 10], 100, &[3]),
        (&[50, 50, 50], 104, &[2, 1]),
        (&[50, 50, 50], 103, &[1, 1, 1]),
        (&[200], 100, &[1]),
        (&[200, 10, 10, 200, 200, 10], 100, &[1, 2, 1, 1, 1]),
        (&[10, 200, 10], 100, &[1, 1, 1]),
        (&[0, 0, 0], 0, &[3]),
        (&[98, 0, 0], 100, &[3]),
        (&[10, 20, 30, 40, 50], 64, &[2, 1, 1, 1]),
    ];
    for (sizes, target, expected) in table {
        let batch: Vec<livekit::ParticipantInfo> = sizes
            .iter()
            .map(|&n| livekit::ParticipantInfo {
                metadata: "x".repeat(n),
                ..Default::default()
            })
            .collect();
        assert_eq!(
            expected,
            lens(chunk_proto_batch(&batch, target)),
            "sizes {sizes:?}, target {target}"
        );
    }
}

// The expected values are the original's.
#[test]
fn stream_ids_and_labels_unpack_as_in_the_original() {
    // packed, then what `UnpackStreamID` and what `UnpackDataTrackLabel` make of it
    type StreamId<'a> = (&'a str, &'a str);
    type DataTrackLabel<'a> = (&'a str, &'a str, &'a str);
    let table: [(&str, StreamId, DataTrackLabel); 8] = [
        (
            "PA_123abc|uuid-id",
            ("PA_123abc", "uuid-id"),
            ("", "PA_123abc|uuid-id", ""),
        ),
        ("PA_123abc", ("PA_123abc", ""), ("", "PA_123abc", "")),
        ("", ("", ""), ("", "", "")),
        ("|", ("", ""), ("", "|", "")),
        ("a|b|c", ("a", "b|c"), ("a", "b", "c")),
        ("a||c", ("a", "|c"), ("a", "", "c")),
        ("|b", ("", "b"), ("", "|b", "")),
        ("a|", ("a", ""), ("", "a|", "")),
    ];
    for (packed, stream_id, data_track_label) in table {
        let (p, tr) = unpack_stream_id(packed);
        assert_eq!(stream_id, (p.as_str(), tr.as_str()), "{packed:?}");
        let (p, tr, l) = unpack_data_track_label(packed);
        assert_eq!(
            data_track_label,
            (p.as_str(), tr.as_str(), l.as_str()),
            "{packed:?}"
        );
    }

    assert_eq!(
        "PA_1|camera",
        pack_sync_stream_id(&livekit::ParticipantId::from("PA_1"), "camera")
    );
    assert_eq!(
        "|",
        pack_stream_id(
            &livekit::ParticipantId::from(""),
            &livekit::TrackId::from("")
        )
    );
}

// The expected values are the original's.
#[test]
fn track_sources_are_grouped_into_streams() {
    assert_eq!(
        "unknown",
        stream_from_track_source(livekit::TrackSource::Unknown)
    );
    assert_eq!(
        "camera",
        stream_from_track_source(livekit::TrackSource::Camera)
    );
    assert_eq!(
        "camera",
        stream_from_track_source(livekit::TrackSource::Microphone)
    );
    assert_eq!(
        "screen",
        stream_from_track_source(livekit::TrackSource::ScreenShare)
    );
    assert_eq!(
        "screen",
        stream_from_track_source(livekit::TrackSource::ScreenShareAudio)
    );
}

#[test]
fn codec_types_are_track_types() {
    assert_eq!(
        livekit::TrackType::Video,
        to_proto_track_kind(RtpCodecType::Video)
    );
    assert_eq!(
        livekit::TrackType::Audio,
        to_proto_track_kind(RtpCodecType::Audio)
    );
}

#[test]
#[should_panic(expected = "unsupported track direction")]
fn a_codec_type_that_is_neither_audio_nor_video_panics() {
    to_proto_track_kind(RtpCodecType::Unknown);
}

#[test]
fn the_closed_pipe_and_the_end_of_the_input_are_eof() {
    assert!(is_eof(&io::Error::from(io::ErrorKind::BrokenPipe)));
    assert!(is_eof(&io::Error::from(io::ErrorKind::UnexpectedEof)));
    assert!(!is_eof(&io::Error::from(io::ErrorKind::ConnectionReset)));
    assert!(!is_eof(&io::Error::other("closed")));
}

// What the logger writes, for the tests of `recover`.
#[derive(Clone, Default)]
struct Lines(Arc<Mutex<Vec<u8>>>);

impl io::Write for Lines {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Lines {
    fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + 'static {
        let writer = self.clone();
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .without_time()
            .with_writer(move || writer.clone())
            .finish()
    }

    fn text(&self) -> String {
        String::from_utf8(self.0.lock().clone()).unwrap()
    }
}

#[test]
fn recover_gives_the_result_or_the_panic_and_logs_the_panic() {
    let lines = Lines::default();
    let l = logger::get_logger().with_values(&[("participant", &"alice")]);
    tracing::subscriber::with_default(lines.subscriber(), || {
        // no panic: the result, and nothing is logged
        let r = recover(Some(&l), || 42);
        assert_eq!(Some(42), r.ok());

        // a panic with a text
        let r = recover(Some(&l), || -> i32 { panic!("boom") });
        assert_eq!(Some(&"boom"), r.unwrap_err().downcast_ref::<&str>());

        // a panic with a formatted text, and no logger: the root logger
        let n = 7;
        let r = recover(None, || -> i32 { panic!("boom {n}") });
        assert_eq!(
            Some("boom 7"),
            r.unwrap_err().downcast_ref::<String>().map(String::as_str)
        );

        // a panic with something else
        let r = recover(None, || -> i32 { panic::panic_any(42u8) });
        assert_eq!(Some(&42u8), r.unwrap_err().downcast_ref::<u8>());
    });

    let out = lines.text();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(3, lines.len(), "{out}");
    assert!(lines[0].starts_with("ERROR"), "{out}");
    assert!(
        lines[0].contains("recovered panic error=boom participant=\"alice\" panic=\"boom\""),
        "{out}"
    );
    assert!(
        lines[1].contains("recovered panic error=boom 7 panic=\"boom 7\""),
        "{out}"
    );
    assert!(
        lines[2].contains("recovered panic error=unknown panic panic=Any { .. }"),
        "{out}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_async_gives_the_result_or_the_panic_and_logs_the_panic() {
    let lines = Lines::default();
    let l = logger::get_logger().with_values(&[("participant", &"alice")]);
    let guard = tracing::subscriber::set_default(lines.subscriber());

    // no panic: the result, also of a future that is not ready at once
    let r = recover_async(Some(&l), async {
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        42
    })
    .await;
    assert_eq!(Some(42), r.ok());

    // a panic after the future has waited
    let r = recover_async(Some(&l), async {
        tokio::task::yield_now().await;
        if lines.text().is_empty() {
            panic!("boom");
        }
        42
    })
    .await;
    assert_eq!(Some(&"boom"), r.unwrap_err().downcast_ref::<&str>());

    // a panic at once, with something that is not a text, and no logger
    let r = recover_async(None, async {
        if lines.text().len() > 1 {
            panic::panic_any(42u8);
        }
        42
    })
    .await;
    assert_eq!(Some(&42u8), r.unwrap_err().downcast_ref::<u8>());

    drop(guard);
    let out = lines.text();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(2, lines.len(), "{out}");
    assert!(
        lines[0].contains("recovered panic error=boom participant=\"alice\" panic=\"boom\""),
        "{out}"
    );
    assert!(
        lines[1].contains("recovered panic error=unknown panic panic=Any { .. }"),
        "{out}"
    );
}

#[test]
fn loggers_get_the_values_of_the_original() {
    let l = logger::get_logger();
    let values = |l: Logger| format!("{l:?}");

    assert_eq!(
        "Logger( participant=\"alice\" participantID=\"PA_1\" remote=true)",
        values(logger_with_participant(
            &l,
            &livekit::ParticipantIdentity::from("alice"),
            &livekit::ParticipantId::from("PA_1"),
            true
        ))
    );
    assert_eq!(
        "Logger( remote=false)",
        values(logger_with_participant(
            &l,
            &livekit::ParticipantIdentity::from(""),
            &livekit::ParticipantId::from(""),
            false
        ))
    );

    assert_eq!(
        "Logger( room=\"lobby\" roomID=\"RM_1\")",
        values(logger_with_room(
            &l,
            &livekit::RoomName::from("lobby"),
            &livekit::RoomId::from("RM_1")
        ))
    );
    assert_eq!(
        "Logger( roomID=\"RM_1\")",
        values(logger_with_room(
            &l,
            &livekit::RoomName::from(""),
            &livekit::RoomId::from("RM_1")
        ))
    );
    assert_eq!(
        "Logger()",
        values(logger_with_room(
            &l,
            &livekit::RoomName::from(""),
            &livekit::RoomId::from("")
        ))
    );

    assert_eq!(
        "Logger( trackID=\"TR_1\" relayed=false)",
        values(logger_with_track(
            &l,
            &livekit::TrackId::from("TR_1"),
            false
        ))
    );
    assert_eq!(
        "Logger()",
        values(logger_with_track(&l, &livekit::TrackId::from(""), true))
    );

    assert_eq!(
        "Logger( transport=\"PUBLISHER\")",
        values(logger_with_pc_target(&l, livekit::SignalTarget::Publisher))
    );
    assert_eq!(
        "Logger( transport=\"SUBSCRIBER\")",
        values(logger_with_pc_target(&l, livekit::SignalTarget::Subscriber))
    );

    assert_eq!(
        "Logger( mime=\"video/VP8\")",
        values(logger_with_codec_mime(&l, mime::MimeType::Vp8))
    );
    assert_eq!(
        "Logger()",
        values(logger_with_codec_mime(&l, mime::MimeType::Unknown))
    );

    // the values are added to those the logger has
    let l = logger_with_room(
        &l,
        &livekit::RoomName::from("lobby"),
        &livekit::RoomId::from(""),
    );
    assert_eq!(
        "Logger( room=\"lobby\" trackID=\"TR_1\" relayed=true)",
        values(logger_with_track(&l, &livekit::TrackId::from("TR_1"), true))
    );
}

// The expected values are the original's: for each address what `MaybeTruncateIP` makes of it
// and whether it `IsIPv6`.
#[test]
fn addresses_are_truncated_as_in_the_original() {
    let table: &[(&str, &str, bool)] = &[
        ("", "", false),
        ("abc", "", false),
        ("1.2.3.4", "1.2.3.4", false),
        ("10.0.0.1", "10.0.0.1", false),
        ("10.1.2.3", "10.1.2.3", false),
        ("172.16.5.4", "172.16.5.4", false),
        ("172.32.5.4", "172.32....", false),
        ("172.31.255.255", "172.31.255.255", false),
        ("192.168.1.100", "192.168.1.100", false),
        ("192.169.1.100", "192.169.1....", false),
        ("203.0.113.57", "203.0.113...", false),
        ("192.0.2.1", "192.0....", false),
        ("198.51.100.7", "198.51.10...", false),
        ("1.2.3.45", "1.2.3.45", false),
        ("1.2.3.456", "", false),
        ("12.3.4.56", "12.3.4...", false),
        ("01.2.3.4", "", false),
        ("1.2.3", "", false),
        ("1.2.3.4.5", "", false),
        ("256.1.1.1", "", false),
        (" 1.2.3.4", "", false),
        ("1.2.3.4 ", "", false),
        ("::", "::", true),
        ("::1", "::1", true),
        ("fc00::1", "fc00::1", true),
        ("fd12:3456::1", "fd12:3456::1", true),
        ("fe80::1", "fe80::1", true),
        ("fe80::1%eth0", "", false),
        ("2001:db8::68", "2001:db8:...", true),
        ("2001:DB8::68", "2001:DB8:...", true),
        ("2001:db8:0:0:0:0:0:68", "2001:db8:0:0:0:0:0...", true),
        ("::ffff:10.0.0.1", "::ffff:10.0.0.1", false),
        ("::ffff:192.0.2.1", "::ffff:192.0....", false),
        ("::ffff:0a00:0001", "::ffff:0a00:0001", false),
        ("::ffff:c000:201", "::ffff:c000:...", false),
        ("::2", "::2", true),
        ("::1234", "::1234", true),
        ("::12345", "", false),
        ("fc00::", "fc00::", true),
        ("fe00::", "fe00::", true),
        ("2001:db8::", "2001:db...", true),
        ("FD00::1", "FD00::1", true),
        ("64:ff9b::192.0.2.33", "64:ff9b::192.0.2...", true),
        ("1:2:3:4:5:6:7::", "1:2:3:4:5:6:...", true),
        ("1:2:3:4:5:6:7:8::", "", false),
        ("::1:2:3:4:5:6:7:8", "", false),
        ("1:2:3:4:5:6:7:8", "1:2:3:4:5:6:...", true),
        ("12345::1", "", false),
        ("::ffff:1.2.3", "", false),
        ("[::1]", "", false),
        ("1.2.3.4:80", "", false),
        ("fb00::1", "fb00::1", true),
        (
            "fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            "fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            true,
        ),
        ("0.0.0.0", "0.0.0.0", false),
        ("1::", "1::", true),
        ("::1.2.3.4", "::1.2....", true),
        ("1:2:3:4:5:6:1.2.3.4", "1:2:3:4:5:6:1.2....", true),
        ("1:2:3:4:5:6:7:1.2.3.4", "", false),
        ("0x1.2.3.4", "", false),
        ("1.2.3.04", "", false),
        ("::00001", "", false),
        ("::ffff:192.168.1.1", "::ffff:192.168.1.1", false),
        ("::fffe:192.168.1.1", "::fffe:192.168....", true),
    ];
    for &(addr, truncated, v6) in table {
        assert_eq!(truncated, maybe_truncate_ip(addr), "{addr:?}");
        assert_eq!(v6, is_ipv6(addr), "{addr:?}");
    }
}

// The expected values are the original's.
#[test]
fn red_is_enabled_as_in_the_original() {
    fn track_info(mime_types: &[&str], disable_red: bool) -> livekit::TrackInfo {
        livekit::TrackInfo {
            codecs: mime_types
                .iter()
                .map(|mime_type| livekit::SimulcastCodecInfo {
                    mime_type: mime_type.to_string(),
                    ..Default::default()
                })
                .collect(),
            disable_red,
            ..Default::default()
        }
    }

    let table: [(&[&str], bool, bool); 8] = [
        (&[], false, true),
        (&[], true, false),
        (&["audio/red"], true, true),
        (&["AUDIO/RED"], false, true),
        (&["audio/opus"], false, false),
        (&[""], true, false),
        (&[""], false, true),
        (&["audio/opus", "audio/red"], false, false),
    ];
    for (mime_types, disable_red, expected) in table {
        assert_eq!(
            expected,
            is_red_enabled(&track_info(mime_types, disable_red)),
            "codecs {mime_types:?}, RED disabled {disable_red}"
        );
    }
}
