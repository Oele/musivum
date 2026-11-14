//! New for musivum: the buffer held against the original, operation by operation.
//!
//! A script is a list of operations on the buffers of a stream, one per line: `new v vp8 ..`
//! makes a factory and the buffer of a stream (kind `vp8rtx` has a buffer for its repair
//! stream too), `w v <hex>` writes a packet into it and `x r <hex>` one into the buffer of
//! the repair stream, `bind`, `pair` (the repair stream is declared), `d` reads what there
//! is to read, `sr` hands it a sender report, `gp` asks for a packet of the history,
//! `phase` asks for the NACK queue, the statistics and a reception report, and so on.
//! `testdata/buffer_reference_test.go`, a Go test laid over the original package at the pin,
//! made the script (three streams: VP8, Opus, VP8 with a repair stream; in order, lost and
//! late, out of order and twice, padding only, a restart of the publisher, sequence numbers
//! and time stamps that wrap) and wrote down what the original answers to each operation;
//! `replay` below runs the script through the port and writes down the same. The header of
//! the Go file says what is left out because the original reads it from the clock.
//!
//! `testdata/buffer_reference.ops` and `testdata/buffer_reference.out` are a short script
//! and the original's answers, pinned here. `MUSIVUM_BUFFER_REFERENCE` may name a directory
//! with larger ones (`<name>.ops` and `<name>.go.out`), which are then replayed as well; what
//! the port answers is written next to them as `<name>.rust.out`.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use bytes::Bytes;

use super::super::{BufferPacketType, Factory, FactoryOfBufferFactory};
use super::*;
use crate::sfu::rtpextension::abscapturetime as act;
use crate::webrtc::sdp;

const REF_VP8_PT: u8 = 96;
const REF_RTX_PT: u8 = 97;
const REF_OPUS_PT: u8 = 111;

struct RefStream {
    kind: String,
    factory: Arc<Factory>,
    primary: Arc<Buffer>,
    rtx: Option<Arc<Buffer>>,
    ssrc: u32,
    rtx_ssrc: u32,
}

// FNV-1a, to keep the lines short.
fn ref_hash(p: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for &b in p {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
        .collect()
}

fn hex_encode(p: &[u8]) -> String {
    p.iter().map(|b| format!("{b:02x}")).collect()
}

fn ref_write_error(err: &Error) -> &'static str {
    if *err == Error::Eof { "eof" } else { "other" }
}

fn number<T: std::str::FromStr>(word: Option<&str>) -> T
where
    T::Err: std::fmt::Debug,
{
    word.expect("a number").parse().unwrap()
}

fn op_new(kind: &str, ssrc: u32, rtx_ssrc: u32) -> RefStream {
    let factory =
        FactoryOfBufferFactory::new(INIT_PACKET_BUFFER_SIZE_VIDEO, INIT_PACKET_BUFFER_SIZE_AUDIO)
            .create_buffer_factory();
    let primary = factory
        .get_or_new(BufferPacketType::Rtp, ssrc)
        .into_buffer()
        .unwrap();
    let rtx = (kind == "vp8rtx").then(|| {
        factory
            .get_or_new(BufferPacketType::Rtp, rtx_ssrc)
            .into_buffer()
            .unwrap()
    });
    RefStream {
        kind: kind.to_string(),
        factory,
        primary,
        rtx,
        ssrc,
        rtx_ssrc,
    }
}

fn op_bind(name: &str, s: &RefStream, out: &mut String) {
    let nack = vec![webrtc::RtcpFeedback {
        r#type: "nack".to_string(),
        ..Default::default()
    }];
    let vp8 = webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: "video/VP8".to_string(),
            clock_rate: 90000,
            rtcp_feedback: nack.clone(),
            ..Default::default()
        },
        payload_type: REF_VP8_PT,
    };
    let (params, codec, bitrate) = match s.kind.as_str() {
        "vp8" => (
            webrtc::RtpParameters {
                header_extensions: vec![webrtc::RtpHeaderExtensionParameter {
                    uri: act::ABS_CAPTURE_TIME_URI.to_string(),
                    id: 2,
                }],
                codecs: vec![vp8.clone()],
            },
            vp8.rtp_codec_capability,
            0,
        ),
        "opus" => {
            let opus = webrtc::RtpCodecParameters {
                rtp_codec_capability: webrtc::RtpCodecCapability {
                    mime_type: "audio/opus".to_string(),
                    clock_rate: 48000,
                    channels: 2,
                    rtcp_feedback: nack,
                    ..Default::default()
                },
                payload_type: REF_OPUS_PT,
            };
            (
                webrtc::RtpParameters {
                    header_extensions: vec![webrtc::RtpHeaderExtensionParameter {
                        uri: sdp::AUDIO_LEVEL_URI.to_string(),
                        id: 1,
                    }],
                    codecs: vec![opus.clone()],
                },
                opus.rtp_codec_capability,
                0,
            )
        }
        "vp8rtx" => {
            let rtx = webrtc::RtpCodecParameters {
                rtp_codec_capability: webrtc::RtpCodecCapability {
                    mime_type: "video/rtx".to_string(),
                    clock_rate: 90000,
                    sdp_fmtp_line: "apt=96".to_string(),
                    ..Default::default()
                },
                payload_type: REF_RTX_PT,
            };
            (
                webrtc::RtpParameters {
                    codecs: vec![vp8.clone(), rtx],
                    ..Default::default()
                },
                vp8.rtp_codec_capability,
                // more packets a second than the bucket has room for: it grows once
                5_000_000,
            )
        }
        kind => panic!("a stream of kind {kind}"),
    };
    let err = s.primary.bind(params, &codec, bitrate);

    // no receiver report by the clock
    let capacity = {
        let mut b = s.primary.lock.write();
        b.last_report_at = 1 << 62;
        b.buffer_base
            .bucket
            .as_ref()
            .map_or(0, |bucket| bucket.capacity())
    };
    writeln!(out, "{name} bind err={} capacity={capacity}", err.is_err()).unwrap();
}

// Reads what there is to read: see `opDrain` of the Go file. Every other read is the one a
// task makes.
fn op_drain(name: &str, s: &RefStream, reads: &mut usize, out: &mut String) {
    let mut buf = [0u8; 1500];
    let mut read = |buf: &mut [u8]| {
        *reads += 1;
        if (*reads).is_multiple_of(2) {
            s.primary.read_extended(buf)
        } else {
            time::runtime().block_on(s.primary.read_extended_async(buf))
        }
    };
    loop {
        if s.primary.is_closed() {
            let err = read(&mut buf).expect_err("a read of a closed buffer");
            writeln!(out, "{name} read err={}", ref_write_error(&err)).unwrap();
            return;
        }

        let (pending, readable) = {
            let lock = s.primary.lock.read();
            let b = &lock.buffer_base;
            let mut tmp = [0u8; 1500];
            let mut readable = 0;
            for ep in &b.ext_packets {
                if b.bucket
                    .as_ref()
                    .unwrap()
                    .get_packet(&mut tmp, ep.ext_sequence_number)
                    .is_ok()
                {
                    readable += 1;
                }
            }
            (b.is_restart_pending, readable)
        };
        if !pending && readable == 0 {
            return;
        }

        let ep = match read(&mut buf) {
            Err(err) => {
                writeln!(out, "{name} read err={}", ref_write_error(&err)).unwrap();
                return;
            }
            Ok(None) => {
                writeln!(out, "{name} restart").unwrap();
                continue;
            }
            Ok(Some(ep)) => ep,
        };

        let ext1 = match ep.packet.get_extension(1) {
            Some(e) => hex_encode(&e),
            None => "-".to_string(),
        };
        let vp8 = match &ep.vp8 {
            Some(vp8) => format!(
                "{},{},{},{},{},{}",
                vp8.picture_id,
                vp8.tl0picidx,
                vp8.tid,
                vp8.keyidx,
                vp8.header_size,
                vp8.is_key_frame
            ),
            None => "-".to_string(),
        };
        writeln!(
            out,
            "{name} ep esn={} ets={} sn={} ts={} m={} pt={} s={} t={} kf={} ooo={} buf={} plen={} ph={:016x} rawlen={} rh={:016x} ext1={ext1} vp8={vp8} act={}",
            ep.ext_sequence_number,
            ep.ext_timestamp,
            ep.packet.sequence_number,
            ep.packet.timestamp,
            ep.packet.marker,
            ep.packet.payload_type,
            ep.video_layer.spatial,
            ep.video_layer.temporal,
            ep.is_key_frame,
            ep.is_out_of_order,
            ep.is_buffered,
            ep.packet.payload.len(),
            ref_hash(&ep.packet.payload),
            ep.raw_packet.len(),
            ref_hash(&ep.raw_packet),
            ep.abs_capture_time_ext.is_some(),
        )
        .unwrap();
        // what the original has by its `RawPacket` being the caller's buffer
        assert_eq!(&buf[..ep.raw_packet.len()], &ep.raw_packet[..]);
    }
}

fn op_sender_report<'a>(
    name: &str,
    s: &RefStream,
    mut words: impl Iterator<Item = &'a str>,
    out: &mut String,
) {
    let ntp_timestamp: u64 = number(words.next());
    let rtp_timestamp: u32 = number(words.next());
    let at: i64 = number(words.next());
    let packets: u32 = number(words.next());
    let octets: u64 = number(words.next());
    s.primary
        .set_sender_report_data(Some(&livekit::RtcpSenderReportState {
            rtp_timestamp,
            ntp_timestamp,
            at,
            packets,
            octets,
            ..Default::default()
        }));
    match s.primary.get_sender_report_data() {
        None => writeln!(out, "{name} sr nil").unwrap(),
        Some(sr) => writeln!(
            out,
            "{name} sr rtp={} rtpext={} ntp={} at={} adj={} pkts={} octets={}",
            sr.rtp_timestamp,
            sr.rtp_timestamp_ext,
            sr.ntp_timestamp,
            sr.at,
            sr.at_adjusted,
            sr.packets,
            sr.octets
        )
        .unwrap(),
    }
}

fn op_get_packet(name: &str, s: &RefStream, esn: u64, out: &mut String) {
    let mut buf = [0u8; 1500];
    match s.primary.get_packet(&mut buf, esn) {
        Err(_) => writeln!(out, "{name} gp esn={esn} err").unwrap(),
        Ok(n) => writeln!(
            out,
            "{name} gp esn={esn} n={n} h={:016x}",
            ref_hash(&buf[..n])
        )
        .unwrap(),
    }
}

fn op_phase(name: &str, s: &RefStream, label: &str, out: &mut String) {
    // the sequence numbers that wait to be asked for again, in the order of the queue. Its
    // entries keep them to themselves: they are read from how an entry is printed.
    let mut queued = Vec::new();
    if let Some(nacker) = &s.primary.lock.read().buffer_base.nacker {
        for n in nacker.nacks() {
            let printed = format!("{n:?}");
            let seq_num = printed
                .split_once("seq_num: ")
                .expect("the sequence number of a nack")
                .1;
            let end = seq_num.find(|c: char| !c.is_ascii_digit()).unwrap();
            queued.push(seq_num[..end].to_string());
        }
    }
    writeln!(out, "{name} phase {label} nackq=[{}]", queued.join(" ")).unwrap();

    match s.primary.get_stats() {
        None => writeln!(out, "{name} stats nil").unwrap(),
        Some(stats) => {
            let mut gaps: Vec<i32> = stats.gap_histogram.keys().copied().collect();
            gaps.sort();
            let histogram: Vec<String> = gaps
                .iter()
                .map(|gap| format!("{gap}:{}", stats.gap_histogram[gap]))
                .collect();
            writeln!(
                out,
                "{name} stats packets={} bytes={} hdr={} lost={} loss={:.4} dup={} dupbytes={} duphdr={} pad={} padbytes={} padhdr={} ooo={} frames={} kf={} fec={} gaps=[{}]",
                stats.packets,
                stats.bytes,
                stats.header_bytes,
                stats.packets_lost,
                stats.packet_loss_percentage,
                stats.packets_duplicate,
                stats.bytes_duplicate,
                stats.header_bytes_duplicate,
                stats.packets_padding,
                stats.bytes_padding,
                stats.header_bytes_padding,
                stats.packets_out_of_order,
                stats.frames,
                stats.key_frames,
                stats.fec_packets,
                histogram.join(" "),
            )
            .unwrap();
        }
    }

    match s.primary.get_delta_stats() {
        None => writeln!(out, "{name} delta nil").unwrap(),
        Some(delta) => {
            let d = delta.rtp_stats.as_ref().unwrap();
            let same = delta
                .layers
                .get(&0)
                .and_then(|layer| layer.as_ref())
                .is_some_and(|layer| Arc::ptr_eq(layer, d));
            writeln!(
                out,
                "{name} delta packets={} bytes={} hdr={} dup={} dupbytes={} duphdr={} pad={} padbytes={} padhdr={} lost={} missing={} ooo={} frames={} layers={} same={same}",
                d.packets,
                d.bytes,
                d.header_bytes,
                d.packets_duplicate,
                d.bytes_duplicate,
                d.header_bytes_duplicate,
                d.packets_padding,
                d.bytes_padding,
                d.header_bytes_padding,
                d.packets_lost,
                d.packets_missing,
                d.packets_out_of_order,
                d.frames,
                delta.layers.len(),
            )
            .unwrap();
        }
    }

    let rr = s
        .primary
        .lock
        .write()
        .buffer_base
        .get_rtcp_reception_report_locked(0);
    match rr {
        None => writeln!(out, "{name} rr nil").unwrap(),
        Some(rr) => writeln!(
            out,
            "{name} rr ssrc={} frac={} total={} last={} lsr={}",
            rr.ssrc,
            rr.fraction_lost,
            rr.total_lost,
            rr.last_sequence_number,
            rr.last_sender_report
        )
        .unwrap(),
    }
}

fn replay(ops: &str) -> String {
    let mut streams: HashMap<String, RefStream> = HashMap::new();
    let mut reads = 0usize;
    let mut out = String::new();
    for line in ops.lines() {
        let mut words = line.split(' ');
        let op = words.next().unwrap();
        let name = words.next().unwrap();
        if op == "new" {
            let kind = words.next().unwrap();
            let s = op_new(kind, number(words.next()), number(words.next()));
            streams.insert(name.to_string(), s);
            writeln!(out, "{name} new").unwrap();
            continue;
        }
        let s = &streams[name];
        match op {
            "pair" => s.factory.set_rtx_pair(s.rtx_ssrc, s.ssrc, ""),
            "bind" => op_bind(name, s, &mut out),
            "w" => {
                let raw = hex_decode(words.next().unwrap());
                if let Err(err) = s.primary.write(Bytes::from(raw)) {
                    writeln!(out, "{name} w err={}", ref_write_error(&err)).unwrap();
                }
            }
            "x" => {
                let raw = hex_decode(words.next().unwrap());
                if let Err(err) = s.rtx.as_ref().unwrap().write(Bytes::from(raw)) {
                    writeln!(out, "{name} x err={}", ref_write_error(&err)).unwrap();
                }
            }
            "d" => op_drain(name, s, &mut reads, &mut out),
            "sr" => op_sender_report(name, s, &mut words, &mut out),
            "gp" => op_get_packet(name, s, number(words.next()), &mut out),
            "rtt" => s.primary.set_rtt(number(words.next())),
            "rd" => s.primary.set_stream_restart_detection(number(words.next())),
            "phase" => op_phase(name, s, words.next().unwrap(), &mut out),
            "close" => {
                let err = s.primary.close();
                writeln!(out, "{name} close err={}", err.is_err()).unwrap();
            }
            op => panic!("an operation {op}"),
        }
    }
    out
}

#[track_caller]
fn assert_same_lines(name: &str, expected: &str, actual: &str) {
    let expected: Vec<&str> = expected.lines().collect();
    let actual: Vec<&str> = actual.lines().collect();
    let mut differing = Vec::new();
    for i in 0..expected.len().max(actual.len()) {
        let (e, a) = (expected.get(i), actual.get(i));
        if e != a {
            differing.push(format!(
                "line {}:\n  the original: {}\n  the port:     {}",
                i + 1,
                e.unwrap_or(&"(no line)"),
                a.unwrap_or(&"(no line)")
            ));
        }
    }
    assert!(
        differing.is_empty(),
        "{name}: {} of {} lines differ, the first of them:\n{}",
        differing.len(),
        expected.len(),
        differing[..differing.len().min(5)].join("\n")
    );
}

#[test]
fn the_buffer_answers_a_pinned_script_as_the_original() {
    let ops = include_str!("testdata/buffer_reference.ops");
    let expected = include_str!("testdata/buffer_reference.out");
    assert_eq!(477, ops.lines().count());
    assert_eq!(355, expected.lines().count());
    assert_same_lines("testdata/buffer_reference", expected, &replay(ops));
}

// With the log captured at the most verbose level every line the buffer writes is
// formatted, the statistics in them included: the answers stay the same.
#[test]
fn the_buffer_logs_the_pinned_script_without_changing_its_answers() {
    let ops = include_str!("testdata/buffer_reference.ops");
    let expected = include_str!("testdata/buffer_reference.out");
    let mut actual = String::new();
    let log = crate::testutils::capture_log(tracing::Level::DEBUG, || actual = replay(ops));
    assert_same_lines("testdata/buffer_reference", expected, &actual);
    for message in [
        "binding track",
        "releasing queued packets on bind",
        "stream restart",
        "rtx payload too short",
        "unexpected rtx payload type",
        "rtp stats",
    ] {
        assert!(log.contains(message), "no line {message:?} in the log");
    }
}

#[test]
fn the_buffer_answers_the_scripts_of_musivum_buffer_reference_as_the_original() {
    let Ok(dir) = std::env::var("MUSIVUM_BUFFER_REFERENCE") else {
        return;
    };
    let mut names = Vec::new();
    for entry in std::fs::read_dir(&dir).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        if let Some(name) = name.strip_suffix(".ops") {
            names.push(name.to_string());
        }
    }
    names.sort();
    assert!(!names.is_empty(), "no scripts in {dir}");
    for name in names {
        let ops = std::fs::read_to_string(format!("{dir}/{name}.ops")).unwrap();
        let expected = std::fs::read_to_string(format!("{dir}/{name}.go.out")).unwrap();
        let actual = replay(&ops);
        std::fs::write(format!("{dir}/{name}.rust.out"), &actual).unwrap();
        println!(
            "{name}: {} operations, {} lines of the original compared",
            ops.lines().count(),
            expected.lines().count()
        );
        assert_same_lines(&name, &expected, &actual);
    }
}
