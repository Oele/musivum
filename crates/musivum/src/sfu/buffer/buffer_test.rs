// Ported from LiveKit, pkg/sfu/buffer/buffer_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the fields of the buffer that the tests reach into
// are behind its lock, which the tests take; a `sync.WaitGroup` is the `WaitGroup` of this
// file, which gives up after ten seconds instead of waiting forever; what a callback that
// runs on another task finds wrong is collected and asserted by the test (a panic there
// would not fail it); the goroutines of `TestGetPacketConcurrentWithWrite` are threads, as
// they block; the two benchmarks (`BenchmarkMemcpu`, `BenchmarkExtPacketFactory`) are left
// out, and so is the assertion on allocations of `TestIngestNoAlloc`, whose one input buffer
// that every write reuses cannot be made here: a write takes its bytes as `Bytes`. The
// differential test at the end and the test of the two ways to wait for a packet are new.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Weak};

use bytes::Bytes;
use parking_lot::{Condvar, Mutex};

use super::*;
use crate::go::time::Duration;
use crate::mediatransportutil::nack;
use crate::sfu::rtpextension::abscapturetime as act;
use crate::testutils;
use crate::webrtc::sdp;

pub(super) fn h265_codec() -> webrtc::RtpCodecParameters {
    webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: "video/h265".to_string(),
            clock_rate: 90000,
            rtcp_feedback: vec![webrtc::RtcpFeedback {
                r#type: "nack".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        },
        payload_type: 116,
    }
}

pub(super) fn vp8_codec() -> webrtc::RtpCodecParameters {
    webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: "video/vp8".to_string(),
            clock_rate: 90000,
            rtcp_feedback: vec![webrtc::RtcpFeedback {
                r#type: "nack".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        },
        payload_type: 96,
    }
}

pub(super) fn opus_codec() -> webrtc::RtpCodecParameters {
    webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: "audio/opus".to_string(),
            clock_rate: 48000,
            ..Default::default()
        },
        payload_type: 111,
    }
}

// Go's `sync.WaitGroup`: `done` panics when it is called more often than was added, as the
// original's does. `wait` gives up after ten seconds.
struct WaitGroup {
    count: Mutex<i64>,
    cond: Condvar,
}

impl WaitGroup {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            count: Mutex::new(0),
            cond: Condvar::new(),
        })
    }

    fn add(&self, delta: i64) {
        *self.count.lock() += delta;
    }

    fn done(&self) {
        let mut count = self.count.lock();
        *count -= 1;
        assert!(*count >= 0, "sync: negative WaitGroup counter");
        if *count == 0 {
            self.cond.notify_all();
        }
    }

    fn wait(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut count = self.count.lock();
        while *count != 0 {
            assert!(
                !self.cond.wait_until(&mut count, deadline).timed_out(),
                "the wait group still waits for {} after 10 s",
                *count
            );
        }
    }
}

const PAYLOAD: [u8; 8] = [0xff, 0xff, 0xff, 0xfd, 0xb4, 0x9f, 0x94, 0x1];

fn packet(payload_type: u8, sequence_number: u16, timestamp: u32, payload: &[u8]) -> rtp::Packet {
    rtp::Packet {
        header: rtp::Header {
            version: 2,
            payload_type,
            sequence_number,
            timestamp,
            ssrc: 123,
            ..Default::default()
        },
        payload: Bytes::copy_from_slice(payload),
        ..Default::default()
    }
}

fn set_codec_type(buff: &Buffer, codec_type: webrtc::RtpCodecType) {
    buff.lock.write().buffer_base.codec_type = codec_type;
}

fn set_rtt_of_nacker(buff: &Buffer, rtt: u32) {
    buff.lock
        .write()
        .buffer_base
        .nacker
        .as_mut()
        .expect("the buffer has a NACK queue")
        .set_rtt(rtt);
}

fn rtp_stats(buff: &Buffer) -> Arc<rtpstats::RtpStatsReceiver> {
    buff.lock
        .read()
        .buffer_base
        .rtp_stats
        .clone()
        .expect("the buffer is bound")
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nack() {
    // nack normal
    {
        let buff = Buffer::new(123, 1, 1);
        set_codec_type(&buff, webrtc::RtpCodecType::Video);
        let wg = WaitGroup::new();
        // 5 tries
        wg.add(5);
        buff.on_rtcp_feedback({
            let wg = wg.clone();
            move |fb| {
                for pkt in &fb {
                    if let Some(p) = pkt.as_any().downcast_ref::<TransportLayerNack>()
                        && p.nacks[0].packet_list()[0] == 1
                        && p.media_ssrc == 123
                    {
                        wg.done();
                    }
                }
            }
        });
        let _ = buff.bind(
            webrtc::RtpParameters {
                header_extensions: Vec::new(),
                codecs: vec![vp8_codec()],
            },
            &vp8_codec().rtp_codec_capability,
            0,
        );
        let rtt = 20u32;
        set_rtt_of_nacker(&buff, rtt);
        for i in 0..15 {
            if i == 1 {
                continue;
            }
            if i < 14 {
                time::sleep(Duration::from_millis(
                    (rtt as f64
                        * nack::NACK_QUEUE_PARAMS_DEFAULT
                            .backoff_factor
                            .powf(i as f64)
                        + 10.0) as i64,
                ))
                .await;
            } else {
                time::sleep(Duration::from_millis(500)).await; // even a long wait should not exceed max retries
            }
            let pkt = packet(96, i as u16, i as u32, &PAYLOAD);
            let b = pkt.marshal().unwrap();
            buff.write(Bytes::from(b)).unwrap();
        }
        wg.wait();
    }

    // nack with seq wrap
    {
        let buff = Buffer::new(123, 1, 1);
        set_codec_type(&buff, webrtc::RtpCodecType::Video);
        let wg = WaitGroup::new();
        let expects: [u16; 4] = [65534, 65535, 0, 1];
        wg.add(5 * expects.len() as i64); // retry 5 times
        buff.on_rtcp_feedback({
            let wg = wg.clone();
            move |fb| {
                for pkt in &fb {
                    if let Some(p) = pkt.as_any().downcast_ref::<TransportLayerNack>()
                        && p.media_ssrc == 123
                    {
                        for v in &p.nacks {
                            v.range(|seq| {
                                if expects.contains(&seq) {
                                    wg.done();
                                } else {
                                    panic!("unexpected nack seq {seq}");
                                }
                                true
                            });
                        }
                    }
                }
            }
        });
        let _ = buff.bind(
            webrtc::RtpParameters {
                header_extensions: Vec::new(),
                codecs: vec![vp8_codec()],
            },
            &vp8_codec().rtp_codec_capability,
            0,
        );
        let rtt = 30u32;
        set_rtt_of_nacker(&buff, rtt);
        for i in 0..15 {
            if i > 0 && i < 5 {
                continue;
            }
            if i < 14 {
                time::sleep(Duration::from_millis(
                    (rtt as f64
                        * nack::NACK_QUEUE_PARAMS_DEFAULT
                            .backoff_factor
                            .powf(i as f64)
                        + 10.0) as i64,
                ))
                .await;
            } else {
                time::sleep(Duration::from_millis(500)).await; // even a long wait should not exceed max retries
            }
            let pkt = packet(96, (i + 65533) as u16, i as u32, &PAYLOAD);
            let b = pkt.marshal().unwrap();
            buff.write(Bytes::from(b)).unwrap();
        }
        wg.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nack_on_rtx() {
    let buff = Buffer::new(123, 1, 1);
    set_codec_type(&buff, webrtc::RtpCodecType::Video);
    let nacked = Arc::new(Mutex::new(Vec::<u16>::new()));
    buff.on_rtcp_feedback({
        let nacked = nacked.clone();
        move |fb| {
            let mut nacked = nacked.lock();
            for pkt in &fb {
                if let Some(p) = pkt.as_any().downcast_ref::<TransportLayerNack>()
                    && p.media_ssrc == 123
                {
                    for pair in &p.nacks {
                        nacked.extend(pair.packet_list());
                    }
                }
            }
        }
    });
    assert_eq!(
        Ok(()),
        buff.bind(
            webrtc::RtpParameters {
                codecs: vec![vp8_codec()],
                ..Default::default()
            },
            &vp8_codec().rtp_codec_capability,
            0,
        )
    );
    buff.lock.write().buffer_base.rtx_payload_type = 97;
    let rtt = 20u32;
    set_rtt_of_nacker(&buff, rtt);

    // sequence numbers 1 and 2 are lost
    for sn in [0u16, 3] {
        let pkt = packet(96, sn, sn as u32, &PAYLOAD);
        let b = pkt.marshal().unwrap();
        assert_eq!(Ok(0), buff.write(Bytes::from(b)));
    }

    time::sleep(Duration::from_millis(2 * rtt as i64)).await;
    nacked.lock().clear();

    // RTX repairs sequence number 2, the NACK for 1 is due and must be sent
    let mut rtx_pkt = packet(97, 500, 2, &[&[0x00u8, 0x02][..], &PAYLOAD[..]].concat());
    rtx_pkt.ssrc = 456;
    buff.write_rtx(&rtx_pkt, time::now().unix_nano());

    assert_eq!(vec![1u16], *nacked.lock());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_buffer() {
    let tests = ["Must not be nil and add packets in sequence"];
    for name in tests {
        let test_packets = [
            packet(96, 65533, 0, &[]),
            packet(96, 65534, 0, &[1]),
            packet(96, 2, 0, &[]),
            packet(96, 65535, 0, &[]),
        ];
        let buff = Buffer::new(123, 1, 1);
        set_codec_type(&buff, webrtc::RtpCodecType::Video);
        buff.on_rtcp_feedback(|_| {});
        let _ = buff.bind(
            webrtc::RtpParameters {
                header_extensions: Vec::new(),
                codecs: vec![vp8_codec()],
            },
            &vp8_codec().rtp_codec_capability,
            0,
        );

        for p in &test_packets {
            let buf = p.marshal().unwrap();
            let _ = buff.write(Bytes::from(buf));
        }
        assert_eq!(2u16, rtp_stats(&buff).highest_sequence_number(), "{name}");
        assert_eq!(
            65536 + 2u64,
            rtp_stats(&buff).extended_highest_sequence_number(),
            "{name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fraction_lost_report() {
    let buff = Buffer::new(123, 1, 1);

    let wg = WaitGroup::new();

    // with loss proxying
    wg.add(1);
    buff.set_audio_loss_proxying(true);
    buff.set_last_fraction_lost_report(55);
    buff.on_rtcp_feedback({
        let wg = wg.clone();
        move |fb| {
            for pkt in &fb {
                if let Some(p) = pkt.as_any().downcast_ref::<ReceiverReport>() {
                    for v in &p.reports {
                        assert_eq!(55, v.fraction_lost);
                    }
                    wg.done();
                }
            }
        }
    });
    let _ = buff.bind(
        webrtc::RtpParameters {
            header_extensions: Vec::new(),
            codecs: vec![opus_codec()],
        },
        &opus_codec().rtp_codec_capability,
        0,
    );
    for i in 0..15 {
        let pkt = packet(111, i as u16, i as u32, &PAYLOAD);
        let b = pkt.marshal().unwrap();
        if i == 1 {
            time::sleep(Duration::from_secs(1)).await;
        }
        assert_eq!(Ok(0), buff.write(Bytes::from(b)));
    }
    wg.wait();

    wg.add(1);
    buff.set_audio_loss_proxying(false);
    buff.on_rtcp_feedback({
        let wg = wg.clone();
        move |fb| {
            for pkt in &fb {
                if let Some(p) = pkt.as_any().downcast_ref::<ReceiverReport>() {
                    for v in &p.reports {
                        assert_eq!(0, v.fraction_lost);
                    }
                    wg.done();
                }
            }
        }
    });
    let _ = buff.bind(
        webrtc::RtpParameters {
            header_extensions: Vec::new(),
            codecs: vec![opus_codec()],
        },
        &opus_codec().rtp_codec_capability,
        0,
    );
    for i in 0..15 {
        let pkt = packet(111, i as u16, i as u32, &PAYLOAD);
        let b = pkt.marshal().unwrap();
        if i == 1 {
            time::sleep(Duration::from_secs(1)).await;
        }
        assert_eq!(Ok(0), buff.write(Bytes::from(b)));
    }
    wg.wait();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_codec_change() {
    // what the callbacks, which run on tasks of their own, find wrong
    let failures = Arc::new(Mutex::new(Vec::<String>::new()));

    // codec change before bind
    let buff = Buffer::new(123, 1, 1);
    let (changed_codec_tx, changed_codec) = mpsc::sync_channel::<webrtc::RtpCodecParameters>(1);
    buff.on_codec_change({
        let failures = failures.clone();
        move |rp| {
            if changed_codec_tx.try_send(rp).is_err() {
                failures
                    .lock()
                    .push("codec change not consumed".to_string());
            }
        }
    });
    buff.on_stream_restart({
        let failures = failures.clone();
        let buff = Arc::downgrade(&buff);
        move |reason| {
            if reason != "codec-change" {
                failures
                    .lock()
                    .push(format!("stream restart for {reason:?}"));
            }

            // read once to clear pending restart
            let mut buf = [0u8; 1500];
            let Some(buff) = Weak::upgrade(&buff) else {
                return;
            };
            match buff.read_extended(&mut buf) {
                Ok(None) => {}
                other => failures
                    .lock()
                    .push(format!("read after a restart gave {other:?}")),
            }
        }
    });

    let mut h265_pkt = packet(116, 1, 1, &PAYLOAD);
    let buf = h265_pkt.marshal().unwrap();
    assert_eq!(Ok(0), buff.write(Bytes::from(buf)));

    assert!(
        changed_codec
            .recv_timeout(std::time::Duration::from_millis(100))
            .is_err(),
        "unexpected codec change"
    );

    // Bind sets up VP8 as expected codec,
    // packet written to the buffer above before Bind is H.265,
    // that should trigger a codec change and a stream restart
    // when the queued packets from Write before Bind get flushed
    let _ = buff.bind(
        webrtc::RtpParameters {
            header_extensions: Vec::new(),
            codecs: vec![vp8_codec(), h265_codec()],
        },
        &vp8_codec().rtp_codec_capability,
        0,
    );

    let c = changed_codec
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("expected codec change");
    assert_eq!(h265_codec(), c);

    // second codec change - writing VP8 packet after Bind should trigger another codec change
    let vp8_pkt = packet(96, 3, 3, &PAYLOAD);
    let buf = vp8_pkt.marshal().unwrap();
    assert_eq!(Ok(0), buff.write(Bytes::from(buf.clone())));

    let c = changed_codec
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("expected codec change");
    assert_eq!(vp8_codec(), c);

    // out of order pkts can't cause codec change
    // rewrite the VP8 packet to start the sequence after a stream restart
    assert_eq!(Ok(0), buff.write(Bytes::from(buf)));

    h265_pkt.sequence_number = 2;
    h265_pkt.timestamp = 2;
    let buf = h265_pkt.marshal().unwrap();
    assert_eq!(Ok(0), buff.write(Bytes::from(buf)));
    assert!(
        changed_codec
            .recv_timeout(std::time::Duration::from_millis(100))
            .is_err(),
        "unexpected codec change"
    );

    // unknown codec should not cause change even if it is in order
    h265_pkt.sequence_number = 4;
    h265_pkt.timestamp = 4;
    h265_pkt.payload_type = 117;
    let buf = h265_pkt.marshal().unwrap();
    assert_eq!(Ok(0), buff.write(Bytes::from(buf)));
    assert!(
        changed_codec
            .recv_timeout(std::time::Duration::from_millis(100))
            .is_err(),
        "unexpected codec change"
    );

    // an in-order packet should change codec again
    h265_pkt.sequence_number = 5;
    h265_pkt.timestamp = 5;
    h265_pkt.payload_type = 116;
    let buf = h265_pkt.marshal().unwrap();
    assert_eq!(Ok(0), buff.write(Bytes::from(buf)));
    let c = changed_codec
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("expected codec change");
    assert_eq!(h265_codec(), c);

    assert_eq!(Vec::<String>::new(), *failures.lock());
}

// Write unmarshals into reused scratch and callers reuse the input buffer, so each ExtPacket
// must keep its own header and extensions, and Write plus ReadExtended must not allocate per packet
#[tokio::test(flavor = "multi_thread")]
async fn test_ingest_no_alloc() {
    struct TestCase {
        name: &'static str,
        codec: webrtc::RtpCodecParameters,
        payload: Vec<u8>,
        // maxAllocs: the assertion on allocations is left out, see below
    }
    let test_cases = [
        TestCase {
            name: "opus",
            codec: opus_codec(),
            payload: vec![0u8; 100],
        },
        // VP8 boxes the parsed descriptor into ExtPacket.Payload
        TestCase {
            name: "vp8",
            codec: vp8_codec(),
            payload: vec![0x90, 0xe0, 0x80, 0x01, 0x05, 0x00, 0x01],
        },
    ];
    for tc in &test_cases {
        let name = tc.name;
        let buff = Buffer::new(123, 100, 100);
        assert_eq!(
            Ok(()),
            buff.bind(
                webrtc::RtpParameters {
                    header_extensions: vec![
                        webrtc::RtpHeaderExtensionParameter {
                            uri: sdp::AUDIO_LEVEL_URI.to_string(),
                            id: 1,
                        },
                        webrtc::RtpHeaderExtensionParameter {
                            uri: act::ABS_CAPTURE_TIME_URI.to_string(),
                            id: 2,
                        },
                    ],
                    codecs: vec![tc.codec.clone()],
                },
                &tc.codec.rtp_codec_capability,
                0,
            ),
            "{name}"
        );

        let marshal = |sn: u16, level: u8| -> Vec<u8> {
            let mut p = packet(tc.codec.payload_type, sn, sn as u32 * 960, &tc.payload);
            p.set_extension(1, Bytes::copy_from_slice(&[level]))
                .unwrap();
            p.set_extension(3, Bytes::from_static(&[0, 0])).unwrap();
            p.marshal().unwrap()
        };

        // one input buffer for all writes, like the transport read loop: here every write
        // has bytes of its own, see the header
        let write = |sn: u16, level: u8| {
            assert_eq!(Ok(0), buff.write(Bytes::from(marshal(sn, level))), "{name}");
        };

        let mut read_buf = vec![0u8; 1500];
        write(1, 10);
        write(2, 20);
        for (want_sn, want_level) in [(1u16, 10u8), (2, 20)] {
            let ep = buff.read_extended(&mut read_buf).unwrap().unwrap();
            assert_eq!(want_sn, ep.packet.sequence_number, "{name}");
            assert_eq!(
                Some(Bytes::copy_from_slice(&[want_level])),
                ep.packet.get_extension(1),
                "{name}"
            );
            assert_eq!(tc.payload, ep.packet.payload, "{name}");
            assert!(ep.abs_capture_time_ext.is_none(), "{name}");
        }

        let mut raw = marshal(3, 30);
        let mut sn = 3u16;
        let mut ingest = || {
            raw[2..4].copy_from_slice(&sn.to_be_bytes());
            raw[4..8].copy_from_slice(&(sn as u32 * 960).to_be_bytes());
            sn += 1;
            assert_eq!(Ok(0), buff.write(Bytes::copy_from_slice(&raw)), "{name}");
            let ep = buff.read_extended(&mut read_buf).unwrap();
            assert!(ep.is_some(), "{name}");
        };
        // allocs := testing.AllocsPerRun(1000, ingest), which calls it once more to warm up.
        // The assertion that there are at most `maxAllocs` allocations per Write plus
        // ReadExtended is left out: counting them needs `unsafe`, and the port allocates
        // where the original does not (see the header of `buffer_base.rs`).
        for _ in 0..1001 {
            ingest();
        }

        assert_eq!(Ok(()), buff.close(), "{name}");
    }
}

// a dropped padding only packet shifts later sequence numbers, ReadExtended must keep the shifted number
// in the header and in RawPacket
#[test]
fn test_ingest_dropped_padding_keeps_adjusted_sequence_number() {
    let buff = Buffer::new(123, 100, 100);
    assert_eq!(
        Ok(()),
        buff.bind(
            webrtc::RtpParameters {
                codecs: vec![opus_codec()],
                ..Default::default()
            },
            &opus_codec().rtp_codec_capability,
            0,
        )
    );

    let write = |sn: u16, padding_only: bool| {
        let mut p = packet(opus_codec().payload_type, sn, sn as u32 * 960, &[]);
        if padding_only {
            p.header.padding = true;
            p.header.padding_size = 20;
        } else {
            p.payload = Bytes::from(vec![0u8; 100]);
        }
        let raw = p.marshal().unwrap();
        assert_eq!(Ok(0), buff.write(Bytes::from(raw)));
    };

    write(40, false);
    write(41, true);
    write(42, false);

    let mut read_buf = vec![0u8; 1500];
    for want_sn in [40u16, 41] {
        let ep = buff.read_extended(&mut read_buf).unwrap().unwrap();
        assert_eq!(want_sn, ep.packet.sequence_number);
        assert_eq!(
            want_sn,
            u16::from_be_bytes([ep.raw_packet[2], ep.raw_packet[3]])
        );
        // what the original has by `RawPacket` being the caller's buffer
        assert_eq!(&read_buf[..ep.raw_packet.len()], &ep.raw_packet[..]);
    }

    assert_eq!(Ok(()), buff.close());
}

// BenchmarkMemcpu and BenchmarkExtPacketFactory are left out.

#[test]
fn test_get_packet_concurrent_with_write() {
    fn new_bound() -> Arc<Buffer> {
        let buff = Buffer::new(123, 1, 1);
        buff.on_rtcp_feedback(|_| {});
        assert_eq!(
            Ok(()),
            buff.bind(
                webrtc::RtpParameters {
                    codecs: vec![opus_codec()],
                    ..Default::default()
                },
                &opus_codec().rtp_codec_capability,
                0,
            )
        );
        buff
    }
    // the payload carries the sequence number, so a read can check it got the packet it asked for;
    // assert, not require, as this also runs off the test goroutine
    fn write(buff: &Buffer, sn: u16) {
        let mut payload = vec![0u8; 100];
        payload[..2].copy_from_slice(&sn.to_be_bytes());
        let pkt = packet(111, sn, sn as u32 * 960, &payload);
        let b = pkt.marshal().unwrap();
        assert_eq!(Ok(0), buff.write(Bytes::from(b)));
    }

    // a read runs while another read holds the lock, a write waits
    {
        let buff = new_bound();
        for sn in 1000u16..1010 {
            write(&buff, sn);
        }
        let esn = rtp_stats(&buff).extended_highest_sequence_number();

        let unlock = buff.lock.read(); // a read in progress

        let read_done = Arc::new(AtomicBool::new(false));
        let reader = std::thread::spawn({
            let (buff, read_done) = (buff.clone(), read_done.clone());
            move || {
                let n = buff.get_packet(&mut [0u8; 1500], esn);
                read_done.store(true, Ordering::SeqCst);
                assert!(matches!(n, Ok(n) if n > 0), "{n:?}");
            }
        });
        // a read must not wait for another read
        testutils::eventually(
            || read_done.load(Ordering::SeqCst),
            std::time::Duration::from_secs(1),
            std::time::Duration::from_millis(5),
        );
        reader.join().unwrap();

        let (write_done_tx, write_done) = mpsc::channel::<()>();
        let writer = std::thread::spawn({
            let buff = buff.clone();
            move || {
                write(&buff, 1010);
                let _ = write_done_tx.send(());
            }
        });
        assert!(
            write_done
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "a write must wait for the read to finish"
        );

        drop(unlock);
        assert!(
            write_done
                .recv_timeout(std::time::Duration::from_secs(1))
                .is_ok(),
            "the write must go through once the read is done"
        );
        writer.join().unwrap();
    }

    // concurrent reads get the packet they ask for while writes continue
    {
        let buff = new_bound();
        const FIRST: u16 = 2000;
        const NUM_PACKETS: u16 = 2000;
        write(&buff, FIRST);
        let rtp_stats = rtp_stats(&buff);
        let base = rtp_stats.extended_highest_sequence_number() - FIRST as u64;

        let stop = Arc::new(AtomicBool::new(false));
        let mut readers = Vec::new();
        for _ in 0..4 {
            readers.push(std::thread::spawn({
                let (buff, rtp_stats, stop) = (buff.clone(), rtp_stats.clone(), stop.clone());
                move || {
                    let mut buf = vec![0u8; 1500];
                    let mut read = 0;
                    while !stop.load(Ordering::SeqCst) {
                        let esn = rtp_stats.extended_highest_sequence_number();
                        let Ok(n) = buff.get_packet(&mut buf, esn) else {
                            continue; // the ring may have moved on
                        };
                        let mut pkt = rtp::Packet::default();
                        pkt.unmarshal(&Bytes::copy_from_slice(&buf[..n])).unwrap();
                        assert_eq!((esn - base) as u16, pkt.sequence_number);
                        assert_eq!(
                            pkt.sequence_number,
                            u16::from_be_bytes([pkt.payload[0], pkt.payload[1]])
                        );
                        read += 1;
                    }
                    read
                }
            }));
        }

        for sn in FIRST + 1..FIRST + NUM_PACKETS {
            write(&buff, sn);
        }
        stop.store(true, Ordering::SeqCst);
        for reader in readers {
            let _read: i32 = reader.join().unwrap();
        }
    }
}

// New for musivum: the original has no test in which a read has to wait. A read waits, as a
// thread and as a task, until a packet is written, the stream is marked for a restart or the
// buffer is closed.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_waits_for_a_packet_a_restart_and_the_close() {
    let buff = Buffer::new(123, 100, 100);
    assert_eq!(
        Ok(()),
        buff.bind(
            webrtc::RtpParameters {
                codecs: vec![opus_codec()],
                ..Default::default()
            },
            &opus_codec().rtp_codec_capability,
            0,
        )
    );
    let write = |sn: u16| {
        let b = packet(111, sn, sn as u32 * 960, &PAYLOAD)
            .marshal()
            .unwrap();
        assert_eq!(Ok(0), buff.write(Bytes::from(b)));
    };
    // what a read gave: the sequence number of a packet, "restart" or the error
    fn outcome(res: Result<Option<ExtPacket>, Error>) -> String {
        match res {
            Ok(Some(ep)) => ep.packet.sequence_number.to_string(),
            Ok(None) => "restart".to_string(),
            Err(err) => err.to_string(),
        }
    }
    const NOTHING_YET: std::time::Duration = std::time::Duration::from_millis(50);
    const SOON: std::time::Duration = std::time::Duration::from_secs(5);

    // a thread
    let (results_tx, results) = mpsc::channel::<String>();
    let reader = std::thread::spawn({
        let buff = buff.clone();
        move || {
            let mut buf = vec![0u8; 1500];
            loop {
                let res = buff.read_extended(&mut buf);
                let closed = matches!(res, Err(Error::Eof));
                results_tx.send(outcome(res)).unwrap();
                if closed {
                    return;
                }
            }
        }
    });
    assert!(results.recv_timeout(NOTHING_YET).is_err());
    write(10);
    assert_eq!(Ok("10".to_string()), results.recv_timeout(SOON));
    assert!(results.recv_timeout(NOTHING_YET).is_err());
    buff.mark_for_restart_stream("test");
    assert_eq!(Ok("restart".to_string()), results.recv_timeout(SOON));
    write(11);
    write(12);
    assert_eq!(Ok("11".to_string()), results.recv_timeout(SOON));
    assert_eq!(Ok("12".to_string()), results.recv_timeout(SOON));
    assert!(results.recv_timeout(NOTHING_YET).is_err());

    // a task, next to the thread: each packet goes to one of the two
    let (task_results_tx, mut task_results) = tokio::sync::mpsc::unbounded_channel::<String>();
    let task = tokio::spawn({
        let buff = buff.clone();
        async move {
            let mut buf = vec![0u8; 1500];
            loop {
                // through the trait, as a receiver calls it
                let provider: &dyn BufferProvider = &*buff;
                let res = provider.read_extended_async(&mut buf).await;
                let closed = matches!(res, Err(Error::Eof));
                task_results_tx.send(outcome(res)).unwrap();
                if closed {
                    return;
                }
            }
        }
    });
    tokio::time::sleep(NOTHING_YET).await;
    assert!(task_results.try_recv().is_err());
    let mut seen = Vec::new();
    for sn in 13..33 {
        write(sn);
        // one of the two readers has it before the next is written
        let deadline = std::time::Instant::now() + SOON;
        seen.push(loop {
            if let Ok(got) = task_results.try_recv() {
                break got;
            }
            if let Ok(got) = results.try_recv() {
                break got;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "nobody read packet {sn}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        });
    }
    let expected: Vec<String> = (13..33).map(|sn| sn.to_string()).collect();
    assert_eq!(expected, seen);

    // closing ends both
    assert_eq!(Ok(()), buff.close());
    assert_eq!(Ok("EOF".to_string()), results.recv_timeout(SOON));
    assert_eq!(
        Some("EOF".to_string()),
        tokio::time::timeout(SOON, task_results.recv())
            .await
            .unwrap()
    );
    reader.join().unwrap();
    task.await.unwrap();

    // and a read of a closed buffer does not wait
    assert_eq!(
        Err(Error::Eof),
        buff.read_extended(&mut [0u8; 1500]).map(|_| ())
    );
}
