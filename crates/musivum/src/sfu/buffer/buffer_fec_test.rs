// Ported from LiveKit, pkg/sfu/buffer/buffer_fec_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the fields of the buffer that the tests reach into
// are behind its lock, which the tests take; `readExtSequenceNumbers` reads as a task does
// (`read_extended_async`) and gives up after five seconds where the original would wait
// forever for a packet that does not come; AES-GCM, which `crypto/aes` and `crypto/cipher`
// give the original, is the crate `ring`'s; the FEC decoder is a value, so the assertion
// that a restart makes another one (`require.NotSame`) is left to the counters that are
// asserted after it; the media packets are those of the port's `rtptest`, whose sizes and
// bytes are not the original's.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Weak};

use bytes::Bytes;
use parking_lot::Mutex;
use ring::aead;

use super::super::{BufferPacketType, FactoryOfBufferFactory};
use super::buffer_test::vp8_codec;
use super::*;
use crate::go::time::Duration;
use crate::sfu::testutils::rtptest;
use crate::webrtc::interceptor::flexfec::FlexEncoder03;

const FEC_TEST_MEDIA_SSRC: u32 = 0x11111111;
const FEC_TEST_FEC_SSRC: u32 = 0x22222222;
const FEC_TEST_FEC_PT: u8 = 115;

fn flex_fec_codec() -> webrtc::RtpCodecParameters {
    webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: webrtc::MIME_TYPE_FLEX_FEC03.to_string(),
            clock_rate: 90000,
            sdp_fmtp_line: "repair-window=10000000".to_string(),
            ..Default::default()
        },
        payload_type: FEC_TEST_FEC_PT,
    }
}

#[track_caller]
fn bind_fec_test_buffer(buff: &Buffer) {
    buff.lock.write().buffer_base.codec_type = webrtc::RtpCodecType::Video;
    assert_eq!(
        Ok(()),
        buff.bind(
            webrtc::RtpParameters {
                codecs: vec![vp8_codec(), flex_fec_codec()],
                ..Default::default()
            },
            &vp8_codec().rtp_codec_capability,
            0,
        )
    );
}

#[track_caller]
fn write_packet(buff: &Buffer, pkt: &rtp::Packet) {
    let raw = pkt.marshal().unwrap();
    assert_eq!(Ok(0), buff.write(Bytes::from(raw)));
}

// readExtSequenceNumbers drains count ExtPackets and returns sequence number
// -> extended sequence number of everything seen.
async fn read_ext_sequence_numbers(buff: &Buffer, count: usize) -> HashMap<u16, u64> {
    let mut seen = HashMap::with_capacity(count);
    let mut buf = [0u8; 1500];
    for i in 0..count {
        let ext_pkt = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            buff.read_extended_async(&mut buf),
        )
        .await
        .unwrap_or_else(|_| panic!("packet {i} of {count} did not come"))
        .unwrap()
        .expect("a packet");
        seen.insert(ext_pkt.packet.sequence_number, ext_pkt.ext_sequence_number);
    }
    seen
}

// requireRecoveredInBucket asserts that the dropped packet was placed into
// the buffer's bucket (where downstream NACKs are served from), matching the
// behavior of RTX repaired packets.
#[track_caller]
fn require_recovered_in_bucket(
    buff: &Buffer,
    dropped: &rtp::Packet,
    ext_sn_by_sn: &HashMap<u16, u64>,
    ref_sn: u16,
) {
    let ref_ext_sn = *ext_sn_by_sn
        .get(&ref_sn)
        .unwrap_or_else(|| panic!("reference sequence number {ref_sn} not seen"));
    let dropped_ext_sn = ref_ext_sn + dropped.sequence_number.wrapping_sub(ref_sn) as u64;

    let mut buf = [0u8; 1500];
    let n = buff
        .get_packet(&mut buf, dropped_ext_sn)
        .expect("recovered packet not found in bucket");

    let mut pkt = rtp::Packet::default();
    pkt.unmarshal(&Bytes::copy_from_slice(&buf[..n])).unwrap();
    assert_eq!(dropped.sequence_number, pkt.sequence_number);
    assert_eq!(dropped.timestamp, pkt.timestamp);
    assert_eq!(dropped.payload, pkt.payload);
}

fn nacks_queued(buff: &Buffer) -> usize {
    buff.lock
        .read()
        .buffer_base
        .nacker
        .as_ref()
        .expect("the buffer has a NACK queue")
        .nacks()
        .len()
}

fn new_buffer(factory: &super::super::Factory, ssrc: u32) -> Arc<Buffer> {
    factory
        .get_or_new(BufferPacketType::Rtp, ssrc)
        .into_buffer()
        .expect("a buffer for RTP")
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_pending_packet_limit() {
    let make_repair_packet = |sequence_number: u16| rtp::Packet {
        header: rtp::Header {
            version: 2,
            payload_type: FEC_TEST_FEC_PT,
            sequence_number,
            ssrc: FEC_TEST_FEC_SSRC,
            ..Default::default()
        },
        payload: Bytes::from(vec![0u8; 1200]),
        ..Default::default()
    };
    let write_repair_packets = |buff: &Buffer, count: usize| {
        for i in 0..count {
            let packet = make_repair_packet(i as u16);
            write_packet(buff, &packet);
        }
    };
    let assert_retained_tail = |buff: &Buffer, total: usize| {
        let b = buff.lock.read();
        assert_eq!(MAX_PENDING_FEC_REPAIR_PACKETS, b.p_packets.len());

        let (mut first, mut last) = (rtp::Packet::default(), rtp::Packet::default());
        first.unmarshal(&b.p_packets[0].packet).unwrap();
        last.unmarshal(&b.p_packets[b.p_packets.len() - 1].packet)
            .unwrap();
        assert_eq!(
            total - MAX_PENDING_FEC_REPAIR_PACKETS,
            first.sequence_number as usize
        );
        assert_eq!(total - 1, last.sequence_number as usize);
    };

    const PACKET_COUNT: usize = MAX_PENDING_FEC_REPAIR_PACKETS + 50;
    // pair declared before repair stream
    {
        let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();
        factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);
        let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);

        write_repair_packets(&fec_buff, PACKET_COUNT);
        assert_retained_tail(&fec_buff, PACKET_COUNT);
    }

    // pair declared after repair packets
    {
        let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();
        let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
        write_repair_packets(&fec_buff, PACKET_COUNT);
        assert_eq!(PACKET_COUNT, fec_buff.lock.read().p_packets.len());

        factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);
        assert_retained_tail(&fec_buff, PACKET_COUNT);
        assert!(fec_buff.lock.read().p_packets.capacity() <= MAX_PENDING_FEC_REPAIR_PACKETS);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_recovers_dropped_packet() {
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);

    bind_fec_test_buffer(&primary);

    // (recoveredDelta, receivedDelta)
    let deltas = Arc::new(Mutex::new((0usize, 0usize)));
    primary.on_fec_recovery({
        let deltas = deltas.clone();
        move |received, recovered, _discarded, _bytes_received| {
            let mut deltas = deltas.lock();
            deltas.0 += recovered;
            deltas.1 += received;
        }
    });

    let media =
        rtptest::generate_vp8_packets(100, 10, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    let mut encoder = FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC);
    let fec_packets = encoder.encode_fec(&media, 2);
    assert!(!fec_packets.is_empty());

    const DROPPED_IDX: usize = 3;
    for (i, pkt) in media.iter().enumerate() {
        if i == DROPPED_IDX {
            continue;
        }
        write_packet(&primary, pkt);
    }
    for pkt in &fec_packets {
        write_packet(&fec_buff, pkt);
    }

    let stats = primary.fec_decoder_stats();
    assert_eq!(fec_packets.len() as u64, stats.fec_packets_received);
    assert_eq!(1, stats.packets_recovered);
    assert_eq!(0, stats.fec_packets_discarded);
    let (recovered_delta, received_delta) = *deltas.lock();
    assert_eq!(1, recovered_delta);
    assert_eq!(fec_packets.len(), received_delta);

    let mut fec_payload_bytes = 0;
    for pkt in &fec_packets {
        fec_payload_bytes += pkt.payload.len();
    }
    let rtp_stats = primary.get_stats().expect("statistics");
    assert_eq!(fec_packets.len() as u32, rtp_stats.fec_packets);
    assert_eq!(fec_payload_bytes as u64, rtp_stats.fec_bytes);
    assert_eq!(0, rtp_stats.fec_packets_discarded);
    assert_eq!(1, rtp_stats.fec_packets_recovered);

    // the 9 received packets flow through the ext packet pipeline, the
    // recovered one fills the bucket like an RTX repair
    let ext_sn_by_sn = read_ext_sequence_numbers(&primary, media.len() - 1).await;
    require_recovered_in_bucket(
        &primary,
        &media[DROPPED_IDX],
        &ext_sn_by_sn,
        media[0].sequence_number,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_recovers_encrypted_payload() {
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();
    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    bind_fec_test_buffer(&primary);

    // aes.NewCipher of a key of 16 bytes and cipher.NewGCM: AES-128-GCM
    let aead = aead::LessSafeKey::new(
        aead::UnboundKey::new(&aead::AES_128_GCM, b"flexfec-e2ee-key").unwrap(),
    );
    let nonce_for = |sequence_number: u16| {
        let mut nonce = [0u8; aead::NONCE_LEN];
        nonce[aead::NONCE_LEN - 8..].copy_from_slice(&(sequence_number as u64).to_be_bytes());
        aead::Nonce::assume_unique_for_key(nonce)
    };

    let mut media =
        rtptest::generate_vp8_packets(120, 5, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    let mut plaintexts: Vec<Vec<u8>> = Vec::with_capacity(media.len());
    for (i, pkt) in media.iter_mut().enumerate() {
        plaintexts.push([&b"encrypted-video-frame-"[..], &[i as u8][..]].concat());
        // E2EE encrypts the encoded frame before RTP packetization. The VP8
        // payload descriptor remains clear while the frame bytes are opaque to
        // the SFU and authenticated end-to-end.
        let mut sealed = plaintexts[i].clone();
        aead.seal_in_place_append_tag(
            nonce_for(pkt.sequence_number),
            aead::Aad::empty(),
            &mut sealed,
        )
        .unwrap();
        pkt.payload = Bytes::from([&[0x10u8][..], &sealed[..]].concat());
    }

    let fec_packets = FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC).encode_fec(&media, 1);
    assert_eq!(1, fec_packets.len());

    const DROPPED_IDX: usize = 2;
    for (i, pkt) in media.iter().enumerate() {
        if i != DROPPED_IDX {
            write_packet(&primary, pkt);
        }
    }
    write_packet(&fec_buff, &fec_packets[0]);

    assert_eq!(1, primary.fec_decoder_stats().packets_recovered);
    let ext_sn_by_sn = read_ext_sequence_numbers(&primary, media.len() - 1).await;
    require_recovered_in_bucket(
        &primary,
        &media[DROPPED_IDX],
        &ext_sn_by_sn,
        media[0].sequence_number,
    );

    let ref_ext_sn = ext_sn_by_sn[&media[0].sequence_number];
    let dropped_ext_sn = ref_ext_sn
        + media[DROPPED_IDX]
            .sequence_number
            .wrapping_sub(media[0].sequence_number) as u64;
    let mut raw = [0u8; 1500];
    let n = primary.get_packet(&mut raw, dropped_ext_sn).unwrap();
    let mut recovered = rtp::Packet::default();
    recovered
        .unmarshal(&Bytes::copy_from_slice(&raw[..n]))
        .unwrap();

    let mut opened = recovered.payload[1..].to_vec();
    let decrypted = aead
        .open_in_place(
            nonce_for(recovered.sequence_number),
            aead::Aad::empty(),
            &mut opened,
        )
        .expect("FlexFEC recovery must preserve authenticated ciphertext byte-for-byte");
    assert_eq!(&plaintexts[DROPPED_IDX][..], &decrypted[..]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_recovery_callback_can_reenter_buffer() {
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);
    bind_fec_test_buffer(&primary);

    let (callback_done_tx, callback_done) = mpsc::sync_channel::<()>(1);
    primary.on_fec_recovery({
        let primary = Arc::downgrade(&primary);
        move |_received, _recovered, _discarded, _bytes_received| {
            if let Some(primary) = Weak::upgrade(&primary) {
                primary.fec_decoder_stats();
            }
            callback_done_tx.send(()).unwrap();
        }
    });

    let media =
        rtptest::generate_vp8_packets(150, 5, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    for pkt in &media {
        write_packet(&primary, pkt);
    }
    let fec_packets = FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC).encode_fec(&media, 1);
    assert!(!fec_packets.is_empty());

    let raw_fec = fec_packets[0].marshal().unwrap();
    let (write_done_tx, write_done) = mpsc::sync_channel::<Result<usize, Error>>(1);
    std::thread::spawn({
        let fec_buff = fec_buff.clone();
        move || {
            let res = fec_buff.write(Bytes::from(raw_fec));
            write_done_tx.send(res).unwrap();
        }
    });

    assert!(
        callback_done
            .recv_timeout(std::time::Duration::from_secs(1))
            .is_ok(),
        "FEC recovery callback deadlocked while re-entering the buffer"
    );

    let res = write_done
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("FEC packet write did not return after callback");
    assert_eq!(Ok(0), res);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_pair_after_packets() {
    // FEC packets arriving before the ssrc-group is known are queued as
    // pending and replayed when the pair is established. Protected media is
    // recovered from the primary packet bucket even when pairing is late.
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    bind_fec_test_buffer(&primary);

    let mut encoder = FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC);
    let media =
        rtptest::generate_vp8_packets(200, 10, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    let fec_packets = encoder.encode_fec(&media, 2);
    assert!(!fec_packets.is_empty());

    const FIRST_DROPPED_IDX: usize = 5;
    for (i, pkt) in media.iter().enumerate() {
        if i == FIRST_DROPPED_IDX {
            continue;
        }
        write_packet(&primary, pkt);
    }

    // fec buffer created by first packet arrival, before the pair is declared
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    for pkt in &fec_packets {
        write_packet(&fec_buff, pkt);
    }

    let stats = primary.fec_decoder_stats();
    assert_eq!(0, stats.fec_packets_received);

    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);

    // Pending FEC is replayed and can use media already in the primary bucket.
    let stats = primary.fec_decoder_stats();
    assert_eq!(fec_packets.len() as u64, stats.fec_packets_received);
    assert_eq!(1, stats.packets_recovered);

    // the next window recovers normally
    let media2 =
        rtptest::generate_vp8_packets(210, 10, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    let fec_packets2 = encoder.encode_fec(&media2, 2);
    assert!(!fec_packets2.is_empty());

    const DROPPED_IDX: usize = 4;
    for (i, pkt) in media2.iter().enumerate() {
        if i == DROPPED_IDX {
            continue;
        }
        write_packet(&primary, pkt);
    }
    for pkt in &fec_packets2 {
        write_packet(&fec_buff, pkt);
    }

    let stats = primary.fec_decoder_stats();
    assert_eq!(2, stats.packets_recovered);

    let ext_sn_by_sn =
        read_ext_sequence_numbers(&primary, media.len() - 1 + media2.len() - 1).await;
    require_recovered_in_bucket(
        &primary,
        &media[FIRST_DROPPED_IDX],
        &ext_sn_by_sn,
        media[0].sequence_number,
    );
    require_recovered_in_bucket(
        &primary,
        &media2[DROPPED_IDX],
        &ext_sn_by_sn,
        media2[0].sequence_number,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_coupled_before_buffers_exist() {
    // pair declared first (from SDP), buffers created later on first packet
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();
    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    bind_fec_test_buffer(&primary);

    let media =
        rtptest::generate_vp8_packets(300, 5, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    let mut encoder = FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC);
    let fec_packets = encoder.encode_fec(&media, 1);
    assert!(!fec_packets.is_empty());

    const DROPPED_IDX: usize = 2;
    for (i, pkt) in media.iter().enumerate() {
        if i == DROPPED_IDX {
            continue;
        }
        write_packet(&primary, pkt);
    }
    for pkt in &fec_packets {
        write_packet(&fec_buff, pkt);
    }

    let stats = primary.fec_decoder_stats();
    assert_eq!(1, stats.packets_recovered);

    let ext_sn_by_sn = read_ext_sequence_numbers(&primary, media.len() - 1).await;
    require_recovered_in_bucket(
        &primary,
        &media[DROPPED_IDX],
        &ext_sn_by_sn,
        media[0].sequence_number,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_sequence_number_wrap() {
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();
    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    bind_fec_test_buffer(&primary);

    let mut media =
        rtptest::generate_vp8_packets(65533, 5, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    for (i, pkt) in media.iter_mut().enumerate() {
        pkt.timestamp = 90000 + 3000 * i as u32;
    }
    let fec_packets = FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC).encode_fec(&media, 1);
    assert!(!fec_packets.is_empty());

    const DROPPED_IDX: usize = 3; // sequence number 0
    for (i, pkt) in media.iter().enumerate() {
        if i != DROPPED_IDX {
            write_packet(&primary, pkt);
        }
    }
    for pkt in &fec_packets {
        write_packet(&fec_buff, pkt);
    }

    assert_eq!(1, primary.fec_decoder_stats().packets_recovered);
    let ext_sn_by_sn = read_ext_sequence_numbers(&primary, media.len() - 1).await;
    require_recovered_in_bucket(
        &primary,
        &media[DROPPED_IDX],
        &ext_sn_by_sn,
        media[0].sequence_number,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_recovery_after_padding_removal() {
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();
    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    bind_fec_test_buffer(&primary);

    let mut media =
        rtptest::generate_vp8_packets(800, 5, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    let mut fec_packets =
        FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC).encode_fec(&media, 1);
    assert_eq!(1, fec_packets.len());

    // Insert a padding-only packet into the publisher sequence-number space.
    // The FEC packet protects the five media packets but not the padding packet.
    for pkt in media.iter_mut().skip(1) {
        pkt.sequence_number += 1;
    }
    let mut mask = 0x8000u16;
    for offset in [0u32, 2, 3, 4, 5] {
        mask |= 1 << (14 - offset);
    }
    let mut fec_payload = fec_packets[0].payload.to_vec();
    fec_payload[18..20].copy_from_slice(&mask.to_be_bytes());
    fec_packets[0].payload = Bytes::from(fec_payload);

    let padding = rtp::Packet {
        header: rtp::Header {
            version: 2,
            padding: true,
            padding_size: 20,
            payload_type: vp8_codec().payload_type,
            sequence_number: 801,
            timestamp: media[0].timestamp,
            ssrc: FEC_TEST_MEDIA_SSRC,
            ..Default::default()
        },
        ..Default::default()
    };

    write_packet(&primary, &media[0]);
    write_packet(&primary, &padding);
    const DROPPED_IDX: usize = 2;
    for (i, pkt) in media.iter().enumerate().skip(1) {
        if i != DROPPED_IDX {
            write_packet(&primary, pkt);
        }
    }
    write_packet(&fec_buff, &fec_packets[0]);

    assert_eq!(1, primary.fec_decoder_stats().packets_recovered);
    let ext_sn_by_sn = read_ext_sequence_numbers(&primary, media.len() - 1).await;
    let base_ext_sn = *ext_sn_by_sn
        .get(&media[0].sequence_number)
        .expect("the first packet was read");

    // The removed padding packet shifts the recovered packet down by one in
    // the bucket/downstream sequence-number space.
    let mut raw = [0u8; 1500];
    let n = primary.get_packet(&mut raw, base_ext_sn + 2).unwrap();
    let mut recovered = rtp::Packet::default();
    recovered
        .unmarshal(&Bytes::copy_from_slice(&raw[..n]))
        .unwrap();
    assert_eq!(
        media[DROPPED_IDX].sequence_number - 1,
        recovered.sequence_number
    );
    assert_eq!(media[DROPPED_IDX].timestamp, recovered.timestamp);
    assert_eq!(media[DROPPED_IDX].payload_type, recovered.payload_type);
    assert_eq!(media[DROPPED_IDX].ssrc, recovered.ssrc);
    assert_eq!(media[DROPPED_IDX].payload, recovered.payload);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_ignores_unexpected_payload_type() {
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);

    // bound without flexfec in negotiated codecs
    primary.lock.write().buffer_base.codec_type = webrtc::RtpCodecType::Video;
    assert_eq!(
        Ok(()),
        primary.bind(
            webrtc::RtpParameters {
                codecs: vec![vp8_codec()],
                ..Default::default()
            },
            &vp8_codec().rtp_codec_capability,
            0,
        )
    );

    let media =
        rtptest::generate_vp8_packets(400, 5, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    let mut encoder = FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC);
    let fec_packets = encoder.encode_fec(&media, 1);
    assert!(!fec_packets.is_empty());

    for pkt in &media {
        write_packet(&primary, pkt);
    }
    for pkt in &fec_packets {
        write_packet(&fec_buff, pkt);
    }

    // no flexfec payload type negotiated, decoder must not be created
    let stats = primary.fec_decoder_stats();
    assert_eq!(0, stats.fec_packets_received);
    assert_eq!(0, stats.packets_recovered);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_ignores_short_twcc_extension() {
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);
    bind_fec_test_buffer(&primary);

    const TWCC_EXT_ID: u8 = 3;
    primary.set_twcc_and_ext_id(Some(twcc::new_transport_wide_cc_responder()), TWCC_EXT_ID);

    let mut fec_packet = rtp::Packet {
        header: rtp::Header {
            version: 2,
            payload_type: FEC_TEST_FEC_PT,
            sequence_number: 1,
            ssrc: FEC_TEST_FEC_SSRC,
            ..Default::default()
        },
        payload: Bytes::from_static(&[0]),
        ..Default::default()
    };
    fec_packet
        .set_extension(TWCC_EXT_ID, Bytes::from_static(&[1]))
        .unwrap();

    write_packet(&fec_buff, &fec_packet);
    assert_eq!(1, primary.fec_decoder_stats().fec_packets_discarded);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_nack_suppression() {
    // a recovered packet must clear the pending NACK for its sequence number
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);
    bind_fec_test_buffer(&primary);

    let media =
        rtptest::generate_vp8_packets(700, 10, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    let mut encoder = FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC);
    let fec_packets = encoder.encode_fec(&media, 2);
    assert!(!fec_packets.is_empty());

    const DROPPED_IDX: usize = 6;
    for (i, pkt) in media.iter().enumerate() {
        if i == DROPPED_IDX {
            continue;
        }
        write_packet(&primary, pkt);
    }

    // the only gap is the dropped packet, exactly one queued NACK
    assert_eq!(
        1,
        nacks_queued(&primary),
        "expected queued NACK for dropped packet"
    );

    for pkt in &fec_packets {
        write_packet(&fec_buff, pkt);
    }
    assert_eq!(1, primary.fec_decoder_stats().packets_recovered);

    assert_eq!(
        0,
        nacks_queued(&primary),
        "NACK for recovered packet not suppressed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_nack_not_sent_for_recovered_packet() {
    // A late media arrival can complete a retained FEC window. RTCP feedback
    // must be generated after FEC recovery so the sequence number repaired by
    // FEC is not NACKed, while an unrelated unrecoverable gap is still NACKed.
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);
    bind_fec_test_buffer(&primary);

    let nacked_sns = Arc::new(Mutex::new(HashMap::<u16, bool>::new()));
    primary.on_rtcp_feedback({
        let nacked_sns = nacked_sns.clone();
        move |fb| {
            let mut nacked_sns = nacked_sns.lock();
            for pkt in &fb {
                if let Some(nack) = pkt.as_any().downcast_ref::<TransportLayerNack>() {
                    for pair in &nack.nacks {
                        for sn in pair.packet_list() {
                            nacked_sns.insert(sn, true);
                        }
                    }
                }
            }
        }
    });

    // single FEC packet protecting the whole window; dropping two of the
    // protected packets keeps the window unrecoverable until one arrives
    let media =
        rtptest::generate_vp8_packets(100, 5, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC); // 100..104
    let fec_packets = FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC).encode_fec(&media, 1);
    assert_eq!(1, fec_packets.len());

    write_packet(&primary, &media[0]); // 100
    write_packet(&primary, &media[1]); // 101
    write_packet(&primary, &media[4]); // 104 -> gap 102, 103

    // an unrelated gap outside the FEC-protected window that FEC cannot recover
    let extra =
        rtptest::generate_vp8_packets(105, 3, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC); // 105..107
    write_packet(&primary, &extra[0]); // 105
    write_packet(&primary, &extra[2]); // 107 -> gap 106
    const UNRECOVERABLE_SN: u16 = 106;

    // FEC packet retained: two protected packets (102, 103) still missing
    write_packet(&fec_buff, &fec_packets[0]);
    assert_eq!(0, primary.fec_decoder_stats().packets_recovered);
    assert_eq!(
        3,
        nacks_queued(&primary),
        "expected queued NACKs for 102, 103, 106"
    );

    // let the queued NACKs age past the minimum NACK interval so they would be
    // sent on the next RTCP generation, and ignore any feedback emitted during
    // setup
    time::sleep(Duration::from_millis(30)).await;
    nacked_sns.lock().clear();

    // late arrival of 102 leaves only 103 missing, recovered in the same write
    write_packet(&primary, &media[2]); // 102
    assert_eq!(1, primary.fec_decoder_stats().packets_recovered);

    // only the unrecoverable gap should remain queued; 102 arrived and 103 was
    // recovered, both cleared from the NACK queue
    assert_eq!(1, nacks_queued(&primary));

    let nacked_sns = nacked_sns.lock();
    assert!(
        nacked_sns.contains_key(&UNRECOVERABLE_SN),
        "expected NACK for unrecoverable gap {UNRECOVERABLE_SN}"
    );
    assert!(
        !nacked_sns.contains_key(&media[2].sequence_number),
        "arrived packet {} must not be NACKed",
        media[2].sequence_number
    );
    assert!(
        !nacked_sns.contains_key(&media[3].sequence_number),
        "FEC-recovered packet {} must not be NACKed",
        media[3].sequence_number
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_buffer_fec_decoder_recreated_on_stream_restart() {
    let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();

    let primary = new_buffer(&factory, FEC_TEST_MEDIA_SSRC);
    let fec_buff = new_buffer(&factory, FEC_TEST_FEC_SSRC);
    factory.set_fec_pair(FEC_TEST_FEC_SSRC, FEC_TEST_MEDIA_SSRC);
    bind_fec_test_buffer(&primary);

    let media =
        rtptest::generate_vp8_packets(100, 5, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    let fec_packets = FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC).encode_fec(&media, 1);
    assert_eq!(1, fec_packets.len());

    const DROPPED_IDX: usize = 2;
    for (i, pkt) in media.iter().enumerate() {
        if i != DROPPED_IDX {
            write_packet(&primary, pkt);
        }
    }
    write_packet(&fec_buff, &fec_packets[0]);
    assert_eq!(1, primary.fec_decoder_stats().packets_recovered);

    // decoderBefore := primary.fecDecoder
    assert!(primary.lock.read().fec_decoder.is_some());

    // Receiver restart and detected discontinuity both land here.
    primary.restart_stream("test");

    // decoderAfter := primary.fecDecoder; require.NotSame(t, decoderBefore, decoderAfter):
    // the decoder is a value here, that it is a new one shows in its counters
    assert!(primary.lock.read().fec_decoder.is_some());
    let stats = primary.fec_decoder_stats();
    assert_eq!(0, stats.fec_packets_received);
    assert_eq!(0, stats.packets_recovered);
    assert_eq!(0, stats.fec_packets_discarded);

    // A new encoder reuses the repair sequence number of the pre-restart FEC
    // packet. The old decoder would discard it as a duplicate.
    let media2 =
        rtptest::generate_vp8_packets(500, 5, vp8_codec().payload_type, FEC_TEST_MEDIA_SSRC);
    let fec_packets2 =
        FlexEncoder03::new(FEC_TEST_FEC_PT, FEC_TEST_FEC_SSRC).encode_fec(&media2, 1);
    assert_eq!(1, fec_packets2.len());
    assert_eq!(
        fec_packets[0].sequence_number,
        fec_packets2[0].sequence_number
    );

    const DROPPED_IDX2: usize = 1;
    for (i, pkt) in media2.iter().enumerate() {
        if i != DROPPED_IDX2 {
            write_packet(&primary, pkt);
        }
    }
    write_packet(&fec_buff, &fec_packets2[0]);

    let stats = primary.fec_decoder_stats();
    assert_eq!(1, stats.fec_packets_received);
    assert_eq!(1, stats.packets_recovered);
    assert_eq!(0, stats.fec_packets_discarded);

    let ext_sn_by_sn = read_ext_sequence_numbers(&primary, media2.len() - 1).await;
    require_recovered_in_bucket(
        &primary,
        &media2[DROPPED_IDX2],
        &ext_sn_by_sn,
        media2[0].sequence_number,
    );
}
