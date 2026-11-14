// Ported from LiveKit, pkg/sfu/redreceiver_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks that carry their names in the
// assertion messages. `dummyDowntrack` is a `FakeTrackSender` whose `write_rtp` returns 1: the
// fake records the packets it is given, and `last_received_pkt` and `received_pkts` read
// them back. The original makes a `&WebRTCReceiver{ReceiverBase: &ReceiverBase{...}}` by
// hand and sets `isRED` on it, before and after it asks for the RED receiver. A receiver
// cannot be made by hand here, and `ReceiverBase::new` takes `is_red` from the codec: so
// `new_receiver(is_red)` makes the base of a receiver that is RED or is not, the assertion on
// what a receiver returns that needs no RED receiver is made on one of the first kind, and
// `w.isRED = ...` after it is a second receiver of the other kind. `get_red_receiver` and
// `get_primary_receiver_for_red` here are the type assertions `.(*RedReceiver)` and
// `.(*RedPrimaryReceiver)` of the original: they give the receiver's transformer, as the
// trait it is held as, and check that it is the receiver that the method returned.

use super::*;
use crate::mediatransportutil::red;
use crate::protocol::logger;
use crate::sfu::{FakeTrackSender, ReceiverBase, ReceiverBaseParams};

const TS_STEP: u32 = 48000 / 1000 * 10;

// dummyDowntrack of the original
fn new_dummy_downtrack() -> Arc<FakeTrackSender> {
    let dt = Arc::new(FakeTrackSender::default());
    dt.write_rtp_returns(1);
    dt
}

// dt.lastReceivedPkt of the original
fn last_received_pkt(dt: &FakeTrackSender) -> rtp::Packet {
    dt.write_rtp_args_for_call(dt.write_rtp_call_count() - 1)
        .0
        .packet
}

// dt.receivedPkts of the original
fn received_pkts(dt: &FakeTrackSender) -> Vec<rtp::Packet> {
    (0..dt.write_rtp_call_count())
        .map(|i| dt.write_rtp_args_for_call(i).0.packet)
        .collect()
}

// `&WebRTCReceiver{ReceiverBase: &ReceiverBase{params: ..., isRED: is_red}}` of the original:
// see the header. A receiver that is RED has the codec that `testRedRedPrimaryReceiver`
// gives it.
fn new_receiver(is_red: bool) -> Arc<ReceiverBase> {
    let codec = if is_red {
        webrtc::RtpCodecParameters {
            payload_type: OPUS_RED_PT,
            rtp_codec_capability: webrtc::RtpCodecCapability {
                mime_type: "audio/red".to_owned(),
                ..Default::default()
            },
        }
    } else {
        webrtc::RtpCodecParameters::default()
    };
    ReceiverBase::new(
        ReceiverBaseParams {
            kind: webrtc::RtpCodecType::Audio,
            logger: logger::get_logger(),
            codec,
            ..Default::default()
        },
        &livekit::TrackInfo::default(),
        ReceiverCodecState::Normal,
    )
}

// require.Equal(t, receiver, w.ReceiverBase)
#[track_caller]
fn require_is_receiver_base(receiver: &Arc<dyn TrackReceiver>, w: &Arc<ReceiverBase>) {
    assert!(
        std::ptr::eq(
            Arc::as_ptr(receiver) as *const (),
            Arc::as_ptr(w) as *const ()
        ),
        "the receiver returned is not the receiver itself"
    );
}

// The transformer of the receiver, which is what `receiver` has to be.
#[track_caller]
fn transformer_of(
    w: &Arc<ReceiverBase>,
    receiver: Arc<dyn TrackReceiver>,
) -> Arc<dyn RedTransformer> {
    let rt = w.load_red_transformer();
    assert!(rt.is_some());
    let rt = rt.unwrap();
    assert!(
        std::ptr::eq(
            Arc::as_ptr(&receiver) as *const (),
            Arc::as_ptr(&rt) as *const ()
        ),
        "the receiver returned is not the RED receiver of the receiver"
    );
    rt
}

// w.GetRedReceiver().(*RedReceiver)
#[track_caller]
fn get_red_receiver(w: &Arc<ReceiverBase>) -> Arc<dyn RedTransformer> {
    transformer_of(w, w.get_red_receiver())
}

// w.GetPrimaryReceiverForRed().(*RedPrimaryReceiver)
#[track_caller]
fn get_primary_receiver_for_red(w: &Arc<ReceiverBase>) -> Arc<dyn RedTransformer> {
    transformer_of(w, w.get_primary_receiver_for_red())
}

fn ext_packet(pkt: &rtp::Packet) -> buffer::ExtPacket {
    buffer::ExtPacket {
        packet: pkt.clone(),
        ..Default::default()
    }
}

#[test]
fn test_red_receiver() {
    let dt = new_dummy_downtrack();

    {
        let name = "normal";
        let w = new_receiver(true);
        require_is_receiver_base(&w.get_red_receiver(), &w);
        // w.isRED = false
        let w = new_receiver(false);
        let red = get_red_receiver(&w);
        assert!(red.add_down_track(dt.clone()).is_ok(), "{name}");

        let header = rtp::Header {
            sequence_number: 65534,
            timestamp: (1u32 << 31) - 2 * TS_STEP,
            payload_type: 111,
            ..Default::default()
        };
        let mut expect_pkt: Vec<Option<rtp::Packet>> = Vec::with_capacity(MAX_RED_COUNT + 1);
        for pkt in generate_pkts(header, 10, TS_STEP) {
            expect_pkt.push(Some(pkt.clone()));
            if expect_pkt.len() > MAX_RED_COUNT + 1 {
                expect_pkt.remove(0);
            }
            red.forward_rtp(&ext_packet(&pkt), 0);
            verify_red_encodings(name, &last_received_pkt(&dt), &expect_pkt);
        }
    }

    {
        let name = "packet lost and jump";
        let w = new_receiver(false);
        let red = get_red_receiver(&w);
        assert!(red.add_down_track(dt.clone()).is_ok(), "{name}");

        let mut header = rtp::Header {
            sequence_number: 65534,
            timestamp: (1u32 << 31) - 2 * TS_STEP,
            payload_type: 111,
            ..Default::default()
        };
        let mut expect_pkt: Vec<Option<rtp::Packet>> = Vec::with_capacity(MAX_RED_COUNT + 1);
        for i in 0..10 {
            if i % 2 == 0 {
                header.sequence_number = header.sequence_number.wrapping_add(1);
                header.timestamp = header.timestamp.wrapping_add(TS_STEP);
                expect_pkt.push(None);
                continue;
            }
            let hbuf = header.marshal().unwrap();
            let pkt1 = rtp::Packet {
                header: header.clone(),
                payload: Bytes::from(hbuf),
                ..Default::default()
            };
            expect_pkt.push(Some(pkt1.clone()));
            if expect_pkt.len() > MAX_RED_COUNT + 1 {
                expect_pkt.drain(..expect_pkt.len() - MAX_RED_COUNT - 1);
            }
            red.forward_rtp(&ext_packet(&pkt1), 0);
            verify_red_encodings(name, &last_received_pkt(&dt), &expect_pkt);
            header.sequence_number = header.sequence_number.wrapping_add(1);
            header.timestamp = header.timestamp.wrapping_add(TS_STEP);
        }

        // jump
        header.sequence_number = header.sequence_number.wrapping_add(10);
        header.timestamp = header.timestamp.wrapping_add(10 * TS_STEP);

        expect_pkt.clear();
        for pkt in generate_pkts(header, 3, TS_STEP) {
            expect_pkt.push(Some(pkt.clone()));
            if expect_pkt.len() > MAX_RED_COUNT + 1 {
                expect_pkt.drain(..expect_pkt.len() - MAX_RED_COUNT - 1);
            }
            red.forward_rtp(&ext_packet(&pkt), 0);
            verify_red_encodings(name, &last_received_pkt(&dt), &expect_pkt);
        }
    }

    {
        let name = "unorder and repeat";
        let w = new_receiver(false);
        let red = get_red_receiver(&w);
        assert!(red.add_down_track(dt.clone()).is_ok(), "{name}");

        let header = rtp::Header {
            sequence_number: 65534,
            timestamp: (1u32 << 31) - 2 * TS_STEP,
            payload_type: 111,
            ..Default::default()
        };
        let mut prev_pkts: Vec<Option<rtp::Packet>> = Vec::new();
        for pkt in generate_pkts(header, 10, TS_STEP) {
            red.forward_rtp(&ext_packet(&pkt), 0);
            prev_pkts.push(Some(pkt));
        }

        // old unorder data don't have red records
        let expect_pkt = &prev_pkts[prev_pkts.len() - 3..prev_pkts.len() - 2];
        red.forward_rtp(&ext_packet(expect_pkt[0].as_ref().unwrap()), 0);
        verify_red_encodings(name, &last_received_pkt(&dt), expect_pkt);

        // repeat packet only have 1 red records
        let expect_pkt = &prev_pkts[prev_pkts.len() - 2..];
        red.forward_rtp(&ext_packet(expect_pkt[1].as_ref().unwrap()), 0);
        verify_red_encodings(name, &last_received_pkt(&dt), expect_pkt);
    }

    {
        let name = "encoding exceed space";
        let w = new_receiver(true);
        require_is_receiver_base(&w.get_red_receiver(), &w);
        // w.isRED = false
        let w = new_receiver(false);
        let red = get_red_receiver(&w);
        assert!(red.add_down_track(dt.clone()).is_ok(), "{name}");

        let header = rtp::Header {
            sequence_number: 65534,
            timestamp: (1u32 << 31) - 2 * TS_STEP,
            payload_type: 111,
            ..Default::default()
        };
        let mut expect_pkt: Vec<Option<rtp::Packet>> = Vec::with_capacity(MAX_RED_COUNT + 1);
        for mut pkt in generate_pkts(header, 10, TS_STEP) {
            // make sure red encodings don't have enough space to encoding redundant packet
            pkt.payload = Bytes::from(vec![0u8; 1000]);
            expect_pkt.clear();
            expect_pkt.push(Some(pkt.clone()));
            red.forward_rtp(&ext_packet(&pkt), 0);
            verify_red_encodings(name, &last_received_pkt(&dt), &expect_pkt);
        }
    }

    {
        let name = "large timestamp gap";
        let w = new_receiver(true);
        require_is_receiver_base(&w.get_red_receiver(), &w);
        // w.isRED = false
        let w = new_receiver(false);
        let red = get_red_receiver(&w);
        assert!(red.add_down_track(dt.clone()).is_ok(), "{name}");

        let header = rtp::Header {
            sequence_number: 65534,
            timestamp: (1u32 << 31) - 2 * TS_STEP,
            payload_type: 111,
            ..Default::default()
        };
        // first few packets normal
        let mut expect_pkt: Vec<Option<rtp::Packet>> = Vec::with_capacity(MAX_RED_COUNT + 1);
        for pkt in generate_pkts(header.clone(), 4, TS_STEP) {
            expect_pkt.push(Some(pkt.clone()));
            if expect_pkt.len() > MAX_RED_COUNT + 1 {
                expect_pkt.remove(0);
            }
            red.forward_rtp(&ext_packet(&pkt), 0);
            verify_red_encodings(name, &last_received_pkt(&dt), &expect_pkt);
        }

        // and then a few packets with a large timestamp jump, should contain only primary
        for pkt in generate_pkts(header, 4, 40 * TS_STEP) {
            red.forward_rtp(&ext_packet(&pkt), 0);
            verify_red_encodings(name, &last_received_pkt(&dt), &[Some(pkt)]);
        }
    }
}

// reads every packet into one reused buffer, the way ReceiverBase.forwardRTP does
#[test]
fn test_red_receiver_reused_read_buffer() {
    // What the original guards against, a history that points into a read buffer which the
    // next packet is written over, cannot happen here, where a payload is a `Bytes` of the
    // packet's own copy of what was read. The test is the original's all the same.
    let opus = webrtc::RtpCodecParameters {
        rtp_codec_capability: webrtc::RtpCodecCapability {
            mime_type: webrtc::MIME_TYPE_OPUS.to_owned(),
            clock_rate: 48000,
            channels: 2,
            ..Default::default()
        },
        payload_type: 111,
    };
    let buff = buffer::Buffer::new(0x5678, 500, 200);
    assert!(
        buff.bind(
            webrtc::RtpParameters {
                codecs: vec![opus.clone()],
                ..Default::default()
            },
            &opus.rtp_codec_capability,
            0
        )
        .is_ok()
    );

    let w = new_receiver(false);
    let rr = get_red_receiver(&w);
    let dt = new_dummy_downtrack();
    assert!(rr.add_down_track(dt.clone()).is_ok());

    let mut payloads: Vec<Vec<u8>> = Vec::new();
    let mut pkt_buf = vec![0u8; bucket::RTP_MAX_PKT_SIZE];
    for i in 0..6usize {
        let payload = vec![(0x10 * (i + 1)) as u8; 60];
        payloads.push(payload.clone());
        let pkt = rtp::Packet {
            header: rtp::Header {
                version: 2,
                payload_type: 111,
                sequence_number: (100 + i) as u16,
                timestamp: (960 * i) as u32,
                ssrc: 0x5678,
                ..Default::default()
            },
            payload: Bytes::from(payload),
            ..Default::default()
        };
        let raw = pkt.marshal();
        assert!(raw.is_ok());
        assert!(buff.write(Bytes::from(raw.unwrap())).is_ok());
        let ext_pkt = buff.read_extended(&mut pkt_buf);
        assert!(ext_pkt.is_ok());
        let ext_pkt = ext_pkt.unwrap();
        assert!(ext_pkt.is_some());
        rr.forward_rtp(&ext_pkt.unwrap(), 0);
    }

    // the last RED packet carries the two packets before it as redundant blocks
    let blocks = red::extract_packets(&last_received_pkt(&dt), 0xFF);
    assert!(blocks.is_ok());
    let blocks = blocks.unwrap();
    assert_eq!(3, blocks.len());
    for (i, block) in blocks.iter().enumerate() {
        assert_eq!(payloads[3 + i], block.payload);
    }
}

#[track_caller]
fn verify_red_encodings(name: &str, red_pkt: &rtp::Packet, red_pkts: &[Option<rtp::Packet>]) {
    let mut solid_pkts: Vec<&rtp::Packet> = Vec::with_capacity(red_pkts.len());
    for pkt in red_pkts.iter().flatten() {
        solid_pkts.push(pkt);
    }
    let pkts_from_red = red::extract_packets(red_pkt, 0xFF);
    assert!(pkts_from_red.is_ok(), "{name}: {pkts_from_red:?}");
    let pkts_from_red = pkts_from_red.unwrap();
    assert_eq!(solid_pkts.len(), pkts_from_red.len(), "{name}");
    for (i, pkt) in pkts_from_red.iter().enumerate() {
        verify_encoding_equal(name, pkt, solid_pkts[i]);
    }
}

#[track_caller]
fn verify_pkts_equal(name: &str, p1s: &[rtp::Packet], p2s: &[rtp::Packet]) {
    assert_eq!(p2s.len(), p1s.len(), "{name}");
    for (i, pkt) in p1s.iter().enumerate() {
        verify_encoding_equal(name, pkt, &p2s[i]);
    }
}

#[track_caller]
fn verify_encoding_equal(name: &str, p1: &rtp::Packet, p2: &rtp::Packet) {
    assert_eq!(p1.header.timestamp, p2.header.timestamp, "{name}");
    assert_eq!(p1.payload_type, p2.payload_type, "{name}");
    assert_eq!(
        p1.payload, p2.payload,
        "{name}: seq1 {}",
        p1.sequence_number
    );
}

fn generate_pkts(mut header: rtp::Header, count: usize, ts_step: u32) -> Vec<rtp::Packet> {
    let mut pkts = Vec::with_capacity(count);
    for _ in 0..count {
        let hbuf = header.marshal().unwrap();
        pkts.push(rtp::Packet {
            header: header.clone(),
            payload: Bytes::from(hbuf),
            ..Default::default()
        });
        header.sequence_number = header.sequence_number.wrapping_add(1);
        header.timestamp = header.timestamp.wrapping_add(ts_step);
    }
    pkts
}

fn generate_red_pkts(pkts: &[rtp::Packet], red_count: usize) -> Vec<rtp::Packet> {
    let mut red_pkts = Vec::with_capacity(pkts.len());
    for (i, pkt) in pkts.iter().enumerate() {
        let mut encoding_pkts: Vec<&rtp::Packet> = Vec::with_capacity(red_count);
        // `j` starts below zero for the first packets, as in the original
        for j in (i as i64 - red_count as i64)..(i as i64) {
            if j < 0 {
                continue;
            }
            encoding_pkts.push(&pkts[j as usize]);
        }
        let mut buf = vec![0u8; MTU_SIZE];
        let mut red_pkt = pkt.clone();
        red_pkt.payload_type = OPUS_RED_PT;
        let encoded = encode_red_for_primary(&encoding_pkts, pkt, &mut buf);
        assert!(encoded.is_ok(), "{encoded:?}");
        buf.truncate(encoded.unwrap());
        red_pkt.payload = Bytes::from(buf);
        red_pkts.push(red_pkt);
    }
    red_pkts
}

#[track_caller]
fn test_red_red_primary_receiver(
    name: &str,
    max_pkt_count: usize,
    red_count: usize,
    send_pkt_idx: &[usize],
    expect_pkt_idx: &[usize],
) {
    let dt = new_dummy_downtrack();
    let w = new_receiver(false);
    require_is_receiver_base(&w.get_primary_receiver_for_red(), &w);
    // w.isRED = true
    let w = new_receiver(true);
    let red = get_primary_receiver_for_red(&w);
    assert!(red.add_down_track(dt.clone()).is_ok(), "{name}");

    let header = rtp::Header {
        sequence_number: 65530,
        timestamp: (1u32 << 31) - 2 * TS_STEP,
        payload_type: 111,
        ..Default::default()
    };
    let primary_pkts = generate_pkts(header, max_pkt_count, TS_STEP);
    let red_pkts = generate_red_pkts(&primary_pkts, red_count);

    for &i in send_pkt_idx {
        red.forward_rtp(&ext_packet(&red_pkts[i]), 0);
    }

    let mut expect_pkts = Vec::with_capacity(expect_pkt_idx.len());
    for &i in expect_pkt_idx {
        expect_pkts.push(primary_pkts[i].clone());
    }

    verify_pkts_equal(name, &expect_pkts, &received_pkts(&dt));
}

#[test]
fn test_red_primary_receiver() {
    let w = new_receiver(false);
    require_is_receiver_base(&w.get_primary_receiver_for_red(), &w);
    // w.isRED = true
    let w = new_receiver(true);
    let _red = get_primary_receiver_for_red(&w);

    {
        let name = "packet should send only once";
        let max_pkt_count = 19;
        let mut send_pkt_index = Vec::new();
        for i in 0..max_pkt_count {
            send_pkt_index.push(i);
        }
        test_red_red_primary_receiver(
            name,
            max_pkt_count,
            MAX_RED_COUNT,
            &send_pkt_index,
            &send_pkt_index,
        );
    }

    {
        let name = "packet duplicate and unorder";
        let max_pkt_count = 19;
        let mut send_pkt_index = Vec::new();
        for i in 0..max_pkt_count {
            send_pkt_index.push(i);
            if i > 0 {
                send_pkt_index.push(i - 1);
            }
            send_pkt_index.push(i);
        }
        test_red_red_primary_receiver(
            name,
            max_pkt_count,
            MAX_RED_COUNT,
            &send_pkt_index,
            &send_pkt_index,
        );
    }

    {
        let name = "full recover";
        let max_pkt_count = 19;
        let mut send_pkt_index = Vec::new();
        let mut recv_pkt_index = Vec::new();
        for i in 0..max_pkt_count {
            recv_pkt_index.push(i);

            // drop packets covered by red encoding
            if i % (MAX_RED_COUNT + 1) != 0 {
                continue;
            }
            send_pkt_index.push(i);
        }

        test_red_red_primary_receiver(
            name,
            max_pkt_count,
            MAX_RED_COUNT,
            &send_pkt_index,
            &recv_pkt_index,
        );
    }

    {
        let name = "lost 2 but red recover 1";
        let max_pkt_count = 19;
        let send_pkt_index = [0, 3, 6, 9, 12];
        let recv_pkt_index = [0, 2, 3, 5, 6, 8, 9, 11, 12];
        test_red_red_primary_receiver(name, max_pkt_count, 1, &send_pkt_index, &recv_pkt_index);
    }

    {
        let name = "part recover and long jump";
        let max_pkt_count = 50;
        let send_pkt_index = [0, 5, 12, 21 /*long jump*/, 24, 27];
        let recv_pkt_index = [0, 3, 4, 5, 10, 11, 12, 19, 20, 21, 22, 23, 24, 25, 26, 27];
        test_red_red_primary_receiver(
            name,
            max_pkt_count,
            MAX_RED_COUNT,
            &send_pkt_index,
            &recv_pkt_index,
        );
    }

    {
        let name = "unorder";
        let max_pkt_count = 50;
        let send_pkt_index = [20, 10 /*unorder can't recover*/, 25, 23, 34];
        let recv_pkt_index = [20, 10, 23, 24, 25, 21, 22, 23, 32, 33, 34];
        test_red_red_primary_receiver(
            name,
            max_pkt_count,
            MAX_RED_COUNT,
            &send_pkt_index,
            &recv_pkt_index,
        );
    }

    {
        let name = "mixed primary codec";
        let dt = new_dummy_downtrack();
        let w = new_receiver(false);
        require_is_receiver_base(&w.get_primary_receiver_for_red(), &w);
        // w.isRED = true
        let w = new_receiver(true);
        let red = get_primary_receiver_for_red(&w);
        assert!(red.add_down_track(dt.clone()).is_ok(), "{name}");

        let primary_pkt = rtp::Packet {
            header: rtp::Header {
                sequence_number: 65530,
                timestamp: (1u32 << 31) - 2 * TS_STEP,
                payload_type: 111,
                ..Default::default()
            },
            payload: Bytes::from_static(&[1, 3, 5, 7, 9]),
            ..Default::default()
        };
        red.forward_rtp(&ext_packet(&primary_pkt), 0);

        verify_pkts_equal(name, &[primary_pkt], &received_pkts(&dt));
    }
}

#[test]
fn test_extract_primary_encoding_for_red() {
    let header = rtp::Header {
        sequence_number: 65530,
        timestamp: (1u32 << 31) - 2 * TS_STEP,
        payload_type: 111,
        ..Default::default()
    };
    let pkts = generate_pkts(header, 10, TS_STEP);
    let red_pkts = generate_red_pkts(&pkts, 2);

    let mut primary_pkts = Vec::with_capacity(red_pkts.len());

    for red_pkt in &red_pkts {
        let payload = red::extract_primary_encoding(&red_pkt.payload);
        assert!(payload.is_ok());
        let mut primary_header = red_pkt.header.clone();
        primary_header.payload_type = 111;
        primary_pkts.push(rtp::Packet {
            header: primary_header,
            payload: red_pkt.payload.slice_ref(payload.unwrap()),
            ..Default::default()
        });
    }

    verify_pkts_equal("", &pkts, &primary_pkts);
}
