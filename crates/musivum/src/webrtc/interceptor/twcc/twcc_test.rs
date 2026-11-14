// Ported from pion/interceptor, pkg/twcc/twcc_test.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; the subtests are blocks that carry their name in the
// assertion messages. The header of a feedback packet, a field of the packet upstream, is
// compared apart from the packet: `header()` where the original compares a packet it built,
// the four bytes on the wire where it compares a packet it unmarshalled. Where the original
// compares symbols as numbers, the enums of `rtc-rtcp` are compared by their numbers.
// `TestRecordDoesNotAllocateInSteadyState` cannot count allocations and checks what stands
// for them (see there). The two benchmarks are left out. The last test is new and says so.

use super::super::arrival_time_map::{MAX_NUMBER_OF_PACKETS, MIN_CAPACITY};
use super::*;
use crate::webrtc::rtcp::Header;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::StatusChunkTypeTcc;

fn rtcp_to_twcc(r#in: &[Box<dyn rtcp::Packet>]) -> Vec<&TransportLayerCc> {
    r#in.iter()
        .map(|pkt| {
            pkt.as_any()
                .downcast_ref::<TransportLayerCc>()
                .unwrap_or_else(|| panic!("Expected TransportLayerCC, got {pkt:?}"))
        })
        .collect()
}

// `Marshal()` of the original. The trait that marshals a value of `rtc-rtcp` is in another
// crate (`rtc-shared`) and cannot be named through `crate::webrtc::rtcp`; as a supertrait of
// `Packet` its methods can be called on a `dyn Packet`.
fn marshal(pkt: &dyn rtcp::Packet) -> Vec<u8> {
    pkt.marshal().expect("the packet marshals").to_vec()
}

// `statusChunk.Marshal()` of the original. For the reason above a chunk cannot be marshalled
// on its own here: it is marshalled as the only chunk of a feedback packet, where it follows
// the 20 bytes of the headers.
fn marshal_chunk(chunk: &PacketStatusChunk) -> Vec<u8> {
    let pkt = TransportLayerCc {
        packet_chunks: vec![chunk.clone()],
        ..Default::default()
    };
    let buf = marshal(&pkt);
    assert_eq!(24, buf.len());
    buf[20..22].to_vec()
}

// `unmarshalled.Unmarshal(bs)` of the original, which fills the header from the wire. Here the
// header is read from the first four bytes and the rest is unmarshalled by `rtc-rtcp`.
fn unmarshal(bs: &[u8]) -> (Header, TransportLayerCc) {
    assert_eq!(2, bs[0] >> 6, "the version");
    let header = Header {
        padding: bs[0] & 0x20 != 0,
        count: bs[0] & 0x1f,
        packet_type: PacketType::from(bs[1]),
        length: u16::from_be_bytes([bs[2], bs[3]]),
    };
    let mut buf = bs;
    let pkts = rtcp::packet::unmarshal(&mut buf).expect("the packet unmarshals");
    assert_eq!(1, pkts.len());
    (header, rtcp_to_twcc(&pkts)[0].clone())
}

// The symbols of a chunk as the numbers the original has them as.
fn numbers(symbols: &[SymbolTypeTcc]) -> Vec<u16> {
    symbols.iter().map(|s| *s as u16).collect()
}

// A list of symbols that the original gives as numbers.
fn symbols(numbers: &[u16]) -> Vec<SymbolTypeTcc> {
    numbers.iter().map(|n| SymbolTypeTcc::from(*n)).collect()
}

#[test]
fn test_chunk_add() {
    {
        let name = "fill with not received";
        let mut test_chunk = Chunk::default();

        for i in 0..MAX_RUN_LENGTH_CAP {
            assert!(
                test_chunk.can_add(SymbolTypeTcc::PacketNotReceived),
                "{name}: {i}"
            );
            test_chunk.add(SymbolTypeTcc::PacketNotReceived);
        }
        assert_eq!(
            vec![0u16; MAX_RUN_LENGTH_CAP],
            numbers(&test_chunk.deltas),
            "{name}"
        );
        assert!(!test_chunk.has_different_types, "{name}");

        assert!(
            !test_chunk.can_add(SymbolTypeTcc::PacketNotReceived),
            "{name}"
        );
        assert!(
            !test_chunk.can_add(SymbolTypeTcc::PacketReceivedSmallDelta),
            "{name}"
        );
        assert!(
            !test_chunk.can_add(SymbolTypeTcc::PacketReceivedLargeDelta),
            "{name}"
        );

        let status_chunk = test_chunk.encode();
        assert!(
            matches!(status_chunk, PacketStatusChunk::RunLengthChunk(_)),
            "{name}"
        );

        let buf = marshal_chunk(&status_chunk);
        assert_eq!(vec![0x1f, 0xff], buf, "{name}");
    }

    {
        let name = "fill with small delta";
        let mut test_chunk = Chunk::default();

        for i in 0..MAX_ONE_BIT_CAP {
            assert!(
                test_chunk.can_add(SymbolTypeTcc::PacketReceivedSmallDelta),
                "{name}: {i}"
            );
            test_chunk.add(SymbolTypeTcc::PacketReceivedSmallDelta);
        }

        assert_eq!(
            numbers(&test_chunk.deltas),
            vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            "{name}"
        );
        assert!(!test_chunk.has_different_types, "{name}");

        assert!(
            !test_chunk.can_add(SymbolTypeTcc::PacketReceivedLargeDelta),
            "{name}"
        );
        assert!(
            !test_chunk.can_add(SymbolTypeTcc::PacketNotReceived),
            "{name}"
        );

        let status_chunk = test_chunk.encode();
        assert!(
            matches!(status_chunk, PacketStatusChunk::RunLengthChunk(_)),
            "{name}"
        );

        let buf = marshal_chunk(&status_chunk);
        assert_eq!(vec![0x20, 0xe], buf, "{name}");
    }

    {
        let name = "fill with large delta";
        let mut test_chunk = Chunk::default();

        for i in 0..MAX_TWO_BIT_CAP {
            assert!(
                test_chunk.can_add(SymbolTypeTcc::PacketReceivedLargeDelta),
                "{name}: {i}"
            );
            test_chunk.add(SymbolTypeTcc::PacketReceivedLargeDelta);
        }

        assert_eq!(
            numbers(&test_chunk.deltas),
            vec![2, 2, 2, 2, 2, 2, 2],
            "{name}"
        );
        assert!(test_chunk.has_large_delta, "{name}");
        assert!(!test_chunk.has_different_types, "{name}");

        assert!(
            !test_chunk.can_add(SymbolTypeTcc::PacketReceivedSmallDelta),
            "{name}"
        );
        assert!(
            !test_chunk.can_add(SymbolTypeTcc::PacketNotReceived),
            "{name}"
        );

        let status_chunk = test_chunk.encode();
        assert!(
            matches!(status_chunk, PacketStatusChunk::RunLengthChunk(_)),
            "{name}"
        );

        let buf = marshal_chunk(&status_chunk);
        assert_eq!(vec![0x40, 0x7], buf, "{name}");
    }

    {
        let name = "fill with different types";
        let mut test_chunk = Chunk::default();

        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketReceivedSmallDelta),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketReceivedSmallDelta);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketReceivedSmallDelta),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketReceivedSmallDelta);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketReceivedSmallDelta),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketReceivedSmallDelta);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketReceivedSmallDelta),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketReceivedSmallDelta);

        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketReceivedLargeDelta),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketReceivedLargeDelta);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketReceivedLargeDelta),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketReceivedLargeDelta);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketReceivedLargeDelta),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketReceivedLargeDelta);

        assert!(
            !test_chunk.can_add(SymbolTypeTcc::PacketReceivedLargeDelta),
            "{name}"
        );

        let status_chunk = test_chunk.encode();
        assert!(
            matches!(status_chunk, PacketStatusChunk::StatusVectorChunk(_)),
            "{name}"
        );

        let buf = marshal_chunk(&status_chunk);
        assert_eq!(vec![0xd5, 0x6a], buf, "{name}");
    }

    {
        let name = "overfill and encode";
        let mut test_chunk = Chunk::default();

        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketReceivedSmallDelta),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketReceivedSmallDelta);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketNotReceived),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketNotReceived);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketNotReceived),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketNotReceived);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketNotReceived),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketNotReceived);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketNotReceived),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketNotReceived);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketNotReceived),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketNotReceived);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketNotReceived),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketNotReceived);
        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketNotReceived),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketNotReceived);

        assert!(
            !test_chunk.can_add(SymbolTypeTcc::PacketReceivedLargeDelta),
            "{name}"
        );

        let status_chunk1 = test_chunk.encode();
        assert!(
            matches!(status_chunk1, PacketStatusChunk::StatusVectorChunk(_)),
            "{name}"
        );
        assert_eq!(1, test_chunk.deltas.len(), "{name}");

        assert!(
            test_chunk.can_add(SymbolTypeTcc::PacketReceivedLargeDelta),
            "{name}"
        );
        test_chunk.add(SymbolTypeTcc::PacketReceivedLargeDelta);

        let status_chunk2 = test_chunk.encode();
        assert!(
            matches!(status_chunk2, PacketStatusChunk::StatusVectorChunk(_)),
            "{name}"
        );

        assert_eq!(0, test_chunk.deltas.len(), "{name}");

        assert_eq!(
            PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                symbol_size: SymbolSizeTypeTcc::TwoBit,
                symbol_list: vec![
                    SymbolTypeTcc::PacketNotReceived,
                    SymbolTypeTcc::PacketReceivedLargeDelta
                ],
                ..Default::default()
            }),
            status_chunk2,
            "{name}"
        );
    }
}

#[test]
fn test_feedback() {
    {
        let name = "add simple";
        let mut f = Feedback::default();

        let got = f.add_received(0, 10);

        assert!(got, "{name}");
    }

    {
        let name = "add too large";
        let mut f = Feedback::default();

        assert!(!f.add_received(12, 8200 * 1000 * 250), "{name}");
    }

    {
        let name = "add received 1";
        let mut test_feedback = Feedback::default();
        test_feedback.set_base(1, 1000 * 1000);

        let got = test_feedback.add_received(1, 1023 * 1000);

        assert!(got, "{name}");
        assert_eq!(2u16, test_feedback.next_sequence_number, "{name}");
        assert_eq!(15i64, test_feedback.ref_timestamp_64ms, "{name}");

        let got = test_feedback.add_received(4, 1086 * 1000);
        assert!(got, "{name}");
        assert_eq!(5u16, test_feedback.next_sequence_number, "{name}");
        assert_eq!(15i64, test_feedback.ref_timestamp_64ms, "{name}");

        assert!(test_feedback.last_chunk.has_different_types, "{name}");
        assert_eq!(4, test_feedback.last_chunk.deltas.len(), "{name}");
        assert!(
            !test_feedback
                .last_chunk
                .deltas
                .contains(&SymbolTypeTcc::PacketReceivedLargeDelta),
            "{name}"
        );
    }

    {
        let name = "add received 2";
        let mut f = Feedback::new(0, 0, 0);
        f.set_base(5, 320 * 1000);

        let got = f.add_received(5, 320 * 1000);
        assert!(got, "{name}");
        let got = f.add_received(7, 448 * 1000);
        assert!(got, "{name}");
        let got = f.add_received(8, 512 * 1000);
        assert!(got, "{name}");
        let got = f.add_received(11, 768 * 1000);
        assert!(got, "{name}");

        let pkt = f.get_rtcp();

        assert!(pkt.header().padding, "{name}");
        assert_eq!(7u16, pkt.header().length, "{name}");
        assert_eq!(5u16, pkt.base_sequence_number, "{name}");
        assert_eq!(7u16, pkt.packet_status_count, "{name}");
        assert_eq!(5u32, pkt.reference_time, "{name}");
        assert_eq!(0u8, pkt.fb_pkt_count, "{name}");
        assert_eq!(1, pkt.packet_chunks.len(), "{name}");

        assert_eq!(
            vec![PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                symbol_size: SymbolSizeTypeTcc::TwoBit,
                symbol_list: vec![
                    SymbolTypeTcc::PacketReceivedSmallDelta,
                    SymbolTypeTcc::PacketNotReceived,
                    SymbolTypeTcc::PacketReceivedLargeDelta,
                    SymbolTypeTcc::PacketReceivedLargeDelta,
                    SymbolTypeTcc::PacketNotReceived,
                    SymbolTypeTcc::PacketNotReceived,
                    SymbolTypeTcc::PacketReceivedLargeDelta,
                ],
                ..Default::default()
            })],
            pkt.packet_chunks,
            "{name}"
        );

        let expected_deltas = [
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                delta: 0,
            },
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedLargeDelta,
                delta: 0x0200 * TYPE_TCC_DELTA_SCALE_FACTOR,
            },
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedLargeDelta,
                delta: 0x0100 * TYPE_TCC_DELTA_SCALE_FACTOR,
            },
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedLargeDelta,
                delta: 0x0400 * TYPE_TCC_DELTA_SCALE_FACTOR,
            },
        ];
        assert_eq!(expected_deltas.len(), pkt.recv_deltas.len(), "{name}");
        for (i, d) in expected_deltas.iter().enumerate() {
            assert_eq!(d, &pkt.recv_deltas[i], "{name}");
        }
    }

    {
        let name = "add received small deltas";
        let mut f = Feedback::new(0, 0, 0);
        let base: i64 = 320 * 1000;
        let delta_us: i64 = 200;
        f.set_base(5, base);

        for i in 0..5i64 {
            let got = f.add_received(5 + (i + 1) as u16, base + delta_us * i);
            assert!(got, "{name}");
        }

        let pkt = f.get_rtcp();

        let expected_deltas = [
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                delta: 0,
            },
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                // NOTE: The delta is less than the scale factor, but it should be rounded up.
                // (rtcp.RecvDelta).Marshal() simply truncates to an interval of the scale factor,
                // so we want to make sure that the deltas have any rounding applied when building
                // the feedback.
                delta: TYPE_TCC_DELTA_SCALE_FACTOR,
            },
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                delta: TYPE_TCC_DELTA_SCALE_FACTOR,
            },
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                // NOTE: This is zero because even though the deltas are all the same, the rounding error has
                // built up enough by this packet to cause it to be rounded down.
                delta: 0,
            },
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                delta: TYPE_TCC_DELTA_SCALE_FACTOR,
            },
        ];
        assert_eq!(expected_deltas.len(), pkt.recv_deltas.len(), "{name}");
        for (i, d) in expected_deltas.iter().enumerate() {
            assert_eq!(d, &pkt.recv_deltas[i], "{name}");
        }
    }

    {
        let name = "add received wrapped sequence number";
        let mut f = Feedback::new(0, 0, 0);
        f.set_base(65535, 320 * 1000);

        let got = f.add_received(65535, 320 * 1000);
        assert!(got, "{name}");
        let got = f.add_received(7, 448 * 1000);
        assert!(got, "{name}");
        let got = f.add_received(8, 512 * 1000);
        assert!(got, "{name}");
        let got = f.add_received(11, 768 * 1000);
        assert!(got, "{name}");

        let pkt = f.get_rtcp();

        assert!(pkt.header().padding, "{name}");
        assert_eq!(7u16, pkt.header().length, "{name}");
        assert_eq!(65535u16, pkt.base_sequence_number, "{name}");
        assert_eq!(13u16, pkt.packet_status_count, "{name}");
        assert_eq!(5u32, pkt.reference_time, "{name}");
        assert_eq!(0u8, pkt.fb_pkt_count, "{name}");
        assert_eq!(2, pkt.packet_chunks.len(), "{name}");

        assert_eq!(
            vec![
                PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                    symbol_size: SymbolSizeTypeTcc::TwoBit,
                    symbol_list: vec![
                        SymbolTypeTcc::PacketReceivedSmallDelta,
                        SymbolTypeTcc::PacketNotReceived,
                        SymbolTypeTcc::PacketNotReceived,
                        SymbolTypeTcc::PacketNotReceived,
                        SymbolTypeTcc::PacketNotReceived,
                        SymbolTypeTcc::PacketNotReceived,
                        SymbolTypeTcc::PacketNotReceived,
                    ],
                    ..Default::default()
                }),
                PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                    symbol_size: SymbolSizeTypeTcc::TwoBit,
                    symbol_list: vec![
                        SymbolTypeTcc::PacketNotReceived,
                        SymbolTypeTcc::PacketReceivedLargeDelta,
                        SymbolTypeTcc::PacketReceivedLargeDelta,
                        SymbolTypeTcc::PacketNotReceived,
                        SymbolTypeTcc::PacketNotReceived,
                        SymbolTypeTcc::PacketReceivedLargeDelta,
                    ],
                    ..Default::default()
                }),
            ],
            pkt.packet_chunks,
            "{name}"
        );

        let expected_deltas = [
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                delta: 0,
            },
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedLargeDelta,
                delta: 0x0200 * TYPE_TCC_DELTA_SCALE_FACTOR,
            },
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedLargeDelta,
                delta: 0x0100 * TYPE_TCC_DELTA_SCALE_FACTOR,
            },
            RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedLargeDelta,
                delta: 0x0400 * TYPE_TCC_DELTA_SCALE_FACTOR,
            },
        ];
        assert_eq!(expected_deltas.len(), pkt.recv_deltas.len(), "{name}");
        for (i, d) in expected_deltas.iter().enumerate() {
            assert_eq!(d, &pkt.recv_deltas[i], "{name}");
        }
    }

    {
        let name = "get RTCP";
        struct TestCase {
            arrival_ts: i64,
            sequence_number: u16,
            want_ref_time: u32,
            want_base_sequence_number: u16,
        }
        let testcases = [
            TestCase {
                arrival_ts: 320,
                sequence_number: 1,
                want_ref_time: 5,
                want_base_sequence_number: 1,
            },
            TestCase {
                arrival_ts: 1000,
                sequence_number: 2,
                want_ref_time: 15,
                want_base_sequence_number: 2,
            },
        ];
        for tt in testcases {
            let mut f = Feedback::new(0, 0, 0);
            f.set_base(tt.sequence_number, tt.arrival_ts * 1000);

            let got = f.get_rtcp();
            assert_eq!(
                tt.want_ref_time, got.reference_time,
                "{name}: set correct base seq and time"
            );
            assert_eq!(
                tt.want_base_sequence_number, got.base_sequence_number,
                "{name}: set correct base seq and time"
            );
        }
    }
}

fn add_run(r: &mut Recorder, sequence_numbers: &[u16], arrival_times: &[i64]) {
    assert_eq!(sequence_numbers.len(), arrival_times.len());

    for i in 0..sequence_numbers.len() {
        r.record(5000, sequence_numbers[i], arrival_times[i]);
    }
}

const SCALE_FACTOR_REFERENCE_TIME: i64 = 64000;

fn increase_time(arrival_time: &mut i64, increase_amount: i64) -> i64 {
    *arrival_time += increase_amount;

    *arrival_time
}

fn marshal_all(pkts: &[Box<dyn rtcp::Packet>]) {
    for pkt in pkts {
        let marshaled = marshal(pkt.as_ref());

        // Chrome expects feedback packets to always be 18 bytes or more.
        // https://source.chromium.org/chromium/chromium/src/+/main:third_party/webrtc/modules/rtp_rtcp/source/rtcp_packet/transport_feedback.cc;l=423?q=transport_feedback.cc&ss=chromium%2Fchromium%2Fsrc
        assert!(marshaled.len() >= 18);
    }
}

#[test]
fn test_build_feedback_packet() {
    let mut recoder = Recorder::new(5000);

    let mut arrival_time: i64 = SCALE_FACTOR_REFERENCE_TIME;
    add_run(
        &mut recoder,
        &[0, 1, 2, 3, 4, 5, 6, 7],
        &[
            SCALE_FACTOR_REFERENCE_TIME,
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR * 256),
        ],
    );

    let rtcp_packets = recoder.build_feedback_packet();
    assert_eq!(1, rtcp_packets.len());

    let pkt = rtcp_to_twcc(&rtcp_packets)[0];
    assert_eq!(
        Header {
            count: FORMAT_TCC,
            packet_type: PacketType::TransportSpecificFeedback,
            padding: true,
            length: 8,
        },
        pkt.header()
    );
    assert_eq!(
        &TransportLayerCc {
            sender_ssrc: 5000,
            media_ssrc: 5000,
            base_sequence_number: 0,
            reference_time: 1,
            fb_pkt_count: 0,
            packet_status_count: 8,
            packet_chunks: vec![
                PacketStatusChunk::RunLengthChunk(RunLengthChunk {
                    type_tcc: StatusChunkTypeTcc::RunLengthChunk,
                    packet_status_symbol: SymbolTypeTcc::PacketReceivedSmallDelta,
                    run_length: 7,
                }),
                PacketStatusChunk::RunLengthChunk(RunLengthChunk {
                    type_tcc: StatusChunkTypeTcc::RunLengthChunk,
                    packet_status_symbol: SymbolTypeTcc::PacketReceivedLargeDelta,
                    run_length: 1,
                }),
            ],
            recv_deltas: vec![
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: 0,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedLargeDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR * 256,
                },
            ],
        },
        pkt
    );
    marshal_all(&rtcp_packets);
}

#[test]
fn test_build_feedback_packet_rolling() {
    let mut recoder = Recorder::new(5000);

    let mut arrival_time: i64 = SCALE_FACTOR_REFERENCE_TIME;
    // The original has `arrivalTime` and `increaseTime(&arrivalTime, ...)` in one slice
    // literal. Go's compiler makes the call before it reads the variable, so both packets get
    // the increased time (seen by running the original): the same is done here.
    let increased = increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR);
    assert_eq!([64250, 64250], [arrival_time, increased]);
    add_run(&mut recoder, &[65534, 65535], &[arrival_time, increased]);

    let rtcp_packets = recoder.build_feedback_packet();
    assert_eq!(1, rtcp_packets.len());

    add_run(
        &mut recoder,
        &[0, 4, 5, 6],
        &[
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
        ],
    );

    let rtcp_packets = recoder.build_feedback_packet();
    assert_eq!(1, rtcp_packets.len());

    let pkt = rtcp_to_twcc(&rtcp_packets)[0];
    assert_eq!(
        Header {
            count: FORMAT_TCC,
            packet_type: PacketType::TransportSpecificFeedback,
            padding: true,
            length: 6,
        },
        pkt.header()
    );
    assert_eq!(
        &TransportLayerCc {
            sender_ssrc: 5000,
            media_ssrc: 5000,
            base_sequence_number: 0,
            reference_time: 1,
            fb_pkt_count: 1,
            packet_status_count: 7,
            packet_chunks: vec![PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                type_tcc: StatusChunkTypeTcc::RunLengthChunk,
                symbol_size: SymbolSizeTypeTcc::TwoBit,
                symbol_list: vec![
                    SymbolTypeTcc::PacketReceivedSmallDelta,
                    SymbolTypeTcc::PacketNotReceived,
                    SymbolTypeTcc::PacketNotReceived,
                    SymbolTypeTcc::PacketNotReceived,
                    SymbolTypeTcc::PacketReceivedSmallDelta,
                    SymbolTypeTcc::PacketReceivedSmallDelta,
                    SymbolTypeTcc::PacketReceivedSmallDelta,
                ],
            })],
            recv_deltas: vec![
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR * 2,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
            ],
        },
        pkt
    );
    marshal_all(&rtcp_packets);
}

#[test]
fn test_build_feedback_packet_min_input() {
    let mut r = Recorder::new(5000);

    let arrival_time: i64 = SCALE_FACTOR_REFERENCE_TIME;
    add_run(&mut r, &[0], &[arrival_time]);

    let pkts = r.build_feedback_packet();
    assert_eq!(1, pkts.len());

    let pkt = rtcp_to_twcc(&pkts)[0];
    assert_eq!(
        Header {
            count: FORMAT_TCC,
            packet_type: PacketType::TransportSpecificFeedback,
            length: 5,
            padding: true,
        },
        pkt.header()
    );
    assert_eq!(
        &TransportLayerCc {
            sender_ssrc: 5000,
            media_ssrc: 5000,
            base_sequence_number: 0,
            reference_time: 1,
            fb_pkt_count: 0,
            packet_status_count: 1,
            packet_chunks: vec![PacketStatusChunk::RunLengthChunk(RunLengthChunk {
                packet_status_symbol: SymbolTypeTcc::from(1),
                type_tcc: StatusChunkTypeTcc::RunLengthChunk,
                run_length: 1,
            })],
            recv_deltas: vec![RecvDelta {
                type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                delta: 0,
            }],
        },
        pkt
    );
    marshal_all(&pkts);
}

#[test]
fn test_build_feedback_packet_missing_packets_between_feedbacks() {
    let mut recorder = Recorder::new(5000);

    // Create a run of received packets.
    let mut arrival_time: i64 = SCALE_FACTOR_REFERENCE_TIME;
    add_run(
        &mut recorder,
        &[0, 1, 2, 3],
        &[
            SCALE_FACTOR_REFERENCE_TIME,
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
        ],
    );
    let rtcp_packets = recorder.build_feedback_packet();
    assert_eq!(1, rtcp_packets.len());

    // Now create another run of received packets, but with a gap.
    add_run(
        &mut recorder,
        &[7, 8, 9],
        &[
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR * 256),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
        ],
    );
    let rtcp_packets = recorder.build_feedback_packet();
    assert_eq!(1, rtcp_packets.len());
    let twcc_packet = rtcp_to_twcc(&rtcp_packets)[0];
    assert_eq!(
        4u16, twcc_packet.base_sequence_number,
        "Base sequence should be one after the end of the previous feedback"
    );
    assert_eq!(
        6u16, twcc_packet.packet_status_count,
        "Feedback should include status for both the lost and received packets"
    );
    let expected_packet_chunks = vec![PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
        type_tcc: StatusChunkTypeTcc::RunLengthChunk,
        symbol_size: SymbolSizeTypeTcc::TwoBit,
        symbol_list: vec![
            SymbolTypeTcc::PacketNotReceived,
            SymbolTypeTcc::PacketNotReceived,
            SymbolTypeTcc::PacketNotReceived,
            SymbolTypeTcc::PacketReceivedSmallDelta,
            SymbolTypeTcc::PacketReceivedSmallDelta,
            SymbolTypeTcc::PacketReceivedSmallDelta,
        ],
    })];
    assert_eq!(expected_packet_chunks, twcc_packet.packet_chunks);
    marshal_all(&rtcp_packets);
}

#[test]
fn test_build_feedback_packet_count() {
    let mut recorder = Recorder::new(5000);

    let arrival_time: i64 = SCALE_FACTOR_REFERENCE_TIME;
    add_run(&mut recorder, &[0, 1], &[arrival_time, arrival_time]);

    let pkts = recorder.build_feedback_packet();
    assert_eq!(1, pkts.len());

    let twcc = rtcp_to_twcc(&pkts)[0];
    assert_eq!(0u8, twcc.fb_pkt_count);

    add_run(&mut recorder, &[0, 1], &[arrival_time, arrival_time]);

    let pkts = recorder.build_feedback_packet();
    assert_eq!(1, pkts.len());

    let twcc = rtcp_to_twcc(&pkts)[0];
    assert_eq!(1u8, twcc.fb_pkt_count);
}

#[test]
fn test_duplicate_packets() {
    let mut r = Recorder::new(5000);

    let arrival_time: i64 = SCALE_FACTOR_REFERENCE_TIME;
    add_run(
        &mut r,
        &[12, 13, 13, 14],
        &[arrival_time, arrival_time, arrival_time, arrival_time],
    );

    let rtcp_packets = r.build_feedback_packet();
    assert_eq!(1, rtcp_packets.len());

    let pkt = rtcp_to_twcc(&rtcp_packets)[0];
    assert_eq!(
        Header {
            count: FORMAT_TCC,
            packet_type: PacketType::TransportSpecificFeedback,
            padding: true,
            length: 6,
        },
        pkt.header()
    );
    assert_eq!(
        &TransportLayerCc {
            sender_ssrc: 5000,
            media_ssrc: 5000,
            base_sequence_number: 12,
            reference_time: 1,
            fb_pkt_count: 0,
            packet_status_count: 3,
            packet_chunks: vec![PacketStatusChunk::RunLengthChunk(RunLengthChunk {
                type_tcc: StatusChunkTypeTcc::RunLengthChunk,
                packet_status_symbol: SymbolTypeTcc::PacketReceivedSmallDelta,
                run_length: 3,
            })],
            recv_deltas: vec![
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: 0,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: 0,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: 0,
                },
            ],
        },
        pkt
    );
    marshal_all(&rtcp_packets);
}

#[test]
fn test_short_deltas() {
    {
        let name = "SplitsOneBitDeltas";
        let mut recorder = Recorder::new(5000);

        let arrival_time: i64 = SCALE_FACTOR_REFERENCE_TIME;
        add_run(
            &mut recorder,
            &[3, 4, 5, 7, 6, 8, 10, 11, 13, 14],
            &[
                arrival_time,
                arrival_time,
                arrival_time,
                arrival_time,
                arrival_time,
                arrival_time,
                arrival_time,
                arrival_time,
                arrival_time,
                arrival_time,
            ],
        );

        let rtcp_packets = recorder.build_feedback_packet();
        assert_eq!(1, rtcp_packets.len(), "{name}");

        let pkt = rtcp_to_twcc(&rtcp_packets)[0];
        let bs = marshal(pkt);
        let (header, unmarshalled) = unmarshal(&bs);

        assert_eq!(
            Header {
                count: FORMAT_TCC,
                packet_type: PacketType::TransportSpecificFeedback,
                padding: true,
                length: 8,
            },
            header,
            "{name}"
        );
        assert_eq!(
            TransportLayerCc {
                sender_ssrc: 5000,
                media_ssrc: 5000,
                base_sequence_number: 3,
                reference_time: 1,
                fb_pkt_count: 0,
                packet_status_count: 12,
                packet_chunks: vec![
                    PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                        type_tcc: StatusChunkTypeTcc::StatusVectorChunk,
                        symbol_size: SymbolSizeTypeTcc::TwoBit,
                        symbol_list: symbols(&[1, 1, 1, 1, 1, 1, 0]),
                    }),
                    PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                        type_tcc: StatusChunkTypeTcc::StatusVectorChunk,
                        symbol_size: SymbolSizeTypeTcc::TwoBit,
                        symbol_list: symbols(&[1, 1, 0, 1, 1, 0, 0]),
                    }),
                ],
                recv_deltas: vec![
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                ],
            },
            unmarshalled,
            "{name}"
        );
        marshal_all(&rtcp_packets);
    }

    {
        let name = "padsTwoBitDeltas";
        let mut r = Recorder::new(5000);

        let arrival_time: i64 = SCALE_FACTOR_REFERENCE_TIME;
        add_run(
            &mut r,
            &[3, 4, 5, 7],
            &[arrival_time, arrival_time, arrival_time, arrival_time],
        );

        let rtcp_packets = r.build_feedback_packet();
        assert_eq!(1, rtcp_packets.len(), "{name}");

        let pkt = rtcp_to_twcc(&rtcp_packets)[0];
        let bs = marshal(pkt);
        let (header, unmarshalled) = unmarshal(&bs);

        assert_eq!(
            Header {
                count: FORMAT_TCC,
                packet_type: PacketType::TransportSpecificFeedback,
                padding: true,
                length: 6,
            },
            header,
            "{name}"
        );
        assert_eq!(
            TransportLayerCc {
                sender_ssrc: 5000,
                media_ssrc: 5000,
                base_sequence_number: 3,
                reference_time: 1,
                fb_pkt_count: 0,
                packet_status_count: 5,
                packet_chunks: vec![PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                    type_tcc: StatusChunkTypeTcc::StatusVectorChunk,
                    symbol_size: SymbolSizeTypeTcc::TwoBit,
                    symbol_list: symbols(&[1, 1, 1, 0, 1, 0, 0]),
                })],
                recv_deltas: vec![
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                    RecvDelta {
                        type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                        delta: 0,
                    },
                ],
            },
            unmarshalled,
            "{name}"
        );
        marshal_all(&rtcp_packets);
    }
}

#[test]
fn test_reordered_packets() {
    let mut recorder = Recorder::new(5000);

    let mut arrival_time: i64 = SCALE_FACTOR_REFERENCE_TIME;
    add_run(
        &mut recorder,
        &[3, 4, 5, 7, 6, 8, 10, 11, 13, 14],
        &[
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
        ],
    );

    let rtcp_packets = recorder.build_feedback_packet();
    assert_eq!(1, rtcp_packets.len());

    let pkt = rtcp_to_twcc(&rtcp_packets)[0];
    let bs = marshal(pkt);
    let (header, unmarshalled) = unmarshal(&bs);

    assert_eq!(
        Header {
            count: FORMAT_TCC,
            packet_type: PacketType::TransportSpecificFeedback,
            padding: true,
            length: 8,
        },
        header
    );
    assert_eq!(
        TransportLayerCc {
            sender_ssrc: 5000,
            media_ssrc: 5000,
            base_sequence_number: 3,
            reference_time: 1,
            fb_pkt_count: 0,
            packet_status_count: 12,
            packet_chunks: vec![
                PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                    type_tcc: StatusChunkTypeTcc::StatusVectorChunk,
                    symbol_size: SymbolSizeTypeTcc::TwoBit,
                    symbol_list: symbols(&[1, 1, 1, 1, 2, 1, 0]),
                }),
                PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                    type_tcc: StatusChunkTypeTcc::StatusVectorChunk,
                    symbol_size: SymbolSizeTypeTcc::TwoBit,
                    symbol_list: symbols(&[1, 1, 0, 1, 1, 0, 0]),
                }),
            ],
            recv_deltas: vec![
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: 2 * TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedLargeDelta,
                    delta: -TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: 2 * TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: TYPE_TCC_DELTA_SCALE_FACTOR,
                },
            ],
        },
        unmarshalled
    );
    marshal_all(&rtcp_packets);
}

#[test]
fn test_packets_hheld() {
    let mut recorder = Recorder::new(5000);
    assert_eq!(0, recorder.packets_held());

    let mut arrival_time: i64 = SCALE_FACTOR_REFERENCE_TIME;
    // As in test_build_feedback_packet_rolling: the original reads `arrivalTime` in the slice
    // literal that also increases it twice, and Go's compiler reads it after the calls.
    let increased = [
        increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
        increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
    ];
    assert_eq!(
        [64500, 64250, 64500],
        [arrival_time, increased[0], increased[1]]
    );
    add_run(
        &mut recorder,
        &[0, 1, 2],
        &[arrival_time, increased[0], increased[1]],
    );
    assert_eq!(recorder.packets_held(), 3);

    add_run(
        &mut recorder,
        &[3, 4],
        &[
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
            increase_time(&mut arrival_time, TYPE_TCC_DELTA_SCALE_FACTOR),
        ],
    );
    assert_eq!(recorder.packets_held(), 5);

    recorder.build_feedback_packet();
    assert_eq!(0, recorder.packets_held());
}

// The original asserts with `testing.AllocsPerRun` that `Record` does not allocate once the
// arrival-time map has reached its maximum size. Counting allocations in Rust needs a global
// allocator for the whole test binary, which one test cannot bring. What can allocate in
// `record` is the reallocation of the map's buffer and nothing else (the start sequence number
// is not a pointer here), so the test asserts that the buffer stays where and as large as it
// is: the same warm-up, the same 1000 runs.
#[test]
fn test_record_does_not_allocate_in_steady_state() {
    let mut recorder = Recorder::new(5000);

    let mut arrival_time: i64 = 0;
    let mut sequence_number: u16 = 0;
    let mut record = |recorder: &mut Recorder| {
        arrival_time += 1000;
        recorder.record(5000, sequence_number, arrival_time);
        sequence_number = sequence_number.wrapping_add(1);
    };

    // Warm up until the arrival-time map reaches its maximum size, after which
    // in-order packets no longer resize it. In normal operation the map stays
    // small because old entries are culled, but culling only runs once
    // BuildFeedbackPacket has consumed the recorded range, and building
    // feedback allocates. Growing the map to its cap instead gives Record a
    // steady state without any builds in the measured loop.
    for _ in 0..MAX_NUMBER_OF_PACKETS as usize + MIN_CAPACITY {
        record(&mut recorder);
    }

    let buffer = |recorder: &Recorder| {
        (
            recorder.arrival_time_map.arrival_times.as_ptr(),
            recorder.arrival_time_map.capacity(),
        )
    };
    let before = buffer(&recorder);
    assert_eq!(MAX_NUMBER_OF_PACKETS as usize, before.1);
    for _ in 0..1000 {
        record(&mut recorder);
        assert_eq!(before, buffer(&recorder));
    }
}

// New for musivum: the marshalled feedback, byte for byte, against what the original builds
// from the same packets. The expected bytes are from pion/interceptor v0.1.49 with pion/rtcp
// v1.2.19 (`Recorder.Record`, `BuildFeedbackPacket` and `Marshal` driven by a Go test with
// these same five series). This is what ties the marshalling of `rtc-rtcp` to pion's.
#[test]
fn feedback_is_byte_for_byte_what_the_original_builds() {
    fn hex(bs: &[u8]) -> String {
        bs.iter().map(|b| format!("{b:02x}")).collect()
    }
    struct Driver {
        name: &'static str,
        r: Recorder,
        n: usize,
        got: Vec<(String, String)>,
    }
    impl Driver {
        fn new(name: &'static str, sender_ssrc: u32) -> Self {
            Self {
                name,
                r: Recorder::new(sender_ssrc),
                n: 0,
                got: Vec::new(),
            }
        }
        fn build(&mut self) {
            for p in self.r.build_feedback_packet() {
                self.got.push((
                    format!("{} {}", self.name, self.n),
                    hex(&marshal(p.as_ref())),
                ));
                self.n += 1;
            }
        }
    }
    let mut got: Vec<(String, String)> = Vec::new();

    // A long series from a linear congruential generator: loss, jitter with small and large
    // deltas, packets that arrive late with an earlier time, duplicates, a sequence number
    // that wraps, and arrival times as large as a clock in microseconds since 1970.
    {
        let mut d = Driver::new("lcg", 0x01020304);
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = || -> u32 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u32
        };
        let mut sn: u16 = 65400;
        let mut at: i64 = 1_763_100_000_000_000;
        let steps: [i64; 4] = [1000, 5000, 20000, 70000];
        let mut recorded = 0;
        for _ in 0..400 {
            at += steps[(next() % 4) as usize];
            if next() % 10 == 0 {
                // lost
                sn = sn.wrapping_add(1);
                continue;
            }
            let mut arrival = at;
            if next() % 13 == 0 {
                arrival = at - 30000;
            }
            d.r.record(0x0a0b0c0d, sn, arrival);
            if next() % 17 == 0 {
                // duplicate, later
                d.r.record(0x0a0b0c0d, sn, arrival + 250);
            }
            if next() % 11 == 0 && sn > 3 {
                // an old one that was lost before, or a duplicate of one that was not
                d.r.record(0x0a0b0c0d, sn - 3, arrival + 500);
            }
            sn = sn.wrapping_add(1);
            recorded += 1;
            if recorded % 20 == 0 {
                d.build();
            }
        }
        d.build();
        got.append(&mut d.got);
    }

    // A long gap: run length chunks at their cap.
    {
        let mut d = Driver::new("gap", 1);
        d.r.record(2, 10, 1_000_000);
        d.r.record(2, 30010, 1_010_000);
        d.r.record(2, 30011, 1_010_100);
        d.build();
        got.append(&mut d.got);
    }

    // Fourteen packets without a large delta, some of them missing: a one bit status vector.
    {
        let mut d = Driver::new("onebit", 1);
        let mut at: i64 = 640_000;
        for sn in [0u16, 1, 3, 4, 5, 8, 9, 10, 11, 13, 14, 15, 16, 20, 21] {
            at += 1000;
            d.r.record(2, sn, at);
        }
        d.build();
        got.append(&mut d.got);
    }

    // Packets that are older than the window when the next packet arrives after a feedback
    // are dropped from the history: one of them that arrives again is new. Within the window
    // a packet that arrives again stays a duplicate.
    {
        let mut d = Driver::new("cull", 1);
        let mut at: i64 = 10_000_000;
        for sn in 0u16..6 {
            at += 2000;
            d.r.record(2, sn, at);
        }
        d.build();
        at += 600_000;
        d.r.record(2, 6, at);
        d.r.record(2, 3, at + 1000);
        d.r.record(2, 7, at + 2000);
        d.build();
        at += 100_000;
        d.r.record(2, 8, at);
        d.r.record(2, 7, at + 1000);
        d.r.record(2, 9, at + 2000);
        d.build();
        got.append(&mut d.got);
    }

    // The feedback packet count wraps after 255.
    {
        let mut d = Driver::new("count", 7);
        let mut at: i64 = 64_000_000;
        for sn in 0u16..258 {
            at += 10_000;
            d.r.record(8, sn, at);
            if sn < 254 {
                d.r.build_feedback_packet();
                d.n += 1;
            } else {
                d.build();
            }
        }
        got.append(&mut d.got);
    }

    let expected = [
        (
            "lcg 0",
            "afcd000d010203040a0b0c0dff78001503cbfc00d969d156d9a5040118141401180118041418501404011850011804011801185004000003",
        ),
        (
            "lcg 1",
            "afcd000d010203040a0b0c0dff8d001603cc0501d516e998d55920011804146414ffd80190011814ffd88c01186450500414011814040002",
        ),
        (
            "lcg 2",
            "afcd000d010203040a0b0c0dffa3001603cc0c02d565e655d2494001541450040118500401185001181404045004016814011c1401180002",
        ),
        (
            "lcg 3",
            "afcd000e010203040a0b0c0dffb8001703cc1403d94ad925e55ad800dc01181404011cff9c8cff8c8c011c50140118145050500118ff8c7cffd80002",
        ),
        (
            "lcg 4",
            "8fcd000c010203040a0b0c0dffcf001703cc1c04c455d695d5a12002d864045050141401180118041414045014ff8c0190080450",
        ),
        (
            "lcg 5",
            "8fcd000d010203040a0b0c0dffe6001803cc2305d649e652d942e5005c14011814012c0401181401180404011c1401180414016c01180414",
        ),
        (
            "lcg 6",
            "afcd000d010203040a0b0c0dfffe001503cc2d06d592d569e659185050011814011c14501404011801180401181401180404011814000003",
        ),
        (
            "lcg 7",
            "afcd000d010203040a0b0c0d0013001503cc3607d558e6a9d55a78141450040118016814ff9c019001180118141414041404011801180002",
        ),
        (
            "lcg 8",
            "afcd000c010203040a0b0c0d0028001503cc4008d156e559d99584645014500118011850041450ff8c7c50011814ff9cc8140401",
        ),
        (
            "lcg 9",
            "afcd000d010203040a0b0c0d003a001803cc4609d566d459d56520036c1404040118500118a0c8541401180450141404ff9c8c0414141401",
        ),
        (
            "lcg 10",
            "8fcd000d010203040a0b0c0d0052001a03cc4d0ac519d682c595ea10f40428ff8c8c140401180118017c5450011814505001180118011854",
        ),
        (
            "lcg 11",
            "8fcd000c010203040a0b0c0d006c001603cc580bd55ae455d5582001580404500401180118ff9cc8181404141404041404011808",
        ),
        (
            "lcg 12",
            "afcd000d010203040a0b0c0d0082001703cc5c0cd990e649d6552002f4011814011850ffa48c011804012c045004ff9c7c14141404500002",
        ),
        (
            "lcg 13",
            "afcd000d010203040a0b0c0d0099001603cc640dd556e859d5a52001180414500414ffd80190011818140118045014040106ff5e50505001",
        ),
        (
            "lcg 14",
            "8fcd000c010203040a0b0c0d00af001803cc6b0ed554d552e469c500341404140450a050145014011cff9cc82801180118046404",
        ),
        (
            "lcg 15",
            "afcd000d010203040a0b0c0d00c7001603cc710fd999e549d4654001b0011804011814011850011850041401680414045401180404011801",
        ),
        (
            "lcg 16",
            "afcd000d010203040a0b0c0d00dd001803cc7c10d685d129d156e50008a00190011818505008011cff8c8c1408140404ff9c019014500002",
        ),
        (
            "lcg 17",
            "afcd000c010203040a0b0c0d00f5001303cc8311da8ad595c5505c01180118034efe6e0190041404011850501464141414000003",
        ),
        (
            "gap 0",
            "afcd00080000000100000002000a753200000f00a0001fff1fff1fff15252002a0280001",
        ),
        (
            "onebit 0",
            "afcd000a00000001000000020000001600000a00b73dd5012001040404040404040404040404040404000003",
        ),
        (
            "cull 0",
            "8fcd000600000001000000020000000600009c002006480808080808",
        ),
        (
            "cull 1",
            "afcd00060000000100000002000300050000a501d090d4fffc080002",
        ),
        (
            "cull 2",
            "afcd00060000000100000002000700030000a502d900d80188080002",
        ),
        (
            "count 254",
            "afcd0005000000070000000800fe000100040ffe2001d801",
        ),
        (
            "count 255",
            "afcd0005000000070000000800ff0001000410ff20010001",
        ),
        (
            "count 256",
            "afcd00050000000700000008010000010004100020012801",
        ),
        (
            "count 257",
            "afcd00050000000700000008010100010004100120015001",
        ),
    ];
    assert_eq!(expected.len(), got.len());
    for (i, (name, bytes)) in expected.iter().enumerate() {
        assert_eq!((*name, *bytes), (got[i].0.as_str(), got[i].1.as_str()));
    }
}
