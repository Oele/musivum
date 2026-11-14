// Ported from LiveKit, pkg/sfu/bwe/sendsidebwe/congestion_detector_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the header of the report is not set by the test, as
// `rtc-rtcp` works out the padding and the length when it marshals.

use super::*;
use crate::protocol::logger;
use crate::webrtc::rtcp;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::{
    RecvDelta, RunLengthChunk, StatusChunkTypeTcc, StatusVectorChunk, SymbolSizeTypeTcc,
};

// the reserved symbol has no receive delta, so its packet is neither acked nor lost,
// and the next received packet uses the next delta
#[test]
fn test_handle_twcc_feedback_reserved_symbol() {
    let run_length = |packet_status_symbol, run_length| {
        PacketStatusChunk::RunLengthChunk(RunLengthChunk {
            type_tcc: StatusChunkTypeTcc::RunLengthChunk,
            packet_status_symbol,
            run_length,
        })
    };

    // small delta, reserved, reserved, small delta, not received
    for (name, chunks) in [
        (
            "status vector",
            vec![PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                type_tcc: StatusChunkTypeTcc::StatusVectorChunk,
                symbol_size: SymbolSizeTypeTcc::TwoBit,
                symbol_list: vec![
                    SymbolTypeTcc::PacketReceivedSmallDelta,
                    SymbolTypeTcc::PacketReceivedWithoutDelta,
                    SymbolTypeTcc::PacketReceivedWithoutDelta,
                    SymbolTypeTcc::PacketReceivedSmallDelta,
                    SymbolTypeTcc::PacketNotReceived,
                    SymbolTypeTcc::PacketNotReceived,
                    SymbolTypeTcc::PacketNotReceived,
                ],
            })],
        ),
        (
            "run length",
            vec![
                run_length(SymbolTypeTcc::PacketReceivedSmallDelta, 1),
                run_length(SymbolTypeTcc::PacketReceivedWithoutDelta, 2),
                run_length(SymbolTypeTcc::PacketReceivedSmallDelta, 1),
                run_length(SymbolTypeTcc::PacketNotReceived, 1),
            ],
        ),
    ] {
        let c = CongestionDetector::new(CongestionDetectorParams {
            config: DEFAULT_CONGESTION_DETECTOR_CONFIG,
            logger: logger::get_logger(),
        });
        // the first packet sent is the send time base and never joins a group, so keep it out of the report
        let now = mono::unix_micro();
        c.packet_tracker.record_packet_send_and_get_sequence_number(
            now,
            1200,
            false,
            ccutils::PROBE_CLUSTER_ID_INVALID,
            false,
        );
        let mut sns = [0u16; 5];
        for (i, sn) in sns.iter_mut().enumerate() {
            *sn = c.packet_tracker.record_packet_send_and_get_sequence_number(
                now + (i as i64 + 1) * 1000,
                1200,
                false,
                ccutils::PROBE_CLUSTER_ID_INVALID,
                false,
            );
        }

        let fb = TransportLayerCc {
            media_ssrc: 1,
            base_sequence_number: sns[0],
            packet_status_count: sns.len() as u16,
            reference_time: 100,
            fb_pkt_count: 1,
            packet_chunks: chunks,
            recv_deltas: vec![
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: 1000,
                },
                RecvDelta {
                    type_tcc_packet: SymbolTypeTcc::PacketReceivedSmallDelta,
                    delta: 2000,
                },
            ],
            ..Default::default()
        };

        // round trip so the report is exactly what a remote peer can deliver
        let fb: &dyn rtcp::Packet = &fb;
        let raw = fb
            .marshal()
            .unwrap_or_else(|err| panic!("{name}: the report marshals: {err}"));
        let mut raw = &raw[..];
        let pkts = rtcp::packet::unmarshal(&mut raw)
            .unwrap_or_else(|err| panic!("{name}: the report unmarshals: {err}"));

        c.handle_twcc_feedback(
            pkts[0]
                .as_any()
                .downcast_ref::<TransportLayerCc>()
                .unwrap_or_else(|| panic!("{name}: a transport-wide feedback report")),
        );

        // receive time is relative to the first received packet
        {
            let packet_tracker = c.packet_tracker.lock.lock();
            let pi = packet_tracker
                .get_packet_info_existing(sns[3])
                .unwrap_or_else(|| panic!("{name}: the packet info"));
            assert_eq!(2000, packet_tracker.packet_infos[pi].recv_time, "{name}");
        }

        let (mut acked, mut lost) = (0, 0);
        for pg in &c.lock.lock().packet_groups {
            acked += pg.acked.num_packets();
            lost += pg.lost.num_packets();
        }
        assert_eq!(2, acked, "{name}");
        assert_eq!(1, lost, "{name}");
    }
}
