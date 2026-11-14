// Ported from LiveKit, pkg/sfu/rtpstats/rtpstats_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; what the tests read of a statistics object they read
// through its lock; `BenchmarkRTPStatsReceiver_Update` is left out.

use bytes::Bytes;

use super::rtpstats_receiver::RESTART_THRESHOLD;
use super::*;
use crate::go::time;
use crate::webrtc::rtp;

fn get_packet(sn: u16, ts: u32, payload_size: usize) -> rtp::Packet {
    rtp::Packet {
        header: rtp::Header {
            sequence_number: sn,
            timestamp: ts,
            ..Default::default()
        },
        payload: Bytes::from(vec![0u8; payload_size]),
        ..Default::default()
    }
}

#[test]
fn test_rtp_stats_receiver_update() {
    let clock_rate = 90000u32;
    let r = RtpStatsReceiver::new(RtpStatsParams::default());
    r.set_clock_rate(clock_rate);

    let mut sequence_number = (rand::random::<f64>() * (1u64 << 16) as f64) as u16;
    let mut timestamp = (rand::random::<f64>() * (1u64 << 32) as f64) as u32;
    let mut packet = get_packet(sequence_number, timestamp, 1000);
    let mut flow_state = r.update(
        time::now().unix_nano(),
        packet.header.sequence_number,
        packet.header.timestamp,
        packet.header.marker,
        packet.header.marshal_size(),
        packet.payload.len(),
        0,
    );
    let _ = flow_state;
    assert!(r.lock.read().rtp_stats_base.rtp_stats_base_lite.initialized);
    assert_eq!(sequence_number, r.lock.read().sequence_number.get_highest());
    assert_eq!(
        sequence_number,
        r.lock.read().sequence_number.get_extended_highest() as u16
    );
    assert_eq!(timestamp, r.lock.read().timestamp.get_highest());
    assert_eq!(
        timestamp,
        r.lock.read().timestamp.get_extended_highest() as u32
    );

    // in-order, no loss
    sequence_number = sequence_number.wrapping_add(1);
    timestamp = timestamp.wrapping_add(3000);
    packet = get_packet(sequence_number, timestamp, 1000);
    flow_state = r.update(
        time::now().unix_nano(),
        packet.header.sequence_number,
        packet.header.timestamp,
        packet.header.marker,
        packet.header.marshal_size(),
        packet.payload.len(),
        0,
    );
    let _ = flow_state;
    assert_eq!(sequence_number, r.lock.read().sequence_number.get_highest());
    assert_eq!(
        sequence_number,
        r.lock.read().sequence_number.get_extended_highest() as u16
    );
    assert_eq!(timestamp, r.lock.read().timestamp.get_highest());
    assert_eq!(
        timestamp,
        r.lock.read().timestamp.get_extended_highest() as u32
    );

    // out-of-order, would cause a restart which is disallowed
    packet = get_packet(
        sequence_number.wrapping_sub(10),
        timestamp.wrapping_sub(30000),
        1000,
    );
    flow_state = r.update(
        time::now().unix_nano(),
        packet.header.sequence_number,
        packet.header.timestamp,
        packet.header.marker,
        packet.header.marshal_size(),
        packet.payload.len(),
        0,
    );
    assert_eq!(
        RtpFlowUnhandledReason::PreStartTimestamp,
        flow_state.unhandled_reason
    );
    assert_eq!(sequence_number, r.lock.read().sequence_number.get_highest());
    assert_eq!(
        sequence_number,
        r.lock.read().sequence_number.get_extended_highest() as u16
    );
    assert_eq!(timestamp, r.lock.read().timestamp.get_highest());
    assert_eq!(
        timestamp,
        r.lock.read().timestamp.get_extended_highest() as u32
    );
    assert_eq!(
        0u64,
        r.lock
            .read()
            .rtp_stats_base
            .rtp_stats_base_lite
            .packets_out_of_order
    );
    assert_eq!(0u64, r.lock.read().rtp_stats_base.packets_duplicate);

    // duplicate of the above out-of-order packet, but would not be handled as it causes a restart
    packet = get_packet(
        sequence_number.wrapping_sub(10),
        timestamp.wrapping_sub(30000),
        1000,
    );
    flow_state = r.update(
        time::now().unix_nano(),
        packet.header.sequence_number,
        packet.header.timestamp,
        packet.header.marker,
        packet.header.marshal_size(),
        packet.payload.len(),
        0,
    );
    assert_eq!(
        RtpFlowUnhandledReason::PreStartTimestamp,
        flow_state.unhandled_reason
    );
    assert_eq!(sequence_number, r.lock.read().sequence_number.get_highest());
    assert_eq!(
        sequence_number,
        r.lock.read().sequence_number.get_extended_highest() as u16
    );
    assert_eq!(timestamp, r.lock.read().timestamp.get_highest());
    assert_eq!(
        timestamp,
        r.lock.read().timestamp.get_extended_highest() as u32
    );
    assert_eq!(
        0u64,
        r.lock
            .read()
            .rtp_stats_base
            .rtp_stats_base_lite
            .packets_out_of_order
    );
    assert_eq!(0u64, r.lock.read().rtp_stats_base.packets_duplicate);

    // loss
    sequence_number = sequence_number.wrapping_add(10);
    timestamp = timestamp.wrapping_add(30000);
    packet = get_packet(sequence_number, timestamp, 1000);
    flow_state = r.update(
        time::now().unix_nano(),
        packet.header.sequence_number,
        packet.header.timestamp,
        packet.header.marker,
        packet.header.marshal_size(),
        packet.payload.len(),
        0,
    );
    assert_eq!(
        sequence_number.wrapping_sub(9) as u64,
        flow_state.loss_start_inclusive
    );
    assert_eq!(sequence_number as u64, flow_state.loss_end_exclusive);
    assert_eq!(
        9u64,
        r.lock
            .read()
            .rtp_stats_base
            .rtp_stats_base_lite
            .packets_lost
    );

    // out-of-order should decrement number of lost packets
    packet = get_packet(
        sequence_number.wrapping_sub(6),
        timestamp.wrapping_sub(18000),
        1000,
    );
    flow_state = r.update(
        time::now().unix_nano(),
        packet.header.sequence_number,
        packet.header.timestamp,
        packet.header.marker,
        packet.header.marshal_size(),
        packet.payload.len(),
        0,
    );
    let _ = flow_state;
    assert_eq!(sequence_number, r.lock.read().sequence_number.get_highest());
    assert_eq!(
        sequence_number,
        r.lock.read().sequence_number.get_extended_highest() as u16
    );
    assert_eq!(timestamp, r.lock.read().timestamp.get_highest());
    assert_eq!(
        timestamp,
        r.lock.read().timestamp.get_extended_highest() as u32
    );
    assert_eq!(
        1u64,
        r.lock
            .read()
            .rtp_stats_base
            .rtp_stats_base_lite
            .packets_out_of_order
    );
    assert_eq!(0u64, r.lock.read().rtp_stats_base.packets_duplicate);
    assert_eq!(
        8u64,
        r.lock
            .read()
            .rtp_stats_base
            .rtp_stats_base_lite
            .packets_lost
    );

    // test sequence number history
    // with a gap
    sequence_number = sequence_number.wrapping_add(2);
    timestamp = timestamp.wrapping_add(6000);
    packet = get_packet(sequence_number, timestamp, 1000);
    flow_state = r.update(
        time::now().unix_nano(),
        packet.header.sequence_number,
        packet.header.timestamp,
        packet.header.marker,
        packet.header.marshal_size(),
        packet.payload.len(),
        0,
    );
    assert_eq!(
        sequence_number.wrapping_sub(1) as u64,
        flow_state.loss_start_inclusive
    );
    assert_eq!(sequence_number as u64, flow_state.loss_end_exclusive);
    assert_eq!(
        9u64,
        r.lock
            .read()
            .rtp_stats_base
            .rtp_stats_base_lite
            .packets_lost
    );
    assert!(
        !r.lock
            .read()
            .history
            .is_set((sequence_number as u64).wrapping_sub(1))
    );

    // out-of-order
    sequence_number = sequence_number.wrapping_sub(1);
    timestamp = timestamp.wrapping_sub(3000);
    packet = get_packet(sequence_number, timestamp, 999);
    flow_state = r.update(
        time::now().unix_nano(),
        packet.header.sequence_number,
        packet.header.timestamp,
        packet.header.marker,
        packet.header.marshal_size(),
        packet.payload.len(),
        0,
    );
    let _ = flow_state;
    assert_eq!(
        8u64,
        r.lock
            .read()
            .rtp_stats_base
            .rtp_stats_base_lite
            .packets_lost
    );
    assert_eq!(
        2u64,
        r.lock
            .read()
            .rtp_stats_base
            .rtp_stats_base_lite
            .packets_out_of_order
    );
    assert!(r.lock.read().history.is_set(sequence_number as u64));

    // padding only
    sequence_number = sequence_number.wrapping_add(2);
    timestamp = timestamp.wrapping_add(3000);
    packet = get_packet(sequence_number, timestamp, 0);
    flow_state = r.update(
        time::now().unix_nano(),
        packet.header.sequence_number,
        packet.header.timestamp,
        packet.header.marker,
        packet.header.marshal_size(),
        packet.payload.len(),
        25,
    );
    let _ = flow_state;
    assert_eq!(
        8u64,
        r.lock
            .read()
            .rtp_stats_base
            .rtp_stats_base_lite
            .packets_lost
    );
    assert_eq!(
        2u64,
        r.lock
            .read()
            .rtp_stats_base
            .rtp_stats_base_lite
            .packets_out_of_order
    );
    assert!(r.lock.read().history.is_set(sequence_number as u64));
    assert!(
        r.lock
            .read()
            .history
            .is_set((sequence_number as u64).wrapping_sub(1))
    );
    assert!(
        r.lock
            .read()
            .history
            .is_set((sequence_number as u64).wrapping_sub(2))
    );

    // old packet, but simulating increasing sequence number after roll over
    packet = get_packet(
        sequence_number.wrapping_add(400),
        timestamp.wrapping_sub(6000),
        300,
    );
    flow_state = r.update(
        time::now().unix_nano(),
        packet.header.sequence_number,
        packet.header.timestamp,
        packet.header.marker,
        packet.header.marshal_size(),
        packet.payload.len(),
        0,
    );
    assert_eq!(
        RtpFlowUnhandledReason::OldSequenceNumber,
        flow_state.unhandled_reason
    );
    assert_eq!(sequence_number, r.lock.read().sequence_number.get_highest());
    assert_eq!(
        sequence_number,
        r.lock.read().sequence_number.get_extended_highest() as u16
    );
    assert_eq!(timestamp, r.lock.read().timestamp.get_highest());
    assert_eq!(
        timestamp,
        r.lock.read().timestamp.get_extended_highest() as u32
    );

    r.stop();
}

#[test]
fn test_rtp_stats_receiver_update_fec() {
    let r = RtpStatsReceiver::new(RtpStatsParams::default());
    r.set_clock_rate(90000);
    let snapshot_id = r.new_snapshot_id();

    let packet = get_packet(100, 3000, 1000);
    r.update(
        time::now().unix_nano(),
        packet.header.sequence_number,
        packet.header.timestamp,
        packet.header.marker,
        packet.header.marshal_size(),
        packet.payload.len(),
        0,
    );
    r.update_fec(3, 900, 1, 2);

    let mut stats = r.to_proto().unwrap();
    assert_eq!(3, stats.fec_packets);
    assert_eq!(900, stats.fec_bytes);
    assert_eq!(1, stats.fec_packets_discarded);
    assert_eq!(2, stats.fec_packets_recovered);

    let mut delta = r.delta_info(snapshot_id).unwrap();
    assert_eq!(3, delta.fec_packets);
    assert_eq!(900, delta.fec_bytes);
    assert_eq!(1, delta.fec_packets_discarded);
    assert_eq!(2, delta.fec_packets_recovered);

    // FEC-only intervals are reported even if no primary packet arrived.
    r.update_fec(2, 500, 1, 1);
    delta = r.delta_info(snapshot_id).unwrap();
    assert_eq!(2, delta.fec_packets);
    assert_eq!(500, delta.fec_bytes);
    assert_eq!(1, delta.fec_packets_discarded);
    assert_eq!(1, delta.fec_packets_recovered);

    r.stop();
    r.update_fec(1, 100, 1, 1);
    stats = r.to_proto().unwrap();
    assert_eq!(5, stats.fec_packets);
    assert_eq!(1400, stats.fec_bytes);
    assert_eq!(2, stats.fec_packets_discarded);
    assert_eq!(3, stats.fec_packets_recovered);
}

#[test]
fn test_rtp_stats_receiver_restart() {
    let clock_rate = 90000u32;
    let r = RtpStatsReceiver::new(RtpStatsParams::default());
    r.set_clock_rate(clock_rate);

    // should not restart till there are at least threshold packets
    assert!(!r.lock.write().maybe_restart(10, 20, 1000));
    assert!(!r.lock.write().maybe_restart(11, 20, 1000));
    assert!(!r.lock.write().maybe_restart(13, 20, 1000));
    assert!(!r.lock.write().maybe_restart(14, 20, 1000));
    // although adding 5th packet should have enough packets for a check,
    // still should not restart as there is a sequence number gap between 11 and 13
    assert!(!r.lock.write().maybe_restart(15, 20, 1000));
    assert!(!r.lock.write().maybe_restart(16, 19, 1000));
    // has enough packets, but still cannot restart because timestamps are not increasing
    assert!(!r.lock.write().maybe_restart(17, 21, 1000));
    assert!(!r.lock.write().maybe_restart(18, 21, 1000));
    assert!(!r.lock.write().maybe_restart(19, 21, 1000));
    // can restart as there are enough packets with proper sequencing
    assert!(r.lock.write().maybe_restart(20, 21, 1000));
    assert_eq!(RESTART_THRESHOLD, r.lock.read().restart_packets_n);

    r.lock.write().reset_restart();
    assert_eq!(0, r.lock.read().restart_packets_n);

    r.stop();
}

#[test]
fn test_rtp_stats_sender_get_interval_stats() {
    // packetsNotFoundMetadata should match lost packets
    {
        let r = RtpStatsSender::new(RtpStatsParams::default(), 1024);
        let stats = r.lock.read().get_interval_stats(0, 10000, 10000);
        assert_eq!(
            8977, stats.packets_not_found_metadata,
            "packetsNotFoundMetadata should match lost packets"
        );
    }
}
