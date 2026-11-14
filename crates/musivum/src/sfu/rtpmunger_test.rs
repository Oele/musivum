// Ported from LiveKit, pkg/sfu/rtpmunger_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `require.ErrorIs` is an equality of the `Result`; a
// test packet is a value, so `require.NotNil` on it has no counterpart.

use super::*;
use crate::protocol::logger;
use crate::sfu::testutils;

fn new_rtp_munger() -> RtpMunger {
    RtpMunger::new(logger::get_logger())
}

#[test]
fn test_set_last_sn_ts() {
    let mut r = new_rtp_munger();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params);
    assert!(ext_pkt.is_ok());
    let ext_pkt = ext_pkt.unwrap();

    r.set_last_sn_ts(&ext_pkt);
    assert_eq!(23332u64, r.ext_highest_incoming_sn);
    assert_eq!(23333u64, r.ext_last_sn);
    assert_eq!(0xabcdefu64, r.ext_last_ts);
    let err = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert!(err.is_err());
    let sn_offset = r.sn_range_map.get_value(r.ext_last_sn);
    assert_eq!(Ok(0u64), sn_offset);
    assert_eq!(0u64, r.sn_offset);
    assert_eq!(0u64, r.ts_offset);
}

#[test]
fn test_update_sn_ts_offsets() {
    let mut r = new_rtp_munger();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();
    r.set_last_sn_ts(&ext_pkt);

    let params = testutils::TestExtPacketParams {
        sequence_number: 33333,
        timestamp: 0xabcdef,
        ssrc: 0x87654321,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();
    r.update_sn_ts_offsets(&ext_pkt, 1, 1);
    assert_eq!(33332u64, r.ext_highest_incoming_sn);
    assert_eq!(23333u64, r.ext_last_sn);
    assert_eq!(0xabcdefu64, r.ext_last_ts);
    let err = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert!(err.is_err());
    let err = r.sn_range_map.get_value(r.ext_last_sn);
    assert!(err.is_err());
    assert_eq!(9999u64, r.sn_offset);
    assert_eq!(0xffff_ffff_ffff_ffffu64, r.ts_offset);
}

#[test]
fn test_packet_dropped() {
    let mut r = new_rtp_munger();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 10,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();
    r.set_last_sn_ts(&ext_pkt);
    assert_eq!(23332u64, r.ext_highest_incoming_sn);
    assert_eq!(23333u64, r.ext_last_sn);
    assert_eq!(0xabcdefu64, r.ext_last_ts);
    let err = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert!(err.is_err());
    let sn_offset = r.sn_range_map.get_value(r.ext_last_sn);
    assert_eq!(Ok(0u64), sn_offset);
    assert_eq!(0u64, r.ts_offset);

    let _ = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker); // update sequence number offset

    // drop a non-head packet, should cause no change in internals
    let params = testutils::TestExtPacketParams {
        sequence_number: 33333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();
    r.packet_dropped(&ext_pkt);
    assert_eq!(23333u64, r.ext_highest_incoming_sn);
    assert_eq!(23333u64, r.ext_last_sn);
    let sn_offset = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert_eq!(Ok(0u64), sn_offset);

    // drop a head packet and check offset increases
    let params = testutils::TestExtPacketParams {
        sequence_number: 44444,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let _ = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker); // update sequence number offset
    assert_eq!(44444u64, r.ext_last_sn);

    r.packet_dropped(&ext_pkt);
    assert_eq!(23333u64, r.ext_last_sn);
    let err = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert!(err.is_err());
    let sn_offset = r
        .sn_range_map
        .get_value(r.ext_highest_incoming_sn.wrapping_add(1));
    assert_eq!(Ok(1u64), sn_offset);

    let params = testutils::TestExtPacketParams {
        sequence_number: 44445,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let _ = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker); // update sequence number offset
    assert_eq!(r.ext_last_sn, 44444u64);
    let sn_offset = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert_eq!(Ok(1u64), sn_offset);
}

#[test]
fn test_out_of_order_sequence_number() {
    let mut r = new_rtp_munger();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 10,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();
    r.set_last_sn_ts(&ext_pkt);
    let _ = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);

    // should not be able to add a missing sequence number to the cache that is before start
    let err = r.sn_range_map.exclude_range(23332, 23333);
    assert!(err.is_err());

    // out-of-order sequence number before start should miss
    let params = testutils::TestExtPacketParams {
        sequence_number: 23331,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 10,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let (_, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert!(err.is_err());

    //  add a missing sequence number to the cache
    let err = r.sn_range_map.exclude_range(23334, 23335);
    assert_eq!(Ok(()), err);

    let params = testutils::TestExtPacketParams {
        sequence_number: 23336,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 10,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();
    let _ = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);

    // out-of-order sequence number should be munged from cache
    let params = testutils::TestExtPacketParams {
        sequence_number: 23335,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 10,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::OutOfOrder,
        ext_sequence_number: 23334,
        ext_timestamp: 0xabcdef,
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Ok(()), err);
    assert_eq!(tp_expected, tp);

    let params = testutils::TestExtPacketParams {
        sequence_number: 23332,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 10,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::OutOfOrder,
        ..Default::default()
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    // The original passes the error to `require.Error` as the message, it does not compare.
    assert!(err.is_err(), "{}", Error::OutOfOrderSequenceNumberCacheMiss);
    assert_eq!(tp_expected, tp);
}

#[test]
fn test_duplicate_sequence_number() {
    let mut r = new_rtp_munger();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();
    r.set_last_sn_ts(&ext_pkt);

    // send first packet through
    let _ = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);

    // send it again - duplicate packet
    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::Duplicate,
        ..Default::default()
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Err(Error::DuplicatePacket), err);
    assert_eq!(tp_expected, tp);
}

#[test]
fn test_padding_only_packet() {
    let mut r = new_rtp_munger();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();
    r.set_last_sn_ts(&ext_pkt);

    // contiguous padding only packet should report an error
    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::Contiguous,
        ..Default::default()
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert!(err.is_err());
    assert_eq!(Err(Error::PaddingOnlyPacket), err);
    assert_eq!(tp_expected, tp);
    assert_eq!(23333u64, r.ext_highest_incoming_sn);
    assert_eq!(23333u64, r.ext_last_sn);
    let err = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert!(err.is_err());

    // padding only packet with a gap should not report an error
    let params = testutils::TestExtPacketParams {
        sequence_number: 23335,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::Gap,
        ext_sequence_number: 23334,
        ext_timestamp: 0xabcdef,
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Ok(()), err);
    assert_eq!(tp_expected, tp);
    assert_eq!(23335u64, r.ext_highest_incoming_sn);
    assert_eq!(23334u64, r.ext_last_sn);
    let sn_offset = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert_eq!(Ok(1u64), sn_offset);
}

#[test]
fn test_gap_in_sequence_number() {
    let mut r = new_rtp_munger();

    let params = testutils::TestExtPacketParams {
        sequence_number: 65533,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 33,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();
    r.set_last_sn_ts(&ext_pkt);

    let (_, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Ok(()), err);

    // three lost packets
    let params = testutils::TestExtPacketParams {
        sequence_number: 1,
        sn_cycles: 1,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 33,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::Gap,
        ext_sequence_number: 65536 + 1,
        ext_timestamp: 0xabcdef,
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Ok(()), err);
    assert_eq!(tp_expected, tp);
    assert_eq!(65536u64 + 1, r.ext_highest_incoming_sn);
    assert_eq!(65536u64 + 1, r.ext_last_sn);
    let sn_offset = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert_eq!(Ok(0u64), sn_offset);

    // ensure missing sequence numbers have correct cached offset
    let mut i = 65534u64;
    while i != 65536 + 1 {
        let offset = r.sn_range_map.get_value(i);
        assert_eq!(Ok(0u64), offset);
        i += 1;
    }

    // a padding only packet should be dropped
    let params = testutils::TestExtPacketParams {
        sequence_number: 2,
        sn_cycles: 1,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::Contiguous,
        ..Default::default()
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Err(Error::PaddingOnlyPacket), err);
    assert_eq!(tp_expected, tp);
    assert_eq!(65536u64 + 2, r.ext_highest_incoming_sn);
    assert_eq!(65536u64 + 1, r.ext_last_sn);
    let err = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert!(err.is_err());

    // a packet with a gap should be adjusting for dropped padding packet
    let params = testutils::TestExtPacketParams {
        sequence_number: 4,
        sn_cycles: 1,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 22,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::Gap,
        ext_sequence_number: 65536 + 3,
        ext_timestamp: 0xabcdef,
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Ok(()), err);
    assert_eq!(tp_expected, tp);
    assert_eq!(65536u64 + 4, r.ext_highest_incoming_sn);
    assert_eq!(65536u64 + 3, r.ext_last_sn);
    let sn_offset = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert_eq!(Ok(1u64), sn_offset);

    // ensure missing sequence number has correct cached offset
    let offset = r.sn_range_map.get_value(65536 + 3);
    assert_eq!(Ok(1u64), offset);

    // another contiguous padding only packet should be dropped
    let params = testutils::TestExtPacketParams {
        sequence_number: 5,
        sn_cycles: 1,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::Contiguous,
        ..Default::default()
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Err(Error::PaddingOnlyPacket), err);
    assert_eq!(tp_expected, tp);
    assert_eq!(65536u64 + 5, r.ext_highest_incoming_sn);
    assert_eq!(65536u64 + 3, r.ext_last_sn);
    let err = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert!(err.is_err());

    // a packet with a gap should be adjusting for dropped packets
    let params = testutils::TestExtPacketParams {
        sequence_number: 7,
        sn_cycles: 1,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 22,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::Gap,
        ext_sequence_number: 65536 + 5,
        ext_timestamp: 0xabcdef,
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Ok(()), err);
    assert_eq!(tp_expected, tp);
    assert_eq!(65536u64 + 7, r.ext_highest_incoming_sn);
    assert_eq!(65536u64 + 5, r.ext_last_sn);
    let sn_offset = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert_eq!(Ok(2u64), sn_offset);

    // ensure missing sequence number has correct cached offset
    let offset = r.sn_range_map.get_value(65536 + 3);
    assert_eq!(Ok(1u64), offset);

    let offset = r.sn_range_map.get_value(65536 + 6);
    assert_eq!(Ok(2u64), offset);

    // check the missing packets
    let params = testutils::TestExtPacketParams {
        sequence_number: 6,
        sn_cycles: 1,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::OutOfOrder,
        ext_sequence_number: 65536 + 4,
        ext_timestamp: 0xabcdef,
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Ok(()), err);
    assert_eq!(tp_expected, tp);
    assert_eq!(65536u64 + 7, r.ext_highest_incoming_sn);
    assert_eq!(65536u64 + 5, r.ext_last_sn);
    let sn_offset = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert_eq!(Ok(2u64), sn_offset);

    let params = testutils::TestExtPacketParams {
        sequence_number: 3,
        sn_cycles: 1,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    let tp_expected = TranslationParamsRtp {
        sn_ordering: SequenceNumberOrdering::OutOfOrder,
        ext_sequence_number: 65536 + 2,
        ext_timestamp: 0xabcdef,
    };

    let (tp, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Ok(()), err);
    assert_eq!(tp_expected, tp);
    assert_eq!(65536u64 + 7, r.ext_highest_incoming_sn);
    assert_eq!(65536u64 + 5, r.ext_last_sn);
    let sn_offset = r.sn_range_map.get_value(r.ext_highest_incoming_sn);
    assert_eq!(Ok(2u64), sn_offset);
}

#[test]
#[allow(clippy::manual_div_ceil)] // the rounding as the original has it
fn test_update_and_get_padding_sn_ts() {
    let mut r = new_rtp_munger();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();
    r.set_last_sn_ts(&ext_pkt);

    // getting padding without forcing marker should fail
    let err = r.update_and_get_padding_sn_ts(10, 10, 5, false, 0);
    assert!(err.is_err());
    assert_eq!(Err(Error::PaddingNotOnFrameBoundary), err);

    // forcing a marker should not error out.
    // And timestamp on first padding should be the same as the last one.
    let num_padding: usize = 10;
    let clock_rate = 10u64;
    let frame_rate = 5u64;
    let mut snts_expected = vec![SnTs::default(); num_padding];
    for (i, snts) in snts_expected.iter_mut().enumerate() {
        *snts = SnTs {
            ext_sequence_number: params.sequence_number as u64 + i as u64 + 1,
            ext_timestamp: params.timestamp as u64
                + ((i as u64 * clock_rate) + frame_rate - 1) / frame_rate,
        };
    }
    let snts = r.update_and_get_padding_sn_ts(
        num_padding,
        clock_rate as u32,
        frame_rate as u32,
        true,
        ext_pkt.ext_timestamp,
    );
    assert!(snts.is_ok());
    let snts = snts.unwrap();
    assert_eq!(snts_expected, snts);

    // now that there is a marker, timestamp should jump on first padding when asked again
    for (i, expected) in snts_expected.iter_mut().enumerate() {
        *expected = SnTs {
            ext_sequence_number: params.sequence_number as u64 + snts.len() as u64 + i as u64 + 1,
            ext_timestamp: snts[snts.len() - 1].ext_timestamp
                + (((i + 1) as u64 * clock_rate) + frame_rate - 1) / frame_rate,
        };
    }
    let snts = r.update_and_get_padding_sn_ts(
        num_padding,
        clock_rate as u32,
        frame_rate as u32,
        false,
        snts[snts.len() - 1].ext_timestamp,
    );
    assert!(snts.is_ok());
    let snts = snts.unwrap();
    assert_eq!(snts_expected, snts);
}

#[test]
fn test_is_on_frame_boundary() {
    let mut r = new_rtp_munger();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();
    r.set_last_sn_ts(&ext_pkt);

    // send it through
    let (_, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Ok(()), err);
    assert!(!r.is_on_frame_boundary());

    // packet with RTP marker
    let params = testutils::TestExtPacketParams {
        marker: true,
        sequence_number: 23334,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet(&params).unwrap();

    // send it through
    let (_, err) = r.update_and_get_sn_ts(&ext_pkt, ext_pkt.packet.header.marker);
    assert_eq!(Ok(()), err);
    assert!(r.is_on_frame_boundary());
}
