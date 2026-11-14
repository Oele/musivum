// Ported from LiveKit, pkg/sfu/codecmunger/vp8_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; where the original checks that the two sizes are 0
// next to an error, the error is all there is to check: a `Result` has no sizes then.

use super::*;
use crate::protocol::logger;
use crate::sfu::testutils;

fn compare(expected: &Vp8, actual: &Vp8) -> bool {
    expected.picture_id_wrap_handler == actual.picture_id_wrap_handler
        && expected.ext_last_picture_id == actual.ext_last_picture_id
        && expected.picture_id_offset == actual.picture_id_offset
        && expected.picture_id_used == actual.picture_id_used
        && expected.last_tl0_pic_idx == actual.last_tl0_pic_idx
        && expected.tl0_pic_idx_offset == actual.tl0_pic_idx_offset
        && expected.tl0_pic_idx_used == actual.tl0_pic_idx_used
        && expected.tid_used == actual.tid_used
        && expected.last_key_idx == actual.last_key_idx
        && expected.key_idx_offset == actual.key_idx_offset
        && expected.key_idx_used == actual.key_idx_used
}

fn new_vp8() -> Vp8 {
    Vp8::new(logger::get_logger())
}

#[test]
fn test_set_last() {
    let mut v = new_vp8();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 13,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8);
    assert!(ext_pkt.is_ok());
    let ext_pkt = ext_pkt.unwrap();

    let expected_vp8 = Vp8 {
        picture_id_wrap_handler: Vp8PictureIdWrapHandler {
            max_picture_id: 13466,
            max_m_bit: true,
            total_wrap: 0,
            last_wrap: 0,
        },
        ext_last_picture_id: 13467,
        picture_id_offset: 0,
        picture_id_used: true,
        last_tl0_pic_idx: 233,
        tl0_pic_idx_offset: 0,
        tl0_pic_idx_used: true,
        tid_used: true,
        last_key_idx: 23,
        key_idx_offset: 0,
        key_idx_used: true,
        ..new_vp8()
    };

    v.set_last(&ext_pkt);
    assert!(compare(&expected_vp8, &v));
}

#[test]
fn test_update_offsets() {
    let mut v = new_vp8();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 13,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    v.set_last(&ext_pkt);

    let params = testutils::TestExtPacketParams {
        sequence_number: 56789,
        timestamp: 0xabcdef,
        ssrc: 0x87654321,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 345,
        l: true,
        tl0picidx: 12,
        t: true,
        tid: 13,
        y: true,
        k: true,
        keyidx: 4,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    v.update_offsets(&ext_pkt);

    let expected_vp8 = Vp8 {
        picture_id_wrap_handler: Vp8PictureIdWrapHandler {
            max_picture_id: 344,
            max_m_bit: true,
            total_wrap: 0,
            last_wrap: 0,
        },
        ext_last_picture_id: 13467,
        picture_id_offset: 345 - 13467 - 1,
        picture_id_used: true,
        last_tl0_pic_idx: 233,
        tl0_pic_idx_offset: ((12 - 233 - 1) & 0xff) as u8,
        tl0_pic_idx_used: true,
        tid_used: true,
        last_key_idx: 23,
        key_idx_offset: ((4 - 23 - 1) & 0x1f) as u8,
        key_idx_used: true,
        ..new_vp8()
    };
    assert!(compare(&expected_vp8, &v));
}

#[test]
fn test_out_of_order_picture_id() {
    let mut v = new_vp8();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let mut vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 1,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    v.set_last(&ext_pkt);
    let _ = v.update_and_get(&ext_pkt, false, false, 2);

    // out-of-order sequence number not in the missing picture id cache
    vp8.picture_id = 13466;
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    let res = v.update_and_get(&ext_pkt, true, false, 2);
    assert_eq!(Err(Error::OutOfOrderVp8PictureIdCacheMiss), res);

    // create a hole in picture id
    vp8.picture_id = 13469;
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    let expected_vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13469,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 1,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    let marshalled_vp8 = marshalled_vp8.unwrap();
    let res = v.update_and_get(&ext_pkt, false, true, 2);
    assert!(res.is_ok(), "{res:?}");
    let (n_in, hdr, n_out) = res.unwrap();
    assert_eq!(6, n_in);
    assert_eq!(marshalled_vp8.as_slice(), &hdr[..n_out]);

    // all three, the last, the current and the in-between should have been added to missing picture id cache
    let (value, ok) = v.picture_id_offset(13467);
    assert!(ok);
    assert_eq!(0, value);

    let (value, ok) = v.picture_id_offset(13468);
    assert!(ok);
    assert_eq!(0, value);

    let (value, ok) = v.picture_id_offset(13469);
    assert!(ok);
    assert_eq!(0, value);

    // out-of-order sequence number should be in the missing picture id cache
    vp8.picture_id = 13468;
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    let expected_vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13468,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 1,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    let marshalled_vp8 = marshalled_vp8.unwrap();
    let res = v.update_and_get(&ext_pkt, true, false, 2);
    assert!(res.is_ok(), "{res:?}");
    let (n_in, hdr, n_out) = res.unwrap();
    assert_eq!(6, n_in);
    assert_eq!(marshalled_vp8.as_slice(), &hdr[..n_out]);
}

#[test]
fn test_temporal_layer_filtering() {
    let mut v = new_vp8();

    let mut params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 1,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    v.set_last(&ext_pkt);

    // translate
    let res = v.update_and_get(&ext_pkt, false, false, 0);
    assert_eq!(Err(Error::FilteredTemporalLayer), res);
    let (dropped, _) = v.dropped_picture_ids.get(13467);
    assert!(dropped);
    assert_eq!(1, v.picture_id_offset);

    // another packet with the same picture id.
    // It should be dropped, but offset should not be updated.
    params.sequence_number = 23334;
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    let res = v.update_and_get(&ext_pkt, false, false, 0);
    assert_eq!(Err(Error::FilteredTemporalLayer), res);
    let (dropped, _) = v.dropped_picture_ids.get(13467);
    assert!(dropped);
    assert_eq!(1, v.picture_id_offset);

    // another packet with the same picture id, but a gap in sequence number.
    // It should be dropped, but offset should not be updated.
    params.sequence_number = 23337;
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    let res = v.update_and_get(&ext_pkt, false, false, 0);
    assert_eq!(Err(Error::FilteredTemporalLayer), res);
    let (dropped, _) = v.dropped_picture_ids.get(13467);
    assert!(dropped);
    assert_eq!(1, v.picture_id_offset);
}

#[test]
fn test_gap_in_sequence_number_same_picture() {
    let mut v = new_vp8();

    let params = testutils::TestExtPacketParams {
        sequence_number: 65533,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 33,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 1,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();
    v.set_last(&ext_pkt);

    let expected_vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 1,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    let marshalled_vp8 = marshalled_vp8.unwrap();
    let res = v.update_and_get(&ext_pkt, false, false, 2);
    assert!(res.is_ok(), "{res:?}");
    let (n_in, hdr, n_out) = res.unwrap();
    assert_eq!(6, n_in);
    assert_eq!(marshalled_vp8.as_slice(), &hdr[..n_out]);

    // telling there is a gap in sequence number will add pictures to missing picture cache
    let expected_vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 1,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    let marshalled_vp8 = marshalled_vp8.unwrap();
    let res = v.update_and_get(&ext_pkt, false, true, 2);
    assert!(res.is_ok(), "{res:?}");
    let (n_in, hdr, n_out) = res.unwrap();
    assert_eq!(6, n_in);
    assert_eq!(marshalled_vp8.as_slice(), &hdr[..n_out]);

    let (value, ok) = v.picture_id_offset(13467);
    assert!(ok);
    assert_eq!(0, value);
}

#[test]
fn test_update_and_get_padding() {
    let mut v = new_vp8();

    let params = testutils::TestExtPacketParams {
        sequence_number: 23333,
        timestamp: 0xabcdef,
        ssrc: 0x12345678,
        payload_size: 20,
        ..Default::default()
    };
    let vp8 = codec::Vp8 {
        first_byte: 25,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 13,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let ext_pkt = testutils::get_test_ext_packet_vp8(&params, &vp8).unwrap();

    v.set_last(&ext_pkt);

    // getting padding with repeat of last picture
    let buf = v.update_and_get_padding(false);
    assert!(buf.is_ok(), "{buf:?}");
    let buf = buf.unwrap();
    let expected_vp8 = codec::Vp8 {
        first_byte: 16,
        i: true,
        m: true,
        picture_id: 13467,
        l: true,
        tl0picidx: 233,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 23,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    let marshalled_vp8 = marshalled_vp8.unwrap();
    assert_eq!(marshalled_vp8, buf);

    // getting padding with new picture
    let buf = v.update_and_get_padding(true);
    assert!(buf.is_ok(), "{buf:?}");
    let buf = buf.unwrap();
    let expected_vp8 = codec::Vp8 {
        first_byte: 16,
        i: true,
        m: true,
        picture_id: 13468,
        l: true,
        tl0picidx: 234,
        t: true,
        tid: 0,
        y: true,
        k: true,
        keyidx: 24,
        header_size: 6,
        is_key_frame: true,
        ..Default::default()
    };
    let marshalled_vp8 = expected_vp8.marshal();
    assert!(marshalled_vp8.is_ok());
    let marshalled_vp8 = marshalled_vp8.unwrap();
    assert_eq!(marshalled_vp8, buf);
}

#[test]
fn test_vp8_picture_id_wrap_handler() {
    let mut v = Vp8PictureIdWrapHandler::default();

    v.init(109, false);
    assert_eq!(109i32, v.max_picture_id());
    assert!(!v.max_m_bit);

    v.update_max_picture_id(109350, true);
    assert_eq!(109350i32, v.max_picture_id());
    assert!(v.max_m_bit);

    // start with something close to the 15-bit wrap around point
    v.init(32766, true);

    // out-of-order, do not wrap
    let ext_picture_id = v.unwrap(32750, true);
    assert_eq!(32750i32, ext_picture_id);
    assert_eq!(0i32, v.total_wrap);
    assert_eq!(0i32, v.last_wrap);

    // wrap at 15-bits
    let ext_picture_id = v.unwrap(5, false);
    assert_eq!(32773i32, ext_picture_id); // 15-bit wrap at 32768 + 5 = 32773
    assert_eq!(32768i32, v.total_wrap);
    assert_eq!(32768i32, v.last_wrap);

    // set things near 7-bit wrap point
    v.update_max_picture_id(32893, false); // 32768 + 125

    // wrap at 7-bits
    let ext_picture_id = v.unwrap(5, true);
    assert_eq!(32901i32, ext_picture_id); // 15-bit wrap at 32768 + 7-bit wrap at 128 + 5 =  32901
    assert_eq!(32896i32, v.total_wrap); // one 15-bit wrap + one 7-bit wrap
    assert_eq!(128i32, v.last_wrap);

    // a new picture in 7-bit mode much with a gap in between.
    // A big enough gap which would have been treated as out-of-order in 7-bit mode.
    v.update_max_picture_id(32901, false);
    let ext_picture_id = v.unwrap(73, false);
    assert_eq!(32841i32, ext_picture_id); // 15-bit wrap at 32768 + 73 =  32841

    // a new picture in 15-bit mode much with a gap in between.
    // A big enough gap which would have been treated as out-of-order in 7-bit mode.
    v.update_max_picture_id(32901, true);
    v.last_wrap = 32768i32;
    let ext_picture_id = v.unwrap(73, false);
    assert_eq!(32969i32, ext_picture_id); // 15-bit wrap at 32768 + 7-bit wrap at 128 + 73 =  32969
}
