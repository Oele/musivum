// Ported from LiveKit, pkg/sfu/sequencer_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks that carry their names in the
// assertion messages, or loops over the table; the tests sleep on the runtime.

use super::*;
use crate::protocol::logger;

#[tokio::test(flavor = "multi_thread")]
async fn test_sequencer() {
    let seq = Sequencer::new(500, false, logger::get_logger()).unwrap();
    let off = 15u16;

    for i in 1u64..518 {
        seq.push(
            time::now().unix_nano(),
            i,
            i + off as u64,
            123,
            true,
            2,
            &[],
            0,
            &[],
            &[],
            0,
        );
    }
    // send the last two out-of-order
    seq.push(
        time::now().unix_nano(),
        519,
        519 + off as u64,
        123,
        false,
        2,
        &[],
        0,
        &[],
        &[],
        0,
    );
    seq.push(
        time::now().unix_nano(),
        518,
        518 + off as u64,
        123,
        true,
        2,
        &[],
        0,
        &[],
        &[],
        0,
    );

    let req: Vec<u16> = vec![57, 58, 62, 63, 513, 514, 515, 516, 517];
    let res = seq.get_ext_packet_metas(&req);
    // nothing should be returned as not enough time has elapsed since sending packet
    assert_eq!(0, res.len());

    time::sleep((IGNORE_RETRANSMISSION + 10) * time::MILLISECOND).await;
    let res = seq.get_ext_packet_metas(&req);
    assert_eq!(req.len(), res.len());
    for (i, val) in res.iter().enumerate() {
        assert_eq!(val.packet_meta.target_seq_no, req[i]);
        assert_eq!(val.packet_meta.source_seq_no, (req[i] - off) as u64);
        assert_eq!(val.packet_meta.layer, 2i8);
        assert_eq!(val.ext_sequence_number, req[i] as u64);
        assert_eq!(val.ext_timestamp, 123u64);
    }
    let res = seq.get_ext_packet_metas(&req);
    assert_eq!(0, res.len());
    time::sleep((IGNORE_RETRANSMISSION + 10) * time::MILLISECOND).await;
    let res = seq.get_ext_packet_metas(&req);
    assert_eq!(req.len(), res.len());
    for (i, val) in res.iter().enumerate() {
        assert_eq!(val.packet_meta.target_seq_no, req[i]);
        assert_eq!(val.packet_meta.source_seq_no, (req[i] - off) as u64);
        assert_eq!(val.packet_meta.layer, 2i8);
        assert_eq!(val.ext_sequence_number, req[i] as u64);
        assert_eq!(val.ext_timestamp, 123u64);
    }

    seq.push(
        time::now().unix_nano(),
        521,
        521 + off as u64,
        123,
        true,
        1,
        &[],
        0,
        &[],
        &[],
        0,
    );
    let m = seq.get_ext_packet_metas(&[521 + off]);
    assert_eq!(0, m.len());
    time::sleep((IGNORE_RETRANSMISSION + 10) * time::MILLISECOND).await;
    let m = seq.get_ext_packet_metas(&[521 + off]);
    assert_eq!(1, m.len());

    seq.push(
        time::now().unix_nano(),
        505,
        505 + off as u64,
        123,
        false,
        1,
        &[],
        0,
        &[],
        &[],
        0,
    );
    let m = seq.get_ext_packet_metas(&[505 + off]);
    assert_eq!(0, m.len());
    time::sleep((IGNORE_RETRANSMISSION + 10) * time::MILLISECOND).await;
    let m = seq.get_ext_packet_metas(&[505 + off]);
    assert_eq!(1, m.len());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sequencer_unmark_nacked() {
    async fn new_pushed(n: u64) -> Sequencer {
        let seq = Sequencer::new(100, false, logger::get_logger()).unwrap();
        for i in 1u64..=n {
            seq.push(
                time::now().unix_nano(),
                i,
                i,
                123,
                true,
                0,
                &[],
                0,
                &[],
                &[],
                0,
            );
        }
        time::sleep((IGNORE_RETRANSMISSION + 10) * time::MILLISECOND).await;
        seq
    }

    {
        let name =
            "an unmarked packet is served again past maxAck, but not inside the duplicate window";
        let seq = new_pushed(50).await;
        for i in 0..MAX_ACK + 2 {
            let res = seq.get_ext_packet_metas(&[40]);
            assert_eq!(1, res.len(), "{name}: NACK {i}");
            seq.unmark_nacked(&res);

            assert!(
                seq.get_ext_packet_metas(&[40]).is_empty(),
                "{name}: a repeat NACK inside the window is ignored"
            );
            time::sleep((IGNORE_RETRANSMISSION + 10) * time::MILLISECOND).await;
        }
    }

    {
        let name = "a reused slot is left alone";
        let seq = new_pushed(50).await;
        let res = seq.get_ext_packet_metas(&[40]);
        assert_eq!(1, res.len(), "{name}");

        // wrap the ring so slot of 40 holds a newer packet that was NACKed once
        for i in 51u64..=140 {
            seq.push(
                time::now().unix_nano(),
                i,
                i,
                123,
                true,
                0,
                &[],
                0,
                &[],
                &[],
                0,
            );
        }
        time::sleep((IGNORE_RETRANSMISSION + 10) * time::MILLISECOND).await;
        assert_eq!(1, seq.get_ext_packet_metas(&[140]).len(), "{name}");
        assert_eq!(
            140,
            seq.inner.lock().meta[res[0].slot].target_seq_no,
            "{name}"
        );

        seq.unmark_nacked(&res);
        assert_eq!(1, seq.inner.lock().meta[res[0].slot].nacked, "{name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sequencer_flush() {
    let seq = Sequencer::new(500, false, logger::get_logger()).unwrap();
    let off = 15u16;

    for i in 1u64..100 {
        seq.push(
            time::now().unix_nano(),
            i,
            i + off as u64,
            123,
            true,
            2,
            &[],
            0,
            &[],
            &[],
            0,
        );
    }
    let pre_flush: Vec<u16> = vec![57 + off, 58 + off];

    // flush discards all recorded metadata on a stream restart
    seq.flush();

    // even after enough time elapses, a NACK for a pre-flush packet retransmits nothing
    time::sleep((IGNORE_RETRANSMISSION + 10) * time::MILLISECOND).await;
    assert_eq!(0, seq.get_ext_packet_metas(&pre_flush).len());

    // the sequencer re-initializes on the next push and works normally for new packets
    for i in 200u64..210 {
        seq.push(
            time::now().unix_nano(),
            i,
            i + off as u64,
            456,
            true,
            3,
            &[],
            0,
            &[],
            &[],
            0,
        );
    }
    let post_flush: Vec<u16> = vec![205 + off];
    assert_eq!(0, seq.get_ext_packet_metas(&post_flush).len()); // not enough time elapsed yet
    time::sleep((IGNORE_RETRANSMISSION + 10) * time::MILLISECOND).await;
    let res = seq.get_ext_packet_metas(&post_flush);
    assert_eq!(1, res.len());
    assert_eq!(205 + off, res[0].packet_meta.target_seq_no);
    assert_eq!(205u64, res[0].packet_meta.source_seq_no);
    assert_eq!(3i8, res[0].packet_meta.layer);

    // pre-flush packets remain non-retransmittable
    assert_eq!(0, seq.get_ext_packet_metas(&pre_flush).len());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sequencer_get_nack_seq_no_exclusion() {
    struct Args {
        seq_no: Vec<u16>,
    }
    struct Input {
        seq_no: u64,
        is_padding: bool,
    }
    struct Fields {
        inputs: Vec<Input>,
        offset: u64,
        marker_odd: bool,
        marker_even: bool,
        codec_bytes_odd: Vec<u8>,
        num_codec_bytes_in_odd: usize,
        codec_bytes_even: Vec<u8>,
        num_codec_bytes_in_even: usize,
        codec_bytes_oversized: Vec<u8>,
        dd_bytes_odd: Vec<u8>,
        dd_bytes_even: Vec<u8>,
        dd_bytes_oversized: Vec<u8>,
        act_bytes_odd: Vec<u8>,
        act_bytes_even: Vec<u8>,
    }
    struct Test {
        name: &'static str,
        fields: Fields,
        args: Args,
        want: Vec<u16>,
    }
    fn input(seq_no: u64, is_padding: bool) -> Input {
        Input { seq_no, is_padding }
    }

    let tests = [Test {
        name: "Should get correct seq numbers",
        fields: Fields {
            inputs: vec![
                input(65526, false),
                input(65524, false),
                input(65525, false),
                input(65529, false),
                input(65530, false),
                input(65531, true),
                input(65533, false),
                input(65532, true),
                input(65534, false),
            ],
            offset: 5,
            marker_odd: true,
            marker_even: false,
            codec_bytes_odd: vec![1, 2, 3, 4],
            num_codec_bytes_in_odd: 3,
            codec_bytes_even: vec![5, 6, 7],
            num_codec_bytes_in_even: 4,
            codec_bytes_oversized: vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
            dd_bytes_odd: vec![8, 9, 10],
            dd_bytes_even: vec![11, 12],
            dd_bytes_oversized: vec![11, 12, 13, 14, 15, 16, 17, 18, 19],
            act_bytes_odd: vec![0, 1, 2, 3, 4, 5, 6, 7],
            act_bytes_even: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        },
        args: Args {
            seq_no: vec![
                65526 + 5,
                65527 + 5,
                65530 + 5,
                0, /* 65531 input */
                1, /* 65532 input */
                2, /* 65533 input */
                3, /* 65534 input */
            ],
        },
        // although 65526 is originally pushed, that would have been reset by 65532 (padding only packet)
        // because of trying to add an exclusion range before highest sequence number which will fail
        // and the resulting fix up of the exclusion range slots
        want: vec![65530, 65533, 65534],
    }];
    for tt in &tests {
        let n = Sequencer::new(5, true, logger::get_logger()).unwrap();

        for i in &tt.fields.inputs {
            if i.is_padding {
                n.push_padding(i.seq_no + tt.fields.offset, i.seq_no + tt.fields.offset);
            } else if i.seq_no % 5 == 0 {
                n.push(
                    time::now().unix_nano(),
                    i.seq_no,
                    i.seq_no + tt.fields.offset,
                    123,
                    tt.fields.marker_odd,
                    3,
                    &tt.fields.codec_bytes_oversized,
                    tt.fields.codec_bytes_oversized.len(),
                    &tt.fields.dd_bytes_oversized,
                    &tt.fields.act_bytes_odd,
                    0,
                );
            } else if i.seq_no % 2 == 0 {
                n.push(
                    time::now().unix_nano(),
                    i.seq_no,
                    i.seq_no + tt.fields.offset,
                    123,
                    tt.fields.marker_even,
                    3,
                    &tt.fields.codec_bytes_even,
                    tt.fields.num_codec_bytes_in_even,
                    &tt.fields.dd_bytes_even,
                    &tt.fields.act_bytes_even,
                    0,
                );
            } else {
                n.push(
                    time::now().unix_nano(),
                    i.seq_no,
                    i.seq_no + tt.fields.offset,
                    123,
                    tt.fields.marker_odd,
                    3,
                    &tt.fields.codec_bytes_odd,
                    tt.fields.num_codec_bytes_in_odd,
                    &tt.fields.dd_bytes_odd,
                    &tt.fields.act_bytes_odd,
                    0,
                );
            }
        }

        time::sleep((IGNORE_RETRANSMISSION + 10) * time::MILLISECOND).await;
        let g = n.get_ext_packet_metas(&tt.args.seq_no);
        let mut got: Vec<u16> = Vec::new();
        for sn in &g {
            let name = tt.name;
            let sn = &sn.packet_meta;
            got.push(sn.source_seq_no as u16);
            if sn.source_seq_no % 5 == 0 {
                assert_eq!(tt.fields.marker_odd, sn.marker, "{name}");
                assert_eq!(
                    tt.fields.codec_bytes_oversized, sn.codec_bytes_slice,
                    "{name}"
                );
                assert_eq!(
                    tt.fields.codec_bytes_oversized.len() as u8,
                    sn.num_codec_bytes_in,
                    "{name}"
                );
                assert_eq!(tt.fields.dd_bytes_oversized, sn.dd_bytes_slice, "{name}");
                assert_eq!(
                    tt.fields.codec_bytes_oversized.len() as u8,
                    sn.dd_bytes_size,
                    "{name}"
                );
                assert_eq!(
                    tt.fields.act_bytes_odd,
                    sn.act_bytes[..sn.act_bytes_size as usize],
                    "{name}"
                );
            } else if sn.source_seq_no % 2 == 0 {
                assert_eq!(tt.fields.marker_even, sn.marker, "{name}");
                assert_eq!(
                    tt.fields.codec_bytes_even,
                    sn.codec_bytes[..sn.num_codec_bytes_out as usize],
                    "{name}"
                );
                assert_eq!(
                    tt.fields.num_codec_bytes_in_even as u8, sn.num_codec_bytes_in,
                    "{name}"
                );
                assert_eq!(
                    tt.fields.dd_bytes_even,
                    sn.dd_bytes[..sn.dd_bytes_size as usize],
                    "{name}"
                );
                assert_eq!(
                    tt.fields.dd_bytes_even.len() as u8,
                    sn.dd_bytes_size,
                    "{name}"
                );
                assert_eq!(
                    tt.fields.act_bytes_even,
                    sn.act_bytes[..sn.act_bytes_size as usize],
                    "{name}"
                );
            } else {
                assert_eq!(tt.fields.marker_odd, sn.marker, "{name}");
                assert_eq!(
                    tt.fields.codec_bytes_odd,
                    sn.codec_bytes[..sn.num_codec_bytes_out as usize],
                    "{name}"
                );
                assert_eq!(
                    tt.fields.num_codec_bytes_in_odd as u8, sn.num_codec_bytes_in,
                    "{name}"
                );
                assert_eq!(
                    tt.fields.dd_bytes_odd,
                    sn.dd_bytes[..sn.dd_bytes_size as usize],
                    "{name}"
                );
                assert_eq!(
                    tt.fields.dd_bytes_odd.len() as u8,
                    sn.dd_bytes_size,
                    "{name}"
                );
                assert_eq!(
                    tt.fields.act_bytes_odd,
                    sn.act_bytes[..sn.act_bytes_size as usize],
                    "{name}"
                );
            }
        }
        assert_eq!(
            tt.want, got,
            "{}: getExtPacketMetas() = {:?}, want {:?}",
            tt.name, got, tt.want
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sequencer_get_nack_seq_no_no_exclusion() {
    struct Args {
        seq_no: Vec<u16>,
    }
    struct Input {
        seq_no: u64,
        is_padding: bool,
    }
    struct Fields {
        inputs: Vec<Input>,
        offset: u64,
        marker_odd: bool,
        marker_even: bool,
        codec_bytes_odd: Vec<u8>,
        num_codec_bytes_in_odd: usize,
        codec_bytes_even: Vec<u8>,
        num_codec_bytes_in_even: usize,
        dd_bytes_odd: Vec<u8>,
        dd_bytes_even: Vec<u8>,
        act_bytes_odd: Vec<u8>,
        act_bytes_even: Vec<u8>,
    }
    struct Test {
        name: &'static str,
        fields: Fields,
        args: Args,
        want: Vec<u16>,
    }
    fn input(seq_no: u64, is_padding: bool) -> Input {
        Input { seq_no, is_padding }
    }

    let tests = [Test {
        name: "Should get correct seq numbers",
        fields: Fields {
            inputs: vec![
                input(2, false),
                input(3, false),
                input(4, false),
                input(7, false),
                input(8, false),
                input(9, true),
                input(11, false),
                input(10, true),
                input(12, false),
                input(13, false),
            ],
            offset: 5,
            marker_odd: true,
            marker_even: false,
            codec_bytes_odd: vec![1, 2, 3, 4],
            num_codec_bytes_in_odd: 3,
            codec_bytes_even: vec![5, 6, 7],
            num_codec_bytes_in_even: 4,
            dd_bytes_odd: vec![8, 9, 10],
            dd_bytes_even: vec![11, 12],
            act_bytes_odd: vec![8, 9, 10],
            act_bytes_even: vec![11, 12],
        },
        args: Args {
            seq_no: vec![4 + 5, 5 + 5, 8 + 5, 9 + 5, 10 + 5, 11 + 5, 12 + 5],
        },
        // although 4 and 8 were originally added, they would be too old after a cycle of sequencer buffer
        want: vec![11, 12],
    }];
    for tt in &tests {
        let n = Sequencer::new(5, false, logger::get_logger()).unwrap();

        for i in &tt.fields.inputs {
            if i.is_padding {
                n.push_padding(i.seq_no + tt.fields.offset, i.seq_no + tt.fields.offset);
            } else if i.seq_no % 2 == 0 {
                n.push(
                    time::now().unix_nano(),
                    i.seq_no,
                    i.seq_no + tt.fields.offset,
                    123,
                    tt.fields.marker_even,
                    3,
                    &tt.fields.codec_bytes_even,
                    tt.fields.num_codec_bytes_in_even,
                    &tt.fields.dd_bytes_even,
                    &tt.fields.act_bytes_even,
                    0,
                );
            } else {
                n.push(
                    time::now().unix_nano(),
                    i.seq_no,
                    i.seq_no + tt.fields.offset,
                    123,
                    tt.fields.marker_odd,
                    3,
                    &tt.fields.codec_bytes_odd,
                    tt.fields.num_codec_bytes_in_odd,
                    &tt.fields.dd_bytes_odd,
                    &tt.fields.act_bytes_odd,
                    0,
                );
            }
        }

        time::sleep((IGNORE_RETRANSMISSION + 10) * time::MILLISECOND).await;
        let g = n.get_ext_packet_metas(&tt.args.seq_no);
        let mut got: Vec<u16> = Vec::new();
        for sn in &g {
            let name = tt.name;
            let sn = &sn.packet_meta;
            got.push(sn.source_seq_no as u16);
            if sn.source_seq_no % 2 == 0 {
                assert_eq!(tt.fields.marker_even, sn.marker, "{name}");
                assert_eq!(
                    tt.fields.codec_bytes_even,
                    sn.codec_bytes[..sn.num_codec_bytes_out as usize],
                    "{name}"
                );
                assert_eq!(
                    tt.fields.num_codec_bytes_in_even as u8, sn.num_codec_bytes_in,
                    "{name}"
                );
                assert_eq!(
                    tt.fields.dd_bytes_even,
                    sn.dd_bytes[..sn.dd_bytes_size as usize],
                    "{name}"
                );
                assert_eq!(
                    tt.fields.dd_bytes_even.len() as u8,
                    sn.dd_bytes_size,
                    "{name}"
                );
                assert_eq!(
                    tt.fields.act_bytes_even,
                    sn.act_bytes[..sn.act_bytes_size as usize],
                    "{name}"
                );
            } else {
                assert_eq!(tt.fields.marker_odd, sn.marker, "{name}");
                assert_eq!(
                    tt.fields.codec_bytes_odd,
                    sn.codec_bytes[..sn.num_codec_bytes_out as usize],
                    "{name}"
                );
                assert_eq!(
                    tt.fields.num_codec_bytes_in_odd as u8, sn.num_codec_bytes_in,
                    "{name}"
                );
                assert_eq!(
                    tt.fields.dd_bytes_odd,
                    sn.dd_bytes[..sn.dd_bytes_size as usize],
                    "{name}"
                );
                assert_eq!(
                    tt.fields.dd_bytes_odd.len() as u8,
                    sn.dd_bytes_size,
                    "{name}"
                );
                assert_eq!(
                    tt.fields.act_bytes_odd,
                    sn.act_bytes[..sn.act_bytes_size as usize],
                    "{name}"
                );
            }
        }
        assert_eq!(
            tt.want, got,
            "{}: getExtPacketMetas() = {:?}, want {:?}",
            tt.name, got, tt.want
        );
    }
}
