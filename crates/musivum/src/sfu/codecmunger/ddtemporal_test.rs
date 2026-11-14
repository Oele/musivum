// Ported from LiveKit, pkg/sfu/codecmunger/ddtemporal_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks of the one test; padding
// that is nil in the original is empty here.

use super::*;
use crate::protocol::logger;
use crate::sfu::testutils;

fn vp8_dd_packet(frame_num: u64, tid: i64) -> buffer::ExtPacket {
    let ext_pkt = testutils::get_test_ext_packet_vp8_dd(
        &testutils::TestExtPacketParams {
            sequence_number: frame_num as u16,
            payload_size: 20,
            ..Default::default()
        },
        &testutils::TestDdParams {
            first_packet_in_frame: true,
            ext_frame_num: frame_num,
            temporal_id: tid,
            decode_targets: testutils::TEST_VP8_DECODE_TARGETS.to_vec(),
            ..Default::default()
        },
    );
    assert!(ext_pkt.is_ok());
    ext_pkt.unwrap()
}

#[test]
fn test_dd_temporal_update_and_get() {
    {
        let name = "filters by temporal layer, header unchanged";
        let mut v = DdTemporal::new(mime::MimeType::Vp8, logger::get_logger());
        let res = v.update_and_get(&vp8_dd_packet(0, 0), false, false, 0);
        assert!(res.is_ok(), "{name}: {res:?}");
        let (input_size, _, n) = res.unwrap();
        assert_eq!(0, input_size, "{name}");
        assert_eq!(0, n, "{name}");

        let res = v.update_and_get(&vp8_dd_packet(1, 2), false, false, 0);
        assert_eq!(Err(Error::FilteredTemporalLayer), res, "{name}");
    }

    {
        let name = "a gap does not exempt a filtered layer";
        let mut v = DdTemporal::new(mime::MimeType::Vp8, logger::get_logger());
        let res = v.update_and_get(&vp8_dd_packet(0, 0), false, false, 0);
        assert!(res.is_ok(), "{name}: {res:?}");
        for frame_num in 2u64..6 {
            let res = v.update_and_get(&vp8_dd_packet(frame_num, 1), false, true, 0);
            assert_eq!(Err(Error::FilteredTemporalLayer), res, "{name}");
        }
    }

    {
        let name = "every packet of a frame gets the decision of its first packet";
        let mut v = DdTemporal::new(mime::MimeType::Vp8, logger::get_logger());
        let res = v.update_and_get(&vp8_dd_packet(1, 2), false, false, 0);
        assert_eq!(Err(Error::FilteredTemporalLayer), res, "{name}");
        let res = v.update_and_get(&vp8_dd_packet(2, 1), false, false, 1);
        assert!(res.is_ok(), "{name}: {res:?}");

        // the layer changed in between, late packets keep the decision of their frame
        let res = v.update_and_get(&vp8_dd_packet(1, 2), true, false, 2);
        assert_eq!(Err(Error::FilteredOutOfOrder), res, "{name}");
        let res = v.update_and_get(&vp8_dd_packet(2, 1), true, false, 0);
        assert!(res.is_ok(), "{name}: {res:?}");
    }

    {
        let name = "late packet of a frame older than the window";
        let mut v = DdTemporal::new(mime::MimeType::Vp8, logger::get_logger());
        let res = v.update_and_get(&vp8_dd_packet(100, 0), false, false, 2);
        assert!(res.is_ok(), "{name}: {res:?}");
        let res = v.update_and_get(
            &vp8_dd_packet(100 - DD_TEMPORAL_FRAME_WINDOW, 0),
            true,
            false,
            2,
        );
        assert_eq!(Err(Error::FilteredOutOfOrder), res, "{name}");
    }

    {
        let name = "layer switch forgets decisions";
        let mut v = DdTemporal::new(mime::MimeType::Vp8, logger::get_logger());
        let res = v.update_and_get(&vp8_dd_packet(1, 2), false, false, 0);
        assert_eq!(Err(Error::FilteredTemporalLayer), res, "{name}");
        v.update_offsets(&vp8_dd_packet(1, 0));
        let res = v.update_and_get(&vp8_dd_packet(1, 2), false, false, 2);
        assert!(res.is_ok(), "{name}: {res:?}");
    }

    {
        let name = "padding";
        let mut v = DdTemporal::new(mime::MimeType::Vp8, logger::get_logger());
        let hdr = v.update_and_get_padding(true);
        assert!(hdr.is_ok(), "{name}: {hdr:?}");
        assert_eq!(vec![0x10u8], hdr.unwrap(), "{name}");

        // other codecs have no payload descriptor to prepend
        for mime_type in [mime::MimeType::Vp9, mime::MimeType::Av1] {
            let hdr = DdTemporal::new(mime_type, logger::get_logger()).update_and_get_padding(true);
            assert!(hdr.is_ok(), "{name}: {hdr:?}");
            // nil in the original
            assert!(hdr.unwrap().is_empty(), "{name}");
        }
    }
}
