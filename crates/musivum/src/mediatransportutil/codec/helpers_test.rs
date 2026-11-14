// Ported from LiveKit's mediatransportutil, pkg/codec/helpers_test.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The test is about `Vp8` (vp8.rs), as in the original.
// The original has one test; the one at the end is new and says so.

use crate::mediatransportutil::codec::*;

#[test]
fn test_vp8_helper_unmarshal() {
    #[derive(Default)]
    struct Args {
        payload: &'static [u8],
    }
    #[derive(Default)]
    struct Test {
        name: &'static str,
        args: Args,
        want_err: bool,
        check_temporal: bool,
        temporal_support: bool,
        check_key_frame: bool,
        key_frame: bool,
        check_picture_id: bool,
        picture_id: u16,
        check_tlz_idx: bool,
        tlz_idx: u8,
        check_temp_id: bool,
        temporal_id: u8,
    }
    let tests = [
        Test {
            name: "Empty or nil payload must return error",
            args: Args { payload: &[] },
            want_err: true,
            ..Default::default()
        },
        Test {
            name: "Temporal must be supported by setting T bit to 1",
            args: Args {
                payload: &[0xff, 0x20, 0x1, 0x2, 0x3, 0x4],
            },
            check_temporal: true,
            temporal_support: true,
            ..Default::default()
        },
        Test {
            name: "Picture must be ID 7 bits by setting M bit to 0 and present by I bit set to 1",
            args: Args {
                payload: &[0xff, 0xff, 0x11, 0x2, 0x3, 0x4],
            },
            check_picture_id: true,
            picture_id: 17,
            ..Default::default()
        },
        Test {
            name: "Picture ID must be 15 bits by setting M bit to 1 and present by I bit set to 1",
            args: Args {
                payload: &[0xff, 0xff, 0x92, 0x67, 0x3, 0x4, 0x5],
            },
            check_picture_id: true,
            picture_id: 4711,
            ..Default::default()
        },
        Test {
            name: "Temporal level zero index must be present if L set to 1",
            args: Args {
                payload: &[0xff, 0xff, 0xff, 0xfd, 0xb4, 0x4, 0x5],
            },
            check_tlz_idx: true,
            tlz_idx: 180,
            ..Default::default()
        },
        Test {
            name: "Temporal index must be present and used if T bit set to 1",
            args: Args {
                payload: &[0xff, 0xff, 0xff, 0xfd, 0xb4, 0x9f, 0x5, 0x6],
            },
            check_temp_id: true,
            temporal_id: 2,
            ..Default::default()
        },
        Test {
            name: "Check if packet is a keyframe by looking at P bit set to 0",
            args: Args {
                payload: &[0xff, 0xff, 0xff, 0xfd, 0xb4, 0x9f, 0x94, 0x1],
            },
            check_key_frame: true,
            key_frame: true,
            ..Default::default()
        },
    ];
    for tt in tests {
        let mut p = Vp8::default();
        let err = p.unmarshal(tt.args.payload);
        assert_eq!(
            tt.want_err,
            err.is_err(),
            "{}: Unmarshal() error = {:?}, wantErr {}",
            tt.name,
            err,
            tt.want_err
        );
        if tt.check_temporal {
            assert_eq!(tt.temporal_support, p.t, "{}", tt.name);
        }
        if tt.check_key_frame {
            assert_eq!(tt.key_frame, p.is_key_frame, "{}", tt.name);
        }
        if tt.check_picture_id {
            assert_eq!(tt.picture_id, p.picture_id, "{}", tt.name);
        }
        if tt.check_tlz_idx {
            assert_eq!(tt.tlz_idx, p.tl0picidx, "{}", tt.name);
        }
        if tt.check_temp_id {
            assert_eq!(tt.temporal_id, p.tid, "{}", tt.name);
        }
    }
}

// ------------------------------------------

// New for musivum: the other function of helpers.go, which the original does not test.
#[test]
fn vpx_picture_id_size_diff_is_the_change_in_bytes() {
    assert_eq!(0, vpx_picture_id_size_diff(false, false));
    assert_eq!(0, vpx_picture_id_size_diff(true, true));
    assert_eq!(1, vpx_picture_id_size_diff(true, false));
    assert_eq!(-1, vpx_picture_id_size_diff(false, true));
}
