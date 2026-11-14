// Ported from LiveKit,
// pkg/sfu/videolayerselector/temporallayerselector/dependencydescriptor_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks that carry their names in the
// messages of their assertions; the frames of the table are made by the helpers `frame` and
// `key_frame`.

use super::*;
use crate::protocol::logger;
use crate::sfu::testutils;

#[derive(Clone, Debug, Default)]
struct Vp8DdFrame {
    tid: i64,
    dtis: &'static str,
    frame_diffs: Vec<i64>,
    key_frame: bool,
}

fn frame(tid: i64, dtis: &'static str, frame_diffs: &[i64]) -> Vp8DdFrame {
    Vp8DdFrame {
        tid,
        dtis,
        frame_diffs: frame_diffs.to_vec(),
        key_frame: false,
    }
}

fn key_frame() -> Vp8DdFrame {
    Vp8DdFrame {
        tid: 0,
        dtis: "SSS",
        key_frame: true,
        ..Default::default()
    }
}

// libwebrtc's three layer VP8 pattern: T0 refreshes "last", T1 refreshes "golden", and
// frame diffs point at the frames in those buffers
fn vp8_dd_pattern() -> Vec<Vp8DdFrame> {
    vec![
        frame(0, "SSS", &[4]),
        frame(2, "--D", &[1]),
        frame(1, "-SS", &[2]),
        frame(2, "--D", &[1, 3]),
        frame(0, "SRR", &[4]),
        frame(2, "--D", &[1, 3]),
        frame(1, "-DS", &[2, 4]),
        frame(2, "--D", &[1, 3]),
    ]
}

fn vp8_dd_packet(frame_num: u64, f: &Vp8DdFrame, first: bool, marker: bool) -> buffer::ExtPacket {
    testutils::get_test_ext_packet_vp8_dd(
        &testutils::TestExtPacketParams {
            sequence_number: frame_num as u16,
            payload_size: 20,
            marker,
            is_key_frame: f.key_frame,
            ..Default::default()
        },
        &testutils::TestDdParams {
            first_packet_in_frame: first,
            ext_frame_num: frame_num,
            temporal_id: f.tid,
            dtis: f.dtis.to_string(),
            frame_diffs: f.frame_diffs.clone(),
            decode_targets: testutils::TEST_VP8_DECODE_TARGETS.to_vec(),
        },
    )
    .unwrap()
}

// runDependencyDescriptor sends one packet per frame: a key frame, the pattern at the start target until
// frame switchAt, then at target 2. It returns the frame number of the up-switch.
fn run_dependency_descriptor(start_target: i32, switch_at: u64) -> (u64, bool) {
    let mut v = DependencyDescriptor::new(logger::get_logger());
    let pattern = vp8_dd_pattern();
    let mut current = 0i32;
    for frame_num in 0u64..40 {
        let mut f = pattern[(frame_num % pattern.len() as u64) as usize].clone();
        if frame_num == 0 {
            f = key_frame();
        }
        let mut target = start_target;
        if frame_num >= switch_at {
            target = 2;
        }
        let (this, next) = v.select(&vp8_dd_packet(frame_num, &f, true, true), current, target);
        if frame_num >= switch_at && next == 2 {
            assert_eq!(2i32, this);
            return (frame_num, true);
        }
        current = next;
    }
    (0, false)
}

#[test]
fn test_dependency_descriptor_select() {
    {
        let name = "from TL0, up-switch at the next T0 switch point";
        // frame 14 is the "-DS" T1 frame, it references T1 frame 10 that was dropped
        let (frame_num, ok) = run_dependency_descriptor(0, 12);
        assert!(ok, "{name}");
        assert_eq!(16u64, frame_num, "{name}");

        // frame 10 is the "-SS" T1 frame, it references only T0
        let (frame_num, ok) = run_dependency_descriptor(0, 10);
        assert!(ok, "{name}");
        assert_eq!(10u64, frame_num, "{name}");
    }

    {
        let name = "from TL1, the -DS frame is a switch point";
        let (frame_num, ok) = run_dependency_descriptor(1, 14);
        assert!(ok, "{name}");
        assert_eq!(14u64, frame_num, "{name}");
    }

    {
        let name = "switch point that references a dropped frame";
        // libwebrtc L1T4: a T2 frame that is a switch point for TL2 but references the
        // previous T2 frame
        let mut v = DependencyDescriptor::new(logger::get_logger());
        let (_, current) = v.select(&vp8_dd_packet(0, &key_frame(), true, true), 1, 1);
        let (_, current) = v.select(
            &vp8_dd_packet(1, &frame(2, "--D", &[1]), true, true),
            current,
            1,
        );
        let (this, next) = v.select(
            &vp8_dd_packet(2, &frame(2, "--S", &[1]), true, true),
            current,
            2,
        );
        assert_eq!(1i32, this, "{name}");
        assert_eq!(1i32, next, "{name}");
    }

    {
        let name = "reference lost upstream";
        let mut v = DependencyDescriptor::new(logger::get_logger());
        let (_, current) = v.select(&vp8_dd_packet(0, &key_frame(), true, true), 0, 0);
        // frame 1 never arrives
        let (this, next) = v.select(
            &vp8_dd_packet(2, &frame(1, "-SS", &[1]), true, true),
            current,
            2,
        );
        assert_eq!(0i32, this, "{name}");
        assert_eq!(0i32, next, "{name}");
    }

    {
        let name = "up-switch only on the first packet of a frame, in order";
        let mut v = DependencyDescriptor::new(logger::get_logger());
        let (_, current) = v.select(&vp8_dd_packet(0, &key_frame(), true, true), 0, 0);
        let f = frame(1, "-SS", &[1]);

        let (_, next) = v.select(&vp8_dd_packet(1, &f, false, true), current, 2);
        assert_eq!(0i32, next, "{name}: second packet");

        let mut late = vp8_dd_packet(1, &f, true, true);
        late.is_out_of_order = true;
        let (_, next) = v.select(&late, current, 2);
        assert_eq!(0i32, next, "{name}: late first packet");

        let (this, next) = v.select(&vp8_dd_packet(1, &f, true, true), current, 2);
        assert_eq!(2i32, this, "{name}");
        assert_eq!(2i32, next, "{name}");
    }

    {
        let name = "late key frame packet of a stream restart";
        let mut v = DependencyDescriptor::new(logger::get_logger());
        let (_, current) = v.select(&vp8_dd_packet(100, &key_frame(), true, true), 0, 0);

        // the restart moves the frame numbers back, and its key frame arrives out of order
        let mut key_frame_packet = vp8_dd_packet(5, &key_frame(), true, true);
        key_frame_packet.is_out_of_order = true;
        let (_, current) = v.select(&key_frame_packet, current, 0);

        let (this, next) = v.select(
            &vp8_dd_packet(6, &frame(1, "-SS", &[1]), true, true),
            current,
            2,
        );
        assert_eq!(2i32, this, "{name}");
        assert_eq!(2i32, next, "{name}");
    }

    {
        let name = "key frame";
        let mut v = DependencyDescriptor::new(logger::get_logger());
        let (this, next) = v.select(&vp8_dd_packet(0, &key_frame(), true, false), 0, 2);
        assert_eq!(2i32, this, "{name}");
        assert_eq!(2i32, next, "{name}");
    }

    {
        let name = "no dependency descriptor";
        let mut v = DependencyDescriptor::new(logger::get_logger());
        let mut ext_pkt = vp8_dd_packet(0, &key_frame(), true, true);
        ext_pkt.dependency_descriptor = None;
        let (this, next) = v.select(&ext_pkt, 0, 2);
        assert_eq!(0i32, this, "{name}");
        assert_eq!(0i32, next, "{name}");
    }

    {
        let name = "down-switch at the end of a frame";
        let mut v = DependencyDescriptor::new(logger::get_logger());
        let f = frame(2, "--D", &[1]);
        let (this, next) = v.select(&vp8_dd_packet(1, &f, true, false), 2, 0);
        assert_eq!(2i32, this, "{name}");
        assert_eq!(2i32, next, "{name}");
        let (this, next) = v.select(&vp8_dd_packet(1, &f, false, true), 2, 0);
        assert_eq!(2i32, this, "{name}");
        assert_eq!(0i32, next, "{name}");
    }
}
