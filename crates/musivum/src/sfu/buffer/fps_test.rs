// Ported from LiveKit, pkg/sfu/buffer/fps_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the cases of a test are a list, run in the order they
// are written in (the original ranges over a map), and `verify_fps` is given the name of the
// case for its messages; a test frame is a value: a frame that the original has twice in a
// list (a duplicate) is a copy, and a lost one, nil there, is `None`; `create_frames` adds a
// frame to its list when it has its frame diffs, where the original appends it first and
// goes on writing through the pointer.

use std::sync::Arc;

use super::super::{ExtDependencyDescriptor, INVALID_LAYER_SPATIAL, VideoLayer};
use super::*;
use crate::mediatransportutil::codec;
use crate::protocol::logger;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::webrtc::rtp;

#[derive(Clone, Debug, Default)]
struct TestFrameInfo {
    header: rtp::Header,
    framenumber: u16,
    spatial: usize,
    temporal: usize,
    frame_diff: Vec<i64>,
}

impl TestFrameInfo {
    fn to_vp8(&self) -> ExtPacket {
        ExtPacket {
            packet: rtp::Packet {
                header: self.header.clone(),
                ..Default::default()
            },
            vp8: Some(codec::Vp8 {
                i: true,
                picture_id: self.framenumber,
                ..Default::default()
            }),
            video_layer: VideoLayer {
                spatial: INVALID_LAYER_SPATIAL,
                temporal: self.temporal as i32,
            },
            ..Default::default()
        }
    }

    fn to_dd(&self) -> ExtPacket {
        ExtPacket {
            packet: rtp::Packet {
                header: self.header.clone(),
                ..Default::default()
            },
            dependency_descriptor: Some(ExtDependencyDescriptor {
                descriptor: Some(dd::DependencyDescriptor {
                    frame_number: self.framenumber,
                    frame_dependencies: Some(Arc::new(dd::FrameDependencyTemplate {
                        frame_diffs: self.frame_diff.clone(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            video_layer: VideoLayer {
                spatial: self.spatial as i32,
                temporal: self.temporal as i32,
            },
            ..Default::default()
        }
    }

    fn to_h26x(&self) -> ExtPacket {
        ExtPacket {
            packet: rtp::Packet {
                header: self.header.clone(),
                ..Default::default()
            },
            video_layer: VideoLayer {
                spatial: INVALID_LAYER_SPATIAL,
                temporal: self.temporal as i32,
            },
            ..Default::default()
        }
    }
}

fn create_frames(
    start_frame_number: u16,
    start_ts: u32,
    mut start_seq: u16,
    total_frames_per_spatial: usize,
    fps: &[Vec<f32>],
    spatial_dependency: bool,
) -> Vec<Vec<TestFrameInfo>> {
    let spatials = fps.len();
    let temporals = fps[0].len();
    let mut frames: Vec<Vec<TestFrameInfo>> = Vec::with_capacity(spatials);
    for _ in 0..spatials {
        frames.push(Vec::with_capacity(total_frames_per_spatial));
    }
    let mut r#fn = start_frame_number;

    let mut next_ts: Vec<Vec<u32>> = vec![Vec::new(); spatials];
    let mut ts_step: Vec<Vec<u32>> = vec![Vec::new(); spatials];
    for i in 0..spatials {
        next_ts[i] = vec![0; temporals];
        ts_step[i] = vec![0; temporals];
        for j in 0..temporals {
            next_ts[i][j] = start_ts;
            ts_step[i][j] = (90000.0 / fps[i][j]) as u32;
        }
    }

    let mut current_ts = vec![0u32; spatials];
    for ts in current_ts.iter_mut() {
        *ts = start_ts;
    }
    for _ in 0..total_frames_per_spatial {
        for s in 0..spatials {
            let mut frame = TestFrameInfo {
                header: rtp::Header {
                    timestamp: current_ts[s],
                    sequence_number: start_seq,
                    ..Default::default()
                },
                framenumber: r#fn,
                spatial: s,
                ..Default::default()
            };
            for t in 0..temporals {
                if current_ts[s] >= next_ts[s][t] {
                    frame.temporal = t;
                    for nt in t..temporals {
                        next_ts[s][nt] = next_ts[s][nt].wrapping_add(ts_step[s][nt]);
                    }
                    break;
                }
            }
            current_ts[s] = current_ts[s].wrapping_add(ts_step[s][temporals - 1]);
            // The original appends the frame to `frames[s]` here. It is added below, when it
            // is complete: the search that comes first passes over the frame itself in the
            // original too (a frame is not earlier than itself).
            r#fn = r#fn.wrapping_add(1);
            start_seq = start_seq.wrapping_add(1);

            for cf in frames[s].iter().rev() {
                if cf.header.timestamp.wrapping_sub(frame.header.timestamp) > 0x80000000 {
                    frame
                        .frame_diff
                        .push(frame.framenumber.wrapping_sub(cf.framenumber) as i64);
                    break;
                }
            }

            if spatial_dependency && frame.spatial > 0 {
                for cf in frames[frame.spatial - 1].iter().rev() {
                    if cf.header.timestamp == frame.header.timestamp {
                        frame
                            .frame_diff
                            .push(frame.framenumber.wrapping_sub(cf.framenumber) as i64);
                        break;
                    }
                }
            }

            frames[s].push(frame);
        }
    }

    frames
}

#[track_caller]
fn verify_fps(name: &str, expect: &[f32], got: &[f32]) {
    assert_eq!(expect.len(), got.len(), "{name}");
    for (e, g) in expect.iter().zip(got) {
        assert!(*g >= *e * 0.9, "{name}: expect {expect:?}, got {got:?}");
        assert!(*g <= *e * 1.1, "{name}: expect {expect:?}, got {got:?}");
    }
}

#[derive(Clone, Debug, Default)]
struct Testcase {
    start_ts: u32,
    start_seq: u16,
    start_frame_number: u16,
    fps: Vec<Vec<f32>>,
    spatial_dependency: bool,
}

#[test]
fn test_fps_vp8() {
    let cases = [
        (
            "normal",
            Testcase {
                start_ts: 12345678,
                start_frame_number: 100,
                fps: vec![
                    vec![5.0, 10.0, 15.0],
                    vec![5.0, 10.0, 15.0],
                    vec![7.5, 15.0, 30.0],
                ],
                ..Default::default()
            },
        ),
        (
            "frame number and timestamp wrap",
            Testcase {
                start_ts: (1u32 << 31) - 10,
                start_frame_number: (1u16 << 15) - 10,
                fps: vec![
                    vec![5.0, 10.0, 15.0],
                    vec![5.0, 10.0, 15.0],
                    vec![7.5, 15.0, 30.0],
                ],
                ..Default::default()
            },
        ),
        (
            "2 temporal layers",
            Testcase {
                start_ts: 12345678,
                start_frame_number: 100,
                fps: vec![vec![7.5, 15.0], vec![7.5, 15.0], vec![15.0, 30.0]],
                ..Default::default()
            },
        ),
    ];

    for (name, c) in &cases {
        let fps = &c.fps;
        let mut frames: Vec<Vec<TestFrameInfo>> = Vec::new();
        let mut vp8calcs: Vec<FrameRateCalculatorVp8> = Vec::with_capacity(fps.len());
        for fps_of_layer in fps {
            vp8calcs.push(FrameRateCalculatorVp8::new(90000, logger::get_logger()));
            frames.push(
                create_frames(
                    c.start_frame_number,
                    c.start_ts,
                    10,
                    200,
                    std::slice::from_ref(fps_of_layer),
                    false,
                )
                .swap_remove(0),
            );
        }

        let mut framerates_got = false;
        for (s, fs) in frames.iter().enumerate() {
            for f in fs {
                if vp8calcs[s].recv_packet(&f.to_vp8()) {
                    framerates_got = true;
                    for calc in &vp8calcs {
                        if !calc.completed() {
                            framerates_got = false;
                            break;
                        }
                    }
                }
            }
        }
        assert!(framerates_got, "{name}");
        for (i, calc) in vp8calcs.iter().enumerate() {
            let fps_expected = &fps[i];
            let fps_got = calc.get_frame_rate();
            verify_fps(name, fps_expected, &fps_got[..fps_expected.len()]);
        }
    }
    {
        let name = "packet lost and duplicate";
        let fps: Vec<Vec<f32>> = vec![vec![7.5, 15.0], vec![7.5, 15.0], vec![15.0, 30.0]];
        let mut frames: Vec<Vec<TestFrameInfo>> = Vec::new();
        let mut vp8calcs: Vec<FrameRateCalculatorVp8> = Vec::with_capacity(fps.len());
        for i in 0..fps.len() {
            vp8calcs.push(FrameRateCalculatorVp8::new(90000, logger::get_logger()));
            frames.push(
                create_frames(100, 12345678, 10, 300, std::slice::from_ref(&fps[i]), false)
                    .swap_remove(0),
            );
            for j in 5..130 {
                if j % 2 == 0 {
                    frames[i][j] = frames[i][j - 1].clone();
                }
            }
        }

        let mut framerates_got = false;
        for (s, fs) in frames.iter().enumerate() {
            for f in fs {
                if vp8calcs[s].recv_packet(&f.to_vp8()) {
                    framerates_got = true;
                    for calc in &vp8calcs {
                        if !calc.completed() {
                            framerates_got = false;
                            break;
                        }
                    }
                }
            }
        }
        assert!(framerates_got, "{name}");
        for (i, calc) in vp8calcs.iter().enumerate() {
            let fps_expected = &fps[i];
            let fps_got = calc.get_frame_rate();
            verify_fps(name, fps_expected, &fps_got[..fps_expected.len()]);
        }
    }
}

#[test]
#[allow(clippy::needless_range_loop)] // the loops as the original has them
fn test_fps_dd() {
    let cases = [
        (
            "normal",
            Testcase {
                start_ts: 12345678,
                start_frame_number: 100,
                fps: vec![
                    vec![5.1, 10.1, 16.0],
                    vec![5.1, 10.1, 16.0],
                    vec![8.0, 15.0, 30.1],
                ],
                spatial_dependency: true,
                ..Default::default()
            },
        ),
        (
            "frame number and timestamp wrap",
            Testcase {
                start_ts: (1u32 << 31) - 10,
                start_frame_number: (1u16 << 15) - 10,
                fps: vec![
                    vec![7.5, 15.0, 30.0],
                    vec![7.5, 15.0, 30.0],
                    vec![7.5, 15.0, 30.0],
                ],
                spatial_dependency: true,
                ..Default::default()
            },
        ),
        (
            "vp8",
            Testcase {
                start_ts: 12345678,
                start_frame_number: 100,
                fps: vec![vec![7.5, 15.0], vec![7.5, 15.0], vec![15.0, 30.0]],
                spatial_dependency: false,
                ..Default::default()
            },
        ),
    ];

    for (name, c) in &cases {
        let fps = &c.fps;
        let frames = create_frames(
            c.start_frame_number,
            c.start_ts,
            10,
            500,
            fps,
            c.spatial_dependency,
        );
        let ddcalc = FrameRateCalculatorDd::new(90000, logger::get_logger());
        ddcalc.set_max_layer((fps.len() - 1) as i32, (fps[0].len() - 1) as i32);
        let mut ddcalcs: Vec<Box<dyn FrameRateCalculator>> = Vec::with_capacity(fps.len());
        for i in 0..fps.len() {
            ddcalcs.push(Box::new(
                ddcalc.get_frame_rate_calculator_for_spatial(i as i32),
            ));
        }

        let mut framerates_got = false;
        for (s, fs) in frames.iter().enumerate() {
            for f in fs {
                if ddcalcs[s].recv_packet(&f.to_dd()) {
                    framerates_got = true;
                    for calc in &ddcalcs {
                        if !calc.completed() {
                            framerates_got = false;
                            break;
                        }
                    }
                }
            }
        }
        assert!(framerates_got, "{name}");
        for (i, calc) in ddcalcs.iter().enumerate() {
            let fps_expected = &fps[i];
            let fps_got = calc.get_frame_rate();
            verify_fps(name, fps_expected, &fps_got[..fps_expected.len()]);
        }
    }

    {
        let name = "packet lost and duplicate";
        let fps: Vec<Vec<f32>> = vec![
            vec![7.5, 15.0, 30.0],
            vec![7.5, 15.0, 30.0],
            vec![7.5, 15.0, 30.0],
        ];
        let mut frames = create_frames(100, 12345678, 10, 500, &fps, true);
        let ddcalc = FrameRateCalculatorDd::new(90000, logger::get_logger());
        ddcalc.set_max_layer((fps.len() - 1) as i32, (fps[0].len() - 1) as i32);
        let mut ddcalcs: Vec<Box<dyn FrameRateCalculator>> = Vec::with_capacity(fps.len());
        for i in 0..fps.len() {
            ddcalcs.push(Box::new(
                ddcalc.get_frame_rate_calculator_for_spatial(i as i32),
            ));
            for j in 5..130 {
                if j % 2 == 0 {
                    frames[i][j] = frames[i][j - 1].clone();
                }
            }
        }

        let mut framerates_got = false;
        for (s, fs) in frames.iter().enumerate() {
            for f in fs {
                if ddcalcs[s].recv_packet(&f.to_dd()) {
                    framerates_got = true;
                    for calc in &ddcalcs {
                        if !calc.completed() {
                            framerates_got = false;
                            break;
                        }
                    }
                }
            }
        }
        assert!(framerates_got, "{name}");
        for (i, calc) in ddcalcs.iter().enumerate() {
            let fps_expected = &fps[i];
            let fps_got = calc.get_frame_rate();
            verify_fps(name, fps_expected, &fps_got[..fps_expected.len()]);
        }
    }
}

#[test]
#[allow(clippy::needless_range_loop)] // the loops as the original has them
fn test_fps_h26x() {
    let cases = [
        (
            "normal",
            Testcase {
                start_ts: 12345678,
                start_seq: 100,
                start_frame_number: 100,
                fps: vec![
                    vec![5.0, 10.0, 15.0],
                    vec![5.0, 10.0, 15.0],
                    vec![7.5, 15.0, 30.0],
                ],
                ..Default::default()
            },
        ),
        (
            "frame number and timestamp wrap",
            Testcase {
                start_ts: (1u32 << 31) - 10,
                start_seq: (1u16 << 15) - 10,
                start_frame_number: (1u16 << 15) - 10,
                fps: vec![
                    vec![5.0, 10.0, 15.0],
                    vec![5.0, 10.0, 15.0],
                    vec![7.5, 15.0, 30.0],
                ],
                ..Default::default()
            },
        ),
        (
            "2 temporal layers",
            Testcase {
                start_ts: 12345678,
                start_frame_number: 100,
                fps: vec![vec![7.5, 15.0], vec![7.5, 15.0], vec![15.0, 30.0]],
                ..Default::default()
            },
        ),
    ];

    for (name, c) in &cases {
        let fps = &c.fps;
        let mut frames: Vec<Vec<TestFrameInfo>> = Vec::new();
        let mut h26xcalcs: Vec<FrameRateCalculatorH26x> = Vec::with_capacity(fps.len());
        for fps_of_layer in fps {
            h26xcalcs.push(FrameRateCalculatorH26x::new(90000, logger::get_logger()));
            frames.push(
                create_frames(
                    c.start_frame_number,
                    c.start_ts,
                    c.start_seq,
                    200,
                    std::slice::from_ref(fps_of_layer),
                    false,
                )
                .swap_remove(0),
            );
        }

        let mut framerates_got = false;
        for (s, fs) in frames.iter().enumerate() {
            for f in fs {
                if h26xcalcs[s].recv_packet(&f.to_h26x()) {
                    framerates_got = true;
                    for calc in &h26xcalcs {
                        if !calc.completed() {
                            framerates_got = false;
                            break;
                        }
                    }
                }
            }
        }
        assert!(framerates_got, "{name}");
        for (i, calc) in h26xcalcs.iter().enumerate() {
            let fps_expected = &fps[i];
            let fps_got = calc.get_frame_rate();
            verify_fps(name, fps_expected, &fps_got[..fps_expected.len()]);
        }
    }

    {
        let name = "packet lost and duplicate";
        let fps: Vec<Vec<f32>> = vec![
            vec![7.5, 15.0, 30.0],
            vec![7.5, 15.0, 30.0],
            vec![7.5, 15.0, 30.0],
        ];
        let mut frames: Vec<Vec<Option<TestFrameInfo>>> = Vec::with_capacity(fps.len());
        let mut h26xcalcs: Vec<Box<dyn FrameRateCalculator>> = Vec::with_capacity(fps.len());
        for i in 0..fps.len() {
            frames.push(
                create_frames(100, 12345678, 10, 500, std::slice::from_ref(&fps[i]), false)
                    .swap_remove(0)
                    .into_iter()
                    .map(Some)
                    .collect(),
            );
            h26xcalcs.push(Box::new(FrameRateCalculatorH26x::new(
                90000,
                logger::get_logger(),
            )));
            for j in 5..130 {
                if j % 2 == 0 {
                    frames[i][j] = frames[i][j - 1].clone();
                }
            }
            for j in 130..230 {
                if j % 3 == 0 {
                    frames[i][j] = None;
                }
            }
            for j in 230..330 {
                if j % 2 == 0 {
                    frames[i].swap(j, j - 1);
                }
            }
        }
        let mut framerates_got = false;
        for (s, fs) in frames.iter().enumerate() {
            for f in fs {
                let Some(f) = f else {
                    continue;
                };
                if h26xcalcs[s].recv_packet(&f.to_h26x()) {
                    framerates_got = true;
                    for calc in &h26xcalcs {
                        if !calc.completed() {
                            framerates_got = false;
                            break;
                        }
                    }
                }
            }
        }
        assert!(framerates_got, "{name}");
        for (i, calc) in h26xcalcs.iter().enumerate() {
            let fps_expected = &fps[i];
            let fps_got = calc.get_frame_rate();
            verify_fps(name, fps_expected, &fps_got[..fps_expected.len()]);
        }
    }
}
