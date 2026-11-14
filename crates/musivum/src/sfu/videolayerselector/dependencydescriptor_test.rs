// Ported from LiveKit, pkg/sfu/videolayerselector/dependencydescriptor_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The subtests of `TestDecodeTarget` are blocks that carry their names in the messages of
//   their assertions.
// - The frames the original keeps pointers to (`frameForwarded`, `frameDropped`,
//   `notDecodableFrame`, `brokenFrame`) are kept as indexes into `frames`. `dd_of`, `fd_of`
//   and `fd_mut` reach into a packet where the original follows its pointers.
// - The decision cache is shared as an `Arc<Mutex<..>>`, see framechain.rs.
// - `TestDependencyDescriptorSelectAllocations`: the assertion on allocations per call is
//   left out, see there. The calls are made as often as `testing.AllocsPerRun` makes them.
// - `hex_decode_string` stands in for Go's `hex.DecodeString`.
// - The two tests at the end are new for musivum and say so.

use std::cmp::Ordering;
use std::collections::HashMap;

use super::*;
use crate::protocol::logger;
use crate::webrtc::rtp;

fn dd_of(ext_pkt: &buffer::ExtPacket) -> &dede::DependencyDescriptor {
    ext_pkt
        .dependency_descriptor
        .as_ref()
        .unwrap()
        .descriptor
        .as_ref()
        .unwrap()
}

fn fd_of(ext_pkt: &buffer::ExtPacket) -> &dede::FrameDependencyTemplate {
    dd_of(ext_pkt).frame_dependencies.as_ref().unwrap()
}

fn fd_mut(ext_pkt: &mut buffer::ExtPacket) -> &mut dede::FrameDependencyTemplate {
    Arc::make_mut(
        ext_pkt
            .dependency_descriptor
            .as_mut()
            .unwrap()
            .descriptor
            .as_mut()
            .unwrap()
            .frame_dependencies
            .as_mut()
            .unwrap(),
    )
}

#[test]
fn test_decode_target() {
    let target = buffer::DependencyDescriptorDecodeTarget {
        target: 1,
        layer: buffer::VideoLayer {
            spatial: 1,
            temporal: 2,
        },
    };

    {
        let name = "No Chain";
        let dt = DecodeTarget::new(target, None);
        assert!(dt.valid(), "{name}");
        // no indication found
        let err = dt.on_frame(
            1,
            &dede::FrameDependencyTemplate {
                decode_target_indications: vec![],
                ..Default::default()
            },
        );
        assert!(err.is_err(), "{name}");

        let ret = dt
            .on_frame(
                1,
                &dede::FrameDependencyTemplate {
                    decode_target_indications: vec![
                        dede::DecodeTargetIndication::NotPresent,
                        dede::DecodeTargetIndication::Required,
                    ],
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(ret.target_valid, "{name}");
        assert_eq!(dede::DecodeTargetIndication::Required, ret.dti, "{name}");
    }

    {
        let name = "With Chain";
        let decisions = Arc::new(Mutex::new(SelectorDecisionCache::new(256, 80)));
        let chain = FrameChain::new(decisions, 1, logger::get_logger());
        let mut dt = DecodeTarget::new(target, Some(chain.clone()));
        chain.begin_update_active();
        dt.update_active(1 << dt.dependency_descriptor_decode_target.target);
        chain.end_update_active();
        assert!(dt.active(), "{name}");
        assert!(!dt.valid(), "{name}");

        // chain intact
        let frame = dede::FrameDependencyTemplate {
            decode_target_indications: vec![
                dede::DecodeTargetIndication::NotPresent,
                dede::DecodeTargetIndication::Required,
            ],
            chain_diffs: vec![0, 0],
            ..Default::default()
        };
        chain.on_frame(1, &frame);
        assert!(dt.valid(), "{name}");
        let ret = dt.on_frame(1, &frame).unwrap();
        assert!(ret.target_valid, "{name}");
        assert_eq!(dede::DecodeTargetIndication::Required, ret.dti, "{name}");
    }
}

#[test]
fn test_frame_chain() {
    let decisions = Arc::new(Mutex::new(SelectorDecisionCache::new(256, 3)));
    let chain = FrameChain::new(decisions.clone(), 0, logger::get_logger());
    assert!(chain.broken());

    // chain intact
    let frame_no_diff = dede::FrameDependencyTemplate {
        chain_diffs: vec![0],
        ..Default::default()
    };
    // not active
    assert!(!chain.on_frame(1, &frame_no_diff));

    chain.begin_update_active();
    chain.update_active(true);
    chain.end_update_active();

    assert!(chain.on_frame(1, &frame_no_diff));
    decisions.lock().add_forwarded(1);

    let frame_diff1 = dede::FrameDependencyTemplate {
        chain_diffs: vec![1],
        ..Default::default()
    };

    assert!(chain.on_frame(2, &frame_diff1));
    decisions.lock().add_forwarded(2);

    // frame 5 arrives first , but frame 4 can be recovered by NACK
    assert!(chain.on_frame(5, &frame_diff1));
    decisions.lock().add_forwarded(5);

    // frame 4 arrives, chain remains intact
    assert!(chain.on_frame(4, &frame_diff1));
    decisions.lock().add_forwarded(4);

    // frame 3 missed by out of nack range, chain broken
    decisions.lock().add_forwarded(7);
    assert!(chain.broken());

    // recovery by non-diff frame
    assert!(chain.on_frame(1000, &frame_no_diff));
    assert!(!chain.broken());
    decisions.lock().add_forwarded(1000);

    // broken by dropped frame
    assert!(chain.on_frame(1002, &frame_diff1));
    decisions.lock().add_dropped(1001);
    assert!(chain.broken());

    // recovery by non-diff frame
    assert!(chain.on_frame(2000, &frame_no_diff));
    decisions.lock().add_forwarded(2000);
    decisions.lock().add_dropped(2001);
    assert!(!chain.on_frame(2002, &frame_diff1));
    assert!(chain.broken());
}

#[test]
fn test_dependency_descriptor() {
    let mut dd_selector = DependencyDescriptor::new(logger::get_logger());
    let target_layer = buffer::VideoLayer {
        spatial: 1,
        temporal: 2,
    };
    dd_selector.set_target(target_layer);
    dd_selector.set_request_spatial(1);

    // no dd ext, dropped
    let ret = dd_selector.select(
        &buffer::ExtPacket {
            packet: rtp::Packet::default(),
            ..Default::default()
        },
        0,
    );
    assert!(!ret.is_selected);
    assert!(!ret.is_relevant);

    // non key frame, dropped
    let ret = dd_selector.select(
        &buffer::ExtPacket {
            is_key_frame: false,
            dependency_descriptor: Some(buffer::ExtDependencyDescriptor {
                descriptor: Some(dede::DependencyDescriptor {
                    frame_number: 1,
                    frame_dependencies: Some(Arc::new(dede::FrameDependencyTemplate {
                        spatial_id: target_layer.spatial as i64,
                        temporal_id: target_layer.temporal as i64,
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            packet: rtp::Packet::default(),
            ..Default::default()
        },
        0,
    );
    assert!(!ret.is_selected);
    assert!(!ret.is_relevant);

    let mut frames = create_dd_frames(
        buffer::VideoLayer {
            spatial: 2,
            temporal: 2,
        },
        3,
    );
    // key frame, update structure and decode targets
    let ret = dd_selector.select(&frames[0], 0);
    assert!(ret.is_selected);
    assert_eq!(dd_selector.get_current(), dd_selector.get_target());
    let (sync, _) = dd_selector.check_sync();
    assert!(sync);

    // forward frame belongs to target layer
    // drop frame exceeds target layer (not present in target layer or lower layer)
    // forward frame not present in target layer but present in lower layer
    let mut belong_target_case = false;
    let mut exceed_target_case = false;
    let mut lower_target_case = false;
    let mut idx = 1usize;
    let mut frame_forwarded: Vec<usize> = Vec::new();
    let mut frame_dropped: Vec<usize> = Vec::new();
    while idx < frames.len() {
        let fd = fd_of(&frames[idx]);
        let ret = dd_selector.select(&frames[idx], 0);
        if fd.spatial_id == target_layer.spatial as i64
            && fd.temporal_id == target_layer.temporal as i64
        {
            assert!(ret.is_selected);
            belong_target_case = true;
            frame_forwarded.push(idx);
        } else if fd.spatial_id < target_layer.spatial as i64 && fd.temporal_id == 0 {
            assert!(ret.is_selected);
            lower_target_case = true;
            frame_forwarded.push(idx);
        } else if fd.spatial_id > target_layer.spatial as i64
            || fd.temporal_id > target_layer.temporal as i64
        {
            assert!(!ret.is_selected);
            exceed_target_case = true;
            frame_dropped.push(idx);
        }

        if belong_target_case && exceed_target_case && lower_target_case {
            break;
        }
        idx += 1;
    }

    assert!(belong_target_case && exceed_target_case && lower_target_case);

    // select frame already forwarded
    let ret = dd_selector.select(&frames[frame_forwarded[0]], 0);
    assert!(ret.is_selected);

    // drop frame already dropped
    let ret = dd_selector.select(&frames[frame_dropped[0]], 0);
    assert!(!ret.is_selected);

    // drop frame present but not decodable (dependency frame missed)
    idx += 1;
    while idx < frames.len() {
        let fd = fd_of(&frames[idx]);
        dd_selector.select(&frames[idx], 0);
        if fd.spatial_id == target_layer.spatial as i64
            && fd.temporal_id == target_layer.temporal as i64
        {
            break;
        }
        idx += 1;
    }
    let not_decodable_frame = idx;
    let frame_diff = dd_of(&frames[not_decodable_frame])
        .frame_number
        .wrapping_sub(dd_of(&frames[frame_dropped[0]]).frame_number) as i64;
    fd_mut(&mut frames[not_decodable_frame]).frame_diffs = vec![frame_diff];
    let ret = dd_selector.select(&frames[not_decodable_frame], 0);
    assert!(!ret.is_selected);

    // target layer broken
    idx += 1;
    while idx < frames.len() {
        let fd = fd_of(&frames[idx]);
        dd_selector.select(&frames[idx], 0);
        if fd.spatial_id == target_layer.spatial as i64
            && fd.temporal_id == target_layer.temporal as i64
        {
            break;
        }
        idx += 1;
    }
    let broken_frame = idx;
    let chain_diff = dd_of(&frames[not_decodable_frame])
        .frame_number
        .wrapping_sub(dd_of(&frames[frame_dropped[0]]).frame_number) as i64;
    fd_mut(&mut frames[broken_frame]).chain_diffs[target_layer.spatial as usize] = chain_diff;
    let ret = dd_selector.select(&frames[broken_frame], 0);
    assert!(!ret.is_selected);

    // switch to lower layer, forward frame
    idx += 1;
    let mut switch_to_lower = false;
    while idx < frames.len() {
        let ret = dd_selector.select(&frames[idx], 0);
        if ret.is_selected {
            assert!(target_layer.greater_than(dd_selector.get_current()));
            switch_to_lower = true;
            break;
        }
        idx += 1;
    }
    assert!(switch_to_lower);

    // not sync with requested layer
    dd_selector.set_request_spatial(target_layer.spatial);
    let (locked, layer) = dd_selector.check_sync();
    assert!(!locked);
    assert_eq!(target_layer.spatial, layer);
    // request to current layer, sync
    dd_selector.set_request_spatial(dd_selector.get_current().spatial);
    let (locked, _) = dd_selector.check_sync();
    assert!(locked);

    // should drop frame that relies on a keyframe is not present in current selection
    let frames_previous = create_dd_frames(
        buffer::VideoLayer {
            spatial: 2,
            temporal: 2,
        },
        1000,
    );
    let ret = dd_selector.select(&frames_previous[1], 0);
    assert!(!ret.is_selected);
    // keyframe lost, out of sync
    let (locked, _) = dd_selector.check_sync();
    assert!(!locked);
}

pub(in super::super) fn create_dd_frames(
    max_layer: buffer::VideoLayer,
    start_frame_number: u16,
) -> Vec<buffer::ExtPacket> {
    let mut start_frame_number = start_frame_number;
    let mut frames: Vec<buffer::ExtPacket> = Vec::new();
    let mut active_bit_mask = 0u32;
    let mut decode_targets: Vec<buffer::DependencyDescriptorDecodeTarget> = Vec::new();
    let mut decode_targets_protect_by_chain: Vec<usize> = Vec::new();
    for i in 0..=max_layer.spatial as i64 {
        for j in 0..=max_layer.temporal as i64 {
            decode_targets.push(buffer::DependencyDescriptorDecodeTarget {
                target: i * (max_layer.temporal + 1) as i64 + j,
                layer: buffer::VideoLayer {
                    spatial: i as i32,
                    temporal: j as i32,
                },
            });
            decode_targets_protect_by_chain.push(i as usize);
            active_bit_mask |= 1 << (i * (max_layer.temporal + 1) as i64 + j) as u32;
        }
    }
    decode_targets.sort_by(|a, b| {
        if a.layer.greater_than(b.layer) {
            return Ordering::Less;
        }
        if b.layer.greater_than(a.layer) {
            return Ordering::Greater;
        }
        Ordering::Equal
    });

    let chain_diffs = vec![0i64; max_layer.spatial as usize + 1];
    let mut dtis = vec![dede::DecodeTargetIndication::default(); decode_targets.len()];
    for dt in &decode_targets {
        dtis[dt.target as usize] = dede::DecodeTargetIndication::Switch;
    }

    let mut templates: Vec<Arc<dede::FrameDependencyTemplate>> =
        vec![Arc::default(); decode_targets.len()];

    for dt in &decode_targets {
        templates[dt.target as usize] = Arc::new(dede::FrameDependencyTemplate {
            spatial_id: dt.layer.spatial as i64,
            temporal_id: dt.layer.temporal as i64,
            chain_diffs: chain_diffs.clone(),
            decode_target_indications: dtis.clone(),
            ..Default::default()
        });
    }
    let key_frame = buffer::ExtPacket {
        is_key_frame: true,
        dependency_descriptor: Some(buffer::ExtDependencyDescriptor {
            descriptor: Some(dede::DependencyDescriptor {
                frame_number: start_frame_number,
                frame_dependencies: Some(Arc::new(dede::FrameDependencyTemplate {
                    spatial_id: 0,
                    temporal_id: 0,
                    chain_diffs: chain_diffs.clone(),
                    decode_target_indications: dtis.clone(),
                    ..Default::default()
                })),
                attached_structure: Some(Arc::new(dede::FrameDependencyStructure {
                    num_decode_targets: ((max_layer.spatial + 1) * (max_layer.temporal + 1))
                        as usize,
                    num_chains: max_layer.spatial as usize + 1,
                    decode_target_protected_by_chain: decode_targets_protect_by_chain,
                    templates,
                    ..Default::default()
                })),
                active_decode_targets_bitmask: Some(active_bit_mask),
                ..Default::default()
            }),
            decode_targets: decode_targets.clone(),
            structure_updated: true,
            active_decode_targets_updated: true,
            integrity: true,
            ext_frame_num: start_frame_number as u64,
            ext_key_frame_num: start_frame_number as u64,
            ..Default::default()
        }),
        packet: rtp::Packet {
            header: rtp::Header {
                ssrc: 1234,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let key_frame_ext_frame_num = key_frame
        .dependency_descriptor
        .as_ref()
        .unwrap()
        .ext_frame_num;

    frames.push(key_frame);

    let mut chain_prev_frame: HashMap<i64, i64> = HashMap::new();
    for i in 0..=max_layer.spatial as i64 {
        chain_prev_frame.insert(i, start_frame_number as i64);
    }
    start_frame_number = start_frame_number.wrapping_add(1);
    for _ in 0..10 {
        for j in (0..decode_targets.len()).rev() {
            let dt = decode_targets[j];
            let mut frame_chain_diffs = vec![0i64; chain_diffs.len()];
            for (i, frame_chain_diff) in frame_chain_diffs.iter_mut().enumerate() {
                *frame_chain_diff = start_frame_number as i64 - chain_prev_frame[&(i as i64)];
            }

            let mut frame_dtis = vec![dede::DecodeTargetIndication::default(); dtis.len()];
            for (k, frame_dti) in frame_dtis.iter_mut().enumerate() {
                if k as i64 >= dt.target {
                    if dt.layer.temporal == 0 {
                        *frame_dti = dede::DecodeTargetIndication::Required;
                    } else {
                        *frame_dti = dede::DecodeTargetIndication::Discardable;
                    }
                } else {
                    *frame_dti = dede::DecodeTargetIndication::NotPresent;
                }
            }

            let frame = buffer::ExtPacket {
                dependency_descriptor: Some(buffer::ExtDependencyDescriptor {
                    descriptor: Some(dede::DependencyDescriptor {
                        frame_number: start_frame_number,
                        frame_dependencies: Some(Arc::new(dede::FrameDependencyTemplate {
                            spatial_id: dt.layer.spatial as i64,
                            temporal_id: dt.layer.temporal as i64,
                            chain_diffs: frame_chain_diffs,
                            decode_target_indications: frame_dtis,
                            ..Default::default()
                        })),
                        ..Default::default()
                    }),
                    decode_targets: decode_targets.clone(),
                    integrity: true,
                    ext_frame_num: start_frame_number as u64,
                    ext_key_frame_num: key_frame_ext_frame_num,
                    ..Default::default()
                }),
                packet: rtp::Packet {
                    header: rtp::Header {
                        ssrc: 1234,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ..Default::default()
            };

            start_frame_number = start_frame_number.wrapping_add(1);

            if dt.layer.temporal == 0 {
                chain_prev_frame.insert(dt.layer.spatial as i64, start_frame_number as i64);
            }

            frames.push(frame);
        }
    }

    frames
}

// same capture as the dependency descriptor package's marshal fixture, an L3T3
// key frame descriptor with the full dependency structure attached
pub(in super::super) const DEPENDENCY_DESCRIPTOR_FIXTURE: &str = "c1017280081485214eafffaaaa863cf0430c10c302afc0aaa0063c00430010c002a000a80006000040001d954926e082b04a0941b820ac1282503157f974000ca864330e222222eca8655304224230eca877530077004200ef008601df010d";

// Go's `hex.DecodeString`.
#[track_caller]
pub(in super::super) fn hex_decode_string(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd length hex string");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("invalid byte in hex string"))
        .collect()
}

#[test]
fn test_video_layer_selector_result_dependency_descriptor() {
    let raw = hex_decode_string(DEPENDENCY_DESCRIPTOR_FIXTURE);

    let mut ext = dede::DependencyDescriptorExtension::default();
    ext.unmarshal(&raw).unwrap();
    let descriptor = ext.descriptor;
    assert!(descriptor.attached_structure.is_some());
    let structure = descriptor.attached_structure.clone();

    // a key frame descriptor carries the full structure and spills to the heap
    let mut key_frame = VideoLayerSelectorResult::default();
    assert_eq!(
        Ok(()),
        key_frame.marshal_dependency_descriptor_extension(&dede::DependencyDescriptorExtension {
            descriptor: descriptor.clone(),
            structure: structure.clone(),
        })
    );
    assert_eq!(0, key_frame.dd_bytes_len);
    assert!(key_frame.dd_bytes_spill.len() > dede::MAX_INLINE_EXTENSION_SIZE);

    // a per-packet descriptor is held inline, byte for byte what Marshal returns
    let mut per_packet = descriptor.clone();
    per_packet.attached_structure = None;
    per_packet.active_decode_targets_bitmask = None;
    let dd_extension = dede::DependencyDescriptorExtension {
        descriptor: per_packet,
        structure,
    };

    let mut result = VideoLayerSelectorResult::default();
    assert_eq!(
        Ok(()),
        result.marshal_dependency_descriptor_extension(&dd_extension)
    );
    assert!(result.dd_bytes_spill.is_empty());
    assert_ne!(0, result.dd_bytes_len);
    assert!(result.dd_bytes_len <= dede::MAX_INLINE_EXTENSION_SIZE);

    let want = dd_extension.marshal().unwrap();
    assert_eq!(want, result.dd_bytes[..result.dd_bytes_len]);
}

#[test]
fn test_dependency_descriptor_select_allocations() {
    for rewrite_frame_number in [false, true] {
        let mut selector = DependencyDescriptor::new(logger::get_logger());
        selector.set_target(buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        });
        selector.set_request_spatial(0);
        let mut frames = create_dd_frames(
            buffer::VideoLayer {
                spatial: 0,
                temporal: 0,
            },
            100,
        );
        assert!(selector.select(&frames[0], 0).is_selected);
        assert!(selector.active_decode_targets_bitmask.is_some());
        if rewrite_frame_number {
            selector.fn_wrapper.offset = 6000;
        }
        let packet = &mut frames[1];
        {
            let ddwdt = packet.dependency_descriptor.as_mut().unwrap();
            ddwdt.ext_frame_num -= 1;
            let descriptor = ddwdt.descriptor.as_mut().unwrap();
            descriptor.frame_number = descriptor.frame_number.wrapping_sub(1);
        }

        let mut selected = true;
        // `testing.AllocsPerRun(1000, ..)` calls the function once to warm up and then 1000
        // times.
        for _ in 0..1001 {
            let ddwdt = packet.dependency_descriptor.as_mut().unwrap();
            ddwdt.ext_frame_num += 1;
            let descriptor = ddwdt.descriptor.as_mut().unwrap();
            descriptor.frame_number = descriptor.frame_number.wrapping_add(1);
            selected = selected && selector.select(packet, 0).is_selected;
        }
        assert!(selected, "rewriteFrameNumber={rewrite_frame_number}");
        // The original asserts here that a selected packet does not allocate. Counting
        // allocations needs an allocator of the test binary's own, which needs `unsafe`.
    }
}

// New for musivum: the original has no test of the `recover()` in `Select`. The writer
// panics on an extension without structure (a nil dereference in the original); the frame is
// dropped and `select` returns. A selector cannot lose its structure by itself, so the test
// takes it away.
#[test]
fn select_survives_a_panic_of_the_writer() {
    let mut selector = DependencyDescriptor::new(logger::get_logger());
    selector.set_target(buffer::VideoLayer {
        spatial: 0,
        temporal: 0,
    });
    selector.set_request_spatial(0);
    let frames = create_dd_frames(
        buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
        100,
    );
    assert!(selector.select(&frames[0], 0).is_selected);

    selector.structure = None;
    let ret = selector.select(&frames[1], 0);
    assert!(!ret.is_selected);
    assert_eq!(0, ret.dd_bytes_len);
    assert!(ret.dd_bytes_spill.is_empty());
    let sd = selector.decisions.lock().get_decision(101);
    assert_eq!(Ok(SelectorDecision::Dropped), sd);
}

// New for musivum: a packet whose descriptor or frame dependencies are absent, where the
// original would dereference nil, is not selected.
#[test]
fn select_drops_a_packet_without_descriptor() {
    let mut selector = DependencyDescriptor::new(logger::get_logger());
    let mut frames = create_dd_frames(
        buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
        100,
    );

    let ddwdt = frames[0].dependency_descriptor.as_mut().unwrap();
    ddwdt.descriptor.as_mut().unwrap().frame_dependencies = None;
    assert!(!selector.select(&frames[0], 0).is_selected);

    frames[0].dependency_descriptor.as_mut().unwrap().descriptor = None;
    assert!(!selector.select(&frames[0], 0).is_selected);
}
