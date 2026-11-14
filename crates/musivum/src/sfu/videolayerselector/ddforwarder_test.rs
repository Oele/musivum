// Ported from LiveKit, pkg/sfu/videolayerselector/ddforwarder_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `forward` gives the decoded descriptor as an `Option`, `None` where the original gives
//   nil; the bitmask of a descriptor is compared as an `Option` where the original
//   dereferences the pointer.
// - The fixture and `hex_decode_string` are in dependencydescriptor_test.rs, as the fixture
//   is in the original.
// - `TestDDForwarderAllocations`: the assertion on allocations per call is left out, see
//   there. The calls are made as often as `testing.AllocsPerRun` makes them.

use super::super::dependencydescriptor::dependencydescriptor_test::{
    DEPENDENCY_DESCRIPTOR_FIXTURE, hex_decode_string,
};
use super::*;
use crate::protocol::logger;
use crate::webrtc::rtp;

// ddForwarderTest builds packets with the L3T3 fixture descriptor and decodes what a
// DDForwarder writes
struct DdForwarderTest {
    key_frame_descriptor: dd::DependencyDescriptor,
    structure: Option<Arc<dd::FrameDependencyStructure>>,
    decode_targets: Vec<buffer::DependencyDescriptorDecodeTarget>,
}

impl DdForwarderTest {
    fn new() -> Self {
        let raw = hex_decode_string(DEPENDENCY_DESCRIPTOR_FIXTURE);
        let mut ext = dd::DependencyDescriptorExtension::default();
        ext.unmarshal(&raw).unwrap();
        let key_frame_descriptor = ext.descriptor;
        assert!(key_frame_descriptor.attached_structure.is_some());

        Self {
            structure: key_frame_descriptor.attached_structure.clone(),
            key_frame_descriptor,
            decode_targets: vec![
                buffer::DependencyDescriptorDecodeTarget {
                    target: 2,
                    layer: buffer::VideoLayer {
                        temporal: 2,
                        ..Default::default()
                    },
                },
                buffer::DependencyDescriptorDecodeTarget {
                    target: 1,
                    layer: buffer::VideoLayer {
                        temporal: 1,
                        ..Default::default()
                    },
                },
                buffer::DependencyDescriptorDecodeTarget {
                    target: 0,
                    layer: buffer::VideoLayer {
                        temporal: 0,
                        ..Default::default()
                    },
                },
            ],
        }
    }

    fn packet(
        &self,
        ssrc: u32,
        ext_frame_num: u64,
        is_key_frame: bool,
        restart_generation: i64,
    ) -> buffer::ExtPacket {
        let mut descriptor = self.key_frame_descriptor.clone();
        descriptor.frame_number = ext_frame_num as u16;
        if !is_key_frame {
            descriptor.attached_structure = None;
            descriptor.active_decode_targets_bitmask = None;
        }
        buffer::ExtPacket {
            packet: rtp::Packet {
                header: rtp::Header {
                    ssrc,
                    ..Default::default()
                },
                ..Default::default()
            },
            is_key_frame,
            dependency_descriptor: Some(buffer::ExtDependencyDescriptor {
                descriptor: Some(descriptor),
                decode_targets: self.decode_targets.clone(),
                ext_frame_num,
                restart_generation,
                // all decode targets active, as the parser reports after a structure
                active_decode_targets_mask: (1 << self.decode_targets.len()) - 1,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    // forward returns the decoded descriptor, nil when f writes none
    #[track_caller]
    fn forward(
        &self,
        f: &mut DdForwarder,
        ext_pkt: &buffer::ExtPacket,
        temporal: i32,
    ) -> Option<dd::DependencyDescriptor> {
        let mut result = VideoLayerSelectorResult::default();
        if !f.forward(ext_pkt, temporal, &mut result) {
            return None;
        }
        let mut dd_bytes = &result.dd_bytes_spill[..];
        if dd_bytes.is_empty() {
            dd_bytes = &result.dd_bytes[..result.dd_bytes_len];
        }
        let mut out = dd::DependencyDescriptorExtension {
            descriptor: dd::DependencyDescriptor::default(),
            structure: self.structure.clone(),
        };
        out.unmarshal(dd_bytes).unwrap();
        Some(out.descriptor)
    }
}

#[test]
fn test_dd_forwarder() {
    let d = DdForwarderTest::new();
    let mut f = DdForwarder::new(logger::get_logger());

    // nothing before the first key frame, there is no structure to write it with
    assert!(d.forward(&mut f, &d.packet(1, 999, false, 0), 0).is_none());

    // the first key frame keeps the publisher's frame number and carries the structure,
    // with the decode targets the subscriber gets, not all of them
    let out = d.forward(&mut f, &d.packet(1, 1000, true, 0), 0).unwrap();
    assert_eq!(1000u16, out.frame_number);
    assert!(out.attached_structure.is_some());
    assert_eq!(Some(1u32 << 0), out.active_decode_targets_bitmask);

    // delta frames name the active decode targets, frames dropped in between leave gaps
    let out = d.forward(&mut f, &d.packet(1, 1001, false, 0), 0).unwrap();
    assert_eq!(1001u16, out.frame_number);
    assert_eq!(Some(1u32 << 0), out.active_decode_targets_bitmask);
    let out = d.forward(&mut f, &d.packet(1, 1003, false, 0), 1).unwrap();
    assert_eq!(1003u16, out.frame_number);
    assert_eq!(Some(1u32 << 0 | 1 << 1), out.active_decode_targets_bitmask);

    // a layer switch: the new layer's lower frame numbers continue after the last one
    let out = d.forward(&mut f, &d.packet(2, 40, true, 0), 0).unwrap();
    assert_eq!(1004u16, out.frame_number);
    assert!(out.attached_structure.is_some());
    let out = d.forward(&mut f, &d.packet(2, 41, false, 0), 0).unwrap();
    assert_eq!(1005u16, out.frame_number);

    // a retransmitted key frame packet keeps its number
    let out = d.forward(&mut f, &d.packet(2, 40, true, 0), 0).unwrap();
    assert_eq!(1004u16, out.frame_number);

    // a late packet of the previous layer is not described
    assert!(d.forward(&mut f, &d.packet(1, 1004, false, 0), 0).is_none());

    // a restarted stream starts its frame numbers over, they still continue
    let out = d.forward(&mut f, &d.packet(2, 5, true, 1), 0).unwrap();
    assert_eq!(1006u16, out.frame_number);
    let out = d.forward(&mut f, &d.packet(2, 6, false, 1), 0).unwrap();
    assert_eq!(1007u16, out.frame_number);
}

#[test]
fn test_dd_forwarder_shared_frame_counter() {
    // libwebrtc numbers the frames of all simulcast layers with one counter
    let d = DdForwarderTest::new();
    let mut f = DdForwarder::new(logger::get_logger());

    assert_eq!(
        100u16,
        d.forward(&mut f, &d.packet(1, 100, true, 0), 0)
            .unwrap()
            .frame_number
    );
    assert_eq!(
        102u16,
        d.forward(&mut f, &d.packet(1, 102, false, 0), 0)
            .unwrap()
            .frame_number
    );

    // the new layer's key frame was encoded before the last frame sent, it moves up
    assert_eq!(
        103u16,
        d.forward(&mut f, &d.packet(2, 101, true, 0), 0)
            .unwrap()
            .frame_number
    );
    assert_eq!(
        106u16,
        d.forward(&mut f, &d.packet(2, 104, false, 0), 0)
            .unwrap()
            .frame_number
    );

    // a key frame ahead of the last frame sent keeps the current offset
    assert_eq!(
        112u16,
        d.forward(&mut f, &d.packet(1, 110, true, 0), 0)
            .unwrap()
            .frame_number
    );
}

#[test]
fn test_dd_forwarder_from_other() {
    let d = DdForwarderTest::new();
    let mut f = DdForwarder::new(logger::get_logger());
    assert_eq!(
        100u16,
        d.forward(&mut f, &d.packet(1, 100, true, 0), 0)
            .unwrap()
            .frame_number
    );
    assert_eq!(
        103u16,
        d.forward(&mut f, &d.packet(1, 103, false, 0), 0)
            .unwrap()
            .frame_number
    );

    // a replacement on the same downtrack, a restarted upstream receiver for example, gets
    // a key frame behind the last frame sent: it continues after the last frame sent
    let mut f = DdForwarder::new_from_other(Some(&f), logger::get_logger());
    assert!(d.forward(&mut f, &d.packet(1, 101, false, 0), 0).is_none());
    assert_eq!(
        104u16,
        d.forward(&mut f, &d.packet(1, 101, true, 0), 0)
            .unwrap()
            .frame_number
    );
    assert_eq!(
        105u16,
        d.forward(&mut f, &d.packet(1, 102, false, 0), 0)
            .unwrap()
            .frame_number
    );

    // a replacement of nothing keeps the publisher's frame numbers
    let mut f = DdForwarder::new_from_other(None, logger::get_logger());
    assert_eq!(
        50u16,
        d.forward(&mut f, &d.packet(2, 50, true, 0), 0)
            .unwrap()
            .frame_number
    );
}

#[test]
fn test_dd_forwarder_restart_on_delta_frame() {
    let d = DdForwarderTest::new();
    let mut f = DdForwarder::new(logger::get_logger());
    assert_eq!(
        1000u16,
        d.forward(&mut f, &d.packet(1, 1000, true, 0), 0)
            .unwrap()
            .frame_number
    );
    assert_eq!(
        1001u16,
        d.forward(&mut f, &d.packet(1, 1001, false, 0), 0)
            .unwrap()
            .frame_number
    );

    // the restarted stream's first packet is a delta frame: it has no structure to be
    // written with, but its numbers already continue
    assert!(d.forward(&mut f, &d.packet(1, 3, false, 1), 0).is_none());
    assert_eq!(
        1003u16,
        d.forward(&mut f, &d.packet(1, 4, true, 1), 0)
            .unwrap()
            .frame_number
    );
    assert_eq!(
        1004u16,
        d.forward(&mut f, &d.packet(1, 5, false, 1), 0)
            .unwrap()
            .frame_number
    );
}

#[test]
fn test_dd_forwarder_publisher_active_decode_targets() {
    let d = DdForwarderTest::new();
    let mut f = DdForwarder::new(logger::get_logger());
    assert_eq!(
        Some(1u32 << 0 | 1 << 1 | 1 << 2),
        d.forward(&mut f, &d.packet(1, 100, true, 0), 2)
            .unwrap()
            .active_decode_targets_bitmask
    );

    // the publisher turned off its upper decode targets on a packet that was not forwarded,
    // the next forwarded packet still reports it
    let mut delta = d.packet(1, 102, false, 0);
    delta
        .dependency_descriptor
        .as_mut()
        .unwrap()
        .active_decode_targets_mask = 1 << 0;
    assert_eq!(
        Some(1u32 << 0),
        d.forward(&mut f, &delta, 2)
            .unwrap()
            .active_decode_targets_bitmask
    );

    // and turned them on again
    assert_eq!(
        Some(1u32 << 0 | 1 << 1 | 1 << 2),
        d.forward(&mut f, &d.packet(1, 103, true, 0), 2)
            .unwrap()
            .active_decode_targets_bitmask
    );
}

#[test]
fn test_dd_forwarder_state() {
    let d = DdForwarderTest::new();
    let mut f = DdForwarder::new(logger::get_logger());
    assert!(f.get_state().is_none());
    assert_eq!(
        1000u16,
        d.forward(&mut f, &d.packet(1, 1000, true, 0), 0)
            .unwrap()
            .frame_number
    );
    assert_eq!(
        1003u16,
        d.forward(&mut f, &d.packet(1, 1003, false, 0), 0)
            .unwrap()
            .frame_number
    );
    let state = f.get_state();
    assert_eq!(1003u64, state.as_ref().unwrap().ext_last_frame_number);

    // a migrated subscriber whose new node's key frame is behind continues after the last
    // number sent
    let mut f = DdForwarder::new(logger::get_logger());
    f.seed_state(state.as_ref());
    assert!(d.forward(&mut f, &d.packet(1, 999, false, 0), 0).is_none());
    assert_eq!(
        1004u16,
        d.forward(&mut f, &d.packet(1, 1001, true, 0), 0)
            .unwrap()
            .frame_number
    );

    // the old node had wrapped its extended numbers, the new node starts from the 16 bit
    // value: the 16 bit numbers still continue
    let mut f = DdForwarder::new(logger::get_logger());
    f.seed_state(Some(&livekit::DependencyDescriptorForwarderState {
        ext_last_frame_number: (1 << 16) + 1003,
    }));
    assert_eq!(
        1004u16,
        d.forward(&mut f, &d.packet(1, 1001, true, 0), 0)
            .unwrap()
            .frame_number
    );
}

#[test]
fn test_dd_forwarder_allocations() {
    let raw = hex_decode_string(DEPENDENCY_DESCRIPTOR_FIXTURE);
    let mut ext = dd::DependencyDescriptorExtension::default();
    ext.unmarshal(&raw).unwrap();
    let key_frame_descriptor = ext.descriptor;

    let decode_targets = vec![buffer::DependencyDescriptorDecodeTarget {
        target: 0,
        layer: buffer::VideoLayer {
            temporal: 0,
            ..Default::default()
        },
    }];
    let key_frame = buffer::ExtPacket {
        packet: rtp::Packet {
            header: rtp::Header {
                ssrc: 1,
                ..Default::default()
            },
            ..Default::default()
        },
        is_key_frame: true,
        dependency_descriptor: Some(buffer::ExtDependencyDescriptor {
            descriptor: Some(key_frame_descriptor.clone()),
            decode_targets: decode_targets.clone(),
            ext_frame_num: 100,
            active_decode_targets_mask: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut delta_descriptor = key_frame_descriptor;
    delta_descriptor.attached_structure = None;
    delta_descriptor.active_decode_targets_bitmask = None;
    let mut delta = buffer::ExtPacket {
        packet: rtp::Packet {
            header: rtp::Header {
                ssrc: 1,
                ..Default::default()
            },
            ..Default::default()
        },
        dependency_descriptor: Some(buffer::ExtDependencyDescriptor {
            descriptor: Some(delta_descriptor),
            decode_targets,
            ext_frame_num: 101,
            active_decode_targets_mask: 1,
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut f = DdForwarder::new(logger::get_logger());
    let mut result = VideoLayerSelectorResult::default();
    assert!(f.forward(&key_frame, 0, &mut result));

    // a delta frame's descriptor fits inline
    // `testing.AllocsPerRun(100, ..)` calls the function once to warm up and then 100 times.
    for _ in 0..101 {
        delta.dependency_descriptor.as_mut().unwrap().ext_frame_num += 1;
        result = VideoLayerSelectorResult::default();
        assert!(f.forward(&delta, 0, &mut result), "not forwarded");
    }
    // The original asserts here that forwarding a delta frame does not allocate. Counting
    // allocations needs an allocator of the test binary's own, which needs `unsafe`.
}
