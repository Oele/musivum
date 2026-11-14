// Ported from LiveKit, pkg/sfu/buffer/dependencydescriptorparser_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; a structure is made as an `Arc`, as the descriptors
// and the parser hold it; the feeder returns the descriptor or the error (the original
// drops the video layer too); `ReleaseExtDependencyDescriptor` has no counterpart, so the
// calls to it are gone; the count of allocations in `TestDependencyDescriptorParserNoAlloc`
// is left out (see there), the rest of that test is kept.

use std::sync::Arc;

use bytes::Bytes;

use super::super::Error;
use super::*;
use crate::protocol::logger;

const DD_TEST_EXT_ID: u8 = 1;

// single spatial layer, single temporal layer, one decode target, one chain
fn new_l1t1_structure(structure_id: i64) -> Arc<dd::FrameDependencyStructure> {
    Arc::new(dd::FrameDependencyStructure {
        structure_id,
        num_decode_targets: 1,
        num_chains: 1,
        decode_target_protected_by_chain: vec![0],
        templates: vec![
            Arc::new(dd::FrameDependencyTemplate {
                // key frame
                decode_target_indications: vec![dd::DecodeTargetIndication::Switch],
                chain_diffs: vec![0],
                ..Default::default()
            }),
            Arc::new(dd::FrameDependencyTemplate {
                // delta frame
                decode_target_indications: vec![dd::DecodeTargetIndication::Required],
                frame_diffs: vec![1],
                chain_diffs: vec![1],
                ..Default::default()
            }),
        ],
        ..Default::default()
    })
}

struct DdTestFeeder {
    parser: DependencyDescriptorParser,
    // structure the writer marshals against, i. e. what the publisher last sent
    structure: Option<Arc<dd::FrameDependencyStructure>>,
    seq: u16,
}

fn new_dd_test_feeder() -> DdTestFeeder {
    DdTestFeeder {
        parser: DependencyDescriptorParser::new(
            DD_TEST_EXT_ID,
            logger::get_logger(),
            |_, _| {},
            false,
        ),
        structure: None,
        seq: 0,
    }
}

impl DdTestFeeder {
    // keyFrame sends a key frame carrying `structure`. Passing the same structure id as
    // the previous key frame models a publisher that repeats an unchanged structure,
    // which is what screen content with frequent key frames does.
    #[track_caller]
    fn key_frame(
        &mut self,
        frame_number: u16,
        structure: &Arc<dd::FrameDependencyStructure>,
    ) -> Result<ExtDependencyDescriptor, Error> {
        self.structure = Some(structure.clone());
        self.feed(frame_number, &structure.templates[0], Some(structure))
    }

    #[track_caller]
    fn delta_frame(&mut self, frame_number: u16) -> Result<ExtDependencyDescriptor, Error> {
        let template = self.structure.as_ref().unwrap().templates[1].clone();
        self.feed(frame_number, &template, None)
    }

    // deltaFrameWithMask sends a delta frame that changes the active decode targets
    #[track_caller]
    fn delta_frame_with_mask(
        &mut self,
        frame_number: u16,
        active_decode_targets: u32,
    ) -> Result<ExtDependencyDescriptor, Error> {
        let template = self.structure.as_ref().unwrap().templates[1].clone();
        self.feed_with_mask(frame_number, &template, None, Some(active_decode_targets))
    }

    #[track_caller]
    fn feed(
        &mut self,
        frame_number: u16,
        template: &Arc<dd::FrameDependencyTemplate>,
        attached_structure: Option<&Arc<dd::FrameDependencyStructure>>,
    ) -> Result<ExtDependencyDescriptor, Error> {
        self.feed_with_mask(frame_number, template, attached_structure, None)
    }

    #[track_caller]
    fn feed_with_mask(
        &mut self,
        frame_number: u16,
        template: &Arc<dd::FrameDependencyTemplate>,
        attached_structure: Option<&Arc<dd::FrameDependencyStructure>>,
        active_decode_targets: Option<u32>,
    ) -> Result<ExtDependencyDescriptor, Error> {
        let dd_val = dd::DependencyDescriptor {
            first_packet_in_frame: true,
            last_packet_in_frame: true,
            frame_number,
            frame_dependencies: Some(template.clone()),
            attached_structure: attached_structure.cloned(),
            active_decode_targets_bitmask: active_decode_targets,
            ..Default::default()
        };
        let buf = dd::DependencyDescriptorExtension {
            descriptor: dd_val,
            structure: self.structure.clone(),
        }
        .marshal()
        .unwrap();

        self.seq = self.seq.wrapping_add(1);
        let mut pkt = rtp::Packet {
            header: rtp::Header {
                sequence_number: self.seq,
                ..Default::default()
            },
            ..Default::default()
        };
        pkt.set_extension(DD_TEST_EXT_ID, Bytes::from(buf)).unwrap();

        let (ext_dd, _) = self.parser.parse(&pkt)?;
        Ok(ext_dd)
    }
}

// A key frame that repeats the current structure must not start dropping frames that
// precede it. Upstream advanced the drop threshold on every structure bearing key
// frame, so with frequent key frames (screen content) a late or retransmitted frame
// arriving after one was discarded as "earlier than current structure".
#[test]
fn test_dependency_descriptor_parser_late_frame_after_repeated_structure() {
    let mut f = new_dd_test_feeder();
    let structure = new_l1t1_structure(0);

    f.key_frame(0, &structure).unwrap();
    f.delta_frame(1).unwrap();
    f.delta_frame(3).unwrap();

    // key frame repeating the same structure id
    f.key_frame(4, &structure).unwrap();

    // frame 2 finally shows up (reordered or retransmitted). The structure has not
    // changed, so it is still decodable and must be forwarded.
    let ext_dd = f.delta_frame(2).unwrap();
    assert_eq!(2, ext_dd.ext_frame_num);
}

// A key frame carrying a *different* structure does invalidate everything before it:
// earlier frames reference templates that no longer exist.
#[test]
fn test_dependency_descriptor_parser_late_frame_after_structure_change() {
    let mut f = new_dd_test_feeder();

    f.key_frame(0, &new_l1t1_structure(0)).unwrap();
    f.delta_frame(1).unwrap();
    f.delta_frame(3).unwrap();

    f.key_frame(4, &new_l1t1_structure(1)).unwrap();

    let err = f.delta_frame(2).err();
    assert_eq!(Some(Error::FrameEarlierThanKeyFrame), err);
}

// An out-of-order key frame must still be dropped: accepting it would regress
// structureExtFrameNum (ExtKeyFrameNum) and replay a stale structure update.
#[test]
fn test_dependency_descriptor_parser_out_of_order_key_frame() {
    let mut f = new_dd_test_feeder();
    let structure = new_l1t1_structure(0);

    f.key_frame(0, &structure).unwrap();
    f.delta_frame(1).unwrap();
    f.key_frame(4, &structure).unwrap();

    let err = f.key_frame(2, &structure).err();
    assert_eq!(Some(Error::FrameEarlierThanKeyFrame), err);
}

// ExtKeyFrameNum keeps tracking every structure bearing key frame, unchanged by the
// split of the drop threshold into its own field.
#[test]
fn test_dependency_descriptor_parser_ext_key_frame_num() {
    let mut f = new_dd_test_feeder();
    let structure = new_l1t1_structure(0);

    let ext_dd = f.key_frame(0, &structure).unwrap();
    assert_eq!(0, ext_dd.ext_key_frame_num);

    let ext_dd = f.delta_frame(1).unwrap();
    assert_eq!(0, ext_dd.ext_key_frame_num);

    // repeated structure still advances ExtKeyFrameNum
    let ext_dd = f.key_frame(4, &structure).unwrap();
    assert_eq!(4, ext_dd.ext_key_frame_num);

    let ext_dd = f.delta_frame(5).unwrap();
    assert_eq!(4, ext_dd.ext_key_frame_num);
}

// every packet carries the stream's current active decode targets, also after the mask
// changed on a packet the forwarder dropped
#[test]
fn test_dependency_descriptor_parser_active_decode_targets_mask() {
    let mut f = new_dd_test_feeder();
    let structure = new_l1t1_structure(0);

    let ext_dd = f.key_frame(0, &structure).unwrap();
    assert_eq!(1, ext_dd.active_decode_targets_mask);

    let ext_dd = f.delta_frame_with_mask(1, 0).unwrap();
    assert_eq!(0, ext_dd.active_decode_targets_mask);

    let ext_dd = f.delta_frame(2).unwrap();
    assert_eq!(0, ext_dd.active_decode_targets_mask);

    let ext_dd = f.key_frame(3, &structure).unwrap();
    assert_eq!(1, ext_dd.active_decode_targets_mask);
}

// a first key frame at RTP sequence number 0 still makes its structure's decode targets
// active
#[test]
fn test_dependency_descriptor_parser_active_decode_targets_mask_at_sequence_zero() {
    let mut f = new_dd_test_feeder();
    f.seq = 0xffff; // the first packet goes out with sequence number 0
    let structure = new_l1t1_structure(0);

    let ext_dd = f.key_frame(0, &structure).unwrap();
    assert_eq!(1, ext_dd.active_decode_targets_mask);

    let ext_dd = f.delta_frame(1).unwrap();
    assert_eq!(1, ext_dd.active_decode_targets_mask);
}

// a pooled ExtDependencyDescriptor carries the storage for its descriptor, so parsing a delta
// frame must not allocate and a reused descriptor must not show the previous packet's values
//
// Here there is no pool and no storage to use again: a descriptor is a value. What the test
// asserts of the descriptors it gets is kept, and so are the runs of the parser, but not the
// count of their allocations.
#[test]
fn test_dependency_descriptor_parser_no_alloc() {
    let mut f = new_dd_test_feeder();
    let mut structure = new_l1t1_structure(0);
    Arc::make_mut(&mut structure).resolutions = vec![dd::RenderResolution {
        width: 640,
        height: 360,
    }];
    let ext_dd = f.key_frame(0, &structure).unwrap();
    let descriptor = ext_dd.descriptor.as_ref().unwrap();
    assert_eq!(
        0,
        descriptor
            .frame_dependencies
            .as_ref()
            .unwrap()
            .frame_diffs
            .len()
    );
    assert_eq!(
        Some(dd::RenderResolution {
            width: 640,
            height: 360
        }),
        descriptor.resolution
    );

    let ext_dd = f.delta_frame(1).unwrap();
    let descriptor = ext_dd.descriptor.as_ref().unwrap();
    assert_eq!(
        vec![1i64],
        descriptor.frame_dependencies.as_ref().unwrap().frame_diffs
    );
    assert_eq!(
        vec![1i64],
        descriptor.frame_dependencies.as_ref().unwrap().chain_diffs
    );
    assert!(descriptor.attached_structure.is_none());

    // the parsed template is a copy, the structure's template is untouched
    assert_eq!(vec![1i64], structure.templates[1].frame_diffs);

    // marshal the packets first, only Parse runs under the allocation count
    let mut bufs: Vec<Bytes> = Vec::with_capacity(128);
    for i in 0..128 {
        let dd_val = dd::DependencyDescriptor {
            first_packet_in_frame: true,
            last_packet_in_frame: true,
            frame_number: (i + 2) as u16,
            frame_dependencies: Some(structure.templates[1].clone()),
            ..Default::default()
        };
        bufs.push(Bytes::from(
            dd::DependencyDescriptorExtension {
                descriptor: dd_val,
                structure: Some(structure.clone()),
            }
            .marshal()
            .unwrap(),
        ));
    }
    let mut pkt = rtp::Packet {
        header: rtp::Header {
            sequence_number: f.seq,
            ..Default::default()
        },
        ..Default::default()
    };
    pkt.set_extension(DD_TEST_EXT_ID, bufs[0].clone()).unwrap();
    // The original runs this under `testing.AllocsPerRun(100, ...)`, which calls it once to
    // warm up and then 100 times, and requires that it does not allocate. Counting
    // allocations takes an allocator of the test's own, which is `unsafe` to write, and the
    // workspace forbids that; and this port's `parse` does allocate (the descriptor's frame
    // dependencies, the copy of the decode targets). The 101 runs are kept.
    for buf in bufs.iter().take(101) {
        pkt.sequence_number = pkt.sequence_number.wrapping_add(1);
        pkt.set_extension(DD_TEST_EXT_ID, buf.clone()).unwrap();
        f.parser.parse(&pkt).unwrap();
    }
}
