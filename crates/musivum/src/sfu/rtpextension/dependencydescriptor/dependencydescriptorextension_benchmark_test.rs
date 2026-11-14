// Ported from LiveKit,
// pkg/sfu/rtpextension/dependencydescriptor/dependencydescriptorextension_benchmark_test.go
// at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The two benchmarks are left out.
// `TestDependencyDescriptorMarshalAllocs` is not ported, see at its place. `hex_decode_string`
// stands in for Go's `hex.DecodeString`.

use std::sync::Arc;

use super::*;

// dependencyDescriptorMarshalFixture is a dependency descriptor captured from
// traffic. The fixture helper decodes its attached structure once, then returns
// a regular per-packet descriptor that references that structure.
pub(super) const DEPENDENCY_DESCRIPTOR_MARSHAL_FIXTURE: &str = "c1017280081485214eafffaaaa863cf0430c10c302afc0aaa0063c00430010c002a000a80006000040001d954926e082b04a0941b820ac1282503157f974000ca864330e222222eca8655304224230eca877530077004200ef008601df010d";

// Go's `hex.DecodeString`, for the tests of this package.
#[track_caller]
pub(super) fn hex_decode_string(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd length hex string");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("invalid byte in hex string"))
        .collect()
}

#[track_caller]
pub(super) fn new_dependency_descriptor_marshal_fixture() -> DependencyDescriptorExtension {
    let buf = hex_decode_string(DEPENDENCY_DESCRIPTOR_MARSHAL_FIXTURE);

    let mut parser = DependencyDescriptorExtension::default();
    parser.unmarshal(&buf).unwrap();
    let mut descriptor = parser.descriptor;
    assert!(
        descriptor.attached_structure.is_some(),
        "fixture did not contain a dependency structure"
    );

    let structure = descriptor.attached_structure.take();
    descriptor.active_decode_targets_bitmask = None;

    DependencyDescriptorExtension {
        descriptor,
        structure,
    }
}

#[track_caller]
pub(super) fn check_dependency_descriptor_marshal(
    buf: &[u8],
    structure: &Option<Arc<FrameDependencyStructure>>,
    frame_number: u16,
) {
    assert!(
        !buf.is_empty(),
        "marshal returned an empty dependency descriptor"
    );

    let mut parser = DependencyDescriptorExtension {
        descriptor: DependencyDescriptor::default(),
        structure: structure.clone(),
    };
    parser.unmarshal(buf).unwrap();
    let decoded = parser.descriptor;
    assert_eq!(frame_number, decoded.frame_number);
}

#[test]
fn test_dependency_descriptor_marshal_round_trip() {
    let mut extension = new_dependency_descriptor_marshal_fixture();

    let first = extension.marshal().unwrap();
    let first_copy = first.clone();
    check_dependency_descriptor_marshal(
        &first,
        &extension.structure,
        extension.descriptor.frame_number,
    );

    extension.descriptor.frame_number = extension.descriptor.frame_number.wrapping_add(1);
    let second = extension.marshal().unwrap();
    check_dependency_descriptor_marshal(
        &second,
        &extension.structure,
        extension.descriptor.frame_number,
    );

    assert_eq!(
        first_copy, first,
        "a later marshal modified a previously returned buffer"
    );
    assert_ne!(
        first, second,
        "different frame numbers produced identical descriptors"
    );
}

#[test]
fn test_dependency_descriptor_marshal_to() {
    let mut extension = new_dependency_descriptor_marshal_fixture();

    // every template of the fixture structure, i. e. the per-packet descriptors
    // the forwarding path marshals, has to match Marshal byte for byte and fit
    // the inline size the forwarding path reserves
    let structure = extension.structure.clone().unwrap();
    for (i, template) in structure.templates.iter().enumerate() {
        extension.descriptor.frame_dependencies = Some(template.clone());
        extension.descriptor.frame_number = i as u16;

        let want = extension.marshal().unwrap();
        assert!(want.len() <= MAX_INLINE_EXTENSION_SIZE);

        let mut scratch = [0u8; MAX_INLINE_EXTENSION_SIZE];
        let n = extension.marshal_to(&mut scratch).unwrap();
        assert_eq!(want, scratch[..n]);
        check_dependency_descriptor_marshal(&scratch[..n], &extension.structure, i as u16);
    }
}

#[test]
fn test_dependency_descriptor_marshal_to_buffer_too_small() {
    let mut extension = new_dependency_descriptor_marshal_fixture();

    let want = extension.marshal().unwrap();

    let err = extension
        .marshal_to(&mut vec![0u8; want.len() - 1])
        .unwrap_err();
    assert_eq!(Error::BufferTooSmall, err);

    let n = extension.marshal_to(&mut vec![0u8; want.len()]).unwrap();
    assert_eq!(want.len(), n);

    // a descriptor carrying the full dependency structure does not fit inline
    extension.descriptor.attached_structure = extension.structure.clone();
    let err = extension
        .marshal_to(&mut [0u8; MAX_INLINE_EXTENSION_SIZE])
        .unwrap_err();
    assert_eq!(Error::BufferTooSmall, err);

    let key_frame = extension.marshal().unwrap();
    assert!(key_frame.len() > MAX_INLINE_EXTENSION_SIZE);
}

// TestDependencyDescriptorMarshalToLargerDescriptors covers the 8 to 16 byte
// band, where the active decode targets bitmask and custom frame dependencies
// land and where the sequencer's own inline copy already spills.
#[test]
fn test_dependency_descriptor_marshal_to_larger_descriptors() {
    let mut extension = new_dependency_descriptor_marshal_fixture();
    let bitmask = 0x1ffu32;

    // the widest per-packet descriptor of the fixture, with the bitmask attached
    let mut widest = 0;
    let structure = extension.structure.clone().unwrap();
    for template in &structure.templates {
        extension.descriptor.frame_dependencies = Some(template.clone());
        extension.descriptor.active_decode_targets_bitmask = Some(bitmask);

        let want = extension.marshal().unwrap();
        widest = widest.max(want.len());

        let mut scratch = [0u8; MAX_INLINE_EXTENSION_SIZE];
        let n = extension.marshal_to(&mut scratch).unwrap();
        assert_eq!(want, scratch[..n]);
    }
    assert!(widest >= 8);

    // custom frame diffs and chain diffs, i. e. a frame that matches no template
    let mut custom = (*structure.templates[0]).clone();
    custom.frame_diffs = vec![1, 300, 4000];
    for i in 0..custom.chain_diffs.len() {
        custom.chain_diffs[i] = 200 + i as i64;
    }
    extension.descriptor.frame_dependencies = Some(Arc::new(custom));
    extension.descriptor.active_decode_targets_bitmask = None;

    let want = extension.marshal().unwrap();
    assert!(want.len() >= 8);
    assert!(want.len() <= MAX_INLINE_EXTENSION_SIZE);

    let mut scratch = [0u8; MAX_INLINE_EXTENSION_SIZE];
    let n = extension.marshal_to(&mut scratch).unwrap();
    assert_eq!(want, scratch[..n]);
    check_dependency_descriptor_marshal(
        &scratch[..n],
        &extension.structure,
        extension.descriptor.frame_number,
    );
}

// TestDependencyDescriptorMarshalAllocs is not ported. It counts the allocations of
// `MarshalTo` (none for a per-packet descriptor) and of the path that spills to `Marshal`
// (one, the slice) with Go's `testing.AllocsPerRun`. Counting allocations in a Rust test
// needs a global allocator of the test's own, which is `unsafe` to write (the workspace
// forbids that) and would be the allocator of every test in this crate. The test is also the
// only user here of `utils.RaceEnabled` from `pkg/sfu/utils`.

// BenchmarkDependencyDescriptorMarshal and BenchmarkDependencyDescriptorMarshalTo are left
// out, as all benchmarks are.
