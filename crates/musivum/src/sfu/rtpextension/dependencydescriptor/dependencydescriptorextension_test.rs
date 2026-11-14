// Ported from LiveKit,
// pkg/sfu/rtpextension/dependencydescriptor/dependencydescriptorextension_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Where the original compares pointers (`require.Same`,
// `require.NotSame`) the `Arc`s are compared.

use std::sync::Arc;

use super::super::dependencydescriptorextension_benchmark_test::{
    hex_decode_string, new_dependency_descriptor_marshal_fixture,
};
use super::*;

#[test]
fn test_dependency_descriptor_unmarshal() {
    // hex bytes from traffic capture
    let hexes = [
        "c1017280081485214eafffaaaa863cf0430c10c302afc0aaa0063c00430010c002a000a80006000040001d954926e082b04a0941b820ac1282503157f974000ca864330e222222eca8655304224230eca877530077004200ef008601df010d",
        "86017340fc",
        "46017340fc",
        "c3017540fc",
        "88017640fc",
        "48017640fc",
        "c2017840fc",
        //
        "c1017280081485214eafffaaaa863cf0430c10c302afc0aaa0063c00430010c002a000a80006000040001d954926e082b04a0941b820ac1282503157f974000ca864330e222222eca8655304224230eca877530077004200ef008601df010d",
        "860173",
        "460173",
        "8b0174",
        "0b0174",
        "0b0174",
        "c30175",
    ];

    let mut structure: Option<Arc<FrameDependencyStructure>> = None;

    for h in hexes {
        let buf = hex_decode_string(h);

        let mut d = DependencyDescriptorExtension {
            structure: structure.clone(),
            descriptor: DependencyDescriptor::default(),
        };
        d.unmarshal(&buf).unwrap();
        let dd_val = d.descriptor;
        if dd_val.attached_structure.is_some() {
            structure = dd_val.attached_structure.clone();
        }

        println!("{dd_val}");
    }
}

// Unmarshal into a descriptor whose FrameDependencies and Resolution are set writes into that
// storage, gives the same result as a fresh descriptor, and leaves the structure's templates intact
#[test]
fn test_dependency_descriptor_unmarshal_into_storage() {
    let hexes = [
        "c1017280081485214eafffaaaa863cf0430c10c302afc0aaa0063c00430010c002a000a80006000040001d954926e082b04a0941b820ac1282503157f974000ca864330e222222eca8655304224230eca877530077004200ef008601df010d",
        "86017340fc",
        "46017340fc",
        "c3017540fc",
        "88017640fc",
        "48017640fc",
        "c2017840fc",
    ];

    let mut structure: Option<Arc<FrameDependencyStructure>> = None;
    let mut fd = Arc::new(FrameDependencyTemplate::default());
    let mut res = RenderResolution::default();
    for h in hexes {
        let buf = hex_decode_string(h);

        let mut fresh = DependencyDescriptorExtension {
            structure: structure.clone(),
            descriptor: DependencyDescriptor::default(),
        };
        fresh.unmarshal(&buf).unwrap();
        let fresh = fresh.descriptor;

        // The descriptor holds the storage while it is read into, where the original points
        // at it; it is taken back below.
        let fd_ptr = Arc::as_ptr(&fd);
        let mut reused = DependencyDescriptorExtension {
            structure: structure.clone(),
            descriptor: DependencyDescriptor {
                frame_dependencies: Some(fd),
                resolution: Some(res),
                ..Default::default()
            },
        };
        reused.unmarshal(&buf).unwrap();
        let reused = reused.descriptor;

        assert!(std::ptr::eq(
            fd_ptr,
            Arc::as_ptr(reused.frame_dependencies.as_ref().unwrap())
        ));
        assert_eq!(fresh.frame_dependencies, reused.frame_dependencies);
        assert_eq!(fresh.resolution, reused.resolution);
        assert_eq!(fresh.to_string(), reused.to_string());

        if fresh.attached_structure.is_some() {
            structure = fresh.attached_structure.clone();
        }

        fd = reused.frame_dependencies.unwrap();
        res = reused.resolution.unwrap_or(res);
    }

    // the templates are copied out, not shared
    for template in &structure.unwrap().templates {
        assert!(!Arc::ptr_eq(template, &fd));
    }
}

// a caller may hand Unmarshal a descriptor that still points at one of the structure's templates,
// custom fields of the packet must not change that template
#[test]
fn test_dependency_descriptor_unmarshal_does_not_write_into_template() {
    let fixture = new_dependency_descriptor_marshal_fixture();
    let structure = fixture.structure;

    // a per-packet descriptor using a template with frame diffs, with one custom diff
    let mut template: Option<Arc<FrameDependencyTemplate>> = None;
    for tmpl in &structure.as_ref().unwrap().templates {
        if !tmpl.frame_diffs.is_empty() {
            template = Some(tmpl.clone());
            break;
        }
    }
    let template = template.expect("no template with frame diffs");
    let want = template.frame_diffs.clone();

    let mut custom = (*template).clone();
    custom.frame_diffs[0] += 2;
    let custom = Arc::new(custom);
    let buf = DependencyDescriptorExtension {
        descriptor: DependencyDescriptor {
            frame_number: 7,
            frame_dependencies: Some(custom.clone()),
            ..Default::default()
        },
        structure: structure.clone(),
    }
    .marshal()
    .unwrap();

    let mut parsed = DependencyDescriptorExtension {
        structure: structure.clone(),
        descriptor: DependencyDescriptor {
            frame_dependencies: Some(template.clone()),
            ..Default::default()
        },
    };
    parsed.unmarshal(&buf).unwrap();
    let parsed = parsed.descriptor;
    assert_eq!(
        custom.frame_diffs,
        parsed.frame_dependencies.as_ref().unwrap().frame_diffs
    );
    assert!(!Arc::ptr_eq(
        &template,
        parsed.frame_dependencies.as_ref().unwrap()
    ));
    assert_eq!(want, template.frame_diffs);
}
