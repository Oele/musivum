//! New for musivum: the port held against the original, byte for byte.
//!
//! `testdata/reference_output.txt` is what LiveKit's own code at the pin writes and reads for
//! a set of inputs. `testdata/reference_output_test.go` printed it, and says how it is run.
//! The test here makes the same inputs in the same order and expects the same lines. Where
//! the port knowingly differs (slices that are nil in Go, `nil_slices` below) it says so.

use std::collections::HashMap;
use std::sync::Arc;

use super::dependencydescriptorextension_benchmark_test::{
    DEPENDENCY_DESCRIPTOR_MARSHAL_FIXTURE, hex_decode_string,
    new_dependency_descriptor_marshal_fixture,
};
use super::*;

use DecodeTargetIndication::{Discardable as D, NotPresent as N, Required as R, Switch as S};

// The lines of the reference output by their names, each to be taken once.
struct Reference(HashMap<&'static str, &'static str>);

impl Reference {
    fn load() -> Self {
        let mut lines = HashMap::new();
        for line in include_str!("testdata/reference_output.txt").lines() {
            let line = line.strip_prefix("REF ").expect("not a line of the output");
            let (name, value) = line.split_once(": ").expect("a line without a name");
            assert!(lines.insert(name, value).is_none(), "{name} is there twice");
        }
        Reference(lines)
    }

    #[track_caller]
    fn take(&mut self, name: &str) -> &'static str {
        self.0
            .remove(name)
            .unwrap_or_else(|| panic!("no line {name} in the reference output"))
    }
}

fn hex_encode_to_string(buf: &[u8]) -> String {
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

// Go's `%v` of an error that may be nil.
fn go_error<T>(result: &Result<T, Error>) -> String {
    match result {
        Ok(_) => "<nil>".to_string(),
        Err(err) => err.to_string(),
    }
}

// Go's `%v` of a slice.
fn go_slice<T: ToString>(s: &[T]) -> String {
    let s: Vec<String> = s.iter().map(|v| v.to_string()).collect();
    format!("[{}]", s.join(" "))
}

// what Marshal gives, and what Unmarshal makes of that
fn marshal_line(ext: &DependencyDescriptorExtension, active_chains: Option<u32>) -> String {
    let buf = match active_chains {
        Some(active_chains) => ext.marshal_with_active_chains(active_chains),
        None => ext.marshal(),
    };
    let buf = match buf {
        Ok(buf) => buf,
        Err(err) => return format!("error: {err}"),
    };
    let mut out = DependencyDescriptorExtension {
        descriptor: DependencyDescriptor::default(),
        structure: ext.structure.clone(),
    };
    match out.unmarshal(&buf) {
        Ok(n) => format!("{} | {n} | {}", hex_encode_to_string(&buf), out.descriptor),
        Err(err) => format!("{} | unmarshal error: {err}", hex_encode_to_string(&buf)),
    }
}

#[track_caller]
fn ref_marshal(
    reference: &mut Reference,
    name: &str,
    ext: &DependencyDescriptorExtension,
    active_chains: Option<u32>,
) {
    assert_eq!(
        reference.take(name),
        marshal_line(ext, active_chains),
        "{name}"
    );
}

// what Unmarshal gives, and what it leaves in the descriptor
#[track_caller]
fn ref_unmarshal(
    reference: &mut Reference,
    name: &str,
    h: &str,
    structure: &Option<Arc<FrameDependencyStructure>>,
    out: DependencyDescriptor,
) -> DependencyDescriptor {
    let buf = hex_decode_string(h);
    let mut ext = DependencyDescriptorExtension {
        descriptor: out,
        structure: structure.clone(),
    };
    let n = ext.unmarshal(&buf);
    let line = format!(
        "{} | {} | {}",
        n.clone().unwrap_or(0),
        go_error(&n),
        ext.descriptor
    );
    assert_eq!(reference.take(name), line, "{name}");
    ext.descriptor
}

fn template(
    spatial_id: i64,
    temporal_id: i64,
    decode_target_indications: &[DecodeTargetIndication],
    frame_diffs: &[i64],
) -> Arc<FrameDependencyTemplate> {
    Arc::new(FrameDependencyTemplate {
        spatial_id,
        temporal_id,
        decode_target_indications: decode_target_indications.to_vec(),
        frame_diffs: frame_diffs.to_vec(),
        chain_diffs: Vec::new(),
    })
}

fn hand_structure() -> Arc<FrameDependencyStructure> {
    Arc::new(FrameDependencyStructure {
        structure_id: 5,
        num_decode_targets: 3,
        num_chains: 0,
        templates: vec![
            template(0, 0, &[S, S, S], &[]),
            template(0, 0, &[S, S, S], &[4]),
            template(0, 1, &[N, D, S], &[2]),
            template(0, 2, &[N, N, D], &[1]),
            template(0, 2, &[N, N, R], &[2, 1]),
        ],
        ..Default::default()
    })
}

#[test]
fn test_reference_output() {
    let mut reference = Reference::load();

    capture(&mut reference);
    fixture(&mut reference);
    hand(&mut reference);
    nil_slices(&mut reference);
    unreadable(&mut reference);
    bit_streams(&mut reference);

    let mut left: Vec<_> = reference.0.keys().collect();
    left.sort();
    assert!(
        left.is_empty(),
        "lines that nothing was held against: {left:?}"
    );
}

// 1. the packets of TestDependencyDescriptorUnmarshal, one after the other
fn capture(reference: &mut Reference) {
    let hexes = [
        DEPENDENCY_DESCRIPTOR_MARSHAL_FIXTURE,
        "86017340fc",
        "46017340fc",
        "c3017540fc",
        "88017640fc",
        "48017640fc",
        "c2017840fc",
        //
        DEPENDENCY_DESCRIPTOR_MARSHAL_FIXTURE,
        "860173",
        "460173",
        "8b0174",
        "0b0174",
        "0b0174",
        "c30175",
    ];
    let mut structure = None;
    for (i, h) in hexes.into_iter().enumerate() {
        let out = ref_unmarshal(
            reference,
            &format!("capture{i}"),
            h,
            &structure,
            DependencyDescriptor::default(),
        );
        if out.attached_structure.is_some() {
            structure = out.attached_structure;
        }
    }
}

// 2. the structure of the fixture, a descriptor for each of its templates
fn fixture(reference: &mut Reference) {
    let mut ext = new_dependency_descriptor_marshal_fixture();
    let structure = ext.structure.clone().unwrap();
    ref_marshal(reference, "fixture", &ext, None);
    for (i, template) in structure.templates.iter().enumerate() {
        ext.descriptor.frame_dependencies = Some(template.clone());
        ext.descriptor.frame_number = i as u16;
        ref_marshal(reference, &format!("template{i}"), &ext, None);
    }
    let all = 0x1ffu32;
    for (i, template) in structure.templates.iter().enumerate() {
        ext.descriptor.frame_dependencies = Some(template.clone());
        ext.descriptor.frame_number = i as u16;
        ext.descriptor.active_decode_targets_bitmask = Some(all);
        ref_marshal(reference, &format!("template{i}+mask"), &ext, None);
    }
    let partial = 0x03fu32;
    ext.descriptor.frame_dependencies = Some(structure.templates[3].clone());
    ext.descriptor.frame_number = 1003;
    ext.descriptor.active_decode_targets_bitmask = Some(partial);
    ref_marshal(reference, "template3+partialmask", &ext, None);

    // custom frame diffs and chain diffs, with all chains active and with some
    let mut custom = (*structure.templates[0]).clone();
    custom.frame_diffs = vec![1, 300, 4000];
    for (i, chain_diff) in custom.chain_diffs.iter_mut().enumerate() {
        *chain_diff = 200 + i as i64;
    }
    ext.descriptor.frame_dependencies = Some(Arc::new(custom));
    ext.descriptor.active_decode_targets_bitmask = None;
    ext.descriptor.frame_number = 0xfffe;
    ref_marshal(reference, "custom", &ext, None);
    for active_chains in [0, 1, 2, 4, 5, 7, 0xfffffff8u32] {
        ref_marshal(
            reference,
            &format!("custom+chains{active_chains:x}"),
            &ext,
            Some(active_chains),
        );
    }

    // custom decode target indications, on a template of the last layer
    let mut custom = (*structure.templates[14]).clone();
    custom.decode_target_indications[8] = R;
    custom.decode_target_indications[0] = S;
    ext.descriptor.frame_dependencies = Some(Arc::new(custom));
    ref_marshal(reference, "customdtis", &ext, None);

    // a key frame: the structure attached
    let mut ext = new_dependency_descriptor_marshal_fixture();
    ext.descriptor.attached_structure = ext.structure.clone();
    ref_marshal(reference, "keyframe", &ext, None);
    // The original writes the captured packet as it was captured, and so does the port.
    assert_eq!(
        DEPENDENCY_DESCRIPTOR_MARSHAL_FIXTURE,
        hex_encode_to_string(&ext.marshal().unwrap())
    );
    let size = ext.descriptor.marshal_size();
    assert_eq!(
        reference.take("keyframe.MarshalSize"),
        format!("{} | {}", size.clone().unwrap_or(0), go_error(&size))
    );
    ext.descriptor.active_decode_targets_bitmask = Some(all);
    ref_marshal(reference, "keyframe+mask", &ext, None);
    ext.descriptor.active_decode_targets_bitmask = Some(partial);
    ref_marshal(reference, "keyframe+partialmask", &ext, None);
    let size = ext.descriptor.marshal_size();
    assert_eq!(
        reference.take("keyframe+partialmask.MarshalSize"),
        format!("{} | {}", size.clone().unwrap_or(0), go_error(&size))
    );
}

fn hand_extension(descriptor: DependencyDescriptor) -> DependencyDescriptorExtension {
    DependencyDescriptorExtension {
        descriptor,
        structure: Some(hand_structure()),
    }
}

// 3. a structure made by hand: no chains, no resolutions, a structure id
fn hand(reference: &mut Reference) {
    let hs = hand_structure();
    let mut hand = |name: &str, descriptor: DependencyDescriptor| {
        ref_marshal(reference, name, &hand_extension(descriptor), None);
    };
    hand(
        "hand.key",
        DependencyDescriptor {
            first_packet_in_frame: true,
            last_packet_in_frame: true,
            frame_number: 1,
            frame_dependencies: Some(hs.templates[0].clone()),
            attached_structure: Some(hs.clone()),
            ..Default::default()
        },
    );
    let mask = 0x3u32;
    hand(
        "hand.key+mask",
        DependencyDescriptor {
            first_packet_in_frame: true,
            frame_number: 2,
            frame_dependencies: Some(hs.templates[0].clone()),
            attached_structure: Some(hs.clone()),
            active_decode_targets_bitmask: Some(mask),
            ..Default::default()
        },
    );
    for (i, template) in hs.templates.iter().enumerate() {
        hand(
            &format!("hand.template{i}"),
            DependencyDescriptor {
                last_packet_in_frame: true,
                frame_number: 10 + i as u16,
                frame_dependencies: Some(template.clone()),
                ..Default::default()
            },
        );
    }
    for fdiff in [1, 15, 16, 17, 255, 256, 257, 4095, 4096] {
        hand(
            &format!("hand.fdiff{fdiff}"),
            DependencyDescriptor {
                frame_number: 0x8001,
                frame_dependencies: Some(template(0, 1, &[N, D, S], &[fdiff])),
                ..Default::default()
            },
        );
    }
    hand(
        "hand.nofdiffs",
        DependencyDescriptor {
            frame_number: 77,
            frame_dependencies: Some(template(0, 2, &[N, N, D], &[])),
            ..Default::default()
        },
    );
    hand(
        "hand.customdtis",
        DependencyDescriptor {
            frame_number: 78,
            frame_dependencies: Some(template(0, 2, &[S, D, R], &[1])),
            ..Default::default()
        },
    );
    hand(
        "hand.customdtis+fdiffs+mask",
        DependencyDescriptor {
            frame_number: 79,
            active_decode_targets_bitmask: Some(mask),
            frame_dependencies: Some(template(0, 1, &[R, R, R], &[7, 300])),
            ..Default::default()
        },
    );
    hand(
        "hand.nolayer",
        DependencyDescriptor {
            frame_number: 80,
            frame_dependencies: Some(template(1, 0, &[S, S, S], &[])),
            ..Default::default()
        },
    );
}

// 4. slices of the descriptor that are nil, and one that is empty
//
// This is where the port cannot be the original: a `Vec` is never nil. See `calculate_match`.
fn nil_slices(reference: &mut Reference) {
    let mask = 0x3u32;

    // Decode target indications that are nil in Go are empty here, and are taken the same
    // way: as the template has them.
    ref_marshal(
        reference,
        "nil.dtis",
        &hand_extension(DependencyDescriptor {
            frame_number: 91,
            frame_dependencies: Some(template(0, 1, &[], &[2])),
            ..Default::default()
        }),
        None,
    );
    ref_marshal(
        reference,
        "nil.dtis+mask",
        &hand_extension(DependencyDescriptor {
            frame_number: 91,
            active_decode_targets_bitmask: Some(mask),
            frame_dependencies: Some(template(0, 1, &[], &[2])),
            ..Default::default()
        }),
        None,
    );

    // Frame diffs that are nil in Go: the original sends the template's frame diffs ([1]),
    // the port says that the frame has none, as the original does for frame diffs that are
    // empty but not nil ("hand.nofdiffs" above).
    let line = marshal_line(
        &hand_extension(DependencyDescriptor {
            frame_number: 90,
            frame_dependencies: Some(template(0, 2, &[N, N, D], &[])),
            ..Default::default()
        }),
        None,
    );
    assert_eq!(
        "08005a | 3 | DependencyDescriptor{FirstPacketInFrame: false, LastPacketInFrame: false, FrameNumber: 90, FrameDependencies: {SpatialId:0 TemporalId:2 DecodeTargetIndications:[- - D] FrameDiffs:[1] ChainDiffs:[]}, Resolution: -, ActiveDecodeTargetsBitmask: -, AttachedStructure: <nil>}",
        reference.take("nil.fdiffs")
    );
    assert_eq!(
        "08005a10 | 4 | DependencyDescriptor{FirstPacketInFrame: false, LastPacketInFrame: false, FrameNumber: 90, FrameDependencies: {SpatialId:0 TemporalId:2 DecodeTargetIndications:[- - D] FrameDiffs:[] ChainDiffs:[]}, Resolution: -, ActiveDecodeTargetsBitmask: -, AttachedStructure: <nil>}",
        line
    );

    // Both nil in Go: the same difference.
    let line = marshal_line(
        &hand_extension(DependencyDescriptor {
            frame_number: 92,
            frame_dependencies: Some(template(0, 2, &[], &[])),
            ..Default::default()
        }),
        None,
    );
    assert_eq!(
        "08005c | 3 | DependencyDescriptor{FirstPacketInFrame: false, LastPacketInFrame: false, FrameNumber: 92, FrameDependencies: {SpatialId:0 TemporalId:2 DecodeTargetIndications:[- - D] FrameDiffs:[1] ChainDiffs:[]}, Resolution: -, ActiveDecodeTargetsBitmask: -, AttachedStructure: <nil>}",
        reference.take("nil.both")
    );
    assert_eq!(
        "08005c10 | 4 | DependencyDescriptor{FirstPacketInFrame: false, LastPacketInFrame: false, FrameNumber: 92, FrameDependencies: {SpatialId:0 TemporalId:2 DecodeTargetIndications:[- - D] FrameDiffs:[] ChainDiffs:[]}, Resolution: -, ActiveDecodeTargetsBitmask: -, AttachedStructure: <nil>}",
        line
    );

    // Decode target indications that are empty but not nil in Go: the original sets the flag
    // for custom ones and writes none, which it cannot read back. Here they are taken as nil.
    let line = marshal_line(
        &hand_extension(DependencyDescriptor {
            frame_number: 93,
            active_decode_targets_bitmask: Some(mask),
            frame_dependencies: Some(template(0, 1, &[], &[2])),
            ..Default::default()
        }),
        None,
    );
    assert_eq!(
        "07005d63 | unmarshal error: EOF",
        reference.take("empty.dtis+mask")
    );
    assert_eq!(
        "07005d43 | 4 | DependencyDescriptor{FirstPacketInFrame: false, LastPacketInFrame: false, FrameNumber: 93, FrameDependencies: {SpatialId:0 TemporalId:1 DecodeTargetIndications:[- D S] FrameDiffs:[2] ChainDiffs:[]}, Resolution: -, ActiveDecodeTargetsBitmask: 11, AttachedStructure: <nil>}",
        line
    );
}

// 5. packets that cannot be read, and what is left in the descriptor
fn unreadable(reference: &mut Reference) {
    let fixture = new_dependency_descriptor_marshal_fixture().structure;
    let hs = Some(hand_structure());
    let new = DependencyDescriptor::default;
    for n in [0, 1, 2, 3, 4, 5, 6, 7, 12, 30, 60, 85, 90, 94] {
        ref_unmarshal(
            reference,
            &format!("cut{n}"),
            &DEPENDENCY_DESCRIPTOR_MARSHAL_FIXTURE[..2 * n],
            &None,
            new(),
        );
    }
    let set = || DependencyDescriptor {
        first_packet_in_frame: true,
        last_packet_in_frame: true,
        frame_number: 9,
        ..Default::default()
    };
    ref_unmarshal(reference, "cut0.set", "", &None, set());
    ref_unmarshal(reference, "cut1.set", "40", &None, set());
    ref_unmarshal(reference, "nostructure", "860173", &None, new());
    ref_unmarshal(reference, "templateindex", "8f0173", &fixture, new());
    ref_unmarshal(reference, "templateindex.last", "bf0173", &fixture, new());
    ref_unmarshal(reference, "customdtis.cut", "86017320", &fixture, new());
    ref_unmarshal(reference, "customfdiffs.none", "86017310", &fixture, new());
    ref_unmarshal(reference, "customfdiffs.cut", "86017312", &fixture, new());
    ref_unmarshal(reference, "customchains.cut", "8601730800", &fixture, new());
    ref_unmarshal(reference, "mask.cut", "86017340", &fixture, new());
    ref_unmarshal(reference, "hand.structureid", "480003", &hs, new());
    ref_unmarshal(reference, "hand.structureid.wrap", "040003", &hs, new());
    // 65 templates; nine temporal layers; five spatial layers
    ref_unmarshal(
        reference,
        "toomanytemplates",
        "c00001800000000000000000000000000000000000000000",
        &None,
        new(),
    );
    ref_unmarshal(reference, "toomanytemporal", "c0000180005555", &None, new());
    ref_unmarshal(reference, "toomanyspatial", "c000018000aa", &None, new());
    // structures made by hand that a packet cannot give: a spatial layer without a
    // resolution; templates that are shorter than the structure says
    let odd = Some(Arc::new(FrameDependencyStructure {
        num_decode_targets: 1,
        resolutions: vec![RenderResolution {
            width: 640,
            height: 360,
        }],
        templates: vec![template(0, 0, &[S], &[]), template(1, 0, &[S], &[])],
        ..Default::default()
    }));
    ref_unmarshal(reference, "spatiallayer.ok", "000003", &odd, new());
    ref_unmarshal(reference, "spatiallayer", "010003", &odd, new());
    let short = Some(Arc::new(FrameDependencyStructure {
        num_decode_targets: 2,
        num_chains: 1,
        templates: vec![template(0, 0, &[S], &[])],
        ..Default::default()
    }));
    ref_unmarshal(reference, "dtimismatch", "00000320ff", &short, new());
    ref_unmarshal(reference, "chaindiffsmismatch", "00000308ff", &short, new());
}

// 6. the bit streams
fn bit_streams(reference: &mut Reference) {
    for (val, num_values) in [
        (0u32, 1u32),
        (0, 2),
        (1, 2),
        (0, 3),
        (1, 3),
        (2, 3),
        (0, 5),
        (2, 5),
        (3, 5),
        (4, 5),
        (5, 10),
        (6, 10),
        (9, 10),
        (0, 33),
        (30, 33),
        (31, 33),
        (32, 33),
        (1 << 30, 1 << 31),
        ((1 << 31) - 1, 1 << 31),
        (5, 5),
        (0, 0),
        (7, (1 << 31) + 1),
    ] {
        let mut buf = [0xa5u8; 5];
        let mut w = BitStreamWriter::new(&mut buf);
        w.write_bits(1, 3).unwrap();
        let err = w.write_non_symmetric(val, num_values);
        let rem = w.remaining_bits();
        let mut r = BitStreamReader::new(&buf);
        r.read_bits(3).unwrap();
        let v = r.read_non_symmetric(num_values);
        let name = format!("nonsymmetric {val}/{num_values}");
        assert_eq!(
            reference.take(&name),
            format!(
                "{} | {} | {rem} | {} | {} | {} | {} | {}",
                hex_encode_to_string(&buf),
                go_error(&err),
                size_non_symmetric_bits(val, num_values),
                v.clone().unwrap_or(0),
                go_error(&v),
                r.remaining_bits(),
                r.bytes_read()
            ),
            "{name}"
        );
    }

    let mut buf = [0x5au8; 13];
    let mut w = BitStreamWriter::new(&mut buf);
    let mut errs = vec![
        w.write_bits(0x5, 3),
        w.write_bits(0xfedcba9876543210, 64),
        w.write_bits(0, 1),
        w.write_bits(0x1234, 13),
        w.write_bits(0xff, 8),
        w.write_bits(0x3, 7),
        w.write_bits(0xffff, 2),
        w.write_bits(0, 3),
        w.write_bits(1, 4),
    ];
    let rem = w.remaining_bits();
    errs.push(w.write_bits(1, 1));
    errs.push(w.write_bits(0xff, 3));
    let rem_after = w.remaining_bits();
    let errs: Vec<String> = errs.iter().map(go_error).collect();
    assert_eq!(
        reference.take("writebits"),
        format!(
            "{} | {} | {rem} | {rem_after}",
            hex_encode_to_string(&buf),
            go_slice(&errs)
        )
    );

    let mut r = BitStreamReader::new(&buf);
    let (mut vals, mut errs, mut rems, mut read) = (vec![], vec![], vec![], vec![]);
    for n in [3, 64, 1, 13, 8, 7, 0, 2, 3, 0, 2, -1, 65, 1, 2, 0, 1] {
        let v = r.read_bits(n);
        vals.push(v.clone().unwrap_or(0));
        errs.push(go_error(&v));
        rems.push(r.remaining_bits());
        read.push(r.bytes_read());
    }
    assert_eq!(
        reference.take("readbits"),
        format!(
            "{} | {} | {} | {} | {}",
            go_slice(&vals),
            go_slice(&errs),
            go_slice(&rems),
            go_slice(&read),
            r.ok()
        )
    );
    r.invalidate();
    let v = r.read_bits(0);
    assert_eq!(
        reference.take("invalidate"),
        format!(
            "{} | {} | {} | {} | {}",
            v.clone().unwrap_or(0),
            go_error(&v),
            r.remaining_bits(),
            r.ok(),
            r.bytes_read()
        )
    );
}
