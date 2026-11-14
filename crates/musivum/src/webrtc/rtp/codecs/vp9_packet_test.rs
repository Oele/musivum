// Ported from pion/rtp, codecs/vp9_packet_test.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; `TestVP9Payloader_Payload` is left out with the
// payloader; the case of a nil packet is left out, as a `&[u8]` is never nil; the test that
// counts allocations looks at the buffers of the packet instead (see there); the helper that
// trims a packet for reuse keeps the buffer of the payload as well.

use super::*;

// The bytes below are written as the original writes them, with the fields that are zero.
#[allow(clippy::identity_op)]
#[test]
fn test_vp9_packet_unmarshal() {
    #[derive(Default)]
    struct Case {
        b: &'static [u8],
        pkt: Vp9Packet,
        err: Option<Error>,
    }
    let cases = [
        // "Nil" of the original (b: nil, err: errNilPacket) cannot be written: a `&[u8]` is
        // never nil.
        (
            "Empty",
            Case {
                b: &[],
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "NonFlexible",
            Case {
                b: &[0x00, 0xAA],
                pkt: Vp9Packet {
                    payload: vec![0xAA],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "NonFlexiblePictureID",
            Case {
                b: &[0x80, 0x02, 0xAA],
                pkt: Vp9Packet {
                    i: true,
                    picture_id: 0x02,
                    payload: vec![0xAA],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "NonFlexiblePictureIDExt",
            Case {
                b: &[0x80, 0x81, 0xFF, 0xAA],
                pkt: Vp9Packet {
                    i: true,
                    picture_id: 0x01FF,
                    payload: vec![0xAA],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "NonFlexiblePictureIDExt_ShortPacket0",
            Case {
                b: &[0x80, 0x81],
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "NonFlexiblePictureIDExt_ShortPacket1",
            Case {
                b: &[0x80],
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "NonFlexibleLayerIndicePictureID",
            Case {
                b: &[0xA0, 0x02, 0x23, 0x01, 0xAA],
                pkt: Vp9Packet {
                    i: true,
                    l: true,
                    picture_id: 0x02,
                    tid: 0x01,
                    sid: 0x01,
                    d: true,
                    tl0picidx: 0x01,
                    payload: vec![0xAA],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "FlexibleLayerIndicePictureID",
            Case {
                b: &[0xB0, 0x02, 0x23, 0x01, 0xAA],
                pkt: Vp9Packet {
                    f: true,
                    i: true,
                    l: true,
                    picture_id: 0x02,
                    tid: 0x01,
                    sid: 0x01,
                    d: true,
                    payload: vec![0x01, 0xAA],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "NonFlexibleLayerIndicePictureID_ShortPacket0",
            Case {
                b: &[0xA0, 0x02, 0x23],
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "NonFlexibleLayerIndicePictureID_ShortPacket1",
            Case {
                b: &[0xA0, 0x02],
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "FlexiblePictureIDRefIndex",
            Case {
                b: &[0xD0, 0x02, 0x03, 0x04, 0xAA],
                pkt: Vp9Packet {
                    i: true,
                    p: true,
                    f: true,
                    picture_id: 0x02,
                    p_diff: vec![0x01, 0x02],
                    payload: vec![0xAA],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "FlexiblePictureIDRefIndex_TooManyPDiff",
            Case {
                b: &[0xD0, 0x02, 0x03, 0x05, 0x07, 0x09, 0x10, 0xAA],
                err: Some(Error::TooManyPDiff),
                ..Default::default()
            },
        ),
        (
            "FlexiblePictureIDRefIndexNoPayload",
            Case {
                b: &[0xD0, 0x02, 0x03, 0x04],
                pkt: Vp9Packet {
                    i: true,
                    p: true,
                    f: true,
                    picture_id: 0x02,
                    p_diff: vec![0x01, 0x02],
                    payload: vec![],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "FlexiblePictureIDRefIndex_ShortPacket0",
            Case {
                b: &[0xD0, 0x02, 0x03],
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "FlexiblePictureIDRefIndex_ShortPacket1",
            Case {
                b: &[0xD0, 0x02],
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "FlexiblePictureIDRefIndex_ShortPacket2",
            Case {
                b: &[0xD0],
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "ScalabilityStructureResolutionsNoPayload",
            Case {
                #[rustfmt::skip]
                b: &[
                    0x0A,
                    (1 << 5) | (1 << 4), // NS:1 Y:1 G:0
                    (640 >> 8) as u8, (640 & 0xff) as u8,
                    (360 >> 8) as u8, (360 & 0xff) as u8,
                    (1280 >> 8) as u8, (1280 & 0xff) as u8,
                    (720 >> 8) as u8, (720 & 0xff) as u8,
                ],
                pkt: Vp9Packet {
                    b: true,
                    v: true,
                    ns: 1,
                    y: true,
                    g: false,
                    ng: 0,
                    width: vec![640, 1280],
                    height: vec![360, 720],
                    payload: vec![],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "ScalabilityStructureNoPayload",
            Case {
                #[rustfmt::skip]
                b: &[
                    0x0A,
                    (1 << 5) | (0 << 4) | (1 << 3), // NS:1 Y:0 G:1
                    2,
                    (0 << 5) | (1 << 4) | (0 << 2), // T:0 U:1 R:0 -
                    (2 << 5) | (0 << 4) | (1 << 2), // T:2 U:0 R:1 -
                    33,
                ],
                pkt: Vp9Packet {
                    b: true,
                    v: true,
                    ns: 1,
                    y: false,
                    g: true,
                    ng: 2,
                    pgtid: vec![0, 2],
                    pgu: vec![true, false],
                    pgp_diff: vec![vec![], vec![33]],
                    payload: vec![],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "ScalabilityStructureReserved",
            Case {
                #[rustfmt::skip]
                b: &[
                    0x0A,
                    (1 << 5) | (0 << 4) | (0 << 3) | (1 << 2) | (1 << 1) | 1, // NS:1 Y:0 G:0, reserved fields set to 1
                ],
                pkt: Vp9Packet {
                    b: true,
                    v: true,
                    ns: 1,
                    y: false,
                    g: false,
                    ng: 0,
                    payload: vec![],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "ScalabilityStructure_ShortPacket0",
            Case {
                b: &[0x0A, 0x10],
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "ScalabilityMissingWidth",
            Case {
                b: b"200",
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "ScalabilityMissingNG",
            Case {
                b: b"b00800000000",
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "ScalabilityMissingTemporalLayerIDs",
            Case {
                b: b"20H0",
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
        (
            "ScalabilityMissingReferenceIndices",
            Case {
                b: b"20H007",
                err: Some(Error::ShortPacket),
                ..Default::default()
            },
        ),
    ];
    for (name, test_case) in cases {
        let mut p = Vp9Packet::default();
        let raw = p.unmarshal(test_case.b);
        match test_case.err {
            None => {
                // assert.NotNil(t, raw): what is returned is the payload that is kept.
                assert_eq!(Ok(&p.payload[..]), raw, "{name}");
                assert_eq!(test_case.pkt, p, "{name}");
            }
            Some(err) => {
                assert_eq!(Err(err), raw, "{name}");
            }
        }
    }
}

// Two descriptors that populate every slice field.
// The first also carries layer info and uses 3 spatial layers; the
// second omits layer info and shrinks to 2 layers, so a reused packet must
// shed the first packet's extra layer and layer-info values.
#[rustfmt::skip]
const VP9_SS3_LAYERS: [u8; 21] = [0xFE, 0x11, 0x51, 0x02, 0x58, 0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0x01, 0x14, 0x03, 0xAA];
#[rustfmt::skip]
const VP9_SS2_LAYERS: [u8; 16] = [0xDE, 0x12, 0x04, 0x38, 0, 7, 0, 8, 0, 9, 0, 10, 0x01, 0x14, 0x05, 0xBB];

// vp9TrimForReuse zeroes a VP9Packet while keeping its slices' capacity, the
// reset a reuse-oriented caller performs between Unmarshal calls.
// (Here the payload is such a buffer as well.)
fn vp9_trim_for_reuse(p: &mut Vp9Packet) {
    fn trim<T>(v: &mut Vec<T>) -> Vec<T> {
        let mut v = std::mem::take(v);
        v.clear();
        v
    }
    *p = Vp9Packet {
        p_diff: trim(&mut p.p_diff),
        width: trim(&mut p.width),
        height: trim(&mut p.height),
        pgtid: trim(&mut p.pgtid),
        pgu: trim(&mut p.pgu),
        pgp_diff: trim(&mut p.pgp_diff),
        payload: trim(&mut p.payload),
        ..Default::default()
    };
}

// Unmarshalling into a reused VP9Packet must produce exactly the same packet
// as a fresh parse.
#[test]
fn test_vp9_packet_unmarshal_reused_packet() {
    let mut fresh = Vp9Packet::default();
    let res = fresh.unmarshal(&VP9_SS2_LAYERS);
    assert!(res.is_ok());

    // After a trim, every field must match a fresh parse.
    let mut reused = Vp9Packet::default();
    let res = reused.unmarshal(&VP9_SS3_LAYERS);
    assert!(res.is_ok());
    vp9_trim_for_reuse(&mut reused);
    let res = reused.unmarshal(&VP9_SS2_LAYERS);
    assert!(res.is_ok());
    assert_eq!(fresh, reused);
}

// Where the buffers of a packet are and how large: they move or grow when one is allocated.
// The lists inside `pgp_diff` are not among them: those are made anew for a packet that has
// them (see the header of vp9_packet.rs).
fn vp9_buffers(p: &Vp9Packet) -> [(usize, usize); 7] {
    [
        (p.p_diff.as_ptr() as usize, p.p_diff.capacity()),
        (p.width.as_ptr() as usize, p.width.capacity()),
        (p.height.as_ptr() as usize, p.height.capacity()),
        (p.pgtid.as_ptr() as usize, p.pgtid.capacity()),
        (p.pgu.as_ptr() as usize, p.pgu.capacity()),
        (p.pgp_diff.as_ptr() as usize, p.pgp_diff.capacity()),
        (p.payload.as_ptr() as usize, p.payload.capacity()),
    ]
}

// The original counts the allocations of 100 runs with `testing.AllocsPerRun` and wants none.
// A Rust test cannot count allocations without replacing the allocator of the whole test
// binary. It checks what it can see: after the first run (which `AllocsPerRun` does not count
// either) the buffers of the packet are neither moved nor grown.
#[test]
fn test_vp9_packet_unmarshal_reused_packet_does_not_allocate() {
    let mut p = Vp9Packet::default();
    vp9_trim_for_reuse(&mut p);
    let res = p.unmarshal(&VP9_SS3_LAYERS);
    assert!(res.is_ok());
    let buffers = vp9_buffers(&p);
    for _ in 0..100 {
        vp9_trim_for_reuse(&mut p);
        let res = p.unmarshal(&VP9_SS3_LAYERS);
        assert!(res.is_ok());
        assert_eq!(buffers, vp9_buffers(&p));
    }
}

// TestVP9Payloader_Payload of the original is not here: `VP9Payloader` is not ported.

#[test]
fn test_vp9_is_partition_head() {
    let vp9 = Vp9Packet::default();
    // SmallPacket
    assert!(
        !vp9.is_partition_head(&[]),
        "Small packet should not be the head of a new partition"
    );
    // NormalPacket
    assert!(
        vp9.is_partition_head(&[0x18, 0x00, 0x00]),
        "VP9 RTP packet with B flag should be head of a new partition",
    );
    assert!(
        !vp9.is_partition_head(&[0x10, 0x00, 0x00]),
        "VP9 RTP packet without B flag should not be head of a new partition",
    );
}
