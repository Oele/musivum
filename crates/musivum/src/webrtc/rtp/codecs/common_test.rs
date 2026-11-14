// Ported from pion/rtp, codecs/common_test.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; `TestCommon_Min` is left out with `minInt`; of
// `TestZeroAllocations` only the VP9 packet is here, the other depacketizers are not ported,
// and it looks at the buffers of the packet instead of counting allocations (see there).

use crate::webrtc::rtp::codecs::Vp9Packet;

// TestCommon_Min of the original is not here: `minInt` is not ported.

// The original counts the allocations of 10 runs with `testing.AllocsPerRun` and wants none.
// A Rust test cannot count allocations without replacing the allocator of the whole test
// binary. It checks what it can see: after the first run (which `AllocsPerRun` does not count
// either) the buffer of the payload is neither moved nor grown, and no other buffer is made.
#[test]
fn test_zero_allocations() {
    struct Tst {
        packet: Vp9Packet,
        data: &'static [u8],
    }
    let tests = [
        // &VP8Packet{}, &H264Packet{} and &AV1Packet{} of the original are not ported.
        Tst {
            packet: Vp9Packet::default(),
            data: &[0xA0, 0x02, 0x23, 0x01, 0xAA],
        },
    ];

    fn buffers(p: &Vp9Packet) -> [(usize, usize); 7] {
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

    for mut test in tests {
        let run = |packet: &mut Vp9Packet| {
            packet.set_zero_allocation(true);
            let res = packet.unmarshal(test.data);
            assert!(res.is_ok());
        };
        run(&mut test.packet);
        let before = buffers(&test.packet);
        for _ in 0..10 {
            run(&mut test.packet);
            assert_eq!(before, buffers(&test.packet));
        }
        // Only the payload has a buffer.
        let allocated = before.iter().filter(|(_, capacity)| *capacity > 0).count();
        assert_eq!(1, allocated);
    }
}
