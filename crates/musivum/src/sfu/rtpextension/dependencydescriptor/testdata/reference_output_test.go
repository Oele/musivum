// New for musivum, not a part of LiveKit: prints what LiveKit's dependency descriptor code
// writes and reads for a set of inputs. Its output is reference_output.txt, the expected
// values of ../reference_test.rs, which makes the same inputs in Rust.
//
// It runs inside LiveKit's package at the pin, without being put there. From the root of
// the repository, or of a git worktree of it (`reference` is in the main checkout), with the
// cap on memory and time that docs/porting-guide.md asks for:
//
//	d=$PWD/crates/musivum/src/sfu/rtpextension/dependencydescriptor/testdata
//	p=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/reference/livekit/pkg/sfu/rtpextension/dependencydescriptor
//	mkdir -p target && echo "{\"Replace\": {\"$p/reference_output_test.go\": \"$d/reference_output_test.go\"}}" > $PWD/target/reference_output_overlay.json
//	(ulimit -d 8388608; timeout 600 scripts/go-reference-test.sh -overlay $PWD/target/reference_output_overlay.json \
//	    -run TestReferenceOutput -v ./pkg/sfu/rtpextension/dependencydescriptor) | grep '^REF ' > $d/reference_output.txt
package dependencydescriptor

import (
	"encoding/hex"
	"fmt"
	"testing"
)

// what Marshal gives, and what Unmarshal makes of that
func refMarshal(name string, ext *DependencyDescriptorExtension, activeChains *uint32) {
	var buf []byte
	var err error
	if activeChains != nil {
		buf, err = ext.MarshalWithActiveChains(*activeChains)
	} else {
		buf, err = ext.Marshal()
	}
	if err != nil {
		fmt.Printf("REF %s: error: %v\n", name, err)
		return
	}
	var out DependencyDescriptor
	n, err := (&DependencyDescriptorExtension{Descriptor: &out, Structure: ext.Structure}).Unmarshal(buf)
	if err != nil {
		fmt.Printf("REF %s: %s | unmarshal error: %v\n", name, hex.EncodeToString(buf), err)
		return
	}
	fmt.Printf("REF %s: %s | %d | %s\n", name, hex.EncodeToString(buf), n, out.String())
}

// what Unmarshal gives, and what it leaves in the descriptor
func refUnmarshal(name string, h string, structure *FrameDependencyStructure, out *DependencyDescriptor) {
	buf, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}
	n, err := (&DependencyDescriptorExtension{Descriptor: out, Structure: structure}).Unmarshal(buf)
	fmt.Printf("REF %s: %d | %v | %s\n", name, n, err, out.String())
}

func refHandStructure() *FrameDependencyStructure {
	S, D, R, N := DecodeTargetSwitch, DecodeTargetDiscardable, DecodeTargetRequired, DecodeTargetNotPresent
	return &FrameDependencyStructure{
		StructureId:      5,
		NumDecodeTargets: 3,
		NumChains:        0,
		Templates: []*FrameDependencyTemplate{
			{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []DecodeTargetIndication{S, S, S}, FrameDiffs: []int{}, ChainDiffs: []int{}},
			{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []DecodeTargetIndication{S, S, S}, FrameDiffs: []int{4}, ChainDiffs: []int{}},
			{SpatialId: 0, TemporalId: 1, DecodeTargetIndications: []DecodeTargetIndication{N, D, S}, FrameDiffs: []int{2}, ChainDiffs: []int{}},
			{SpatialId: 0, TemporalId: 2, DecodeTargetIndications: []DecodeTargetIndication{N, N, D}, FrameDiffs: []int{1}, ChainDiffs: []int{}},
			{SpatialId: 0, TemporalId: 2, DecodeTargetIndications: []DecodeTargetIndication{N, N, R}, FrameDiffs: []int{2, 1}, ChainDiffs: []int{}},
		},
	}
}

func TestReferenceOutput(t *testing.T) {
	// 1. the packets of TestDependencyDescriptorUnmarshal, one after the other
	{
		hexes := []string{
			dependencyDescriptorMarshalFixture,
			"86017340fc",
			"46017340fc",
			"c3017540fc",
			"88017640fc",
			"48017640fc",
			"c2017840fc",
			//
			dependencyDescriptorMarshalFixture,
			"860173",
			"460173",
			"8b0174",
			"0b0174",
			"0b0174",
			"c30175",
		}
		var structure *FrameDependencyStructure
		for i, h := range hexes {
			var out DependencyDescriptor
			refUnmarshal(fmt.Sprintf("capture%d", i), h, structure, &out)
			if out.AttachedStructure != nil {
				structure = out.AttachedStructure
			}
		}
	}

	// 2. the structure of the fixture, a descriptor for each of its templates
	ext := newDependencyDescriptorMarshalFixture(t)
	refMarshal("fixture", ext, nil)
	for i, template := range ext.Structure.Templates {
		ext.Descriptor.FrameDependencies = template
		ext.Descriptor.FrameNumber = uint16(i)
		refMarshal(fmt.Sprintf("template%d", i), ext, nil)
	}
	all := uint32(0x1ff)
	for i, template := range ext.Structure.Templates {
		ext.Descriptor.FrameDependencies = template
		ext.Descriptor.FrameNumber = uint16(i)
		ext.Descriptor.ActiveDecodeTargetsBitmask = &all
		refMarshal(fmt.Sprintf("template%d+mask", i), ext, nil)
	}
	partial := uint32(0x03f)
	ext.Descriptor.FrameDependencies = ext.Structure.Templates[3]
	ext.Descriptor.FrameNumber = 1003
	ext.Descriptor.ActiveDecodeTargetsBitmask = &partial
	refMarshal("template3+partialmask", ext, nil)

	// custom frame diffs and chain diffs, with all chains active and with some
	custom := ext.Structure.Templates[0].Clone()
	custom.FrameDiffs = []int{1, 300, 4000}
	for i := range custom.ChainDiffs {
		custom.ChainDiffs[i] = 200 + i
	}
	ext.Descriptor.FrameDependencies = custom
	ext.Descriptor.ActiveDecodeTargetsBitmask = nil
	ext.Descriptor.FrameNumber = 0xfffe
	refMarshal("custom", ext, nil)
	for _, activeChains := range []uint32{0, 1, 2, 4, 5, 7, 0xfffffff8} {
		activeChains := activeChains
		refMarshal(fmt.Sprintf("custom+chains%x", activeChains), ext, &activeChains)
	}

	// custom decode target indications, on a template of the last layer
	custom = ext.Structure.Templates[14].Clone()
	custom.DecodeTargetIndications[8] = DecodeTargetRequired
	custom.DecodeTargetIndications[0] = DecodeTargetSwitch
	ext.Descriptor.FrameDependencies = custom
	refMarshal("customdtis", ext, nil)

	// a key frame: the structure attached
	ext = newDependencyDescriptorMarshalFixture(t)
	ext.Descriptor.AttachedStructure = ext.Structure
	refMarshal("keyframe", ext, nil)
	size, err := ext.Descriptor.MarshalSize()
	fmt.Printf("REF keyframe.MarshalSize: %d | %v\n", size, err)
	ext.Descriptor.ActiveDecodeTargetsBitmask = &all
	refMarshal("keyframe+mask", ext, nil)
	ext.Descriptor.ActiveDecodeTargetsBitmask = &partial
	refMarshal("keyframe+partialmask", ext, nil)
	size, err = ext.Descriptor.MarshalSize()
	fmt.Printf("REF keyframe+partialmask.MarshalSize: %d | %v\n", size, err)

	// 3. a structure made by hand: no chains, no resolutions, a structure id
	hs := refHandStructure()
	S, D, R, N := DecodeTargetSwitch, DecodeTargetDiscardable, DecodeTargetRequired, DecodeTargetNotPresent
	hand := func(name string, d *DependencyDescriptor) {
		refMarshal(name, &DependencyDescriptorExtension{Descriptor: d, Structure: hs}, nil)
	}
	hand("hand.key", &DependencyDescriptor{FirstPacketInFrame: true, LastPacketInFrame: true, FrameNumber: 1,
		FrameDependencies: hs.Templates[0], AttachedStructure: hs})
	mask := uint32(0x3)
	hand("hand.key+mask", &DependencyDescriptor{FirstPacketInFrame: true, FrameNumber: 2,
		FrameDependencies: hs.Templates[0], AttachedStructure: hs, ActiveDecodeTargetsBitmask: &mask})
	for i, template := range hs.Templates {
		hand(fmt.Sprintf("hand.template%d", i), &DependencyDescriptor{LastPacketInFrame: true, FrameNumber: uint16(10 + i), FrameDependencies: template})
	}
	for _, fdiff := range []int{1, 15, 16, 17, 255, 256, 257, 4095, 4096} {
		hand(fmt.Sprintf("hand.fdiff%d", fdiff), &DependencyDescriptor{FrameNumber: 0x8001,
			FrameDependencies: &FrameDependencyTemplate{TemporalId: 1, DecodeTargetIndications: []DecodeTargetIndication{N, D, S}, FrameDiffs: []int{fdiff}, ChainDiffs: []int{}}})
	}
	hand("hand.nofdiffs", &DependencyDescriptor{FrameNumber: 77,
		FrameDependencies: &FrameDependencyTemplate{TemporalId: 2, DecodeTargetIndications: []DecodeTargetIndication{N, N, D}, FrameDiffs: []int{}, ChainDiffs: []int{}}})
	hand("hand.customdtis", &DependencyDescriptor{FrameNumber: 78,
		FrameDependencies: &FrameDependencyTemplate{TemporalId: 2, DecodeTargetIndications: []DecodeTargetIndication{S, D, R}, FrameDiffs: []int{1}, ChainDiffs: []int{}}})
	hand("hand.customdtis+fdiffs+mask", &DependencyDescriptor{FrameNumber: 79, ActiveDecodeTargetsBitmask: &mask,
		FrameDependencies: &FrameDependencyTemplate{TemporalId: 1, DecodeTargetIndications: []DecodeTargetIndication{R, R, R}, FrameDiffs: []int{7, 300}, ChainDiffs: []int{}}})
	hand("hand.nolayer", &DependencyDescriptor{FrameNumber: 80,
		FrameDependencies: &FrameDependencyTemplate{SpatialId: 1, DecodeTargetIndications: []DecodeTargetIndication{S, S, S}, FrameDiffs: []int{}, ChainDiffs: []int{}}})

	// 4. slices of the descriptor that are nil, and one that is empty
	hand("nil.fdiffs", &DependencyDescriptor{FrameNumber: 90,
		FrameDependencies: &FrameDependencyTemplate{TemporalId: 2, DecodeTargetIndications: []DecodeTargetIndication{N, N, D}}})
	hand("nil.dtis", &DependencyDescriptor{FrameNumber: 91,
		FrameDependencies: &FrameDependencyTemplate{TemporalId: 1, FrameDiffs: []int{2}}})
	hand("nil.dtis+mask", &DependencyDescriptor{FrameNumber: 91, ActiveDecodeTargetsBitmask: &mask,
		FrameDependencies: &FrameDependencyTemplate{TemporalId: 1, FrameDiffs: []int{2}}})
	hand("nil.both", &DependencyDescriptor{FrameNumber: 92,
		FrameDependencies: &FrameDependencyTemplate{TemporalId: 2}})
	hand("empty.dtis+mask", &DependencyDescriptor{FrameNumber: 93, ActiveDecodeTargetsBitmask: &mask,
		FrameDependencies: &FrameDependencyTemplate{TemporalId: 1, DecodeTargetIndications: []DecodeTargetIndication{}, FrameDiffs: []int{2}}})

	// 5. packets that cannot be read, and what is left in the descriptor
	fixture := newDependencyDescriptorMarshalFixture(t).Structure
	for _, n := range []int{0, 1, 2, 3, 4, 5, 6, 7, 12, 30, 60, 85, 90, 94} {
		refUnmarshal(fmt.Sprintf("cut%d", n), dependencyDescriptorMarshalFixture[:2*n], nil, &DependencyDescriptor{})
	}
	refUnmarshal("cut0.set", "", nil, &DependencyDescriptor{FirstPacketInFrame: true, LastPacketInFrame: true, FrameNumber: 9})
	refUnmarshal("cut1.set", "40", nil, &DependencyDescriptor{FirstPacketInFrame: true, LastPacketInFrame: true, FrameNumber: 9})
	refUnmarshal("nostructure", "860173", nil, &DependencyDescriptor{})
	refUnmarshal("templateindex", "8f0173", fixture, &DependencyDescriptor{})
	refUnmarshal("templateindex.last", "bf0173", fixture, &DependencyDescriptor{})
	refUnmarshal("customdtis.cut", "86017320", fixture, &DependencyDescriptor{})
	refUnmarshal("customfdiffs.none", "86017310", fixture, &DependencyDescriptor{})
	refUnmarshal("customfdiffs.cut", "86017312", fixture, &DependencyDescriptor{})
	refUnmarshal("customchains.cut", "8601730800", fixture, &DependencyDescriptor{})
	refUnmarshal("mask.cut", "86017340", fixture, &DependencyDescriptor{})
	refUnmarshal("hand.structureid", "480003", hs, &DependencyDescriptor{})
	refUnmarshal("hand.structureid.wrap", "040003", hs, &DependencyDescriptor{})
	// 65 templates; nine temporal layers; five spatial layers
	refUnmarshal("toomanytemplates", "c00001800000000000000000000000000000000000000000", nil, &DependencyDescriptor{})
	refUnmarshal("toomanytemporal", "c0000180005555", nil, &DependencyDescriptor{})
	refUnmarshal("toomanyspatial", "c000018000aa", nil, &DependencyDescriptor{})
	// structures made by hand that a packet cannot give: a spatial layer without a
	// resolution; templates that are shorter than the structure says
	odd := &FrameDependencyStructure{
		NumDecodeTargets: 1,
		Resolutions:      []RenderResolution{{Width: 640, Height: 360}},
		Templates: []*FrameDependencyTemplate{
			{DecodeTargetIndications: []DecodeTargetIndication{S}},
			{SpatialId: 1, DecodeTargetIndications: []DecodeTargetIndication{S}},
		},
	}
	refUnmarshal("spatiallayer.ok", "000003", odd, &DependencyDescriptor{})
	refUnmarshal("spatiallayer", "010003", odd, &DependencyDescriptor{})
	short := &FrameDependencyStructure{
		NumDecodeTargets: 2,
		NumChains:        1,
		Templates: []*FrameDependencyTemplate{
			{DecodeTargetIndications: []DecodeTargetIndication{S}},
		},
	}
	refUnmarshal("dtimismatch", "00000320ff", short, &DependencyDescriptor{})
	refUnmarshal("chaindiffsmismatch", "00000308ff", short, &DependencyDescriptor{})

	// 6. the bit streams
	for _, c := range [][2]uint32{{0, 1}, {0, 2}, {1, 2}, {0, 3}, {1, 3}, {2, 3}, {0, 5}, {2, 5}, {3, 5}, {4, 5}, {5, 10}, {6, 10}, {9, 10}, {0, 33}, {30, 33}, {31, 33}, {32, 33}, {1 << 30, 1 << 31}, {(1 << 31) - 1, 1 << 31}, {5, 5}, {0, 0}, {7, (1 << 31) + 1}} {
		buf := make([]byte, 5)
		for i := range buf {
			buf[i] = 0xa5
		}
		w := NewBitStreamWriter(buf)
		_ = w.WriteBits(1, 3)
		err := w.WriteNonSymmetric(c[0], c[1])
		rem := w.RemainingBits()
		r := NewBitStreamReader(buf)
		_, _ = r.ReadBits(3)
		v, rerr := r.ReadNonSymmetric(c[1])
		fmt.Printf("REF nonsymmetric %d/%d: %s | %v | %d | %d | %d | %v | %d | %d\n", c[0], c[1], hex.EncodeToString(buf), err, rem, SizeNonSymmetricBits(c[0], c[1]), v, rerr, r.RemainingBits(), r.BytesRead())
	}
	{
		buf := make([]byte, 13)
		for i := range buf {
			buf[i] = 0x5a
		}
		w := NewBitStreamWriter(buf)
		errs := []error{
			w.WriteBits(0x5, 3), w.WriteBits(0xfedcba9876543210, 64), w.WriteBits(0, 1), w.WriteBits(0x1234, 13), w.WriteBits(0xff, 8), w.WriteBits(0x3, 7),
			w.WriteBits(0xffff, 2), w.WriteBits(0, 3), w.WriteBits(1, 4),
		}
		rem := w.RemainingBits()
		errs = append(errs, w.WriteBits(1, 1), w.WriteBits(0xff, 3))
		fmt.Printf("REF writebits: %s | %v | %d | %d\n", hex.EncodeToString(buf), errs, rem, w.RemainingBits())
		r := NewBitStreamReader(buf)
		var vals []uint64
		var rerrs []error
		var rems []int
		var read []int
		for _, n := range []int{3, 64, 1, 13, 8, 7, 0, 2, 3, 0, 2, -1, 65, 1, 2, 0, 1} {
			v, err := r.ReadBits(n)
			vals = append(vals, v)
			rerrs = append(rerrs, err)
			rems = append(rems, r.RemainingBits())
			read = append(read, r.BytesRead())
		}
		fmt.Printf("REF readbits: %v | %v | %v | %v | %v\n", vals, rerrs, rems, read, r.Ok())
		r.Invalidate()
		v, err := r.ReadBits(0)
		fmt.Printf("REF invalidate: %d | %v | %d | %v | %d\n", v, err, r.RemainingBits(), r.Ok(), r.BytesRead())
	}
	_ = R
}
