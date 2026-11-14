// New for musivum, not a part of LiveKit: prints what pion's FlexFEC-03 encoder writes and what
// LiveKit's FlexFEC decoder makes of it, for a set of inputs. Its output is
// reference_output.txt, the expected values of ../reference_test.rs, which makes the same
// inputs in Rust.
//
// It runs inside LiveKit's package at the pin, without being put there. From the root of
// the repository, or of a git worktree of it (`reference` is in the main checkout), with the
// cap on memory and time that docs/porting-guide.md asks for:
//
//	d=$PWD/crates/musivum/src/sfu/flexfec/testdata
//	p=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/reference/livekit/pkg/sfu/flexfec
//	mkdir -p target && echo "{\"Replace\": {\"$p/reference_output_test.go\": \"$d/reference_output_test.go\"}}" > $PWD/target/reference_output_overlay.json
//	(ulimit -d 8388608; timeout 600 scripts/go-reference-test.sh -overlay $PWD/target/reference_output_overlay.json \
//	    -run TestReferenceOutput -v ./pkg/sfu/flexfec) | grep '^REF ' > $d/reference_output.txt
package flexfec

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	pionflexfec "github.com/pion/interceptor/pkg/flexfec"
	"github.com/pion/rtp"

	"github.com/livekit/protocol/logger"
)

// SplitMix64, which the Rust side has too: both make the same packets from the same seed.
type refRng struct{ s uint64 }

func (r *refRng) uint64() uint64 {
	r.s += 0x9E3779B97F4A7C15
	z := r.s
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *refRng) intn(n int) int {
	return int(r.uint64() % uint64(n))
}

func (r *refRng) read(p []byte) {
	for i := range p {
		p[i] = byte(r.uint64())
	}
}

// FNV-1a, to keep the lines short.
const refHashInit = uint64(0xcbf29ce484222325)

func refHash(h uint64, p []byte) uint64 {
	for _, b := range p {
		h ^= uint64(b)
		h *= 0x100000001b3
	}
	return h
}

// media packets as those of makeMediaPackets in decoder_test.go, some of them with a CSRC,
// a header extension or the version 3, and some of about the largest size that the decoder
// takes or just above it
func refMediaPackets(rng *refRng, baseSN uint16, count int) []rtp.Packet {
	packets := make([]rtp.Packet, 0, count)
	for i := 0; i < count; i++ {
		size := 100 + rng.intn(900)
		if rng.intn(40) == 0 {
			size = 1470 + rng.intn(40)
		}
		payload := make([]byte, size)
		rng.read(payload)
		packet := rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    testMediaPT,
				SequenceNumber: baseSN + uint16(i),
				Timestamp:      3000 * uint32(i),
				SSRC:           testMediaSSRC,
				Marker:         i == count-1,
			},
			Payload: payload,
		}
		if rng.intn(4) == 0 {
			packet.CSRC = []uint32{uint32(rng.uint64())}
		}
		if rng.intn(4) == 0 {
			ext := make([]byte, 2)
			rng.read(ext)
			if err := packet.SetExtension(3, ext); err != nil {
				panic(err)
			}
		}
		if rng.intn(16) == 0 {
			packet.Version = 3
		}
		packets = append(packets, packet)
	}
	return packets
}

// what the encoder gives for a run of media packets
func refEncode(name string, encoder *pionflexfec.FlexEncoder03, rng *refRng, baseSN uint16, count int, numFEC uint32) {
	media := refMediaPackets(rng, baseSN, count)
	fec := encoder.EncodeFec(media, numFEC)
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d", len(fec))
	for i := range fec {
		raw, err := fec[i].Marshal()
		if err != nil {
			panic(err)
		}
		head := fec[i].Payload
		if len(head) > 32 {
			head = head[:32]
		}
		fmt.Fprintf(&sb, " | %d %d %s %016x", fec[i].SequenceNumber, len(fec[i].Payload), hex.EncodeToString(head), refHash(refHashInit, raw))
	}
	fmt.Printf("REF %s: %s\n", name, sb.String())
}

// what the parser makes of a FEC header
func refParse(name string, data []byte) {
	fec, err := parseFlexFEC03Header(data)
	if err != nil {
		fmt.Printf("REF %s: error: %v\n", name, err)
		return
	}
	seqs := fec.protectedSequences(nil)
	h := refHashInit
	for _, seq := range seqs {
		h = refHash(h, []byte{byte(seq >> 8), byte(seq)})
	}
	fmt.Printf("REF %s: %d %d %x %x %x %d | %d %016x\n", name,
		fec.protectedSSRC, fec.seqNumBase, fec.mask0, fec.mask1, fec.mask2, len(fec.payload), len(seqs), h)
}

// a FEC payload with something wrong in it
func refCorrupt(rng *refRng, payload []byte) []byte {
	p := append([]byte(nil), payload...)
	switch rng.intn(10) {
	case 0: // truncated
		n := rng.intn(33)
		p = p[:n]
	case 1: // R bit
		p[0] |= 0x80
	case 2: // F bit
		p[0] |= 0x40
	case 3: // SSRC count
		p[8] = byte(rng.intn(4))
	case 4: // another SSRC
		i := 12 + rng.intn(4)
		bit := rng.intn(8)
		p[i] ^= 1 << bit
	case 5: // any bit of the header
		i := rng.intn(32)
		bit := rng.intn(8)
		p[i] ^= 1 << bit
	case 6: // another base
		base := binary.BigEndian.Uint16(p[16:18]) + uint16(rng.intn(41)) - 20
		binary.BigEndian.PutUint16(p[16:18], base)
	case 7: // the first mask is not the last
		p[18] &= 0x7f
	case 8: // another length
		i := 2 + rng.intn(2)
		bit := rng.intn(8)
		p[i] ^= 1 << bit
	case 9: // any bit
		i := rng.intn(len(p))
		bit := rng.intn(8)
		p[i] ^= 1 << bit
	}
	return p
}

// the round trips of pion's flexfec_03_test.go, with LiveKit's decoder in the place of
// pion's: whichever single packet of a run is lost, the FEC packets bring it back
func refRoundTrip(t *testing.T, name string, seqStart uint16, seqLen int, fecCount uint32) {
	media := make([]rtp.Packet, 0, seqLen)
	for i := 0; i < seqLen; i++ {
		packet := rtp.Packet{
			Header: rtp.Header{
				Marker:         true,
				Version:        2,
				PayloadType:    testMediaPT,
				SequenceNumber: seqStart + uint16(i),
				Timestamp:      3653407706,
				SSRC:           testMediaSSRC,
			},
			Payload: []byte{1, 2, 3, 4, 5, byte(i)},
		}
		if err := packet.SetExtension(1, []byte{0xAA, 0xAA}); err != nil {
			panic(err)
		}
		media = append(media, packet)
	}
	fec := pionflexfec.NewFlexEncoder03(testFECPT, testFECSSRC).EncodeFec(media, fecCount)

	broughtBack := 0
	h := refHashInit
	for lost := range media {
		decoder := newTestDecoder(testFECSSRC, testMediaSSRC, logger.GetLogger())
		var recovered []*rtp.Packet
		for i := range media {
			if i != lost {
				recovered = append(recovered, decoder.DecodeFEC(&media[i])...)
			}
		}
		early := len(recovered)
		for i := range fec {
			recovered = append(recovered, decoder.DecodeFEC(&fec[i])...)
		}

		expected, err := media[lost].Marshal()
		if err != nil {
			panic(err)
		}
		for _, r := range recovered {
			actual, err := r.Marshal()
			if err != nil {
				panic(err)
			}
			h = refHash(h, actual)
			if early == 0 && len(recovered) == 1 && bytes.Equal(expected, actual) {
				broughtBack++
			}
		}
	}
	if broughtBack != len(media) {
		t.Errorf("round trip %s: %d of %d packets brought back", name, broughtBack, len(media))
	}
	fmt.Printf("REF round trip %s: %d %d %d %016x\n", name, len(media), len(fec), broughtBack, h)
}

type refScenario struct {
	name string
	seed uint64
	// the first media sequence number, and the first of the FEC stream
	baseSN     uint16
	fecSeqBase uint16
	// windows of minCount to maxCount media packets, each with 1 to maxFEC FEC packets
	windows  int
	minCount int
	maxCount int
	maxFEC   int
	// chances in percent: a media packet lost, a FEC packet lost, a packet sent twice, a FEC
	// packet corrupted, a packet swapped with one up to swapSpan places later
	lossPct    int
	fecLossPct int
	dupPct     int
	corruptPct int
	swapPct    int
	swapSpan   int
	// after every gapEvery windows the media sequence number jumps by gapMin and up to
	// gapSpan-1 more, after every fecJumpEvery windows the one of the FEC stream by fecJump
	// (0: never)
	gapEvery     int
	gapMin       uint16
	gapSpan      int
	fecJumpEvery int
	fecJump      uint16
}

// what the decoder recovers from a stream of packets, and what it is left with
func refRunScenario(sc refScenario) {
	rng := &refRng{s: sc.seed}
	encoder := pionflexfec.NewFlexEncoder03(testFECPT, testFECSSRC)
	decoder := newTestDecoder(testFECSSRC, testMediaSSRC, logger.GetLogger())

	var events []rtp.Packet
	sn := sc.baseSN
	fecShift := sc.fecSeqBase - 1000
	for w := 0; w < sc.windows; w++ {
		count := sc.minCount + rng.intn(sc.maxCount-sc.minCount+1)
		numFEC := 1 + rng.intn(sc.maxFEC)
		media := refMediaPackets(rng, sn, count)
		fec := encoder.EncodeFec(media, uint32(numFEC))

		for i := range media {
			if rng.intn(100) < sc.lossPct {
				continue
			}
			events = append(events, media[i])
			if rng.intn(100) < sc.dupPct {
				events = append(events, media[i])
			}
		}
		for i := range fec {
			packet := fec[i]
			packet.SequenceNumber += fecShift
			if rng.intn(100) < sc.fecLossPct {
				continue
			}
			if rng.intn(100) < sc.corruptPct {
				packet.Payload = refCorrupt(rng, packet.Payload)
			}
			events = append(events, packet)
			if rng.intn(100) < sc.dupPct {
				events = append(events, packet)
			}
		}

		sn += uint16(count)
		if sc.gapEvery > 0 && (w+1)%sc.gapEvery == 0 {
			sn += sc.gapMin + uint16(rng.intn(sc.gapSpan))
		}
		if sc.fecJumpEvery > 0 && (w+1)%sc.fecJumpEvery == 0 {
			fecShift += sc.fecJump
		}
	}

	for i := range events {
		if rng.intn(100) < sc.swapPct {
			j := i + 1 + rng.intn(sc.swapSpan)
			if j < len(events) {
				events[i], events[j] = events[j], events[i]
			}
		}
	}

	// as DecodeFEC of the test decoder, which keeps what arrives and what is recovered, but a
	// recovered packet that cannot be marshalled is not kept
	var seqs []string
	h := refHashInit
	for i := range events {
		packet := &events[i]
		if packet.SSRC == testMediaSSRC {
			raw, err := packet.Marshal()
			if err != nil {
				panic(err)
			}
			decoder.mediaPackets[packet.SequenceNumber] = raw
		}
		for _, recovered := range decoder.Decoder.DecodeFEC(packet) {
			seqs = append(seqs, fmt.Sprintf("%d", recovered.SequenceNumber))
			raw, err := recovered.Marshal()
			if err != nil {
				h = refHash(h, []byte("E"))
				continue
			}
			h = refHash(h, raw)
			decoder.mediaPackets[recovered.SequenceNumber] = raw
		}
	}

	var retained []string
	for i := range decoder.receivedFECPackets {
		retained = append(retained, fmt.Sprintf("%d", decoder.receivedFECPackets[i].packet.SequenceNumber))
	}
	stats := decoder.Stats()
	fmt.Printf("REF scenario %s: %d | %d %d %d %d | %d [%s] %016x | [%s]\n", sc.name, len(events),
		stats.FECPacketsReceived, stats.FECBytesReceived, stats.FECPacketsDiscarded, stats.PacketsRecovered,
		len(seqs), strings.Join(seqs, " "), h, strings.Join(retained, " "))
}

func TestReferenceOutput(t *testing.T) {
	// the encoder: one that is used again and again, so that its coverage is kept, updated
	// and left as it was (more media packets than it takes), and new ones
	rng := &refRng{s: 1}
	encoder := pionflexfec.NewFlexEncoder03(testFECPT, testFECSSRC)
	for i, c := range []struct {
		baseSN uint16
		count  int
		numFEC uint32
	}{
		{100, 5, 1}, {105, 5, 1}, {110, 10, 2}, {120, 10, 3}, {65530, 15, 1}, {200, 16, 1},
		{300, 46, 2}, {400, 47, 1}, {500, 109, 2}, {700, 110, 3}, {900, 111, 1}, {1100, 3, 5},
		{1200, 0, 1}, {1300, 7, 0}, {1400, 60, 4}, {65500, 100, 10}, {2000, 100, 40},
	} {
		refEncode(fmt.Sprintf("encode %d", i), encoder, rng, c.baseSN, c.count, c.numFEC)
	}
	refEncode("encode too many", pionflexfec.NewFlexEncoder03(testFECPT, testFECSSRC), rng, 100, 111, 1)
	refEncode("encode one", pionflexfec.NewFlexEncoder03(testFECPT, testFECSSRC), rng, 65535, 1, 1)

	for _, c := range []struct {
		name     string
		seqStart uint16
		seqLen   int
		fecCount uint32
	}{
		{"first", 1, 5, 2}, {"last", 65531, 5, 2}, {"wrap", 65533, 5, 2}, {"whole range", 0, 109, 2},
		{"single FEC short window", 42, 8, 1}, {"dual FEC mask2 coverage", 5, 40, 2},
		{"triple FEC mask3 coverage", 120, 90, 3}, {"wrap around sequences", 65510, 60, 4},
		{"high media count", 41000, 100, 10},
	} {
		refRoundTrip(t, c.name, c.seqStart, c.seqLen, c.fecCount)
	}

	// the parser
	rng = &refRng{s: 2}
	for i := 0; i < 200; i++ {
		data := make([]byte, rng.intn(48))
		rng.read(data)
		if len(data) > 8 && rng.intn(4) != 0 {
			data[0] &= 0x3f
			data[8] = 1
		}
		if len(data) > 26 && rng.intn(2) == 0 {
			data[18] &= 0x7f
			if rng.intn(2) == 0 {
				data[20] &= 0x7f
			}
		}
		refParse(fmt.Sprintf("parse %d", i), data)
	}

	// the decoder
	for _, sc := range []refScenario{
		{name: "complete", seed: 10, baseSN: 100, fecSeqBase: 1000, windows: 20, minCount: 3, maxCount: 12, maxFEC: 2, swapSpan: 8},
		{name: "small", seed: 11, baseSN: 100, fecSeqBase: 1000, windows: 60, minCount: 3, maxCount: 12, maxFEC: 2,
			lossPct: 10, fecLossPct: 5, dupPct: 3, swapPct: 10, swapSpan: 8},
		{name: "second mask", seed: 12, baseSN: 5000, fecSeqBase: 1000, windows: 40, minCount: 16, maxCount: 46, maxFEC: 3,
			lossPct: 5, fecLossPct: 5, dupPct: 2, swapPct: 10, swapSpan: 8},
		{name: "third mask", seed: 13, baseSN: 20000, fecSeqBase: 1000, windows: 40, minCount: 47, maxCount: 109, maxFEC: 4,
			lossPct: 3, fecLossPct: 5, dupPct: 2, swapPct: 10, swapSpan: 8},
		{name: "wrap", seed: 14, baseSN: 65000, fecSeqBase: 65500, windows: 60, minCount: 5, maxCount: 40, maxFEC: 3,
			lossPct: 8, fecLossPct: 5, dupPct: 3, swapPct: 15, swapSpan: 8},
		{name: "lossy", seed: 15, baseSN: 300, fecSeqBase: 1000, windows: 80, minCount: 5, maxCount: 30, maxFEC: 5,
			lossPct: 25, fecLossPct: 20, dupPct: 5, swapPct: 30, swapSpan: 40},
		{name: "retained", seed: 16, baseSN: 60000, fecSeqBase: 65300, windows: 300, minCount: 4, maxCount: 8, maxFEC: 2,
			lossPct: 45, fecLossPct: 5, dupPct: 2, swapPct: 20, swapSpan: 20},
		{name: "corrupt", seed: 17, baseSN: 40000, fecSeqBase: 1000, windows: 150, minCount: 5, maxCount: 60, maxFEC: 3,
			lossPct: 10, fecLossPct: 5, dupPct: 3, corruptPct: 40, swapPct: 10, swapSpan: 8},
		{name: "gaps", seed: 18, baseSN: 100, fecSeqBase: 1000, windows: 160, minCount: 8, maxCount: 12, maxFEC: 2,
			lossPct: 20, fecLossPct: 5, dupPct: 1, swapPct: 5, swapSpan: 8, gapEvery: 7, gapSpan: 400},
		{name: "half way", seed: 21, baseSN: 100, fecSeqBase: 1000, windows: 400, minCount: 8, maxCount: 12, maxFEC: 2,
			lossPct: 30, fecLossPct: 5, dupPct: 1, swapPct: 5, swapSpan: 8, gapEvery: 20, gapMin: 0x7ffd, gapSpan: 7},
		{name: "fec jumps", seed: 19, baseSN: 100, fecSeqBase: 30000, windows: 80, minCount: 4, maxCount: 10, maxFEC: 2,
			lossPct: 35, fecLossPct: 5, dupPct: 1, swapPct: 5, swapSpan: 8, fecJumpEvery: 7, fecJump: 0x4100},
		{name: "sparse", seed: 22, baseSN: 50000, fecSeqBase: 1000, windows: 12, minCount: 90, maxCount: 109, maxFEC: 45,
			lossPct: 10, fecLossPct: 5, dupPct: 1, swapPct: 10, swapSpan: 8},
		{name: "everything", seed: 20, baseSN: 64000, fecSeqBase: 65000, windows: 150, minCount: 1, maxCount: 109, maxFEC: 6,
			lossPct: 12, fecLossPct: 10, dupPct: 4, corruptPct: 15, swapPct: 20, swapSpan: 30,
			gapEvery: 37, gapSpan: 300, fecJumpEvery: 29, fecJump: 0x4100},
	} {
		refRunScenario(sc)
	}
}
