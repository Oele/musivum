// New for musivum, not a part of LiveKit's mediatransportutil: prints everything the package
// (and pion's VP9Packet, which it uses) says about each of a list of payloads, one line per
// payload. Its output for reference_payloads.txt is reference_output.txt, the expected values
// of ../reference_test.rs, which says the same about the same payloads in Rust.
//
// It runs next to copies of the package's files at the pin, in a scratch module that has
// LiveKit's requirements, so that pion/rtp is the version LiveKit uses (the module cache is
// read-only and takes no overlay, and mediatransportutil's own go.mod names an older
// pion/rtp). From the root of the repository, or of a git worktree of it (`reference` is in
// the main checkout), with the cap on memory and time that docs/porting-guide.md asks for:
//
//	d=$PWD/crates/musivum/src/mediatransportutil/codec/testdata
//	r=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/reference
//	o=$r/gomod/github.com/livekit/mediatransportutil@v0.0.0-20261014192331-dd8e31a0adc0/pkg/codec
//	t=$PWD/target/codec-reference; rm -rf $t; mkdir -p $t
//	sed '1s/.*/module codecreference/' $r/livekit/go.mod > $t/go.mod; cp $r/livekit/go.sum $t/
//	cp $o/vp8.go $o/h26xhelper.go $o/helpers.go $o/keyframe.go $d/reference_output_test.go $t/
//	(cd $t; ulimit -d 8388608; GOMODCACHE=$r/gomod GOFLAGS="-modcacherw -mod=mod" GOPROXY=off \
//	    GOCACHE=$HOME/.cache/musivum/gocache MUSIVUM_CODEC_PAYLOADS=$d/reference_payloads.txt \
//	    timeout 600 go test -count=1 -run TestReferenceOutput -v .) | grep '^REF ' > $d/reference_output.txt
//
// (-count=1: without it a result comes from Go's test cache, which does not see that the
// file of payloads changed.) MUSIVUM_CODEC_PAYLOADS may name any file of payloads, one in hex
// per line: the output for a larger one, kept outside the repository, is what
// ../reference_test.rs reads when MUSIVUM_CODEC_REFERENCE names it.
package codec

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pion/rtp/codecs"
)

func refB2s(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// what f gives, or "panic"
func refSafe(f func() string) (s string) {
	defer func() {
		if r := recover(); r != nil {
			s = "panic"
		}
	}()
	return f()
}

func refErrStr(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

// the fields in the order of the struct
func refVP8Fields(v *VP8) string {
	return fmt.Sprintf("%d,%s,%s,%s,%d,%s,%d,%s,%d,%s,%s,%d,%d,%s",
		v.FirstByte, refB2s(v.S), refB2s(v.I), refB2s(v.M), v.PictureID, refB2s(v.L), v.TL0PICIDX,
		refB2s(v.T), v.TID, refB2s(v.Y), refB2s(v.K), v.KEYIDX, v.HeaderSize, refB2s(v.IsKeyFrame))
}

func refSPSStr(info *SPSInfo, err error) string {
	if err != nil {
		return "err:" + err.Error()
	}
	return fmt.Sprintf("%d,%d,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d",
		info.ChromaFormatIDC, info.PicWidthInLumaSamples, info.PicHeightInLumaSamples,
		refB2s(info.ConformanceWindowFlag), info.ConfWinLeftOffset, info.ConfWinRightOffset,
		info.ConfWinTopOffset, info.ConfWinBottomOffset, info.CodedWidth, info.CodedHeight,
		info.DisplayWidth, info.DisplayHeight)
}

// the fields in the order of the struct, the eight flags of the first byte together
func refVP9Fields(p *codecs.VP9Packet) string {
	return fmt.Sprintf("%s%s%s%s%s%s%s%s,%d,%d,%s,%d,%s,%v,%d,%d,%s,%s,%d,%v,%v,%v,%v,%v,%x",
		refB2s(p.I), refB2s(p.P), refB2s(p.L), refB2s(p.F), refB2s(p.B), refB2s(p.E), refB2s(p.V), refB2s(p.Z),
		p.PictureID, p.TID, refB2s(p.U), p.SID, refB2s(p.D), p.PDiff, p.TL0PICIDX,
		p.NS, refB2s(p.Y), refB2s(p.G), p.NG, p.Width, p.Height, p.PGTID, p.PGU, p.PGPDiff, p.Payload)
}

func refSizeStr(sz VideoSize) string {
	return fmt.Sprintf("%dx%d", sz.Width, sz.Height)
}

// One VP9 packet that every payload is read into, as LiveKit's buffer does with the packet
// it keeps: trimmed after an error only.
var refReuse codecs.VP9Packet

func refTrimReuse(p *codecs.VP9Packet) {
	*p = codecs.VP9Packet{
		PDiff:   p.PDiff[:0],
		Width:   p.Width[:0],
		Height:  p.Height[:0],
		PGTID:   p.PGTID[:0],
		PGU:     p.PGU[:0],
		PGPDiff: p.PGPDiff[:0],
	}
}

// The bit reader on the payload without its first byte, which chooses what is read.
func refBrDump(payload []byte) string {
	if len(payload) < 1 {
		return ""
	}
	br := NewBitReader(payload[1:])
	var sb strings.Builder
	switch payload[0] % 4 {
	case 0:
		for i := 0; i < 64; i++ {
			v, err := br.ReadUE()
			if err != nil {
				sb.WriteString("e")
				break
			}
			fmt.Fprintf(&sb, "%d,", v)
		}
	case 1:
		for i := 0; i < 64; i++ {
			v, err := br.ReadSE()
			if err != nil {
				sb.WriteString("e")
				break
			}
			fmt.Fprintf(&sb, "%d,", v)
		}
	case 2:
		ns := []int{1, 3, 8, 13, 32, 48, 64, 65, 7, 0}
		for i := 0; i < 64; i++ {
			v, err := br.ReadBits(ns[i%len(ns)])
			if err != nil {
				sb.WriteString("e")
				break
			}
			fmt.Fprintf(&sb, "%d,", v)
		}
	case 3:
		for i := 0; i < 16; i++ {
			f, err := br.ReadFlag()
			if err != nil {
				sb.WriteString("e")
				break
			}
			fmt.Fprintf(&sb, "%s,", refB2s(f))
			v, err := br.ReadUE()
			if err != nil {
				sb.WriteString("e")
				break
			}
			fmt.Fprintf(&sb, "%d,", v)
			s, err := br.ReadSE()
			if err != nil {
				sb.WriteString("e")
				break
			}
			fmt.Fprintf(&sb, "%d,", s)
			b, err := br.ReadBits(5)
			if err != nil {
				sb.WriteString("e")
				break
			}
			fmt.Fprintf(&sb, "%d,", b)
		}
	}
	return sb.String()
}

func refAt(payload []byte, i int) byte {
	if i < len(payload) {
		return payload[i]
	}
	return 0
}

// refDumpLine gives everything the package says about a payload.
func refDumpLine(payload []byte) string {
	var parts []string
	add := func(name string, f func() string) {
		parts = append(parts, name+":"+refSafe(f))
	}

	add("kf", func() string {
		return refB2s(IsH264KeyFrame(payload)) + refB2s(IsH265KeyFrame(payload)) + refB2s(IsVP8KeyFrame(payload)) +
			refB2s(IsVP9KeyFrame(nil, payload)) + refB2s(IsAV1KeyFrame(payload))
	})
	add("ikf", func() string {
		return refB2s(IsKeyFrame("H264", payload)) + refB2s(IsKeyFrame("h265", payload)) + refB2s(IsKeyFrame("VP8", payload)) +
			refB2s(IsKeyFrame("vp9", payload)) + refB2s(IsKeyFrame("Av1", payload)) + refB2s(IsKeyFrame("opus", payload))
	})
	add("h264sz", func() string { return refSizeStr(ExtractH264VideoSize(payload)) })
	add("h265sz", func() string { return refSizeStr(ExtractH265VideoSize(payload)) })
	add("sps264", func() string { return refSPSStr(parseH264SPS(payload)) })
	add("sps265", func() string { return refSPSStr(parseH265SPS(payload)) })
	add("sc", func() string { return fmt.Sprintf("%x", stripStartCode(payload)) })
	add("epb", func() string { return fmt.Sprintf("%x", removeEmulationPreventionBytes(payload)) })

	var v VP8
	vp8Err := v.Unmarshal(payload)
	add("vp8", func() string { return refErrStr(vp8Err) + ";" + refVP8Fields(&v) })
	add("vp8m", func() string {
		buf, err := v.Marshal()
		if err != nil {
			return "err:" + err.Error()
		}
		return fmt.Sprintf("%x", buf)
	})
	add("vp8sz", func() string { return refSizeStr(ExtractVP8VideoSize(&v, payload)) })

	// A VP8 descriptor made from the bytes of the payload, written to a buffer that may be
	// too small.
	x := VP8{
		FirstByte:  refAt(payload, 0),
		S:          refAt(payload, 1)&1 != 0,
		I:          refAt(payload, 1)&2 != 0,
		M:          refAt(payload, 1)&4 != 0,
		L:          refAt(payload, 1)&8 != 0,
		T:          refAt(payload, 1)&16 != 0,
		Y:          refAt(payload, 1)&32 != 0,
		K:          refAt(payload, 1)&64 != 0,
		PictureID:  uint16(refAt(payload, 2))<<8 | uint16(refAt(payload, 3)),
		TL0PICIDX:  refAt(payload, 4),
		TID:        refAt(payload, 5),
		KEYIDX:     refAt(payload, 6),
		HeaderSize: int(refAt(payload, 7) % 10),
	}
	add("vp8x", func() string {
		buf, err := x.Marshal()
		if err != nil {
			return "err:" + err.Error()
		}
		return fmt.Sprintf("%x", buf)
	})
	add("vp8xt", func() string {
		buf := make([]byte, int(refAt(payload, 8)%9))
		for i := range buf {
			buf[i] = 0xEE
		}
		n, err := x.MarshalTo(buf)
		if err != nil {
			return "err:" + err.Error()
		}
		return fmt.Sprintf("%d;%x", n, buf)
	})

	var p codecs.VP9Packet
	raw, vp9Err := p.Unmarshal(payload)
	add("vp9", func() string {
		if vp9Err != nil {
			return "err:" + vp9Err.Error()
		}
		return refVP9Fields(&p) + ";" + fmt.Sprintf("%x", raw)
	})
	add("vp9kf", func() string {
		if vp9Err != nil || (p.B && len(p.Payload) == 0) {
			return "-"
		}
		return refB2s(IsVP9KeyFrame(&p, payload))
	})
	add("vp9ph", func() string {
		return refB2s(p.IsPartitionHead(payload)) + refB2s(p.IsPartitionTail(true, payload)) + refB2s(p.IsPartitionTail(false, payload))
	})
	add("vp9r", func() string {
		_, err := refReuse.Unmarshal(payload)
		if err != nil {
			refTrimReuse(&refReuse)
			return "err:" + err.Error()
		}
		return refVP9Fields(&refReuse)
	})
	add("br", func() string { return refBrDump(payload) })

	return strings.Join(parts, "|")
}

func TestReferenceOutput(t *testing.T) {
	path := os.Getenv("MUSIVUM_CODEC_PAYLOADS")
	if path == "" {
		t.Skip("MUSIVUM_CODEC_PAYLOADS does not name a file of payloads")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		line := sc.Text()
		// a comment, or the name of a group of payloads
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		payload := []byte{}
		if line != "empty" {
			if payload, err = hex.DecodeString(line); err != nil {
				t.Fatal(err)
			}
		}
		// one write per line: what the package logs must not get into the middle of one
		os.Stdout.WriteString("REF " + hex.EncodeToString(payload) + "\t" + refDumpLine(payload) + "\n")
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
}
