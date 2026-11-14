// New for musivum, not a part of LiveKit: runs the commands of reference_input.txt on
// LiveKit's video layer selectors and prints what they give. Its output is
// reference_output.txt, the expected values of ../reference_test.rs, which runs the same
// commands on the port. reference_input.txt says what the commands are.
//
// It runs inside LiveKit's package at the pin, without being put there. From the root of
// the repository, or of a git worktree of it (`reference` is in the main checkout), with the
// cap on memory and time that docs/porting-guide.md asks for:
//
//	d=$PWD/crates/musivum/src/sfu/videolayerselector/testdata
//	p=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/reference/livekit/pkg/sfu/videolayerselector
//	mkdir -p target && echo "{\"Replace\": {\"$p/reference_output_test.go\": \"$d/reference_output_test.go\"}}" > $PWD/target/vls_reference_overlay.json
//	(ulimit -d 8388608; MUSIVUM_REFERENCE_INPUT=$d/reference_input.txt timeout 600 scripts/go-reference-test.sh \
//	    -overlay $PWD/target/vls_reference_overlay.json -run TestReferenceOutput -v ./pkg/sfu/videolayerselector) | grep '^REF ' > $d/reference_output.txt
package videolayerselector

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/videolayerselector/temporallayerselector"
	"github.com/livekit/mediatransportutil/pkg/codec"
	"github.com/livekit/protocol/logger"
)

func refInt(s string) int32 {
	v, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return int32(v)
}

func refLayer(spatial string, temporal string) buffer.VideoLayer {
	return buffer.VideoLayer{Spatial: refInt(spatial), Temporal: refInt(temporal)}
}

func refKind(vls VideoLayerSelector) string {
	switch vls.(type) {
	case *Null:
		return "null"
	case *Simulcast:
		return "simulcast"
	case *VP9:
		return "vp9"
	case *DependencyDescriptor:
		return "dd"
	}
	return "?"
}

func TestReferenceOutput(t *testing.T) {
	f, err := os.Open(os.Getenv("MUSIVUM_REFERENCE_INPUT"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var vls VideoLayerSelector
	scanner := bufio.NewScanner(f)
	n := 0
	for scanner.Scan() {
		n++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		c := strings.Fields(line)
		out := c[0]
		switch c[0] {
		case "new":
			switch c[1] {
			case "null":
				vls = NewNull(logger.GetLogger())
			case "simulcast":
				vls = NewSimulcast(logger.GetLogger())
			case "vp9":
				vls = NewVP9(logger.GetLogger())
			case "dd":
				vls = NewDependencyDescriptor(logger.GetLogger())
			default:
				t.Fatalf("line %d: %s", n, line)
			}
		case "from":
			switch c[1] {
			case "simulcast":
				if s := NewSimulcastFromOther(vls); s != nil {
					vls = s
				} else {
					out = "from nil"
				}
			case "vp9":
				vls = NewVP9FromOther(vls)
			case "dd":
				vls = NewDependencyDescriptorFromOther(vls)
			default:
				t.Fatalf("line %d: %s", n, line)
			}
		case "start":
			vls.SetEnableStartAtDesiredQuality(c[1] == "1")
		case "max":
			vls.SetMax(refLayer(c[1], c[2]))
		case "maxspatial":
			vls.SetMaxSpatial(refInt(c[1]))
		case "maxtemporal":
			vls.SetMaxTemporal(refInt(c[1]))
		case "seen":
			vls.SetMaxSeen(refLayer(c[1], c[2]))
		case "seenspatial":
			vls.SetMaxSeenSpatial(refInt(c[1]))
		case "seentemporal":
			vls.SetMaxSeenTemporal(refInt(c[1]))
		case "target":
			vls.SetTarget(refLayer(c[1], c[2]))
		case "req":
			vls.SetRequestSpatial(refInt(c[1]))
		case "current":
			vls.SetCurrent(refLayer(c[1], c[2]))
		case "tls":
			switch c[1] {
			case "none":
				vls.SetTemporalLayerSelector(nil)
			case "null":
				vls.SetTemporalLayerSelector(temporallayerselector.NewNull())
			case "vp8":
				vls.SetTemporalLayerSelector(temporallayerselector.NewVP8(logger.GetLogger()))
			default:
				t.Fatalf("line %d: %s", n, line)
			}
		case "rollback":
			vls.Rollback()
		case "pkt":
			// pkt <layer> <spatial> <temporal> <key frame> <marker> <VP9 flags>
			extPkt := &buffer.ExtPacket{
				VideoLayer: refLayer(c[2], c[3]),
				Packet:     &rtp.Packet{Header: rtp.Header{Marker: c[5] == "1", SSRC: 1234}},
				IsKeyFrame: c[4] == "1",
			}
			if c[6] != "-" {
				extPkt.VP9 = &codecs.VP9Packet{
					U: strings.Contains(c[6], "U"),
					B: strings.Contains(c[6], "B"),
					E: strings.Contains(c[6], "E"),
					P: strings.Contains(c[6], "P"),
				}
			}
			r := vls.Select(extPkt, refInt(c[1]))
			out = fmt.Sprintf("pkt sel=%v rel=%v sw=%v res=%v m=%v dd=%d/%d", r.IsSelected, r.IsRelevant, r.IsSwitching, r.IsResuming, r.RTPMarker, r.DDBytesLen, len(r.DDBytesSpill))
		case "tpkt":
			// tpkt <temporal> <key frame> <marker> <VP8 flags>
			extPkt := &buffer.ExtPacket{
				VideoLayer: buffer.VideoLayer{Temporal: refInt(c[1])},
				Packet:     &rtp.Packet{Header: rtp.Header{Marker: c[3] == "1", SSRC: 1234}},
				IsKeyFrame: c[2] == "1",
			}
			if c[4] != "-" {
				extPkt.VP8 = &codec.VP8{
					IsKeyFrame: strings.Contains(c[4], "K"),
					S:          strings.Contains(c[4], "S"),
					Y:          strings.Contains(c[4], "Y"),
					TID:        uint8(refInt(c[1])),
				}
			}
			out = fmt.Sprintf("tpkt this=%d", vls.SelectTemporal(extPkt))
		default:
			t.Fatalf("line %d: %s", n, line)
		}

		locked, layer := vls.CheckSync()
		fmt.Printf("REF %d: %s | kind=%s cur=%d/%d tgt=%d/%d max=%d/%d seen=%d/%d req=%d sync=%v/%d over=%v\n",
			n, out, refKind(vls),
			vls.GetCurrent().Spatial, vls.GetCurrent().Temporal,
			vls.GetTarget().Spatial, vls.GetTarget().Temporal,
			vls.GetMax().Spatial, vls.GetMax().Temporal,
			vls.GetMaxSeen().Spatial, vls.GetMaxSeen().Temporal,
			vls.GetRequestSpatial(), locked, layer, vls.IsOvershootOkay(),
		)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}
