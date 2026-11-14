// New for musivum, not a part of LiveKit: writes three streams into LiveKit's buffer (VP8,
// Opus, VP8 with a repair stream) and prints what comes out. A line that starts with "OPS "
// is an operation on a buffer (a packet written, a bind, a sender report, ...), a line that
// starts with "REF " is what the original answered. ../buffer_reference_test.rs replays the
// operations through the port and expects the same answers; buffer_reference.ops and
// buffer_reference.out next to this file are a short run, pinned there.
//
// What is printed leaves out what the original reads from the clock: start and end times,
// durations, rates, jitter, the NACKs and PLIs sent and their counts (a sequence number is
// asked for once it has been missing for 20 ms and again by the round trip time, and the key
// frame seeder asks for key frames every second), receiver reports as the buffer sends them
// (once a second: the test asks for the reception report itself), the audio level. In the
// place of the NACKs it prints which sequence numbers wait in the NACK queue after each
// phase. A sequence number leaves that queue after it was asked for five times: the test
// sets a round trip time so long that this takes two seconds, which a run does not take.
//
// It runs inside LiveKit's package at the pin, without being put there. From the root of
// the repository, or of a git worktree of it (`reference` is in the main checkout), with the
// cap on memory and time that docs/porting-guide.md asks for:
//
//	d=$PWD/crates/musivum/src/sfu/buffer/testdata
//	p=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/reference/livekit/pkg/sfu/buffer
//	mkdir -p target && echo "{\"Replace\": {\"$p/buffer_reference_test.go\": \"$d/buffer_reference_test.go\"}}" > $PWD/target/buffer_reference_overlay.json
//	(ulimit -d 8388608; timeout 600 scripts/go-reference-test.sh -overlay $PWD/target/buffer_reference_overlay.json \
//	    -run TestBufferReference -v ./pkg/sfu/buffer) > target/buffer_reference.txt
//	grep '^OPS ' target/buffer_reference.txt | cut -c5- > $d/buffer_reference.ops
//	grep '^REF ' target/buffer_reference.txt | cut -c5- > $d/buffer_reference.out
//
// With MUSIVUM_BUFFER_REFERENCE_PACKETS=<n> in the environment a phase has about n packets
// (4 without it) and there is a phase more, in which the reader falls behind.
package buffer

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/pion/transport/v4/packetio"
	"github.com/pion/webrtc/v4"

	act "github.com/livekit/livekit-server/pkg/sfu/rtpextension/abscapturetime"
	"github.com/livekit/protocol/livekit"
)

// SplitMix64
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
func refHash(p []byte) uint64 {
	h := uint64(0xcbf29ce484222325)
	for _, b := range p {
		h ^= uint64(b)
		h *= 0x100000001b3
	}
	return h
}

const (
	refVP8PT  = 96
	refRTXPT  = 97
	refOpusPT = 111
)

type refStream struct {
	name string
	kind string // vp8, opus, vp8rtx

	factory *Factory
	primary *Buffer
	rtx     *Buffer
	ssrc    uint32
	rtxSSRC uint32

	rng              refRng
	sn               uint16
	ts               uint32
	rtxSN            uint16
	pictureID        uint16
	tl0              uint8
	frames           int
	packetsLeft      int
	tid              uint8
	keyFrame         bool
	firstOfFrame     bool
	lastESN          uint64
	senderReports    int
	senderReportTime int64
}

// ---- the operations: each prints itself and what the buffer answers

func (s *refStream) opNew() {
	fmt.Printf("OPS new %s %s %d %d\n", s.name, s.kind, s.ssrc, s.rtxSSRC)
	s.factory = NewFactoryOfBufferFactory(InitPacketBufferSizeVideo, InitPacketBufferSizeAudio).CreateBufferFactory()
	s.primary = s.factory.GetOrNew(packetio.RTPBufferPacket, s.ssrc).(*Buffer)
	if s.kind == "vp8rtx" {
		s.rtx = s.factory.GetOrNew(packetio.RTPBufferPacket, s.rtxSSRC).(*Buffer)
	}
	fmt.Printf("REF %s new\n", s.name)
}

func (s *refStream) opPair() {
	fmt.Printf("OPS pair %s\n", s.name)
	s.factory.SetRTXPair(s.rtxSSRC, s.ssrc, "")
}

func (s *refStream) opBind() {
	fmt.Printf("OPS bind %s\n", s.name)
	nack := []webrtc.RTCPFeedback{{Type: "nack"}}
	vp8 := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: "video/VP8", ClockRate: 90000, RTCPFeedback: nack},
		PayloadType:        refVP8PT,
	}
	var params webrtc.RTPParameters
	var codec webrtc.RTPCodecCapability
	bitrate := 0
	switch s.kind {
	case "vp8":
		params = webrtc.RTPParameters{
			HeaderExtensions: []webrtc.RTPHeaderExtensionParameter{{URI: act.AbsCaptureTimeURI, ID: 2}},
			Codecs:           []webrtc.RTPCodecParameters{vp8},
		}
		codec = vp8.RTPCodecCapability
	case "opus":
		opus := webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: "audio/opus", ClockRate: 48000, Channels: 2, RTCPFeedback: nack},
			PayloadType:        refOpusPT,
		}
		params = webrtc.RTPParameters{
			HeaderExtensions: []webrtc.RTPHeaderExtensionParameter{{URI: sdp.AudioLevelURI, ID: 1}},
			Codecs:           []webrtc.RTPCodecParameters{opus},
		}
		codec = opus.RTPCodecCapability
	case "vp8rtx":
		rtx := webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: "video/rtx", ClockRate: 90000, SDPFmtpLine: "apt=96"},
			PayloadType:        refRTXPT,
		}
		params = webrtc.RTPParameters{Codecs: []webrtc.RTPCodecParameters{vp8, rtx}}
		codec = vp8.RTPCodecCapability
		// more packets a second than the bucket has room for: it grows once
		bitrate = 5_000_000
	}
	err := s.primary.Bind(params, codec, bitrate)

	// no receiver report by the clock
	s.primary.Lock()
	s.primary.lastReportAt = 1 << 62
	capacity := 0
	if s.primary.bucket != nil {
		capacity = s.primary.bucket.Capacity()
	}
	s.primary.Unlock()
	fmt.Printf("REF %s bind err=%t capacity=%d\n", s.name, err != nil, capacity)
}

func refWriteError(err error) string {
	if err == io.EOF {
		return "eof"
	}
	return "other"
}

func (s *refStream) opWrite(raw []byte) {
	fmt.Printf("OPS w %s %s\n", s.name, hex.EncodeToString(raw))
	if _, err := s.primary.Write(raw); err != nil {
		fmt.Printf("REF %s w err=%s\n", s.name, refWriteError(err))
	}
}

func (s *refStream) opWriteRTX(raw []byte) {
	fmt.Printf("OPS x %s %s\n", s.name, hex.EncodeToString(raw))
	if _, err := s.rtx.Write(raw); err != nil {
		fmt.Printf("REF %s x err=%s\n", s.name, refWriteError(err))
	}
}

// Reads what there is to read. ReadExtended waits when there is nothing, also after it has
// thrown away packets that are not in the bucket any more: so this looks first how many of
// the packets that wait are still there, and reads that many.
func (s *refStream) opDrain() {
	fmt.Printf("OPS d %s\n", s.name)
	var buf [1500]byte
	for {
		if s.primary.IsClosed() {
			_, err := s.primary.ReadExtended(buf[:])
			fmt.Printf("REF %s read err=%s\n", s.name, refWriteError(err))
			return
		}

		s.primary.RLock()
		pending := s.primary.isRestartPending
		readable := 0
		var tmp [1500]byte
		for i := 0; i < s.primary.extPackets.Len(); i++ {
			if _, err := s.primary.bucket.GetPacket(tmp[:], s.primary.extPackets.At(i).ExtSequenceNumber); err == nil {
				readable++
			}
		}
		s.primary.RUnlock()
		if !pending && readable == 0 {
			return
		}

		ep, err := s.primary.ReadExtended(buf[:])
		if err != nil {
			fmt.Printf("REF %s read err=%s\n", s.name, refWriteError(err))
			return
		}
		if ep == nil {
			fmt.Printf("REF %s restart\n", s.name)
			continue
		}

		ext1 := "-"
		if e := ep.Packet.GetExtension(1); e != nil {
			ext1 = hex.EncodeToString(e)
		}
		vp8 := "-"
		if ep.VP8 != nil {
			vp8 = fmt.Sprintf("%d,%d,%d,%d,%d,%t", ep.VP8.PictureID, ep.VP8.TL0PICIDX, ep.VP8.TID, ep.VP8.KEYIDX, ep.VP8.HeaderSize, ep.VP8.IsKeyFrame)
		}
		fmt.Printf(
			"REF %s ep esn=%d ets=%d sn=%d ts=%d m=%t pt=%d s=%d t=%d kf=%t ooo=%t buf=%t plen=%d ph=%016x rawlen=%d rh=%016x ext1=%s vp8=%s act=%t\n",
			s.name,
			ep.ExtSequenceNumber, ep.ExtTimestamp,
			ep.Packet.SequenceNumber, ep.Packet.Timestamp, ep.Packet.Marker, ep.Packet.PayloadType,
			ep.Spatial, ep.Temporal,
			ep.IsKeyFrame, ep.IsOutOfOrder, ep.IsBuffered,
			len(ep.Packet.Payload), refHash(ep.Packet.Payload),
			len(ep.RawPacket), refHash(ep.RawPacket),
			ext1, vp8, ep.AbsCaptureTimeExt != nil,
		)
		s.lastESN = ep.ExtSequenceNumber
		ReleaseExtPacket(ep)
	}
}

func (s *refStream) opSenderReport() {
	s.senderReports++
	s.senderReportTime += 1_000_000_000 + int64(s.rng.intn(1000))*1_000_000
	at := s.senderReportTime
	ntp := (uint64(at/1_000_000_000)+2208988800)<<32 | uint64(at%1_000_000_000)*4
	packets := uint32(s.senderReports * 100)
	octets := uint64(s.senderReports * 100_000)
	fmt.Printf("OPS sr %s %d %d %d %d %d\n", s.name, ntp, s.ts, at, packets, octets)
	s.primary.SetSenderReportData(&livekit.RTCPSenderReportState{
		RtpTimestamp: s.ts,
		NtpTimestamp: ntp,
		At:           at,
		Packets:      packets,
		Octets:       octets,
	})
	sr := s.primary.GetSenderReportData()
	if sr == nil {
		fmt.Printf("REF %s sr nil\n", s.name)
		return
	}
	fmt.Printf("REF %s sr rtp=%d rtpext=%d ntp=%d at=%d adj=%d pkts=%d octets=%d\n", s.name, sr.RtpTimestamp, sr.RtpTimestampExt, sr.NtpTimestamp, sr.At, sr.AtAdjusted, sr.Packets, sr.Octets)
}

func (s *refStream) opGetPacket(esn uint64) {
	fmt.Printf("OPS gp %s %d\n", s.name, esn)
	var buf [1500]byte
	n, err := s.primary.GetPacket(buf[:], esn)
	if err != nil {
		fmt.Printf("REF %s gp esn=%d err\n", s.name, esn)
		return
	}
	fmt.Printf("REF %s gp esn=%d n=%d h=%016x\n", s.name, esn, n, refHash(buf[:n]))
}

func (s *refStream) opRTT(rtt uint32) {
	fmt.Printf("OPS rtt %s %d\n", s.name, rtt)
	s.primary.SetRTT(rtt)
}

func (s *refStream) opRestartDetection(enable bool) {
	fmt.Printf("OPS rd %s %t\n", s.name, enable)
	s.primary.SetStreamRestartDetection(enable)
}

func (s *refStream) opPhase(label string) {
	fmt.Printf("OPS phase %s %s\n", s.name, label)

	// the sequence numbers that wait to be asked for again, in the order of the queue. Its
	// entries keep them to themselves: they are read from how an entry is printed.
	queued := []string{}
	s.primary.RLock()
	if s.primary.nacker != nil {
		for _, n := range s.primary.nacker.Nacks() {
			queued = append(queued, refNackSeqNum.FindStringSubmatch(fmt.Sprintf("%+v", *n))[1])
		}
	}
	s.primary.RUnlock()
	fmt.Printf("REF %s phase %s nackq=[%s]\n", s.name, label, strings.Join(queued, " "))

	if stats := s.primary.GetStats(); stats == nil {
		fmt.Printf("REF %s stats nil\n", s.name)
	} else {
		gaps := make([]int, 0, len(stats.GapHistogram))
		for gap := range stats.GapHistogram {
			gaps = append(gaps, int(gap))
		}
		sort.Ints(gaps)
		var histogram []string
		for _, gap := range gaps {
			histogram = append(histogram, fmt.Sprintf("%d:%d", gap, stats.GapHistogram[int32(gap)]))
		}
		fmt.Printf(
			"REF %s stats packets=%d bytes=%d hdr=%d lost=%d loss=%s dup=%d dupbytes=%d duphdr=%d pad=%d padbytes=%d padhdr=%d ooo=%d frames=%d kf=%d fec=%d gaps=[%s]\n",
			s.name,
			stats.Packets, stats.Bytes, stats.HeaderBytes,
			stats.PacketsLost, strconv.FormatFloat(float64(stats.PacketLossPercentage), 'f', 4, 32),
			stats.PacketsDuplicate, stats.BytesDuplicate, stats.HeaderBytesDuplicate,
			stats.PacketsPadding, stats.BytesPadding, stats.HeaderBytesPadding,
			stats.PacketsOutOfOrder, stats.Frames, stats.KeyFrames, stats.FecPackets,
			strings.Join(histogram, " "),
		)
	}

	if delta := s.primary.GetDeltaStats(); delta == nil {
		fmt.Printf("REF %s delta nil\n", s.name)
	} else {
		d := delta.RTPStats
		fmt.Printf(
			"REF %s delta packets=%d bytes=%d hdr=%d dup=%d dupbytes=%d duphdr=%d pad=%d padbytes=%d padhdr=%d lost=%d missing=%d ooo=%d frames=%d layers=%d same=%t\n",
			s.name,
			d.Packets, d.Bytes, d.HeaderBytes,
			d.PacketsDuplicate, d.BytesDuplicate, d.HeaderBytesDuplicate,
			d.PacketsPadding, d.BytesPadding, d.HeaderBytesPadding,
			d.PacketsLost, d.PacketsMissing, d.PacketsOutOfOrder, d.Frames,
			len(delta.Layers), delta.Layers[0] == d,
		)
	}

	s.primary.Lock()
	rr := s.primary.GetRtcpReceptionReportLocked(0)
	s.primary.Unlock()
	if rr == nil {
		fmt.Printf("REF %s rr nil\n", s.name)
	} else {
		fmt.Printf("REF %s rr ssrc=%d frac=%d total=%d last=%d lsr=%d\n", s.name, rr.SSRC, rr.FractionLost, rr.TotalLost, rr.LastSequenceNumber, rr.LastSenderReport)
	}

	// what a retransmission would be answered from
	for _, back := range []uint64{0, 1, 2, 7, 40, 330, 700} {
		s.opGetPacket(s.lastESN - back)
	}
	s.opGetPacket(s.lastESN + 1)
}

var refNackSeqNum = regexp.MustCompile(`seqNum:(\d+)`)

func (s *refStream) opClose() {
	fmt.Printf("OPS close %s\n", s.name)
	err := s.primary.Close()
	fmt.Printf("REF %s close err=%t\n", s.name, err != nil)
}

// ---- the packets

// The next media packet of the stream.
func (s *refStream) media() *rtp.Packet {
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			SequenceNumber: s.sn,
			SSRC:           s.ssrc,
		},
	}
	s.sn++

	if s.kind == "opus" {
		s.ts += 960
		pkt.PayloadType = refOpusPT
		pkt.Timestamp = s.ts
		pkt.Payload = make([]byte, 10+s.rng.intn(50))
		s.rng.read(pkt.Payload)
		if err := pkt.SetExtension(1, []byte{byte(s.rng.intn(256))}); err != nil {
			panic(err)
		}
		return pkt
	}

	if s.packetsLeft == 0 {
		// a new frame
		s.frames++
		s.packetsLeft = 1 + s.rng.intn(3)
		s.firstOfFrame = true
		s.ts += 3000
		s.pictureID = (s.pictureID + 1) & 0x7fff
		s.keyFrame = s.frames%9 == 1
		s.tid = []uint8{0, 2, 1, 2}[s.frames%4]
		if s.keyFrame {
			s.tid = 0
		}
		if s.tid == 0 {
			s.tl0++
		}
	}
	s.packetsLeft--

	pkt.PayloadType = refVP8PT
	pkt.Timestamp = s.ts
	pkt.Marker = s.packetsLeft == 0

	size := 10 + s.rng.intn(50)
	if s.rng.intn(40) == 0 {
		size = 1100
	}
	payload := make([]byte, 6+size)
	s.rng.read(payload)
	payload[0] = 0x80 // X
	if s.firstOfFrame {
		payload[0] |= 0x10 // S
	}
	payload[1] = 0xf0 // I, L, T, K
	payload[2] = 0x80 | byte(s.pictureID>>8)
	payload[3] = byte(s.pictureID)
	payload[4] = s.tl0
	payload[5] = s.tid<<6 | byte(s.frames/9)&0x1f
	if s.firstOfFrame {
		if s.keyFrame {
			payload[6] &^= 0x01
			// the start code and the size of the picture
			copy(payload[9:], []byte{0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01})
			if s.kind == "vp8" {
				ext := make([]byte, 8)
				s.rng.read(ext)
				if err := pkt.SetExtension(2, ext); err != nil {
					panic(err)
				}
			}
		} else {
			payload[6] |= 0x01
		}
	}
	s.firstOfFrame = false
	pkt.Payload = payload
	return pkt
}

// A packet that is only padding, in the place of the next media packet.
func (s *refStream) padding() *rtp.Packet {
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			Padding:        true,
			PaddingSize:    byte(1 + s.rng.intn(200)),
			PayloadType:    refVP8PT,
			SequenceNumber: s.sn,
			Timestamp:      s.ts,
			SSRC:           s.ssrc,
		},
	}
	if s.kind == "opus" {
		pkt.PayloadType = refOpusPT
	}
	s.sn++
	return pkt
}

// The packet as its publisher sends it again on the repair stream.
func (s *refStream) repair(pkt *rtp.Packet) *rtp.Packet {
	s.rtxSN++
	payload := make([]byte, 2+len(pkt.Payload))
	payload[0] = byte(pkt.SequenceNumber >> 8)
	payload[1] = byte(pkt.SequenceNumber)
	copy(payload[2:], pkt.Payload)
	return &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			Marker:         pkt.Marker,
			PayloadType:    refRTXPT,
			SequenceNumber: s.rtxSN,
			Timestamp:      pkt.Timestamp,
			SSRC:           s.rtxSSRC,
		},
		Payload: payload,
	}
}

func refMarshal(pkt *rtp.Packet) []byte {
	raw, err := pkt.Marshal()
	if err != nil {
		panic(err)
	}
	return raw
}

func (s *refStream) write(pkt *rtp.Packet) {
	s.opWrite(refMarshal(pkt))
	s.opDrain()
}

// A packet that was lost arrives after all: on the repair stream if there is one.
func (s *refStream) late(pkt *rtp.Packet) {
	if s.kind == "vp8rtx" {
		s.opWriteRTX(refMarshal(s.repair(pkt)))
		s.opDrain()
		return
	}
	s.write(pkt)
}

// ---- the stream

func (s *refStream) run(n int, burst bool) {
	s.opNew()

	// before the bind: packets wait, and what comes on the repair stream is dropped
	first := s.media()
	s.opWrite(refMarshal(first))
	if s.kind == "vp8rtx" {
		s.opWriteRTX(refMarshal(s.repair(first)))
		s.opPair()
		s.opWriteRTX(refMarshal(s.repair(first)))
	}
	s.opWrite(refMarshal(s.media()))
	s.opWrite(refMarshal(s.media()))
	s.opDrain()
	s.opBind()
	s.opDrain()
	s.opRTT(100_000)
	s.opPhase("bound")

	// in order
	for i := 0; i < n; i++ {
		s.write(s.media())
		if i == n/2 {
			s.opSenderReport()
		}
	}
	s.opPhase("in-order")

	// lost, and most of it late
	type latePacket struct {
		pkt *rtp.Packet
		due int
	}
	var lost []latePacket
	for i := 0; i < 2*n; i++ {
		pkt := s.media()
		if s.rng.intn(5) == 0 {
			if s.rng.intn(4) != 0 {
				lost = append(lost, latePacket{pkt, i + 2 + s.rng.intn(8)})
			}
		} else {
			s.write(pkt)
		}
		kept := lost[:0]
		for _, l := range lost {
			if l.due <= i {
				s.late(l.pkt)
			} else {
				kept = append(kept, l)
			}
		}
		lost = kept
	}
	for _, l := range lost {
		s.late(l.pkt)
	}
	s.opSenderReport()
	s.opPhase("lost")

	// out of order and twice
	for i := 0; i < 2*n; i += 4 {
		window := []*rtp.Packet{s.media(), s.media(), s.media(), s.media()}
		for j := len(window) - 1; j > 0; j-- {
			k := s.rng.intn(j + 1)
			window[j], window[k] = window[k], window[j]
		}
		for j, pkt := range window {
			s.write(pkt)
			if s.rng.intn(6) == 0 {
				s.write(window[s.rng.intn(j+1)])
			}
		}
	}
	s.opPhase("reordered")

	// padding only: in its place, late, twice; and what a repair stream has besides repairs
	var heldBack *rtp.Packet
	heldBackFor := 0
	for i := 0; i < 2*n; i++ {
		switch r := s.rng.intn(16); {
		case r < 3:
			pkt := s.padding()
			s.write(pkt)
			if r == 0 {
				s.write(pkt)
			}
		case r == 3 && heldBack == nil:
			heldBack = s.padding()
			heldBackFor = 2
		case r == 4 && s.kind == "vp8rtx":
			// padding only on the repair stream, a repair with another payload type, one
			// that is too short to be one, a repair of a packet that is there
			probe := &rtp.Packet{Header: rtp.Header{
				Version: 2, Padding: true, PaddingSize: 100, PayloadType: refRTXPT,
				SequenceNumber: s.rtxSN + 1, Timestamp: s.ts, SSRC: s.rtxSSRC,
			}}
			s.rtxSN++
			s.opWriteRTX(refMarshal(probe))
			other := s.repair(&rtp.Packet{Header: rtp.Header{SequenceNumber: s.sn - 1, Timestamp: s.ts}, Payload: []byte{1, 2, 3}})
			other.PayloadType = 99
			s.opWriteRTX(refMarshal(other))
			short := s.repair(&rtp.Packet{Header: rtp.Header{SequenceNumber: s.sn - 1, Timestamp: s.ts}})
			short.Payload = short.Payload[:1]
			s.opWriteRTX(refMarshal(short))
			s.opDrain()
		default:
			pkt := s.media()
			s.write(pkt)
			if r == 5 {
				s.late(pkt)
			}
		}
		if heldBack != nil {
			if heldBackFor == 0 {
				s.write(heldBack)
				heldBack = nil
			}
			heldBackFor--
		}
	}
	if heldBack != nil {
		s.write(heldBack)
	}
	s.opSenderReport()
	s.opPhase("padding")

	if burst {
		// the reader falls behind: more packets than the bucket holds before it reads
		s.primary.RLock()
		capacity := s.primary.bucket.Capacity()
		s.primary.RUnlock()
		for i := 0; i < capacity+50; i++ {
			s.opWrite(refMarshal(s.media()))
		}
		s.opDrain()
		s.opPhase("burst")
	}

	// the publisher starts over: sequence numbers go on, time stamps start again far back.
	// The Opus stream is not told to look for that and drops the packets.
	if s.kind != "opus" {
		s.opRestartDetection(true)
	}
	s.ts -= 50_000_000
	before := s.lastESN
	for i := 0; i < 12; i++ {
		s.write(s.media())
	}
	s.opGetPacket(before)
	s.opSenderReport()
	s.opPhase("restarted")

	// not a packet, a packet of no stream, and the end
	s.opWrite([]byte{0x80, 0x60, 0x00})
	probe := s.media()
	probe.SSRC = 0
	s.opWrite(refMarshal(probe))
	s.opDrain()
	s.opClose()
	s.opWrite(refMarshal(s.media()))
	s.opDrain()
	s.opClose()
}

func TestBufferReference(t *testing.T) {
	n := 4
	burst := false
	if v := os.Getenv("MUSIVUM_BUFFER_REFERENCE_PACKETS"); v != "" {
		var err error
		if n, err = strconv.Atoi(v); err != nil {
			t.Fatal(err)
		}
		burst = true
	}

	streams := []*refStream{
		{name: "v", kind: "vp8", ssrc: 0x1001, rng: refRng{1}, sn: 65500, ts: 0xffff0000, senderReportTime: 1_700_000_000_000_000_000},
		{name: "o", kind: "opus", ssrc: 0x1002, rng: refRng{2}, sn: 100, ts: 1000, senderReportTime: 1_700_000_000_000_000_000},
		{name: "r", kind: "vp8rtx", ssrc: 0x1003, rtxSSRC: 0x2003, rng: refRng{3}, sn: 65530, ts: 500_000, rtxSN: 7000, senderReportTime: 1_700_000_000_000_000_000},
	}
	for _, s := range streams {
		s.run(n, burst)
	}
}
