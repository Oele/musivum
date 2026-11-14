// New for musivum, not a part of LiveKit: prints what the original gives where the port's
// tests need more than the original's tests say. ../scorer_reference_test.rs and
// ../connectionstats_reference_test.rs do the same with the port and have the printed lines
// as their expected values (the first one a part of them: MUSIVUM_SCORER_REFERENCE may name a
// file with all the REF-SCORER lines, without that prefix, to have them all compared).
//
// It is laid over the package pkg/sfu/connectionquality of LiveKit at the pin, which is not
// written to. From the root of the repository, or of a git worktree of it, with the cap on
// memory and time that docs/porting-guide.md asks for:
//
//	d=$PWD/crates/musivum/src/sfu/connectionquality/testdata
//	r=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/reference/livekit
//	mkdir -p target && printf '{"Replace": {"%s": "%s"}}\n' \
//	    $r/pkg/sfu/connectionquality/musivum_reference_output_test.go $d/reference_output_test.go > target/connectionquality-overlay.json
//	(ulimit -d 8388608 && timeout 600 scripts/go-reference-test.sh -overlay $PWD/target/connectionquality-overlay.json \
//	    -count=1 -run TestReferenceOutput -v ./pkg/sfu/connectionquality) | grep '^REF-'
package connectionquality

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/rtpstats"
	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

// ------------------------------------------------
// the scorer

type mrng struct{ state uint64 }

func (r *mrng) next() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *mrng) intn(n int64) int64 {
	return int64(r.next() % uint64(n))
}

const (
	mKindAudio = iota
	mKindDTX
	mKindVideo
	mKindScreen
)

type mScenario struct {
	name          string
	kind          int
	mime          mime.MimeType
	fec           bool
	mime2         mime.MimeType
	fec2          bool
	includeRTT    bool
	includeJitter bool
	enableBitrate bool
	seed          uint64
	windows       int
}

var mBitrates = []int64{150_000, 500_000, 1_200_000, 2_500_000}
var mDistances = []float64{0.0, 0.5, 1.0, 1.5, 2.0, 2.6}
var mLayers = []int64{400, 1200, 2500}

func mMs(n int64) time.Duration {
	return time.Duration(n) * time.Millisecond
}

func mRun(sc mScenario, lines *[]string) {
	rng := &mrng{state: sc.seed}
	q := newQualityScorer(qualityScorerParams{
		IncludeRTT:         sc.includeRTT,
		IncludeJitter:      sc.includeJitter,
		EnableBitrateScore: sc.enableBitrate,
		Logger:             logger.GetLogger(),
	})
	at := time.Unix(1_700_000_000, 0)
	q.StartAt(getPacketLossWeight(sc.mime, sc.fec), at)

	emit := func(i int, tag string) {
		score32, quality := q.GetScoreAndQuality()
		mos, _ := q.GetMOSAndQuality()
		*lines = append(*lines, fmt.Sprintf("%s %d %s %016x %08x %08x %d %d",
			sc.name, i, tag, math.Float64bits(q.score), math.Float32bits(score32), math.Float32bits(mos), int(quality), q.ppsMode))
	}

	video := sc.kind == mKindVideo || sc.kind == mKindScreen
	muted, layerMuted, paused := false, false, false
	speech := true
	layerPps10 := int64(2500)
	expected := int64(0)

	for i := 0; i < sc.windows; i++ {
		durMs := int64(5000)
		if rng.intn(10) == 0 {
			durMs = 4000 + rng.intn(2500)
		}
		start := at
		end := at.Add(mMs(durMs))

		if i == sc.windows/2 {
			q.UpdatePacketLossWeight(getPacketLossWeight(sc.mime2, sc.fec2))
		}

		// mute
		r := rng.intn(100)
		off1 := rng.intn(durMs)
		off2 := off1 + rng.intn(durMs-off1+1)
		t1 := start.Add(mMs(off1))
		t2 := start.Add(mMs(off2))
		if muted {
			if r < 50 {
				q.UpdateMuteAt(false, t1)
				muted = false
				emit(i, "unmute")
			}
		} else {
			if r < 6 {
				q.UpdateMuteAt(true, t1)
				muted = true
				emit(i, "mute")
			} else if r < 9 {
				q.UpdateMuteAt(true, t1)
				emit(i, "mute")
				q.UpdateMuteAt(false, t2)
				emit(i, "unmute")
			} else if r < 12 {
				q.UpdateMuteAt(false, t1)
				emit(i, "unmute-again")
			}
		}

		if video {
			// layer mute
			r = rng.intn(100)
			t := start.Add(mMs(rng.intn(durMs)))
			if layerMuted {
				if r < 50 {
					q.UpdateLayerMuteAt(false, t)
					layerMuted = false
					emit(i, "layer-unmute")
				}
			} else if r < 4 {
				q.UpdateLayerMuteAt(true, t)
				layerMuted = true
				emit(i, "layer-mute")
			} else if r < 6 {
				q.UpdateLayerMuteAt(false, t)
				emit(i, "layer-unmute-again")
			}

			// pause
			r = rng.intn(100)
			t = start.Add(mMs(rng.intn(durMs)))
			if paused {
				if r < 50 {
					q.UpdatePauseAt(false, t)
					paused = false
					emit(i, "resume")
				}
			} else if r < 4 {
				q.UpdatePauseAt(true, t)
				paused = true
				emit(i, "pause")
			} else if r < 6 {
				q.UpdatePauseAt(false, t)
				emit(i, "resume-again")
			}

			// bitrate transitions
			r = rng.intn(100)
			if i == 0 || r < 30 {
				n := 1 + rng.intn(2)
				off := int64(0)
				for k := int64(0); k < n; k++ {
					off += rng.intn(durMs / 2)
					expected = mBitrates[rng.intn(int64(len(mBitrates)))]
					expected += rng.intn(50_000)
					q.AddBitrateTransitionAt(expected, start.Add(mMs(off)))
				}
			} else if r < 32 {
				// a sample before the last one, which the aggregator refuses
				q.AddBitrateTransitionAt(mBitrates[rng.intn(int64(len(mBitrates)))], start.Add(-time.Second))
			} else if r < 34 {
				// a sample beyond the end of the window: the update that follows cannot aggregate
				q.AddBitrateTransitionAt(expected, end.Add(time.Second))
			}

			// layer transitions
			r = rng.intn(100)
			if r < 20 {
				n := 1 + rng.intn(2)
				off := int64(0)
				for k := int64(0); k < n; k++ {
					off += rng.intn(durMs / 2)
					q.AddLayerTransitionAt(mDistances[rng.intn(int64(len(mDistances)))], start.Add(mMs(off)))
				}
			}
		}

		// the window
		var pps10 int64
		switch sc.kind {
		case mKindAudio:
			pps10 = 500
		case mKindDTX:
			r = rng.intn(100)
			if r < 15 {
				speech = !speech
			}
			if speech {
				pps10 = 500
			} else {
				pps10 = 25
			}
		case mKindVideo:
			r = rng.intn(100)
			if r < 8 {
				layerPps10 = mLayers[rng.intn(int64(len(mLayers)))]
			}
			pps10 = layerPps10
		case mKindScreen:
			r = rng.intn(100)
			if r < 60 {
				pps10 = rng.intn(100)
			} else {
				pps10 = rng.intn(3000)
			}
		}
		packets := uint32(pps10 * durMs / 10000)
		r = rng.intn(100)
		if r < 5 {
			packets = 0
		}

		// loss
		r = rng.intn(100)
		permille := int64(0)
		if r < 55 {
			permille = 0
		} else if r < 75 {
			permille = rng.intn(30)
		} else if r < 90 {
			permille = 30 + rng.intn(70)
		} else {
			permille = 100 + rng.intn(300)
		}
		lost := uint32(int64(packets) * permille / 1000)
		r = rng.intn(100)
		if r < 3 {
			lost += uint32(rng.intn(5))
		}

		var missing, ooo, padding uint32
		if video {
			r = rng.intn(100)
			if r < 10 {
				missing = uint32(rng.intn(int64(lost) + 1))
			}
			r = rng.intn(100)
			if r < 10 {
				ooo = uint32(rng.intn(int64(lost) + 3))
			}
			r = rng.intn(100)
			if r < 15 {
				padding = uint32(rng.intn(50))
			}
		}

		// bytes
		size := int64(100)
		if video {
			size = 900
		}
		bytes := uint64(int64(packets) * (size/2 + rng.intn(size)))
		if video {
			r = rng.intn(100)
			if r < 10 {
				bytes = bytes / uint64(2+rng.intn(60))
			} else if r < 13 {
				bytes = 0
			}
		}

		// round trip time
		r = rng.intn(100)
		var rtt uint32
		if r < 70 {
			rtt = uint32(rng.intn(80))
		} else if r < 90 {
			rtt = uint32(rng.intn(400))
		} else {
			rtt = uint32(rng.intn(1500))
		}

		// jitter, in microseconds
		r = rng.intn(100)
		var jitter float64
		if r < 70 {
			jitter = float64(rng.intn(20_000))
		} else {
			jitter = float64(rng.intn(400_000))
		}
		jitter += float64(rng.intn(1000)) / 1000.0

		// the last RTCP
		r = rng.intn(100)
		var lastRTCPAt time.Time
		if r < 40 {
			lastRTCPAt = end.Add(-time.Second)
		} else if r < 60 {
			lastRTCPAt = start.Add(-10 * time.Second)
		}

		stat := &windowStat{
			startedAt:         start,
			duration:          mMs(durMs),
			packets:           packets,
			packetsPadding:    padding,
			packetsLost:       lost,
			packetsMissing:    missing,
			packetsOutOfOrder: ooo,
			bytes:             bytes,
			rttMax:            rtt,
			jitterMax:         jitter,
			lastRTCPAt:        lastRTCPAt,
		}
		at = end
		q.UpdateAt(stat, at)
		emit(i, "window")
	}
}

func mScenarios() []mScenario {
	return []mScenario{
		{name: "opus", kind: mKindAudio, mime: mime.MimeTypeOpus, fec: false, mime2: mime.MimeTypeOpus, fec2: true,
			includeRTT: true, includeJitter: true, seed: 1, windows: 100},
		{name: "opus-dtx", kind: mKindDTX, mime: mime.MimeTypeOpus, fec: true, mime2: mime.MimeTypeRED, fec2: false,
			includeRTT: true, includeJitter: true, seed: 2, windows: 100},
		{name: "red", kind: mKindAudio, mime: mime.MimeTypeRED, fec: false, mime2: mime.MimeTypePCMU, fec2: false,
			includeRTT: false, includeJitter: true, seed: 3, windows: 100},
		{name: "red-dtx", kind: mKindDTX, mime: mime.MimeTypeRED, fec: true, mime2: mime.MimeTypeOpus, fec2: false,
			includeRTT: true, includeJitter: false, seed: 4, windows: 100},
		{name: "video", kind: mKindVideo, mime: mime.MimeTypeVP8, fec: false, mime2: mime.MimeTypeVP9, fec2: false,
			includeRTT: true, includeJitter: true, enableBitrate: true, seed: 5, windows: 100},
		{name: "screen", kind: mKindScreen, mime: mime.MimeTypeH264, fec: false, mime2: mime.MimeTypeAV1, fec2: false,
			includeRTT: false, includeJitter: false, enableBitrate: true, seed: 6, windows: 100},
	}
}

// What the scorer gives after every window and every event of six scenarios:
// ../scorer_reference_test.rs does the same with the port.
func TestReferenceOutputScorer(t *testing.T) {
	var lines []string
	for _, sc := range mScenarios() {
		mRun(sc, &lines)
	}
	for _, line := range lines {
		fmt.Println("REF-SCORER", line)
	}
}

// ------------------------------------------------
// the statistics a provider gives, as they are reported

type mSenderProvider struct {
	streams     map[uint32]*buffer.StreamStatsWithLayers
	lastRR      time.Time
	packetsSent uint64
}

func (p *mSenderProvider) GetDeltaStatsSender() map[uint32]*buffer.StreamStatsWithLayers {
	return p.streams
}

func (p *mSenderProvider) GetPrimaryStreamLastReceiverReportTime() time.Time {
	return p.lastRR
}

func (p *mSenderProvider) GetPrimaryStreamPacketsSent() uint64 {
	return p.packetsSent
}

func mDelta(base time.Time, k uint32) *rtpstats.RTPDeltaInfo {
	return &rtpstats.RTPDeltaInfo{
		StartTime:            base.Add(60 * time.Second),
		EndTime:              base.Add(65 * time.Second),
		Packets:              250 + k,
		Bytes:                30000 + uint64(k)*100,
		HeaderBytes:          3000 + uint64(k),
		PacketsDuplicate:     3 + k,
		BytesDuplicate:       360 + uint64(k),
		HeaderBytesDuplicate: 36,
		PacketsPadding:       4 + k,
		BytesPadding:         400 + uint64(k),
		HeaderBytesPadding:   48,
		PacketsLost:          10 + k,
		PacketsMissing:       2,
		PacketsOutOfOrder:    5 + k,
		Frames:               20 + k,
		RttMax:               120 + k,
		JitterMax:            25000.7 + float64(k),
		Nacks:                7 + k,
		NackRepeated:         1,
		Plis:                 2 + k,
		Firs:                 1 + k,
	}
}

func mStat(base time.Time, stat *livekit.AnalyticsStat) string {
	if stat == nil {
		return "no stat"
	}
	streams := append([]*livekit.AnalyticsStream{}, stat.Streams...)
	sort.Slice(streams, func(i, j int) bool { return streams[i].Ssrc < streams[j].Ssrc })
	s := fmt.Sprintf("score=%08x mime=%s", math.Float32bits(stat.Score), stat.Mime)
	for _, as := range streams {
		s += fmt.Sprintf(" [ssrc=%d start=%d end=%d pp=%d pb=%d rp=%d rb=%d padp=%d padb=%d lost=%d ooo=%d frames=%d rtt=%d jitter=%d nacks=%d plis=%d firs=%d layers=",
			as.Ssrc,
			as.StartTime.AsTime().Sub(base).Milliseconds(),
			as.EndTime.AsTime().Sub(base).Milliseconds(),
			as.PrimaryPackets, as.PrimaryBytes, as.RetransmitPackets, as.RetransmitBytes,
			as.PaddingPackets, as.PaddingBytes, as.PacketsLost, as.PacketsOutOfOrder,
			as.Frames, as.Rtt, as.Jitter, as.Nacks, as.Plis, as.Firs,
		)
		layers := append([]*livekit.AnalyticsVideoLayer{}, as.VideoLayers...)
		sort.Slice(layers, func(i, j int) bool { return layers[i].Layer < layers[j].Layer })
		for _, l := range layers {
			s += fmt.Sprintf("(%d %d %d %d)", l.Layer, l.Packets, l.Bytes, l.Frames)
		}
		s += "]"
	}
	return s
}

type mSession struct {
	cs    *ConnectionStats
	last  *livekit.AnalyticsStat
	calls int
}

// What getStat hands to the callback for the streams a provider gives:
// ../connectionstats_reference_test.rs takes the same steps with the port.
func TestReferenceOutputConnectionStats(t *testing.T) {
	var lines []string
	// to the millisecond, as the times are compared after a round trip through a timestamp
	base := time.UnixMilli(time.Now().UnixMilli())

	newSession := func(params ConnectionStatsParams, mimeType mime.MimeType) *mSession {
		s := &mSession{}
		params.Logger = logger.GetLogger()
		s.cs = NewConnectionStats(params)
		s.cs.OnStatsUpdate(func(cs *ConnectionStats, stat *livekit.AnalyticsStat) {
			if cs != s.cs {
				t.Fatal("not the statistics the callback was given to")
			}
			s.last = stat
			s.calls++
		})
		s.cs.StartAt(mimeType, false, base.Add(-5*time.Second))
		return s
	}
	step := func(name string, s *mSession) {
		s.last = nil
		time.Sleep(2 * time.Millisecond)
		s.cs.getStat()
		mos, quality := s.cs.GetScoreAndQuality()
		lines = append(lines, fmt.Sprintf("%s: calls=%d mos=%08x quality=%d %s",
			name, s.calls, math.Float32bits(mos), int(quality), mStat(base, s.last)))
	}

	// a receiver of audio
	trp := newTestReceiverProvider()
	r := newSession(ConnectionStatsParams{ReceiverProvider: trp, IncludeRTT: true, IncludeJitter: true}, mime.MimeTypeOpus)
	step("receiver, no streams", r)
	trp.setStreams(map[uint32]*buffer.StreamStatsWithLayers{
		1: {RTPStats: mDelta(base, 0)},
	})
	step("receiver, audio", r)
	trp.setStreams(map[uint32]*buffer.StreamStatsWithLayers{
		1: {RTPStats: mDelta(base, 1), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 1)}},
		2: {RTPStats: mDelta(base, 2), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 2)}},
	})
	step("receiver, audio, two streams", r)
	trp.setStreams(map[uint32]*buffer.StreamStatsWithLayers{
		1: {},
		2: {Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 2)}},
	})
	step("receiver, streams without statistics", r)

	// a receiver of video
	rv := newSession(ConnectionStatsParams{ReceiverProvider: trp}, mime.MimeTypeVP8)
	noFrames := mDelta(base, 5)
	noFrames.Frames = 0
	trp.setStreams(map[uint32]*buffer.StreamStatsWithLayers{
		10: {RTPStats: mDelta(base, 3), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 3)}},
		11: {RTPStats: mDelta(base, 4), Layers: map[int32]*rtpstats.RTPDeltaInfo{1: mDelta(base, 4)}},
		12: {Layers: map[int32]*rtpstats.RTPDeltaInfo{2: mDelta(base, 5)}},
		13: {RTPStats: mDelta(base, 6), Layers: map[int32]*rtpstats.RTPDeltaInfo{2: nil, 1: noFrames}},
	})
	step("receiver, simulcast", rv)
	trp.setStreams(map[uint32]*buffer.StreamStatsWithLayers{
		20: {RTPStats: mDelta(base, 7), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 8), 1: mDelta(base, 9), 2: mDelta(base, 10)}},
	})
	step("receiver, layers of one stream", rv)
	trp.setStreams(map[uint32]*buffer.StreamStatsWithLayers{
		20: {RTPStats: mDelta(base, 7), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 8)}},
	})
	step("receiver, one stream with one layer", rv)
	rv.cs.UpdateCodec(mime.MimeTypeOpus, true)
	trp.setStreams(map[uint32]*buffer.StreamStatsWithLayers{
		20: {RTPStats: mDelta(base, 7), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 8), 1: mDelta(base, 9), 2: mDelta(base, 10)}},
	})
	step("receiver, codec updated to audio", rv)

	// a sender of video, which also has a receiver provider that is not to be asked
	tsp := &mSenderProvider{}
	s := newSession(ConnectionStatsParams{SenderProvider: tsp, ReceiverProvider: trp}, mime.MimeTypeVP8)
	step("sender, not streaming", s)
	tsp.packetsSent = 100
	step("sender, streaming, no report yet", s)

	tsp.packetsSent = 200
	tsp.lastRR = base.Add(-time.Hour)
	remote := mDelta(base, 12)
	remote.JitterMax = 30000.5
	tsp.streams = map[uint32]*buffer.StreamStatsWithLayers{
		5: {RTPStats: mDelta(base, 11), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 11)}, RTPStatsRemoteView: remote},
	}
	step("sender, report from before the start", s)

	tsp.packetsSent = 300
	tsp.lastRR = base.Add(time.Hour)
	remote = mDelta(base, 12)
	remote.JitterMax = 30000.5
	tsp.streams = map[uint32]*buffer.StreamStatsWithLayers{
		5: {RTPStats: mDelta(base, 11), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 11)}, RTPStatsRemoteView: remote},
	}
	step("sender, remote view", s)

	tsp.packetsSent = 400
	remote = mDelta(base, 14)
	remote.JitterMax = 100.5
	remote.PacketsLost = 3
	remote.PacketsMissing = 8
	tsp.streams = map[uint32]*buffer.StreamStatsWithLayers{
		5: {RTPStats: mDelta(base, 13), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 13)}, RTPStatsRemoteView: remote},
	}
	step("sender, remote view with less jitter, and more missing than lost", s)

	tsp.packetsSent = 500
	tsp.streams = map[uint32]*buffer.StreamStatsWithLayers{
		5: {RTPStats: mDelta(base, 15), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 15)}},
	}
	step("sender, no remote view", s)

	// no packets sent since
	step("sender, stopped streaming", s)

	tsp.packetsSent = 600
	remote = mDelta(base, 17)
	remote.PacketsLost = 60
	remote.JitterMax = 90000.25
	remote2 := mDelta(base, 19)
	remote2.PacketsLost = 45
	remote2.JitterMax = 70000.75
	tsp.streams = map[uint32]*buffer.StreamStatsWithLayers{
		5: {RTPStats: mDelta(base, 16), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 16)}, RTPStatsRemoteView: remote},
		6: {RTPStats: mDelta(base, 18), Layers: map[int32]*rtpstats.RTPDeltaInfo{0: mDelta(base, 18)}, RTPStatsRemoteView: remote2},
	}
	step("sender, two streams", s)

	// closed: the calls on the scorer are dropped, the score stays
	s.cs.Close()
	s.cs.UpdatePauseAt(true, base)
	s.cs.UpdateMuteAt(true, base)
	s.cs.UpdateLayerMuteAt(true, base)
	mos, quality := s.cs.GetScoreAndQuality()
	lines = append(lines, fmt.Sprintf("sender, closed: mos=%08x quality=%d", math.Float32bits(mos), int(quality)))

	// neither provider
	n := newSession(ConnectionStatsParams{}, mime.MimeTypeVP8)
	step("no provider", n)

	for _, line := range lines {
		fmt.Println("REF-STATS", line)
	}
}
