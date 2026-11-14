// New for musivum, not a part of LiveKit: prints what the helpers of utils.go give for two
// real descriptions (../../sdp/testdata/offer.sdp and answer.sdp), for candidates as they
// are and as JSON that Go's decoder takes or refuses, and for ICE servers. Its output is
// reference_output.txt, the expected values of ../reference_test.rs, which asks the port the
// same. The original has no tests of its own.
//
// It runs inside a copy of livekit/protocol at the pin. From the root of the repository, or
// of a git worktree of it (`reference` is in the main checkout), with the cap on memory and
// time that docs/porting-guide.md asks for:
//
//	d=$PWD/crates/musivum/src/protocol/signalling/testdata
//	m=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/reference/gomod
//	rm -rf target/protocol-copy && mkdir -p target
//	cp -r $m/github.com/livekit/protocol@v1.53.1-0.20261023175527-46332a036e25 target/protocol-copy
//	chmod -R u+w target/protocol-copy && cp $d/reference_output_test.go target/protocol-copy/signalling/
//	(cd target/protocol-copy && ulimit -d 8388608 && MUSIVUM_SDP_TESTDATA=$d/../../sdp/testdata GOMODCACHE=$m \
//	    GOFLAGS=-modcacherw GOCACHE=$HOME/.cache/musivum/gocache GOPROXY=off \
//	    timeout 600 go test -count=1 -run TestReferenceOutput -v ./signalling) | grep '^REF ' > $d/reference_output.txt
package signalling

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"

	"github.com/livekit/protocol/livekit"
)

// A string in quotes, written the same way on the Rust side: all but printable ASCII as the
// number of the character.
func refQuote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r >= 0x20 && r < 0x7f:
			b.WriteRune(r)
		default:
			fmt.Fprintf(&b, "\\u{%x}", r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func refOptional(s *string) string {
	if s == nil {
		return "nil"
	}
	return refQuote(*s)
}

func refMap(m map[string]string) string {
	pairs := make([]string, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, k+"="+v)
	}
	sort.Strings(pairs)
	return "[" + strings.Join(pairs, " ") + "]"
}

func TestReferenceOutput(t *testing.T) {
	for _, name := range []string{"offer", "answer"} {
		data, err := os.ReadFile(os.Getenv("MUSIVUM_SDP_TESTDATA") + "/" + name + ".sdp")
		if err != nil {
			t.Fatal(err)
		}
		text := strings.ReplaceAll(string(data), "\n", "\r\n")
		typ := webrtc.SDPTypeOffer
		if name == "answer" {
			typ = webrtc.SDPTypeAnswer
		}
		sd := webrtc.SessionDescription{Type: typ, SDP: text}
		p := ToProtoSessionDescription(sd, 7, map[string]string{"0": "TR_a", "7": "TR_b"})
		fmt.Printf("REF %s to_proto: type=%s sdp_len=%d sdp_equal=%v id=%d mid_to_track_id=%s\n",
			name, refQuote(p.Type), len(p.Sdp), p.Sdp == text, p.Id, refMap(p.MidToTrackId))
		back, id, m := FromProtoSessionDescription(p)
		fmt.Printf("REF %s from_proto: type=%s sdp_equal=%v id=%d mid_to_track_id=%s\n",
			name, refQuote(back.Type.String()), back.SDP == text, id, refMap(m))
	}
	fmt.Printf("REF to_proto empty: nil=%v\n",
		ToProtoSessionDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer}, 1, map[string]string{"0": "TR_a"}) == nil)
	for i, typ := range []webrtc.SDPType{webrtc.SDPTypeOffer, webrtc.SDPTypePranswer, webrtc.SDPTypeAnswer, webrtc.SDPTypeRollback, webrtc.SDPType(0)} {
		p := ToProtoSessionDescription(webrtc.SessionDescription{Type: typ, SDP: "x"}, 0, nil)
		back, id, m := FromProtoSessionDescription(p)
		fmt.Printf("REF type %d: proto=%s back=%s id=%d mid_to_track_id=%s\n", i, refQuote(p.Type), refQuote(back.Type.String()), id, refMap(m))
	}
	for _, typ := range []string{"OFFER", "", "unknown", "Unspecified", "answer "} {
		back, _, _ := FromProtoSessionDescription(&livekit.SessionDescription{Type: typ, Sdp: "x"})
		fmt.Printf("REF type from %s: back=%s\n", refQuote(typ), refQuote(back.Type.String()))
	}

	mid := "0"
	index := uint16(0)
	ufrag := "wkc6"
	empty := ""
	last := uint16(65535)
	for i, c := range []struct {
		init   webrtc.ICECandidateInit
		target livekit.SignalTarget
		final  bool
	}{
		{webrtc.ICECandidateInit{
			Candidate:        "candidate:2353504611 1 udp 2122194687 127.0.0.1 50071 typ host generation 0 ufrag wkc6 network-id 4 network-cost 50",
			SDPMid:           &mid,
			SDPMLineIndex:    &index,
			UsernameFragment: &ufrag,
		}, livekit.SignalTarget_SUBSCRIBER, true},
		{webrtc.ICECandidateInit{Candidate: "candidate:1 1 udp 1 127.0.0.1 9 typ host"}, livekit.SignalTarget_PUBLISHER, false},
		{webrtc.ICECandidateInit{}, livekit.SignalTarget_PUBLISHER, true},
		{webrtc.ICECandidateInit{
			Candidate:        "<a&b>  \"\\\x01\x08\x0c\n\r\t\x7fé\U0001F600/",
			SDPMid:           &empty,
			SDPMLineIndex:    &last,
			UsernameFragment: &empty,
		}, livekit.SignalTarget_SUBSCRIBER, false},
	} {
		tr := ToProtoTrickle(c.init, c.target, c.final)
		fmt.Printf("REF to_proto_trickle %d: %s target=%d final=%v\n", i, tr.CandidateInit, tr.Target, tr.Final)
		back, err := FromProtoTrickle(tr)
		fmt.Printf("REF to_proto_trickle %d and back: equal=%v err=%v\n", i,
			back.Candidate == c.init.Candidate && refOptional(back.SDPMid) == refOptional(c.init.SDPMid) &&
				(back.SDPMLineIndex == nil) == (c.init.SDPMLineIndex == nil) &&
				(back.SDPMLineIndex == nil || *back.SDPMLineIndex == *c.init.SDPMLineIndex) &&
				refOptional(back.UsernameFragment) == refOptional(c.init.UsernameFragment),
			err != nil)
	}

	for i, in := range []string{
		`{"candidate":"candidate:2353504611 1 udp 2122194687 127.0.0.1 50071 typ host generation 0 ufrag wkc6 network-id 4 network-cost 50","sdpMid":"0","sdpMLineIndex":3,"usernameFragment":"wkc6"}`,
		`{"candidate":"candidate:1 1 udp 1 127.0.0.1 9 typ host","sdpMid":null,"sdpMLineIndex":null,"usernameFragment":null}`,
		`{"candidate":"candidate:1 1 udp 1 127.0.0.1 9 typ host"}`,
		`{"sdpMid":"1"}`,
		`{"candidate":null,"sdpMLineIndex":0}`,
		`{"candidate":"c","unknown":[1,{"candidate":"inner"}],"sdpMid":"a"}`,
		`{}`,
		`null`,
		``,
		`{"candidate":5}`,
		`{"candidate":"c","sdpMLineIndex":65535}`,
		`{"candidate":"c","sdpMLineIndex":65536}`,
		`{"candidate":"c","sdpMLineIndex":-1}`,
		`{"candidate":"c","sdpMLineIndex":1.5}`,
		`{"candidate":"c","sdpMLineIndex":3.0}`,
		`{"candidate":"c","sdpMLineIndex":1e1}`,
		`{"candidate":"c","sdpMLineIndex":"1"}`,
		`{"candidate":"c","sdpMid":7}`,
		`{"candidate":"c","usernameFragment":{}}`,
		`[1]`,
		`"text"`,
		`true`,
		`5`,
		`{"candidate":"c"`,
		`{"candidate":"c"} x`,
		" \n{ \"candidate\" : \"c\" }\t ",
		`{"Candidate":"c","SDPMID":"x","sdpmlineindex":2,"USERNAMEFRAGMENT":"u"}`,
		`{"candidate":"first","candidate":"second"}`,
		`{"candidate":"exact","CANDIDATE":"folded"}`,
		`{"CANDIDATE":"folded","candidate":"exact"}`,
		`{"candidate":"kept","candidate":null}`,
		`{"sdpMid":"a","sdpMid":null,"sdpMLineIndex":1,"sdpMLineIndex":null}`,
		`{"candidate":"aé\n\"\\\/ 😀"}`,
		`{"candidate":"c","url":"stun:127.0.0.1:3478"}`,
		`{"candidate":"c","sdpMid":"0","sdpMLineIndex":true}`,
	} {
		ci, err := FromProtoTrickle(&livekit.TrickleRequest{CandidateInit: in})
		lineIndex := "nil"
		if ci.SDPMLineIndex != nil {
			lineIndex = fmt.Sprintf("%d", *ci.SDPMLineIndex)
		}
		fmt.Printf("REF from_proto_trickle %d: candidate=%s mid=%s index=%s ufrag=%s err=%v\n", i,
			refQuote(ci.Candidate), refOptional(ci.SDPMid), lineIndex, refOptional(ci.UsernameFragment), err != nil)
	}

	fmt.Printf("REF from_proto_ice_servers none: len=%d\n", len(FromProtoIceServers(nil)))
	for i, s := range FromProtoIceServers([]*livekit.ICEServer{
		{Urls: []string{"stun:127.0.0.1:3478"}},
		{Urls: []string{"turn:127.0.0.1:3478?transport=udp", "turns:127.0.0.1:5349?transport=tcp"}, Username: "user", Credential: "secret"},
		{},
	}) {
		urls := make([]string, 0, len(s.URLs))
		for _, u := range s.URLs {
			urls = append(urls, refQuote(u))
		}
		fmt.Printf("REF from_proto_ice_servers %d: urls=[%s] username=%s credential=%s\n", i,
			strings.Join(urls, " "), refQuote(s.Username), refQuote(s.Credential.(string)))
	}
}
