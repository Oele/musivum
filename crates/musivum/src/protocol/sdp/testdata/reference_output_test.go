// New for musivum, not a part of LiveKit: prints what the helpers of sdp.go give for two
// real descriptions (offer.sdp and answer.sdp here: an offer of a browser and the answer of
// LiveKit's server to it, shortened to three media sections and to the candidates on the
// loopback interface) and for a few made up to reach what those two do not. Its output is
// reference_output.txt, the expected values of ../reference_test.rs, which asks the port the
// same.
//
// It runs inside a copy of livekit/protocol at the pin. From the root of the repository, or
// of a git worktree of it (`reference` is in the main checkout), with the cap on memory and
// time that docs/porting-guide.md asks for:
//
//	d=$PWD/crates/musivum/src/protocol/sdp/testdata
//	m=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/reference/gomod
//	rm -rf target/protocol-copy && mkdir -p target
//	cp -r $m/github.com/livekit/protocol@v1.53.1-0.20261023175527-46332a036e25 target/protocol-copy
//	chmod -R u+w target/protocol-copy && cp $d/reference_output_test.go target/protocol-copy/sdp/
//	(cd target/protocol-copy && ulimit -d 8388608 && MUSIVUM_SDP_TESTDATA=$d GOMODCACHE=$m \
//	    GOFLAGS=-modcacherw GOCACHE=$HOME/.cache/musivum/gocache GOPROXY=off \
//	    timeout 600 go test -count=1 -run TestReferenceOutput -v ./sdp) | grep '^REF ' > $d/reference_output.txt
package sdp

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
)

func refQuoted(list []string) string {
	quoted := make([]string, 0, len(list))
	for _, s := range list {
		quoted = append(quoted, fmt.Sprintf("%q", s))
	}
	return strings.Join(quoted, " ")
}

func refErr(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}

func TestReferenceOutput(t *testing.T) {
	type description struct {
		name string
		sdp  string
	}
	descriptions := []description{}
	for _, name := range []string{"offer", "answer"} {
		data, err := os.ReadFile(os.Getenv("MUSIVUM_SDP_TESTDATA") + "/" + name + ".sdp")
		if err != nil {
			t.Fatal(err)
		}
		descriptions = append(descriptions, description{name, strings.ReplaceAll(string(data), "\n", "\r\n")})
	}
	head := "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n"
	descriptions = append(descriptions,
		description{"nothing", head + "m=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=mid:0\r\n"},
		description{"properties", head + "a=fingerprint\r\na=group\r\nm=audio 9/2 RTP/AVP 0 8 9\r\na=mid\r\na=ice-ufrag:a\r\na=msid\r\na=simulcast\r\na=setup\r\n"},
		description{"conflicts", head + "a=fingerprint:sha-256 AA\r\na=ice-ufrag:a\r\na=ice-pwd:b\r\na=group:LS 0 1\r\n" +
			"m=audio 9 UDP/TLS/RTP/SAVPF 0 8 101\r\nc=IN IP4 127.0.0.1\r\na=mid:0\r\na=fingerprint:sha-256 BB\r\na=ice-ufrag:c\r\na=ice-pwd:b\r\n" +
			"a=msid:onlystream\r\na=setup:holdconn\r\na=simulcast:recv q;h\r\na=rtpmap:101 telephone-event/8000\r\na=fmtp:101 0-16; x=1\r\n" +
			"a=rtcp-fb:* nack\r\na=rtcp-fb:101 nack\r\na=rtcp-fb:101 nack pli\r\n" +
			"m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:1\r\na=setup:passive\r\na=rtpmap:96 VP8/90000\r\n"},
		description{"password", head + "a=ice-ufrag:a\r\na=ice-pwd:b\r\na=fingerprint:sha-256 AA BB\r\na=ice-lite\r\na=ice-options:trickle\r\na=group:bundle 1 0\r\n" +
			"m=audio 9 UDP/TLS/RTP/SAVPF 300 96\r\na=mid:0\r\na=ice-ufrag:a\r\na=ice-pwd:c\r\na=ssrc:1234 msid:stream track\r\na=msid:s t extra\r\na=simulcast:send q;h;f extra\r\n" +
			"m=video 9 UDP/TLS/RTP/SAVPF 096 98\r\nc=IN IP6 ::1\r\na=mid:1\r\na=end-of-candidates\r\na=candidate:1 1 udp 1 127.0.0.1 9 typ host\r\na=rtpmap:96 VP8/90000\r\na=rtpmap:98 VP9/90000\r\n"},
		description{"payloads", "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\na=ice-ufrag:a\r\na=group:BUNDLE audio\r\n" +
			"m=audio 9 UDP/TLS/RTP/SAVPF 0 96\r\na=ssrc:1234 MSID:stream track\r\na=ssrc:5678 msid:other track2\r\na=simulcast:send q,h;~f\r\na=setup:active\r\n" +
			"m=video 9 UDP/TLS/RTP/SAVPF 0 8 9\r\na=mid:video\r\na=msid:- track3\r\na=rtpmap:0 other/90000/2\r\na=fmtp:8 x=y\r\na=rtcp-fb:9 nack\r\na=rtcp-fb:9 nack\r\na=rtcp-fb:* nack\r\n" +
			"m=video 9 UDP/TLS/RTP/SAVPF 96 +97\r\na=mid:more\r\na=rtpmap:96 VP8/90000\r\na=rtpmap:97 rtx/90000\r\n"},
	)

	for _, d := range descriptions {
		sd := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: d.sdp}
		parsed, err := sd.Unmarshal()
		if err != nil {
			t.Fatal(d.name, err)
		}
		marshalled, err := parsed.Marshal()
		if err != nil {
			t.Fatal(d.name, err)
		}
		fmt.Printf("REF %s marshal_gives_the_text_back: %v\n", d.name, string(marshalled) == d.sdp)

		fp, alg, err := ExtractFingerprint(parsed)
		fmt.Printf("REF %s fingerprint: %q %q err=%s\n", d.name, fp, alg, refErr(err))
		fmt.Printf("REF %s dtls_role: %s\n", d.name, ExtractDTLSRole(parsed))
		ufrag, pwd, err := ExtractICECredential(parsed)
		fmt.Printf("REF %s ice_credential: %q %q err=%s\n", d.name, ufrag, pwd, refErr(err))
		fmt.Printf("REF %s ip: %q\n", d.name, GetIP(parsed))
		mid, found := GetBundleMid(parsed)
		fmt.Printf("REF %s bundle_mid: %q %v\n", d.name, mid, found)
		for i, m := range parsed.MediaDescriptions {
			fmt.Printf("REF %s media %d mid: %q\n", d.name, i, GetMidValue(m))
			streamID, ok := ExtractStreamID(m)
			fmt.Printf("REF %s media %d stream_id: %q %v\n", d.name, i, streamID, ok)
			fmt.Printf("REF %s media %d media_stream_track: %q\n", d.name, i, GetMediaStreamTrack(m))
			rids, ok := GetSimulcastRids(m)
			fmt.Printf("REF %s media %d simulcast_rids: [%s] %v\n", d.name, i, refQuoted(rids), ok)
			codecs, err := CodecsFromMediaDescription(m)
			fmt.Printf("REF %s media %d codecs: %d err=%s\n", d.name, i, len(codecs), refErr(err))
			for j, c := range codecs {
				fmt.Printf("REF %s media %d codec %d: %d %q %d %q %q [%s]\n", d.name, i, j,
					c.PayloadType, c.Name, c.ClockRate, c.EncodingParameters, c.Fmtp, refQuoted(c.RTCPFeedback))
			}
		}

		frag, err := ExtractSDPFragment(parsed)
		fmt.Printf("REF %s fragment: err=%s\n", d.name, refErr(err))
		if err != nil {
			continue
		}
		text, err := frag.Marshal()
		fmt.Printf("REF %s fragment marshal: %q err=%s\n", d.name, text, refErr(err))
		fmt.Printf("REF %s fragment mid: %q\n", d.name, frag.Mid())
		fmt.Printf("REF %s fragment candidates: [%s]\n", d.name, refQuoted(frag.Candidates()))
		ufrag, pwd, err = frag.ExtractICECredential()
		fmt.Printf("REF %s fragment ice_credential: %q %q err=%s\n", d.name, ufrag, pwd, refErr(err))

		// the fragment read back from its text, and patched into each of the descriptions
		reparsed := &SDPFragment{}
		err = reparsed.Unmarshal(text)
		fmt.Printf("REF %s fragment unmarshal: err=%s\n", d.name, refErr(err))
		if err != nil {
			continue
		}
		again, _ := reparsed.Marshal()
		fmt.Printf("REF %s fragment unmarshal marshal: %q\n", d.name, again)
		for _, into := range descriptions {
			target := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: into.sdp}
			parsedTarget, err := target.Unmarshal()
			if err != nil {
				t.Fatal(into.name, err)
			}
			err = reparsed.PatchICECredentialAndCandidatesIntoSDP(parsedTarget)
			fmt.Printf("REF %s fragment patch into %s: err=%s\n", d.name, into.name, refErr(err))
			if err != nil {
				continue
			}
			patched, _ := parsedTarget.Marshal()
			fmt.Printf("REF %s fragment patch into %s gives: %q\n", d.name, into.name, string(patched))
		}
	}

	// fragments as text
	for i, text := range []string{
		"a=ice-ufrag:u\r\na=ice-pwd:p\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=mid:0\r\na=ice-ufrag:v\r\n",
		"a=ice-ufrag:u\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=ice-pwd:p\r\na=ice-pwd:q\r\na=ice-lite\r\na=end-of-candidates\r\na=candidate:\r\na=candidate:1 1 udp 1 127.0.0.1 9 typ host   \r\n",
		"a=ice-pwd:p\r\nm=x\r\n",
		"m=audio 9 UDP/TLS/RTP/SAVPF 0\na=mid:0 \r\r\n\n  \nc=IN IP4 127.0.0.1\na=group:bundle 0 1\na=ice-options:trickle\nb=AS:1\na=mid\na=end-of-candidates:x\n",
		"a=group:BUNDLE\r\na=group:LS 1\r\na=mid:5\r\na=candidate:lost\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=ice-ufrag:u\r\na=ice-pwd:p\r\na=ice-ufrag:\r\n",
		"a=group:BUNDLE 1\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\n",
		"m=é\r\na=mid:é\r\na=ice-ufrag:é:è\r\na=ice-pwd:p\r\n",
		"é=1\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=mid:0\r\n",
		"m=audio 9 UDP/TLS/RTP/SAVPF 0\r\naé\r\n",
		"mé=audio\r\n",
	} {
		frag := &SDPFragment{}
		err := frag.Unmarshal(text)
		fmt.Printf("REF text %d unmarshal: err=%s\n", i, refErr(err))
		if err != nil {
			continue
		}
		marshalled, _ := frag.Marshal()
		fmt.Printf("REF text %d marshal: %q\n", i, marshalled)
		fmt.Printf("REF text %d mid: %q\n", i, frag.Mid())
		fmt.Printf("REF text %d candidates: [%s]\n", i, refQuoted(frag.Candidates()))
		ufrag, pwd, err := frag.ExtractICECredential()
		fmt.Printf("REF text %d ice_credential: %q %q err=%s\n", i, ufrag, pwd, refErr(err))
	}
}
