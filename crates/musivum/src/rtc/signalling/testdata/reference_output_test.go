// New for musivum, not a part of LiveKit: prints, encoded, what the builders of signalling.go
// make for fixed arguments. ../signalling_test.rs builds the same with the port and has the
// printed bytes as its expected values. The original has no tests of signalling.go.
//
// It is laid over the package pkg/rtc/signalling of LiveKit at the pin, which is not written
// to. From the root of the repository, or of a git worktree of it, with the cap on memory and
// time that docs/porting-guide.md asks for:
//
//	d=$PWD/crates/musivum/src/rtc/signalling/testdata
//	r=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/reference/livekit
//	mkdir -p target && printf '{"Replace": {"%s": "%s"}}\n' \
//	    $r/pkg/rtc/signalling/musivum_reference_output_test.go $d/reference_output_test.go > target/signalling-overlay.json
//	(ulimit -d 8388608 && timeout 600 scripts/go-reference-test.sh -overlay $PWD/target/signalling-overlay.json \
//	    -count=1 -run TestReferenceOutput -v ./pkg/rtc/signalling) | grep '^REF '
package signalling

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"google.golang.org/protobuf/proto"
)

func TestReferenceOutput(t *testing.T) {
	s := NewSignalling(SignallingParams{Logger: logger.GetLogger()})
	dump := func(name string, msg proto.Message) {
		if msg == nil {
			fmt.Printf("REF %s nil\n", name)
			return
		}
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("REF %s %s\n", name, hex.EncodeToString(b))
	}

	dump("join_response", s.SignalJoinResponse(&livekit.JoinResponse{
		Room: &livekit.Room{
			Sid:             "RM_abc",
			Name:            "a-room",
			EmptyTimeout:    300,
			MaxParticipants: 20,
			CreationTime:    1700000000,
			NumParticipants: 2,
			ActiveRecording: true,
		},
		Participant: &livekit.ParticipantInfo{
			Sid:      "PA_1",
			Identity: "alice",
			State:    livekit.ParticipantInfo_JOINED,
			JoinedAt: 1700000001,
			Name:     "Alice",
			Version:  3,
			Permission: &livekit.ParticipantPermission{
				CanSubscribe:   true,
				CanPublish:     true,
				CanPublishData: true,
			},
		},
		OtherParticipants: []*livekit.ParticipantInfo{{
			Sid:      "PA_2",
			Identity: "bob",
			State:    livekit.ParticipantInfo_ACTIVE,
			Tracks: []*livekit.TrackInfo{{
				Sid:       "TR_v",
				Type:      livekit.TrackType_VIDEO,
				Name:      "camera",
				Width:     1280,
				Height:    720,
				Simulcast: true,
				Source:    livekit.TrackSource_CAMERA,
				Mid:       "1",
			}},
		}},
		ServerVersion: "1.13.11",
		IceServers: []*livekit.ICEServer{{
			Urls:       []string{"turn:turn.example.org:3478"},
			Username:   "u",
			Credential: "c",
		}},
		SubscriberPrimary: true,
		ServerRegion:      "here",
		PingTimeout:       15,
		PingInterval:      5,
		ServerInfo: &livekit.ServerInfo{
			Edition:  livekit.ServerInfo_Standard,
			Version:  "1.13.11",
			Protocol: 18,
			NodeId:   "ND_x",
		},
		SifTrailer:  []byte{1, 2, 3},
		FastPublish: true,
	}))

	dump("participant_update", s.SignalParticipantUpdate([]*livekit.ParticipantInfo{
		{
			Sid:              "PA_2",
			Identity:         "bob",
			State:            livekit.ParticipantInfo_DISCONNECTED,
			Version:          7,
			DisconnectReason: livekit.DisconnectReason_CLIENT_INITIATED,
		},
		{
			Sid:         "PA_3",
			Identity:    "carol",
			Metadata:    "{\"x\":1}",
			IsPublisher: true,
			Kind:        livekit.ParticipantInfo_AGENT,
			Attributes:  map[string]string{"k": "v"},
		},
	}))
	dump("participant_update_empty", s.SignalParticipantUpdate(nil))

	dump("speaker_update", s.SignalSpeakerUpdate([]*livekit.SpeakerInfo{
		{Sid: "PA_1", Level: 0.75, Active: true},
		{Sid: "PA_2", Level: 0.125},
	}))
	dump("speaker_update_empty", s.SignalSpeakerUpdate([]*livekit.SpeakerInfo{}))

	dump("room_update", s.SignalRoomUpdate(&livekit.Room{
		Sid:      "RM_abc",
		Name:     "a-room",
		Metadata: "meta",
	}))

	dump("refresh_token", s.SignalRefreshToken("a.token.value"))

	dump("sdp_offer", s.SignalSdpOffer(&livekit.SessionDescription{
		Type:         "offer",
		Sdp:          "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\n",
		Id:           4,
		MidToTrackId: map[string]string{"0": "TR_a"},
	}))

	dump("leave_request", s.SignalLeaveRequest(&livekit.LeaveRequest{
		CanReconnect: true,
		Reason:       livekit.DisconnectReason_SERVER_SHUTDOWN,
		Action:       livekit.LeaveRequest_RECONNECT,
		Regions: &livekit.RegionSettings{
			Regions: []*livekit.RegionInfo{{
				Region:   "r1",
				Url:      "wss://r1.example.org",
				Distance: 12,
			}},
		},
	}))
}
