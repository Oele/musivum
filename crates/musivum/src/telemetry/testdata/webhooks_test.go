package telemetry_test

// Written for musivum: what LiveKit's telemetry service sends as webhooks for a room, which
// `the_webhooks_of_a_room_are_those_of_the_go_original` in `events_test.rs` compares
// musivum's with. It is laid over LiveKit's `pkg/telemetry` at the pin and writes what it
// received, made comparable, to the file that MUSIVUM_WEBHOOK_GOLDEN names:
//
//   ulimit -d 8388608; timeout 600 scripts/go-reference-test.sh \
//     -overlay=<a file that maps reference/livekit/pkg/telemetry/musivum_webhooks_test.go
//     to this one> -run TestMusivumWebhooksOfARoom -count=1 ./pkg/telemetry/
//
// with MUSIVUM_WEBHOOK_GOLDEN=<absolute path of webhooks_of_the_go_original.json>. The
// normalization is that of `normalize` in `events_test.rs`.

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/telemetry"
	"github.com/livekit/livekit-server/pkg/telemetry/telemetryfakes"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/webhook"
)

type musivumReceived struct {
	header http.Header
	body   []byte
}

func TestMusivumWebhooksOfARoom(t *testing.T) {
	golden := os.Getenv("MUSIVUM_WEBHOOK_GOLDEN")
	require.NotEmpty(t, golden)

	var mu sync.Mutex
	var received []musivumReceived
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		received = append(received, musivumReceived{header: r.Header.Clone(), body: body})
		mu.Unlock()
	}))
	defer s.Close()

	notifier, err := webhook.NewDefaultNotifier(
		webhook.WebHookConfig{URLs: []string{s.URL}, APIKey: "mykey"},
		auth.NewSimpleKeyProvider("mykey", "mysecret"),
	)
	require.NoError(t, err)
	ts := telemetry.NewTelemetryService(notifier, &telemetryfakes.FakeAnalyticsService{})

	room, participant, track := MusivumWebhookRoom()
	guard := &telemetry.ReferenceGuard{}
	ctx := context.Background()
	ts.RoomStarted(ctx, room)
	ts.ParticipantActive(ctx, room, participant, &livekit.AnalyticsClientMeta{Region: "a-region"}, false, false, guard)
	ts.TrackPublished(ctx, room, livekit.ParticipantID(participant.Sid), livekit.ParticipantIdentity(participant.Identity), track, true)
	ts.ParticipantLeft(ctx, room, participant, true, guard)
	ts.RoomEnded(ctx, room, livekit.RoomEndReason_ROOM_END_IDLE_TIMEOUT)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 5
	}, 10*time.Second, 10*time.Millisecond)
	notifier.Stop(false)

	var out []any
	for _, r := range received {
		out = append(out, musivumNormalize(t, r))
	}
	data, err := json.MarshalIndent(out, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(golden, append(data, '\n'), 0o644))
}

// The room, participant and track of the test, the same as `webhook_room` of events_test.rs.
func MusivumWebhookRoom() (*livekit.Room, *livekit.ParticipantInfo, *livekit.TrackInfo) {
	room := &livekit.Room{
		Sid:             "RM_aRoomOfTheTest",
		Name:            "a-room",
		EmptyTimeout:    300,
		MaxParticipants: 10,
		CreationTime:    1700000000,
		CreationTimeMs:  1700000000123,
		EnabledCodecs:   []*livekit.Codec{{Mime: "audio/opus"}, {Mime: "video/VP8", FmtpLine: "x=1"}},
		Metadata:        "some metadata",
		NumParticipants: 1,
		Version:         &livekit.TimedVersion{UnixMicro: 1700000000123456, Ticks: 2},
	}
	participant := &livekit.ParticipantInfo{
		Sid:      "PA_aParticipant",
		Identity: "alice",
		State:    livekit.ParticipantInfo_ACTIVE,
		JoinedAt: 1700000001,
		Name:     "Alice",
		Version:  3,
		Permission: &livekit.ParticipantPermission{
			CanSubscribe:      true,
			CanPublish:        true,
			CanPublishData:    true,
			CanPublishSources: []livekit.TrackSource{livekit.TrackSource_CAMERA, livekit.TrackSource_MICROPHONE},
		},
		IsPublisher: true,
		Kind:        livekit.ParticipantInfo_STANDARD,
		Attributes:  map[string]string{"a": "b"},
	}
	track := &livekit.TrackInfo{
		Sid:        "TR_aTrack",
		Type:       livekit.TrackType_VIDEO,
		Name:       "camera",
		Width:      1280,
		Height:     720,
		Simulcast:  true,
		Source:     livekit.TrackSource_CAMERA,
		MimeType:   "video/VP8",
		Mid:        "1",
		DisableRed: true,
		Stream:     "camera",
		Layers: []*livekit.VideoLayer{
			{Quality: livekit.VideoQuality_LOW, Width: 320, Height: 180, Bitrate: 150000, Ssrc: 1111},
			{Quality: livekit.VideoQuality_HIGH, Width: 1280, Height: 720, Bitrate: 1700000, Ssrc: 2222, Rid: "f"},
		},
		Codecs: []*livekit.SimulcastCodecInfo{{
			MimeType:       "video/VP8",
			Mid:            "1",
			Cid:            "a-cid",
			VideoLayerMode: livekit.VideoLayer_ONE_SPATIAL_LAYER_PER_STREAM,
		}},
		Version: &livekit.TimedVersion{UnixMicro: 1700000002000000},
	}
	return room, participant, track
}

func musivumNormalize(t *testing.T, r musivumReceived) map[string]any {
	now := time.Now().Unix()

	var body map[string]any
	require.NoError(t, json.Unmarshal(r.body, &body))
	id, _ := body["id"].(string)
	require.True(t, strings.HasPrefix(id, "EV_"), id)
	body["id"] = fmt.Sprintf("EV_ and %d characters", len(id)-3)
	createdAt, err := strconv.ParseInt(body["createdAt"].(string), 10, 64)
	require.NoError(t, err)
	require.InDelta(t, now, createdAt, 60)
	body["createdAt"] = "<now>"

	parts := strings.Split(r.header.Get("Authorization"), ".")
	require.Len(t, parts, 3)
	var jwtHeader, claims map[string]any
	data, err := base64.RawURLEncoding.DecodeString(parts[0])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &jwtHeader))
	data, err = base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &claims))

	sum := sha256.Sum256(r.body)
	require.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), claims["sha256"])
	claims["sha256"] = "<sha256 of the body>"
	exp, nbf := int64(claims["exp"].(float64)), int64(claims["nbf"].(float64))
	require.InDelta(t, now, nbf, 60)
	delete(claims, "exp")
	delete(claims, "nbf")
	claims["exp - nbf"] = exp - nbf
	if iat, ok := claims["iat"].(float64); ok {
		require.InDelta(t, now, int64(iat), 60)
		claims["iat"] = "<now>"
	}

	return map[string]any{
		"contentType": r.header.Get("Content-Type"),
		"userAgent":   r.header.Get("User-Agent"),
		"jwtHeader":   jwtHeader,
		"claims":      claims,
		"body":        body,
	}
}
