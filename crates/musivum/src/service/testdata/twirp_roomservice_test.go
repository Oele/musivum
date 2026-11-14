package service_test

// Written for musivum: what LiveKit's Twirp server of the room service answers for a set of
// requests, which `the_room_service_answers_as_the_go_original` in `twirp_test.rs` compares
// musivum's answers with. The room service is upstream's, with the server options of
// server.go, over fake psrpc clients, a fake router, store and allocator; their answers
// depend on the request alone, and musivum's test sets up fakes that answer the same. It is
// laid over LiveKit's `pkg/service` at the pin and writes the requests with the answers to
// the file that MUSIVUM_TWIRP_GOLDEN names:
//
//   ulimit -d 8388608; MUSIVUM_TWIRP_GOLDEN=<absolute path of
//   twirp_roomservice_of_the_go_original.json> timeout 600 scripts/go-reference-test.sh \
//     -overlay=<a file that maps reference/livekit/pkg/service/musivum_twirp_test.go to this
//     one> -run TestMusivumTwirpRoomService -count=1 ./pkg/service/
//
// Go's protojson puts a space after a comma or not, at random per binary; the JSON answers are
// written without it.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twitchtv/twirp"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/rpc/rpcfakes"
	"github.com/livekit/protocol/utils"
	"github.com/livekit/protocol/utils/xtwirp"
	"github.com/livekit/psrpc"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/livekit-server/pkg/service/servicefakes"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
)

type musivumTwirpCase struct {
	Name            string `json:"name"`
	Method          string `json:"method"`
	Path            string `json:"path"`
	ContentType     string `json:"content_type"`
	Grants          string `json:"grants"`
	BodyJSON        string `json:"body_json,omitempty"`
	BodyProtobuf    string `json:"body_protobuf,omitempty"`
	Status          int    `json:"status"`
	RespContentType string `json:"resp_content_type"`
	RespJSON        string `json:"resp_json,omitempty"`
	RespProtobuf    string `json:"resp_protobuf,omitempty"`
}

const (
	musivumPrefix = "/twirp/livekit.RoomService/"
	musivumPB     = "application/protobuf"
	musivumJSON   = "application/json"
	// the room no server is registered for
	musivumNoServer = "gone"
)

func musivumRoom(name, metadata string) *livekit.Room {
	return &livekit.Room{
		Sid:              "RM_" + name,
		Name:             name,
		EmptyTimeout:     300,
		DepartureTimeout: 20,
		CreationTime:     1700000000,
		CreationTimeMs:   1700000000123,
		Metadata:         metadata,
		EnabledCodecs:    []*livekit.Codec{{Mime: "audio/opus"}, {Mime: "video/vp8", FmtpLine: "x=1"}},
		NumParticipants:  2,
		Version:          &livekit.TimedVersion{UnixMicro: 1700000000123456, Ticks: 7},
	}
}

func musivumTrack(sid string, muted bool) *livekit.TrackInfo {
	return &livekit.TrackInfo{
		Sid:    sid,
		Type:   livekit.TrackType_VIDEO,
		Name:   "camera \"front\" <hd> & é ",
		Muted:  muted,
		Width:  1280,
		Height: 720,
		Source: livekit.TrackSource_CAMERA,
		Layers: []*livekit.VideoLayer{
			{Quality: livekit.VideoQuality_HIGH, Width: 1280, Height: 720, Bitrate: 1500000},
		},
		MimeType: "video/vp8",
		Stream:   "camera",
		Version:  &livekit.TimedVersion{UnixMicro: 1, Ticks: 2},
	}
}

func musivumParticipant(identity, name, metadata string, attributes map[string]string, permission *livekit.ParticipantPermission) *livekit.ParticipantInfo {
	if permission == nil {
		permission = &livekit.ParticipantPermission{
			CanSubscribe:      true,
			CanPublish:        true,
			CanPublishData:    true,
			CanPublishSources: []livekit.TrackSource{livekit.TrackSource_CAMERA, livekit.TrackSource_MICROPHONE},
		}
	}
	return &livekit.ParticipantInfo{
		Sid:         "PA_" + identity,
		Identity:    identity,
		State:       livekit.ParticipantInfo_ACTIVE,
		Tracks:      []*livekit.TrackInfo{musivumTrack("TR_1", false)},
		Metadata:    metadata,
		JoinedAt:    1700000000,
		JoinedAtMs:  1700000000123,
		Name:        name,
		Version:     3,
		Permission:  permission,
		IsPublisher: true,
		Kind:        livekit.ParticipantInfo_STANDARD,
		Attributes:  attributes,
		KindDetails: []livekit.ParticipantInfo_KindDetail{livekit.ParticipantInfo_FORWARDED},
	}
}

func musivumGrants(spec string) *auth.ClaimGrants {
	if spec == "" {
		return nil
	}
	parts := strings.Split(spec, ":")
	v := &auth.VideoGrant{}
	switch parts[0] {
	case "create":
		v.RoomCreate = true
	case "list":
		v.RoomList = true
	case "admin":
		v.RoomAdmin = true
		v.Room = parts[1]
		if len(parts) > 2 {
			v.DestinationRoom = parts[2]
		}
	}
	return &auth.ClaimGrants{Video: v}
}

// leaves out the space Go's protojson may put after a comma
func musivumNormalize(b []byte) string {
	var out []byte
	inString, escaped := false, false
	for i, c := range b {
		switch {
		case inString:
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
		case c == '"':
			inString = true
		case c == ' ' && i > 0 && b[i-1] == ',':
			continue
		}
		out = append(out, c)
	}
	return string(out)
}

func newMusivumRoomService(t *testing.T) *service.RoomService {
	router := &routingfakes.FakeRouter{}
	router.CreateRoomStub = func(_ context.Context, req *livekit.CreateRoomRequest) (*livekit.Room, error) {
		return musivumRoom(req.Name, req.Metadata), nil
	}
	allocator := &servicefakes.FakeRoomAllocator{}
	store := &servicefakes.FakeObjectStore{}
	store.RoomExistsStub = func(_ context.Context, name livekit.RoomName) (bool, error) {
		return name != "missing", nil
	}
	store.HasParticipantStub = func(_ context.Context, _ livekit.RoomName, identity livekit.ParticipantIdentity) (bool, error) {
		return identity != "nobody", nil
	}
	store.ListRoomsStub = func(_ context.Context, names []livekit.RoomName) ([]*livekit.Room, error) {
		rooms := []*livekit.Room{}
		for _, n := range names {
			rooms = append(rooms, musivumRoom(string(n), ""))
		}
		if names == nil {
			rooms = append(rooms, musivumRoom("r", "m"))
		}
		return rooms, nil
	}

	roomClient := &rpcfakes.FakeTypedRoomClient{}
	roomClient.DeleteRoomStub = func(_ context.Context, topic rpc.RoomTopic, _ *livekit.DeleteRoomRequest, _ ...psrpc.RequestOption) (*livekit.DeleteRoomResponse, error) {
		if topic == musivumNoServer {
			return nil, psrpc.ErrNoResponse
		}
		return &livekit.DeleteRoomResponse{}, nil
	}
	roomClient.SendDataStub = func(_ context.Context, topic rpc.RoomTopic, req *livekit.SendDataRequest, _ ...psrpc.RequestOption) (*livekit.SendDataResponse, error) {
		if topic == musivumNoServer {
			return nil, psrpc.ErrNoResponse
		}
		if req.Room == "lost" {
			return nil, service.ErrRoomNotFound
		}
		return &livekit.SendDataResponse{}, nil
	}
	roomClient.UpdateRoomMetadataStub = func(_ context.Context, topic rpc.RoomTopic, req *livekit.UpdateRoomMetadataRequest, _ ...psrpc.RequestOption) (*livekit.Room, error) {
		if topic == musivumNoServer {
			return nil, psrpc.ErrNoResponse
		}
		return musivumRoom(req.Room, req.Metadata), nil
	}
	roomClient.ListParticipantsStub = func(_ context.Context, topic rpc.RoomTopic, req *livekit.ListParticipantsRequest, _ ...psrpc.RequestOption) (*livekit.ListParticipantsResponse, error) {
		if topic == musivumNoServer {
			return nil, psrpc.ErrNoResponse
		}
		return &livekit.ListParticipantsResponse{Participants: []*livekit.ParticipantInfo{
			musivumParticipant("p", "n", "m", map[string]string{"b": "2", "a": "1"}, nil),
			musivumParticipant("q", "", "", nil, nil),
		}}, nil
	}
	roomClient.GetParticipantStub = func(_ context.Context, topic rpc.RoomTopic, req *livekit.RoomParticipantIdentity, _ ...psrpc.RequestOption) (*livekit.ParticipantInfo, error) {
		if topic == musivumNoServer {
			return nil, psrpc.ErrNoResponse
		}
		return musivumParticipant(req.Identity, "n", "m", nil, nil), nil
	}

	noServer := func(topic rpc.ParticipantTopic) bool {
		return strings.HasPrefix(string(topic), musivumNoServer+"_")
	}
	participantClient := &rpcfakes.FakeTypedParticipantClient{}
	participantClient.RemoveParticipantStub = func(_ context.Context, topic rpc.ParticipantTopic, _ *livekit.RoomParticipantIdentity, _ ...psrpc.RequestOption) (*livekit.RemoveParticipantResponse, error) {
		if noServer(topic) {
			return nil, psrpc.ErrNoResponse
		}
		return &livekit.RemoveParticipantResponse{}, nil
	}
	participantClient.MutePublishedTrackStub = func(_ context.Context, topic rpc.ParticipantTopic, req *livekit.MuteRoomTrackRequest, _ ...psrpc.RequestOption) (*livekit.MuteRoomTrackResponse, error) {
		if noServer(topic) {
			return nil, psrpc.ErrNoResponse
		}
		return &livekit.MuteRoomTrackResponse{Track: musivumTrack(req.TrackSid, req.Muted)}, nil
	}
	participantClient.UpdateParticipantStub = func(_ context.Context, topic rpc.ParticipantTopic, req *livekit.UpdateParticipantRequest, _ ...psrpc.RequestOption) (*livekit.ParticipantInfo, error) {
		if noServer(topic) {
			return nil, psrpc.ErrNoResponse
		}
		return musivumParticipant(req.Identity, req.Name, req.Metadata, req.Attributes, req.Permission), nil
	}
	participantClient.UpdateSubscriptionsStub = func(_ context.Context, topic rpc.ParticipantTopic, _ *livekit.UpdateSubscriptionsRequest, _ ...psrpc.RequestOption) (*livekit.UpdateSubscriptionsResponse, error) {
		if noServer(topic) {
			return nil, psrpc.ErrNoResponse
		}
		return &livekit.UpdateSubscriptionsResponse{}, nil
	}
	participantClient.ForwardParticipantStub = func(_ context.Context, topic rpc.ParticipantTopic, _ *livekit.ForwardParticipantRequest, _ ...psrpc.RequestOption) (*livekit.ForwardParticipantResponse, error) {
		if noServer(topic) {
			return nil, psrpc.ErrNoResponse
		}
		return &livekit.ForwardParticipantResponse{}, nil
	}
	participantClient.MoveParticipantStub = func(_ context.Context, topic rpc.ParticipantTopic, _ *livekit.MoveParticipantRequest, _ ...psrpc.RequestOption) (*livekit.MoveParticipantResponse, error) {
		if noServer(topic) {
			return nil, psrpc.ErrNoResponse
		}
		// what the room manager answers
		return nil, errors.New("not implemented")
	}
	participantClient.PerformRpcStub = func(_ context.Context, topic rpc.ParticipantTopic, req *livekit.PerformRpcRequest, _ ...psrpc.RequestOption) (*livekit.PerformRpcResponse, error) {
		if noServer(topic) {
			return nil, psrpc.ErrNoResponse
		}
		if req.DestinationIdentity == "slow" {
			return nil, utils.DataChannelRpcErrorFromBuiltInCodes(utils.DataChannelRpcResponseTimeout, "").PsrpcError()
		}
		return &livekit.PerformRpcResponse{Payload: "pong:" + req.Payload}, nil
	}

	svc, err := service.NewRoomService(
		config.LimitConfig{
			MaxMetadataSize:          10,
			MaxAttributesSize:        20,
			MaxRoomNameLength:        8,
			MaxParticipantNameLength: 8,
		},
		config.APIConfig{EnablePsrpcForGetListParticpants: true},
		router,
		allocator,
		store,
		nil,
		rpc.NewTopicFormatter(),
		roomClient,
		participantClient,
	)
	require.NoError(t, err)
	return svc
}

func TestMusivumTwirpRoomService(t *testing.T) {
	golden := os.Getenv("MUSIVUM_TWIRP_GOLDEN")
	require.NotEmpty(t, golden)
	require.NoError(t, prometheus.Init("musivum-test-node", livekit.NodeType_SERVER))

	serverOptions := []any{
		twirp.WithServerHooks(twirp.ChainHooks(
			service.TwirpLogger(),
			service.TwirpEgressID(),
			service.TwirpRequestStatusReporter(),
		)),
	}
	for _, opt := range xtwirp.DefaultServerOptions() {
		serverOptions = append(serverOptions, opt)
	}
	server := livekit.NewRoomServiceServer(newMusivumRoomService(t), serverOptions...)

	pb := func(m proto.Message) []byte {
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
		require.NoError(t, err)
		return b
	}
	js := func(s string) []byte { return []byte(s) }
	nonce := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	topic := "t"

	type request struct {
		name, method, path, contentType, grants string
		body                                    []byte
	}
	post := func(name, method, contentType, grants string, body []byte) request {
		return request{name, http.MethodPost, musivumPrefix + method, contentType, grants, body}
	}
	requests := []request{
		post("CreateRoom protobuf", "CreateRoom", musivumPB, "create", pb(&livekit.CreateRoomRequest{Name: "r", EmptyTimeout: 300, Metadata: "meta"})),
		post("CreateRoom json", "CreateRoom", musivumJSON+"; charset=utf-8", "create", js(`{"name":"r","emptyTimeout":300,"metadata":"meta","unknownField":{"x":[1,2]}}`)),
		post("CreateRoom without a token", "CreateRoom", musivumPB, "", pb(&livekit.CreateRoomRequest{Name: "r"})),
		post("CreateRoom with a room name too long", "CreateRoom", musivumJSON, "create", js(`{"name":"a-long-room"}`)),
		post("CreateRoom with egress", "CreateRoom", musivumJSON, "create", js(`{"name":"r","egress":{}}`)),
		post("CreateRoom with agent metadata too big", "CreateRoom", musivumJSON, "create", js(`{"name":"r","agents":[{"agent_name":"bot","metadata":"0123456789a"}]}`)),
		post("ListRooms protobuf", "ListRooms", musivumPB, "list", pb(&livekit.ListRoomsRequest{Names: []string{"a", "b"}})),
		post("ListRooms json", "ListRooms", musivumJSON, "list", js(`{}`)),
		post("ListRooms without the grant", "ListRooms", musivumJSON, "admin:r", js(`{}`)),
		post("DeleteRoom protobuf", "DeleteRoom", musivumPB, "create", pb(&livekit.DeleteRoomRequest{Room: "r"})),
		post("DeleteRoom json", "DeleteRoom", musivumJSON, "create", js(`{"room":"r"}`)),
		post("DeleteRoom of a room that does not exist", "DeleteRoom", musivumJSON, "create", js(`{"room":"missing"}`)),
		post("DeleteRoom of a room with no server", "DeleteRoom", musivumJSON, "create", js(`{"room":"gone"}`)),
		post("ListParticipants protobuf", "ListParticipants", musivumPB, "admin:r", pb(&livekit.ListParticipantsRequest{Room: "r"})),
		post("ListParticipants json", "ListParticipants", musivumJSON, "admin:r", js(`{"room":"r"}`)),
		post("ListParticipants of another room", "ListParticipants", musivumJSON, "admin:r", js(`{"room":"other"}`)),
		post("GetParticipant protobuf", "GetParticipant", musivumPB, "admin:r", pb(&livekit.RoomParticipantIdentity{Room: "r", Identity: "p"})),
		post("GetParticipant json", "GetParticipant", musivumJSON, "admin:r", js(`{"room":"r","identity":"p"}`)),
		post("RemoveParticipant protobuf", "RemoveParticipant", musivumPB, "admin:r", pb(&livekit.RoomParticipantIdentity{Room: "r", Identity: "p"})),
		post("RemoveParticipant json", "RemoveParticipant", musivumJSON, "admin:r", js(`{"room":"r","identity":"p"}`)),
		post("RemoveParticipant of a participant not there", "RemoveParticipant", musivumJSON, "admin:r", js(`{"room":"r","identity":"nobody"}`)),
		post("MutePublishedTrack protobuf", "MutePublishedTrack", musivumPB, "admin:r", pb(&livekit.MuteRoomTrackRequest{Room: "r", Identity: "p", TrackSid: "TR_1", Muted: true})),
		post("MutePublishedTrack json", "MutePublishedTrack", musivumJSON, "admin:r", js(`{"room":"r","identity":"p","track_sid":"TR_1","muted":true}`)),
		post("UpdateParticipant protobuf", "UpdateParticipant", musivumPB, "admin:r", pb(&livekit.UpdateParticipantRequest{Room: "r", Identity: "p", Name: "nn", Metadata: "md", Attributes: map[string]string{"k": "v", "a": "b"}})),
		post("UpdateParticipant json", "UpdateParticipant", musivumJSON, "admin:r", js(`{"room":"r","identity":"p","name":"nn","metadata":"md","attributes":{"k":"v"},"permission":{"canPublish":true,"can_publish_sources":["CAMERA","NO_SUCH_SOURCE",2],"hidden":null}}`)),
		post("UpdateParticipant with a name too long", "UpdateParticipant", musivumJSON, "admin:r", js(`{"room":"r","identity":"p","name":"a-long-name"}`)),
		post("UpdateSubscriptions protobuf", "UpdateSubscriptions", musivumPB, "admin:r", pb(&livekit.UpdateSubscriptionsRequest{Room: "r", Identity: "p", TrackSids: []string{"TR_1"}, Subscribe: true, ParticipantTracks: []*livekit.ParticipantTracks{{ParticipantSid: "PA_q", TrackSids: []string{"TR_2"}}}})),
		post("UpdateSubscriptions json", "UpdateSubscriptions", musivumJSON, "admin:r", js(`{"room":"r","identity":"p","trackSids":["TR_1"],"subscribe":true,"participantTracks":[{"participantSid":"PA_q","trackSids":["TR_2"]}]}`)),
		post("SendData protobuf", "SendData", musivumPB, "admin:r", pb(&livekit.SendDataRequest{Room: "r", Data: []byte("hello"), Kind: livekit.DataPacket_RELIABLE, DestinationIdentities: []string{"p"}, Topic: &topic, Nonce: nonce})),
		post("SendData json", "SendData", musivumJSON, "admin:r", js(`{"room":"r","data":"aGVsbG8","kind":"LOSSY","topic":"t","nonce":"AAECAwQFBgcICQoLDA0ODw=="}`)),
		post("SendData with a bad nonce", "SendData", musivumJSON, "admin:r", js(`{"room":"r","nonce":"AAEC"}`)),
		post("SendData to a room its server does not know", "SendData", musivumJSON, "admin:lost", js(`{"room":"lost"}`)),
		post("UpdateRoomMetadata protobuf", "UpdateRoomMetadata", musivumPB, "admin:r", pb(&livekit.UpdateRoomMetadataRequest{Room: "r", Metadata: "new"})),
		post("UpdateRoomMetadata json", "UpdateRoomMetadata", musivumJSON, "admin:r", js(`{"room":"r","metadata":"new"}`)),
		post("UpdateRoomMetadata too big", "UpdateRoomMetadata", musivumJSON, "admin:r", js(`{"room":"r","metadata":"0123456789a"}`)),
		post("UpdateRoomMetadata of a room that does not exist", "UpdateRoomMetadata", musivumJSON, "admin:missing", js(`{"room":"missing","metadata":"x"}`)),
		post("ForwardParticipant protobuf", "ForwardParticipant", musivumPB, "admin:r:r2", pb(&livekit.ForwardParticipantRequest{Room: "r", Identity: "p", DestinationRoom: "r2"})),
		post("ForwardParticipant json", "ForwardParticipant", musivumJSON, "admin:r:r2", js(`{"room":"r","identity":"p","destinationRoom":"r2"}`)),
		post("ForwardParticipant into the same room", "ForwardParticipant", musivumJSON, "admin:r:r", js(`{"room":"r","identity":"p","destination_room":"r"}`)),
		post("MoveParticipant protobuf", "MoveParticipant", musivumPB, "admin:r:r2", pb(&livekit.MoveParticipantRequest{Room: "r", Identity: "p", DestinationRoom: "r2"})),
		post("MoveParticipant json", "MoveParticipant", musivumJSON, "admin:r:r2", js(`{"room":"r","identity":"p","destination_room":"r2"}`)),
		post("PerformRpc protobuf", "PerformRpc", musivumPB, "admin:r", pb(&livekit.PerformRpcRequest{Room: "r", DestinationIdentity: "p", Method: "m", Payload: "ping", ResponseTimeoutMs: 1000})),
		post("PerformRpc json", "PerformRpc", musivumJSON, "admin:r", js(`{"room":"r","destinationIdentity":"p","method":"m","payload":"ping","responseTimeoutMs":"1000"}`)),
		post("PerformRpc without a destination", "PerformRpc", musivumJSON, "admin:r", js(`{"room":"r","method":"m"}`)),
		post("PerformRpc that times out", "PerformRpc", musivumJSON, "admin:r", js(`{"room":"r","destination_identity":"slow","method":"m"}`)),
		post("PerformRpc to a participant with no server", "PerformRpc", musivumJSON, "admin:gone", js(`{"room":"gone","destination_identity":"p","method":"m"}`)),

		{"GET", http.MethodGet, musivumPrefix + "ListRooms", musivumJSON, "list", nil},
		post("unknown method", "NoSuchMethod", musivumJSON, "list", js(`{}`)),
		{"unknown service", http.MethodPost, "/twirp/livekit.Other/ListRooms", musivumJSON, "list", js(`{}`)},
		{"wrong prefix", http.MethodPost, "/api/livekit.RoomService/ListRooms", musivumJSON, "list", js(`{}`)},
		{"escaped path", http.MethodPost, "/twirp/livekit.RoomService/List%52ooms", musivumJSON, "list", js(`{}`)},
		post("no content type", "ListRooms", "", "list", js(`{}`)),
		post("text content type", "ListRooms", "text/plain", "list", js(`{}`)),
		post("content type in capitals", "ListRooms", "Application/JSON ; charset=utf-8", "list", js(`{}`)),
		post("malformed protobuf", "ListRooms", musivumPB, "list", []byte{0xff, 0xff}),
		post("malformed json", "ListRooms", musivumJSON, "list", js(`{"names":`)),
		post("empty json body", "ListRooms", musivumJSON, "list", js(``)),
		post("json that is no object", "ListRooms", musivumJSON, "list", js(`[]`)),
		post("json with a field twice", "CreateRoom", musivumJSON, "create", js(`{"name":"r","emptyTimeout":1,"empty_timeout":2}`)),
		post("json with trailing content", "ListRooms", musivumJSON, "list", js(`{"names":["a"]} trailing`)),
		post("json with a field of the wrong type", "CreateRoom", musivumJSON, "create", js(`{"name":5}`)),
	}

	var cases []musivumTwirpCase
	for _, r := range requests {
		req := httptest.NewRequest(r.method, r.path, bytes.NewReader(r.body))
		if r.contentType != "" {
			req.Header.Set("Content-Type", r.contentType)
		}
		if g := musivumGrants(r.grants); g != nil {
			req = req.WithContext(service.WithGrants(req.Context(), g, ""))
		}
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		c := musivumTwirpCase{
			Name:            r.name,
			Method:          r.method,
			Path:            r.path,
			ContentType:     r.contentType,
			Grants:          r.grants,
			Status:          rec.Code,
			RespContentType: rec.Header().Get("Content-Type"),
		}
		if r.contentType == musivumPB {
			c.BodyProtobuf = base64.StdEncoding.EncodeToString(r.body)
		} else {
			c.BodyJSON = string(r.body)
		}
		if c.RespContentType == musivumJSON {
			c.RespJSON = musivumNormalize(rec.Body.Bytes())
		} else {
			c.RespProtobuf = base64.StdEncoding.EncodeToString(rec.Body.Bytes())
		}
		cases = append(cases, c)
	}

	out, err := json.MarshalIndent(cases, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(golden, append(out, '\n'), 0o644))
}
