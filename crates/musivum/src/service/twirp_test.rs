// Ported from LiveKit, pkg/service/twirp_test.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `errors.As(err, &tErr)` is `twirp_error_as`. The tests
// after it are new for musivum.

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use musivum_protocol::Message as _;
use prost_reflect::DynamicMessage;

use super::*;
use crate::config::{ApiConfig, LimitConfig};
use crate::protocol::auth::{ClaimGrants, VideoGrant};
use crate::protocol::livekit;
use crate::protocol::utils as putils;
use crate::routing::{
    FakeParticipantServerImpl, FakeRoomServerImpl, FakeRouter, MessageRouter, PsrpcError,
    PsrpcErrorCode, format_participant_topic, format_room_topic, new_local_message_bus,
    new_topic_formatter, new_typed_participant_client, new_typed_room_client,
};
use crate::rtc;
use crate::service::{
    FakeObjectStore, FakeOssServiceStoreState, FakeRoomAllocator, OssServiceStore, RoomAllocator,
    ServiceStore, with_grants,
};

#[test]
fn test_convert_err_to_twirp() {
    {
        // handles not found
        let err = Error::RoomNotFound;
        let t_err = twirp_error_as(&err);
        assert!(t_err.is_some(), "handles not found");
        assert_eq!(
            TwirpErrorCode::NotFound,
            t_err.unwrap().code(),
            "handles not found"
        );
    }
}

// ------------------------------------------------
// New for musivum: the room service through its Twirp server, against what the original
// answers. `testdata/twirp_roomservice_test.go`, run at the pin over LiveKit's `pkg/service`
// (how is written at its top), wrote `testdata/twirp_roomservice_of_the_go_original.json`:
// the requests and the original's answers. The fakes here answer as those of the Go test do,
// from the request alone; the calls into rooms and participants go through the bus to fake
// servers, and a room or participant of `gone` has none (the Go test's psrpc clients answer
// `ErrNoResponse` for it).

fn musivum_room(name: &str, metadata: &str) -> livekit::Room {
    livekit::Room {
        sid: format!("RM_{name}"),
        name: name.into(),
        empty_timeout: 300,
        departure_timeout: 20,
        creation_time: 1700000000,
        creation_time_ms: 1700000000123,
        metadata: metadata.into(),
        enabled_codecs: vec![
            livekit::Codec {
                mime: "audio/opus".into(),
                ..Default::default()
            },
            livekit::Codec {
                mime: "video/vp8".into(),
                fmtp_line: "x=1".into(),
            },
        ],
        num_participants: 2,
        version: Some(livekit::TimedVersion {
            unix_micro: 1700000000123456,
            ticks: 7,
        }),
        ..Default::default()
    }
}

#[allow(deprecated)]
fn musivum_track(sid: &str, muted: bool) -> livekit::TrackInfo {
    livekit::TrackInfo {
        sid: sid.into(),
        r#type: livekit::TrackType::Video as i32,
        name: "camera \"front\" <hd> & é\u{2028}".into(),
        muted,
        width: 1280,
        height: 720,
        source: livekit::TrackSource::Camera as i32,
        layers: vec![livekit::VideoLayer {
            quality: livekit::VideoQuality::High as i32,
            width: 1280,
            height: 720,
            bitrate: 1500000,
            ..Default::default()
        }],
        mime_type: "video/vp8".into(),
        stream: "camera".into(),
        version: Some(livekit::TimedVersion {
            unix_micro: 1,
            ticks: 2,
        }),
        ..Default::default()
    }
}

fn musivum_participant(
    identity: &str,
    name: &str,
    metadata: &str,
    attributes: HashMap<String, String>,
    permission: Option<livekit::ParticipantPermission>,
) -> livekit::ParticipantInfo {
    let permission = permission.unwrap_or(livekit::ParticipantPermission {
        can_subscribe: true,
        can_publish: true,
        can_publish_data: true,
        can_publish_sources: vec![
            livekit::TrackSource::Camera as i32,
            livekit::TrackSource::Microphone as i32,
        ],
        ..Default::default()
    });
    livekit::ParticipantInfo {
        sid: format!("PA_{identity}"),
        identity: identity.into(),
        state: livekit::participant_info::State::Active as i32,
        tracks: vec![musivum_track("TR_1", false)],
        metadata: metadata.into(),
        joined_at: 1700000000,
        joined_at_ms: 1700000000123,
        name: name.into(),
        version: 3,
        permission: Some(permission),
        is_publisher: true,
        kind: livekit::participant_info::Kind::Standard as i32,
        attributes,
        kind_details: vec![livekit::participant_info::KindDetail::Forwarded as i32],
        ..Default::default()
    }
}

fn musivum_grants(spec: &str) -> Option<ClaimGrants> {
    if spec.is_empty() {
        return None;
    }
    let parts: Vec<&str> = spec.split(':').collect();
    let mut v = VideoGrant::default();
    match parts[0] {
        "create" => v.room_create = true,
        "list" => v.room_list = true,
        "admin" => {
            v.room_admin = true;
            v.room = parts[1].into();
            if parts.len() > 2 {
                v.destination_room = parts[2].into();
            }
        }
        _ => {}
    }
    Some(ClaimGrants {
        video: Some(v),
        ..Default::default()
    })
}

fn new_musivum_room_service_server() -> RoomServiceServer {
    let router = Arc::new(FakeRouter::default());
    router.create_room_calls(|req| Ok(musivum_room(&req.name, &req.metadata)));
    let allocator = Arc::new(FakeRoomAllocator::default());
    let store = Arc::new(FakeObjectStore::default());
    store.room_exists_calls(|name| Ok(name.as_str() != "missing"));
    store
        .embedded::<FakeOssServiceStoreState>()
        .has_participant_calls(|_, identity| Ok(identity.as_str() != "nobody"));
    store.list_rooms_calls(|names| {
        let mut rooms = Vec::new();
        for n in names.unwrap_or_default() {
            rooms.push(musivum_room(n.as_str(), ""));
        }
        if names.is_none() {
            rooms.push(musivum_room("r", "m"));
        }
        Ok(rooms)
    });

    let bus = new_local_message_bus();

    let room_server = Arc::new(FakeRoomServerImpl::default());
    room_server.delete_room_calls(|_| Ok(livekit::DeleteRoomResponse {}));
    room_server.send_data_calls(|req| {
        if req.room == "lost" {
            return Err(Error::RoomNotFound.into());
        }
        Ok(livekit::SendDataResponse {})
    });
    room_server.update_room_metadata_calls(|req| Ok(musivum_room(&req.room, &req.metadata)));
    room_server.list_participants_calls(|_| {
        Ok(livekit::ListParticipantsResponse {
            participants: vec![
                musivum_participant(
                    "p",
                    "n",
                    "m",
                    HashMap::from([("b".into(), "2".into()), ("a".into(), "1".into())]),
                    None,
                ),
                musivum_participant("q", "", "", HashMap::new(), None),
            ],
        })
    });
    room_server.get_participant_calls(|req| {
        Ok(musivum_participant(
            &req.identity,
            "n",
            "m",
            HashMap::new(),
            None,
        ))
    });
    for room in ["r", "lost"] {
        bus.register_room_server(&format_room_topic(&room.into()), room_server.clone());
    }

    let participant_server = Arc::new(FakeParticipantServerImpl::default());
    participant_server.remove_participant_calls(|_| Ok(livekit::RemoveParticipantResponse {}));
    participant_server.mute_published_track_calls(|req| {
        Ok(livekit::MuteRoomTrackResponse {
            track: Some(musivum_track(&req.track_sid, req.muted)),
        })
    });
    participant_server.update_participant_calls(|req| {
        Ok(musivum_participant(
            &req.identity,
            &req.name,
            &req.metadata,
            req.attributes.clone(),
            req.permission.clone(),
        ))
    });
    participant_server.update_subscriptions_calls(|_| Ok(livekit::UpdateSubscriptionsResponse {}));
    participant_server.forward_participant_calls(|_| Ok(livekit::ForwardParticipantResponse {}));
    // what the room manager answers, as psrpc carries it
    participant_server.move_participant_calls(|_| {
        Err(PsrpcError::new(PsrpcErrorCode::Unknown, "not implemented"))
    });
    participant_server.perform_rpc_calls(|req| {
        if req.destination_identity == "slow" {
            return Err(Error::Rtc(rtc::Error::DataChannelRpc(
                putils::data_channel_rpc_error_from_built_in_codes(
                    putils::DATA_CHANNEL_RPC_RESPONSE_TIMEOUT,
                    "",
                ),
            ))
            .into());
        }
        Ok(livekit::PerformRpcResponse {
            payload: format!("pong:{}", req.payload),
        })
    });
    for identity in ["p", "slow"] {
        bus.register_participant_server(
            &format_participant_topic(&"r".into(), &identity.into()),
            participant_server.clone(),
        );
    }

    let svc = RoomService::new(
        LimitConfig {
            max_metadata_size: 10,
            max_attributes_size: 20,
            max_room_name_length: 8,
            max_participant_name_length: 8,
            ..Default::default()
        },
        ApiConfig {
            enable_psrpc_for_get_list_particpants: true,
            ..Default::default()
        },
        router as Arc<dyn MessageRouter>,
        allocator as Arc<dyn RoomAllocator>,
        store.clone() as Arc<dyn ServiceStore>,
        Some(store as Arc<dyn OssServiceStore>),
        new_topic_formatter(),
        new_typed_room_client(bus.clone()).unwrap(),
        new_typed_participant_client(bus).unwrap(),
    )
    .unwrap();
    new_room_service_server(svc, livekit_server_options())
}

fn musivum_request(
    method: &str,
    path: &str,
    content_type: &str,
    grants: &str,
) -> http::request::Parts {
    let mut builder = http::Request::builder().method(method).uri(path);
    if !content_type.is_empty() {
        builder = builder.header("Content-Type", content_type);
    }
    let (mut parts, ()) = builder.body(()).unwrap().into_parts();
    if let Some(grants) = musivum_grants(grants) {
        with_grants(&mut parts.extensions, Some(grants), "");
    }
    parts
}

// The message a protobuf response of the method's path holds, to compare by value: the order
// of a map's entries on the wire is Go's map order, which is random.
fn musivum_response_message(path: &str, body: &[u8]) -> DynamicMessage {
    let method = path.rsplit('/').next().unwrap();
    let name = match method {
        "CreateRoom" | "UpdateRoomMetadata" => "livekit.Room",
        "ListRooms" => "livekit.ListRoomsResponse",
        "DeleteRoom" => "livekit.DeleteRoomResponse",
        "ListParticipants" => "livekit.ListParticipantsResponse",
        "GetParticipant" | "UpdateParticipant" => "livekit.ParticipantInfo",
        "RemoveParticipant" => "livekit.RemoveParticipantResponse",
        "MutePublishedTrack" => "livekit.MuteRoomTrackResponse",
        "UpdateSubscriptions" => "livekit.UpdateSubscriptionsResponse",
        "SendData" => "livekit.SendDataResponse",
        "ForwardParticipant" => "livekit.ForwardParticipantResponse",
        "MoveParticipant" => "livekit.MoveParticipantResponse",
        "PerformRpc" => "livekit.PerformRpcResponse",
        other => panic!("no method {other}"),
    };
    DynamicMessage::decode(protojson::message_descriptor(name).unwrap(), body).unwrap()
}

#[test]
fn the_room_service_answers_as_the_go_original() {
    let cases: Vec<serde_json::Value> = serde_json::from_str(include_str!(
        "testdata/twirp_roomservice_of_the_go_original.json"
    ))
    .unwrap();
    assert_eq!(61, cases.len());
    let s = |c: &serde_json::Value, key: &str| c[key].as_str().unwrap_or_default().to_string();

    let server = new_musivum_room_service_server();
    for c in &cases {
        let name = s(c, "name");
        let req = musivum_request(
            &s(c, "method"),
            &s(c, "path"),
            &s(c, "content_type"),
            &s(c, "grants"),
        );
        let body = match c.get("body_protobuf") {
            Some(b) => STANDARD.decode(b.as_str().unwrap()).unwrap(),
            None => s(c, "body_json").into_bytes(),
        };

        let resp = server.serve_http(&req, &body);

        assert_eq!(
            c["status"].as_u64().unwrap() as u16,
            resp.status().as_u16(),
            "{name}"
        );
        let content_type = resp.headers()["Content-Type"].to_str().unwrap();
        assert_eq!(s(c, "resp_content_type"), content_type, "{name}");
        assert_eq!(
            resp.body().len().to_string(),
            resp.headers()["Content-Length"].to_str().unwrap(),
            "{name}"
        );
        if content_type == "application/json" {
            assert_eq!(
                s(c, "resp_json"),
                String::from_utf8(resp.body().clone()).unwrap(),
                "{name}"
            );
        } else {
            let expected = STANDARD.decode(s(c, "resp_protobuf")).unwrap();
            assert_eq!(
                musivum_response_message(&s(c, "path"), &expected),
                musivum_response_message(&s(c, "path"), resp.body()),
                "{name}"
            );
        }
    }
}

// New for musivum: the hooks of server.go log each call and count it by its status.
#[test]
fn a_call_is_logged_and_counted() {
    let server = new_musivum_room_service_server();
    let count = || {
        crate::telemetry::prometheus::PROM_TWIRP_REQUEST_STATUS_COUNTER
            .with_label_values(["RoomService", "DeleteRoom", "4xx", "not_found"])
            .get()
    };
    let before = count();
    let log = crate::testutils::capture_log(tracing::Level::INFO, || {
        let req = musivum_request(
            "POST",
            "/twirp/livekit.RoomService/DeleteRoom",
            "application/json",
            "create",
        );
        let resp = server.serve_http(&req, br#"{"room":"missing"}"#);
        assert_eq!(404, resp.status().as_u16());
    });
    assert!(count() > before);
    for part in [
        "API RoomService.DeleteRoom",
        "service",
        "\"RoomService\"",
        "method",
        "\"DeleteRoom\"",
        "room",
        "\"missing\"",
        "duration",
        "status",
        "\"404\"",
        "error",
        "\"requested room does not exist\"",
        "code",
        "\"not_found\"",
    ] {
        assert!(log.contains(part), "{part} not in {log}");
    }
}

// New for musivum: the path prefix of the server, and a panic of the service answered with
// Twirp's internal error.
#[test]
fn the_path_prefix_and_a_panic() {
    let server = new_musivum_room_service_server();
    assert_eq!(ROOM_SERVICE_PATH_PREFIX, server.path_prefix());

    let router = Arc::new(FakeRouter::default());
    router.create_room_calls(|_| panic!("a panic of the router"));
    let svc = RoomService::new(
        LimitConfig::default(),
        ApiConfig::default(),
        router as Arc<dyn MessageRouter>,
        Arc::new(FakeRoomAllocator::default()) as Arc<dyn RoomAllocator>,
        Arc::new(FakeObjectStore::default()) as Arc<dyn ServiceStore>,
        None,
        new_topic_formatter(),
        new_typed_room_client(new_local_message_bus()).unwrap(),
        new_typed_participant_client(new_local_message_bus()).unwrap(),
    )
    .unwrap();
    let server = new_room_service_server(
        svc,
        TwirpServerOptions {
            path_prefix: Some("/api/v1".into()),
            ..Default::default()
        },
    );
    assert_eq!("/api/v1/livekit.RoomService/", server.path_prefix());

    let req = musivum_request(
        "POST",
        "/api/v1/livekit.RoomService/CreateRoom",
        "application/protobuf",
        "create",
    );
    let body = livekit::CreateRoomRequest {
        name: "r".into(),
        ..Default::default()
    }
    .encode_to_vec();
    let resp = server.serve_http(&req, &body);
    assert_eq!(500, resp.status().as_u16());
    assert_eq!(
        br#"{"code":"internal","msg":"Internal service panic"}"#.to_vec(),
        *resp.body()
    );
}
