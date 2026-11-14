// Ported from LiveKit, pkg/telemetry/events_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The fixture is that of `stats_test.rs`, as upstream's
// tests of the package share it; the fake's arguments are the event alone (no context). A
// guard is an `Arc` the service is handed. The sleeps are the runtime's.

use std::sync::Arc;

use crate::go::time::{self, MILLISECOND};
use crate::protocol::livekit;
use crate::telemetry::ReferenceGuard;
use crate::telemetry::stats::stats_test::create_fixture;
use crate::testutils::eventually_async;

fn client_info() -> livekit::ClientInfo {
    livekit::ClientInfo {
        sdk: 2,
        version: "v1".to_string(),
        os: "mac".to_string(),
        os_version: "v1".to_string(),
        device_model: "DM1".to_string(),
        browser: "chrome".to_string(),
        browser_version: "97.0.1".to_string(),
        ..Default::default()
    }
}

fn room() -> livekit::Room {
    livekit::Room {
        sid: "RoomSid".to_string(),
        name: "RoomName".to_string(),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_participant_join_event_is_sent() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = "part1";
    let client_info = client_info();
    let client_meta = livekit::AnalyticsClientMeta {
        region: "dark-side".to_string(),
        node: "moon".to_string(),
        client_addr: "127.0.0.1".to_string(),
        client_connect_time: 420,
        ..Default::default()
    };
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    let guard = Arc::new(ReferenceGuard::default());

    // do
    fixture.sut.participant_joined(
        &room,
        &participant_info,
        Some(&client_info),
        Some(&client_meta),
        true,
        Some(guard),
    );
    time::sleep(500 * MILLISECOND).await;

    // test
    assert_eq!(1, fixture.analytics.send_event_call_count());
    let event = fixture.analytics.send_event_args_for_call(0);
    assert_eq!(
        livekit::AnalyticsEventType::ParticipantJoined,
        event.r#type()
    );
    assert_eq!(part_sid, event.participant_id);
    assert_eq!(Some(participant_info), event.participant);
    assert_eq!(room.sid, event.room_id);
    assert_eq!(Some(room), event.room);

    let event_client_info = event.client_info.unwrap();
    assert_eq!(client_info.sdk, event_client_info.sdk);
    assert_eq!(client_info.version, event_client_info.version);
    assert_eq!(client_info.os, event_client_info.os);
    assert_eq!(client_info.os_version, event_client_info.os_version);
    assert_eq!(client_info.device_model, event_client_info.device_model);
    assert_eq!(client_info.browser, event_client_info.browser);
    assert_eq!(
        client_info.browser_version,
        event_client_info.browser_version
    );

    let event_client_meta = event.client_meta.unwrap();
    assert_eq!(client_meta.region, event_client_meta.region);
    assert_eq!(client_meta.node, event_client_meta.node);
    assert_eq!(client_meta.client_addr, event_client_meta.client_addr);
    assert_eq!(
        client_meta.client_connect_time,
        event_client_meta.client_connect_time
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_participant_left_event_is_sent() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = "part1";
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    let guard = Arc::new(ReferenceGuard::default());

    // do
    fixture.sut.participant_active(
        &room,
        &participant_info,
        &livekit::AnalyticsClientMeta::default(),
        false,
        false,
        Some(guard.clone()),
    );
    fixture
        .sut
        .participant_left(&room, &participant_info, true, Some(guard));
    time::sleep(500 * MILLISECOND).await;

    // test
    assert_eq!(2, fixture.analytics.send_event_call_count());
    let event = fixture.analytics.send_event_args_for_call(1);
    assert_eq!(livekit::AnalyticsEventType::ParticipantLeft, event.r#type());
    assert_eq!(part_sid, event.participant_id);
    assert_eq!(room.sid, event.room_id);
    assert_eq!(Some(room), event.room);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_track_update_event_is_sent() {
    let fixture = create_fixture();

    // prepare
    let room_id = "room1";
    let room_name = "RoomName";
    let part_id = "part1";
    let track_id = "track1";
    let layer = livekit::VideoLayer {
        quality: livekit::VideoQuality::High as i32,
        width: 360,
        height: 720,
        bitrate: 2048,
        ..Default::default()
    };

    let track_info = livekit::TrackInfo {
        sid: track_id.to_string(),
        r#type: livekit::TrackType::Video as i32,
        muted: false,
        simulcast: false,
        disable_dtx: false,
        layers: vec![layer.clone()],
        ..Default::default()
    };

    // do
    fixture.sut.track_published_update(
        &livekit::Room {
            sid: room_id.to_string(),
            name: room_name.to_string(),
            ..Default::default()
        },
        &livekit::ParticipantId::from(part_id),
        &track_info,
    );
    time::sleep(500 * MILLISECOND).await;

    // test
    assert_eq!(1, fixture.analytics.send_event_call_count());
    let event = fixture.analytics.send_event_args_for_call(0);
    assert_eq!(
        livekit::AnalyticsEventType::TrackPublishedUpdate,
        event.r#type()
    );
    assert_eq!(part_id, event.participant_id);

    let track = event.track.unwrap();
    assert_eq!(track_id, track.sid);
    assert!(!track.layers.is_empty());
    assert_eq!(layer.width, track.layers[0].width);
    assert_eq!(layer.height, track.layers[0].height);
    assert_eq!(layer.quality, track.layers[0].quality);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_participant_active_event_is_sent() {
    let fixture = create_fixture();

    // prepare participant to change status
    let room = room();
    let part_sid = "part1";

    let client_info = client_info();
    let client_meta = livekit::AnalyticsClientMeta {
        region: "dark-side".to_string(),
        node: "moon".to_string(),
        client_addr: "127.0.0.1".to_string(),
        ..Default::default()
    };
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    let guard = Arc::new(ReferenceGuard::default());

    // do
    fixture.sut.participant_joined(
        &room,
        &participant_info,
        Some(&client_info),
        Some(&client_meta),
        true,
        Some(guard.clone()),
    );
    time::sleep(500 * MILLISECOND).await;

    // test
    assert_eq!(1, fixture.analytics.send_event_call_count());
    let event = fixture.analytics.send_event_args_for_call(0);

    // test
    // do
    let client_meta_connect = livekit::AnalyticsClientMeta {
        client_connect_time: 420,
        ..Default::default()
    };

    fixture.sut.participant_active(
        &room,
        &participant_info,
        &client_meta_connect,
        false,
        false,
        Some(guard),
    );
    time::sleep(500 * MILLISECOND).await;

    assert_eq!(2, fixture.analytics.send_event_call_count());
    let event_active = fixture.analytics.send_event_args_for_call(1);
    assert_eq!(
        livekit::AnalyticsEventType::ParticipantActive,
        event_active.r#type()
    );
    assert_eq!(part_sid, event_active.participant_id);
    assert_eq!(room.sid, event_active.room_id);
    assert_eq!(Some(room), event.room);

    assert_eq!(
        client_meta_connect.client_connect_time,
        event_active.client_meta.unwrap().client_connect_time
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_track_subscribed_event_is_sent() {
    let fixture = create_fixture();

    // prepare participant to change status
    let room = room();
    let part_sid = "part1";
    let publisher_info = livekit::ParticipantInfo {
        sid: "pub1".to_string(),
        identity: "publisher".to_string(),
        ..Default::default()
    };
    let track_info = livekit::TrackInfo {
        sid: "tr1".to_string(),
        r#type: livekit::TrackType::Video as i32,
        ..Default::default()
    };

    let client_info = client_info();
    let client_meta = livekit::AnalyticsClientMeta {
        region: "dark-side".to_string(),
        node: "moon".to_string(),
        client_addr: "127.0.0.1".to_string(),
        ..Default::default()
    };
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    let guard = Arc::new(ReferenceGuard::default());

    // do
    fixture.sut.participant_joined(
        &room,
        &participant_info,
        Some(&client_info),
        Some(&client_meta),
        true,
        Some(guard),
    );
    time::sleep(500 * MILLISECOND).await;

    // test
    assert_eq!(1, fixture.analytics.send_event_call_count());
    let event = fixture.analytics.send_event_args_for_call(0);
    assert_eq!(Some(room.clone()), event.room);

    // do
    fixture.sut.track_subscribed(
        &room,
        &livekit::ParticipantId::from(part_sid),
        &track_info,
        &publisher_info,
        true,
    );
    time::sleep(500 * MILLISECOND).await;

    let analytics = fixture.analytics.clone();
    eventually_async(
        move || analytics.send_event_call_count() == 2,
        std::time::Duration::from_secs(1),
        std::time::Duration::from_millis(50),
    )
    .await;
    let event_track_subscribed = fixture.analytics.send_event_args_for_call(1);
    assert_eq!(
        livekit::AnalyticsEventType::TrackSubscribed,
        event_track_subscribed.r#type()
    );
    assert_eq!(part_sid, event_track_subscribed.participant_id);
    let track = event_track_subscribed.track.unwrap();
    assert_eq!(track_info.sid, track.sid);
    assert_eq!(track_info.r#type, track.r#type);
    let publisher = event_track_subscribed.publisher.unwrap();
    assert_eq!(publisher_info.sid, publisher.sid);
    assert_eq!(publisher_info.identity, publisher.identity);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_room_ended_reason_is_sent() {
    let fixture = create_fixture();

    let room = room();
    fixture
        .sut
        .room_ended(&room, livekit::RoomEndReason::RoomEndApiDelete);

    fixture.flush().await;

    assert_eq!(1, fixture.analytics.send_event_call_count());
    let event = fixture.analytics.send_event_args_for_call(0);
    assert_eq!(livekit::AnalyticsEventType::RoomEnded, event.r#type());
    assert_eq!(room.sid, event.room_id);
    assert_eq!(
        livekit::RoomEndReason::RoomEndApiDelete,
        event.room_end_reason()
    );
}

// The room, participant and track of the next test, the same as `MusivumWebhookRoom` of
// `testdata/webhooks_test.go`.
fn webhook_room() -> (livekit::Room, livekit::ParticipantInfo, livekit::TrackInfo) {
    let room = livekit::Room {
        sid: "RM_aRoomOfTheTest".to_string(),
        name: "a-room".to_string(),
        empty_timeout: 300,
        max_participants: 10,
        creation_time: 1700000000,
        creation_time_ms: 1700000000123,
        enabled_codecs: vec![
            livekit::Codec {
                mime: "audio/opus".to_string(),
                ..Default::default()
            },
            livekit::Codec {
                mime: "video/VP8".to_string(),
                fmtp_line: "x=1".to_string(),
            },
        ],
        metadata: "some metadata".to_string(),
        num_participants: 1,
        version: Some(livekit::TimedVersion {
            unix_micro: 1700000000123456,
            ticks: 2,
        }),
        ..Default::default()
    };
    let participant = livekit::ParticipantInfo {
        sid: "PA_aParticipant".to_string(),
        identity: "alice".to_string(),
        state: livekit::participant_info::State::Active as i32,
        joined_at: 1700000001,
        name: "Alice".to_string(),
        version: 3,
        permission: Some(livekit::ParticipantPermission {
            can_subscribe: true,
            can_publish: true,
            can_publish_data: true,
            can_publish_sources: vec![
                livekit::TrackSource::Camera as i32,
                livekit::TrackSource::Microphone as i32,
            ],
            ..Default::default()
        }),
        is_publisher: true,
        kind: livekit::participant_info::Kind::Standard as i32,
        attributes: [("a".to_string(), "b".to_string())].into(),
        ..Default::default()
    };
    let track = livekit::TrackInfo {
        sid: "TR_aTrack".to_string(),
        r#type: livekit::TrackType::Video as i32,
        name: "camera".to_string(),
        width: 1280,
        height: 720,
        simulcast: true,
        source: livekit::TrackSource::Camera as i32,
        mime_type: "video/VP8".to_string(),
        mid: "1".to_string(),
        disable_red: true,
        stream: "camera".to_string(),
        layers: vec![
            livekit::VideoLayer {
                quality: livekit::VideoQuality::Low as i32,
                width: 320,
                height: 180,
                bitrate: 150000,
                ssrc: 1111,
                ..Default::default()
            },
            livekit::VideoLayer {
                quality: livekit::VideoQuality::High as i32,
                width: 1280,
                height: 720,
                bitrate: 1700000,
                ssrc: 2222,
                rid: "f".to_string(),
                ..Default::default()
            },
        ],
        codecs: vec![livekit::SimulcastCodecInfo {
            mime_type: "video/VP8".to_string(),
            mid: "1".to_string(),
            cid: "a-cid".to_string(),
            video_layer_mode: livekit::video_layer::Mode::OneSpatialLayerPerStream as i32,
            ..Default::default()
        }],
        version: Some(livekit::TimedVersion {
            unix_micro: 1700000002000000,
            ticks: 0,
        }),
        ..Default::default()
    };
    (room, participant, track)
}

// What may differ between two sends of the same webhook (its id, the times, the signature)
// replaced by what it was checked to be; `musivumNormalize` of `testdata/webhooks_test.go`.
fn normalize(r: &http::Request<bytes::Bytes>) -> serde_json::Value {
    use base64::Engine as _;
    use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};

    let now = time::now().unix_secs();
    let header = |name: &str| {
        r.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string()
    };

    let mut body: Value = serde_json::from_slice(r.body()).unwrap();
    let id = body["id"].as_str().unwrap().to_string();
    assert!(id.starts_with("EV_"), "{id}");
    body["id"] = format!("EV_ and {} characters", id.len() - 3).into();
    let created_at: i64 = body["createdAt"].as_str().unwrap().parse().unwrap();
    assert!((now - created_at).abs() <= 60, "{created_at}");
    body["createdAt"] = "<now>".into();

    let authorization = header("Authorization");
    let parts: Vec<&str> = authorization.split('.').collect();
    assert_eq!(3, parts.len());
    let jwt_header: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
    let mut claims: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();

    let sum = Sha256::digest(r.body());
    assert_eq!(Value::from(STANDARD.encode(sum)), claims["sha256"]);
    claims["sha256"] = "<sha256 of the body>".into();
    let (exp, nbf) = (
        claims["exp"].as_i64().unwrap(),
        claims["nbf"].as_i64().unwrap(),
    );
    assert!((now - nbf).abs() <= 60, "{nbf}");
    let object = claims.as_object_mut().unwrap();
    object.remove("exp");
    object.remove("nbf");
    claims["exp - nbf"] = (exp - nbf).into();
    if let Some(iat) = claims.get("iat").and_then(Value::as_i64) {
        assert!((now - iat).abs() <= 60, "{iat}");
        claims["iat"] = "<now>".into();
    }

    json!({
        "contentType": header("Content-Type"),
        "userAgent": header("User-Agent"),
        "jwtHeader": jwt_header,
        "claims": claims,
        "body": body,
    })
}

// New for musivum: the webhooks of a room, from its start to its end, as a receiver gets them
// (the body as JSON, the headers, and the claims of the token in `Authorization`), compared
// with what LiveKit's telemetry service sends for the same calls: `testdata/webhooks_test.go`,
// run at the pin, wrote `testdata/webhooks_of_the_go_original.json`.
#[tokio::test(flavor = "multi_thread")]
async fn the_webhooks_of_a_room_are_those_of_the_go_original() {
    use crate::protocol::auth::SimpleKeyProvider;
    use crate::protocol::webhook::{self, webhook_test::new_server};
    use crate::telemetry::{NullAnalyticService, new_telemetry_service};

    let s = new_server().await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.handler(move |r| {
        let _ = tx.send(r);
    });

    let notifier = webhook::new_default_notifier(
        &webhook::WebHookConfig {
            urls: vec![s.url()],
            api_key: "mykey".to_string(),
            ..Default::default()
        },
        Arc::new(SimpleKeyProvider::new("mykey", "mysecret")),
    )
    .unwrap();
    let ts = new_telemetry_service(Some(notifier.clone()), Arc::new(NullAnalyticService {}));

    let (room, participant, track) = webhook_room();
    let guard = Arc::new(ReferenceGuard::default());
    ts.room_started(&room);
    ts.participant_active(
        &room,
        &participant,
        &livekit::AnalyticsClientMeta {
            region: "a-region".to_string(),
            ..Default::default()
        },
        false,
        false,
        Some(guard.clone()),
    );
    ts.track_published(
        &room,
        &livekit::ParticipantId::from(&participant.sid),
        &livekit::ParticipantIdentity::from(&participant.identity),
        &track,
        true,
    );
    ts.participant_left(&room, &participant, true, Some(guard));
    ts.room_ended(&room, livekit::RoomEndReason::RoomEndIdleTimeout);

    let mut received = Vec::new();
    for _ in 0..5 {
        let r = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("a webhook within 10 s")
            .unwrap();
        received.push(normalize(&r));
    }
    notifier.stop(false);
    s.stop();

    let got = serde_json::Value::Array(received);
    let want: serde_json::Value =
        serde_json::from_str(include_str!("testdata/webhooks_of_the_go_original.json")).unwrap();
    assert!(
        want == got,
        "musivum sends\n{}\nLiveKit sends\n{}",
        serde_json::to_string_pretty(&got).unwrap(),
        serde_json::to_string_pretty(&want).unwrap()
    );
}
