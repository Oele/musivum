// Ported from LiveKit, pkg/service/utils_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `redisClientDocker` and `redisClient` (helpers for
// the Redis store's tests) are not ported, nor is `TestRemoveDoubleSlashes`: there is no
// `RemoveDoubleSlashes` (middleware of Go's net/http). The tests after
// `test_decompress_gzip` are new; their expected values are what the original gave in a run
// on the same inputs.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use flate2::Compression;
use flate2::write::GzEncoder;
use http::Request;

use super::*;
use crate::config::Config;
use crate::protocol::auth::{ClaimGrants, VideoGrant};
use crate::service::{LocalStore, ObjectStore, new_room_allocator, with_grants};

#[test]
fn test_is_valid_domain() {
    let list = HashMap::from([
        ("turn.myhost.com", true),
        ("turn.google.com", true),
        ("https://host.com", false),
        ("turn://host.com", false),
    ]);
    for (key, result) in list {
        is_valid_domain(key);
        assert_eq!(is_valid_domain(key), result);
    }
}

fn compress(payload: &[u8]) -> Vec<u8> {
    let mut gw = GzEncoder::new(Vec::new(), Compression::best());
    gw.write_all(payload).unwrap();
    gw.finish().unwrap()
}

#[test]
fn test_decompress_gzip() {
    {
        // small payload
        let out = decompress_gzip(&compress(b"hello world")).unwrap();
        assert_eq!(b"hello world".to_vec(), out);
    }

    {
        // payload exactly at cap
        let raw = vec![0u8; DEFAULT_MAX_HEADER_BYTES];
        let out = decompress_gzip(&compress(&raw)).unwrap();
        assert_eq!(DEFAULT_MAX_HEADER_BYTES, out.len());
    }

    {
        // payload one byte over capd
        let raw = vec![0u8; DEFAULT_MAX_HEADER_BYTES + 1];
        let err = decompress_gzip(&compress(&raw)).unwrap_err();
        assert_eq!(Error::GzipTooLarge, err);
    }

    {
        // gzip decompression bomb
        // 100 MB of zeros
        let raw = vec![0u8; 100 << 20];
        let compressed = compress(&raw);
        assert!(
            compressed.len() < 1 << 20,
            "sanity: bomb input should compress dramatically"
        );

        let err = decompress_gzip(&compressed).unwrap_err();
        assert_eq!(Error::GzipTooLarge, err);
    }

    {
        // malformed gzip compression
        let err = decompress_gzip(b"not gzip data").unwrap_err();
        assert!(err.to_string().contains("cannot read decompressed"));
    }
}

// New for musivum: an empty input is no gzip data either, as Go's reader says (EOF).
#[test]
fn test_decompress_gzip_empty() {
    let err = decompress_gzip(b"").unwrap_err();
    assert_eq!("cannot read decompressed data: EOF", err.to_string());
}

fn request(uri: &str, headers: &[(&str, &str)]) -> Parts {
    let mut b = Request::get(uri);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let mut r = b.body(()).unwrap().into_parts().0;
    // httptest.NewRequest's peer
    r.extensions
        .insert(RemoteAddr("192.0.2.1:1234".parse().unwrap()));
    r
}

// New for musivum: ValidateConnectRequest's refusals, with the statuses and texts of a run of
// the original (no room is created automatically; at most 8 bytes of room name and 6 of
// identity).
#[test]
fn test_validate_connect_request_refusals() {
    let mut conf = Config::new("", true).unwrap();
    conf.room.auto_create = false;
    let mut lim = conf.limit.clone();
    lim.max_room_name_length = 8;
    lim.max_participant_identity_length = 6;
    let store = LocalStore::new();
    let ra = new_room_allocator(Arc::new(conf), store.clone() as Arc<dyn ObjectStore>);

    let join = |room: &str| ClaimGrants {
        identity: "alice".into(),
        video: Some(VideoGrant {
            room_join: true,
            room: room.into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let validate = |grants: Option<ClaimGrants>, params: ValidateConnectRequestParams| {
        let mut r = request("/rtc/v1", &[]);
        if let Some(grants) = grants {
            with_grants(&mut r.extensions, Some(grants), "key");
        }
        let (res, code, err) = validate_connect_request(
            &crate::protocol::logger::get_logger(),
            &r,
            &lim,
            params,
            None,
            ra.as_ref(),
        );
        (res, code.as_u16(), err.err().map(|e| e.to_string()))
    };
    let no_params = ValidateConnectRequestParams::default;

    let cases: Vec<(
        &str,
        Option<ClaimGrants>,
        ValidateConnectRequestParams,
        u16,
        &str,
    )> = vec![
        (
            "no grants",
            None,
            no_params(),
            401,
            "no permissions to access the room",
        ),
        (
            "no video grant",
            Some(ClaimGrants {
                identity: "alice".into(),
                ..Default::default()
            }),
            no_params(),
            401,
            "no permissions to access the room",
        ),
        (
            "no join permission",
            Some(ClaimGrants {
                identity: "alice".into(),
                video: Some(VideoGrant {
                    room: "r".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            no_params(),
            401,
            "permissions denied",
        ),
        (
            "identity empty",
            Some(ClaimGrants {
                identity: String::new(),
                ..join("r")
            }),
            no_params(),
            400,
            "identity cannot be empty",
        ),
        (
            "identity too long",
            Some(ClaimGrants {
                identity: "alice-x".into(),
                ..join("r")
            }),
            no_params(),
            400,
            "participant identity length exceeds limits: max length 6",
        ),
        (
            "no room name",
            Some(join("")),
            no_params(),
            400,
            "no room name",
        ),
        (
            "room name too long",
            Some(join("room-name")),
            no_params(),
            400,
            "room name length exceeds limits: max length 8",
        ),
        (
            "publish without permission",
            Some({
                let mut g = join("r");
                g.video.as_mut().unwrap().set_can_publish(false);
                g
            }),
            ValidateConnectRequestParams {
                publish: "x".into(),
                ..Default::default()
            },
            401,
            "no permissions to access the room",
        ),
        (
            "room does not exist",
            Some(join("r")),
            no_params(),
            404,
            "requested room does not exist",
        ),
        (
            "metadata without permission",
            Some(join("r")),
            ValidateConnectRequestParams {
                metadata: "m".into(),
                ..Default::default()
            },
            404,
            "requested room does not exist",
        ),
        (
            "room from params",
            Some(ClaimGrants {
                video: Some(VideoGrant {
                    room_join: true,
                    ..Default::default()
                }),
                ..join("")
            }),
            ValidateConnectRequestParams {
                room_name: "toolongname".into(),
                ..Default::default()
            },
            400,
            "room name length exceeds limits: max length 8",
        ),
    ];
    for (name, grants, params, code, text) in cases {
        let (_, got_code, got_err) = validate(grants, params);
        assert_eq!(
            (code, Some(text.to_string())),
            (got_code, got_err),
            "{name}"
        );
    }

    // with the room created
    ra.create_room(
        &livekit::CreateRoomRequest {
            name: "r".into(),
            ..Default::default()
        },
        true,
    )
    .unwrap();

    for (name, params) in [
        (
            "metadata without permission, room exists",
            ValidateConnectRequestParams {
                metadata: "m".into(),
                ..Default::default()
            },
        ),
        (
            "attributes without permission, room exists",
            ValidateConnectRequestParams {
                attributes: HashMap::from([("a".to_string(), "b".to_string())]),
                ..Default::default()
            },
        ),
    ] {
        let (_, code, err) = validate(Some(join("r")), params);
        assert_eq!(
            (401, Some("no permissions to access the room".to_string())),
            (code, err),
            "{name}"
        );
    }

    // accepted: the metadata and the attributes (not the empty one) go into the grants
    let mut g = join("r");
    g.video.as_mut().unwrap().set_can_update_own_metadata(true);
    g.attributes = HashMap::from([
        ("keep".to_string(), "1".to_string()),
        ("a".to_string(), "old".to_string()),
    ]);
    let (res, code, err) = validate(
        Some(g.clone()),
        ValidateConnectRequestParams {
            metadata: "m".into(),
            attributes: HashMap::from([
                ("a".to_string(), "b".to_string()),
                ("c".to_string(), String::new()),
            ]),
            ..Default::default()
        },
    );
    assert_eq!((200, None), (code, err));
    assert_eq!("r", res.room_name.as_str());
    let grants = res.grants.unwrap();
    assert_eq!("m", grants.metadata);
    assert_eq!(
        HashMap::from([
            ("a".to_string(), "b".to_string()),
            ("keep".to_string(), "1".to_string()),
        ]),
        grants.attributes
    );
    assert_eq!(
        Some(livekit::CreateRoomRequest {
            name: "r".into(),
            ..Default::default()
        }),
        res.create_room_request
    );

    // a node over its limits (the original's router with a node), and under them
    let mut r = request("/rtc/v1", &[]);
    with_grants(&mut r.extensions, Some(g), "key");
    let node = livekit::Node {
        region: "region-a".into(),
        stats: Some(livekit::NodeStats {
            num_tracks_in: 6,
            num_tracks_out: 5,
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut lim2 = lim.clone();
    lim2.num_tracks = 10;
    let (res, code, err) = validate_connect_request(
        &crate::protocol::logger::get_logger(),
        &r,
        &lim2,
        ValidateConnectRequestParams::default(),
        Some(&node),
        ra.as_ref(),
    );
    assert_eq!(
        (
            503,
            Some("node has exceeded its configured limit".to_string())
        ),
        (code.as_u16(), err.err().map(|e| e.to_string()))
    );
    assert_eq!("region-a", res.region);
    lim2.num_tracks = 12;
    let (res, code, err) = validate_connect_request(
        &crate::protocol::logger::get_logger(),
        &r,
        &lim2,
        ValidateConnectRequestParams::default(),
        Some(&node),
        ra.as_ref(),
    );
    assert_eq!((200, None), (code.as_u16(), err.err()));
    assert_eq!("region-a", res.region);

    // publishing on a second connection: subscribing off, the identity marked
    let mut g = join("r");
    g.video.as_mut().unwrap().set_can_publish(true);
    g.room_preset = "preset".into();
    let (res, code, err) = validate(
        Some(g),
        ValidateConnectRequestParams {
            publish: "x".into(),
            ..Default::default()
        },
    );
    assert_eq!((200, None), (code, err));
    let grants = res.grants.unwrap();
    assert_eq!("alice#x", grants.identity);
    assert!(!grants.video.unwrap().get_can_subscribe());
    assert_eq!(
        Some(livekit::CreateRoomRequest {
            name: "r".into(),
            room_preset: "preset".into(),
            ..Default::default()
        }),
        res.create_room_request
    );
}

// New for musivum: the query a JavaScript SDK sends on /rtc (names from rtcservice.go and
// ParseClientInfo), and what the original made of it; the JSON is the original's.
#[test]
fn test_parse_client_info() {
    let r = request(
        "/rtc?access_token=x&auto_subscribe=1&sdk=js&version=2.24.0&protocol=16&client_protocol=1&adaptive_stream=1&network=wifi&capabilities=CAP_COMPRESSION_DEFLATE_RAW,%20NOPE,,CAP_UNKNOWN",
        &[
            (
                "User-Agent",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ),
            ("X-Forwarded-For", "198.51.100.7"),
        ],
    );
    let ci = parse_client_info(&r);
    let want: serde_json::Value = serde_json::from_str(
        r#"{"sdk":"JS", "version":"2.24.0", "protocol":16, "os":"Mac OS X", "osVersion":"10.15.7", "deviceModel":"Mac", "browser":"Chrome", "browserVersion":"131.0.0", "address":"198.51.100.7", "network":"wifi", "clientProtocol":1, "capabilities":["CAP_COMPRESSION_DEFLATE_RAW"]}"#,
    )
    .unwrap();
    assert_eq!(want, serde_json::to_value(&ci).unwrap());

    // what the client says is kept; the address of the peer
    let mut r = request(
        "/rtc?sdk=js&browser=Firefox&browser_version=133.0&os=Linux&device_model=",
        &[(
            "User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
        )],
    );
    r.extensions
        .insert(RemoteAddr("[2001:db8::1]:4000".parse().unwrap()));
    let ci = parse_client_info(&r);
    let want: serde_json::Value = serde_json::from_str(
        r#"{"sdk":"JS", "os":"Linux", "deviceModel":"Mac", "browser":"Firefox", "browserVersion":"133.0", "address":"2001:db8::1"}"#,
    )
    .unwrap();
    assert_eq!(want, serde_json::to_value(&ci).unwrap());

    // an SDK that is not a browser: no user agent is read
    let r = request(
        "/rtc?sdk=android&protocol=abc",
        &[("User-Agent", "okhttp/4.12.0")],
    );
    let ci = parse_client_info(&r);
    let want: serde_json::Value =
        serde_json::from_str(r#"{"sdk":"ANDROID", "address":"192.0.2.1"}"#).unwrap();
    assert_eq!(want, serde_json::to_value(&ci).unwrap());
}

// New for musivum: the join request of a current JavaScript SDK (on /rtc/v1) carries this
// client info, without browser and system; the original filled them in from the user agent
// of a headless Chrome as below.
#[test]
fn test_augment_client_info() {
    let r = request(
        "/rtc/v1?access_token=x&join_request=x",
        &[(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/157.0.0.0 Safari/537.36",
        )],
    );
    let mut ci = livekit::ClientInfo {
        sdk: Sdk::Js as i32,
        version: "2.24.0".into(),
        protocol: 18,
        client_protocol: 2,
        capabilities: vec![Capability::CapCompressionDeflateRaw as i32],
        ..Default::default()
    };
    augment_client_info(&mut ci, &r);
    let want: serde_json::Value = serde_json::from_str(
        r#"{"sdk": "JS", "version": "2.24.0", "protocol": 18, "os": "Linux", "deviceModel": "Other", "browser": "HeadlessChrome", "browserVersion": "157.0.0", "address": "192.0.2.1", "clientProtocol": 2, "capabilities": ["CAP_COMPRESSION_DEFLATE_RAW"]}"#,
    )
    .unwrap();
    assert_eq!(want, serde_json::to_value(&ci).unwrap());
}

// New for musivum: the quirks of the original's domain expression, the client's address, and
// Go's reading of a query.
#[test]
fn test_domain_ip_and_query() {
    for (domain, valid) in [
        ("turn.myhost.com", true),
        ("TURN.Example.ORG.", true),
        ("a-b.c", true),
        ("a..b", false),
        ("nodot", false),
        ("turn.\u{17f}ite.com", true),
        ("\u{212a}.com", true),
        ("turn.ex_ample.com", false),
    ] {
        assert_eq!(valid, is_valid_domain(domain), "{domain}");
    }

    for (header, value, ip) in [
        ("CF-Connecting-IP", "203.0.113.1", "203.0.113.1"),
        (
            "X-Forwarded-For",
            "203.0.113.2, 10.0.0.1",
            "203.0.113.2, 10.0.0.1",
        ),
        ("X-Real-IP", "203.0.113.3", "203.0.113.3"),
    ] {
        assert_eq!(ip, get_client_ip(&request("/rtc", &[(header, value)])));
    }
    assert_eq!("192.0.2.1", get_client_ip(&request("/rtc", &[])));
    let mut r = request("/rtc", &[]);
    r.extensions.remove::<RemoteAddr>();
    assert_eq!("", get_client_ip(&r));

    let values = Values::parse_query("a=1&a=2&b=x+y%21&c=%zz&d;=1&&e&f=%41");
    assert_eq!("1", values.get("a"));
    assert_eq!("x y!", values.get("b"));
    assert_eq!("", values.get("c"));
    assert_eq!("", values.get("d"));
    assert_eq!("", values.get("e"));
    assert_eq!("A", values.get("f"));
    assert_eq!(5, values.0.len());

    assert!(bool_value("1") && bool_value("true"));
    assert!(!bool_value("True") && !bool_value("0") && !bool_value(""));
}
