// Ported from LiveKit's protocol, auth/accesstoken_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The key and the secret of a test are made up here
// (the original draws them with `guid.New` and `utils.RandomSecret`, which are not ported).
// The tests at the end are new and say so.

use std::sync::atomic::{AtomicU32, Ordering};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use super::*;
use crate::protocol::auth::parse_api_token;
use crate::protocol::livekit::participant_info::{Kind, KindDetail};
use crate::protocol::livekit::{TrackSource, encoded_file_output};

#[test]
fn test_access_token() {
    // keys must be set
    {
        let token = AccessToken::new("", "");
        let err = token.to_jwt().unwrap_err();
        assert_eq!(Error::KeysMissing, err);
    }

    // generates a decode-able key
    {
        let (api_key, secret) = api_keypair();
        let video_grant = VideoGrant {
            room_join: true,
            room: "myroom".into(),
            ..Default::default()
        };
        let sip_grant = SipGrant {
            admin: true,
            ..Default::default()
        };
        let agent_grant = AgentGrant {
            admin: true,
            ..Default::default()
        };
        let inference_grant = InferenceGrant { perform: true };
        let at = AccessToken::new(&api_key, &secret)
            .set_video_grant(video_grant.clone())
            .set_sip_grant(sip_grant.clone())
            .set_inference_grant(inference_grant.clone())
            .set_agent_grant(agent_grant.clone())
            .set_valid_for(time::MINUTE * 5)
            .set_kind(Kind::Agent)
            .set_identity("user");
        let value = at.to_jwt().unwrap();

        assert_eq!(3, value.split('.').count());

        // ensure it's a valid JWT
        let decoded = jwt::parse_unverified::<TokenClaims>(&value).unwrap().claims;
        let decoded_grant = decoded.claim_grants;

        assert_eq!(Kind::Agent, decoded_grant.get_participant_kind());
        assert_eq!(Some(video_grant), decoded_grant.video);
        assert_eq!(Some(sip_grant), decoded_grant.sip);
        assert_eq!(Some(agent_grant), decoded_grant.agent);
        assert_eq!(Some(inference_grant), decoded_grant.inference);
    }

    // missing kind should be interpreted as standard
    {
        let (api_key, secret) = api_keypair();
        let value = AccessToken::new(&api_key, &secret)
            .set_video_grant(VideoGrant {
                room_join: true,
                room: "myroom".into(),
                ..Default::default()
            })
            .to_jwt()
            .unwrap();
        let decoded = jwt::parse_unverified::<TokenClaims>(&value).unwrap().claims;

        // default validity
        assert_eq!(Kind::Standard, decoded.claim_grants.get_participant_kind());
    }

    // default validity should be more than a minute
    {
        let (api_key, secret) = api_keypair();
        let video_grant = VideoGrant {
            room_join: true,
            room: "myroom".into(),
            ..Default::default()
        };
        let at = AccessToken::new(&api_key, &secret).set_video_grant(video_grant.clone());
        let value = at.to_jwt().unwrap();
        let decoded = jwt::parse_unverified::<TokenClaims>(&value).unwrap().claims;
        assert_eq!(Some(video_grant), decoded.claim_grants.video);

        // default validity
        let expires_at = decoded.registered_claims.expires_at.unwrap();
        let issued_at = decoded.registered_claims.issued_at.unwrap();
        assert!(expires_at.time().sub(issued_at.time()) > time::MINUTE);
    }

    // room configuration serialization and deserialization
    {
        let (api_key, secret) = api_keypair();
        let room_config = livekit::RoomConfiguration {
            agents: vec![livekit::RoomAgentDispatch {
                agent_name: "agent1".into(),
                metadata: "metadata1".into(),
                ..Default::default()
            }],
            sync_streams: true,
            egress: Some(livekit::RoomEgress {
                room: Some(livekit::RoomCompositeEgressRequest {
                    file_outputs: vec![livekit::EncodedFileOutput {
                        disable_manifest: true,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let video_grant = VideoGrant {
            room_join: true,
            room: "test-room".into(),
            ..Default::default()
        };
        let at = AccessToken::new(&api_key, &secret)
            .set_video_grant(video_grant)
            .set_room_config(room_config);

        let value = at.to_jwt().unwrap();

        // Parse and verify the token
        let decoded = jwt::parse_unverified::<TokenClaims>(&value).unwrap().claims;

        // Check if the room configuration was correctly serialized and deserialized
        let room_decoded: livekit::RoomConfiguration =
            decoded.claim_grants.room_config.unwrap().into();
        let agents = &room_decoded.agents;
        assert_eq!(1, agents.len());
        assert_eq!("agent1", agents[0].agent_name);
        assert_eq!("metadata1", agents[0].metadata);
        let egress = room_decoded.egress.as_ref().unwrap();
        assert!(egress.room.as_ref().unwrap().file_outputs[0].disable_manifest);

        // Ensure that we are encoding room configuration with camelCase
        let parts: Vec<&str> = value.split('.').collect();
        assert_eq!(3, parts.len(), "JWT should have three parts");
        let payload = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();

        // Parse the JSON
        let json_payload: serde_json::Value = serde_json::from_slice(&payload).unwrap();

        // Navigate to the agents array
        let room = json_payload["roomConfig"]
            .as_object()
            .expect("room should be a map");
        let agents_json = room["agents"]
            .as_array()
            .expect("agents should be an array");
        assert_eq!(1, agents.len(), "there should be one agent");

        // Check if agentName is in camelCase
        let agent = agents_json[0].as_object().expect("agent should be a map");
        assert!(
            agent.contains_key("agentName"),
            "agentName should be present in camelCase"
        );
        assert!(
            !agent.contains_key("agent_name"),
            "agent_name should not be present in snake_case"
        );

        // room configuration blocks sensitive credentials by default
        {
            let (api_key, secret) = api_keypair();
            let room_config = livekit::RoomConfiguration {
                egress: Some(livekit::RoomEgress {
                    room: Some(livekit::RoomCompositeEgressRequest {
                        file_outputs: vec![livekit::EncodedFileOutput {
                            output: Some(encoded_file_output::Output::S3(livekit::S3Upload {
                                secret: "super-secret".into(),
                                ..Default::default()
                            })),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let err = AccessToken::new(&api_key, &secret)
                .set_video_grant(VideoGrant {
                    room_join: true,
                    room: "test-room".into(),
                    ..Default::default()
                })
                .set_room_config(room_config)
                .to_jwt()
                .unwrap_err();
            assert_eq!(Error::SensitiveCredentials, err);
        }

        // room configuration allows sensitive credentials when enabled
        {
            let (api_key, secret) = api_keypair();
            let room_config = livekit::RoomConfiguration {
                egress: Some(livekit::RoomEgress {
                    room: Some(livekit::RoomCompositeEgressRequest {
                        file_outputs: vec![livekit::EncodedFileOutput {
                            output: Some(encoded_file_output::Output::S3(livekit::S3Upload {
                                secret: "super-secret".into(),
                                ..Default::default()
                            })),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let value = AccessToken::new(&api_key, &secret)
                .set_video_grant(VideoGrant {
                    room_join: true,
                    room: "test-room".into(),
                    ..Default::default()
                })
                .set_room_config(room_config)
                .set_allow_sensitive_credentials(true)
                .to_jwt()
                .unwrap();

            let v = parse_api_token(&value).unwrap();
            let (_, claims) = v.verify(&secret).unwrap();

            let rc = claims.get_room_configuration().unwrap();
            let room = rc.egress.as_ref().unwrap().room.as_ref().unwrap();
            assert!(!room.file_outputs.is_empty());
            let Some(encoded_file_output::Output::S3(s3_out)) = &room.file_outputs[0].output else {
                panic!("the output is not S3");
            };
            assert_eq!("super-secret", s3_out.secret);
        }
    }
}

// The original draws both at random. Nothing in the tests depends on what they are: here
// they only differ from one call to the next.
fn api_keypair() -> (String, String) {
    static COUNT: AtomicU32 = AtomicU32::new(0);
    let n = COUNT.fetch_add(1, Ordering::Relaxed);
    (
        format!("APItest{n:08}"),
        format!("secret{n:08}0123456789abcdefghijklmnopqrstuvwxyz"),
    )
}

#[test]
fn test_agent_endpoint_grant_round_trip() {
    let (api_key, secret) = api_keypair();
    let grant = AgentEndpointGrant {
        call: true,
        agent_name: "my-agent".into(),
        deployment: "prod".into(),
    };
    let raw = AccessToken::new(&api_key, &secret)
        .set_agent_endpoint_grant(grant.clone())
        .set_valid_for(time::MINUTE)
        .to_jwt()
        .unwrap();

    // the claim key is camelCase
    assert!(decode_claims(&raw).contains(r#""agentEndpoint""#));

    let v = parse_api_token(&raw).unwrap();
    let (_, decoded) = v.verify(&secret).unwrap();
    assert_eq!(Some(grant), decoded.agent_endpoint);
}

#[test]
fn test_agent_thread_grant_round_trip() {
    let (api_key, secret) = api_keypair();
    let grant = AgentThreadGrant {
        scope: "user-1".into(),
        thread_ids: vec!["AT_1".into()],
        read: true,
        write: true,
        ..Default::default()
    };
    let raw = AccessToken::new(&api_key, &secret)
        .set_agent_thread_grant(grant.clone())
        .set_valid_for(time::MINUTE)
        .to_jwt()
        .unwrap();

    // the claim keys are camelCase
    let claims = decode_claims(&raw);
    assert!(claims.contains(r#""agentThread""#));
    assert!(claims.contains(r#""scope""#));
    assert!(claims.contains(r#""threadIds""#));

    let v = parse_api_token(&raw).unwrap();
    let (_, decoded) = v.verify(&secret).unwrap();
    assert_eq!(Some(grant), decoded.agent_thread);
}

fn decode_claims(raw: &str) -> String {
    let parts: Vec<&str> = raw.split('.').collect();
    assert_eq!(3, parts.len());
    let body = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();
    String::from_utf8(body).unwrap()
}

// The key and the secret of the tokens that go between this code and the Go original. They
// are made up for these tests.
const INTEROP_KEY: &str = "APImusivumTestKey";
const INTEROP_SECRET: &str = "musivum-test-secret-0123456789abcdef";

// New for musivum: for the same claims and the same times, this code signs the very token
// the Go original signs. The token below was printed by the original (golang-jwt signing the
// original's tokenClaims, as ToJWT does), so it is one the original makes, and takes.
#[test]
fn signs_the_token_the_go_implementation_signs() {
    const GO_TOKEN_FIXED: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJBUEltdXNpdnVtVGVzdEtleSIsInN1YiI6InVzZXItMSIsImV4cCI6NDEwMjQ0NDgwMCwibmJmIjoxNzAwMDAwMDAwLCJpYXQiOjE3MDAwMDAwMDAsImlkZW50aXR5IjoidXNlci0xIiwibmFtZSI6IlVzZXIgT25lIiwidmlkZW8iOnsicm9vbUpvaW4iOnRydWUsInJvb20iOiJteXJvb20iLCJjYW5QdWJsaXNoRGF0YSI6ZmFsc2V9LCJyb29tQ29uZmlnIjp7Im5hbWUiOiJteXJvb20iLCJtYXhQYXJ0aWNpcGFudHMiOjR9LCJtZXRhZGF0YSI6Im1kIiwiYXR0cmlidXRlcyI6eyJrMSI6InYxIiwiazIiOiJ2MiJ9fQ._2y62fnK3GbAVLu1f45JcooVKftUIXvUYwCZIldqGC8";

    let claims = TokenClaims {
        registered_claims: RegisteredClaims {
            issuer: INTEROP_KEY.into(),
            subject: "user-1".into(),
            issued_at: Some(NumericDate(1700000000)),
            not_before: Some(NumericDate(1700000000)),
            expires_at: Some(NumericDate(4102444800)),
            ..Default::default()
        },
        claim_grants: ClaimGrants {
            identity: "user-1".into(),
            name: "User One".into(),
            video: Some(VideoGrant {
                room_join: true,
                room: "myroom".into(),
                can_publish_data: Some(false),
                ..Default::default()
            }),
            metadata: "md".into(),
            attributes: HashMap::from([
                ("k2".to_string(), "v2".to_string()),
                ("k1".to_string(), "v1".to_string()),
            ]),
            room_config: Some(RoomConfiguration(livekit::RoomConfiguration {
                name: "myroom".into(),
                max_participants: 4,
                ..Default::default()
            })),
            ..Default::default()
        },
    };
    assert_eq!(
        r#"{"iss":"APImusivumTestKey","sub":"user-1","exp":4102444800,"nbf":1700000000,"iat":1700000000,"identity":"user-1","name":"User One","video":{"roomJoin":true,"room":"myroom","canPublishData":false},"roomConfig":{"name":"myroom","maxParticipants":4},"metadata":"md","attributes":{"k1":"v1","k2":"v2"}}"#,
        serde_json::to_string(&claims).unwrap()
    );
    assert_eq!(
        GO_TOKEN_FIXED,
        jwt::signed_string(&claims, INTEROP_SECRET.as_bytes()).unwrap()
    );
}

// New for musivum: a token with everything in it, as `to_jwt` makes it now. It is verified
// here, and printed, so that it can be given to the Go original to verify
// (`cargo test a_token_for_the_go_implementation -- --nocapture`).
#[test]
fn a_token_for_the_go_implementation() {
    let mut video = VideoGrant {
        room_join: true,
        room: "myroom".into(),
        hidden: true,
        ..Default::default()
    };
    video.set_can_publish(true);
    video.set_can_subscribe(false);
    video.set_can_publish_sources(&[TrackSource::Camera, TrackSource::ScreenShareAudio]);
    let room_config = livekit::RoomConfiguration {
        name: "myroom".into(),
        empty_timeout: 300,
        max_participants: 4,
        sync_streams: true,
        agents: vec![livekit::RoomAgentDispatch {
            agent_name: "agent1".into(),
            metadata: "metadata1".into(),
            ..Default::default()
        }],
        egress: Some(livekit::RoomEgress {
            room: Some(livekit::RoomCompositeEgressRequest {
                file_outputs: vec![livekit::EncodedFileOutput {
                    disable_manifest: true,
                    output: Some(encoded_file_output::Output::S3(livekit::S3Upload {
                        bucket: "bucket".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let attributes = HashMap::from([
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string()),
    ]);
    let at = AccessToken::new(INTEROP_KEY, INTEROP_SECRET)
        .set_identity("user-1")
        .set_name("User One")
        .set_kind(Kind::Agent)
        .set_kind_detail(&[KindDetail::CloudAgent, KindDetail::Forwarded])
        .set_video_grant(video.clone())
        .set_sip_grant(SipGrant {
            admin: true,
            call: true,
        })
        .set_agent_grant(AgentGrant {
            admin: true,
            dispatch_admin: true,
            ..Default::default()
        })
        .set_inference_grant(InferenceGrant { perform: true })
        .set_observability_grant(ObservabilityGrant { write: true })
        .set_agent_endpoint_grant(AgentEndpointGrant {
            call: true,
            agent_name: "a".into(),
            deployment: "prod".into(),
        })
        .set_agent_thread_grant(AgentThreadGrant {
            scope: "u1".into(),
            thread_ids: vec!["AT_1".into()],
            read: true,
            ..Default::default()
        })
        .set_metadata(r#"{"a":"<b>&c"}"#)
        .set_attributes(&attributes)
        .set_sha256("abc123")
        .set_room_preset("preset-1")
        .set_room_config(room_config.clone())
        .set_valid_for(time::HOUR * (100 * 365 * 24));
    let token = at.to_jwt().unwrap();
    println!("RUST_TOKEN_FULL {token}");

    // What this code writes differs from what the original writes for these grants only in
    // the times and in "<", ">" and "&", which Go writes as \u escapes (003c, 003e, 0026).
    let claims = decode_claims(&token);
    let (times, grants) = claims.split_once(r#","identity":"#).unwrap();
    assert!(times.starts_with(r#"{"iss":"APImusivumTestKey","sub":"user-1","exp":"#));
    assert_eq!(
        concat!(
            r#""user-1","name":"User One","kind":"agent","kindDetails":["cloud_agent","forwarded"],"#,
            r#""video":{"roomJoin":true,"room":"myroom","canPublish":true,"canSubscribe":false,"#,
            r#""canPublishSources":["camera","screen_share_audio"],"hidden":true},"#,
            r#""sip":{"admin":true,"call":true},"agent":{"admin":true,"dispatchAdmin":true},"#,
            r#""inference":{"perform":true},"observability":{"write":true},"#,
            r#""agentEndpoint":{"call":true,"agentName":"a","deployment":"prod"},"#,
            r#""agentThread":{"scope":"u1","threadIds":["AT_1"],"read":true},"#,
            r#""roomConfig":{"name":"myroom","emptyTimeout":300,"maxParticipants":4,"#,
            r#""egress":{"room":{"fileOutputs":[{"disableManifest":true,"s3":{"bucket":"bucket"}}]}},"#,
            r#""syncStreams":true,"agents":[{"agentName":"agent1","metadata":"metadata1"}]},"#,
            r#""roomPreset":"preset-1","sha256":"abc123","metadata":"{\"a\":\"<b>&c\"}","#,
            r#""attributes":{"k1":"v1","k2":"v2"}}"#,
        ),
        grants
    );

    let v = parse_api_token(&token).unwrap();
    assert_eq!(INTEROP_KEY, v.api_key());
    assert_eq!("user-1", v.identity());
    let (registered, grants) = v.verify(INTEROP_SECRET).unwrap();
    assert_eq!(at.get_grants(), &grants);
    assert_eq!(Some(video), grants.video);
    assert_eq!(Some(&room_config), grants.get_room_configuration());
    assert_eq!(attributes, grants.attributes);

    // Issued now, good from now, for a hundred years.
    let now = jwt::time_now().unix_secs();
    let issued_at = registered.issued_at.unwrap().unix();
    assert!((now - 5..=now).contains(&issued_at), "{issued_at} at {now}");
    assert_eq!(
        Some(issued_at),
        registered.not_before.map(NumericDate::unix)
    );
    assert_eq!(
        Some(issued_at + 100 * 365 * 24 * 3600),
        registered.expires_at.map(NumericDate::unix)
    );

    // Two more for the original to verify. The least token: no identity, and a video grant
    // with nothing in it.
    let token = AccessToken::new(INTEROP_KEY, INTEROP_SECRET)
        .set_video_grant(VideoGrant::default())
        .to_jwt()
        .unwrap();
    println!("RUST_TOKEN_MIN {token}");

    // And one with text that JSON can write in more than one way (Go writes "<", ">", "&"
    // and the two separators as \u escapes, this code as they are), and with a room
    // configuration whose fields this code writes in another order than the original (what
    // is one of several comes last here, and the tags come in any order).
    let attributes = HashMap::from([
        (INTEROP_TEXT.to_string(), INTEROP_TEXT.to_string()),
        ("b".to_string(), "2".to_string()),
        ("a".to_string(), "1".to_string()),
    ]);
    let room_config = livekit::RoomConfiguration {
        name: INTEROP_TEXT.into(),
        tags: attributes.clone(),
        egress: Some(livekit::RoomEgress {
            room: Some(livekit::RoomCompositeEgressRequest {
                room_name: "r".into(),
                options: Some(livekit::room_composite_egress_request::Options::Preset(
                    livekit::EncodingOptionsPreset::H2641080p30 as i32,
                )),
                file_outputs: vec![livekit::EncodedFileOutput {
                    file_type: livekit::EncodedFileType::Mp4 as i32,
                    output: Some(encoded_file_output::Output::Gcp(livekit::GcpUpload {
                        bucket: "bucket".into(),
                        ..Default::default()
                    })),
                    filepath: "path".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let at = AccessToken::new(INTEROP_KEY, INTEROP_SECRET)
        .set_identity(INTEROP_TEXT)
        .set_name(INTEROP_TEXT)
        .set_metadata(INTEROP_TEXT)
        .set_attributes(&attributes)
        .set_room_config(room_config.clone());
    let token = at.to_jwt().unwrap();
    println!("RUST_TOKEN_TEXT {token}");
    let v = parse_api_token(&token).unwrap();
    assert_eq!(INTEROP_TEXT, v.identity());
    let (_, grants) = v.verify(INTEROP_SECRET).unwrap();
    assert_eq!(at.get_grants(), &grants);
    assert_eq!(INTEROP_TEXT, grants.name);
    assert_eq!(Some(&room_config), grants.get_room_configuration());
}

// Text with every character that JSON has a short escape for, some that it has none for, and
// some that Go's JSON writer escapes although it need not.
const INTEROP_TEXT: &str =
    "<a href='x'>&</a>\u{2028}\u{2029}\0\u{1f}\"\\/\u{8}\u{c}\n\r\t\u{7f}\u{e9}\u{1f600}";

// New for musivum: how long a token is good for, and what a token made of nothing has in it.
#[test]
fn validity_and_the_least_token() {
    let token = AccessToken::new(INTEROP_KEY, INTEROP_SECRET)
        .set_video_grant(VideoGrant::default())
        .to_jwt()
        .unwrap();
    assert!(token.starts_with("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."));
    let claims: serde_json::Value = serde_json::from_str(&decode_claims(&token)).unwrap();
    let claims = claims.as_object().unwrap();
    // No subject and no identity when none was set, and nothing of the grants but the one
    // that was.
    let mut names: Vec<&str> = claims.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(vec!["exp", "iat", "iss", "nbf", "video"], names);
    assert_eq!(INTEROP_KEY, claims["iss"]);
    assert_eq!(serde_json::json!({}), claims["video"]);
    // Whole seconds; six hours when nothing else is said.
    let iat = claims["iat"].as_i64().unwrap();
    assert_eq!(Some(iat), claims["nbf"].as_i64());
    assert_eq!(Some(iat + 6 * 3600), claims["exp"].as_i64());

    let exp_after = |valid_for: Duration| {
        let token = AccessToken::new(INTEROP_KEY, INTEROP_SECRET)
            .set_valid_for(valid_for)
            .to_jwt()
            .unwrap();
        let claims = jwt::parse_unverified::<RegisteredClaims>(&token)
            .unwrap()
            .claims;
        claims.expires_at.unwrap().unix() - claims.issued_at.unwrap().unix()
    };
    assert_eq!(90, exp_after(time::SECOND * 90));
    // A time that is not more than nothing is no time set.
    assert_eq!(6 * 3600, exp_after(Duration::ZERO));
    assert_eq!(6 * 3600, exp_after(-time::HOUR));
    // The longest time there is, which ends after the last moment a `Time` can be.
    assert!((9223372036..=9223372037).contains(&exp_after(Duration::MAX)));

    // Only a secret and a key together sign a token.
    assert_eq!(
        Err(Error::KeysMissing),
        AccessToken::new(INTEROP_KEY, "").to_jwt()
    );
    assert_eq!(
        Err(Error::KeysMissing),
        AccessToken::new("", INTEROP_SECRET).to_jwt()
    );
}

// New for musivum: the setters the original's tests do not call.
#[test]
fn the_other_setters() {
    let attributes = HashMap::from([("a".to_string(), "1".to_string())]);
    let more = HashMap::from([
        ("a".to_string(), "2".to_string()),
        ("b".to_string(), "3".to_string()),
    ]);
    let at = AccessToken::new(INTEROP_KEY, INTEROP_SECRET)
        .set_name("name")
        .set_metadata("metadata")
        .set_sha256("sha")
        .set_room_preset("preset")
        .set_kind_detail(&[KindDetail::Forwarded])
        .set_attributes(&attributes)
        .set_attributes(&HashMap::new())
        .set_attributes(&more)
        .set_agents(vec![livekit::RoomAgentDispatch {
            agent_name: "agent".into(),
            ..Default::default()
        }])
        .add_grant(VideoGrant {
            room_list: true,
            ..Default::default()
        })
        .add_sip_grant(SipGrant {
            call: true,
            ..Default::default()
        });
    let grants = at.get_grants();
    assert_eq!("name", grants.name);
    assert_eq!("metadata", grants.metadata);
    assert_eq!("sha", grants.sha256);
    assert_eq!("preset", grants.room_preset);
    assert_eq!(vec![KindDetail::Forwarded], grants.get_kind_details());
    // Attributes are added to those that are there.
    assert_eq!(more, grants.attributes);
    assert!(grants.video.as_ref().unwrap().room_list);
    assert!(grants.sip.as_ref().unwrap().call);
    // The agents made a room configuration to be in.
    let config = grants.get_room_configuration().unwrap();
    assert_eq!("agent", config.agents[0].agent_name);

    // Agents are set in the room configuration that is there, and nil takes a grant or the
    // configuration away.
    let at = at
        .set_room_config(livekit::RoomConfiguration {
            name: "room".into(),
            ..Default::default()
        })
        .set_agents(vec![]);
    let config = at.get_grants().get_room_configuration().unwrap();
    assert_eq!("room", config.name);
    assert!(config.agents.is_empty());
    let at = at
        .set_room_config(None)
        .set_video_grant(None)
        .set_sip_grant(None);
    assert_eq!(None, at.get_grants().get_room_configuration());
    assert_eq!(None, at.get_grants().video);
    assert_eq!(None, at.get_grants().sip);
}
