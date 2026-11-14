// Ported from LiveKit, pkg/service/roomservice_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. A context with grants is an `Extensions` with grants;
// `err.(twirp.Error)` is `Error::Twirp`. `TestAgentDispatchMetadataLimits` and its helper
// `newTestAgentDispatchService` are not ported: agents (agent_dispatch_service.go) are left out.
// The service is made with a `FakeServiceStore` that is no `OSSServiceStore`, as upstream.
// In `TestMetaDataLimits` the two services of the last part run in a fixed order (Go's map
// gives them in any).

use std::collections::HashMap;
use std::sync::Arc;

use http::Extensions;

use super::*;
use crate::config::{ApiConfig, LimitConfig};
use crate::go::time::{self, Duration};
use crate::protocol::auth::{ClaimGrants, VideoGrant};
use crate::protocol::livekit;
use crate::protocol::utils as putils;
use crate::routing::{
    FakeRouter, FakeTypedParticipantClient, FakeTypedRoomClient, MessageRouter, RequestOpts,
    new_topic_formatter,
};
use crate::service::{
    FakeRoomAllocator, FakeServiceStore, RoomAllocator, ServiceStore, with_grants,
};

fn with_grants_ctx(video: VideoGrant) -> Extensions {
    let mut ctx = Extensions::new();
    with_grants(
        &mut ctx,
        Some(ClaimGrants {
            video: Some(video),
            ..Default::default()
        }),
        "",
    );
    ctx
}

#[test]
fn test_delete_room() {
    {
        // missing permissions
        let svc = new_test_room_service(LimitConfig::default());
        let ctx = with_grants_ctx(VideoGrant::default());
        let res = svc.room_service.delete_room(
            &ctx,
            &livekit::DeleteRoomRequest {
                room: "testroom".into(),
            },
        );
        assert!(res.is_err(), "missing permissions");
    }
}

#[test]
fn test_meta_data_limits() {
    let admin_ctx = || with_grants_ctx(VideoGrant::default());
    let create_ctx = || {
        with_grants_ctx(VideoGrant {
            room_create: true,
            ..Default::default()
        })
    };
    #[track_caller]
    fn require_invalid_arg<T: std::fmt::Debug>(name: &str, res: Result<T, Error>) {
        match res {
            Err(Error::Twirp(terr)) => {
                assert_eq!(TwirpErrorCode::InvalidArgument, terr.code(), "{name}")
            }
            other => panic!("{name}: expected twirp error, got {other:?}"),
        }
    }

    {
        let name = "metadata exceeds limit";
        let svc = new_test_room_service(LimitConfig {
            max_metadata_size: 5,
            ..Default::default()
        });

        let res = svc.room_service.update_participant(
            &admin_ctx(),
            &livekit::UpdateParticipantRequest {
                room: "testroom".into(),
                identity: "123".into(),
                metadata: "abcdefg".into(),
                ..Default::default()
            },
        );
        require_invalid_arg(name, res);

        let res = svc.room_service.update_room_metadata(
            &admin_ctx(),
            &livekit::UpdateRoomMetadataRequest {
                room: "testroom".into(),
                metadata: "abcdefg".into(),
            },
        );
        require_invalid_arg(name, res);

        let res = svc.room_service.create_room(
            &create_ctx(),
            &livekit::CreateRoomRequest {
                name: "testroom".into(),
                metadata: "abcdefg".into(),
                ..Default::default()
            },
        );
        require_invalid_arg(name, res);

        let res = svc.room_service.create_room(
            &create_ctx(),
            &livekit::CreateRoomRequest {
                name: "testroom".into(),
                agents: vec![livekit::RoomAgentDispatch {
                    agent_name: "bot".into(),
                    metadata: "abcdefg".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        require_invalid_arg(name, res);
    }

    {
        let name = "embedded agent dispatch in CreateRoom exceeds attributes limit";
        let svc = new_test_room_service(LimitConfig {
            max_attributes_size: 5,
            ..Default::default()
        });
        let res = svc.room_service.create_room(
            &create_ctx(),
            &livekit::CreateRoomRequest {
                name: "testroom".into(),
                agents: vec![livekit::RoomAgentDispatch {
                    agent_name: "bot".into(),
                    attributes: HashMap::from([("key".into(), "abcdefg".into())]),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        require_invalid_arg(name, res);
    }

    let not_exceeds_limits_svc = [
        (
            "metadata exceeds limits",
            new_test_room_service(LimitConfig {
                max_metadata_size: 5,
                max_attributes_size: 5,
                ..Default::default()
            }),
        ),
        (
            "metadata no limits",
            new_test_room_service(LimitConfig::default()),
        ),
    ];

    for (n, svc) in &not_exceeds_limits_svc {
        let res = svc.room_service.update_participant(
            &admin_ctx(),
            &livekit::UpdateParticipantRequest {
                room: "testroom".into(),
                identity: "123".into(),
                metadata: "abc".into(),
                ..Default::default()
            },
        );
        match res {
            Err(Error::Twirp(terr)) => {
                assert_ne!(TwirpErrorCode::InvalidArgument, terr.code(), "{n}")
            }
            other => panic!("{n}: expected twirp error, got {other:?}"),
        }

        let res = svc.room_service.update_room_metadata(
            &admin_ctx(),
            &livekit::UpdateRoomMetadataRequest {
                room: "testroom".into(),
                metadata: "abc".into(),
            },
        );
        match res {
            Err(Error::Twirp(terr)) => {
                assert_ne!(TwirpErrorCode::InvalidArgument, terr.code(), "{n}")
            }
            other => panic!("{n}: expected twirp error, got {other:?}"),
        }

        let res = svc.room_service.create_room(
            &create_ctx(),
            &livekit::CreateRoomRequest {
                name: "testroom".into(),
                metadata: "abc".into(),
                agents: vec![livekit::RoomAgentDispatch {
                    agent_name: "bot".into(),
                    metadata: "abc".into(),
                    attributes: HashMap::from([("k".into(), "v".into())]),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        if let Err(err) = res {
            match err {
                Error::Twirp(terr) => {
                    assert_ne!(TwirpErrorCode::InvalidArgument, terr.code(), "{n}")
                }
                other => panic!("{n}: expected twirp error, got {other:?}"),
            }
        }
    }
}

// TestAgentDispatchMetadataLimits and newTestAgentDispatchService: not ported (agents are left
// out).

fn new_test_room_service(limit_conf: LimitConfig) -> TestRoomService {
    let router = Arc::new(FakeRouter::default());
    let allocator = Arc::new(FakeRoomAllocator::default());
    let store = Arc::new(FakeServiceStore::default());
    let participant_client = Arc::new(FakeTypedParticipantClient::default());
    let svc = RoomService::new(
        limit_conf,
        ApiConfig {
            execution_timeout: Duration(2),
            ..Default::default()
        },
        router.clone() as Arc<dyn MessageRouter>,
        allocator.clone() as Arc<dyn RoomAllocator>,
        store.clone() as Arc<dyn ServiceStore>,
        None,
        new_topic_formatter(),
        Arc::new(FakeTypedRoomClient::default()),
        participant_client.clone(),
    )
    .unwrap();
    TestRoomService {
        room_service: svc,
        router,
        allocator,
        store,
        participant_client,
    }
}

#[test]
fn test_perform_rpc_request_timeout() {
    let ctx = with_grants_ctx(VideoGrant {
        room_admin: true,
        room: "testroom".into(),
        ..Default::default()
    });

    let request_timeout = |svc: &TestRoomService, call: usize| -> Duration {
        let (_, _, opts) = svc.participant_client.perform_rpc_args_for_call(call);
        let mut ro = RequestOpts::default();
        for o in &opts {
            o.apply(&mut ro);
        }
        ro.timeout
    };

    {
        // default response timeout
        let svc = new_test_room_service(LimitConfig::default());
        let res = svc.room_service.perform_rpc(
            &ctx,
            &livekit::PerformRpcRequest {
                room: "testroom".into(),
                destination_identity: "p".into(),
                method: "m".into(),
                ..Default::default()
            },
        );
        assert!(res.is_ok(), "default response timeout: {res:?}");
        assert_eq!(
            putils::DATA_CHANNEL_RPC_DEFAULT_RESPONSE_TIMEOUT
                + putils::DATA_CHANNEL_RPC_MAX_ROUND_TRIP_LATENCY,
            request_timeout(&svc, 0),
            "default response timeout"
        );
    }

    {
        // requested response timeout
        let svc = new_test_room_service(LimitConfig::default());
        let res = svc.room_service.perform_rpc(
            &ctx,
            &livekit::PerformRpcRequest {
                room: "testroom".into(),
                destination_identity: "p".into(),
                method: "m".into(),
                response_timeout_ms: 500,
                ..Default::default()
            },
        );
        assert!(res.is_ok(), "requested response timeout: {res:?}");
        assert_eq!(
            500 * time::MILLISECOND + putils::DATA_CHANNEL_RPC_MAX_ROUND_TRIP_LATENCY,
            request_timeout(&svc, 0),
            "requested response timeout"
        );
    }
}

#[allow(dead_code)]
struct TestRoomService {
    room_service: Arc<RoomService>,
    router: Arc<FakeRouter>,
    allocator: Arc<FakeRoomAllocator>,
    store: Arc<FakeServiceStore>,
    participant_client: Arc<FakeTypedParticipantClient>,
}
