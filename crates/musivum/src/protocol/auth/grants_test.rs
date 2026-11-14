// Ported from LiveKit's protocol, auth/grants_test.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Where the original asserts that two nil pointers are
// the same, this asserts that both are `None`; where it asserts that two pointers are not
// the same, that a change to the clone leaves the original as it was. A nil grant is a
// `None`. The tests at the end are new and say so.

use super::*;
use crate::protocol::livekit::participant_info::{Kind, KindDetail};

#[test]
fn test_grants() {
    // clone default grant
    {
        let grants = ClaimGrants::default();
        let clone = grants.clone();
        assert!(grants.video.is_none() && clone.video.is_none());
        assert!(grants.agent.is_none() && clone.agent.is_none());
        assert!(grants.inference.is_none() && clone.inference.is_none());
        assert!(grants.sip.is_none() && clone.sip.is_none());
        assert!(grants.agent_endpoint.is_none() && clone.agent_endpoint.is_none());
        assert!(grants.agent_thread.is_none() && clone.agent_thread.is_none());
        assert_eq!(grants, clone, "clone default grant");
        assert_eq!(grants.video, clone.video, "clone default grant");
    }

    // clone nil video
    {
        let grants = ClaimGrants {
            identity: "identity".into(),
            name: "name".into(),
            sha256: "sha256".into(),
            metadata: "metadata".into(),
            ..Default::default()
        };
        let clone = grants.clone();
        assert!(grants.video.is_none() && clone.video.is_none());
        assert_eq!(grants, clone, "clone nil video");
        assert_eq!(grants.video, clone.video, "clone nil video");

        // require SIP
        assert!(grants.sip.is_none() && clone.sip.is_none());
        assert_eq!(grants.sip, clone.sip);
        // require Agent
        assert!(grants.agent.is_none() && clone.agent.is_none());
        assert_eq!(grants.agent, clone.agent);
        // require Inference
        assert!(grants.inference.is_none() && clone.inference.is_none());
        assert_eq!(grants.inference, clone.inference);
        // require AgentEndpoint
        assert!(grants.agent_endpoint.is_none() && clone.agent_endpoint.is_none());
        assert_eq!(grants.agent_endpoint, clone.agent_endpoint);
        // require AgentThread
        assert!(grants.agent_thread.is_none() && clone.agent_thread.is_none());
        assert_eq!(grants.agent_thread, clone.agent_thread);
    }

    // clone with video
    {
        let tr = true;
        let fa = false;
        let video = VideoGrant {
            room_create: true,
            room_list: false,
            room_record: true,
            room_admin: false,
            room_join: true,
            room: "room".into(),
            can_publish: Some(tr),
            can_subscribe: Some(fa),
            can_publish_data: None,
            hidden: true,
            recorder: false,
            can_subscribe_metrics: Some(tr),
            ..Default::default()
        };
        let grants = ClaimGrants {
            identity: "identity".into(),
            name: "name".into(),
            kind: "kind".into(),
            video: Some(video),
            sha256: "sha256".into(),
            metadata: "metadata".into(),
            ..Default::default()
        };
        let mut clone = grants.clone();
        assert!(grants.video.is_some() && clone.video.is_some());
        assert!(
            grants.video.as_ref().unwrap().can_publish_data.is_none()
                && clone.video.as_ref().unwrap().can_publish_data.is_none()
        );
        assert_eq!(grants, clone, "clone with video");
        assert_eq!(grants.video, clone.video, "clone with video");

        // The clone's video grant and what it points to are its own.
        let clone_video = clone.video.as_mut().unwrap();
        clone_video.can_publish = Some(false);
        clone_video.can_subscribe = Some(true);
        clone_video.room = "another".into();
        let video = grants.video.as_ref().unwrap();
        assert_eq!(Some(true), video.can_publish);
        assert_eq!(Some(false), video.can_subscribe);
        assert_eq!("room", video.room);
    }

    // clone with SIP
    {
        let sip = SipGrant {
            admin: true,
            ..Default::default()
        };
        let grants = ClaimGrants {
            identity: "identity".into(),
            name: "name".into(),
            kind: "kind".into(),
            sip: Some(sip),
            sha256: "sha256".into(),
            metadata: "metadata".into(),
            ..Default::default()
        };
        let mut clone = grants.clone();
        assert_eq!(
            grants.sip.as_ref().unwrap().admin,
            clone.sip.as_ref().unwrap().admin
        );
        assert_eq!(grants, clone, "clone with SIP");
        assert_eq!(grants.sip, clone.sip, "clone with SIP");

        clone.sip.as_mut().unwrap().admin = false;
        assert!(grants.sip.as_ref().unwrap().admin);
    }

    // clone with Agent
    {
        let agent = AgentGrant {
            admin: true,
            dispatch_admin: true,
            ..Default::default()
        };
        let grants = ClaimGrants {
            identity: "identity".into(),
            name: "name".into(),
            kind: "kind".into(),
            agent: Some(agent),
            sha256: "sha256".into(),
            metadata: "metadata".into(),
            ..Default::default()
        };
        let mut clone = grants.clone();
        assert_eq!(
            grants.agent.as_ref().unwrap().admin,
            clone.agent.as_ref().unwrap().admin
        );
        assert_eq!(grants, clone, "clone with Agent");
        assert_eq!(grants.agent, clone.agent, "clone with Agent");

        clone.agent.as_mut().unwrap().admin = false;
        assert!(grants.agent.as_ref().unwrap().admin);
    }

    // clone with Inference
    {
        let inference = InferenceGrant { perform: true };
        let grants = ClaimGrants {
            identity: "identity".into(),
            name: "name".into(),
            kind: "kind".into(),
            inference: Some(inference),
            sha256: "sha256".into(),
            metadata: "metadata".into(),
            ..Default::default()
        };
        let mut clone = grants.clone();
        assert_eq!(
            grants.inference.as_ref().unwrap().perform,
            clone.inference.as_ref().unwrap().perform
        );
        assert_eq!(grants, clone, "clone with Inference");
        assert_eq!(grants.inference, clone.inference, "clone with Inference");

        clone.inference.as_mut().unwrap().perform = false;
        assert!(grants.inference.as_ref().unwrap().perform);
    }
}

#[test]
fn test_participant_kind() {
    let (kind_min, kind_max) = (Kind::Standard, Kind::Agent);
    for k in kind_min as i32..=kind_max as i32 {
        let k = Kind::try_from(k).unwrap();
        assert_eq!(k, kind_to_proto(&kind_from_proto(k)), "{}", k.as_str_name());
    }
    let kind_next = kind_max as i32 + 1;
    assert!(
        Kind::try_from(kind_next).is_err(),
        "Please update kindMax to match protobuf. Missing value: {kind_next}"
    );
}

#[test]
fn test_participant_kind_detail() {
    let (detail_min, detail_max) = (KindDetail::CloudAgent, KindDetail::ConnectorTwilio);
    let mut details = Vec::new();
    for k in detail_min as i32..=detail_max as i32 {
        details.push(KindDetail::try_from(k).unwrap());
    }

    assert_eq!(
        details,
        kind_details_to_proto(&kind_details_from_proto(&details))
    );
}

#[test]
fn test_room_configuration_check_credentials() {
    use livekit::{
        AliOssUpload, AutoParticipantEgress, AutoTrackEgress, AzureBlobUpload, EncodedFileOutput,
        GcpUpload, ImageOutput, RoomCompositeEgressRequest, RoomEgress, S3Upload,
        SegmentedFileOutput, StreamOutput, StreamProtocol, auto_track_egress, encoded_file_output,
        image_output, segmented_file_output,
    };

    fn config(egress: RoomEgress) -> RoomConfiguration {
        RoomConfiguration(livekit::RoomConfiguration {
            egress: Some(egress),
            ..Default::default()
        })
    }

    // nil egress returns nil
    {
        let config = RoomConfiguration::default();
        assert_eq!(Ok(()), config.check_credentials());
    }

    // empty egress returns nil
    {
        let config = config(RoomEgress::default());
        assert_eq!(Ok(()), config.check_credentials());
    }

    // participant file output with S3 secret fails
    {
        let config = config(RoomEgress {
            participant: Some(AutoParticipantEgress {
                file_outputs: vec![EncodedFileOutput {
                    output: Some(encoded_file_output::Output::S3(S3Upload {
                        access_key: "access".into(),
                        secret: "secret".into(), // This should trigger error
                        bucket: "bucket".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Err(Error::SensitiveCredentials),
            config.check_credentials(),
            "participant file output with S3 secret fails"
        );
    }

    // participant file output with S3 but no secret passes
    {
        let config = config(RoomEgress {
            participant: Some(AutoParticipantEgress {
                file_outputs: vec![EncodedFileOutput {
                    output: Some(encoded_file_output::Output::S3(S3Upload {
                        access_key: "access".into(),
                        secret: "".into(), // No secret
                        bucket: "bucket".into(),
                        region: "us-west-2".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Ok(()),
            config.check_credentials(),
            "participant file output with S3 but no secret passes"
        );
    }

    // participant segment output with GCP credentials fails
    {
        let config = config(RoomEgress {
            participant: Some(AutoParticipantEgress {
                segment_outputs: vec![SegmentedFileOutput {
                    output: Some(segmented_file_output::Output::Gcp(GcpUpload {
                        credentials: "credentials".into(), // This should trigger error
                        bucket: "bucket".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Err(Error::SensitiveCredentials),
            config.check_credentials(),
            "participant segment output with GCP credentials fails"
        );
    }

    // participant segment output with GCP but no credentials passes
    {
        let config = config(RoomEgress {
            participant: Some(AutoParticipantEgress {
                segment_outputs: vec![SegmentedFileOutput {
                    output: Some(segmented_file_output::Output::Gcp(GcpUpload {
                        credentials: "".into(), // No credentials
                        bucket: "bucket".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Ok(()),
            config.check_credentials(),
            "participant segment output with GCP but no credentials passes"
        );
    }

    // room file output with Azure account key fails
    {
        let config = config(RoomEgress {
            room: Some(RoomCompositeEgressRequest {
                file_outputs: vec![EncodedFileOutput {
                    output: Some(encoded_file_output::Output::Azure(AzureBlobUpload {
                        account_name: "account".into(),
                        account_key: "key".into(), // This should trigger error
                        container_name: "container".into(),
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Err(Error::SensitiveCredentials),
            config.check_credentials(),
            "room file output with Azure account key fails"
        );
    }

    // room segment output with AliOSS secret fails
    {
        let config = config(RoomEgress {
            room: Some(RoomCompositeEgressRequest {
                segment_outputs: vec![SegmentedFileOutput {
                    output: Some(segmented_file_output::Output::AliOss(AliOssUpload {
                        access_key: "access".into(),
                        secret: "secret".into(), // This should trigger error
                        bucket: "bucket".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Err(Error::SensitiveCredentials),
            config.check_credentials(),
            "room segment output with AliOSS secret fails"
        );
    }

    // room image output with valid config passes
    {
        let config = config(RoomEgress {
            room: Some(RoomCompositeEgressRequest {
                image_outputs: vec![ImageOutput {
                    capture_interval: 5,
                    width: 1920,
                    height: 1080,
                    output: Some(image_output::Output::S3(S3Upload {
                        access_key: "access".into(),
                        secret: "".into(), // No secret
                        bucket: "bucket".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Ok(()),
            config.check_credentials(),
            "room image output with valid config passes"
        );
    }

    // room stream outputs always fail
    {
        let config = config(RoomEgress {
            room: Some(RoomCompositeEgressRequest {
                stream_outputs: vec![StreamOutput {
                    protocol: StreamProtocol::Rtmp as i32,
                    urls: vec!["rtmp://example.com/live".into()],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Err(Error::SensitiveCredentials),
            config.check_credentials(),
            "room stream outputs always fail"
        );
    }

    // tracks output with S3 secret fails
    {
        let config = config(RoomEgress {
            tracks: Some(AutoTrackEgress {
                filepath: "output.mp4".into(),
                output: Some(auto_track_egress::Output::S3(S3Upload {
                    access_key: "access".into(),
                    secret: "secret".into(), // This should trigger error
                    bucket: "bucket".into(),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Err(Error::SensitiveCredentials),
            config.check_credentials(),
            "tracks output with S3 secret fails"
        );
    }

    // tracks output without credentials passes
    {
        let config = config(RoomEgress {
            tracks: Some(AutoTrackEgress {
                filepath: "output.mp4".into(),
                output: Some(auto_track_egress::Output::Gcp(GcpUpload {
                    credentials: "".into(), // No credentials
                    bucket: "bucket".into(),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Ok(()),
            config.check_credentials(),
            "tracks output without credentials passes"
        );
    }

    // multiple outputs with mixed credentials
    {
        let config = config(RoomEgress {
            participant: Some(AutoParticipantEgress {
                file_outputs: vec![
                    EncodedFileOutput {
                        output: Some(encoded_file_output::Output::S3(S3Upload {
                            access_key: "access".into(),
                            secret: "".into(), // No secret - OK
                            bucket: "bucket1".into(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                    EncodedFileOutput {
                        output: Some(encoded_file_output::Output::Gcp(GcpUpload {
                            credentials: "credentials".into(), // Has credentials - should fail
                            bucket: "bucket2".into(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Err(Error::SensitiveCredentials),
            config.check_credentials(),
            "multiple outputs with mixed credentials"
        );
    }

    // all cloud providers without credentials pass
    {
        let config = config(RoomEgress {
            room: Some(RoomCompositeEgressRequest {
                file_outputs: vec![
                    EncodedFileOutput {
                        output: Some(encoded_file_output::Output::S3(S3Upload {
                            access_key: "access".into(),
                            secret: "".into(), // No secret
                            bucket: "s3bucket".into(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                    EncodedFileOutput {
                        output: Some(encoded_file_output::Output::Gcp(GcpUpload {
                            credentials: "".into(), // No credentials
                            bucket: "gcpbucket".into(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                    EncodedFileOutput {
                        output: Some(encoded_file_output::Output::Azure(AzureBlobUpload {
                            account_name: "account".into(),
                            account_key: "".into(), // No key
                            container_name: "container".into(),
                        })),
                        ..Default::default()
                    },
                    EncodedFileOutput {
                        output: Some(encoded_file_output::Output::AliOss(AliOssUpload {
                            access_key: "access".into(),
                            secret: "".into(), // No secret
                            bucket: "alibucket".into(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            Ok(()),
            config.check_credentials(),
            "all cloud providers without credentials pass"
        );
    }
}

#[test]
fn test_agent_endpoint_grant_allows() {
    struct Case {
        name: &'static str,
        grant: Option<AgentEndpointGrant>,
        agent_name: &'static str,
        deployment: &'static str,
        want: bool,
    }
    let grant = |call: bool, agent_name: &str, deployment: &str| {
        Some(AgentEndpointGrant {
            call,
            agent_name: agent_name.into(),
            deployment: deployment.into(),
        })
    };
    let case = |name, grant, agent_name, deployment, want| Case {
        name,
        grant,
        agent_name,
        deployment,
        want,
    };
    let cases = [
        case("nil grant", None, "a", "prod", false),
        case(
            "call unset with scope",
            grant(false, "a", "prod"),
            "a",
            "prod",
            false,
        ),
        case("wildcard", grant(true, "", ""), "a", "prod", true),
        case("agent match", grant(true, "a", ""), "a", "prod", true),
        case("agent mismatch", grant(true, "a", ""), "b", "prod", false),
        case(
            "deployment match",
            grant(true, "", "prod"),
            "a",
            "prod",
            true,
        ),
        case(
            "deployment mismatch",
            grant(true, "", "prod"),
            "a",
            "staging",
            false,
        ),
        case("both match", grant(true, "a", "prod"), "a", "prod", true),
        case(
            "both set, deployment differs",
            grant(true, "a", "prod"),
            "a",
            "staging",
            false,
        ),
        case(
            "agent case differs",
            grant(true, "Agent", ""),
            "agent",
            "prod",
            false,
        ),
        case(
            "deployment case differs",
            grant(true, "", "Prod"),
            "a",
            "prod",
            false,
        ),
        case(
            "empty deployment is wildcard",
            grant(true, "", ""),
            "a",
            "production",
            true,
        ),
        case(
            "default pinned by literal",
            grant(true, "", "default"),
            "a",
            "default",
            true,
        ),
        case(
            "default pin rejects others",
            grant(true, "", "default"),
            "a",
            "prod",
            false,
        ),
    ];
    for c in cases {
        // A grant that is not there allows nothing.
        let allows = c
            .grant
            .as_ref()
            .is_some_and(|grant| grant.allows(c.agent_name, c.deployment));
        assert_eq!(c.want, allows, "{}", c.name);
    }
}

#[test]
fn test_agent_endpoint_grant_clone_independent() {
    let grants = ClaimGrants {
        agent_endpoint: Some(AgentEndpointGrant {
            call: true,
            agent_name: "a".into(),
            deployment: "prod".into(),
        }),
        ..Default::default()
    };
    let mut clone = grants.clone();

    let clone_endpoint = clone.agent_endpoint.as_mut().unwrap();
    clone_endpoint.call = false;
    clone_endpoint.agent_name = "b".into();
    assert!(grants.agent_endpoint.as_ref().unwrap().call);
    assert_eq!("a", grants.agent_endpoint.as_ref().unwrap().agent_name);
}

#[test]
fn test_agent_thread_grant_checks() {
    // Scope covers list and create only; read, write and delete need the id.
    let g = AgentThreadGrant {
        scope: "u1".into(),
        thread_ids: vec!["AT_1".into()],
        list: true,
        create: true,
        read: true,
        ..Default::default()
    };
    assert!(g.can_read("AT_1"));
    assert!(!g.can_read("AT_2"));
    assert!(!g.can_read(""));
    assert!(!g.can_write("AT_1"));
    assert!(!g.can_delete("AT_1"));
    assert!(g.can_list("u1"));
    assert!(!g.can_list("u2"));
    assert!(!g.can_list(""));
    assert!(g.can_create("u1"));
    assert!(!g.can_create("u2"));
    assert!(!g.can_update());

    let ids_only = AgentThreadGrant {
        thread_ids: vec!["AT_4".into()],
        create: true,
        write: true,
        ..Default::default()
    };
    assert!(ids_only.can_write("AT_4"));
    assert!(!ids_only.can_create(""));

    // All covers every thread and scope, still gated by the action flags.
    let all = AgentThreadGrant {
        all: true,
        read: true,
        list: true,
        ..Default::default()
    };
    assert!(all.can_read("AT_9"));
    assert!(all.can_list("any"));
    assert!(!all.can_read(""));
    assert!(!all.can_delete("AT_9"));
    assert!(!all.can_create("any"));
    assert!(!all.can_update());
    assert!(
        AgentThreadGrant {
            all: true,
            write: true,
            ..Default::default()
        }
        .can_update()
    );

    // A grant that is not there allows nothing.
    let none: Option<AgentThreadGrant> = None;
    assert!(!none.as_ref().is_some_and(|g| g.can_read("AT_1")));
    assert!(!none.as_ref().is_some_and(|g| g.can_create("u1")));
    assert!(!none.as_ref().is_some_and(|g| g.can_update()));
}

#[test]
fn test_agent_thread_grant_clone_independent() {
    let grants = ClaimGrants {
        agent_thread: Some(AgentThreadGrant {
            scope: "u1".into(),
            thread_ids: vec!["AT_1".into()],
            write: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut clone = grants.clone();
    assert_eq!(grants.agent_thread, clone.agent_thread);

    let clone_thread = clone.agent_thread.as_mut().unwrap();
    clone_thread.write = false;
    clone_thread.thread_ids[0] = "AT_2".into();
    clone_thread.thread_ids.push("AT_3".into());
    assert!(grants.agent_thread.as_ref().unwrap().write);
    assert_eq!(
        vec!["AT_1".to_string()],
        grants.agent_thread.as_ref().unwrap().thread_ids
    );
}

// New for musivum: what the original's test of the thread grant leaves open. It never asks
// whether a grant that may write, but not everywhere, may update (a mutation of `can_update`
// that drops `all` passes it), nor about write and delete with an id that is not listed.
#[test]
fn a_thread_grant_updates_only_with_all() {
    let ids_only = AgentThreadGrant {
        scope: "u1".into(),
        thread_ids: vec!["AT_4".into()],
        write: true,
        delete: true,
        ..Default::default()
    };
    assert!(!ids_only.can_update());
    assert!(ids_only.can_write("AT_4"));
    assert!(ids_only.can_delete("AT_4"));
    assert!(!ids_only.can_write("AT_5"));
    assert!(!ids_only.can_delete("AT_5"));
    assert!(!ids_only.can_delete(""));
    assert!(!ids_only.can_read("AT_4"));
    assert!(!ids_only.can_list("u1"));
    assert!(!ids_only.can_create("u1"));

    let all = AgentThreadGrant {
        all: true,
        write: true,
        delete: true,
        create: true,
        ..Default::default()
    };
    assert!(all.can_update());
    assert!(all.can_delete("AT_9"));
    assert!(all.can_create(""));
    assert!(!all.can_delete(""));
    let all_without_write = AgentThreadGrant {
        all: true,
        read: true,
        ..Default::default()
    };
    assert!(!all_without_write.can_update());
}

// New for musivum: the kinds the original's test stops short of, and what is made of kinds,
// details and sources that have no name. The expected values are what the Go original gives
// for the same input.
#[test]
fn kinds_and_sources_without_a_name() {
    let mut g = ClaimGrants::default();
    for (kind, name) in [
        (Kind::Standard, "standard"),
        (Kind::Ingress, "ingress"),
        (Kind::Egress, "egress"),
        (Kind::Sip, "sip"),
        (Kind::Agent, "agent"),
        (Kind::Connector, "connector"),
        (Kind::Bridge, "bridge"),
    ] {
        g.set_participant_kind(kind);
        assert_eq!(name, g.kind);
        assert_eq!(kind, g.get_participant_kind());
    }
    // A number that is no kind is written as the number, and read as the standard kind.
    g.set_participant_kind(5);
    assert_eq!("5", g.kind);
    assert_eq!(Kind::Standard, g.get_participant_kind());

    for (kind, want) in [
        ("AGENT", Kind::Agent),
        ("Agent", Kind::Agent),
        ("agent", Kind::Agent),
        ("", Kind::Standard),
        ("4", Kind::Standard),
        ("nonsense", Kind::Standard),
        // Go puts one character in upper case at a time: "ß" stays, and is not "SS".
        ("ingre\u{df}", Kind::Standard),
        ("\u{17f}ip", Kind::Sip),
        ("s\u{131}p", Kind::Sip),
    ] {
        g.kind = kind.into();
        assert_eq!(want, g.get_participant_kind(), "{kind:?}");
    }

    g.set_kind_detail(&[99, KindDetail::Simulation as i32]);
    assert_eq!(vec!["99", "simulation"], g.kind_details);
    assert_eq!(vec![KindDetail::Simulation], g.get_kind_details());
    g.set_kind_detail(&[KindDetail::BridgeRtsp, KindDetail::ConnectorWhatsapp]);
    assert_eq!(vec!["bridge_rtsp", "connector_whatsapp"], g.kind_details);

    let mut v = VideoGrant::default();
    v.set_can_publish_sources(&[TrackSource::Camera as i32, 17]);
    assert_eq!(vec!["camera", "17"], v.can_publish_sources);
    assert_eq!(
        vec![TrackSource::Camera, TrackSource::Unknown],
        v.get_can_publish_sources()
    );
    assert!(v.get_can_publish_source(TrackSource::Camera));
    assert!(!v.get_can_publish_source(TrackSource::Microphone));

    // Sources that were not written by this code: the list is read in any case, a single
    // source is looked up as it is written. That is the original's behaviour.
    v.can_publish_sources = vec!["Camera".into(), "SCREEN_SHARE".into(), "nonsense".into()];
    assert_eq!(
        vec![
            TrackSource::Camera,
            TrackSource::ScreenShare,
            TrackSource::Unknown
        ],
        v.get_can_publish_sources()
    );
    assert!(!v.get_can_publish_source(TrackSource::Camera));
    assert!(!v.get_can_publish_source(TrackSource::ScreenShare));

    v.set_can_publish_sources::<TrackSource>(&[]);
    assert!(v.can_publish_sources.is_empty());
    assert!(v.get_can_publish_sources().is_empty());
    assert!(v.get_can_publish_source(TrackSource::Camera));
}

// New for musivum: what a grant allows when nothing is said, and the way to the protocol's
// permission and back. The original has no test of these.
#[test]
fn permissions_of_a_video_grant() {
    let mut v = VideoGrant::default();
    assert!(v.get_can_publish());
    assert!(v.get_can_publish_data());
    assert!(v.get_can_subscribe());
    assert!(!v.get_can_update_own_metadata());
    assert!(!v.get_can_subscribe_metrics());
    assert!(!v.get_can_manage_agent_session());
    assert!(v.get_can_publish_source(TrackSource::Camera));

    // Who may not publish may not publish data either, unless that is said.
    v.set_can_publish(false);
    assert!(!v.get_can_publish());
    assert!(!v.get_can_publish_data());
    assert!(!v.get_can_publish_source(TrackSource::Camera));
    v.set_can_publish_data(true);
    assert!(v.get_can_publish_data());

    v.set_can_publish(true);
    v.set_can_subscribe(false);
    v.set_can_publish_sources(&[TrackSource::Microphone, TrackSource::ScreenShareAudio]);
    v.set_can_update_own_metadata(true);
    v.set_can_subscribe_metrics(true);
    v.set_can_manage_agent_session(true);
    v.hidden = true;
    v.recorder = true;
    v.agent = true;
    assert!(v.get_can_publish_source(TrackSource::Microphone));
    assert!(!v.get_can_publish_source(TrackSource::Camera));

    let permission = v.to_permission();
    assert_eq!(
        livekit::ParticipantPermission {
            can_subscribe: false,
            can_publish: true,
            can_publish_data: true,
            can_publish_sources: vec![
                TrackSource::Microphone as i32,
                TrackSource::ScreenShareAudio as i32
            ],
            hidden: true,
            recorder: true,
            can_update_metadata: true,
            agent: true,
            can_subscribe_metrics: true,
            can_manage_agent_session: true,
        },
        permission
    );
    assert!(v.matches_permission(Some(&permission)));
    assert!(!v.matches_permission(None));

    // Every field of the permission counts.
    let changes: [fn(&mut livekit::ParticipantPermission); 10] = [
        |p| p.can_subscribe = true,
        |p| p.can_publish = false,
        |p| p.can_publish_data = false,
        |p| p.can_publish_sources.truncate(1),
        |p| p.hidden = false,
        |p| p.recorder = false,
        |p| p.can_update_metadata = false,
        |p| p.agent = false,
        |p| p.can_subscribe_metrics = false,
        |p| p.can_manage_agent_session = false,
    ];
    for (i, change) in changes.iter().enumerate() {
        let mut other = permission.clone();
        change(&mut other);
        assert!(!v.matches_permission(Some(&other)), "change {i}");

        let mut updated = VideoGrant::default();
        updated.update_from_permission(Some(&other));
        assert_eq!(other, updated.to_permission(), "change {i}");
        assert!(updated.matches_permission(Some(&other)), "change {i}");
    }

    // A grant that says nothing matches the permission it is turned into, and nothing
    // changes it.
    let v = VideoGrant::default();
    assert!(v.matches_permission(Some(&v.to_permission())));
    let mut updated = v.clone();
    updated.update_from_permission(None);
    assert_eq!(v, updated);
}

// New for musivum: the JSON form, which is what other servers and SDKs read. The expected
// texts are what the Go original writes for the same grants.
#[test]
fn grants_are_written_as_the_go_implementation_writes_them() {
    let json = |grants: &ClaimGrants| serde_json::to_string(grants).unwrap();
    let f = Some(false);

    assert_eq!("{}", json(&ClaimGrants::default()));
    assert_eq!(
        r#"{"video":{}}"#,
        json(&ClaimGrants {
            video: Some(VideoGrant::default()),
            ..Default::default()
        })
    );
    // A permission that is false is written; one that is not set is not.
    assert_eq!(
        r#"{"video":{"canPublish":false,"canSubscribe":false,"canPublishData":false,"canUpdateOwnMetadata":false,"canSubscribeMetrics":false,"canManageAgentSession":false}}"#,
        json(&ClaimGrants {
            video: Some(VideoGrant {
                can_publish: f,
                can_subscribe: f,
                can_publish_data: f,
                can_update_own_metadata: f,
                can_subscribe_metrics: f,
                can_manage_agent_session: f,
                ..Default::default()
            }),
            ..Default::default()
        })
    );
    // Grants that are there and empty are written, empty.
    assert_eq!(
        r#"{"sip":{},"agent":{},"inference":{},"observability":{},"agentEndpoint":{},"agentThread":{}}"#,
        json(&ClaimGrants {
            sip: Some(SipGrant::default()),
            agent: Some(AgentGrant::default()),
            inference: Some(InferenceGrant::default()),
            observability: Some(ObservabilityGrant::default()),
            agent_endpoint: Some(AgentEndpointGrant::default()),
            agent_thread: Some(AgentThreadGrant::default()),
            ..Default::default()
        })
    );
    assert_eq!(
        r#"{"roomConfig":{}}"#,
        json(&ClaimGrants {
            room_config: Some(RoomConfiguration::default()),
            ..Default::default()
        })
    );

    let all = ClaimGrants {
        identity: "i".into(),
        name: "n".into(),
        kind: "agent".into(),
        kind_details: vec!["forwarded".into()],
        video: Some(VideoGrant {
            room_create: true,
            room_list: true,
            room_record: true,
            room_admin: true,
            room_join: true,
            room: "r".into(),
            can_publish: f,
            can_subscribe: f,
            can_publish_data: f,
            can_publish_sources: vec!["camera".into()],
            can_update_own_metadata: f,
            ingress_admin: true,
            hidden: true,
            recorder: true,
            agent: true,
            can_subscribe_metrics: f,
            can_manage_agent_session: f,
            destination_room: "d".into(),
        }),
        sip: Some(SipGrant {
            admin: true,
            call: true,
        }),
        agent: Some(AgentGrant {
            admin: true,
            simulation_admin: true,
            database_admin: true,
            dispatch_admin: true,
        }),
        inference: Some(InferenceGrant { perform: true }),
        observability: Some(ObservabilityGrant { write: true }),
        agent_endpoint: Some(AgentEndpointGrant {
            call: true,
            agent_name: "a".into(),
            deployment: "d".into(),
        }),
        agent_thread: Some(AgentThreadGrant {
            all: true,
            scope: "s".into(),
            thread_ids: vec!["t".into()],
            list: true,
            create: true,
            write: true,
            read: true,
            delete: true,
        }),
        room_config: Some(RoomConfiguration(livekit::RoomConfiguration {
            name: "r".into(),
            empty_timeout: 1,
            departure_timeout: 2,
            max_participants: 3,
            metadata: "m".into(),
            min_playout_delay: 4,
            max_playout_delay: 5,
            sync_streams: true,
            agents: vec![livekit::RoomAgentDispatch {
                agent_name: "a".into(),
                metadata: "m".into(),
                ..Default::default()
            }],
            tags: HashMap::from([("t".to_string(), "v".to_string())]),
            egress: Some(livekit::RoomEgress {
                room: Some(livekit::RoomCompositeEgressRequest {
                    room_name: "r".into(),
                    layout: "grid".into(),
                    audio_only: true,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        })),
        room_preset: "p".into(),
        sha256: "s".into(),
        metadata: "m".into(),
        attributes: HashMap::from([
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "1".to_string()),
        ]),
    };
    let written = json(&all);
    assert_eq!(
        concat!(
            r#"{"identity":"i","name":"n","kind":"agent","kindDetails":["forwarded"],"#,
            r#""video":{"roomCreate":true,"roomList":true,"roomRecord":true,"roomAdmin":true,"roomJoin":true,"room":"r","#,
            r#""canPublish":false,"canSubscribe":false,"canPublishData":false,"canPublishSources":["camera"],"#,
            r#""canUpdateOwnMetadata":false,"ingressAdmin":true,"hidden":true,"recorder":true,"agent":true,"#,
            r#""canSubscribeMetrics":false,"canManageAgentSession":false,"destinationRoom":"d"},"#,
            r#""sip":{"admin":true,"call":true},"#,
            r#""agent":{"admin":true,"simulationAdmin":true,"databaseAdmin":true,"dispatchAdmin":true},"#,
            r#""inference":{"perform":true},"observability":{"write":true},"#,
            r#""agentEndpoint":{"call":true,"agentName":"a","deployment":"d"},"#,
            r#""agentThread":{"all":true,"scope":"s","threadIds":["t"],"list":true,"create":true,"write":true,"read":true,"delete":true},"#,
            r#""roomConfig":{"name":"r","emptyTimeout":1,"departureTimeout":2,"maxParticipants":3,"metadata":"m","#,
            r#""egress":{"room":{"roomName":"r","layout":"grid","audioOnly":true}},"#,
            r#""minPlayoutDelay":4,"maxPlayoutDelay":5,"syncStreams":true,"#,
            r#""agents":[{"agentName":"a","metadata":"m"}],"tags":{"t":"v"}},"#,
            r#""roomPreset":"p","sha256":"s","metadata":"m","attributes":{"a":"1","b":"2"}}"#,
        ),
        written
    );

    // And read back, it is what was written.
    let read: ClaimGrants = json::from_slice(written.as_bytes()).unwrap();
    assert_eq!(all, read);
    // An empty grant too: it is there, not left out.
    let read: ClaimGrants = json::from_slice(br#"{"video":{},"sip":{},"roomConfig":{}}"#).unwrap();
    assert_eq!(Some(VideoGrant::default()), read.video);
    assert_eq!(Some(SipGrant::default()), read.sip);
    assert_eq!(Some(RoomConfiguration::default()), read.room_config);
    assert_eq!(None, read.agent);
}

// New for musivum: what is logged of the grants. The original leaves out the name, the
// metadata, the attributes and the hash, and logs the room configuration without what the
// protocol marks as sensitive.
#[test]
fn the_logged_form_leaves_out_what_is_private() {
    let mut video = VideoGrant {
        room_join: true,
        room: "the-room".into(),
        ..Default::default()
    };
    video.set_can_subscribe(false);
    let grants = ClaimGrants {
        identity: "the-identity".into(),
        name: "PRIVATE-name".into(),
        kind: "agent".into(),
        video: Some(video),
        sip: Some(SipGrant {
            admin: true,
            call: false,
        }),
        room_config: Some(RoomConfiguration(livekit::RoomConfiguration {
            name: "the-room".into(),
            metadata: "PRIVATE-room-metadata".into(),
            agents: vec![livekit::RoomAgentDispatch {
                agent_name: "the-agent".into(),
                metadata: "PRIVATE-agent-metadata".into(),
                ..Default::default()
            }],
            egress: Some(livekit::RoomEgress {
                tracks: Some(livekit::AutoTrackEgress {
                    filepath: "PRIVATE-path".into(),
                    output: Some(livekit::auto_track_egress::Output::S3(livekit::S3Upload {
                        access_key: "PRIVATE-access-key".into(),
                        secret: "PRIVATE-secret".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })),
        room_preset: "the-preset".into(),
        sha256: "PRIVATE-sha256".into(),
        metadata: "PRIVATE-metadata".into(),
        attributes: HashMap::from([("PRIVATE-key".to_string(), "PRIVATE-value".to_string())]),
        ..Default::default()
    };
    let logged = format!("{grants:?}");
    assert!(!logged.contains("PRIVATE"), "{logged}");
    for part in [
        r#"Identity: "the-identity""#,
        r#"Kind: "agent""#,
        r#"RoomJoin: true, Room: "the-room", CanPublish: not-set, CanSubscribe: false, CanPublishData: not-set"#,
        "SIP: Some(SipGrant { Admin: true, Call: false })",
        "Agent: None",
        r#"name: "the-room""#,
        "metadata: <redacted (21 bytes)>",
        "egress: true",
        r#"agents: ["the-agent"]"#,
        r#"RoomPreset: "the-preset""#,
    ] {
        assert!(logged.contains(part), "{part} is not in {logged}");
    }
}
