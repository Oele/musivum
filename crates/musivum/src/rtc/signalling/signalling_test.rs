// New for musivum: the original has no test of signalling.go.

use std::collections::HashMap;

use musivum_protocol::Message as _;

use super::*;
use crate::protocol::livekit::{leave_request, participant_info, server_info};
use crate::protocol::logger;

fn encoded(msg: Option<Message>) -> Option<String> {
    msg.map(|msg| match msg {
        Message::SignalResponse(response) => response
            .encode_to_vec()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        Message::SignalRequest(_) => panic!("a builder made a request"),
    })
}

// New for musivum. Builds some of the messages and compares them, encoded, with what the
// builders of the original make for the same arguments: the bytes are what
// `testdata/reference_output_test.go` printed, a Go test laid over the package at the pin
// (`go test -overlay`, each message marshalled with
// `proto.MarshalOptions{Deterministic: true}`), `None` where it printed nil.
#[test]
fn the_messages_are_the_ones_the_original_builds() {
    let s = Signalling::new(SignallingParams {
        logger: logger::get_logger(),
    });

    assert_eq!(
        Some(
            "0aca010a1f0a06524d5f6162631206612d726f6f6d18ac0220142880e2cfaa064802500112260a04\
             50415f311205616c69636518013081e2cfaa064a05416c69636550035a060801100118011a2c0a04\
             50415f321203626f621802221d0a0454525f7610011a0663616d65726128800a30d0053801480162\
             01312207312e31332e31312a220a1a7475726e3a7475726e2e6578616d706c652e6f72673a333437\
             381201751a016330014a0468657265500f580562111207312e31332e313118122a044e445f786a03\
             0102037801"
                .to_string()
        ),
        encoded(s.signal_join_response(livekit::JoinResponse {
            room: Some(livekit::Room {
                sid: "RM_abc".to_string(),
                name: "a-room".to_string(),
                empty_timeout: 300,
                max_participants: 20,
                creation_time: 1700000000,
                num_participants: 2,
                active_recording: true,
                ..Default::default()
            }),
            participant: Some(livekit::ParticipantInfo {
                sid: "PA_1".to_string(),
                identity: "alice".to_string(),
                state: participant_info::State::Joined as i32,
                joined_at: 1700000001,
                name: "Alice".to_string(),
                version: 3,
                permission: Some(livekit::ParticipantPermission {
                    can_subscribe: true,
                    can_publish: true,
                    can_publish_data: true,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            other_participants: vec![livekit::ParticipantInfo {
                sid: "PA_2".to_string(),
                identity: "bob".to_string(),
                state: participant_info::State::Active as i32,
                tracks: vec![livekit::TrackInfo {
                    sid: "TR_v".to_string(),
                    r#type: livekit::TrackType::Video as i32,
                    name: "camera".to_string(),
                    width: 1280,
                    height: 720,
                    simulcast: true,
                    source: livekit::TrackSource::Camera as i32,
                    mid: "1".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            server_version: "1.13.11".to_string(),
            ice_servers: vec![livekit::IceServer {
                urls: vec!["turn:turn.example.org:3478".to_string()],
                username: "u".to_string(),
                credential: "c".to_string(),
            }],
            subscriber_primary: true,
            server_region: "here".to_string(),
            ping_timeout: 15,
            ping_interval: 5,
            server_info: Some(livekit::ServerInfo {
                edition: server_info::Edition::Standard as i32,
                version: "1.13.11".to_string(),
                protocol: 18,
                node_id: "ND_x".to_string(),
                ..Default::default()
            }),
            sif_trailer: vec![1, 2, 3],
            fast_publish: true,
            ..Default::default()
        })),
        "join_response"
    );

    assert_eq!(
        Some(
            "2a380a120a0450415f321203626f62180350078001010a220a0450415f3312056361726f6c2a077b\
             2278223a317d680170047a060a016b120176"
                .to_string()
        ),
        encoded(s.signal_participant_update(vec![
            livekit::ParticipantInfo {
                sid: "PA_2".to_string(),
                identity: "bob".to_string(),
                state: participant_info::State::Disconnected as i32,
                version: 7,
                disconnect_reason: livekit::DisconnectReason::ClientInitiated as i32,
                ..Default::default()
            },
            livekit::ParticipantInfo {
                sid: "PA_3".to_string(),
                identity: "carol".to_string(),
                metadata: "{\"x\":1}".to_string(),
                is_publisher: true,
                kind: participant_info::Kind::Agent as i32,
                attributes: HashMap::from([("k".to_string(), "v".to_string())]),
                ..Default::default()
            },
        ])),
        "participant_update"
    );
    assert_eq!(
        None,
        encoded(s.signal_participant_update(Vec::new())),
        "participant_update_empty"
    );

    assert_eq!(
        Some("521c0a0d0a0450415f31150000403f18010a0b0a0450415f32150000003e".to_string()),
        encoded(s.signal_speaker_update(vec![
            livekit::SpeakerInfo {
                sid: "PA_1".to_string(),
                level: 0.75,
                active: true,
            },
            livekit::SpeakerInfo {
                sid: "PA_2".to_string(),
                level: 0.125,
                ..Default::default()
            },
        ])),
        "speaker_update"
    );
    assert_eq!(
        None,
        encoded(s.signal_speaker_update(Vec::new())),
        "speaker_update_empty"
    );

    assert_eq!(
        Some("5a180a160a06524d5f6162631206612d726f6f6d42046d657461".to_string()),
        encoded(s.signal_room_update(livekit::Room {
            sid: "RM_abc".to_string(),
            name: "a-room".to_string(),
            metadata: "meta".to_string(),
            ..Default::default()
        })),
        "room_update"
    );

    assert_eq!(
        Some("82010d612e746f6b656e2e76616c7565".to_string()),
        encoded(s.signal_refresh_token("a.token.value".to_string())),
        "refresh_token"
    );

    assert_eq!(
        Some(
            "1a3a0a056f666665721224763d300d0a6f3d2d2031203220494e20495034203132372e302e302e31\
             0d0a733d2d0d0a180422090a0130120454525f61"
                .to_string()
        ),
        encoded(s.signal_sdp_offer(livekit::SessionDescription {
            r#type: "offer".to_string(),
            sdp: "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\n".to_string(),
            id: 4,
            mid_to_track_id: HashMap::from([("0".to_string(), "TR_a".to_string())]),
        })),
        "sdp_offer"
    );

    assert_eq!(
        Some(
            "4226080110031802221e0a1c0a02723112147773733a2f2f72312e6578616d706c652e6f7267180c"
                .to_string()
        ),
        encoded(s.signal_leave_request(livekit::LeaveRequest {
            can_reconnect: true,
            reason: livekit::DisconnectReason::ServerShutdown as i32,
            action: leave_request::Action::Reconnect as i32,
            regions: Some(livekit::RegionSettings {
                regions: vec![livekit::RegionInfo {
                    region: "r1".to_string(),
                    url: "wss://r1.example.org".to_string(),
                    distance: 12,
                }],
            }),
        })),
        "leave_request"
    );
}
