// New for musivum: the generated code of the original has no tests. What the bus does in
// place of psrpc's: a call reaches the server registered for its topic, a topic without one
// gives psrpc's `ErrNoResponse`, and taking a replaced registration away leaves its
// successor.

use std::sync::Arc;

use super::*;
use crate::protocol::livekit;
use crate::routing::new_local_message_bus;

#[test]
fn topics_are_formatted_as_upstream() {
    let f = new_topic_formatter();
    assert_eq!(
        ParticipantTopic("room_alice".into()),
        f.participant_topic(&"room".into(), &"alice".into())
    );
    assert_eq!(RoomTopic("room".into()), f.room_topic(&"room".into()));
}

#[test]
fn a_call_reaches_the_server_of_its_topic() {
    let bus = new_local_message_bus();
    let rooms = new_typed_room_client(bus.clone()).unwrap();
    let participants = new_typed_participant_client(bus.clone()).unwrap();

    let room_server = Arc::new(FakeRoomServerImpl::default());
    room_server.update_room_metadata_returns(Ok(livekit::Room {
        name: "r".into(),
        metadata: "m".into(),
        ..Default::default()
    }));
    bus.register_room_server(&format_room_topic(&"r".into()), room_server.clone());

    let participant_server = Arc::new(FakeParticipantServerImpl::default());
    participant_server.perform_rpc_returns(Err(PsrpcError::new(
        PsrpcErrorCode::NotFound,
        "participant does not exist",
    )));
    bus.register_participant_server(
        &format_participant_topic(&"r".into(), &"p".into()),
        participant_server.clone(),
    );

    let req = livekit::UpdateRoomMetadataRequest {
        room: "r".into(),
        metadata: "m".into(),
    };
    let room = rooms
        .update_room_metadata(&RoomTopic("r".into()), &req)
        .unwrap();
    assert_eq!("m", room.metadata);
    assert_eq!(req, room_server.update_room_metadata_args_for_call(0));

    let err = participants
        .perform_rpc(
            &ParticipantTopic("r_p".into()),
            &livekit::PerformRpcRequest::default(),
            &[],
        )
        .unwrap_err();
    assert_eq!(PsrpcErrorCode::NotFound, err.code());
    assert_eq!("participant does not exist", err.to_string());

    // no server for the topic
    let err = rooms
        .send_data(&RoomTopic("other".into()), &Default::default())
        .unwrap_err();
    assert_eq!(PsrpcError::no_response(), err);
    let err = participants
        .remove_participant(&ParticipantTopic("r_q".into()), &Default::default())
        .unwrap_err();
    assert_eq!(PsrpcErrorCode::Unavailable, err.code());
    assert_eq!("no response from servers", err.to_string());
    assert_eq!(0, room_server.send_data_call_count());
    assert_eq!(0, participant_server.remove_participant_call_count());
}

#[test]
fn killing_a_replaced_server_leaves_its_successor() {
    let bus = new_local_message_bus();
    let participants = new_typed_participant_client(bus.clone()).unwrap();
    let topic = ParticipantTopic("r_p".into());

    let first = Arc::new(FakeParticipantServerImpl::default());
    let second = Arc::new(FakeParticipantServerImpl::default());
    let first_id = bus.register_participant_server(&topic, first.clone());
    let second_id = bus.register_participant_server(&topic, second.clone());
    assert_ne!(first_id, second_id);

    bus.deregister_participant_server(&topic, first_id);
    participants
        .update_subscriptions(&topic, &Default::default())
        .unwrap();
    assert_eq!(0, first.update_subscriptions_call_count());
    assert_eq!(1, second.update_subscriptions_call_count());

    bus.deregister_participant_server(&topic, second_id);
    assert_eq!(
        Err(PsrpcError::no_response()),
        participants.update_subscriptions(&topic, &Default::default())
    );

    let room = RoomTopic("r".into());
    let id = bus.register_room_server(&room, Arc::new(FakeRoomServerImpl::default()));
    bus.deregister_room_server(&room, id);
    assert_eq!(
        Err(PsrpcError::no_response()),
        new_typed_room_client(bus.clone())
            .unwrap()
            .delete_room(&room, &Default::default())
    );
}

#[test]
fn request_options_apply_as_in_psrpc() {
    let mut ro = RequestOpts::default();
    let opts = vec![with_request_timeout(crate::go::time::SECOND)];
    for o in &opts {
        o.apply(&mut ro);
    }
    assert_eq!(crate::go::time::SECOND, ro.timeout);
}
