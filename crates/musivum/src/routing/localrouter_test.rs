//! Tests of the local router, new for musivum: the original has none of it, and none of its
//! signal client but `TestSignal` of `pkg/service`, which needs the service's signal server.
//!
//! A session started through the local router reaches the signal server of the node, which
//! the room manager's side registers on the bus, with the `StartSession` and the session's
//! source of requests and sink of responses; what either side writes arrives at the other,
//! and closing either sink ends the session for both. The expected values and orders are
//! those the same steps gave through the original's local router, its signal client and the
//! signal server of `pkg/service` over psrpc's local bus, with the service's fake session
//! handler in place of the room manager (a Go test laid over `pkg/routing` with
//! `go test -overlay`, run with `scripts/go-reference-test.sh`). Where the original differs,
//! the test says so: a write to a closed stream is `ChannelClosed` here and psrpc's
//! "stream closed" there, and a node without a server is refused at once here and after
//! psrpc's timeout of three seconds there.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use super::*;
use crate::go::atomic;
use crate::protocol::auth::{ClaimGrants, VideoGrant};
use crate::routing::{
    FakeRoomManagerClient, LocalNodeImpl, Message, MessageBus, MessageSink, MessageSource,
    RoomManagerServerImpl, SignalServerImpl, create_router, new_local_message_bus,
    new_room_manager_client, new_signal_client, participant_init_from_start_session,
};

struct Handed {
    ss: livekit::StartSession,
    src: Arc<dyn MessageSource>,
    sink: Arc<dyn MessageSink>,
}

// The room manager's side: what `pkg/service`'s signal server hands to its session handler.
#[derive(Default)]
struct TestSignalServer {
    handed: Mutex<Vec<Handed>>,
    fail: atomic::Bool,
}

impl SignalServerImpl for TestSignalServer {
    fn relay_signal(
        &self,
        start_session: livekit::StartSession,
        request_source: Arc<dyn MessageSource>,
        response_sink: Arc<dyn MessageSink>,
    ) -> Result<(), Error> {
        self.handed.lock().push(Handed {
            ss: start_session,
            src: request_source,
            sink: response_sink.clone(),
        });
        if self.fail.load() {
            response_sink.write_message(pong(9)).unwrap();
            // the server half closes its sink when the handler fails
            response_sink.close();
            return Err(Error::IncorrectRtcNode);
        }
        Ok(())
    }
}

impl TestSignalServer {
    fn last(&self) -> Handed {
        let handed = self.handed.lock();
        let h = handed.last().unwrap();
        Handed {
            ss: h.ss.clone(),
            src: h.src.clone(),
            sink: h.sink.clone(),
        }
    }
}

#[derive(Default)]
struct TestRoomManagerServer {
    requests: Mutex<Vec<livekit::CreateRoomRequest>>,
}

impl RoomManagerServerImpl for TestRoomManagerServer {
    fn create_room(&self, req: &livekit::CreateRoomRequest) -> Result<livekit::Room, Error> {
        self.requests.lock().push(req.clone());
        Ok(livekit::Room {
            name: req.name.clone(),
            sid: "RM_test".to_string(),
            ..Default::default()
        })
    }
}

fn ping(n: i64) -> Message {
    livekit::SignalRequest {
        message: Some(livekit::signal_request::Message::Ping(n)),
    }
    .into()
}

fn pong(n: i64) -> Message {
    livekit::SignalResponse {
        message: Some(livekit::signal_response::Message::Pong(n)),
    }
    .into()
}

#[derive(Debug, PartialEq)]
enum Read {
    Message(Message),
    Closed,
    Nothing,
}

async fn read(src: &Arc<dyn MessageSource>) -> Read {
    match tokio::time::timeout(Duration::from_millis(500), src.read_chan().recv()).await {
        Ok(Some(msg)) => Read::Message(msg),
        Ok(None) => Read::Closed,
        Err(_) => Read::Nothing,
    }
}

fn setup() -> (
    Arc<LocalRouter>,
    Arc<dyn SignalClient>,
    Arc<TestSignalServer>,
    Arc<MessageBus>,
) {
    let node = LocalNodeImpl::new(None).unwrap();
    node.set_node_id(&"ND_test".into());
    let bus = new_local_message_bus();
    let signal_client = new_signal_client(node.node_id(), bus.clone()).unwrap();
    let router = LocalRouter::new(
        node.clone(),
        signal_client.clone(),
        Arc::new(FakeRoomManagerClient::default()),
        NodeStatsConfig::default(),
    );
    let server = Arc::new(TestSignalServer::default());
    bus.register_signal_server(&node.node_id(), server.clone());
    (router, signal_client, server, bus)
}

fn participant_init() -> ParticipantInit {
    ParticipantInit {
        identity: "alice".into(),
        name: "Alice".into(),
        region: "elsewhere".to_string(),
        grants: Some(ClaimGrants {
            identity: "alice".to_string(),
            video: Some(VideoGrant {
                room_join: true,
                room: "room1".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        create_room: Some(livekit::CreateRoomRequest {
            name: "room1".to_string(),
            empty_timeout: 30,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_started_through_the_local_router_reaches_the_signal_server() {
    let (router, signal_client, server, _bus) = setup();
    let room1 = livekit::RoomName::from("room1");

    // 1. a session, closed by the client
    let res = router
        .start_participant_signal(&room1, participant_init())
        .unwrap();
    assert_eq!(livekit::NodeId::from("ND_test"), res.node_id);
    assert_eq!("", res.node_selection_reason);
    let g = server.last();
    let req_sink = res.request_sink.clone().unwrap();
    let res_source = res.response_source.clone().unwrap();
    assert_eq!(res.connection_id.as_str(), g.ss.connection_id);
    assert!(res.connection_id.as_str().starts_with("CO_"));
    assert_eq!(res.connection_id, g.sink.connection_id());
    assert_eq!(res.connection_id, g.src.connection_id());
    assert_eq!(res.connection_id, req_sink.connection_id());
    assert_eq!(res.connection_id, res_source.connection_id());

    // what the server reads from the `StartSession`, with its own region
    assert_eq!("room1", g.ss.room_name);
    let pi = participant_init_from_start_session(&g.ss, "region1").unwrap();
    assert_eq!(livekit::ParticipantIdentity::from("alice"), pi.identity);
    assert_eq!(livekit::ParticipantName::from("Alice"), pi.name);
    assert_eq!("region1", pi.region);
    let create_room = pi.create_room.as_ref().unwrap();
    assert_eq!(
        ("room1", 30),
        (create_room.name.as_str(), create_room.empty_timeout)
    );
    let grants = pi.grants.as_ref().unwrap();
    assert_eq!("alice", grants.identity);
    assert_eq!("room1", grants.video.as_ref().unwrap().room);
    assert_eq!(livekit::ParticipantId::from(""), pi.id);
    assert_eq!(1, signal_client.active_count());

    assert_eq!(Ok(()), req_sink.write_message(ping(1)));
    assert_eq!(Read::Message(ping(1)), read(&g.src).await);
    assert_eq!(Ok(()), g.sink.write_message(pong(2)));
    assert_eq!(Read::Message(pong(2)), read(&res_source).await);
    assert_eq!(Ok(()), req_sink.write_message(ping(3)));
    req_sink.close();
    assert!(req_sink.is_closed());
    assert_eq!(Read::Message(ping(3)), read(&g.src).await);
    assert_eq!(Read::Closed, read(&g.src).await);
    assert_eq!(Read::Closed, read(&res_source).await);
    assert!(res_source.is_closed() && g.src.is_closed() && g.sink.is_closed());
    // "stream closed" in the original
    assert_eq!(Err(Error::ChannelClosed), req_sink.write_message(ping(4)));
    assert_eq!(Err(Error::ChannelClosed), g.sink.write_message(pong(5)));
    assert_eq!(0, signal_client.active_count());

    // 2. a session, closed by the server
    let res = router
        .start_participant_signal(&room1, participant_init())
        .unwrap();
    let g = server.last();
    let (req_sink, res_source) = (res.request_sink.unwrap(), res.response_source.unwrap());
    assert_eq!(Ok(()), g.sink.write_message(pong(6)));
    g.sink.close();
    assert_eq!(Read::Message(pong(6)), read(&res_source).await);
    assert_eq!(Read::Closed, read(&res_source).await);
    assert_eq!(Read::Closed, read(&g.src).await);
    assert!(req_sink.is_closed() && res_source.is_closed() && g.src.is_closed());
    assert_eq!(Err(Error::ChannelClosed), req_sink.write_message(ping(7)));
    assert_eq!(0, signal_client.active_count());

    // 3. the session handler fails after writing a response: the start succeeds, and the
    // client reads the response before the end
    server.fail.store(true);
    let res = router
        .start_participant_signal(&room1, participant_init())
        .unwrap();
    let g = server.last();
    let (req_sink, res_source) = (res.request_sink.unwrap(), res.response_source.unwrap());
    assert_eq!(Read::Message(pong(9)), read(&res_source).await);
    assert_eq!(Read::Closed, read(&res_source).await);
    assert_eq!(Read::Closed, read(&g.src).await);
    assert!(req_sink.is_closed() && res_source.is_closed() && g.src.is_closed());
    assert!(g.sink.is_closed());
    assert_eq!(0, signal_client.active_count());

    // 4. the request source closed by the server side alone ends nothing else
    server.fail.store(false);
    let res = router
        .start_participant_signal(&room1, participant_init())
        .unwrap();
    let g = server.last();
    let (req_sink, res_source) = (res.request_sink.unwrap(), res.response_source.unwrap());
    g.src.close();
    assert!(!req_sink.is_closed() && !res_source.is_closed() && !g.sink.is_closed());
    assert_eq!(Ok(()), req_sink.write_message(ping(8)));
    assert_eq!(Ok(()), g.sink.write_message(pong(8)));
    assert_eq!(Read::Message(pong(8)), read(&res_source).await);
    assert_eq!(1, signal_client.active_count());
    req_sink.close();
    assert_eq!(0, signal_client.active_count());

    // 5. a node without a signal server (after three seconds in the original)
    let err = router
        .start_participant_signal_with_node_id(&room1, participant_init(), &"ND_other".into())
        .err()
        .unwrap();
    assert_eq!("request timed out", err.to_string());
    assert_eq!(4, server.handed.lock().len());

    // 6. the local router's node
    let n = router.get_node_for_room(&room1).unwrap();
    assert_eq!("ND_test", n.id);
    assert_eq!("", router.get_region());
    assert_eq!(livekit::NodeState::Serving, n.state());
    let err = router.get_node(&"ND_other".into()).unwrap_err();
    assert_eq!("could not find object", err.to_string());
    router.drain();
    let n = router.get_node_for_room(&room1).unwrap();
    assert_eq!(livekit::NodeState::ShuttingDown, n.state());
}

// New for musivum: a room is created through the room manager registered on the bus for the
// router's node, and the local router answers for its one node.
#[test]
fn a_room_is_created_by_the_room_manager_of_the_node() {
    let (_, signal_client, _, bus) = setup();
    let node = LocalNodeImpl::new(None).unwrap();
    node.set_node_id(&"ND_test".into());
    let room_manager = Arc::new(TestRoomManagerServer::default());
    let room_manager_client =
        new_room_manager_client(bus.clone(), crate::config::RoomConfig::default()).unwrap();
    let router = create_router(
        node.clone(),
        signal_client,
        room_manager_client,
        NodeStatsConfig::default(),
    );

    let req = livekit::CreateRoomRequest {
        name: "room1".to_string(),
        ..Default::default()
    };
    assert_eq!(
        Err(Error::RequestTimedOut),
        router.create_room(&req).map(|_| ())
    );
    bus.register_room_manager_server(&node.node_id(), room_manager.clone());
    let room = router.create_room(&req).unwrap();
    assert_eq!(
        ("room1", "RM_test"),
        (room.name.as_str(), room.sid.as_str())
    );
    assert_eq!(vec![req.clone()], *room_manager.requests.lock());
    bus.deregister_room_manager_server(&node.node_id());
    assert_eq!(
        Err(Error::RequestTimedOut),
        router.create_room(&req).map(|_| ())
    );

    assert_eq!(1, router.list_nodes().unwrap().len());
    assert_eq!(
        Ok(()),
        router.set_node_for_room(&"room1".into(), &"ND_x".into())
    );
    assert_eq!(Ok(()), router.clear_room_state(&"room1".into()));
    assert_eq!(Ok(()), router.register_node());
    assert_eq!(Ok(()), router.unregister_node());
    assert_eq!(Ok(()), router.remove_dead_nodes());
}
